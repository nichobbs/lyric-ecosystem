//! tests/client_tests.l - the ActiveMQ backend against an in-process STOMP
//! broker on a loopback socket.
//!
//! `FakeBroker` below speaks enough STOMP 1.2 to stand in for ActiveMQ: it
//! authenticates, stores sent messages per destination, dispatches them to
//! the connection's subscription, tracks unacknowledged deliveries, applies
//! transactions on COMMIT, drops a NACKed message and returns unsettled
//! deliveries (flagged `redelivered`) when a consumer leaves. It ignores
//! selectors and dispatches only to a subscription on the connection that
//! just sent a frame, which is all a single-connection queue needs.
//!
//! A few destinations and logins trigger failures on purpose:
//!   passcode `wrong`              ERROR on CONNECT, then the socket closes
//!   login `ancient`               CONNECTED with `version:1.0`
//!   /queue/fail.error             SEND is answered with an ERROR frame
//!   /queue/fail.drop              SEND makes the broker close the socket
//!   /queue/fail.silent            SEND is accepted but never receipted
//!   /queue/fail.dropconsume       SUBSCRIBE is receipted, then the socket closes
//!   /queue/fail.stray             SEND is answered with an ERROR frame without a receipt-id
//!   /queue/slow.send              SEND is receipted after 2.5 s, and not stored
//!   /queue/slow.subscribe.*       SUBSCRIBE is receipted after 2.5 s
//!   /queue/flaky.*                the first ACK, NACK or COMMIT of an ACK is refused with an
//!                                 ERROR frame; the delivery stays unsettled
//!   login `stall`                 CONNECTED, then nothing is read for 8 s, then the socket closes
//!   header `fake-pad:N`           the MESSAGE carries a `padding` header of N characters
//!   /queue/fail.storedrop.*       SEND is stored, then the socket closes without a receipt
//!
//! For the heart-beat and reconnect tests a broker of its own is started
//! (`startKnobBroker`) and steered through its fields: the `heart-beat` it
//! answers with, whether its sessions send heart-beats, whether it refuses
//! logins or subscriptions, and `dropConnections`, which closes every open
//! connection the way a failing network does. It counts the logins,
//! subscriptions and client heart-beats it sees.
//!
//! The broker is started three times: on plain TCP, behind TLS, and behind
//! TLS with a required client certificate. The TLS ones use the TEST-ONLY
//! certificates at the end of this file.
//!
//! The same suite runs against a real ActiveMQ in
//! `tests/integration/activemq_integration_tests.l`.
@test_module
package Mq.ActiveMq.ClientTests

import Std.Core
import Std.Collections
import Std.Testing
import Std.Encoding
import Std.Parse
import Std.String as Str
import Std.Environment as Env
import Std.File as File
import Std.Tls as Tls
import Testing.Conformance
import Testing.Conformance.Mq as MqConformance
import Mq
import Mq.ActiveMq as ActiveMq
import Mq.ActiveMq.Extras as Extras
import Net.Tcp as K
import Net.Tcp.Testing as KT
import Mq.ActiveMq.Stomp as Stomp

// ─── The fake broker ─────────────────────────────────────────────────────────
// How long the slow destinations delay their answer, and how long a stalled
// connection stays open without being read.
val SLOW_MS: Int = 2500

val STALL_MS: Int = 8000

record Stored {
  headers: slice[Stomp.Header]
  body: slice[Byte]
  redelivered: Bool
}

ref record FakeBroker {
  server: KT.ServerSock
  // Whether every accepted connection starts with a TLS handshake.
  secured: Bool
  tls: KT.ServerTls
  lock: K.Lock
  var queues: Map[String, List[Stored]]
  var counter: Int
  // The `heart-beat` header of CONNECTED.
  var heartBeat: String
  // Every session writes a heart-beat this often (0 = never), while
  // `beating`.
  var beatEveryMs: Int
  var beating: Bool
  var refuseLogins: Bool
  var refuseSubscribes: Bool
  // What the broker saw: CONNECT frames, the `heart-beat` header of the last
  // one, SUBSCRIBE frames, heart-beat bytes from clients.
  var logins: Int
  var lastOffer: String
  var subscribes: Int
  var beatsSeen: Int
  // Stalled connections whose pause has run out.
  var stallsEnded: Int
  var sessions: List[Session]
}

ref record Session {
  broker: FakeBroker
  sock: K.Sock
  // Serializes writes: replies and heart-beats come from two threads.
  writeLock: K.Lock
  var alive: Bool
  var subscriptionId: String
  var destination: String
  var unacked: Map[String, Stored]
  var unackedOrder: List[String]
  var txSends: Map[String, List[Stomp.Frame]]
  var txAcks: Map[String, List[String]]
  // Settlements still to be refused with an ERROR frame (`/queue/flaky.*`).
  var refuseLeft: Int
}

func h(name: in String, value: in String): Stomp.Header {
  Stomp.Header(name = name, value = value)
}

func sessionWrite(s: in Session, bytes: in slice[Byte]): Unit {
  K.lockEnter(s.writeLock)
  val _ = K.sockWrite(s.sock, bytes)
  K.lockExit(s.writeLock)
}

func reply(s: in Session, command: in String, headers: in slice[Stomp.Header], body: in String): Unit {
  match Stomp.encodeFrame(Stomp.textFrame(command, headers, body)) {
    case Ok(bytes) -> sessionWrite(s, bytes)
    case Err(_) -> ()
  }
}

func sessionAlive(s: in Session): Bool {
  K.lockEnter(s.writeLock)
  val alive = s.alive
  K.lockExit(s.writeLock)
  alive
}

// The session's heart-beats, on a thread of their own.
func beatLoop(s: in Session, everyMs: in Int): Unit {
  val b = s.broker
  while sessionAlive(s) {
    K.lockEnter(b.lock)
    val beating = b.beating
    K.lockExit(b.lock)
    if beating {
      sessionWrite(s, encodeUtf8("\n"))
    }
    K.sleepMs(everyMs)
  }
}

// Close every open connection without a word, as a failing network does.
func dropConnections(b: in FakeBroker): Unit {
  K.lockEnter(b.lock)
  val open = b.sessions.toArray()
  K.lockExit(b.lock)
  for s in open {
    K.sockClose(s.sock)
  }
}

func receipt(s: in Session, frame: in Stomp.Frame): Unit {
  match Stomp.headerValue(frame, "receipt") {
    case Some(id) -> reply(s, "RECEIPT", [h("receipt-id", id)], "")
    case None -> ()
  }
}

func brokerError(s: in Session, frame: in Stomp.Frame, message: in String): Unit {
  val headers: List[Stomp.Header] = newList()
  headers.add(h("message", message))
  match Stomp.headerValue(frame, "receipt") {
    case Some(id) -> headers.add(h("receipt-id", id))
    case None -> ()
  }
  reply(s, "ERROR", headers.toArray(), message + "\n\tat fake.Broker.handle(Broker.java:1)")
}

func queueOf(b: in FakeBroker, destination: in String): List[Stored] {
  if not b.queues.containsKey(destination) {
    val fresh: List[Stored] = newList()
    b.queues[destination] = fresh
  }
  b.queues[destination]
}

func enqueue(b: in FakeBroker, frame: in Stomp.Frame): Unit {
  val kept: List[Stomp.Header] = newList()
  for header in frame.headers {
    if header.name != "receipt" and header.name != "transaction" and header.name != "content-length" {
      kept.add(header)
    }
  }
  K.lockEnter(b.lock)
  queueOf(b, Stomp.headerOr(frame, "destination", "")).add(
    Stored(headers = kept.toArray(), body = frame.body, redelivered = false)
  )
  K.lockExit(b.lock)
}

// `count` characters of text.
func filler(count: in Int): String {
  var text = "x"
  while text.length < count {
    text = text + text
  }
  Str.substring(text, 0, count)
}

// The size a `fake-pad` header asks for, 0 without one.
func padOf(stored: in Stored): Int {
  for header in stored.headers {
    if header.name == "fake-pad" {
      return match parseOptInt(header.value) { case Some(n) -> n; case None -> 0 }
    }
  }
  0
}

// Dispatch everything waiting on the session's subscribed destination.
func pump(s: in Session): Unit {
  if s.subscriptionId.length == 0 {
    return
  }
  val b = s.broker
  K.lockEnter(b.lock)
  val waiting = queueOf(b, s.destination)
  while waiting.count > 0 {
    val stored = waiting[0]
    waiting.removeAt(0)
    b.counter = b.counter + 1
    val ackId = "fake-ack:" + b.counter.toString()
    val headers: List[Stomp.Header] = newList()
    for header in stored.headers {
      headers.add(header)
    }
    headers.add(h("ack", ackId))
    headers.add(h("subscription", s.subscriptionId))
    headers.add(h("message-id", "ID:fake:" + b.counter.toString()))
    val pad = padOf(stored)
    if pad > 0 {
      headers.add(h("padding", filler(pad)))
    }
    if stored.redelivered {
      headers.add(h("redelivered", "true"))
    }
    s.unacked[ackId] = stored
    s.unackedOrder.add(ackId)
    match Stomp.encodeFrame(Stomp.Frame(command = "MESSAGE", headers = headers.toArray(), body = stored.body)) {
      case Ok(bytes) -> sessionWrite(s, bytes)
      case Err(_) -> ()
    }
  }
  K.lockExit(b.lock)
}

// Put the session's unsettled deliveries back at the head of their queue.
func returnUnacked(s: in Session): Unit {
  if s.unackedOrder.count == 0 {
    return
  }
  val b = s.broker
  K.lockEnter(b.lock)
  val rebuilt: List[Stored] = newList()
  for ackId in s.unackedOrder.toArray() {
    if s.unacked.containsKey(ackId) {
      val stored = s.unacked[ackId]
      rebuilt.add(Stored(headers = stored.headers, body = stored.body, redelivered = true))
    }
  }
  for waiting in queueOf(b, s.destination).toArray() {
    rebuilt.add(waiting)
  }
  b.queues[s.destination] = rebuilt
  s.unacked = newMap()
  s.unackedOrder = newList()
  K.lockExit(b.lock)
}

func settleFake(s: in Session, frame: in Stomp.Frame, ackId: in String): Bool {
  K.lockEnter(s.broker.lock)
  val known = s.unacked.containsKey(ackId)
  if known {
    s.unacked.remove(ackId)
  }
  K.lockExit(s.broker.lock)
  if not known {
    brokerError(s, frame, "Unexpected ACK received for message-id [null]")
  }
  known
}

// Dispatch what waits on `destination` to the other connections subscribed
// to it: what a connection that ended gave back goes to a consumer that is
// already there, as on a real broker.
func pumpSubscribers(b: in FakeBroker, destination: in String): Unit {
  K.lockEnter(b.lock)
  val open = b.sessions.toArray()
  K.lockExit(b.lock)
  for other in open {
    K.lockEnter(b.lock)
    val subscribed = other.subscriptionId.length > 0 and other.destination == destination
    K.lockExit(b.lock)
    if subscribed {
      pump(other)
    }
  }
}

// Refuse a settlement of a `/queue/flaky.*` subscription, once.
func refuses(s: in Session, frame: in Stomp.Frame): Bool {
  if s.refuseLeft < 1 {
    return false
  }
  s.refuseLeft = s.refuseLeft - 1
  brokerError(s, frame, "simulated settlement failure")
  true
}

// Handle one frame. False ends the connection.
func handle(s: in Session, frame: in Stomp.Frame): Bool {
  val command = frame.command
  val tx = Stomp.headerOr(frame, "transaction", "")
  if command == "CONNECT" or command == "STOMP" {
    val b = s.broker
    K.lockEnter(b.lock)
    b.logins = b.logins + 1
    b.lastOffer = Stomp.headerOr(frame, "heart-beat", "")
    val refused = b.refuseLogins
    val heartBeat = b.heartBeat
    val beatEveryMs = b.beatEveryMs
    K.lockExit(b.lock)
    if refused or Stomp.headerOr(frame, "passcode", "") == "wrong" {
      reply(
        s,
        "ERROR",
        [h("message", "User name [" + Stomp.headerOr(frame, "login", "") + "] or password is invalid.")],
        ""
      )
      return false
    }
    val version = if Stomp.headerOr(frame, "login", "") == "ancient" { "1.0" } else { "1.2" }
    reply(s, "CONNECTED", [h("server", "FakeMQ/1.0"), h("version", version), h("heart-beat", heartBeat)], "")
    if beatEveryMs > 0 {
      val _ = KT.startBackground({ -> beatLoop(s, beatEveryMs) })
    }
    if Stomp.headerOr(frame, "login", "") == "stall" {
      K.sleepMs(STALL_MS)
      K.lockEnter(b.lock)
      b.stallsEnded = b.stallsEnded + 1
      K.lockExit(b.lock)
      return false
    }
    return true
  }
  if command == "SEND" {
    val destination = Stomp.headerOr(frame, "destination", "")
    if destination == "/queue/fail.error" {
      brokerError(s, frame, "simulated broker failure")
      return true
    }
    if destination == "/queue/fail.drop" {
      return false
    }
    if Str.startsWith(destination, "/queue/fail.storedrop.") {
      enqueue(s.broker, frame)
      return false
    }
    if destination == "/queue/fail.silent" {
      return true
    }
    if destination == "/queue/fail.stray" {
      reply(s, "ERROR", [h("message", "stray failure")], "")
      return true
    }
    if destination == "/queue/slow.send" {
      K.sleepMs(SLOW_MS)
      receipt(s, frame)
      return true
    }
    if tx.length > 0 {
      s.txSends[tx].add(frame)
    } else {
      enqueue(s.broker, frame)
    }
    receipt(s, frame)
    return true
  }
  if command == "SUBSCRIBE" {
    val b = s.broker
    K.lockEnter(b.lock)
    b.subscribes = b.subscribes + 1
    val refused = b.refuseSubscribes
    K.lockExit(b.lock)
    if refused {
      brokerError(s, frame, "User lyric is not authorized to read from this destination")
      return true
    }
    K.lockEnter(b.lock)
    s.subscriptionId = Stomp.headerOr(frame, "id", "")
    s.destination = Stomp.headerOr(frame, "destination", "")
    K.lockExit(b.lock)
    if Str.startsWith(s.destination, "/queue/slow.subscribe.") {
      K.sleepMs(SLOW_MS)
    }
    if Str.startsWith(s.destination, "/queue/flaky.") {
      s.refuseLeft = 1
    }
    receipt(s, frame)
    return s.destination != "/queue/fail.dropconsume"
  }
  if command == "UNSUBSCRIBE" {
    returnUnacked(s)
    s.subscriptionId = ""
    receipt(s, frame)
    return true
  }
  if command == "ACK" {
    val ackId = Stomp.headerOr(frame, "id", "")
    if tx.length > 0 {
      s.txAcks[tx].add(ackId)
      return true
    }
    if refuses(s, frame) {
      return true
    }
    if settleFake(s, frame, ackId) {
      receipt(s, frame)
    }
    return true
  }
  if command == "NACK" {
    if refuses(s, frame) {
      return true
    }
    if settleFake(s, frame, Stomp.headerOr(frame, "id", "")) {
      receipt(s, frame)
    }
    return true
  }
  if command == "BEGIN" {
    val sends: List[Stomp.Frame] = newList()
    val acks: List[String] = newList()
    s.txSends[tx] = sends
    s.txAcks[tx] = acks
    return true
  }
  if command == "COMMIT" {
    if s.txAcks[tx].count > 0 and refuses(s, frame) {
      s.txSends.remove(tx)
      s.txAcks.remove(tx)
      return true
    }
    for sent in s.txSends[tx].toArray() {
      enqueue(s.broker, sent)
    }
    for ackId in s.txAcks[tx].toArray() {
      val _ = settleFake(s, frame, ackId)
    }
    s.txSends.remove(tx)
    s.txAcks.remove(tx)
    receipt(s, frame)
    return true
  }
  if command == "ABORT" {
    s.txSends.remove(tx)
    s.txAcks.remove(tx)
    receipt(s, frame)
    return true
  }
  if command == "DISCONNECT" {
    receipt(s, frame)
    return false
  }
  brokerError(s, frame, "Unknown STOMP action: " + command)
  true
}

func serve(b: in FakeBroker, sock: in K.Sock): Unit {
  val s = Session(
    broker = b,
    sock = sock,
    writeLock = K.newLock(),
    alive = true,
    subscriptionId = "",
    destination = "",
    unacked = newMap(),
    unackedOrder = newList(),
    txSends = newMap(),
    txAcks = newMap(),
    refuseLeft = 0
  )
  K.lockEnter(b.lock)
  b.sessions.add(s)
  K.lockExit(b.lock)
  val decoder = Stomp.newDecoder(1048576)
  var open = true
  while open {
    match Stomp.nextFrame(decoder) {
      case Err(_) -> {
        open = false
      }
      case Ok(Some(frame)) -> {
        open = handle(s, frame)
        if open {
          pump(s)
        }
      }
      case Ok(None) -> match K.sockRead(sock, 16384, 60000) {
        case Ok(Data(bytes)) -> {
          // Line ends that arrive while no frame is under way are the
          // client's heart-beats.
          if Stomp.buffered(decoder) == 0 and onlyLineEnds(bytes) {
            K.lockEnter(b.lock)
            b.beatsSeen = b.beatsSeen + bytes.length
            K.lockExit(b.lock)
          }
          Stomp.feed(decoder, bytes)
        }
        case _ -> {
          open = false
        }
      }
    }
  }
  K.lockEnter(s.writeLock)
  s.alive = false
  K.lockExit(s.writeLock)
  K.lockEnter(b.lock)
  var at = 0
  while at < b.sessions.count {
    if b.sessions[at].writeLock == s.writeLock {
      b.sessions.removeAt(at)
    } else {
      at += 1
    }
  }
  K.lockExit(b.lock)
  returnUnacked(s)
  K.sockClose(sock)
  if s.destination.length > 0 {
    pumpSubscribers(b, s.destination)
  }
}

func onlyLineEnds(bytes: in slice[Byte]): Bool {
  for b in bytes {
    if b.toInt() != 10 and b.toInt() != 13 {
      return false
    }
  }
  true
}

// A connection whose handshake fails is dropped; the broker keeps listening.
func serveAccepted(b: in FakeBroker, accepted: in K.Sock): Unit {
  if not b.secured {
    serve(b, accepted)
    return
  }
  match KT.sockAcceptTls(accepted, b.tls, 5000) {
    case Ok(sock) -> serve(b, sock)
    case Err(_) -> ()
  }
}

func acceptLoop(b: in FakeBroker): Unit {
  var listening = true
  while listening {
    match KT.serverAccept(b.server) {
      case Ok(sock) -> {
        val _ = KT.startBackground({ -> serveAccepted(b, sock) })
      }
      case Err(_) -> {
        listening = false
      }
    }
  }
}

func startBrokerWith(secured: in Bool, tls: in KT.ServerTls): FakeBroker {
  val server = match KT.serverListen("127.0.0.1", 0) {
    case Ok(s) -> s
    case Err(e) -> panic("the fake broker cannot listen: " + e)
  }
  val b = FakeBroker(
    server = server,
    secured = secured,
    tls = tls,
    lock = K.newLock(),
    queues = newMap(),
    counter = 0,
    heartBeat = "0,0",
    beatEveryMs = 0,
    beating = true,
    refuseLogins = false,
    refuseSubscribes = false,
    logins = 0,
    lastOffer = "",
    subscribes = 0,
    beatsSeen = 0,
    stallsEnded = 0,
    sessions = newList()
  )
  val _ = KT.startBackground({ -> acceptLoop(b) })
  b
}

func serverTls(clientCaPem: in String): KT.ServerTls {
  KT.ServerTls(certPem = TEST_ONLY_SERVER_CERT, keyPem = TEST_ONLY_SERVER_KEY, clientCaPem = clientCaPem)
}

// One broker of each kind for the whole module, started by the first test
// that needs it.
ref record BrokerSlot {
  var broker: Option[FakeBroker]
  var tlsBroker: Option[FakeBroker]
  var mutualTlsBroker: Option[FakeBroker]
  var names: Int
}

val slot: BrokerSlot = BrokerSlot(broker = None, tlsBroker = None, mutualTlsBroker = None, names = 0)

func brokerPort(): Int {
  match slot.broker {
    case Some(b) -> KT.serverPort(b.server)
    case None -> {
      val b = startBrokerWith(false, serverTls(""))
      slot.broker = Some(value = b)
      KT.serverPort(b.server)
    }
  }
}

// The broker behind TLS (server certificate only).
func tlsBrokerPort(): Int {
  match slot.tlsBroker {
    case Some(b) -> KT.serverPort(b.server)
    case None -> {
      val b = startBrokerWith(true, serverTls(""))
      slot.tlsBroker = Some(value = b)
      KT.serverPort(b.server)
    }
  }
}

// The broker behind TLS that requires a client certificate from the CA.
func mutualTlsBrokerPort(): Int {
  match slot.mutualTlsBroker {
    case Some(b) -> KT.serverPort(b.server)
    case None -> {
      val b = startBrokerWith(true, serverTls(TEST_ONLY_CA))
      slot.mutualTlsBroker = Some(value = b)
      KT.serverPort(b.server)
    }
  }
}

func freshName(): String {
  slot.names = slot.names + 1
  "/queue/test." + slot.names.toString()
}

func optionsFor(destination: in String): ActiveMq.ConnectOptions {
  val base = ActiveMq.defaultOptions("127.0.0.1", brokerPort(), destination)
  base.copy(login = "lyric", passcode = "secret", requestTimeoutMs = 3000)
}

func open(options: in ActiveMq.ConnectOptions): ActiveMq.ActiveMqQueue {
  match ActiveMq.connectWith(options) {
    case Ok(q) -> q
    case Err(e) -> panic("connect failed: " + e)
  }
}

func openFresh(): ActiveMq.ActiveMqQueue {
  open(optionsFor(freshName()))
}

func message(id: in String, body: in String): Mq.Message {
  Mq.Message(id = id, body = body, headers = [], deliveryCount = 0)
}

func mustPublish(q: in ActiveMq.ActiveMqQueue, m: in Mq.Message): Unit {
  match q.publish(m) {
    case Ok(_) -> ()
    case Err(e) -> panic("publish failed: " + e)
  }
}

ref record Got {
  var messages: List[Mq.Message]
}

func keep(got: in Got, m: in Mq.Message): Result[Unit, String] {
  got.messages.add(m)
  Ok(())
}

func mustTake(q: in ActiveMq.ActiveMqQueue): Mq.Message {
  val got = Got(messages = newList())
  match q.consume(2000, { m -> keep(got, m) }) {
    case Ok(_) -> got.messages[0]
    case Err(e) -> {
      val status = ActiveMq.status(q)
      panic(
        "consume failed: " + e + " (reconnects: " + status.reconnects.toString() + ", lost because: \"" + status.lostBecause +
          "\")"
      )
    }
  }
}

func expectTimeout(q: in ActiveMq.ActiveMqQueue, label: in String): Unit {
  val got = Got(messages = newList())
  match q.consume(50, { m -> keep(got, m) }) {
    case Err(e) -> assertEqual(e, "timeout", label)
    case Ok(_) -> assertTrue(false, label + ": delivered " + got.messages[0].id)
  }
}

func errorOf(r: in Result[Unit, String], label: in String): String {
  match r {
    case Err(e) -> e
    case Ok(_) -> {
      assertTrue(false, label + ": expected an Err")
      ""
    }
  }
}

func expectContains(text: in String, part: in String, label: in String): Unit {
  assertTrue(Str.contains(text, part), label + ": got \"" + text + "\"")
}

// ─── Conformance ─────────────────────────────────────────────────────────────
func makeEnds(): MqConformance.QueueEnds {
  val q = openFresh()
  val publisher: Mq.MessageQueue = q
  val consumer: Mq.QueueConsumer = q
  MqConformance.QueueEnds(publisher = publisher, consumer = consumer)
}

test "the backend conforms to MessageQueue and QueueConsumer over the fake broker" {
  requireConformance(MqConformance.runQueue({ -> makeEnds() }))
}

// ─── Connecting ──────────────────────────────────────────────────────────────
test "connecting reports the broker's server header and destination" {
  val q = openFresh()
  assertEqual(ActiveMq.serverInfo(q), "FakeMQ/1.0", "server")
  assertTrue(Str.startsWith(ActiveMq.destination(q), "/queue/test."), "destination")
  assertTrue(ActiveMq.isOpen(q), "open")
  q.close()
  assertTrue(not ActiveMq.isOpen(q), "closed")
}

test "a refused connection is an Err naming the endpoint" {
  val server = match KT.serverListen("127.0.0.1", 0) { case Ok(s) -> s; case Err(e) -> panic(e) }
  val port = KT.serverPort(server)
  KT.serverClose(server)
  match ActiveMq.connectWith(ActiveMq.defaultOptions("127.0.0.1", port, "/queue/a")) {
    case Ok(_) -> assertTrue(false, "nothing listens there")
    case Err(e) -> expectContains(e, "activemq: cannot connect to 127.0.0.1:" + port.toString(), "refused")
  }
}

test "bad credentials surface the broker's ERROR frame" {
  val options = optionsFor(freshName())
  match ActiveMq.connectWith(options.copy(passcode = "wrong")) {
    case Ok(_) -> assertTrue(false, "the passcode is wrong")
    case Err(e) -> {
      expectContains(e, "activemq: broker error:", "prefix")
      expectContains(e, "password is invalid", "the broker's message")
      assertTrue(not Str.contains(e, "wrong"), "the passcode is not echoed")
    }
  }
}

test "a broker that only speaks STOMP 1.0 is refused" {
  val options = optionsFor(freshName())
  match ActiveMq.connectWith(options.copy(login = "ancient")) {
    case Ok(_) -> assertTrue(false, "version 1.0 must be refused")
    case Err(e) -> expectContains(e, "this client needs 1.2", "version")
  }
}

test "invalid options are rejected before any connection is made" {
  val good = ActiveMq.defaultOptions("127.0.0.1", 1, "/queue/a")
  expectContains(errorOfConnect(good.copy(host = "")), "no broker host", "host")
  expectContains(errorOfConnect(good.copy(port = 0)), "out of range", "port")
  expectContains(errorOfConnect(good.copy(destination = "orders")), "not a STOMP destination", "destination")
  expectContains(errorOfConnect(good.copy(prefetchSize = -1)), "prefetchSize", "prefetch")
  expectContains(errorOfConnect(good.copy(connectTimeoutMs = -1)), "connectTimeoutMs", "connect timeout")
  expectContains(errorOfConnect(good.copy(requestTimeoutMs = 0)), "requestTimeoutMs", "request timeout")
  expectContains(errorOfConnect(good.copy(maxFrameBytes = 0)), "maxFrameBytes", "frame limit")
}

func errorOfConnect(options: in ActiveMq.ConnectOptions): String {
  match ActiveMq.connectWith(options) {
    case Ok(q) -> {
      q.close()
      "connected"
    }
    case Err(e) -> e
  }
}

test "connectUrl resolves queue names and rejects bad input" {
  val url = "stomp://lyric:secret@127.0.0.1:" + brokerPort().toString()
  slot.names = slot.names + 1
  val name = "bare." + slot.names.toString()
  match ActiveMq.connectUrl(url, name) {
    case Ok(q) -> {
      assertEqual(ActiveMq.destination(q), "/queue/" + name, "a bare name is a queue")
      q.close()
    }
    case Err(e) -> assertTrue(false, e)
  }
  match ActiveMq.connectUrl(url, "/topic/news") {
    case Ok(q) -> {
      assertEqual(ActiveMq.destination(q), "/topic/news", "a destination is taken as written")
      q.close()
    }
    case Err(e) -> assertTrue(false, e)
  }
  match ActiveMq.connectUrl(url, "") {
    case Ok(_) -> assertTrue(false, "an empty queue name")
    case Err(e) -> expectContains(e, "no queue name", "empty queue name")
  }
  match ActiveMq.connectUrl("amqp://127.0.0.1", "q") {
    case Ok(_) -> assertTrue(false, "a non-STOMP URL")
    case Err(e) -> expectContains(e, "invalid broker URL", "bad URL")
  }
}

test "connectFromEnv reads the URL, queue name and ack mode" {
  Env.setVar("LYRIC_CONFIG_MQ_ACTIVEMQ_URL", "")
  Env.setVar("LYRIC_CONFIG_MQ_ACTIVEMQ_QUEUENAME", "")
  Env.setVar("LYRIC_CONFIG_MQ_ACTIVEMQ_ACKMODE", "")
  match ActiveMq.connectFromEnv() {
    case Ok(_) -> assertTrue(false, "no URL is configured")
    case Err(e) -> expectContains(e, "LYRIC_CONFIG_MQ_ACTIVEMQ_URL", "missing URL")
  }
  Env.setVar("LYRIC_CONFIG_MQ_ACTIVEMQ_URL", "stomp://lyric:secret@127.0.0.1:" + brokerPort().toString())
  match ActiveMq.connectFromEnv() {
    case Ok(_) -> assertTrue(false, "no queue name is configured")
    case Err(e) -> expectContains(e, "LYRIC_CONFIG_MQ_ACTIVEMQ_QUEUENAME", "missing queue name")
  }
  slot.names = slot.names + 1
  Env.setVar("LYRIC_CONFIG_MQ_ACTIVEMQ_QUEUENAME", "env." + slot.names.toString())
  Env.setVar("LYRIC_CONFIG_MQ_ACTIVEMQ_ACKMODE", "sometimes")
  match ActiveMq.connectFromEnv() {
    case Ok(_) -> assertTrue(false, "an unknown ack mode")
    case Err(e) -> expectContains(e, "ACKMODE", "bad ack mode")
  }
  Env.setVar("LYRIC_CONFIG_MQ_ACTIVEMQ_ACKMODE", "Auto")
  match ActiveMq.connectFromEnv() {
    case Err(e) -> assertTrue(false, e)
    case Ok(q) -> {
      mustPublish(q, message("env-1", "from env"))
      assertEqual(mustTake(q).body, "from env", "round trip")
      assertEqualInt(ActiveMq.unsettledCount(q), 0, "Auto mode acknowledged it")
      q.close()
    }
  }
  Env.setVar("LYRIC_CONFIG_MQ_ACTIVEMQ_URL", "")
  Env.setVar("LYRIC_CONFIG_MQ_ACTIVEMQ_QUEUENAME", "")
  Env.setVar("LYRIC_CONFIG_MQ_ACTIVEMQ_ACKMODE", "")
}

// ─── Broker failures ─────────────────────────────────────────────────────────
test "a broker ERROR frame fails the publish and leaves the connection usable" {
  val q = openFresh()
  val e = errorOf(
    ActiveMq.publishWithHeaders(q, message("m", "x"), [h("destination", "/queue/fail.error")]),
    "publish to the failing destination"
  )
  assertEqual(e, "activemq: broker error: simulated broker failure", "the ERROR frame's message header")
  assertTrue(ActiveMq.isOpen(q), "an ERROR frame alone does not close the connection")
  mustPublish(q, message("after", "still works"))
  assertEqual(mustTake(q).id, "after", "the connection still works")
  q.close()
}

// These look at a connection that stays lost, so reconnecting is off.
func lasting(options: in ActiveMq.ConnectOptions): ActiveMq.ConnectOptions {
  options.copy(reconnect = ActiveMq.noReconnect())
}

test "a broker that drops the connection during a publish" {
  val q = open(lasting(optionsFor(freshName())))
  val e = errorOf(
    ActiveMq.publishWithHeaders(q, message("m", "x"), [h("destination", "/queue/fail.drop")]),
    "publish while the broker hangs up"
  )
  expectContains(e, "activemq: connection lost:", "lost")
  assertTrue(not ActiveMq.isOpen(q), "the connection is marked lost")
  expectContains(errorOf(q.publish(message("n", "y")), "publish afterwards"), "connection lost", "later publish")
  expectContains(errorOf(q.consume(10, { m -> Ok(()) }), "consume afterwards"), "connection lost", "later consume")
  expectContains(errorOf(q.ack("m"), "ack afterwards"), "connection lost", "later ack")
  q.close()
  q.close()
}

test "a broker that drops the connection in the middle of a consume" {
  val q = open(lasting(optionsFor("/queue/fail.dropconsume")))
  val e = errorOf(q.consume(2000, { m -> Ok(()) }), "consume while the broker hangs up")
  assertTrue(e != "timeout", "a lost connection is not a timeout")
  expectContains(e, "activemq: connection lost: the broker closed the connection", "lost")
  q.close()
}

test "a broker that never confirms a publish times the request out" {
  val options = lasting(optionsFor(freshName()))
  val q = open(options.copy(requestTimeoutMs = 150))
  val e = errorOf(
    ActiveMq.publishWithHeaders(q, message("m", "x"), [h("destination", "/queue/fail.silent")]),
    "publish without a receipt"
  )
  expectContains(e, "did not answer within 150 ms", "request timeout")
  assertTrue(not ActiveMq.isOpen(q), "an unconfirmed request leaves the connection unusable")
  q.close()
}

// ─── Settlement ──────────────────────────────────────────────────────────────
test "operations on a closed connection are errors and close is idempotent" {
  val q = openFresh()
  q.close()
  q.close()
  expectContains(errorOf(q.publish(message("m", "x")), "publish"), "connection is closed", "publish")
  expectContains(errorOf(q.consume(0, { m -> Ok(()) }), "consume"), "connection is closed", "consume")
  expectContains(errorOf(q.ack("m"), "ack"), "connection is closed", "ack")
  expectContains(errorOf(q.nack("m", true), "nack"), "connection is closed", "nack")
  expectContains(errorOf(ActiveMq.subscribe(q), "subscribe"), "connection is closed", "subscribe")
}

test "settling an unknown or already settled id is an Err" {
  val q = openFresh()
  expectContains(errorOf(q.ack("nope"), "ack unknown"), "no unsettled delivery has message id 'nope'", "unknown id")
  mustPublish(q, message("m", "x"))
  val got = mustTake(q)
  assertEqualInt(ActiveMq.unsettledCount(q), 1, "one delivery in flight")
  assertTrue(isOk(q.ack(got.id)), "first ack")
  assertEqualInt(ActiveMq.unsettledCount(q), 0, "settled")
  assertTrue(isErr(q.ack(got.id)), "second ack of the same delivery")
  assertTrue(isErr(q.nack(got.id, true)), "nack after ack")
  q.close()
}

test "two deliveries with the same id settle oldest first" {
  val q = openFresh()
  mustPublish(q, message("dup", "first"))
  mustPublish(q, message("dup", "second"))
  assertEqual(mustTake(q).body, "first", "first delivery")
  assertEqual(mustTake(q).body, "second", "second delivery")
  assertEqualInt(ActiveMq.unsettledCount(q), 2, "both in flight")
  assertTrue(isOk(q.nack("dup", true)), "requeue the oldest")
  assertTrue(isOk(q.ack("dup")), "ack the other")
  val again = mustTake(q)
  assertEqual(again.body, "first", "the requeued one is the first delivery")
  assertEqualInt(again.deliveryCount, 1, "its count went up")
  assertTrue(isOk(q.ack("dup")), "ack the redelivery")
  expectTimeout(q, "nothing is left")
  q.close()
}

test "a requeue keeps the body and headers and counts every round" {
  val q = openFresh()
  val body = "payload \u{0000} with NUL, \"quotes\", café 世\nline2"
  val sent = Mq.Message(
    id = "r:1",
    body = body,
    headers = [
      Mq.MessageHeader(key = "content-type", value = "text/plain"),
      Mq.MessageHeader(key = "k:colon\nnewline", value = "v\\backslash"),
      Mq.MessageHeader(key = "content-type", value = "duplicate key kept")
    ],
    deliveryCount = 0
  )
  mustPublish(q, sent)
  var round = 0
  while round < 3 {
    val got = mustTake(q)
    assertEqual(got.id, "r:1", "id in round " + round.toString())
    assertEqual(got.body, body, "body in round " + round.toString())
    assertEqualInt(got.deliveryCount, round, "deliveryCount in round " + round.toString())
    assertEqualInt(got.headers.length, 3, "header count in round " + round.toString())
    assertEqual(got.headers[1].key, "k:colon\nnewline", "an awkward header key")
    assertEqual(got.headers[1].value, "v\\backslash", "an awkward header value")
    assertEqual(got.headers[2].value, "duplicate key kept", "a repeated key keeps its place")
    assertTrue(isOk(q.nack(got.id, true)), "requeue")
    round += 1
  }
  val last = mustTake(q)
  assertEqualInt(last.deliveryCount, 3, "after three requeues")
  assertTrue(isOk(q.nack(last.id, false)), "reject")
  expectTimeout(q, "a rejected message is gone")
  q.close()
}

test "a published deliveryCount is carried to the consumer" {
  val q = openFresh()
  mustPublish(q, Mq.Message(id = "c", body = "x", headers = [], deliveryCount = 4))
  assertEqualInt(mustTake(q).deliveryCount, 4, "deliveryCount")
  q.close()
}

test "publishBatch is one transaction, in order" {
  val q = openFresh()
  match q.publishBatch([message("a", "1"), message("b", "2"), message("c", "3")]) {
    case Ok(_) -> ()
    case Err(e) -> assertTrue(false, e)
  }
  assertEqual(mustTake(q).id, "a", "first")
  assertEqual(mustTake(q).id, "b", "second")
  assertEqual(mustTake(q).id, "c", "third")
  expectTimeout(q, "exactly the batch was published")
  q.close()
}

test "an empty message id is refused on publish" {
  val q = openFresh()
  val none: slice[Stomp.Header] = []
  expectContains(
    errorOf(ActiveMq.publishWithHeaders(q, message("", "x"), none), "publish"),
    "empty message id",
    "empty id"
  )
  q.close()
}

test "a message id that cannot travel in a header is refused, and the connection survives" {
  val q = openFresh()
  expectContains(errorOf(q.publish(message("bad\u{0000}id", "x")), "NUL in an id"), "NUL", "NUL id")
  assertTrue(ActiveMq.isOpen(q), "nothing was written")
  mustPublish(q, message("good", "x"))
  assertEqual(mustTake(q).id, "good", "still usable")
  q.close()
}

// ─── Extras ──────────────────────────────────────────────────────────────────
test "publishWith sends the message and refuses invalid options" {
  val q = openFresh()
  val base = Extras.defaultSendOptions()
  val full = base.withPersistent(false).copy(
    priority = 7,
    timeToLiveMs = 60000i64,
    correlationId = "corr",
    replyTo = "/queue/replies",
    jmsType = "invoice",
    properties = [Extras.Property(name = "color", value = "red")]
  )
  match Extras.publishWith(q, message("x-1", "with options"), full) {
    case Ok(_) -> ()
    case Err(e) -> assertTrue(false, e)
  }
  val got = mustTake(q)
  assertEqual(got.id, "x-1", "id")
  assertEqual(got.body, "with options", "body")
  assertEqualInt(got.headers.length, 0, "delivery options are not message headers")
  assertTrue(isOk(q.ack(got.id)), "ack")
  expectContains(
    errorOf(Extras.publishWith(q, message("m", "x"), base.copy(priority = 10)), "priority"),
    "outside 0..9",
    "priority"
  )
  expectContains(
    errorOf(Extras.publishWith(q, message("m", "x"), base.copy(priority = -2)), "priority"),
    "outside 0..9",
    "priority"
  )
  expectContains(
    errorOf(Extras.publishWith(q, message("m", "x"), base.copy(timeToLiveMs = -1i64)), "ttl"),
    "timeToLiveMs",
    "time to live"
  )
  expectContains(
    errorOf(
      Extras.publishWith(
        q,
        message("m", "x"),
        base.copy(properties = [Extras.Property(name = "bad-name", value = "v")])
      ),
      "property name"
    ),
    "cannot be used as a JMS property name",
    "property name"
  )
  expectContains(
    errorOf(
      Extras.publishWith(
        q,
        message("m", "x"),
        base.copy(properties = [Extras.Property(name = "a", value = "1"), Extras.Property(name = "a", value = "2")])
      ),
      "duplicate property"
    ),
    "is set twice",
    "duplicate property"
  )
  expectTimeout(q, "nothing was published by the refused calls")
  q.close()
}

// ─── Auto acknowledgement ────────────────────────────────────────────────────
test "Auto mode acknowledges on Ok and requeues on Err" {
  val options = optionsFor(freshName())
  val q = open(options.copy(ackMode = Mq.AckMode.Auto))
  mustPublish(q, message("auto", "x"))
  val refused = errorOf(q.consume(2000, { m -> Err("not now") }), "a failing handler")
  assertEqual(refused, "not now", "the handler's error is returned")
  assertEqualInt(ActiveMq.unsettledCount(q), 0, "the failed delivery was settled by the requeue")
  val again = mustTake(q)
  assertEqual(again.id, "auto", "redelivered")
  assertEqualInt(again.deliveryCount, 1, "with a higher count")
  assertEqualInt(ActiveMq.unsettledCount(q), 0, "acknowledged by consume")
  assertTrue(isOk(q.ack("auto")), "ack is a no-op in Auto mode")
  assertTrue(isOk(q.nack("auto", true)), "nack is a no-op in Auto mode")
  assertTrue(isErr(q.ack("")), "an empty id is still an Err")
  expectTimeout(q, "nothing is redelivered after the Auto ack")
  q.close()
}

// ─── Leaving and foreign messages ────────────────────────────────────────────
test "deliveries left unsettled at close go back to the queue, flagged redelivered" {
  val destination = freshName()
  val first = open(optionsFor(destination))
  mustPublish(first, message("left-1", "a"))
  mustPublish(first, message("left-2", "b"))
  assertEqual(mustTake(first).id, "left-1", "delivered to the first consumer")
  assertEqual(mustTake(first).id, "left-2", "delivered to the first consumer")
  first.close()
  val second = open(optionsFor(destination))
  val a = mustTake(second)
  assertEqual(a.id, "left-1", "order is kept")
  assertEqualInt(a.deliveryCount, 1, "the broker's redelivered flag counts as one delivery")
  val b = mustTake(second)
  assertEqual(b.id, "left-2", "second message")
  assertTrue(isOk(second.ack(a.id)), "ack")
  assertTrue(isOk(second.ack(b.id)), "ack")
  second.close()
}

// A producer that is not this client: raw frames on a socket.
func rawSend(destination: in String, headers: in slice[Stomp.Header], body: in slice[Byte]): Unit {
  val sock = match K.sockConnect("127.0.0.1", brokerPort(), 2000) { case Ok(s) -> s; case Err(e) -> panic(e) }
  val all: List[Stomp.Header] = newList()
  all.add(h("destination", destination))
  all.add(h("receipt", "raw-1"))
  for header in headers {
    all.add(header)
  }
  val frames = [
    Stomp.bareFrame("CONNECT", [h("accept-version", "1.2"), h("host", "localhost")]),
    Stomp.Frame(command = "SEND", headers = all.toArray(), body = body),
    Stomp.bareFrame("DISCONNECT", [h("receipt", "raw-2")])
  ]
  val decoder = Stomp.newDecoder(65536)
  for frame in frames {
    match Stomp.encodeFrame(frame) {
      case Ok(bytes) -> {
        val _ = K.sockWrite(sock, bytes)
      }
      case Err(e) -> panic(e)
    }
    var answered = false
    while not answered {
      match Stomp.nextFrame(decoder) {
        case Ok(Some(_)) -> {
          answered = true
        }
        case Ok(None) -> match K.sockRead(sock, 4096, 2000) {
          case Ok(Data(bytes)) -> Stomp.feed(decoder, bytes)
          case _ -> panic("the fake broker did not answer a raw frame")
        }
        case Err(e) -> panic(e)
      }
    }
  }
  K.sockClose(sock)
}

test "a message from another producer is delivered with its broker id and headers" {
  val destination = freshName()
  rawSend(destination, [h("content-type", "text/plain"), h("trace", "abc")], encodeUtf8("from elsewhere"))
  val q = open(optionsFor(destination))
  val got = mustTake(q)
  assertTrue(Str.startsWith(got.id, "ID:"), "the id is the broker's message-id, got " + got.id)
  assertEqual(got.body, "from elsewhere", "body")
  assertEqualInt(got.headers.length, 2, "application headers only")
  assertEqual(got.headers[0].key, "content-type", "first header")
  assertEqual(got.headers[1].value, "abc", "second header")
  assertEqualInt(got.deliveryCount, 0, "first delivery")
  assertTrue(isOk(q.nack(got.id, true)), "a foreign message can be requeued")
  val again = mustTake(q)
  assertEqual(again.body, "from elsewhere", "requeued body")
  assertEqualInt(again.deliveryCount, 1, "requeued count")
  assertEqualInt(again.headers.length, 2, "requeued headers")
  assertTrue(isOk(q.ack(again.id)), "ack")
  q.close()
}

func rejecting(options: in ActiveMq.ConnectOptions): ActiveMq.ConnectOptions {
  options.copy(onUndeliverable = ActiveMq.UndeliverablePolicy.RejectToBroker)
}

test "with RejectToBroker a body that is not UTF-8 is rejected to the broker and reported" {
  val destination = freshName()
  val binary: slice[Byte] = [255u8, 254u8, 0u8, 1u8]
  val none: slice[Stomp.Header] = []
  rawSend(destination, none, binary)
  val q = open(rejecting(optionsFor(destination)))
  val e = errorOf(q.consume(2000, { m -> Ok(()) }), "consume a binary message")
  expectContains(e, "cannot be delivered: its body is not UTF-8 text", "reason")
  expectContains(e, "rejected to the broker's dead-letter handling", "outcome")
  assertEqualInt(ActiveMq.unsettledCount(q), 0, "it is not in flight")
  expectTimeout(q, "the rejected message does not come back")
  mustPublish(q, message("next", "fine"))
  assertEqual(mustTake(q).id, "next", "the destination is not blocked")
  q.close()
}

test "with RejectToBroker malformed lyric headers are rejected to the broker and reported" {
  val destination = freshName()
  rawSend(destination, [h("lyric_id", "x"), h("lyric_headers", "not json")], encodeUtf8("body"))
  rawSend(destination, [h("lyric_id", "y"), h("lyric_delivery_count", "many")], encodeUtf8("body"))
  val q = open(rejecting(optionsFor(destination)))
  expectContains(
    errorOf(q.consume(2000, { m -> Ok(()) }), "bad lyric_headers"),
    "lyric_headers header is malformed",
    "headers"
  )
  expectContains(
    errorOf(q.consume(2000, { m -> Ok(()) }), "bad count"),
    "lyric_delivery_count header is not a number",
    "count"
  )
  expectTimeout(q, "both were rejected")
  q.close()
}

// ─── Regression tests of the README audit ────────────────────────────────────
func freshFlaky(): String {
  slot.names = slot.names + 1
  "/queue/flaky." + slot.names.toString()
}

func mustOk(r: in Result[Unit, String], label: in String): Unit {
  match r {
    case Ok(_) -> ()
    case Err(e) -> assertTrue(false, label + ": " + e)
  }
}

test "an ack, a reject and a requeue the broker refuses can be repeated" {
  val acked = open(optionsFor(freshFlaky()))
  mustPublish(acked, message("f-1", "x"))
  val first = mustTake(acked)
  expectContains(errorOf(acked.ack(first.id), "refused ack"), "broker error: simulated settlement failure", "ack")
  assertTrue(ActiveMq.isOpen(acked), "the connection survives the ERROR frame")
  assertEqualInt(ActiveMq.unsettledCount(acked), 1, "the delivery is still unsettled after a refused ack")
  mustOk(acked.ack(first.id), "the second ack")
  assertEqualInt(ActiveMq.unsettledCount(acked), 0, "settled by the second ack")
  expectTimeout(acked, "acknowledged")
  acked.close()

  val rejected = open(optionsFor(freshFlaky()))
  mustPublish(rejected, message("f-2", "x"))
  val second = mustTake(rejected)
  expectContains(errorOf(rejected.nack(second.id, false), "refused reject"), "simulated settlement failure", "nack")
  assertEqualInt(ActiveMq.unsettledCount(rejected), 1, "the delivery is still unsettled after a refused reject")
  mustOk(rejected.nack(second.id, false), "the second reject")
  expectTimeout(rejected, "rejected")
  rejected.close()

  val requeued = open(optionsFor(freshFlaky()))
  mustPublish(requeued, message("f-3", "x"))
  val third = mustTake(requeued)
  expectContains(errorOf(requeued.nack(third.id, true), "refused requeue"), "simulated settlement failure", "requeue")
  assertEqualInt(ActiveMq.unsettledCount(requeued), 1, "the delivery is still unsettled after a refused requeue")
  mustOk(requeued.nack(third.id, true), "the second requeue")
  val again = mustTake(requeued)
  assertEqualInt(again.deliveryCount, 1, "requeued once: the refused transaction left no copy")
  mustOk(requeued.ack(again.id), "ack")
  expectTimeout(requeued, "exactly one copy")
  requeued.close()
}

// A handler that accepts. Not written `{ m -> Ok(()) }`: on dotnet that lambda
// builds a `Result` of the wrong instantiation, and a `consume` that calls it
// fails with an invalid cast (see the README's known gaps).
func accept(m: in Mq.Message): Result[Unit, String] {
  Ok(())
}

test "Auto mode: a settlement the broker refuses leaves the delivery settleable" {
  val base = optionsFor(freshFlaky())
  val q = open(base.copy(ackMode = Mq.AckMode.Auto))
  mustPublish(q, message("a-1", "x"))
  expectContains(
    errorOf(q.consume(2000, { m -> accept(m) }), "the automatic ack is refused"),
    "simulated settlement failure",
    "auto ack"
  )
  assertEqualInt(ActiveMq.unsettledCount(q), 1, "the delivery is still unsettled after the refused automatic ack")
  mustOk(q.ack("a-1"), "ack by hand")
  assertEqualInt(ActiveMq.unsettledCount(q), 0, "settled by hand")
  mustOk(q.ack("a-1"), "with nothing unsettled, ack is a no-op again")
  expectTimeout(q, "acknowledged")
  q.close()

  val otherBase = optionsFor(freshFlaky())
  val other = open(otherBase.copy(ackMode = Mq.AckMode.Auto))
  mustPublish(other, message("a-2", "x"))
  val e = errorOf(other.consume(2000, { m -> Err("not now") }), "the automatic requeue is refused")
  expectContains(e, "not now (and requeueing the message failed:", "auto requeue")
  assertEqualInt(
    ActiveMq.unsettledCount(other),
    1,
    "the delivery is still unsettled after the refused automatic requeue"
  )
  mustOk(other.nack("a-2", true), "requeue by hand")
  val again = mustTake(other)
  assertEqualInt(again.deliveryCount, 1, "requeued once")
  expectTimeout(other, "exactly one copy")
  other.close()
}

// The value of header `name` on the oldest message stored for `destination`,
// "" when it has none.
func storedHeader(destination: in String, name: in String): String {
  val b = match slot.broker { case Some(found) -> found; case None -> panic("no broker") }
  K.lockEnter(b.lock)
  val waiting = queueOf(b, destination)
  val value = if waiting.count == 0 {
    "<nothing stored>"
  } else {
    Stomp.headerOr(Stomp.bareFrame("SEND", waiting[0].headers), name, "")
  }
  K.lockExit(b.lock)
  value
}

test "publishWith inherits the connection's persistence unless the options set it" {
  val transient = optionsFor(freshName())
  val q = open(transient.copy(persistent = false))
  match Extras.publishWith(q, message("p-1", "x"), Extras.defaultSendOptions()) {
    case Ok(_) -> ()
    case Err(e) -> assertTrue(false, e)
  }
  assertEqual(
    storedHeader(ActiveMq.destination(q), "persistent"),
    "",
    "default send options on a non-persistent connection send no persistent header"
  )
  q.close()
  val durable = open(optionsFor(freshName()))
  match Extras.publishWith(durable, message("p-2", "x"), Extras.defaultSendOptions()) {
    case Ok(_) -> ()
    case Err(e) -> assertTrue(false, e)
  }
  assertEqual(storedHeader(ActiveMq.destination(durable), "persistent"), "true", "a persistent connection")
  durable.close()
  val explicit = open(optionsFor(freshName()))
  mustOk(
    Extras.publishWith(explicit, message("p-3", "x"), Extras.defaultSendOptions().withPersistent(false)),
    "an explicit non-persistent send"
  )
  assertEqual(storedHeader(ActiveMq.destination(explicit), "persistent"), "false", "the option wins when it is set")
  explicit.close()
  val raised = open(transient.copy(persistent = false, destination = freshName()))
  mustOk(
    Extras.publishWith(raised, message("p-4", "x"), Extras.defaultSendOptions().withPersistent(true)),
    "an explicit persistent send"
  )
  assertEqual(storedHeader(ActiveMq.destination(raised), "persistent"), "true", "also on a non-persistent connection")
  raised.close()
}

test "a body over maxFrameBytes is skipped and the connection stays usable" {
  val destination = freshName()
  val publisher = open(optionsFor(destination))
  mustPublish(publisher, message("big", filler(5000)))
  mustPublish(publisher, message("small", "fits"))
  val base = optionsFor(destination)
  val q = open(base.copy(maxFrameBytes = 1024))
  val e = errorOf(q.consume(2000, { m -> Ok(()) }), "an oversize body")
  expectContains(e, "5000 bytes exceeds", "the reason")
  assertTrue(ActiveMq.isOpen(q), "an oversize message does not break the connection: " + e)
  expectContains(e, "left unacknowledged", "the default policy")
  val next = mustTake(q)
  assertEqual(next.id, "small", "the next message is delivered on the same connection")
  mustOk(q.ack(next.id), "ack")
  val bigId = Str.substring(e, 18, Str.indexOfRaw(e, " cannot be delivered") - 18)
  expectContains(errorOf(q.nack(bigId, true), "requeue a message that was not read"), "nothing to requeue", "requeue")
  assertEqualInt(ActiveMq.unsettledCount(q), 1, "still unsettled")
  q.close()

  // It went back to the queue. A connection that rejects takes it off, and a
  // connection with a higher limit would have read it.
  val strict = open(rejecting(base.copy(maxFrameBytes = 1024)))
  val again = errorOf(strict.consume(2000, { m -> Ok(()) }), "the oversize body again")
  expectContains(again, "5000 bytes exceeds the limit of 1024 bytes", "the reason")
  expectContains(again, "rejected to the broker's dead-letter handling", "RejectToBroker")
  assertTrue(ActiveMq.isOpen(strict), "the connection survives the rejection too")
  assertEqualInt(ActiveMq.unsettledCount(strict), 0, "nothing is in flight")
  expectTimeout(strict, "the rejected message is gone")
  strict.close()
  publisher.close()
}

test "an oversize message that arrives while a publish waits for its receipt is kept for consume" {
  val destination = freshName()
  val base = optionsFor(destination)
  val q = open(base.copy(maxFrameBytes = 1024))
  expectTimeout(q, "subscribe")
  mustPublish(q, message("big", filler(3000)))
  mustPublish(q, message("small", "fits"))
  val e = errorOf(q.consume(2000, { m -> Ok(()) }), "the oversize body")
  expectContains(e, "3000 bytes exceeds", "reported by consume, not by the publish")
  assertEqual(mustTake(q).id, "small", "then the next message")
  q.close()
}

test "a header section over 64 KiB is skipped and the connection stays usable" {
  val destination = freshName()
  val publisher = open(optionsFor(destination))
  mustOk(
    ActiveMq.publishWithHeaders(publisher, message("wide", "x"), [h("fake-pad", "70000")]),
    "publish the message with the padded header"
  )
  mustPublish(publisher, message("small", "fits"))
  val q = open(optionsFor(destination))
  val e = errorOf(q.consume(2000, { m -> Ok(()) }), "oversize headers")
  expectContains(e, "65536 bytes", "the reason")
  assertTrue(ActiveMq.isOpen(q), "oversize headers do not break the connection: " + e)
  val next = mustTake(q)
  assertEqual(next.id, "small", "the next message is delivered on the same connection")
  mustOk(q.ack(next.id), "ack")
  q.close()
  publisher.close()
}

test "a negative deliveryCount is refused on publish" {
  val q = openFresh()
  val negative = Mq.Message(id = "n", body = "x", headers = [], deliveryCount = -1)
  expectContains(errorOf(q.publish(negative), "publish"), "deliveryCount", "publish")
  expectContains(errorOf(q.publishBatch([message("ok", "x"), negative]), "publishBatch"), "deliveryCount", "batch")
  assertTrue(ActiveMq.isOpen(q), "nothing was written")
  expectTimeout(q, "nothing was published, not even the valid message of the batch")
  q.close()
}

test "an ERROR frame without a receipt-id is a lost connection, not the current request's failure" {
  val q = open(lasting(optionsFor(freshName())))
  val e = errorOf(
    ActiveMq.publishWithHeaders(q, message("m", "x"), [h("destination", "/queue/fail.stray")]),
    "publish answered by an uncorrelated ERROR"
  )
  expectContains(e, "activemq: connection lost:", "an uncorrelated ERROR is connection-level")
  expectContains(e, "stray failure", "the broker's text is kept")
  assertTrue(not ActiveMq.isOpen(q), "the connection is unusable afterwards")
  q.close()
}

test "consume(0) does not wait for the SUBSCRIBE receipt" {
  slot.names = slot.names + 1
  val base = optionsFor("/queue/slow.subscribe." + slot.names.toString())
  val q = open(base.copy(requestTimeoutMs = 10000))
  val started = K.monotonicMillis()
  val e = errorOf(q.consume(0, { m -> Ok(()) }), "a poll while the subscription is being opened")
  val elapsed = K.monotonicMillis() - started
  assertEqual(e, "timeout", "the poll times out")
  assertTrue(elapsed < 1500i64, "consume(0) returned after " + elapsed.toString() + " ms")
  mustPublish(q, message("s-1", "after the subscription"))
  val got = mustTake(q)
  assertEqual(got.id, "s-1", "the subscription was completed by the later calls")
  mustOk(q.ack(got.id), "ack")
  q.close()
}

func publishSlowly(q: in ActiveMq.ActiveMqQueue): Unit {
  val _ = ActiveMq.publishWithHeaders(q, message("slow", "x"), [h("destination", "/queue/slow.send")])
}

test "consume does not wait past its timeout for the connection's lock" {
  val base = optionsFor(freshName())
  val q = open(base.copy(requestTimeoutMs = 10000))
  expectTimeout(q, "subscribe")
  val publisher = KT.startBackground({ -> publishSlowly(q) })
  K.sleepMs(300)
  val started = K.monotonicMillis()
  val e = errorOf(q.consume(100, { m -> Ok(()) }), "a consume while a publish holds the connection")
  val elapsed = K.monotonicMillis() - started
  assertEqual(e, "timeout", "the consume times out")
  assertTrue(elapsed < 1500i64, "consume(100) returned after " + elapsed.toString() + " ms")
  KT.waitBackground(publisher)
  q.close()
}

test "a publish to a broker that stops reading ends with the request timeout" {
  val base = lasting(optionsFor(freshName()))
  val q = open(base.copy(login = "stall", requestTimeoutMs = 1000))
  val body = filler(33554432)
  val ended = stallsEndedOf(theBroker())
  val e = errorOf(q.publish(message("huge", body)), "a publish nobody reads")
  expectContains(e, "activemq: connection lost:", "a write that cannot finish loses the connection")
  // The request timeout ended it, not the broker closing its socket when its
  // pause is over. A count, not a clock: encoding 32 MiB takes seconds on a
  // loaded machine.
  assertEqualInt(stallsEndedOf(theBroker()), ended, "the publish returned while the broker was still reading nothing")
  assertTrue(not ActiveMq.isOpen(q), "the connection is unusable afterwards")
  q.close()
}

test "a message this client cannot represent is left on the broker by default" {
  val destination = freshName()
  val binary: slice[Byte] = [255u8, 254u8, 0u8, 1u8]
  val none: slice[Stomp.Header] = []
  rawSend(destination, none, binary)
  val q = open(optionsFor(destination))
  val e = errorOf(q.consume(2000, { m -> Ok(()) }), "consume a binary message")
  expectContains(e, "cannot be delivered: its body is not UTF-8 text", "reason")
  expectContains(e, "left unacknowledged", "outcome")
  assertTrue(ActiveMq.isOpen(q), "the connection stays usable")
  assertEqualInt(ActiveMq.unsettledCount(q), 1, "it is tracked as unsettled")
  mustPublish(q, message("next", "fine"))
  val next = mustTake(q)
  assertEqual(next.id, "next", "later messages are delivered")
  mustOk(q.ack(next.id), "ack")
  q.close()
  val second = open(optionsFor(destination))
  val again = errorOf(second.consume(2000, { m -> Ok(()) }), "the binary message is still on the broker")
  expectContains(again, "its body is not UTF-8 text", "it was not destroyed")
  // The error names the broker id under which the application may settle it.
  val marker = "cannot be delivered"
  val brokerId = Str.substring(again, 18, Str.indexOfRaw(again, " " + marker) - 18)
  assertTrue(Str.startsWith(brokerId, "ID:"), "the broker id in the error: " + again)
  mustOk(second.nack(brokerId, true), "a small undeliverable message can be requeued as it is")
  val third = errorOf(second.consume(2000, { m -> Ok(()) }), "the requeued copy")
  expectContains(third, "its body is not UTF-8 text", "the copy is as undeliverable")
  assertEqualInt(ActiveMq.unsettledCount(second), 1, "only the copy is unsettled")
  val copyId = Str.substring(third, 18, Str.indexOfRaw(third, " " + marker) - 18)
  mustOk(second.nack(copyId, false), "the application rejects it itself")
  assertEqualInt(ActiveMq.unsettledCount(second), 0, "settled")
  expectTimeout(second, "gone")
  second.close()
}

test "an undeliverable message in Auto mode is left for ack or nack" {
  val destination = freshName()
  rawSend(destination, [h("lyric_id", "x"), h("lyric_headers", "not json")], encodeUtf8("body"))
  val base = optionsFor(destination)
  val q = open(base.copy(ackMode = Mq.AckMode.Auto))
  val e = errorOf(q.consume(2000, { m -> Ok(()) }), "malformed headers")
  expectContains(e, "lyric_headers header is malformed", "reason")
  expectContains(e, "left unacknowledged", "outcome")
  val brokerId = Str.substring(e, 18, Str.indexOfRaw(e, " cannot be delivered") - 18)
  assertEqualInt(ActiveMq.unsettledCount(q), 1, "tracked")
  mustOk(q.ack(brokerId), "ack acts on it even in Auto mode")
  assertEqualInt(ActiveMq.unsettledCount(q), 0, "settled")
  q.close()
}

test "connectFromEnv reads the undeliverable policy" {
  val prefix = "LYRIC_CONFIG_MQ_ACTIVEMQ_"
  val destination = freshName()
  val binary: slice[Byte] = [255u8, 254u8]
  val none: slice[Stomp.Header] = []
  rawSend(destination, none, binary)
  Env.setVar(prefix + "URL", "stomp://lyric:secret@127.0.0.1:" + brokerPort().toString())
  Env.setVar(prefix + "QUEUENAME", destination)
  Env.setVar(prefix + "ONUNDELIVERABLE", "sometimes")
  match ActiveMq.connectFromEnv() {
    case Ok(_) -> assertTrue(false, "an unknown policy")
    case Err(e) -> expectContains(e, "ONUNDELIVERABLE must be Leave or Reject", "a bad policy")
  }
  Env.setVar(prefix + "ONUNDELIVERABLE", "Reject")
  match ActiveMq.connectFromEnv() {
    case Err(e) -> assertTrue(false, e)
    case Ok(q) -> {
      expectContains(
        errorOf(q.consume(2000, { m -> Ok(()) }), "a binary message"),
        "rejected to the broker's dead-letter handling",
        "Reject"
      )
      q.close()
    }
  }
  Env.setVar(prefix + "URL", "")
  Env.setVar(prefix + "QUEUENAME", "")
  Env.setVar(prefix + "ONUNDELIVERABLE", "")
}

// A peer that sends `first`, then closes the connection at once, without a
// TLS shutdown when it is TLS.
func sayAndHangUp(server: in KT.ServerSock, secured: in Bool, first: in slice[Byte]): Unit {
  val accepted = match KT.serverAccept(server) { case Ok(s) -> s; case Err(_) -> return }
  val sock = if secured {
    match KT.sockAcceptTls(accepted, serverTls(""), 5000) {
      case Ok(s) -> s
      case Err(_) -> return
    }
  } else {
    accepted
  }
  // Wait for the CONNECT frame, as a broker does.
  val _ = K.sockRead(sock, 4096, 5000)
  if first.length > 0 {
    val _ = K.sockWrite(sock, first)
  }
  K.sockClose(sock)
}

func loginAgainst(secured: in Bool, first: in slice[Byte]): String {
  val server = match KT.serverListen("127.0.0.1", 0) { case Ok(s) -> s; case Err(e) -> panic(e) }
  val peer = KT.startBackground({ -> sayAndHangUp(server, secured, first) })
  val base = ActiveMq.defaultOptions("127.0.0.1", KT.serverPort(server), "/queue/a")
  val options = base.copy(login = "lyric", passcode = "hunter2", requestTimeoutMs = 5000)
  val e = errorOfConnect(if secured { options.withTls().withExclusiveCaCertificate(TEST_ONLY_CA) } else { options })
  KT.waitBackground(peer)
  KT.serverClose(server)
  assertTrue(not Str.contains(e, "hunter2"), "the passcode is not echoed: " + e)
  e
}

test "an ERROR frame followed at once by the broker's close is still read" {
  val refusal = match Stomp.encodeFrame(Stomp.textFrame("ERROR", [h("message", "password is invalid")], "")) {
    case Ok(bytes) -> bytes
    case Err(e) -> panic(e)
  }
  var n = 0
  while n < 20 {
    assertEqual(loginAgainst(false, refusal), "activemq: broker error: password is invalid", "plain TCP")
    assertEqual(loginAgainst(true, refusal), "activemq: broker error: password is invalid", "TLS")
    n += 1
  }
}

test "a broker that closes the connection during login without an answer" {
  val nothing: slice[Byte] = []
  for secured in [false, true] {
    val e = loginAgainst(secured, nothing)
    expectContains(e, "activemq: connection lost:", "a loss")
    expectContains(e, "ended the connection during login", "what happened")
    expectContains(e, "refuses the login or passcode", "the usual cause")
  }
  // Half an ERROR frame is no answer either.
  val e = loginAgainst(false, encodeUtf8("ERROR\nmessage:password is inv"))
  expectContains(e, "ended the connection during login", "a truncated ERROR frame")
}

ref record Flag {
  var done: Bool
}

func idleUntil(flag: in Flag): Unit {
  while not flag.done {
    K.sleepMs(20)
  }
}

func roundTripUnderLoad(options: in ActiveMq.ConnectOptions, label: in String): Unit {
  match ActiveMq.connectWith(options) {
    case Err(e) -> assertTrue(false, label + ": connect: " + e)
    case Ok(q) -> {
      mustOk(q.publish(message("busy", label)), label + ": publish")
      val got = Got(messages = newList())
      mustOk(q.consume(1000, { m -> keep(got, m) }), label + ": consume(1000)")
      q.close()
    }
  }
}

test "a busy thread pool does not delay connecting, publishing or consuming" {
  val plain = optionsFor(freshName())
  val secured = tlsOptionsFor(freshName())
  val flag = Flag(done = false)
  var n = 0
  while n < 64 {
    val _ = KT.startPoolWork({ -> idleUntil(flag) })
    n += 1
  }
  K.sleepMs(200)
  roundTripUnderLoad(plain, "plain TCP")
  roundTripUnderLoad(secured, "TLS")
  flag.done = true
}

// ─── Threads ─────────────────────────────────────────────────────────────────
func publishMany(q: in ActiveMq.ActiveMqQueue, count: in Int): Unit {
  var n = 0
  while n < count {
    mustPublish(q, message("t-" + n.toString(), n.toString()))
    n += 1
  }
}

test "a publisher thread and a consuming thread share one connection" {
  val q = openFresh()
  val count = 40
  val publisher = KT.startBackground({ -> publishMany(q, count) })
  val got = Got(messages = newList())
  var failures = 0
  while got.messages.count < count and failures < 50 {
    match q.consume(1000, { m -> keep(got, m) }) {
      case Ok(_) -> {
        val last = got.messages[got.messages.count - 1]
        assertTrue(isOk(q.ack(last.id)), "ack " + last.id)
      }
      case Err(_) -> {
        failures += 1
      }
    }
  }
  KT.waitBackground(publisher)
  assertEqualInt(got.messages.count, count, "every message arrived")
  var n = 0
  while n < got.messages.count {
    assertEqual(got.messages[n].id, "t-" + n.toString(), "in publish order")
    n += 1
  }
  assertEqualInt(ActiveMq.unsettledCount(q), 0, "all settled")
  q.close()
}

// ─── Heart-beats and reconnecting ────────────────────────────────────────────
// A broker of its own for one test, answering CONNECT with `heartBeat` and
// writing a heart-beat every `beatEveryMs` ms on each connection (0 = never).
func startKnobBroker(heartBeat: in String, beatEveryMs: in Int): FakeBroker {
  val b = startBrokerWith(false, serverTls(""))
  K.lockEnter(b.lock)
  b.heartBeat = heartBeat
  b.beatEveryMs = beatEveryMs
  K.lockExit(b.lock)
  b
}

func stopKnobBroker(b: in FakeBroker): Unit {
  KT.serverClose(b.server)
  dropConnections(b)
}

func setBeating(b: in FakeBroker, on: in Bool): Unit {
  K.lockEnter(b.lock)
  b.beating = on
  K.lockExit(b.lock)
}

func setRefuseLogins(b: in FakeBroker, on: in Bool): Unit {
  K.lockEnter(b.lock)
  b.refuseLogins = on
  K.lockExit(b.lock)
}

func setRefuseSubscribes(b: in FakeBroker, on: in Bool): Unit {
  K.lockEnter(b.lock)
  b.refuseSubscribes = on
  K.lockExit(b.lock)
}

func theBroker(): FakeBroker {
  val _ = brokerPort()
  match slot.broker {
    case Some(b) -> b
    case None -> panic("no broker")
  }
}

func stallsEndedOf(b: in FakeBroker): Int {
  K.lockEnter(b.lock)
  val n = b.stallsEnded
  K.lockExit(b.lock)
  n
}

func loginsOf(b: in FakeBroker): Int {
  K.lockEnter(b.lock)
  val n = b.logins
  K.lockExit(b.lock)
  n
}

func subscribesOf(b: in FakeBroker): Int {
  K.lockEnter(b.lock)
  val n = b.subscribes
  K.lockExit(b.lock)
  n
}

func beatsSeenBy(b: in FakeBroker): Int {
  K.lockEnter(b.lock)
  val n = b.beatsSeen
  K.lockExit(b.lock)
  n
}

func lastOfferTo(b: in FakeBroker): String {
  K.lockEnter(b.lock)
  val offer = b.lastOffer
  K.lockExit(b.lock)
  offer
}

// Reconnecting after 100 ms, at most 200 ms apart.
func fastReconnect(): ActiveMq.ReconnectOptions {
  val base = ActiveMq.defaultReconnect()
  base.copy(initialDelayMs = 100, maxDelayMs = 200)
}

// Options for a knob broker: no heart-beats, quick reconnects.
func knobOptions(b: in FakeBroker, destination: in String): ActiveMq.ConnectOptions {
  val base = ActiveMq.defaultOptions("127.0.0.1", KT.serverPort(b.server), destination)
  base.copy(
    login = "lyric",
    passcode = "secret",
    requestTimeoutMs = 3000,
    heartBeatSendMs = 0,
    heartBeatReceiveMs = 0,
    reconnect = fastReconnect()
  )
}

// Wait for `condition`, looking every 10 ms; the test fails when it does not
// hold within `limitMs`. The limits are far above what a healthy run needs,
// so a slow machine does not fail a test.
func waitUntil(label: in String, limitMs: in Int, condition: in () -> Bool): Unit {
  val deadline = K.monotonicMillis() + limitMs.toLong()
  while not condition() {
    if K.monotonicMillis() >= deadline {
      assertTrue(false, label + ": not within " + limitMs.toString() + " ms")
      return
    }
    K.sleepMs(10)
  }
}

func takeWithin(q: in ActiveMq.ActiveMqQueue, limitMs: in Int, label: in String): Mq.Message {
  val got = Got(messages = newList())
  match q.consume(limitMs, { m -> keep(got, m) }) {
    case Ok(_) -> got.messages[0]
    case Err(e) -> panic(label + ": consume failed: " + e)
  }
}

// Publish `m`, trying again while the connection is away.
func publishEventually(q: in ActiveMq.ActiveMqQueue, m: in Mq.Message, limitMs: in Int, label: in String): Unit {
  val deadline = K.monotonicMillis() + limitMs.toLong()
  var last = ""
  var published = false
  while not published and K.monotonicMillis() < deadline {
    match q.publish(m) {
      case Ok(_) -> {
        published = true
      }
      case Err(e) -> {
        last = e
        K.sleepMs(20)
      }
    }
  }
  assertTrue(published, label + ": no publish succeeded within " + limitMs.toString() + " ms, the last error: " + last)
}

func lostBecause(q: in ActiveMq.ActiveMqQueue): String {
  val status = ActiveMq.status(q)
  status.lostBecause
}

func reconnectsOf(q: in ActiveMq.ActiveMqQueue): Int {
  val status = ActiveMq.status(q)
  status.reconnects
}

func isReconnecting(q: in ActiveMq.ActiveMqQueue): Bool {
  val status = ActiveMq.status(q)
  status.reconnecting
}

test "heart-beats are negotiated as STOMP 1.2 says" {
  val b = startKnobBroker("300,200", 0)
  val base = knobOptions(b, "/queue/hb").copy(reconnect = ActiveMq.noReconnect())
  // The larger of what one side guarantees and the other asks for.
  val q = open(base.copy(heartBeatSendMs = 100, heartBeatReceiveMs = 150))
  assertEqual(lastOfferTo(b), "100,150", "the CONNECT frame offers the configured intervals")
  val agreed = ActiveMq.status(q)
  assertEqualInt(agreed.heartBeatSendMs, 200, "the client sends as often as the broker asks, not more")
  assertEqualInt(agreed.heartBeatReceiveMs, 300, "the broker sends as often as it can, not more")
  assertTrue(ActiveMq.hasBackgroundThread(q), "heart-beats need the background thread")
  q.close()
  val slower = open(base.copy(heartBeatSendMs = 4000, heartBeatReceiveMs = 5000))
  assertEqual(lastOfferTo(b), "4000,5000", "the offer")
  val slow = ActiveMq.status(slower)
  assertEqualInt(slow.heartBeatSendMs, 4000, "the client's own limit wins when it is larger")
  assertEqualInt(slow.heartBeatReceiveMs, 5000, "the client's wish wins when it is larger")
  slower.close()
  // 0 on either side switches a direction off.
  val off = open(base)
  assertEqual(lastOfferTo(b), "0,0", "no heart-beats offered")
  val none = ActiveMq.status(off)
  assertEqualInt(none.heartBeatSendMs, 0, "nothing to send")
  assertEqualInt(none.heartBeatReceiveMs, 0, "nothing to expect")
  assertTrue(not ActiveMq.hasBackgroundThread(off), "no heart-beats and no reconnecting: no thread")
  off.close()
  K.lockEnter(b.lock)
  b.heartBeat = "0,0"
  K.lockExit(b.lock)
  val refused = open(base.copy(heartBeatSendMs = 100, heartBeatReceiveMs = 150))
  val declined = ActiveMq.status(refused)
  assertEqualInt(declined.heartBeatSendMs, 0, "a broker that wants none gets none")
  assertEqualInt(declined.heartBeatReceiveMs, 0, "a broker that sends none is not expected to")
  refused.close()
  expectContains(errorOfConnect(base.copy(heartBeatSendMs = -1)), "heartBeatSendMs", "a negative interval")
  expectContains(errorOfConnect(base.copy(heartBeatReceiveMs = -1)), "heartBeatReceiveMs", "a negative interval")
  stopKnobBroker(b)
}

test "an idle connection sends heart-beats, and frames still pass between them" {
  val b = startKnobBroker("0,100", 0)
  val base = knobOptions(b, "/queue/idle").copy(reconnect = ActiveMq.noReconnect())
  val q = open(base.copy(heartBeatSendMs = 100))
  waitUntil("three heart-beats from an idle client", 20000, { -> beatsSeenBy(b) >= 3 })
  mustPublish(q, message("between", "beats"))
  assertEqual(mustTake(q).id, "between", "a frame between heart-beats")
  assertTrue(isOk(q.ack("between")), "ack")
  val seen = beatsSeenBy(b)
  waitUntil("heart-beats go on after the frames", 20000, { -> beatsSeenBy(b) >= seen + 2 })
  assertTrue(ActiveMq.isOpen(q), "still connected")
  q.close()
  waitUntil("the background thread ends after close", 10000, { -> not ActiveMq.hasBackgroundThread(q) })
  stopKnobBroker(b)
  // A broker that asks for none gets none.
  val quiet = startKnobBroker("0,0", 0)
  val silent = open(knobOptions(quiet, "/queue/idle").copy(heartBeatSendMs = 100, reconnect = ActiveMq.noReconnect()))
  K.sleepMs(500)
  assertEqualInt(beatsSeenBy(quiet), 0, "no heart-beat was agreed, so none is sent")
  silent.close()
  stopKnobBroker(quiet)
}

test "a broker that stops sending heart-beats is taken for dead" {
  // Heart-beats agreed every second, sent ten times as often: the connection
  // must outlive three intervals of idling, and die when they stop.
  val b = startKnobBroker("1000,0", 100)
  val base = knobOptions(b, "/queue/dead").copy(heartBeatReceiveMs = 1000, reconnect = ActiveMq.noReconnect())
  val q = open(base)
  K.sleepMs(3000)
  assertTrue(ActiveMq.isOpen(q), "heart-beats keep an idle connection alive: " + lostBecause(q))
  val waiting = open(base)
  setBeating(b, false)
  // An idle connection: the background thread notices.
  waitUntil("the silent broker is noticed", 30000, { -> not ActiveMq.isOpen(q) })
  expectContains(lostBecause(q), "the broker sent nothing for", "the reason")
  expectContains(lostBecause(q), "heart-beat every 1000 ms", "the agreed interval")
  val later = errorOf(q.publish(message("m", "x")), "publish to the dead connection")
  expectContains(later, "activemq: connection lost: the broker sent nothing for", "later operations say why")
  waitUntil("the background thread ends with the connection", 10000, { -> not ActiveMq.hasBackgroundThread(q) })
  q.close()
  // A connection in the middle of a consume: the consume ends with the
  // reason, not with "timeout".
  val e = errorOf(waiting.consume(30000, { m -> accept(m) }), "consume from a silent broker")
  expectContains(e, "the broker sent nothing for", "a dead peer is not an empty queue")
  waiting.close()
  stopKnobBroker(b)
}

test "a missing heart-beat leads to a new connection" {
  val b = startKnobBroker("200,0", 50)
  val q = open(knobOptions(b, "/queue/revive").copy(heartBeatReceiveMs = 200))
  setBeating(b, false)
  waitUntil("a reconnect after the heart-beats stopped", 30000, { -> reconnectsOf(q) >= 1 })
  setBeating(b, true)
  publishEventually(q, message("alive", "again"), 30000, "after the broker came back")
  assertEqual(takeWithin(q, 30000, "after the broker came back").id, "alive", "the new connection works")
  q.close()
  stopKnobBroker(b)
}

test "a dropped connection is re-established, subscribes again and goes on receiving" {
  val b = startKnobBroker("0,0", 0)
  val q = open(knobOptions(b, "/queue/again"))
  mustPublish(q, message("m1", "one"))
  assertEqual(mustTake(q).id, "m1", "delivered on the first connection")
  assertEqualInt(loginsOf(b), 1, "one login")
  assertEqualInt(subscribesOf(b), 1, "one subscription")
  dropConnections(b)
  // `consume` waits through the outage; the broker redelivers what the lost
  // connection had not settled.
  val again = takeWithin(q, 30000, "after the drop")
  assertEqual(again.id, "m1", "the unsettled delivery comes back")
  assertEqualInt(again.deliveryCount, 1, "flagged redelivered by the broker")
  assertEqualInt(reconnectsOf(q), 1, "one reconnect")
  assertEqualInt(loginsOf(b), 2, "CONNECT was sent again")
  assertEqualInt(subscribesOf(b), 2, "SUBSCRIBE was sent again")
  assertTrue(ActiveMq.isOpen(q), "connected")
  assertEqualInt(ActiveMq.unsettledCount(q), 1, "the redelivery replaced the dead delivery")
  assertTrue(isOk(q.ack("m1")), "the redelivery can be settled")
  expectContains(errorOf(q.ack("m1"), "a second ack"), "no unsettled delivery", "nothing is left of the dead delivery")
  // Messages published after the reconnect arrive on the new subscription.
  mustPublish(q, message("m2", "two"))
  assertEqual(mustTake(q).id, "m2", "published and delivered on the new connection")
  assertTrue(isOk(q.ack("m2")), "ack")
  expectTimeout(q, "nothing else")
  // A second outage works the same way.
  dropConnections(b)
  waitUntil("the second reconnect", 30000, { -> reconnectsOf(q) == 2 })
  mustPublish(q, message("m3", "three"))
  assertEqual(mustTake(q).id, "m3", "after the second reconnect")
  assertTrue(isOk(q.ack("m3")), "ack")
  assertEqualInt(subscribesOf(b), 3, "subscribed a third time")
  q.close()
  stopKnobBroker(b)
}

test "a connection that never subscribed reconnects without subscribing" {
  val b = startKnobBroker("0,0", 0)
  val q = open(knobOptions(b, "/queue/publisher"))
  mustPublish(q, message("p1", "x"))
  dropConnections(b)
  waitUntil("the reconnect", 30000, { -> reconnectsOf(q) == 1 })
  assertEqualInt(subscribesOf(b), 0, "a publisher does not become a consumer")
  mustPublish(q, message("p2", "y"))
  assertEqual(mustTake(q).id, "p1", "both messages are on the queue")
  assertEqual(mustTake(q).id, "p2", "in order")
  q.close()
  stopKnobBroker(b)
}

test "while the connection is away: publish fails at once, unsent; in-flight requests are unknown; old deliveries are stale" {
  val b = startKnobBroker("0,0", 0)
  // Six seconds without a connection, so that the calls below meet it.
  val slowly = fastReconnect().copy(initialDelayMs = 6000, maxDelayMs = 6000, jitterPercent = 0)
  val q = open(knobOptions(b, "/queue/away").copy(reconnect = slowly))
  mustPublish(q, message("keep", "k"))
  assertEqual(mustTake(q).id, "keep", "a delivery on the first connection")
  // The broker stores this message and drops the connection before the
  // receipt.
  val inFlight = errorOf(
    ActiveMq.publishWithHeaders(
      q,
      message("held", "h"),
      [h("destination", "/queue/fail.storedrop.away")]
    ),
    "a publish whose receipt never comes"
  )
  expectContains(inFlight, "activemq: connection lost:", "the loss is reported")
  assertTrue(ActiveMq.isOutcomeUnknownError(inFlight), "the outcome is unknown: " + inFlight)
  assertTrue(not ActiveMq.isNotConnectedError(inFlight), "it is not the retryable error")
  assertTrue(not ActiveMq.isOpen(q), "not connected")
  assertTrue(isReconnecting(q), "reconnecting")
  // A publish now is refused before anything is written.
  val started = K.monotonicMillis()
  val refused = errorOf(q.publish(message("later", "l")), "publish while away")
  assertTrue(K.monotonicMillis() - started < 2000i64, "it fails at once")
  assertTrue(ActiveMq.isNotConnectedError(refused), "the retryable error: " + refused)
  assertTrue(not ActiveMq.isOutcomeUnknownError(refused), "nothing was sent, so nothing is unknown")
  expectContains(refused, "connection lost:", "it names the cause")
  val batch = errorOf(q.publishBatch([message("b1", "x"), message("b2", "y")]), "batch while away")
  assertTrue(ActiveMq.isNotConnectedError(batch), "a batch is refused the same way: " + batch)
  // The delivery of the lost connection cannot be settled: its ack id died
  // with the connection.
  assertEqualInt(ActiveMq.unsettledCount(q), 0, "deliveries of the lost connection do not count")
  val stale = errorOf(q.ack("keep"), "ack of a delivery of the lost connection")
  assertTrue(ActiveMq.isStaleDeliveryError(stale), "the stale error: " + stale)
  expectContains(stale, "the broker redelivers the message", "what happens next")
  val unknownId = errorOf(q.nack("keep", true), "a second settlement")
  assertTrue(ActiveMq.isNotConnectedError(unknownId), "the stale delivery is reported once: " + unknownId)
  val poll = errorOf(q.consume(0, { m -> accept(m) }), "consume(0) while away")
  assertTrue(ActiveMq.isNotConnectedError(poll), "consume(0) does not wait: " + poll)
  val short = errorOf(q.consume(100, { m -> accept(m) }), "consume(100) while away")
  assertTrue(ActiveMq.isNotConnectedError(short), "a consume that ends while away says so, not timeout: " + short)
  // The broker redelivers; nothing was sent twice and "later" never was.
  val again = takeWithin(q, 30000, "after the reconnect")
  assertEqual(again.id, "keep", "redelivered")
  assertEqualInt(again.deliveryCount, 1, "flagged redelivered")
  assertTrue(isOk(q.ack("keep")), "ack of the redelivery")
  expectTimeout(q, "the refused publish and batch were never sent")
  q.close()
  val other = open(knobOptions(b, "/queue/fail.storedrop.away"))
  assertEqual(takeWithin(other, 30000, "the held message").id, "held", "the broker had taken the message")
  assertTrue(isOk(other.ack("held")), "ack")
  expectTimeout(other, "the in-flight publish was not sent again")
  other.close()
  stopKnobBroker(b)
}

test "a delivery of a lost connection is still stale after the reconnect, until the redelivery replaces it" {
  val b = startKnobBroker("0,0", 0)
  val q = open(knobOptions(b, "/queue/stale"))
  mustPublish(q, message("s1", "x"))
  assertEqual(mustTake(q).id, "s1", "delivered")
  dropConnections(b)
  waitUntil("the reconnect", 30000, { -> reconnectsOf(q) == 1 })
  val stale = errorOf(q.nack("s1", true), "nack of the old delivery on the new connection")
  assertTrue(ActiveMq.isStaleDeliveryError(stale), "the stale error: " + stale)
  val again = takeWithin(q, 30000, "the redelivery")
  assertEqual(again.id, "s1", "the broker redelivered it")
  assertTrue(isOk(q.ack("s1")), "the redelivery is settled")
  expectTimeout(q, "no copy was made by the refused requeue")
  q.close()
  stopKnobBroker(b)
}

test "publishWaitMs lets a publish wait for the new connection" {
  val b = startKnobBroker("0,0", 0)
  val patient = fastReconnect().copy(publishWaitMs = 30000)
  val q = open(knobOptions(b, "/queue/patient").copy(reconnect = patient))
  val inFlight = errorOf(
    ActiveMq.publishWithHeaders(
      q,
      message("held", "h"),
      [h("destination", "/queue/fail.storedrop.patient")]
    ),
    "a publish whose receipt never comes"
  )
  assertTrue(ActiveMq.isOutcomeUnknownError(inFlight), "the in-flight publish is still unknown: " + inFlight)
  mustPublish(q, message("after", "a"))
  assertEqualInt(reconnectsOf(q), 1, "the publish went out on the new connection")
  assertEqual(mustTake(q).id, "after", "and arrived")
  q.close()
  expectContains(
    errorOfConnect(knobOptions(b, "/queue/x").copy(reconnect = patient.copy(publishWaitMs = -1))),
    "publishWaitMs",
    "a negative wait"
  )
  stopKnobBroker(b)
}

test "a batch interrupted by a lost connection is not applied in part and not sent again" {
  val b = startKnobBroker("0,0", 0)
  val q = open(knobOptions(b, "/queue/fail.drop"))
  val e = errorOf(q.publishBatch([message("b1", "x"), message("b2", "y")]), "a batch the broker hangs up on")
  expectContains(e, "activemq: connection lost:", "the loss")
  assertTrue(
    ActiveMq.isOutcomeUnknownError(e) or Str.contains(e, "the transaction was not committed"),
    "it says what became of the transaction: " + e
  )
  q.close()
  stopKnobBroker(b)
}

test "an ERROR frame that ends the connection leads to a new one" {
  val b = startKnobBroker("0,0", 0)
  val q = open(knobOptions(b, "/queue/stray"))
  val e = errorOf(
    ActiveMq.publishWithHeaders(q, message("m", "x"), [h("destination", "/queue/fail.stray")]),
    "publish answered by an uncorrelated ERROR"
  )
  expectContains(e, "the broker sent an ERROR frame that answers no request in flight", "the cause")
  assertTrue(ActiveMq.isOutcomeUnknownError(e), "the publish in flight is unknown: " + e)
  waitUntil("the reconnect", 30000, { -> reconnectsOf(q) == 1 })
  mustPublish(q, message("next", "n"))
  assertEqual(mustTake(q).id, "next", "the new connection works")
  q.close()
  stopKnobBroker(b)
}

test "reconnecting gives up after maxAttempts with an error that says so" {
  val b = startKnobBroker("0,0", 0)
  val bounded = fastReconnect().copy(maxAttempts = 3)
  val q = open(knobOptions(b, "/queue/gone").copy(reconnect = bounded))
  // Nothing listens any more: every attempt is refused.
  stopKnobBroker(b)
  waitUntil(
    "three failed attempts",
    60000,
    { -> Str.contains(lostBecause(q), "reconnecting was given up after 3 failed attempt(s)") }
  )
  val status = ActiveMq.status(q)
  assertTrue(not status.connected, "not connected")
  assertTrue(not status.reconnecting, "no longer reconnecting")
  assertEqualInt(status.failedAttempts, 3, "three attempts were made")
  expectContains(status.lastAttemptError, "activemq: cannot connect to 127.0.0.1:", "why the last one failed")
  val e = errorOf(q.publish(message("m", "x")), "publish after giving up")
  expectContains(e, "activemq: connection lost:", "a lost connection")
  expectContains(e, "reconnecting was given up after 3 failed attempt(s)", "it says that it gave up")
  expectContains(e, "cannot connect to 127.0.0.1:", "and why the last attempt failed")
  assertTrue(not ActiveMq.isNotConnectedError(e), "it is final, not the retryable error")
  val started = K.monotonicMillis()
  val consumed = errorOf(q.consume(20000, { m -> accept(m) }), "consume after giving up")
  assertTrue(K.monotonicMillis() - started < 10000i64, "consume does not wait for a connection that will not come")
  expectContains(consumed, "reconnecting was given up", "consume reports it too")
  waitUntil("the background thread ends", 10000, { -> not ActiveMq.hasBackgroundThread(q) })
  q.close()
  expectContains(errorOf(q.publish(message("m", "x")), "publish after close"), "connection is closed", "closed")
  val base = ActiveMq.defaultOptions("127.0.0.1", 1, "/queue/gone")
  expectContains(errorOfConnect(base.copy(reconnect = bounded.copy(maxAttempts = -1))), "maxAttempts", "attempts")
  expectContains(errorOfConnect(base.copy(reconnect = bounded.copy(initialDelayMs = 0))), "initialDelayMs", "delay")
  expectContains(errorOfConnect(base.copy(reconnect = bounded.copy(maxDelayMs = 50))), "maxDelayMs", "cap")
  expectContains(errorOfConnect(base.copy(reconnect = bounded.copy(jitterPercent = 101))), "jitterPercent", "jitter")
}

test "a broker that refuses the new login or subscription ends the attempts at once" {
  val b = startKnobBroker("0,0", 0)
  val q = open(knobOptions(b, "/queue/denied"))
  setRefuseLogins(b, true)
  dropConnections(b)
  waitUntil(
    "the refused login is final",
    30000,
    { -> Str.contains(lostBecause(q), "the broker refused the new connection") }
  )
  expectContains(lostBecause(q), "password is invalid", "the broker's reason")
  waitUntil("the background thread ends", 10000, { -> not ActiveMq.hasBackgroundThread(q) })
  assertEqualInt(loginsOf(b), 2, "one attempt, not a loop")
  val e = errorOf(q.publish(message("m", "x")), "publish after the refusal")
  expectContains(e, "activemq: connection lost:", "lost for good")
  assertTrue(not ActiveMq.isNotConnectedError(e), "not the retryable error")
  assertTrue(not Str.contains(e, "secret"), "the passcode is not in the error")
  q.close()
  setRefuseLogins(b, false)
  val consumer = open(knobOptions(b, "/queue/denied"))
  mustOk(ActiveMq.subscribe(consumer), "subscribe")
  setRefuseSubscribes(b, true)
  dropConnections(b)
  waitUntil(
    "the refused subscription is final",
    30000,
    { -> Str.contains(lostBecause(consumer), "the broker refused the subscription") }
  )
  expectContains(lostBecause(consumer), "not authorized", "the broker's reason")
  waitUntil("the background thread ends", 10000, { -> not ActiveMq.hasBackgroundThread(consumer) })
  consumer.close()
  stopKnobBroker(b)
}

test "close during the wait for a reconnect returns at once and ends the background thread" {
  val b = startKnobBroker("0,0", 0)
  val minute = fastReconnect().copy(initialDelayMs = 60000, maxDelayMs = 60000, jitterPercent = 0)
  val q = open(knobOptions(b, "/queue/closing").copy(reconnect = minute))
  assertTrue(ActiveMq.hasBackgroundThread(q), "reconnecting needs the background thread")
  dropConnections(b)
  waitUntil("the loss is noticed", 30000, { -> isReconnecting(q) })
  val started = K.monotonicMillis()
  q.close()
  val elapsed = K.monotonicMillis() - started
  assertTrue(elapsed < 5000i64, "close returned after " + elapsed.toString() + " ms of a 60 s wait")
  waitUntil("the background thread ends", 10000, { -> not ActiveMq.hasBackgroundThread(q) })
  assertEqualInt(loginsOf(b), 1, "no attempt was made after close")
  assertTrue(not isReconnecting(q), "a closed connection does not reconnect")
  expectContains(errorOf(q.publish(message("m", "x")), "publish after close"), "connection is closed", "closed")
  // The thread of a healthy connection ends with close as well.
  val healthy = open(knobOptions(b, "/queue/closing"))
  assertTrue(ActiveMq.hasBackgroundThread(healthy), "running")
  healthy.close()
  waitUntil("the background thread ends", 10000, { -> not ActiveMq.hasBackgroundThread(healthy) })
  assertEqualInt(loginsOf(b), 2, "and nothing reconnected behind it")
  stopKnobBroker(b)
}

test "connectFromEnv reads the heart-beat and reconnect settings" {
  val b = startKnobBroker("500,500", 0)
  Env.setVar("LYRIC_CONFIG_MQ_ACTIVEMQ_URL", "stomp://lyric:secret@127.0.0.1:" + KT.serverPort(b.server).toString())
  Env.setVar("LYRIC_CONFIG_MQ_ACTIVEMQ_QUEUENAME", "env.keeper")
  Env.setVar("LYRIC_CONFIG_MQ_ACTIVEMQ_HEARTBEATSENDMS", "0")
  Env.setVar("LYRIC_CONFIG_MQ_ACTIVEMQ_HEARTBEATRECEIVEMS", "0")
  Env.setVar("LYRIC_CONFIG_MQ_ACTIVEMQ_RECONNECT", "False")
  match ActiveMq.connectFromEnv() {
    case Err(e) -> assertTrue(false, e)
    case Ok(q) -> {
      assertEqual(lastOfferTo(b), "0,0", "heart-beats switched off")
      assertTrue(not ActiveMq.hasBackgroundThread(q), "nothing to do in the background")
      q.close()
    }
  }
  Env.setVar("LYRIC_CONFIG_MQ_ACTIVEMQ_HEARTBEATSENDMS", "700")
  Env.setVar("LYRIC_CONFIG_MQ_ACTIVEMQ_HEARTBEATRECEIVEMS", "")
  Env.setVar("LYRIC_CONFIG_MQ_ACTIVEMQ_RECONNECT", "")
  Env.setVar("LYRIC_CONFIG_MQ_ACTIVEMQ_RECONNECTINITIALDELAYMS", "40000")
  Env.setVar("LYRIC_CONFIG_MQ_ACTIVEMQ_RECONNECTMAXATTEMPTS", "2")
  match ActiveMq.connectFromEnv() {
    case Err(e) -> assertTrue(false, e)
    case Ok(q) -> {
      assertEqual(lastOfferTo(b), "700,10000", "one variable set, the other at its default")
      assertTrue(q.options.reconnect.enabled, "reconnecting is on by default")
      assertEqualInt(q.options.reconnect.initialDelayMs, 40000, "initial delay")
      assertEqualInt(q.options.reconnect.maxDelayMs, 40000, "the cap follows a larger initial delay")
      assertEqualInt(q.options.reconnect.maxAttempts, 2, "attempts")
      q.close()
    }
  }
  Env.setVar("LYRIC_CONFIG_MQ_ACTIVEMQ_RECONNECTMAXDELAYMS", "10")
  match ActiveMq.connectFromEnv() {
    case Ok(_) -> assertTrue(false, "a cap below the initial delay")
    case Err(e) -> expectContains(e, "maxDelayMs", "the cap")
  }
  Env.setVar("LYRIC_CONFIG_MQ_ACTIVEMQ_RECONNECTMAXDELAYMS", "")
  Env.setVar("LYRIC_CONFIG_MQ_ACTIVEMQ_RECONNECTINITIALDELAYMS", "")
  Env.setVar("LYRIC_CONFIG_MQ_ACTIVEMQ_RECONNECTMAXATTEMPTS", "many")
  match ActiveMq.connectFromEnv() {
    case Ok(_) -> assertTrue(false, "attempts that are not a number")
    case Err(e) -> expectContains(e, "LYRIC_CONFIG_MQ_ACTIVEMQ_RECONNECTMAXATTEMPTS must be a whole number", "attempts")
  }
  Env.setVar("LYRIC_CONFIG_MQ_ACTIVEMQ_RECONNECTMAXATTEMPTS", "")
  Env.setVar("LYRIC_CONFIG_MQ_ACTIVEMQ_HEARTBEATSENDMS", "-5")
  match ActiveMq.connectFromEnv() {
    case Ok(_) -> assertTrue(false, "a negative interval")
    case Err(e) -> expectContains(e, "LYRIC_CONFIG_MQ_ACTIVEMQ_HEARTBEATSENDMS must not be negative", "interval")
  }
  Env.setVar("LYRIC_CONFIG_MQ_ACTIVEMQ_HEARTBEATSENDMS", "")
  Env.setVar("LYRIC_CONFIG_MQ_ACTIVEMQ_RECONNECT", "sometimes")
  match ActiveMq.connectFromEnv() {
    case Ok(_) -> assertTrue(false, "an unknown reconnect setting")
    case Err(e) -> expectContains(e, "LYRIC_CONFIG_MQ_ACTIVEMQ_RECONNECT must be true or false", "reconnect")
  }
  Env.setVar("LYRIC_CONFIG_MQ_ACTIVEMQ_RECONNECT", "")
  Env.setVar("LYRIC_CONFIG_MQ_ACTIVEMQ_URL", "")
  Env.setVar("LYRIC_CONFIG_MQ_ACTIVEMQ_QUEUENAME", "")
  stopKnobBroker(b)
}

// ─── TLS ─────────────────────────────────────────────────────────────────────
// Options for the TLS broker that trust only the test CA.
func tlsOptionsFor(destination: in String): ActiveMq.ConnectOptions {
  val base = ActiveMq.defaultOptions("127.0.0.1", tlsBrokerPort(), destination)
  base.copy(login = "lyric", passcode = "secret", requestTimeoutMs = 3000, connectTimeoutMs = 5000).withTls().withExclusiveCaCertificate(
    TEST_ONLY_CA
  )
}

func mutualTlsOptionsFor(destination: in String): ActiveMq.ConnectOptions {
  tlsOptionsFor(destination).copy(port = mutualTlsBrokerPort())
}

func makeTlsEnds(): MqConformance.QueueEnds {
  val q = open(tlsOptionsFor(freshName()))
  val publisher: Mq.MessageQueue = q
  val consumer: Mq.QueueConsumer = q
  MqConformance.QueueEnds(publisher = publisher, consumer = consumer)
}

func expectRoundTrip(options: in ActiveMq.ConnectOptions, label: in String): Unit {
  match ActiveMq.connectWith(options) {
    case Err(e) -> assertTrue(false, label + ": " + e)
    case Ok(q) -> {
      mustPublish(q, message("tls-1", label))
      val m = mustTake(q)
      assertEqual(m.body, label, label)
      match q.ack(m.id) {
        case Ok(_) -> ()
        case Err(e) -> assertTrue(false, label + ": ack: " + e)
      }
      q.close()
    }
  }
}

func expectHandshakeFailure(options: in ActiveMq.ConnectOptions, label: in String): Unit {
  val e = errorOfConnect(options)
  expectContains(e, "activemq: TLS handshake with 127.0.0.1:", label)
  assertTrue(not Str.contains(e, "secret"), label + ": the passcode is not echoed")
}

test "the backend conforms to MessageQueue and QueueConsumer over TLS" {
  requireConformance(MqConformance.runQueue({ -> makeTlsEnds() }))
}

test "TLS: the server is verified against a configured CA, exclusive or additional" {
  expectRoundTrip(tlsOptionsFor(freshName()), "exclusive CA")
  val additional = ActiveMq.defaultOptions("127.0.0.1", tlsBrokerPort(), freshName()).copy(login = "lyric", passcode = "secret", requestTimeoutMs = 3000).withTls().withCaCertificate(
    TEST_ONLY_CA
  )
  expectRoundTrip(additional, "additional CA")
  val q = open(tlsOptionsFor(freshName()))
  assertEqual(ActiveMq.serverInfo(q), "FakeMQ/1.0", "the CONNECTED frame came through TLS")
  q.close()
}

test "TLS: a server whose CA is not trusted is refused" {
  val untrusted = ActiveMq.defaultOptions("127.0.0.1", tlsBrokerPort(), freshName()).copy(login = "lyric", passcode = "secret", connectTimeoutMs = 5000).withTls()
  expectHandshakeFailure(untrusted, "platform trust store only")
  expectHandshakeFailure(untrusted.withExclusiveCaCertificate(TEST_ONLY_OTHER_CA), "another CA")
  expectHandshakeFailure(untrusted.withCaCertificate(TEST_ONLY_OTHER_CA), "another CA, additional")
}

test "TLS: the certificate name is checked against the host or the configured server name" {
  // The certificate names localhost and 127.0.0.1.
  expectRoundTrip(tlsOptionsFor(freshName()).withTlsServerName("localhost"), "server name override")
  expectHandshakeFailure(tlsOptionsFor(freshName()).withTlsServerName("broker.invalid"), "a name the certificate lacks")
}

test "TLS: verification is skipped only with both keys of the insecure policy" {
  val insecure = ActiveMq.defaultOptions("127.0.0.1", tlsBrokerPort(), freshName()).copy(login = "lyric", passcode = "secret", requestTimeoutMs = 3000, connectTimeoutMs = 5000).withTls().withTlsServerName("broker.invalid").withInsecureSkipVerify()
  Env.setVar("LYRIC_TLS_ALLOW_INSECURE", "")
  assertTrue(not ActiveMq.skipsVerification(insecure.tls), "the option alone does not skip verification")
  expectHandshakeFailure(insecure, "the option without the environment variable")
  Env.setVar("LYRIC_TLS_ALLOW_INSECURE", "true")
  expectHandshakeFailure(insecure, "the variable must be exactly 1")
  Env.setVar("LYRIC_TLS_ALLOW_INSECURE", "1")
  val secure = tlsOptionsFor(freshName()).withTlsServerName("broker.invalid")
  assertTrue(not ActiveMq.skipsVerification(secure.tls), "the environment variable alone does not skip verification")
  expectHandshakeFailure(secure, "the environment variable without the option")
  assertTrue(ActiveMq.skipsVerification(insecure.tls), "both keys")
  expectRoundTrip(insecure, "both keys")
  Env.setVar("LYRIC_TLS_ALLOW_INSECURE", "")
}

test "TLS: a required client certificate is presented, and its absence refused" {
  val base = mutualTlsOptionsFor(freshName())
  expectRoundTrip(base.withClientIdentity(TEST_ONLY_CLIENT_CERT, TEST_ONLY_CLIENT_KEY), "mutual TLS")
  // With TLS 1.3 the client learns of the refusal after its own handshake
  // has finished, so the failure is either a handshake or a CONNECT error.
  val without = errorOfConnect(base)
  assertTrue(Str.startsWith(without, "activemq: "), "no client certificate: " + without)
  assertTrue(without != "connected", "no client certificate must not connect")
  val stray = errorOfConnect(base.withClientIdentity(TEST_ONLY_STRAY_CERT, TEST_ONLY_STRAY_KEY))
  assertTrue(Str.startsWith(stray, "activemq: "), "a client certificate from another CA: " + stray)
  assertTrue(stray != "connected", "a client certificate from another CA must not connect")
}

test "TLS: a minimum version of TLS 1.3 connects" {
  expectRoundTrip(tlsOptionsFor(freshName()).withMinTlsVersion(Tls.TlsVersion.Tls13), "TLS 1.3 only")
}

test "TLS: consume timeouts, large bodies and broker failures behave as over plain TCP" {
  val q = open(tlsOptionsFor(freshName()))
  expectTimeout(q, "nothing published yet")
  expectTimeout(q, "still nothing")
  var body = ""
  var i = 0
  while i < 4000 {
    body = body + "0123456789abcdefghijklmnopqrstuvwxyz-" + i.toString() + ";"
    i += 1
  }
  mustPublish(q, message("big", body))
  val got = mustTake(q)
  assertEqual(got.body, body, "a body of several TLS records")
  match q.ack("big") {
    case Ok(_) -> ()
    case Err(e) -> assertTrue(false, "ack: " + e)
  }
  expectTimeout(q, "after the acknowledgement")
  q.close()

  val failing = open(tlsOptionsFor("/queue/fail.error"))
  expectContains(errorOf(failing.publish(message("e", "x")), "ERROR"), "activemq: broker error:", "an ERROR frame")
  failing.close()
  val dropped = open(tlsOptionsFor("/queue/fail.drop"))
  expectContains(
    errorOf(dropped.publish(message("d", "x")), "drop"),
    "activemq: connection lost",
    "a dropped connection"
  )
  dropped.close()
  val silent = open(tlsOptionsFor("/queue/fail.silent").copy(requestTimeoutMs = 300))
  expectContains(errorOf(silent.publish(message("s", "x")), "silent"), "did not answer within 300 ms", "no receipt")
  silent.close()
  match ActiveMq.connectWith(tlsOptionsFor(freshName()).copy(passcode = "wrong")) {
    case Ok(_) -> assertTrue(false, "the passcode is wrong")
    case Err(e) -> expectContains(e, "password is invalid", "the broker's ERROR on CONNECT")
  }
}

test "TLS: the transport must match the connector" {
  // TLS towards a plain connector: the handshake never completes.
  val tlsToPlain = ActiveMq.defaultOptions("127.0.0.1", brokerPort(), freshName()).copy(connectTimeoutMs = 1500).withTls().withExclusiveCaCertificate(
    TEST_ONLY_CA
  )
  expectHandshakeFailure(tlsToPlain, "TLS to a plain connector")
  // Plain STOMP towards a TLS connector: no CONNECTED frame comes back.
  val plainToTls = ActiveMq.defaultOptions("127.0.0.1", tlsBrokerPort(), freshName())
  val e = errorOfConnect(plainToTls.copy(login = "lyric", passcode = "secret", requestTimeoutMs = 1500))
  assertTrue(Str.startsWith(e, "activemq: "), "plain to a TLS connector: " + e)
  assertTrue(e != "connected", "plain STOMP must not get through a TLS connector")
}

test "TLS options are validated before any connection is made" {
  val plain = ActiveMq.defaultOptions("127.0.0.1", 1, "/queue/a")
  expectContains(errorOfConnect(plain.withCaCertificate(TEST_ONLY_CA)), "the connection is not TLS", "a CA without TLS")
  expectContains(errorOfConnect(plain.withInsecureSkipVerify()), "the connection is not TLS", "skip-verify without TLS")
  expectContains(errorOfConnect(plain.withTlsServerName("h")), "the connection is not TLS", "a server name without TLS")
  expectContains(
    errorOfConnect(plain.withMinTlsVersion(Tls.TlsVersion.Tls13)),
    "the connection is not TLS",
    "a version without TLS"
  )
  val tls = plain.withTls()
  expectContains(errorOfConnect(tls.withCaCertificate("not a certificate")), "TLS CA certificate:", "a malformed CA")
  expectContains(
    errorOfConnect(tls.withExclusiveCaCertificate("-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n")),
    "TLS exclusive CA certificate:",
    "a malformed exclusive CA"
  )
  expectContains(
    errorOfConnect(tls.withClientIdentity(TEST_ONLY_CLIENT_CERT, "")),
    "without its private key",
    "a certificate alone"
  )
  expectContains(
    errorOfConnect(tls.withClientIdentity("", TEST_ONLY_CLIENT_KEY)),
    "without its certificate",
    "a key alone"
  )
  expectContains(
    errorOfConnect(tls.withClientIdentity(TEST_ONLY_CLIENT_CERT, "not a key")),
    "TLS client identity:",
    "a malformed key"
  )
  expectContains(
    errorOfConnect(tls.withClientIdentity("not a certificate", TEST_ONLY_CLIENT_KEY)),
    "TLS client identity:",
    "a malformed certificate"
  )
  val sec1 = "-----BEGIN EC PRIVATE KEY-----\nAAAA\n-----END EC PRIVATE KEY-----\n"
  expectContains(errorOfConnect(tls.withClientIdentity(TEST_ONLY_CLIENT_CERT, sec1)), "PKCS8", "a SEC1 key is refused")
  val e = errorOfConnect(tls.withClientIdentity(TEST_ONLY_CLIENT_CERT, TEST_ONLY_STRAY_KEY))
  expectContains(e, "TLS client identity:", "a key that belongs to another certificate")
}

test "a stomp+ssl URL turns TLS on, with the TLS default port" {
  match ActiveMq.optionsFromUrl("stomp+ssl://u:p@broker.example", "orders") {
    case Err(e) -> assertTrue(false, e)
    case Ok(o) -> {
      assertTrue(o.tls.enabled, "TLS")
      assertEqualInt(o.port, 61614, "the default TLS port")
      assertEqual(o.destination, "/queue/orders", "destination")
      assertEqual(o.login, "u", "login")
      assertTrue(not o.tls.insecureSkipVerify, "verification is on")
    }
  }
  match ActiveMq.optionsFromUrl("stomp://broker.example", "orders") {
    case Err(e) -> assertTrue(false, e)
    case Ok(o) -> assertTrue(not o.tls.enabled, "stomp:// is plain TCP")
  }
  val url = "stomp+ssl://lyric:secret@127.0.0.1:" + tlsBrokerPort().toString()
  // No CA is configured, so the URL alone must fail on the test certificate.
  match ActiveMq.connectUrl(url, "orders") {
    case Ok(_) -> assertTrue(false, "the test CA is not in the platform trust store")
    case Err(e) -> expectContains(e, "activemq: TLS handshake with 127.0.0.1:", "connectUrl over TLS")
  }
  match ActiveMq.optionsFromUrl(url, freshName()) {
    case Err(e) -> assertTrue(false, e)
    case Ok(o) -> expectRoundTrip(o.copy(requestTimeoutMs = 3000).withCaCertificate(TEST_ONLY_CA), "URL plus a CA")
  }
}

func writeTestOnlyFile(name: in String, text: in String): String {
  val path = Env.getVarOrDefault("TMPDIR", "/tmp") + "/lyric-mq-activemq-test-only-" + name
  match File.writeText(path, text) {
    case Ok(_) -> path
    case Err(_) -> panic("cannot write " + path)
  }
}

test "connectFromEnv reads the TLS settings" {
  val caPath = writeTestOnlyFile("ca.pem", TEST_ONLY_CA)
  val certPath = writeTestOnlyFile("client.pem", TEST_ONLY_CLIENT_CERT)
  val keyPath = writeTestOnlyFile("client.key", TEST_ONLY_CLIENT_KEY)
  val prefix = "LYRIC_CONFIG_MQ_ACTIVEMQ_"
  slot.names = slot.names + 1
  Env.setVar(prefix + "URL", "stomp+ssl://lyric:secret@127.0.0.1:" + mutualTlsBrokerPort().toString())
  Env.setVar(prefix + "QUEUENAME", "env.tls." + slot.names.toString())
  Env.setVar(prefix + "ACKMODE", "")
  Env.setVar(prefix + "TLSCAFILE", caPath + ".missing")
  match ActiveMq.connectFromEnv() {
    case Ok(_) -> assertTrue(false, "the CA file does not exist")
    case Err(e) -> expectContains(e, "LYRIC_CONFIG_MQ_ACTIVEMQ_TLSCAFILE", "an unreadable CA file")
  }
  Env.setVar(prefix + "TLSCAFILE", caPath)
  Env.setVar(prefix + "TLSCAEXCLUSIVE", "maybe")
  match ActiveMq.connectFromEnv() {
    case Ok(_) -> assertTrue(false, "TLSCAEXCLUSIVE is not a boolean")
    case Err(e) -> expectContains(e, "TLSCAEXCLUSIVE must be true or false", "a bad boolean")
  }
  Env.setVar(prefix + "TLSCAEXCLUSIVE", "true")
  Env.setVar(prefix + "TLSMINVERSION", "1.1")
  match ActiveMq.connectFromEnv() {
    case Ok(_) -> assertTrue(false, "TLS 1.1 cannot be asked for")
    case Err(e) -> expectContains(e, "TLSMINVERSION must be 1.2 or 1.3", "a bad version")
  }
  Env.setVar(prefix + "TLSMINVERSION", "1.3")
  Env.setVar(prefix + "TLSCERTFILE", certPath)
  match ActiveMq.connectFromEnv() {
    case Ok(_) -> assertTrue(false, "a certificate without a key")
    case Err(e) -> expectContains(e, "without its private key", "a certificate file alone")
  }
  Env.setVar(prefix + "TLSKEYFILE", keyPath)
  Env.setVar(prefix + "TLSSERVERNAME", "localhost")
  match ActiveMq.connectFromEnv() {
    case Err(e) -> assertTrue(false, e)
    case Ok(q) -> {
      mustPublish(q, message("env-tls", "from env over TLS"))
      assertEqual(mustTake(q).body, "from env over TLS", "round trip")
      q.close()
    }
  }
  Env.setVar(prefix + "TLSSERVERNAME", "broker.invalid")
  match ActiveMq.connectFromEnv() {
    case Ok(_) -> assertTrue(false, "the certificate does not name broker.invalid")
    case Err(e) -> expectContains(e, "TLS handshake", "the configured server name is checked")
  }
  for field in ["URL", "QUEUENAME", "TLSCAFILE", "TLSCAEXCLUSIVE", "TLSMINVERSION", "TLSCERTFILE", "TLSKEYFILE", "TLSSERVERNAME"] {
    Env.setVar(prefix + field, "")
  }
  val _ = File.deleteFile(caPath)
  val _ = File.deleteFile(certPath)
  val _ = File.deleteFile(keyPath)
}

// ─── TEST-ONLY certificates and keys ─────────────────────────────────────────
// Everything below was generated for this test file with `openssl` (EC P-256,
// valid until 2126) and is used by nothing else. The private keys are public
// by being here: never trust these certificates outside this file.
//
//   TEST_ONLY_CA            self-signed CA
//   TEST_ONLY_SERVER_*      issued by the CA; SAN DNS:localhost, IP:127.0.0.1
//   TEST_ONLY_CLIENT_*      issued by the CA; clientAuth
//   TEST_ONLY_OTHER_CA      a second, unrelated self-signed CA
//   TEST_ONLY_STRAY_*       a client certificate issued by the other CA
// test-only
val TEST_ONLY_CA: String = "-----BEGIN CERTIFICATE-----\n" + "MIIB4TCCAYegAwIBAgIUMdZcqE1iGtInxR0FPbqibzyXeHYwCgYIKoZIzj0EAwIw\n" + "PTEkMCIGA1UECgwbbHlyaWMtbXEtYWN0aXZlbXEgVEVTVCBPTkxZMRUwEwYDVQQD\n" + "DAx0ZXN0LW9ubHkgQ0EwIBcNMjYxMTEyMjA0MzQyWhgPMjEyNjEwMTkyMDQzNDJa\n" + "MD0xJDAiBgNVBAoMG2x5cmljLW1xLWFjdGl2ZW1xIFRFU1QgT05MWTEVMBMGA1UE\n" + "AwwMdGVzdC1vbmx5IENBMFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEYAYCGXrA\n" + "I2ewAr+wvyoRAfbXD36+mTug7dD9P79g9FPZ4pb/G1LPreTm3JJpvL0rPMcjyZ6i\n" + "atNgUShoEQtM1KNjMGEwHQYDVR0OBBYEFBO96Lq71dvpw7aQAdoxUqxZdTZiMB8G\n" + "A1UdIwQYMBaAFBO96Lq71dvpw7aQAdoxUqxZdTZiMA8GA1UdEwEB/wQFMAMBAf8w\n" + "DgYDVR0PAQH/BAQDAgEGMAoGCCqGSM49BAMCA0gAMEUCIEPshpyn3+k8gCVsrkjG\n" + "hpyEoRE8+JDdAiWTbTZM17iqAiEAzarnIdQWuq0m+22jQ8ljnODgMXejV5pJ30E3\n" + "C5N+2hs=\n" + "-----END CERTIFICATE-----\n"

// test-only
val TEST_ONLY_SERVER_CERT: String = "-----BEGIN CERTIFICATE-----\n" + "MIICATCCAaagAwIBAgIUCKVnu3yl92vqNWhjbOod6MrPh4gwCgYIKoZIzj0EAwIw\n" + "PTEkMCIGA1UECgwbbHlyaWMtbXEtYWN0aXZlbXEgVEVTVCBPTkxZMRUwEwYDVQQD\n" + "DAx0ZXN0LW9ubHkgQ0EwIBcNMjYxMTEyMjA0MzQyWhgPMjEyNjEwMTkyMDQzNDJa\n" + "MDoxJDAiBgNVBAoMG2x5cmljLW1xLWFjdGl2ZW1xIFRFU1QgT05MWTESMBAGA1UE\n" + "AwwJbG9jYWxob3N0MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAE4UqMwZLtPgfr\n" + "qsc8bdkA+Ma4as92fSn9nFyRnSlkW2qbsPx1fsPpbzdnx8noQeU9ng2B7JZdGFT/\n" + "N41Ri4aJnqOBhDCBgTAaBgNVHREEEzARgglsb2NhbGhvc3SHBH8AAAEwEwYDVR0l\n" + "BAwwCgYIKwYBBQUHAwEwDgYDVR0PAQH/BAQDAgeAMB0GA1UdDgQWBBQwtXKELMB8\n" + "diGydfIRw6aYVl0dVzAfBgNVHSMEGDAWgBQTvei6u9Xb6cO2kAHaMVKsWXU2YjAK\n" + "BggqhkjOPQQDAgNJADBGAiEA9nWrmJBPYADaQCWfcLkxFIe+5NJtvPZWY4DzWJoz\n" + "cV0CIQD4sNqVPsbSEbuQuHAh+m82dFOxaOzuTU+Ptb+s6uKrzw==\n" + "-----END CERTIFICATE-----\n"

// test-only private key
val TEST_ONLY_SERVER_KEY: String = "-----BEGIN PRIVATE KEY-----\n" + "MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQg0iMsh8kO7i1GbAKc\n" + "gI7OOrGRS/k+bbNUtMsYmBA68X+hRANCAAThSozBku0+B+uqxzxt2QD4xrhqz3Z9\n" + "Kf2cXJGdKWRbapuw/HV+w+lvN2fHyehB5T2eDYHsll0YVP83jVGLhome\n" + "-----END PRIVATE KEY-----\n"

// test-only
val TEST_ONLY_CLIENT_CERT: String = "-----BEGIN CERTIFICATE-----\n" + "MIIB6jCCAY+gAwIBAgIUCKVnu3yl92vqNWhjbOod6MrPh4kwCgYIKoZIzj0EAwIw\n" + "PTEkMCIGA1UECgwbbHlyaWMtbXEtYWN0aXZlbXEgVEVTVCBPTkxZMRUwEwYDVQQD\n" + "DAx0ZXN0LW9ubHkgQ0EwIBcNMjYxMTEyMjA0MzQyWhgPMjEyNjEwMTkyMDQzNDJa\n" + "MEExJDAiBgNVBAoMG2x5cmljLW1xLWFjdGl2ZW1xIFRFU1QgT05MWTEZMBcGA1UE\n" + "AwwQdGVzdC1vbmx5IGNsaWVudDBZMBMGByqGSM49AgEGCCqGSM49AwEHA0IABBPG\n" + "NQcnPleid2CUSj3TcqY/7T2mH54mNqqRP89ugZtmCmPrKeguQejvY8i04nToDPGg\n" + "9Lno+WXA7UeDh7aEGYijZzBlMBMGA1UdJQQMMAoGCCsGAQUFBwMCMA4GA1UdDwEB\n" + "/wQEAwIHgDAdBgNVHQ4EFgQULwRAoPjdU+MFvJYdA4EeB9XhWDUwHwYDVR0jBBgw\n" + "FoAUE73ourvV2+nDtpAB2jFSrFl1NmIwCgYIKoZIzj0EAwIDSQAwRgIhAPHfud/j\n" + "lfy9BqSFYumS5MW7ljCZC+hBeO9fXZiiSbgDAiEAx+DPE3IVzjYVclCL4k/TT2+y\n" + "C9kbvqMHtdvi2dWjbJc=\n" + "-----END CERTIFICATE-----\n"

// test-only private key
val TEST_ONLY_CLIENT_KEY: String = "-----BEGIN PRIVATE KEY-----\n" + "MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgn3/zm5+BLXEqOfVa\n" + "81O7AX4r9UiOMVTJpfWUi9mn5CChRANCAAQTxjUHJz5XondglEo903KmP+09ph+e\n" + "JjaqkT/PboGbZgpj6ynoLkHo72PItOJ06AzxoPS56PllwO1Hg4e2hBmI\n" + "-----END PRIVATE KEY-----\n"

// test-only
val TEST_ONLY_OTHER_CA: String = "-----BEGIN CERTIFICATE-----\n" + "MIIB7DCCAZOgAwIBAgIUIff2SUzilDgPnfhpZuCjgCv0cuswCgYIKoZIzj0EAwIw\n" + "QzEkMCIGA1UECgwbbHlyaWMtbXEtYWN0aXZlbXEgVEVTVCBPTkxZMRswGQYDVQQD\n" + "DBJ0ZXN0LW9ubHkgb3RoZXIgQ0EwIBcNMjYxMTEyMjA0MzQyWhgPMjEyNjEwMTky\n" + "MDQzNDJaMEMxJDAiBgNVBAoMG2x5cmljLW1xLWFjdGl2ZW1xIFRFU1QgT05MWTEb\n" + "MBkGA1UEAwwSdGVzdC1vbmx5IG90aGVyIENBMFkwEwYHKoZIzj0CAQYIKoZIzj0D\n" + "AQcDQgAEUR1JFo0CD3Njte9zvDQRwZpfqkN99Cr/tWDv/py3OxILiIUZxPIAJjKZ\n" + "IwaEIBHAjfKazW3H6DwAKRDyLRMuKaNjMGEwHQYDVR0OBBYEFOc3kuRypGZLrgq+\n" + "ULOmQ53rPQc7MB8GA1UdIwQYMBaAFOc3kuRypGZLrgq+ULOmQ53rPQc7MA8GA1Ud\n" + "EwEB/wQFMAMBAf8wDgYDVR0PAQH/BAQDAgEGMAoGCCqGSM49BAMCA0cAMEQCIFDY\n" + "fnOZLZvo50DDnsjqmvU8QERfKI4BuZKAF1URYAMgAiA+BnCvnTP5byJjxU558h8x\n" + "lTB/jAfGEwVj5amWPzTAww==\n" + "-----END CERTIFICATE-----\n"

// test-only
val TEST_ONLY_STRAY_CERT: String = "-----BEGIN CERTIFICATE-----\n" + "MIIB9DCCAZugAwIBAgIUW4CJBIZ52Zh1nR8hK5SdBEl3Uh0wCgYIKoZIzj0EAwIw\n" + "QzEkMCIGA1UECgwbbHlyaWMtbXEtYWN0aXZlbXEgVEVTVCBPTkxZMRswGQYDVQQD\n" + "DBJ0ZXN0LW9ubHkgb3RoZXIgQ0EwIBcNMjYxMTEyMjA0MzQyWhgPMjEyNjEwMTky\n" + "MDQzNDJaMEcxJDAiBgNVBAoMG2x5cmljLW1xLWFjdGl2ZW1xIFRFU1QgT05MWTEf\n" + "MB0GA1UEAwwWdGVzdC1vbmx5IHN0cmF5IGNsaWVudDBZMBMGByqGSM49AgEGCCqG\n" + "SM49AwEHA0IABIkfflimVwpiL9W8mKqin/dEkFuBwKThiXNqht9NVJ/5ONO4xJgo\n" + "JKcroGDsJK0HsEL5vjBGzRxk6sdGUbh/p+2jZzBlMBMGA1UdJQQMMAoGCCsGAQUF\n" + "BwMCMA4GA1UdDwEB/wQEAwIHgDAdBgNVHQ4EFgQU8hV86rSPyWSoYYNIY19fk/G0\n" + "ptIwHwYDVR0jBBgwFoAU5zeS5HKkZkuuCr5Qs6ZDnes9BzswCgYIKoZIzj0EAwID\n" + "RwAwRAIgZDqq5uIxulT/iFszvxubUpoYw/9zr/0EhsEkzkX2ErkCIH9j9qmllwSG\n" + "ce8eoOWDafL1iEcoqdAufQP53uCudIrc\n" + "-----END CERTIFICATE-----\n"

// test-only private key
val TEST_ONLY_STRAY_KEY: String = "-----BEGIN PRIVATE KEY-----\n" + "MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQg/irIMjHltlSLzkqe\n" + "AitdTtpknt7mMQqZFfyCqWygP3ihRANCAASJH35YplcKYi/VvJiqop/3RJBbgcCk\n" + "4YlzaobfTVSf+TjTuMSYKCSnK6Bg7CStB7BC+b4wRs0cZOrHRlG4f6ft\n" + "-----END PRIVATE KEY-----\n"
