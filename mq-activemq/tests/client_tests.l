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
import Std.String as Str
import Std.Environment as Env
import Std.File as File
import Std.Tls as Tls
import Testing.Conformance
import Testing.Conformance.Mq as MqConformance
import Mq
import Mq.ActiveMq as ActiveMq
import Mq.ActiveMq.Extras as Extras
import Mq.ActiveMq.Kernel as K
import Mq.ActiveMq.Stomp as Stomp

// ─── The fake broker ─────────────────────────────────────────────────────────
record Stored {
  headers: slice[Stomp.Header]
  body: slice[Byte]
  redelivered: Bool
}

ref record FakeBroker {
  server: K.ServerSock
  // Whether every accepted connection starts with a TLS handshake.
  secured: Bool
  tls: K.ServerTls
  lock: K.Lock
  var queues: Map[String, List[Stored]]
  var counter: Int
}

ref record Session {
  broker: FakeBroker
  sock: K.Sock
  var subscriptionId: String
  var destination: String
  var unacked: Map[String, Stored]
  var unackedOrder: List[String]
  var txSends: Map[String, List[Stomp.Frame]]
  var txAcks: Map[String, List[String]]
}

func h(name: in String, value: in String): Stomp.Header {
  Stomp.Header(name = name, value = value)
}

func reply(s: in Session, command: in String, headers: in slice[Stomp.Header], body: in String): Unit {
  match Stomp.encodeFrame(Stomp.textFrame(command, headers, body)) {
    case Ok(bytes) -> {
      val _ = K.sockWrite(s.sock, bytes)
    }
    case Err(_) -> ()
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
    if stored.redelivered {
      headers.add(h("redelivered", "true"))
    }
    s.unacked[ackId] = stored
    s.unackedOrder.add(ackId)
    match Stomp.encodeFrame(Stomp.Frame(command = "MESSAGE", headers = headers.toArray(), body = stored.body)) {
      case Ok(bytes) -> {
        val _ = K.sockWrite(s.sock, bytes)
      }
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
  K.lockExit(b.lock)
  s.unacked = newMap()
  s.unackedOrder = newList()
}

func settleFake(s: in Session, frame: in Stomp.Frame, ackId: in String): Bool {
  if s.unacked.containsKey(ackId) {
    s.unacked.remove(ackId)
    return true
  }
  brokerError(s, frame, "Unexpected ACK received for message-id [null]")
  false
}

// Handle one frame. False ends the connection.
func handle(s: in Session, frame: in Stomp.Frame): Bool {
  val command = frame.command
  val tx = Stomp.headerOr(frame, "transaction", "")
  if command == "CONNECT" or command == "STOMP" {
    if Stomp.headerOr(frame, "passcode", "") == "wrong" {
      reply(
        s,
        "ERROR",
        [h("message", "User name [" + Stomp.headerOr(frame, "login", "") + "] or password is invalid.")],
        ""
      )
      return false
    }
    val version = if Stomp.headerOr(frame, "login", "") == "ancient" { "1.0" } else { "1.2" }
    reply(s, "CONNECTED", [h("server", "FakeMQ/1.0"), h("version", version), h("heart-beat", "0,0")], "")
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
    if destination == "/queue/fail.silent" {
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
    s.subscriptionId = Stomp.headerOr(frame, "id", "")
    s.destination = Stomp.headerOr(frame, "destination", "")
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
    if settleFake(s, frame, ackId) {
      receipt(s, frame)
    }
    return true
  }
  if command == "NACK" {
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
    subscriptionId = "",
    destination = "",
    unacked = newMap(),
    unackedOrder = newList(),
    txSends = newMap(),
    txAcks = newMap()
  )
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
        case Ok(Data(bytes)) -> Stomp.feed(decoder, bytes)
        case _ -> {
          open = false
        }
      }
    }
  }
  returnUnacked(s)
  K.sockClose(sock)
}

// A connection whose handshake fails is dropped; the broker keeps listening.
func serveAccepted(b: in FakeBroker, accepted: in K.Sock): Unit {
  if not b.secured {
    serve(b, accepted)
    return
  }
  match K.sockAcceptTls(accepted, b.tls, 5000) {
    case Ok(sock) -> serve(b, sock)
    case Err(_) -> ()
  }
}

func acceptLoop(b: in FakeBroker): Unit {
  var listening = true
  while listening {
    match K.serverAccept(b.server) {
      case Ok(sock) -> {
        val _ = K.startBackground({ -> serveAccepted(b, sock) })
      }
      case Err(_) -> {
        listening = false
      }
    }
  }
}

func startBrokerWith(secured: in Bool, tls: in K.ServerTls): FakeBroker {
  val server = match K.serverListen("127.0.0.1", 0) {
    case Ok(s) -> s
    case Err(e) -> panic("the fake broker cannot listen: " + e)
  }
  val b = FakeBroker(server = server, secured = secured, tls = tls, lock = K.newLock(), queues = newMap(), counter = 0)
  val _ = K.startBackground({ -> acceptLoop(b) })
  b
}

func serverTls(clientCaPem: in String): K.ServerTls {
  K.ServerTls(certPem = TEST_ONLY_SERVER_CERT, keyPem = TEST_ONLY_SERVER_KEY, clientCaPem = clientCaPem)
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
    case Some(b) -> K.serverPort(b.server)
    case None -> {
      val b = startBrokerWith(false, serverTls(""))
      slot.broker = Some(value = b)
      K.serverPort(b.server)
    }
  }
}

// The broker behind TLS (server certificate only).
func tlsBrokerPort(): Int {
  match slot.tlsBroker {
    case Some(b) -> K.serverPort(b.server)
    case None -> {
      val b = startBrokerWith(true, serverTls(""))
      slot.tlsBroker = Some(value = b)
      K.serverPort(b.server)
    }
  }
}

// The broker behind TLS that requires a client certificate from the CA.
func mutualTlsBrokerPort(): Int {
  match slot.mutualTlsBroker {
    case Some(b) -> K.serverPort(b.server)
    case None -> {
      val b = startBrokerWith(true, serverTls(TEST_ONLY_CA))
      slot.mutualTlsBroker = Some(value = b)
      K.serverPort(b.server)
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
    case Err(e) -> panic("consume failed: " + e)
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
  val server = match K.serverListen("127.0.0.1", 0) { case Ok(s) -> s; case Err(e) -> panic(e) }
  val port = K.serverPort(server)
  K.serverClose(server)
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

test "a broker that drops the connection during a publish" {
  val q = openFresh()
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
  val q = open(optionsFor("/queue/fail.dropconsume"))
  val e = errorOf(q.consume(2000, { m -> Ok(()) }), "consume while the broker hangs up")
  assertTrue(e != "timeout", "a lost connection is not a timeout")
  expectContains(e, "activemq: connection lost: the broker closed the connection", "lost")
  q.close()
}

test "a broker that never confirms a publish times the request out" {
  val options = optionsFor(freshName())
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
  val full = base.copy(
    persistent = false,
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

test "a body that is not UTF-8 is rejected to the broker and reported" {
  val destination = freshName()
  val binary: slice[Byte] = [255u8, 254u8, 0u8, 1u8]
  val none: slice[Stomp.Header] = []
  rawSend(destination, none, binary)
  val q = open(optionsFor(destination))
  val e = errorOf(q.consume(2000, { m -> Ok(()) }), "consume a binary message")
  expectContains(e, "cannot be delivered: its body is not UTF-8 text", "reason")
  expectContains(e, "rejected to the broker's dead-letter handling", "outcome")
  assertEqualInt(ActiveMq.unsettledCount(q), 0, "it is not in flight")
  expectTimeout(q, "the rejected message does not come back")
  mustPublish(q, message("next", "fine"))
  assertEqual(mustTake(q).id, "next", "the destination is not blocked")
  q.close()
}

test "malformed lyric headers are rejected to the broker and reported" {
  val destination = freshName()
  rawSend(destination, [h("lyric_id", "x"), h("lyric_headers", "not json")], encodeUtf8("body"))
  rawSend(destination, [h("lyric_id", "y"), h("lyric_delivery_count", "many")], encodeUtf8("body"))
  val q = open(optionsFor(destination))
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
  val publisher = K.startBackground({ -> publishMany(q, count) })
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
  K.waitBackground(publisher)
  assertEqualInt(got.messages.count, count, "every message arrived")
  var n = 0
  while n < got.messages.count {
    assertEqual(got.messages[n].id, "t-" + n.toString(), "in publish order")
    n += 1
  }
  assertEqualInt(ActiveMq.unsettledCount(q), 0, "all settled")
  q.close()
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
