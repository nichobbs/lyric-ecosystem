//! tests/client_tests.l - the RabbitMQ backend against an in-process AMQP
//! 0-9-1 broker on a loopback socket.
//!
//! `FakeBroker` below speaks enough of the server side to stand in for
//! RabbitMQ: the protocol header, `connection.start` / `tune` / `open`, one
//! channel in confirm mode, queues and exchanges (default, direct, fanout),
//! `basic.publish` with confirms and returns, `basic.qos` and
//! `basic.consume` with a prefetch window, `basic.ack` / `basic.nack`,
//! redelivery (the `redelivered` flag, and `x-delivery-count` on a queue
//! declared as quorum), dead-lettering through the default exchange, channel
//! errors with the real reply codes, and heartbeats.
//!
//! A few names trigger failures on purpose:
//!   password `wrong`              connection.close 403 ACCESS_REFUSED
//!   user `legacy`                 the socket closes at authentication
//!   virtual host `missing`        connection.close 530 NOT_ALLOWED
//!   routing key `fail.drop`       a publish makes the broker close the socket
//!   routing key `fail.silent`     a publish is accepted but never confirmed
//!   routing key `fail.nack`       a publish is answered with basic.nack
//!   routing key `fail.blocked`    a publish is answered with connection.blocked
//!   queue `fail.dropconsume`      basic.consume is answered, then the socket closes
//!   queue `fail.forced`           basic.consume is answered, then connection.close 320
//!   queue `fail.cancel`           basic.consume is answered, then the queue is
//!                                 deleted and the consumer cancelled
//!   message id ending `+hold`     the confirm is held back and covered by the
//!                                 next one (`multiple`)
//!
//! and `FakeBroker.mode` changes the handshake: `mismatch` answers the
//! protocol header with a protocol header, `old-version` announces AMQP 8-0,
//! `no-plain` offers only EXTERNAL, `mute` never sends a heartbeat.
//!
//! The broker is started on plain TCP, behind TLS, and behind TLS with a
//! required client certificate. The TLS ones use the TEST-ONLY certificates
//! at the end of this file.
//!
//! The same client runs against a real RabbitMQ in
//! `tests/integration/rabbitmq_integration_tests.l`.
@test_module
package Mq.RabbitMq.ClientTests

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
import Mq.RabbitMq as RabbitMq
import Mq.RabbitMq.Extras
import Mq.RabbitMq.Extras as Extras
import Mq.RabbitMq.Kernel as K
import Mq.RabbitMq.Amqp as Amqp

// ─── The fake broker ─────────────────────────────────────────────────────────
record Stored {
  properties: Amqp.Properties
  body: slice[Byte]
  redelivered: Bool
  // How often the message was requeued; reported as x-delivery-count by a
  // quorum queue.
  requeues: Int
}

ref record FakeQueue {
  name: String
  durable: Bool
  arguments: slice[Amqp.TableEntry]
  var ready: List[Stored]
}

record Binding {
  queue: String
  exchange: String
  key: String
}

ref record FakeBroker {
  server: K.ServerSock
  // Whether every accepted connection starts with a TLS handshake.
  secured: Bool
  tls: K.ServerTls
  lock: K.Lock
  var queues: Map[String, FakeQueue]
  var exchanges: Map[String, String]
  var bindings: List[Binding]
  // Knobs.
  var mode: String
  var frameMax: Int
  var heartbeat: Int
  // What the broker saw.
  var heartbeatsSeen: Int
  var tunedHeartbeat: Int
  var tunedFrameMax: Long
  var clientProduct: String
  var connectionName: String
  var cleanCloses: Int
  var largestFrame: Int
}

record Unacked {
  queue: String
  stored: Stored
}

ref record Session {
  broker: FakeBroker
  sock: K.Sock
  var frameMax: Int
  var heartbeatMs: Long
  var lastBeat: Long
  var closing: Bool
  var channelClosing: Bool
  var confirming: Bool
  var publishSeq: Long
  var held: Bool
  var consumerQueue: String
  var consumerTag: String
  var prefetch: Int
  var nextTag: Long
  var unacked: Map[Long, Unacked]
  var unackedOrder: List[Long]
  var pubActive: Bool
  var pubExchange: String
  var pubKey: String
  var pubMandatory: Bool
  var pubHasHeader: Bool
  var pubProperties: Amqp.Properties
  var pubSize: Long
  var pubGot: Long
  var pubChunks: List[slice[Byte]]
}

func send(s: in Session, channel: in Int, m: in Amqp.Method): Unit {
  match Amqp.methodFrame(channel, m) {
    case Ok(bytes) -> {
      val _ = K.sockWrite(s.sock, bytes)
    }
    case Err(_) -> ()
  }
}

func sendContent(s: in Session, m: in Amqp.Method, properties: in Amqp.Properties, body: in slice[Byte]): Unit {
  match Amqp.contentFrames(1, m, properties, body, s.frameMax) {
    case Ok(bytes) -> {
      val _ = K.sockWrite(s.sock, bytes)
    }
    case Err(_) -> ()
  }
}

func isQuorum(q: in FakeQueue): Bool {
  match Amqp.tableGet(q.arguments, "x-queue-type") {
    case Some(v) -> RabbitMq.fieldText(v) == "quorum"
    case None -> false
  }
}

func pushFront(q: in FakeQueue, stored: in Stored): Unit {
  val rebuilt: List[Stored] = newList()
  rebuilt.add(stored)
  for waiting in q.ready.toArray() {
    rebuilt.add(waiting)
  }
  q.ready = rebuilt
}

// Put the session's unsettled deliveries back at the head of their queues.
func returnUnacked(s: in Session): Unit {
  val b = s.broker
  K.lockEnter(b.lock)
  var i = s.unackedOrder.count - 1
  while i >= 0 {
    val tag = s.unackedOrder[i]
    if s.unacked.containsKey(tag) {
      val u = s.unacked[tag]
      if b.queues.containsKey(u.queue) {
        val q = b.queues[u.queue]
        pushFront(
          q,
          Stored(properties = u.stored.properties, body = u.stored.body, redelivered = true, requeues = u.stored.requeues)
        )
      }
    }
    i -= 1
  }
  K.lockExit(b.lock)
  s.unacked = newMap()
  s.unackedOrder = newList()
}

func channelError(s: in Session, code: in Int, text: in String, classId: in Int, methodId: in Int): Unit {
  returnUnacked(s)
  s.consumerTag = ""
  s.consumerQueue = ""
  s.pubActive = false
  s.channelClosing = true
  send(s, 1, Amqp.ChannelClose(replyCode = code, replyText = text, classId = classId, methodId = methodId))
}

// Deliver what is ready on the consumed queue, as far as the prefetch allows.
func pump(s: in Session): Unit {
  if s.consumerTag.length == 0 or s.channelClosing {
    return
  }
  val b = s.broker
  K.lockEnter(b.lock)
  if b.queues.containsKey(s.consumerQueue) {
    val q = b.queues[s.consumerQueue]
    while q.ready.count > 0 and (s.prefetch == 0 or s.unackedOrder.count < s.prefetch) {
      val stored = q.ready[0]
      q.ready.removeAt(0)
      s.nextTag = s.nextTag + 1i64
      s.unacked[s.nextTag] = Unacked(queue = q.name, stored = stored)
      s.unackedOrder.add(s.nextTag)
      var properties = stored.properties
      if isQuorum(q) and stored.requeues > 0 {
        val counted: slice[Amqp.TableEntry] = [Amqp.entry("x-delivery-count", Amqp.fLong(stored.requeues.toLong()))]
        properties = properties.copy(headers = properties.headers.concat(counted))
      }
      val deliver = Amqp.BasicDeliver(
        consumerTag = s.consumerTag,
        deliveryTag = s.nextTag,
        redelivered = stored.redelivered,
        exchange = "",
        routingKey = q.name
      )
      sendContent(s, deliver, properties, stored.body)
    }
  }
  K.lockExit(b.lock)
}

func forget(s: in Session, tag: in Long): Unit {
  s.unacked.remove(tag)
  val still: List[Long] = newList()
  for t in s.unackedOrder.toArray() {
    if t != tag {
      still.add(t)
    }
  }
  s.unackedOrder = still
}

// Where a rejected message of `q` goes: the queue named by its dead-letter
// routing key, when its dead-letter exchange is the default exchange.
func deadLetter(b: in FakeBroker, q: in FakeQueue, stored: in Stored): Unit {
  val exchange = match Amqp.tableGet(q.arguments, "x-dead-letter-exchange") {
    case Some(v) -> RabbitMq.fieldText(v)
    case None -> return
  }
  val key = match Amqp.tableGet(q.arguments, "x-dead-letter-routing-key") {
    case Some(v) -> RabbitMq.fieldText(v)
    case None -> return
  }
  if exchange.length > 0 or not b.queues.containsKey(key) {
    return
  }
  val death: slice[Amqp.TableEntry] = [
    Amqp.entry("count", Amqp.fLong(1i64)),
    Amqp.entry("reason", Amqp.fText("rejected")),
    Amqp.entry("queue", Amqp.fText(q.name))
  ]
  val deaths: slice[Amqp.FieldValue] = [Amqp.FTable(entries = death)]
  val added: slice[Amqp.TableEntry] = [Amqp.entry("x-death", Amqp.FArray(items = deaths))]
  val properties = stored.properties.copy(headers = stored.properties.headers.concat(added))
  b.queues[key].ready.add(Stored(properties = properties, body = stored.body, redelivered = false, requeues = 0))
}

func settleFake(s: in Session, tag: in Long, acked: in Bool, requeue: in Bool): Unit {
  if not s.unacked.containsKey(tag) {
    channelError(s, 406, "PRECONDITION_FAILED - unknown delivery tag " + tag.toString(), 60, 80)
    return
  }
  val u = s.unacked[tag]
  forget(s, tag)
  if acked {
    return
  }
  val b = s.broker
  K.lockEnter(b.lock)
  if b.queues.containsKey(u.queue) {
    val q = b.queues[u.queue]
    if requeue {
      pushFront(
        q,
        Stored(properties = u.stored.properties, body = u.stored.body, redelivered = true, requeues = u.stored.requeues + 1)
      )
    } else {
      deadLetter(b, q, u.stored)
    }
  }
  K.lockExit(b.lock)
}

// The queues a publish to `exchange` with `key` reaches; `Err` when the
// exchange does not exist.
func route(b: in FakeBroker, exchange: in String, key: in String): Result[List[String], String] {
  val targets: List[String] = newList()
  if exchange.length == 0 {
    if b.queues.containsKey(key) {
      targets.add(key)
    }
    return Ok(targets)
  }
  if not b.exchanges.containsKey(exchange) {
    return Err("NOT_FOUND - no exchange '" + exchange + "' in vhost '/'")
  }
  val fanout = b.exchanges[exchange] == "fanout"
  for binding in b.bindings.toArray() {
    if binding.exchange == exchange and (fanout or binding.key == key or binding.key == "#") {
      if b.queues.containsKey(binding.queue) {
        targets.add(binding.queue)
      }
    }
  }
  Ok(targets)
}

// A complete publish has arrived. False ends the connection.
func finishPublish(s: in Session): Bool {
  val b = s.broker
  val body: slice[Byte] = Amqp.joinChunks(s.pubChunks)
  val properties = s.pubProperties
  val exchange = s.pubExchange
  val key = s.pubKey
  s.pubActive = false
  s.pubHasHeader = false
  s.pubChunks = newList()
  s.pubGot = 0i64
  if s.confirming {
    s.publishSeq = s.publishSeq + 1i64
  }
  if key == "fail.drop" {
    return false
  }
  if key == "fail.silent" {
    return true
  }
  if key == "fail.nack" {
    send(s, 1, Amqp.BasicNack(deliveryTag = s.publishSeq, multiple = false, requeue = false))
    return true
  }
  if key == "fail.blocked" {
    send(s, 0, Amqp.ConnectionBlocked(reason = "low on memory"))
    return true
  }
  K.lockEnter(b.lock)
  val routed = route(b, exchange, key)
  match routed {
    case Ok(targets) -> {
      for name in targets.toArray() {
        b.queues[name].ready.add(Stored(properties = properties, body = body, redelivered = false, requeues = 0))
      }
    }
    case Err(_) -> ()
  }
  K.lockExit(b.lock)
  match routed {
    case Err(text) -> {
      channelError(s, 404, text, 60, 40)
      return true
    }
    case Ok(targets) -> {
      if targets.count == 0 and s.pubMandatory {
        sendContent(
          s,
          Amqp.BasicReturn(replyCode = 312, replyText = "NO_ROUTE", exchange = exchange, routingKey = key),
          properties,
          body
        )
      }
    }
  }
  if s.confirming {
    if Str.endsWith(properties.messageId, "+hold") {
      s.held = true
    } else {
      send(s, 1, Amqp.BasicAck(deliveryTag = s.publishSeq, multiple = s.held))
      s.held = false
    }
  }
  true
}

func credentials(response: in slice[Byte]): slice[String] {
  // PLAIN: NUL user NUL password.
  val parts: List[String] = newList()
  var start = 1
  var i = 1
  while i <= response.length {
    if i == response.length or response[i] == 0u8 {
      match tryDecodeUtf8(response.slice(start, i)) {
        case Some(text) -> parts.add(text)
        case None -> parts.add("")
      }
      start = i + 1
    }
    i += 1
  }
  parts.toArray()
}

func declareFakeQueue(
  s: in Session,
  name: in String,
  passive: in Bool,
  durable: in Bool,
  arguments: in slice[Amqp.TableEntry],
): Unit {
  val b = s.broker
  K.lockEnter(b.lock)
  var failure = ""
  var code = 0
  var count = 0
  if b.queues.containsKey(name) {
    val existing = b.queues[name]
    count = existing.ready.count
    val same = match Amqp.encodeTable(existing.arguments) {
      case Ok(mine) -> match Amqp.encodeTable(arguments) {
        case Ok(theirs) -> encodeHex(mine) == encodeHex(theirs)
        case Err(_) -> false
      }
      case Err(_) -> false
    }
    if not passive and (existing.durable != durable or not same) {
      code = 406
      failure = "PRECONDITION_FAILED - inequivalent arg for queue '" + name + "' in vhost '/'"
    }
  } else if passive {
    code = 404
    failure = "NOT_FOUND - no queue '" + name + "' in vhost '/'"
  } else {
    b.queues[name] = FakeQueue(name = name, durable = durable, arguments = arguments, ready = newList())
  }
  K.lockExit(b.lock)
  if code != 0 {
    channelError(s, code, failure, 50, 10)
  } else {
    send(s, 1, Amqp.QueueDeclareOk(queue = name, messageCount = count.toLong(), consumerCount = 0i64))
  }
}

// Handle one method. False ends the connection.
func handleMethod(s: in Session, channel: in Int, m: in Amqp.Method): Bool {
  val b = s.broker
  if s.channelClosing and channel == 1 {
    match m {
      case ChannelCloseOk -> {
        s.channelClosing = false
      }
      case _ -> ()
    }
    return true
  }
  match m {
    case ConnectionStartOk(properties, _, response, _) -> {
      b.clientProduct = match Amqp.tableGet(properties, "product") {
        case Some(v) -> RabbitMq.fieldText(v)
        case None -> ""
      }
      b.connectionName = match Amqp.tableGet(properties, "connection_name") {
        case Some(v) -> RabbitMq.fieldText(v)
        case None -> ""
      }
      val parts = credentials(response)
      val user = if parts.length > 0 { parts[0] } else { "" }
      val password = if parts.length > 1 { parts[1] } else { "" }
      if user == "legacy" {
        return false
      }
      if password == "wrong" {
        s.closing = true
        send(
          s,
          0,
          Amqp.ConnectionClose(
            replyCode = 403,
            replyText = "ACCESS_REFUSED - Login was refused using authentication mechanism PLAIN. For details see the broker logfile.",
            classId = 0,
            methodId = 0
          )
        )
        return true
      }
      send(s, 0, Amqp.ConnectionTune(channelMax = 2047, frameMax = b.frameMax.toLong(), heartbeat = b.heartbeat))
      true
    }
    case ConnectionTuneOk(_, frameMax, heartbeat) -> {
      b.tunedFrameMax = frameMax
      b.tunedHeartbeat = heartbeat
      s.frameMax = frameMax.toInt()
      s.heartbeatMs = heartbeat.toLong() * 1000i64
      true
    }
    case ConnectionOpen(vhost) -> {
      if vhost == "missing" {
        s.closing = true
        send(
          s,
          0,
          Amqp.ConnectionClose(
            replyCode = 530,
            replyText = "NOT_ALLOWED - vhost missing not found",
            classId = 10,
            methodId = 40
          )
        )
      } else {
        send(s, 0, Amqp.ConnectionOpenOk)
      }
      true
    }
    case ConnectionClose(_, _, _, _) -> {
      b.cleanCloses = b.cleanCloses + 1
      send(s, 0, Amqp.ConnectionCloseOk)
      false
    }
    case ConnectionCloseOk -> false
    case ChannelOpen -> {
      s.confirming = false
      s.publishSeq = 0i64
      s.held = false
      s.nextTag = 0i64
      send(s, 1, Amqp.ChannelOpenOk)
      true
    }
    case ChannelClose(_, _, _, _) -> {
      returnUnacked(s)
      s.consumerTag = ""
      send(s, 1, Amqp.ChannelCloseOk)
      true
    }
    case ConfirmSelect -> {
      s.confirming = true
      send(s, 1, Amqp.ConfirmSelectOk)
      true
    }
    case QueueDeclare(name, passive, durable, _, _, arguments) -> {
      declareFakeQueue(s, name, passive, durable, arguments)
      true
    }
    case QueuePurge(name) -> {
      K.lockEnter(b.lock)
      var count = -1
      if b.queues.containsKey(name) {
        count = b.queues[name].ready.count
        b.queues[name].ready = newList()
      }
      K.lockExit(b.lock)
      if count < 0 {
        channelError(s, 404, "NOT_FOUND - no queue '" + name + "' in vhost '/'", 50, 30)
      } else {
        send(s, 1, Amqp.QueuePurgeOk(messageCount = count.toLong()))
      }
      true
    }
    case QueueDelete(name, _, _) -> {
      K.lockEnter(b.lock)
      var count = 0
      if b.queues.containsKey(name) {
        count = b.queues[name].ready.count
        b.queues.remove(name)
      }
      K.lockExit(b.lock)
      send(s, 1, Amqp.QueueDeleteOk(messageCount = count.toLong()))
      true
    }
    case ExchangeDeclare(name, kind, _, _, _, _, _) -> {
      K.lockEnter(b.lock)
      val clash = b.exchanges.containsKey(name) and b.exchanges[name] != kind
      if not clash {
        b.exchanges[name] = kind
      }
      K.lockExit(b.lock)
      if clash {
        channelError(s, 406, "PRECONDITION_FAILED - inequivalent arg 'type' for exchange '" + name + "' in vhost '/'", 40, 10)
      } else {
        send(s, 1, Amqp.ExchangeDeclareOk)
      }
      true
    }
    case ExchangeDelete(name, _) -> {
      K.lockEnter(b.lock)
      b.exchanges.remove(name)
      K.lockExit(b.lock)
      send(s, 1, Amqp.ExchangeDeleteOk)
      true
    }
    case QueueBind(queue, exchange, key, _) -> {
      K.lockEnter(b.lock)
      val known = b.exchanges.containsKey(exchange) and b.queues.containsKey(queue)
      if known {
        b.bindings.add(Binding(queue = queue, exchange = exchange, key = key))
      }
      K.lockExit(b.lock)
      if known {
        send(s, 1, Amqp.QueueBindOk)
      } else {
        channelError(s, 404, "NOT_FOUND - no exchange '" + exchange + "' in vhost '/'", 50, 20)
      }
      true
    }
    case BasicQos(_, count, _) -> {
      s.prefetch = count
      send(s, 1, Amqp.BasicQosOk)
      true
    }
    case BasicConsume(queue, _, _, _, _, _) -> {
      K.lockEnter(b.lock)
      val exists = b.queues.containsKey(queue)
      K.lockExit(b.lock)
      if not exists {
        channelError(s, 404, "NOT_FOUND - no queue '" + queue + "' in vhost '/'", 60, 20)
        return true
      }
      val tag = "fake-ctag-" + queue
      send(s, 1, Amqp.BasicConsumeOk(consumerTag = tag))
      if queue == "fail.dropconsume" {
        return false
      }
      if queue == "fail.forced" {
        s.closing = true
        send(
          s,
          0,
          Amqp.ConnectionClose(
            replyCode = 320,
            replyText = "CONNECTION_FORCED - broker forced connection closure with reason 'shutdown'",
            classId = 0,
            methodId = 0
          )
        )
        return true
      }
      if queue == "fail.cancel" {
        K.lockEnter(b.lock)
        b.queues.remove(queue)
        K.lockExit(b.lock)
        send(s, 1, Amqp.BasicCancel(consumerTag = tag, noWait = true))
        return true
      }
      s.consumerQueue = queue
      s.consumerTag = tag
      true
    }
    case BasicPublish(exchange, key, mandatory, _) -> {
      s.pubActive = true
      s.pubExchange = exchange
      s.pubKey = key
      s.pubMandatory = mandatory
      true
    }
    case BasicAck(tag, _) -> {
      settleFake(s, tag, true, false)
      true
    }
    case BasicNack(tag, _, requeue) -> {
      settleFake(s, tag, false, requeue)
      true
    }
    case BasicReject(tag, requeue) -> {
      settleFake(s, tag, false, requeue)
      true
    }
    case _ -> {
      s.closing = true
      send(
        s,
        0,
        Amqp.ConnectionClose(
          replyCode = 540,
          replyText = "NOT_IMPLEMENTED - " + Amqp.methodName(m),
          classId = 0,
          methodId = 0
        )
      )
      true
    }
  }
}

// Handle one frame. False ends the connection.
func handleFrame(s: in Session, frame: in Amqp.Frame): Bool {
  val b = s.broker
  if frame.payload.length + Amqp.FRAME_OVERHEAD > b.largestFrame {
    b.largestFrame = frame.payload.length + Amqp.FRAME_OVERHEAD
  }
  if frame.kind == Amqp.FRAME_HEARTBEAT {
    b.heartbeatsSeen = b.heartbeatsSeen + 1
    return true
  }
  if frame.kind == Amqp.FRAME_METHOD {
    return match Amqp.decodeMethod(frame.payload) {
      case Ok(m) -> handleMethod(s, frame.channel, m)
      case Err(_) -> false
    }
  }
  if s.channelClosing or not s.pubActive {
    return true
  }
  if frame.kind == Amqp.FRAME_HEADER {
    match Amqp.decodeContentHeader(frame.payload) {
      case Err(_) -> return false
      case Ok(h) -> {
        s.pubHasHeader = true
        s.pubProperties = h.properties
        s.pubSize = h.bodySize
        if h.bodySize == 0i64 {
          return finishPublish(s)
        }
      }
    }
    return true
  }
  s.pubChunks.add(frame.payload)
  s.pubGot = s.pubGot + frame.payload.length.toLong()
  if s.pubGot >= s.pubSize {
    return finishPublish(s)
  }
  true
}

// Read exactly the 8-byte protocol header; what follows it goes to `decoder`.
func readProtocolHeader(sock: in K.Sock, decoder: in Amqp.Decoder): Bool {
  var got: slice[Byte] = bytesOf([])
  while got.length < 8 {
    match K.sockRead(sock, 4096, 5000) {
      case Ok(Data(bytes)) -> {
        got = got.concat(bytes)
      }
      case _ -> return false
    }
  }
  if got.length > 8 {
    Amqp.feed(decoder, got.slice(8, got.length))
  }
  Amqp.isProtocolHeader(got.slice(0, 8))
}

func serve(b: in FakeBroker, sock: in K.Sock): Unit {
  val s = Session(
    broker = b,
    sock = sock,
    frameMax = 4096,
    heartbeatMs = 0i64,
    lastBeat = K.monotonicMillis(),
    closing = false,
    channelClosing = false,
    confirming = false,
    publishSeq = 0i64,
    held = false,
    consumerQueue = "",
    consumerTag = "",
    prefetch = 0,
    nextTag = 0i64,
    unacked = newMap(),
    unackedOrder = newList(),
    pubActive = false,
    pubExchange = "",
    pubKey = "",
    pubMandatory = false,
    pubHasHeader = false,
    pubProperties = Amqp.noProperties(),
    pubSize = 0i64,
    pubGot = 0i64,
    pubChunks = newList()
  )
  val decoder = Amqp.newDecoder(0)
  if not readProtocolHeader(sock, decoder) or b.mode == "mismatch" {
    // What RabbitMQ does with a header it does not accept: it answers with
    // the one it does and closes.
    val _ = K.sockWrite(sock, Amqp.protocolHeader())
    K.sockClose(sock)
    return
  }
  val serverProperties: slice[Amqp.TableEntry] = [
    Amqp.entry("product", Amqp.fText("FakeRabbit")),
    Amqp.entry("version", Amqp.fText("1.0"))
  ]
  send(
    s,
    0,
    Amqp.ConnectionStart(
      versionMajor = if b.mode == "old-version" { 8 } else { 0 },
      versionMinor = if b.mode == "old-version" { 0 } else { 9 },
      serverProperties = serverProperties,
      mechanisms = if b.mode == "no-plain" { "EXTERNAL" } else { "PLAIN AMQPLAIN" },
      locales = "en_US"
    )
  )
  var open = true
  while open {
    match Amqp.nextFrame(decoder) {
      case Err(_) -> {
        open = false
      }
      case Ok(Some(frame)) -> {
        open = handleFrame(s, frame)
        if open and not s.closing {
          pump(s)
        }
      }
      case Ok(None) -> match K.sockRead(sock, 65536, 50) {
        case Ok(Data(bytes)) -> Amqp.feed(decoder, bytes)
        case Ok(TimedOut) -> {
          val now = K.monotonicMillis()
          if s.heartbeatMs > 0i64 and b.mode != "mute" and now - s.lastBeat >= s.heartbeatMs / 2i64 {
            val _ = K.sockWrite(sock, Amqp.heartbeatFrame())
            s.lastBeat = now
          }
          if not s.closing {
            pump(s)
          }
        }
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
  val b = FakeBroker(
    server = server,
    secured = secured,
    tls = tls,
    lock = K.newLock(),
    queues = newMap(),
    exchanges = newMap(),
    bindings = newList(),
    mode = "",
    frameMax = 131072,
    heartbeat = 60,
    heartbeatsSeen = 0,
    tunedHeartbeat = -1,
    tunedFrameMax = -1i64,
    clientProduct = "",
    connectionName = "",
    cleanCloses = 0,
    largestFrame = 0
  )
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

func broker(): FakeBroker {
  match slot.broker {
    case Some(b) -> b
    case None -> {
      val b = startBrokerWith(false, serverTls(""))
      slot.broker = Some(value = b)
      b
    }
  }
}

func brokerPort(): Int {
  K.serverPort(broker().server)
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
  "test." + slot.names.toString()
}

func optionsOn(port: in Int, queue: in String): RabbitMq.ConnectOptions {
  RabbitMq.defaultOptions("127.0.0.1", port, queue).copy(user = "lyric", password = "secret", requestTimeoutMs = 3000)
}

func optionsFor(queue: in String): RabbitMq.ConnectOptions {
  optionsOn(brokerPort(), queue)
}

func open(options: in RabbitMq.ConnectOptions): RabbitMq.RabbitMqQueue {
  match RabbitMq.connectWith(options) {
    case Ok(q) -> q
    case Err(e) -> panic("connect failed: " + e)
  }
}

func openFresh(): RabbitMq.RabbitMqQueue {
  open(optionsFor(freshName()))
}

func message(id: in String, body: in String): Mq.Message {
  Mq.Message(id = id, body = body, headers = [], deliveryCount = 0)
}

func mustPublish(q: in RabbitMq.RabbitMqQueue, m: in Mq.Message): Unit {
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

func mustTake(q: in RabbitMq.RabbitMqQueue): Mq.Message {
  val got = Got(messages = newList())
  match q.consume(3000, { m -> keep(got, m) }) {
    case Ok(_) -> got.messages[0]
    case Err(e) -> panic("consume failed: " + e)
  }
}

func expectTimeout(q: in RabbitMq.RabbitMqQueue, label: in String): Unit {
  val got = Got(messages = newList())
  match q.consume(150, { m -> keep(got, m) }) {
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

func errorOfConnect(options: in RabbitMq.ConnectOptions): String {
  match RabbitMq.connectWith(options) {
    case Ok(q) -> {
      q.close()
      "connected"
    }
    case Err(e) -> e
  }
}

func expectContains(text: in String, part: in String, label: in String): Unit {
  assertTrue(Str.contains(text, part), label + ": got \"" + text + "\"")
}

func headerOf(m: in Mq.Message, key: in String): String {
  for h in m.headers {
    if h.key == key {
      return h.value
    }
  }
  "<absent>"
}

// Put a message straight into a queue of the broker, as another producer
// would.
func inject(queue: in String, properties: in Amqp.Properties, body: in slice[Byte]): Unit {
  val b = broker()
  K.lockEnter(b.lock)
  b.queues[queue].ready.add(Stored(properties = properties, body = body, redelivered = false, requeues = 0))
  K.lockExit(b.lock)
}

func readyCount(queue: in String): Int {
  val b = broker()
  K.lockEnter(b.lock)
  val count = if b.queues.containsKey(queue) { b.queues[queue].ready.count } else { -1 }
  K.lockExit(b.lock)
  count
}

func bytesOf(values: in slice[Int]): slice[Byte] {
  val acc: List[Byte] = newList()
  for v in values {
    acc.add(v.toByte())
  }
  acc.toArray()
}

func bigBody(size: in Int): String {
  val sb = Str.StringBuilder.new()
  var i = 0
  while i < size / 10 {
    sb.append("0123456789")
    i += 1
  }
  sb.toString()
}

// The conformance suite asks for a fresh queue per case and never closes
// one; the connections are closed when the suite is done, so the broker's
// session threads end with it.
ref record Opened {
  var queues: List[RabbitMq.RabbitMqQueue]
}

val opened: Opened = Opened(queues = newList())

func conformance(factory: () -> MqConformance.QueueEnds): Checks {
  val checks = MqConformance.runQueue(factory)
  for q in opened.queues.toArray() {
    q.close()
  }
  opened.queues = newList()
  checks
}

func makeEnds(): MqConformance.QueueEnds {
  val q = openFresh()
  opened.queues.add(q)
  val publisher: Mq.MessageQueue = q
  val consumer: Mq.QueueConsumer = q
  MqConformance.QueueEnds(publisher = publisher, consumer = consumer)
}

func makeQuorumEnds(): MqConformance.QueueEnds {
  val q = open(optionsFor(freshName()).withQuorumQueue())
  opened.queues.add(q)
  val publisher: Mq.MessageQueue = q
  val consumer: Mq.QueueConsumer = q
  MqConformance.QueueEnds(publisher = publisher, consumer = consumer)
}

// ─── Conformance ─────────────────────────────────────────────────────────────
test "the backend conforms to MessageQueue and QueueConsumer over the fake broker" {
  requireConformance(conformance({ -> makeEnds() }))
}

test "the backend conforms on a queue that counts deliveries like a quorum queue" {
  requireConformance(conformance({ -> makeQuorumEnds() }))
}

// ─── Connecting ──────────────────────────────────────────────────────────────
test "connecting reports the broker and identifies the client" {
  val q = open(optionsFor("orders").copy(connectionName = "billing-worker"))
  assertEqual(RabbitMq.serverInfo(q), "FakeRabbit 1.0", "product and version from connection.start")
  assertEqual(RabbitMq.queueName(q), "orders", "queue")
  assertTrue(RabbitMq.isOpen(q), "open")
  assertEqual(broker().clientProduct, "lyric-mq-rabbitmq", "the client names itself")
  assertEqual(broker().connectionName, "billing-worker", "and the connection")
  assertEqualInt(RabbitMq.lastReplyCode(q), 0, "no error yet")
  assertEqual(RabbitMq.blockedReason(q), "", "not blocked")
  q.close()
  assertTrue(not RabbitMq.isOpen(q), "closed")
}

func knobBroker(mode: in String, frameMax: in Int, heartbeat: in Int): FakeBroker {
  val b = startBrokerWith(false, serverTls(""))
  b.mode = mode
  b.frameMax = frameMax
  b.heartbeat = heartbeat
  b
}

test "the frame size is the smaller of the two limits, and 0 from the broker means the client's" {
  val small = knobBroker("", 8192, 60)
  val q = open(optionsOn(K.serverPort(small.server), "q"))
  assertEqualInt(RabbitMq.negotiatedFrameMax(q), 8192, "the broker's limit is smaller")
  assertEqualLong(small.tunedFrameMax, 8192i64, "tune-ok carries it")
  val body = bigBody(50000)
  mustPublish(q, message("big", body))
  assertTrue(small.largestFrame <= 8192, "no frame is larger than the limit: " + small.largestFrame.toString())
  val got = mustTake(q)
  assertTrue(got.body == body, "the body is reassembled from the broker's frames")
  q.close()
  K.serverClose(small.server)
  val unlimited = knobBroker("", 0, 60)
  val r = open(optionsOn(K.serverPort(unlimited.server), "q").copy(frameMax = 16384))
  assertEqualInt(RabbitMq.negotiatedFrameMax(r), 16384, "the client's limit when the broker has none")
  r.close()
  K.serverClose(unlimited.server)
  val wide = knobBroker("", 1048576, 60)
  val w = open(optionsOn(K.serverPort(wide.server), "q"))
  assertEqualInt(RabbitMq.negotiatedFrameMax(w), 131072, "the client's limit is smaller")
  w.close()
  K.serverClose(wide.server)
}

test "the heartbeat is off unless asked for, then the smaller of the two intervals" {
  val b = knobBroker("", 131072, 60)
  val port = K.serverPort(b.server)
  val off = open(optionsOn(port, "q"))
  assertEqualInt(RabbitMq.heartbeatSeconds(off), 0, "off by default")
  assertEqualInt(b.tunedHeartbeat, 0, "tune-ok says 0")
  off.close()
  val shorter = open(optionsOn(port, "q").copy(heartbeatSeconds = 30))
  assertEqualInt(RabbitMq.heartbeatSeconds(shorter), 30, "the client's interval is shorter")
  shorter.close()
  val longer = open(optionsOn(port, "q").copy(heartbeatSeconds = 120))
  assertEqualInt(RabbitMq.heartbeatSeconds(longer), 60, "the broker's interval is shorter")
  assertEqualInt(b.tunedHeartbeat, 60, "tune-ok says 60")
  longer.close()
  K.serverClose(b.server)
  val none = knobBroker("", 131072, 0)
  val q = open(optionsOn(K.serverPort(none.server), "q").copy(heartbeatSeconds = 20))
  assertEqualInt(RabbitMq.heartbeatSeconds(q), 20, "the client's interval when the broker proposes none")
  q.close()
  K.serverClose(none.server)
}

test "a refused connection is an Err naming the endpoint" {
  val server = match K.serverListen("127.0.0.1", 0) { case Ok(s) -> s; case Err(e) -> panic(e) }
  val port = K.serverPort(server)
  K.serverClose(server)
  val e = errorOfConnect(RabbitMq.defaultOptions("127.0.0.1", port, "a"))
  expectContains(e, "rabbitmq: cannot connect to 127.0.0.1:" + port.toString(), "refused")
}

test "bad credentials surface the broker's 403, or the dropped socket of an old broker" {
  val e = errorOfConnect(optionsFor("a").copy(password = "wrong"))
  expectContains(e, "rabbitmq: cannot open a connection to 127.0.0.1:", "refused, not lost")
  expectContains(e, "403 ACCESS_REFUSED - Login was refused", "the broker's reason")
  assertTrue(not Str.contains(e, "wrong"), "the password is not echoed")
  val dropped = errorOfConnect(optionsFor("a").copy(user = "legacy"))
  expectContains(
    dropped,
    "closed the connection during authentication; check the user and password",
    "a broker that just hangs up"
  )
}

test "an unknown virtual host surfaces the broker's 530" {
  val e = errorOfConnect(optionsFor("a").copy(virtualHost = "missing"))
  expectContains(e, "530 NOT_ALLOWED - vhost missing not found", "the broker's reason")
}

test "a peer that does not speak AMQP 0-9-1 with PLAIN is refused" {
  val mismatch = knobBroker("mismatch", 131072, 60)
  expectContains(
    errorOfConnect(optionsOn(K.serverPort(mismatch.server), "a")),
    "does not accept AMQP 0-9-1",
    "a protocol header in reply"
  )
  K.serverClose(mismatch.server)
  val ancient = knobBroker("old-version", 131072, 60)
  expectContains(
    errorOfConnect(optionsOn(K.serverPort(ancient.server), "a")),
    "speaks AMQP 8-0; this client needs 0-9-1",
    "AMQP 0-8"
  )
  K.serverClose(ancient.server)
  val external = knobBroker("no-plain", 131072, 60)
  expectContains(
    errorOfConnect(optionsOn(K.serverPort(external.server), "a")),
    "does not offer PLAIN authentication (it offers: EXTERNAL)",
    "no PLAIN"
  )
  K.serverClose(external.server)
}

test "invalid options are rejected before any connection is made" {
  val good = RabbitMq.defaultOptions("127.0.0.1", 1, "a")
  expectContains(errorOfConnect(good.copy(host = "")), "no broker host", "host")
  expectContains(errorOfConnect(good.copy(port = 0)), "out of range", "port")
  expectContains(errorOfConnect(good.copy(user = "")), "no user", "user")
  expectContains(errorOfConnect(good.copy(password = "a\u{0000}b")), "NUL", "a NUL in the password")
  expectContains(errorOfConnect(good.copy(queue = "")), "no queue name", "queue")
  expectContains(errorOfConnect(good.copy(queue = Str.repeat("q", 256))), "longer than 255 bytes", "a long queue name")
  expectContains(errorOfConnect(good.copy(virtualHost = Str.repeat("v", 256))), "longer than 255 bytes", "a long vhost")
  expectContains(errorOfConnect(good.copy(prefetchCount = -1)), "prefetchCount", "prefetch")
  expectContains(errorOfConnect(good.copy(prefetchCount = 65536)), "prefetchCount", "prefetch over 16 bits")
  expectContains(errorOfConnect(good.copy(heartbeatSeconds = -1)), "heartbeatSeconds", "heartbeat")
  expectContains(errorOfConnect(good.copy(frameMax = 4095)), "frameMax must be at least 4096", "frame limit")
  expectContains(errorOfConnect(good.copy(connectTimeoutMs = -1)), "connectTimeoutMs", "connect timeout")
  expectContains(errorOfConnect(good.copy(requestTimeoutMs = 0)), "requestTimeoutMs", "request timeout")
  expectContains(errorOfConnect(good.copy(maxMessageBytes = 0)), "maxMessageBytes", "message limit")
  val badArgument: slice[Amqp.TableEntry] = [Amqp.entry("", Amqp.FVoid)]
  expectContains(
    errorOfConnect(good.copy(queueArguments = badArgument)),
    "queueArguments",
    "a malformed argument table"
  )
  assertTrue(isOk(RabbitMq.validateOptions(good)), "validateOptions accepts the defaults")
  assertTrue(isErr(RabbitMq.validateOptions(good.copy(host = ""))), "and refuses what connectWith refuses")
}

test "connectUrl takes the host, credentials, virtual host and parameters from the URL" {
  val url = "amqp://lyric:secret@127.0.0.1:" + brokerPort().toString()
  val name = freshName()
  match RabbitMq.connectUrl(url + "/%2f?heartbeat=7", name) {
    case Err(e) -> assertTrue(false, e)
    case Ok(q) -> {
      assertEqual(RabbitMq.queueName(q), name, "queue")
      assertEqual(q.options.virtualHost, "/", "the decoded virtual host")
      assertEqualInt(RabbitMq.heartbeatSeconds(q), 7, "the heartbeat parameter")
      mustPublish(q, message("u-1", "via url"))
      assertEqual(mustTake(q).body, "via url", "round trip")
      q.close()
    }
  }
  match RabbitMq.optionsFromUrl("amqp://broker.example", "orders") {
    case Err(e) -> assertTrue(false, e)
    case Ok(o) -> {
      assertEqualInt(o.port, 5672, "the default port")
      assertEqual(o.user, "guest", "the default user")
      assertEqual(o.virtualHost, "/", "the default virtual host")
      assertTrue(not o.tls.enabled, "amqp:// is plain TCP")
      assertEqualInt(o.heartbeatSeconds, 0, "no heartbeat unless asked for")
    }
  }
  match RabbitMq.optionsFromUrl("amqp://h/prod?connection_timeout=1500&frame_max=8192", "orders") {
    case Err(e) -> assertTrue(false, e)
    case Ok(o) -> {
      assertEqual(o.virtualHost, "prod", "virtual host")
      assertEqualInt(o.connectTimeoutMs, 1500, "connection_timeout")
      assertEqualInt(o.frameMax, 8192, "frame_max")
    }
  }
  expectContains(errorOfConnectUrl("amqp://127.0.0.1", ""), "no queue name", "an empty queue name")
  expectContains(errorOfConnectUrl("http://127.0.0.1", "a"), "unsupported scheme", "another scheme")
  expectContains(errorOfConnectUrl(url + "/missing", "a"), "530 NOT_ALLOWED", "the URL's virtual host is used")
}

func errorOfConnectUrl(url: in String, queue: in String): String {
  match RabbitMq.connectUrl(url, queue) {
    case Ok(q) -> {
      q.close()
      "connected"
    }
    case Err(e) -> e
  }
}

test "connectFromEnv reads the URL, queue name, ack mode and prefetch" {
  val prefix = "LYRIC_CONFIG_MQ_RABBITMQ_"
  for field in ["URL", "QUEUENAME", "ACKMODE", "PREFETCHCOUNT"] {
    Env.setVar(prefix + field, "")
  }
  match RabbitMq.connectFromEnv() {
    case Ok(_) -> assertTrue(false, "no URL is configured")
    case Err(e) -> expectContains(e, "LYRIC_CONFIG_MQ_RABBITMQ_URL", "missing URL")
  }
  Env.setVar(prefix + "URL", "amqp://lyric:secret@127.0.0.1:" + brokerPort().toString())
  match RabbitMq.connectFromEnv() {
    case Ok(_) -> assertTrue(false, "no queue name is configured")
    case Err(e) -> expectContains(e, "LYRIC_CONFIG_MQ_RABBITMQ_QUEUENAME", "missing queue name")
  }
  Env.setVar(prefix + "QUEUENAME", freshName())
  Env.setVar(prefix + "ACKMODE", "sometimes")
  match RabbitMq.connectFromEnv() {
    case Ok(_) -> assertTrue(false, "an unknown ack mode")
    case Err(e) -> expectContains(e, "ACKMODE", "bad ack mode")
  }
  Env.setVar(prefix + "ACKMODE", "Auto")
  Env.setVar(prefix + "PREFETCHCOUNT", "many")
  match RabbitMq.connectFromEnv() {
    case Ok(_) -> assertTrue(false, "a prefetch that is not a number")
    case Err(e) -> expectContains(e, "PREFETCHCOUNT", "bad prefetch")
  }
  Env.setVar(prefix + "PREFETCHCOUNT", "3")
  match RabbitMq.optionsFromEnv() {
    case Err(e) -> assertTrue(false, e)
    case Ok(o) -> assertEqualInt(o.prefetchCount, 3, "prefetch from the environment")
  }
  match RabbitMq.connectFromEnv() {
    case Err(e) -> assertTrue(false, e)
    case Ok(q) -> {
      mustPublish(q, message("env-1", "from env"))
      assertEqual(mustTake(q).body, "from env", "round trip")
      assertEqualInt(RabbitMq.unsettledCount(q), 0, "Auto mode acknowledged it")
      q.close()
    }
  }
  for field in ["URL", "QUEUENAME", "ACKMODE", "PREFETCHCOUNT"] {
    Env.setVar(prefix + field, "")
  }
}

test "declare = false checks the queue; a differing declaration is refused" {
  val name = freshName()
  expectContains(
    errorOfConnect(optionsFor(name).copy(declare = false)),
    "404 NOT_FOUND - no queue '" + name + "'",
    "passive"
  )
  val owner = open(optionsFor(name))
  val user = open(optionsFor(name).copy(declare = false, durable = false))
  mustPublish(user, message("p", "x"))
  assertEqual(mustTake(owner).id, "p", "both connections use the one queue")
  user.close()
  expectContains(errorOfConnect(optionsFor(name).copy(durable = false)), "406 PRECONDITION_FAILED", "durable differs")
  expectContains(
    errorOfConnect(optionsFor(name).withMessageTtl(1000i64)),
    "406 PRECONDITION_FAILED",
    "arguments differ"
  )
  owner.close()
}

// ─── Broker failures ─────────────────────────────────────────────────────────
test "a channel error fails the operation, the channel is reopened and the connection stays usable" {
  val q = openFresh()
  mustPublish(q, message("before", "x"))
  assertEqual(mustTake(q).id, "before", "delivered")
  val e = errorOf(
    Extras.publishWith(q, message("x", "x"), Extras.toExchange(q, "no.such.exchange", "k")),
    "no exchange"
  )
  expectContains(
    e,
    "rabbitmq: the broker closed the channel: 404 NOT_FOUND - no exchange 'no.such.exchange'",
    "the reason"
  )
  assertEqualInt(RabbitMq.lastReplyCode(q), 404, "lastReplyCode")
  assertTrue(RabbitMq.isOpen(q), "the connection survives")
  assertEqualInt(RabbitMq.unsettledCount(q), 0, "the old channel's deliveries are forgotten")
  expectContains(
    errorOf(q.ack("before"), "ack on the old channel"),
    "no unsettled delivery",
    "cannot be settled any more"
  )
  val again = mustTake(q)
  assertEqual(again.id, "before", "the broker requeued it")
  assertEqualInt(again.deliveryCount, 1, "as a redelivery")
  assertTrue(isOk(q.ack(again.id)), "ack on the new channel")
  mustPublish(q, message("after", "y"))
  assertEqual(mustTake(q).id, "after", "confirms work on the new channel")
  q.close()
}

test "a broker that drops the connection during a publish" {
  val q = openFresh()
  val e = errorOf(Extras.publishWith(q, message("d", "x"), Extras.toExchange(q, "", "fail.drop")), "dropped")
  expectContains(e, "rabbitmq: connection lost: the broker closed the connection", "the publish fails")
  assertTrue(not RabbitMq.isOpen(q), "the connection is marked lost")
  expectContains(
    errorOf(q.publish(message("e", "x")), "after the loss"),
    "connection lost",
    "every later operation fails"
  )
  expectContains(errorOf(q.consume(0, { m -> Ok(()) }), "consume after the loss"), "connection lost", "consume too")
  expectContains(errorOf(q.ack("d"), "ack after the loss"), "connection lost", "ack too")
  expectContains(errorOf(RabbitMq.keepAlive(q), "keepAlive after the loss"), "connection lost", "keepAlive too")
  q.close()
}

test "a broker that drops the connection in the middle of a consume" {
  val q = open(optionsFor("fail.dropconsume"))
  val e = errorOf(q.consume(2000, { m -> Ok(()) }), "dropped while waiting")
  expectContains(e, "rabbitmq: connection lost: the broker closed the connection", "the consume fails")
  assertTrue(not RabbitMq.isOpen(q), "lost")
  q.close()
}

test "a broker that closes the connection with a reply code" {
  val q = open(optionsFor("fail.forced"))
  val e = errorOf(q.consume(2000, { m -> Ok(()) }), "forced close")
  expectContains(
    e,
    "the broker closed the connection: 320 CONNECTION_FORCED - broker forced connection closure",
    "the reason"
  )
  assertEqualInt(RabbitMq.lastReplyCode(q), 320, "lastReplyCode")
  assertTrue(not RabbitMq.isOpen(q), "lost")
  q.close()
}

test "a broker that never confirms a publish times the publish out" {
  val q = open(optionsFor(freshName()).copy(requestTimeoutMs = 300))
  val e = errorOf(Extras.publishWith(q, message("s", "x"), Extras.toExchange(q, "", "fail.silent")), "no confirm")
  expectContains(e, "the broker did not confirm the publish within 300 ms", "a missed confirm")
  assertTrue(not RabbitMq.isOpen(q), "its fate is unknown, so the connection is given up")
  q.close()
}

test "a broker that has blocked publishers says so in the timeout" {
  val q = open(optionsFor(freshName()).copy(requestTimeoutMs = 300))
  val e = errorOf(Extras.publishWith(q, message("b", "x"), Extras.toExchange(q, "", "fail.blocked")), "blocked")
  expectContains(e, "it has blocked publishers (low on memory)", "connection.blocked is reported")
  q.close()
}

test "a publish the broker refuses with basic.nack is an Err and the connection stays usable" {
  val q = openFresh()
  val e = errorOf(Extras.publishWith(q, message("n", "x"), Extras.toExchange(q, "", "fail.nack")), "nacked")
  expectContains(e, "the broker refused the message (basic.nack)", "a negative confirm")
  assertTrue(RabbitMq.isOpen(q), "still open")
  mustPublish(q, message("ok", "x"))
  assertEqual(mustTake(q).id, "ok", "the next publish is confirmed")
  q.close()
}

test "an unroutable mandatory publish is an Err; without mandatory it is confirmed and dropped" {
  val q = openFresh()
  val e = errorOf(Extras.publishWith(q, message("lost", "x"), Extras.toExchange(q, "", "nobody.home")), "unroutable")
  expectContains(e, "the broker returned message 'lost' as unroutable: 312 NO_ROUTE", "basic.return")
  assertEqualInt(RabbitMq.lastReplyCode(q), 312, "lastReplyCode")
  val relaxed = Extras.toExchange(q, "", "nobody.home").copy(mandatory = false)
  assertTrue(isOk(Extras.publishWith(q, message("dropped", "x"), relaxed)), "not mandatory")
  mustPublish(q, message("kept", "x"))
  assertEqual(mustTake(q).id, "kept", "an earlier return does not taint the next publish")
  q.close()
}

test "one confirm with the multiple flag settles a whole batch" {
  val q = openFresh()
  val batch = [message("a+hold", "1"), message("b+hold", "2"), message("c", "3")]
  match q.publishBatch(batch) {
    case Ok(_) -> ()
    case Err(e) -> assertTrue(false, "publishBatch failed: " + e)
  }
  for expected in ["a+hold", "b+hold", "c"] {
    assertEqual(mustTake(q).id, expected, "batch order")
  }
  q.close()
}

test "a consumer the broker cancels is restarted by the next consume, which reports what the broker says" {
  val q = open(optionsFor("fail.cancel"))
  val e = errorOf(q.consume(1000, { m -> Ok(()) }), "the queue is gone")
  expectContains(e, "404 NOT_FOUND - no queue 'fail.cancel'", "the new basic.consume is refused")
  assertTrue(RabbitMq.isOpen(q), "a channel error, not a lost connection")
  q.close()
}

test "operations on a closed connection are errors, close is idempotent and uses the close handshake" {
  val before = broker().cleanCloses
  val q = openFresh()
  q.close()
  q.close()
  K.sleepMs(100)
  assertEqualInt(broker().cleanCloses, before + 1, "the broker received one connection.close")
  expectContains(errorOf(q.publish(message("m", "x")), "publish"), "the connection is closed", "publish")
  expectContains(errorOf(q.consume(0, { m -> Ok(()) }), "consume"), "the connection is closed", "consume")
  expectContains(errorOf(q.ack("m"), "ack"), "the connection is closed", "ack")
  expectContains(errorOf(q.nack("m", true), "nack"), "the connection is closed", "nack")
  expectContains(errorOf(RabbitMq.subscribe(q), "subscribe"), "the connection is closed", "subscribe")
  expectContains(errorOf(RabbitMq.keepAlive(q), "keepAlive"), "the connection is closed", "keepAlive")
  assertTrue(isErr(Extras.purgeQueue(q, "x")), "request")
}

// ─── Settling ────────────────────────────────────────────────────────────────
test "settling an unknown or already settled id is an Err" {
  val q = openFresh()
  expectContains(
    errorOf(q.ack("never-delivered"), "unknown id"),
    "no unsettled delivery has message id 'never-delivered'",
    "ack"
  )
  expectContains(errorOf(q.nack("never-delivered", true), "unknown id"), "no unsettled delivery", "nack")
  mustPublish(q, message("m", "x"))
  val got = mustTake(q)
  assertEqualInt(RabbitMq.unsettledCount(q), 1, "one unsettled")
  assertTrue(isOk(q.ack(got.id)), "ack")
  assertEqualInt(RabbitMq.unsettledCount(q), 0, "settled")
  expectContains(errorOf(q.ack(got.id), "second ack"), "no unsettled delivery", "a second ack")
  expectContains(errorOf(q.ack(""), "empty id"), "empty message id", "an empty id on ack")
  expectContains(errorOf(q.nack("", false), "empty id"), "empty message id", "an empty id on nack")
  q.close()
}

test "two deliveries with the same id settle oldest first" {
  val name = freshName()
  val q = open(optionsFor(name))
  mustPublish(q, message("same", "first"))
  mustPublish(q, message("same", "second"))
  assertEqual(mustTake(q).body, "first", "first delivery")
  assertEqual(mustTake(q).body, "second", "second delivery")
  assertEqualInt(RabbitMq.unsettledCount(q), 2, "both unsettled under one id")
  assertTrue(isOk(q.nack("same", true)), "requeue the older")
  assertTrue(isOk(q.ack("same")), "ack the newer")
  val back = mustTake(q)
  assertEqual(back.body, "first", "the older one was the one requeued")
  assertEqualInt(back.deliveryCount, 1, "as a redelivery")
  assertTrue(isOk(q.ack("same")), "ack")
  expectTimeout(q, "nothing is left")
  q.close()
}

test "a requeue keeps the id, body and headers; without a delivery count the flag counts as 1" {
  val q = openFresh()
  val body = "payload \u{0000} with NUL, \"quotes\", café 世\nline2"
  val sent = Mq.Message(
    id = "r:1 é",
    body = body,
    headers = [
      Mq.MessageHeader(key = "content-type", value = "text/plain"),
      Mq.MessageHeader(key = "k:colon\nnewline", value = "v\\backslash"),
      Mq.MessageHeader(key = "content-type", value = "a repeated key")
    ],
    deliveryCount = 0
  )
  mustPublish(q, sent)
  var round = 0
  while round < 3 {
    val got = mustTake(q)
    assertEqual(got.id, "r:1 é", "id in round " + round.toString())
    assertEqual(got.body, body, "body in round " + round.toString())
    assertEqualInt(got.deliveryCount, if round == 0 { 0 } else { 1 }, "deliveryCount in round " + round.toString())
    assertEqualInt(got.headers.length, 3, "header count in round " + round.toString())
    assertEqual(got.headers[1].key, "k:colon\nnewline", "an awkward header key")
    assertEqual(got.headers[1].value, "v\\backslash", "an awkward header value")
    assertEqual(got.headers[2].value, "a repeated key", "a repeated key keeps its place")
    assertTrue(isOk(q.nack(got.id, true)), "requeue")
    round += 1
  }
  q.close()
}

test "x-delivery-count becomes deliveryCount and is not a header" {
  val q = open(optionsFor(freshName()).withQuorumQueue())
  mustPublish(
    q,
    Mq.Message(id = "q-1", body = "x", headers = [Mq.MessageHeader(key = "k", value = "v")], deliveryCount = 0)
  )
  var round = 0
  while round < 4 {
    val got = mustTake(q)
    assertEqualInt(got.deliveryCount, round, "deliveryCount in round " + round.toString())
    assertEqualInt(got.headers.length, 1, "only the message's own header")
    assertTrue(isOk(q.nack(got.id, true)), "requeue")
    round += 1
  }
  val name = RabbitMq.queueName(q)
  q.close()
  val counted: slice[Amqp.TableEntry] = [Amqp.entry("x-delivery-count", Amqp.fLong(7i64))]
  val r = open(optionsFor(name).withQuorumQueue())
  val _ = Extras.purgeQueue(r, name)
  inject(name, Amqp.noProperties().copy(messageId = "seven", headers = counted), encodeUtf8("x"))
  val got = mustTake(r)
  assertEqualInt(got.deliveryCount, 7, "a count from the broker is taken as it is")
  assertEqualInt(got.headers.length, 0, "and not delivered as a header")
  r.close()
}

test "nack without requeue discards, or dead-letters when the queue has a dead-letter exchange" {
  val plain = openFresh()
  mustPublish(plain, message("gone", "x"))
  assertTrue(isOk(plain.nack(mustTake(plain).id, false)), "reject")
  expectTimeout(plain, "not redelivered")
  assertEqualInt(readyCount(RabbitMq.queueName(plain)), 0, "discarded")
  plain.close()
  val name = freshName()
  val dead = open(optionsFor(name + ".dead"))
  val q = open(optionsFor(name).withDeadLetter("", name + ".dead"))
  mustPublish(
    q,
    Mq.Message(id = "poison", body = "p", headers = [Mq.MessageHeader(key = "why", value = "test")], deliveryCount = 0)
  )
  assertTrue(isOk(q.nack(mustTake(q).id, false)), "reject")
  expectTimeout(q, "not redelivered")
  val letter = mustTake(dead)
  assertEqual(letter.id, "poison", "the dead letter keeps its id")
  assertEqual(headerOf(letter, "why"), "test", "and its headers")
  assertEqual(
    headerOf(letter, "x-death"),
    "[{\"count\":1,\"reason\":\"rejected\",\"queue\":\"" + name + "\"}]",
    "x-death is JSON text"
  )
  dead.close()
  q.close()
}

test "an empty message id is refused on publish" {
  val q = openFresh()
  expectContains(
    errorOf(RabbitMq.publishTo(q, "", RabbitMq.queueName(q), true, message("", "x"), Amqp.noProperties()), "publishTo"),
    "empty message id",
    "empty id"
  )
  expectContains(
    errorOf(Extras.publishWith(q, message("", "x"), Extras.defaultPublishOptions(q)), "publishWith"),
    "empty message id",
    "publishWith"
  )
  // Through the interface an empty id violates its `requires:` clause.
  assertPanics("publish with an empty id", { -> ignoreResult(q.publish(message("", "x"))) })
  expectTimeout(q, "nothing was published")
  q.close()
}

func ignoreResult(r: in Result[Unit, String]): Unit {
  ()
}

test "a message that cannot be encoded is refused before anything is sent" {
  val q = openFresh()
  val longKey = Mq.Message(
    id = "m",
    body = "x",
    headers = [Mq.MessageHeader(key = Str.repeat("k", 256), value = "v")],
    deliveryCount = 0
  )
  expectContains(errorOf(q.publish(longKey), "a long header key"), "message 'm' cannot be sent", "names the message")
  expectContains(errorOf(q.publish(longKey), "a long header key"), "exceeds the AMQP limit of 255", "says why")
  val emptyKey = Mq.Message(
    id = "m",
    body = "x",
    headers = [Mq.MessageHeader(key = "", value = "v")],
    deliveryCount = 0
  )
  expectContains(errorOf(q.publish(emptyKey), "an empty header key"), "empty name", "an empty header key")
  expectContains(
    errorOf(q.publish(message(Str.repeat("i", 256), "x")), "a long id"),
    "exceeds the AMQP limit of 255",
    "a long id"
  )
  assertTrue(RabbitMq.isOpen(q), "nothing was written")
  mustPublish(q, message("good", "x"))
  assertEqual(mustTake(q).id, "good", "still usable")
  q.close()
}

test "publishBatch pipelines the batch and delivers it in order" {
  val q = open(optionsFor(freshName()).copy(prefetchCount = 0))
  val batch: List[Mq.Message] = newList()
  var n = 0
  while n < 100 {
    batch.add(message("b-" + n.toString(), "body " + n.toString()))
    n += 1
  }
  match q.publishBatch(batch.toArray()) {
    case Ok(_) -> ()
    case Err(e) -> assertTrue(false, "publishBatch failed: " + e)
  }
  n = 0
  while n < 100 {
    val got = mustTake(q)
    assertEqual(got.id, "b-" + n.toString(), "batch order")
    assertTrue(isOk(q.ack(got.id)), "ack")
    n += 1
  }
  expectTimeout(q, "exactly the batch was published")
  val withEmptyId = [message("x", "1"), message("", "2")]
  // The interface makes ids a precondition of publishBatch. Where the
  // inherited clause is enforced (jvm) the call is a contract violation;
  // where it is not (dotnet, lyric-lang#9043) the backend returns an Err.
  var refused = false
  try {
    match q.publishBatch(withEmptyId) {
      case Err(e) -> {
        refused = Str.contains(e, "empty message id")
      }
      case Ok(_) -> ()
    }
  } catch Bug as _ {
    refused = true
  }
  assertTrue(refused, "a batch with an empty id is refused before anything is sent")
  expectTimeout(q, "nothing of the refused batch was published")
  q.close()
}

func refuse(m: in Mq.Message): Result[Unit, String] {
  Err("handler said no")
}

test "Auto mode acknowledges on Ok and requeues on Err" {
  val q = open(optionsFor(freshName()).copy(ackMode = Mq.AckMode.Auto))
  mustPublish(q, message("auto-1", "x"))
  assertEqual(
    errorOf(q.consume(3000, { m -> refuse(m) }), "a failing handler"),
    "handler said no",
    "the handler's error"
  )
  val again = mustTake(q)
  assertEqual(again.id, "auto-1", "requeued")
  assertEqualInt(again.deliveryCount, 1, "as a redelivery")
  assertEqualInt(RabbitMq.unsettledCount(q), 0, "acknowledged when the handler returned Ok")
  assertTrue(isOk(q.ack("auto-1")), "ack is a no-op in Auto mode")
  assertTrue(isOk(q.nack("auto-1", true)), "nack is a no-op in Auto mode")
  assertTrue(isErr(q.ack("")), "an empty id is still an Err")
  expectTimeout(q, "nothing is left")
  q.close()
}

test "the prefetch window limits unsettled deliveries; 0 lifts the limit" {
  val q = open(optionsFor(freshName()).copy(prefetchCount = 2))
  for id in ["a", "b", "c"] {
    mustPublish(q, message(id, "x"))
  }
  assertEqual(mustTake(q).id, "a", "first")
  assertEqual(mustTake(q).id, "b", "second")
  expectTimeout(q, "the window of two is full")
  assertTrue(isOk(q.ack("a")), "ack")
  assertEqual(mustTake(q).id, "c", "the window opened")
  q.close()
  val wide = open(optionsFor(freshName()).copy(prefetchCount = 0))
  var n = 0
  while n < 30 {
    mustPublish(wide, message("w-" + n.toString(), "x"))
    n += 1
  }
  n = 0
  while n < 30 {
    assertEqual(mustTake(wide).id, "w-" + n.toString(), "no window")
    n += 1
  }
  wide.close()
}

test "deliveries left unsettled at close go back to the queue, flagged redelivered" {
  val name = freshName()
  val first = open(optionsFor(name))
  mustPublish(first, message("left-1", "a"))
  mustPublish(first, message("left-2", "b"))
  assertEqual(mustTake(first).id, "left-1", "delivered")
  first.close()
  val second = open(optionsFor(name))
  val a = mustTake(second)
  assertEqual(a.id, "left-1", "order is kept")
  assertEqualInt(a.deliveryCount, 1, "redelivered")
  val b = mustTake(second)
  assertEqual(b.id, "left-2", "the prefetched one too")
  assertEqualInt(b.deliveryCount, 1, "it had reached the first client's window")
  second.close()
}

test "a publisher that never consumes takes no messages" {
  val name = freshName()
  val publisher = open(optionsFor(name))
  mustPublish(publisher, message("only", "x"))
  K.sleepMs(150)
  assertEqualInt(readyCount(name), 1, "the message waits in the queue")
  publisher.close()
}

// ─── Messages from other producers ───────────────────────────────────────────
test "a message without a message-id gets an id from its delivery tag" {
  val q = openFresh()
  inject(RabbitMq.queueName(q), Amqp.noProperties(), encodeUtf8("anonymous"))
  val got = mustTake(q)
  assertEqual(got.id, "delivery-1", "the delivery tag stands in")
  assertEqual(got.body, "anonymous", "body")
  assertEqualInt(got.headers.length, 0, "no headers")
  assertTrue(isOk(q.ack(got.id)), "and settles the delivery")
  assertEqualInt(RabbitMq.unsettledCount(q), 0, "settled")
  q.close()
}

test "header values of other types are delivered as text" {
  val q = openFresh()
  val nested: slice[Amqp.TableEntry] = [Amqp.entry("n", Amqp.fInt(1)), Amqp.entry("s", Amqp.fText("a\"b\n\u{0001}"))]
  val items: slice[Amqp.FieldValue] = [
    Amqp.fInt(1),
    Amqp.fText("two"),
    Amqp.FBool(value = false),
    Amqp.FVoid,
    Amqp.FDouble(bits = 4609434218613702656i64),
    Amqp.FDecimal(scale = 1, unscaled = 25),
    Amqp.FTable(entries = nested)
  ]
  val typed: slice[Amqp.TableEntry] = [
    Amqp.entry("int", Amqp.fInt(-42)),
    Amqp.entry("long", Amqp.fLong(9007199254740993i64)),
    Amqp.entry("byte", Amqp.FInteger(tag = 98, value = -1i64)),
    Amqp.entry("bool", Amqp.FBool(value = true)),
    Amqp.entry("float", Amqp.FFloat(bits = 1069547520)),
    Amqp.entry("double", Amqp.FDouble(bits = 4609434218613702656i64)),
    Amqp.entry("decimal", Amqp.FDecimal(scale = 2, unscaled = -1234)),
    Amqp.entry("small", Amqp.FDecimal(scale = 3, unscaled = 5)),
    Amqp.entry("whole", Amqp.FDecimal(scale = 0, unscaled = 7)),
    Amqp.entry("time", Amqp.FTimestamp(seconds = 1700000000i64)),
    Amqp.entry("void", Amqp.FVoid),
    Amqp.entry("bytes", Amqp.FBytes(bytes = encodeUtf8("raw"))),
    Amqp.entry("binary", Amqp.FString(bytes = bytesOf([255, 254]))),
    Amqp.entry("array", Amqp.FArray(items = items)),
    Amqp.entry("table", Amqp.FTable(entries = nested))
  ]
  inject(RabbitMq.queueName(q), Amqp.noProperties().copy(messageId = "typed", headers = typed), encodeUtf8("x"))
  val got = mustTake(q)
  assertEqualInt(got.headers.length, 15, "every header arrives")
  assertEqual(headerOf(got, "int"), "-42", "an integer")
  assertEqual(headerOf(got, "long"), "9007199254740993", "a 64-bit integer")
  assertEqual(headerOf(got, "byte"), "-1", "a signed byte")
  assertEqual(headerOf(got, "bool"), "true", "a boolean")
  assertEqual(headerOf(got, "float"), "1.5", "a float")
  assertEqual(headerOf(got, "double"), "1.5", "a double")
  assertEqual(headerOf(got, "decimal"), "-12.34", "a decimal")
  assertEqual(headerOf(got, "small"), "0.005", "a decimal below one")
  assertEqual(headerOf(got, "whole"), "7", "a decimal without a scale")
  assertEqual(headerOf(got, "time"), "1700000000", "a timestamp")
  assertEqual(headerOf(got, "void"), "", "void")
  assertEqual(headerOf(got, "bytes"), "raw", "a byte array that is text")
  assertEqual(headerOf(got, "binary"), "//4=", "bytes that are not UTF-8, as base64")
  assertEqual(
    headerOf(got, "array"),
    "[1,\"two\",false,null,\"1.5\",2.5,{\"n\":1,\"s\":\"a\\\"b\\n\\u0001\"}]",
    "an array as JSON"
  )
  assertEqual(headerOf(got, "table"), "{\"n\":1,\"s\":\"a\\\"b\\n\\u0001\"}", "a table as JSON")
  q.close()
}

test "a body that is not UTF-8 is rejected without requeueing and reported; the queue moves on" {
  val name = freshName()
  val dead = open(optionsFor(name + ".dead"))
  val q = open(optionsFor(name).withDeadLetter("", name + ".dead"))
  inject(name, Amqp.noProperties().copy(messageId = "binary"), bytesOf([255, 254, 0]))
  inject(name, Amqp.noProperties(), bytesOf([192]))
  mustPublish(q, message("text", "fine"))
  val e = errorOf(q.consume(3000, { m -> Ok(()) }), "a binary body")
  expectContains(e, "rabbitmq: message 'binary' cannot be delivered: its body is not UTF-8 text", "names the message")
  expectContains(e, "it was rejected without requeueing", "says what happened to it")
  val f = errorOf(q.consume(3000, { m -> Ok(()) }), "a binary body without an id")
  expectContains(f, "rabbitmq: delivery 2 cannot be delivered", "names the delivery")
  assertEqual(mustTake(q).id, "text", "the queue moves on")
  assertEqualInt(RabbitMq.unsettledCount(q), 1, "only the good message is unsettled")
  K.sleepMs(150)
  assertEqualInt(readyCount(name + ".dead"), 2, "the broker dead-lettered both")
  dead.close()
  q.close()
}

test "a delivery larger than maxMessageBytes is rejected without being kept" {
  val q = open(optionsFor(freshName()).copy(maxMessageBytes = 1000, frameMax = 4096))
  mustPublish(q, message("too-big", bigBody(20000)))
  mustPublish(q, message("fits", bigBody(1000)))
  val e = errorOf(q.consume(3000, { m -> Ok(()) }), "an oversized delivery")
  expectContains(
    e,
    "message 'too-big' cannot be delivered: its body of 20000 bytes exceeds maxMessageBytes (1000)",
    "the reason"
  )
  assertEqual(mustTake(q).id, "fits", "a body of exactly the limit is delivered")
  expectTimeout(q, "the oversized message was not requeued")
  q.close()
}

// ─── Extras ──────────────────────────────────────────────────────────────────
test "exchanges: declare, bind, publish with a routing key, delete" {
  val orders = openFresh()
  val audit = openFresh()
  assertTrue(isOk(Extras.declareExchange(orders, "events", "direct", Extras.defaultExchangeOptions())), "declare")
  assertTrue(
    isOk(Extras.declareExchange(orders, "events", "direct", Extras.defaultExchangeOptions())),
    "declaring again is a no-op"
  )
  assertTrue(isOk(Extras.bindQueue(orders, "events", "order")), "bind orders")
  assertTrue(isOk(Extras.bindQueue(audit, "events", "#")), "bind audit")
  assertTrue(
    isOk(Extras.publishWith(orders, message("o-1", "placed"), Extras.toExchange(orders, "events", "order"))),
    "publish"
  )
  assertTrue(
    isOk(Extras.publishWith(orders, message("p-1", "paid"), Extras.toExchange(orders, "events", "payment"))),
    "publish"
  )
  assertEqual(mustTake(orders).id, "o-1", "the order queue gets the order")
  expectTimeout(orders, "and not the payment")
  assertEqual(mustTake(audit).id, "o-1", "the audit queue gets both")
  assertEqual(mustTake(audit).id, "p-1", "in order")
  val clash = errorOf(
    Extras.declareExchange(orders, "events", "fanout", Extras.defaultExchangeOptions()),
    "another type"
  )
  expectContains(clash, "406 PRECONDITION_FAILED", "an exchange cannot change its type")
  expectContains(
    errorOf(Extras.bindQueue(orders, "no.such", "k"), "bind to nothing"),
    "404 NOT_FOUND",
    "an unknown exchange"
  )
  assertTrue(isOk(Extras.deleteExchange(orders, "events", false)), "delete")
  val gone = errorOf(
    Extras.publishWith(orders, message("z", "z"), Extras.toExchange(orders, "events", "order")),
    "deleted"
  )
  expectContains(gone, "404 NOT_FOUND", "the exchange is gone")
  audit.close()
  orders.close()
}

test "queues: declare, inspect, purge and delete another queue" {
  val q = openFresh()
  val other = freshName()
  match Extras.declareQueue(q, other, Extras.defaultQueueOptions()) {
    case Ok(info) -> {
      assertEqual(info.name, other, "name")
      assertEqualLong(info.messageCount, 0i64, "empty")
    }
    case Err(e) -> assertTrue(false, e)
  }
  assertTrue(isOk(Extras.publishWith(q, message("o-1", "x"), Extras.toExchange(q, "", other))), "publish to it")
  assertTrue(isOk(Extras.publishWith(q, message("o-2", "x"), Extras.toExchange(q, "", other))), "twice")
  match Extras.inspectQueue(q, other) {
    case Ok(info) -> assertEqualLong(info.messageCount, 2i64, "two wait")
    case Err(e) -> assertTrue(false, e)
  }
  match Extras.purgeQueue(q, other) {
    case Ok(n) -> assertEqualLong(n, 2i64, "purged")
    case Err(e) -> assertTrue(false, e)
  }
  match Extras.deleteQueue(q, other) {
    case Ok(n) -> assertEqualLong(n, 0i64, "deleted")
    case Err(e) -> assertTrue(false, e)
  }
  match Extras.inspectQueue(q, other) {
    case Ok(_) -> assertTrue(false, "the queue was deleted")
    case Err(e) -> expectContains(e, "404 NOT_FOUND", "gone")
  }
  match RabbitMq.request(q, Amqp.BasicQos(prefetchSize = 0i64, prefetchCount = 1, global = false)) {
    case Ok(_) -> assertTrue(false, "request must refuse methods the client's state depends on")
    case Err(e) -> expectContains(e, "basic.qos cannot be sent through request", "refused")
  }
  q.close()
}

test "publishWith carries the message properties and refuses invalid ones" {
  val name = freshName()
  val q = open(optionsFor(name))
  val base = Extras.defaultPublishOptions(q)
  assertEqual(base.routingKey, name, "the default target is the connection's queue")
  assertTrue(base.mandatory and base.persistent, "mandatory and persistent like Mq.publish")
  val full = base.copy(
    persistent = false,
    priority = 7,
    expirationMs = 60000i64,
    contentType = "application/json",
    contentEncoding = "utf-8",
    correlationId = "corr-1",
    replyTo = "replies",
    kind = "order.placed",
    appId = "tests",
    timestamp = 1700000000i64
  )
  match Extras.propertiesOf(full) {
    case Err(e) -> assertTrue(false, e)
    case Ok(p) -> {
      assertEqualInt(p.deliveryMode, 1, "transient")
      assertEqualInt(p.priority, 7, "priority")
      assertEqual(p.expiration, "60000", "expiration as decimal text")
      assertEqual(p.contentType, "application/json", "content-type")
      assertEqual(p.correlationId, "corr-1", "correlation-id")
      assertEqual(p.replyTo, "replies", "reply-to")
      assertEqual(p.kind, "order.placed", "type")
      assertEqual(p.appId, "tests", "app-id")
      assertEqualLong(p.timestamp, 1700000000i64, "timestamp")
    }
  }
  assertTrue(isOk(Extras.publishWith(q, message("full", "x"), full)), "publish")
  K.lockEnter(broker().lock)
  val stored = broker().queues[name].ready[0].properties
  K.lockExit(broker().lock)
  assertEqual(stored.messageId, "full", "message-id reached the broker")
  assertEqualInt(stored.deliveryMode, 1, "delivery-mode reached the broker")
  assertEqualInt(stored.priority, 7, "priority reached the broker")
  assertEqual(stored.expiration, "60000", "expiration reached the broker")
  assertEqual(stored.kind, "order.placed", "type reached the broker")
  mustPublish(q, message("default", "x"))
  K.lockEnter(broker().lock)
  val plain = broker().queues[name].ready[1].properties
  K.lockExit(broker().lock)
  assertEqualInt(plain.deliveryMode, 2, "Mq.publish is persistent by default")
  assertEqual(plain.contentType, "", "and sets no other property")
  expectContains(
    errorOf(Extras.publishWith(q, message("m", "x"), base.copy(priority = 256)), "priority"),
    "priority",
    "priority"
  )
  expectContains(
    errorOf(Extras.publishWith(q, message("m", "x"), base.copy(priority = -1)), "priority"),
    "priority",
    "priority"
  )
  expectContains(
    errorOf(Extras.publishWith(q, message("m", "x"), base.copy(expirationMs = -1i64)), "expiration"),
    "expirationMs",
    "expiration"
  )
  expectContains(
    errorOf(Extras.publishWith(q, message("m", "x"), base.copy(timestamp = -1i64)), "timestamp"),
    "timestamp",
    "timestamp"
  )
  q.close()
  val transient = open(optionsFor(freshName()).copy(persistent = false))
  mustPublish(transient, message("t", "x"))
  K.lockEnter(broker().lock)
  val mode = broker().queues[RabbitMq.queueName(transient)].ready[0].properties.deliveryMode
  K.lockExit(broker().lock)
  assertEqualInt(mode, 1, "persistent = false publishes transient messages")
  transient.close()
}

// ─── Heartbeats ──────────────────────────────────────────────────────────────
test "with a heartbeat the client beats while it waits, and keepAlive beats on request" {
  val b = knobBroker("", 131072, 1)
  val q = open(optionsOn(K.serverPort(b.server), "q").copy(heartbeatSeconds = 1))
  assertEqualInt(RabbitMq.heartbeatSeconds(q), 1, "negotiated")
  val got = Got(messages = newList())
  match q.consume(2600, { m -> keep(got, m) }) {
    case Err(e) -> assertEqual(e, "timeout", "the wait ends in a timeout, not a lost connection")
    case Ok(_) -> assertTrue(false, "nothing was published")
  }
  assertTrue(b.heartbeatsSeen >= 3, "the broker saw heartbeats during the wait: " + b.heartbeatsSeen.toString())
  mustPublish(q, message("alive", "x"))
  val before = b.heartbeatsSeen
  assertTrue(isOk(RabbitMq.keepAlive(q)), "keepAlive")
  K.sleepMs(150)
  assertTrue(b.heartbeatsSeen > before, "keepAlive sent a heartbeat frame")
  assertEqual(mustTake(q).id, "alive", "deliveries that arrived during keepAlive are kept")
  q.close()
  K.serverClose(b.server)
}

test "with a heartbeat a silent broker is given up on after two intervals" {
  val b = knobBroker("mute", 131072, 1)
  val q = open(optionsOn(K.serverPort(b.server), "q").copy(heartbeatSeconds = 1))
  val started = K.monotonicMillis()
  val e = errorOf(q.consume(8000, { m -> Ok(()) }), "a silent broker")
  val elapsed = K.monotonicMillis() - started
  expectContains(e, "rabbitmq: connection lost: no frame from the broker for", "heartbeat timeout")
  assertTrue(elapsed < 6000i64, "well before the consume timeout: " + elapsed.toString() + " ms")
  assertTrue(not RabbitMq.isOpen(q), "lost")
  q.close()
  K.serverClose(b.server)
}

test "without a heartbeat nothing is sent while waiting, and keepAlive is a liveness probe" {
  val b = knobBroker("", 131072, 60)
  val q = open(optionsOn(K.serverPort(b.server), "q"))
  expectTimeout(q, "an idle wait")
  assertEqualInt(b.heartbeatsSeen, 0, "no heartbeat frames")
  assertTrue(isOk(RabbitMq.keepAlive(q)), "keepAlive on a live connection")
  K.sleepMs(150)
  assertEqualInt(b.heartbeatsSeen, 1, "keepAlive always sends one")
  q.close()
  K.serverClose(b.server)
}

// ─── Threads ─────────────────────────────────────────────────────────────────
func publishMany(q: in RabbitMq.RabbitMqQueue, count: in Int): Unit {
  var n = 0
  while n < count {
    mustPublish(q, message("t-" + n.toString(), n.toString()))
    n += 1
  }
}

test "a publisher thread and a consuming thread share one connection" {
  val q = open(optionsFor(freshName()).copy(prefetchCount = 0))
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
  assertEqualInt(RabbitMq.unsettledCount(q), 0, "all settled")
  q.close()
}

// ─── TLS ─────────────────────────────────────────────────────────────────────
// Options for the TLS broker that trust only the test CA.
func tlsOptionsFor(queue: in String): RabbitMq.ConnectOptions {
  optionsOn(tlsBrokerPort(), queue).copy(connectTimeoutMs = 5000).withTls().withExclusiveCaCertificate(TEST_ONLY_CA)
}

func mutualTlsOptionsFor(queue: in String): RabbitMq.ConnectOptions {
  tlsOptionsFor(queue).copy(port = mutualTlsBrokerPort())
}

func makeTlsEnds(): MqConformance.QueueEnds {
  val q = open(tlsOptionsFor(freshName()))
  opened.queues.add(q)
  val publisher: Mq.MessageQueue = q
  val consumer: Mq.QueueConsumer = q
  MqConformance.QueueEnds(publisher = publisher, consumer = consumer)
}

func expectRoundTrip(options: in RabbitMq.ConnectOptions, label: in String): Unit {
  match RabbitMq.connectWith(options) {
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

func expectHandshakeFailure(options: in RabbitMq.ConnectOptions, label: in String): Unit {
  val e = errorOfConnect(options)
  expectContains(e, "rabbitmq: TLS handshake with 127.0.0.1:", label)
  assertTrue(not Str.contains(e, "secret"), label + ": the password is not echoed")
}

test "the backend conforms to MessageQueue and QueueConsumer over TLS" {
  requireConformance(conformance({ -> makeTlsEnds() }))
}

test "TLS: the server is verified against a configured CA, exclusive or additional" {
  expectRoundTrip(tlsOptionsFor(freshName()), "exclusive CA")
  val additional = optionsOn(tlsBrokerPort(), freshName()).withTls().withCaCertificate(TEST_ONLY_CA)
  expectRoundTrip(additional, "additional CA")
  val q = open(tlsOptionsFor(freshName()))
  assertEqual(RabbitMq.serverInfo(q), "FakeRabbit 1.0", "connection.start came through TLS")
  q.close()
}

test "TLS: a server whose CA is not trusted is refused" {
  val untrusted = optionsOn(tlsBrokerPort(), freshName()).copy(connectTimeoutMs = 5000).withTls()
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
  val insecure = optionsOn(tlsBrokerPort(), freshName()).copy(connectTimeoutMs = 5000).withTls().withTlsServerName("broker.invalid").withInsecureSkipVerify()
  Env.setVar("LYRIC_TLS_ALLOW_INSECURE", "")
  assertTrue(not RabbitMq.skipsVerification(insecure.tls), "the option alone does not skip verification")
  expectHandshakeFailure(insecure, "the option without the environment variable")
  Env.setVar("LYRIC_TLS_ALLOW_INSECURE", "true")
  expectHandshakeFailure(insecure, "the variable must be exactly 1")
  Env.setVar("LYRIC_TLS_ALLOW_INSECURE", "1")
  assertTrue(RabbitMq.insecureAllowedByEnvironment(), "the environment allows it")
  val secure = tlsOptionsFor(freshName()).withTlsServerName("broker.invalid")
  assertTrue(not RabbitMq.skipsVerification(secure.tls), "the environment variable alone does not skip verification")
  expectHandshakeFailure(secure, "the environment variable without the option")
  assertTrue(RabbitMq.skipsVerification(insecure.tls), "both keys")
  expectRoundTrip(insecure, "both keys")
  Env.setVar("LYRIC_TLS_ALLOW_INSECURE", "")
}

test "TLS: a required client certificate is presented, and its absence refused" {
  val base = mutualTlsOptionsFor(freshName())
  expectRoundTrip(base.withClientIdentity(TEST_ONLY_CLIENT_CERT, TEST_ONLY_CLIENT_KEY), "mutual TLS")
  // With TLS 1.3 the client learns of the refusal after its own handshake
  // has finished, so the failure is either a handshake or a connection error.
  val without = errorOfConnect(base)
  assertTrue(Str.startsWith(without, "rabbitmq: "), "no client certificate: " + without)
  assertTrue(without != "connected", "no client certificate must not connect")
  val stray = errorOfConnect(base.withClientIdentity(TEST_ONLY_STRAY_CERT, TEST_ONLY_STRAY_KEY))
  assertTrue(Str.startsWith(stray, "rabbitmq: "), "a client certificate from another CA: " + stray)
  assertTrue(stray != "connected", "a client certificate from another CA must not connect")
}

test "TLS: a minimum version of TLS 1.3 connects" {
  expectRoundTrip(tlsOptionsFor(freshName()).withMinTlsVersion(Tls.TlsVersion.Tls13), "TLS 1.3 only")
}

test "TLS: consume timeouts, large bodies and broker failures behave as over plain TCP" {
  val q = open(tlsOptionsFor(freshName()))
  expectTimeout(q, "a timeout over TLS")
  val body = bigBody(300000)
  mustPublish(q, message("big", body))
  val got = mustTake(q)
  assertTrue(got.body == body, "a body larger than a TLS record")
  assertTrue(isOk(q.nack(got.id, true)), "requeue")
  assertEqualInt(mustTake(q).deliveryCount, 1, "redelivered")
  val e = errorOf(
    Extras.publishWith(q, message("x", "x"), Extras.toExchange(q, "no.such.exchange", "k")),
    "no exchange"
  )
  expectContains(e, "404 NOT_FOUND", "a channel error over TLS")
  mustPublish(q, message("after", "x"))
  val dropped = errorOf(Extras.publishWith(q, message("d", "x"), Extras.toExchange(q, "", "fail.drop")), "dropped")
  expectContains(dropped, "connection lost", "a dropped TLS connection")
  q.close()
}

test "TLS: the transport must match the listener" {
  // TLS towards a plain listener: the handshake never completes.
  val tlsToPlain = optionsOn(brokerPort(), freshName()).copy(connectTimeoutMs = 1500).withTls().withExclusiveCaCertificate(
    TEST_ONLY_CA
  )
  expectHandshakeFailure(tlsToPlain, "TLS to a plain listener")
  // Plain AMQP towards a TLS listener: no connection.start comes back.
  val e = errorOfConnect(optionsOn(tlsBrokerPort(), freshName()).copy(requestTimeoutMs = 1500))
  assertTrue(Str.startsWith(e, "rabbitmq: "), "plain to a TLS listener: " + e)
  assertTrue(e != "connected", "plain AMQP must not get through a TLS listener")
}

test "TLS options are validated before any connection is made" {
  val plain = RabbitMq.defaultOptions("127.0.0.1", 1, "a")
  expectContains(errorOfConnect(plain.withCaCertificate(TEST_ONLY_CA)), "the connection is not TLS", "a CA without TLS")
  expectContains(errorOfConnect(plain.withInsecureSkipVerify()), "the connection is not TLS", "skip-verify without TLS")
  expectContains(errorOfConnect(plain.withTlsServerName("h")), "the connection is not TLS", "a server name without TLS")
  expectContains(
    errorOfConnect(plain.withMinTlsVersion(Tls.TlsVersion.Tls13)),
    "the connection is not TLS",
    "a version without TLS"
  )
  expectContains(
    errorOfConnect(plain.withClientIdentity(TEST_ONLY_CLIENT_CERT, TEST_ONLY_CLIENT_KEY)),
    "the connection is not TLS",
    "a client identity without TLS"
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

test "an amqps URL turns TLS on, with the TLS default port" {
  match RabbitMq.optionsFromUrl("amqps://u:p@broker.example", "orders") {
    case Err(e) -> assertTrue(false, e)
    case Ok(o) -> {
      assertTrue(o.tls.enabled, "TLS")
      assertEqualInt(o.port, 5671, "the default TLS port")
      assertEqual(o.user, "u", "user")
      assertTrue(not o.tls.insecureSkipVerify, "verification is on")
    }
  }
  val url = "amqps://lyric:secret@127.0.0.1:" + tlsBrokerPort().toString()
  // No CA is configured, so the URL alone must fail on the test certificate.
  match RabbitMq.connectUrl(url, "orders") {
    case Ok(_) -> assertTrue(false, "the test CA is not in the platform trust store")
    case Err(e) -> expectContains(e, "rabbitmq: TLS handshake with 127.0.0.1:", "connectUrl over TLS")
  }
  match RabbitMq.optionsFromUrl(url, freshName()) {
    case Err(e) -> assertTrue(false, e)
    case Ok(o) -> expectRoundTrip(o.copy(requestTimeoutMs = 3000).withCaCertificate(TEST_ONLY_CA), "URL plus a CA")
  }
}

func writeTestOnlyFile(name: in String, text: in String): String {
  val path = Env.getVarOrDefault("TMPDIR", "/tmp") + "/lyric-mq-rabbitmq-test-only-" + name
  match File.writeText(path, text) {
    case Ok(_) -> path
    case Err(_) -> panic("cannot write " + path)
  }
}

test "connectFromEnv reads the TLS settings" {
  val caPath = writeTestOnlyFile("ca.pem", TEST_ONLY_CA)
  val certPath = writeTestOnlyFile("client.pem", TEST_ONLY_CLIENT_CERT)
  val keyPath = writeTestOnlyFile("client.key", TEST_ONLY_CLIENT_KEY)
  val prefix = "LYRIC_CONFIG_MQ_RABBITMQ_"
  Env.setVar(prefix + "URL", "amqps://lyric:secret@127.0.0.1:" + mutualTlsBrokerPort().toString())
  Env.setVar(prefix + "QUEUENAME", freshName())
  Env.setVar(prefix + "ACKMODE", "")
  Env.setVar(prefix + "PREFETCHCOUNT", "")
  Env.setVar(prefix + "TLSCAFILE", caPath + ".missing")
  match RabbitMq.connectFromEnv() {
    case Ok(_) -> assertTrue(false, "the CA file does not exist")
    case Err(e) -> expectContains(e, "LYRIC_CONFIG_MQ_RABBITMQ_TLSCAFILE", "an unreadable CA file")
  }
  Env.setVar(prefix + "TLSCAFILE", caPath)
  Env.setVar(prefix + "TLSCAEXCLUSIVE", "maybe")
  match RabbitMq.connectFromEnv() {
    case Ok(_) -> assertTrue(false, "TLSCAEXCLUSIVE is not a boolean")
    case Err(e) -> expectContains(e, "TLSCAEXCLUSIVE must be true or false", "a bad boolean")
  }
  Env.setVar(prefix + "TLSCAEXCLUSIVE", "true")
  Env.setVar(prefix + "TLSMINVERSION", "1.1")
  match RabbitMq.connectFromEnv() {
    case Ok(_) -> assertTrue(false, "TLS 1.1 cannot be asked for")
    case Err(e) -> expectContains(e, "TLSMINVERSION must be 1.2 or 1.3", "a bad version")
  }
  Env.setVar(prefix + "TLSMINVERSION", "1.3")
  Env.setVar(prefix + "TLSCERTFILE", certPath)
  match RabbitMq.connectFromEnv() {
    case Ok(_) -> assertTrue(false, "a certificate without a key")
    case Err(e) -> expectContains(e, "without its private key", "a certificate file alone")
  }
  Env.setVar(prefix + "TLSKEYFILE", keyPath)
  Env.setVar(prefix + "TLSSERVERNAME", "localhost")
  match RabbitMq.connectFromEnv() {
    case Err(e) -> assertTrue(false, e)
    case Ok(q) -> {
      mustPublish(q, message("env-tls", "from env over TLS"))
      assertEqual(mustTake(q).body, "from env over TLS", "round trip")
      q.close()
    }
  }
  Env.setVar(prefix + "TLSSERVERNAME", "broker.invalid")
  match RabbitMq.connectFromEnv() {
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
// valid for a century) and is used by nothing else. The private keys are
// public by being here: never trust these certificates outside this file.
//
//   TEST_ONLY_CA            self-signed CA
//   TEST_ONLY_SERVER_*      issued by the CA; SAN DNS:localhost, IP:127.0.0.1
//   TEST_ONLY_CLIENT_*      issued by the CA; clientAuth
//   TEST_ONLY_OTHER_CA      a second, unrelated self-signed CA
//   TEST_ONLY_STRAY_*       a client certificate issued by the other CA
// test-only
val TEST_ONLY_CA: String = "-----BEGIN CERTIFICATE-----\n" + "MIIB4TCCAYegAwIBAgIUO+alojJ+juJ7cg6z28A8OSN33vUwCgYIKoZIzj0EAwIw\n" + "PTEkMCIGA1UECgwbbHlyaWMtbXEtcmFiYml0bXEgVEVTVCBPTkxZMRUwEwYDVQQD\n" + "DAx0ZXN0LW9ubHkgQ0EwIBcNMjYxMTEyMjE1MDI0WhgPMjEyNjEwMTkyMTUwMjRa\n" + "MD0xJDAiBgNVBAoMG2x5cmljLW1xLXJhYmJpdG1xIFRFU1QgT05MWTEVMBMGA1UE\n" + "AwwMdGVzdC1vbmx5IENBMFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEESQdLYyW\n" + "ZWY9gYcdqTtW2C2ObMRhf0EFKQBBEurMny5PFLCHSlT2+ELH7nS3h79TDnoaG3D6\n" + "xgP6PXSr1po24qNjMGEwHQYDVR0OBBYEFJkeOWX0aCSy0QeFPnQ6qxUXGyFOMB8G\n" + "A1UdIwQYMBaAFJkeOWX0aCSy0QeFPnQ6qxUXGyFOMA8GA1UdEwEB/wQFMAMBAf8w\n" + "DgYDVR0PAQH/BAQDAgEGMAoGCCqGSM49BAMCA0gAMEUCIHAE8WDgfTjkSlWf/8GH\n" + "VrkkAs5aW1xbnAHn3rVTCaCLAiEA2bxbxB7aKRl8c+oM89SyVfOwnBUQf1Vz+Mza\n" + "ll/ns2o=\n" + "-----END CERTIFICATE-----\n"

// test-only
val TEST_ONLY_SERVER_CERT: String = "-----BEGIN CERTIFICATE-----\n" + "MIICADCCAaagAwIBAgIUWHnO8RnX2hYAaZx9ROoWuFxPzGIwCgYIKoZIzj0EAwIw\n" + "PTEkMCIGA1UECgwbbHlyaWMtbXEtcmFiYml0bXEgVEVTVCBPTkxZMRUwEwYDVQQD\n" + "DAx0ZXN0LW9ubHkgQ0EwIBcNMjYxMTEyMjE1MDI0WhgPMjEyNjEwMTkyMTUwMjRa\n" + "MDoxJDAiBgNVBAoMG2x5cmljLW1xLXJhYmJpdG1xIFRFU1QgT05MWTESMBAGA1UE\n" + "AwwJbG9jYWxob3N0MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEQc3sTXYiKelP\n" + "xE4hcnOzZtLLHcxDlc+TtbeHxFJ7+Y1j/y/ifbox9TYkMpIdFmMqi5/ZbuqMuGz2\n" + "D12d4YMwaqOBhDCBgTAaBgNVHREEEzARgglsb2NhbGhvc3SHBH8AAAEwEwYDVR0l\n" + "BAwwCgYIKwYBBQUHAwEwDgYDVR0PAQH/BAQDAgeAMB0GA1UdDgQWBBQNkHtYl1Ur\n" + "vQ9ypSlHyyoT+kdvszAfBgNVHSMEGDAWgBSZHjll9GgkstEHhT50OqsVFxshTjAK\n" + "BggqhkjOPQQDAgNIADBFAiEAi4kRzl2mhvJqv7Svp5G2BEvGa94s0gNzuSSVsje4\n" + "likCICAKSvPFhBm63HXS3NRefLJT1tjqRifMUETMdENf7rEN\n" + "-----END CERTIFICATE-----\n"

// test-only
val TEST_ONLY_SERVER_KEY: String = "-----BEGIN PRIVATE KEY-----\n" + "MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQg4WwIB+fW3rI9DGpN\n" + "IR6q8KzjRN2sw4m88Q3IqVRJF56hRANCAARBzexNdiIp6U/ETiFyc7Nm0ssdzEOV\n" + "z5O1t4fEUnv5jWP/L+J9ujH1NiQykh0WYyqLn9lu6oy4bPYPXZ3hgzBq\n" + "-----END PRIVATE KEY-----\n"

// test-only
val TEST_ONLY_CLIENT_CERT: String = "-----BEGIN CERTIFICATE-----\n" + "MIIB6jCCAY+gAwIBAgIUWHnO8RnX2hYAaZx9ROoWuFxPzGMwCgYIKoZIzj0EAwIw\n" + "PTEkMCIGA1UECgwbbHlyaWMtbXEtcmFiYml0bXEgVEVTVCBPTkxZMRUwEwYDVQQD\n" + "DAx0ZXN0LW9ubHkgQ0EwIBcNMjYxMTEyMjE1MDI0WhgPMjEyNjEwMTkyMTUwMjRa\n" + "MEExJDAiBgNVBAoMG2x5cmljLW1xLXJhYmJpdG1xIFRFU1QgT05MWTEZMBcGA1UE\n" + "AwwQdGVzdC1vbmx5IGNsaWVudDBZMBMGByqGSM49AgEGCCqGSM49AwEHA0IABMlW\n" + "do3iaP4SSClUDVQxbnEm5hVwFHhWpLDPiJczJ8jtRUQVRhMacdDcbgYZ5BAWnhbw\n" + "HWlEM0B6eCK+PR5xbJ+jZzBlMBMGA1UdJQQMMAoGCCsGAQUFBwMCMA4GA1UdDwEB\n" + "/wQEAwIHgDAdBgNVHQ4EFgQUQnKUeR3uJ5PoS/cgIeTb6r3lQmkwHwYDVR0jBBgw\n" + "FoAUmR45ZfRoJLLRB4U+dDqrFRcbIU4wCgYIKoZIzj0EAwIDSQAwRgIhAPtjWGyT\n" + "ZYqrTJqyaWa+mJ79Oa5PwQ57BKp26XMTgURwAiEA24DTyEg1Z1mFPItyqifkx61h\n" + "Anz4syHIwMETqQS+EwM=\n" + "-----END CERTIFICATE-----\n"

// test-only
val TEST_ONLY_CLIENT_KEY: String = "-----BEGIN PRIVATE KEY-----\n" + "MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgSG41j0EuUT+oKwOk\n" + "zyYPDPbFNzkcCDwycnVp1xEt6xGhRANCAATJVnaN4mj+EkgpVA1UMW5xJuYVcBR4\n" + "VqSwz4iXMyfI7UVEFUYTGnHQ3G4GGeQQFp4W8B1pRDNAengivj0ecWyf\n" + "-----END PRIVATE KEY-----\n"

// test-only
val TEST_ONLY_OTHER_CA: String = "-----BEGIN CERTIFICATE-----\n" + "MIIB7jCCAZOgAwIBAgIUdwp7hNvnYOcu3DZBP9fJvK0jo9gwCgYIKoZIzj0EAwIw\n" + "QzEkMCIGA1UECgwbbHlyaWMtbXEtcmFiYml0bXEgVEVTVCBPTkxZMRswGQYDVQQD\n" + "DBJ0ZXN0LW9ubHkgb3RoZXIgQ0EwIBcNMjYxMTEyMjE1MDI0WhgPMjEyNjEwMTky\n" + "MTUwMjRaMEMxJDAiBgNVBAoMG2x5cmljLW1xLXJhYmJpdG1xIFRFU1QgT05MWTEb\n" + "MBkGA1UEAwwSdGVzdC1vbmx5IG90aGVyIENBMFkwEwYHKoZIzj0CAQYIKoZIzj0D\n" + "AQcDQgAEXj7PS5EapDOsTkwXW543lzAWirriOO3W4glg8AQ0YeOUll6mqv1C85K2\n" + "r5zqvpGCOBdRZt3+sL5lhLh9BaGRF6NjMGEwHQYDVR0OBBYEFNmFPXtqIBXV1243\n" + "ohEcDKDuOzCjMB8GA1UdIwQYMBaAFNmFPXtqIBXV1243ohEcDKDuOzCjMA8GA1Ud\n" + "EwEB/wQFMAMBAf8wDgYDVR0PAQH/BAQDAgEGMAoGCCqGSM49BAMCA0kAMEYCIQDM\n" + "47DMS1SQhVsR5NwHKO4SujtvKwaT85udN20v4ZS8qAIhAOZsIK4c6XaqwuG4MAGT\n" + "Lb8OMoDT8c/0Oe+INt0GeL8t\n" + "-----END CERTIFICATE-----\n"

// test-only
val TEST_ONLY_STRAY_CERT: String = "-----BEGIN CERTIFICATE-----\n" + "MIIB9jCCAZugAwIBAgIUEA/XnPW/Vwt5Lzi/Yol1uuNuhPgwCgYIKoZIzj0EAwIw\n" + "QzEkMCIGA1UECgwbbHlyaWMtbXEtcmFiYml0bXEgVEVTVCBPTkxZMRswGQYDVQQD\n" + "DBJ0ZXN0LW9ubHkgb3RoZXIgQ0EwIBcNMjYxMTEyMjE1MDI0WhgPMjEyNjEwMTky\n" + "MTUwMjRaMEcxJDAiBgNVBAoMG2x5cmljLW1xLXJhYmJpdG1xIFRFU1QgT05MWTEf\n" + "MB0GA1UEAwwWdGVzdC1vbmx5IHN0cmF5IGNsaWVudDBZMBMGByqGSM49AgEGCCqG\n" + "SM49AwEHA0IABE36T5kW6qtA+UyWNlH8H7VzUa7j1Y6bJP6Z8yrUdOTJdrvL0xkD\n" + "hHUqszJG5IJkdPUrKMxTlpgmNpTglBqCDK6jZzBlMBMGA1UdJQQMMAoGCCsGAQUF\n" + "BwMCMA4GA1UdDwEB/wQEAwIHgDAdBgNVHQ4EFgQUr5kXlrTnYA5Rn4LCYE7sSuAh\n" + "NpcwHwYDVR0jBBgwFoAU2YU9e2ogFdXXbjeiERwMoO47MKMwCgYIKoZIzj0EAwID\n" + "SQAwRgIhAMtaBKuLR+uVuFxj57+4gmM5OzryevlIPQvVLa/cEmEvAiEA42MNluRI\n" + "Gd0VYE0JudFBQEYXR1Q+WJRf85TNmVMFYH4=\n" + "-----END CERTIFICATE-----\n"

// test-only
val TEST_ONLY_STRAY_KEY: String = "-----BEGIN PRIVATE KEY-----\n" + "MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgkAebPkHzNEA0OPq7\n" + "+olx/ELbveMxmqlS8tTyP60MyeWhRANCAARN+k+ZFuqrQPlMljZR/B+1c1Gu49WO\n" + "myT+mfMq1HTkyXa7y9MZA4R1KrMyRuSCZHT1KyjMU5aYJjaU4JQaggyu\n" + "-----END PRIVATE KEY-----\n"
