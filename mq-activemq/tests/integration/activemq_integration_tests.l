//! tests/integration/activemq_integration_tests.l - the backend against a
//! real Apache ActiveMQ Classic broker.
//!
//! Runs only with `--features integration`. The broker is named by
//! `LYRIC_ACTIVEMQ_URL` (`stomp://login:passcode@host:port`); every test fails
//! with a message saying so when the variable is unset. The broker must
//! enforce authentication (the bad-credentials test expects a refusal) and
//! use the default dead-letter strategy (the shared `ActiveMQ.DLQ` queue).
//! Each test uses its own destination, named with a fresh UUID.
//!
//! The TLS tests at the end need the same broker's TLS connectors and the
//! certificates behind them, and fail the same way when a variable is unset:
//!
//!   LYRIC_ACTIVEMQ_TLS_URL    `stomp+ssl://login:passcode@host:port`, a
//!                             connector that asks for no client certificate
//!   LYRIC_ACTIVEMQ_MTLS_URL   the same for a `needClientAuth=true` connector
//!   LYRIC_ACTIVEMQ_TLS_CA     PEM file: the CA of the broker's certificate,
//!                             which must name the URLs' host
//!   LYRIC_ACTIVEMQ_TLS_CERT   PEM file: a client certificate the broker trusts
//!   LYRIC_ACTIVEMQ_TLS_KEY    PEM file: its PKCS8 private key
//!
//! `scripts/ci/start-activemq.sh` starts such a broker and names all of them.
@cfg(feature = "integration")
@test_module
package Mq.ActiveMq.IntegrationTests

import Std.Core
import Std.Collections
import Std.Testing
import Std.Encoding
import Std.String as Str
import Std.Uuid as Uuid
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
import Mq.ActiveMq.Url as Url

func brokerUrl(): String {
  val url = Env.getVarOrDefault("LYRIC_ACTIVEMQ_URL", "")
  if url.length == 0 {
    panic(
      "LYRIC_ACTIVEMQ_URL is not set; the integration tests need a running ActiveMQ, e.g. LYRIC_ACTIVEMQ_URL=stomp://user:passcode@127.0.0.1:61613"
    )
  }
  url
}

func brokerAddress(): Url.BrokerAddress {
  match Url.parseUrl(brokerUrl()) {
    case Ok(a) -> a
    case Err(e) -> panic("LYRIC_ACTIVEMQ_URL: " + e)
  }
}

func unique(prefix: in String): String {
  prefix + "." + Uuid.uuidToString(Uuid.newUuid())
}

func optionsFor(destination: in String): ActiveMq.ConnectOptions {
  val address = brokerAddress()
  val base = ActiveMq.defaultOptions(address.host, address.port, destination)
  base.copy(login = address.login, passcode = address.passcode)
}

func open(options: in ActiveMq.ConnectOptions): ActiveMq.ActiveMqQueue {
  match ActiveMq.connectWith(options) {
    case Ok(q) -> q
    case Err(e) -> panic("connect failed: " + e)
  }
}

func openQueue(): ActiveMq.ActiveMqQueue {
  open(optionsFor(Extras.queueDestination(unique("lyric.it"))))
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

func mustPublishWith(q: in ActiveMq.ActiveMqQueue, m: in Mq.Message, options: in Extras.SendOptions): Unit {
  match Extras.publishWith(q, m, options) {
    case Ok(_) -> ()
    case Err(e) -> panic("publishWith failed: " + e)
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
  match q.consume(5000, { m -> keep(got, m) }) {
    case Ok(_) -> got.messages[0]
    case Err(e) -> panic("consume failed: " + e)
  }
}

func expectNothing(q: in ActiveMq.ActiveMqQueue, waitMs: in Int, label: in String): Unit {
  val got = Got(messages = newList())
  match q.consume(waitMs, { m -> keep(got, m) }) {
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
ref record Opened {
  var queues: List[ActiveMq.ActiveMqQueue]
}

val opened: Opened = Opened(queues = newList())

func makeEnds(): MqConformance.QueueEnds {
  val q = openQueue()
  opened.queues.add(q)
  val publisher: Mq.MessageQueue = q
  val consumer: Mq.QueueConsumer = q
  MqConformance.QueueEnds(publisher = publisher, consumer = consumer)
}

test "ActiveMQ conforms to MessageQueue and QueueConsumer" {
  val checks = MqConformance.runQueue({ -> makeEnds() })
  for q in opened.queues.toArray() {
    q.close()
  }
  requireConformance(checks)
}

// ─── Connecting ──────────────────────────────────────────────────────────────
test "the broker identifies itself as ActiveMQ" {
  val q = openQueue()
  assertTrue(Str.startsWith(ActiveMq.serverInfo(q), "ActiveMQ/"), "server header: " + ActiveMq.serverInfo(q))
  q.close()
}

test "connectUrl reaches the broker named by the URL" {
  val name = unique("lyric.it.url")
  match ActiveMq.connectUrl(brokerUrl(), name) {
    case Err(e) -> assertTrue(false, e)
    case Ok(q) -> {
      assertEqual(ActiveMq.destination(q), "/queue/" + name, "destination")
      mustPublish(q, message("u-1", "via url"))
      val got = mustTake(q)
      assertEqual(got.body, "via url", "round trip")
      assertTrue(isOk(q.ack(got.id)), "ack")
      q.close()
    }
  }
}

test "a refused connection is an Err" {
  val server = match K.serverListen("127.0.0.1", 0) { case Ok(s) -> s; case Err(e) -> panic(e) }
  val port = K.serverPort(server)
  K.serverClose(server)
  match ActiveMq.connectWith(ActiveMq.defaultOptions("127.0.0.1", port, "/queue/a")) {
    case Ok(_) -> assertTrue(false, "nothing listens there")
    case Err(e) -> expectContains(e, "activemq: cannot connect to 127.0.0.1:" + port.toString(), "refused")
  }
}

test "bad credentials are refused by the broker" {
  val options = optionsFor(Extras.queueDestination(unique("lyric.it")))
  match ActiveMq.connectWith(options.copy(passcode = "definitely-not-the-passcode")) {
    case Ok(q) -> {
      q.close()
      assertTrue(false, "the broker accepted a wrong passcode; it must enforce authentication for this suite")
    }
    case Err(e) -> expectLoginRefused(e, "plain TCP")
  }
}

// ActiveMQ answers a wrong passcode with an ERROR frame and closes the
// connection at once. Now and then the close overtakes the frame (a bare
// close on plain TCP, a failed read on TLS), so the refusal is either the
// broker's error or a connection lost during login; never anything else, and
// never with the passcode in it.
func expectLoginRefused(e: in String, label: in String): Unit {
  val refused = Str.startsWith(e, "activemq: broker error:")
  val closed = Str.startsWith(e, "activemq: connection lost:") and Str.contains(e, "ended the connection during login")
  assertTrue(refused or closed, label + ": a refused login, got \"" + e + "\"")
  assertTrue(not Str.contains(e, "definitely-not-the-passcode"), label + ": the passcode is not echoed")
}

test "a wrong passcode is always refused, as the broker's error or as a close during login" {
  val plain = optionsFor(Extras.queueDestination(unique("lyric.it")))
  val secured = tlsOptionsFor(tlsQueueName())
  var n = 0
  var errors = 0
  while n < 150 {
    match ActiveMq.connectWith(plain.copy(passcode = "definitely-not-the-passcode")) {
      case Ok(q) -> {
        q.close()
        assertTrue(false, "the broker accepted a wrong passcode")
      }
      case Err(e) -> {
        expectLoginRefused(e, "plain TCP")
        if Str.startsWith(e, "activemq: broker error:") {
          errors += 1
        }
      }
    }
    match ActiveMq.connectWith(secured.copy(passcode = "definitely-not-the-passcode")) {
      case Ok(q) -> {
        q.close()
        assertTrue(false, "the broker accepted a wrong passcode over TLS")
      }
      case Err(e) -> {
        expectLoginRefused(e, "TLS")
        if Str.startsWith(e, "activemq: broker error:") {
          errors += 1
        }
      }
    }
    n += 1
  }
  assertTrue(errors > 150, "the ERROR frame is what usually arrives: " + errors.toString() + " of 300")
}

test "a broken selector is always reported as the broker's error, and the connection survives" {
  var n = 0
  while n < 40 {
    val options = if n % 2 == 0 {
      optionsFor(Extras.queueDestination(unique("lyric.it")))
    } else {
      tlsOptionsFor(tlsQueueName())
    }
    val q = open(options.copy(selector = "color = = 'red' ("))
    val e = errorOf(q.consume(1000, { m -> Ok(()) }), "subscribe with a broken selector")
    expectContains(e, "activemq: broker error:", "an ERROR frame carrying the SUBSCRIBE's receipt-id")
    assertTrue(ActiveMq.isOpen(q), "a refused SUBSCRIBE leaves the connection usable")
    mustPublish(q, message("s-" + n.toString(), "still publishing"))
    q.close()
    n += 1
  }
}

test "a broker ERROR frame is reported, here for an invalid selector" {
  val options = optionsFor(Extras.queueDestination(unique("lyric.it")))
  val q = open(options.copy(selector = "color = = 'red' ("))
  val e = errorOf(q.consume(1000, { m -> Ok(()) }), "subscribe with a broken selector")
  expectContains(e, "activemq: broker error:", "an ERROR frame")
  q.close()
}

test "a second ack of one delivery is refused and the connection survives" {
  val q = openQueue()
  mustPublish(q, message("m", "x"))
  val got = mustTake(q)
  assertTrue(isOk(q.ack(got.id)), "ack")
  assertTrue(isErr(q.ack(got.id)), "the client refuses a second ack itself")
  mustPublish(q, message("n", "y"))
  assertEqual(mustTake(q).id, "n", "still usable")
  assertTrue(isOk(q.ack("n")), "ack")
  q.close()
}

// ─── Redelivery and dead-lettering ───────────────────────────────────────────
test "a requeue keeps the body and headers and counts every round" {
  val q = openQueue()
  val body = "payload \u{0000} with NUL, \"quotes\", café 世\nline2"
  val sent = Mq.Message(
    id = "r:1 é",
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
  while round < 4 {
    val got = mustTake(q)
    assertEqual(got.id, "r:1 é", "id in round " + round.toString())
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
  assertEqualInt(last.deliveryCount, 4, "after four requeues")
  assertTrue(isOk(q.ack(last.id)), "ack")
  expectNothing(q, 300, "nothing is left")
  q.close()
}

test "a requeued message goes behind the messages already waiting" {
  val options = optionsFor(Extras.queueDestination(unique("lyric.it")))
  val q = open(options.copy(prefetchSize = 1))
  mustPublish(q, message("first", "1"))
  mustPublish(q, message("second", "2"))
  val a = mustTake(q)
  assertEqual(a.id, "first", "first delivery")
  assertTrue(isOk(q.nack(a.id, true)), "requeue the first")
  val b = mustTake(q)
  assertEqual(b.id, "second", "the waiting message comes next")
  assertTrue(isOk(q.ack(b.id)), "ack")
  val c = mustTake(q)
  assertEqual(c.id, "first", "then the requeued one")
  assertEqualInt(c.deliveryCount, 1, "with its count raised")
  assertTrue(isOk(q.ack(c.id)), "ack")
  q.close()
}

test "deliveries left unsettled at close are redelivered to the next consumer" {
  val destination = Extras.queueDestination(unique("lyric.it"))
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
  assertEqualInt(b.deliveryCount, 1, "also flagged")
  assertTrue(isOk(second.ack(a.id)), "ack")
  assertTrue(isOk(second.ack(b.id)), "ack")
  expectNothing(second, 300, "nothing is left")
  second.close()
}

func deadLetterConsumer(messageId: in String): ActiveMq.ActiveMqQueue {
  val options = optionsFor("/queue/ActiveMQ.DLQ")
  open(options.copy(selector = Extras.selectorForId(messageId)))
}

test "a rejected persistent message moves to the broker's dead-letter queue" {
  val q = openQueue()
  val id = unique("dlq")
  mustPublish(
    q,
    Mq.Message(id = id, body = "poison", headers = [Mq.MessageHeader(key = "why", value = "test")], deliveryCount = 0)
  )
  val got = mustTake(q)
  assertTrue(isOk(q.nack(got.id, false)), "reject")
  expectNothing(q, 300, "a rejected message is not redelivered")
  q.close()
  val dlq = deadLetterConsumer(id)
  val dead = mustTake(dlq)
  assertEqual(dead.id, id, "the dead letter keeps its id")
  assertEqual(dead.body, "poison", "and its body")
  assertEqualInt(dead.headers.length, 1, "and its headers")
  assertTrue(isOk(dlq.ack(dead.id)), "remove it from the dead-letter queue")
  dlq.close()
}

test "a rejected non-persistent message is discarded" {
  val q = openQueue()
  val id = unique("dlq.np")
  val options = Extras.defaultSendOptions()
  mustPublishWith(q, message(id, "transient"), options.withPersistent(false))
  val got = mustTake(q)
  assertTrue(isOk(q.nack(got.id, false)), "reject")
  expectNothing(q, 300, "not redelivered")
  q.close()
  val dlq = deadLetterConsumer(id)
  expectNothing(dlq, 700, "not dead-lettered either")
  dlq.close()
}

// ─── Batches, Auto mode, large and foreign messages ──────────────────────────
test "publishBatch delivers the whole batch in order" {
  val q = openQueue()
  val batch: List[Mq.Message] = newList()
  var n = 0
  while n < 25 {
    batch.add(message("b-" + n.toString(), n.toString()))
    n += 1
  }
  match q.publishBatch(batch.toArray()) {
    case Ok(_) -> ()
    case Err(e) -> assertTrue(false, e)
  }
  n = 0
  while n < 25 {
    val got = mustTake(q)
    assertEqual(got.id, "b-" + n.toString(), "batch order")
    assertTrue(isOk(q.ack(got.id)), "ack")
    n += 1
  }
  expectNothing(q, 300, "exactly the batch")
  q.close()
}

test "Auto mode acknowledges on Ok and requeues on Err" {
  val options = optionsFor(Extras.queueDestination(unique("lyric.it")))
  val q = open(options.copy(ackMode = Mq.AckMode.Auto))
  mustPublish(q, message("auto", "x"))
  val refused = errorOf(q.consume(5000, { m -> Err("not now") }), "a failing handler")
  assertEqual(refused, "not now", "the handler's error is returned")
  val again = mustTake(q)
  assertEqual(again.id, "auto", "redelivered")
  assertEqualInt(again.deliveryCount, 1, "with a higher count")
  assertEqualInt(ActiveMq.unsettledCount(q), 0, "acknowledged by consume")
  q.close()
  val check = open(options)
  expectNothing(check, 500, "the acknowledged message is gone for good")
  check.close()
}

test "a 300 KiB body round-trips" {
  val parts: List[String] = newList()
  var n = 0
  while n < 30000 {
    parts.add("0123456789")
    n += 1
  }
  parts.add("é世 end")
  val body = Str.joinList("", parts)
  val q = openQueue()
  mustPublish(q, message("large", body))
  val got = mustTake(q)
  assertEqualInt(got.body.length, body.length, "length")
  assertTrue(got.body == body, "content")
  assertTrue(isOk(q.ack(got.id)), "ack")
  q.close()
}

// A producer that is not this client: raw STOMP frames on a socket.
func rawSend(destination: in String, headers: in slice[Stomp.Header], body: in slice[Byte]): Unit {
  val address = brokerAddress()
  val sock = match K.sockConnect(address.host, address.port, 5000) { case Ok(s) -> s; case Err(e) -> panic(e) }
  val all: List[Stomp.Header] = newList()
  all.add(Stomp.Header(name = "destination", value = destination))
  all.add(Stomp.Header(name = "receipt", value = "raw-1"))
  for header in headers {
    all.add(header)
  }
  val frames = [
    Stomp.bareFrame(
      "CONNECT",
      [
        Stomp.Header(name = "accept-version", value = "1.2"),
        Stomp.Header(name = "host", value = address.host),
        Stomp.Header(name = "login", value = address.login),
        Stomp.Header(name = "passcode", value = address.passcode)
      ]
    ),
    Stomp.Frame(command = "SEND", headers = all.toArray(), body = body),
    Stomp.bareFrame("DISCONNECT", [Stomp.Header(name = "receipt", value = "raw-2")])
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
        case Ok(Some(answer)) -> {
          if answer.command == "ERROR" {
            panic("the broker refused a raw frame: " + Stomp.headerOr(answer, "message", ""))
          }
          answered = true
        }
        case Ok(None) -> match K.sockRead(sock, 4096, 5000) {
          case Ok(Data(bytes)) -> Stomp.feed(decoder, bytes)
          case _ -> panic("the broker did not answer a raw frame")
        }
        case Err(e) -> panic(e)
      }
    }
  }
  K.sockClose(sock)
}

test "a text message from another STOMP producer is delivered with its broker id and headers" {
  val destination = Extras.queueDestination(unique("lyric.it"))
  rawSend(destination, [Stomp.Header(name = "trace", value = "abc")], encodeUtf8("from elsewhere"))
  val q = open(optionsFor(destination))
  val got = mustTake(q)
  assertTrue(Str.startsWith(got.id, "ID:"), "the id is the broker's message-id, got " + got.id)
  assertEqual(got.body, "from elsewhere", "body")
  assertEqualInt(got.headers.length, 1, "application headers only")
  assertEqual(got.headers[0].key, "trace", "header key")
  assertEqual(got.headers[0].value, "abc", "header value")
  assertEqualInt(got.deliveryCount, 0, "first delivery")
  assertTrue(isOk(q.nack(got.id, true)), "a foreign message can be requeued")
  val again = mustTake(q)
  assertEqual(again.body, "from elsewhere", "requeued body")
  assertEqualInt(again.deliveryCount, 1, "requeued count")
  assertTrue(isOk(q.ack(again.id)), "ack")
  q.close()
}

func rejecting(options: in ActiveMq.ConnectOptions): ActiveMq.ConnectOptions {
  options.copy(onUndeliverable = ActiveMq.UndeliverablePolicy.RejectToBroker)
}

// The broker id an "activemq: message ID cannot be delivered: ..." error names.
func brokerIdIn(e: in String): String {
  val stop = Str.indexOfRaw(e, " cannot be delivered")
  if stop < 18 {
    panic("not an undeliverable-message error: " + e)
  }
  Str.substring(e, 18, stop - 18)
}

test "a binary message is left on the queue by default and does not block the connection" {
  val destination = Extras.queueDestination(unique("lyric.it"))
  val binary: slice[Byte] = [255u8, 254u8, 0u8, 1u8]
  rawSend(destination, [Stomp.Header(name = "persistent", value = "true")], binary)
  val q = open(optionsFor(destination))
  val e = errorOf(q.consume(5000, { m -> Ok(()) }), "consume a binary message")
  expectContains(e, "cannot be delivered: its body is not UTF-8 text", "reason")
  expectContains(e, "left unacknowledged on the broker", "outcome")
  assertEqualInt(ActiveMq.unsettledCount(q), 1, "tracked under its broker id")
  mustPublish(q, message("next", "fine"))
  val got = mustTake(q)
  assertEqual(got.id, "next", "the connection goes on delivering")
  assertTrue(isOk(q.ack(got.id)), "ack")
  q.close()

  // Still there for another consumer; this one settles it by its broker id.
  val second = open(optionsFor(destination))
  val again = errorOf(second.consume(5000, { m -> Ok(()) }), "the binary message again")
  expectContains(again, "its body is not UTF-8 text", "the message was not destroyed")
  match second.ack(brokerIdIn(again)) {
    case Ok(_) -> ()
    case Err(problem) -> assertTrue(false, "ack by broker id: " + problem)
  }
  assertEqualInt(ActiveMq.unsettledCount(second), 0, "settled")
  second.close()
  val third = open(optionsFor(destination))
  expectNothing(third, 500, "acknowledged for good")
  third.close()
}

test "with RejectToBroker a binary message is rejected to the dead-letter queue" {
  val destination = Extras.queueDestination(unique("lyric.it"))
  val binary: slice[Byte] = [255u8, 254u8, 0u8, 1u8]
  rawSend(destination, [Stomp.Header(name = "persistent", value = "true")], binary)
  val q = open(rejecting(optionsFor(destination)))
  val e = errorOf(q.consume(5000, { m -> Ok(()) }), "consume a binary message")
  expectContains(e, "cannot be delivered: its body is not UTF-8 text", "reason")
  expectContains(e, "rejected to the broker's dead-letter handling", "outcome")
  assertEqualInt(ActiveMq.unsettledCount(q), 0, "not in flight")
  mustPublish(q, message("next", "fine"))
  val got = mustTake(q)
  assertEqual(got.id, "next", "the queue is not blocked")
  assertTrue(isOk(q.ack(got.id)), "ack")
  q.close()
  val check = open(optionsFor(destination))
  expectNothing(check, 500, "the rejected message left the queue")
  check.close()
}

func bodyOf(size: in Int): String {
  var text = "0123456789"
  while text.length < size {
    text = text + text
  }
  Str.substring(text, 0, size)
}

test "a body over maxFrameBytes does not break the connection, and stays on the queue by default" {
  val destination = Extras.queueDestination(unique("lyric.it"))
  val id = unique("big")
  val publisher = open(optionsFor(destination))
  mustPublish(publisher, message(id, bodyOf(200000)))
  mustPublish(publisher, message("small", "fits"))
  val base = optionsFor(destination)
  val limited = base.copy(maxFrameBytes = 65536)
  val q = open(limited)
  val e = errorOf(q.consume(5000, { m -> Ok(()) }), "an oversize body")
  expectContains(e, "its body of 200000 bytes exceeds the limit of 65536 bytes", "the reason")
  expectContains(e, "left unacknowledged on the broker", "the default policy")
  assertTrue(ActiveMq.isOpen(q), "the connection is still usable")
  val next = mustTake(q)
  assertEqual(next.id, "small", "the next message arrives on the same connection")
  assertTrue(isOk(q.ack(next.id)), "ack")
  expectContains(
    errorOf(q.nack(brokerIdIn(e), true), "requeue of a message that was not read"),
    "nothing to requeue",
    "requeue"
  )
  q.close()

  // Not lost: a consumer with room for it reads it, flagged as redelivered.
  val roomy = open(base)
  val whole = mustTake(roomy)
  assertEqual(whole.id, id, "the large message is still on the queue")
  assertEqualInt(whole.body.length, 200000, "intact")
  assertEqualInt(whole.deliveryCount, 1, "redelivered")
  assertTrue(isOk(roomy.nack(whole.id, true)), "put it back")
  roomy.close()

  // RejectToBroker dead-letters it, and the connection survives that too.
  val strict = open(rejecting(limited))
  val rejected = errorOf(strict.consume(5000, { m -> Ok(()) }), "the oversize body, rejecting")
  expectContains(rejected, "exceeds the limit of 65536 bytes", "the reason")
  expectContains(rejected, "rejected to the broker's dead-letter handling", "the outcome")
  assertTrue(ActiveMq.isOpen(strict), "the connection is still usable")
  mustPublish(strict, message("after", "x"))
  assertEqual(mustTake(strict).id, "after", "and delivers what follows")
  assertTrue(isOk(strict.ack("after")), "ack")
  strict.close()
  val dlq = deadLetterConsumer(id)
  val dead = mustTake(dlq)
  assertEqual(dead.id, id, "the persistent message is in the dead-letter queue")
  assertTrue(isOk(dlq.ack(dead.id)), "remove it")
  dlq.close()
  publisher.close()
}

test "a header section over 64 KiB does not break the connection" {
  val destination = Extras.queueDestination(unique("lyric.it"))
  val publisher = open(optionsFor(destination))
  val wide: List[Stomp.Header] = newList()
  var n = 0
  while n < 9 {
    wide.add(Stomp.Header(name = "pad" + n.toString(), value = bodyOf(9000)))
    n += 1
  }
  match ActiveMq.publishWithHeaders(publisher, message("wide", "x"), wide.toArray()) {
    case Ok(_) -> ()
    case Err(e) -> assertTrue(false, "publishing 81 KiB of headers: " + e)
  }
  mustPublish(publisher, message("small", "fits"))
  val q = open(rejecting(optionsFor(destination)))
  val e = errorOf(q.consume(5000, { m -> Ok(()) }), "oversize headers")
  expectContains(e, "its headers exceed 65536 bytes", "the reason")
  expectContains(e, "rejected to the broker's dead-letter handling", "the ack id was read from beyond the limit")
  assertTrue(ActiveMq.isOpen(q), "the connection is still usable")
  val next = mustTake(q)
  assertEqual(next.id, "small", "the next message arrives on the same connection")
  assertTrue(isOk(q.ack(next.id)), "ack")
  q.close()
  val dlq = open(optionsFor("/queue/ActiveMQ.DLQ").copy(selector = "lyric_id = 'wide' AND pad0 IS NOT NULL"))
  val dead = errorOf(dlq.consume(5000, { m -> Ok(()) }), "the dead letter is as wide")
  expectContains(dead, "its headers exceed 65536 bytes", "it reached the dead-letter queue")
  match dlq.ack(brokerIdIn(dead)) {
    case Ok(_) -> ()
    case Err(problem) -> assertTrue(false, "remove the dead letter: " + problem)
  }
  dlq.close()
  publisher.close()
}

test "a negative deliveryCount is refused on publish" {
  val q = openQueue()
  val negative = Mq.Message(id = "n", body = "x", headers = [], deliveryCount = -1)
  expectContains(errorOf(q.publish(negative), "publish"), "negative deliveryCount", "publish")
  expectContains(errorOf(q.publishBatch([message("ok", "x"), negative]), "batch"), "negative deliveryCount", "batch")
  expectNothing(q, 300, "nothing was published")
  q.close()
}

test "publishWith with default options keeps the connection's persistence" {
  val destination = Extras.queueDestination(unique("lyric.it"))
  val base = optionsFor(destination)
  val transient = open(base.copy(persistent = false))
  mustPublishWith(transient, message("inherits", "1"), Extras.defaultSendOptions())
  mustPublishWith(transient, message("raised", "2"), Extras.defaultSendOptions().withPersistent(true))
  assertEqual(
    takeSelected(destination, "JMSDeliveryMode = 'NON_PERSISTENT'", "the inherited mode"),
    "inherits",
    "default send options on a non-persistent connection"
  )
  assertEqual(
    takeSelected(destination, "JMSDeliveryMode = 'PERSISTENT'", "the explicit mode"),
    "raised",
    "an explicit option wins"
  )
  transient.close()
}

test "the first consume(0) returns at once and the subscription completes afterwards" {
  val q = openQueue()
  mustPublish(q, message("first", "x"))
  val started = K.monotonicMillis()
  val polled = q.consume(0, { m -> keepNothing(m) })
  val elapsed = K.monotonicMillis() - started
  assertTrue(elapsed < 3000i64, "consume(0) returned after " + elapsed.toString() + " ms")
  match polled {
    case Ok(_) -> assertTrue(isOk(q.ack("first")), "ack")
    case Err(e) -> {
      assertEqual(e, "timeout", "a first poll may come before the broker's dispatch")
      val got = mustTake(q)
      assertEqual(got.id, "first", "the message arrives once the subscription is confirmed")
      assertTrue(isOk(q.ack(got.id)), "ack")
    }
  }
  q.close()
}

func keepNothing(m: in Mq.Message): Result[Unit, String] {
  Ok(())
}

// ─── Extras: delivery options, selectors, topics ─────────────────────────────
test "a selector on a property delivers only the matching messages" {
  val destination = Extras.queueDestination(unique("lyric.it"))
  val publisher = open(optionsFor(destination))
  val base = Extras.defaultSendOptions()
  mustPublishWith(
    publisher,
    message("blue-1", "b"),
    base.copy(properties = [Extras.Property(name = "color", value = "blue")])
  )
  mustPublishWith(
    publisher,
    message("red-1", "r"),
    base.copy(properties = [Extras.Property(name = "color", value = "red")])
  )
  mustPublishWith(
    publisher,
    message("red-2", "r'"),
    base.copy(properties = [Extras.Property(name = "color", value = "red")])
  )
  val options = optionsFor(destination)
  val reds = open(options.copy(selector = Extras.selectorEquals("color", "red")))
  val first = mustTake(reds)
  assertEqual(first.id, "red-1", "first red")
  val second = mustTake(reds)
  assertEqual(second.id, "red-2", "second red")
  assertTrue(isOk(reds.ack(first.id)), "ack")
  assertTrue(isOk(reds.ack(second.id)), "ack")
  expectNothing(reds, 300, "blue is not selected")
  reds.close()
  val rest = open(options)
  val blue = mustTake(rest)
  assertEqual(blue.id, "blue-1", "the unselected message is still queued")
  assertTrue(isOk(rest.ack(blue.id)), "ack")
  rest.close()
  publisher.close()
}

func takeSelected(destination: in String, selector: in String, label: in String): String {
  val options = optionsFor(destination)
  val q = open(options.copy(selector = selector))
  val got = mustTake(q)
  assertTrue(isOk(q.ack(got.id)), "ack " + label)
  expectNothing(q, 200, "only one message matches " + label)
  q.close()
  got.id
}

test "priority, correlation id, type and delivery mode reach the broker" {
  val destination = Extras.queueDestination(unique("lyric.it"))
  val publisher = open(optionsFor(destination))
  val base = Extras.defaultSendOptions()
  mustPublishWith(publisher, message("urgent", "1"), base.copy(priority = 9))
  mustPublishWith(publisher, message("correlated", "2"), base.copy(correlationId = "corr-42"))
  mustPublishWith(publisher, message("typed", "3"), base.copy(jmsType = "invoice"))
  mustPublishWith(publisher, message("transient", "4"), base.withPersistent(false))
  assertEqual(takeSelected(destination, "JMSPriority = 9", "priority"), "urgent", "JMSPriority")
  assertEqual(
    takeSelected(destination, "JMSCorrelationID = 'corr-42'", "correlation"),
    "correlated",
    "JMSCorrelationID"
  )
  assertEqual(takeSelected(destination, "JMSType = 'invoice'", "type"), "typed", "JMSType")
  assertEqual(
    takeSelected(destination, "JMSDeliveryMode = 'NON_PERSISTENT'", "delivery mode"),
    "transient",
    "JMSDeliveryMode"
  )
  publisher.close()
}

test "a message past its time to live is not delivered" {
  val q = openQueue()
  val base = Extras.defaultSendOptions()
  mustPublishWith(q, message("short-lived", "x"), base.withPersistent(false).copy(timeToLiveMs = 200i64))
  mustPublishWith(q, message("long-lived", "y"), base.copy(timeToLiveMs = 60000i64))
  K.sleepMs(700)
  val got = mustTake(q)
  assertEqual(got.id, "long-lived", "only the unexpired message arrives")
  assertTrue(isOk(q.ack(got.id)), "ack")
  expectNothing(q, 300, "the expired one never arrives")
  q.close()
}

test "invalid send options are refused before anything is sent" {
  val q = openQueue()
  val base = Extras.defaultSendOptions()
  expectContains(
    errorOf(Extras.publishWith(q, message("m", "x"), base.copy(priority = 10)), "priority"),
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
  expectNothing(q, 300, "nothing was published")
  q.close()
}

test "a topic delivers a copy to every subscriber and nothing to a late one" {
  val destination = Extras.topicDestination(unique("lyric.it.topic"))
  val one = open(optionsFor(destination))
  val two = open(optionsFor(destination))
  assertTrue(isOk(ActiveMq.subscribe(one)), "first subscription")
  assertTrue(isOk(ActiveMq.subscribe(two)), "second subscription")
  val publisher = open(optionsFor(destination))
  mustPublish(publisher, message("tick", "42"))
  val a = mustTake(one)
  val b = mustTake(two)
  assertEqual(a.id, "tick", "first subscriber")
  assertEqual(b.id, "tick", "second subscriber")
  assertEqual(b.body, "42", "body")
  assertTrue(isOk(one.ack(a.id)), "ack")
  assertTrue(isOk(two.ack(b.id)), "ack")
  val late = open(optionsFor(destination))
  expectNothing(late, 300, "a subscription opened after the publish gets nothing")
  late.close()
  publisher.close()
  one.close()
  two.close()
}

// ─── Several consumers ───────────────────────────────────────────────────────
func drain(q: in ActiveMq.ActiveMqQueue, got: in Got): Unit {
  var idle = 0
  while idle < 3 {
    val before = got.messages.count
    match q.consume(300, { m -> keep(got, m) }) {
      case Ok(_) -> {
        val _ = q.ack(got.messages[before].id)
        idle = 0
      }
      case Err(_) -> {
        idle += 1
      }
    }
  }
}

test "two consumers on their own threads share a queue without loss or duplication" {
  val destination = Extras.queueDestination(unique("lyric.it"))
  val publisher = open(optionsFor(destination))
  val options = optionsFor(destination)
  val left = open(options.copy(prefetchSize = 1))
  val right = open(options.copy(prefetchSize = 1))
  assertTrue(isOk(ActiveMq.subscribe(left)), "left subscription")
  assertTrue(isOk(ActiveMq.subscribe(right)), "right subscription")
  val count = 60
  var n = 0
  while n < count {
    mustPublish(publisher, message("w-" + n.toString(), n.toString()))
    n += 1
  }
  val leftGot = Got(messages = newList())
  val rightGot = Got(messages = newList())
  val leftWork = K.startBackground({ -> drain(left, leftGot) })
  val rightWork = K.startBackground({ -> drain(right, rightGot) })
  K.waitBackground(leftWork)
  K.waitBackground(rightWork)
  val seen: Map[String, Bool] = newMap()
  for m in leftGot.messages.toArray() {
    seen[m.id] = true
  }
  for m in rightGot.messages.toArray() {
    seen[m.id] = true
  }
  assertEqualInt(leftGot.messages.count + rightGot.messages.count, count, "every message was delivered once")
  assertEqualInt(seen.count, count, "and no id twice")
  assertTrue(leftGot.messages.count > 0 and rightGot.messages.count > 0, "both consumers took part")
  left.close()
  right.close()
  publisher.close()
}

// ─── TLS ─────────────────────────────────────────────────────────────────────
func requiredEnv(name: in String, what: in String): String {
  val value = Env.getVarOrDefault(name, "")
  if value.length == 0 {
    panic(
      name + " is not set; the TLS integration tests need " + what +
        " (scripts/ci/start-activemq.sh starts a broker and names it)"
    )
  }
  value
}

func tlsAddressFrom(name: in String, what: in String): Url.BrokerAddress {
  match Url.parseUrl(requiredEnv(name, what)) {
    case Ok(a) -> {
      if not a.tls {
        panic(name + " must be a stomp+ssl:// URL")
      }
      a
    }
    case Err(e) -> panic(name + ": " + e)
  }
}

func tlsAddress(): Url.BrokerAddress {
  tlsAddressFrom("LYRIC_ACTIVEMQ_TLS_URL", "the URL of a stomp+ssl connector")
}

func mutualTlsAddress(): Url.BrokerAddress {
  tlsAddressFrom("LYRIC_ACTIVEMQ_MTLS_URL", "the URL of a stomp+ssl connector with needClientAuth=true")
}

func pemFrom(name: in String, what: in String): String {
  val path = requiredEnv(name, what)
  match File.readText(path) {
    case Ok(text) -> text
    case Err(_) -> panic(name + ": cannot read " + path)
  }
}

func caPem(): String {
  pemFrom("LYRIC_ACTIVEMQ_TLS_CA", "the PEM file of the CA that issued the broker's certificate")
}

func clientCertPem(): String {
  pemFrom("LYRIC_ACTIVEMQ_TLS_CERT", "the PEM file of a client certificate the broker trusts")
}

func clientKeyPem(): String {
  pemFrom("LYRIC_ACTIVEMQ_TLS_KEY", "the PEM file of the client certificate's PKCS8 private key")
}

// TLS to `address` with no trust configured: the platform trust store.
func bareTlsOptions(address: in Url.BrokerAddress, destination: in String): ActiveMq.ConnectOptions {
  val base = ActiveMq.defaultOptions(address.host, address.port, destination)
  base.copy(login = address.login, passcode = address.passcode).withTls()
}

func tlsOptionsFor(destination: in String): ActiveMq.ConnectOptions {
  bareTlsOptions(tlsAddress(), destination).withExclusiveCaCertificate(caPem())
}

func mutualTlsOptionsFor(destination: in String): ActiveMq.ConnectOptions {
  bareTlsOptions(mutualTlsAddress(), destination).withExclusiveCaCertificate(caPem())
}

func tlsQueueName(): String {
  Extras.queueDestination(unique("lyric.it.tls"))
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

func expectHandshakeFailure(options: in ActiveMq.ConnectOptions, label: in String): Unit {
  val e = errorOfConnect(options)
  expectContains(e, "activemq: TLS handshake with ", label)
  assertTrue(not Str.contains(e, options.passcode), label + ": the passcode is not echoed")
}

func expectRoundTrip(options: in ActiveMq.ConnectOptions, label: in String): Unit {
  match ActiveMq.connectWith(options) {
    case Err(e) -> assertTrue(false, label + ": " + e)
    case Ok(q) -> {
      assertTrue(Str.startsWith(ActiveMq.serverInfo(q), "ActiveMQ/"), label + ": server header")
      mustPublish(q, message("tls-1", label))
      val got = mustTake(q)
      assertEqual(got.body, label, label)
      assertTrue(isOk(q.ack(got.id)), label + ": ack")
      q.close()
    }
  }
}

func makeTlsEnds(): MqConformance.QueueEnds {
  val q = open(tlsOptionsFor(tlsQueueName()))
  opened.queues.add(q)
  val publisher: Mq.MessageQueue = q
  val consumer: Mq.QueueConsumer = q
  MqConformance.QueueEnds(publisher = publisher, consumer = consumer)
}

test "TLS: ActiveMQ conforms to MessageQueue and QueueConsumer over stomp+ssl" {
  opened.queues = newList()
  val checks = MqConformance.runQueue({ -> makeTlsEnds() })
  for q in opened.queues.toArray() {
    q.close()
  }
  requireConformance(checks)
}

test "TLS: the broker is verified against the custom CA, exclusive or additional" {
  expectRoundTrip(tlsOptionsFor(tlsQueueName()), "exclusive CA")
  expectRoundTrip(bareTlsOptions(tlsAddress(), tlsQueueName()).withCaCertificate(caPem()), "additional CA")
  expectRoundTrip(tlsOptionsFor(tlsQueueName()).withMinTlsVersion(Tls.TlsVersion.Tls13), "TLS 1.3 only")
}

test "TLS: without the CA the broker's certificate is refused" {
  expectHandshakeFailure(bareTlsOptions(tlsAddress(), tlsQueueName()), "platform trust store only")
  match ActiveMq.connectUrl(requiredEnv("LYRIC_ACTIVEMQ_TLS_URL", "the URL of a stomp+ssl connector"), "lyric.it.tls.url") {
    case Ok(q) -> {
      q.close()
      assertTrue(false, "connectUrl verified a certificate from a CA nobody configured")
    }
    case Err(e) -> expectContains(e, "activemq: TLS handshake with ", "connectUrl with the platform trust store")
  }
}

test "TLS: a host name the certificate does not carry is refused" {
  val options = tlsOptionsFor(tlsQueueName())
  expectHandshakeFailure(options.withTlsServerName("broker.invalid"), "a name the certificate lacks")
  // The script's certificate names both localhost and 127.0.0.1.
  expectRoundTrip(options.withTlsServerName("localhost"), "a server name the certificate carries")
}

test "TLS: skip-verify needs the option and LYRIC_TLS_ALLOW_INSECURE together" {
  val insecure = bareTlsOptions(tlsAddress(), tlsQueueName()).withTlsServerName("broker.invalid").withInsecureSkipVerify()
  Env.setVar("LYRIC_TLS_ALLOW_INSECURE", "")
  expectHandshakeFailure(insecure, "the option without the environment variable")
  Env.setVar("LYRIC_TLS_ALLOW_INSECURE", "1")
  expectHandshakeFailure(
    bareTlsOptions(tlsAddress(), tlsQueueName()).withTlsServerName("broker.invalid"),
    "the environment variable without the option"
  )
  expectRoundTrip(insecure, "both keys")
  Env.setVar("LYRIC_TLS_ALLOW_INSECURE", "")
}

test "mutual TLS: a client certificate from the broker's CA is accepted" {
  val options = mutualTlsOptionsFor(tlsQueueName())
  expectRoundTrip(options.withClientIdentity(clientCertPem(), clientKeyPem()), "mutual TLS")
  // The same identity is harmless on the connector that asks for none.
  expectRoundTrip(
    tlsOptionsFor(tlsQueueName()).withClientIdentity(clientCertPem(), clientKeyPem()),
    "an unrequested identity"
  )
}

test "mutual TLS: a connection without a client certificate is refused" {
  // With TLS 1.3 the client's own handshake finishes before the broker
  // rejects it, so the failure is a handshake error or a lost connection.
  val e = errorOfConnect(mutualTlsOptionsFor(tlsQueueName()))
  assertTrue(e != "connected", "the broker accepted a client without a certificate")
  assertTrue(
    Str.contains(e, "activemq: TLS handshake with ") or Str.contains(e, "activemq: connection lost") or
      Str.contains(e, "did not answer CONNECT"),
    "no client certificate: got \"" + e + "\""
  )
}

test "TLS: consume timeouts, receipts, transactions and broker errors behave as over plain TCP" {
  val q = open(tlsOptionsFor(tlsQueueName()))
  expectNothing(q, 300, "an empty queue times out")
  expectNothing(q, 0, "a poll of an empty queue times out")

  // Receipts: publish returns once the broker has the message.
  mustPublish(q, message("r-1", "first"))
  val first = mustTake(q)
  assertEqual(first.id, "r-1", "delivered")

  // Transactions: a requeue (BEGIN, SEND, ACK, COMMIT) and a batch.
  assertTrue(isOk(q.nack(first.id, true)), "requeue")
  val again = mustTake(q)
  assertEqual(again.body, "first", "the requeued body")
  assertEqualInt(again.deliveryCount, 1, "the requeue was counted")
  assertTrue(isOk(q.ack(again.id)), "ack")
  val batch: List[Mq.Message] = newList()
  var n = 0
  while n < 10 {
    batch.add(message("b-" + n.toString(), n.toString()))
    n += 1
  }
  assertTrue(isOk(q.publishBatch(batch.toArray())), "batch")
  n = 0
  while n < 10 {
    val got = mustTake(q)
    assertEqual(got.id, "b-" + n.toString(), "batch order")
    assertTrue(isOk(q.ack(got.id)), "ack")
    n += 1
  }

  // A body of many TLS records.
  var body = ""
  n = 0
  while n < 6000 {
    body = body + "0123456789abcdefghijklmnopqrstuvwxyz-" + n.toString() + ";"
    n += 1
  }
  mustPublish(q, message("big", body))
  val big = mustTake(q)
  assertEqual(big.body, body, "a large body")
  assertTrue(isOk(q.ack(big.id)), "ack")
  assertTrue(isErr(q.ack(big.id)), "a second ack is refused by the client")
  expectNothing(q, 300, "nothing is left")
  q.close()
  assertTrue(isErr(q.publish(message("late", "x"))), "a closed connection refuses a publish")

  // Error mapping: ERROR frames on CONNECT and on SUBSCRIBE.
  match ActiveMq.connectWith(tlsOptionsFor(tlsQueueName()).copy(passcode = "definitely-not-the-passcode")) {
    case Ok(bad) -> {
      bad.close()
      assertTrue(false, "the broker accepted a wrong passcode over TLS")
    }
    case Err(e) -> expectLoginRefused(e, "TLS")
  }
  val broken = open(tlsOptionsFor(tlsQueueName()).copy(selector = "color = = 'red' ("))
  val e = errorOf(broken.consume(1000, { m -> Ok(()) }), "subscribe with a broken selector")
  expectContains(e, "activemq: broker error:", "an ERROR frame on SUBSCRIBE")
  broken.close()
}

test "TLS: deliveries left unsettled at close are redelivered to the next consumer" {
  val options = tlsOptionsFor(tlsQueueName())
  val first = open(options)
  mustPublish(first, message("left", "x"))
  assertEqual(mustTake(first).id, "left", "delivered")
  first.close()
  val second = open(options)
  val got = mustTake(second)
  assertEqual(got.id, "left", "redelivered")
  assertEqualInt(got.deliveryCount, 1, "flagged redelivered")
  assertTrue(isOk(second.ack(got.id)), "ack")
  second.close()
}

test "TLS: connectFromEnv reads the CA, the client identity and the server name" {
  val prefix = "LYRIC_CONFIG_MQ_ACTIVEMQ_"
  val name = unique("lyric.it.tls.env")
  Env.setVar(prefix + "URL", requiredEnv("LYRIC_ACTIVEMQ_MTLS_URL", "the URL of a needClientAuth connector"))
  Env.setVar(prefix + "QUEUENAME", name)
  Env.setVar(prefix + "ACKMODE", "Auto")
  Env.setVar(prefix + "TLSCAFILE", requiredEnv("LYRIC_ACTIVEMQ_TLS_CA", "the CA file"))
  Env.setVar(prefix + "TLSCAEXCLUSIVE", "true")
  Env.setVar(prefix + "TLSCERTFILE", requiredEnv("LYRIC_ACTIVEMQ_TLS_CERT", "the client certificate file"))
  Env.setVar(prefix + "TLSKEYFILE", requiredEnv("LYRIC_ACTIVEMQ_TLS_KEY", "the client key file"))
  Env.setVar(prefix + "TLSSERVERNAME", "localhost")
  match ActiveMq.connectFromEnv() {
    case Err(e) -> assertTrue(false, e)
    case Ok(q) -> {
      assertEqual(ActiveMq.destination(q), "/queue/" + name, "destination")
      mustPublish(q, message("env", "from env over mutual TLS"))
      assertEqual(mustTake(q).body, "from env over mutual TLS", "round trip")
      q.close()
    }
  }
  Env.setVar(prefix + "TLSCAFILE", "")
  Env.setVar(prefix + "TLSCAEXCLUSIVE", "")
  match ActiveMq.connectFromEnv() {
    case Ok(q) -> {
      q.close()
      assertTrue(false, "without TLSCAFILE the broker's CA is unknown")
    }
    case Err(e) -> expectContains(e, "activemq: TLS handshake with ", "no CA file")
  }
  for field in ["URL", "QUEUENAME", "ACKMODE", "TLSCERTFILE", "TLSKEYFILE", "TLSSERVERNAME"] {
    Env.setVar(prefix + field, "")
  }
}
