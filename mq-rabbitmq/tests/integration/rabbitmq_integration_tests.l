//! tests/integration/rabbitmq_integration_tests.l - the backend against a
//! real RabbitMQ broker.
//!
//! Runs only with `--features integration`. The broker is named by
//! `LYRIC_RABBITMQ_URL` (`amqp://user:password@host:port/vhost`); every test
//! fails with a message saying so when the variable is unset. The user needs
//! configure, write and read permission on the virtual host. Each test uses
//! its own queues and exchanges, named with a fresh UUID, and deletes them.
//!
//! The TLS tests at the end need TLS listeners and the certificates behind
//! them, and fail the same way when a variable is unset:
//!
//!   LYRIC_RABBITMQ_TLS_URL    `amqps://user:password@host:port/vhost`, a
//!                             listener that does not require a client
//!                             certificate
//!   LYRIC_RABBITMQ_MTLS_URL   the same for a listener that requires one
//!                             (`fail_if_no_peer_cert = true`)
//!   LYRIC_RABBITMQ_TLS_CA     PEM file: the CA of the broker's certificate,
//!                             which must name the URLs' host
//!   LYRIC_RABBITMQ_TLS_CERT   PEM file: a client certificate the broker trusts
//!   LYRIC_RABBITMQ_TLS_KEY    PEM file: its PKCS8 private key
//!
//! `scripts/ci/start-rabbitmq.sh` starts such a broker and names all of them.
@cfg(feature = "integration")
@test_module
package Mq.RabbitMq.IntegrationTests

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
import Mq.RabbitMq as RabbitMq
import Mq.RabbitMq.Extras
import Mq.RabbitMq.Extras as Extras
import Net.Tcp as K
import Net.Tcp.Testing as KT
import Mq.RabbitMq.Amqp as Amqp
import Mq.RabbitMq.Url as Url

func brokerUrl(): String {
  val url = Env.getVarOrDefault("LYRIC_RABBITMQ_URL", "")
  if url.length == 0 {
    panic(
      "LYRIC_RABBITMQ_URL is not set; the integration tests need a running RabbitMQ, e.g. LYRIC_RABBITMQ_URL=amqp://user:password@127.0.0.1:5672/vhost (scripts/ci/start-rabbitmq.sh starts one)"
    )
  }
  url
}

func unique(prefix: in String): String {
  prefix + "." + Uuid.uuidToString(Uuid.newUuid())
}

func optionsFor(queue: in String): RabbitMq.ConnectOptions {
  match RabbitMq.optionsFromUrl(brokerUrl(), queue) {
    case Ok(o) -> o
    case Err(e) -> panic("LYRIC_RABBITMQ_URL: " + e)
  }
}

func open(options: in RabbitMq.ConnectOptions): RabbitMq.RabbitMqQueue {
  match RabbitMq.connectWith(options) {
    case Ok(q) -> q
    case Err(e) -> panic("connect failed: " + e)
  }
}

func openQueue(): RabbitMq.RabbitMqQueue {
  open(optionsFor(unique("lyric.it")))
}

// Delete the connection's queue and close the connection.
func dispose(q: in RabbitMq.RabbitMqQueue): Unit {
  val _ = Extras.deleteQueue(q, RabbitMq.queueName(q))
  q.close()
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

func mustPublishWith(q: in RabbitMq.RabbitMqQueue, m: in Mq.Message, options: in Extras.PublishOptions): Unit {
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

func mustTake(q: in RabbitMq.RabbitMqQueue): Mq.Message {
  val got = Got(messages = newList())
  match q.consume(5000, { m -> keep(got, m) }) {
    case Ok(_) -> got.messages[0]
    case Err(e) -> panic("consume failed: " + e)
  }
}

func expectNothing(q: in RabbitMq.RabbitMqQueue, waitMs: in Int, label: in String): Unit {
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

func connectError(options: in RabbitMq.ConnectOptions, label: in String): String {
  match RabbitMq.connectWith(options) {
    case Err(e) -> e
    case Ok(q) -> {
      q.close()
      assertTrue(false, label + ": expected the connection to fail")
      ""
    }
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

// ─── Conformance ─────────────────────────────────────────────────────────────
ref record Opened {
  var queues: List[RabbitMq.RabbitMqQueue]
}

val opened: Opened = Opened(queues = newList())

func makeEnds(): MqConformance.QueueEnds {
  val q = openQueue()
  opened.queues.add(q)
  val publisher: Mq.MessageQueue = q
  val consumer: Mq.QueueConsumer = q
  MqConformance.QueueEnds(publisher = publisher, consumer = consumer)
}

func makeQuorumEnds(): MqConformance.QueueEnds {
  val q = open(optionsFor(unique("lyric.it.quorum")).withQuorumQueue())
  opened.queues.add(q)
  val publisher: Mq.MessageQueue = q
  val consumer: Mq.QueueConsumer = q
  MqConformance.QueueEnds(publisher = publisher, consumer = consumer)
}

func disposeOpened(): Unit {
  for q in opened.queues.toArray() {
    dispose(q)
  }
  opened.queues = newList()
}

test "RabbitMQ conforms to MessageQueue and QueueConsumer" {
  val checks = MqConformance.runQueue({ -> makeEnds() })
  disposeOpened()
  requireConformance(checks)
}

test "a quorum queue conforms to MessageQueue and QueueConsumer" {
  val checks = MqConformance.runQueue({ -> makeQuorumEnds() })
  disposeOpened()
  requireConformance(checks)
}

// ─── Connecting ──────────────────────────────────────────────────────────────
test "the broker identifies itself as RabbitMQ and negotiates the frame size" {
  val q = openQueue()
  assertTrue(Str.startsWith(RabbitMq.serverInfo(q), "RabbitMQ "), "server: " + RabbitMq.serverInfo(q))
  assertEqualInt(RabbitMq.negotiatedFrameMax(q), 131072, "the default frame-max")
  assertEqualInt(RabbitMq.heartbeatSeconds(q), 0, "heartbeats are off by default")
  assertTrue(RabbitMq.isOpen(q), "open")
  dispose(q)
  assertTrue(not RabbitMq.isOpen(q), "closed")
  expectContains(errorOf(q.publish(message("late", "x")), "publish after close"), "closed", "after close")
}

test "connectUrl reaches the broker and virtual host named by the URL" {
  val name = unique("lyric.it.url")
  match RabbitMq.connectUrl(brokerUrl(), name) {
    case Err(e) -> assertTrue(false, e)
    case Ok(q) -> {
      assertEqual(RabbitMq.queueName(q), name, "queue")
      mustPublish(q, message("u-1", "via url"))
      val got = mustTake(q)
      assertEqual(got.body, "via url", "round trip")
      assertTrue(isOk(q.ack(got.id)), "ack")
      dispose(q)
    }
  }
}

test "a refused connection is an Err" {
  val server = match KT.serverListen("127.0.0.1", 0) { case Ok(s) -> s; case Err(e) -> panic(e) }
  val port = KT.serverPort(server)
  KT.serverClose(server)
  val e = connectError(RabbitMq.defaultOptions("127.0.0.1", port, "a"), "nothing listens there")
  expectContains(e, "rabbitmq: cannot connect to 127.0.0.1:" + port.toString(), "refused")
}

test "bad credentials are refused with 403 ACCESS_REFUSED" {
  val options = optionsFor(unique("lyric.it"))
  val e = connectError(options.copy(password = "definitely-not-the-password"), "a wrong password")
  expectContains(e, "403 ACCESS_REFUSED", "the reply code")
  expectContains(e, "cannot open a connection", "refused, not lost")
  assertTrue(not Str.contains(e, "definitely-not-the-password"), "the password is not echoed")
}

test "a virtual host that does not exist is refused with 530 NOT_ALLOWED" {
  val options = optionsFor(unique("lyric.it"))
  val e = connectError(options.copy(virtualHost = unique("no-such-vhost")), "an unknown virtual host")
  expectContains(e, "530 NOT_ALLOWED", "the reply code")
}

test "declare = false checks that the queue exists: 404 NOT_FOUND otherwise" {
  val name = unique("lyric.it.absent")
  val e = connectError(optionsFor(name).copy(declare = false), "a queue nobody declared")
  expectContains(e, "404 NOT_FOUND", "the reply code")
  val owner = open(optionsFor(name))
  val user = open(optionsFor(name).copy(declare = false))
  mustPublish(user, message("p", "x"))
  assertEqual(mustTake(owner).id, "p", "both connections use the one queue")
  user.close()
  dispose(owner)
}

test "a declaration that differs from the existing queue is refused with 406 PRECONDITION_FAILED" {
  val name = unique("lyric.it.mismatch")
  val owner = open(optionsFor(name))
  val e = connectError(optionsFor(name).copy(durable = false), "the same name, not durable")
  expectContains(e, "406 PRECONDITION_FAILED", "the reply code")
  val f = connectError(optionsFor(name).withMessageTtl(1000i64), "the same name with an argument")
  expectContains(f, "406 PRECONDITION_FAILED", "the reply code")
  dispose(owner)
}

// ─── Settling ────────────────────────────────────────────────────────────────
test "a second ack of one delivery is refused by the client and the connection survives" {
  val q = openQueue()
  mustPublish(q, message("m", "x"))
  val got = mustTake(q)
  assertTrue(isOk(q.ack(got.id)), "ack")
  expectContains(
    errorOf(q.ack(got.id), "second ack"),
    "no unsettled delivery",
    "the client refuses a second ack itself"
  )
  assertEqualInt(RabbitMq.unsettledCount(q), 0, "nothing unsettled")
  mustPublish(q, message("n", "y"))
  assertEqual(mustTake(q).id, "n", "still usable")
  assertTrue(isOk(q.ack("n")), "ack")
  dispose(q)
}

test "a requeue keeps the id, body and headers; a classic queue's deliveryCount stops at 1" {
  val q = openQueue()
  val body = "payload \u{0000} with NUL, \"quotes\", café 世\nline2"
  val sent = Mq.Message(
    id = "r:1 é",
    body = body,
    headers = [
      Mq.MessageHeader(key = "content-type", value = "text/plain"),
      Mq.MessageHeader(key = "k:colon\nnewline", value = "v\\backslash"),
      Mq.MessageHeader(key = "empty", value = "")
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
    assertEqual(got.headers[0].key, "content-type", "headers keep their order")
    assertEqual(got.headers[1].key, "k:colon\nnewline", "an awkward header key")
    assertEqual(got.headers[1].value, "v\\backslash", "an awkward header value")
    assertEqual(got.headers[2].value, "", "an empty header value")
    assertTrue(isOk(q.nack(got.id, true)), "requeue")
    round += 1
  }
  val last = mustTake(q)
  assertTrue(isOk(q.ack(last.id)), "ack")
  expectNothing(q, 300, "nothing is left")
  dispose(q)
}

test "a quorum queue counts every redelivery" {
  val q = open(optionsFor(unique("lyric.it.quorum")).withQuorumQueue())
  mustPublish(
    q,
    Mq.Message(id = "q-1", body = "x", headers = [Mq.MessageHeader(key = "k", value = "v")], deliveryCount = 0)
  )
  var round = 0
  while round < 3 {
    val got = mustTake(q)
    assertEqualInt(got.deliveryCount, round, "deliveryCount in round " + round.toString())
    assertEqualInt(got.headers.length, 1, "x-delivery-count is not reported as a header")
    assertTrue(isOk(q.nack(got.id, true)), "requeue")
    round += 1
  }
  val last = mustTake(q)
  assertEqualInt(last.deliveryCount, 3, "after three requeues")
  assertTrue(isOk(q.ack(last.id)), "ack")
  dispose(q)
}

test "with a prefetch of 1 a requeued message is redelivered before the ones behind it" {
  val q = open(optionsFor(unique("lyric.it")).copy(prefetchCount = 1))
  mustPublish(q, message("first", "1"))
  mustPublish(q, message("second", "2"))
  val a = mustTake(q)
  assertEqual(a.id, "first", "first delivery")
  assertTrue(isOk(q.nack(a.id, true)), "requeue the first")
  val b = mustTake(q)
  assertEqual(b.id, "first", "the requeued message returns to the head of the queue")
  assertEqualInt(b.deliveryCount, 1, "flagged as redelivered")
  assertTrue(isOk(q.ack(b.id)), "ack")
  val c = mustTake(q)
  assertEqual(c.id, "second", "then the one that waited")
  assertEqualInt(c.deliveryCount, 0, "which was never delivered before")
  assertTrue(isOk(q.ack(c.id)), "ack")
  dispose(q)
}

test "with a wider prefetch the messages already pushed to the client come before a requeued one" {
  val q = open(optionsFor(unique("lyric.it")).copy(prefetchCount = 10))
  mustPublish(q, message("first", "1"))
  mustPublish(q, message("second", "2"))
  val a = mustTake(q)
  assertEqual(a.id, "first", "first delivery")
  // Give the broker time to push the second message into the window.
  K.sleepMs(300)
  assertTrue(isOk(q.nack(a.id, true)), "requeue the first")
  val b = mustTake(q)
  assertEqual(b.id, "second", "the prefetched message comes next")
  val c = mustTake(q)
  assertEqual(c.id, "first", "then the requeued one")
  assertEqualInt(c.deliveryCount, 1, "flagged as redelivered")
  assertTrue(isOk(q.ack(b.id)), "ack")
  assertTrue(isOk(q.ack(c.id)), "ack")
  dispose(q)
}

test "a prefetch of 1 delivers nothing more until the delivery is settled" {
  val q = open(optionsFor(unique("lyric.it")).copy(prefetchCount = 1))
  mustPublish(q, message("a", "1"))
  mustPublish(q, message("b", "2"))
  assertEqual(mustTake(q).id, "a", "first delivery")
  expectNothing(q, 400, "the window is full")
  assertTrue(isOk(q.ack("a")), "ack")
  assertEqual(mustTake(q).id, "b", "the window opened")
  assertTrue(isOk(q.ack("b")), "ack")
  dispose(q)
}

test "deliveries left unsettled at close are redelivered to the next consumer" {
  val name = unique("lyric.it")
  val first = open(optionsFor(name))
  mustPublish(first, message("left-1", "a"))
  mustPublish(first, message("left-2", "b"))
  assertEqual(mustTake(first).id, "left-1", "delivered to the first consumer")
  assertEqual(mustTake(first).id, "left-2", "delivered to the first consumer")
  assertEqualInt(RabbitMq.unsettledCount(first), 2, "two unsettled")
  first.close()
  val second = open(optionsFor(name))
  val a = mustTake(second)
  assertEqual(a.id, "left-1", "order is kept")
  assertEqualInt(a.deliveryCount, 1, "the broker's redelivered flag counts as one delivery")
  val b = mustTake(second)
  assertEqual(b.id, "left-2", "second message")
  assertEqualInt(b.deliveryCount, 1, "also flagged")
  assertTrue(isOk(second.ack(a.id)), "ack")
  assertTrue(isOk(second.ack(b.id)), "ack")
  expectNothing(second, 300, "nothing is left")
  dispose(second)
}

test "a publisher that never consumes takes no messages" {
  val name = unique("lyric.it")
  val publisher = open(optionsFor(name))
  val consumer = open(optionsFor(name))
  mustPublish(publisher, message("only", "x"))
  mustPublish(publisher, message("twice", "y"))
  assertEqual(mustTake(consumer).id, "only", "the consumer gets the first")
  assertEqual(mustTake(consumer).id, "twice", "and the second")
  assertTrue(isOk(consumer.ack("only")), "ack")
  assertTrue(isOk(consumer.ack("twice")), "ack")
  publisher.close()
  dispose(consumer)
}

// ─── Dead-lettering ──────────────────────────────────────────────────────────
test "nack without requeue discards the message when the queue has no dead-letter exchange" {
  val q = openQueue()
  mustPublish(q, message("gone", "poison"))
  val got = mustTake(q)
  assertTrue(isOk(q.nack(got.id, false)), "reject")
  expectNothing(q, 400, "a rejected message is not redelivered")
  match Extras.inspectQueue(q, RabbitMq.queueName(q)) {
    case Ok(info) -> assertEqualLong(info.messageCount, 0i64, "the queue is empty")
    case Err(e) -> assertTrue(false, e)
  }
  dispose(q)
}

test "nack without requeue dead-letters the message when the queue has a dead-letter exchange" {
  val name = unique("lyric.it.work")
  val deadName = name + ".dead"
  val q = open(optionsFor(name).withDeadLetter("", deadName))
  val dead = open(optionsFor(deadName))
  mustPublish(
    q,
    Mq.Message(
      id = "poison-1",
      body = "poison",
      headers = [Mq.MessageHeader(key = "why", value = "test")],
      deliveryCount = 0
    )
  )
  val got = mustTake(q)
  assertTrue(isOk(q.nack(got.id, false)), "reject")
  expectNothing(q, 300, "a rejected message is not redelivered")
  val letter = mustTake(dead)
  assertEqual(letter.id, "poison-1", "the dead letter keeps its id")
  assertEqual(letter.body, "poison", "and its body")
  assertEqual(headerOf(letter, "why"), "test", "and its headers")
  assertEqualInt(letter.deliveryCount, 0, "it is a first delivery from the dead-letter queue")
  val death = headerOf(letter, "x-death")
  expectContains(death, "\"reason\":\"rejected\"", "x-death is delivered as JSON")
  expectContains(death, "\"queue\":\"" + name + "\"", "x-death names the queue it came from")
  assertEqual(headerOf(letter, "x-first-death-reason"), "rejected", "the broker's own string headers")
  assertTrue(isOk(dead.ack(letter.id)), "ack")
  dispose(dead)
  dispose(q)
}

test "a quorum queue dead-letters a message that exceeds its delivery limit" {
  val name = unique("lyric.it.limited")
  val deadName = name + ".dead"
  val q = open(optionsFor(name).withQuorumQueue().withDeliveryLimit(2).withDeadLetter("", deadName))
  val dead = open(optionsFor(deadName))
  mustPublish(q, message("limited", "x"))
  var round = 0
  while round < 3 {
    val got = mustTake(q)
    assertEqualInt(got.deliveryCount, round, "deliveryCount in round " + round.toString())
    assertTrue(isOk(q.nack(got.id, true)), "requeue")
    round += 1
  }
  expectNothing(q, 400, "the limit is reached")
  val letter = mustTake(dead)
  assertEqual(letter.id, "limited", "dead-lettered")
  expectContains(headerOf(letter, "x-death"), "\"reason\":\"delivery_limit\"", "for the delivery limit")
  assertTrue(isOk(dead.ack(letter.id)), "ack")
  dispose(dead)
  dispose(q)
}

// ─── Batches, Auto mode, large and foreign messages ──────────────────────────
test "publishBatch delivers the whole batch in order" {
  val q = open(optionsFor(unique("lyric.it")).copy(prefetchCount = 0))
  val batch: List[Mq.Message] = newList()
  var n = 0
  while n < 200 {
    batch.add(message("b-" + n.toString(), "body " + n.toString()))
    n += 1
  }
  match q.publishBatch(batch.toArray()) {
    case Ok(_) -> ()
    case Err(e) -> assertTrue(false, "publishBatch failed: " + e)
  }
  n = 0
  while n < 200 {
    val got = mustTake(q)
    assertEqual(got.id, "b-" + n.toString(), "batch order")
    assertTrue(isOk(q.ack(got.id)), "ack")
    n += 1
  }
  expectNothing(q, 300, "nothing is left")
  dispose(q)
}

func refuse(m: in Mq.Message): Result[Unit, String] {
  Err("handler said no")
}

test "Auto mode acknowledges on Ok and requeues on Err" {
  val q = open(optionsFor(unique("lyric.it")).copy(ackMode = Mq.AckMode.Auto))
  mustPublish(q, message("auto-1", "x"))
  val e = errorOf(q.consume(5000, { m -> refuse(m) }), "a failing handler")
  assertEqual(e, "handler said no", "the handler's error")
  val again = mustTake(q)
  assertEqual(again.id, "auto-1", "requeued")
  assertEqualInt(again.deliveryCount, 1, "as a redelivery")
  assertEqualInt(RabbitMq.unsettledCount(q), 0, "acknowledged when the handler returned Ok")
  assertTrue(isOk(q.ack("auto-1")), "ack is a no-op in Auto mode")
  expectNothing(q, 300, "nothing is left")
  dispose(q)
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

test "a 300 KiB body is split into frames and reassembled" {
  val q = openQueue()
  val body = bigBody(307200)
  mustPublish(q, message("big", body))
  val got = mustTake(q)
  assertEqualInt(got.body.length, 307200, "length")
  assertTrue(got.body == body, "content")
  assertTrue(isOk(q.ack(got.id)), "ack")
  dispose(q)
}

test "the smallest frame size still carries a large body" {
  val q = open(optionsFor(unique("lyric.it")).copy(frameMax = 4096))
  assertEqualInt(RabbitMq.negotiatedFrameMax(q), 4096, "the broker accepts the smaller limit")
  val body = bigBody(50000)
  mustPublish(q, message("small-frames", body))
  val got = mustTake(q)
  assertTrue(got.body == body, "content")
  assertTrue(isOk(q.ack(got.id)), "ack")
  dispose(q)
}

test "a delivery larger than maxMessageBytes is rejected and does not block the queue" {
  val name = unique("lyric.it")
  val q = open(optionsFor(name).copy(maxMessageBytes = 1000))
  mustPublish(q, message("too-big", bigBody(5000)))
  mustPublish(q, message("fine", "small"))
  val e = errorOf(q.consume(5000, { m -> Ok(()) }), "an oversized delivery")
  expectContains(e, "message 'too-big' cannot be delivered", "names the message")
  expectContains(e, "exceeds maxMessageBytes", "says why")
  val next = mustTake(q)
  assertEqual(next.id, "fine", "the queue moves on")
  assertTrue(isOk(q.ack(next.id)), "ack")
  expectNothing(q, 300, "the oversized message was not requeued")
  dispose(q)
}

test "header values of other types are delivered as text" {
  val q = openQueue()
  val nested: slice[Amqp.TableEntry] = [Amqp.entry("n", Amqp.fInt(1)), Amqp.entry("s", Amqp.fText("x"))]
  val items: slice[Amqp.FieldValue] = [Amqp.fInt(1), Amqp.fText("two"), Amqp.FBool(value = false)]
  val foreign: slice[Amqp.TableEntry] = [
    Amqp.entry("int", Amqp.fInt(-42)),
    Amqp.entry("long", Amqp.fLong(9007199254740993i64)),
    Amqp.entry("bool", Amqp.FBool(value = true)),
    Amqp.entry("double", Amqp.FDouble(bits = 4609434218613702656i64)),
    Amqp.entry("decimal", Amqp.FDecimal(scale = 2, unscaled = 1234)),
    Amqp.entry("time", Amqp.FTimestamp(seconds = 1700000000i64)),
    Amqp.entry("void", Amqp.FVoid),
    Amqp.entry("bytes", Amqp.FBytes(bytes = encodeUtf8("raw"))),
    Amqp.entry("array", Amqp.FArray(items = items)),
    Amqp.entry("table", Amqp.FTable(entries = nested))
  ]
  val sent = Mq.Message(
    id = "typed",
    body = "x",
    headers = [Mq.MessageHeader(key = "text", value = "plain")],
    deliveryCount = 0
  )
  match RabbitMq.publishTo(q, "", RabbitMq.queueName(q), true, sent, Amqp.noProperties().copy(headers = foreign)) {
    case Ok(_) -> ()
    case Err(e) -> assertTrue(false, e)
  }
  val got = mustTake(q)
  assertEqualInt(got.headers.length, 11, "every header arrives")
  assertEqual(headerOf(got, "int"), "-42", "an integer")
  assertEqual(headerOf(got, "long"), "9007199254740993", "a 64-bit integer")
  assertEqual(headerOf(got, "bool"), "true", "a boolean")
  assertEqual(headerOf(got, "double"), "1.5", "a double")
  assertEqual(headerOf(got, "decimal"), "12.34", "a decimal")
  assertEqual(headerOf(got, "time"), "1700000000", "a timestamp")
  assertEqual(headerOf(got, "void"), "", "void")
  assertEqual(headerOf(got, "bytes"), "raw", "a byte array")
  assertEqual(headerOf(got, "array"), "[1,\"two\",false]", "an array")
  assertEqual(headerOf(got, "table"), "{\"n\":1,\"s\":\"x\"}", "a table")
  assertEqual(headerOf(got, "text"), "plain", "the message's own header")
  assertTrue(isOk(q.ack(got.id)), "ack")
  dispose(q)
}

test "repeated keys, awkward keys and the order of headers survive the broker" {
  val q = openQueue()
  val sent = [
    Mq.MessageHeader(key = "k", value = "1"),
    Mq.MessageHeader(key = "other", value = "o"),
    Mq.MessageHeader(key = "k", value = "2"),
    Mq.MessageHeader(key = "", value = "an empty key"),
    Mq.MessageHeader(key = Str.repeat("k", 300), value = "a key over 255 bytes"),
    Mq.MessageHeader(key = "quote\"back\\slash\nline", value = "café 世 😀"),
    Mq.MessageHeader(key = "a", value = "last, and first in the alphabet")
  ]
  mustPublish(q, Mq.Message(id = "dup", body = "x", headers = sent, deliveryCount = 0))
  var round = 0
  while round < 2 {
    val got = mustTake(q)
    assertEqualInt(got.headers.length, sent.length, "every header, round " + round.toString())
    var i = 0
    while i < got.headers.length and i < sent.length {
      assertEqual(got.headers[i].key, sent[i].key, "key " + i.toString() + ", round " + round.toString())
      assertEqual(got.headers[i].value, sent[i].value, "value " + i.toString() + ", round " + round.toString())
      i += 1
    }
    if round == 0 {
      assertTrue(isOk(q.nack(got.id, true)), "requeue")
    } else {
      assertTrue(isOk(q.ack(got.id)), "ack")
    }
    round += 1
  }
  dispose(q)
}

// ─── Routing ─────────────────────────────────────────────────────────────────
test "a mandatory publish nobody receives is an Err with 312 NO_ROUTE; without mandatory it is dropped" {
  val q = openQueue()
  val nowhere = unique("lyric.it.nowhere")
  val e = errorOf(Extras.publishWith(q, message("lost", "x"), Extras.toExchange(q, "", nowhere)), "unroutable")
  expectContains(e, "312 NO_ROUTE", "the reply code")
  expectContains(e, "message 'lost'", "names the message")
  assertEqualInt(RabbitMq.lastReplyCode(q), 312, "lastReplyCode")
  val relaxed = Extras.toExchange(q, "", nowhere).copy(mandatory = false)
  assertTrue(isOk(Extras.publishWith(q, message("dropped", "x"), relaxed)), "confirmed although nobody received it")
  mustPublish(q, message("kept", "x"))
  assertEqual(mustTake(q).id, "kept", "the connection is unaffected")
  assertTrue(isOk(q.ack("kept")), "ack")
  dispose(q)
}

test "publishing to an exchange that does not exist is 404; the channel is reopened and usable" {
  val q = openQueue()
  mustPublish(q, message("before", "x"))
  assertEqual(mustTake(q).id, "before", "delivered")
  val e = errorOf(
    Extras.publishWith(q, message("x", "x"), Extras.toExchange(q, unique("no.such.exchange"), "k")),
    "no exchange"
  )
  expectContains(e, "the broker closed the channel: 404 NOT_FOUND", "the channel error")
  assertEqualInt(RabbitMq.lastReplyCode(q), 404, "lastReplyCode")
  assertTrue(RabbitMq.isOpen(q), "the connection survives a channel error")
  assertEqualInt(RabbitMq.unsettledCount(q), 0, "the old channel's deliveries are forgotten")
  expectContains(
    errorOf(q.ack("before"), "ack on the old channel"),
    "no unsettled delivery",
    "cannot be acknowledged any more"
  )
  val again = mustTake(q)
  assertEqual(again.id, "before", "the broker requeued the unsettled delivery")
  assertEqualInt(again.deliveryCount, 1, "as a redelivery")
  assertTrue(isOk(q.ack(again.id)), "ack on the new channel")
  mustPublish(q, message("after", "y"))
  assertEqual(mustTake(q).id, "after", "publishing works on the new channel")
  assertTrue(isOk(q.ack("after")), "ack")
  dispose(q)
}

test "a topic exchange routes by binding pattern" {
  val exchange = unique("lyric.it.topic")
  val orders = open(optionsFor(unique("lyric.it.orders")))
  val audit = open(optionsFor(unique("lyric.it.audit")))
  assertTrue(
    isOk(Extras.declareExchange(orders, exchange, "topic", Extras.defaultExchangeOptions().copy(durable = false))),
    "declare"
  )
  assertTrue(isOk(Extras.bindQueue(orders, exchange, "order.*")), "bind orders")
  assertTrue(isOk(Extras.bindQueue(audit, exchange, "#")), "bind audit")
  mustPublishWith(orders, message("o-1", "placed"), Extras.toExchange(orders, exchange, "order.placed"))
  mustPublishWith(orders, message("p-1", "paid"), Extras.toExchange(orders, exchange, "payment.received"))
  assertEqual(mustTake(orders).id, "o-1", "the order queue gets the order")
  expectNothing(orders, 300, "and not the payment")
  assertEqual(mustTake(audit).id, "o-1", "the audit queue gets both")
  assertEqual(mustTake(audit).id, "p-1", "in order")
  mustPublishWith(orders, message("z", "z"), Extras.toExchange(orders, exchange, "nobody.listens"))
  assertEqual(mustTake(audit).id, "z", "delivered to audit")
  val redeclared = Extras.declareExchange(
    orders,
    exchange,
    "fanout",
    Extras.defaultExchangeOptions().copy(durable = false)
  )
  expectContains(errorOf(redeclared, "another type"), "406 PRECONDITION_FAILED", "an exchange cannot change its type")
  assertTrue(isOk(Extras.deleteExchange(orders, exchange, false)), "delete the exchange")
  dispose(audit)
  dispose(orders)
}

test "declareQueue, inspectQueue, purgeQueue and deleteQueue manage other queues" {
  val q = openQueue()
  val other = unique("lyric.it.other")
  match Extras.declareQueue(q, other, Extras.defaultQueueOptions()) {
    case Ok(info) -> {
      assertEqual(info.name, other, "the declared name")
      assertEqualLong(info.messageCount, 0i64, "empty")
    }
    case Err(e) -> assertTrue(false, e)
  }
  mustPublishWith(q, message("o-1", "x"), Extras.toExchange(q, "", other))
  mustPublishWith(q, message("o-2", "x"), Extras.toExchange(q, "", other))
  match Extras.inspectQueue(q, other) {
    case Ok(info) -> assertEqualLong(info.messageCount, 2i64, "two messages wait")
    case Err(e) -> assertTrue(false, e)
  }
  match Extras.purgeQueue(q, other) {
    case Ok(n) -> assertEqualLong(n, 2i64, "purged")
    case Err(e) -> assertTrue(false, e)
  }
  match Extras.deleteQueue(q, other) {
    case Ok(n) -> assertEqualLong(n, 0i64, "deleted empty")
    case Err(e) -> assertTrue(false, e)
  }
  match Extras.inspectQueue(q, other) {
    case Ok(_) -> assertTrue(false, "the queue was deleted")
    case Err(e) -> expectContains(e, "404 NOT_FOUND", "gone")
  }
  mustPublish(q, message("still", "x"))
  assertEqual(mustTake(q).id, "still", "usable after the channel error")
  dispose(q)
}

// ─── Message properties ──────────────────────────────────────────────────────
test "a priority queue delivers higher priorities first" {
  val q = open(optionsFor(unique("lyric.it.priority")).withMaxPriority(9).copy(prefetchCount = 1))
  val base = Extras.defaultPublishOptions(q)
  mustPublishWith(q, message("low", "x"), base.copy(priority = 1))
  mustPublishWith(q, message("high", "x"), base.copy(priority = 9))
  mustPublishWith(q, message("mid", "x"), base.copy(priority = 5))
  for expected in ["high", "mid", "low"] {
    val got = mustTake(q)
    assertEqual(got.id, expected, "priority order")
    assertTrue(isOk(q.ack(got.id)), "ack")
  }
  dispose(q)
}

test "a message past its expiration is not delivered" {
  val q = openQueue()
  mustPublishWith(q, message("short-lived", "x"), Extras.defaultPublishOptions(q).copy(expirationMs = 100i64))
  mustPublishWith(q, message("long-lived", "x"), Extras.defaultPublishOptions(q).copy(expirationMs = 60000i64))
  K.sleepMs(500)
  val got = mustTake(q)
  assertEqual(got.id, "long-lived", "the expired message is gone")
  assertTrue(isOk(q.ack(got.id)), "ack")
  expectNothing(q, 300, "nothing else")
  dispose(q)
}

test "a queue's message TTL expires messages into its dead-letter queue" {
  val name = unique("lyric.it.ttl")
  val deadName = name + ".dead"
  val dead = open(optionsFor(deadName))
  val publisher = open(optionsFor(name).withMessageTtl(100i64).withDeadLetter("", deadName))
  mustPublish(publisher, message("expiring", "x"))
  val letter = mustTake(dead)
  assertEqual(letter.id, "expiring", "expired into the dead-letter queue")
  expectContains(headerOf(letter, "x-death"), "\"reason\":\"expired\"", "for expiry")
  assertTrue(isOk(dead.ack(letter.id)), "ack")
  dispose(publisher)
  dispose(dead)
}

test "invalid publish options are refused before anything is sent" {
  val q = openQueue()
  val base = Extras.defaultPublishOptions(q)
  expectContains(
    errorOf(Extras.publishWith(q, message("m", "x"), base.copy(priority = 256)), "priority"),
    "priority",
    "priority"
  )
  expectContains(
    errorOf(Extras.publishWith(q, message("m", "x"), base.copy(expirationMs = -1i64)), "expiration"),
    "expirationMs",
    "expiration"
  )
  expectContains(
    errorOf(q.publish(message(Str.repeat("i", 256), "x")), "a long message id"),
    "exceeds the AMQP limit of 255",
    "message id length"
  )
  expectNothing(q, 300, "nothing was published")
  dispose(q)
}

// ─── Heartbeats ──────────────────────────────────────────────────────────────
test "with a heartbeat a blocked consume keeps the connection alive" {
  val q = open(optionsFor(unique("lyric.it.hb")).copy(heartbeatSeconds = 1))
  assertEqualInt(RabbitMq.heartbeatSeconds(q), 1, "negotiated")
  expectNothing(q, 3500, "three and a half intervals inside consume")
  mustPublish(q, message("alive", "x"))
  assertEqual(mustTake(q).id, "alive", "the connection is alive")
  assertTrue(isOk(q.ack("alive")), "ack")
  dispose(q)
}

test "with a heartbeat keepAlive carries an idle connection; without it the broker disconnects" {
  val name = unique("lyric.it.hb")
  val kept = open(optionsFor(name).copy(heartbeatSeconds = 1))
  val idle = open(optionsFor(name).copy(heartbeatSeconds = 1))
  var n = 0
  while n < 9 {
    K.sleepMs(400)
    assertTrue(isOk(RabbitMq.keepAlive(kept)), "keepAlive")
    n += 1
  }
  mustPublish(kept, message("kept", "x"))
  val e = errorOf(idle.publish(message("idle", "x")), "after 3.6 s of silence")
  expectContains(e, "rabbitmq: connection lost:", "the broker gave up on the silent connection")
  assertTrue(not RabbitMq.isOpen(idle), "lost")
  assertEqual(mustTake(kept).id, "kept", "only the kept connection's message arrived")
  assertTrue(isOk(kept.ack("kept")), "ack")
  dispose(kept)
}

// ─── Threads ─────────────────────────────────────────────────────────────────
func drainInto(q: in RabbitMq.RabbitMqQueue, got: in Got): Unit {
  var more = true
  while more {
    match q.consume(1500, { m -> keep(got, m) }) {
      case Ok(_) -> {
        val last = got.messages[got.messages.count - 1]
        val _ = q.ack(last.id)
      }
      case Err(_) -> {
        more = false
      }
    }
  }
}

test "two consumers on their own threads share a queue without loss or duplication" {
  val name = unique("lyric.it")
  val publisher = open(optionsFor(name))
  val left = open(optionsFor(name).copy(prefetchCount = 1))
  val right = open(optionsFor(name).copy(prefetchCount = 1))
  assertTrue(isOk(RabbitMq.subscribe(left)), "left consumer")
  assertTrue(isOk(RabbitMq.subscribe(right)), "right consumer")
  val count = 60
  var n = 0
  while n < count {
    mustPublish(publisher, message("w-" + n.toString(), n.toString()))
    n += 1
  }
  val leftGot = Got(messages = newList())
  val rightGot = Got(messages = newList())
  val leftWork = KT.startBackground({ -> drainInto(left, leftGot) })
  val rightWork = KT.startBackground({ -> drainInto(right, rightGot) })
  KT.waitBackground(leftWork)
  KT.waitBackground(rightWork)
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
  dispose(publisher)
}

func publishMany(q: in RabbitMq.RabbitMqQueue, prefix: in String, count: in Int, failures: in Got): Unit {
  var n = 0
  while n < count {
    match q.publish(message(prefix + n.toString(), "x")) {
      case Ok(_) -> ()
      case Err(e) -> failures.messages.add(message(prefix + n.toString(), e))
    }
    n += 1
  }
}

test "one connection shared by a consumer thread and two publisher threads loses nothing" {
  val q = open(optionsFor(unique("lyric.it.shared")).copy(prefetchCount = 0))
  assertTrue(isOk(RabbitMq.subscribe(q)), "consumer")
  val failures = Got(messages = newList())
  val got = Got(messages = newList())
  val consumer = KT.startBackground({ -> drainInto(q, got) })
  val a = KT.startBackground({ -> publishMany(q, "a-", 40, failures) })
  val b = KT.startBackground({ -> publishMany(q, "b-", 40, failures) })
  KT.waitBackground(a)
  KT.waitBackground(b)
  KT.waitBackground(consumer)
  assertEqualInt(failures.messages.count, 0, "every publish was confirmed")
  val seen: Map[String, Bool] = newMap()
  for m in got.messages.toArray() {
    seen[m.id] = true
  }
  assertEqualInt(got.messages.count, 80, "every message was delivered once")
  assertEqualInt(seen.count, 80, "and no id twice")
  dispose(q)
}

// ─── TLS ─────────────────────────────────────────────────────────────────────
func requiredEnv(name: in String, what: in String): String {
  val value = Env.getVarOrDefault(name, "")
  if value.length == 0 {
    panic(
      name + " is not set; the TLS integration tests need " + what +
        " (scripts/ci/start-rabbitmq.sh starts a broker and names it)"
    )
  }
  value
}

func tlsOptionsFrom(name: in String, what: in String, queue: in String): RabbitMq.ConnectOptions {
  match RabbitMq.optionsFromUrl(requiredEnv(name, what), queue) {
    case Ok(o) -> {
      if not o.tls.enabled {
        panic(name + " must be an amqps:// URL")
      }
      o
    }
    case Err(e) -> panic(name + ": " + e)
  }
}

func tlsOptions(queue: in String): RabbitMq.ConnectOptions {
  tlsOptionsFrom("LYRIC_RABBITMQ_TLS_URL", "the URL of a TLS listener", queue)
}

func mtlsOptions(queue: in String): RabbitMq.ConnectOptions {
  tlsOptionsFrom("LYRIC_RABBITMQ_MTLS_URL", "the URL of a TLS listener that requires a client certificate", queue)
}

func pemOf(name: in String, what: in String): String {
  val path = requiredEnv(name, what)
  match File.readText(path) {
    case Ok(text) -> text
    case Err(_) -> panic("cannot read the file named by " + name + " (" + path + ")")
  }
}

func caPem(): String {
  pemOf("LYRIC_RABBITMQ_TLS_CA", "the CA certificate of the broker (a PEM file)")
}

func clientCertPem(): String {
  pemOf("LYRIC_RABBITMQ_TLS_CERT", "a client certificate the broker trusts (a PEM file)")
}

func clientKeyPem(): String {
  pemOf("LYRIC_RABBITMQ_TLS_KEY", "the client certificate's PKCS8 private key (a PEM file)")
}

func verified(queue: in String): RabbitMq.ConnectOptions {
  tlsOptions(queue).withExclusiveCaCertificate(caPem())
}

func roundTrip(q: in RabbitMq.RabbitMqQueue, id: in String): Unit {
  mustPublish(q, message(id, "over TLS"))
  val got = mustTake(q)
  assertEqual(got.id, id, "round trip")
  assertEqual(got.body, "over TLS", "body")
  assertTrue(isOk(q.ack(got.id)), "ack")
}

func makeTlsEnds(): MqConformance.QueueEnds {
  val q = open(verified(unique("lyric.it.tls")))
  opened.queues.add(q)
  val publisher: Mq.MessageQueue = q
  val consumer: Mq.QueueConsumer = q
  MqConformance.QueueEnds(publisher = publisher, consumer = consumer)
}

test "TLS: RabbitMQ conforms to MessageQueue and QueueConsumer over amqps" {
  val checks = MqConformance.runQueue({ -> makeTlsEnds() })
  disposeOpened()
  requireConformance(checks)
}

test "TLS: the broker is verified against the custom CA, exclusive or additional" {
  val exclusive = open(verified(unique("lyric.it.tls")))
  roundTrip(exclusive, "tls-exclusive")
  dispose(exclusive)
  val additional = open(tlsOptions(unique("lyric.it.tls")).withCaCertificate(caPem()))
  roundTrip(additional, "tls-additional")
  dispose(additional)
}

test "TLS: without the CA the broker's certificate is refused" {
  val e = connectError(tlsOptions(unique("lyric.it.tls")), "the platform trust store does not hold the test CA")
  expectContains(e, "rabbitmq: TLS handshake with", "a handshake failure")
  match RabbitMq.connectUrl(requiredEnv("LYRIC_RABBITMQ_TLS_URL", "the URL of a TLS listener"), unique("lyric.it.tls")) {
    case Ok(q) -> {
      q.close()
      assertTrue(false, "connectUrl must verify the certificate too")
    }
    case Err(f) -> expectContains(f, "TLS handshake", "connectUrl verifies")
  }
}

test "TLS: a host name the certificate does not carry is refused" {
  val options = verified(unique("lyric.it.tls")).withTlsServerName("rabbitmq.example.invalid")
  expectContains(connectError(options, "a wrong server name"), "TLS handshake", "the name is checked")
  val named = open(verified(unique("lyric.it.tls")).withTlsServerName("localhost"))
  roundTrip(named, "tls-named")
  dispose(named)
}

test "TLS: skip-verify needs the option and LYRIC_TLS_ALLOW_INSECURE together" {
  val requested = tlsOptions(unique("lyric.it.tls")).withTlsServerName("rabbitmq.example.invalid").withInsecureSkipVerify()
  Env.setVar("LYRIC_TLS_ALLOW_INSECURE", "")
  assertTrue(not RabbitMq.skipsVerification(requested.tls), "the option alone does not skip verification")
  expectContains(connectError(requested, "the option alone"), "TLS handshake", "verification stays on")
  Env.setVar("LYRIC_TLS_ALLOW_INSECURE", "1")
  expectContains(
    connectError(tlsOptions(unique("lyric.it.tls")), "the variable alone"),
    "TLS handshake",
    "the variable alone does nothing"
  )
  assertTrue(RabbitMq.skipsVerification(requested.tls), "both keys together")
  val q = open(requested)
  roundTrip(q, "tls-insecure")
  dispose(q)
  Env.setVar("LYRIC_TLS_ALLOW_INSECURE", "")
}

test "TLS: TLS 1.3 can be required" {
  val q = open(verified(unique("lyric.it.tls")).withMinTlsVersion(Tls.TlsVersion.Tls13))
  roundTrip(q, "tls13")
  dispose(q)
}

test "mutual TLS: a client certificate from the broker's CA is accepted where one is required" {
  val options = mtlsOptions(unique("lyric.it.mtls")).withExclusiveCaCertificate(caPem()).withClientIdentity(
    clientCertPem(),
    clientKeyPem()
  )
  val q = open(options)
  roundTrip(q, "mtls")
  dispose(q)
}

test "mutual TLS: a connection without a client certificate is refused where one is required" {
  val options = mtlsOptions(unique("lyric.it.mtls")).withExclusiveCaCertificate(caPem())
  val e = connectError(options, "no client certificate")
  assertTrue(
    Str.contains(e, "TLS handshake") or Str.contains(e, "cannot open a connection"),
    "refused during or right after the handshake: " + e
  )
}

test "mutual TLS: the listener that does not require a certificate still accepts one" {
  val q = open(verified(unique("lyric.it.tls")).withClientIdentity(clientCertPem(), clientKeyPem()))
  roundTrip(q, "tls-optional-cert")
  dispose(q)
}

test "TLS: a large body, a channel error and a requeue behave as over plain TCP" {
  val q = open(verified(unique("lyric.it.tls")))
  val body = bigBody(307200)
  mustPublish(q, message("big", body))
  val got = mustTake(q)
  assertTrue(got.body == body, "a body larger than a TLS record")
  assertTrue(isOk(q.nack(got.id, true)), "requeue")
  val again = mustTake(q)
  assertEqualInt(again.deliveryCount, 1, "redelivered")
  assertTrue(isOk(q.ack(again.id)), "ack")
  expectNothing(q, 300, "a timeout over TLS")
  val e = errorOf(
    Extras.publishWith(q, message("x", "x"), Extras.toExchange(q, unique("no.such.exchange"), "k")),
    "no exchange"
  )
  expectContains(e, "404 NOT_FOUND", "a channel error over TLS")
  roundTrip(q, "after-error")
  dispose(q)
}

test "TLS: connectFromEnv reads the CA, the client identity and the server name" {
  val name = unique("lyric.it.env")
  Env.setVar(
    "LYRIC_CONFIG_MQ_RABBITMQ_URL",
    requiredEnv("LYRIC_RABBITMQ_MTLS_URL", "the URL of the mutual-TLS listener")
  )
  Env.setVar("LYRIC_CONFIG_MQ_RABBITMQ_QUEUENAME", name)
  Env.setVar("LYRIC_CONFIG_MQ_RABBITMQ_TLSCAFILE", requiredEnv("LYRIC_RABBITMQ_TLS_CA", "the CA file"))
  Env.setVar("LYRIC_CONFIG_MQ_RABBITMQ_TLSCAEXCLUSIVE", "true")
  Env.setVar(
    "LYRIC_CONFIG_MQ_RABBITMQ_TLSCERTFILE",
    requiredEnv("LYRIC_RABBITMQ_TLS_CERT", "the client certificate file")
  )
  Env.setVar("LYRIC_CONFIG_MQ_RABBITMQ_TLSKEYFILE", requiredEnv("LYRIC_RABBITMQ_TLS_KEY", "the client key file"))
  Env.setVar("LYRIC_CONFIG_MQ_RABBITMQ_TLSSERVERNAME", "localhost")
  Env.setVar("LYRIC_CONFIG_MQ_RABBITMQ_TLSMINVERSION", "1.2")
  match RabbitMq.connectFromEnv() {
    case Err(e) -> assertTrue(false, e)
    case Ok(q) -> {
      roundTrip(q, "from-env")
      dispose(q)
    }
  }
  for field in ["URL", "QUEUENAME", "TLSCAFILE", "TLSCAEXCLUSIVE", "TLSCERTFILE", "TLSKEYFILE", "TLSSERVERNAME", "TLSMINVERSION"] {
    Env.setVar("LYRIC_CONFIG_MQ_RABBITMQ_" + field, "")
  }
}
