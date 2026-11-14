//! tests/integration/sqs_integration_tests.l - the backend against a real
//! SQS-compatible server (ElasticMQ in CI; Amazon SQS works too).
//!
//! Needs four variables and is compiled only with `--features integration`:
//!
//!   LYRIC_SQS_ENDPOINT=http://127.0.0.1:9324 LYRIC_SQS_REGION=elasticmq \
//!   AWS_ACCESS_KEY_ID=x AWS_SECRET_ACCESS_KEY=x \
//!     lyric test --manifest mq-sqs/lyric.toml --target dotnet --features integration
//!
//! `scripts/ci/start-elasticmq.sh` starts the server and prints them.
//!
//! `LYRIC_SQS_CA_FILE` is optional: a PEM file with a certificate authority to
//! trust in addition to the system trust store (`withCaCertificateFile`), for
//! a server whose certificate is private (AWS Emulator in CI).
//!
//! Every test creates its own queues (`lyric-it-<run>-<n>`) and deletes them
//! again. The conformance suite runs on FIFO queues: two of its cases assert
//! publish order, which a standard queue does not promise (README, "The
//! conformance suite").
@cfg(feature = "integration")
@test_module
package Mq.Sqs.IntegrationTests

import Std.Core
import Std.Collections
import Std.Environment as Env
import Std.String as Str
import Std.Testing
import Std.Time as Time
import Std.Uuid as Uuid
import Testing.Conformance
import Testing.Conformance.Mq as MqConformance
import Mq
import Mq.Sqs
import Mq.Sqs.Extras

func required(name: in String, example: in String): String {
  val v = Env.getVarOrDefault(name, "")
  if v.length == 0 {
    panic(name + " is not set: the integration tests need an SQS-compatible server, for example " + name + "=" + example)
  }
  v
}

func baseConfig(queueUrl: in String): SqsConfig {
  val cfg = endpointConfig(required("LYRIC_SQS_ENDPOINT", "http://127.0.0.1:9324"), queueUrl, required("LYRIC_SQS_REGION", "elasticmq"), required("AWS_ACCESS_KEY_ID", "x"), required("AWS_SECRET_ACCESS_KEY", "x")).withRequestTimeout(
    15000
  )
  val caFile = Env.getVarOrDefault("LYRIC_SQS_CA_FILE", "")
  if caFile.length == 0 {
    return cfg
  }
  match cfg.withCaCertificateFile(caFile) {
    case Ok(trusting) -> trusting
    case Err(e) -> panic("LYRIC_SQS_CA_FILE: " + e.text())
  }
}

func admin(): SqsClient {
  match openClient(baseConfig("")) {
    case Ok(c) -> c
    case Err(e) -> panic("cannot open a client: " + e.text())
  }
}

val runId: String = Uuid.uuidToString(Uuid.newUuid()).substring(0, 8)

ref record Counter {
  var next: Int
}

val counter: Counter = Counter(next = 0)

func freshName(fifo: in Bool): String {
  counter.next = counter.next + 1
  "lyric-it-" + runId + "-" + counter.next.toString() + (if fifo { ".fifo" } else { "" })
}

func create(settings: in QueueSettings): String {
  match createQueue(admin(), freshName(settings.fifo), settings) {
    case Ok(url) -> url
    case Err(e) -> panic("cannot create a queue: " + e.text())
  }
}

func open(cfg: in SqsConfig): SqsQueue {
  match connectWith(cfg) {
    case Ok(q) -> q
    case Err(e) -> panic("cannot connect: " + e)
  }
}

func drop(url: in String): Unit {
  val _ = deleteQueue(admin(), url)
}

func message(id: in String, body: in String): Mq.Message {
  Mq.Message(id = id, body = body, headers = [], deliveryCount = 0)
}

ref record Slot {
  var message: Mq.Message
  var got: Bool
}

func keep(slot: in Slot, m: in Mq.Message): Result[Unit, String] {
  slot.message = m
  slot.got = true
  Ok(())
}

// The next message within `timeoutMs`, or the consume error.
func take(q: in SqsQueue, timeoutMs: in Int): Result[Mq.Message, String] {
  val slot = Slot(message = message("", ""), got = false)
  val result = q.consume(timeoutMs, { m -> keep(slot, m) })
  match result {
    case Ok(_) -> Ok(slot.message)
    case Err(e) -> Err(e)
  }
}

func mustTake(q: in SqsQueue, timeoutMs: in Int): Mq.Message {
  match take(q, timeoutMs) {
    case Ok(m) -> m
    case Err(e) -> panic("no message: " + e)
  }
}

func takeError(q: in SqsQueue, timeoutMs: in Int): String {
  match take(q, timeoutMs) {
    case Ok(m) -> "delivered " + m.id
    case Err(e) -> e
  }
}

func must(r: in Result[Unit, String], what: in String): Unit {
  match r {
    case Ok(_) -> ()
    case Err(e) -> panic(what + ": " + e)
  }
}

func errorOf(r: in Result[Unit, String]): String {
  match r {
    case Ok(_) -> "ok"
    case Err(e) -> e
  }
}

func assertContains(text: in String, part: in String, label: in String): Unit {
  assertTrue(Str.contains(text, part), label + ": got \"" + text + "\"")
}

// ─── Conformance ─────────────────────────────────────────────────────────────
func fifoEnds(created: in List[String]): MqConformance.QueueEnds {
  val url = create(fifoQueue())
  created.add(url)
  val q = open(baseConfig(url).withReceiveBatch(10))
  val publisher: Mq.MessageQueue = q
  val consumer: Mq.QueueConsumer = q
  MqConformance.QueueEnds(publisher = publisher, consumer = consumer)
}

test "the backend conforms to MessageQueue and QueueConsumer on FIFO queues" {
  val created: List[String] = newList()
  val checks = MqConformance.runQueue({ -> fifoEnds(created) })
  var i = 0
  while i < created.count {
    drop(created[i])
    i = i + 1
  }
  requireConformance(checks)
  assertEqualInt(caseCount(checks), 11, "every conformance case ran")
  assertEqualInt(created.count, 11, "each case had its own queue")
}

// ─── Standard queues ─────────────────────────────────────────────────────────
test "a standard queue: publish, consume, ack, requeue, drop" {
  val url = create(standardQueue())
  val q = open(baseConfig(url))
  val sent = Mq.Message(
    id = "order 1/é",
    body = "line1\nline2\t\"quoted\" \\ back\u{0001}ctl café 世 😀",
    headers = [
      Mq.MessageHeader(key = "content-type", value = "text/plain"),
      Mq.MessageHeader(key = "k\"1", value = "v\n1")
    ],
    deliveryCount = 0
  )
  must(q.publish(sent), "publish")
  val first = mustTake(q, 5000)
  assertEqual(first.id, "order 1/é", "id")
  assertEqual(first.body, sent.body, "a body with a control character survives")
  assertEqualInt(first.headers.length, 2, "headers")
  assertEqual(first.headers[1].key, "k\"1", "header key")
  assertEqual(first.headers[1].value, "v\n1", "header value")
  assertEqualInt(first.deliveryCount, 0, "first delivery")
  assertEqualInt(unsettledCount(q), 1, "one delivery in flight")
  assertEqual(takeError(q, 0), "timeout", "in flight means invisible")

  must(q.nack(first.id, true), "nack(requeue)")
  val again = mustTake(q, 5000)
  assertEqualInt(again.deliveryCount, 1, "ApproximateReceiveCount 2 is the first redelivery")
  must(q.ack(again.id), "ack")
  assertEqualInt(unsettledCount(q), 0, "settled")
  assertEqual(takeError(q, 0), "timeout", "an acked message is gone")
  assertContains(errorOf(q.ack(again.id)), "no delivery of message 'order 1/é' is in flight", "a second ack")

  must(q.publish(message("empty", "")), "publish an empty body")
  val empty = mustTake(q, 5000)
  assertEqual(empty.body, "", "an empty body comes back empty")
  must(q.nack(empty.id, false), "nack(drop)")
  assertEqual(takeError(q, 1200), "timeout", "a dropped message is not redelivered")
  q.close()
  drop(url)
}

test "publishBatch of 25 messages is three requests and every message arrives" {
  val url = create(standardQueue())
  val q = open(baseConfig(url).withReceiveBatch(10))
  val batch: List[Mq.Message] = newList()
  var i = 0
  while i < 25 {
    batch.add(message("b-" + i.toString(), "body " + i.toString()))
    i = i + 1
  }
  must(q.publishBatch(batch.toArray()), "publishBatch")
  val seen: Map[String, String] = newMap()
  var n = 0
  while n < 25 {
    val m = mustTake(q, 5000)
    seen[m.id] = m.body
    must(q.ack(m.id), "ack " + m.id)
    n = n + 1
  }
  assertEqualInt(mapSize(seen), 25, "25 distinct messages")
  assertEqual(seen["b-24"], "body 24", "the last one")
  assertEqual(takeError(q, 0), "timeout", "nothing else")
  q.close()
  drop(url)
}

// ─── Long polling ────────────────────────────────────────────────────────────
func elapsedMs(since: in Long): Int {
  ((Time.monotonicNanos() - since) / 1000000i64).toInt()
}

test "consume waits the whole timeout on an empty queue, in whole and fractional seconds" {
  val url = create(standardQueue())
  val q = open(baseConfig(url))
  var started = Time.monotonicNanos()
  assertEqual(takeError(q, 0), "timeout", "a poll")
  assertTrue(elapsedMs(started) < 1000, "a poll does not wait: " + elapsedMs(started).toString() + " ms")
  started = Time.monotonicNanos()
  assertEqual(takeError(q, 300), "timeout", "under a second")
  var took = elapsedMs(started)
  assertTrue(took >= 300 and took < 1500, "300 ms is 300 ms: " + took.toString())
  started = Time.monotonicNanos()
  assertEqual(takeError(q, 2300), "timeout", "2.3 seconds")
  took = elapsedMs(started)
  assertTrue(took >= 2300 and took < 4000, "2300 ms is a 2 s long poll and short polls: " + took.toString())
  q.close()
  drop(url)
}

test "consume over 20 seconds is more than one long poll" {
  val url = create(standardQueue())
  val q = open(baseConfig(url))
  val started = Time.monotonicNanos()
  assertEqual(takeError(q, 21500), "timeout", "21.5 seconds")
  val took = elapsedMs(started)
  assertTrue(took >= 21500 and took < 25000, "the wait was the timeout: " + took.toString() + " ms")
  q.close()
  drop(url)
}

test "a long poll returns as soon as a delayed message becomes visible" {
  val url = create(standardQueue())
  val q = open(baseConfig(url))
  val started = Time.monotonicNanos()
  match publishDelayed(q, message("late", "x"), 2) {
    case Ok(sqsId) -> assertTrue(sqsId.length > 0, "SQS assigned a MessageId")
    case Err(e) -> assertTrue(false, "publishDelayed: " + e.text())
  }
  assertEqual(takeError(q, 0), "timeout", "not visible yet")
  val m = mustTake(q, 15000)
  val took = elapsedMs(started)
  assertEqual(m.id, "late", "the delayed message")
  assertTrue(
    took >= 1500 and took < 8000,
    "delivered when the delay ended, not at the timeout: " + took.toString() + " ms"
  )
  must(q.ack(m.id), "ack")
  q.close()
  drop(url)
}

// ─── Visibility ──────────────────────────────────────────────────────────────
test "an unsettled delivery comes back when its visibility timeout passes" {
  val url = create(standardQueue())
  val q = open(baseConfig(url).withVisibilityTimeout(1))
  must(q.publish(message("v", "x")), "publish")
  val first = mustTake(q, 5000)
  assertEqualInt(first.deliveryCount, 0, "first delivery")
  Time.sleepMillis(1300)
  assertEqualInt(unsettledCount(q), 0, "the consumer no longer counts the expired delivery")
  assertContains(errorOf(q.ack("v")), "its visibility timeout passed", "the old delivery cannot be acked")
  val again = mustTake(q, 5000)
  assertEqualInt(again.deliveryCount, 1, "redelivered by SQS")
  match extendVisibility(q, "v", 5) {
    case Ok(_) -> ()
    case Err(e) -> assertTrue(false, "extendVisibility: " + e.text())
  }
  Time.sleepMillis(1300)
  assertEqual(takeError(q, 0), "timeout", "still hidden after the original second")
  must(q.ack("v"), "ack after the extension")
  q.close()
  drop(url)
}

// ─── Dead letters ────────────────────────────────────────────────────────────
test "nack without requeue copies the message to the configured dead-letter queue" {
  val url = create(standardQueue())
  val dlqUrl = create(standardQueue())
  val q = open(baseConfig(url).withDeadLetterQueue(dlqUrl))
  val dlq = open(baseConfig(dlqUrl))
  val sent = Mq.Message(
    id = "poison",
    body = "bad\u{0002}",
    headers = [Mq.MessageHeader(key = "k", value = "v")],
    deliveryCount = 0
  )
  must(q.publish(sent), "publish")
  val m = mustTake(q, 5000)
  must(q.nack(m.id, false), "nack(drop)")
  assertEqual(takeError(q, 0), "timeout", "gone from the source queue")
  val dead = mustTake(dlq, 5000)
  assertEqual(dead.id, "poison", "the same id")
  assertEqual(dead.body, "bad\u{0002}", "the same body")
  assertEqualInt(dead.headers.length, 1, "the same headers")
  must(dlq.ack(dead.id), "ack the dead letter")
  q.close()
  dlq.close()
  drop(url)
  drop(dlqUrl)
}

test "a redrive policy moves a message after maxReceiveCount receives" {
  val dlqUrl = create(standardQueue())
  val arn = match queueArn(admin(), dlqUrl) { case Ok(a) -> a; case Err(e) -> panic("queueArn: " + e.text()) }
  val url = create(standardQueue().withRedrive(arn, 2))
  val q = open(baseConfig(url))
  val dlq = open(baseConfig(dlqUrl))
  must(q.publish(message("r", "x")), "publish")
  must(q.nack(mustTake(q, 5000).id, true), "first nack")
  val second = mustTake(q, 5000)
  assertEqualInt(second.deliveryCount, 1, "second receive")
  must(q.nack(second.id, true), "second nack")
  assertEqual(takeError(q, 1200), "timeout", "the third receive never happens")
  val dead = mustTake(dlq, 5000)
  assertEqual(dead.id, "r", "SQS moved it to the dead-letter queue")
  must(dlq.ack(dead.id), "ack")
  match queueAttributes(admin(), url) {
    case Err(e) -> assertTrue(false, "queueAttributes: " + e.text())
    case Ok(attributes) -> assertContains(
      match mapGet(attributes, "RedrivePolicy") { case Some(p) -> p; case None -> "" },
      "\"maxReceiveCount\":2",
      "the policy is on the queue"
    )
  }
  q.close()
  dlq.close()
  drop(url)
  drop(dlqUrl)
}

// ─── FIFO ────────────────────────────────────────────────────────────────────
test "a FIFO queue: order, group blocking, deduplication by id" {
  val url = create(fifoQueue())
  val q = open(baseConfig(url))
  for id in ["a", "b", "c"] {
    must(q.publish(message(id, id)), "publish " + id)
  }
  must(q.publish(message("a", "a again")), "publishing an id again is accepted")
  val first = mustTake(q, 5000)
  assertEqual(first.id, "a", "the head of the group")
  assertEqual(first.body, "a", "the duplicate was not stored")
  // One message per receive: the group's next message waits for the first.
  assertEqual(takeError(q, 0), "timeout", "the group is blocked while its head is in flight")
  must(q.ack("a"), "ack a")
  assertEqual(mustTake(q, 5000).id, "b", "then b")
  must(q.ack("b"), "ack b")
  assertEqual(mustTake(q, 5000).id, "c", "then c")
  must(q.ack("c"), "ack c")
  assertEqual(takeError(q, 0), "timeout", "the duplicate 'a' never arrives")

  // Another group is not blocked.
  must(q.publish(message("d", "d")), "publish d")
  match publishToGroup(q, message("e", "e"), "other") {
    case Ok(_) -> ()
    case Err(e) -> assertTrue(false, "publishToGroup: " + e.text())
  }
  // SQS orders within a group, not across groups.
  val one = mustTake(q, 5000).id
  val two = mustTake(q, 5000).id
  assertTrue(
    (one == "d" and two == "e") or (one == "e" and two == "d"),
    "both groups deliver while the other's message is in flight: " + one + ", " + two
  )
  must(q.ack("d"), "ack d")
  must(q.ack("e"), "ack e")
  q.close()
  drop(url)
}

test "a FIFO queue with a receive batch keeps order across a requeue" {
  val url = create(fifoQueue())
  val q = open(baseConfig(url).withReceiveBatch(10).withDeduplication(Deduplication.Unique))
  must(q.publishBatch([message("1", "x"), message("2", "x"), message("3", "x")]), "publishBatch")
  val head = mustTake(q, 5000)
  assertEqual(head.id, "1", "the head")
  assertEqualInt(waitingCount(q), 2, "2 and 3 came with it and wait in the consumer")
  must(q.nack("1", true), "requeue the head")
  assertEqualInt(waitingCount(q), 0, "the ones behind it went back too")
  val again = mustTake(q, 5000)
  assertEqual(again.id, "1", "the head is redelivered first")
  assertEqualInt(again.deliveryCount, 1, "as a redelivery")
  must(q.ack("1"), "ack 1")
  assertEqual(mustTake(q, 5000).id, "2", "then 2")
  assertEqual(mustTake(q, 5000).id, "3", "then 3")
  must(q.ack("2"), "ack 2")
  must(q.ack("3"), "ack 3")
  q.close()
  drop(url)
}

// ─── Auto acknowledgement ────────────────────────────────────────────────────
func refuse(m: in Mq.Message): Result[Unit, String] {
  Err("handler refused " + m.id)
}

test "AckMode.Auto deletes on Ok and requeues on Err" {
  val url = create(standardQueue())
  val q = open(baseConfig(url).withAckMode(Mq.AckMode.Auto))
  must(q.publish(message("auto", "x")), "publish")
  val failed = q.consume(5000, { m -> refuse(m) })
  assertEqual(errorOf(failed), "handler refused auto", "the handler's error")
  val m = mustTake(q, 5000)
  assertEqualInt(m.deliveryCount, 1, "requeued and redelivered")
  assertEqualInt(unsettledCount(q), 0, "settled by consume")
  must(q.ack("auto"), "ack is a no-op")
  assertEqual(takeError(q, 0), "timeout", "deleted when the handler returned Ok")
  q.close()
  drop(url)
}

// ─── Queue management and errors ─────────────────────────────────────────────
test "queue management: create, look up, attributes, purge, delete" {
  val client = admin()
  val name = freshName(false)
  val url = match createQueue(client, name, standardQueue().copy(visibilityTimeoutSeconds = 45)) {
    case Ok(u) -> u
    case Err(e) -> panic("createQueue: " + e.text())
  }
  match queueUrl(client, name) {
    case Ok(found) -> assertEqual(found, url, "GetQueueUrl returns the URL CreateQueue did")
    case Err(e) -> assertTrue(false, "queueUrl: " + e.text())
  }
  match createQueue(client, name, standardQueue().copy(visibilityTimeoutSeconds = 45)) {
    case Ok(same) -> assertEqual(same, url, "creating it again with the same attributes is idempotent")
    case Err(e) -> assertTrue(false, "second createQueue: " + e.text())
  }
  val q = open(baseConfig(url))
  must(q.publishBatch([message("p1", "x"), message("p2", "x")]), "publish two")
  match queueAttributes(client, url) {
    case Err(e) -> assertTrue(false, "queueAttributes: " + e.text())
    case Ok(attributes) -> {
      assertEqual(match mapGet(attributes, "VisibilityTimeout") { case Some(v) -> v; case None -> "" }, "45", "the timeout")
      assertEqual(
        match mapGet(attributes, "ApproximateNumberOfMessages") { case Some(v) -> v; case None -> "" },
        "2",
        "two messages"
      )
    }
  }
  match setVisibilityTimeout(client, url, 20) {
    case Ok(_) -> ()
    case Err(e) -> assertTrue(false, "setVisibilityTimeout: " + e.text())
  }
  match purgeQueue(client, url) {
    case Ok(_) -> ()
    case Err(e) -> assertTrue(false, "purgeQueue: " + e.text())
  }
  assertEqual(takeError(q, 1200), "timeout", "purged")
  match deleteQueue(client, url) {
    case Ok(_) -> ()
    case Err(e) -> assertTrue(false, "deleteQueue: " + e.text())
  }
  match queueUrl(client, name) {
    case Ok(found) -> assertTrue(false, "the deleted queue still resolves: " + found)
    case Err(e) -> assertEqual(e.code, "QueueDoesNotExist", "the error code: " + e.text())
  }
  assertContains(errorOf(q.publish(message("x", "x"))), "QueueDoesNotExist", "publishing to a deleted queue")
  assertContains(takeError(q, 0), "QueueDoesNotExist", "consuming from a deleted queue")
  q.close()
  assertEqual(errorOf(q.publish(message("x", "x"))), "sqs: the queue is closed", "after close")
}

test "close returns unsettled deliveries to the queue" {
  val url = create(standardQueue())
  val q = open(baseConfig(url).withReceiveBatch(10))
  must(q.publishBatch([message("c1", "x"), message("c2", "x"), message("c3", "x")]), "publish three")
  val first = mustTake(q, 5000)
  q.close()
  assertTrue(not isOpen(q), "closed")
  assertEqual(errorOf(q.ack(first.id)), "sqs: the queue is closed", "ack after close")
  val other = open(baseConfig(url).withReceiveBatch(10))
  val seen: Map[String, Bool] = newMap()
  var n = 0
  while n < 3 {
    val m = mustTake(other, 5000)
    seen[m.id] = true
    must(other.ack(m.id), "ack " + m.id)
    n = n + 1
  }
  assertEqualInt(mapSize(seen), 3, "all three were receivable at once, without waiting 30 s")
  other.close()
  drop(url)
}
