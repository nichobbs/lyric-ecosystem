//! Mq.Sqs - Amazon SQS backend for `Mq.MessageQueue` and `Mq.QueueConsumer`,
//! in pure Lyric: the AWS JSON 1.0 protocol over `Std.Http`, signed with AWS
//! Signature Version 4 (`Lyric.Aws.SigV4`).
//!
//! One `SqsQueue` is one queue. It implements both interfaces, so the same
//! value publishes and consumes.
//!
//!   import Mq
//!   import Mq.Sqs as Sqs
//!
//!   val cfg = match Sqs.configFromEnv(queueUrl) { case Ok(c) -> c; case Err(e) -> return Err(e.text()) }
//!   val queue = Sqs.connectWith(cfg)?
//!   Mq.publish(queue, Mq.Message(id = "o-1", body = "{}", headers = [], deliveryCount = 0))?
//!   Mq.consume(queue, 5000, { m -> handle(m) })?
//!   Mq.ack(queue, "o-1")?
//!   queue.close()
//!
//! How the interface maps onto SQS:
//!
//! - `publish` is `SendMessage`; `Ok` means SQS stored the message.
//!   `publishBatch` is `SendMessageBatch`, split into requests of at most ten
//!   messages and `maxBatchBytes` bytes, sent in order. SQS applies a batch
//!   entry by entry, so a batch is never all-or-nothing: when an entry or a
//!   request fails, `publishBatch` stops and returns an `Err` that names the
//!   message ids that were not accepted; everything accepted before that
//!   stays in the queue, and later requests are not sent.
//! - A message's `id`, headers and body encoding travel in message
//!   attributes (`Mq.Sqs.Wire`). `deliveryCount` is SQS's
//!   `ApproximateReceiveCount` minus one.
//! - `consume(timeoutMs, handler)` is `ReceiveMessage` with long polling.
//!   `WaitTimeSeconds` is whole seconds and at most 20, so the wait is a
//!   series of long polls of up to 20 s for the whole seconds of the timeout,
//!   then short polls 100 ms apart for what is left of a second.
//!   `consume(0, ...)` is one short poll. The handler runs on one message;
//!   with `receiveBatchSize > 1` the others received with it wait in this
//!   consumer, in flight, for the next `consume`.
//! - SQS settles a delivery by its receipt handle, not by a message id. The
//!   consumer remembers the handle of every delivery it hands to a handler,
//!   under the message's id, until the delivery is settled or its visibility
//!   timeout has passed. `ack(id)` is `DeleteMessage`.
//! - `nack(id, true)` is `ChangeMessageVisibility` to 0: the message is
//!   receivable again at once. Messages received in the same request and not
//!   yet handed to a handler are returned with it, so a FIFO queue's order
//!   holds.
//! - `nack(id, false)` deletes the message, after copying it to
//!   `deadLetterQueueUrl` when the configuration names one. SQS has no call
//!   that moves a message to a dead-letter queue: its redrive policy only acts
//!   once a message has been received `maxReceiveCount` times, so leaving a
//!   nacked message to it would redeliver the message until then, which is
//!   what `requeue = false` rules out.
//! - `close` returns every delivery this consumer still holds (handed out and
//!   unsettled, or waiting) to the queue and refuses further operations.
//!
//! With `AckMode.Auto`, `consume` deletes the message when the handler
//! returns `Ok` and makes it visible again when it returns `Err`; `ack` and
//! `nack` do nothing.
//!
//! A standard queue delivers at least once and in no guaranteed order. A FIFO
//! queue (`.fifo`) delivers in order within a message group and does not
//! release the next message of a group while an earlier one is in flight:
//! see the README for what that means for `receiveBatchSize`.
//!
//! An `SqsQueue` may be shared between threads: its bookkeeping is a
//! protected type, and no lock is held across a request or a handler.
@runtime_checked
@io
package Mq.Sqs

import Std.Core
import Std.Collections
import Std.Environment as Env
import Std.Hash as Hash
import Std.Encoding as Enc
import Std.String as Str
import Std.Time as Time
import Std.Uuid as Uuid
import Aws.SigV4.Env as AwsEnv
import Mq
import Mq.Sqs.Client
import Mq.Sqs.Address
import Mq.Sqs.Wire
pub use Mq
pub use Mq.Sqs.Client

val EMPTY_ID: String = "sqs: empty message id"

val CLOSED: String = "sqs: the queue is closed"

// The pause between short polls while less than a second of a consume
// timeout is left.
val SHORT_POLL_INTERVAL_MS: Int = 100

/// A delivery this consumer holds: received, and either waiting for a
/// `consume` or handed to a handler and not yet settled.
pub record Flight {
  incoming: Incoming
  // Monotonic milliseconds after which the receipt handle no longer hides
  // the message.
  deadlineMs: Long
}

/// The consumer's bookkeeping: which deliveries it holds and under which
/// message id. Public only because `SqsQueue` carries it; use `ack`, `nack`,
/// `unsettledCount` and `waitingCount`, not its entries.
pub protected type QueueState {
  // Received and not yet handed to a handler, in the order SQS returned them.
  var waiting: List[Flight] = newList()
  var inFlight: Map[String, List[Flight]] = newMap()
  var closed: Bool = false
  entry isClosed(): Bool {
    return closed
  }
  entry stash(items: in List[Flight]) {
    var i = 0
    while i < items.count {
      waiting.add(items[i])
      i = i + 1
    }
  }
  // The oldest waiting delivery whose visibility timeout has not passed; the
  // ones that have are dropped, SQS redelivers them.
  entry next(nowMs: in Long): Option[Flight] {
    while waiting.count > 0 {
      val f = waiting[0]
      waiting.removeAt(0)
      if f.deadlineMs > nowMs {
        return Some(value = f)
      }
    }
    return None
  }
  // Remember a delivery under its id, and forget every delivery whose
  // visibility timeout has passed.
  entry admit(id: in String, flight: in Flight, nowMs: in Long) {
    val ids = mapKeys(inFlight)
    var i = 0
    while i < ids.count {
      val prior: List[Flight] = inFlight[ids[i]]
      val kept: List[Flight] = newList()
      var k = 0
      while k < prior.count {
        if prior[k].deadlineMs > nowMs {
          kept.add(prior[k])
        }
        k = k + 1
      }
      if kept.count == 0 {
        inFlight.remove(ids[i])
      } else if kept.count != prior.count {
        inFlight[ids[i]] = kept
      }
      i = i + 1
    }
    if inFlight.containsKey(id) {
      val more: List[Flight] = inFlight[id]
      more.add(flight)
    } else {
      val fresh: List[Flight] = newList()
      fresh.add(flight)
      inFlight[id] = fresh
    }
  }
  // Take the oldest live delivery of `id` out of the in-flight set.
  entry claim(id: in String, nowMs: in Long): Option[Flight] {
    if not inFlight.containsKey(id) {
      return None
    }
    val held: List[Flight] = inFlight[id]
    var found: Option[Flight] = None
    while held.count > 0 and not isSome(found) {
      val f = held[0]
      held.removeAt(0)
      if f.deadlineMs > nowMs {
        found = Some(value = f)
      }
    }
    if held.count == 0 {
      inFlight.remove(id)
    }
    return found
  }
  // Change the deadline of the oldest live delivery of `id`; returns its
  // receipt handle.
  entry retime(id: in String, nowMs: in Long, deadlineMs: in Long): Option[String] {
    if not inFlight.containsKey(id) {
      return None
    }
    val held: List[Flight] = inFlight[id]
    var i = 0
    while i < held.count {
      if held[i].deadlineMs > nowMs {
        val f = held[i]
        held[i] = Flight(incoming = f.incoming, deadlineMs = deadlineMs)
        return Some(value = f.incoming.receiptHandle)
      }
      i = i + 1
    }
    return None
  }
  // The receipt handles of the waiting deliveries, which stop waiting.
  entry drain(): List[String] {
    val handles: List[String] = newList()
    var i = 0
    while i < waiting.count {
      handles.add(waiting[i].incoming.receiptHandle)
      i = i + 1
    }
    waiting = newList()
    return handles
  }
  // Close, and return every receipt handle still held.
  entry shut(): List[String] {
    val handles: List[String] = newList()
    if closed {
      return handles
    }
    closed = true
    var i = 0
    while i < waiting.count {
      handles.add(waiting[i].incoming.receiptHandle)
      i = i + 1
    }
    waiting = newList()
    val lists = mapValues(inFlight)
    var k = 0
    while k < lists.count {
      val held: List[Flight] = lists[k]
      var j = 0
      while j < held.count {
        handles.add(held[j].incoming.receiptHandle)
        j = j + 1
      }
      k = k + 1
    }
    inFlight = newMap()
    return handles
  }
  entry unsettled(nowMs: in Long): Int {
    var total = 0
    val lists = mapValues(inFlight)
    var k = 0
    while k < lists.count {
      val held: List[Flight] = lists[k]
      var j = 0
      while j < held.count {
        if held[j].deadlineMs > nowMs {
          total = total + 1
        }
        j = j + 1
      }
      k = k + 1
    }
    return total
  }
  entry waitingCount(): Int {
    return waiting.count
  }
}

/// An SQS queue, open for publishing and consuming. Obtain one from
/// `connectWith` or `connectFromEnv`.
pub record SqsQueue {
  cfg: SqsConfig
  client: SqsClient
  /// The queue's URL, validated and split.
  queue: QueueUrl
  state: QueueState
}

func monotonicMillis(): Long {
  Time.monotonicNanos() / 1000000i64
}

func configError(message: in String): String {
  "sqs: " + message
}

// A `MessageGroupId` or `MessageDeduplicationId`: 1 to 128 characters of
// letters, digits and ASCII punctuation.
func isFifoToken(text: in String): Bool {
  if text.length == 0 or text.length > 128 {
    return false
  }
  var i = 0
  while i < text.length {
    val u = Str.codeUnitAt(text, i)
    if u < 33 or u > 126 {
      return false
    }
    i = i + 1
  }
  true
}

/// The `MessageDeduplicationId` that stands for message id `id`: the id
/// itself when SQS accepts it as one (1 to 128 letters, digits and ASCII
/// punctuation), otherwise the lowercase hex SHA-256 of its UTF-8 bytes.
@pure
pub func deduplicationIdFor(id: in String): String {
  if isFifoToken(id) {
    id
  } else {
    Str.toLower(Hash.sha256OfBytes(Enc.encodeUtf8(id)))
  }
}

/// Validate `cfg` and open its queue. No request is made: a wrong credential
/// or a queue that does not exist shows up on the first operation.
///
/// `Err` for anything `openClient` refuses, an empty or malformed queue URL,
/// a visibility timeout outside 1 to 43200 seconds, a receive batch outside 1
/// to 10, a non-positive `maxBatchBytes`, a malformed dead-letter queue URL,
/// or, for a FIFO queue, a message group id SQS would refuse.
pub func connectWith(cfg: in SqsConfig): Result[SqsQueue, String] {
  if cfg.queueUrl.length == 0 {
    return Err(configError("no queue URL configured"))
  }
  val queue = match parseQueueUrl(cfg.queueUrl) { case Ok(q) -> q; case Err(e) -> return Err(configError(e)) }
  val client = match openClient(cfg) { case Ok(c) -> c; case Err(e) -> return Err(e.text()) }
  if cfg.visibilityTimeoutSeconds < 1 or cfg.visibilityTimeoutSeconds > 43200 {
    return Err(configError("visibilityTimeoutSeconds must be 1 to 43200, got " + cfg.visibilityTimeoutSeconds.toString()))
  }
  if cfg.receiveBatchSize < 1 or cfg.receiveBatchSize > 10 {
    return Err(configError("receiveBatchSize must be 1 to 10, got " + cfg.receiveBatchSize.toString()))
  }
  if cfg.maxBatchBytes < 1 {
    return Err(configError("maxBatchBytes must be > 0, got " + cfg.maxBatchBytes.toString()))
  }
  if cfg.deadLetterQueueUrl.length > 0 {
    match parseQueueUrl(cfg.deadLetterQueueUrl) {
      case Ok(_) -> ()
      case Err(e) -> return Err(configError("dead-letter queue: " + e))
    }
  }
  if queue.fifo and not isFifoToken(cfg.messageGroupId) {
    return Err(configError("messageGroupId must be 1 to 128 letters, digits and ASCII punctuation for a FIFO queue"))
  }
  Ok(SqsQueue(cfg = cfg, client = client, queue = queue, state = QueueState()))
}

func envOr(suffix: in String, fallback: in String): String {
  val v = Env.getVarOrDefault("LYRIC_CONFIG_MQ_SQS_" + suffix, "")
  if v.length > 0 {
    v
  } else {
    fallback
  }
}

func parseSmallInt(text: in String): Option[Int] {
  if text.length == 0 or text.length > 6 {
    return None
  }
  var n = 0
  var i = 0
  while i < text.length {
    val u = Str.codeUnitAt(text, i)
    if u < 48 or u > 57 {
      return None
    }
    n = n * 10 + (u - 48)
    i = i + 1
  }
  Some(value = n)
}

/// Open the queue the environment describes:
///
/// - `LYRIC_CONFIG_MQ_SQS_QUEUEURL`: the queue URL (required)
/// - `LYRIC_CONFIG_MQ_SQS_ACKMODE`: `Manual` (default) or `Auto`
/// - `LYRIC_CONFIG_MQ_SQS_VISIBILITYTIMEOUT`: seconds, default 30
/// - `LYRIC_CONFIG_MQ_SQS_RECEIVEBATCH`: 1 to 10, default 1
/// - `LYRIC_CONFIG_MQ_SQS_MESSAGEGROUP`: the FIFO message group, default
///   `default`
/// - `LYRIC_CONFIG_MQ_SQS_DEADLETTERQUEUEURL`: the queue `nack(id, false)`
///   copies to (default: none)
/// - the credentials, region and endpoint of `configFromEnv`
///   (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`,
///   `AWS_REGION` or `AWS_DEFAULT_REGION`, `AWS_ENDPOINT_URL_SQS` or
///   `AWS_ENDPOINT_URL`)
pub func connectFromEnv(): Result[SqsQueue, String] {
  val queueUrl = envOr("QUEUEURL", "")
  if queueUrl.length == 0 {
    return Err("sqs: no queue URL configured (set LYRIC_CONFIG_MQ_SQS_QUEUEURL)")
  }
  val base = match configFromEnv(queueUrl) { case Ok(c) -> c; case Err(e) -> return Err(e.text()) }
  val modeText = Str.toLower(envOr("ACKMODE", "manual"))
  val mode = if modeText == "manual" {
    Mq.AckMode.Manual
  } else if modeText == "auto" {
    Mq.AckMode.Auto
  } else {
    return Err("sqs: LYRIC_CONFIG_MQ_SQS_ACKMODE must be Manual or Auto")
  }
  val visibility = match parseSmallInt(envOr("VISIBILITYTIMEOUT", "30")) {
    case Some(n) -> n
    case None -> return Err("sqs: LYRIC_CONFIG_MQ_SQS_VISIBILITYTIMEOUT must be a number of seconds")
  }
  val batch = match parseSmallInt(envOr("RECEIVEBATCH", "1")) {
    case Some(n) -> n
    case None -> return Err("sqs: LYRIC_CONFIG_MQ_SQS_RECEIVEBATCH must be a number from 1 to 10")
  }
  connectWith(
    base.copy(
      ackMode = mode,
      visibilityTimeoutSeconds = visibility,
      receiveBatchSize = batch,
      messageGroupId = envOr("MESSAGEGROUP", "default"),
      deadLetterQueueUrl = envOr("DEADLETTERQUEUEURL", "")
    )
  )
}

// ─── Publishing ──────────────────────────────────────────────────────────────
// The FIFO fields of a message sent to `target`: none for a standard queue.
func sendFieldsFor(q: in SqsQueue, target: in QueueUrl, messageId: in String): SendFields {
  if not target.fifo {
    return SendFields(delaySeconds = -1, messageGroupId = "", deduplicationId = "")
  }
  val dedupe = match q.cfg.deduplication {
    case ById -> deduplicationIdFor(messageId)
    case Unique -> Uuid.uuidToString(Uuid.newUuid())
    case ContentBased -> ""
  }
  SendFields(delaySeconds = -1, messageGroupId = q.cfg.messageGroupId, deduplicationId = dedupe)
}

func cannotPublish(id: in String, why: in String): SqsError {
  SqsError(code = "INVALID_ARGUMENT", message = "cannot publish message '" + id + "': " + why, status = 0)
}

/// Publish `message` with explicit send fields: a delay in seconds
/// (`delaySeconds < 0` leaves the queue's delay in force), and, for a FIFO
/// queue, a message group and a deduplication id (an empty one takes the
/// configuration's). Returns the `MessageId` SQS assigned. This is what
/// `publish` and `Mq.Sqs.Extras.publishWith` are built on.
pub func publishFields(q: in SqsQueue, message: in Mq.Message, fields: in SendFields): Result[String, SqsError] {
  if q.state.isClosed() {
    return Err(SqsError(code = "CLOSED", message = "the queue is closed", status = 0))
  }
  val outgoing = match encodeMessage(message) {
    case Ok(o) -> o
    case Err(why) -> return Err(cannotPublish(message.id, why))
  }
  val defaults = sendFieldsFor(q, q.queue, message.id)
  val send = SendFields(
    delaySeconds = fields.delaySeconds,
    messageGroupId = if fields.messageGroupId.length > 0 {
      fields.messageGroupId
    } else {
      defaults.messageGroupId
    },
    deduplicationId = if fields.deduplicationId.length > 0 {
      fields.deduplicationId
    } else {
      defaults.deduplicationId
    }
  )
  q.client.sendMessage(q.queue.url, outgoing, send)
}

func quoted(ids: in List[String]): String {
  val sb = Str.StringBuilder.new()
  var i = 0
  while i < ids.count {
    if i > 0 {
      sb.append(", ")
    }
    sb.append("'")
    sb.append(ids[i])
    sb.append("'")
    i = i + 1
  }
  sb.toString()
}

func plural(n: in Int, noun: in String): String {
  n.toString() + " " + noun + (if n == 1 { "" } else { "s" })
}

func publishBatchOf(q: in SqsQueue, messages: in slice[Mq.Message]): Result[Unit, String] {
  if q.state.isClosed() {
    return Err(CLOSED)
  }
  // Every message is encoded before any is sent, so a message SQS cannot
  // carry fails the call while the queue is still untouched.
  val outgoing: List[Outgoing] = newList()
  val sizes: List[Int] = newList()
  for m in messages {
    match encodeMessage(m) {
      case Ok(o) -> {
        outgoing.add(o)
        sizes.add(outgoingSize(o))
      }
      case Err(why) -> return Err(cannotPublish(m.id, why).text() + "; nothing was published")
    }
  }
  val plan = planBatches(sizes, 10, q.cfg.maxBatchBytes)
  var start = 0
  var request = 0
  while request < plan.count {
    val entries: List[BatchEntry] = newList()
    var k = 0
    while k < plan[request] {
      entries.add(
        BatchEntry(
          entryId = k.toString(),
          outgoing = outgoing[start + k],
          send = sendFieldsFor(q, q.queue, messages[start + k].id)
        )
      )
      k = k + 1
    }
    val later = messages.length - start - plan[request]
    val tail = "; " + plural(start, "message") + " of earlier requests " + (if start == 1 { "stays" } else { "stay" }) +
      " published and " +
      plural(later, "later message") +
      (if later == 1 { " was" } else { " were" }) +
      " not sent"
    match q.client.sendMessageBatch(q.queue.url, entries) {
      case Err(e) -> {
        val unknown: List[String] = newList()
        var j = 0
        while j < plan[request] {
          unknown.add(messages[start + j].id)
          j = j + 1
        }
        // A response with an SQS error code refused the whole request; no
        // response at all leaves its fate unknown.
        val fate = if e.status > 0 { "none of " } else { "it is not known whether SQS accepted " }
        return Err(
          "sqs: publishBatch: request " + (request + 1).toString() + " of " + plan.count.toString() + ": " + e.message +
            "; " +
            fate +
            quoted(unknown) +
            (if e.status > 0 { " was accepted" } else { "" }) +
            tail
        )
      }
      case Ok(outcome) -> {
        // An entry is published only when SQS lists it as successful.
        val refused: List[String] = newList()
        var j = 0
        while j < plan[request] {
          if not listContains(outcome.successful, j.toString()) {
            var reason = "not acknowledged"
            var f = 0
            while f < outcome.failed.count {
              if outcome.failed[f].entryId == j.toString() {
                reason = outcome.failed[f].code +
                  (if outcome.failed[f].message.length > 0 { ": " + outcome.failed[f].message } else { "" })
              }
              f = f + 1
            }
            refused.add(messages[start + j].id + "' (" + reason + ")")
          }
          j = j + 1
        }
        if refused.count > 0 {
          val sb = Str.StringBuilder.new()
          var r = 0
          while r < refused.count {
            if r > 0 {
              sb.append(", ")
            }
            sb.append("'")
            sb.append(refused[r])
            r = r + 1
          }
          return Err(
            "sqs: publishBatch: " + plural(refused.count, "message") + " of request " + (request + 1).toString() + " of " +
              plan.count.toString() +
              " not accepted: " +
              sb.toString() +
              "; the other " +
              plural(plan[request] - refused.count, "message") +
              " of that request " +
              (if plan[request] - refused.count == 1 { "was" } else { "were" }) +
              " accepted" +
              tail
          )
        }
      }
    }
    start = start + plan[request]
    request = request + 1
  }
  Ok(())
}

// `Mq.MessageQueue.publishBatch` requires `allHaveIds(messages)`. The JVM
// backend resolves that inherited clause in the implementing package and
// fails with J008 when the name is not declared there (see the README's known
// gaps), so this package declares the same predicate, and uses it to refuse a
// batch with an empty id before anything is sent.
@pure
func allHaveIds(messages: in slice[Mq.Message]): Bool {
  for m in messages {
    if m.id.length == 0 {
      return false
    }
  }
  true
}

impl Mq.MessageQueue for SqsQueue {
  func publish(message: in Mq.Message): Result[Unit, String]
    requires: message.id.length > 0
  {
    val defaults = SendFields(delaySeconds = -1, messageGroupId = "", deduplicationId = "")
    return match publishFields(self, message, defaults) { case Ok(_) -> Ok(()); case Err(e) -> Err(e.text()) }
  }

  func publishBatch(messages: in slice[Mq.Message]): Result[Unit, String]
    requires: messages.length > 0
  {
    if not allHaveIds(messages) {
      return Err(EMPTY_ID)
    }
    return publishBatchOf(self, messages)
  }
}

// ─── Consuming ───────────────────────────────────────────────────────────────
func isAuto(q: in SqsQueue): Bool {
  match q.cfg.ackMode {
    case Auto -> true
    case Manual -> false
  }
}

// Return deliveries to the queue, ten per request. Best effort: a delivery
// that cannot be returned becomes visible when its timeout passes.
func release(q: in SqsQueue, handles: in List[String]): Unit {
  var start = 0
  while start < handles.count {
    val chunk: List[String] = newList()
    var k = 0
    while k < 10 and start + k < handles.count {
      chunk.add(handles[start + k])
      k = k + 1
    }
    val _ = q.client.changeVisibilityBatch(q.queue.url, chunk, 0)
    start = start + chunk.count
  }
}

func deadLetter(q: in SqsQueue, flight: in Flight, id: in String): Result[Unit, SqsError] {
  val target = match parseQueueUrl(q.cfg.deadLetterQueueUrl) {
    case Ok(t) -> t
    case Err(e) -> return Err(SqsError(code = "INVALID_CONFIG", message = "dead-letter queue: " + e, status = 0))
  }
  // The copy is the SQS message as it was received: the same body and the
  // same `lyric.*` attributes, so a consumer of the dead-letter queue sees
  // the same `Mq.Message`.
  val copy = Outgoing(body = flight.incoming.body, attributes = flight.incoming.attributes)
  match q.client.sendMessage(target.url, copy, sendFieldsFor(q, target, id)) {
    case Ok(_) -> Ok(())
    case Err(e) -> Err(e)
  }
}

// `how`: 0 acknowledges, 1 drops (dead-letters), 2 requeues.
func settle(q: in SqsQueue, messageId: in String, how: in Int): Result[Unit, String] {
  if messageId.length == 0 {
    return Err(EMPTY_ID)
  }
  if q.state.isClosed() {
    return Err(CLOSED)
  }
  val now = monotonicMillis()
  val flight = match q.state.claim(messageId, now) {
    case Some(f) -> f
    case None -> return Err(
      "sqs: no delivery of message '" + messageId +
        "' is in flight: it was settled already, or its visibility timeout passed and SQS redelivers it"
    )
  }
  val handle = flight.incoming.receiptHandle
  var outcome: Result[Unit, SqsError] = Ok(())
  if how == 2 {
    outcome = q.client.changeVisibility(q.queue.url, handle, 0)
  } else {
    if how == 1 and q.cfg.deadLetterQueueUrl.length > 0 {
      outcome = deadLetter(q, flight, messageId)
    }
    outcome = match outcome { case Ok(_) -> q.client.deleteMessage(q.queue.url, handle); case Err(e) -> Err(e) }
  }
  match outcome {
    case Ok(_) -> {
      if how == 2 {
        // What was received after the requeued message must not overtake it.
        release(q, q.state.drain())
      }
      Ok(())
    }
    case Err(e) -> {
      // Still in flight as far as this consumer knows: the caller may retry.
      q.state.admit(messageId, flight, now)
      Err(e.text())
    }
  }
}

func deliver(q: in SqsQueue, flight: in Flight, handler: (Mq.Message) -> Result[Unit, String]): Result[Unit, String] {
  val id = idOf(flight.incoming)
  q.state.admit(id, flight, monotonicMillis())
  val message = match decodeMessage(flight.incoming) {
    case Ok(m) -> m
    case Err(why) -> return Err(
      "sqs: message '" + id + "' cannot be decoded: " + why +
        "; it stays in flight under that id until it is settled or its visibility timeout passes"
    )
  }
  val outcome = handler(message)
  if not isAuto(q) {
    return outcome
  }
  match outcome {
    case Ok(_) -> settle(q, id, 0)
    case Err(e) -> match settle(q, id, 2) {
      case Ok(_) -> Err(e)
      case Err(requeueError) -> Err(e + " (and requeueing the message failed: " + requeueError + ")")
    }
  }
}

/// The `WaitTimeSeconds` of the next receive when `remainingMs` of a consume
/// timeout are left: the whole seconds, at most 20; 0 (a short poll) under a
/// second.
@pure
pub func waitSecondsFor(remainingMs: in Long): Int {
  if remainingMs < 1000i64 {
    0
  } else if remainingMs >= 20000i64 {
    20
  } else {
    (remainingMs / 1000i64).toInt()
  }
}

impl Mq.QueueConsumer for SqsQueue {
  func consume(timeoutMs: in Int, handler: (Mq.Message) -> Result[Unit, String]): Result[Unit, String]
    requires: timeoutMs >= 0
  {
    // The clock is read in whole milliseconds, rounded down: one more keeps
    // a wait from ending a fraction of a millisecond early.
    val deadline = monotonicMillis() + timeoutMs.toLong() + (if timeoutMs > 0 { 1i64 } else { 0i64 })
    var first = true
    while true {
      if self.state.isClosed() {
        return Err(CLOSED)
      }
      match self.state.next(monotonicMillis()) {
        case Some(flight) -> return deliver(self, flight, handler)
        case None -> ()
      }
      val remaining = deadline - monotonicMillis()
      if not first and remaining <= 0i64 {
        return Err("timeout")
      }
      first = false
      val waitSeconds = waitSecondsFor(remaining)
      val requestedAt = monotonicMillis()
      val received = match self.client.receiveMessages(self.queue.url, self.cfg.receiveBatchSize, waitSeconds, self.cfg.visibilityTimeoutSeconds) {
        case Ok(messages) -> messages
        case Err(e) -> return Err(e.text())
      }
      if received.count > 0 {
        // The visibility timeout runs from when SQS received the request;
        // counting from before it was sent errs on the safe side.
        val expires = requestedAt + self.cfg.visibilityTimeoutSeconds.toLong() * 1000i64
        val flights: List[Flight] = newList()
        var i = 0
        while i < received.count {
          flights.add(Flight(incoming = received[i], deadlineMs = expires))
          i = i + 1
        }
        self.state.stash(flights)
      } else if waitSeconds == 0 {
        val left = deadline - monotonicMillis()
        if left <= 0i64 {
          return Err("timeout")
        }
        Time.sleepMillis(if left < SHORT_POLL_INTERVAL_MS.toLong() { left.toInt() } else { SHORT_POLL_INTERVAL_MS })
      }
    }
    return Err("timeout")
  }

  func ack(messageId: in String): Result[Unit, String] {
    if messageId.length > 0 and isAuto(self) {
      return Ok(())
    }
    return settle(self, messageId, 0)
  }

  func nack(messageId: in String, requeue: in Bool): Result[Unit, String] {
    if messageId.length > 0 and isAuto(self) {
      return Ok(())
    }
    return settle(self, messageId, if requeue { 2 } else { 1 })
  }

  func close(): Unit {
    release(self, self.state.shut())
  }
}

// ─── Introspection and visibility ────────────────────────────────────────────
/// True until `close` is called.
pub func isOpen(q: in SqsQueue): Bool {
  not q.state.isClosed()
}

/// The number of deliveries handed to a handler, not yet settled, whose
/// visibility timeout has not passed.
pub func unsettledCount(q: in SqsQueue): Int {
  q.state.unsettled(monotonicMillis())
}

/// The number of deliveries received with an earlier one and waiting for the
/// next `consume` (always 0 with `receiveBatchSize = 1`).
pub func waitingCount(q: in SqsQueue): Int {
  q.state.waitingCount()
}

/// Give the unsettled delivery of `messageId` `seconds` more of invisibility,
/// counted from now (`ChangeMessageVisibility`), for a handler's work that
/// outlasts the visibility timeout. `Mq.Sqs.Extras.extendVisibility` is the
/// public name.
pub func setVisibility(q: in SqsQueue, messageId: in String, seconds: in Int): Result[Unit, SqsError]
  requires: seconds >= 1 and seconds <= 43200
{
  if q.state.isClosed() {
    return Err(SqsError(code = "CLOSED", message = "the queue is closed", status = 0))
  }
  val now = monotonicMillis()
  val handle = match q.state.retime(messageId, now, now + seconds.toLong() * 1000i64) {
    case Some(h) -> h
    case None -> return Err(
      SqsError(
        code = "NOT_IN_FLIGHT",
        message = "no delivery of message '" + messageId + "' is in flight",
        status = 0
      )
    )
  }
  q.client.changeVisibility(q.queue.url, handle, seconds)
}
