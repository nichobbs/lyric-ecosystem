//! Mq.Sqs.Extras - what Amazon SQS offers beyond `Mq.MessageQueue` and
//! `Mq.QueueConsumer`: queue management (`CreateQueue`, `GetQueueUrl`,
//! `PurgeQueue`, `DeleteQueue`, queue attributes), FIFO queues (message
//! groups, deduplication ids), per-message delay, visibility timeouts and the
//! redrive policy.
//!
//! Queue management takes an `SqsClient` (`openClient`, or `queue.client`)
//! and reports `SqsError`s, whose `code` is the SQS error code
//! (`QueueDoesNotExist`, `QueueNameExists`, `PurgeQueueInProgress`).
@runtime_checked
@io
package Mq.Sqs.Extras

import Std.Core
import Std.Collections
import Std.JsonValue
import Std.String as Str
import Mq
import Mq.Sqs
import Mq.Sqs.Address
import Mq.Sqs.Wire

// ─── Queue management ────────────────────────────────────────────────────────
/// The attributes a queue is created with. A negative number, or an empty
/// string, leaves SQS's default in force.
pub record QueueSettings {
  /// A FIFO queue. Its name must end in `.fifo`.
  fifo: Bool
  /// FIFO only: deduplicate by the SHA-256 of the body when a message is
  /// sent without a deduplication id (`Deduplication.ContentBased`).
  contentBasedDeduplication: Bool
  /// How long a received message stays hidden, 0 to 43200 seconds (SQS
  /// default 30). An `SqsQueue` sends its own with every receive.
  visibilityTimeoutSeconds: Int
  /// How long a sent message stays invisible at first, 0 to 900 seconds.
  delaySeconds: Int
  /// How long SQS keeps a message, 60 to 1209600 seconds (default 4 days).
  messageRetentionSeconds: Int
  /// The ARN of the queue SQS moves a message to once it has been received
  /// `maxReceiveCount` times without being deleted.
  deadLetterTargetArn: String
  maxReceiveCount: Int
}

/// Settings of a standard queue with SQS's defaults.
pub func standardQueue(): QueueSettings {
  QueueSettings(
    fifo = false,
    contentBasedDeduplication = false,
    visibilityTimeoutSeconds = -1,
    delaySeconds = -1,
    messageRetentionSeconds = -1,
    deadLetterTargetArn = "",
    maxReceiveCount = 0
  )
}

/// Settings of a FIFO queue with SQS's defaults.
pub func fifoQueue(): QueueSettings {
  standardQueue().copy(fifo = true)
}

/// `settings` with a redrive policy.
pub func QueueSettings.withRedrive(
  settings: in QueueSettings, deadLetterTargetArn: in String, maxReceiveCount: in Int
): QueueSettings
  requires: maxReceiveCount >= 1 and maxReceiveCount <= 1000
{
  settings.copy(deadLetterTargetArn = deadLetterTargetArn, maxReceiveCount = maxReceiveCount)
}

/// The `RedrivePolicy` attribute value: after `maxReceiveCount` receives
/// without a delete, SQS moves a message to the queue `deadLetterTargetArn`
/// names.
@pure
pub func redrivePolicyJson(deadLetterTargetArn: in String, maxReceiveCount: in Int): String
  requires: maxReceiveCount >= 1 and maxReceiveCount <= 1000
{
  val fields: List[JsonField] = newList()
  fields.add(JsonField(name = "deadLetterTargetArn", value = JString(value = deadLetterTargetArn)))
  fields.add(JsonField(name = "maxReceiveCount", value = JInt(value = maxReceiveCount.toLong())))
  writeValue(JObject(fields = fields))
}

func settingAttributes(settings: in QueueSettings): List[Attribute] {
  val acc: List[Attribute] = newList()
  if settings.fifo {
    acc.add(Attribute(name = "FifoQueue", dataType = "String", value = "true"))
    if settings.contentBasedDeduplication {
      acc.add(Attribute(name = "ContentBasedDeduplication", dataType = "String", value = "true"))
    }
  }
  if settings.visibilityTimeoutSeconds >= 0 {
    acc.add(
      Attribute(name = "VisibilityTimeout", dataType = "String", value = settings.visibilityTimeoutSeconds.toString())
    )
  }
  if settings.delaySeconds >= 0 {
    acc.add(Attribute(name = "DelaySeconds", dataType = "String", value = settings.delaySeconds.toString()))
  }
  if settings.messageRetentionSeconds >= 0 {
    acc.add(
      Attribute(name = "MessageRetentionPeriod", dataType = "String", value = settings.messageRetentionSeconds.toString())
    )
  }
  if settings.deadLetterTargetArn.length > 0 {
    acc.add(
      Attribute(
        name = "RedrivePolicy",
        dataType = "String",
        value = redrivePolicyJson(settings.deadLetterTargetArn, settings.maxReceiveCount)
      )
    )
  }
  acc
}

func invalid(message: in String): SqsError {
  SqsError(code = "INVALID_ARGUMENT", message = message, status = 0)
}

func urlIn(action: in String, doc: in JsonValue): Result[String, SqsError] {
  match getString(doc, "QueueUrl") {
    case Some(url) -> Ok(url)
    case None -> Err(
      SqsError(
        code = "MALFORMED_RESPONSE",
        message = action + ": the response has no QueueUrl",
        status = 200
      )
    )
  }
}

/// CreateQueue: create the queue `name` and return its URL. Creating a queue
/// that exists with the same attributes returns its URL; with different ones
/// it is `Err(QueueNameExists)`. `Err(INVALID_ARGUMENT)`, with nothing sent,
/// for a name SQS would refuse or a FIFO setting that disagrees with the
/// `.fifo` suffix.
pub func createQueue(client: in SqsClient, name: in String, settings: in QueueSettings): Result[String, SqsError] {
  if not isValidQueueName(name) {
    return Err(
      invalid("invalid queue name '" + name + "': 1 to 80 letters, digits, '-' and '_', '.fifo' for a FIFO queue")
    )
  }
  if settings.fifo != Str.endsWith(name, ".fifo") {
    return Err(invalid("a FIFO queue's name ends in '.fifo' and a standard queue's does not: '" + name + "'"))
  }
  if settings.deadLetterTargetArn.length > 0 and (settings.maxReceiveCount < 1 or settings.maxReceiveCount > 1000) {
    return Err(invalid("maxReceiveCount must be 1 to 1000, got " + settings.maxReceiveCount.toString()))
  }
  val body = attributeMapRequest("QueueName", name, "Attributes", settingAttributes(settings))
  match client.call("CreateQueue", body, 0) {
    case Err(e) -> Err(e)
    case Ok(doc) -> urlIn("CreateQueue", doc)
  }
}

/// GetQueueUrl: the URL of the queue `name` in this client's account.
/// `Err(QueueDoesNotExist)` when there is none.
pub func queueUrl(client: in SqsClient, name: in String): Result[String, SqsError] {
  match client.call("GetQueueUrl", stringFieldsRequest(["QueueName"], [name]), 0) {
    case Err(e) -> Err(e)
    case Ok(doc) -> urlIn("GetQueueUrl", doc)
  }
}

/// PurgeQueue: delete every message in the queue. SQS takes up to 60 seconds
/// to finish and refuses a second purge within that time
/// (`PurgeQueueInProgress`).
pub func purgeQueue(client: in SqsClient, queueUrl: in String): Result[Unit, SqsError] {
  match client.call("PurgeQueue", stringFieldsRequest(["QueueUrl"], [queueUrl]), 0) {
    case Err(e) -> Err(e)
    case Ok(_) -> Ok(())
  }
}

/// DeleteQueue: delete the queue and everything in it.
pub func deleteQueue(client: in SqsClient, queueUrl: in String): Result[Unit, SqsError] {
  match client.call("DeleteQueue", stringFieldsRequest(["QueueUrl"], [queueUrl]), 0) {
    case Err(e) -> Err(e)
    case Ok(_) -> Ok(())
  }
}

/// GetQueueAttributes: every attribute of the queue, by name
/// (`ApproximateNumberOfMessages`, `QueueArn`, `VisibilityTimeout`,
/// `RedrivePolicy`, `FifoQueue`).
pub func queueAttributes(client: in SqsClient, queueUrl: in String): Result[Map[String, String], SqsError] {
  match client.call("GetQueueAttributes", queueAttributesRequest(queueUrl), 0) {
    case Err(e) -> Err(e)
    case Ok(doc) -> {
      val acc: Map[String, String] = newMap()
      match getObject(doc, "Attributes") {
        case None -> ()
        case Some(fields) -> {
          var i = 0
          while i < fields.count {
            match asString(fields[i].value) {
              case Some(v) -> {
                acc[fields[i].name] = v
              }
              case None -> ()
            }
            i = i + 1
          }
        }
      }
      Ok(acc)
    }
  }
}

/// The queue's ARN, which a redrive policy names.
pub func queueArn(client: in SqsClient, queueUrl: in String): Result[String, SqsError] {
  match queueAttributes(client, queueUrl) {
    case Err(e) -> Err(e)
    case Ok(attributes) -> match mapGet(attributes, "QueueArn") {
      case Some(arn) -> Ok(arn)
      case None -> Err(
        SqsError(
          code = "MALFORMED_RESPONSE",
          message = "GetQueueAttributes: the response has no QueueArn",
          status = 200
        )
      )
    }
  }
}

func setAttribute(client: in SqsClient, queueUrl: in String, name: in String, value: in String): Result[Unit, SqsError] {
  val attributes: List[Attribute] = newList()
  attributes.add(Attribute(name = name, dataType = "String", value = value))
  match client.call("SetQueueAttributes", attributeMapRequest("QueueUrl", queueUrl, "Attributes", attributes), 0) {
    case Err(e) -> Err(e)
    case Ok(_) -> Ok(())
  }
}

/// SetQueueAttributes: give the queue a redrive policy. SQS then moves a
/// message to the dead-letter queue once it has been received
/// `maxReceiveCount` times without being deleted.
pub func setRedrivePolicy(
  client: in SqsClient,
  queueUrl: in String,
  deadLetterTargetArn: in String,
  maxReceiveCount: in Int,
): Result[Unit, SqsError]
  requires: maxReceiveCount >= 1 and maxReceiveCount <= 1000
{
  setAttribute(client, queueUrl, "RedrivePolicy", redrivePolicyJson(deadLetterTargetArn, maxReceiveCount))
}

/// SetQueueAttributes: the queue's own visibility timeout, for consumers
/// that do not send one.
pub func setVisibilityTimeout(client: in SqsClient, queueUrl: in String, seconds: in Int): Result[Unit, SqsError]
  requires: seconds >= 0 and seconds <= 43200
{
  setAttribute(client, queueUrl, "VisibilityTimeout", seconds.toString())
}

// ─── Sending ─────────────────────────────────────────────────────────────────
/// What one publish may set beyond the message.
pub record SendOptions {
  /// Seconds before the message becomes receivable, 0 to 900; negative
  /// leaves the queue's delay in force. SQS refuses a per-message delay on a
  /// FIFO queue.
  delaySeconds: Int
  /// FIFO: the message group; empty takes the queue configuration's.
  messageGroupId: String
  /// FIFO: the deduplication id; empty takes the configuration's rule.
  deduplicationId: String
}

/// No delay, and the queue configuration's group and deduplication.
pub func defaultSendOptions(): SendOptions {
  SendOptions(delaySeconds = -1, messageGroupId = "", deduplicationId = "")
}

/// Publish `message` with `options`. Returns the `MessageId` SQS assigned.
pub func publishWith(q: in SqsQueue, message: in Mq.Message, options: in SendOptions): Result[String, SqsError]
  requires: message.id.length > 0
  requires: options.delaySeconds <= 900
{
  publishFields(
    q,
    message,
    SendFields(
      delaySeconds = options.delaySeconds,
      messageGroupId = options.messageGroupId,
      deduplicationId = options.deduplicationId
    )
  )
}

/// Publish `message` so that it becomes receivable `delaySeconds` from now
/// (standard queues only).
pub func publishDelayed(q: in SqsQueue, message: in Mq.Message, delaySeconds: in Int): Result[String, SqsError]
  requires: message.id.length > 0
  requires: delaySeconds >= 0 and delaySeconds <= 900
{
  publishWith(q, message, defaultSendOptions().copy(delaySeconds = delaySeconds))
}

/// Publish `message` to the FIFO message group `groupId`.
pub func publishToGroup(q: in SqsQueue, message: in Mq.Message, groupId: in String): Result[String, SqsError]
  requires: message.id.length > 0
  requires: groupId.length > 0
{
  publishWith(q, message, defaultSendOptions().copy(messageGroupId = groupId))
}

// ─── Visibility ──────────────────────────────────────────────────────────────
/// Keep the unsettled delivery of `messageId` hidden for `seconds` from now
/// (`ChangeMessageVisibility`), for work that outlasts the visibility
/// timeout. `Err(NOT_IN_FLIGHT)` when the consumer holds no live delivery of
/// that id.
pub func extendVisibility(q: in SqsQueue, messageId: in String, seconds: in Int): Result[Unit, SqsError]
  requires: seconds >= 1 and seconds <= 43200
{
  setVisibility(q, messageId, seconds)
}
