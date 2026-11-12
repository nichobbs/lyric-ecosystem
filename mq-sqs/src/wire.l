//! Mq.Sqs.Wire - the Amazon SQS JSON 1.0 wire format, and how an `Mq.Message`
//! is carried in an SQS message.
//!
//! Pure functions: request bodies are built and responses parsed here, with
//! no network, so `tests/wire_tests.l` checks every one against literal JSON.
//!
//! An `Mq.Message` travels as:
//!
//! - the body: `Message.body`, as it is, when SQS can carry it. SQS accepts
//!   only XML 1.0 characters (tab, line feed, carriage return, U+0020 to
//!   U+D7FF, U+E000 to U+FFFD and the supplementary planes) and no empty
//!   body, so a body with any other character is sent as the base64 of its
//!   UTF-8 bytes and an empty body as `-`; the `lyric.body-encoding`
//!   attribute (`base64`, `empty`) says which.
//! - `lyric.id`: `Message.id`. SQS assigns its own `MessageId`; it is not the
//!   application's id.
//! - `lyric.headers`: `Message.headers` as a JSON array of
//!   `{"key":...,"value":...}` in order, present when there are headers. One
//!   attribute, because SQS allows ten attributes per message, restricts
//!   their names to letters, digits, `-`, `_` and `.`, and returns them
//!   unordered.
//!
//! `deliveryCount` is not sent: it is SQS's `ApproximateReceiveCount` minus
//! one on the way back.
//!
//! A message from another producer (no `lyric.id`) is delivered with the SQS
//! `MessageId` as its id, its body as it is, and its `String` and `Number`
//! attributes as headers in name order; binary attributes are dropped.
@runtime_checked
package Mq.Sqs.Wire

import Std.Core
import Std.Collections
import Std.Encoding as Enc
import Std.JsonValue
import Std.String as Str
import Mq

/// The attribute that carries `Message.id`.
@pure
pub func attributeId(): String {
  "lyric.id"
}

/// The attribute that carries `Message.headers` as a JSON array.
@pure
pub func attributeHeaders(): String {
  "lyric.headers"
}

/// The attribute that says how the body is encoded when it is not sent as it
/// is: `base64` or `empty`.
@pure
pub func attributeBodyEncoding(): String {
  "lyric.body-encoding"
}

/// A message attribute with a string value (`DataType` `String`, `Number` or
/// a custom subtype of either).
pub record Attribute {
  name: String
  dataType: String
  value: String
}

/// An SQS message to send: the body and the attributes.
pub record Outgoing {
  body: String
  attributes: List[Attribute]
}

/// An SQS message as received.
pub record Incoming {
  /// The id SQS assigned.
  messageId: String
  receiptHandle: String
  body: String
  attributes: List[Attribute]
  /// `ApproximateReceiveCount`: 1 on the first receive. 0 when the server
  /// did not report it.
  receiveCount: Int
}

// ─── Text SQS can carry ──────────────────────────────────────────────────────
func isSurrogate(u: in Int): Bool {
  u >= 0xD800 and u <= 0xDFFF
}

/// True when every character of `text` is one SQS accepts in a body or a
/// string attribute: tab, line feed, carriage return, U+0020 to U+D7FF,
/// U+E000 to U+FFFD, and well-formed surrogate pairs.
@pure
pub func isSqsText(text: in String): Bool {
  var i = 0
  while i < text.length {
    val u = Str.codeUnitAt(text, i)
    if u >= 0xD800 and u <= 0xDBFF {
      if i + 1 >= text.length {
        return false
      }
      val low = Str.codeUnitAt(text, i + 1)
      if low < 0xDC00 or low > 0xDFFF {
        return false
      }
      i = i + 2
    } else {
      val ok = u == 9 or u == 10 or u == 13 or (u >= 0x20 and u <= 0xD7FF) or (u >= 0xE000 and u <= 0xFFFD)
      if not ok {
        return false
      }
      i = i + 1
    }
  }
  true
}

func hex4(u: in Int): String {
  val digits = "0123456789abcdef"
  digits.substring((u / 4096) % 16, 1) + digits.substring((u / 256) % 16, 1) + digits.substring((u / 16) % 16, 1) +
    digits.substring(u % 16, 1)
}

/// `text` as a JSON string literal in which every character SQS would refuse
/// in an attribute value is a `\uXXXX` escape, so the literal is always SQS
/// text whatever `text` holds.
@pure
pub func jsonStringLiteral(text: in String): String {
  val sb = Str.StringBuilder.new()
  sb.append("\"")
  var i = 0
  while i < text.length {
    val u = Str.codeUnitAt(text, i)
    if u == 34 {
      sb.append("\\\"")
    } else if u == 92 {
      sb.append("\\\\")
    } else if u == 10 {
      sb.append("\\n")
    } else if u == 13 {
      sb.append("\\r")
    } else if u == 9 {
      sb.append("\\t")
    } else if u < 0x20 or isSurrogate(u) or u >= 0xFFFE {
      sb.append("\\u")
      sb.append(hex4(u))
    } else {
      sb.append(text.substring(i, 1))
    }
    i = i + 1
  }
  sb.append("\"")
  sb.toString()
}

/// `headers` as the JSON array `lyric.headers` carries.
@pure
pub func headersJson(headers: in slice[Mq.MessageHeader]): String {
  val sb = Str.StringBuilder.new()
  sb.append("[")
  var first = true
  for h in headers {
    if not first {
      sb.append(",")
    }
    first = false
    sb.append("{\"key\":")
    sb.append(jsonStringLiteral(h.key))
    sb.append(",\"value\":")
    sb.append(jsonStringLiteral(h.value))
    sb.append("}")
  }
  sb.append("]")
  sb.toString()
}

/// The headers in a `lyric.headers` value. `Err` when it is not a JSON array
/// of objects with string `key` and `value`.
@pure
pub func headersFromJson(text: in String): Result[slice[Mq.MessageHeader], String] {
  val doc = match parseValue(text) {
    case Ok(v) -> v
    case Err(e) -> return Err("lyric.headers is not JSON: " + e.message())
  }
  val items = match asArray(doc) { case Some(xs) -> xs; case None -> return Err("lyric.headers is not a JSON array") }
  val acc: List[Mq.MessageHeader] = newList()
  var i = 0
  while i < items.count {
    val key = match getString(items[i], "key") {
      case Some(k) -> k
      case None -> return Err("lyric.headers entry " + i.toString() + " has no string 'key'")
    }
    val value = match getString(items[i], "value") {
      case Some(v) -> v
      case None -> return Err("lyric.headers entry " + i.toString() + " has no string 'value'")
    }
    acc.add(Mq.MessageHeader(key = key, value = value))
    i = i + 1
  }
  Ok(acc.toArray())
}

// ─── Mq.Message to and from an SQS message ───────────────────────────────────
func stringAttribute(name: in String, value: in String): Attribute {
  Attribute(name = name, dataType = "String", value = value)
}

/// The SQS message that carries `message`. `Err` when the id holds a
/// character SQS cannot carry in an attribute (a control character, a lone
/// surrogate, U+FFFE or U+FFFF): the id is the handle `ack` and `nack` take,
/// so it is refused, not altered.
@pure
pub func encodeMessage(message: in Mq.Message): Result[Outgoing, String] {
  if message.id.length == 0 {
    return Err("empty message id")
  }
  if not isSqsText(message.id) {
    return Err("message id holds a character SQS cannot carry (control character or unpaired surrogate)")
  }
  val attributes: List[Attribute] = newList()
  attributes.add(stringAttribute(attributeId(), message.id))
  if message.headers.length > 0 {
    attributes.add(stringAttribute(attributeHeaders(), headersJson(message.headers)))
  }
  var body = message.body
  if message.body.length == 0 {
    body = "-"
    attributes.add(stringAttribute(attributeBodyEncoding(), "empty"))
  } else if not isSqsText(message.body) {
    body = Enc.encodeBase64(Enc.encodeUtf8(message.body))
    attributes.add(stringAttribute(attributeBodyEncoding(), "base64"))
  }
  Ok(Outgoing(body = body, attributes = attributes))
}

/// The bytes `outgoing` counts against SQS's message and batch size limits:
/// the UTF-8 body plus each attribute's name, data type and value.
@pure
pub func outgoingSize(outgoing: in Outgoing): Int {
  var total = Enc.encodeUtf8(outgoing.body).length
  var i = 0
  while i < outgoing.attributes.count {
    val a = outgoing.attributes[i]
    total = total + Enc.encodeUtf8(a.name).length + Enc.encodeUtf8(a.dataType).length + Enc.encodeUtf8(a.value).length
    i = i + 1
  }
  total
}

func attributeValue(attributes: in List[Attribute], name: in String): Option[String] {
  var i = 0
  while i < attributes.count {
    if attributes[i].name == name {
      return Some(value = attributes[i].value)
    }
    i = i + 1
  }
  None
}

/// The id `incoming` is delivered under: its `lyric.id`, or the SQS
/// `MessageId` for a message from another producer.
@pure
pub func idOf(incoming: in Incoming): String {
  match attributeValue(incoming.attributes, attributeId()) {
    case Some(id) -> if id.length > 0 { id } else { incoming.messageId }
    case None -> incoming.messageId
  }
}

func ordinalLess(a: in String, b: in String): Bool {
  var i = 0
  while i < a.length and i < b.length {
    val x = Str.codeUnitAt(a, i)
    val y = Str.codeUnitAt(b, i)
    if x != y {
      return x < y
    }
    i = i + 1
  }
  a.length < b.length
}

// The attributes of a message from another producer, as headers in name
// order.
func foreignHeaders(attributes: in List[Attribute]): slice[Mq.MessageHeader] {
  val acc: List[Mq.MessageHeader] = newList()
  var i = 0
  while i < attributes.count {
    val a = attributes[i]
    if not Str.startsWith(a.name, "lyric.") {
      acc.add(Mq.MessageHeader(key = a.name, value = a.value))
      var j = acc.count - 1
      while j > 0 and ordinalLess(acc[j].key, acc[j - 1].key) {
        val tmp = acc[j]
        acc[j] = acc[j - 1]
        acc[j - 1] = tmp
        j = j - 1
      }
    }
    i = i + 1
  }
  acc.toArray()
}

/// The `Mq.Message` `incoming` carries. `deliveryCount` is the receive count
/// minus one. `Err` when a `lyric.*` attribute is not what this module writes
/// (an unknown body encoding, base64 that does not decode to UTF-8, headers
/// that are not the JSON array).
@pure
pub func decodeMessage(incoming: in Incoming): Result[Mq.Message, String] {
  val body = match attributeValue(incoming.attributes, attributeBodyEncoding()) {
    case None -> incoming.body
    case Some(encoding) -> {
      if encoding == "empty" {
        ""
      } else if encoding == "base64" {
        val bytes = match Enc.tryDecodeBase64(incoming.body) {
          case Some(b) -> b
          case None -> return Err("the body is marked base64 and is not base64")
        }
        match Enc.tryDecodeUtf8(bytes) {
          case Some(text) -> text
          case None -> return Err("the body is marked base64 and does not decode to UTF-8")
        }
      } else {
        return Err("unknown " + attributeBodyEncoding() + " '" + encoding + "'")
      }
    }
  }
  val headers = match attributeValue(incoming.attributes, attributeHeaders()) {
    case Some(json) -> match headersFromJson(json) { case Ok(hs) -> hs; case Err(e) -> return Err(e) }
    case None -> match attributeValue(incoming.attributes, attributeId()) {
      case Some(_) -> []
      case None -> foreignHeaders(incoming.attributes)
    }
  }
  Ok(
    Mq.Message(
      id = idOf(incoming),
      body = body,
      headers = headers,
      deliveryCount = if incoming.receiveCount > 0 { incoming.receiveCount - 1 } else { 0 }
    )
  )
}

// ─── Requests ────────────────────────────────────────────────────────────────
func field(name: in String, value: in JsonValue): JsonField {
  JsonField(name = name, value = value)
}

func text(value: in String): JsonValue {
  JString(value = value)
}

func number(value: in Int): JsonValue {
  JInt(value = value.toLong())
}

func attributesJson(attributes: in List[Attribute]): JsonValue {
  val fields: List[JsonField] = newList()
  var i = 0
  while i < attributes.count {
    val inner: List[JsonField] = newList()
    inner.add(field("DataType", text(attributes[i].dataType)))
    inner.add(field("StringValue", text(attributes[i].value)))
    fields.add(field(attributes[i].name, JObject(fields = inner)))
    i = i + 1
  }
  JObject(fields = fields)
}

/// What a send carries besides the message. `delaySeconds < 0` leaves the
/// queue's delay in force; an empty `messageGroupId` or `deduplicationId` is
/// not sent.
pub record SendFields {
  delaySeconds: Int
  messageGroupId: String
  deduplicationId: String
}

func addSendFields(fields: in List[JsonField], outgoing: in Outgoing, send: in SendFields): Unit {
  fields.add(field("MessageBody", text(outgoing.body)))
  if outgoing.attributes.count > 0 {
    fields.add(field("MessageAttributes", attributesJson(outgoing.attributes)))
  }
  if send.delaySeconds >= 0 {
    fields.add(field("DelaySeconds", number(send.delaySeconds)))
  }
  if send.messageGroupId.length > 0 {
    fields.add(field("MessageGroupId", text(send.messageGroupId)))
  }
  if send.deduplicationId.length > 0 {
    fields.add(field("MessageDeduplicationId", text(send.deduplicationId)))
  }
}

/// The body of a `SendMessage` request.
@pure
pub func sendMessageRequest(queueUrl: in String, outgoing: in Outgoing, send: in SendFields): String {
  val fields: List[JsonField] = newList()
  fields.add(field("QueueUrl", text(queueUrl)))
  addSendFields(fields, outgoing, send)
  writeValue(JObject(fields = fields))
}

/// One entry of a `SendMessageBatch` request. `entryId` identifies the entry
/// in the response; it is not the message's id.
pub record BatchEntry {
  entryId: String
  outgoing: Outgoing
  send: SendFields
}

/// The body of a `SendMessageBatch` request.
@pure
pub func sendMessageBatchRequest(queueUrl: in String, entries: in List[BatchEntry]): String {
  val items: List[JsonValue] = newList()
  var i = 0
  while i < entries.count {
    val fields: List[JsonField] = newList()
    fields.add(field("Id", text(entries[i].entryId)))
    addSendFields(fields, entries[i].outgoing, entries[i].send)
    items.add(JObject(fields = fields))
    i = i + 1
  }
  val fields: List[JsonField] = newList()
  fields.add(field("QueueUrl", text(queueUrl)))
  fields.add(field("Entries", JArray(items = items)))
  writeValue(JObject(fields = fields))
}

/// The body of a `ReceiveMessage` request for at most `maxMessages` messages,
/// waiting `waitSeconds` for the first, each hidden for
/// `visibilityTimeoutSeconds` once received. Every message attribute and the
/// receive count are asked for.
@pure
pub func receiveMessageRequest(
  queueUrl: in String,
  maxMessages: in Int,
  waitSeconds: in Int,
  visibilityTimeoutSeconds: in Int,
): String
  requires: maxMessages >= 1 and maxMessages <= 10
  requires: waitSeconds >= 0 and waitSeconds <= 20
  requires: visibilityTimeoutSeconds >= 0 and visibilityTimeoutSeconds <= 43200
{
  val all: List[JsonValue] = newList()
  all.add(text("All"))
  val system: List[JsonValue] = newList()
  system.add(text("ApproximateReceiveCount"))
  val fields: List[JsonField] = newList()
  fields.add(field("QueueUrl", text(queueUrl)))
  fields.add(field("MaxNumberOfMessages", number(maxMessages)))
  fields.add(field("WaitTimeSeconds", number(waitSeconds)))
  fields.add(field("VisibilityTimeout", number(visibilityTimeoutSeconds)))
  fields.add(field("MessageAttributeNames", JArray(items = all)))
  fields.add(field("MessageSystemAttributeNames", JArray(items = system)))
  writeValue(JObject(fields = fields))
}

/// The body of a `DeleteMessage` request.
@pure
pub func deleteMessageRequest(queueUrl: in String, receiptHandle: in String): String {
  val fields: List[JsonField] = newList()
  fields.add(field("QueueUrl", text(queueUrl)))
  fields.add(field("ReceiptHandle", text(receiptHandle)))
  writeValue(JObject(fields = fields))
}

/// The body of a `ChangeMessageVisibility` request.
@pure
pub func changeVisibilityRequest(queueUrl: in String, receiptHandle: in String, seconds: in Int): String
  requires: seconds >= 0 and seconds <= 43200
{
  val fields: List[JsonField] = newList()
  fields.add(field("QueueUrl", text(queueUrl)))
  fields.add(field("ReceiptHandle", text(receiptHandle)))
  fields.add(field("VisibilityTimeout", number(seconds)))
  writeValue(JObject(fields = fields))
}

/// The body of a `ChangeMessageVisibilityBatch` request giving every handle
/// in `receiptHandles` (at most ten) the same timeout. Entry ids are the
/// handles' positions: `0`, `1`, ...
@pure
pub func changeVisibilityBatchRequest(queueUrl: in String, receiptHandles: in List[String], seconds: in Int): String
  requires: receiptHandles.count >= 1 and receiptHandles.count <= 10
  requires: seconds >= 0 and seconds <= 43200
{
  val items: List[JsonValue] = newList()
  var i = 0
  while i < receiptHandles.count {
    val fields: List[JsonField] = newList()
    fields.add(field("Id", text(i.toString())))
    fields.add(field("ReceiptHandle", text(receiptHandles[i])))
    fields.add(field("VisibilityTimeout", number(seconds)))
    items.add(JObject(fields = fields))
    i = i + 1
  }
  val fields: List[JsonField] = newList()
  fields.add(field("QueueUrl", text(queueUrl)))
  fields.add(field("Entries", JArray(items = items)))
  writeValue(JObject(fields = fields))
}

/// A request body that is one object of string fields, in the order given:
/// `GetQueueUrl`, `PurgeQueue`, `DeleteQueue`.
@pure
pub func stringFieldsRequest(names: in slice[String], values: in slice[String]): String
  requires: names.length == values.length
{
  val fields: List[JsonField] = newList()
  var i = 0
  while i < names.length {
    fields.add(field(names[i], text(values[i])))
    i = i + 1
  }
  writeValue(JObject(fields = fields))
}

/// A request body of `QueueUrl` or `QueueName` (`key`) and a map of string
/// attributes under `mapName`: `CreateQueue`, `SetQueueAttributes`.
@pure
pub func attributeMapRequest(
  key: in String, value: in String, mapName: in String, attributes: in List[Attribute]
): String {
  val inner: List[JsonField] = newList()
  var i = 0
  while i < attributes.count {
    inner.add(field(attributes[i].name, text(attributes[i].value)))
    i = i + 1
  }
  val fields: List[JsonField] = newList()
  fields.add(field(key, text(value)))
  if attributes.count > 0 {
    fields.add(field(mapName, JObject(fields = inner)))
  }
  writeValue(JObject(fields = fields))
}

/// The body of a `GetQueueAttributes` request for every attribute.
@pure
pub func queueAttributesRequest(queueUrl: in String): String {
  val all: List[JsonValue] = newList()
  all.add(text("All"))
  val fields: List[JsonField] = newList()
  fields.add(field("QueueUrl", text(queueUrl)))
  fields.add(field("AttributeNames", JArray(items = all)))
  writeValue(JObject(fields = fields))
}

// ─── Responses ───────────────────────────────────────────────────────────────
/// The document in a response body. An empty body (`DeleteMessage` and other
/// actions without output answer with none on some servers) is the empty
/// object.
@pure
pub func parseDocument(body: in String): Result[JsonValue, String] {
  if Str.trim(body).length == 0 {
    val none: List[JsonField] = newList()
    return Ok(JObject(fields = none))
  }
  match parseValue(body) {
    case Ok(v) -> match asObject(v) { case Some(_) -> Ok(v); case None -> Err("the response is not a JSON object") }
    case Err(e) -> Err("the response is not JSON: " + e.message())
  }
}

func parseCount(text: in String): Int {
  var n = 0
  var i = 0
  if text.length == 0 or text.length > 9 {
    return 0
  }
  while i < text.length {
    val u = Str.codeUnitAt(text, i)
    if u < 48 or u > 57 {
      return 0
    }
    n = n * 10 + (u - 48)
    i = i + 1
  }
  n
}

func incomingAttributes(message: in JsonValue): List[Attribute] {
  val acc: List[Attribute] = newList()
  match getObject(message, "MessageAttributes") {
    case None -> ()
    case Some(fields) -> {
      var i = 0
      while i < fields.count {
        val dataType = match getString(fields[i].value, "DataType") { case Some(t) -> t; case None -> "" }
        // Binary attributes have no StringValue and are not carried.
        if Str.startsWith(dataType, "String") or Str.startsWith(dataType, "Number") {
          match getString(fields[i].value, "StringValue") {
            case Some(v) -> acc.add(Attribute(name = fields[i].name, dataType = dataType, value = v))
            case None -> ()
          }
        }
        i = i + 1
      }
    }
  }
  acc
}

/// The messages in a `ReceiveMessage` response, in the order the server
/// returned them. A response without `Messages` holds none.
@pure
pub func parseReceiveResponse(doc: in JsonValue): Result[List[Incoming], String] {
  val acc: List[Incoming] = newList()
  val items = match getField(doc, "Messages") {
    case None -> return Ok(acc)
    case Some(v) -> if isNull(v) {
      return Ok(acc)
    } else {
      match asArray(v) {
        case Some(xs) -> xs
        case None -> return Err("ReceiveMessage: 'Messages' is not an array")
      }
    }
  }
  var i = 0
  while i < items.count {
    val handle = match getString(items[i], "ReceiptHandle") {
      case Some(h) -> h
      case None -> return Err("ReceiveMessage: a message has no ReceiptHandle")
    }
    val body = match getString(items[i], "Body") {
      case Some(b) -> b
      case None -> return Err("ReceiveMessage: a message has no Body")
    }
    val messageId = match getString(items[i], "MessageId") { case Some(m) -> m; case None -> "" }
    val count = match getField(items[i], "Attributes") {
      case Some(system) -> match getString(system, "ApproximateReceiveCount") {
        case Some(n) -> parseCount(n)
        case None -> 0
      }
      case None -> 0
    }
    acc.add(
      Incoming(
        messageId = messageId,
        receiptHandle = handle,
        body = body,
        attributes = incomingAttributes(items[i]),
        receiveCount = count
      )
    )
    i = i + 1
  }
  Ok(acc)
}

/// An entry a batch action did not apply.
pub record BatchFailure {
  entryId: String
  code: String
  message: String
  /// True when the request was at fault, false when SQS was.
  senderFault: Bool
}

/// What a batch action reported: the entry ids that were applied and the
/// entries that were not.
pub record BatchOutcome {
  successful: List[String]
  failed: List[BatchFailure]
}

/// The outcome in a `SendMessageBatch`, `ChangeMessageVisibilityBatch` or
/// `DeleteMessageBatch` response.
@pure
pub func parseBatchResponse(doc: in JsonValue): Result[BatchOutcome, String] {
  val successful: List[String] = newList()
  val failed: List[BatchFailure] = newList()
  match getArray(doc, "Successful") {
    case None -> ()
    case Some(items) -> {
      var i = 0
      while i < items.count {
        match getString(items[i], "Id") {
          case Some(id) -> successful.add(id)
          case None -> return Err("a batch result entry has no Id")
        }
        i = i + 1
      }
    }
  }
  match getArray(doc, "Failed") {
    case None -> ()
    case Some(items) -> {
      var i = 0
      while i < items.count {
        val id = match getString(items[i], "Id") {
          case Some(v) -> v
          case None -> return Err("a batch error entry has no Id")
        }
        failed.add(
          BatchFailure(
            entryId = id,
            code = match getString(items[i], "Code") { case Some(c) -> c; case None -> "" },
            message = match getString(items[i], "Message") { case Some(m) -> m; case None -> "" },
            senderFault = match getBool(items[i], "SenderFault") { case Some(b) -> b; case None -> false }
          )
        )
        i = i + 1
      }
    }
  }
  Ok(BatchOutcome(successful = successful, failed = failed))
}

/// The error in a failed response.
pub record Fault {
  /// The error code: `__type` without its namespace
  /// (`com.amazonaws.sqs#QueueDoesNotExist` is `QueueDoesNotExist`).
  code: String
  message: String
}

/// The error in a response body, or `None` when the body is not a JSON error
/// document (an HTML page from a proxy, an empty body).
@pure
pub func parseFault(body: in String): Option[Fault] {
  val doc = match parseValue(body) { case Ok(v) -> v; case Err(_) -> return None }
  val kind = match getString(doc, "__type") {
    case Some(t) -> t
    case None -> match getString(doc, "code") { case Some(c) -> c; case None -> return None }
  }
  val hash = Str.lastIndexOfRaw(kind, "#")
  val code = if hash >= 0 { kind.substring(hash + 1) } else { kind }
  if code.length == 0 {
    return None
  }
  val message = match getString(doc, "message") {
    case Some(m) -> m
    case None -> match getString(doc, "Message") { case Some(m) -> m; case None -> "" }
  }
  Some(value = Fault(code = code, message = message))
}

/// How `sizes` (bytes per message, in order) are split into consecutive
/// batches of at most `maxEntries` messages and `maxBytes` bytes: the length
/// of each batch. A message larger than `maxBytes` is a batch of its own.
@pure
pub func planBatches(sizes: in List[Int], maxEntries: in Int, maxBytes: in Int): List[Int]
  requires: maxEntries >= 1
  requires: maxBytes >= 1
{
  val plan: List[Int] = newList()
  var count = 0
  var bytes = 0
  var i = 0
  while i < sizes.count {
    if count > 0 and (count == maxEntries or bytes + sizes[i] > maxBytes) {
      plan.add(count)
      count = 0
      bytes = 0
    }
    count = count + 1
    bytes = bytes + sizes[i]
    i = i + 1
  }
  if count > 0 {
    plan.add(count)
  }
  plan
}
