//! tests/fake_sqs_tests.l - the SQS backend over real HTTP, against an SQS
//! server written in Lyric on `Std.HttpServer`.
//!
//! The fake server does what SQS does with a request, so nothing here is
//! answered from a canned response:
//!
//!   * it verifies the AWS Signature Version 4 of every request with the
//!     shared secret, rebuilding the canonical request from the method, path
//!     and signed headers it actually received and the SHA-256 of the body it
//!     actually received (`InvalidSignatureException`,
//!     `UnrecognizedClientException`, `MissingAuthenticationTokenException`
//!     are its answers otherwise), and it wants the session token of
//!     temporary credentials sent and signed;
//!   * queues hold messages with a visibility deadline, a receive count and
//!     a receipt handle that changes on every receive. `ReceiveMessage` waits
//!     up to `WaitTimeSeconds` for a message, hides what it returns for the
//!     visibility timeout and counts the receive; `DeleteMessage` and
//!     `ChangeMessageVisibility` want the current handle of a message that is
//!     in flight;
//!   * a FIFO queue returns messages in order, does not release a message of
//!     a group while an earlier one is in flight, and drops a deduplication
//!     id it has seen. A standard queue returns the newest message first:
//!     SQS promises no order there, so the fake keeps none;
//!   * a redrive policy moves a message to its dead-letter queue once it has
//!     been received `maxReceiveCount` times;
//!   * SQS's limits are enforced: the characters of a body and of attribute
//!     values, a non-empty body, ten entries and 262144 bytes per batch.
//!
//! Queue names that begin with `fail-` answer with a failure on purpose
//! (throttling, an HTML error page, a body that is not JSON, a redirect, a
//! slow response), and a queue whose name begins with `partial` refuses the
//! batch entries whose id begins with `bad-` and forgets those that begin
//! with `lost-`.
//!
//! `Testing.Conformance.Mq` runs against it end to end, then the
//! backend-specific behaviour.
//!
//! The server runs on its own thread: a `Task.Run` worker on `--target dotnet`
//! (`spawn` is synchronous there), a `spawn` inside a `scope` on `--target
//! jvm`, a `Std.Task` scope thread on `--target native`. No assertion runs while it is waiting for a request: a scenario
//! returns its findings as lines and ends by asking the server to stop.
@test_module
package Mq.Sqs.FakeSqsTests

import Std.Core
import Std.Collections
import Std.Encoding as Enc
import Std.Environment as Env
import Std.Errors
import Std.File as File
import Std.Http
import Std.HttpServer
import Std.JsonValue
import Std.String as Str
import Std.Task as StdTask
import Std.Testing
import Std.Time as Time
import Std.Tls as Tls
import Testing.Conformance
import Testing.Conformance.Mq as MqConformance
import Aws.SigV4
import Mq
import Aws.Credentials
import Aws.Retry
import Mq.Sqs
import Mq.Sqs.Extras
import Mq.Sqs.Wire

// ─── The fake server's state ─────────────────────────────────────────────────
// Documentation-style values, not credentials of anything.
val ACCESS_KEY: String = "AKIAFAKEFAKEFAKEFAKE"

val SECRET_KEY: String = "fake/secret+key/for-tests-only"

val TEMP_ACCESS_KEY: String = "ASIAFAKETEMPORARYKEY"

val TEMP_SECRET_KEY: String = "fake/temporary+secret/for-tests-only"

val TEMP_TOKEN: String = "FakeSessionToken//for+tests/only=="

val REGION: String = "eu-test-1"

val ACCOUNT: String = "000000000000"

val STOP_PATH: String = "/__stop"

val LOG_PATH: String = "/__log"

// `GET /__lastkey`: the access key id that signed the last accepted request.
val LAST_KEY_PATH: String = "/__lastkey"

ref record FakeMessage {
  messageId: String
  body: String
  attributes: List[JsonField]
  groupId: String
  var visibleAt: Long
  var receiveCount: Int
  var handle: String
}

ref record FakeQueue {
  name: String
  fifo: Bool
  var attributes: Map[String, String]
  var messages: List[FakeMessage]
  // Deduplication id to the MessageId first stored under it.
  var seen: Map[String, String]
}

ref record FakeSqs {
  // `http` or `https`: the scheme of the queue URLs the server hands out.
  scheme: String
  port: Int
  var queues: Map[String, FakeQueue]
  var requestCount: Int
  var counter: Int
  var log: List[String]
  // Requests received per queue whose name makes the server misbehave.
  var attempts: Map[String, Int]
  var lastKeyId: String
}

func newFakeSqs(scheme: in String, port: in Int): FakeSqs {
  FakeSqs(
    scheme = scheme,
    port = port,
    queues = newMap(),
    requestCount = 0,
    counter = 0,
    log = newList(),
    attempts = newMap(),
    lastKeyId = ""
  )
}

// Counts a request to a misbehaving queue and returns how many it has had.
func countAttempt(sqs: in FakeSqs, name: in String): Int {
  var n = 1
  if sqs.attempts.containsKey(name) {
    val before: Int = sqs.attempts[name]
    n = before + 1
  }
  sqs.attempts[name] = n
  n
}

func nextId(sqs: in FakeSqs, prefix: in String): String {
  sqs.counter = sqs.counter + 1
  prefix + sqs.counter.toString()
}

// ─── Request inspection ──────────────────────────────────────────────────────
func headerValue(ctx: in HttpContext, name: in String): String {
  val wanted = Str.toLower(name)
  val entries = mapEntries(requestHeaders(ctx))
  var found = ""
  for e in entries {
    if Str.toLower(e.key) == wanted {
      found = e.value
    }
  }
  found
}

func textField(doc: in JsonValue, name: in String): String {
  match getString(doc, name) {
    case Some(v) -> v
    case None -> ""
  }
}

func intField(doc: in JsonValue, name: in String, fallback: in Int): Int {
  match getInt(doc, name) {
    case Some(n) -> n.toInt()
    case None -> fallback
  }
}

func hasField(doc: in JsonValue, name: in String): Bool {
  isSome(getField(doc, name))
}

func digits(text: in String, fallback: in Int): Int {
  if text.length == 0 or text.length > 9 {
    return fallback
  }
  var n = 0
  var i = 0
  while i < text.length {
    val u = Str.codeUnitAt(text, i)
    if u < 48 or u > 57 {
      return fallback
    }
    n = n * 10 + (u - 48)
    i = i + 1
  }
  n
}

// ─── Responses ───────────────────────────────────────────────────────────────
func jsonField(name: in String, value: in JsonValue): JsonField {
  JsonField(name = name, value = value)
}

// Qualified: on the JVM `Std.HttpServer` exports a `JObject` of its own.
func jsonObject(fields: in List[JsonField]): JsonValue {
  Std.JsonValue.JObject(fields = fields)
}

func jsonText(value: in String): JsonValue {
  JString(value = value)
}

func respondDocument(ctx: in HttpContext, fields: in List[JsonField]): Unit {
  val none: Map[String, String] = newMap()
  respondBytesWithHeaders(ctx, 200, "application/x-amz-json-1.0", Enc.encodeUtf8(writeValue(jsonObject(fields))), none)
}

// An action without output: 200 and no body, as Amazon SQS answers.
func respondNothing(ctx: in HttpContext): Unit {
  val none: Map[String, String] = newMap()
  val empty: slice[Byte] = []
  respondBytesWithHeaders(ctx, 200, "application/x-amz-json-1.0", empty, none)
}

func respondFault(ctx: in HttpContext, status: in Int, kind: in String, message: in String): Unit {
  val fields: List[JsonField] = newList()
  fields.add(jsonField("__type", jsonText(kind)))
  fields.add(jsonField("message", jsonText(message)))
  val none: Map[String, String] = newMap()
  respondBytesWithHeaders(
    ctx,
    status,
    "application/x-amz-json-1.0",
    Enc.encodeUtf8(writeValue(jsonObject(fields))),
    none
  )
}

func sqsFault(ctx: in HttpContext, code: in String, message: in String): Unit {
  respondFault(ctx, 400, "com.amazonaws.sqs#" + code, message)
}

// ─── Signature verification ──────────────────────────────────────────────────
// Seconds since the Unix epoch for `yyyymmddThhmmssZ` (the civil-from-days
// algorithm, proleptic Gregorian calendar).
func amzDateToEpochSeconds(amzDate: in String): Long {
  val y0 = digits(amzDate.substring(0, 4), 0)
  val m = digits(amzDate.substring(4, 2), 0)
  val d = digits(amzDate.substring(6, 2), 0)
  val y = if m <= 2 { y0 - 1 } else { y0 }
  val era = y / 400
  val yoe = y - era * 400
  val mp = if m > 2 { m - 3 } else { m + 9 }
  val doy = (153 * mp + 2) / 5 + d - 1
  val doe = yoe * 365 + yoe / 4 - yoe / 100 + doy
  val days = (era * 146097 + doe - 719468).toLong()
  days * 86400i64 +
    (digits(amzDate.substring(9, 2), 0) * 3600 + digits(amzDate.substring(11, 2), 0) * 60 + digits(amzDate.substring(13, 2), 0)).toLong()
}

// The value of `name=` in an `Authorization` header, up to the next comma.
func authField(authorization: in String, name: in String): String {
  val at = Str.indexOfRaw(authorization, name + "=")
  if at < 0 {
    return ""
  }
  val rest = authorization.substring(at + name.length + 1)
  val comma = Str.indexOfRaw(rest, ",")
  if comma >= 0 {
    rest.substring(0, comma)
  } else {
    rest
  }
}

record AuthProblem {
  kind: String
  message: String
}

func authProblem(kind: in String, message: in String): Option[AuthProblem] {
  Some(value = AuthProblem(kind = "com.amazon.coral.service#" + kind, message = message))
}

// Verify a request signed in its `Authorization` header: `None` when the
// signature is the one this server computes.
func verifyAuth(ctx: in HttpContext, body: in slice[Byte]): Option[AuthProblem] {
  val authorization = headerValue(ctx, "Authorization")
  if not Str.startsWith(authorization, "AWS4-HMAC-SHA256 ") {
    return authProblem("MissingAuthenticationTokenException", "Missing Authentication Token")
  }
  val credential = authField(authorization, "Credential")
  val signedNames = authField(authorization, "SignedHeaders")
  val signature = authField(authorization, "Signature")
  val amzDate = headerValue(ctx, "x-amz-date")
  if amzDate.length != 16 {
    return authProblem("IncompleteSignatureException", "x-amz-date is missing")
  }
  var creds = Credentials(accessKeyId = ACCESS_KEY, secretAccessKey = SECRET_KEY, sessionToken = "")
  if Str.startsWith(credential, TEMP_ACCESS_KEY + "/") {
    creds = Credentials(accessKeyId = TEMP_ACCESS_KEY, secretAccessKey = TEMP_SECRET_KEY, sessionToken = TEMP_TOKEN)
  } else if not Str.startsWith(credential, ACCESS_KEY + "/") {
    return authProblem("UnrecognizedClientException", "The security token included in the request is invalid.")
  }
  if credential != creds.accessKeyId + "/" + credentialScope(amzDate, REGION, "sqs") {
    return authProblem(
      "InvalidSignatureException",
      "Credential should be scoped to a valid region and to the service sqs."
    )
  }
  val skew = Time.nowEpochMillis() / 1000i64 - amzDateToEpochSeconds(amzDate)
  if skew > 900i64 or skew < -900i64 {
    return authProblem("InvalidSignatureException", "Signature expired: " + amzDate)
  }
  val required = ";" + signedNames + ";"
  if not Str.contains(required, ";host;") or not Str.contains(required, ";x-amz-date;") or not Str.contains(required, ";x-amz-target;") {
    return authProblem("InvalidSignatureException", "host, x-amz-date and x-amz-target must be signed")
  }
  if creds.sessionToken.length > 0 {
    if headerValue(ctx, "x-amz-security-token") != creds.sessionToken {
      return authProblem("UnrecognizedClientException", "The security token included in the request is invalid.")
    }
    if not Str.contains(required, ";x-amz-security-token;") {
      return authProblem("InvalidSignatureException", "x-amz-security-token must be signed")
    }
  }
  val headers: List[Pair] = newList()
  val names = Str.split(signedNames, ";")
  var i = 0
  while i < names.length {
    headers.add(Pair(name = names[i], value = headerValue(ctx, names[i])))
    i = i + 1
  }
  val noQuery: List[Pair] = newList()
  val expected = signRequest(
    requestMethod(ctx),
    canonicalUri(urlDecode(requestPath(ctx)), false),
    noQuery,
    headers,
    sha256Hex(body),
    creds,
    REGION,
    "sqs",
    amzDate
  )
  if expected.signature == signature {
    None
  } else {
    authProblem(
      "InvalidSignatureException",
      "The request signature we calculated does not match the signature you provided."
    )
  }
}

// ─── Limits ──────────────────────────────────────────────────────────────────
// Written out here, not taken from the library under test: the characters
// SQS accepts in a body and in a string attribute.
func acceptableText(text: in String): Bool {
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
      if u < 0x20 and u != 9 and u != 10 and u != 13 {
        return false
      }
      if (u >= 0xDC00 and u <= 0xDFFF) or u > 0xFFFD {
        return false
      }
      i = i + 1
    }
  }
  true
}

func acceptableAttributeName(name: in String): Bool {
  if name.length == 0 or name.length > 256 or Str.startsWith(name, ".") or Str.endsWith(name, ".") or Str.contains(name, "..") {
    return false
  }
  val lower = Str.toLower(name)
  if Str.startsWith(lower, "aws.") or Str.startsWith(lower, "amazon.") {
    return false
  }
  var i = 0
  while i < name.length {
    val u = Str.codeUnitAt(name, i)
    val ok = (u >= 48 and u <= 57) or (u >= 65 and u <= 90) or (u >= 97 and u <= 122) or u == 45 or u == 46 or u == 95
    if not ok {
      return false
    }
    i = i + 1
  }
  true
}

func acceptableToken(text: in String): Bool {
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

val MAX_BYTES: Int = 262144

// A message as one send asks for it, validated.
record Wanted {
  body: String
  attributes: List[JsonField]
  groupId: String
  dedupeId: String
  delaySeconds: Int
  bytes: Int
  // The `lyric.id` attribute, for the failure triggers.
  lyricId: String
}

record Refusal {
  code: String
  message: String
}

func refusal(code: in String, message: in String): Result[Wanted, Refusal] {
  Err(Refusal(code = code, message = message))
}

func wantedOf(queue: in FakeQueue, entry: in JsonValue): Result[Wanted, Refusal] {
  val body = match getString(entry, "MessageBody") {
    case Some(b) -> b
    case None -> return refusal("MissingParameter", "The request must contain the parameter MessageBody.")
  }
  if body.length == 0 {
    return refusal("MissingParameter", "The request must contain the parameter MessageBody.")
  }
  if not acceptableText(body) {
    return refusal("InvalidMessageContents", "Invalid characters found in the message body.")
  }
  var bytes = Enc.encodeUtf8(body).length
  var lyricId = ""
  val attributes: List[JsonField] = match getObject(entry, "MessageAttributes") {
    case Some(fields) -> fields
    case None -> newList()
  }
  if attributes.count > 10 {
    return refusal(
      "InvalidParameterValue",
      "Number of message attributes [" + attributes.count.toString() + "] exceeds the allowed maximum [10]."
    )
  }
  var i = 0
  while i < attributes.count {
    val name = attributes[i].name
    val dataType = textField(attributes[i].value, "DataType")
    val value = textField(attributes[i].value, "StringValue")
    if not acceptableAttributeName(name) {
      return refusal("InvalidParameterValue", "Message attribute name '" + name + "' is invalid.")
    }
    if not (Str.startsWith(dataType, "String") or Str.startsWith(dataType, "Number")) {
      return refusal("InvalidParameterValue", "The type of message attribute '" + name + "' is invalid.")
    }
    if value.length == 0 {
      return refusal("InvalidParameterValue", "Message attribute '" + name + "' must contain a non-empty value.")
    }
    if not acceptableText(value) {
      return refusal("InvalidMessageContents", "Invalid characters found in message attribute '" + name + "'.")
    }
    if name == "lyric.id" {
      lyricId = value
    }
    bytes = bytes + Enc.encodeUtf8(name).length + Enc.encodeUtf8(dataType).length + Enc.encodeUtf8(value).length
    i = i + 1
  }
  if bytes > MAX_BYTES {
    return refusal(
      "InvalidParameterValue",
      "One or more parameters are invalid. Reason: Message must be shorter than 262144 bytes."
    )
  }
  val groupId = textField(entry, "MessageGroupId")
  var dedupeId = textField(entry, "MessageDeduplicationId")
  if queue.fifo {
    if groupId.length == 0 {
      return refusal("MissingParameter", "The request must contain the parameter MessageGroupId.")
    }
    if not acceptableToken(groupId) {
      return refusal("InvalidParameterValue", "Value for parameter MessageGroupId is invalid.")
    }
    if dedupeId.length == 0 {
      if mapGet(queue.attributes, "ContentBasedDeduplication") == Some(value = "true") {
        dedupeId = sha256Hex(Enc.encodeUtf8(body))
      } else {
        return refusal(
          "InvalidParameterValue",
          "The queue should either have ContentBasedDeduplication enabled or MessageDeduplicationId provided explicitly"
        )
      }
    } else if not acceptableToken(dedupeId) {
      return refusal("InvalidParameterValue", "Value for parameter MessageDeduplicationId is invalid.")
    }
    if hasField(entry, "DelaySeconds") {
      return refusal(
        "InvalidParameterValue",
        "Value for parameter DelaySeconds is invalid. Reason: The request include parameter that is not valid for this queue type."
      )
    }
  } else if dedupeId.length > 0 {
    return refusal(
      "InvalidParameterValue",
      "Value for parameter MessageDeduplicationId is invalid. Reason: The request includes a parameter that is not valid for this queue type."
    )
  }
  val delay = intField(
    entry,
    "DelaySeconds",
    digits(match mapGet(queue.attributes, "DelaySeconds") { case Some(d) -> d; case None -> "0" }, 0)
  )
  if delay < 0 or delay > 900 {
    return refusal("InvalidParameterValue", "Value for parameter DelaySeconds is invalid.")
  }
  Ok(
    Wanted(
      body = body,
      attributes = attributes,
      groupId = groupId,
      dedupeId = dedupeId,
      delaySeconds = delay,
      bytes = bytes,
      lyricId = lyricId
    )
  )
}

// Store a validated message; returns its MessageId (the first one's, for a
// duplicate).
func store(sqs: in FakeSqs, queue: in FakeQueue, wanted: in Wanted): String {
  if queue.fifo and queue.seen.containsKey(wanted.dedupeId) {
    return queue.seen[wanted.dedupeId]
  }
  val id = nextId(sqs, "m-")
  if queue.fifo {
    queue.seen[wanted.dedupeId] = id
  }
  queue.messages.add(
    FakeMessage(
      messageId = id,
      body = wanted.body,
      attributes = wanted.attributes,
      groupId = wanted.groupId,
      visibleAt = Time.nowEpochMillis() + wanted.delaySeconds.toLong() * 1000i64,
      receiveCount = 0,
      handle = ""
    )
  )
  id
}

// ─── Actions ─────────────────────────────────────────────────────────────────
func queueUrlOf(sqs: in FakeSqs, name: in String): String {
  sqs.scheme + "://127.0.0.1:" + sqs.port.toString() + "/" + ACCOUNT + "/" + name
}

func queueArnOf(name: in String): String {
  "arn:aws:sqs:" + REGION + ":" + ACCOUNT + ":" + name
}

func createQueueAction(sqs: in FakeSqs, ctx: in HttpContext, doc: in JsonValue): Unit {
  val name = textField(doc, "QueueName")
  val asked: Map[String, String] = newMap()
  match getObject(doc, "Attributes") {
    case None -> ()
    case Some(fields) -> {
      var i = 0
      while i < fields.count {
        match asString(fields[i].value) {
          case Some(v) -> {
            asked[fields[i].name] = v
          }
          case None -> ()
        }
        i = i + 1
      }
    }
  }
  val fifo = mapGet(asked, "FifoQueue") == Some(value = "true")
  if fifo != Str.endsWith(name, ".fifo") {
    sqsFault(
      ctx,
      "InvalidParameterValue",
      "The name of a FIFO queue can only include alphanumeric characters, hyphens, or underscores, must end with .fifo suffix and be 1 to 80 in length."
    )
    return
  }
  if sqs.queues.containsKey(name) {
    val existing: FakeQueue = sqs.queues[name]
    for e in mapEntries(asked) {
      if mapGet(existing.attributes, e.key) != Some(value = e.value) {
        sqsFault(
          ctx,
          "QueueNameExists",
          "A queue already exists with the same name and a different value for attribute " + e.key
        )
        return
      }
    }
  } else {
    val attributes: Map[String, String] = newMap()
    attributes["VisibilityTimeout"] = "30"
    attributes["DelaySeconds"] = "0"
    attributes["QueueArn"] = queueArnOf(name)
    for e in mapEntries(asked) {
      attributes[e.key] = e.value
    }
    sqs.queues[name] = FakeQueue(name = name, fifo = fifo, attributes = attributes, messages = newList(), seen = newMap())
  }
  val fields: List[JsonField] = newList()
  fields.add(jsonField("QueueUrl", jsonText(queueUrlOf(sqs, name))))
  respondDocument(ctx, fields)
}

func sendAction(sqs: in FakeSqs, ctx: in HttpContext, queue: in FakeQueue, doc: in JsonValue): Unit {
  match wantedOf(queue, doc) {
    case Err(r) -> sqsFault(ctx, r.code, r.message)
    case Ok(wanted) -> {
      // The message is stored and the answer is lost: what a client sees as
      // a 500 after SQS has accepted the message.
      if Str.startsWith(queue.name, "lost-reply") and countAttempt(sqs, queue.name) == 1 {
        val _ = store(sqs, queue, wanted)
        sqs.log.add("stored, reply lost")
        respondFault(ctx, 500, "com.amazonaws.sqs#InternalError", "We encountered an internal error. Please try again.")
        return
      }
      val fields: List[JsonField] = newList()
      if not Str.startsWith(queue.name, "noid") {
        fields.add(jsonField("MessageId", jsonText(store(sqs, queue, wanted))))
      }
      respondDocument(ctx, fields)
    }
  }
}

// The bytes an entry counts against the batch limit, valid or not.
func entryBytes(entry: in JsonValue): Int {
  var bytes = Enc.encodeUtf8(textField(entry, "MessageBody")).length
  match getObject(entry, "MessageAttributes") {
    case None -> ()
    case Some(fields) -> {
      var i = 0
      while i < fields.count {
        bytes = bytes + Enc.encodeUtf8(fields[i].name).length + Enc.encodeUtf8(textField(fields[i].value, "DataType")).length +
          Enc.encodeUtf8(textField(fields[i].value, "StringValue")).length
        i = i + 1
      }
    }
  }
  bytes
}

func sendBatchAction(sqs: in FakeSqs, ctx: in HttpContext, queue: in FakeQueue, doc: in JsonValue): Unit {
  val entries: List[JsonValue] = match getArray(doc, "Entries") { case Some(items) -> items; case None -> newList() }
  if entries.count == 0 {
    sqsFault(ctx, "EmptyBatchRequest", "There should be at least one SendMessageBatchRequestEntry in the request.")
    return
  }
  if entries.count > 10 {
    sqsFault(
      ctx,
      "TooManyEntriesInBatchRequest",
      "Maximum number of entries per request are 10. You have sent " + entries.count.toString() + "."
    )
    return
  }
  val ids: Map[String, Bool] = newMap()
  val wanted: List[Result[Wanted, Refusal]] = newList()
  var total = 0
  var i = 0
  while i < entries.count {
    val id = textField(entries[i], "Id")
    if id.length == 0 or ids.containsKey(id) {
      sqsFault(ctx, "BatchEntryIdsNotDistinct", "Id " + id + " repeated.")
      return
    }
    ids[id] = true
    total = total + entryBytes(entries[i])
    wanted.add(wantedOf(queue, entries[i]))
    i = i + 1
  }
  if total > MAX_BYTES {
    sqsFault(
      ctx,
      "BatchRequestTooLong",
      "Batch requests cannot be longer than 262144 bytes. You have sent " + total.toString() + " bytes."
    )
    return
  }
  sqs.log.add("SendMessageBatch n=" + entries.count.toString() + " bytes=" + total.toString())
  val successful: List[JsonValue] = newList()
  val failed: List[JsonValue] = newList()
  var k = 0
  while k < entries.count {
    val id = textField(entries[k], "Id")
    match wanted[k] {
      case Err(r) -> {
        val f: List[JsonField] = newList()
        f.add(jsonField("Id", jsonText(id)))
        f.add(jsonField("Code", jsonText(r.code)))
        f.add(jsonField("Message", jsonText(r.message)))
        f.add(jsonField("SenderFault", JBool(value = true)))
        failed.add(jsonObject(f))
      }
      case Ok(w) -> {
        val partial = Str.startsWith(queue.name, "partial")
        if partial and Str.startsWith(w.lyricId, "bad-") {
          val f: List[JsonField] = newList()
          f.add(jsonField("Id", jsonText(id)))
          f.add(jsonField("Code", jsonText("InternalError")))
          f.add(jsonField("Message", jsonText("We encountered an internal error. Please try again.")))
          f.add(jsonField("SenderFault", JBool(value = false)))
          failed.add(jsonObject(f))
        } else if partial and Str.startsWith(w.lyricId, "lost-") {
          // Neither stored nor reported.
          ()
        } else {
          val s: List[JsonField] = newList()
          s.add(jsonField("Id", jsonText(id)))
          s.add(jsonField("MessageId", jsonText(store(sqs, queue, w))))
          successful.add(jsonObject(s))
        }
      }
    }
    k = k + 1
  }
  val fields: List[JsonField] = newList()
  if successful.count > 0 {
    fields.add(jsonField("Successful", JArray(items = successful)))
  }
  if failed.count > 0 {
    fields.add(jsonField("Failed", JArray(items = failed)))
  }
  respondDocument(ctx, fields)
}

func namesInclude(doc: in JsonValue, member: in String, wanted: in String): Bool {
  match getArray(doc, member) {
    case None -> false
    case Some(items) -> {
      var i = 0
      while i < items.count {
        val name = match asString(items[i]) { case Some(n) -> n; case None -> "" }
        if name == wanted or name == "All" or name == ".*" {
          return true
        }
        i = i + 1
      }
      false
    }
  }
}

func findQueueByArn(sqs: in FakeSqs, arn: in String): Option[FakeQueue] {
  for q in mapValues(sqs.queues).toArray() {
    if queueArnOf(q.name) == arn {
      return Some(value = q)
    }
  }
  None
}

// Messages `queue` may return now, in the order it returns them. Messages a
// redrive policy has given up on are moved to the dead-letter queue first.
func receivable(sqs: in FakeSqs, queue: in FakeQueue, now: in Long): List[FakeMessage] {
  match mapGet(queue.attributes, "RedrivePolicy") {
    case None -> ()
    case Some(policy) -> match parseValue(policy) {
      case Err(_) -> ()
      case Ok(doc) -> {
        val maxReceives = intField(doc, "maxReceiveCount", 1000)
        match findQueueByArn(sqs, textField(doc, "deadLetterTargetArn")) {
          case None -> ()
          case Some(target) -> {
            val kept: List[FakeMessage] = newList()
            var i = 0
            while i < queue.messages.count {
              val m = queue.messages[i]
              if m.visibleAt <= now and m.receiveCount >= maxReceives {
                m.handle = ""
                target.messages.add(m)
              } else {
                kept.add(m)
              }
              i = i + 1
            }
            queue.messages = kept
          }
        }
      }
    }
  }
  val acc: List[FakeMessage] = newList()
  if queue.fifo {
    val blocked: Map[String, Bool] = newMap()
    var i = 0
    while i < queue.messages.count {
      if queue.messages[i].visibleAt > now {
        blocked[queue.messages[i].groupId] = true
      }
      i = i + 1
    }
    var k = 0
    while k < queue.messages.count {
      if not blocked.containsKey(queue.messages[k].groupId) {
        acc.add(queue.messages[k])
      }
      k = k + 1
    }
  } else {
    // Newest first: a standard queue owes nobody an order.
    var k = queue.messages.count - 1
    while k >= 0 {
      if queue.messages[k].visibleAt <= now {
        acc.add(queue.messages[k])
      }
      k = k - 1
    }
  }
  acc
}

func receiveAction(sqs: in FakeSqs, ctx: in HttpContext, queue: in FakeQueue, doc: in JsonValue): Unit {
  val max = intField(doc, "MaxNumberOfMessages", 1)
  val wait = intField(doc, "WaitTimeSeconds", 0)
  val timeout = intField(
    doc,
    "VisibilityTimeout",
    digits(
      match mapGet(queue.attributes, "VisibilityTimeout") { case Some(v) -> v; case None -> "30" },
      30
    )
  )
  if max < 1 or max > 10 {
    sqsFault(
      ctx,
      "InvalidParameterValue",
      "Value " + max.toString() +
        " for parameter MaxNumberOfMessages is invalid. Reason: Must be between 1 and 10, if provided."
    )
    return
  }
  if wait < 0 or wait > 20 {
    sqsFault(
      ctx,
      "InvalidParameterValue",
      "Value " + wait.toString() +
        " for parameter WaitTimeSeconds is invalid. Reason: Must be >= 0 and <= 20, if provided."
    )
    return
  }
  if timeout < 0 or timeout > 43200 {
    sqsFault(
      ctx,
      "InvalidParameterValue",
      "Value " + timeout.toString() +
        " for parameter VisibilityTimeout is invalid. Reason: Must be between 0 and 43200, if provided."
    )
    return
  }
  val until = Time.nowEpochMillis() + wait.toLong() * 1000i64
  var ready = receivable(sqs, queue, Time.nowEpochMillis())
  while ready.count == 0 and Time.nowEpochMillis() < until {
    Time.sleepMillis(10)
    ready = receivable(sqs, queue, Time.nowEpochMillis())
  }
  val now = Time.nowEpochMillis()
  val messages: List[JsonValue] = newList()
  var i = 0
  while i < ready.count and i < max {
    val m = ready[i]
    m.receiveCount = m.receiveCount + 1
    m.visibleAt = now + timeout.toLong() * 1000i64
    m.handle = nextId(sqs, "rh-")
    val fields: List[JsonField] = newList()
    fields.add(jsonField("MessageId", jsonText(m.messageId)))
    fields.add(jsonField("ReceiptHandle", jsonText(m.handle)))
    fields.add(jsonField("Body", jsonText(m.body)))
    if namesInclude(doc, "MessageSystemAttributeNames", "ApproximateReceiveCount") or namesInclude(doc, "AttributeNames", "ApproximateReceiveCount") {
      val system: List[JsonField] = newList()
      system.add(jsonField("ApproximateReceiveCount", jsonText(m.receiveCount.toString())))
      fields.add(jsonField("Attributes", jsonObject(system)))
    }
    if m.attributes.count > 0 and namesInclude(doc, "MessageAttributeNames", "") {
      fields.add(jsonField("MessageAttributes", jsonObject(m.attributes)))
    }
    messages.add(jsonObject(fields))
    i = i + 1
  }
  sqs.log.add(
    "ReceiveMessage max=" + max.toString() + " wait=" + wait.toString() + " vt=" + timeout.toString() + " -> " +
      messages.count.toString()
  )
  // As Amazon SQS answers: no `Messages` member when there is none.
  val fields: List[JsonField] = newList()
  if messages.count > 0 {
    fields.add(jsonField("Messages", JArray(items = messages)))
  }
  respondDocument(ctx, fields)
}

func findByHandle(queue: in FakeQueue, handle: in String): Int {
  var i = 0
  while i < queue.messages.count {
    if handle.length > 0 and queue.messages[i].handle == handle {
      return i
    }
    i = i + 1
  }
  -1
}

func deleteAction(sqs: in FakeSqs, ctx: in HttpContext, queue: in FakeQueue, doc: in JsonValue): Unit {
  val at = findByHandle(queue, textField(doc, "ReceiptHandle"))
  if at < 0 {
    sqsFault(ctx, "ReceiptHandleIsInvalid", "The input receipt handle is invalid.")
    return
  }
  if queue.fifo and queue.messages[at].visibleAt <= Time.nowEpochMillis() {
    sqsFault(
      ctx,
      "InvalidParameterValue",
      "Value for parameter ReceiptHandle is invalid. Reason: The receipt handle has expired."
    )
    return
  }
  queue.messages.removeAt(at)
  sqs.log.add("DeleteMessage")
  respondNothing(ctx)
}

// "" when the visibility was changed, else the error code.
func changeVisibility(queue: in FakeQueue, handle: in String, seconds: in Int): String {
  val at = findByHandle(queue, handle)
  if at < 0 {
    return "ReceiptHandleIsInvalid"
  }
  val now = Time.nowEpochMillis()
  if queue.messages[at].visibleAt <= now {
    return "MessageNotInflight"
  }
  if seconds < 0 or seconds > 43200 {
    return "InvalidParameterValue"
  }
  queue.messages[at].visibleAt = now + seconds.toLong() * 1000i64
  ""
}

func changeVisibilityAction(sqs: in FakeSqs, ctx: in HttpContext, queue: in FakeQueue, doc: in JsonValue): Unit {
  val seconds = intField(doc, "VisibilityTimeout", -1)
  val problem = changeVisibility(queue, textField(doc, "ReceiptHandle"), seconds)
  if problem.length > 0 {
    sqsFault(ctx, problem, "ChangeMessageVisibility was refused: " + problem)
    return
  }
  sqs.log.add("ChangeMessageVisibility vt=" + seconds.toString())
  respondNothing(ctx)
}

func changeVisibilityBatchAction(sqs: in FakeSqs, ctx: in HttpContext, queue: in FakeQueue, doc: in JsonValue): Unit {
  val entries: List[JsonValue] = match getArray(doc, "Entries") { case Some(items) -> items; case None -> newList() }
  if entries.count == 0 or entries.count > 10 {
    sqsFault(
      ctx,
      if entries.count == 0 { "EmptyBatchRequest" } else { "TooManyEntriesInBatchRequest" },
      "1 to 10 entries"
    )
    return
  }
  val successful: List[JsonValue] = newList()
  val failed: List[JsonValue] = newList()
  var i = 0
  while i < entries.count {
    val id = textField(entries[i], "Id")
    val problem = changeVisibility(
      queue,
      textField(entries[i], "ReceiptHandle"),
      intField(entries[i], "VisibilityTimeout", -1)
    )
    val f: List[JsonField] = newList()
    f.add(jsonField("Id", jsonText(id)))
    if problem.length == 0 {
      successful.add(jsonObject(f))
    } else {
      f.add(jsonField("Code", jsonText(problem)))
      f.add(jsonField("SenderFault", JBool(value = true)))
      failed.add(jsonObject(f))
    }
    i = i + 1
  }
  sqs.log.add("ChangeMessageVisibilityBatch n=" + entries.count.toString() + " ok=" + successful.count.toString())
  val fields: List[JsonField] = newList()
  if successful.count > 0 {
    fields.add(jsonField("Successful", JArray(items = successful)))
  }
  if failed.count > 0 {
    fields.add(jsonField("Failed", JArray(items = failed)))
  }
  respondDocument(ctx, fields)
}

func attributesAction(sqs: in FakeSqs, ctx: in HttpContext, queue: in FakeQueue): Unit {
  val now = Time.nowEpochMillis()
  var visible = 0
  var hidden = 0
  var i = 0
  while i < queue.messages.count {
    if queue.messages[i].visibleAt <= now {
      visible = visible + 1
    } else {
      hidden = hidden + 1
    }
    i = i + 1
  }
  val inner: List[JsonField] = newList()
  for e in mapEntries(queue.attributes) {
    inner.add(jsonField(e.key, jsonText(e.value)))
  }
  inner.add(jsonField("ApproximateNumberOfMessages", jsonText(visible.toString())))
  inner.add(jsonField("ApproximateNumberOfMessagesNotVisible", jsonText(hidden.toString())))
  val fields: List[JsonField] = newList()
  fields.add(jsonField("Attributes", jsonObject(inner)))
  respondDocument(ctx, fields)
}

func setAttributesAction(ctx: in HttpContext, queue: in FakeQueue, doc: in JsonValue): Unit {
  match getObject(doc, "Attributes") {
    case None -> ()
    case Some(fields) -> {
      var i = 0
      while i < fields.count {
        match asString(fields[i].value) {
          case Some(v) -> {
            queue.attributes[fields[i].name] = v
          }
          case None -> ()
        }
        i = i + 1
      }
    }
  }
  respondNothing(ctx)
}

// The failures a `fail-` queue name asks for. True when one was served.
func servedFailure(ctx: in HttpContext, name: in String): Bool {
  val none: Map[String, String] = newMap()
  if name == "fail-throttle" {
    sqsFault(ctx, "RequestThrottled", "Rate exceeded")
  } else if name == "fail-html" {
    respondBytesWithHeaders(ctx, 502, "text/html", Enc.encodeUtf8("<html><body>502 Bad Gateway</body></html>"), none)
  } else if name == "fail-garbage" {
    respondBytesWithHeaders(ctx, 200, "application/x-amz-json-1.0", Enc.encodeUtf8("this is not JSON"), none)
  } else if name == "fail-redirect" {
    val headers: Map[String, String] = newMap()
    headers["Location"] = "http://127.0.0.1:1/elsewhere"
    val empty: slice[Byte] = []
    respondBytesWithHeaders(ctx, 307, "text/plain", empty, headers)
  } else if name == "fail-slow" {
    Time.sleepMillis(1500)
    respondNothing(ctx)
  } else {
    return false
  }
  true
}

func handle(sqs: in FakeSqs, ctx: in HttpContext): Unit {
  sqs.requestCount = sqs.requestCount + 1
  val body = requestBodyBytes(ctx)
  match verifyAuth(ctx, body) {
    case Some(problem) -> {
      sqs.log.add("refused " + problem.kind.substring(Str.lastIndexOfRaw(problem.kind, "#") + 1))
      respondFault(ctx, 403, problem.kind, problem.message)
      return
    }
    case None -> ()
  }
  sqs.lastKeyId = if Str.contains(headerValue(ctx, "Authorization"), "Credential=" + TEMP_ACCESS_KEY + "/") {
    TEMP_ACCESS_KEY
  } else {
    ACCESS_KEY
  }
  if requestMethod(ctx) != "POST" or requestPath(ctx) != "/" {
    respondFault(ctx, 404, "com.amazon.coral.service#UnknownOperationException", "POST / is the only route")
    return
  }
  if headerValue(ctx, "Content-Type") != "application/x-amz-json-1.0" {
    respondFault(
      ctx,
      400,
      "com.amazon.coral.service#SerializationException",
      "Content-Type must be application/x-amz-json-1.0, got " + headerValue(ctx, "Content-Type")
    )
    return
  }
  val target = headerValue(ctx, "X-Amz-Target")
  if not Str.startsWith(target, "AmazonSQS.") {
    respondFault(ctx, 400, "com.amazon.coral.service#UnknownOperationException", "unknown target " + target)
    return
  }
  val action = target.substring(10)
  val text = match Enc.tryDecodeUtf8(body) { case Some(t) -> t; case None -> "" }
  val doc = match parseValue(text) {
    case Ok(d) -> d
    case Err(_) -> {
      respondFault(ctx, 400, "com.amazon.coral.service#SerializationException", "the request body is not JSON")
      return
    }
  }
  if action == "CreateQueue" {
    createQueueAction(sqs, ctx, doc)
    return
  }
  if action == "GetQueueUrl" {
    val name = textField(doc, "QueueName")
    if sqs.queues.containsKey(name) {
      val fields: List[JsonField] = newList()
      fields.add(jsonField("QueueUrl", jsonText(queueUrlOf(sqs, name))))
      respondDocument(ctx, fields)
    } else {
      sqsFault(ctx, "QueueDoesNotExist", "The specified queue does not exist.")
    }
    return
  }
  val url = textField(doc, "QueueUrl")
  val prefix = queueUrlOf(sqs, "")
  val name = if Str.startsWith(url, prefix) { url.substring(prefix.length) } else { "" }
  if servedFailure(ctx, name) {
    return
  }
  // Throttled twice, then served.
  if Str.startsWith(name, "retry-throttle") and countAttempt(sqs, name) <= 2 {
    sqs.log.add("throttled " + action)
    sqsFault(ctx, "RequestThrottled", "Rate exceeded")
    return
  }
  // Fails once inside the server, before doing anything, then served.
  if Str.startsWith(name, "retry-500") and countAttempt(sqs, name) == 1 {
    sqs.log.add("failed " + action)
    respondFault(ctx, 500, "com.amazonaws.sqs#InternalError", "We encountered an internal error. Please try again.")
    return
  }
  if not sqs.queues.containsKey(name) {
    sqsFault(ctx, "QueueDoesNotExist", "The specified queue does not exist.")
    return
  }
  val queue: FakeQueue = sqs.queues[name]
  if action == "SendMessage" {
    sqs.log.add("SendMessage")
    sendAction(sqs, ctx, queue, doc)
  } else if action == "SendMessageBatch" {
    sendBatchAction(sqs, ctx, queue, doc)
  } else if action == "ReceiveMessage" {
    receiveAction(sqs, ctx, queue, doc)
  } else if action == "DeleteMessage" {
    deleteAction(sqs, ctx, queue, doc)
  } else if action == "ChangeMessageVisibility" {
    changeVisibilityAction(sqs, ctx, queue, doc)
  } else if action == "ChangeMessageVisibilityBatch" {
    changeVisibilityBatchAction(sqs, ctx, queue, doc)
  } else if action == "GetQueueAttributes" {
    attributesAction(sqs, ctx, queue)
  } else if action == "SetQueueAttributes" {
    setAttributesAction(ctx, queue, doc)
  } else if action == "PurgeQueue" {
    queue.messages = newList()
    respondNothing(ctx)
  } else if action == "DeleteQueue" {
    sqs.queues.remove(name)
    respondNothing(ctx)
  } else {
    respondFault(ctx, 400, "com.amazon.coral.service#UnknownOperationException", "unknown action " + action)
  }
}

func serveUntilStopped(l: in HttpListener, scheme: in String, port: in Int): Unit {
  val sqs = newFakeSqs(scheme, port)
  var running = true
  while running {
    val ctx = nextContext(l)
    if requestPath(ctx) == STOP_PATH {
      respondText(ctx, 200, "served " + sqs.requestCount.toString())
      running = false
    } else if requestPath(ctx) == LAST_KEY_PATH {
      respondText(ctx, 200, sqs.lastKeyId)
    } else if requestPath(ctx) == LOG_PATH {
      respondText(ctx, 200, Str.joinList("\n", sqs.log))
      sqs.log = newList()
    } else {
      handleGuarded(sqs, ctx)
    }
  }
}

// A bug in the fake must surface as a failed request, not as a server thread
// that died and a test run that waits for it. Native has no unwinding
// (D-N-003): there a bug in the fake ends the test process with its message.
@cfg(not(target = "native"))
func handleGuarded(sqs: in FakeSqs, ctx: in HttpContext): Unit {
  try {
    handle(sqs, ctx)
  } catch Bug as b {
    respondFault(ctx, 500, "com.amazonaws.sqs#FakeServerBug", b.message)
  }
}

@cfg(target = "native")
func handleGuarded(sqs: in FakeSqs, ctx: in HttpContext): Unit {
  handle(sqs, ctx)
}

// ─── Running a scenario against the server ───────────────────────────────────
@cfg(target = "dotnet")
extern type Task = "System.Threading.Tasks.Task"

@cfg(target = "dotnet")
@externStatic
@externTarget("System.Threading.Tasks.Task.Run")
func taskRun(work: in () -> Unit): Task = ()

@cfg(target = "dotnet")
@externInstance
@externTarget("System.Threading.Tasks.Task.Wait")
func taskWait(t: in Task): Unit = ()

func endpointOf(port: in Int): String {
  "http://127.0.0.1:" + port.toString()
}

@cfg(target = "dotnet")
func withFakeSqs(port: in Int, scenario: in (String) -> String): String {
  val l = startListener(endpointOf(port) + "/")
  val server = taskRun({ -> serveUntilStopped(l, "http", port) })
  val transcript = scenario(endpointOf(port))
  taskWait(server)
  stopListener(l)
  transcript
}

@cfg(target = "native")
func withFakeSqs(port: in Int, scenario: in (String) -> String): String {
  val l = startListener(endpointOf(port) + "/")
  val server = StdTask.makeScope()
  StdTask.scopeSpawn(server, { -> serveUntilStopped(l, "http", port) })
  val transcript = scenario(endpointOf(port))
  val _ = StdTask.awaitAll(server)
  stopListener(l)
  transcript
}

@cfg(target = "jvm")
func withFakeSqs(port: in Int, scenario: in (String) -> String): String {
  val l = startListener(endpointOf(port) + "/")
  var transcript = ""
  scope {
    val server = spawn serveUntilStopped(l, "http", port)
    transcript = scenario(endpointOf(port))
    await server
  }
  stopListener(l)
  transcript
}

func plainGet(url: in String): String {
  match await defaultClient().get(url) {
    case Err(e) -> "request failed: " + HttpError.message(e)
    case Ok(resp) -> match await HttpResponse.bodyText(resp) { case Ok(text) -> text; case Err(_) -> "unreadable" }
  }
}

// Asks the server to stop; the last thing every scenario does. Returns what
// the server said: how many SQS requests it served.
func stopFake(endpoint: in String): String {
  plainGet(endpoint + STOP_PATH)
}

// What the server did since the last call, one action per line, joined
// with `; `.
func serverLog(endpoint: in String): String {
  Str.replace(plainGet(endpoint + LOG_PATH), "\n", "; ")
}

// Forget what the server did so far, so the next `serverLog` shows only what
// follows.
func discardLog(endpoint: in String): Unit {
  val _ = serverLog(endpoint)
}

// A scenario's findings: `label=value` lines.
ref record Notes {
  var lines: List[String]
}

func newNotes(): Notes {
  Notes(lines = newList())
}

func note(n: in Notes, label: in String, value: in String): Unit {
  n.lines.add(label + "=" + Str.replace(value, "\n", "\\n"))
}

func finish(n: in Notes, endpoint: in String): String {
  note(n, "stopped", stopFake(endpoint))
  Str.joinList("\n", n.lines)
}

// A panic in a scenario is a finding on the managed targets, so that the fake
// is still stopped; on native it ends the test process with its message.
@cfg(not(target = "native"))
func scenarioGuarded(scenario: in (String, Notes) -> Unit, endpoint: in String, n: in Notes): Unit {
  try {
    scenario(endpoint, n)
  } catch Bug as b {
    note(n, "panicked", b.message)
  }
}

@cfg(target = "native")
func scenarioGuarded(scenario: in (String, Notes) -> Unit, endpoint: in String, n: in Notes): Unit {
  scenario(endpoint, n)
}

// The findings of `scenario`, run against a fresh fake on `port`.
func run(port: in Int, scenario: in (String, Notes) -> Unit): slice[String] {
  Str.split(
    withFakeSqs(port, { endpoint -> val n = newNotes(); scenarioGuarded(scenario, endpoint, n); finish(n, endpoint) }),
    "\n"
  )
}

func found(lines: in slice[String], label: in String): String {
  for line in lines {
    if Str.startsWith(line, label + "=") {
      return line.substring(label.length + 1)
    }
  }
  "<no '" + label + "' line; the scenario said: " + Str.join(" | ", lines) + ">"
}

func check(lines: in slice[String], label: in String, expected: in String): Unit {
  assertEqual(found(lines, label), expected, label)
}

func checkContains(lines: in slice[String], label: in String, part: in String): Unit {
  val value = found(lines, label)
  assertTrue(Str.contains(value, part), label + ": expected to contain \"" + part + "\", got \"" + value + "\"")
}

// Every scenario must have run to its end and stopped the server.
func expectClean(lines: in slice[String]): Unit {
  for line in lines {
    assertTrue(not Str.startsWith(line, "panicked="), "the scenario panicked: " + line)
  }
  assertTrue(
    Str.startsWith(found(lines, "stopped"), "served "),
    "the fake server stopped cleanly: " + found(lines, "stopped")
  )
}

// ─── Clients ─────────────────────────────────────────────────────────────────
func configFor(endpoint: in String, queueUrl: in String): SqsConfig {
  endpointConfig(endpoint, queueUrl, REGION, ACCESS_KEY, SECRET_KEY).withRequestTimeout(10000)
}

func adminFor(endpoint: in String): SqsClient {
  match openClient(configFor(endpoint, "")) {
    case Ok(c) -> c
    case Err(e) -> panic("openClient failed: " + e.text())
  }
}

func create(endpoint: in String, name: in String, settings: in QueueSettings): String {
  match createQueue(adminFor(endpoint), name, settings) {
    case Ok(url) -> url
    case Err(e) -> panic("createQueue " + name + " failed: " + e.text())
  }
}

func open(cfg: in SqsConfig): SqsQueue {
  match connectWith(cfg) {
    case Ok(q) -> q
    case Err(e) -> panic("connectWith failed: " + e)
  }
}

func standard(endpoint: in String, name: in String): SqsQueue {
  open(configFor(endpoint, create(endpoint, name, standardQueue())))
}

func message(id: in String, body: in String): Mq.Message {
  Mq.Message(id = id, body = body, headers = [], deliveryCount = 0)
}

ref record Slot {
  var message: Mq.Message
}

func keep(slot: in Slot, m: in Mq.Message): Result[Unit, String] {
  slot.message = m
  Ok(())
}

// What `consume` delivered within `timeoutMs`, as `id/body/deliveryCount`,
// or the error.
func take(q: in SqsQueue, timeoutMs: in Int): String {
  val slot = Slot(message = message("", ""))
  val result = q.consume(timeoutMs, { m -> keep(slot, m) })
  match result {
    case Ok(_) -> slot.message.id + "/" + slot.message.body + "/" + slot.message.deliveryCount.toString()
    case Err(e) -> e
  }
}

func outcome(r: in Result[Unit, String]): String {
  match r {
    case Ok(_) -> "ok"
    case Err(e) -> e
  }
}

func sqsOutcome(r: in Result[Unit, SqsError]): String {
  match r {
    case Ok(_) -> "ok"
    case Err(e) -> e.code + ": " + e.message
  }
}

func sentOutcome(r: in Result[String, SqsError]): String {
  match r {
    case Ok(id) -> "ok " + id
    case Err(e) -> e.code + ": " + e.message
  }
}

// ─── Conformance ─────────────────────────────────────────────────────────────
ref record QueueCounter {
  var next: Int
}

// A fresh, empty queue per conformance case.
func freshEnds(endpoint: in String, counter: in QueueCounter, fifo: in Bool): MqConformance.QueueEnds {
  counter.next = counter.next + 1
  val name = "conformance-" + counter.next.toString() + (if fifo { ".fifo" } else { "" })
  val url = create(endpoint, name, if fifo { fifoQueue() } else { standardQueue() })
  // A FIFO queue holds back the rest of a message group while one of its
  // messages is in flight, so a consumer that is to be handed the next
  // message before it settles the first must receive them together.
  val q = open(if fifo { configFor(endpoint, url).withReceiveBatch(10) } else { configFor(endpoint, url) })
  val publisher: Mq.MessageQueue = q
  val consumer: Mq.QueueConsumer = q
  MqConformance.QueueEnds(publisher = publisher, consumer = consumer)
}

func conformanceScenario(endpoint: in String, fifo: in Bool, into: in List[Checks]): String {
  val counter = QueueCounter(next = 0)
  val checks = MqConformance.runQueue({ -> freshEnds(endpoint, counter, fifo) })
  into.add(checks)
  stopFake(endpoint)
}

test "SqsQueue conforms to MessageQueue and QueueConsumer on FIFO queues, against the signature-checking fake" {
  val ran: List[Checks] = newList()
  val stopped = withFakeSqs(19801, { endpoint -> conformanceScenario(endpoint, true, ran) })
  assertEqualInt(ran.count, 1, "the suite ran: " + stopped)
  requireConformance(ran[0])
  assertEqualInt(caseCount(ran[0]), 11, "every conformance case ran")
  // 11 cases over HTTP: far more requests than cases, so the suite did not
  // pass by never reaching the server.
  assertTrue(
    stopped.startsWith("served ") and stopped.length >= 9,
    "the suite's requests reached the server: " + stopped
  )
}

test "on a standard queue only the two publish-order cases can fail, and do when SQS reorders" {
  val ran: List[Checks] = newList()
  val stopped = withFakeSqs(19802, { endpoint -> conformanceScenario(endpoint, false, ran) })
  assertEqualInt(ran.count, 1, "the suite ran: " + stopped)
  assertEqualInt(caseCount(ran[0]), 11, "every conformance case ran")
  val failed = failures(ran[0])
  // The fake returns a standard queue's newest message first. "a, b, c" then
  // arrives as "c, b, a" (two wrong positions) and "x, y" as "y, x" (two).
  assertEqualInt(failed.length, 4, "four expectations fail: " + Str.join(" | ", failed))
  for line in failed {
    assertTrue(Str.contains(line, "delivery order"), "only delivery order fails on a standard queue: " + line)
  }
}

// ─── A standard queue ────────────────────────────────────────────────────────
func standardScenario(endpoint: in String, n: in Notes): Unit {
  val q = standard(endpoint, "orders")
  val body = "line1\nline2\t\"quoted\" \\ back\u{0001}ctl café 世 😀"
  val sent = Mq.Message(
    id = "order 1/é",
    body = body,
    headers = [
      Mq.MessageHeader(key = "content-type", value = "text/plain"),
      Mq.MessageHeader(key = "k\"1", value = "v\n1")
    ],
    deliveryCount = 5
  )
  note(n, "publish", outcome(q.publish(sent)))
  val slot = Slot(message = message("", ""))
  note(n, "consume", outcome(q.consume(1000, { m -> keep(slot, m) })))
  val got = slot.message
  note(n, "id", got.id)
  note(n, "bodySame", if got.body == body { "yes" } else { "no: " + got.body })
  note(n, "headers", got.headers.length.toString())
  if got.headers.length == 2 {
    note(n, "header0", got.headers[0].key + "=" + got.headers[0].value)
    note(n, "header1", got.headers[1].key + "=" + got.headers[1].value)
  }
  note(n, "deliveryCount", got.deliveryCount.toString())
  note(n, "unsettled", unsettledCount(q).toString())
  note(n, "hidden", take(q, 0))
  note(n, "requeue", outcome(q.nack(got.id, true)))
  note(n, "redelivered", if Str.endsWith(take(q, 1000), "/1") { "deliveryCount 1" } else { "wrong" })
  note(n, "ack", outcome(q.ack(got.id)))
  note(n, "afterAck", take(q, 0))
  note(n, "secondAck", outcome(q.ack(got.id)))
  note(n, "unknownNack", outcome(q.nack("never-seen", true)))
  note(n, "emptyAck", outcome(q.ack("")))
  note(n, "emptyNack", outcome(q.nack("", false)))
  note(n, "unsettledAfter", unsettledCount(q).toString())

  note(n, "publishEmpty", outcome(q.publish(message("empty", ""))))
  note(n, "empty", take(q, 1000))
  note(n, "drop", outcome(q.nack("empty", false)))
  note(n, "afterDrop", take(q, 0))
  note(n, "log", serverLog(endpoint))

  note(n, "badId", outcome(q.publish(message("bell\u{0007}", "x"))))
  note(n, "sentNothing", serverLog(endpoint))
  q.close()
}

test "a standard queue: every character survives, deliveries are settled by id" {
  val lines = run(19803, { endpoint, n -> standardScenario(endpoint, n) })
  expectClean(lines)
  check(lines, "publish", "ok")
  check(lines, "consume", "ok")
  check(lines, "id", "order 1/é")
  check(lines, "bodySame", "yes")
  check(lines, "headers", "2")
  check(lines, "header0", "content-type=text/plain")
  check(lines, "header1", "k\"1=v\\n1")
  check(lines, "deliveryCount", "0")
  check(lines, "unsettled", "1")
  check(lines, "hidden", "timeout")
  check(lines, "requeue", "ok")
  check(lines, "redelivered", "deliveryCount 1")
  check(lines, "ack", "ok")
  check(lines, "afterAck", "timeout")
  check(
    lines,
    "secondAck",
    "sqs: no delivery of message 'order 1/é' is in flight: it was settled already, or its visibility timeout passed and SQS redelivers it"
  )
  checkContains(lines, "unknownNack", "no delivery of message 'never-seen' is in flight")
  check(lines, "emptyAck", "sqs: empty message id")
  check(lines, "emptyNack", "sqs: empty message id")
  check(lines, "unsettledAfter", "0")
  check(lines, "publishEmpty", "ok")
  check(lines, "empty", "empty//0")
  check(lines, "drop", "ok")
  check(lines, "afterDrop", "timeout")
  check(
    lines,
    "log",
    "SendMessage; ReceiveMessage max=1 wait=1 vt=30 -> 1; ReceiveMessage max=1 wait=0 vt=30 -> 0; " +
      "ChangeMessageVisibility vt=0; ReceiveMessage max=1 wait=1 vt=30 -> 1; DeleteMessage; " +
      "ReceiveMessage max=1 wait=0 vt=30 -> 0; SendMessage; ReceiveMessage max=1 wait=1 vt=30 -> 1; DeleteMessage; " +
      "ReceiveMessage max=1 wait=0 vt=30 -> 0"
  )
  check(
    lines,
    "badId",
    "sqs: cannot publish message 'bell\u{0007}': message id holds a character SQS cannot carry (control character or unpaired surrogate)"
  )
  check(lines, "sentNothing", "")
}

// ─── Batches ─────────────────────────────────────────────────────────────────
func batchScenario(endpoint: in String, n: in Notes): Unit {
  val q = open(configFor(endpoint, create(endpoint, "batch", standardQueue())).withReceiveBatch(10))
  val batch: List[Mq.Message] = newList()
  var i = 0
  while i < 25 {
    batch.add(message("b-" + i.toString(), "body " + i.toString()))
    i = i + 1
  }
  note(n, "publish25", outcome(q.publishBatch(batch.toArray())))
  val seen: Map[String, String] = newMap()
  var k = 0
  var problem = ""
  while k < 25 and problem.length == 0 {
    val slot = Slot(message = message("", ""))
    match q.consume(1000, { m -> keep(slot, m) }) {
      case Err(e) -> {
        problem = e
      }
      case Ok(_) -> {
        seen[slot.message.id] = slot.message.body
        match q.ack(slot.message.id) {
          case Ok(_) -> ()
          case Err(e) -> {
            problem = e
          }
        }
      }
    }
    k = k + 1
  }
  note(
    n,
    "received25",
    if problem.length > 0 { problem } else { mapSize(seen).toString() + " distinct, b-24=" + seen["b-24"] }
  )
  note(n, "drained", take(q, 0))
  note(n, "log25", serverLog(endpoint))

  // Three messages of 100000 bytes do not fit one 262144-byte request.
  val big = Str.repeat("x", 100000)
  note(n, "publishBig", outcome(q.publishBatch([message("big-1", big), message("big-2", big), message("big-3", big)])))
  note(n, "logBig", serverLog(endpoint))
  // A message SQS refuses for its size goes in a request of its own, after
  // the ones before it were accepted.
  val huge = Str.repeat("y", 300000)
  note(n, "publishHuge", outcome(q.publishBatch([message("ok-1", "x"), message("huge", huge), message("never", "x")])))
  note(n, "logHuge", serverLog(endpoint))
  note(n, "badId", outcome(q.publishBatch([message("fine", "x"), message("tab\u{0001}", "x")])))
  note(n, "logRefused", serverLog(endpoint))
  // A lower limit splits earlier.
  val small = open(configFor(endpoint, q.queue.url).copy(maxBatchBytes = 40))
  note(
    n,
    "publishSmall",
    outcome(
      small.publishBatch([message("s-1", "0123456789"), message("s-2", "0123456789"), message("s-3", "0123456789")])
    )
  )
  note(n, "logSmall", serverLog(endpoint))
  q.close()

  val partial = open(configFor(endpoint, create(endpoint, "partial-q", standardQueue())))
  val mixed: List[Mq.Message] = newList()
  for id in ["p-0", "bad-1", "p-2", "lost-3", "p-4", "p-5", "p-6", "p-7", "p-8", "p-9", "p-10", "p-11"] {
    mixed.add(message(id, "x"))
  }
  note(n, "partial", outcome(partial.publishBatch(mixed.toArray())))
  note(n, "logPartial", serverLog(endpoint))
  partial.close()
}

test "publishBatch splits at ten messages and at the byte limit, and names what was not accepted" {
  val lines = run(19804, { endpoint, n -> batchScenario(endpoint, n) })
  expectClean(lines)
  check(lines, "publish25", "ok")
  check(lines, "received25", "25 distinct, b-24=body 24")
  check(lines, "drained", "timeout")
  checkContains(
    lines,
    "log25",
    "SendMessageBatch n=10 bytes=230; SendMessageBatch n=10 bytes=250; SendMessageBatch n=5 bytes=125; "
  )
  check(lines, "publishBig", "ok")
  check(lines, "logBig", "SendMessageBatch n=2 bytes=200038; SendMessageBatch n=1 bytes=100019")
  check(
    lines,
    "publishHuge",
    "sqs: publishBatch: request 2 of 3: SendMessageBatch failed: BatchRequestTooLong (HTTP 400): " +
      "Batch requests cannot be longer than 262144 bytes. You have sent 300018 bytes.; none of 'huge' was accepted; " +
      "1 message of earlier requests stays published and 1 later message was not sent"
  )
  check(lines, "logHuge", "SendMessageBatch n=1 bytes=19")
  check(
    lines,
    "badId",
    "sqs: cannot publish message 'tab\u{0001}': message id holds a character SQS cannot carry (control character or unpaired surrogate); nothing was published"
  )
  check(lines, "logRefused", "")
  check(lines, "publishSmall", "ok")
  check(
    lines,
    "logSmall",
    "SendMessageBatch n=1 bytes=27; SendMessageBatch n=1 bytes=27; SendMessageBatch n=1 bytes=27"
  )
  check(
    lines,
    "partial",
    "sqs: publishBatch: 2 messages of request 1 of 2 not accepted: 'bad-1' (InternalError: We encountered an internal error. Please try again.), " +
      "'lost-3' (not acknowledged); the other 8 messages of that request were accepted; " +
      "0 messages of earlier requests stay published and 2 later messages were not sent"
  )
  check(lines, "logPartial", "SendMessageBatch n=10 bytes=185")
}

// ─── Long polling ────────────────────────────────────────────────────────────
func elapsedMs(since: in Long): Int {
  ((Time.monotonicNanos() - since) / 1000000i64).toInt()
}

func within(ms: in Int, low: in Int, high: in Int): String {
  if ms >= low and ms < high {
    "yes"
  } else {
    "no: " + ms.toString() + " ms"
  }
}

// The `wait=` values of the receives in a server log, in order.
func waits(log: in String): String {
  val acc: List[String] = newList()
  for entry in Str.split(log, "; ") {
    val at = Str.indexOfRaw(entry, "wait=")
    if at >= 0 {
      val rest = entry.substring(at + 5)
      acc.add(rest.substring(0, Str.indexOfRaw(rest, " ")))
    }
  }
  Str.joinList(",", acc)
}

func pollingScenario(endpoint: in String, n: in Notes): Unit {
  val q = standard(endpoint, "polling")
  var started = Time.monotonicNanos()
  note(n, "poll", take(q, 0))
  note(n, "pollFast", within(elapsedMs(started), 0, 500))
  note(n, "pollWaits", waits(serverLog(endpoint)))

  started = Time.monotonicNanos()
  note(n, "short", take(q, 250))
  note(n, "shortTime", within(elapsedMs(started), 250, 900))
  val shortWaits = waits(serverLog(endpoint))
  // Polls 100 ms apart for 250 ms: two to four of them, none of them long.
  note(
    n,
    "shortWaits",
    if shortWaits == "0,0" or shortWaits == "0,0,0" or shortWaits == "0,0,0,0" {
      "short polls only"
    } else {
      shortWaits
    }
  )

  started = Time.monotonicNanos()
  note(n, "mixed", take(q, 2300))
  note(n, "mixedTime", within(elapsedMs(started), 2300, 3200))
  val mixedWaits = waits(serverLog(endpoint))
  note(
    n,
    "mixedWaits",
    if Str.startsWith(mixedWaits, "2,0,0") { "a 2 s long poll, then short polls" } else { mixedWaits }
  )

  // A message that becomes visible during the wait ends it at once.
  started = Time.monotonicNanos()
  note(n, "delayed", sentOutcome(publishDelayed(q, message("late", "x"), 1)))
  note(n, "notYet", take(q, 0))
  note(n, "late", take(q, 10000))
  note(n, "lateTime", within(elapsedMs(started), 900, 3000))
  val lateLog = serverLog(endpoint)
  note(n, "lateWaits", waits(lateLog))
  note(n, "ackLate", outcome(q.ack("late")))
  q.close()
}

test "consume honours its timeout: whole seconds are long polls, the rest short polls" {
  val lines = run(19805, { endpoint, n -> pollingScenario(endpoint, n) })
  expectClean(lines)
  check(lines, "poll", "timeout")
  check(lines, "pollFast", "yes")
  check(lines, "pollWaits", "0")
  check(lines, "short", "timeout")
  check(lines, "shortTime", "yes")
  check(lines, "shortWaits", "short polls only")
  check(lines, "mixed", "timeout")
  check(lines, "mixedTime", "yes")
  check(lines, "mixedWaits", "a 2 s long poll, then short polls")
  check(lines, "delayed", "ok m-1")
  check(lines, "notYet", "timeout")
  check(lines, "late", "late/x/0")
  check(lines, "lateTime", "yes")
  check(lines, "lateWaits", "0,10")
  check(lines, "ackLate", "ok")
}

test "WaitTimeSeconds is the whole seconds left, capped at 20" {
  assertEqualInt(waitSecondsFor(0i64), 0, "nothing left")
  assertEqualInt(waitSecondsFor(-5i64), 0, "past the deadline")
  assertEqualInt(waitSecondsFor(999i64), 0, "under a second is a short poll")
  assertEqualInt(waitSecondsFor(1000i64), 1, "one second")
  assertEqualInt(waitSecondsFor(1999i64), 1, "never more than what is left")
  assertEqualInt(waitSecondsFor(19999i64), 19, "19 seconds")
  assertEqualInt(waitSecondsFor(20000i64), 20, "20 seconds")
  assertEqualInt(waitSecondsFor(20001i64), 20, "the SQS maximum")
  assertEqualInt(waitSecondsFor(3600000i64), 20, "an hour is many 20 s polls")
  assertEqualInt(waitSecondsFor(2147483647i64), 20, "the largest timeout")
}

// ─── Visibility ──────────────────────────────────────────────────────────────
func visibilityScenario(endpoint: in String, n: in Notes): Unit {
  val url = create(endpoint, "visibility", standardQueue())
  val q = open(configFor(endpoint, url).withVisibilityTimeout(1))
  note(n, "publish", outcome(q.publish(message("v", "x"))))
  note(n, "first", take(q, 1000))
  Time.sleepMillis(1200)
  note(n, "unsettledAfterExpiry", unsettledCount(q).toString())
  note(n, "staleAck", outcome(q.ack("v")))
  note(n, "second", take(q, 1000))
  note(n, "extend", sqsOutcome(extendVisibility(q, "v", 4)))
  Time.sleepMillis(1200)
  note(n, "stillHidden", take(q, 0))
  note(n, "unsettledExtended", unsettledCount(q).toString())
  note(n, "ackExtended", outcome(q.ack("v")))
  note(n, "extendUnknown", sqsOutcome(extendVisibility(q, "nobody", 4)))
  q.close()

  // Messages received together and not handed out in time are not handed
  // out late: their handles no longer hold them.
  val batched = open(configFor(endpoint, url).withVisibilityTimeout(1).withReceiveBatch(10))
  note(n, "publishTwo", outcome(batched.publishBatch([message("w1", "x"), message("w2", "x")])))
  val head = take(batched, 1000)
  note(
    n,
    "headIsOneOfThem",
    if Str.startsWith(head, "w1/x/0") or Str.startsWith(head, "w2/x/0") { "yes" } else { head }
  )
  note(n, "waiting", waitingCount(batched).toString())
  Time.sleepMillis(1200)
  discardLog(endpoint)
  val next = take(batched, 1000)
  note(n, "afterExpiry", if Str.endsWith(next, "/x/1") { "a redelivery from SQS" } else { next })
  note(n, "refetched", serverLog(endpoint))
  batched.close()
}

test "a visibility timeout ends a delivery: the handle is forgotten and SQS redelivers" {
  val lines = run(19806, { endpoint, n -> visibilityScenario(endpoint, n) })
  expectClean(lines)
  check(lines, "publish", "ok")
  check(lines, "first", "v/x/0")
  check(lines, "unsettledAfterExpiry", "0")
  checkContains(lines, "staleAck", "its visibility timeout passed and SQS redelivers it")
  check(lines, "second", "v/x/1")
  check(lines, "extend", "ok")
  check(lines, "stillHidden", "timeout")
  check(lines, "unsettledExtended", "1")
  check(lines, "ackExtended", "ok")
  check(lines, "extendUnknown", "NOT_IN_FLIGHT: no delivery of message 'nobody' is in flight")
  check(lines, "publishTwo", "ok")
  check(lines, "headIsOneOfThem", "yes")
  check(lines, "waiting", "1")
  check(lines, "afterExpiry", "a redelivery from SQS")
  check(lines, "refetched", "ReceiveMessage max=10 wait=1 vt=1 -> 2")
}

// ─── Dead letters ────────────────────────────────────────────────────────────
func deadLetterScenario(endpoint: in String, n: in Notes): Unit {
  val url = create(endpoint, "work", standardQueue())
  val dlqUrl = create(endpoint, "work-dead", standardQueue())
  val q = open(configFor(endpoint, url).withDeadLetterQueue(dlqUrl))
  val dlq = open(configFor(endpoint, dlqUrl))
  val poison = Mq.Message(
    id = "poison",
    body = "bad\u{0002}",
    headers = [Mq.MessageHeader(key = "k", value = "v")],
    deliveryCount = 0
  )
  note(n, "publish", outcome(q.publish(poison)))
  note(n, "take", if Str.startsWith(take(q, 1000), "poison/") { "poison" } else { "wrong" })
  discardLog(endpoint)
  note(n, "drop", outcome(q.nack("poison", false)))
  note(n, "dropLog", serverLog(endpoint))
  note(n, "sourceEmpty", take(q, 0))
  val slot = Slot(message = message("", ""))
  note(n, "dead", outcome(dlq.consume(1000, { m -> keep(slot, m) })))
  note(n, "deadId", slot.message.id)
  note(n, "deadBody", if slot.message.body == "bad\u{0002}" { "same" } else { "different" })
  note(n, "deadHeaders", slot.message.headers.length.toString())
  note(n, "ackDead", outcome(dlq.ack("poison")))
  q.close()
  dlq.close()

  // A dead-letter queue that does not exist: the message is not deleted.
  val orphan = open(configFor(endpoint, url).withDeadLetterQueue(endpoint + "/" + ACCOUNT + "/no-such-queue"))
  note(n, "publish2", outcome(orphan.publish(message("kept", "x"))))
  note(n, "take2", take(orphan, 1000))
  note(n, "dropFails", outcome(orphan.nack("kept", false)))
  note(n, "stillInFlight", unsettledCount(orphan).toString())
  note(n, "ackInstead", outcome(orphan.ack("kept")))
  orphan.close()

  // The redrive policy is SQS's own: it acts on receive counts.
  val client = adminFor(endpoint)
  val arn = match queueArn(client, dlqUrl) { case Ok(a) -> a; case Err(e) -> e.text() }
  note(n, "arn", arn)
  val redriven = open(configFor(endpoint, create(endpoint, "redriven", standardQueue().withRedrive(arn, 2))))
  val dead = open(configFor(endpoint, dlqUrl))
  note(n, "publish3", outcome(redriven.publish(message("r", "x"))))
  note(n, "receive1", take(redriven, 1000))
  note(n, "nack1", outcome(redriven.nack("r", true)))
  note(n, "receive2", take(redriven, 1000))
  note(n, "nack2", outcome(redriven.nack("r", true)))
  note(n, "receive3", take(redriven, 0))
  note(n, "movedBySqs", if Str.startsWith(take(dead, 1000), "r/x/") { "yes" } else { "no" })
  redriven.close()
  dead.close()
}

test "nack without requeue deletes, after copying to the configured dead-letter queue" {
  val lines = run(19807, { endpoint, n -> deadLetterScenario(endpoint, n) })
  expectClean(lines)
  check(lines, "publish", "ok")
  check(lines, "take", "poison")
  check(lines, "drop", "ok")
  check(lines, "dropLog", "SendMessage; DeleteMessage")
  check(lines, "sourceEmpty", "timeout")
  check(lines, "dead", "ok")
  check(lines, "deadId", "poison")
  check(lines, "deadBody", "same")
  check(lines, "deadHeaders", "1")
  check(lines, "ackDead", "ok")
  check(lines, "take2", "kept/x/0")
  check(
    lines,
    "dropFails",
    "sqs: SendMessage failed: QueueDoesNotExist (HTTP 400): The specified queue does not exist."
  )
  check(lines, "stillInFlight", "1")
  check(lines, "ackInstead", "ok")
  check(lines, "arn", "arn:aws:sqs:eu-test-1:000000000000:work-dead")
  check(lines, "receive1", "r/x/0")
  check(lines, "receive2", "r/x/1")
  check(lines, "receive3", "timeout")
  check(lines, "movedBySqs", "yes")
}

// ─── FIFO queues ─────────────────────────────────────────────────────────────
func fifoScenario(endpoint: in String, n: in Notes): Unit {
  val url = create(endpoint, "ordered.fifo", fifoQueue())
  val q = open(configFor(endpoint, url))
  for id in ["a", "b", "c"] {
    note(n, "publish-" + id, outcome(q.publish(message(id, id))))
  }
  note(n, "duplicate", outcome(q.publish(message("a", "a again"))))
  note(n, "head", take(q, 1000))
  note(n, "blocked", take(q, 0))
  note(n, "ack-a", outcome(q.ack("a")))
  note(n, "then", take(q, 1000))
  note(n, "ack-b", outcome(q.ack("b")))
  note(n, "last", take(q, 1000))
  note(n, "ack-c", outcome(q.ack("c")))
  note(n, "noDuplicate", take(q, 0))

  note(n, "publish-d", outcome(q.publish(message("d", "d"))))
  note(n, "publish-e", sentOutcome(publishToGroup(q, message("e", "e"), "other")))
  val one = take(q, 1000)
  val two = take(q, 1000)
  note(
    n,
    "twoGroups",
    if (one == "d/d/0" and two == "e/e/0") or (one == "e/e/0" and two == "d/d/0") { "both" } else { one + " " + two }
  )
  note(n, "ack-d", outcome(q.ack("d")))
  note(n, "ack-e", outcome(q.ack("e")))
  note(n, "delayRefused", sentOutcome(publishDelayed(q, message("f", "f"), 1)))
  // An id a deduplication id cannot be is replaced by its SHA-256.
  val longId = Str.repeat("long id ", 20)
  note(n, "dedupeLong", deduplicationIdFor(longId).length.toString())
  note(n, "dedupeShort", deduplicationIdFor("o-1"))
  note(n, "publishLong", outcome(q.publish(message(longId, "x"))))
  note(n, "publishLongAgain", outcome(q.publish(message(longId, "y"))))
  note(n, "long", if take(q, 1000) == longId + "/x/0" { "the first one" } else { "wrong" })
  note(n, "ack-long", outcome(q.ack(longId)))
  note(n, "longOnce", take(q, 0))
  q.close()

  // Deduplication.Unique: the same id twice is two messages.
  val unique = open(configFor(endpoint, url).withDeduplication(Deduplication.Unique).withReceiveBatch(10))
  note(n, "unique", outcome(unique.publishBatch([message("u", "1"), message("u", "2")])))
  note(n, "unique1", take(unique, 1000))
  note(n, "unique2", take(unique, 1000))
  note(n, "ackUnique1", outcome(unique.ack("u")))
  note(n, "ackUnique2", outcome(unique.ack("u")))
  note(n, "ackUnique3", if Str.contains(outcome(unique.ack("u")), "no delivery") { "no third" } else { "wrong" })

  // A receive batch keeps FIFO order across a requeue.
  note(n, "three", outcome(unique.publishBatch([message("1", "x"), message("2", "x"), message("3", "x")])))
  note(n, "first", take(unique, 1000))
  note(n, "waiting", waitingCount(unique).toString())
  discardLog(endpoint)
  note(n, "requeue", outcome(unique.nack("1", true)))
  note(n, "requeueLog", serverLog(endpoint))
  note(n, "waitingAfter", waitingCount(unique).toString())
  note(n, "again", take(unique, 1000) + " " + take(unique, 1000) + " " + take(unique, 1000))
  unique.close()

  // Deduplication.ContentBased needs the queue attribute.
  val needsId = open(configFor(endpoint, url).withDeduplication(Deduplication.ContentBased))
  note(n, "contentRefused", outcome(needsId.publish(message("cb", "x"))))
  needsId.close()
  val cbUrl = create(endpoint, "content.fifo", fifoQueue().copy(contentBasedDeduplication = true))
  val content = open(configFor(endpoint, cbUrl).withDeduplication(Deduplication.ContentBased).withMessageGroup("g/1"))
  note(n, "content1", outcome(content.publish(message("cb-1", "same body"))))
  note(n, "content2", outcome(content.publish(message("cb-2", "same body"))))
  note(n, "contentFirst", take(content, 1000))
  note(n, "ackContent", outcome(content.ack("cb-1")))
  note(n, "contentOnce", take(content, 0))
  content.close()
}

test "a FIFO queue: order, group blocking, deduplication" {
  val lines = run(19808, { endpoint, n -> fifoScenario(endpoint, n) })
  expectClean(lines)
  check(lines, "duplicate", "ok")
  check(lines, "head", "a/a/0")
  check(lines, "blocked", "timeout")
  check(lines, "then", "b/b/0")
  check(lines, "last", "c/c/0")
  check(lines, "noDuplicate", "timeout")
  checkContains(lines, "publish-e", "ok m-")
  check(lines, "twoGroups", "both")
  checkContains(
    lines,
    "delayRefused",
    "InvalidParameterValue: SendMessage failed: InvalidParameterValue (HTTP 400): Value for parameter DelaySeconds is invalid."
  )
  check(lines, "dedupeLong", "64")
  check(lines, "dedupeShort", "o-1")
  check(lines, "publishLong", "ok")
  check(lines, "publishLongAgain", "ok")
  check(lines, "long", "the first one")
  check(lines, "longOnce", "timeout")
  check(lines, "unique", "ok")
  check(lines, "unique1", "u/1/0")
  check(lines, "unique2", "u/2/0")
  check(lines, "ackUnique1", "ok")
  check(lines, "ackUnique2", "ok")
  check(lines, "ackUnique3", "no third")
  check(lines, "three", "ok")
  check(lines, "first", "1/x/0")
  check(lines, "waiting", "2")
  check(lines, "requeue", "ok")
  check(lines, "requeueLog", "ChangeMessageVisibility vt=0; ChangeMessageVisibilityBatch n=2 ok=2")
  check(lines, "waitingAfter", "0")
  check(lines, "again", "1/x/1 2/x/1 3/x/1")
  checkContains(
    lines,
    "contentRefused",
    "The queue should either have ContentBasedDeduplication enabled or MessageDeduplicationId provided explicitly"
  )
  check(lines, "content1", "ok")
  check(lines, "content2", "ok")
  check(lines, "contentFirst", "cb-1/same body/0")
  check(lines, "contentOnce", "timeout")
}

// ─── Auto acknowledgement, close ─────────────────────────────────────────────
func refuse(m: in Mq.Message): Result[Unit, String] {
  Err("handler refused " + m.id)
}

func autoScenario(endpoint: in String, n: in Notes): Unit {
  val url = create(endpoint, "auto", standardQueue())
  val q = open(configFor(endpoint, url).withAckMode(Mq.AckMode.Auto))
  note(n, "publish", outcome(q.publish(message("auto", "x"))))
  val failed = q.consume(1000, { m -> refuse(m) })
  note(n, "handlerError", outcome(failed))
  note(n, "redelivered", take(q, 1000))
  note(n, "unsettled", unsettledCount(q).toString())
  note(n, "ackNoop", outcome(q.ack("auto")))
  note(n, "nackNoop", outcome(q.nack("anything", true)))
  note(n, "emptyAck", outcome(q.ack("")))
  note(n, "gone", take(q, 0))
  note(n, "log", serverLog(endpoint))
  q.close()

  // Manual mode: a failing handler leaves the delivery to the caller.
  val manual = open(configFor(endpoint, url))
  note(n, "publish2", outcome(manual.publish(message("m", "x"))))
  val refused = manual.consume(1000, { m -> refuse(m) })
  note(n, "manualError", outcome(refused))
  note(n, "manualUnsettled", unsettledCount(manual).toString())
  note(n, "manualNack", outcome(manual.nack("m", true)))
  manual.close()

  // close returns what the consumer still holds.
  val holder = open(configFor(endpoint, url).withReceiveBatch(10))
  note(n, "publish3", outcome(holder.publishBatch([message("c1", "x"), message("c2", "x")])))
  val held = take(holder, 1000)
  note(n, "heldOne", if Str.endsWith(held, "/x/0") or Str.endsWith(held, "/x/1") { "yes" } else { held })
  discardLog(endpoint)
  holder.close()
  holder.close()
  note(n, "closeLog", serverLog(endpoint))
  note(n, "open", if isOpen(holder) { "open" } else { "closed" })
  note(n, "publishClosed", outcome(holder.publish(message("x", "x"))))
  note(n, "batchClosed", outcome(holder.publishBatch([message("x", "x")])))
  note(n, "consumeClosed", take(holder, 0))
  note(n, "ackClosed", outcome(holder.ack("c1")))
  note(n, "extendClosed", sqsOutcome(extendVisibility(holder, "c1", 5)))
  val other = open(configFor(endpoint, url).withReceiveBatch(10))
  var count = 0
  while Str.contains(take(other, 0), "/x/") {
    count = count + 1
  }
  note(n, "availableAtOnce", count.toString())
  other.close()
}

test "AckMode.Auto settles in consume; close returns what the consumer holds" {
  val lines = run(19809, { endpoint, n -> autoScenario(endpoint, n) })
  expectClean(lines)
  check(lines, "handlerError", "handler refused auto")
  check(lines, "redelivered", "auto/x/1")
  check(lines, "unsettled", "0")
  check(lines, "ackNoop", "ok")
  check(lines, "nackNoop", "ok")
  check(lines, "emptyAck", "sqs: empty message id")
  check(lines, "gone", "timeout")
  check(
    lines,
    "log",
    "SendMessage; ReceiveMessage max=1 wait=1 vt=30 -> 1; ChangeMessageVisibility vt=0; " +
      "ReceiveMessage max=1 wait=1 vt=30 -> 1; DeleteMessage; ReceiveMessage max=1 wait=0 vt=30 -> 0"
  )
  check(lines, "manualError", "handler refused m")
  check(lines, "manualUnsettled", "1")
  check(lines, "manualNack", "ok")
  check(lines, "heldOne", "yes")
  check(lines, "closeLog", "ChangeMessageVisibilityBatch n=3 ok=3")
  check(lines, "open", "closed")
  check(lines, "publishClosed", "sqs: the queue is closed")
  check(lines, "batchClosed", "sqs: the queue is closed")
  check(lines, "consumeClosed", "sqs: the queue is closed")
  check(lines, "ackClosed", "sqs: the queue is closed")
  check(lines, "extendClosed", "CLOSED: the queue is closed")
  check(lines, "availableAtOnce", "3")
}

// ─── Signatures and errors ───────────────────────────────────────────────────
func publishTo(cfg: in SqsConfig): String {
  match connectWith(cfg) {
    case Err(e) -> "config: " + e
    case Ok(q) -> outcome(q.publish(message("probe", "x")))
  }
}

func errorScenario(endpoint: in String, n: in Notes): Unit {
  val url = create(endpoint, "secure", standardQueue())
  val good = configFor(endpoint, url)
  note(n, "good", publishTo(good))
  note(n, "wrongSecret", publishTo(good.copy(secretAccessKey = "not the secret")))
  note(n, "unknownKey", publishTo(good.copy(accessKeyId = "AKIAUNKNOWNUNKNOWN00")))
  note(n, "wrongRegion", publishTo(good.copy(region = "eu-other-9")))
  val temporary = good.copy(accessKeyId = TEMP_ACCESS_KEY, secretAccessKey = TEMP_SECRET_KEY)
  note(n, "temporary", publishTo(temporary.withSessionToken(TEMP_TOKEN)))
  note(n, "temporaryNoToken", publishTo(temporary))
  note(n, "temporaryWrongToken", publishTo(temporary.withSessionToken("stale")))
  note(n, "authLog", serverLog(endpoint))

  val base = endpoint + "/" + ACCOUNT + "/"
  note(n, "missingQueue", publishTo(configFor(endpoint, base + "missing")))
  note(n, "missingQueueConsume", take(open(configFor(endpoint, base + "missing")), 0))
  note(n, "throttled", publishTo(configFor(endpoint, base + "fail-throttle")))
  note(n, "html", publishTo(configFor(endpoint, base + "fail-html")))
  note(n, "garbage", publishTo(configFor(endpoint, base + "fail-garbage")))
  note(n, "redirect", publishTo(configFor(endpoint, base + "fail-redirect")))
  note(n, "slow", publishTo(configFor(endpoint, base + "fail-slow").withRequestTimeout(300)))
  note(n, "noMessageId", publishTo(configFor(endpoint, create(endpoint, "noid", standardQueue()))))
  note(
    n,
    "nothingListening",
    publishTo(configFor("http://127.0.0.1:1", "http://127.0.0.1:1/" + ACCOUNT + "/q").withRequestTimeout(3000))
  )
  val batchFail = open(configFor(endpoint, base + "fail-throttle"))
  note(n, "batchRefused", outcome(batchFail.publishBatch([message("t-1", "x"), message("t-2", "x")])))
  val batchSlow = open(configFor(endpoint, base + "fail-slow").withRequestTimeout(300))
  note(n, "batchUnknown", outcome(batchSlow.publishBatch([message("u-1", "x")])))
  // Let the slow responses the client gave up on drain before the server
  // is asked to stop.
  Time.sleepMillis(3200)

  // What the client reports as codes.
  val client = adminFor(endpoint)
  match queueUrl(client, "missing") {
    case Ok(u) -> note(n, "lookupCode", "ok " + u)
    case Err(e) -> note(n, "lookupCode", e.code + "/" + e.status.toString())
  }
  match queueAttributes(client, base + "fail-html") {
    case Ok(_) -> note(n, "htmlCode", "ok")
    case Err(e) -> note(n, "htmlCode", e.code + "/" + e.status.toString())
  }
  match queueAttributes(client, base + "fail-garbage") {
    case Ok(_) -> note(n, "garbageCode", "ok")
    case Err(e) -> note(n, "garbageCode", e.code + "/" + e.status.toString())
  }
}

test "every request is signed for sqs, and failures keep SQS's code" {
  val lines = run(19810, { endpoint, n -> errorScenario(endpoint, n) })
  expectClean(lines)
  check(lines, "good", "ok")
  check(
    lines,
    "wrongSecret",
    "sqs: SendMessage failed: InvalidSignatureException (HTTP 403): The request signature we calculated does not match the signature you provided."
  )
  check(
    lines,
    "unknownKey",
    "sqs: SendMessage failed: UnrecognizedClientException (HTTP 403): The security token included in the request is invalid."
  )
  check(
    lines,
    "wrongRegion",
    "sqs: SendMessage failed: InvalidSignatureException (HTTP 403): Credential should be scoped to a valid region and to the service sqs."
  )
  check(lines, "temporary", "ok")
  checkContains(lines, "temporaryNoToken", "UnrecognizedClientException (HTTP 403)")
  checkContains(lines, "temporaryWrongToken", "UnrecognizedClientException (HTTP 403)")
  check(
    lines,
    "authLog",
    "SendMessage; refused InvalidSignatureException; refused UnrecognizedClientException; refused InvalidSignatureException; " +
      "SendMessage; refused UnrecognizedClientException; refused UnrecognizedClientException"
  )
  check(
    lines,
    "missingQueue",
    "sqs: SendMessage failed: QueueDoesNotExist (HTTP 400): The specified queue does not exist."
  )
  check(
    lines,
    "missingQueueConsume",
    "sqs: ReceiveMessage failed: QueueDoesNotExist (HTTP 400): The specified queue does not exist."
  )
  check(lines, "throttled", "sqs: SendMessage failed: RequestThrottled (HTTP 400): Rate exceeded")
  check(lines, "html", "sqs: SendMessage failed: SQS answered HTTP 502")
  checkContains(lines, "garbage", "sqs: SendMessage: the response is not JSON: ")
  check(lines, "redirect", "sqs: SendMessage failed: SQS answered HTTP 307")
  check(lines, "slow", "sqs: SendMessage timed out after 300 ms at http://127.0.0.1:19810")
  check(lines, "noMessageId", "sqs: SendMessage: the response has no MessageId")
  checkContains(lines, "nothingListening", "sqs: SendMessage got no response from http://127.0.0.1:1: ")
  check(
    lines,
    "batchRefused",
    "sqs: publishBatch: request 1 of 1: SendMessageBatch failed: RequestThrottled (HTTP 400): Rate exceeded; " +
      "none of 't-1', 't-2' was accepted; 0 messages of earlier requests stay published and 0 later messages were not sent"
  )
  check(
    lines,
    "batchUnknown",
    "sqs: publishBatch: request 1 of 1: SendMessageBatch timed out after 300 ms at http://127.0.0.1:19810; " +
      "it is not known whether SQS accepted 'u-1'; 0 messages of earlier requests stay published and 0 later messages were not sent"
  )
  check(lines, "lookupCode", "QueueDoesNotExist/400")
  check(lines, "htmlCode", "HTTP_502/502")
  check(lines, "garbageCode", "MALFORMED_RESPONSE/200")
}

// ─── Queue management ────────────────────────────────────────────────────────
func attribute(client: in SqsClient, url: in String, name: in String): String {
  match queueAttributes(client, url) {
    case Err(e) -> e.code
    case Ok(attributes) -> match mapGet(attributes, name) { case Some(v) -> v; case None -> "<absent>" }
  }
}

func managementScenario(endpoint: in String, n: in Notes): Unit {
  val client = adminFor(endpoint)
  note(n, "badName", sentOutcome(createQueue(client, "no spaces", standardQueue())))
  note(n, "fifoWithoutSuffix", sentOutcome(createQueue(client, "plain", fifoQueue())))
  note(n, "suffixWithoutFifo", sentOutcome(createQueue(client, "plain.fifo", standardQueue())))
  note(
    n,
    "badRedrive",
    sentOutcome(createQueue(client, "r", standardQueue().copy(deadLetterTargetArn = "arn:x", maxReceiveCount = 0)))
  )
  note(n, "nothingSent", serverLog(endpoint))
  val settings = standardQueue().copy(visibilityTimeoutSeconds = 45, delaySeconds = 0, messageRetentionSeconds = 3600)
  note(n, "create", sentOutcome(createQueue(client, "managed", settings)))
  note(n, "createAgain", sentOutcome(createQueue(client, "managed", settings)))
  note(n, "createDifferent", sentOutcome(createQueue(client, "managed", settings.copy(visibilityTimeoutSeconds = 60))))
  note(n, "lookup", sentOutcome(queueUrl(client, "managed")))
  val url = endpoint + "/" + ACCOUNT + "/managed"
  note(n, "visibility", attribute(client, url, "VisibilityTimeout"))
  note(n, "retention", attribute(client, url, "MessageRetentionPeriod"))
  note(n, "arn", sentOutcome(queueArn(client, url)))
  val q = open(configFor(endpoint, url))
  note(n, "publish", outcome(q.publishBatch([message("p1", "x"), message("p2", "x")])))
  note(n, "count", attribute(client, url, "ApproximateNumberOfMessages"))
  note(n, "setVisibility", sqsOutcome(setVisibilityTimeout(client, url, 20)))
  note(n, "visibilityNow", attribute(client, url, "VisibilityTimeout"))
  note(n, "setRedrive", sqsOutcome(setRedrivePolicy(client, url, "arn:aws:sqs:eu-test-1:000000000000:dead", 5)))
  note(n, "redrive", attribute(client, url, "RedrivePolicy"))
  note(n, "purge", sqsOutcome(purgeQueue(client, url)))
  note(n, "purged", take(q, 0))
  note(n, "delete", sqsOutcome(deleteQueue(client, url)))
  note(n, "lookupDeleted", sentOutcome(queueUrl(client, "managed")))
  note(n, "purgeDeleted", sqsOutcome(purgeQueue(client, url)))
  note(n, "fifo", sentOutcome(createQueue(client, "managed.fifo", fifoQueue().copy(contentBasedDeduplication = true))))
  note(
    n,
    "fifoAttribute",
    attribute(client, url + ".fifo", "FifoQueue") + "/" + attribute(client, url + ".fifo", "ContentBasedDeduplication")
  )
  q.close()
}

test "queue management: create, look up, attributes, redrive policy, purge, delete" {
  val lines = run(19811, { endpoint, n -> managementScenario(endpoint, n) })
  expectClean(lines)
  check(
    lines,
    "badName",
    "INVALID_ARGUMENT: invalid queue name 'no spaces': 1 to 80 letters, digits, '-' and '_', '.fifo' for a FIFO queue"
  )
  check(
    lines,
    "fifoWithoutSuffix",
    "INVALID_ARGUMENT: a FIFO queue's name ends in '.fifo' and a standard queue's does not: 'plain'"
  )
  check(
    lines,
    "suffixWithoutFifo",
    "INVALID_ARGUMENT: a FIFO queue's name ends in '.fifo' and a standard queue's does not: 'plain.fifo'"
  )
  check(lines, "badRedrive", "INVALID_ARGUMENT: maxReceiveCount must be 1 to 1000, got 0")
  check(lines, "nothingSent", "")
  check(lines, "create", "ok http://127.0.0.1:19811/000000000000/managed")
  check(lines, "createAgain", "ok http://127.0.0.1:19811/000000000000/managed")
  checkContains(lines, "createDifferent", "QueueNameExists: CreateQueue failed: QueueNameExists (HTTP 400)")
  check(lines, "lookup", "ok http://127.0.0.1:19811/000000000000/managed")
  check(lines, "visibility", "45")
  check(lines, "retention", "3600")
  check(lines, "arn", "ok arn:aws:sqs:eu-test-1:000000000000:managed")
  check(lines, "publish", "ok")
  check(lines, "count", "2")
  check(lines, "setVisibility", "ok")
  check(lines, "visibilityNow", "20")
  check(lines, "setRedrive", "ok")
  check(lines, "redrive", "{\"deadLetterTargetArn\":\"arn:aws:sqs:eu-test-1:000000000000:dead\",\"maxReceiveCount\":5}")
  check(lines, "purge", "ok")
  check(lines, "purged", "timeout")
  check(lines, "delete", "ok")
  checkContains(lines, "lookupDeleted", "QueueDoesNotExist: GetQueueUrl failed: QueueDoesNotExist (HTTP 400)")
  checkContains(lines, "purgeDeleted", "QueueDoesNotExist: PurgeQueue failed")
  check(lines, "fifo", "ok http://127.0.0.1:19811/000000000000/managed.fifo")
  check(lines, "fifoAttribute", "true/true")
}

// ─── Messages from other producers ───────────────────────────────────────────
func foreignScenario(endpoint: in String, n: in Notes): Unit {
  val url = create(endpoint, "foreign", standardQueue())
  val q = open(configFor(endpoint, url))
  // Sent the way another SDK would: no lyric.* attributes.
  val plain = "{\"QueueUrl\":\"" + url + "\",\"MessageBody\":\"from elsewhere\",\"MessageAttributes\":{" +
    "\"trace\":{\"DataType\":\"String\",\"StringValue\":\"t-1\"}," +
    "\"attempt\":{\"DataType\":\"Number\",\"StringValue\":\"3\"}}}"
  match q.client.call("SendMessage", plain, 0) {
    case Ok(_) -> note(n, "sendForeign", "ok")
    case Err(e) -> note(n, "sendForeign", e.text())
  }
  val slot = Slot(message = message("", ""))
  note(n, "consume", outcome(q.consume(1000, { m -> keep(slot, m) })))
  note(n, "id", slot.message.id)
  note(n, "body", slot.message.body)
  var headers = ""
  for h in slot.message.headers {
    headers = headers + h.key + "=" + h.value + ";"
  }
  note(n, "headers", headers)
  note(n, "ack", outcome(q.ack(slot.message.id)))

  // A lyric.* attribute this library did not write: the delivery is
  // reported, stays in flight under its id and can be dropped.
  val broken = "{\"QueueUrl\":\"" + url + "\",\"MessageBody\":\"x\",\"MessageAttributes\":{" +
    "\"lyric.id\":{\"DataType\":\"String\",\"StringValue\":\"broken\"}," +
    "\"lyric.body-encoding\":{\"DataType\":\"String\",\"StringValue\":\"rot13\"}}}"
  match q.client.call("SendMessage", broken, 0) {
    case Ok(_) -> note(n, "sendBroken", "ok")
    case Err(e) -> note(n, "sendBroken", e.text())
  }
  note(n, "undecodable", take(q, 1000))
  note(n, "inFlight", unsettledCount(q).toString())
  note(n, "dropBroken", outcome(q.nack("broken", false)))
  note(n, "empty", take(q, 0))
  q.close()
}

test "a message from another producer is delivered; an undecodable one is reported and can be settled" {
  val lines = run(19812, { endpoint, n -> foreignScenario(endpoint, n) })
  expectClean(lines)
  check(lines, "sendForeign", "ok")
  check(lines, "consume", "ok")
  check(lines, "id", "m-1")
  check(lines, "body", "from elsewhere")
  check(lines, "headers", "attempt=3;trace=t-1;")
  check(lines, "ack", "ok")
  check(lines, "sendBroken", "ok")
  check(
    lines,
    "undecodable",
    "sqs: message 'broken' cannot be decoded: unknown lyric.body-encoding 'rot13'; " +
      "it stays in flight under that id until it is settled or its visibility timeout passes"
  )
  check(lines, "inFlight", "1")
  check(lines, "dropBroken", "ok")
  check(lines, "empty", "timeout")
}

// ─── Timeouts and refused connections ────────────────────────────────────────
// A listener nobody pulls requests from accepts a connection, reads the
// request and never answers: every request to it ends at its deadline.
// Nothing listens on port 1: every request to it is refused at once.
func silentConfig(timeoutMs: in Int): SqsConfig {
  endpointConfig("http://127.0.0.1:19813", "http://127.0.0.1:19813/" + ACCOUNT + "/silent", REGION, ACCESS_KEY, SECRET_KEY).withRequestTimeout(
    timeoutMs
  )
}

func refusedConfig(): SqsConfig {
  endpointConfig("http://127.0.0.1:1", "http://127.0.0.1:1/" + ACCOUNT + "/q", REGION, ACCESS_KEY, SECRET_KEY).withRequestTimeout(
    5000
  )
}

func codeOf(r: in Result[Map[String, String], SqsError]): String {
  match r {
    case Ok(_) -> "ok"
    case Err(e) -> e.code + "/" + e.status.toString()
  }
}

test "a request that is not answered in time is a TIMEOUT on every attempt; a refused one is CONNECTION_FAILED" {
  val silent = startListener("http://127.0.0.1:19813/")
  // Short deadlines of varying length, many times: on --target jvm the JDK's
  // timer and the deadline token disagree about a third of the time, and
  // every attempt must still be reported as the same timeout.
  var i = 0
  while i < 40 {
    val ms = 20 + i % 7
    val q = open(silentConfig(ms))
    assertEqual(
      outcome(q.publish(message("t", "x"))),
      "sqs: SendMessage timed out after " + ms.toString() + " ms at http://127.0.0.1:19813",
      "attempt " + i.toString()
    )
    i = i + 1
  }
  val q = open(silentConfig(60))
  val client = q.client
  // Every call path goes through the same classification.
  assertEqual(take(q, 0), "sqs: ReceiveMessage timed out after 60 ms at http://127.0.0.1:19813", "a short poll")
  // A long poll's deadline is the request timeout plus the seconds it waits;
  // a slow answer is an error, never the interface's "timeout".
  assertEqual(take(q, 1000), "sqs: ReceiveMessage timed out after 1060 ms at http://127.0.0.1:19813", "a long poll")
  assertEqual(
    outcome(q.publishBatch([message("u-1", "x"), message("u-2", "x")])),
    "sqs: publishBatch: request 1 of 1: SendMessageBatch timed out after 60 ms at http://127.0.0.1:19813; " +
      "it is not known whether SQS accepted 'u-1', 'u-2'; 0 messages of earlier requests stay published and 0 later messages were not sent",
    "a batch whose fate is unknown"
  )
  assertEqual(
    sqsOutcome(client.deleteMessage(q.queue.url, "rh")),
    "TIMEOUT: DeleteMessage timed out after 60 ms at http://127.0.0.1:19813",
    "DeleteMessage"
  )
  assertEqual(
    sqsOutcome(client.changeVisibility(q.queue.url, "rh", 0)),
    "TIMEOUT: ChangeMessageVisibility timed out after 60 ms at http://127.0.0.1:19813",
    "ChangeMessageVisibility"
  )
  assertEqual(codeOf(queueAttributes(client, q.queue.url)), "TIMEOUT/0", "Extras: GetQueueAttributes")
  assertEqual(
    sentOutcome(createQueue(client, "new-queue", standardQueue())),
    "TIMEOUT: CreateQueue timed out after 60 ms at http://127.0.0.1:19813",
    "Extras: CreateQueue"
  )
  assertEqual(
    sqsOutcome(purgeQueue(client, q.queue.url)),
    "TIMEOUT: PurgeQueue timed out after 60 ms at http://127.0.0.1:19813",
    "Extras: PurgeQueue"
  )
  stopListener(silent)

  val refused = open(refusedConfig())
  var k = 0
  while k < 5 {
    val text = outcome(refused.publish(message("r", "x")))
    assertTrue(
      Str.startsWith(text, "sqs: SendMessage got no response from http://127.0.0.1:1: "),
      "a refused connection is not a timeout: " + text
    )
    k = k + 1
  }
  assertEqual(
    codeOf(queueAttributes(refused.client, refused.queue.url)),
    "CONNECTION_FAILED/0",
    "the code of a refusal"
  )
  val batch = outcome(refused.publishBatch([message("u-1", "x")]))
  assertTrue(
    Str.startsWith(
      batch,
      "sqs: publishBatch: request 1 of 1: SendMessageBatch got no response from http://127.0.0.1:1: "
    ),
    "a refused batch: " + batch
  )
}

// ─── TLS ─────────────────────────────────────────────────────────────────────
// TEST FIXTURES, NOT SECRETS. The certificates and the private key below exist
// only for the hermetic TLS tests of the AWS backends in this repository (the
// same four are in storage-s3/tests/fake_s3_tests.l): a throwaway CA, a server
// certificate for `localhost` / `127.0.0.1` signed by it, and a second CA that
// signed nothing. They protect nothing and are valid until 2126. Made with:
//
//   openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out X.key
//   openssl req -x509 -new -key ca.key -sha256 -days 36500 \
//     -subj "/O=Lyric AWS backends test fixture/CN=test fixture CA (not a secret)" \
//     -addext "basicConstraints=critical,CA:TRUE" -addext "keyUsage=critical,keyCertSign,cRLSign" -out ca.crt
//   openssl req -new -key server.key -subj "/O=Lyric AWS backends test fixture/CN=localhost" -out server.csr
//   openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial -sha256 -days 36500 \
//     -extfile server.ext -out server.crt      (subjectAltName=DNS:localhost,IP:127.0.0.1)
val FIXTURE_CA_PEM: String = "-----BEGIN CERTIFICATE-----\nMIICDjCCAbOgAwIBAgIUehV33rhTgQ0MIpdI/2vALz16WHIwCgYIKoZIzj0EAwIw\nUzEoMCYGA1UECgwfTHlyaWMgQVdTIGJhY2tlbmRzIHRlc3QgZml4dHVyZTEnMCUG\nA1UEAwwedGVzdCBmaXh0dXJlIENBIChub3QgYSBzZWNyZXQpMCAXDTI2MTExMzA3\nNDkzNFoYDzIxMjYxMDIwMDc0OTM0WjBTMSgwJgYDVQQKDB9MeXJpYyBBV1MgYmFj\na2VuZHMgdGVzdCBmaXh0dXJlMScwJQYDVQQDDB50ZXN0IGZpeHR1cmUgQ0EgKG5v\ndCBhIHNlY3JldCkwWTATBgcqhkjOPQIBBggqhkjOPQMBBwNCAASlTjPdK05iaHbC\nP8/DRkaOkAbSHlo/5usXvOpQK4ebMwgH+6+sjVY+4srGM/5gY1Ad27A2BJVQ4IrQ\njFAm9zMYo2MwYTAdBgNVHQ4EFgQUJhRJqHEzNL474DOA1wtcBP8LM94wHwYDVR0j\nBBgwFoAUJhRJqHEzNL474DOA1wtcBP8LM94wDwYDVR0TAQH/BAUwAwEB/zAOBgNV\nHQ8BAf8EBAMCAQYwCgYIKoZIzj0EAwIDSQAwRgIhAKru9e/hbUoMCQ9/Nz11sj+1\nWfzr4m0/s1z1FMKPKGB7AiEA+lt6MMiDNToff6ngfv7m8ztBYUsHlQoF5Kq7izgn\nOdg=\n-----END CERTIFICATE-----\n"

val FIXTURE_SERVER_CERT_PEM: String = "-----BEGIN CERTIFICATE-----\nMIICJTCCAcugAwIBAgIUby5WyizAQTgdThfE+RT04AjHDyMwCgYIKoZIzj0EAwIw\nUzEoMCYGA1UECgwfTHlyaWMgQVdTIGJhY2tlbmRzIHRlc3QgZml4dHVyZTEnMCUG\nA1UEAwwedGVzdCBmaXh0dXJlIENBIChub3QgYSBzZWNyZXQpMCAXDTI2MTExMzA3\nNDkzNFoYDzIxMjYxMDIwMDc0OTM0WjA+MSgwJgYDVQQKDB9MeXJpYyBBV1MgYmFj\na2VuZHMgdGVzdCBmaXh0dXJlMRIwEAYDVQQDDAlsb2NhbGhvc3QwWTATBgcqhkjO\nPQIBBggqhkjOPQMBBwNCAASqrA4/CynXzAIAQGdvnp7GPHGthl3P6qJ2qZfxM/2p\nHzQlu8UAdbIRSPdi6tD0454iL9e5ldRxVJ3e1Pig3W1Vo4GPMIGMMAkGA1UdEwQC\nMAAwDgYDVR0PAQH/BAQDAgeAMBMGA1UdJQQMMAoGCCsGAQUFBwMBMBoGA1UdEQQT\nMBGCCWxvY2FsaG9zdIcEfwAAATAdBgNVHQ4EFgQU/je8BKSYH9HeLVMBa18JsP8y\nhLAwHwYDVR0jBBgwFoAUJhRJqHEzNL474DOA1wtcBP8LM94wCgYIKoZIzj0EAwID\nSAAwRQIhAM5g73n49syQr/I2s+6Vsfs0Ojw2kwxAnv8zsswLxIw+AiAcN2KJd/i/\nxMz8T83NmZfFivdfE10h/SkK51VsHIttfg==\n-----END CERTIFICATE-----\n"

val FIXTURE_SERVER_KEY_PEM: String = "-----BEGIN PRIVATE KEY-----\nMIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgtKwkAL49aCgKU8Be\nefwIxJ9fk9Vp3UQnXex9b9hiCwyhRANCAASqrA4/CynXzAIAQGdvnp7GPHGthl3P\n6qJ2qZfxM/2pHzQlu8UAdbIRSPdi6tD0454iL9e5ldRxVJ3e1Pig3W1V\n-----END PRIVATE KEY-----\n"

// A CA that did not sign the server certificate.
val FIXTURE_OTHER_CA_PEM: String = "-----BEGIN CERTIFICATE-----\nMIICHTCCAcOgAwIBAgIUARJtxHD1seTCMo8Yy05/b8YDT2EwCgYIKoZIzj0EAwIw\nWzEoMCYGA1UECgwfTHlyaWMgQVdTIGJhY2tlbmRzIHRlc3QgZml4dHVyZTEvMC0G\nA1UEAwwmYW5vdGhlciB0ZXN0IGZpeHR1cmUgQ0EgKG5vdCBhIHNlY3JldCkwIBcN\nMjYxMTEzMDc0OTM0WhgPMjEyNjEwMjAwNzQ5MzRaMFsxKDAmBgNVBAoMH0x5cmlj\nIEFXUyBiYWNrZW5kcyB0ZXN0IGZpeHR1cmUxLzAtBgNVBAMMJmFub3RoZXIgdGVz\ndCBmaXh0dXJlIENBIChub3QgYSBzZWNyZXQpMFkwEwYHKoZIzj0CAQYIKoZIzj0D\nAQcDQgAEj31puhUVhDm9h62ooQv3xD+A+VxMIWBBPMjPG2QLQXiPCf9hSPIVAxV8\nq4VC09dnHKzwuYtonrrvVnsr52vOm6NjMGEwHQYDVR0OBBYEFJmzXefGlc3B9qWW\nae54NCLQy6LHMB8GA1UdIwQYMBaAFJmzXefGlc3B9qWWae54NCLQy6LHMA8GA1Ud\nEwEB/wQFMAMBAf8wDgYDVR0PAQH/BAQDAgEGMAoGCCqGSM49BAMCA0gAMEUCIQDU\nauGTenHbZvZ5bCf8/JkpOShiIvdCuqhdF6seQWKltQIgGdUI0OPFU9Y599Q3sX0Y\nS4bDv5i7emOh6NvklppE0fo=\n-----END CERTIFICATE-----\n"

func tlsEndpointOf(port: in Int): String {
  "https://127.0.0.1:" + port.toString()
}

func startFakeTls(port: in Int): HttpListener {
  val identity = match Tls.Identity.fromPem(FIXTURE_SERVER_CERT_PEM, FIXTURE_SERVER_KEY_PEM) {
    case Ok(i) -> i
    case Err(e) -> panic("fixture identity: " + Tls.TlsError.message(e))
  }
  match startListenerTls("127.0.0.1", port, Tls.TlsServerConfig(identity = identity)) {
    case Ok(l) -> l
    case Err(_) -> panic("cannot start the HTTPS fake on port " + port.toString())
  }
}

@cfg(target = "dotnet")
func withFakeSqsTls(port: in Int, scenario: in (String) -> String): String {
  val l = startFakeTls(port)
  val server = taskRun({ -> serveUntilStopped(l, "https", port) })
  val transcript = scenario(tlsEndpointOf(port))
  taskWait(server)
  stopListener(l)
  transcript
}

@cfg(target = "native")
func withFakeSqsTls(port: in Int, scenario: in (String) -> String): String {
  val l = startFakeTls(port)
  val server = StdTask.makeScope()
  StdTask.scopeSpawn(server, { -> serveUntilStopped(l, "https", port) })
  val transcript = scenario(tlsEndpointOf(port))
  val _ = StdTask.awaitAll(server)
  stopListener(l)
  transcript
}

@cfg(target = "jvm")
func withFakeSqsTls(port: in Int, scenario: in (String) -> String): String {
  val l = startFakeTls(port)
  var transcript = ""
  scope {
    val server = spawn serveUntilStopped(l, "https", port)
    transcript = scenario(tlsEndpointOf(port))
    await server
  }
  stopListener(l)
  transcript
}

// `stopFake` for the HTTPS fake: a plain client that trusts the fixture CA.
func stopFakeTls(endpoint: in String): String {
  val ca = match Tls.Certificate.fromPem(FIXTURE_CA_PEM) {
    case Ok(c) -> c
    case Err(e) -> return "stop failed: " + Tls.TlsError.message(e)
  }
  match await HttpClientBuilder.new().withCaCertificate(ca).build().get(endpoint + STOP_PATH) {
    case Err(e) -> "stop failed: " + HttpError.message(e)
    case Ok(resp) -> match await HttpResponse.bodyText(resp) { case Ok(text) -> text; case Err(_) -> "stop unreadable" }
  }
}

// Creates the queue `name` through `cfg`, publishes one message, consumes it
// and acknowledges it: `ok`, or the first error.
func tlsRoundTrip(cfg: in SqsConfig, name: in String): String {
  val admin = match openClient(cfg) { case Ok(c) -> c; case Err(e) -> return e.code + " " + e.message }
  val url = match createQueue(admin, name, standardQueue()) {
    case Ok(u) -> u
    case Err(e) -> return e.code + " " + e.message
  }
  val q = match connectWith(cfg.copy(queueUrl = url)) { case Ok(q) -> q; case Err(e) -> return e }
  match q.publish(message("m-" + name, "over TLS")) {
    case Err(e) -> return e
    case Ok(_) -> ()
  }
  val got = take(q, 1000)
  val acked = outcome(q.ack("m-" + name))
  q.close()
  if got == "m-" + name + "/over TLS/0" and acked == "ok" {
    "ok " + url
  } else {
    got + " " + acked
  }
}

func tlsScenario(endpoint: in String): String {
  val base = configFor(endpoint, "")
  var text = "additive=" + tlsRoundTrip(base.withCaCertificate(FIXTURE_CA_PEM), "a")
  text = text + "\nexclusive=" + tlsRoundTrip(base.withExclusiveCaCertificate(FIXTURE_CA_PEM), "b")
  text = text + "\nexclusiveWins=" +
    tlsRoundTrip(base.withCaCertificate(FIXTURE_OTHER_CA_PEM).withExclusiveCaCertificate(FIXTURE_CA_PEM), "c")
  text = text + "\nsystemOnly=" + tlsRoundTrip(base, "d")
  text = text + "\notherAdditive=" + tlsRoundTrip(base.withCaCertificate(FIXTURE_OTHER_CA_PEM), "e")
  text = text + "\notherExclusive=" + tlsRoundTrip(base.withExclusiveCaCertificate(FIXTURE_OTHER_CA_PEM), "f")
  // The refused handshakes did not break the server or reach it.
  text = text + "\nafter=" + tlsRoundTrip(base.withCaCertificate(FIXTURE_CA_PEM), "g")
  text + "\n" + stopFakeTls(endpoint)
}

test "an HTTPS server with a private CA is reachable with withCaCertificate, and refused without it" {
  val lines = Str.split(withFakeSqsTls(19814, tlsScenario), "\n")
  assertEqualInt(lines.length, 8, "eight lines: " + Str.join(" | ", lines))
  val prefix = "https://127.0.0.1:19814/" + ACCOUNT + "/"
  assertEqual(lines[0], "additive=ok " + prefix + "a", "the CA added to the system trust store")
  assertEqual(lines[1], "exclusive=ok " + prefix + "b", "the CA as the only trusted authority")
  assertEqual(lines[2], "exclusiveWins=ok " + prefix + "c", "withExclusiveCaCertificate wins over withCaCertificate")
  assertTrue(lines[3].startsWith("systemOnly=CONNECTION_FAILED "), "the system trust store alone: " + lines[3])
  assertTrue(lines[4].startsWith("otherAdditive=CONNECTION_FAILED "), "a CA that did not sign it: " + lines[4])
  assertTrue(lines[5].startsWith("otherExclusive=CONNECTION_FAILED "), "only a CA that did not sign it: " + lines[5])
  assertEqual(lines[6], "after=ok " + prefix + "g", "the server still serves after refused handshakes")
  // Four round trips of four signed requests each (CreateQueue, SendMessage,
  // ReceiveMessage, DeleteMessage); nothing else got through.
  assertEqual(lines[7], "served 16", "only the trusted clients' requests reached the server")
}

func openText(cfg: in SqsConfig): String {
  match openClient(cfg) {
    case Ok(_) -> "ok"
    case Err(e) -> e.code + ": " + e.message
  }
}

test "a CA certificate that is not a PEM certificate is INVALID_CONFIG before any request" {
  val base = configFor("https://127.0.0.1:1", "")
  val notPem = openText(base.withCaCertificate("not a certificate"))
  assertTrue(notPem.startsWith("INVALID_CONFIG"), "additive, not PEM: " + notPem)
  assertTrue(Str.contains(notPem, "withCaCertificate"), "the message names the option: " + notPem)
  val exclusive = openText(
    base.withExclusiveCaCertificate("-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n")
  )
  assertTrue(exclusive.startsWith("INVALID_CONFIG"), "exclusive, not a certificate: " + exclusive)
  assertTrue(Str.contains(exclusive, "withExclusiveCaCertificate"), "the message names the option: " + exclusive)
  assertTrue(
    openText(base.withCaCertificate(FIXTURE_SERVER_KEY_PEM)).startsWith("INVALID_CONFIG"),
    "a private key is not a certificate"
  )
  assertEqual(openText(base.withCaCertificate(FIXTURE_CA_PEM)), "ok", "a real certificate opens")
  val queueUrl = "https://127.0.0.1:1/" + ACCOUNT + "/orders"
  match connectWith(configFor("https://127.0.0.1:1", queueUrl).withCaCertificate("nope")) {
    case Ok(_) -> panic("an unusable CA certificate was accepted")
    case Err(e) -> assertTrue(Str.startsWith(e, "sqs: the certificate given to withCaCertificate"), "connectWith: " + e)
  }
  // A CA certificate does not make plain HTTP to another host acceptable.
  val remote = openText(configFor("http://sqs.example.com", "").withCaCertificate(FIXTURE_CA_PEM))
  assertTrue(remote.startsWith("INVALID_CONFIG"), "http:// stays loopback-only: " + remote)
  // On a loopback http:// endpoint the option has nothing to verify, but is
  // still validated.
  val plain = configFor("http://127.0.0.1:1", "")
  assertTrue(openText(plain.withCaCertificate("nope")).startsWith("INVALID_CONFIG"), "validated on http:// too")
  assertEqual(openText(plain.withCaCertificate(FIXTURE_CA_PEM)), "ok", "and accepted there")
}

func fileText(r: in Result[SqsConfig, SqsError]): String {
  match r {
    case Ok(cfg) -> "ok " + cfg.additionalCaCertificatePem.length.toString() + "/" +
      cfg.caCertificatePem.length.toString()
    case Err(e) -> e.code + " " + e.message
  }
}

test "withCaCertificateFile reads a PEM file, and a missing or empty one is INVALID_CONFIG" {
  val base = configFor("https://127.0.0.1:1", "")
  val dir = Env.getVarOrDefault("TMPDIR", "/tmp")
  val path = dir + "/lyric-sqs-fixture-ca-" + Time.nowEpochMillis().toString() + ".pem"
  match File.writeText(path, FIXTURE_CA_PEM) {
    case Ok(_) -> ()
    case Err(_) -> panic("cannot write " + path)
  }
  val n = FIXTURE_CA_PEM.length.toString()
  assertEqual(fileText(base.withCaCertificateFile(path)), "ok " + n + "/0", "the additive form")
  assertEqual(fileText(base.withExclusiveCaCertificateFile(path)), "ok 0/" + n, "the exclusive form")
  match File.writeText(path, "") {
    case Ok(_) -> ()
    case Err(_) -> panic("cannot write " + path)
  }
  assertEqual(
    fileText(base.withCaCertificateFile(path)),
    "INVALID_CONFIG the CA certificate file is empty: " + path,
    "an empty file"
  )
  val _ = File.deleteFile(path)
  assertEqual(
    fileText(base.withCaCertificateFile(path)),
    "INVALID_CONFIG cannot read the CA certificate: " + path,
    "a missing file"
  )
  assertEqual(
    fileText(base.withExclusiveCaCertificateFile("")),
    "INVALID_CONFIG the path of the CA certificate is empty",
    "an empty path"
  )
}

// ─── Retries ─────────────────────────────────────────────────────────────────
// The standard policy with its waits recorded instead of slept, and the
// random source at the top of its range: the waits are 100 ms, then 200 ms.
func recordingRetry(sleeps: in List[Int]): RetryPolicy {
  standardRetry().withSleep({ ms: Int -> sleeps.add(ms) }).withRandom({ -> 1.0 })
}

func sleptMs(sleeps: in List[Int]): String {
  val acc: List[String] = newList()
  for ms in sleeps {
    acc.add(ms.toString())
  }
  sleeps.clear()
  Str.joinList(",", acc)
}

func retryScenario(endpoint: in String, n: in Notes): Unit {
  val sleeps: List[Int] = newList()
  val policy = recordingRetry(sleeps)
  val client = adminFor(endpoint)
  // Throttling: SQS did not take the request, so even a send to a standard
  // queue is repeated.
  val throttled = open(configFor(endpoint, create(endpoint, "retry-throttle-send", standardQueue())).withRetry(policy))
  discardLog(endpoint)
  note(n, "throttledSend", outcome(throttled.publish(message("t-1", "x"))))
  note(n, "throttledSendLog", serverLog(endpoint))
  note(n, "throttledSendWaits", sleptMs(sleeps))
  note(n, "throttledSendStored", attribute(client, throttled.queue.url, "ApproximateNumberOfMessages"))
  // And a receive.
  val throttledReceive = open(
    configFor(endpoint, create(endpoint, "retry-throttle-receive", standardQueue())).withRetry(
      policy
    )
  )
  discardLog(endpoint)
  note(n, "throttledReceive", take(throttledReceive, 0))
  note(n, "throttledReceiveLog", serverLog(endpoint))
  note(n, "throttledReceiveWaits", sleptMs(sleeps))
  // Retries switched off: the throttling error is returned.
  val once = open(configFor(endpoint, create(endpoint, "retry-throttle-once", standardQueue())).withoutRetries())
  discardLog(endpoint)
  note(n, "withoutRetries", outcome(once.publish(message("t-2", "x"))))
  note(n, "withoutRetriesLog", serverLog(endpoint))
  // A 500 leaves the outcome unknown. A send to a standard queue is not
  // repeated: the message might be there already.
  val failing = open(configFor(endpoint, create(endpoint, "retry-500-send", standardQueue())).withRetry(policy))
  discardLog(endpoint)
  note(n, "send500", outcome(failing.publish(message("f-1", "x"))))
  note(n, "send500Log", serverLog(endpoint))
  note(n, "send500Waits", sleptMs(sleeps))
  // Nor is a receive: the messages of a lost answer would stay hidden.
  val failingReceive = open(
    configFor(endpoint, create(endpoint, "retry-500-receive", standardQueue())).withRetry(policy)
  )
  discardLog(endpoint)
  note(n, "receive500", take(failingReceive, 0))
  note(n, "receive500Log", serverLog(endpoint))
  note(n, "receive500Waits", sleptMs(sleeps))
  // An idempotent action is repeated after a 500.
  val attributesUrl = create(endpoint, "retry-500-attributes", standardQueue())
  val retrying = match openClient(configFor(endpoint, "").withRetry(policy)) {
    case Ok(c) -> c
    case Err(e) -> panic(e.text())
  }
  discardLog(endpoint)
  note(n, "attributes500", attribute(retrying, attributesUrl, "ApproximateNumberOfMessages"))
  note(n, "attributes500Log", serverLog(endpoint))
  note(n, "attributes500Waits", sleptMs(sleeps))
  // The dangerous case: SQS stored the message and the answer was lost. On
  // a standard queue the send is not repeated, so the queue holds it once.
  val lost = open(configFor(endpoint, create(endpoint, "lost-reply-standard", standardQueue())).withRetry(policy))
  discardLog(endpoint)
  note(n, "lostStandard", outcome(lost.publish(message("l-1", "x"))))
  note(n, "lostStandardLog", serverLog(endpoint))
  note(n, "lostStandardWaits", sleptMs(sleeps))
  note(n, "lostStandardStored", attribute(client, lost.queue.url, "ApproximateNumberOfMessages"))
  // On a FIFO queue the send is repeated, and the deduplication id makes
  // SQS accept the repeat without storing the message again.
  val lostFifo = open(configFor(endpoint, create(endpoint, "lost-reply.fifo", fifoQueue())).withRetry(policy))
  discardLog(endpoint)
  note(n, "lostFifo", outcome(lostFifo.publish(message("l-2", "x"))))
  note(n, "lostFifoLog", serverLog(endpoint))
  note(n, "lostFifoWaits", sleptMs(sleeps))
  note(n, "lostFifoStored", attribute(client, lostFifo.queue.url, "ApproximateNumberOfMessages"))
  // An error that would be the same next time is not retried.
  val base = endpoint + "/" + ACCOUNT + "/"
  val missing = open(configFor(endpoint, base + "no-such-queue").withRetry(policy))
  note(n, "missing", outcome(missing.publish(message("m-1", "x"))))
  note(n, "missingWaits", sleptMs(sleeps))
  // Throttled every time: three attempts, then the error.
  val always = open(configFor(endpoint, base + "fail-throttle").withRetry(policy))
  note(n, "always", outcome(always.publish(message("a-1", "x"))))
  note(n, "alwaysWaits", sleptMs(sleeps))
  val five = open(configFor(endpoint, base + "fail-throttle").withRetry(policy.withMaxAttempts(5)))
  note(n, "five", outcome(five.publish(message("a-2", "x"))))
  note(n, "fiveWaits", sleptMs(sleeps))
}

test "throttling is retried for every action; an unknown outcome only where a repeat is harmless" {
  val lines = run(19541, { endpoint, n -> retryScenario(endpoint, n) })
  expectClean(lines)
  check(lines, "throttledSend", "ok")
  check(lines, "throttledSendLog", "throttled SendMessage; throttled SendMessage; SendMessage")
  check(lines, "throttledSendWaits", "100,200")
  check(lines, "throttledSendStored", "1")
  check(lines, "throttledReceive", "timeout")
  check(
    lines,
    "throttledReceiveLog",
    "throttled ReceiveMessage; throttled ReceiveMessage; ReceiveMessage max=1 wait=0 vt=30 -> 0"
  )
  check(lines, "throttledReceiveWaits", "100,200")
  check(lines, "withoutRetries", "sqs: SendMessage failed: RequestThrottled (HTTP 400): Rate exceeded")
  check(lines, "withoutRetriesLog", "throttled SendMessage")
  check(
    lines,
    "send500",
    "sqs: SendMessage failed: InternalError (HTTP 500): We encountered an internal error. Please try again."
  )
  check(lines, "send500Log", "failed SendMessage")
  check(lines, "send500Waits", "")
  checkContains(lines, "receive500", "ReceiveMessage failed: InternalError (HTTP 500)")
  check(lines, "receive500Log", "failed ReceiveMessage")
  check(lines, "receive500Waits", "")
  check(lines, "attributes500", "0")
  check(lines, "attributes500Log", "failed GetQueueAttributes")
  check(lines, "attributes500Waits", "100")
  checkContains(lines, "lostStandard", "SendMessage failed: InternalError (HTTP 500)")
  check(lines, "lostStandardLog", "SendMessage; stored, reply lost")
  check(lines, "lostStandardWaits", "")
  check(lines, "lostStandardStored", "1")
  check(lines, "lostFifo", "ok")
  check(lines, "lostFifoLog", "SendMessage; stored, reply lost; SendMessage")
  check(lines, "lostFifoWaits", "100")
  check(lines, "lostFifoStored", "1")
  checkContains(lines, "missing", "QueueDoesNotExist")
  check(lines, "missingWaits", "")
  check(lines, "always", "sqs: SendMessage failed: RequestThrottled (HTTP 400): Rate exceeded")
  check(lines, "alwaysWaits", "100,200")
  check(lines, "fiveWaits", "100,200,400,800")
}

test "a connection that cannot be opened is retried even for a send; only idempotent actions say so" {
  val sleeps: List[Int] = newList()
  val refused = open(refusedConfig().withRetry(recordingRetry(sleeps)))
  val text = outcome(refused.publish(message("r", "x")))
  assertTrue(Str.startsWith(text, "sqs: SendMessage got no response from http://127.0.0.1:1: "), text)
  assertEqual(sleptMs(sleeps), "100,200", "nothing was sent, so the send is attempted three times")
  assertTrue(isIdempotentAction("DeleteMessage"), "DeleteMessage")
  assertTrue(isIdempotentAction("ChangeMessageVisibility"), "ChangeMessageVisibility")
  assertTrue(isIdempotentAction("GetQueueAttributes"), "GetQueueAttributes")
  assertTrue(isIdempotentAction("CreateQueue"), "CreateQueue")
  assertFalse(isIdempotentAction("SendMessage"), "SendMessage")
  assertFalse(isIdempotentAction("SendMessageBatch"), "SendMessageBatch")
  assertFalse(isIdempotentAction("ReceiveMessage"), "ReceiveMessage")
  assertFalse(isIdempotentAction("PurgeQueue"), "PurgeQueue")
  assertFalse(isIdempotentAction("DeleteQueue"), "DeleteQueue")
  assertFalse(isIdempotentAction("SomethingNew"), "an unknown action")
  assertTrue(isFifoQueueUrl("https://sqs.eu-west-1.amazonaws.com/123456789012/orders.fifo"), "a FIFO queue URL")
  assertFalse(isFifoQueueUrl("https://sqs.eu-west-1.amazonaws.com/123456789012/orders"), "a standard queue URL")
}

// ─── Credential providers ────────────────────────────────────────────────────
ref record Clock {
  var nowMs: Long
}

// A provider that hands out the long-term test key until `switchAtMs`, with
// an expiry, and the temporary key after it.
ref record Rotating {
  clock: Clock
  switchAtMs: Long
  var calls: Int
  var broken: Bool
}

impl CredentialProvider for Rotating {
  func resolve(): Result[ResolvedCredentials, CredentialError] {
    self.calls = self.calls + 1
    if self.broken {
      return Err(
        CredentialError(code = "REQUEST_FAILED", provider = "rotating", message = "the credential service is down")
      )
    }
    if self.clock.nowMs < self.switchAtMs {
      Ok(
        ResolvedCredentials(
          credentials = Credentials(accessKeyId = ACCESS_KEY, secretAccessKey = SECRET_KEY, sessionToken = ""),
          expiresAtEpochMs = Some(value = self.switchAtMs + 300000i64),
          source = "rotating"
        )
      )
    } else {
      Ok(
        ResolvedCredentials(
          credentials = Credentials(
            accessKeyId = TEMP_ACCESS_KEY,
            secretAccessKey = TEMP_SECRET_KEY,
            sessionToken = TEMP_TOKEN
          ),
          expiresAtEpochMs = Some(value = self.switchAtMs + 7200000i64),
          source = "rotating"
        )
      )
    }
  }
}

func rotationScenario(endpoint: in String, n: in Notes): Unit {
  val url = create(endpoint, "rotation", standardQueue())
  val t0 = 1800000000000i64
  val clock = Clock(nowMs = t0)
  // The first credentials expire five minutes after `switchAtMs`; the cache
  // refreshes five minutes before expiry, which is at `switchAtMs`.
  val inner = Rotating(clock = clock, switchAtMs = t0 + 3600000i64, calls = 0, broken = false)
  val cache = cachedWith(
    inner,
    CacheOptions(refreshWindowMs = 300000i64, retryAfterFailureMs = 30000i64, nowMs = { -> clock.nowMs })
  )
  // Empty static keys: the provider is the only source.
  val q = open(
    endpointConfig(endpoint, url, REGION, "", "").withCredentialProvider(cache.asProvider()).withRequestTimeout(
      10000
    )
  )
  note(n, "first", outcome(q.publish(message("c-1", "x"))))
  note(n, "firstSignedBy", plainGet(endpoint + LAST_KEY_PATH) + " calls=" + inner.calls.toString())
  clock.nowMs = t0 + 3599999i64
  note(n, "before", outcome(q.publish(message("c-2", "x"))))
  note(n, "beforeSignedBy", plainGet(endpoint + LAST_KEY_PATH) + " calls=" + inner.calls.toString())
  // The clock reaches the refresh window: the next request is signed with
  // the temporary key and its session token, which the server verifies.
  clock.nowMs = t0 + 3600000i64
  note(n, "after", outcome(q.publish(message("c-3", "x"))))
  note(n, "afterSignedBy", plainGet(endpoint + LAST_KEY_PATH) + " calls=" + inner.calls.toString())
  note(n, "consumed", take(q, 1000))
  // A provider with nothing to give: the operation fails without a request.
  inner.broken = true
  cache.invalidate()
  discardLog(endpoint)
  note(n, "noCredentials", outcome(q.publish(message("c-4", "x"))))
  note(n, "noCredentialsLog", serverLog(endpoint))
  inner.broken = false
  q.close()
}

test "credentials from a provider are refreshed mid-run, and the next request is signed with the new ones" {
  val lines = run(19542, { endpoint, n -> rotationScenario(endpoint, n) })
  expectClean(lines)
  check(lines, "first", "ok")
  check(lines, "firstSignedBy", ACCESS_KEY + " calls=1")
  check(lines, "before", "ok")
  check(lines, "beforeSignedBy", ACCESS_KEY + " calls=1")
  check(lines, "after", "ok")
  check(lines, "afterSignedBy", TEMP_ACCESS_KEY + " calls=2")
  checkContains(lines, "consumed", "c-")
  check(lines, "noCredentials", "sqs: no AWS credentials: rotating: the credential service is down")
  check(lines, "noCredentialsLog", "")
}

func sharedFilesScenario(endpoint: in String, n: in Notes): Unit {
  val url = create(endpoint, "shared-files", standardQueue())
  val dir = Env.getVarOrDefault("TMPDIR", "/tmp") + "/lyric-sqs-shared-" + Time.nowEpochMillis().toString()
  match File.createDir(dir) {
    case Ok(_) -> ()
    case Err(_) -> panic("cannot create " + dir)
  }
  match File.writeText(dir + "/credentials", "[ci]\naws_access_key_id = " + ACCESS_KEY + "\naws_secret_access_key = " + SECRET_KEY + "\n") {
    case Ok(_) -> ()
    case Err(_) -> panic("cannot write the credentials file")
  }
  match File.writeText(dir + "/config", "[profile ci]\nregion = " + REGION + "\nendpoint_url = " + endpoint + "\nsqs =\n  endpoint_url = http://127.0.0.1:1\n") {
    case Ok(_) -> ()
    case Err(_) -> panic("cannot write the config file")
  }
  val values: Map[String, String] = newMap()
  values["AWS_CONFIG_FILE"] = dir + "/config"
  values["AWS_SHARED_CREDENTIALS_FILE"] = dir + "/credentials"
  values["AWS_EC2_METADATA_DISABLED"] = "true"
  values["AWS_PROFILE"] = "ci"
  match configFromEnvironment(url, environmentOf(values)) {
    case Err(e) -> note(n, "config", e.code + ": " + e.message)
    case Ok(cfg) -> {
      note(
        n,
        "config",
        "key=" + cfg.accessKeyId + " provider=" + (if isSome(cfg.credentialProvider) { "yes" } else { "no" }) + " region=" +
          cfg.region +
          " endpoint=" +
          (if cfg.endpoint == endpoint { "the fake" } else { cfg.endpoint })
      )
      val q = open(cfg)
      note(n, "publish", outcome(q.publish(message("s-1", "from a profile"))))
      note(n, "consume", take(q, 1000))
      q.close()
    }
  }
  val _ = File.deleteDirRecursive(dir)
}

test "a queue configured from a profile of the shared files signs requests the server accepts" {
  val lines = run(19543, { endpoint, n -> sharedFilesScenario(endpoint, n) })
  expectClean(lines)
  check(lines, "config", "key= provider=yes region=" + REGION + " endpoint=the fake")
  check(lines, "publish", "ok")
  check(lines, "consume", "s-1/from a profile/0")
}
