//! tests/wire_tests.l - `Mq.Sqs.Wire`: the JSON request bodies, the response
//! parsers, and how an `Mq.Message` is carried in an SQS message. Request
//! bodies are compared with literal JSON; responses are the documents Amazon
//! SQS and ElasticMQ send.
@test_module
package Mq.Sqs.WireTests

import Std.Core
import Std.Collections
import Std.JsonValue
import Std.String as Str
import Std.Testing
import Mq
import Mq.Sqs.Wire

func message(id: in String, body: in String): Mq.Message {
  Mq.Message(id = id, body = body, headers = [], deliveryCount = 0)
}

func encoded(m: in Mq.Message): Outgoing {
  match encodeMessage(m) {
    case Ok(o) -> o
    case Err(e) -> panic("encodeMessage failed: " + e)
  }
}

func attributesText(attributes: in List[Attribute]): String {
  var text = ""
  var i = 0
  while i < attributes.count {
    text = text + (if i > 0 { ";" } else { "" }) + attributes[i].name + ":" + attributes[i].dataType + "=" +
      attributes[i].value
    i = i + 1
  }
  text
}

func noFields(): SendFields {
  SendFields(delaySeconds = -1, messageGroupId = "", deduplicationId = "")
}

func document(text: in String): JsonValue {
  match parseDocument(text) {
    case Ok(d) -> d
    case Err(e) -> panic("parseDocument failed: " + e)
  }
}

test "a plain message is its body and a lyric.id attribute" {
  val o = encoded(message("o-1", "{\"n\":1}"))
  assertEqual(o.body, "{\"n\":1}", "the body is sent as it is")
  assertEqual(attributesText(o.attributes), "lyric.id:String=o-1", "only the id")
  assertEqualInt(outgoingSize(o), 7 + 8 + 6 + 3, "body, attribute name, data type and value")
}

test "headers travel as one JSON attribute, in order, with every character escaped that SQS refuses" {
  val m = Mq.Message(
    id = "h",
    body = "x",
    headers = [
      Mq.MessageHeader(key = "content-type", value = "application/json"),
      Mq.MessageHeader(key = "k\"1", value = "v\n1\u{0001}"),
      Mq.MessageHeader(key = "content-type", value = "again"),
      Mq.MessageHeader(key = "", value = "")
    ],
    deliveryCount = 7
  )
  val o = encoded(m)
  assertEqual(
    attributesText(o.attributes),
    "lyric.id:String=h;lyric.headers:String=[{\"key\":\"content-type\",\"value\":\"application/json\"}," +
      "{\"key\":\"k\\\"1\",\"value\":\"v\\n1\\u0001\"},{\"key\":\"content-type\",\"value\":\"again\"}," +
      "{\"key\":\"\",\"value\":\"\"}]",
    "the attributes"
  )
  assertTrue(isSqsText(o.attributes[1].value), "the attribute value is text SQS accepts")
  val back = Incoming(
    messageId = "sqs-1",
    receiptHandle = "rh",
    body = o.body,
    attributes = o.attributes,
    receiveCount = 1
  )
  match decodeMessage(back) {
    case Err(e) -> assertTrue(false, "decode: " + e)
    case Ok(got) -> {
      assertEqual(got.id, "h", "id")
      assertEqualInt(got.headers.length, 4, "header count")
      assertEqual(got.headers[1].key, "k\"1", "a quoted key")
      assertEqual(got.headers[1].value, "v\n1\u{0001}", "a control character in a value")
      assertEqual(got.headers[2].value, "again", "a duplicate key keeps its place")
      assertEqual(got.headers[3].key, "", "an empty key and value survive")
      assertEqualInt(got.deliveryCount, 0, "deliveryCount comes from the receive count, not from the sender")
    }
  }
}

test "a body SQS cannot carry is sent as base64, an empty one as a placeholder" {
  assertTrue(isSqsText("tab\tnl\ncr\r café 世 😀"), "ordinary text, astral characters included")
  assertTrue(not isSqsText("a\u{0001}b"), "a control character")
  assertTrue(not isSqsText("a\u{0000}"), "NUL")
  assertTrue(not isSqsText("￾"), "U+FFFE")
  assertTrue(not isSqsText("￿"), "U+FFFF")
  assertTrue(isSqsText("�"), "U+FFFD is the last BMP character allowed")
  assertTrue(isSqsText(""), "the empty string has no bad character")

  val control = encoded(message("c", "line1\n\u{0001}ctl café"))
  assertEqual(control.body, "bGluZTEKAWN0bCBjYWbDqQ==", "base64 of the UTF-8 bytes")
  assertEqual(attributesText(control.attributes), "lyric.id:String=c;lyric.body-encoding:String=base64", "marked")
  val empty = encoded(message("e", ""))
  assertEqual(empty.body, "-", "SQS refuses an empty body")
  assertEqual(attributesText(empty.attributes), "lyric.id:String=e;lyric.body-encoding:String=empty", "marked")

  for o in [control, empty] {
    val back = Incoming(messageId = "s", receiptHandle = "rh", body = o.body, attributes = o.attributes, receiveCount = 3)
    match decodeMessage(back) {
      case Err(e) -> assertTrue(false, "decode: " + e)
      case Ok(got) -> {
        assertEqual(got.body, if o.body == "-" { "" } else { "line1\n\u{0001}ctl café" }, "the body comes back")
        assertEqualInt(got.deliveryCount, 2, "third receive is the second redelivery")
      }
    }
  }
  // A body that merely looks encoded is not touched.
  val literal = encoded(message("l", "-"))
  assertEqual(attributesText(literal.attributes), "lyric.id:String=l", "a literal '-' needs no marker")
}

func encodeError(m: in Mq.Message): String {
  match encodeMessage(m) {
    case Ok(_) -> "ok"
    case Err(e) -> e
  }
}

test "an id SQS cannot carry is refused, not altered" {
  assertEqual(encodeError(message("", "x")), "empty message id", "empty")
  assertEqual(
    encodeError(message("a\u{0007}b", "x")),
    "message id holds a character SQS cannot carry (control character or unpaired surrogate)",
    "a control character"
  )
  assertEqual(encodeError(message("id with spaces / ünïcode 世", "x")), "ok", "anything printable is fine")
}

func decodeText(incoming: in Incoming): String {
  match decodeMessage(incoming) {
    case Err(e) -> "Err: " + e
    case Ok(m) -> {
      var text = m.id + " | " + m.body + " | " + m.deliveryCount.toString()
      for h in m.headers {
        text = text + " | " + h.key + "=" + h.value
      }
      text
    }
  }
}

func attributes(flat: in slice[String]): List[Attribute] {
  val acc: List[Attribute] = newList()
  var i = 0
  while i + 2 < flat.length {
    acc.add(Attribute(name = flat[i], dataType = flat[i + 1], value = flat[i + 2]))
    i = i + 3
  }
  acc
}

test "a message from another producer keeps its SQS id and its attributes as headers" {
  val foreign = Incoming(
    messageId = "5fea7756-0ea4-451a-a703-a558b933e274",
    receiptHandle = "rh",
    body = "hello",
    attributes = attributes(
      ["zeta", "String", "z", "Alpha", "String.custom", "a", "count", "Number", "3"]
    ),
    receiveCount = 1
  )
  assertEqual(
    decodeText(foreign),
    "5fea7756-0ea4-451a-a703-a558b933e274 | hello | 0 | Alpha=a | count=3 | zeta=z",
    "attributes in name order"
  )
  val bare = Incoming(messageId = "m", receiptHandle = "rh", body = "b", attributes = newList(), receiveCount = 0)
  assertEqual(decodeText(bare), "m | b | 0", "no attributes, no receive count reported")
  val ours = Incoming(
    messageId = "m",
    receiptHandle = "rh",
    body = "b",
    attributes = attributes(["lyric.id", "String", "mine", "trace", "String", "t"]),
    receiveCount = 2
  )
  assertEqual(decodeText(ours), "mine | b | 1", "with lyric.id the headers are lyric.headers only")
}

test "a lyric attribute that is not what this library writes is an Err, not a guess" {
  val badEncoding = Incoming(
    messageId = "m",
    receiptHandle = "rh",
    body = "b",
    attributes = attributes(["lyric.id", "String", "x", "lyric.body-encoding", "String", "gzip"]),
    receiveCount = 1
  )
  assertEqual(decodeText(badEncoding), "Err: unknown lyric.body-encoding 'gzip'", "encoding")
  val badBase64 = Incoming(
    messageId = "m",
    receiptHandle = "rh",
    body = "not base64!",
    attributes = attributes(["lyric.id", "String", "x", "lyric.body-encoding", "String", "base64"]),
    receiveCount = 1
  )
  assertEqual(decodeText(badBase64), "Err: the body is marked base64 and is not base64", "base64")
  val notUtf8 = Incoming(
    messageId = "m",
    receiptHandle = "rh",
    body = "/w==",
    attributes = attributes(["lyric.id", "String", "x", "lyric.body-encoding", "String", "base64"]),
    receiveCount = 1
  )
  assertEqual(decodeText(notUtf8), "Err: the body is marked base64 and does not decode to UTF-8", "UTF-8")
  val badHeaders = Incoming(
    messageId = "m",
    receiptHandle = "rh",
    body = "b",
    attributes = attributes(["lyric.id", "String", "x", "lyric.headers", "String", "{\"key\":1}"]),
    receiveCount = 1
  )
  assertEqual(decodeText(badHeaders), "Err: lyric.headers is not a JSON array", "headers shape")
  val badEntry = Incoming(
    messageId = "m",
    receiptHandle = "rh",
    body = "b",
    attributes = attributes(["lyric.id", "String", "x", "lyric.headers", "String", "[{\"key\":\"k\"}]"]),
    receiveCount = 1
  )
  assertEqual(decodeText(badEntry), "Err: lyric.headers entry 0 has no string 'value'", "headers entry")
  assertEqual(idOf(badEntry), "x", "the id is still known, so the delivery can be settled")
}

test "SendMessage and SendMessageBatch request bodies" {
  val o = encoded(message("o-1", "hi \"there\"\n"))
  assertEqual(
    sendMessageRequest("https://sqs.eu-west-1.amazonaws.com/1/q", o, noFields()),
    "{\"QueueUrl\":\"https://sqs.eu-west-1.amazonaws.com/1/q\",\"MessageBody\":\"hi \\\"there\\\"\\n\"," +
      "\"MessageAttributes\":{\"lyric.id\":{\"DataType\":\"String\",\"StringValue\":\"o-1\"}}}",
    "a standard queue"
  )
  val fifo = SendFields(delaySeconds = 5, messageGroupId = "g1", deduplicationId = "o-1")
  assertEqual(
    sendMessageRequest("u", Outgoing(body = "b", attributes = newList()), fifo),
    "{\"QueueUrl\":\"u\",\"MessageBody\":\"b\",\"DelaySeconds\":5,\"MessageGroupId\":\"g1\",\"MessageDeduplicationId\":\"o-1\"}",
    "delay, group and deduplication id; no attributes when there are none"
  )
  val entries: List[BatchEntry] = newList()
  entries.add(BatchEntry(entryId = "0", outgoing = encoded(message("a", "1")), send = noFields()))
  entries.add(
    BatchEntry(
      entryId = "1",
      outgoing = encoded(message("b", "2")),
      send = SendFields(delaySeconds = -1, messageGroupId = "g", deduplicationId = "b")
    )
  )
  assertEqual(
    sendMessageBatchRequest("u", entries),
    "{\"QueueUrl\":\"u\",\"Entries\":[" +
      "{\"Id\":\"0\",\"MessageBody\":\"1\",\"MessageAttributes\":{\"lyric.id\":{\"DataType\":\"String\",\"StringValue\":\"a\"}}}," +
      "{\"Id\":\"1\",\"MessageBody\":\"2\",\"MessageAttributes\":{\"lyric.id\":{\"DataType\":\"String\",\"StringValue\":\"b\"}}," +
      "\"MessageGroupId\":\"g\",\"MessageDeduplicationId\":\"b\"}]}",
    "a batch"
  )
}

test "ReceiveMessage, DeleteMessage, ChangeMessageVisibility and queue request bodies" {
  assertEqual(
    receiveMessageRequest("u", 10, 20, 30),
    "{\"QueueUrl\":\"u\",\"MaxNumberOfMessages\":10,\"WaitTimeSeconds\":20,\"VisibilityTimeout\":30," +
      "\"MessageAttributeNames\":[\"All\"],\"MessageSystemAttributeNames\":[\"ApproximateReceiveCount\"]}",
    "receive"
  )
  assertEqual(deleteMessageRequest("u", "rh+/="), "{\"QueueUrl\":\"u\",\"ReceiptHandle\":\"rh+/=\"}", "delete")
  assertEqual(
    changeVisibilityRequest("u", "rh", 0),
    "{\"QueueUrl\":\"u\",\"ReceiptHandle\":\"rh\",\"VisibilityTimeout\":0}",
    "change visibility"
  )
  val handles: List[String] = newList()
  handles.add("h0")
  handles.add("h1")
  assertEqual(
    changeVisibilityBatchRequest("u", handles, 0),
    "{\"QueueUrl\":\"u\",\"Entries\":[{\"Id\":\"0\",\"ReceiptHandle\":\"h0\",\"VisibilityTimeout\":0}," +
      "{\"Id\":\"1\",\"ReceiptHandle\":\"h1\",\"VisibilityTimeout\":0}]}",
    "change visibility batch"
  )
  assertEqual(stringFieldsRequest(["QueueName"], ["orders"]), "{\"QueueName\":\"orders\"}", "GetQueueUrl")
  assertEqual(
    attributeMapRequest(
      "QueueName",
      "q.fifo",
      "Attributes",
      attributes(["FifoQueue", "String", "true", "DelaySeconds", "String", "5"])
    ),
    "{\"QueueName\":\"q.fifo\",\"Attributes\":{\"FifoQueue\":\"true\",\"DelaySeconds\":\"5\"}}",
    "CreateQueue"
  )
  assertEqual(
    attributeMapRequest("QueueName", "q", "Attributes", newList()),
    "{\"QueueName\":\"q\"}",
    "CreateQueue, defaults"
  )
  assertEqual(queueAttributesRequest("u"), "{\"QueueUrl\":\"u\",\"AttributeNames\":[\"All\"]}", "GetQueueAttributes")
}

func received(text: in String): String {
  match parseReceiveResponse(document(text)) {
    case Err(e) -> "Err: " + e
    case Ok(messages) -> {
      var line = messages.count.toString()
      var i = 0
      while i < messages.count {
        line = line + " | " + messages[i].messageId + "," + messages[i].receiptHandle + "," + messages[i].body + "," +
          messages[i].receiveCount.toString() +
          "," +
          attributesText(messages[i].attributes)
        i = i + 1
      }
      line
    }
  }
}

test "a ReceiveMessage response, as Amazon SQS and as ElasticMQ send it" {
  assertEqual(received("{}"), "0", "Amazon SQS: no Messages member when the queue is empty")
  assertEqual(received(""), "0", "an empty body")
  assertEqual(received("{\"Messages\":[]}"), "0", "ElasticMQ: an empty array")
  assertEqual(received("{\"Messages\":null}"), "0", "null")
  assertEqual(
    received(
      "{\"Messages\":[{\"Attributes\":{\"ApproximateReceiveCount\":\"2\"},\"Body\":\"a\"," +
        "\"MD5OfBody\":\"0cc175b9c0f1b6a831c399e269772661\",\"MessageAttributes\":{" +
        "\"lyric.id\":{\"DataType\":\"String\",\"StringValue\":\"a-1\"}," +
        "\"blob\":{\"DataType\":\"Binary\",\"BinaryValue\":\"AAE=\"}," +
        "\"n\":{\"DataType\":\"Number.int\",\"StringValue\":\"7\"}}," +
        "\"MessageId\":\"7cdbc3e0\",\"ReceiptHandle\":\"7cdbc3e0#8a17442d\"}," +
        "{\"Body\":\"b\",\"MessageId\":\"c81e6b4c\",\"ReceiptHandle\":\"rh2\"}]}"
    ),
    "2 | 7cdbc3e0,7cdbc3e0#8a17442d,a,2,lyric.id:String=a-1;n:Number.int=7 | c81e6b4c,rh2,b,0,",
    "two messages; the binary attribute is dropped"
  )
  assertEqual(received("{\"Messages\":{}}"), "Err: ReceiveMessage: 'Messages' is not an array", "a wrong shape")
  assertEqual(
    received("{\"Messages\":[{\"Body\":\"b\"}]}"),
    "Err: ReceiveMessage: a message has no ReceiptHandle",
    "no handle"
  )
  assertEqual(
    received("{\"Messages\":[{\"ReceiptHandle\":\"r\"}]}"),
    "Err: ReceiveMessage: a message has no Body",
    "no body"
  )
}

func batchText(text: in String): String {
  match parseBatchResponse(document(text)) {
    case Err(e) -> "Err: " + e
    case Ok(outcome) -> {
      var line = "ok=" + Str.joinList(",", outcome.successful)
      var i = 0
      while i < outcome.failed.count {
        val f = outcome.failed[i]
        line = line + " | " + f.entryId + ":" + f.code + ":" + f.message + ":" +
          (if f.senderFault { "sender" } else { "server" })
        i = i + 1
      }
      line
    }
  }
}

test "a batch response lists what was applied and what was not" {
  assertEqual(
    batchText(
      "{\"Successful\":[{\"Id\":\"0\",\"MessageId\":\"m0\",\"MD5OfMessageBody\":\"x\"},{\"Id\":\"2\",\"MessageId\":\"m2\"}]," +
        "\"Failed\":[{\"Id\":\"1\",\"Code\":\"InternalError\",\"Message\":\"try again\",\"SenderFault\":false}," +
        "{\"Id\":\"3\",\"Code\":\"InvalidMessageContents\",\"SenderFault\":true}]}"
    ),
    "ok=0,2 | 1:InternalError:try again:server | 3:InvalidMessageContents::sender",
    "partial failure"
  )
  assertEqual(batchText("{\"Successful\":[{\"Id\":\"0\"}]}"), "ok=0", "no Failed member")
  assertEqual(batchText("{}"), "ok=", "nothing listed")
  assertEqual(
    batchText("{\"Failed\":[{\"Code\":\"X\"}]}"),
    "Err: a batch error entry has no Id",
    "an entry without an id"
  )
}

func faultText(body: in String): String {
  match parseFault(body) {
    case Some(f) -> f.code + " | " + f.message
    case None -> "none"
  }
}

test "an error document gives the code without its namespace" {
  assertEqual(
    faultText(
      "{\"__type\":\"com.amazonaws.sqs#QueueDoesNotExist\",\"message\":\"The specified queue does not exist.\"}"
    ),
    "QueueDoesNotExist | The specified queue does not exist.",
    "Amazon SQS"
  )
  assertEqual(
    faultText(
      "{\"Message\":\"The specified queue does not exist.\",\"__type\":\"com.amazonaws.sqs#QueueDoesNotExist\"}"
    ),
    "QueueDoesNotExist | The specified queue does not exist.",
    "ElasticMQ capitalises Message"
  )
  assertEqual(
    faultText("{\"__type\":\"com.amazon.coral.service#InvalidSignatureException\",\"message\":\"m\"}"),
    "InvalidSignatureException | m",
    "a service-level error"
  )
  assertEqual(faultText("{\"__type\":\"ThrottlingException\"}"), "ThrottlingException | ", "no namespace, no message")
  assertEqual(faultText("<html><body>502 Bad Gateway</body></html>"), "none", "not JSON")
  assertEqual(faultText(""), "none", "empty")
  assertEqual(faultText("{\"message\":\"no type\"}"), "none", "no code")
  assertEqual(faultText("[1]"), "none", "not an object")
}

func plan(sizes: in slice[Int], maxEntries: in Int, maxBytes: in Int): String {
  val list: List[Int] = newList()
  for s in sizes {
    list.add(s)
  }
  val parts: List[String] = newList()
  val lengths = planBatches(list, maxEntries, maxBytes)
  var i = 0
  while i < lengths.count {
    parts.add(lengths[i].toString())
    i = i + 1
  }
  Str.joinList(",", parts)
}

test "a batch is split at ten entries and at the byte limit, in order" {
  assertEqual(plan([1, 1, 1], 10, 100), "3", "one request")
  assertEqual(plan([1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1], 10, 100), "10,2", "ten entries per request")
  assertEqual(plan([60, 40, 1, 99, 100, 100], 10, 100), "2,2,1,1", "the byte limit, filled exactly")
  assertEqual(plan([50, 500, 50], 10, 100), "1,1,1", "an oversized message goes alone")
  assertEqual(plan([], 10, 100), "", "nothing to send")
}

test "parseDocument refuses what is not a JSON object" {
  match parseDocument("[]") {
    case Ok(_) -> assertTrue(false, "an array is not a response document")
    case Err(e) -> assertEqual(e, "the response is not a JSON object", "an array")
  }
  match parseDocument("<html>") {
    case Ok(_) -> assertTrue(false, "HTML is not a response document")
    case Err(e) -> assertTrue(e.startsWith("the response is not JSON: "), "HTML: " + e)
  }
}
