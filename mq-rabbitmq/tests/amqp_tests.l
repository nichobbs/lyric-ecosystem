//! tests/amqp_tests.l - the AMQP 0-9-1 codec: the protocol header, frames
//! and the incremental decoder, field tables with every RabbitMQ type,
//! methods in both directions, content headers, body splitting, and malformed
//! input of each kind.
@test_module
package Mq.RabbitMq.AmqpTests

import Std.Core
import Std.Collections
import Std.Testing
import Std.Encoding
import Std.String as Str
import Mq.RabbitMq.Amqp as Amqp

func bytesOf(values: in slice[Int]): slice[Byte] {
  val acc: List[Byte] = newList()
  for v in values {
    acc.add(v.toByte())
  }
  acc.toArray()
}

func hex(bytes: in slice[Byte]): String {
  Str.toLower(encodeHex(bytes))
}

func unhex(text: in String): slice[Byte] {
  match tryDecodeHex(text) {
    case Some(b) -> b
    case None -> panic("bad hex in test: " + text)
  }
}

func encoded(m: in Amqp.Method): slice[Byte] {
  match Amqp.encodeMethod(m) {
    case Ok(b) -> b
    case Err(e) -> panic("encode failed: " + e)
  }
}

func decoded(payload: in slice[Byte]): Amqp.Method {
  match Amqp.decodeMethod(payload) {
    case Ok(m) -> m
    case Err(e) -> panic("decode failed: " + e)
  }
}

// Encode, decode and encode again: the name and the bytes must survive.
func roundTrips(m: in Amqp.Method): Unit {
  val first = encoded(m)
  val back = decoded(first)
  assertEqual(Amqp.methodName(back), Amqp.methodName(m), "the method survives decoding")
  assertEqual(hex(encoded(back)), hex(first), Amqp.methodName(m) + " re-encodes to the same bytes")
}

func decodeOne(d: in Amqp.Decoder): Amqp.Frame {
  match Amqp.nextFrame(d) {
    case Ok(Some(f)) -> f
    case Ok(None) -> panic("expected a complete frame")
    case Err(e) -> panic("frame decode failed: " + e)
  }
}

func expectIncomplete(d: in Amqp.Decoder, label: in String): Unit {
  match Amqp.nextFrame(d) {
    case Ok(None) -> assertTrue(true, label)
    case Ok(Some(_)) -> assertTrue(false, label + ": a frame was produced")
    case Err(e) -> assertTrue(false, label + ": " + e)
  }
}

func frameError(wire: in slice[Byte], frameMax: in Int): String {
  val d = Amqp.newDecoder(frameMax)
  Amqp.feed(d, wire)
  match Amqp.nextFrame(d) {
    case Err(e) -> e
    case Ok(_) -> ""
  }
}

func args(): slice[Amqp.TableEntry] {
  [Amqp.entry("x-queue-type", Amqp.fText("quorum")), Amqp.entry("x-message-ttl", Amqp.fInt(60000))]
}

// ─── Protocol header and frames ──────────────────────────────────────────────
test "the protocol header is AMQP 0 0 9 1" {
  assertEqual(hex(Amqp.protocolHeader()), "414d515000000901", "the eight bytes")
  assertTrue(Amqp.isProtocolHeader(Amqp.protocolHeader()), "recognised")
  assertTrue(not Amqp.isProtocolHeader(unhex("414d515000000900")), "another version is not 0-9-1")
  assertTrue(not Amqp.isProtocolHeader(unhex("414d51")), "a short prefix is not a header")
}

test "a frame is type, channel, size, payload and the end octet" {
  val wire = Amqp.encodeFrame(Amqp.FRAME_BODY, 1, bytesOf([104, 105]))
  assertEqual(hex(wire), "03000100000002" + "6869" + "ce", "a two-byte body frame on channel 1")
  assertEqual(hex(Amqp.heartbeatFrame()), "0800000000000" + "0ce", "a heartbeat frame")
}

test "the decoder yields frames however the bytes arrive" {
  val one = Amqp.encodeFrame(Amqp.FRAME_METHOD, 0, encoded(Amqp.ConnectionCloseOk))
  val two = Amqp.encodeFrame(Amqp.FRAME_BODY, 1, encodeUtf8("payload"))
  val wire = one.concat(Amqp.heartbeatFrame()).concat(two)
  val d = Amqp.newDecoder(4096)
  var i = 0
  var seen = 0
  val kinds: List[Int] = newList()
  while i < wire.length {
    Amqp.feed(d, wire.slice(i, i + 1))
    match Amqp.nextFrame(d) {
      case Ok(Some(f)) -> {
        kinds.add(f.kind)
        seen += 1
      }
      case Ok(None) -> ()
      case Err(e) -> assertTrue(false, "byte-at-a-time decoding failed: " + e)
    }
    i += 1
  }
  assertEqualInt(seen, 3, "three frames from byte-sized pieces")
  assertEqualInt(kinds[0], Amqp.FRAME_METHOD, "first the method frame")
  assertEqualInt(kinds[1], Amqp.FRAME_HEARTBEAT, "then the heartbeat")
  assertEqualInt(kinds[2], Amqp.FRAME_BODY, "then the body")
  assertEqualInt(Amqp.buffered(d), 0, "nothing is left over")
  val all = Amqp.newDecoder(4096)
  Amqp.feed(all, wire)
  assertEqualInt(decodeOne(all).kind, Amqp.FRAME_METHOD, "first of three in one feed")
  assertEqualInt(decodeOne(all).kind, Amqp.FRAME_HEARTBEAT, "second of three in one feed")
  val body = decodeOne(all)
  assertEqualInt(body.channel, 1, "the body frame's channel")
  assertEqual(hex(body.payload), hex(encodeUtf8("payload")), "the body frame's payload")
  expectIncomplete(all, "no fourth frame")
}

test "an incomplete frame waits for the rest" {
  val wire = Amqp.encodeFrame(Amqp.FRAME_BODY, 1, encodeUtf8("0123456789"))
  val d = Amqp.newDecoder(4096)
  Amqp.feed(d, wire.slice(0, 3))
  expectIncomplete(d, "less than a frame header")
  Amqp.feed(d, wire.slice(3, 12))
  expectIncomplete(d, "a header and part of the payload")
  Amqp.feed(d, wire.slice(12, wire.length))
  assertEqualInt(decodeOne(d).payload.length, 10, "the whole payload once it has all arrived")
}

test "malformed frames are refused" {
  assertTrue(frameError(unhex("0300010000000268690a"), 4096).contains("frame-end"), "a wrong end octet")
  assertTrue(frameError(unhex("0700000000000" + "0ce"), 4096).contains("unknown frame type 7"), "an unknown frame type")
  assertTrue(
    frameError(Amqp.protocolHeader(), 4096).contains("does not accept AMQP 0-9-1"),
    "a protocol header where a frame should be"
  )
  val big = Amqp.encodeFrame(Amqp.FRAME_BODY, 1, encodeUtf8("0123456789"))
  assertTrue(frameError(big, 16).contains("exceeds the negotiated frame-max"), "a frame over the limit")
  assertEqual(frameError(big, 18), "", "a frame of exactly frame-max bytes is accepted")
  assertEqual(frameError(big, 0), "", "frame-max 0 is no limit")
  assertTrue(frameError(unhex("030001ffffffff00"), 0).contains("too large"), "a size no buffer could hold")
}

// ─── Field tables ────────────────────────────────────────────────────────────
func everyType(): slice[Amqp.TableEntry] {
  val nested: slice[Amqp.TableEntry] = [Amqp.entry("inner", Amqp.fText("v")), Amqp.entry("n", Amqp.FVoid)]
  val items: slice[Amqp.FieldValue] = [
    Amqp.fInt(1),
    Amqp.fText("two"),
    Amqp.FBool(value = true),
    Amqp.FTable(entries = nested)
  ];
  [
    Amqp.entry("bool", Amqp.FBool(value = true)),
    Amqp.entry("int8", Amqp.FInteger(tag = 98, value = -128i64)),
    Amqp.entry("uint8", Amqp.FInteger(tag = 66, value = 255i64)),
    Amqp.entry("int16", Amqp.FInteger(tag = 115, value = -2i64)),
    Amqp.entry("uint16", Amqp.FInteger(tag = 117, value = 65535i64)),
    Amqp.entry("int32", Amqp.fInt(-2147483647)),
    Amqp.entry("uint32", Amqp.FInteger(tag = 105, value = 4294967295i64)),
    Amqp.entry("int64", Amqp.fLong(-9007199254740993i64)),
    Amqp.entry("float", Amqp.FFloat(bits = 1069547520)),
    Amqp.entry("double", Amqp.FDouble(bits = -4616189618054758400i64)),
    Amqp.entry("decimal", Amqp.FDecimal(scale = 2, unscaled = -1234)),
    Amqp.entry("string", Amqp.fText("café 世")),
    Amqp.entry("bytes", Amqp.FBytes(bytes = bytesOf([0, 255, 128]))),
    Amqp.entry("array", Amqp.FArray(items = items)),
    Amqp.entry("timestamp", Amqp.FTimestamp(seconds = 1700000000i64)),
    Amqp.entry("table", Amqp.FTable(entries = nested)),
    Amqp.entry("void", Amqp.FVoid)
  ]
}

func tableBytes(table: in slice[Amqp.TableEntry]): slice[Byte] {
  match Amqp.encodeTable(table) {
    case Ok(b) -> b
    case Err(e) -> panic("table encode failed: " + e)
  }
}

func tableError(wire: in slice[Byte]): String {
  match Amqp.decodeTable(wire) {
    case Err(e) -> e
    case Ok(_) -> ""
  }
}

test "a table with every RabbitMQ field type round-trips byte for byte" {
  val wire = tableBytes(everyType())
  match Amqp.decodeTable(wire) {
    case Err(e) -> assertTrue(false, e)
    case Ok(back) -> {
      assertEqualInt(back.length, 17, "every entry decoded")
      assertEqual(hex(tableBytes(back)), hex(wire), "re-encoding gives the same bytes")
      assertEqual(back[0].name, "bool", "entries keep their order")
      match Amqp.tableGet(back, "int8") {
        case Some(FInteger(tag, v)) -> {
          assertEqualInt(tag, 98, "the integer keeps its width")
          assertEqualLong(v, -128i64, "a signed 8-bit value is sign-extended")
        }
        case _ -> assertTrue(false, "int8 did not decode as an integer")
      }
      match Amqp.tableGet(back, "int32") {
        case Some(FInteger(_, v)) -> assertEqualLong(v, -2147483647i64, "a signed 32-bit value is sign-extended")
        case _ -> assertTrue(false, "int32 did not decode as an integer")
      }
      match Amqp.tableGet(back, "uint32") {
        case Some(FInteger(_, v)) -> assertEqualLong(v, 4294967295i64, "an unsigned 32-bit value stays positive")
        case _ -> assertTrue(false, "uint32 did not decode as an integer")
      }
      match Amqp.tableGet(back, "string") {
        case Some(FString(bytes)) -> assertEqual(hex(bytes), hex(encodeUtf8("café 世")), "the string's bytes")
        case _ -> assertTrue(false, "string did not decode as a long string")
      }
      match Amqp.tableGet(back, "array") {
        case Some(FArray(items)) -> assertEqualInt(items.length, 4, "the array's items")
        case _ -> assertTrue(false, "array did not decode as an array")
      }
      assertTrue(not isSome(Amqp.tableGet(back, "missing")), "an absent name is None")
    }
  }
}

test "a table's wire form is length, then name, tag and value per entry" {
  val table: slice[Amqp.TableEntry] = [Amqp.entry("a", Amqp.fInt(1)), Amqp.entry("b", Amqp.fText("x"))]
  assertEqual(
    hex(tableBytes(table)),
    "0000000f" + "0161" + "49" + "00000001" + "0162" + "53" + "00000001" + "78",
    "bytes"
  )
  assertEqual(hex(tableBytes(Amqp.emptyTable())), "00000000", "an empty table is a zero length")
}

test "tables that cannot be encoded are refused" {
  val long = Str.repeat("k", 256)
  assertTrue(isErr(Amqp.encodeTable([Amqp.entry(long, Amqp.FVoid)])), "a name over 255 bytes")
  assertTrue(isErr(Amqp.encodeTable([Amqp.entry("", Amqp.FVoid)])), "an empty name")
  assertTrue(isErr(Amqp.encodeTable([Amqp.entry("n", Amqp.FInteger(tag = 66, value = 256i64))])), "an integer too wide")
  assertTrue(
    isErr(Amqp.encodeTable([Amqp.entry("n", Amqp.FInteger(tag = 90, value = 1i64))])),
    "an unknown integer tag"
  )
  var deep: Amqp.FieldValue = Amqp.FVoid
  var i = 0
  while i < 20 {
    val wrapped: slice[Amqp.TableEntry] = [Amqp.entry("t", deep)]
    deep = Amqp.FTable(entries = wrapped)
    i += 1
  }
  assertTrue(isErr(Amqp.encodeTable([Amqp.entry("deep", deep)])), "nesting past the limit")
}

test "malformed tables are refused" {
  assertTrue(tableError(unhex("000000")).contains("malformed field table"), "a cut length")
  assertTrue(tableError(unhex("00000005" + "0161")).contains("truncated"), "a length past the end")
  assertTrue(tableError(unhex("00000003" + "0161" + "5a")).contains("unknown type tag 90"), "an unknown type tag")
  assertTrue(tableError(unhex("00000004" + "0161" + "4900")).contains("truncated"), "a value cut short")
  assertTrue(tableError(unhex("00000003" + "0561" + "56")).contains("truncated"), "a name longer than the table")
  assertTrue(
    tableError(unhex("00000008" + "0161" + "53" + "000000ff" + "78")).contains("truncated"),
    "a string past the end"
  )
  assertTrue(tableError(unhex("00000003" + "01ff" + "56")).contains("UTF-8"), "a name that is not UTF-8")
  assertTrue(tableError(unhex("00000003" + "0161" + "56" + "00")).contains("trailing"), "bytes after the table")
  // 20 nested arrays: `A` + length for each level.
  var nested = unhex("56")
  var level = 0
  while level < 20 {
    val w = bytesOf([65, 0, 0, 0, nested.length])
    nested = w.concat(nested)
    level += 1
  }
  val wire = bytesOf([0, 0, 0, nested.length + 2, 1, 97]).concat(nested)
  assertTrue(tableError(wire).contains("nests deeper"), "arrays nested past the limit")
}

// ─── Methods ─────────────────────────────────────────────────────────────────
test "every method round-trips" {
  roundTrips(
    Amqp.ConnectionStart(
      versionMajor = 0,
      versionMinor = 9,
      serverProperties = args(),
      mechanisms = "PLAIN AMQPLAIN",
      locales = "en_US"
    )
  )
  roundTrips(
    Amqp.ConnectionStartOk(
      clientProperties = args(),
      mechanism = "PLAIN",
      response = bytesOf([0, 117, 0, 112]),
      locale = "en_US"
    )
  )
  roundTrips(Amqp.ConnectionSecure(challenge = bytesOf([1, 2])))
  roundTrips(Amqp.ConnectionTune(channelMax = 2047, frameMax = 131072i64, heartbeat = 60))
  roundTrips(Amqp.ConnectionTuneOk(channelMax = 1, frameMax = 4096i64, heartbeat = 0))
  roundTrips(Amqp.ConnectionOpen(virtualHost = "/"))
  roundTrips(Amqp.ConnectionOpenOk)
  roundTrips(Amqp.ConnectionClose(replyCode = 320, replyText = "CONNECTION_FORCED", classId = 0, methodId = 0))
  roundTrips(Amqp.ConnectionCloseOk)
  roundTrips(Amqp.ConnectionBlocked(reason = "low on memory"))
  roundTrips(Amqp.ConnectionUnblocked)
  roundTrips(Amqp.ChannelOpen)
  roundTrips(Amqp.ChannelOpenOk)
  roundTrips(Amqp.ChannelFlow(active = true))
  roundTrips(Amqp.ChannelFlowOk(active = false))
  roundTrips(Amqp.ChannelClose(replyCode = 404, replyText = "NOT_FOUND - no queue", classId = 50, methodId = 10))
  roundTrips(Amqp.ChannelCloseOk)
  roundTrips(
    Amqp.ExchangeDeclare(
      exchange = "events",
      kind = "topic",
      passive = false,
      durable = true,
      autoDelete = false,
      isInternal = true,
      arguments = args()
    )
  )
  roundTrips(Amqp.ExchangeDeclareOk)
  roundTrips(Amqp.ExchangeDelete(exchange = "events", ifUnused = true))
  roundTrips(Amqp.ExchangeDeleteOk)
  roundTrips(
    Amqp.QueueDeclare(
      queue = "orders",
      passive = false,
      durable = true,
      exclusive = false,
      autoDelete = true,
      arguments = args()
    )
  )
  roundTrips(Amqp.QueueDeclareOk(queue = "orders", messageCount = 7i64, consumerCount = 4294967295i64))
  roundTrips(Amqp.QueueBind(queue = "orders", exchange = "events", routingKey = "order.#", arguments = args()))
  roundTrips(Amqp.QueueBindOk)
  roundTrips(Amqp.QueuePurge(queue = "orders"))
  roundTrips(Amqp.QueuePurgeOk(messageCount = 3i64))
  roundTrips(Amqp.QueueDelete(queue = "orders", ifUnused = false, ifEmpty = true))
  roundTrips(Amqp.QueueDeleteOk(messageCount = 0i64))
  roundTrips(Amqp.BasicQos(prefetchSize = 0i64, prefetchCount = 10, global = false))
  roundTrips(Amqp.BasicQosOk)
  roundTrips(
    Amqp.BasicConsume(
      queue = "orders",
      consumerTag = "c-1",
      noLocal = false,
      noAck = false,
      exclusive = true,
      arguments = args()
    )
  )
  roundTrips(Amqp.BasicConsumeOk(consumerTag = "c-1"))
  roundTrips(Amqp.BasicCancel(consumerTag = "c-1", noWait = true))
  roundTrips(Amqp.BasicCancelOk(consumerTag = "c-1"))
  roundTrips(Amqp.BasicPublish(exchange = "", routingKey = "orders", mandatory = true, immediate = false))
  roundTrips(Amqp.BasicReturn(replyCode = 312, replyText = "NO_ROUTE", exchange = "", routingKey = "nowhere"))
  roundTrips(
    Amqp.BasicDeliver(
      consumerTag = "c-1",
      deliveryTag = 1099511627776i64,
      redelivered = true,
      exchange = "",
      routingKey = "orders"
    )
  )
  roundTrips(Amqp.BasicAck(deliveryTag = 5i64, multiple = true))
  roundTrips(Amqp.BasicReject(deliveryTag = 5i64, requeue = true))
  roundTrips(Amqp.BasicNack(deliveryTag = 5i64, multiple = false, requeue = true))
  roundTrips(Amqp.ConfirmSelect)
  roundTrips(Amqp.ConfirmSelectOk)
}

test "decoded methods carry their arguments" {
  match decoded(encoded(Amqp.ConnectionTune(channelMax = 2047, frameMax = 131072i64, heartbeat = 60))) {
    case ConnectionTune(channelMax, frameMax, heartbeat) -> {
      assertEqualInt(channelMax, 2047, "channel-max")
      assertEqualLong(frameMax, 131072i64, "frame-max")
      assertEqualInt(heartbeat, 60, "heartbeat")
    }
    case _ -> assertTrue(false, "not a connection.tune")
  }
  match decoded(encoded(Amqp.BasicDeliver(consumerTag = "c-1", deliveryTag = 1099511627776i64, redelivered = true, exchange = "ex", routingKey = "rk"))) {
    case BasicDeliver(consumerTag, deliveryTag, redelivered, exchange, routingKey) -> {
      assertEqual(consumerTag, "c-1", "consumer-tag")
      assertEqualLong(deliveryTag, 1099511627776i64, "a delivery tag wider than 32 bits")
      assertTrue(redelivered, "redelivered")
      assertEqual(exchange, "ex", "exchange")
      assertEqual(routingKey, "rk", "routing-key")
    }
    case _ -> assertTrue(false, "not a basic.deliver")
  }
  match decoded(encoded(Amqp.ChannelClose(replyCode = 406, replyText = "PRECONDITION_FAILED", classId = 50, methodId = 10))) {
    case ChannelClose(code, text, classId, methodId) -> {
      assertEqualInt(code, 406, "reply-code")
      assertEqual(text, "PRECONDITION_FAILED", "reply-text")
      assertEqualInt(classId, 50, "class-id")
      assertEqualInt(methodId, 10, "method-id")
    }
    case _ -> assertTrue(false, "not a channel.close")
  }
  match decoded(encoded(Amqp.BasicNack(deliveryTag = 9i64, multiple = false, requeue = true))) {
    case BasicNack(tag, multiple, requeue) -> {
      assertEqualLong(tag, 9i64, "delivery-tag")
      assertTrue(not multiple, "the first bit")
      assertTrue(requeue, "the second bit")
    }
    case _ -> assertTrue(false, "not a basic.nack")
  }
}

test "method wire forms match the specification" {
  // queue.declare: class 50, method 10, reserved short, shortstr, the five
  // bits packed into one octet (durable = bit 1, auto-delete = bit 3), table.
  val declare = Amqp.QueueDeclare(
    queue = "q",
    passive = false,
    durable = true,
    exclusive = false,
    autoDelete = true,
    arguments = Amqp.emptyTable()
  )
  assertEqual(hex(encoded(declare)), "0032000a" + "0000" + "0171" + "0a" + "00000000", "queue.declare")
  val publish = Amqp.BasicPublish(exchange = "", routingKey = "q", mandatory = true, immediate = false)
  assertEqual(hex(encoded(publish)), "003c0028" + "0000" + "00" + "0171" + "01", "basic.publish")
  assertEqual(
    hex(encoded(Amqp.BasicAck(deliveryTag = 258i64, multiple = false))),
    "003c0050" + "0000000000000102" + "00",
    "basic.ack"
  )
  val tuneOk = Amqp.ConnectionTuneOk(channelMax = 1, frameMax = 131072i64, heartbeat = 30)
  assertEqual(hex(encoded(tuneOk)), "000a001f" + "0001" + "00020000" + "001e", "connection.tune-ok")
  val open = Amqp.ConnectionOpen(virtualHost = "/")
  assertEqual(hex(encoded(open)), "000a0028" + "012f" + "00" + "00", "connection.open")
  match Amqp.methodFrame(1, Amqp.ConfirmSelect) {
    case Ok(wire) -> assertEqual(hex(wire), "01000100000005" + "0055000a00" + "ce", "confirm.select as a frame")
    case Err(e) -> assertTrue(false, e)
  }
}

test "methods that cannot be encoded or decoded are refused" {
  val long = Str.repeat("q", 256)
  assertTrue(isErr(Amqp.encodeMethod(Amqp.QueuePurge(queue = long))), "a queue name over 255 bytes")
  assertTrue(isErr(Amqp.encodeMethod(Amqp.Unknown(classId = 90, methodId = 10))), "an unknown method")
  // A close reason is clipped rather than refused: it is diagnostic text.
  val closing = Amqp.ChannelClose(replyCode = 500, replyText = Str.repeat("é", 200), classId = 0, methodId = 0)
  match decoded(encoded(closing)) {
    case ChannelClose(_, text, _, _) -> assertEqualInt(
      text.length,
      127,
      "a reply text is cut to 255 bytes on a character boundary"
    )
    case _ -> assertTrue(false, "not a channel.close")
  }
  assertTrue(isErr(Amqp.decodeMethod(unhex("003c"))), "no method id")
  assertTrue(isErr(Amqp.decodeMethod(unhex("003c0050" + "00000000"))), "basic.ack cut short")
  assertTrue(isErr(Amqp.decodeMethod(unhex("0032000b" + "05" + "6162"))), "a short string past the end")
  assertTrue(isErr(Amqp.decodeMethod(unhex("000a000a" + "0009" + "000000ff"))), "a table past the end")
  match Amqp.decodeMethod(unhex("005a000a")) {
    case Ok(Unknown(classId, methodId)) -> {
      assertEqualInt(classId, 90, "the unknown class")
      assertEqualInt(methodId, 10, "the unknown method")
    }
    case _ -> assertTrue(false, "tx.select is outside this client's set and decodes as Unknown")
  }
  assertEqual(Amqp.methodName(decoded(unhex("003c0046"))), "method 60.70", "basic.get decodes as Unknown too")
}

test "carriesContent names the three content methods" {
  assertTrue(
    Amqp.carriesContent(Amqp.BasicPublish(exchange = "", routingKey = "q", mandatory = false, immediate = false)),
    "publish"
  )
  assertTrue(
    Amqp.carriesContent(Amqp.BasicReturn(replyCode = 312, replyText = "", exchange = "", routingKey = "")),
    "return"
  )
  assertTrue(not Amqp.carriesContent(Amqp.BasicAck(deliveryTag = 1i64, multiple = false)), "ack carries none")
}

// ─── Content headers and bodies ──────────────────────────────────────────────
func fullProperties(): Amqp.Properties {
  Amqp.Properties(
    contentType = "application/json",
    contentEncoding = "utf-8",
    headers = [Amqp.entry("k", Amqp.fText("v"))],
    deliveryMode = 2,
    priority = 9,
    correlationId = "corr",
    replyTo = "reply.q",
    expiration = "60000",
    messageId = "m-1",
    timestamp = 1700000000i64,
    kind = "order.placed",
    userId = "guest",
    appId = "tests"
  )
}

func headerBytes(size: in Long, p: in Amqp.Properties): slice[Byte] {
  match Amqp.encodeContentHeader(size, p) {
    case Ok(b) -> b
    case Err(e) -> panic("content header encode failed: " + e)
  }
}

test "a content header with every property round-trips" {
  val wire = headerBytes(5i64, fullProperties())
  match Amqp.decodeContentHeader(wire) {
    case Err(e) -> assertTrue(false, e)
    case Ok(h) -> {
      assertEqualLong(h.bodySize, 5i64, "body size")
      assertEqual(h.properties.contentType, "application/json", "content-type")
      assertEqual(h.properties.contentEncoding, "utf-8", "content-encoding")
      assertEqualInt(h.properties.headers.length, 1, "headers")
      assertEqualInt(h.properties.deliveryMode, 2, "delivery-mode")
      assertEqualInt(h.properties.priority, 9, "priority")
      assertEqual(h.properties.correlationId, "corr", "correlation-id")
      assertEqual(h.properties.replyTo, "reply.q", "reply-to")
      assertEqual(h.properties.expiration, "60000", "expiration")
      assertEqual(h.properties.messageId, "m-1", "message-id")
      assertEqualLong(h.properties.timestamp, 1700000000i64, "timestamp")
      assertEqual(h.properties.kind, "order.placed", "type")
      assertEqual(h.properties.userId, "guest", "user-id")
      assertEqual(h.properties.appId, "tests", "app-id")
      assertEqual(hex(headerBytes(h.bodySize, h.properties)), hex(wire), "re-encoding gives the same bytes")
    }
  }
}

test "a content header's wire form is class, weight, size, flags, then the present properties" {
  val p = Amqp.noProperties().copy(deliveryMode = 2, messageId = "m")
  assertEqual(
    hex(headerBytes(3i64, p)),
    "003c" + "0000" + "0000000000000003" + "1080" + "02" + "016d",
    "two properties"
  )
  assertEqual(hex(headerBytes(0i64, Amqp.noProperties())), "003c" + "0000" + "0000000000000000" + "0000", "none")
}

test "malformed content headers are refused" {
  assertTrue(isErr(Amqp.decodeContentHeader(unhex("003c0000"))), "cut before the body size")
  assertTrue(isErr(Amqp.decodeContentHeader(unhex("00320000" + "0000000000000000" + "0000"))), "another class")
  assertTrue(
    isErr(Amqp.decodeContentHeader(unhex("003c0000" + "0000000000000000" + "0080"))),
    "a flagged property that is absent"
  )
  assertTrue(
    isErr(Amqp.decodeContentHeader(unhex("003c0000" + "0000000000000000" + "0001"))),
    "continued property flags"
  )
  assertTrue(isErr(Amqp.decodeContentHeader(unhex("003c0000" + "8000000000000000" + "0000"))), "a negative body size")
  assertTrue(isErr(Amqp.encodeContentHeader(0i64, Amqp.noProperties().copy(priority = 256))), "a priority over 255")
  assertTrue(
    isErr(Amqp.encodeContentHeader(0i64, Amqp.noProperties().copy(messageId = Str.repeat("m", 256)))),
    "a long message id"
  )
}

func framesOf(wire: in slice[Byte], frameMax: in Int): List[Amqp.Frame] {
  val d = Amqp.newDecoder(frameMax)
  Amqp.feed(d, wire)
  val frames: List[Amqp.Frame] = newList()
  var more = true
  while more {
    match Amqp.nextFrame(d) {
      case Ok(Some(f)) -> frames.add(f)
      case Ok(None) -> {
        more = false
      }
      case Err(e) -> panic("frame decode failed: " + e)
    }
  }
  frames
}

test "a body is split into frames of at most frame-max and reassembles" {
  val acc: List[Byte] = newList()
  var i = 0
  while i < 10000 {
    acc.add((i % 251).toByte())
    i += 1
  }
  val body = acc.toArray()
  val publish = Amqp.BasicPublish(exchange = "", routingKey = "q", mandatory = true, immediate = false)
  match Amqp.contentFrames(1, publish, Amqp.noProperties().copy(messageId = "m"), body, 4096) {
    case Err(e) -> assertTrue(false, e)
    case Ok(wire) -> {
      val frames = framesOf(wire, 4096)
      assertEqualInt(frames.count, 5, "method, header and three body frames")
      assertEqualInt(frames[0].kind, Amqp.FRAME_METHOD, "the method frame first")
      assertEqualInt(frames[1].kind, Amqp.FRAME_HEADER, "then the content header")
      assertEqualInt(frames[2].payload.length, 4088, "a full body frame is frame-max minus the overhead")
      assertEqualInt(frames[3].payload.length, 4088, "the second body frame")
      assertEqualInt(frames[4].payload.length, 10000 - 2 * 4088, "the rest")
      match Amqp.decodeContentHeader(frames[1].payload) {
        case Ok(h) -> assertEqualLong(h.bodySize, 10000i64, "the header announces the whole body")
        case Err(e) -> assertTrue(false, e)
      }
      val joined = frames[2].payload.concat(frames[3].payload).concat(frames[4].payload)
      assertEqual(hex(joined), hex(body), "the frames reassemble to the body")
    }
  }
}

test "an empty body has no body frame, and frame-max 0 means one frame" {
  val publish = Amqp.BasicPublish(exchange = "", routingKey = "q", mandatory = false, immediate = false)
  val none: slice[Byte] = []
  match Amqp.contentFrames(1, publish, Amqp.noProperties(), none, 4096) {
    case Ok(wire) -> assertEqualInt(framesOf(wire, 4096).count, 2, "method and header only")
    case Err(e) -> assertTrue(false, e)
  }
  match Amqp.contentFrames(1, publish, Amqp.noProperties(), encodeUtf8(Str.repeat("x", 9000)), 0) {
    case Ok(wire) -> assertEqualInt(framesOf(wire, 0).count, 3, "one body frame without a limit")
    case Err(e) -> assertTrue(false, e)
  }
}

test "properties that do not fit one frame are refused" {
  val publish = Amqp.BasicPublish(exchange = "", routingKey = "q", mandatory = false, immediate = false)
  val headers: List[Amqp.TableEntry] = newList()
  var i = 0
  while i < 40 {
    headers.add(Amqp.entry("h" + i.toString(), Amqp.fText(Str.repeat("v", 200))))
    i += 1
  }
  val p = Amqp.noProperties().copy(headers = headers.toArray())
  match Amqp.contentFrames(1, publish, p, encodeUtf8("x"), 4096) {
    case Err(e) -> assertTrue(e.contains("frame-max"), "the error names the limit")
    case Ok(_) -> assertTrue(false, "an oversized content header must be refused")
  }
}
