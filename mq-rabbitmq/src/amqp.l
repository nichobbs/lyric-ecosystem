//! Mq.RabbitMq.Amqp - the AMQP 0-9-1 wire format: frames, field tables,
//! methods and content headers, with an incremental frame decoder.
//!
//! Everything here is computation over bytes; no socket is touched. Both
//! directions of every method are implemented (the client encodes what the
//! broker decodes and the reverse), which is what lets the hermetic tests run
//! the client against a broker written in Lyric.
//!
//! A frame is `type(1) channel(2) size(4) payload(size) 0xCE`. The four types
//! are method (1), content header (2), content body (3) and heartbeat (8).
//!
//! Field tables use the type tags RabbitMQ uses, which differ from the
//! 0-9-1 specification's in places (`s` is a signed 16-bit integer, not a
//! short string; `x` is a byte array): `t b B s u I i l f d D S A T F V x`.
//!
//! Only the methods the client needs are modelled. Any other method decodes
//! as `Unknown(classId, methodId)` so the caller can refuse it by name.
@runtime_checked
package Mq.RabbitMq.Amqp

import Std.Core
import Std.Collections
import Std.Encoding
import Std.String as Str

// ─── Constants ───────────────────────────────────────────────────────────────
/// Frame types.
pub val FRAME_METHOD: Int = 1

pub val FRAME_HEADER: Int = 2

pub val FRAME_BODY: Int = 3

pub val FRAME_HEARTBEAT: Int = 8

/// The octet that ends every frame.
pub val FRAME_END: Int = 206

/// The smallest frame-max a peer may negotiate (the specification's
/// `frame-min-size`), and the limit in force before `connection.tune-ok`.
pub val FRAME_MIN_SIZE: Int = 4096

/// Bytes of a frame around its payload: the 7-byte header and the end octet.
pub val FRAME_OVERHEAD: Int = 8

/// How deeply field tables and arrays may nest before decoding refuses them.
pub val MAX_TABLE_DEPTH: Int = 16

/// Class ids.
pub val CLASS_CONNECTION: Int = 10

pub val CLASS_CHANNEL: Int = 20

pub val CLASS_EXCHANGE: Int = 40

pub val CLASS_QUEUE: Int = 50

pub val CLASS_BASIC: Int = 60

pub val CLASS_CONFIRM: Int = 85

/// The 8 bytes a client sends first: `AMQP` 0 0 9 1.
pub func protocolHeader(): slice[Byte] {
  val acc: List[Byte] = newList()
  acc.add(65u8)
  acc.add(77u8)
  acc.add(81u8)
  acc.add(80u8)
  acc.add(0u8)
  acc.add(0u8)
  acc.add(9u8)
  acc.add(1u8)
  acc.toArray()
}

/// Whether `bytes` starts with the 0-9-1 protocol header.
pub func isProtocolHeader(bytes: in slice[Byte]): Bool {
  val expected = protocolHeader()
  if bytes.length < expected.length {
    return false
  }
  var i = 0
  while i < expected.length {
    if bytes[i] != expected[i] {
      return false
    }
    i += 1
  }
  true
}

// ─── Field tables ────────────────────────────────────────────────────────────
/// One value of a field table. The integer cases keep their wire width in
/// `tag` (the ASCII code of `b B s u I i l`), so a table decodes and encodes
/// to the same bytes.
pub union FieldValue {
  case FBool(value: Bool)
  case FInteger(tag: Int, value: Long)
  // IEEE 754 single precision, as its 32 bits.
  case FFloat(bits: Int)
  // IEEE 754 double precision, as its 64 bits.
  case FDouble(bits: Long)
  // `unscaled / 10^scale`.
  case FDecimal(scale: Int, unscaled: Int)
  // A long string: bytes, UTF-8 by convention.
  case FString(bytes: slice[Byte])
  // A byte array (`x`).
  case FBytes(bytes: slice[Byte])
  case FArray(items: slice[FieldValue])
  // Seconds since the Unix epoch.
  case FTimestamp(seconds: Long)
  case FTable(entries: slice[TableEntry])
  case FVoid
}

/// One named entry of a field table.
pub record TableEntry {
  name: String
  value: FieldValue
}

/// An empty field table.
pub func emptyTable(): slice[TableEntry] {
  val none: slice[TableEntry] = []
  none
}

/// A long-string value holding `text` as UTF-8.
pub func fText(text: in String): FieldValue {
  FString(bytes = encodeUtf8(text))
}

/// A signed 32-bit integer value (`I`).
pub func fInt(value: in Int): FieldValue {
  FInteger(tag = 73, value = value.toLong())
}

/// A signed 64-bit integer value (`l`).
pub func fLong(value: in Long): FieldValue {
  FInteger(tag = 108, value = value)
}

/// A table entry.
pub func entry(name: in String, value: in FieldValue): TableEntry {
  TableEntry(name = name, value = value)
}

/// The first entry called `name`.
pub func tableGet(table: in slice[TableEntry], name: in String): Option[FieldValue] {
  for e in table {
    if e.name == name {
      return Some(value = e.value)
    }
  }
  None
}

// ─── Byte writer ─────────────────────────────────────────────────────────────
// Accumulates bytes; the first value that cannot be represented is kept in
// `problem` and reported when the caller finishes.
ref record Writer {
  bytes: List[Byte]
  var problem: String
  var bitCount: Int
  var bitValue: Int
}

func newWriter(): Writer {
  Writer(bytes = newList(), problem = "", bitCount = 0, bitValue = 0)
}

func fail(w: in Writer, why: in String): Unit {
  if w.problem.length == 0 {
    w.problem = why
  }
}

func flushBits(w: in Writer): Unit {
  if w.bitCount > 0 {
    w.bytes.add(w.bitValue.toByte())
    w.bitCount = 0
    w.bitValue = 0
  }
}

func putOctet(w: in Writer, v: in Int): Unit {
  flushBits(w)
  w.bytes.add(v.and(255).toByte())
}

func putShort(w: in Writer, v: in Int): Unit {
  flushBits(w)
  w.bytes.add(v.shr(8).and(255).toByte())
  w.bytes.add(v.and(255).toByte())
}

func putLong(w: in Writer, v: in Long): Unit {
  flushBits(w)
  w.bytes.add(v.shr(24).and(255i64).toInt().toByte())
  w.bytes.add(v.shr(16).and(255i64).toInt().toByte())
  w.bytes.add(v.shr(8).and(255i64).toInt().toByte())
  w.bytes.add(v.and(255i64).toInt().toByte())
}

func putLongLong(w: in Writer, v: in Long): Unit {
  flushBits(w)
  var shift = 56
  while shift >= 0 {
    w.bytes.add(v.shr(shift).and(255i64).toInt().toByte())
    shift -= 8
  }
}

func putBytes(w: in Writer, bytes: in slice[Byte]): Unit {
  flushBits(w)
  for b in bytes {
    w.bytes.add(b)
  }
}

func putShortStr(w: in Writer, text: in String): Unit {
  val bytes = encodeUtf8(text)
  if bytes.length > 255 {
    fail(w, "a short string of " + bytes.length.toString() + " bytes exceeds the AMQP limit of 255")
    putOctet(w, 0)
    return
  }
  putOctet(w, bytes.length)
  putBytes(w, bytes)
}

func putLongStr(w: in Writer, bytes: in slice[Byte]): Unit {
  putLong(w, bytes.length.toLong())
  putBytes(w, bytes)
}

// Consecutive bits share octets, least significant bit first.
func putBit(w: in Writer, v: in Bool): Unit {
  if w.bitCount == 8 {
    flushBits(w)
  }
  if v {
    w.bitValue = w.bitValue.or(1.shl(w.bitCount))
  }
  w.bitCount += 1
}

func finish(w: in Writer): Result[slice[Byte], String] {
  flushBits(w)
  if w.problem.length > 0 {
    return Err(w.problem)
  }
  Ok(w.bytes.toArray())
}

func integerWidthProblem(tag: in Int, v: in Long): String {
  val ok = if tag == 98 {
    v >= -128i64 and v <= 127i64
  } else if tag == 66 {
    v >= 0i64 and v <= 255i64
  } else if tag == 115 {
    v >= -32768i64 and v <= 32767i64
  } else if tag == 117 {
    v >= 0i64 and v <= 65535i64
  } else if tag == 73 {
    v >= -2147483648i64 and v <= 2147483647i64
  } else if tag == 105 {
    v >= 0i64 and v <= 4294967295i64
  } else if tag == 108 {
    true
  } else {
    return "field table integer has an unknown type tag " + tag.toString()
  }
  if ok {
    ""
  } else {
    "field table integer " + v.toString() + " does not fit its type"
  }
}

func putFieldValue(w: in Writer, value: in FieldValue, depth: in Int): Unit {
  if depth > MAX_TABLE_DEPTH {
    fail(w, "a field table nests deeper than " + MAX_TABLE_DEPTH.toString() + " levels")
    return
  }
  match value {
    case FBool(b) -> {
      putOctet(w, 116)
      putOctet(w, if b { 1 } else { 0 })
    }
    case FInteger(tag, v) -> {
      val problem = integerWidthProblem(tag, v)
      if problem.length > 0 {
        fail(w, problem)
        return
      }
      putOctet(w, tag)
      if tag == 98 or tag == 66 {
        putOctet(w, v.and(255i64).toInt())
      } else if tag == 115 or tag == 117 {
        putShort(w, v.and(65535i64).toInt())
      } else if tag == 73 or tag == 105 {
        putLong(w, v)
      } else {
        putLongLong(w, v)
      }
    }
    case FFloat(bits) -> {
      putOctet(w, 102)
      putLong(w, bits.toLong())
    }
    case FDouble(bits) -> {
      putOctet(w, 100)
      putLongLong(w, bits)
    }
    case FDecimal(scale, unscaled) -> {
      if scale < 0 or scale > 255 {
        fail(w, "a decimal scale of " + scale.toString() + " is out of range")
        return
      }
      putOctet(w, 68)
      putOctet(w, scale)
      putLong(w, unscaled.toLong())
    }
    case FString(bytes) -> {
      putOctet(w, 83)
      putLongStr(w, bytes)
    }
    case FBytes(bytes) -> {
      putOctet(w, 120)
      putLongStr(w, bytes)
    }
    case FArray(items) -> {
      putOctet(w, 65)
      val inner = newWriter()
      for item in items {
        putFieldValue(inner, item, depth + 1)
      }
      flushBits(inner)
      if inner.problem.length > 0 {
        fail(w, inner.problem)
      }
      putLongStr(w, inner.bytes.toArray())
    }
    case FTimestamp(seconds) -> {
      putOctet(w, 84)
      putLongLong(w, seconds)
    }
    case FTable(entries) -> {
      putOctet(w, 70)
      putTableAt(w, entries, depth + 1)
    }
    case FVoid -> putOctet(w, 86)
  }
}

func putTableAt(w: in Writer, table: in slice[TableEntry], depth: in Int): Unit {
  val inner = newWriter()
  for e in table {
    if e.name.length == 0 {
      fail(w, "a field table entry has an empty name")
    }
    putShortStr(inner, e.name)
    putFieldValue(inner, e.value, depth)
  }
  flushBits(inner)
  if inner.problem.length > 0 {
    fail(w, inner.problem)
  }
  putLongStr(w, inner.bytes.toArray())
}

func putTable(w: in Writer, table: in slice[TableEntry]): Unit {
  putTableAt(w, table, 1)
}

/// The wire bytes of `table`, length prefix included.
pub func encodeTable(table: in slice[TableEntry]): Result[slice[Byte], String] {
  val w = newWriter()
  putTable(w, table)
  finish(w)
}

// ─── Byte reader ─────────────────────────────────────────────────────────────
// Reads past the end, or of malformed data, set `problem` and yield zero
// values; the caller checks once at the end.
ref record Reader {
  buf: slice[Byte]
  var pos: Int
  limit: Int
  var problem: String
  var bitsLeft: Int
  var bitValue: Int
}

func newReader(buf: in slice[Byte]): Reader {
  Reader(buf = buf, pos = 0, limit = buf.length, problem = "", bitsLeft = 0, bitValue = 0)
}

func subReader(buf: in slice[Byte], from: in Int, to: in Int): Reader {
  Reader(buf = buf, pos = from, limit = to, problem = "", bitsLeft = 0, bitValue = 0)
}

func bad(r: in Reader, why: in String): Unit {
  if r.problem.length == 0 {
    r.problem = why
  }
}

func has(r: in Reader, count: in Int): Bool {
  r.bitsLeft = 0
  if r.problem.length > 0 {
    return false
  }
  if count < 0 or r.limit - r.pos < count {
    bad(r, "truncated")
    return false
  }
  true
}

func getOctet(r: in Reader): Int {
  if not has(r, 1) {
    return 0
  }
  val v = r.buf[r.pos].toInt().and(255)
  r.pos += 1
  v
}

func getShort(r: in Reader): Int {
  if not has(r, 2) {
    return 0
  }
  val v = r.buf[r.pos].toInt().and(255).shl(8).or(r.buf[r.pos + 1].toInt().and(255))
  r.pos += 2
  v
}

// An unsigned 32-bit value.
func getLong(r: in Reader): Long {
  if not has(r, 4) {
    return 0i64
  }
  var v = 0i64
  var i = 0
  while i < 4 {
    v = v.shl(8).or(r.buf[r.pos + i].toInt().and(255).toLong())
    i += 1
  }
  r.pos += 4
  v
}

func getLongLong(r: in Reader): Long {
  if not has(r, 8) {
    return 0i64
  }
  var v = 0i64
  var i = 0
  while i < 8 {
    v = v.shl(8).or(r.buf[r.pos + i].toInt().and(255).toLong())
    i += 1
  }
  r.pos += 8
  v
}

func getBytes(r: in Reader, count: in Int): slice[Byte] {
  if not has(r, count) {
    val none: slice[Byte] = []
    return none
  }
  val taken = r.buf.slice(r.pos, r.pos + count)
  r.pos += count
  taken
}

// A 32-bit length that must fit what is left to read.
func getLength(r: in Reader): Int {
  val n = getLong(r)
  if n > (r.limit - r.pos).toLong() {
    bad(r, "truncated")
    return 0
  }
  n.toInt()
}

func getShortStr(r: in Reader): String {
  val n = getOctet(r)
  val bytes = getBytes(r, n)
  match tryDecodeUtf8(bytes) {
    case Some(s) -> s
    case None -> {
      bad(r, "a short string is not valid UTF-8")
      ""
    }
  }
}

func getLongStr(r: in Reader): slice[Byte] {
  val n = getLength(r)
  getBytes(r, n)
}

func getBit(r: in Reader): Bool {
  if r.bitsLeft == 0 {
    r.bitValue = getOctet(r)
    r.bitsLeft = 8
  }
  val v = r.bitValue.and(1) == 1
  r.bitValue = r.bitValue.shr(1)
  r.bitsLeft -= 1
  v
}

func signExtend(v: in Long, bits: in Int): Long {
  val sign = 1i64.shl(bits - 1)
  if v.and(sign) != 0i64 {
    v - 1i64.shl(bits)
  } else {
    v
  }
}

func getFieldValue(r: in Reader, depth: in Int): FieldValue {
  if depth > MAX_TABLE_DEPTH {
    bad(r, "a field table nests deeper than " + MAX_TABLE_DEPTH.toString() + " levels")
    return FVoid
  }
  val tag = getOctet(r)
  if r.problem.length > 0 {
    return FVoid
  }
  if tag == 116 {
    FBool(value = getOctet(r) != 0)
  } else if tag == 98 {
    FInteger(tag = tag, value = signExtend(getOctet(r).toLong(), 8))
  } else if tag == 66 {
    FInteger(tag = tag, value = getOctet(r).toLong())
  } else if tag == 115 {
    FInteger(tag = tag, value = signExtend(getShort(r).toLong(), 16))
  } else if tag == 117 {
    FInteger(tag = tag, value = getShort(r).toLong())
  } else if tag == 73 {
    FInteger(tag = tag, value = signExtend(getLong(r), 32))
  } else if tag == 105 {
    FInteger(tag = tag, value = getLong(r))
  } else if tag == 108 {
    FInteger(tag = tag, value = getLongLong(r))
  } else if tag == 102 {
    FFloat(bits = signExtend(getLong(r), 32).toInt())
  } else if tag == 100 {
    FDouble(bits = getLongLong(r))
  } else if tag == 68 {
    val scale = getOctet(r)
    FDecimal(scale = scale, unscaled = signExtend(getLong(r), 32).toInt())
  } else if tag == 83 {
    FString(bytes = getLongStr(r))
  } else if tag == 120 {
    FBytes(bytes = getLongStr(r))
  } else if tag == 65 {
    val n = getLength(r)
    val inner = subReader(r.buf, r.pos, r.pos + n)
    val items: List[FieldValue] = newList()
    while inner.problem.length == 0 and inner.pos < inner.limit {
      items.add(getFieldValue(inner, depth + 1))
    }
    if inner.problem.length > 0 {
      bad(r, inner.problem)
    }
    r.pos += n
    FArray(items = items.toArray())
  } else if tag == 84 {
    FTimestamp(seconds = getLongLong(r))
  } else if tag == 70 {
    FTable(entries = getTableAt(r, depth + 1))
  } else if tag == 86 {
    FVoid
  } else {
    bad(r, "a field table uses the unknown type tag " + tag.toString())
    FVoid
  }
}

func getTableAt(r: in Reader, depth: in Int): slice[TableEntry] {
  val n = getLength(r)
  val inner = subReader(r.buf, r.pos, r.pos + n)
  val entries: List[TableEntry] = newList()
  while inner.problem.length == 0 and inner.pos < inner.limit {
    val name = getShortStr(inner)
    val value = getFieldValue(inner, depth)
    entries.add(TableEntry(name = name, value = value))
  }
  if inner.problem.length > 0 {
    bad(r, inner.problem)
  }
  r.pos += n
  entries.toArray()
}

func getTable(r: in Reader): slice[TableEntry] {
  getTableAt(r, 1)
}

/// Decode a field table (length prefix included) that fills `bytes` exactly.
pub func decodeTable(bytes: in slice[Byte]): Result[slice[TableEntry], String] {
  val r = newReader(bytes)
  val table = getTable(r)
  if r.problem.length > 0 {
    return Err("malformed field table: " + r.problem)
  }
  if r.pos != r.limit {
    return Err("malformed field table: trailing bytes")
  }
  Ok(table)
}

// ─── Frames ──────────────────────────────────────────────────────────────────
/// One frame: its type, channel and payload.
pub record Frame {
  kind: Int
  channel: Int
  payload: slice[Byte]
}

/// The wire bytes of a frame.
pub func encodeFrame(kind: in Int, channel: in Int, payload: in slice[Byte]): slice[Byte] {
  val w = newWriter()
  putOctet(w, kind)
  putShort(w, channel)
  putLong(w, payload.length.toLong())
  val head = w.bytes.toArray()
  val tail: slice[Byte] = [206u8]
  head.concat(payload).concat(tail)
}

/// A heartbeat frame (always on channel 0).
pub func heartbeatFrame(): slice[Byte] {
  val none: slice[Byte] = []
  encodeFrame(FRAME_HEARTBEAT, 0, none)
}

/// An incremental frame decoder. `feed` it bytes as they arrive and call
/// `nextFrame` until it reports `None`.
pub ref record Decoder {
  var buffer: slice[Byte]
  var chunks: List[slice[Byte]]
  var chunkBytes: Int
  // The buffered byte count below which no complete frame can be present.
  var need: Int
  // The largest frame accepted, overhead included; 0 = no limit.
  var frameMax: Int
}

/// A decoder that refuses frames larger than `frameMax` bytes (0 = no limit).
pub func newDecoder(frameMax: in Int): Decoder
  requires: frameMax >= 0
{
  val empty: slice[Byte] = []
  Decoder(buffer = empty, chunks = newList(), chunkBytes = 0, need = 0, frameMax = frameMax)
}

/// Change the frame size limit (after `connection.tune`).
pub func setFrameMax(d: in Decoder, frameMax: in Int): Unit
  requires: frameMax >= 0
{
  d.frameMax = frameMax
}

/// Hand the decoder the next bytes from the wire.
pub func feed(d: in Decoder, bytes: in slice[Byte]): Unit {
  if bytes.length > 0 {
    d.chunks.add(bytes)
    d.chunkBytes = d.chunkBytes + bytes.length
  }
}

/// The number of bytes fed but not yet returned as part of a frame.
pub func buffered(d: in Decoder): Int {
  d.buffer.length + d.chunkBytes
}

// Pairwise concatenation: every byte is copied O(log n) times however many
// chunks a large frame arrived in.
func joinRange(chunks: in List[slice[Byte]], from: in Int, to: in Int): slice[Byte] {
  if to - from == 1 {
    return chunks[from]
  }
  val mid = from + (to - from) / 2
  joinRange(chunks, from, mid).concat(joinRange(chunks, mid, to))
}

func absorbChunks(d: in Decoder): Unit {
  if d.chunks.count > 0 {
    val joined = joinRange(d.chunks, 0, d.chunks.count)
    d.buffer = if d.buffer.length == 0 { joined } else { d.buffer.concat(joined) }
    d.chunks = newList()
    d.chunkBytes = 0
  }
}

/// The next complete frame, `Ok(None)` when more bytes are needed. An `Err`
/// is a framing violation: the stream cannot be resynchronised after it.
pub func nextFrame(d: in Decoder): Result[Option[Frame], String] {
  if d.buffer.length + d.chunkBytes < 7 or d.buffer.length + d.chunkBytes < d.need {
    return Ok(None)
  }
  absorbChunks(d)
  val buf = d.buffer
  val r = newReader(buf)
  val kind = getOctet(r)
  val channel = getShort(r)
  val size = getLong(r)
  if kind != FRAME_METHOD and kind != FRAME_HEADER and kind != FRAME_BODY and kind != FRAME_HEARTBEAT {
    // "AMQP" where a frame should start: the peer answered the protocol
    // header with its own, i.e. it does not speak 0-9-1.
    if isProtocolHeaderPrefix(buf) {
      return Err("the peer answered with a protocol header: it does not accept AMQP 0-9-1")
    }
    return Err("unknown frame type " + kind.toString())
  }
  if d.frameMax > 0 and size > (d.frameMax - FRAME_OVERHEAD).toLong() {
    return Err(
      "a frame of " + size.toString() + " payload bytes exceeds the negotiated frame-max of " +
        d.frameMax.toString()
    )
  }
  if size > 2147483000i64 {
    return Err("a frame of " + size.toString() + " payload bytes is too large")
  }
  val total = 7 + size.toInt() + 1
  if buf.length < total {
    d.need = total
    return Ok(None)
  }
  if buf[total - 1].toInt().and(255) != FRAME_END {
    return Err("a frame does not end with the frame-end octet")
  }
  val frame = Frame(kind = kind, channel = channel, payload = buf.slice(7, total - 1))
  d.buffer = buf.slice(total, buf.length)
  d.need = 0
  Ok(Some(value = frame))
}

func isProtocolHeaderPrefix(buf: in slice[Byte]): Bool {
  buf.length >= 4 and buf[0] == 65u8 and buf[1] == 77u8 and buf[2] == 81u8 and buf[3] == 80u8
}

// ─── Methods ─────────────────────────────────────────────────────────────────
/// The methods this client and its test broker exchange.
pub union Method {
  case ConnectionStart(versionMajor: Int, versionMinor: Int, serverProperties: slice[TableEntry], mechanisms: String, locales: String)
  case ConnectionStartOk(clientProperties: slice[TableEntry], mechanism: String, response: slice[Byte], locale: String)
  case ConnectionSecure(challenge: slice[Byte])
  case ConnectionTune(channelMax: Int, frameMax: Long, heartbeat: Int)
  case ConnectionTuneOk(channelMax: Int, frameMax: Long, heartbeat: Int)
  case ConnectionOpen(virtualHost: String)
  case ConnectionOpenOk
  case ConnectionClose(replyCode: Int, replyText: String, classId: Int, methodId: Int)
  case ConnectionCloseOk
  case ConnectionBlocked(reason: String)
  case ConnectionUnblocked
  case ChannelOpen
  case ChannelOpenOk
  case ChannelFlow(active: Bool)
  case ChannelFlowOk(active: Bool)
  case ChannelClose(replyCode: Int, replyText: String, classId: Int, methodId: Int)
  case ChannelCloseOk
  case ExchangeDeclare(exchange: String, kind: String, passive: Bool, durable: Bool, autoDelete: Bool, isInternal: Bool, arguments: slice[TableEntry])
  case ExchangeDeclareOk
  case ExchangeDelete(exchange: String, ifUnused: Bool)
  case ExchangeDeleteOk
  case QueueDeclare(queue: String, passive: Bool, durable: Bool, exclusive: Bool, autoDelete: Bool, arguments: slice[TableEntry])
  case QueueDeclareOk(queue: String, messageCount: Long, consumerCount: Long)
  case QueueBind(queue: String, exchange: String, routingKey: String, arguments: slice[TableEntry])
  case QueueBindOk
  case QueuePurge(queue: String)
  case QueuePurgeOk(messageCount: Long)
  case QueueDelete(queue: String, ifUnused: Bool, ifEmpty: Bool)
  case QueueDeleteOk(messageCount: Long)
  case BasicQos(prefetchSize: Long, prefetchCount: Int, global: Bool)
  case BasicQosOk
  case BasicConsume(queue: String, consumerTag: String, noLocal: Bool, noAck: Bool, exclusive: Bool, arguments: slice[TableEntry])
  case BasicConsumeOk(consumerTag: String)
  case BasicCancel(consumerTag: String, noWait: Bool)
  case BasicCancelOk(consumerTag: String)
  case BasicPublish(exchange: String, routingKey: String, mandatory: Bool, immediate: Bool)
  case BasicReturn(replyCode: Int, replyText: String, exchange: String, routingKey: String)
  case BasicDeliver(consumerTag: String, deliveryTag: Long, redelivered: Bool, exchange: String, routingKey: String)
  case BasicAck(deliveryTag: Long, multiple: Bool)
  case BasicReject(deliveryTag: Long, requeue: Bool)
  case BasicNack(deliveryTag: Long, multiple: Bool, requeue: Bool)
  case ConfirmSelect
  case ConfirmSelectOk
  case Unknown(classId: Int, methodId: Int)
}

func putMethodId(w: in Writer, classId: in Int, methodId: in Int): Unit {
  putShort(w, classId)
  putShort(w, methodId)
}

func putCloseArgs(w: in Writer, replyCode: in Int, replyText: in String, classId: in Int, methodId: in Int): Unit {
  putShort(w, replyCode)
  putShortStr(w, clip(replyText, 255))
  putShort(w, classId)
  putShort(w, methodId)
}

// `text` cut to at most `maxBytes` UTF-8 bytes, on a character boundary.
func clip(text: in String, maxBytes: in Int): String {
  if encodeUtf8(text).length <= maxBytes {
    return text
  }
  var stop = if text.length < maxBytes { text.length } else { maxBytes }
  while stop > 0 {
    // Do not cut between the halves of a surrogate pair.
    val unit = text.codeUnitAt(stop - 1)
    val splitsPair = unit >= 55296 and unit <= 56319
    if not splitsPair and encodeUtf8(Str.substring(text, 0, stop)).length <= maxBytes {
      return Str.substring(text, 0, stop)
    }
    stop -= 1
  }
  ""
}

/// The payload of a method frame for `m`. An `Err` names the argument that
/// cannot be represented (a short string over 255 bytes, a bad field table).
/// `Unknown` cannot be encoded.
pub func encodeMethod(m: in Method): Result[slice[Byte], String] {
  val w = newWriter()
  match m {
    case ConnectionStart(major, minor, properties, mechanisms, locales) -> {
      putMethodId(w, 10, 10)
      putOctet(w, major)
      putOctet(w, minor)
      putTable(w, properties)
      putLongStr(w, encodeUtf8(mechanisms))
      putLongStr(w, encodeUtf8(locales))
    }
    case ConnectionStartOk(properties, mechanism, response, locale) -> {
      putMethodId(w, 10, 11)
      putTable(w, properties)
      putShortStr(w, mechanism)
      putLongStr(w, response)
      putShortStr(w, locale)
    }
    case ConnectionSecure(challenge) -> {
      putMethodId(w, 10, 20)
      putLongStr(w, challenge)
    }
    case ConnectionTune(channelMax, frameMax, heartbeat) -> {
      putMethodId(w, 10, 30)
      putShort(w, channelMax)
      putLong(w, frameMax)
      putShort(w, heartbeat)
    }
    case ConnectionTuneOk(channelMax, frameMax, heartbeat) -> {
      putMethodId(w, 10, 31)
      putShort(w, channelMax)
      putLong(w, frameMax)
      putShort(w, heartbeat)
    }
    case ConnectionOpen(virtualHost) -> {
      putMethodId(w, 10, 40)
      putShortStr(w, virtualHost)
      putShortStr(w, "")
      putBit(w, false)
    }
    case ConnectionOpenOk -> {
      putMethodId(w, 10, 41)
      putShortStr(w, "")
    }
    case ConnectionClose(replyCode, replyText, classId, methodId) -> {
      putMethodId(w, 10, 50)
      putCloseArgs(w, replyCode, replyText, classId, methodId)
    }
    case ConnectionCloseOk -> putMethodId(w, 10, 51)
    case ConnectionBlocked(reason) -> {
      putMethodId(w, 10, 60)
      putShortStr(w, clip(reason, 255))
    }
    case ConnectionUnblocked -> putMethodId(w, 10, 61)
    case ChannelOpen -> {
      putMethodId(w, 20, 10)
      putShortStr(w, "")
    }
    case ChannelOpenOk -> {
      putMethodId(w, 20, 11)
      putLong(w, 0i64)
    }
    case ChannelFlow(active) -> {
      putMethodId(w, 20, 20)
      putBit(w, active)
    }
    case ChannelFlowOk(active) -> {
      putMethodId(w, 20, 21)
      putBit(w, active)
    }
    case ChannelClose(replyCode, replyText, classId, methodId) -> {
      putMethodId(w, 20, 40)
      putCloseArgs(w, replyCode, replyText, classId, methodId)
    }
    case ChannelCloseOk -> putMethodId(w, 20, 41)
    case ExchangeDeclare(exchange, kind, passive, durable, autoDelete, isInternal, arguments) -> {
      putMethodId(w, 40, 10)
      putShort(w, 0)
      putShortStr(w, exchange)
      putShortStr(w, kind)
      putBit(w, passive)
      putBit(w, durable)
      putBit(w, autoDelete)
      putBit(w, isInternal)
      putBit(w, false)
      putTable(w, arguments)
    }
    case ExchangeDeclareOk -> putMethodId(w, 40, 11)
    case ExchangeDelete(exchange, ifUnused) -> {
      putMethodId(w, 40, 20)
      putShort(w, 0)
      putShortStr(w, exchange)
      putBit(w, ifUnused)
      putBit(w, false)
    }
    case ExchangeDeleteOk -> putMethodId(w, 40, 21)
    case QueueDeclare(queue, passive, durable, exclusive, autoDelete, arguments) -> {
      putMethodId(w, 50, 10)
      putShort(w, 0)
      putShortStr(w, queue)
      putBit(w, passive)
      putBit(w, durable)
      putBit(w, exclusive)
      putBit(w, autoDelete)
      putBit(w, false)
      putTable(w, arguments)
    }
    case QueueDeclareOk(queue, messageCount, consumerCount) -> {
      putMethodId(w, 50, 11)
      putShortStr(w, queue)
      putLong(w, messageCount)
      putLong(w, consumerCount)
    }
    case QueueBind(queue, exchange, routingKey, arguments) -> {
      putMethodId(w, 50, 20)
      putShort(w, 0)
      putShortStr(w, queue)
      putShortStr(w, exchange)
      putShortStr(w, routingKey)
      putBit(w, false)
      putTable(w, arguments)
    }
    case QueueBindOk -> putMethodId(w, 50, 21)
    case QueuePurge(queue) -> {
      putMethodId(w, 50, 30)
      putShort(w, 0)
      putShortStr(w, queue)
      putBit(w, false)
    }
    case QueuePurgeOk(messageCount) -> {
      putMethodId(w, 50, 31)
      putLong(w, messageCount)
    }
    case QueueDelete(queue, ifUnused, ifEmpty) -> {
      putMethodId(w, 50, 40)
      putShort(w, 0)
      putShortStr(w, queue)
      putBit(w, ifUnused)
      putBit(w, ifEmpty)
      putBit(w, false)
    }
    case QueueDeleteOk(messageCount) -> {
      putMethodId(w, 50, 41)
      putLong(w, messageCount)
    }
    case BasicQos(prefetchSize, prefetchCount, global) -> {
      putMethodId(w, 60, 10)
      putLong(w, prefetchSize)
      putShort(w, prefetchCount)
      putBit(w, global)
    }
    case BasicQosOk -> putMethodId(w, 60, 11)
    case BasicConsume(queue, consumerTag, noLocal, noAck, exclusive, arguments) -> {
      putMethodId(w, 60, 20)
      putShort(w, 0)
      putShortStr(w, queue)
      putShortStr(w, consumerTag)
      putBit(w, noLocal)
      putBit(w, noAck)
      putBit(w, exclusive)
      putBit(w, false)
      putTable(w, arguments)
    }
    case BasicConsumeOk(consumerTag) -> {
      putMethodId(w, 60, 21)
      putShortStr(w, consumerTag)
    }
    case BasicCancel(consumerTag, noWait) -> {
      putMethodId(w, 60, 30)
      putShortStr(w, consumerTag)
      putBit(w, noWait)
    }
    case BasicCancelOk(consumerTag) -> {
      putMethodId(w, 60, 31)
      putShortStr(w, consumerTag)
    }
    case BasicPublish(exchange, routingKey, mandatory, immediate) -> {
      putMethodId(w, 60, 40)
      putShort(w, 0)
      putShortStr(w, exchange)
      putShortStr(w, routingKey)
      putBit(w, mandatory)
      putBit(w, immediate)
    }
    case BasicReturn(replyCode, replyText, exchange, routingKey) -> {
      putMethodId(w, 60, 50)
      putShort(w, replyCode)
      putShortStr(w, clip(replyText, 255))
      putShortStr(w, exchange)
      putShortStr(w, routingKey)
    }
    case BasicDeliver(consumerTag, deliveryTag, redelivered, exchange, routingKey) -> {
      putMethodId(w, 60, 60)
      putShortStr(w, consumerTag)
      putLongLong(w, deliveryTag)
      putBit(w, redelivered)
      putShortStr(w, exchange)
      putShortStr(w, routingKey)
    }
    case BasicAck(deliveryTag, multiple) -> {
      putMethodId(w, 60, 80)
      putLongLong(w, deliveryTag)
      putBit(w, multiple)
    }
    case BasicReject(deliveryTag, requeue) -> {
      putMethodId(w, 60, 90)
      putLongLong(w, deliveryTag)
      putBit(w, requeue)
    }
    case BasicNack(deliveryTag, multiple, requeue) -> {
      putMethodId(w, 60, 120)
      putLongLong(w, deliveryTag)
      putBit(w, multiple)
      putBit(w, requeue)
    }
    case ConfirmSelect -> {
      putMethodId(w, 85, 10)
      putBit(w, false)
    }
    case ConfirmSelectOk -> putMethodId(w, 85, 11)
    case Unknown(classId, methodId) -> {
      fail(w, "method " + classId.toString() + "." + methodId.toString() + " cannot be encoded")
    }
  }
  finish(w)
}

func textOf(r: in Reader, bytes: in slice[Byte]): String {
  match tryDecodeUtf8(bytes) {
    case Some(s) -> s
    case None -> {
      bad(r, "a string argument is not valid UTF-8")
      ""
    }
  }
}

func decodeConnection(r: in Reader, methodId: in Int): Method {
  if methodId == 10 {
    val major = getOctet(r)
    val minor = getOctet(r)
    val properties = getTable(r)
    val mechanisms = textOf(r, getLongStr(r))
    val locales = textOf(r, getLongStr(r))
    ConnectionStart(
      versionMajor = major,
      versionMinor = minor,
      serverProperties = properties,
      mechanisms = mechanisms,
      locales = locales
    )
  } else if methodId == 11 {
    val properties = getTable(r)
    val mechanism = getShortStr(r)
    val response = getLongStr(r)
    val locale = getShortStr(r)
    ConnectionStartOk(clientProperties = properties, mechanism = mechanism, response = response, locale = locale)
  } else if methodId == 20 {
    ConnectionSecure(challenge = getLongStr(r))
  } else if methodId == 30 {
    val channelMax = getShort(r)
    val frameMax = getLong(r)
    val heartbeat = getShort(r)
    ConnectionTune(channelMax = channelMax, frameMax = frameMax, heartbeat = heartbeat)
  } else if methodId == 31 {
    val channelMax = getShort(r)
    val frameMax = getLong(r)
    val heartbeat = getShort(r)
    ConnectionTuneOk(channelMax = channelMax, frameMax = frameMax, heartbeat = heartbeat)
  } else if methodId == 40 {
    val vhost = getShortStr(r)
    val _ = getShortStr(r)
    val _ = getBit(r)
    ConnectionOpen(virtualHost = vhost)
  } else if methodId == 41 {
    val _ = getShortStr(r)
    ConnectionOpenOk
  } else if methodId == 50 {
    val code = getShort(r)
    val text = getShortStr(r)
    val classId = getShort(r)
    val failing = getShort(r)
    ConnectionClose(replyCode = code, replyText = text, classId = classId, methodId = failing)
  } else if methodId == 51 {
    ConnectionCloseOk
  } else if methodId == 60 {
    ConnectionBlocked(reason = getShortStr(r))
  } else if methodId == 61 {
    ConnectionUnblocked
  } else {
    Unknown(classId = 10, methodId = methodId)
  }
}

func decodeChannel(r: in Reader, methodId: in Int): Method {
  if methodId == 10 {
    val _ = getShortStr(r)
    ChannelOpen
  } else if methodId == 11 {
    val _ = getLongStr(r)
    ChannelOpenOk
  } else if methodId == 20 {
    ChannelFlow(active = getBit(r))
  } else if methodId == 21 {
    ChannelFlowOk(active = getBit(r))
  } else if methodId == 40 {
    val code = getShort(r)
    val text = getShortStr(r)
    val classId = getShort(r)
    val failing = getShort(r)
    ChannelClose(replyCode = code, replyText = text, classId = classId, methodId = failing)
  } else if methodId == 41 {
    ChannelCloseOk
  } else {
    Unknown(classId = 20, methodId = methodId)
  }
}

func decodeExchange(r: in Reader, methodId: in Int): Method {
  if methodId == 10 {
    val _ = getShort(r)
    val exchange = getShortStr(r)
    val kind = getShortStr(r)
    val passive = getBit(r)
    val durable = getBit(r)
    val autoDelete = getBit(r)
    val isInternal = getBit(r)
    val _ = getBit(r)
    val arguments = getTable(r)
    ExchangeDeclare(
      exchange = exchange,
      kind = kind,
      passive = passive,
      durable = durable,
      autoDelete = autoDelete,
      isInternal = isInternal,
      arguments = arguments
    )
  } else if methodId == 11 {
    ExchangeDeclareOk
  } else if methodId == 20 {
    val _ = getShort(r)
    val exchange = getShortStr(r)
    val ifUnused = getBit(r)
    val _ = getBit(r)
    ExchangeDelete(exchange = exchange, ifUnused = ifUnused)
  } else if methodId == 21 {
    ExchangeDeleteOk
  } else {
    Unknown(classId = 40, methodId = methodId)
  }
}

func decodeQueue(r: in Reader, methodId: in Int): Method {
  if methodId == 10 {
    val _ = getShort(r)
    val queue = getShortStr(r)
    val passive = getBit(r)
    val durable = getBit(r)
    val exclusive = getBit(r)
    val autoDelete = getBit(r)
    val _ = getBit(r)
    val arguments = getTable(r)
    QueueDeclare(
      queue = queue,
      passive = passive,
      durable = durable,
      exclusive = exclusive,
      autoDelete = autoDelete,
      arguments = arguments
    )
  } else if methodId == 11 {
    val queue = getShortStr(r)
    val messages = getLong(r)
    val consumers = getLong(r)
    QueueDeclareOk(queue = queue, messageCount = messages, consumerCount = consumers)
  } else if methodId == 20 {
    val _ = getShort(r)
    val queue = getShortStr(r)
    val exchange = getShortStr(r)
    val routingKey = getShortStr(r)
    val _ = getBit(r)
    val arguments = getTable(r)
    QueueBind(queue = queue, exchange = exchange, routingKey = routingKey, arguments = arguments)
  } else if methodId == 21 {
    QueueBindOk
  } else if methodId == 30 {
    val _ = getShort(r)
    val queue = getShortStr(r)
    val _ = getBit(r)
    QueuePurge(queue = queue)
  } else if methodId == 31 {
    QueuePurgeOk(messageCount = getLong(r))
  } else if methodId == 40 {
    val _ = getShort(r)
    val queue = getShortStr(r)
    val ifUnused = getBit(r)
    val ifEmpty = getBit(r)
    val _ = getBit(r)
    QueueDelete(queue = queue, ifUnused = ifUnused, ifEmpty = ifEmpty)
  } else if methodId == 41 {
    QueueDeleteOk(messageCount = getLong(r))
  } else {
    Unknown(classId = 50, methodId = methodId)
  }
}

func decodeBasic(r: in Reader, methodId: in Int): Method {
  if methodId == 10 {
    val size = getLong(r)
    val count = getShort(r)
    val global = getBit(r)
    BasicQos(prefetchSize = size, prefetchCount = count, global = global)
  } else if methodId == 11 {
    BasicQosOk
  } else if methodId == 20 {
    val _ = getShort(r)
    val queue = getShortStr(r)
    val tag = getShortStr(r)
    val noLocal = getBit(r)
    val noAck = getBit(r)
    val exclusive = getBit(r)
    val _ = getBit(r)
    val arguments = getTable(r)
    BasicConsume(
      queue = queue,
      consumerTag = tag,
      noLocal = noLocal,
      noAck = noAck,
      exclusive = exclusive,
      arguments = arguments
    )
  } else if methodId == 21 {
    BasicConsumeOk(consumerTag = getShortStr(r))
  } else if methodId == 30 {
    val tag = getShortStr(r)
    val noWait = getBit(r)
    BasicCancel(consumerTag = tag, noWait = noWait)
  } else if methodId == 31 {
    BasicCancelOk(consumerTag = getShortStr(r))
  } else if methodId == 40 {
    val _ = getShort(r)
    val exchange = getShortStr(r)
    val routingKey = getShortStr(r)
    val mandatory = getBit(r)
    val immediate = getBit(r)
    BasicPublish(exchange = exchange, routingKey = routingKey, mandatory = mandatory, immediate = immediate)
  } else if methodId == 50 {
    val code = getShort(r)
    val text = getShortStr(r)
    val exchange = getShortStr(r)
    val routingKey = getShortStr(r)
    BasicReturn(replyCode = code, replyText = text, exchange = exchange, routingKey = routingKey)
  } else if methodId == 60 {
    val consumerTag = getShortStr(r)
    val deliveryTag = getLongLong(r)
    val redelivered = getBit(r)
    val exchange = getShortStr(r)
    val routingKey = getShortStr(r)
    BasicDeliver(
      consumerTag = consumerTag,
      deliveryTag = deliveryTag,
      redelivered = redelivered,
      exchange = exchange,
      routingKey = routingKey
    )
  } else if methodId == 80 {
    val tag = getLongLong(r)
    val multiple = getBit(r)
    BasicAck(deliveryTag = tag, multiple = multiple)
  } else if methodId == 90 {
    val tag = getLongLong(r)
    val requeue = getBit(r)
    BasicReject(deliveryTag = tag, requeue = requeue)
  } else if methodId == 120 {
    val tag = getLongLong(r)
    val multiple = getBit(r)
    val requeue = getBit(r)
    BasicNack(deliveryTag = tag, multiple = multiple, requeue = requeue)
  } else {
    Unknown(classId = 60, methodId = methodId)
  }
}

/// Decode the payload of a method frame. A method outside this client's set
/// is `Unknown`; a payload that is cut short or malformed is an `Err`.
pub func decodeMethod(payload: in slice[Byte]): Result[Method, String] {
  val r = newReader(payload)
  val classId = getShort(r)
  val methodId = getShort(r)
  if r.problem.length > 0 {
    return Err("malformed method frame: no class and method id")
  }
  val m = if classId == 10 {
    decodeConnection(r, methodId)
  } else if classId == 20 {
    decodeChannel(r, methodId)
  } else if classId == 40 {
    decodeExchange(r, methodId)
  } else if classId == 50 {
    decodeQueue(r, methodId)
  } else if classId == 60 {
    decodeBasic(r, methodId)
  } else if classId == 85 and methodId == 10 {
    val _ = getBit(r)
    ConfirmSelect
  } else if classId == 85 and methodId == 11 {
    ConfirmSelectOk
  } else {
    Unknown(classId = classId, methodId = methodId)
  }
  if r.problem.length > 0 {
    return Err("malformed method " + classId.toString() + "." + methodId.toString() + ": " + r.problem)
  }
  Ok(m)
}

/// `class.method` in words, for error messages.
pub func methodName(m: in Method): String {
  match m {
    case ConnectionStart(_, _, _, _, _) -> "connection.start"
    case ConnectionStartOk(_, _, _, _) -> "connection.start-ok"
    case ConnectionSecure(_) -> "connection.secure"
    case ConnectionTune(_, _, _) -> "connection.tune"
    case ConnectionTuneOk(_, _, _) -> "connection.tune-ok"
    case ConnectionOpen(_) -> "connection.open"
    case ConnectionOpenOk -> "connection.open-ok"
    case ConnectionClose(_, _, _, _) -> "connection.close"
    case ConnectionCloseOk -> "connection.close-ok"
    case ConnectionBlocked(_) -> "connection.blocked"
    case ConnectionUnblocked -> "connection.unblocked"
    case ChannelOpen -> "channel.open"
    case ChannelOpenOk -> "channel.open-ok"
    case ChannelFlow(_) -> "channel.flow"
    case ChannelFlowOk(_) -> "channel.flow-ok"
    case ChannelClose(_, _, _, _) -> "channel.close"
    case ChannelCloseOk -> "channel.close-ok"
    case ExchangeDeclare(_, _, _, _, _, _, _) -> "exchange.declare"
    case ExchangeDeclareOk -> "exchange.declare-ok"
    case ExchangeDelete(_, _) -> "exchange.delete"
    case ExchangeDeleteOk -> "exchange.delete-ok"
    case QueueDeclare(_, _, _, _, _, _) -> "queue.declare"
    case QueueDeclareOk(_, _, _) -> "queue.declare-ok"
    case QueueBind(_, _, _, _) -> "queue.bind"
    case QueueBindOk -> "queue.bind-ok"
    case QueuePurge(_) -> "queue.purge"
    case QueuePurgeOk(_) -> "queue.purge-ok"
    case QueueDelete(_, _, _) -> "queue.delete"
    case QueueDeleteOk(_) -> "queue.delete-ok"
    case BasicQos(_, _, _) -> "basic.qos"
    case BasicQosOk -> "basic.qos-ok"
    case BasicConsume(_, _, _, _, _, _) -> "basic.consume"
    case BasicConsumeOk(_) -> "basic.consume-ok"
    case BasicCancel(_, _) -> "basic.cancel"
    case BasicCancelOk(_) -> "basic.cancel-ok"
    case BasicPublish(_, _, _, _) -> "basic.publish"
    case BasicReturn(_, _, _, _) -> "basic.return"
    case BasicDeliver(_, _, _, _, _) -> "basic.deliver"
    case BasicAck(_, _) -> "basic.ack"
    case BasicReject(_, _) -> "basic.reject"
    case BasicNack(_, _, _) -> "basic.nack"
    case ConfirmSelect -> "confirm.select"
    case ConfirmSelectOk -> "confirm.select-ok"
    case Unknown(classId, methodId) -> "method " + classId.toString() + "." + methodId.toString()
  }
}

/// Whether `m` is followed by a content header and body frames.
pub func carriesContent(m: in Method): Bool {
  match m {
    case BasicPublish(_, _, _, _) -> true
    case BasicReturn(_, _, _, _) -> true
    case BasicDeliver(_, _, _, _, _) -> true
    case _ -> false
  }
}

// ─── Content headers ─────────────────────────────────────────────────────────
/// The `basic` class properties of a message. An empty string, a zero and an
/// empty table mean "not present".
pub record Properties {
  contentType: String
  contentEncoding: String
  headers: slice[TableEntry]
  /// 1 = transient, 2 = persistent.
  deliveryMode: Int
  /// 0 to 255; 0 is also what an absent priority means to the broker.
  priority: Int
  correlationId: String
  replyTo: String
  /// Per-message TTL in milliseconds, as decimal text.
  expiration: String
  messageId: String
  /// Seconds since the Unix epoch.
  timestamp: Long
  kind: String
  userId: String
  appId: String
}

/// Properties with nothing set.
pub func noProperties(): Properties {
  Properties(
    contentType = "",
    contentEncoding = "",
    headers = emptyTable(),
    deliveryMode = 0,
    priority = 0,
    correlationId = "",
    replyTo = "",
    expiration = "",
    messageId = "",
    timestamp = 0i64,
    kind = "",
    userId = "",
    appId = ""
  )
}

/// A content header: the size of the body that follows and its properties.
pub record ContentHeader {
  bodySize: Long
  properties: Properties
}

func flagIf(present: in Bool, bit: in Int): Int {
  if present {
    1.shl(bit)
  } else {
    0
  }
}

/// The payload of a content header frame.
pub func encodeContentHeader(bodySize: in Long, p: in Properties): Result[slice[Byte], String] {
  if p.deliveryMode < 0 or p.deliveryMode > 255 {
    return Err("delivery-mode " + p.deliveryMode.toString() + " is out of range")
  }
  if p.priority < 0 or p.priority > 255 {
    return Err("priority " + p.priority.toString() + " is out of range (0 to 255)")
  }
  val flags = flagIf(p.contentType.length > 0, 15).or(flagIf(p.contentEncoding.length > 0, 14)).or(flagIf(p.headers.length > 0, 13)).or(flagIf(p.deliveryMode > 0, 12)).or(flagIf(p.priority > 0, 11)).or(flagIf(p.correlationId.length > 0, 10)).or(flagIf(p.replyTo.length > 0, 9)).or(flagIf(p.expiration.length > 0, 8)).or(flagIf(p.messageId.length > 0, 7)).or(flagIf(p.timestamp != 0i64, 6)).or(flagIf(p.kind.length > 0, 5)).or(flagIf(p.userId.length > 0, 4)).or(
    flagIf(p.appId.length > 0, 3)
  )
  val w = newWriter()
  putShort(w, CLASS_BASIC)
  putShort(w, 0)
  putLongLong(w, bodySize)
  putShort(w, flags)
  if p.contentType.length > 0 {
    putShortStr(w, p.contentType)
  }
  if p.contentEncoding.length > 0 {
    putShortStr(w, p.contentEncoding)
  }
  if p.headers.length > 0 {
    putTable(w, p.headers)
  }
  if p.deliveryMode > 0 {
    putOctet(w, p.deliveryMode)
  }
  if p.priority > 0 {
    putOctet(w, p.priority)
  }
  if p.correlationId.length > 0 {
    putShortStr(w, p.correlationId)
  }
  if p.replyTo.length > 0 {
    putShortStr(w, p.replyTo)
  }
  if p.expiration.length > 0 {
    putShortStr(w, p.expiration)
  }
  if p.messageId.length > 0 {
    putShortStr(w, p.messageId)
  }
  if p.timestamp != 0i64 {
    putLongLong(w, p.timestamp)
  }
  if p.kind.length > 0 {
    putShortStr(w, p.kind)
  }
  if p.userId.length > 0 {
    putShortStr(w, p.userId)
  }
  if p.appId.length > 0 {
    putShortStr(w, p.appId)
  }
  finish(w)
}

/// Decode the payload of a content header frame.
pub func decodeContentHeader(payload: in slice[Byte]): Result[ContentHeader, String] {
  val r = newReader(payload)
  val classId = getShort(r)
  val _ = getShort(r)
  val bodySize = getLongLong(r)
  val flags = getShort(r)
  if r.problem.length > 0 {
    return Err("malformed content header: " + r.problem)
  }
  if classId != CLASS_BASIC {
    return Err("content header for class " + classId.toString() + ", expected basic (60)")
  }
  if bodySize < 0i64 {
    return Err("content header declares a negative body size")
  }
  if flags.and(1) != 0 {
    return Err("content header continues its property flags, which the basic class never does")
  }
  val contentType = if flags.and(32768) != 0 { getShortStr(r) } else { "" }
  val contentEncoding = if flags.and(16384) != 0 { getShortStr(r) } else { "" }
  val headers = if flags.and(8192) != 0 { getTable(r) } else { emptyTable() }
  val deliveryMode = if flags.and(4096) != 0 { getOctet(r) } else { 0 }
  val priority = if flags.and(2048) != 0 { getOctet(r) } else { 0 }
  val correlationId = if flags.and(1024) != 0 { getShortStr(r) } else { "" }
  val replyTo = if flags.and(512) != 0 { getShortStr(r) } else { "" }
  val expiration = if flags.and(256) != 0 { getShortStr(r) } else { "" }
  val messageId = if flags.and(128) != 0 { getShortStr(r) } else { "" }
  val timestamp = if flags.and(64) != 0 { getLongLong(r) } else { 0i64 }
  val kind = if flags.and(32) != 0 { getShortStr(r) } else { "" }
  val userId = if flags.and(16) != 0 { getShortStr(r) } else { "" }
  val appId = if flags.and(8) != 0 { getShortStr(r) } else { "" }
  if flags.and(4) != 0 {
    val _ = getShortStr(r)
  }
  if r.problem.length > 0 {
    return Err("malformed content header: " + r.problem)
  }
  Ok(
    ContentHeader(
      bodySize = bodySize,
      properties = Properties(
        contentType = contentType,
        contentEncoding = contentEncoding,
        headers = headers,
        deliveryMode = deliveryMode,
        priority = priority,
        correlationId = correlationId,
        replyTo = replyTo,
        expiration = expiration,
        messageId = messageId,
        timestamp = timestamp,
        kind = kind,
        userId = userId,
        appId = appId
      )
    )
  )
}

// ─── Whole messages ──────────────────────────────────────────────────────────
/// The wire bytes of a method frame on `channel`.
pub func methodFrame(channel: in Int, m: in Method): Result[slice[Byte], String] {
  match encodeMethod(m) {
    case Ok(payload) -> Ok(encodeFrame(FRAME_METHOD, channel, payload))
    case Err(e) -> Err(e)
  }
}

/// The wire bytes of a content-carrying method with its header and body:
/// the method frame, the content header frame and the body split into frames
/// of at most `frameMax` bytes each (overhead included; 0 = one frame).
pub func contentFrames(
  channel: in Int, m: in Method, properties: in Properties, body: in slice[Byte], frameMax: in Int
): Result[slice[Byte], String]
  requires: frameMax == 0 or frameMax >= FRAME_MIN_SIZE
{
  val method: slice[Byte] = match methodFrame(channel, m) { case Ok(b) -> b; case Err(e) -> return Err(e) }
  val header: slice[Byte] = match encodeContentHeader(body.length.toLong(), properties) {
    case Ok(b) -> b
    case Err(e) -> return Err(e)
  }
  if frameMax > 0 and header.length > frameMax - FRAME_OVERHEAD {
    return Err(
      "the message properties need " + header.length.toString() + " bytes, more than the negotiated frame-max of " +
        frameMax.toString() +
        " allows"
    )
  }
  val parts: List[slice[Byte]] = newList()
  parts.add(method)
  parts.add(encodeFrame(FRAME_HEADER, channel, header))
  val chunk = if frameMax == 0 { body.length } else { frameMax - FRAME_OVERHEAD }
  var offset = 0
  while offset < body.length {
    val stop = if body.length - offset > chunk { offset + chunk } else { body.length }
    parts.add(encodeFrame(FRAME_BODY, channel, body.slice(offset, stop)))
    offset = stop
  }
  Ok(joinRange(parts, 0, parts.count))
}

/// `chunks` concatenated in order.
pub func joinChunks(chunks: in List[slice[Byte]]): slice[Byte] {
  if chunks.count == 0 {
    val none: slice[Byte] = []
    return none
  }
  joinRange(chunks, 0, chunks.count)
}
