//! Redis.Resp - the Redis serialization protocol, versions 2 and 3.
//!
//! Pure protocol code with no I/O: `encodeCommand` turns a command into the
//! bytes a client sends (an array of bulk strings), and `decode` reads one
//! reply out of a byte buffer, reporting `Incomplete` when the buffer ends
//! before the reply does. `RespDecoder` / `feed` wrap `decode` for a caller
//! that receives the stream in arbitrary chunks.
//!
//! Every RESP2 type and every RESP3 type a Redis server sends is decoded.
//! RESP3 attributes (`|`) are read and dropped, since they annotate the reply
//! that follows them. The RESP3 streamed forms (`$?`, `*?`) are rejected:
//! no Redis release emits them.
@runtime_checked
@pure
package Redis.Resp

import Std.Core
import Std.Collections
import Std.Encoding

/// One decoded reply.
@experimental
pub union RespValue {
  /// `+OK` - a status line.
  case RSimple(text: String)
  /// `-ERR ...` or the RESP3 blob error `!`.
  case RError(message: String)
  /// `:42`
  case RInt(value: Long)
  /// `$5\r\nhello` - a binary-safe string.
  case RBulk(bytes: slice[Byte])
  /// `$-1`, `*-1` or the RESP3 `_`.
  case RNull
  /// `*N`
  case RArray(items: List[RespValue])
  /// RESP3 `,3.14` - the number as the server wrote it (`inf`, `-inf` and `nan` included).
  case RDouble(text: String)
  /// RESP3 `#t` / `#f`
  case RBool(value: Bool)
  /// RESP3 `(123456789012345678901234567890`
  case RBigNumber(text: String)
  /// RESP3 `=` - a string with a three-letter format tag (`txt`, `mkd`).
  case RVerbatim(format: String, bytes: slice[Byte])
  /// RESP3 `%N` - `entries` alternates key, value.
  case RMap(entries: List[RespValue])
  /// RESP3 `~N`
  case RSet(items: List[RespValue])
  /// RESP3 `>N` - an out-of-band message (pub/sub, invalidation).
  case RPush(items: List[RespValue])
}

/// The outcome of reading one reply from a buffer.
@experimental
pub union DecodeStep {
  /// A whole reply; `next` is the offset of the byte after it.
  case Decoded(value: RespValue, next: Int)
  /// The buffer ends inside the reply.
  case Incomplete
  /// The bytes are not RESP.
  case Malformed(message: String)
}

// A reply nested deeper than this is refused instead of recursing without bound.
val maxDepth: Int = 128

// Redis caps a bulk string at 512 MiB (`proto-max-bulk-len`).
val maxBulkLength: Long = 536870912i64

val maxAggregateLength: Long = 16777216i64

func byteAt(buf: in slice[Byte], i: in Int): Int {
  buf[i].toInt().and(255)
}

func pushAscii(acc: in List[Byte], text: in String): Unit {
  val bytes = encodeUtf8(text)
  var i = 0
  while i < bytes.length {
    acc.add(bytes[i])
    i = i + 1
  }
}

func pushBytes(acc: in List[Byte], bytes: in slice[Byte]): Unit {
  var i = 0
  while i < bytes.length {
    acc.add(bytes[i])
    i = i + 1
  }
}

/// The bytes of a command: `*<n>` followed by each argument as a bulk string.
@experimental
pub func encodeCommandBytes(args: in slice[slice[Byte]]): slice[Byte]
  requires: args.length > 0
{
  val acc: List[Byte] = newList()
  pushAscii(acc, "*" + args.length.toString() + "\r\n")
  var i = 0
  while i < args.length {
    val arg: slice[Byte] = args[i]
    pushAscii(acc, "$" + arg.length.toString() + "\r\n")
    pushBytes(acc, arg)
    pushAscii(acc, "\r\n")
    i = i + 1
  }
  acc.toArray()
}

/// `encodeCommandBytes` for arguments given as text (sent as UTF-8).
@experimental
pub func encodeCommand(args: in slice[String]): slice[Byte]
  requires: args.length > 0
{
  val acc: List[Byte] = newList()
  pushAscii(acc, "*" + args.length.toString() + "\r\n")
  var i = 0
  while i < args.length {
    val arg = encodeUtf8(args[i])
    pushAscii(acc, "$" + arg.length.toString() + "\r\n")
    pushBytes(acc, arg)
    pushAscii(acc, "\r\n")
    i = i + 1
  }
  acc.toArray()
}

// Offset of the `\r` of the first CRLF at or after `from`, or -1.
func findCrlf(buf: in slice[Byte], from: in Int): Int {
  var i = from
  while i + 1 < buf.length {
    if byteAt(buf, i) == 13 and byteAt(buf, i + 1) == 10 {
      return i
    }
    i = i + 1
  }
  0 - 1
}

func textOf(buf: in slice[Byte], lo: in Int, hi: in Int): String {
  match tryDecodeUtf8(buf.slice(lo, hi)) {
    case Some(s) -> s
    case None -> latin1Of(buf, lo, hi)
  }
}

// A status or error line that is not UTF-8 is shown byte for byte.
func latin1Of(buf: in slice[Byte], lo: in Int, hi: in Int): String {
  var acc = ""
  var i = lo
  while i < hi {
    acc = acc + codepointToString(byteAt(buf, i))
    i = i + 1
  }
  acc
}

union IntParse {
  case IntOk(value: Long)
  case IntBad
}

func parseIntegerLine(buf: in slice[Byte], lo: in Int, hi: in Int): IntParse {
  var i = lo
  var negative = false
  if i < hi and byteAt(buf, i) == 45 {
    negative = true
    i = i + 1
  } else if i < hi and byteAt(buf, i) == 43 {
    i = i + 1
  }
  if i >= hi or hi - i > 19 {
    return IntBad
  }
  // Accumulated as a negative number, so the most negative Long is reachable.
  var acc: Long = 0i64
  while i < hi {
    val d = byteAt(buf, i) - 48
    if d < 0 or d > 9 {
      return IntBad
    }
    if acc < (0i64 - 922337203685477580i64) {
      return IntBad
    }
    acc = acc * 10i64
    if acc < (0i64 - 9223372036854775807i64 - 1i64) + d.toLong() {
      return IntBad
    }
    acc = acc - d.toLong()
    i = i + 1
  }
  if negative {
    return IntOk(value = acc)
  }
  if acc == (0i64 - 9223372036854775807i64 - 1i64) {
    return IntBad
  }
  IntOk(value = 0i64 - acc)
}

func typeName(marker: in Int): String {
  codepointToString(marker)
}

// A length-prefixed payload followed by CRLF: `$`, `!`, `=`.
func decodeBlob(buf: in slice[Byte], marker: in Int, lineStart: in Int, lineEnd: in Int): DecodeStep {
  match parseIntegerLine(buf, lineStart, lineEnd) {
    case IntBad -> Malformed(message = "invalid length after '" + typeName(marker) + "'")
    case IntOk(n) -> {
      if n == (0i64 - 1i64) and marker == 36 {
        return Decoded(value = RNull, next = lineEnd + 2)
      }
      if n < 0i64 or n > maxBulkLength {
        return Malformed(message = "bulk length out of range: " + n.toString())
      }
      val begin = lineEnd + 2
      val stop = begin + n.toInt()
      if buf.length < stop + 2 {
        return Incomplete
      }
      if byteAt(buf, stop) != 13 or byteAt(buf, stop + 1) != 10 {
        return Malformed(message = "bulk payload is not followed by CRLF")
      }
      if marker == 33 {
        return Decoded(value = RError(message = textOf(buf, begin, stop)), next = stop + 2)
      }
      if marker == 61 {
        if n < 4i64 or byteAt(buf, begin + 3) != 58 {
          return Malformed(message = "verbatim string without a format prefix")
        }
        return Decoded(
          value = RVerbatim(format = textOf(buf, begin, begin + 3), bytes = buf.slice(begin + 4, stop)),
          next = stop + 2
        )
      }
      Decoded(value = RBulk(bytes = buf.slice(begin, stop)), next = stop + 2)
    }
  }
}

func aggregateOf(marker: in Int, items: in List[RespValue]): RespValue {
  if marker == 37 {
    RMap(entries = items)
  } else if marker == 126 {
    RSet(items = items)
  } else if marker == 62 {
    RPush(items = items)
  } else {
    RArray(items = items)
  }
}

// `*`, `~`, `>` hold N replies; `%` and `|` hold N pairs.
func decodeAggregate(buf: in slice[Byte], marker: in Int, lineStart: in Int, lineEnd: in Int, depth: in Int): DecodeStep {
  match parseIntegerLine(buf, lineStart, lineEnd) {
    case IntBad -> Malformed(message = "invalid length after '" + typeName(marker) + "'")
    case IntOk(n) -> {
      if n == (0i64 - 1i64) and marker == 42 {
        return Decoded(value = RNull, next = lineEnd + 2)
      }
      if n < 0i64 or n > maxAggregateLength {
        return Malformed(message = "aggregate length out of range: " + n.toString())
      }
      val pairs = marker == 37 or marker == 124
      val count = if pairs { n.toInt() * 2 } else { n.toInt() }
      val items: List[RespValue] = newList()
      var pos = lineEnd + 2
      var i = 0
      while i < count {
        match decodeAt(buf, pos, depth + 1) {
          case Incomplete -> return Incomplete
          case Malformed(m) -> return Malformed(message = m)
          case Decoded(v, next) -> {
            items.add(v)
            pos = next
          }
        }
        i = i + 1
      }
      if marker == 124 {
        // An attribute map annotates the reply after it; the reply is the value.
        return decodeAt(buf, pos, depth + 1)
      }
      Decoded(value = aggregateOf(marker, items), next = pos)
    }
  }
}

func decodeAt(buf: in slice[Byte], offset: in Int, depth: in Int): DecodeStep {
  if depth > maxDepth {
    return Malformed(message = "reply nested deeper than " + maxDepth.toString() + " levels")
  }
  if offset >= buf.length {
    return Incomplete
  }
  val marker = byteAt(buf, offset)
  val known = marker == 43 or marker == 45 or marker == 58 or marker == 36 or marker == 42 or marker == 95 or
    marker == 35 or
    marker == 44 or
    marker == 40 or
    marker == 33 or
    marker == 61 or
    marker == 37 or
    marker == 126 or
    marker == 62 or
    marker == 124
  if not known {
    return Malformed(message = "unknown RESP type byte " + marker.toString())
  }
  val lineStart = offset + 1
  val lineEnd = findCrlf(buf, lineStart)
  if lineEnd < 0 {
    return Incomplete
  }
  val after = lineEnd + 2
  if marker == 43 {
    return Decoded(value = RSimple(text = textOf(buf, lineStart, lineEnd)), next = after)
  }
  if marker == 45 {
    return Decoded(value = RError(message = textOf(buf, lineStart, lineEnd)), next = after)
  }
  if marker == 58 {
    return match parseIntegerLine(buf, lineStart, lineEnd) {
      case IntOk(n) -> Decoded(value = RInt(value = n), next = after)
      case IntBad -> Malformed(message = "invalid integer reply")
    }
  }
  if marker == 95 {
    if lineEnd != lineStart {
      return Malformed(message = "invalid null reply")
    }
    return Decoded(value = RNull, next = after)
  }
  if marker == 35 {
    if lineEnd == lineStart + 1 and byteAt(buf, lineStart) == 116 {
      return Decoded(value = RBool(value = true), next = after)
    }
    if lineEnd == lineStart + 1 and byteAt(buf, lineStart) == 102 {
      return Decoded(value = RBool(value = false), next = after)
    }
    return Malformed(message = "invalid boolean reply")
  }
  if marker == 44 {
    return Decoded(value = RDouble(text = textOf(buf, lineStart, lineEnd)), next = after)
  }
  if marker == 40 {
    return Decoded(value = RBigNumber(text = textOf(buf, lineStart, lineEnd)), next = after)
  }
  if lineEnd == lineStart + 1 and byteAt(buf, lineStart) == 63 {
    return Malformed(message = "streamed RESP3 replies are not supported")
  }
  if marker == 36 or marker == 33 or marker == 61 {
    return decodeBlob(buf, marker, lineStart, lineEnd)
  }
  decodeAggregate(buf, marker, lineStart, lineEnd, depth)
}

/// Read one reply starting at `offset`.
@experimental
pub func decode(buf: in slice[Byte], offset: in Int): DecodeStep
  requires: offset >= 0
{
  decodeAt(buf, offset, 0)
}

/// The bytes received so far that do not yet form a whole reply.
@experimental
pub record RespDecoder {
  pending: slice[Byte]
  failed: Bool
}

/// What `feed` produced: the decoder to use next, the replies completed by the
/// bytes just fed, and the protocol error that stopped decoding, if any.
@experimental
pub record FeedResult {
  decoder: RespDecoder
  values: List[RespValue]
  error: Option[String]
}

/// A decoder with nothing buffered.
@experimental
pub func newDecoder(): RespDecoder {
  val empty: slice[Byte] = []
  RespDecoder(pending = empty, failed = false)
}

/// Add `bytes` to the stream and return every reply now complete. After a
/// protocol error the stream cannot be resynchronised, so the returned decoder
/// is `failed` and must not be fed again.
@experimental
pub func feed(decoder: in RespDecoder, bytes: in slice[Byte]): FeedResult
  requires: not decoder.failed
{
  var working = decoder.pending.concat(bytes)
  val values: List[RespValue] = newList()
  var error: Option[String] = None
  var more = true
  while more {
    match decodeAt(working, 0, 0) {
      case Incomplete -> {
        more = false
      }
      case Malformed(m) -> {
        error = Some(value = m)
        more = false
      }
      case Decoded(v, next) -> {
        values.add(v)
        working = working.slice(next, working.length)
      }
    }
  }
  val failed = match error { case Some(_) -> true; case None -> false }
  FeedResult(decoder = RespDecoder(pending = working, failed = failed), values = values, error = error)
}

/// The reply as text when it is a string of some kind (status, bulk or
/// verbatim, decoded as UTF-8), else `None`.
@experimental
pub func asText(v: in RespValue): Option[String] {
  match v {
    case RSimple(t) -> Some(value = t)
    case RBulk(b) -> tryDecodeUtf8(b)
    case RVerbatim(_, b) -> tryDecodeUtf8(b)
    case RDouble(t) -> Some(value = t)
    case RBigNumber(t) -> Some(value = t)
    case _ -> None
  }
}

/// A short description of the reply's type, for error messages.
@experimental
pub func kindOf(v: in RespValue): String {
  match v {
    case RSimple(_) -> "simple string"
    case RError(_) -> "error"
    case RInt(_) -> "integer"
    case RBulk(_) -> "bulk string"
    case RNull -> "null"
    case RArray(_) -> "array"
    case RDouble(_) -> "double"
    case RBool(_) -> "boolean"
    case RBigNumber(_) -> "big number"
    case RVerbatim(_, _) -> "verbatim string"
    case RMap(_) -> "map"
    case RSet(_) -> "set"
    case RPush(_) -> "push"
  }
}
