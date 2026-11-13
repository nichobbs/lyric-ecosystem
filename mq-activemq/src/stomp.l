//! Mq.ActiveMq.Stomp - STOMP 1.2 frames: the encoder, and an incremental
//! decoder fed with whatever bytes the socket produced.
//!
//! A frame is a command line, header lines, a blank line, a body and a NUL
//! byte. Header names and values are UTF-8 with the four escapes of STOMP 1.2
//! (`\n`, `\r`, `\c` for a colon, `\\`); `CONNECT` and `CONNECTED` are the two
//! frames the specification exempts from escaping. A body is raw bytes, sized
//! by `content-length` when the header is present and by the first NUL
//! otherwise. Lines may end in LF or CRLF, and any number of bare line ends
//! between frames (heart-beats) are skipped. When a header repeats, the first
//! occurrence is the one `headerValue` reports, as the specification requires.
//!
//! Limits. A header section (command line included) may hold `MAX_HEADER_BYTES`
//! bytes and a body the decoder's `maxFrameBytes`. A frame over either limit is
//! not an error of the stream: the decoder reads it to its end without keeping
//! it, reports it as `TooLarge` with its command and the headers it kept, and
//! goes on with the next frame. Memory stays bounded while it does.
@runtime_checked
package Mq.ActiveMq.Stomp

import Std.Core
import Std.Collections
import Std.Encoding
import Std.Parse
import Std.String as Str

/// One frame header.
pub record Header {
  name: String
  value: String
}

/// One STOMP frame.
pub record Frame {
  command: String
  headers: slice[Header]
  body: slice[Byte]
}

/// The largest header section (command line included) the decoder accepts.
pub val MAX_HEADER_BYTES: Int = 65536

val LF: Byte = 10u8

val CR: Byte = 13u8

val NUL: Byte = 0u8

val COLON: Byte = 58u8

/// A frame with a UTF-8 text body.
pub func textFrame(command: in String, headers: in slice[Header], body: in String): Frame {
  Frame(command = command, headers = headers, body = encodeUtf8(body))
}

/// A frame without a body.
pub func bareFrame(command: in String, headers: in slice[Header]): Frame {
  val empty: slice[Byte] = []
  Frame(command = command, headers = headers, body = empty)
}

/// The value of the first header called `name`.
pub func headerValue(frame: in Frame, name: in String): Option[String] {
  for h in frame.headers {
    if h.name == name {
      return Some(value = h.value)
    }
  }
  None
}

/// `headerValue`, with `fallback` when the header is absent.
pub func headerOr(frame: in Frame, name: in String, fallback: in String): String {
  match headerValue(frame, name) {
    case Some(v) -> v
    case None -> fallback
  }
}

/// The body as text, `None` when it is not valid UTF-8.
pub func bodyText(frame: in Frame): Option[String] {
  tryDecodeUtf8(frame.body)
}

func escapesHeaders(command: in String): Bool {
  command != "CONNECT" and command != "CONNECTED"
}

/// A header name or value with the STOMP 1.2 escapes applied.
pub func escapeHeader(text: in String): String {
  val sb = Str.StringBuilder.new()
  var start = 0
  var i = 0
  while i < text.length {
    val unit = text.codeUnitAt(i)
    val replacement = if unit == 92 {
      "\\\\"
    } else if unit == 10 {
      "\\n"
    } else if unit == 13 {
      "\\r"
    } else if unit == 58 {
      "\\c"
    } else {
      ""
    }
    if replacement.length > 0 {
      sb.append(Str.substring(text, start, i - start))
      sb.append(replacement)
      start = i + 1
    }
    i += 1
  }
  sb.append(Str.substring(text, start))
  sb.toString()
}

/// The inverse of `escapeHeader`. An escape STOMP 1.2 does not define, or a
/// trailing backslash, is an `Err`.
pub func unescapeHeader(text: in String): Result[String, String] {
  if not Str.contains(text, "\\") {
    return Ok(text)
  }
  val sb = Str.StringBuilder.new()
  var start = 0
  var i = 0
  while i < text.length {
    if text.codeUnitAt(i) == 92 {
      if i + 1 >= text.length {
        return Err("STOMP header ends in an unfinished escape")
      }
      val next = text.codeUnitAt(i + 1)
      val replacement = if next == 92 {
        "\\"
      } else if next == 110 {
        "\n"
      } else if next == 114 {
        "\r"
      } else if next == 99 {
        ":"
      } else {
        return Err("STOMP header uses an undefined escape")
      }
      sb.append(Str.substring(text, start, i - start))
      sb.append(replacement)
      start = i + 2
      i += 2
    } else {
      i += 1
    }
  }
  sb.append(Str.substring(text, start))
  Ok(sb.toString())
}

// True when `text` holds a UTF-16 code unit equal to `unit`.
func hasUnit(text: in String, unit: in Int): Bool {
  var i = 0
  while i < text.length {
    if text.codeUnitAt(i) == unit {
      return true
    }
    i += 1
  }
  false
}

// A NUL cannot be carried in a header (it ends the frame), and outside the
// escaping frames neither can a line end or, in a name, a colon.
func headerProblem(command: in String, h: in Header): Option[String] {
  if h.name.length == 0 {
    return Some(value = "a STOMP header name is empty")
  }
  if hasUnit(h.name, 0) or hasUnit(h.value, 0) {
    return Some(value = "a STOMP header contains a NUL character")
  }
  if not escapesHeaders(command) {
    if hasUnit(h.name, 10) or hasUnit(h.name, 13) or hasUnit(h.value, 10) or hasUnit(h.value, 13) {
      return Some(value = "a " + command + " header cannot contain a line end")
    }
    if hasUnit(h.name, 58) {
      return Some(value = "a " + command + " header name cannot contain a colon")
    }
  }
  None
}

/// The wire bytes of `frame`. A `content-length` header is added whenever the
/// body is not empty (one supplied by the caller is ignored), so a body may
/// hold NUL bytes. A header that cannot be represented is an `Err`.
pub func encodeFrame(frame: in Frame): Result[slice[Byte], String] {
  if frame.command.length == 0 {
    return Err("a STOMP frame needs a command")
  }
  val escaped = escapesHeaders(frame.command)
  val sb = Str.StringBuilder.new()
  sb.append(frame.command)
  sb.append("\n")
  for h in frame.headers {
    match headerProblem(frame.command, h) {
      case Some(problem) -> return Err(problem)
      case None -> ()
    }
    if h.name != "content-length" {
      sb.append(if escaped { escapeHeader(h.name) } else { h.name })
      sb.append(":")
      sb.append(if escaped { escapeHeader(h.value) } else { h.value })
      sb.append("\n")
    }
  }
  if frame.body.length > 0 {
    sb.append("content-length:" + frame.body.length.toString() + "\n")
  }
  sb.append("\n")
  val head = encodeUtf8(sb.toString())
  val terminator: slice[Byte] = [NUL]
  Ok(head.concat(frame.body).concat(terminator))
}

/// A frame the decoder read past without keeping, because its header section
/// or its body was over the limit.
pub record Skipped {
  command: String
  /// For an oversize body: all of the frame's headers. For an oversize header
  /// section: the headers that ended within the first `MAX_HEADER_BYTES`
  /// bytes, and of the later ones only `ack`, `message-id`, `subscription`,
  /// `destination` and `content-length` (those no longer than
  /// `SKIPPED_LINE_BYTES`).
  headers: slice[Header]
  /// Which limit the frame was over, as the end of a sentence about it: "its
  /// body of 5000 bytes exceeds the limit of 1024 bytes".
  reason: String
}

/// What `nextDecoded` produced.
pub union Decoded {
  case Complete(frame: Frame)
  case TooLarge(skipped: Skipped)
}

/// The longest header line kept from the part of a header section beyond
/// `MAX_HEADER_BYTES`.
pub val SKIPPED_LINE_BYTES: Int = 1024

val SKIP_NONE: Int = 0

// Reading header lines beyond `MAX_HEADER_BYTES`.
val SKIP_HEADERS: Int = 1

// Dropping a body of known length, then its NUL.
val SKIP_COUNTED: Int = 2

// Dropping a body up to its NUL.
val SKIP_TO_NUL: Int = 3

/// An incremental frame decoder. `feed` it bytes as they arrive and call
/// `nextDecoded` (or `nextFrame`) until it reports `None`.
pub ref record Decoder {
  var buffer: slice[Byte]
  var chunks: List[slice[Byte]]
  var chunkBytes: Int
  // The buffered byte count below which no complete frame can be present.
  var need: Int
  // Where the search for a body's terminating NUL resumes.
  var scanned: Int
  // The frame being skipped, when `skipMode` is not `SKIP_NONE`.
  var skipMode: Int
  var skipCommand: String
  var skipHeaders: List[Header]
  var skipReason: String
  // The frame's content-length, -1 while none has been seen.
  var skipDeclared: Int
  // Body bytes still to drop (`SKIP_COUNTED`).
  var skipLeft: Int
  // Inside a header line whose start has already been dropped.
  var skipMidLine: Bool
  maxFrameBytes: Int
}

/// A decoder that skips frames whose body is larger than `maxFrameBytes`.
pub func newDecoder(maxFrameBytes: in Int): Decoder
  requires: maxFrameBytes > 0
{
  val empty: slice[Byte] = []
  Decoder(
    buffer = empty,
    chunks = newList(),
    chunkBytes = 0,
    need = 0,
    scanned = 0,
    skipMode = SKIP_NONE,
    skipCommand = "",
    skipHeaders = newList(),
    skipReason = "",
    skipDeclared = -1,
    skipLeft = 0,
    skipMidLine = false,
    maxFrameBytes = maxFrameBytes
  )
}

/// Hand the decoder the next bytes from the wire.
pub func feed(d: in Decoder, bytes: in slice[Byte]): Unit {
  if bytes.length > 0 {
    d.chunks.add(bytes)
    d.chunkBytes = d.chunkBytes + bytes.length
  }
}

/// The number of bytes fed but not yet returned as part of a frame or dropped.
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

func consume(d: in Decoder, count: in Int): Unit {
  d.buffer = d.buffer.slice(count, d.buffer.length)
  d.need = 0
  d.scanned = 0
}

func lineText(buf: in slice[Byte], from: in Int, to: in Int): Result[String, String] {
  var stop = to
  if stop > from and buf[stop - 1] == CR {
    stop = stop - 1
  }
  match tryDecodeUtf8(buf.slice(from, stop)) {
    case Some(s) -> Ok(s)
    case None -> Err("a STOMP header line is not valid UTF-8")
  }
}

func parseHeaderLine(command: in String, line: in String): Result[Header, String] {
  val colon = Str.indexOfRaw(line, ":")
  if colon < 0 {
    return Err("a STOMP header line has no colon")
  }
  val rawName = Str.substring(line, 0, colon)
  val rawValue = Str.substring(line, colon + 1)
  if not escapesHeaders(command) {
    return Ok(Header(name = rawName, value = rawValue))
  }
  val name = match unescapeHeader(rawName) { case Ok(v) -> v; case Err(e) -> return Err(e) }
  val value = match unescapeHeader(rawValue) { case Ok(v) -> v; case Err(e) -> return Err(e) }
  Ok(Header(name = name, value = value))
}

func parseLength(text: in String): Result[Int, String] {
  match tryParseInt(Str.trim(text)) {
    case Ok(len) -> if len >= 0 { Ok(len) } else { Err("a STOMP content-length is negative") }
    case Err(_) -> Err("a STOMP content-length is not a number")
  }
}

// The first content-length among `headers`, -1 without one.
func declaredLength(headers: in List[Header]): Result[Int, String] {
  for h in headers.toArray() {
    if h.name == "content-length" {
      return parseLength(h.value)
    }
  }
  Ok(-1)
}

func beginSkip(
  d: in Decoder,
  mode: in Int,
  command: in String,
  headers: in List[Header],
  declared: in Int,
  reason: in String,
): Unit {
  d.skipMode = mode
  d.skipCommand = command
  d.skipHeaders = headers
  d.skipDeclared = declared
  d.skipLeft = if declared > 0 { declared } else { 0 }
  d.skipReason = reason
  d.skipMidLine = false
}

func finishSkip(d: in Decoder): Decoded {
  val skipped = Skipped(command = d.skipCommand, headers = d.skipHeaders.toArray(), reason = d.skipReason)
  d.skipMode = SKIP_NONE
  d.skipCommand = ""
  d.skipHeaders = newList()
  d.skipReason = ""
  d.skipDeclared = -1
  d.skipLeft = 0
  d.skipMidLine = false
  TooLarge(skipped = skipped)
}

func isKeptWhenSkipping(name: in String): Bool {
  name == "ack" or name == "message-id" or name == "subscription" or name == "destination" or name == "content-length"
}

func hasNamed(headers: in List[Header], name: in String): Bool {
  for h in headers.toArray() {
    if h.name == name {
      return true
    }
  }
  false
}

// One header line of the part beyond `MAX_HEADER_BYTES`: kept only when it is
// one of the headers a caller needs to answer the frame and the first of its
// name. A line that does not parse is dropped like the rest; a content-length
// that does not parse is an error, because the frame's end depends on it.
func keepSkippedLine(d: in Decoder, buf: in slice[Byte], from: in Int, to: in Int): Result[Unit, String] {
  val line = match lineText(buf, from, to) { case Ok(v) -> v; case Err(_) -> return Ok(()) }
  val header = match parseHeaderLine(d.skipCommand, line) { case Ok(v) -> v; case Err(_) -> return Ok(()) }
  if not isKeptWhenSkipping(header.name) or hasNamed(d.skipHeaders, header.name) {
    return Ok(())
  }
  if header.name == "content-length" {
    val declared = match parseLength(header.value) { case Ok(n) -> n; case Err(e) -> return Err(e) }
    d.skipDeclared = declared
    d.skipLeft = declared
  }
  d.skipHeaders.add(header)
  Ok(())
}

// Go on dropping the frame being skipped. `Ok(None)` when its end has not
// arrived yet; whatever was buffered has been dropped by then, except the
// start of a short header line.
func continueSkip(d: in Decoder): Result[Option[Decoded], String] {
  val buf = d.buffer
  var pos = 0
  while true {
    if d.skipMode == SKIP_HEADERS {
      var lf = pos
      while lf < buf.length and buf[lf] != LF {
        lf += 1
      }
      if lf >= buf.length {
        if buf.length - pos > SKIPPED_LINE_BYTES {
          d.skipMidLine = true
          pos = buf.length
        }
        consume(d, pos)
        return Ok(None)
      }
      if d.skipMidLine {
        d.skipMidLine = false
      } else if lf == pos or (lf == pos + 1 and buf[pos] == CR) {
        d.skipMode = if d.skipDeclared >= 0 { SKIP_COUNTED } else { SKIP_TO_NUL }
      } else if lf - pos <= SKIPPED_LINE_BYTES {
        match keepSkippedLine(d, buf, pos, lf) {
          case Ok(_) -> ()
          case Err(e) -> return Err(e)
        }
      }
      pos = lf + 1
    } else if d.skipMode == SKIP_COUNTED {
      val available = buf.length - pos
      if d.skipLeft > 0 {
        val drop = if available < d.skipLeft { available } else { d.skipLeft }
        d.skipLeft = d.skipLeft - drop
        pos = pos + drop
      }
      if d.skipLeft > 0 or pos >= buf.length {
        consume(d, pos)
        return Ok(None)
      }
      if buf[pos] != NUL {
        return Err("a STOMP frame body is not followed by a NUL byte")
      }
      consume(d, pos + 1)
      return Ok(Some(value = finishSkip(d)))
    } else {
      while pos < buf.length and buf[pos] != NUL {
        pos += 1
      }
      if pos >= buf.length {
        consume(d, pos)
        return Ok(None)
      }
      consume(d, pos + 1)
      return Ok(Some(value = finishSkip(d)))
    }
  }
  Ok(None)
}

/// The next frame, `Ok(None)` when more bytes are needed. A frame over a
/// limit is reported as `TooLarge` once all of it has been read and dropped;
/// the frames after it decode as usual. An `Err` is a protocol violation: the
/// stream cannot be resynchronised after it.
pub func nextDecoded(d: in Decoder): Result[Option[Decoded], String] {
  if d.skipMode != SKIP_NONE {
    absorbChunks(d)
    return continueSkip(d)
  }
  if d.buffer.length + d.chunkBytes < d.need {
    return Ok(None)
  }
  absorbChunks(d)
  val buf = d.buffer
  // Line ends between frames are heart-beats.
  var pos = 0
  while pos < buf.length and (buf[pos] == LF or buf[pos] == CR) {
    pos += 1
  }
  if pos > 0 {
    consume(d, pos)
    return nextDecoded(d)
  }
  if buf.length == 0 {
    return Ok(None)
  }
  // The header section ends at the first empty line.
  val lineStarts: List[Int] = newList()
  val lineEnds: List[Int] = newList()
  var lineStart = 0
  var bodyStart = -1
  var i = 0
  val headerLimit = if buf.length < MAX_HEADER_BYTES { buf.length } else { MAX_HEADER_BYTES }
  while i < headerLimit and bodyStart < 0 {
    if buf[i] == LF {
      val blank = i == lineStart or (i == lineStart + 1 and buf[lineStart] == CR)
      if blank {
        bodyStart = i + 1
      } else {
        lineStarts.add(lineStart)
        lineEnds.add(i)
        lineStart = i + 1
      }
    }
    i += 1
  }
  if bodyStart < 0 and buf.length < MAX_HEADER_BYTES {
    return Ok(None)
  }
  if lineStarts.count == 0 {
    if bodyStart < 0 {
      return Err("a STOMP frame's command line exceeds " + MAX_HEADER_BYTES.toString() + " bytes")
    }
    return Err("a STOMP frame has no command line")
  }
  val command = match lineText(buf, lineStarts[0], lineEnds[0]) { case Ok(v) -> v; case Err(e) -> return Err(e) }
  val headers: List[Header] = newList()
  var n = 1
  while n < lineStarts.count {
    val line = match lineText(buf, lineStarts[n], lineEnds[n]) { case Ok(v) -> v; case Err(e) -> return Err(e) }
    val header = match parseHeaderLine(command, line) { case Ok(v) -> v; case Err(e) -> return Err(e) }
    headers.add(header)
    n += 1
  }
  val declared = match declaredLength(headers) { case Ok(v) -> v; case Err(e) -> return Err(e) }
  if bodyStart < 0 {
    // The header section runs past the limit: keep what ended within it and
    // read the rest line by line.
    beginSkip(d, SKIP_HEADERS, command, headers, declared, "its headers exceed " + MAX_HEADER_BYTES.toString() + " bytes")
    consume(d, lineStart)
    return continueSkip(d)
  }
  var bodyEnd = -1
  if declared >= 0 {
    if declared > d.maxFrameBytes {
      beginSkip(
        d,
        SKIP_COUNTED,
        command,
        headers,
        declared,
        "its body of " + declared.toString() + " bytes exceeds the limit of " + d.maxFrameBytes.toString() + " bytes"
      )
      consume(d, bodyStart)
      return continueSkip(d)
    }
    if buf.length < bodyStart + declared + 1 {
      d.need = bodyStart + declared + 1
      return Ok(None)
    }
    if buf[bodyStart + declared] != NUL {
      return Err("a STOMP frame body is not followed by a NUL byte")
    }
    bodyEnd = bodyStart + declared
  } else {
    var j = if d.scanned > bodyStart { d.scanned } else { bodyStart }
    while j < buf.length and bodyEnd < 0 {
      if buf[j] == NUL {
        bodyEnd = j
      }
      j += 1
    }
    if bodyEnd < 0 {
      if buf.length - bodyStart > d.maxFrameBytes {
        beginSkip(
          d,
          SKIP_TO_NUL,
          command,
          headers,
          -1,
          "its body exceeds the limit of " + d.maxFrameBytes.toString() + " bytes"
        )
        consume(d, buf.length)
        return Ok(None)
      }
      d.scanned = buf.length
      d.need = buf.length + 1
      return Ok(None)
    }
    if bodyEnd - bodyStart > d.maxFrameBytes {
      beginSkip(
        d,
        SKIP_TO_NUL,
        command,
        headers,
        -1,
        "its body exceeds the limit of " + d.maxFrameBytes.toString() + " bytes"
      )
      consume(d, bodyEnd + 1)
      return Ok(Some(value = finishSkip(d)))
    }
  }
  val frame = Frame(command = command, headers = headers.toArray(), body = buf.slice(bodyStart, bodyEnd))
  consume(d, bodyEnd + 1)
  Ok(Some(value = Complete(frame = frame)))
}

/// `nextDecoded` for a caller that has no use for a frame over a limit: such
/// a frame is an `Err` naming it. The stream is still in step after that
/// `Err`, but a caller that cannot tell it from a protocol violation should
/// give the connection up.
pub func nextFrame(d: in Decoder): Result[Option[Frame], String] {
  match nextDecoded(d) {
    case Err(e) -> Err(e)
    case Ok(None) -> Ok(None)
    case Ok(Some(item)) -> match item {
      case Complete(frame) -> Ok(Some(value = frame))
      case TooLarge(skipped) -> Err(
        "a STOMP " + skipped.command + " frame was skipped: " +
          skipped.reason
      )
    }
  }
}
