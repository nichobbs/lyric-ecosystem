//! Cache.Memcached.Meta - the memcached meta text protocol (`mg`, `ms`, `md`,
//! `ma`, `mn`), memcached 1.6.10 and later.
//!
//! Pure protocol code with no I/O: the `encode...` functions produce the
//! bytes a client sends, and `decode` reads one reply out of a byte buffer,
//! reporting `Incomplete` when the buffer ends before the reply does.
//!
//! Every key travels base64-encoded with the `b` flag, so a key may hold any
//! bytes; the functions here take the key already encoded (`Keys.wireKey`).
//! Besides the meta commands, three classic commands that have no meta form
//! are encoded and their replies decoded: `version`, `flush_all`, and the
//! `set` that carries the credentials of memcached's ASCII authentication.
@runtime_checked
@pure
package Cache.Memcached.Meta

import Std.Core
import Std.Collections
import Std.Encoding
import Std.Parse
import Std.String as Str

/// One decoded reply. `flags` are the return flags as sent, one token each
/// (`"t-1"`, `"c42"`, `"O7"`).
@experimental
pub union MetaReply {
  /// `VA <size> <flags>*` and the data block: a hit that returns the value.
  case Value(data: slice[Byte], flags: List[String])
  /// `HD <flags>*`: success without a value.
  case Head(flags: List[String])
  /// `EN`: `mg` found nothing.
  case Miss(flags: List[String])
  /// `NF`: the key of an `ms`, `md` or `ma` does not exist.
  case NotFound(flags: List[String])
  /// `NS`: `ms` did not store (an add on an existing key, for example).
  case NotStored(flags: List[String])
  /// `EX`: the compare-and-swap value did not match.
  case Exists(flags: List[String])
  /// `MN`: the reply to `mn`.
  case NoOp
  /// `VERSION <text>`.
  case Version(text: String)
  /// `OK`: the reply to `flush_all`.
  case Done
  /// `STORED`: the reply to the authentication `set`.
  case Stored
  /// `CLIENT_ERROR <message>`: the server refused the request as malformed.
  case ClientError(message: String)
  /// `SERVER_ERROR <message>`: the server could not carry the request out.
  case ServerError(message: String)
  /// `ERROR`: the server does not know the command.
  case UnknownCommand
}

/// The outcome of reading one reply from a buffer.
@experimental
pub union DecodeStep {
  /// A whole reply; `next` is the offset of the byte after it.
  case Decoded(reply: MetaReply, next: Int)
  /// The buffer ends inside the reply.
  case Incomplete
  /// The bytes are not a memcached reply.
  case Malformed(message: String)
}

// A reply line is a status and a few flags; memcached caps a command line at
// 2048 bytes and sends nothing longer.
val maxLineBytes: Int = 8192

// memcached refuses an item size limit (`-I`) above 1 GiB.
val maxValueBytes: Long = 1073741824i64

func byteAt(buf: in slice[Byte], i: in Int): Int {
  buf[i].toInt().and(255)
}

func pushAll(acc: in List[Byte], bytes: in slice[Byte]): Unit {
  var i = 0
  while i < bytes.length {
    acc.add(bytes[i])
    i = i + 1
  }
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

// The line as text. A byte outside printable ASCII is shown as `?`: the
// statuses and flags are ASCII, and an error message is only reported.
func lineText(buf: in slice[Byte], lo: in Int, hi: in Int): String {
  var acc = ""
  var i = lo
  while i < hi {
    val b = byteAt(buf, i)
    acc = acc + (if b >= 32 and b < 127 { codepointToString(b) } else { "?" })
    i = i + 1
  }
  acc
}

func shown(line: in String): String {
  if line.length > 80 {
    Str.substring(line, 0, 80) + "..."
  } else {
    line
  }
}

// The tokens of `line` after the first `skip`, empty ones dropped.
func flagTokens(parts: in slice[String], skip: in Int): List[String] {
  val flags: List[String] = newList()
  var i = skip
  while i < parts.length {
    val token: String = parts[i]
    if token.length > 0 {
      flags.add(token)
    }
    i = i + 1
  }
  flags
}

func isDigits(text: in String): Bool {
  if text.length == 0 {
    return false
  }
  var i = 0
  while i < text.length {
    val unit = text.codeUnitAt(i)
    if unit < 48 or unit > 57 {
      return false
    }
    i = i + 1
  }
  true
}

func afterStatus(line: in String, status: in String): String {
  if line.length > status.length + 1 {
    Str.substring(line, status.length + 1)
  } else {
    ""
  }
}

func decodeValue(buf: in slice[Byte], parts: in slice[String], dataStart: in Int, line: in String): DecodeStep {
  if parts.length < 2 {
    return Malformed(message = "a VA reply without a size: " + shown(line))
  }
  val sizeText: String = parts[1]
  if not isDigits(sizeText) or sizeText.length > 10 {
    return Malformed(message = "a VA reply with an unusable size: " + shown(line))
  }
  val size = match parseOptLong(sizeText) {
    case Some(n) -> n
    case None -> return Malformed(message = "a VA reply with an unusable size: " + shown(line))
  }
  if size > maxValueBytes {
    return Malformed(message = "a VA reply announces " + sizeText + " bytes, more than memcached can store")
  }
  val dataEnd = dataStart + size.toInt()
  if buf.length < dataEnd + 2 {
    return Incomplete
  }
  if byteAt(buf, dataEnd) != 13 or byteAt(buf, dataEnd + 1) != 10 {
    return Malformed(message = "the value of a VA reply is not followed by CRLF")
  }
  Decoded(reply = Value(data = buf.slice(dataStart, dataEnd), flags = flagTokens(parts, 2)), next = dataEnd + 2)
}

/// Read one reply starting at `offset`.
@experimental
pub func decode(buf: in slice[Byte], offset: in Int): DecodeStep
  requires: offset >= 0
{
  val lineEnd = findCrlf(buf, offset)
  if lineEnd < 0 {
    if buf.length - offset > maxLineBytes {
      return Malformed(message = "a reply line is longer than " + maxLineBytes.toString() + " bytes")
    }
    return Incomplete
  }
  if lineEnd - offset > maxLineBytes {
    return Malformed(message = "a reply line is longer than " + maxLineBytes.toString() + " bytes")
  }
  val line = lineText(buf, offset, lineEnd)
  val next = lineEnd + 2
  val parts = line.split(" ")
  val status: String = if parts.length > 0 { parts[0] } else { "" }
  if status == "VA" {
    return decodeValue(buf, parts, next, line)
  }
  if status == "HD" {
    return Decoded(reply = Head(flags = flagTokens(parts, 1)), next = next)
  }
  if status == "EN" {
    return Decoded(reply = Miss(flags = flagTokens(parts, 1)), next = next)
  }
  if status == "NF" {
    return Decoded(reply = NotFound(flags = flagTokens(parts, 1)), next = next)
  }
  if status == "NS" {
    return Decoded(reply = NotStored(flags = flagTokens(parts, 1)), next = next)
  }
  if status == "EX" {
    return Decoded(reply = Exists(flags = flagTokens(parts, 1)), next = next)
  }
  if line == "MN" {
    return Decoded(reply = NoOp, next = next)
  }
  if status == "VERSION" and parts.length >= 2 {
    return Decoded(reply = Version(text = afterStatus(line, status)), next = next)
  }
  if line == "OK" {
    return Decoded(reply = Done, next = next)
  }
  if line == "STORED" {
    return Decoded(reply = Stored, next = next)
  }
  if line == "ERROR" {
    return Decoded(reply = UnknownCommand, next = next)
  }
  if status == "CLIENT_ERROR" {
    return Decoded(reply = ClientError(message = afterStatus(line, status)), next = next)
  }
  if status == "SERVER_ERROR" {
    return Decoded(reply = ServerError(message = afterStatus(line, status)), next = next)
  }
  Malformed(message = "not a memcached reply: " + shown(line))
}

/// The value of the return flag `letter` (`"t"`, `"c"`, `"O"`): the token
/// that starts with it, without the letter.
@experimental
pub func flagValue(flags: in List[String], letter: in String): Option[String]
  requires: letter.length == 1
{
  for token in flags {
    if token.startsWith(letter) {
      return Some(value = Str.substring(token, 1))
    }
  }
  None
}

/// The status of `reply` as memcached writes it, for messages.
@experimental
pub func statusOf(reply: in MetaReply): String {
  match reply {
    case Value(_, _) -> "VA"
    case Head(_) -> "HD"
    case Miss(_) -> "EN"
    case NotFound(_) -> "NF"
    case NotStored(_) -> "NS"
    case Exists(_) -> "EX"
    case NoOp -> "MN"
    case Version(_) -> "VERSION"
    case Done -> "OK"
    case Stored -> "STORED"
    case ClientError(_) -> "CLIENT_ERROR"
    case ServerError(_) -> "SERVER_ERROR"
    case UnknownCommand -> "ERROR"
  }
}

func commandLine(verb: in String, wireKey: in String, middle: in String, flags: in String): String {
  verb + " " + wireKey + middle + " b" + (if flags.length > 0 { " " + flags } else { "" }) + "\r\n"
}

/// A meta command without a data block: `<verb> <key> b <flags>`. `verb` is
/// `mg`, `md` or `ma`; `wireKey` the base64 form of the key; `flags` the
/// request flags separated by one space (`"v t"`), or `""`.
@experimental
pub func encodeCommand(verb: in String, wireKey: in String, flags: in String): slice[Byte]
  requires: verb == "mg" or verb == "md" or verb == "ma"
  requires: wireKey.length > 0
{
  encodeUtf8(commandLine(verb, wireKey, "", flags))
}

/// `ms <key> <length> b <flags>` and the data block.
@experimental
pub func encodeSet(wireKey: in String, value: in slice[Byte], flags: in String): slice[Byte]
  requires: wireKey.length > 0
{
  val acc: List[Byte] = newList()
  pushAll(acc, encodeUtf8(commandLine("ms", wireKey, " " + value.length.toString(), flags)))
  pushAll(acc, value)
  pushAll(acc, encodeUtf8("\r\n"))
  acc.toArray()
}

/// `mn`: the no-op whose reply (`MN`) ends a run of quiet commands.
@experimental
pub func encodeNoOp(): slice[Byte] {
  encodeUtf8("mn\r\n")
}

/// The classic `version` command.
@experimental
pub func encodeVersion(): slice[Byte] {
  encodeUtf8("version\r\n")
}

/// The classic `flush_all` command, which empties the whole server.
@experimental
pub func encodeFlushAll(): slice[Byte] {
  encodeUtf8("flush_all\r\n")
}

/// The first command on a connection to a server started with an
/// authentication file (`memcached -Y`): a classic `set` whose data block is
/// `<user> <password>`. Such a server ignores the key and the expiry. A
/// server WITHOUT an authentication file would store the token as an item,
/// so the expiry sent is -1: the item is expired the moment it is stored and
/// no client can read the credentials back.
@experimental
pub func encodeAuth(user: in String, password: in String): slice[Byte]
  requires: user.length > 0 and password.length > 0
{
  val token = encodeUtf8(user + " " + password)
  val acc: List[Byte] = newList()
  pushAll(acc, encodeUtf8("set auth 0 -1 " + token.length.toString() + "\r\n"))
  pushAll(acc, token)
  pushAll(acc, encodeUtf8("\r\n"))
  acc.toArray()
}

/// Several encoded commands as one request, for one write.
@experimental
pub func concatAll(parts: in List[slice[Byte]]): slice[Byte] {
  val acc: List[Byte] = newList()
  for part in parts {
    pushAll(acc, part)
  }
  acc.toArray()
}
