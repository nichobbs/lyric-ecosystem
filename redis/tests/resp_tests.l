//! tests/resp_tests.l - the RESP encoder and decoder, with no I/O.
@test_module
package Redis.RespTests

import Std.Core
import Std.Collections
import Std.Encoding
import Std.Testing
import Redis.Resp

func bytes(text: in String): slice[Byte] {
  encodeUtf8(text)
}

func textOf(b: in slice[Byte]): String {
  match tryDecodeUtf8(b) {
    case Some(s) -> s
    case None -> "<binary>"
  }
}

func showList(items: in List[RespValue]): String {
  var acc = ""
  for item in items {
    if acc.length > 0 {
      acc = acc + ","
    }
    acc = acc + show(item)
  }
  acc
}

// A canonical rendering, so a whole reply compares as one string.
func show(v: in RespValue): String {
  match v {
    case RSimple(t) -> "simple(" + t + ")"
    case RError(m) -> "error(" + m + ")"
    case RInt(n) -> "int(" + n.toString() + ")"
    case RBulk(b) -> "bulk(" + textOf(b) + ")"
    case RNull -> "null"
    case RArray(items) -> "array[" + showList(items) + "]"
    case RDouble(t) -> "double(" + t + ")"
    case RBool(b) -> if b { "bool(true)" } else { "bool(false)" }
    case RBigNumber(t) -> "big(" + t + ")"
    case RVerbatim(f, b) -> "verbatim(" + f + ":" + textOf(b) + ")"
    case RMap(entries) -> "map[" + showList(entries) + "]"
    case RSet(items) -> "set[" + showList(items) + "]"
    case RPush(items) -> "push[" + showList(items) + "]"
  }
}

// The rendering of the single reply `wire` holds, or what went wrong.
func decodeAll(wire: in String): String {
  val buf = bytes(wire)
  match decode(buf, 0) {
    case Decoded(v, next) -> if next == buf.length { show(v) } else { "trailing bytes after " + show(v) }
    case Incomplete -> "incomplete"
    case Malformed(m) -> "malformed: " + m
  }
}

func isMalformed(wire: in String): Bool {
  match decode(bytes(wire), 0) {
    case Malformed(_) -> true
    case _ -> false
  }
}

// ─── Encoder ─────────────────────────────────────────────────────────────────
test "a command is an array of bulk strings" {
  assertEqual(textOf(encodeCommand(["GET", "key"])), "*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n", "GET key")
  assertEqual(textOf(encodeCommand(["PING"])), "*1\r\n$4\r\nPING\r\n", "PING")
}

test "an empty argument and an argument holding CRLF are length-prefixed" {
  assertEqual(textOf(encodeCommand(["SET", "k", ""])), "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$0\r\n\r\n", "empty value")
  assertEqual(
    textOf(encodeCommand(["SET", "k", "a\r\nb"])),
    "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$4\r\na\r\nb\r\n",
    "CRLF inside a value"
  )
}

test "bulk lengths count UTF-8 bytes, not characters" {
  assertEqual(textOf(encodeCommand(["ECHO", "café"])), "*2\r\n$4\r\nECHO\r\n$5\r\ncafé\r\n", "five bytes")
}

test "binary arguments are sent verbatim" {
  val payload: slice[Byte] = [0.toByte(), 255.toByte(), 13.toByte(), 10.toByte()]
  val args: slice[slice[Byte]] = [bytes("SET"), bytes("k"), payload]
  val wire = encodeCommandBytes(args)
  val head = bytes("*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$4\r\n")
  assertEqualInt(wire.length, head.length + 4 + 2, "total length")
  assertEqualInt(wire[head.length].toInt().and(255), 0, "first payload byte")
  assertEqualInt(wire[head.length + 1].toInt().and(255), 255, "second payload byte")
  assertEqualInt(wire[head.length + 2].toInt().and(255), 13, "third payload byte")
  assertEqualInt(wire[wire.length - 2].toInt().and(255), 13, "trailing CR")
  assertEqualInt(wire[wire.length - 1].toInt().and(255), 10, "trailing LF")
}

// ─── RESP2 decoding ──────────────────────────────────────────────────────────
test "RESP2 scalar replies" {
  assertEqual(decodeAll("+OK\r\n"), "simple(OK)", "status")
  assertEqual(decodeAll("-ERR unknown command\r\n"), "error(ERR unknown command)", "error")
  assertEqual(decodeAll(":42\r\n"), "int(42)", "integer")
  assertEqual(decodeAll(":-7\r\n"), "int(-7)", "negative integer")
  assertEqual(decodeAll(":0\r\n"), "int(0)", "zero")
  assertEqual(decodeAll("$5\r\nhello\r\n"), "bulk(hello)", "bulk")
  assertEqual(decodeAll("$0\r\n\r\n"), "bulk()", "empty bulk")
  assertEqual(decodeAll("$-1\r\n"), "null", "null bulk")
  assertEqual(decodeAll("*-1\r\n"), "null", "null array")
}

test "integer replies cover the whole Long range and reject overflow" {
  assertEqual(decodeAll(":9223372036854775807\r\n"), "int(9223372036854775807)", "Long max")
  assertEqual(decodeAll(":-9223372036854775808\r\n"), "int(-9223372036854775808)", "Long min")
  assertTrue(isMalformed(":9223372036854775808\r\n"), "one past Long max")
  assertTrue(isMalformed(":-9223372036854775809\r\n"), "one past Long min")
  assertTrue(isMalformed(":12a\r\n"), "a letter in an integer")
  assertTrue(isMalformed(":\r\n"), "an empty integer")
  assertTrue(isMalformed(":-\r\n"), "a lone sign")
}

test "a bulk string may hold CRLF and is cut by its length" {
  assertEqual(decodeAll("$4\r\na\r\nb\r\n"), "bulk(a\r\nb)", "CRLF inside the payload")
}

test "arrays nest and may be empty" {
  assertEqual(decodeAll("*0\r\n"), "array[]", "empty array")
  assertEqual(
    decodeAll("*3\r\n:1\r\n$1\r\na\r\n*2\r\n+x\r\n$-1\r\n"),
    "array[int(1),bulk(a),array[simple(x),null]]",
    "nested array"
  )
}

test "a SCAN reply decodes as cursor plus key array" {
  assertEqual(
    decodeAll("*2\r\n$1\r\n0\r\n*2\r\n$2\r\nk1\r\n$2\r\nk2\r\n"),
    "array[bulk(0),array[bulk(k1),bulk(k2)]]",
    "SCAN"
  )
}

// ─── RESP3 decoding ──────────────────────────────────────────────────────────
test "RESP3 scalar replies" {
  assertEqual(decodeAll("_\r\n"), "null", "null")
  assertEqual(decodeAll("#t\r\n"), "bool(true)", "true")
  assertEqual(decodeAll("#f\r\n"), "bool(false)", "false")
  assertEqual(decodeAll(",3.14\r\n"), "double(3.14)", "double")
  assertEqual(decodeAll(",-inf\r\n"), "double(-inf)", "negative infinity")
  assertEqual(
    decodeAll("(3492890328409238509324850943850943825024385\r\n"),
    "big(3492890328409238509324850943850943825024385)",
    "big number"
  )
  assertEqual(decodeAll("!21\r\nSYNTAX invalid syntax\r\n"), "error(SYNTAX invalid syntax)", "blob error")
  assertEqual(decodeAll("=15\r\ntxt:Some string\r\n"), "verbatim(txt:Some string)", "verbatim string")
}

test "RESP3 aggregates" {
  assertEqual(
    decodeAll("%2\r\n+first\r\n:1\r\n+second\r\n:2\r\n"),
    "map[simple(first),int(1),simple(second),int(2)]",
    "map"
  )
  assertEqual(decodeAll("~2\r\n+a\r\n+b\r\n"), "set[simple(a),simple(b)]", "set")
  assertEqual(
    decodeAll(">3\r\n$7\r\nmessage\r\n$2\r\nch\r\n$2\r\nhi\r\n"),
    "push[bulk(message),bulk(ch),bulk(hi)]",
    "push"
  )
  assertEqual(decodeAll("%0\r\n"), "map[]", "empty map")
}

test "a RESP3 attribute is dropped and the reply after it is returned" {
  assertEqual(
    decodeAll("|1\r\n+key-popularity\r\n%1\r\n$1\r\na\r\n,0.1923\r\n*2\r\n:2039123\r\n:9543892\r\n"),
    "array[int(2039123),int(9543892)]",
    "attribute before an array"
  )
}

test "a HELLO 3 reply decodes as a map" {
  assertEqual(
    decodeAll("%3\r\n$6\r\nserver\r\n$5\r\nredis\r\n$5\r\nproto\r\n:3\r\n$7\r\nmodules\r\n*0\r\n"),
    "map[bulk(server),bulk(redis),bulk(proto),int(3),bulk(modules),array[]]",
    "HELLO"
  )
}

// ─── Incomplete and malformed input ──────────────────────────────────────────
test "every strict prefix of a reply is incomplete" {
  val wire = "*3\r\n:1\r\n$5\r\nhello\r\n%1\r\n+k\r\n#t\r\n"
  val whole = bytes(wire)
  var cut = 0
  while cut < whole.length {
    match decode(whole.slice(0, cut), 0) {
      case Incomplete -> ()
      case Decoded(_, _) -> assertTrue(false, "decoded a reply from only " + cut.toString() + " bytes")
      case Malformed(m) -> assertTrue(false, "prefix of " + cut.toString() + " bytes was malformed: " + m)
    }
    cut = cut + 1
  }
  assertEqual(decodeAll(wire), "array[int(1),bulk(hello),map[simple(k),bool(true)]]", "the whole reply")
}

test "decode reads from an offset and reports where the reply ends" {
  val buf = bytes("+OK\r\n:5\r\n")
  match decode(buf, 0) {
    case Decoded(v, next) -> {
      assertEqual(show(v), "simple(OK)", "first reply")
      assertEqualInt(next, 5, "offset after the first reply")
      match decode(buf, next) {
        case Decoded(v2, next2) -> {
          assertEqual(show(v2), "int(5)", "second reply")
          assertEqualInt(next2, buf.length, "offset after the second reply")
        }
        case _ -> assertTrue(false, "the second reply must decode")
      }
    }
    case _ -> assertTrue(false, "the first reply must decode")
  }
}

test "malformed input is reported, not guessed at" {
  assertTrue(isMalformed("hello\r\n"), "an unknown type byte")
  assertTrue(isMalformed("$abc\r\n"), "a non-numeric bulk length")
  assertTrue(isMalformed("$-2\r\n"), "a negative bulk length other than -1")
  assertTrue(isMalformed("$3\r\nabcde\r\n"), "a bulk payload not followed by CRLF")
  assertTrue(isMalformed("*x\r\n"), "a non-numeric array length")
  assertTrue(isMalformed("*-2\r\n"), "a negative array length other than -1")
  assertTrue(isMalformed("#x\r\n"), "an invalid boolean")
  assertTrue(isMalformed("_x\r\n"), "a null with a payload")
  assertTrue(isMalformed("=3\r\nabc\r\n"), "a verbatim string without a format")
  assertTrue(isMalformed("$?\r\n"), "a streamed string")
  assertTrue(isMalformed("$999999999999\r\n"), "a bulk length over the protocol limit")
  assertTrue(isMalformed("*2\r\n:1\r\nbad\r\n"), "a malformed element inside an array")
}

test "nesting deeper than the limit is refused" {
  var wire = ""
  var i = 0
  while i < 200 {
    wire = wire + "*1\r\n"
    i = i + 1
  }
  wire = wire + ":1\r\n"
  assertTrue(isMalformed(wire), "200 levels of nesting")
}

// ─── Incremental decoder ─────────────────────────────────────────────────────
test "feeding one byte at a time yields the same replies" {
  val wire = bytes("+OK\r\n$5\r\nhello\r\n*2\r\n:1\r\n$-1\r\n_\r\n%1\r\n+k\r\n,1.5\r\n")
  var decoder = newDecoder()
  var seen = ""
  var i = 0
  while i < wire.length {
    val r = feed(decoder, wire.slice(i, i + 1))
    decoder = r.decoder
    match r.error {
      case Some(m) -> assertTrue(false, "unexpected protocol error: " + m)
      case None -> ()
    }
    for v in r.values {
      seen = seen + show(v) + ";"
    }
    i = i + 1
  }
  assertEqual(seen, "simple(OK);bulk(hello);array[int(1),null];null;map[simple(k),double(1.5)];", "replies in order")
  assertEqualInt(decoder.pending.length, 0, "nothing left buffered")
}

test "one feed may complete several replies and keep a partial one" {
  val r = feed(newDecoder(), bytes(":1\r\n:2\r\n$5\r\nhel"))
  assertEqualInt(r.values.count, 2, "two complete replies")
  assertEqualInt(r.decoder.pending.length, 7, "the partial bulk stays buffered")
  val r2 = feed(r.decoder, bytes("lo\r\n"))
  assertEqualInt(r2.values.count, 1, "the bulk completes")
  assertEqual(show(r2.values[0]), "bulk(hello)", "the completed bulk")
}

test "a protocol error stops the decoder and keeps the replies before it" {
  val r = feed(newDecoder(), bytes("+OK\r\n?bad\r\n+never\r\n"))
  assertEqualInt(r.values.count, 1, "the reply before the error")
  assertTrue(r.decoder.failed, "the decoder is failed")
  match r.error {
    case Some(_) -> ()
    case None -> assertTrue(false, "the error must be reported")
  }
}

// ─── Helpers ─────────────────────────────────────────────────────────────────
test "asText reads string-like replies only" {
  match asText(RSimple(text = "OK")) {
    case Some(t) -> assertEqual(t, "OK", "status")
    case None -> assertTrue(false, "a status is text")
  }
  match asText(RBulk(bytes = bytes("v"))) {
    case Some(t) -> assertEqual(t, "v", "bulk")
    case None -> assertTrue(false, "a bulk string is text")
  }
  match asText(RInt(value = 1i64)) {
    case Some(_) -> assertTrue(false, "an integer is not text")
    case None -> ()
  }
  val invalid: slice[Byte] = [255.toByte(), 254.toByte()]
  match asText(RBulk(bytes = invalid)) {
    case Some(_) -> assertTrue(false, "invalid UTF-8 is not text")
    case None -> ()
  }
}
