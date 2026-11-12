//! tests/stomp_tests.l - the STOMP 1.2 frame codec: encoding, header
//! escaping, incremental decoding, heart-beats, CRLF, bodies with and without
//! `content-length`, and malformed input.
@test_module
package Mq.ActiveMq.StompTests

import Std.Core
import Std.Collections
import Std.Testing
import Std.Encoding
import Std.String as Str
import Mq.ActiveMq.Stomp as Stomp

func bytesOf(text: in String): slice[Byte] {
  encodeUtf8(text)
}

func textOf(bytes: in slice[Byte]): String {
  match tryDecodeUtf8(bytes) {
    case Some(s) -> s
    case None -> panic("not UTF-8")
  }
}

func encoded(frame: in Stomp.Frame): String {
  match Stomp.encodeFrame(frame) {
    case Ok(bytes) -> textOf(bytes)
    case Err(e) -> panic("encode failed: " + e)
  }
}

func decodeOne(d: in Stomp.Decoder): Stomp.Frame {
  match Stomp.nextFrame(d) {
    case Ok(Some(f)) -> f
    case Ok(None) -> panic("expected a complete frame")
    case Err(e) -> panic("decode failed: " + e)
  }
}

func expectIncomplete(d: in Stomp.Decoder, label: in String): Unit {
  match Stomp.nextFrame(d) {
    case Ok(None) -> assertTrue(true, label)
    case Ok(Some(_)) -> assertTrue(false, label + ": a frame was produced")
    case Err(e) -> assertTrue(false, label + ": " + e)
  }
}

func expectDecodeError(wire: in String, label: in String): Unit {
  val d = Stomp.newDecoder(1024)
  Stomp.feed(d, bytesOf(wire))
  match Stomp.nextFrame(d) {
    case Err(e) -> assertTrue(e.length > 0, label)
    case Ok(_) -> assertTrue(false, label + ": expected an Err")
  }
}

test "a frame without a body encodes to command, headers, blank line, NUL" {
  val f = Stomp.bareFrame(
    "SUBSCRIBE",
    [Stomp.Header(name = "id", value = "s1"), Stomp.Header(name = "ack", value = "client-individual")]
  )
  assertEqual(encoded(f), "SUBSCRIBE\nid:s1\nack:client-individual\n\n\u{0000}", "wire form")
}

test "a body gets a content-length counted in UTF-8 bytes" {
  val f = Stomp.textFrame("SEND", [Stomp.Header(name = "destination", value = "/queue/a")], "café")
  assertEqual(encoded(f), "SEND\ndestination:/queue/a\ncontent-length:5\n\ncafé\u{0000}", "wire form")
}

test "a caller's content-length header is replaced by the real one" {
  val f = Stomp.textFrame("SEND", [Stomp.Header(name = "content-length", value = "999")], "ab")
  assertEqual(encoded(f), "SEND\ncontent-length:2\n\nab\u{0000}", "wire form")
}

test "header names and values are escaped" {
  assertEqual(Stomp.escapeHeader("a:b\nc\rd\\e"), "a\\cb\\nc\\rd\\\\e", "escape")
  val f = Stomp.bareFrame("SEND", [Stomp.Header(name = "k:1", value = "line1\nline2\\")])
  assertEqual(encoded(f), "SEND\nk\\c1:line1\\nline2\\\\\n\n\u{0000}", "wire form")
}

test "CONNECT headers are not escaped" {
  val f = Stomp.bareFrame("CONNECT", [Stomp.Header(name = "passcode", value = "a:b\\c")])
  assertEqual(encoded(f), "CONNECT\npasscode:a:b\\c\n\n\u{0000}", "wire form")
}

test "headers that cannot be represented are rejected" {
  val nul = Stomp.bareFrame("SEND", [Stomp.Header(name = "k", value = "a\u{0000}b")])
  assertTrue(isErr(Stomp.encodeFrame(nul)), "a NUL in a header value")
  val lineEnd = Stomp.bareFrame("CONNECT", [Stomp.Header(name = "login", value = "a\nb")])
  assertTrue(isErr(Stomp.encodeFrame(lineEnd)), "a line end in a CONNECT header")
  val colon = Stomp.bareFrame("CONNECT", [Stomp.Header(name = "a:b", value = "v")])
  assertTrue(isErr(Stomp.encodeFrame(colon)), "a colon in a CONNECT header name")
  val unnamed = Stomp.bareFrame("SEND", [Stomp.Header(name = "", value = "v")])
  assertTrue(isErr(Stomp.encodeFrame(unnamed)), "an empty header name")
  val noCommand = Stomp.bareFrame("", [])
  assertTrue(isErr(Stomp.encodeFrame(noCommand)), "an empty command")
}

test "unescape reverses escape and rejects undefined escapes" {
  match Stomp.unescapeHeader("a\\cb\\nc\\rd\\\\e") {
    case Ok(s) -> assertEqual(s, "a:b\nc\rd\\e", "unescape")
    case Err(e) -> assertTrue(false, e)
  }
  assertTrue(isErr(Stomp.unescapeHeader("bad\\t")), "\\t is not a STOMP escape")
  assertTrue(isErr(Stomp.unescapeHeader("dangling\\")), "a trailing backslash")
}

test "an encoded frame decodes to the same frame" {
  val body = "line1\nline2\t\"quoted\" \\ back\u{0001}ctl café 世 \u{0000} after-nul"
  val sent = Stomp.textFrame(
    "SEND",
    [Stomp.Header(name = "destination", value = "/queue/a:b"), Stomp.Header(name = "k\"1", value = "v\n1 é")],
    body
  )
  val d = Stomp.newDecoder(1024)
  match Stomp.encodeFrame(sent) {
    case Ok(bytes) -> Stomp.feed(d, bytes)
    case Err(e) -> assertTrue(false, e)
  }
  val got = decodeOne(d)
  assertEqual(got.command, "SEND", "command")
  assertEqualInt(got.headers.length, 3, "header count (content-length added)")
  assertEqual(got.headers[0].value, "/queue/a:b", "destination")
  assertEqual(got.headers[1].name, "k\"1", "header name")
  assertEqual(got.headers[1].value, "v\n1 é", "header value")
  match Stomp.bodyText(got) {
    case Some(s) -> assertEqual(s, body, "body")
    case None -> assertTrue(false, "body was not UTF-8")
  }
  expectIncomplete(d, "nothing is left")
  assertEqualInt(Stomp.buffered(d), 0, "the buffer is drained")
}

test "a frame arriving one byte at a time decodes once it is complete" {
  val wire = bytesOf("MESSAGE\nmessage-id:1\ncontent-length:5\n\nhe\u{0000}lo\u{0000}")
  val d = Stomp.newDecoder(1024)
  var i = 0
  while i < wire.length - 1 {
    Stomp.feed(d, wire.slice(i, i + 1))
    expectIncomplete(d, "incomplete after byte " + i.toString())
    i += 1
  }
  Stomp.feed(d, wire.slice(wire.length - 1, wire.length))
  val f = decodeOne(d)
  assertEqual(f.command, "MESSAGE", "command")
  assertEqualInt(f.body.length, 5, "the body keeps its NUL byte")
}

test "a body without content-length ends at the first NUL, across feeds" {
  val d = Stomp.newDecoder(1024)
  Stomp.feed(d, bytesOf("MESSAGE\nmessage-id:1\n\nhel"))
  expectIncomplete(d, "body not terminated yet")
  Stomp.feed(d, bytesOf("lo"))
  expectIncomplete(d, "still not terminated")
  Stomp.feed(d, bytesOf("\u{0000}"))
  val f = decodeOne(d)
  assertEqual(textOf(f.body), "hello", "body")
}

test "several frames, heart-beats and CRLF line ends in one feed" {
  val d = Stomp.newDecoder(1024)
  Stomp.feed(d, bytesOf("\n\r\nRECEIPT\r\nreceipt-id:r1\r\n\r\n\u{0000}\n\nMESSAGE\nack:a\\c1\n\nx\u{0000}\n"))
  val first = decodeOne(d)
  assertEqual(first.command, "RECEIPT", "first command")
  assertEqual(Stomp.headerOr(first, "receipt-id", ""), "r1", "CRLF is stripped from a header value")
  val second = decodeOne(d)
  assertEqual(second.command, "MESSAGE", "second command")
  assertEqual(Stomp.headerOr(second, "ack", ""), "a:1", "the ack header is unescaped")
  assertEqual(textOf(second.body), "x", "second body")
  expectIncomplete(d, "only a heart-beat is left")
  assertEqualInt(Stomp.buffered(d), 0, "the trailing line end was consumed")
}

test "the first of repeated headers wins" {
  val d = Stomp.newDecoder(1024)
  Stomp.feed(d, bytesOf("MESSAGE\nfoo:first\nfoo:second\n\n\u{0000}"))
  val f = decodeOne(d)
  assertEqual(Stomp.headerOr(f, "foo", ""), "first", "first occurrence")
  assertEqualInt(f.headers.length, 2, "both are kept in the list")
  assertTrue(not isSome(Stomp.headerValue(f, "bar")), "an absent header is None")
  assertEqual(Stomp.headerOr(f, "bar", "fallback"), "fallback", "headerOr falls back")
}

test "CONNECTED headers are taken literally" {
  val d = Stomp.newDecoder(1024)
  Stomp.feed(d, bytesOf("CONNECTED\nserver:a\\cb\nversion:1.2\n\n\u{0000}"))
  val f = decodeOne(d)
  assertEqual(Stomp.headerOr(f, "server", ""), "a\\cb", "no unescaping in CONNECTED")
}

test "a large frame split into many chunks is reassembled" {
  val sb: List[String] = newList()
  var n = 0
  while n < 2000 {
    sb.add("0123456789")
    n += 1
  }
  val body = Str.joinList("", sb)
  val wire = match Stomp.encodeFrame(Stomp.textFrame("SEND", [], body)) { case Ok(b) -> b; case Err(e) -> panic(e) }
  val d = Stomp.newDecoder(1048576)
  var at = 0
  while at < wire.length {
    val stop = if at + 777 < wire.length { at + 777 } else { wire.length }
    Stomp.feed(d, wire.slice(at, stop))
    at = stop
  }
  val f = decodeOne(d)
  assertEqualInt(f.body.length, 20000, "body length")
  assertEqual(textOf(f.body), body, "body content")
}

test "malformed frames are errors" {
  expectDecodeError("MESSAGE\nno-colon-here\n\n\u{0000}", "a header line without a colon")
  expectDecodeError("MESSAGE\nk:bad\\t\n\n\u{0000}", "an undefined escape")
  expectDecodeError("MESSAGE\ncontent-length:abc\n\n\u{0000}", "a non-numeric content-length")
  expectDecodeError("MESSAGE\ncontent-length:-1\n\n\u{0000}", "a negative content-length")
  expectDecodeError("MESSAGE\ncontent-length:2\n\nabc\u{0000}", "a body longer than its content-length")
  expectDecodeError("MESSAGE\ncontent-length:5000\n\n", "a content-length over the frame limit")
}

test "an unterminated body over the frame limit is an error" {
  val d = Stomp.newDecoder(16)
  Stomp.feed(d, bytesOf("MESSAGE\n\n0123456789012345678901234567890123456789"))
  assertTrue(isErr(Stomp.nextFrame(d)), "body exceeds the limit")
}

test "a header section over the limit is an error" {
  val parts: List[String] = newList()
  parts.add("MESSAGE\n")
  var n = 0
  while n < 7000 {
    parts.add("k:0123456789\n")
    n += 1
  }
  val d = Stomp.newDecoder(1024)
  Stomp.feed(d, bytesOf(Str.joinList("", parts)))
  assertTrue(isErr(Stomp.nextFrame(d)), "headers exceed the limit")
}
