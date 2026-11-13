//! tests/meta_tests.l - the meta protocol codec: what each command looks like
//! on the wire, every reply memcached sends, replies split at any byte, and
//! bytes that are not a reply.
@test_module
package Cache.Memcached.MetaTests

import Std.Core
import Std.Collections
import Std.Encoding
import Std.String as Str
import Std.Testing
import Cache.Memcached.Meta

func wire(text: in String): slice[Byte] {
  encodeUtf8(text)
}

func textOf(bytes: in slice[Byte]): String {
  match tryDecodeUtf8(bytes) {
    case Some(s) -> s
    case None -> "<not UTF-8>"
  }
}

// The reply at the start of `text`, shown as `STATUS[data]{flags} next=N`.
func show(text: in String): String {
  match decode(wire(text), 0) {
    case Incomplete -> "incomplete"
    case Malformed(m) -> "malformed: " + m
    case Decoded(reply, next) -> describe(reply) + " next=" + next.toString()
  }
}

func describe(reply: in MetaReply): String {
  match reply {
    case Value(data, flags) -> "VA[" + textOf(data) + "]{" + Str.joinList(",", flags) + "}"
    case Head(flags) -> "HD{" + Str.joinList(",", flags) + "}"
    case Miss(flags) -> "EN{" + Str.joinList(",", flags) + "}"
    case NotFound(flags) -> "NF{" + Str.joinList(",", flags) + "}"
    case NotStored(flags) -> "NS{" + Str.joinList(",", flags) + "}"
    case Exists(flags) -> "EX{" + Str.joinList(",", flags) + "}"
    case NoOp -> "MN"
    case Version(text) -> "VERSION[" + text + "]"
    case Done -> "OK"
    case Stored -> "STORED"
    case ClientError(m) -> "CLIENT_ERROR[" + m + "]"
    case ServerError(m) -> "SERVER_ERROR[" + m + "]"
    case UnknownCommand -> "ERROR"
  }
}

func isMalformed(text: in String): Bool {
  match decode(wire(text), 0) {
    case Malformed(_) -> true
    case _ -> false
  }
}

// ─── Encoding ────────────────────────────────────────────────────────────────
test "a command carries the base64 key, the b flag and its flags" {
  assertEqual(textOf(encodeCommand("mg", "Zm9v", "v t")), "mg Zm9v b v t\r\n", "mg with flags")
  assertEqual(textOf(encodeCommand("md", "Zm9v", "")), "md Zm9v b\r\n", "md without flags")
  assertEqual(textOf(encodeCommand("ma", "Zm9v", "v")), "ma Zm9v b v\r\n", "ma")
}

test "a set announces the length of its data block in bytes" {
  assertEqual(textOf(encodeSet("Zm9v", wire("bar"), "T60")), "ms Zm9v 3 b T60\r\nbar\r\n", "ms with a TTL")
  assertEqual(textOf(encodeSet("Zm9v", wire(""), "")), "ms Zm9v 0 b\r\n\r\n", "an empty value")
  assertEqual(textOf(encodeSet("Zm9v", wire("é世"), "ME T0")), "ms Zm9v 5 b ME T0\r\né世\r\n", "bytes, not characters")
  assertEqual(
    textOf(encodeSet("Zm9v", wire("a\r\nmn\r\nb"), "")),
    "ms Zm9v 8 b\r\na\r\nmn\r\nb\r\n",
    "a value with CRLF is carried by its length"
  )
}

test "the commands without a key" {
  assertEqual(textOf(encodeNoOp()), "mn\r\n", "mn")
  assertEqual(textOf(encodeVersion()), "version\r\n", "version")
  assertEqual(textOf(encodeFlushAll()), "flush_all\r\n", "flush_all")
  assertEqual(
    textOf(encodeAuth("app", "s3cret")),
    "set auth 0 -1 10\r\napp s3cret\r\n",
    "the authentication set, born expired"
  )
}

test "commands are joined into one request" {
  val parts: List[slice[Byte]] = newList()
  parts.add(encodeCommand("mg", "YQ==", "v"))
  parts.add(encodeSet("Yg==", wire("x"), "T0"))
  parts.add(encodeNoOp())
  assertEqual(textOf(concatAll(parts)), "mg YQ== b v\r\nms Yg== 1 b T0\r\nx\r\nmn\r\n", "three commands, one write")
}

test "a verb that is not a meta command is a contract violation" {
  assertPanics("get", { -> val _ = encodeCommand("get", "Zm9v", "") })
  assertPanics("ms without data", { -> val _ = encodeCommand("ms", "Zm9v", "") })
  assertPanics("an empty key", { -> val _ = encodeCommand("mg", "", "") })
}

// ─── Decoding ────────────────────────────────────────────────────────────────
test "a value reply carries the data and the flags" {
  assertEqual(show("VA 3\r\nbar\r\n"), "VA[bar]{} next=11", "no flags")
  assertEqual(show("VA 3 t-1 f0 c6 kZm9v b s3 O77\r\nbar\r\n"), "VA[bar]{t-1,f0,c6,kZm9v,b,s3,O77} next=36", "flags")
  assertEqual(show("VA 0\r\n\r\n"), "VA[]{} next=8", "an empty value")
  assertEqual(show("VA 5 c9\r\né世\r\nMN\r\n"), "VA[é世]{c9} next=16", "the size is in bytes; what follows is left")
  assertEqual(show("VA 6\r\na\r\nb\r\n\r\n"), "VA[a\r\nb\r\n]{} next=14", "CRLF inside the data")
}

test "the statuses without a value" {
  assertEqual(show("HD\r\n"), "HD{} next=4", "HD")
  assertEqual(show("HD t2592000 c10\r\n"), "HD{t2592000,c10} next=17", "HD with flags")
  assertEqual(show("EN\r\n"), "EN{} next=4", "EN")
  assertEqual(show("EN O1 kbm9wZQ== b\r\n"), "EN{O1,kbm9wZQ==,b} next=19", "EN with flags")
  assertEqual(show("NF\r\n"), "NF{} next=4", "NF")
  assertEqual(show("NS\r\n"), "NS{} next=4", "NS")
  assertEqual(show("EX\r\n"), "EX{} next=4", "EX")
  assertEqual(show("MN\r\n"), "MN next=4", "MN")
}

test "the replies of the classic commands" {
  assertEqual(show("VERSION 1.6.24\r\n"), "VERSION[1.6.24] next=16", "version")
  assertEqual(show("VERSION 1.6.24 extra words\r\n"), "VERSION[1.6.24 extra words] next=28", "version with more text")
  assertEqual(show("OK\r\n"), "OK next=4", "flush_all")
  assertEqual(show("STORED\r\n"), "STORED next=8", "the authentication set")
}

test "the error replies" {
  assertEqual(
    show("CLIENT_ERROR bad command line format\r\nERROR\r\n"),
    "CLIENT_ERROR[bad command line format] next=38",
    "CLIENT_ERROR"
  )
  assertEqual(show("CLIENT_ERROR\r\n"), "CLIENT_ERROR[] next=14", "CLIENT_ERROR without a message")
  assertEqual(
    show("SERVER_ERROR object too large for cache\r\n"),
    "SERVER_ERROR[object too large for cache] next=41",
    "SERVER_ERROR"
  )
  assertEqual(show("ERROR\r\n"), "ERROR next=7", "ERROR")
}

test "decoding starts at the offset" {
  val buf = wire("HD\r\nVA 1\r\nx\r\nMN\r\n")
  match decode(buf, 4) {
    case Decoded(reply, next) -> {
      assertEqual(describe(reply), "VA[x]{}", "the second reply")
      assertEqualInt(next, 13, "the offset of the third")
    }
    case _ -> assertTrue(false, "the second reply is whole")
  }
  match decode(buf, 17) {
    case Incomplete -> ()
    case _ -> assertTrue(false, "nothing is left at the end of the buffer")
  }
}

test "a reply cut at any byte is incomplete, never wrong" {
  val whole = "VA 5 t-1 c3\r\nhello\r\n"
  var cut = 0
  while cut < whole.length {
    assertEqual(show(Str.substring(whole, 0, cut)), "incomplete", "cut after " + cut.toString() + " bytes")
    cut = cut + 1
  }
  assertEqual(show(whole), "VA[hello]{t-1,c3} next=20", "the whole reply")
  assertEqual(show("H"), "incomplete", "one byte of a status")
  assertEqual(show("HD\r"), "incomplete", "a CR without its LF")
  assertEqual(show(""), "incomplete", "nothing")
}

test "flags are found by their letter" {
  val flags: List[String] = newList()
  flags.add("t-1")
  flags.add("c42")
  flags.add("O7")
  match flagValue(flags, "c") {
    case Some(v) -> assertEqual(v, "42", "the CAS value")
    case None -> assertTrue(false, "c is present")
  }
  match flagValue(flags, "t") {
    case Some(v) -> assertEqual(v, "-1", "the lifetime")
    case None -> assertTrue(false, "t is present")
  }
  match flagValue(flags, "s") {
    case Some(_) -> assertTrue(false, "s is absent")
    case None -> ()
  }
  assertPanics("a flag is one letter", { -> val _ = flagValue(flags, "ct") })
}

test "the status of a reply is what memcached wrote" {
  val none: List[String] = newList()
  assertEqual(statusOf(Head(flags = none)), "HD", "HD")
  assertEqual(statusOf(Miss(flags = none)), "EN", "EN")
  assertEqual(statusOf(NotFound(flags = none)), "NF", "NF")
  assertEqual(statusOf(NotStored(flags = none)), "NS", "NS")
  assertEqual(statusOf(Exists(flags = none)), "EX", "EX")
  assertEqual(statusOf(NoOp), "MN", "MN")
  assertEqual(statusOf(Done), "OK", "OK")
  assertEqual(statusOf(Stored), "STORED", "STORED")
  assertEqual(statusOf(UnknownCommand), "ERROR", "ERROR")
  assertEqual(statusOf(ClientError(message = "x")), "CLIENT_ERROR", "CLIENT_ERROR")
  assertEqual(statusOf(ServerError(message = "x")), "SERVER_ERROR", "SERVER_ERROR")
  assertEqual(statusOf(Version(text = "1.6.24")), "VERSION", "VERSION")
  assertEqual(statusOf(Value(data = wire("x"), flags = none)), "VA", "VA")
}

// ─── Malformed replies ───────────────────────────────────────────────────────
test "bytes that are not a reply are malformed" {
  assertTrue(isMalformed("+OK\r\n"), "a RESP status")
  assertTrue(isMalformed("HTTP/1.1 400 Bad Request\r\n"), "an HTTP response")
  assertTrue(isMalformed("\r\n"), "an empty line")
  assertTrue(isMalformed("hd\r\n"), "a status in lower case")
  assertTrue(isMalformed("MN extra\r\n"), "MN with something after it")
  assertTrue(isMalformed("OK now\r\n"), "OK with something after it")
  assertTrue(isMalformed("VERSION\r\n"), "VERSION without a version")
  assertTrue(isMalformed("VALUE foo 0 3\r\nbar\r\nEND\r\n"), "a reply of the classic get")
}

test "a value reply with a bad size or a bad end is malformed" {
  assertTrue(isMalformed("VA\r\n"), "no size")
  assertTrue(isMalformed("VA x\r\nbar\r\n"), "a size that is not a number")
  assertTrue(isMalformed("VA -1\r\n"), "a negative size")
  assertTrue(isMalformed("VA 3.5\r\nbar\r\n"), "a fractional size")
  assertTrue(isMalformed("VA 99999999999\r\n"), "a size with too many digits")
  assertTrue(isMalformed("VA 2000000000\r\n"), "a size above what memcached stores")
  assertTrue(isMalformed("VA 3\r\nbarXY"), "data not followed by CRLF")
  assertTrue(isMalformed("VA 2\r\nbar\r\n"), "data longer than announced")
  assertEqual(show("VA 4\r\nbar\r\n"), "incomplete", "data shorter than announced waits for more")
}

test "an endless line is malformed instead of buffered forever" {
  assertEqual(show(Str.repeat("x", 8192)), "incomplete", "a line may still end")
  assertTrue(isMalformed(Str.repeat("x", 8193)), "more than the longest line without CRLF")
  assertTrue(isMalformed("HD " + Str.repeat("x", 9000) + "\r\n"), "a line that is too long with CRLF")
}

test "a byte outside ASCII in a status line is shown, not trusted" {
  val bytes: List[Byte] = newList()
  for b in wire("SERVER_ERROR caf") {
    bytes.add(b)
  }
  bytes.add(233.toByte())
  for b in wire("\r\n") {
    bytes.add(b)
  }
  match decode(bytes.toArray(), 0) {
    case Decoded(reply, _) -> assertEqual(describe(reply), "SERVER_ERROR[caf?]", "the stray byte becomes ?")
    case _ -> assertTrue(false, "the line is a reply")
  }
}
