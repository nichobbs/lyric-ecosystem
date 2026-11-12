//! tests/url_tests.l - broker URL parsing.
@test_module
package Mq.ActiveMq.UrlTests

import Std.Core
import Std.Testing
import Std.String as Str
import Mq.ActiveMq.Url as Url

func parsed(url: in String): Url.BrokerAddress {
  match Url.parseUrl(url) {
    case Ok(a) -> a
    case Err(e) -> panic("parse failed: " + e)
  }
}

func rejected(url: in String): String {
  match Url.parseUrl(url) {
    case Ok(_) -> panic("expected '" + url + "' to be rejected")
    case Err(e) -> e
  }
}

test "host only takes the default port and no credentials" {
  val a = parsed("stomp://broker.example")
  assertEqual(a.host, "broker.example", "host")
  assertEqualInt(a.port, 61613, "default port")
  assertEqual(a.login, "", "login")
  assertEqual(a.passcode, "", "passcode")
}

test "host, port, login and passcode" {
  val a = parsed("stomp://lyric:secret@127.0.0.1:61699")
  assertEqual(a.host, "127.0.0.1", "host")
  assertEqualInt(a.port, 61699, "port")
  assertEqual(a.login, "lyric", "login")
  assertEqual(a.passcode, "secret", "passcode")
}

test "a trailing slash and an upper-case scheme are accepted" {
  val a = parsed("STOMP://h:1/")
  assertEqual(a.host, "h", "host")
  assertEqualInt(a.port, 1, "port")
}

test "credentials are percent-decoded" {
  val a = parsed("stomp://us%40er:p%3Ass%2Fw%40rd%20caf%C3%A9@host:2")
  assertEqual(a.login, "us@er", "login")
  assertEqual(a.passcode, "p:ss/w@rd café", "passcode")
}

test "a login without a passcode" {
  val a = parsed("stomp://solo@host")
  assertEqual(a.login, "solo", "login")
  assertEqual(a.passcode, "", "passcode")
}

test "an IPv6 literal in brackets" {
  val a = parsed("stomp://u:p@[::1]:61700")
  assertEqual(a.host, "::1", "host")
  assertEqualInt(a.port, 61700, "port")
  val b = parsed("stomp://[fe80::1]")
  assertEqual(b.host, "fe80::1", "host without a port")
  assertEqualInt(b.port, 61613, "default port")
}

test "malformed URLs are rejected with a reason" {
  assertTrue(Str.contains(rejected("localhost:61613"), "expected stomp://"), "no scheme")
  assertTrue(Str.contains(rejected("amqp://h"), "unsupported scheme"), "another protocol")
  assertTrue(Str.contains(rejected("stomp+ssl://h"), "TLS is not supported"), "TLS")
  assertTrue(Str.contains(rejected("stomp://"), "host is empty"), "no host")
  assertTrue(Str.contains(rejected("stomp://h:abc"), "not a number"), "non-numeric port")
  assertTrue(Str.contains(rejected("stomp://h:0"), "out of range"), "port 0")
  assertTrue(Str.contains(rejected("stomp://h:70000"), "out of range"), "port too large")
  assertTrue(Str.contains(rejected("stomp://h/queue"), "not allowed"), "a path")
  assertTrue(Str.contains(rejected("stomp://h?x=1"), "not allowed"), "a query")
  assertTrue(Str.contains(rejected("stomp://:p@h"), "login"), "empty login")
  assertTrue(Str.contains(rejected("stomp://u:%zz@h"), "hexadecimal"), "bad percent escape")
  assertTrue(Str.contains(rejected("stomp://u:%4@h"), "hexadecimal"), "short percent escape")
  assertTrue(Str.contains(rejected("stomp://u:%FF@h"), "UTF-8"), "percent escape that is not UTF-8")
  assertTrue(Str.contains(rejected("stomp://[::1"), "closing bracket"), "unclosed IPv6")
  assertTrue(Str.contains(rejected("stomp://[::1]x"), "unexpected text"), "junk after IPv6")
}

test "an error never repeats the passcode" {
  val e = rejected("stomp://user:hunter2@h:notaport")
  assertTrue(not Str.contains(e, "hunter2"), "the passcode is not echoed")
}
