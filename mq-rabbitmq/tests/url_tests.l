//! tests/url_tests.l - broker URL parsing.
@test_module
package Mq.RabbitMq.UrlTests

import Std.Core
import Std.Testing
import Std.String as Str
import Mq.RabbitMq.Url as Url

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

test "host only takes the default port, the guest user and the default virtual host" {
  val a = parsed("amqp://broker.example")
  assertEqual(a.host, "broker.example", "host")
  assertEqualInt(a.port, 5672, "default port")
  assertEqual(a.user, "guest", "default user")
  assertEqual(a.password, "guest", "default password")
  assertEqual(a.virtualHost, "/", "default virtual host")
  assertTrue(not a.tls, "amqp:// is plain TCP")
  assertEqualInt(a.heartbeatSeconds, -1, "no heartbeat parameter")
  assertEqualInt(a.connectTimeoutMs, -1, "no connection_timeout parameter")
  assertEqualInt(a.frameMax, -1, "no frame_max parameter")
}

test "the full form: user, password, host, port and virtual host" {
  val a = parsed("amqp://app:s3cret@broker.example:5673/production")
  assertEqual(a.user, "app", "user")
  assertEqual(a.password, "s3cret", "password")
  assertEqual(a.host, "broker.example", "host")
  assertEqualInt(a.port, 5673, "port")
  assertEqual(a.virtualHost, "production", "virtual host")
}

test "amqps selects TLS and the TLS default port" {
  val a = parsed("amqps://broker.example")
  assertTrue(a.tls, "TLS")
  assertEqualInt(a.port, 5671, "default TLS port")
  val b = parsed("AMQPS://lyric:s%40cret@[::1]:5700/v")
  assertTrue(b.tls, "an upper-case scheme")
  assertEqual(b.host, "::1", "IPv6 host")
  assertEqualInt(b.port, 5700, "port after the bracket")
  assertEqual(b.password, "s@cret", "percent-decoded password")
  assertEqual(parsed("amqp://[::1]").host, "::1", "IPv6 host without a port")
  assertEqualInt(parsed("amqp://[fe80::1]/v").port, 5672, "IPv6 host with a virtual host")
}

test "the virtual host follows the RabbitMQ URI specification" {
  assertEqual(parsed("amqp://h").virtualHost, "/", "no path: the default virtual host")
  assertEqual(parsed("amqp://h/%2f").virtualHost, "/", "%2f: the default virtual host spelled out")
  assertEqual(parsed("amqp://h/%2F").virtualHost, "/", "%2F as well")
  assertEqual(parsed("amqp://h/").virtualHost, "", "a bare slash: the virtual host with the empty name")
  assertEqual(parsed("amqp://h/orders").virtualHost, "orders", "a name")
  assertEqual(parsed("amqp://h/a%2Fb").virtualHost, "a/b", "a name with an encoded slash")
  assertEqual(parsed("amqp://h/%2Fprod").virtualHost, "/prod", "a name that starts with a slash")
  assertEqual(parsed("amqp://h/caf%C3%A9%20%E4%B8%96").virtualHost, "café 世", "UTF-8 through percent-encoding")
  assertEqual(parsed("amqp://u:p@h:1/v%40w").virtualHost, "v@w", "an encoded @ in the virtual host")
  expectContains(rejected("amqp://h/a/b"), "more than the virtual host", "two path segments")
  expectContains(rejected("amqp://h/%zz"), "virtual host:", "a bad escape in the virtual host")
  expectContains(rejected("amqp://h/%ff"), "not valid UTF-8", "bytes that are not UTF-8")
}

func expectContains(text: in String, part: in String, label: in String): Unit {
  assertTrue(Str.contains(text, part), label + ": got \"" + text + "\"")
}

test "credentials are percent-decoded, and a password may be empty or hold a colon" {
  val a = parsed("amqp://us%65r:p%3Aa%2Fss%40@h")
  assertEqual(a.user, "user", "user")
  assertEqual(a.password, "p:a/ss@", "password")
  assertEqual(parsed("amqp://u:a:b@h").password, "a:b", "everything after the first colon")
  assertEqual(parsed("amqp://u@h").password, "", "a user without a password")
  assertEqual(parsed("amqp://u:@h").password, "", "an empty password")
  assertEqual(parsed("amqp://u:p@ss@h").host, "h", "the last @ ends the credentials")
  assertEqual(parsed("amqp://u:p@ss@h").password, "p@ss", "an unencoded @ in the password")
  expectContains(rejected("amqp://:p@h"), "user before '@' is empty", "an empty user")
  expectContains(rejected("amqp://u:%4@h"), "password:", "a cut escape")
}

test "the heartbeat, connection_timeout and frame_max parameters are read; others are refused" {
  val a = parsed("amqp://h/v?heartbeat=30&connection_timeout=2500&frame_max=8192")
  assertEqualInt(a.heartbeatSeconds, 30, "heartbeat")
  assertEqualInt(a.connectTimeoutMs, 2500, "connection_timeout")
  assertEqualInt(a.frameMax, 8192, "frame_max")
  assertEqual(a.virtualHost, "v", "the virtual host ends at the question mark")
  assertEqualInt(parsed("amqp://h?heartbeat=0").heartbeatSeconds, 0, "heartbeat=0 is explicit")
  assertEqual(parsed("amqp://h?heartbeat=5").virtualHost, "/", "parameters without a path")
  assertEqualInt(parsed("amqp://h?").heartbeatSeconds, -1, "an empty query")
  expectContains(rejected("amqp://h?heartbeat=soon"), "parameter heartbeat is not a number", "a bad number")
  expectContains(rejected("amqp://h?heartbeat=-1"), "must not be negative", "a negative number")
  expectContains(rejected("amqp://h?heartbeat=70000"), "out of range", "a heartbeat over 16 bits")
  val e = rejected("amqp://h?verify=verify_none&password=hunter2")
  expectContains(e, "unsupported parameter 'verify'", "an unknown parameter is named")
  assertTrue(not Str.contains(e, "verify_none"), "its value is not echoed")
  assertTrue(not Str.contains(e, "hunter2"), "nor a later one")
}

test "malformed URLs are rejected without echoing the password" {
  expectContains(rejected("broker.example:5672"), "expected amqp://", "no scheme")
  expectContains(rejected("stomp://h"), "unsupported scheme 'stomp'", "another scheme")
  expectContains(rejected("amqp://"), "host is empty", "no host")
  expectContains(rejected("amqp://u:p@"), "host is empty", "credentials without a host")
  expectContains(rejected("amqp://h:port"), "is not a number", "a port that is not a number")
  expectContains(rejected("amqp://h:0"), "out of range", "port 0")
  expectContains(rejected("amqp://h:65536"), "out of range", "port 65536")
  expectContains(rejected("amqp://[::1"), "closing bracket", "an unclosed IPv6 literal")
  expectContains(rejected("amqp://[::1]x"), "unexpected text", "text after an IPv6 literal")
  expectContains(rejected("amqp://h/v#frag"), "fragment", "a fragment")
  val e = rejected("amqp://app:hunter2@h:notaport/v")
  assertTrue(not Str.contains(e, "hunter2"), "the password is not in the error: " + e)
}

test "an IPv6 address without brackets is rejected" {
  expectContains(rejected("amqp://::1"), "brackets", "::1 is not host '::' and port 1")
  expectContains(rejected("amqp://::1:5672"), "brackets", "with a port")
  expectContains(rejected("amqp://u:p@fe80::1/v"), "brackets", "after credentials, before a virtual host")
  expectContains(rejected("amqps://2001:db8::7:5671"), "amqps://[::1]:5671", "the error shows the form")
}

test "percentDecode decodes UTF-8 sequences and rejects bad escapes" {
  assertEqual(match Url.percentDecode("a%20b") { case Ok(s) -> s; case Err(e) -> e }, "a b", "a space")
  assertEqual(match Url.percentDecode("plain") { case Ok(s) -> s; case Err(e) -> e }, "plain", "nothing to decode")
  assertTrue(isErr(Url.percentDecode("%")), "a lone percent")
  assertTrue(isErr(Url.percentDecode("%4")), "one digit")
  assertTrue(isErr(Url.percentDecode("%gg")), "not hexadecimal")
  assertTrue(isErr(Url.percentDecode("%c3")), "half a UTF-8 sequence")
}
