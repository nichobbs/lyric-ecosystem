//! tests/url_tests.l - `Net.Url`: schemes, percent-decoding, user
//! information, hosts and ports, IPv6 literals.
@test_module
package Net.Url.Tests

import Std.Core
import Std.Testing
import Std.String as Str
import Net.Url as Url

func schemes(): slice[Url.Scheme] {
  [Url.Scheme(name = "tcp", defaultPort = 9000, tls = false), Url.Scheme(name = "tcps", defaultPort = 9443, tls = true)]
}

func matched(url: in String, ignoreCase: in Bool): Url.SchemeMatch {
  match Url.matchScheme(url, schemes(), ignoreCase) {
    case Ok(m) -> m
    case Err(p) -> panic("'" + url + "': " + Url.schemeProblemText(p, schemes()))
  }
}

func schemeError(url: in String, ignoreCase: in Bool): String {
  match Url.matchScheme(url, schemes(), ignoreCase) {
    case Ok(_) -> "matched"
    case Err(p) -> Url.schemeProblemText(p, schemes())
  }
}

test "a scheme is found among the accepted ones, with its default port and TLS flag" {
  val plain = matched("tcp://host:1", true)
  assertEqual(plain.scheme.name, "tcp", "the scheme")
  assertEqualInt(plain.scheme.defaultPort, 9000, "its default port")
  assertTrue(not plain.scheme.tls, "plain")
  assertEqual(plain.rest, "host:1", "the rest")
  val secure = matched("TCPS://u:p@host/path?x=1", true)
  assertEqual(secure.scheme.name, "tcps", "an upper-case scheme, ignoring case")
  assertTrue(secure.scheme.tls, "TLS")
  assertEqual(secure.rest, "u:p@host/path?x=1", "the rest is untouched")
  assertEqual(matched("tcp://", true).rest, "", "nothing after the scheme")
}

test "an unknown or missing scheme is a problem, and case matters when asked" {
  assertEqual(schemeError("http://host", true), "unsupported scheme 'http', expected tcp or tcps", "another scheme")
  assertEqual(schemeError("host:9000", true), "no scheme, expected tcp or tcps", "no scheme")
  assertEqual(schemeError("TCP://host", false), "unsupported scheme 'TCP', expected tcp or tcps", "case-sensitive")
  match Url.matchScheme("Http://host", schemes(), true) {
    case Err(UnsupportedScheme(name)) -> assertEqual(name, "http", "the name as compared")
    case _ -> assertTrue(false, "expected UnsupportedScheme")
  }
  match Url.matchScheme("host", schemes(), true) {
    case Err(NoScheme) -> ()
    case _ -> assertTrue(false, "expected NoScheme")
  }
}

func decoded(text: in String): String {
  match Url.percentDecode(text) {
    case Ok(s) -> s
    case Err(p) -> "<" + Url.escapeProblemText(p) + ">"
  }
}

test "percent-decoding reads the escapes as UTF-8 bytes" {
  assertEqual(decoded("plain"), "plain", "nothing to decode")
  assertEqual(decoded("s%40cret%3A%2F"), "s@cret:/", "reserved characters")
  assertEqual(decoded("caf%C3%A9"), "café", "two bytes, one character")
  assertEqual(decoded("%e2%82%ac"), "€", "lower-case digits")
  assertEqual(decoded("é%20é"), "é é", "text outside the escapes is kept")
  match Url.percentDecode("%4") {
    case Err(TruncatedEscape) -> ()
    case _ -> assertTrue(false, "a short escape")
  }
  match Url.percentDecode("abc%") {
    case Err(TruncatedEscape) -> ()
    case _ -> assertTrue(false, "a lone percent sign")
  }
  match Url.percentDecode("%zz") {
    case Err(InvalidEscape) -> ()
    case _ -> assertTrue(false, "not hexadecimal")
  }
  match Url.percentDecode("%FF") {
    case Err(EscapeNotUtf8) -> ()
    case _ -> assertTrue(false, "not UTF-8")
  }
  assertTrue(Str.contains(decoded("%zz"), "hexadecimal"), "the text of a bad escape")
  assertTrue(Str.contains(decoded("%FF"), "UTF-8"), "the text of bad bytes")
}

func userInfo(authority: in String): Url.UserInfo {
  match Url.splitUserInfo(authority) {
    case Ok(u) -> u
    case Err(p) -> panic("'" + authority + "': " + Url.authorityProblemText(p, "user", "password", schemes()[0]))
  }
}

test "user information is cut at the last @ and split at its first colon" {
  val none = userInfo("host:1")
  assertTrue(not none.present, "no @")
  assertEqual(none.user, "", "no user")
  assertEqual(none.hostPort, "host:1", "the whole authority")
  val both = userInfo("app:s%40cret@host:1")
  assertTrue(both.present, "an @")
  assertEqual(both.user, "app", "user")
  assertEqual(both.password, "s@cret", "password, decoded")
  assertEqual(both.hostPort, "host:1", "host and port")
  val userOnly = userInfo("app@host")
  assertEqual(userOnly.user, "app", "user alone")
  assertEqual(userOnly.password, "", "no password")
  val passwordOnly = userInfo(":secret@host")
  assertEqual(passwordOnly.user, "", "an empty user is the caller's to judge")
  assertEqual(passwordOnly.password, "secret", "password alone")
  val literalAt = userInfo("app:p@ss:w@rd@host")
  assertEqual(literalAt.password, "p@ss:w@rd", "a literal @ and colon in the password")
  assertEqual(literalAt.hostPort, "host", "the last @ ends the user information")
  val v6 = userInfo("u:p@[::1]:7")
  assertEqual(v6.hostPort, "[::1]:7", "an IPv6 host after credentials")
  match Url.splitUserInfo("a%zz:p@h") {
    case Err(UserEscape(InvalidEscape)) -> ()
    case _ -> assertTrue(false, "a bad escape in the user")
  }
  match Url.splitUserInfo("a:%FF@h") {
    case Err(PasswordEscape(EscapeNotUtf8)) -> ()
    case _ -> assertTrue(false, "a bad escape in the password")
  }
}

func hostPort(text: in String): Url.HostPort {
  match Url.parseHostPort(text, 9000) {
    case Ok(hp) -> hp
    case Err(p) -> panic("'" + text + "': " + Url.authorityProblemText(p, "user", "password", schemes()[0]))
  }
}

func hostError(text: in String): String {
  match Url.parseHostPort(text, 9000) {
    case Ok(hp) -> "parsed as " + hp.host + " port " + hp.port.toString()
    case Err(p) -> Url.authorityProblemText(p, "user", "password", schemes()[1])
  }
}

test "a host takes the default port unless one is written" {
  assertEqual(hostPort("broker.example").host, "broker.example", "host")
  assertEqualInt(hostPort("broker.example").port, 9000, "default port")
  assertEqualInt(hostPort("broker.example:61613").port, 61613, "explicit port")
  assertEqual(hostPort("127.0.0.1:1").host, "127.0.0.1", "an IPv4 address")
  assertEqualInt(hostPort("h:65535").port, 65535, "the highest port")
  assertEqual(hostError(""), "the host is empty", "nothing")
  assertEqual(hostError(":9000"), "the host is empty", "a port alone")
  assertEqual(hostError("h:"), "port '' is not a number", "a colon alone")
  assertEqual(hostError("h:http"), "port 'http' is not a number", "a named port")
  assertEqual(hostError("h:0"), "port 0 is out of range", "port 0")
  assertEqual(hostError("h:65536"), "port 65536 is out of range", "a port too high")
}

test "an IPv6 literal is written in brackets" {
  assertEqual(hostPort("[::1]").host, "::1", "without a port")
  assertEqualInt(hostPort("[::1]").port, 9000, "the default port")
  assertEqual(hostPort("[fe80::1]:7").host, "fe80::1", "with a port")
  assertEqualInt(hostPort("[fe80::1]:7").port, 7, "the port")
  assertEqual(hostError("[::1"), "an IPv6 address is missing its closing bracket", "unclosed")
  assertEqual(hostError("[::1]x"), "unexpected text after the IPv6 address", "text after the bracket")
  assertEqual(hostError("[::1]:x"), "port 'x' is not a number", "a bad port after the bracket")
  assertEqual(hostError("[]"), "the host is empty", "empty brackets")
}

test "an IPv6 literal without brackets is refused, not read as a host and a port" {
  val hint = "an IPv6 address must be written in brackets, as in tcps://[::1]:9443"
  assertEqual(hostError("::1"), hint, "::1 is not host '::' and port 1")
  assertEqual(hostError("::1:9000"), hint, "with what looks like a port")
  assertEqual(hostError("2001:db8::7:61614"), hint, "a full address")
  match Url.parseHostPort("fe80::1", 9000) {
    case Err(UnbracketedIpv6) -> ()
    case _ -> assertTrue(false, "expected UnbracketedIpv6")
  }
}

test "no problem text repeats the password" {
  val text = match Url.splitUserInfo("user:hunter2%zz@h") {
    case Ok(_) -> "parsed"
    case Err(p) -> Url.authorityProblemText(p, "user", "password", schemes()[0])
  }
  assertTrue(Str.startsWith(text, "password: "), "the password half is named: " + text)
  assertTrue(not Str.contains(text, "hunter2"), "and not echoed")
}
