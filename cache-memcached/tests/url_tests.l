//! tests/url_tests.l - memcached connection URLs.
@test_module
package Cache.Memcached.UrlTests

import Std.Core
import Std.String as Str
import Std.Testing
import Cache.Memcached.Url

func parsed(url: in String): MemcachedUrl {
  match parseMemcachedUrl(url) {
    case Ok(u) -> u
    case Err(e) -> panic("'" + url + "' must parse: " + e)
  }
}

func problem(url: in String): String {
  match parseMemcachedUrl(url) {
    case Ok(_) -> "<parsed>"
    case Err(e) -> e
  }
}

test "host and port" {
  val u = parsed("memcached://cache.internal:11311")
  assertEqual(u.host, "cache.internal", "host")
  assertEqualInt(u.port, 11311, "port")
  assertEqual(u.username, "", "no user")
  assertEqual(u.password, "", "no password")
  assertFalse(u.tls, "plain")
}

test "the port defaults to 11211" {
  assertEqualInt(parsed("memcached://localhost").port, 11211, "plain")
  assertEqualInt(parsed("memcached+tls://localhost").port, 11211, "TLS")
  assertEqualInt(parsed("memcached://localhost/").port, 11211, "a trailing slash is allowed")
}

test "the +tls scheme switches TLS on" {
  assertTrue(parsed("memcached+tls://cache.internal:11212").tls, "memcached+tls")
  assertFalse(parsed("memcached://cache.internal:11212").tls, "memcached")
  assertTrue(parsed("MEMCACHED+TLS://cache.internal").tls, "the scheme is matched without regard to case")
}

test "credentials are percent-decoded" {
  val u = parsed("memcached://app:s3cret@cache.internal")
  assertEqual(u.username, "app", "user")
  assertEqual(u.password, "s3cret", "password")
  val escaped = parsed("memcached://app:p%40ss%3Aw%2Ford@cache.internal:1")
  assertEqual(escaped.password, "p@ss:w/ord", "an escaped password")
  assertEqual(parsed("memcached://app:a@b@host").password, "a@b", "the last @ ends the credentials")
}

test "credentials need both halves" {
  assertEqual(
    problem("memcached://app@host"),
    "credentials need a user name and a password (memcached://user:password@host)",
    "a user without a password"
  )
  assertEqual(
    problem("memcached://:secret@host"),
    "credentials need a user name and a password (memcached://user:password@host)",
    "a password without a user"
  )
}

test "IPv6 hosts are written in brackets" {
  val u = parsed("memcached://[::1]:11999")
  assertEqual(u.host, "::1", "the address without brackets")
  assertEqualInt(u.port, 11999, "port")
  assertEqualInt(parsed("memcached://[fe80::1]").port, 11211, "default port")
  assertTrue(Str.contains(problem("memcached://::1"), "brackets"), "without brackets it is refused")
}

test "other schemes are refused" {
  assertEqual(problem("redis://host"), "unsupported scheme 'redis', expected memcached or memcached+tls", "redis")
  assertEqual(
    problem("memcacheds://host"),
    "unsupported scheme 'memcacheds', expected memcached or memcached+tls",
    "memcacheds"
  )
  assertEqual(problem("host:11211"), "no scheme, expected memcached or memcached+tls", "no scheme")
}

test "a URL names one server, without path or parameters" {
  assertEqual(problem("memcached://host/0"), "a memcached URL has no path", "a path")
  assertEqual(problem("memcached://host?timeout=5"), "a memcached URL has no parameters", "a query")
  assertEqual(problem("memcached://host#x"), "a memcached URL has no parameters", "a fragment")
  assertEqual(
    problem("memcached://a:11211,b:11211"),
    "a memcached URL names one server; a server list is not supported",
    "a server list"
  )
}

test "unusable authorities are refused" {
  assertEqual(problem("memcached://"), "the host is empty", "no host")
  assertEqual(problem("memcached://host:port"), "port 'port' is not a number", "a port that is not a number")
  assertEqual(problem("memcached://host:70000"), "port 70000 is out of range", "a port out of range")
  assertEqual(problem("memcached://host:0"), "port 0 is out of range", "port 0")
}

test "an error never shows the password" {
  val text = problem("memcached://app:hunter2@host:notaport")
  assertFalse(Str.contains(text, "hunter2"), "a bad port")
  val escape = problem("memcached://app:hunter2%zz@host")
  assertFalse(Str.contains(escape, "hunter2"), "a bad escape")
  assertTrue(Str.contains(escape, "password"), "but says where the problem is")
}
