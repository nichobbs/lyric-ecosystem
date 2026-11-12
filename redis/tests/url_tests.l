//! tests/url_tests.l - `redis://` URL parsing.
@test_module
package Redis.UrlTests

import Std.Core
import Std.Testing
import Std.String as Str
import Redis.Url

func parsed(url: in String): RedisUrl {
  match parseRedisUrl(url) {
    case Ok(u) -> u
    case Err(e) -> {
      assertTrue(false, "expected " + url + " to parse, got: " + e)
      RedisUrl(host = "", port = 0, username = "", password = "", database = 0, protocol = 0)
    }
  }
}

func errorOf(url: in String): String {
  match parseRedisUrl(url) {
    case Ok(_) -> {
      assertTrue(false, "expected " + url + " to be rejected")
      ""
    }
    case Err(e) -> e
  }
}

test "host only: default port, database 0, no credentials" {
  val u = parsed("redis://cache.internal")
  assertEqual(u.host, "cache.internal", "host")
  assertEqualInt(u.port, 6379, "default port")
  assertEqualInt(u.database, 0, "default database")
  assertEqual(u.username, "", "no user")
  assertEqual(u.password, "", "no password")
  assertEqualInt(u.protocol, 0, "no protocol choice")
}

test "host, port and database" {
  val u = parsed("redis://127.0.0.1:6380/3")
  assertEqual(u.host, "127.0.0.1", "host")
  assertEqualInt(u.port, 6380, "port")
  assertEqualInt(u.database, 3, "database")
}

test "a trailing slash selects database 0" {
  assertEqualInt(parsed("redis://localhost:6379/").database, 0, "database")
}

test "password only" {
  val u = parsed("redis://:s3cret@localhost:6379/0")
  assertEqual(u.username, "", "no user")
  assertEqual(u.password, "s3cret", "password")
}

test "ACL user and password" {
  val u = parsed("redis://app:s3cret@redis.example.com:7000/2")
  assertEqual(u.username, "app", "user")
  assertEqual(u.password, "s3cret", "password")
  assertEqual(u.host, "redis.example.com", "host")
  assertEqualInt(u.port, 7000, "port")
  assertEqualInt(u.database, 2, "database")
}

test "credentials are percent-decoded" {
  val u = parsed("redis://us%40er:p%3Aa%2Fss%40w%C3%A9rd@localhost")
  assertEqual(u.username, "us@er", "user")
  assertEqual(u.password, "p:a/ss@wérd", "password")
}

test "a password may contain a colon and an at sign" {
  val u = parsed("redis://app:a:b@c@localhost:6379")
  assertEqual(u.username, "app", "user")
  assertEqual(u.password, "a:b@c", "password")
  assertEqual(u.host, "localhost", "host")
}

test "an IPv6 host is written in brackets" {
  val u = parsed("redis://[::1]:6380/1")
  assertEqual(u.host, "::1", "host")
  assertEqualInt(u.port, 6380, "port")
  assertEqualInt(u.database, 1, "database")
  assertEqualInt(parsed("redis://[fe80::1]").port, 6379, "default port")
}

test "query parameters choose the database and the protocol" {
  val u = parsed("redis://localhost?db=5&protocol=3")
  assertEqualInt(u.database, 5, "database")
  assertEqualInt(u.protocol, 3, "protocol")
  assertEqualInt(parsed("redis://localhost/1?protocol=2").protocol, 2, "protocol 2")
}

test "rediss is refused with a TLS message" {
  assertTrue(Str.contains(errorOf("rediss://localhost:6379"), "TLS"), "the error names TLS")
}

test "malformed URLs are rejected" {
  val _ = errorOf("http://localhost")
  val _ = errorOf("localhost:6379")
  val _ = errorOf("redis://")
  val _ = errorOf("redis://:6379")
  val _ = errorOf("redis://localhost:0")
  val _ = errorOf("redis://localhost:70000")
  val _ = errorOf("redis://localhost:abc")
  val _ = errorOf("redis://localhost/abc")
  val _ = errorOf("redis://localhost/-1")
  val _ = errorOf("redis://localhost?protocol=4")
  val _ = errorOf("redis://localhost?tls=true")
  val _ = errorOf("redis://[::1")
  val _ = errorOf("redis://user@localhost")
  val _ = errorOf("redis://u:p%zz@localhost")
  val _ = errorOf("redis://u:p%4@localhost")
}

test "an error never repeats the password" {
  val e = errorOf("redis://app:hunter2@localhost:notaport")
  assertFalse(Str.contains(e, "hunter2"), "the password is not in the error")
}
