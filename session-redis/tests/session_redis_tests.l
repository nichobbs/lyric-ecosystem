//! tests/session_redis_tests.l - the parts of the Redis session store that
//! need no server: session ids, key naming, option validation, the handling
//! of malformed ids (answered without contacting Redis) and of an
//! unreachable server (an `Err`, never a panic). The conformance suite runs
//! against a real server in tests/integration.
@test_module
package Session.Redis.StoreTests

import Std.Core
import Std.Collections
import Std.String as Str
import Std.Testing
import Std.Tls as Tls
import Session
import Session.Redis as RedisSessions
import Session.Redis.Admin as Admin
import Redis as RedisLib
import Redis.Kernel as Kernel

// A loopback port with nothing listening: bound, read back, released.
func closedPort(): Int {
  match Kernel.serverListen("127.0.0.1", 0) {
    case Err(m) -> panic("cannot reserve a port: " + m)
    case Ok(server) -> {
      val port = Kernel.serverPort(server)
      Kernel.serverClose(server)
      port
    }
  }
}

func unreachableStore(): RedisSessions.RedisSessionStore {
  val client = RedisLib.lazy(RedisLib.configFor("127.0.0.1", closedPort()))
  RedisSessions.fromClient(client, RedisSessions.options("unit:", 60))
}

func isLowerHex(s: in String): Bool {
  var i = 0
  while i < s.length {
    val u = Str.codeUnitAt(s, i)
    val ok = (u >= 48 and u <= 57) or (u >= 97 and u <= 102)
    if not ok {
      return false
    }
    i = i + 1
  }
  true
}

func codeOfUnit(r: in Result[Unit, SessionError]): String {
  match r {
    case Ok(_) -> "OK"
    case Err(e) -> e.code
  }
}

test "session ids are 64 hex characters, valid, and never repeat" {
  val seen: Map[String, Bool] = newMap()
  var i = 0
  while i < 500 {
    val id = RedisSessions.newSessionId()
    assertEqualInt(id.length, 64, "256 bits as hex")
    assertTrue(isLowerHex(id), "lower-case hex")
    assertTrue(isValidSessionId(id), "accepted by Session.isValidSessionId")
    assertFalse(seen.containsKey(id), "ids do not repeat")
    seen[id] = true
    i = i + 1
  }
}

test "a session is stored under the prefix" {
  assertEqual(
    RedisSessions.redisKey(RedisSessions.options("app:session:", 60), "abc"),
    "app:session:abc",
    "prefixed key"
  )
}

test "an empty prefix and an out-of-range TTL are rejected" {
  assertPanics("an empty prefix", { -> val _ = RedisSessions.options("", 60) })
  assertPanics("a zero TTL", { -> val _ = RedisSessions.options("p:", 0) })
  assertPanics("a TTL over a year", { -> val _ = RedisSessions.options("p:", 31536001) })
  assertEqualInt(RedisSessions.options("p:", 31536000).ttlSeconds, maxSessionTtlSeconds(), "a year is the limit")
}

test "a malformed id is answered without contacting Redis" {
  val store = unreachableStore()
  for id in ["", "has space", "../escape", "semi;colon", "new\nline", "wild*card"] {
    match store.load(id) {
      case Ok(None) -> ()
      case _ -> assertTrue(false, "load of a malformed id is Ok(None)")
    }
    match store.get(id, "k") {
      case Ok(None) -> ()
      case _ -> assertTrue(false, "get on a malformed id is Ok(None)")
    }
    assertEqual(codeOfUnit(store.set(id, "k", "v")), "SESSION_NOT_FOUND", "set on a malformed id")
    assertEqual(codeOfUnit(store.delete(id, "k")), "OK", "delete on a malformed id")
    assertEqual(codeOfUnit(store.clear(id)), "OK", "clear on a malformed id")
    assertEqual(codeOfUnit(store.touch(id)), "OK", "touch on a malformed id")
    assertEqual(codeOfUnit(store.destroy(id)), "OK", "destroy on a malformed id")
    match Admin.remainingMillis(store, id) {
      case Ok(None) -> ()
      case _ -> assertTrue(false, "remainingMillis of a malformed id is Ok(None)")
    }
    val entries: Map[String, String] = newMap()
    val data = SessionData(id = id, entries = entries, createdAt = 1i64, lastAccessedAt = 1i64, expiresAt = 0i64)
    assertEqual(codeOfUnit(store.save(data)), "INVALID_SESSION_ID", "save of a malformed id")
  }
  assertEqualInt(store.client.connections(), 0, "no connection was attempted")
}

test "with Redis unreachable every operation is an Err with its own code" {
  val store = unreachableStore()
  val id = RedisSessions.newSessionId()
  match store.create() {
    case Err(e) -> {
      assertEqual(e.code, "CREATE_FAILED", "create")
      assertTrue(Str.contains(e.message, "127.0.0.1"), "the message names the endpoint")
    }
    case Ok(_) -> assertTrue(false, "create must fail")
  }
  match store.load(id) {
    case Err(e) -> assertEqual(e.code, "LOAD_FAILED", "load")
    case Ok(_) -> assertTrue(false, "load must fail")
  }
  match store.get(id, "k") {
    case Err(e) -> assertEqual(e.code, "GET_FAILED", "get")
    case Ok(_) -> assertTrue(false, "get must fail")
  }
  assertEqual(codeOfUnit(store.set(id, "k", "v")), "SET_FAILED", "set")
  assertEqual(codeOfUnit(store.delete(id, "k")), "DELETE_FAILED", "delete")
  assertEqual(codeOfUnit(store.clear(id)), "CLEAR_FAILED", "clear")
  assertEqual(codeOfUnit(store.touch(id)), "TOUCH_FAILED", "touch")
  assertEqual(codeOfUnit(store.destroy(id)), "DESTROY_FAILED", "destroy")
  val entries: Map[String, String] = newMap()
  val data = SessionData(id = id, entries = entries, createdAt = 1i64, lastAccessedAt = 1i64, expiresAt = 0i64)
  assertEqual(codeOfUnit(store.save(data)), "SAVE_FAILED", "save")
  match Admin.sessionCount(store) {
    case Err(e) -> assertEqual(e.code, "SCAN_FAILED", "sessionCount")
    case Ok(_) -> assertTrue(false, "sessionCount must fail")
  }
}

test "save rejects a negative expiry before contacting Redis" {
  val store = unreachableStore()
  val entries: Map[String, String] = newMap()
  val data = SessionData(
    id = RedisSessions.newSessionId(),
    entries = entries,
    createdAt = 1i64,
    lastAccessedAt = 1i64,
    expiresAt = 0i64 - 5i64
  )
  match store.save(data) {
    case Err(e) -> assertTrue(Str.contains(e.message, "expiresAt"), "the message names the field")
    case Ok(_) -> assertTrue(false, "a negative expiry must be rejected")
  }
  assertEqualInt(store.client.connections(), 0, "no connection was attempted")
}

test "connect reports an unusable URL and an unreachable server without the password" {
  match RedisSessions.connect("redis://app:hunter2@127.0.0.1:notaport", RedisSessions.options("p:", 60)) {
    case Err(e) -> {
      assertEqual(e.code, "CONNECT_FAILED", "an unusable URL")
      assertTrue(Str.contains(e.message, "invalid Redis configuration"), "the message says what is wrong")
      assertFalse(Str.contains(e.message, "hunter2"), "the password is not in the message")
    }
    case Ok(_) -> assertTrue(false, "an unusable URL must be refused")
  }
  val url = "redis://app:hunter2@127.0.0.1:" + closedPort().toString()
  match RedisSessions.connect(url, RedisSessions.options("p:", 60)) {
    case Err(e) -> {
      assertEqual(e.code, "CONNECT_FAILED", "nothing listening")
      assertFalse(Str.contains(e.message, "hunter2"), "the password is not in the message")
    }
    case Ok(_) -> assertTrue(false, "nothing listens on the port")
  }
}

test "a rediss URL is dialled, and connectWith carries the TLS options" {
  val port = closedPort().toString()
  match RedisSessions.connect("rediss://app:hunter2@127.0.0.1:" + port, RedisSessions.options("p:", 60)) {
    case Err(e) -> {
      assertEqual(e.code, "CONNECT_FAILED", "a TLS URL is accepted and dialled")
      assertTrue(Str.contains(e.message, "cannot connect to Redis"), "the failure is the dial, not the URL")
      assertFalse(Str.contains(e.message, "hunter2"), "the password is not in the message")
    }
    case Ok(_) -> assertTrue(false, "nothing listens on the port")
  }
  val tls = match RedisLib.configFromUrl("rediss://127.0.0.1:" + port) {
    case Ok(cfg) -> cfg
    case Err(e) -> panic("the URL must parse: " + e.message())
  }
  val named = RedisLib.withTlsServerName(RedisLib.withMinTlsVersion(tls, Tls.Tls13), "sessions.internal")
  match RedisSessions.connectWith(named, RedisSessions.options("p:", 60)) {
    case Err(e) -> assertTrue(Str.contains(e.message, "cannot connect to Redis"), "usable TLS options reach the dial")
    case Ok(_) -> assertTrue(false, "nothing listens on the port")
  }
  val badCa = RedisLib.withExclusiveCaCertificate(tls, "not a certificate")
  match RedisSessions.connectWith(badCa, RedisSessions.options("p:", 60)) {
    case Err(e) -> {
      assertEqual(e.code, "CONNECT_FAILED", "the store's connect code")
      assertTrue(Str.contains(e.message, "invalid Redis configuration"), "an unusable CA is reported before dialling")
    }
    case Ok(_) -> assertTrue(false, "an unusable CA must be refused")
  }
  val plain = RedisLib.withTlsServerName(RedisLib.configFor("127.0.0.1", closedPort()), "sessions.internal")
  match RedisSessions.connectWith(plain, RedisSessions.options("p:", 60)) {
    case Err(e) -> assertTrue(
      Str.contains(e.message, "invalid Redis configuration"),
      "TLS options without TLS are refused"
    )
    case Ok(_) -> assertTrue(false, "TLS options on a plain connection must be refused")
  }
}
