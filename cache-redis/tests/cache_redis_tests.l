//! tests/cache_redis_tests.l - the parts of the Redis cache store that need
//! no server: key naming, option validation, and how the `CacheStore`
//! methods behave when Redis cannot be reached (a miss and a recorded error,
//! never a panic). The conformance suite runs against a real server in
//! tests/integration.
@test_module
package Cache.Redis.StoreTests

import Std.Core
import Std.Testing
import Std.Tls as Tls
import Cache
import Cache.Redis as RedisCache
import Cache.Redis.Extras as Extras
import Redis as RedisLib
import Net.Tcp.Testing as KT

// A loopback port with nothing listening: bound, read back, released.
func closedPort(): Int {
  match KT.serverListen("127.0.0.1", 0) {
    case Err(m) -> panic("cannot reserve a port: " + m)
    case Ok(server) -> {
      val port = KT.serverPort(server)
      KT.serverClose(server)
      port
    }
  }
}

func unreachableStore(): RedisCache.RedisCacheStore {
  val client = RedisLib.lazy(RedisLib.configFor("127.0.0.1", closedPort()))
  RedisCache.fromClient(client, RedisCache.options("unit:"))
}

test "a cache key is stored under the prefix" {
  assertEqual(RedisCache.redisKey(RedisCache.options("app:cache:"), "users:42"), "app:cache:users:42", "prefixed key")
  assertEqual(
    RedisCache.redisKey(RedisCache.options("p"), "k with spaces"),
    "pk with spaces",
    "no separator is invented"
  )
}

test "options default to real seconds" {
  assertEqualInt(RedisCache.options("p:").ttlUnitMillis, 1000, "one second per TTL unit")
  assertEqualInt(RedisCache.optionsWithTtlUnit("p:", 50).ttlUnitMillis, 50, "a shortened unit")
}

test "an empty prefix and an out-of-range TTL unit are rejected" {
  assertPanics("an empty prefix", { -> val _ = RedisCache.options("") })
  assertPanics("a zero TTL unit", { -> val _ = RedisCache.optionsWithTtlUnit("p:", 0) })
  assertPanics("a TTL unit over one second", { -> val _ = RedisCache.optionsWithTtlUnit("p:", 1001) })
}

test "with Redis unreachable a get is a miss and the failure is recorded" {
  val store = unreachableStore()
  assertEqualInt(store.errors.errorCount(), 0, "no error before the first call")
  match store.get("k") {
    case None -> ()
    case Some(_) -> assertTrue(false, "an unreachable server cannot produce a hit")
  }
  assertEqualInt(store.errors.errorCount(), 1, "the failed get was counted")
  match store.errors.lastError() {
    case Some(e) -> assertEqual(e.code(), "CONNECT_FAILED", "the recorded error")
    case None -> assertTrue(false, "the failure must be recorded")
  }
}

test "with Redis unreachable the writes are dropped and recorded, not thrown" {
  val store = unreachableStore()
  store.set("k", "v", 0)
  store.set("k", "v", 30)
  store.delete("k")
  store.clear()
  assertEqualInt(store.errors.errorCount(), 4, "each failed write was counted")
  val asInterface: Cache.CacheStore = store
  Cache.set(asInterface, "k", "v")
  match Cache.get(asInterface, "k") {
    case None -> ()
    case Some(_) -> assertTrue(false, "nothing was stored")
  }
  assertEqualInt(store.errors.errorCount(), 6, "the wrappers go through the same store")
}

test "the Result-returning functions report the failure" {
  val store = unreachableStore()
  match RedisCache.tryGet(store, "k") {
    case Err(e) -> assertEqual(e.code(), "CONNECT_FAILED", "tryGet")
    case Ok(_) -> assertTrue(false, "tryGet must fail")
  }
  match RedisCache.trySet(store, "k", "v", 10) {
    case Err(e) -> assertEqual(e.code(), "CONNECT_FAILED", "trySet")
    case Ok(_) -> assertTrue(false, "trySet must fail")
  }
  match RedisCache.tryDelete(store, "k") {
    case Err(e) -> assertEqual(e.code(), "CONNECT_FAILED", "tryDelete")
    case Ok(_) -> assertTrue(false, "tryDelete must fail")
  }
  match RedisCache.tryClear(store) {
    case Err(e) -> assertEqual(e.code(), "CONNECT_FAILED", "tryClear")
    case Ok(_) -> assertTrue(false, "tryClear must fail")
  }
  match Extras.setIfAbsent(store, "k", "v", 0) {
    case Err(e) -> assertEqual(e.code(), "CONNECT_FAILED", "setIfAbsent")
    case Ok(_) -> assertTrue(false, "setIfAbsent must fail")
  }
  match Extras.lifetimeOf(store, "k") {
    case Err(e) -> assertEqual(e.code(), "CONNECT_FAILED", "lifetimeOf")
    case Ok(_) -> assertTrue(false, "lifetimeOf must fail")
  }
  assertEqualInt(store.errors.errorCount(), 0, "the Result functions do not touch the error log")
}

test "connect reports an unusable URL and an unreachable server" {
  match RedisCache.connect("redis://", RedisCache.options("p:")) {
    case Err(e) -> assertEqual(e.code(), "INVALID_CONFIG", "a URL without a host")
    case Ok(_) -> assertTrue(false, "a URL without a host must be refused")
  }
  match RedisCache.connect("redis://127.0.0.1:" + closedPort().toString(), RedisCache.options("p:")) {
    case Err(e) -> assertEqual(e.code(), "CONNECT_FAILED", "nothing listening")
    case Ok(_) -> assertTrue(false, "nothing listens on the port")
  }
}

test "a rediss URL is dialled, and connectWith carries the TLS options" {
  val port = closedPort().toString()
  match RedisCache.connect("rediss://127.0.0.1:" + port, RedisCache.options("p:")) {
    case Err(e) -> assertEqual(e.code(), "CONNECT_FAILED", "a TLS URL is accepted and dialled")
    case Ok(_) -> assertTrue(false, "nothing listens on the port")
  }
  val tls = match RedisLib.configFromUrl("rediss://127.0.0.1:" + port) {
    case Ok(cfg) -> cfg
    case Err(e) -> panic("the URL must parse: " + e.message())
  }
  val named = RedisLib.withTlsServerName(RedisLib.withMinTlsVersion(tls, Tls.Tls13), "cache.internal")
  match RedisCache.connectWith(named, RedisCache.options("p:")) {
    case Err(e) -> assertEqual(e.code(), "CONNECT_FAILED", "usable TLS options reach the dial")
    case Ok(_) -> assertTrue(false, "nothing listens on the port")
  }
  match RedisCache.connectWith(RedisLib.withExclusiveCaCertificate(tls, "not a certificate"), RedisCache.options("p:")) {
    case Err(e) -> assertEqual(e.code(), "INVALID_CONFIG", "an unusable CA is reported before dialling")
    case Ok(_) -> assertTrue(false, "an unusable CA must be refused")
  }
  val plain = RedisLib.withTlsServerName(RedisLib.configFor("127.0.0.1", closedPort()), "cache.internal")
  match RedisCache.connectWith(plain, RedisCache.options("p:")) {
    case Err(e) -> assertEqual(e.code(), "INVALID_CONFIG", "TLS options without TLS are refused")
    case Ok(_) -> assertTrue(false, "TLS options on a plain connection must be refused")
  }
}

test "the interface contract rejects an empty key" {
  val store = unreachableStore()
  assertPanics("get with an empty key", { -> val _ = store.get("") })
  assertPanics("set with a negative TTL", { -> store.set("k", "v", 0 - 1) })
}
