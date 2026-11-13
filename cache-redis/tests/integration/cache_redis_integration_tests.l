//! tests/integration/cache_redis_integration_tests.l - the `CacheStore`
//! conformance suite and the backend's own behaviour against a real Redis.
//!
//! Needs `LYRIC_REDIS_URL` and is compiled only with `--features integration`:
//!
//!   LYRIC_REDIS_URL=redis://127.0.0.1:16379 \
//!     lyric test --manifest cache-redis/lyric.toml --target dotnet --features integration
//!
//! The conformance suite runs a second time over TLS with a client
//! certificate, against the server `scripts/ci/start-redis-tls.sh` starts:
//! `LYRIC_REDIS_MTLS_URL` (a `rediss://` URL), `LYRIC_REDIS_TLS_CA`,
//! `LYRIC_REDIS_TLS_CERT` and `LYRIC_REDIS_TLS_KEY` (PEM files).
//!
//! Every key is written under a random prefix and removed again.
//!
//! Time: the conformance suite advances its clock by 26 "seconds" in all.
//! Redis has no clock to inject, so the stores under conformance are built
//! with a TTL unit of 50 ms (`optionsWithTtlUnit`) and the clock hook sleeps
//! 50 ms per second: every expiry the suite observes is still performed by
//! Redis (`SET ... PX`), only on a shorter scale. A separate test checks an
//! expiry with the production unit of one real second.
@cfg(feature = "integration")
@test_module
package Cache.Redis.IntegrationTests

import Std.Core
import Std.Collections
import Std.Environment as Env
import Std.Testing
import Std.Time
import Std.Uuid
import Cache
import Cache.Redis as RedisCache
import Cache.Redis.Extras as Extras
import Redis as RedisLib
import Net.Tcp.Testing as KT
import Testing.Conformance
import Testing.Conformance.Cache as CacheConformance

func serverUrl(): String {
  val url = Env.getVarOrDefault("LYRIC_REDIS_URL", "")
  if url.length == 0 {
    panic(
      "LYRIC_REDIS_URL is not set: the integration tests need a Redis server, for example LYRIC_REDIS_URL=redis://127.0.0.1:16379"
    )
  }
  url
}

val runId: String = uuidToString(newUuid())

func freshPrefix(): String {
  "lyric-cache-it:" + runId + ":" + uuidToString(newUuid()) + ":"
}

func newClient(): RedisLib.RedisClient {
  match RedisLib.connect(serverUrl()) {
    case Ok(c) -> c
    case Err(e) -> panic("cannot connect to LYRIC_REDIS_URL: " + e.message())
  }
}

func storeWith(opts: in RedisCache.RedisCacheOptions): RedisCache.RedisCacheStore {
  RedisCache.fromClient(newClient(), opts)
}

func ok[T](r: in Result[T, RedisLib.RedisError], what: in String): T {
  match r {
    case Ok(v) -> v
    case Err(e) -> panic(what + " failed: " + e.message())
  }
}

func expectHit(v: in Option[String], expected: in String, what: in String): Unit {
  match v {
    case Some(s) -> assertEqual(s, expected, what)
    case None -> assertTrue(false, what + ": expected a hit")
  }
}

func expectMiss(v: in Option[String], what: in String): Unit {
  match v {
    case Some(s) -> assertTrue(false, what + ": expected a miss, got " + s)
    case None -> ()
  }
}

// ─── Conformance ─────────────────────────────────────────────────────────────
val conformanceTtlUnitMillis: Int = 50

val conformanceStores: List[RedisCache.RedisCacheStore] = newList()

func conformanceStore(): Cache.CacheStore {
  val store = storeWith(RedisCache.optionsWithTtlUnit(freshPrefix(), conformanceTtlUnitMillis))
  conformanceStores.add(store)
  val asInterface: Cache.CacheStore = store
  asInterface
}

test "RedisCacheStore conforms to CacheStore" {
  requireConformance(
    CacheConformance.run({ -> conformanceStore() }, { seconds -> sleepMillis(seconds * conformanceTtlUnitMillis) })
  )
  assertTrue(conformanceStores.count >= 12, "the suite built a store per case")
  for store in conformanceStores {
    assertEqualInt(store.errors.errorCount(), 0, "no Redis failure was swallowed during conformance")
    val _ = ok(RedisCache.tryClear(store), "cleanup")
    store.client.close()
  }
}

// ─── Conformance over TLS ────────────────────────────────────────────────────
func requiredEnv(name: in String): String {
  val value = Env.getVarOrDefault(name, "")
  if value.length == 0 {
    panic(
      name +
        " is not set: the TLS conformance run needs a Redis server with TLS; run scripts/ci/start-redis-tls.sh, which starts one and prints LYRIC_REDIS_MTLS_URL, LYRIC_REDIS_TLS_CA, LYRIC_REDIS_TLS_CERT and LYRIC_REDIS_TLS_KEY"
    )
  }
  value
}

// The server that requires a client certificate: the custom CA and the
// client identity both travel through `connectWith`.
func tlsConfig(): RedisLib.RedisConfig {
  val fromUrl = ok(RedisLib.configFromUrl(requiredEnv("LYRIC_REDIS_MTLS_URL")), "LYRIC_REDIS_MTLS_URL")
  if not fromUrl.tls.enabled {
    panic("LYRIC_REDIS_MTLS_URL must be a rediss:// URL")
  }
  val withCa = ok(
    RedisLib.withExclusiveCaCertificateFile(fromUrl, requiredEnv("LYRIC_REDIS_TLS_CA")),
    "LYRIC_REDIS_TLS_CA"
  )
  ok(
    RedisLib.withClientIdentityFiles(withCa, requiredEnv("LYRIC_REDIS_TLS_CERT"), requiredEnv("LYRIC_REDIS_TLS_KEY")),
    "LYRIC_REDIS_TLS_CERT / LYRIC_REDIS_TLS_KEY"
  )
}

val tlsConformanceStores: List[RedisCache.RedisCacheStore] = newList()

func tlsConformanceStore(): Cache.CacheStore {
  val opts = RedisCache.optionsWithTtlUnit(freshPrefix(), conformanceTtlUnitMillis)
  val store = ok(RedisCache.connectWith(tlsConfig(), opts), "connectWith over TLS")
  tlsConformanceStores.add(store)
  val asInterface: Cache.CacheStore = store
  asInterface
}

test "RedisCacheStore conforms to CacheStore over TLS with a client certificate" {
  requireConformance(
    CacheConformance.run({ -> tlsConformanceStore() }, { seconds -> sleepMillis(seconds * conformanceTtlUnitMillis) })
  )
  assertTrue(tlsConformanceStores.count >= 12, "the suite built a store per case")
  for store in tlsConformanceStores {
    assertEqualInt(store.errors.errorCount(), 0, "no Redis failure was swallowed during conformance")
    val _ = ok(RedisCache.tryClear(store), "cleanup")
    store.client.close()
  }
}

test "connectWith without the client certificate or the CA does not produce a store" {
  val opts = RedisCache.options(freshPrefix())
  val fromUrl = ok(RedisLib.configFromUrl(requiredEnv("LYRIC_REDIS_MTLS_URL")), "LYRIC_REDIS_MTLS_URL")
  match RedisCache.connectWith(fromUrl, opts) {
    case Err(e) -> assertEqual(e.code(), "TLS_FAILED", "the test CA is not a system CA")
    case Ok(_) -> assertTrue(false, "an untrusted server must be refused")
  }
  match RedisCache.connect(requiredEnv("LYRIC_REDIS_MTLS_URL"), opts) {
    case Err(e) -> assertEqual(e.code(), "TLS_FAILED", "nor through connect(url)")
    case Ok(_) -> assertTrue(false, "an untrusted server must be refused")
  }
  val withCa = ok(
    RedisLib.withExclusiveCaCertificateFile(fromUrl, requiredEnv("LYRIC_REDIS_TLS_CA")),
    "LYRIC_REDIS_TLS_CA"
  )
  // The server drops a client without a certificate after the handshake, so
  // the failure is either the open or the first command.
  match RedisCache.connectWith(withCa, opts) {
    case Err(e) -> assertTrue(e.code() == "TLS_FAILED" or e.code() == "CONNECTION_LOST", "refused: " + e.code())
    case Ok(store) -> {
      match RedisCache.tryGet(store, "k") {
        case Err(e) -> assertTrue(e.code() == "TLS_FAILED" or e.code() == "CONNECTION_LOST", "refused: " + e.code())
        case Ok(_) -> assertTrue(false, "a client without a certificate must be refused")
      }
      store.client.close()
    }
  }
}

// ─── Expiry ──────────────────────────────────────────────────────────────────
test "with the production unit a TTL is real seconds, enforced by Redis" {
  val store = storeWith(RedisCache.options(freshPrefix()))
  store.set("k", "v", 1)
  match ok(Extras.lifetimeOf(store, "k"), "lifetimeOf") {
    case Expiring(ms) -> assertTrue(ms > 500i64 and ms <= 1000i64, "about one second to live")
    case _ -> assertTrue(false, "the entry has an expiry")
  }
  expectHit(store.get("k"), "v", "present before the TTL elapses")
  sleepMillis(1300)
  expectMiss(store.get("k"), "absent after one second")
  match ok(Extras.lifetimeOf(store, "k"), "lifetimeOf") {
    case Absent -> ()
    case _ -> assertTrue(false, "Redis removed the entry")
  }
  store.client.close()
}

test "lifetimeOf tells permanent, expiring and absent entries apart" {
  val store = storeWith(RedisCache.options(freshPrefix()))
  store.set("forever", "v", 0)
  store.set("hour", "v", 3600)
  match ok(Extras.lifetimeOf(store, "forever"), "lifetimeOf") {
    case Permanent -> ()
    case _ -> assertTrue(false, "ttlSeconds = 0 is permanent")
  }
  match ok(Extras.lifetimeOf(store, "hour"), "lifetimeOf") {
    case Expiring(ms) -> assertTrue(ms > 3500000i64 and ms <= 3600000i64, "about an hour")
    case _ -> assertTrue(false, "an hour-long entry is expiring")
  }
  match ok(Extras.lifetimeOf(store, "never-set"), "lifetimeOf") {
    case Absent -> ()
    case _ -> assertTrue(false, "an unset key is absent")
  }
  store.clear()
  store.client.close()
}

// ─── Key namespace ───────────────────────────────────────────────────────────
test "clear removes this store's keys and nothing else" {
  val client = newClient()
  val base = freshPrefix()
  // The second prefix extends the first with glob metacharacters: a clear of
  // either must not be confused by them.
  val plain = RedisCache.fromClient(client, RedisCache.options(base + "a:"))
  val tricky = RedisCache.fromClient(client, RedisCache.options(base + "a[*]?:"))
  val foreignKey = base + "not-a-cache-key"
  ok(RedisLib.set(client, foreignKey, "keep"), "SET of a foreign key")
  plain.set("k", "plain", 0)
  plain.set("ttl", "plain", 600)
  tricky.set("k", "tricky", 0)
  assertEqualInt(ok(Extras.entryCount(plain), "entryCount"), 2, "two entries in the plain store")
  assertEqualInt(ok(Extras.entryCount(tricky), "entryCount"), 1, "one entry in the tricky store")
  tricky.clear()
  expectMiss(tricky.get("k"), "the cleared store is empty")
  expectHit(plain.get("k"), "plain", "the other store is untouched")
  plain.clear()
  expectMiss(plain.get("k"), "the plain store is empty")
  expectMiss(plain.get("ttl"), "the entry with a TTL is gone too")
  match ok(RedisLib.get(client, foreignKey), "GET of the foreign key") {
    case Some(v) -> assertEqual(v, "keep", "a key outside every prefix survives both clears")
    case None -> assertTrue(false, "clear deleted a key outside its prefix")
  }
  val _ = ok(RedisLib.del(client, [foreignKey]), "DEL")
  client.close()
}

test "clear empties a store larger than one SCAN page" {
  val store = storeWith(RedisCache.options(freshPrefix()))
  var i = 0
  while i < 1300 {
    store.set("k" + i.toString(), "v", 0)
    i = i + 1
  }
  assertEqualInt(ok(Extras.entryCount(store), "entryCount"), 1300, "every entry was stored")
  assertEqualLong(ok(RedisCache.tryClear(store), "tryClear"), 1300i64, "every entry was deleted")
  assertEqualInt(ok(Extras.entryCount(store), "entryCount"), 0, "the store is empty")
  assertEqualInt(store.errors.errorCount(), 0, "nothing failed")
  store.client.close()
}

test "entries are plain Redis strings under the prefix" {
  val client = newClient()
  val prefix = freshPrefix()
  val store = RedisCache.fromClient(client, RedisCache.options(prefix))
  store.set("users:42", "{\"name\":\"Ada\"}", 0)
  match ok(RedisLib.get(client, prefix + "users:42"), "GET") {
    case Some(v) -> assertEqual(v, "{\"name\":\"Ada\"}", "the raw value")
    case None -> assertTrue(false, "the entry is stored at prefix + key")
  }
  ok(RedisLib.set(client, prefix + "written-by-another-client", "x"), "SET")
  expectHit(store.get("written-by-another-client"), "x", "the store reads keys other clients wrote under its prefix")
  store.clear()
  client.close()
}

// ─── Extras ──────────────────────────────────────────────────────────────────
test "setIfAbsent stores once" {
  val store = storeWith(RedisCache.options(freshPrefix()))
  assertTrue(ok(Extras.setIfAbsent(store, "lock", "first", 30), "setIfAbsent"), "the first caller stores")
  assertFalse(ok(Extras.setIfAbsent(store, "lock", "second", 30), "setIfAbsent"), "the second caller does not")
  expectHit(store.get("lock"), "first", "the first value stays")
  match ok(Extras.lifetimeOf(store, "lock"), "lifetimeOf") {
    case Expiring(_) -> ()
    case _ -> assertTrue(false, "the lock carries its TTL")
  }
  store.delete("lock")
  assertTrue(ok(Extras.setIfAbsent(store, "lock", "third", 0), "setIfAbsent"), "free again after delete")
  store.clear()
  store.client.close()
}

test "tryDelete reports whether the key existed" {
  val store = storeWith(RedisCache.options(freshPrefix()))
  store.set("k", "v", 0)
  assertTrue(ok(RedisCache.tryDelete(store, "k"), "tryDelete"), "the key existed")
  assertFalse(ok(RedisCache.tryDelete(store, "k"), "tryDelete"), "the key is gone")
  store.client.close()
}

// ─── Failure and recovery ────────────────────────────────────────────────────
test "a store survives its connection being dropped" {
  val store = storeWith(RedisCache.options(freshPrefix()))
  store.set("k", "v", 0)
  store.client.disconnect()
  expectHit(store.get("k"), "v", "the next call reconnects")
  assertEqualInt(store.errors.errorCount(), 0, "no failure was recorded")
  store.clear()
  store.client.close()
}

test "a closed client turns reads into recorded misses" {
  val store = storeWith(RedisCache.options(freshPrefix()))
  store.set("k", "v", 0)
  val cleaner = RedisCache.fromClient(newClient(), store.options)
  store.client.close()
  expectMiss(store.get("k"), "a closed client cannot read")
  match store.errors.lastError() {
    case Some(e) -> assertEqual(e.code(), "CLIENT_CLOSED", "the recorded error")
    case None -> assertTrue(false, "the failure must be recorded")
  }
  cleaner.clear()
  cleaner.client.close()
}

// ─── Thread safety ───────────────────────────────────────────────────────────
func writeMany(store: in RedisCache.RedisCacheStore, worker: in Int, count: in Int): Unit {
  var i = 0
  while i < count {
    store.set("w" + worker.toString() + ":" + i.toString(), worker.toString(), 0)
    i = i + 1
  }
}

test "one store shared by six threads stores every entry" {
  val store = storeWith(RedisCache.options(freshPrefix()))
  val workers: List[KT.Background] = newList()
  var w = 0
  while w < 6 {
    val id = w
    workers.add(KT.startBackground({ -> writeMany(store, id, 100) }))
    w = w + 1
  }
  for worker in workers {
    KT.waitBackground(worker)
  }
  assertEqualInt(store.errors.errorCount(), 0, "no write failed")
  assertEqualInt(ok(Extras.entryCount(store), "entryCount"), 600, "every write landed")
  expectHit(store.get("w5:99"), "5", "an entry written by the last worker")
  store.clear()
  store.client.close()
}
