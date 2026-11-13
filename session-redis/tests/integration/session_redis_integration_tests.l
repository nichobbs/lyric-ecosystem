//! tests/integration/session_redis_integration_tests.l - the `SessionStore`
//! conformance suite and the backend's own behaviour against a real Redis.
//!
//! Needs `LYRIC_REDIS_URL` and is compiled only with `--features integration`:
//!
//!   LYRIC_REDIS_URL=redis://127.0.0.1:16379 \
//!     lyric test --manifest session-redis/lyric.toml --target dotnet --features integration
//!
//! The conformance suite runs a further time over TLS with a client
//! certificate, against the server `scripts/ci/start-redis-tls.sh` starts:
//! `LYRIC_REDIS_MTLS_URL` (a `rediss://` URL), `LYRIC_REDIS_TLS_CA`,
//! `LYRIC_REDIS_TLS_CERT` and `LYRIC_REDIS_TLS_KEY` (PEM files).
//!
//! Every session is written under a random prefix and removed again.
@cfg(feature = "integration")
@test_module
package Session.Redis.IntegrationTests

import Std.Core
import Std.Collections
import Std.Environment as Env
import Std.Testing
import Std.Time
import Std.Uuid
import Session
import Session.Redis as RedisSessions
import Session.Redis.Admin as Admin
import Redis as RedisLib
import Net.Tcp.Testing as KT
import Testing.Conformance
import Testing.Conformance.Session as SessionConformance

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
  "lyric-session-it:" + runId + ":" + uuidToString(newUuid()) + ":"
}

func newClient(): RedisLib.RedisClient {
  match RedisLib.connect(serverUrl()) {
    case Ok(c) -> c
    case Err(e) -> panic("cannot connect to LYRIC_REDIS_URL: " + e.message())
  }
}

func storeWithTtl(ttlSeconds: in Int): RedisSessions.RedisSessionStore {
  RedisSessions.fromClient(newClient(), RedisSessions.options(freshPrefix(), ttlSeconds))
}

func ok[T](r: in Result[T, SessionError], what: in String): T {
  match r {
    case Ok(v) -> v
    case Err(e) -> panic(what + " failed: " + e.code + ": " + e.message)
  }
}

func redisOk[T](r: in Result[T, RedisLib.RedisError], what: in String): T {
  match r {
    case Ok(v) -> v
    case Err(e) -> panic(what + " failed: " + e.message())
  }
}

func remaining(store: in RedisSessions.RedisSessionStore, id: in String): Long {
  match ok(Admin.remainingMillis(store, id), "remainingMillis") {
    case Some(ms) -> ms
    case None -> 0i64 - 1i64
  }
}

func loaded(store: in RedisSessions.RedisSessionStore, id: in String): SessionData {
  match ok(store.load(id), "load") {
    case Some(d) -> d
    case None -> panic("the session must load")
  }
}

func finish(store: in RedisSessions.RedisSessionStore): Unit {
  val _ = ok(Admin.destroyAll(store), "cleanup")
  store.client.close()
}

// ─── Conformance ─────────────────────────────────────────────────────────────
val conformanceStores: List[RedisSessions.RedisSessionStore] = newList()

func conformanceStore(): SessionStore {
  val store = storeWithTtl(600)
  conformanceStores.add(store)
  val asInterface: SessionStore = store
  asInterface
}

test "RedisSessionStore conforms to SessionStore" {
  requireConformance(SessionConformance.run({ -> conformanceStore() }))
  assertTrue(conformanceStores.count >= 15, "the suite built a store per case")
  for store in conformanceStores {
    finish(store)
  }
}

val resp3Stores: List[RedisSessions.RedisSessionStore] = newList()

func resp3Store(): SessionStore {
  val cfg = match RedisLib.configFromUrl(serverUrl()) {
    case Ok(c) -> RedisLib.withProtocol(c, 3)
    case Err(e) -> panic("LYRIC_REDIS_URL is not usable: " + e.message())
  }
  val store = RedisSessions.fromClient(
    redisOk(RedisLib.open(cfg), "open over RESP3"),
    RedisSessions.options(freshPrefix(), 600)
  )
  resp3Stores.add(store)
  val asInterface: SessionStore = store
  asInterface
}

test "RedisSessionStore conforms to SessionStore over RESP3" {
  requireConformance(SessionConformance.run({ -> resp3Store() }))
  assertTrue(resp3Stores.count >= 15, "the suite built a store per case")
  for store in resp3Stores {
    finish(store)
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

// The server that requires a client certificate, with the custom CA and
// without the client identity.
func tlsConfigWithoutIdentity(): RedisLib.RedisConfig {
  val fromUrl = redisOk(RedisLib.configFromUrl(requiredEnv("LYRIC_REDIS_MTLS_URL")), "LYRIC_REDIS_MTLS_URL")
  if not fromUrl.tls.enabled {
    panic("LYRIC_REDIS_MTLS_URL must be a rediss:// URL")
  }
  redisOk(RedisLib.withExclusiveCaCertificateFile(fromUrl, requiredEnv("LYRIC_REDIS_TLS_CA")), "LYRIC_REDIS_TLS_CA")
}

func tlsConfig(): RedisLib.RedisConfig {
  redisOk(
    RedisLib.withClientIdentityFiles(
      tlsConfigWithoutIdentity(),
      requiredEnv("LYRIC_REDIS_TLS_CERT"),
      requiredEnv("LYRIC_REDIS_TLS_KEY")
    ),
    "LYRIC_REDIS_TLS_CERT / LYRIC_REDIS_TLS_KEY"
  )
}

val tlsStores: List[RedisSessions.RedisSessionStore] = newList()

func tlsStore(): SessionStore {
  val store = ok(
    RedisSessions.connectWith(tlsConfig(), RedisSessions.options(freshPrefix(), 600)),
    "connectWith over TLS"
  )
  tlsStores.add(store)
  val asInterface: SessionStore = store
  asInterface
}

test "RedisSessionStore conforms to SessionStore over TLS with a client certificate" {
  requireConformance(SessionConformance.run({ -> tlsStore() }))
  assertTrue(tlsStores.count >= 15, "the suite built a store per case")
  for store in tlsStores {
    finish(store)
  }
}

test "connectWith without the CA or the client certificate does not produce a working store" {
  val opts = RedisSessions.options(freshPrefix(), 600)
  match RedisSessions.connect(requiredEnv("LYRIC_REDIS_MTLS_URL"), opts) {
    case Err(e) -> {
      assertEqual(e.code, "CONNECT_FAILED", "the store's connect code")
      assertTrue(e.message.startsWith("Redis TLS handshake failed"), "the test CA is not a system CA: " + e.message)
    }
    case Ok(_) -> assertTrue(false, "an untrusted server must be refused")
  }
  // The server drops a client without a certificate after the handshake, so
  // the failure is either the connect or the first operation.
  match RedisSessions.connectWith(tlsConfigWithoutIdentity(), opts) {
    case Err(e) -> assertEqual(e.code, "CONNECT_FAILED", "refused at connect")
    case Ok(store) -> {
      match store.create() {
        case Err(e) -> assertEqual(e.code, "CREATE_FAILED", "refused at the first operation")
        case Ok(_) -> assertTrue(false, "a client without a certificate must be refused")
      }
      store.client.close()
    }
  }
}

// ─── Expiry ──────────────────────────────────────────────────────────────────
test "create gives the session the store's TTL and load reports it" {
  val store = storeWithTtl(120)
  val before = nowEpochMillis()
  val id = ok(store.create(), "create")
  val ms = remaining(store, id)
  assertTrue(ms > 110000i64 and ms <= 120000i64, "about 120 seconds to live")
  val data = loaded(store, id)
  assertTrue(data.createdAt >= before and data.createdAt <= nowEpochMillis(), "createdAt is now")
  assertEqualLong(data.lastAccessedAt, data.createdAt, "lastAccessedAt starts at createdAt")
  assertTrue(
    data.expiresAt > before + 110000i64 and data.expiresAt <= nowEpochMillis() + 120000i64,
    "expiresAt is about 120 seconds ahead"
  )
  finish(store)
}

test "set, delete and clear keep the expiry; touch renews it" {
  val store = storeWithTtl(100)
  val id = ok(store.create(), "create")
  redisOk(RedisLib.pexpire(store.client, RedisSessions.redisKey(store.options, id), 20000i64), "PEXPIRE")
  ok(store.set(id, "a", "1"), "set")
  ok(store.set(id, "b", "2"), "set")
  ok(store.delete(id, "a"), "delete")
  ok(store.clear(id), "clear")
  val afterWrites = remaining(store, id)
  assertTrue(afterWrites > 0i64 and afterWrites <= 20000i64, "the writes did not extend the expiry")
  ok(store.touch(id), "touch")
  val afterTouch = remaining(store, id)
  assertTrue(afterTouch > 90000i64 and afterTouch <= 100000i64, "touch renewed the expiry to the store's TTL")
  finish(store)
}

test "writes and touch advance lastAccessedAt and keep createdAt" {
  val store = storeWithTtl(100)
  val id = ok(store.create(), "create")
  val created = loaded(store, id)
  sleepMillis(30)
  ok(store.set(id, "a", "1"), "set")
  val afterSet = loaded(store, id)
  assertEqualLong(afterSet.createdAt, created.createdAt, "createdAt is unchanged by set")
  assertTrue(afterSet.lastAccessedAt > created.lastAccessedAt, "set advanced lastAccessedAt")
  sleepMillis(30)
  ok(store.touch(id), "touch")
  val afterTouch = loaded(store, id)
  assertTrue(afterTouch.lastAccessedAt > afterSet.lastAccessedAt, "touch advanced lastAccessedAt")
  assertEqualInt(afterTouch.entries.count, 1, "touch kept the entry")
  finish(store)
}

test "an expired session is gone: load is None and set cannot revive it" {
  val store = storeWithTtl(1)
  val id = ok(store.create(), "create")
  ok(store.set(id, "user", "alice"), "set")
  sleepMillis(1400)
  match ok(store.load(id), "load") {
    case None -> ()
    case Some(_) -> assertTrue(false, "the session expired one second after create")
  }
  match ok(store.get(id, "user"), "get") {
    case None -> ()
    case Some(_) -> assertTrue(false, "an expired session has no entries")
  }
  match store.set(id, "user", "mallory") {
    case Err(e) -> assertEqual(e.code, "SESSION_NOT_FOUND", "set on an expired session")
    case Ok(_) -> assertTrue(false, "set must not revive an expired session")
  }
  ok(store.touch(id), "touch of an expired session")
  match ok(store.load(id), "load") {
    case None -> ()
    case Some(_) -> assertTrue(false, "touch must not revive an expired session")
  }
  assertEqualInt(ok(Admin.sessionCount(store), "sessionCount"), 0, "nothing is left in Redis")
  finish(store)
}

test "save honours expiresAt: a future time, no expiry, and a time already past" {
  val store = storeWithTtl(100)
  val entries: Map[String, String] = newMap()
  entries["k"] = "v"
  val future = RedisSessions.newSessionId()
  val now = nowEpochMillis()
  ok(
    store.save(
      SessionData(id = future, entries = entries, createdAt = 5i64, lastAccessedAt = 6i64, expiresAt = now + 40000i64)
    ),
    "save with a future expiry"
  )
  val ms = remaining(store, future)
  assertTrue(ms > 30000i64 and ms <= 40000i64, "the expiry is the one saved")
  val data = loaded(store, future)
  assertEqualLong(data.createdAt, 5i64, "createdAt round-trips")
  assertEqualLong(data.lastAccessedAt, 6i64, "lastAccessedAt round-trips")
  assertEqual(data.entries["k"], "v", "the entry round-trips")
  val forever = RedisSessions.newSessionId()
  ok(
    store.save(SessionData(id = forever, entries = entries, createdAt = 5i64, lastAccessedAt = 6i64, expiresAt = 0i64)),
    "save without an expiry"
  )
  assertEqualLong(remaining(store, forever), 0i64, "expiresAt = 0 is no expiry")
  assertEqualLong(loaded(store, forever).expiresAt, 0i64, "load reports no expiry as 0")
  val past = RedisSessions.newSessionId()
  ok(
    store.save(
      SessionData(id = past, entries = entries, createdAt = 5i64, lastAccessedAt = 6i64, expiresAt = now - 1000i64)
    ),
    "save with a past expiry"
  )
  match ok(store.load(past), "load") {
    case None -> ()
    case Some(_) -> assertTrue(false, "a session saved as already expired does not load")
  }
  finish(store)
}

test "load then save keeps the remaining lifetime" {
  val store = storeWithTtl(100)
  val id = ok(store.create(), "create")
  redisOk(RedisLib.pexpire(store.client, RedisSessions.redisKey(store.options, id), 30000i64), "PEXPIRE")
  val data = loaded(store, id)
  val entries: Map[String, String] = data.entries
  entries["cart"] = "3 items"
  ok(store.save(data), "save")
  val ms = remaining(store, id)
  assertTrue(ms > 25000i64 and ms <= 30500i64, "the round trip did not extend the session")
  match ok(store.get(id, "cart"), "get") {
    case Some(v) -> assertEqual(v, "3 items", "the saved entry")
    case None -> assertTrue(false, "the saved entry must be there")
  }
  finish(store)
}

// ─── Contents ────────────────────────────────────────────────────────────────
test "keys and values round-trip verbatim" {
  val store = storeWithTtl(100)
  val id = ok(store.create(), "create")
  val pairs: Map[String, String] = newMap()
  pairs[""] = "empty key"
  pairs["empty value"] = ""
  pairs["d:looks-like-a-field"] = "v1"
  pairs["m:created"] = "not metadata"
  pairs["café 世界"] = "  spaced \t and\r\nnewline  "
  pairs["json"] = "{\"a\":[1,2,3]}"
  for k in mapKeys(pairs) {
    ok(store.set(id, k, pairs[k]), "set")
  }
  val data = loaded(store, id)
  assertEqualInt(data.entries.count, pairs.count, "every entry, and only the entries")
  for k in mapKeys(pairs) {
    assertEqual(data.entries[k], pairs[k], "entry via load")
    match ok(store.get(id, k), "get") {
      case Some(v) -> assertEqual(v, pairs[k], "entry via get")
      case None -> assertTrue(false, "the entry must be there")
    }
  }
  assertTrue(data.createdAt > 1000000000000i64, "an entry named m:created does not overwrite the metadata")
  ok(store.clear(id), "clear")
  assertEqualInt(loaded(store, id).entries.count, 0, "clear removed every entry")
  assertTrue(loaded(store, id).createdAt == data.createdAt, "clear kept the metadata")
  finish(store)
}

test "a session with many large entries round-trips through save and load" {
  val store = storeWithTtl(100)
  val id = ok(store.create(), "create")
  var big = "0123456789abcdef"
  while big.length < 20000 {
    big = big + big
  }
  val entries: Map[String, String] = newMap()
  var i = 0
  while i < 150 {
    entries["key-" + i.toString()] = big + i.toString()
    i = i + 1
  }
  ok(
    store.save(
      SessionData(
        id = id,
        entries = entries,
        createdAt = 1i64,
        lastAccessedAt = 2i64,
        expiresAt = nowEpochMillis() + 60000i64
      )
    ),
    "save"
  )
  val data = loaded(store, id)
  assertEqualInt(data.entries.count, 150, "every entry")
  assertTrue(data.entries["key-149"] == big + "149", "the last entry is intact")
  finish(store)
}

// ─── Layout and isolation ────────────────────────────────────────────────────
test "a session is one Redis hash under the prefix" {
  val store = storeWithTtl(100)
  val id = ok(store.create(), "create")
  ok(store.set(id, "user", "alice"), "set")
  val raw = redisOk(RedisLib.hgetAll(store.client, RedisSessions.redisKey(store.options, id)), "HGETALL")
  assertEqualInt(raw.count, 3, "two metadata fields and one entry")
  assertEqual(raw["d:user"], "alice", "the entry field")
  assertTrue(raw.containsKey("m:created") and raw.containsKey("m:accessed"), "the metadata fields")
  finish(store)
}

test "stores with different prefixes do not see each other's sessions" {
  val client = newClient()
  val a = RedisSessions.fromClient(client, RedisSessions.options(freshPrefix(), 100))
  val b = RedisSessions.fromClient(client, RedisSessions.options(freshPrefix(), 100))
  val id = ok(a.create(), "create in a")
  ok(a.set(id, "k", "from-a"), "set in a")
  match ok(b.load(id), "load in b") {
    case None -> ()
    case Some(_) -> assertTrue(false, "b must not see a's session")
  }
  match b.set(id, "k", "from-b") {
    case Err(e) -> assertEqual(e.code, "SESSION_NOT_FOUND", "b cannot write a's session")
    case Ok(_) -> assertTrue(false, "b must not write a's session")
  }
  val idB = ok(b.create(), "create in b")
  assertEqualLong(ok(Admin.destroyAll(b), "destroyAll in b"), 1i64, "b removed its one session")
  match ok(b.load(idB), "load in b") {
    case None -> ()
    case Some(_) -> assertTrue(false, "destroyAll removed b's session")
  }
  match ok(a.get(id, "k"), "get in a") {
    case Some(v) -> assertEqual(v, "from-a", "a's session survives b's destroyAll")
    case None -> assertTrue(false, "a's session must survive")
  }
  assertEqualInt(ok(Admin.sessionCount(a), "sessionCount"), 1, "a holds one session")
  val _ = ok(Admin.destroyAll(a), "cleanup")
  client.close()
}

test "a key of another Redis type under the prefix is an error, not a session" {
  val store = storeWithTtl(100)
  val id = RedisSessions.newSessionId()
  redisOk(RedisLib.set(store.client, RedisSessions.redisKey(store.options, id), "a plain string"), "SET")
  match store.load(id) {
    case Err(e) -> assertEqual(e.code, "LOAD_FAILED", "load of a non-hash key")
    case Ok(_) -> assertTrue(false, "a string key is not a session")
  }
  match store.set(id, "k", "v") {
    case Err(e) -> assertEqual(e.code, "SET_FAILED", "set on a non-hash key")
    case Ok(_) -> assertTrue(false, "set must not write into a non-hash key")
  }
  ok(store.destroy(id), "destroy removes it")
  finish(store)
}

// ─── Atomicity ───────────────────────────────────────────────────────────────
func setMany(
  store: in RedisSessions.RedisSessionStore,
  id: in String,
  worker: in Int,
  count: in Int,
  failures: in List[String],
): Unit {
  var i = 0
  while i < count {
    match store.set(id, "w" + worker.toString() + "-" + i.toString(), worker.toString()) {
      case Ok(_) -> ()
      case Err(e) -> failures.add(e.code)
    }
    i = i + 1
  }
}

test "concurrent sets on one session from separate connections lose no update" {
  val owner = storeWithTtl(100)
  val id = ok(owner.create(), "create")
  val failures: List[String] = newList()
  val stores: List[RedisSessions.RedisSessionStore] = newList()
  val workers: List[KT.Background] = newList()
  var w = 0
  while w < 6 {
    val store = RedisSessions.fromClient(newClient(), owner.options)
    stores.add(store)
    val worker = w
    workers.add(KT.startBackground({ -> setMany(store, id, worker, 60, failures) }))
    w = w + 1
  }
  for worker in workers {
    KT.waitBackground(worker)
  }
  assertEqualInt(failures.count, 0, "no set failed")
  assertEqualInt(loaded(owner, id).entries.count, 360, "every key from every connection is present")
  for store in stores {
    store.client.close()
  }
  finish(owner)
}

func setUntilGone(store: in RedisSessions.RedisSessionStore, id: in String, outcomes: in List[String]): Unit {
  var i = 0
  var gone = false
  while i < 400 and not gone {
    match store.set(id, "k", i.toString()) {
      case Ok(_) -> ()
      case Err(e) -> {
        outcomes.add(e.code)
        gone = true
      }
    }
    i = i + 1
  }
}

test "a set racing a destroy never recreates the session" {
  var round = 0
  while round < 10 {
    val owner = storeWithTtl(100)
    val writer = RedisSessions.fromClient(newClient(), owner.options)
    val id = ok(owner.create(), "create")
    val outcomes: List[String] = newList()
    val job = KT.startBackground({ -> setUntilGone(writer, id, outcomes) })
    sleepMillis(5)
    ok(owner.destroy(id), "destroy")
    KT.waitBackground(job)
    match ok(owner.load(id), "load") {
      case None -> ()
      case Some(_) -> assertTrue(false, "a set after destroy recreated the session")
    }
    for code in outcomes {
      assertEqual(code, "SESSION_NOT_FOUND", "the only failure a racing set may see")
    }
    assertEqualInt(ok(Admin.sessionCount(owner), "sessionCount"), 0, "nothing is left in Redis")
    writer.client.close()
    finish(owner)
    round = round + 1
  }
}

// ─── Failure and recovery ────────────────────────────────────────────────────
test "a store survives its connection being dropped" {
  val store = storeWithTtl(100)
  val id = ok(store.create(), "create")
  ok(store.set(id, "k", "v"), "set")
  store.client.disconnect()
  match ok(store.get(id, "k"), "get after the drop") {
    case Some(v) -> assertEqual(v, "v", "the next call reconnects")
    case None -> assertTrue(false, "the entry must be there")
  }
  finish(store)
}

test "a closed client turns every operation into an Err" {
  val store = storeWithTtl(100)
  val id = ok(store.create(), "create")
  val cleaner = RedisSessions.fromClient(newClient(), store.options)
  store.client.close()
  match store.create() {
    case Err(e) -> assertEqual(e.code, "CREATE_FAILED", "create")
    case Ok(_) -> assertTrue(false, "create must fail")
  }
  match store.load(id) {
    case Err(e) -> assertEqual(e.code, "LOAD_FAILED", "load")
    case Ok(_) -> assertTrue(false, "load must fail")
  }
  match store.set(id, "k", "v") {
    case Err(e) -> assertEqual(e.code, "SET_FAILED", "set")
    case Ok(_) -> assertTrue(false, "set must fail")
  }
  finish(cleaner)
}
