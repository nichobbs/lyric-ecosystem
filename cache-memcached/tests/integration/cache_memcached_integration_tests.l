//! tests/integration/cache_memcached_integration_tests.l - the `CacheStore`
//! conformance suite and the backend's own behaviour against real memcached
//! servers.
//!
//! Compiled only with `--features integration`, and needs the three servers
//! `scripts/ci/start-memcached.sh` starts:
//!
//!   eval "$(scripts/ci/start-memcached.sh)"
//!   lyric test --manifest cache-memcached/lyric.toml --target dotnet --features integration
//!
//!   LYRIC_MEMCACHED_URL       a plain server (`memcached://127.0.0.1:19960`)
//!   LYRIC_MEMCACHED_TLS_URL   a server with TLS (`memcached+tls://localhost:19961`)
//!   LYRIC_MEMCACHED_TLS_CA    the PEM file of the CA of its certificate
//!   LYRIC_MEMCACHED_AUTH_URL  a server with an authentication file, with the
//!                             credentials (`memcached://lyric:...@127.0.0.1:19962`).
//!                             The suite runs `flush_all` on THIS server: it
//!                             must be a disposable one.
//!
//! Every test works in namespaces with a random name. Nothing but the
//! `flush_all` test removes other items, and the entries of the namespaces
//! are orphaned by a final `clear()`, which is all memcached allows.
//!
//! Time: memcached counts expiry in whole seconds on its own clock, which
//! cannot be set, so the conformance suite's clock is a real sleep: one
//! second per second the suite advances, 26 in all (the suite has no shorter
//! path: its expiry case stores with a TTL of 10 and looks before and after).
//! It runs twice, plain and over TLS.
@cfg(feature = "integration")
@test_module
package Cache.Memcached.IntegrationTests

import Std.Core
import Std.Collections
import Std.Encoding
import Std.Environment as Env
import Std.File as File
import Std.String as Str
import Std.Testing
import Std.Time
import Std.Uuid
import Cache
import Cache.Memcached as MemcachedCache
import Cache.Memcached.Client as Client
import Cache.Memcached.Extras as Extras
import Cache.Memcached.Keys as Keys
import Cache.Memcached.Meta as Meta
import Net.Tls as NetTls
import Testing.Conformance
import Testing.Conformance.Cache as CacheConformance

func requiredEnv(name: in String): String {
  val value = Env.getVarOrDefault(name, "")
  if value.length == 0 {
    panic(
      name +
        " is not set: the integration tests need memcached servers; run scripts/ci/start-memcached.sh, which starts them and prints LYRIC_MEMCACHED_URL, LYRIC_MEMCACHED_TLS_URL, LYRIC_MEMCACHED_TLS_CA and LYRIC_MEMCACHED_AUTH_URL"
    )
  }
  value
}

func serverUrl(): String {
  requiredEnv("LYRIC_MEMCACHED_URL")
}

val runId: String = uuidToString(newUuid())

func freshNamespace(): String {
  "lyric-it:" + Str.substring(runId, 0, 8) + ":" + uuidToString(newUuid())
}

func ok[T](r: in Result[T, Client.MemcachedError], what: in String): T {
  match r {
    case Ok(v) -> v
    case Err(e) -> panic(what + " failed: " + e.message())
  }
}

func codeOf[T](r: in Result[T, Client.MemcachedError], what: in String): String {
  match r {
    case Ok(_) -> "OK (" + what + " did not fail)"
    case Err(e) -> e.code()
  }
}

func messageOf[T](r: in Result[T, Client.MemcachedError]): String {
  match r {
    case Ok(_) -> ""
    case Err(e) -> e.message()
  }
}

// Not generic: see "Known gaps" in the README.
func okReplies(r: in Result[List[Meta.MetaReply], Client.MemcachedError], what: in String): List[Meta.MetaReply] {
  match r {
    case Ok(v) -> v
    case Err(e) -> panic(what + " failed: " + e.message())
  }
}

func statuses(replies: in List[Meta.MetaReply]): String {
  val names: List[String] = newList()
  for reply in replies {
    names.add(Meta.statusOf(reply))
  }
  Str.joinList(" ", names)
}

func newClient(): Client.MemcachedClient {
  match Client.connect(serverUrl()) {
    case Ok(c) -> c
    case Err(e) -> panic("cannot connect to LYRIC_MEMCACHED_URL: " + e.message())
  }
}

func newStore(namespace: in String): MemcachedCache.MemcachedCacheStore {
  MemcachedCache.fromClient(newClient(), MemcachedCache.options(namespace))
}

func expectHit(v: in Option[String], expected: in String, what: in String): Unit {
  match v {
    case Some(s) -> assertTrue(s == expected, what + ": another value was read")
    case None -> assertTrue(false, what + ": expected a hit")
  }
}

func expectMiss(v: in Option[String], what: in String): Unit {
  match v {
    case Some(_) -> assertTrue(false, what + ": expected a miss")
    case None -> ()
  }
}

func finish(store: in MemcachedCache.MemcachedCacheStore): Unit {
  val _ = MemcachedCache.tryClear(store)
  store.client.close()
}

// ─── Conformance ─────────────────────────────────────────────────────────────
// memcached's clock cannot be moved: the suite's seconds are real ones.
func waitSeconds(seconds: in Int): Unit {
  sleepMillis(seconds * 1000)
}

test "MemcachedCacheStore conforms to CacheStore on a real memcached" {
  val stores: List[MemcachedCache.MemcachedCacheStore] = newList()
  val factory: () -> Cache.CacheStore = { ->
    val store = newStore(freshNamespace())
    stores.add(store)
    val asInterface: Cache.CacheStore = store
    asInterface
  }
  requireConformance(CacheConformance.run(factory, { seconds -> waitSeconds(seconds) }))
  assertTrue(stores.count >= 12, "the suite built a store per case")
  for store in stores {
    assertEqualInt(store.errors.errorCount(), 0, "no memcached failure was swallowed during conformance")
    finish(store)
  }
}

// ─── Conformance over TLS ────────────────────────────────────────────────────
func tlsConfig(): Client.MemcachedConfig {
  val fromUrl = ok(Client.configFromUrl(requiredEnv("LYRIC_MEMCACHED_TLS_URL")), "LYRIC_MEMCACHED_TLS_URL")
  if not fromUrl.tls.enabled {
    panic("LYRIC_MEMCACHED_TLS_URL must be a memcached+tls:// URL")
  }
  val caPath = requiredEnv("LYRIC_MEMCACHED_TLS_CA")
  val caPem = match File.readText(caPath) {
    case Ok(text) -> text
    case Err(_) -> panic("cannot read the file named by LYRIC_MEMCACHED_TLS_CA: " + caPath)
  }
  Client.withTlsOptions(fromUrl, fromUrl.tls.withExclusiveCaCertificate(caPem))
}

test "MemcachedCacheStore conforms to CacheStore over TLS" {
  val stores: List[MemcachedCache.MemcachedCacheStore] = newList()
  val factory: () -> Cache.CacheStore = { ->
    val store = ok(
      MemcachedCache.connectWith(tlsConfig(), MemcachedCache.options(freshNamespace())),
      "connectWith over TLS"
    )
    stores.add(store)
    val asInterface: Cache.CacheStore = store
    asInterface
  }
  requireConformance(CacheConformance.run(factory, { seconds -> waitSeconds(seconds) }))
  assertTrue(stores.count >= 12, "the suite built a store per case")
  for store in stores {
    assertEqualInt(store.errors.errorCount(), 0, "no memcached failure was swallowed during conformance")
    finish(store)
  }
}

test "without the CA the TLS server is not trusted, and TLS 1.3 can be required" {
  val url = requiredEnv("LYRIC_MEMCACHED_TLS_URL")
  assertEqual(codeOf(Client.connect(url), "connect"), "TLS_FAILED", "the test CA is not a system CA")
  assertEqual(
    codeOf(MemcachedCache.connect(url, MemcachedCache.options(freshNamespace())), "connect"),
    "TLS_FAILED",
    "nor for a store"
  )
  val trusted = tlsConfig()
  val modern = Client.withTlsOptions(trusted, trusted.tls.withMinTlsVersion(Std.Tls.TlsVersion.Tls13))
  val client = ok(Client.open(modern), "TLS 1.3 only")
  ok(Client.ping(client), "ping over TLS 1.3")
  client.close()
  val renamed = Client.withTlsOptions(trusted, trusted.tls.withTlsServerName("cache.example"))
  assertEqual(codeOf(Client.open(renamed), "open"), "TLS_FAILED", "the certificate does not name cache.example")
}

// ─── The server ──────────────────────────────────────────────────────────────
test "the server answers the liveness check and is recent enough" {
  val client = newClient()
  ok(Client.ping(client), "mn")
  val version = ok(Client.serverVersion(client), "version")
  assertTrue(Client.versionSupported(version), "memcached " + version + " is at least " + Client.minimumServerVersion())
  assertEqual(client.serverVersion(), version, "the version read at the handshake")
  val store = MemcachedCache.fromClient(client, MemcachedCache.options(freshNamespace()))
  ok(MemcachedCache.ping(store), "ping through the store")
  client.close()
}

test "memcached refuses what the fake server refuses, in the same words" {
  val client = newClient()
  val tooLong = Keys.wireKey(Str.repeat("k", 187))
  val long = client.run(Meta.encodeSet(tooLong, encodeUtf8("x"), ""), 1)
  assertTrue(Str.contains(messageOfRun(long), "bad command line format"), "a key over 250 bytes")
  assertFalse(client.isConnected(), "the connection is closed after a CLIENT_ERROR")
  val longest = Keys.wireKey(Str.repeat("k", 186))
  assertEqual(
    statuses(okReplies(client.run(Meta.encodeSet(longest, encodeUtf8("x"), "T5"), 1), "ms")),
    "HD",
    "186 bytes fit"
  )
  assertTrue(
    Str.contains(messageOfRun(client.run(encodeUtf8("mg !!! b v\r\n"), 1)), "error decoding key"),
    "bad base64"
  )
  assertTrue(Str.contains(messageOfRun(client.run(encodeUtf8("mg a b Z\r\n"), 1)), "invalid flag"), "an unknown flag")
  assertEqual(codeOfRun(client.run(encodeUtf8("bogus\r\n"), 1)), "REQUEST_REFUSED", "an unknown command")
  val chunk = client.run(encodeUtf8("ms " + Keys.wireKey("k") + " 2 b\r\nxyzzy\r\n"), 1)
  assertTrue(Str.contains(messageOfRun(chunk), "bad data chunk"), "data longer than announced")
  ok(Client.ping(client), "the client recovers every time")
  client.close()
}

func messageOfRun(r: in Result[List[Meta.MetaReply], Client.MemcachedError]): String {
  match r {
    case Ok(_) -> ""
    case Err(e) -> e.message()
  }
}

func codeOfRun(r: in Result[List[Meta.MetaReply], Client.MemcachedError]): String {
  match r {
    case Ok(_) -> "OK"
    case Err(e) -> e.code()
  }
}

// ─── Expiry ──────────────────────────────────────────────────────────────────
test "a TTL above 30 days is counted as that many seconds by memcached" {
  val store = newStore(freshNamespace())
  val fortyDays = 40 * 86400
  ok(MemcachedCache.trySet(store, "long", "v", fortyDays), "trySet")
  expectHit(store.get("long"), "v", "the entry is there, not expired in 1970")
  match ok(Extras.lifetimeOf(store, "long"), "lifetimeOf") {
    case Extras.Expiring(seconds) -> assertTrue(
      seconds > fortyDays.toLong() - 30i64 and seconds <= fortyDays.toLong() + 30i64,
      "about 40 days remain: " + seconds.toString()
    )
    case _ -> assertTrue(false, "the entry has an expiry")
  }
  ok(MemcachedCache.trySet(store, "month", "v", 2592000), "trySet")
  match ok(Extras.lifetimeOf(store, "month"), "lifetimeOf") {
    case Extras.Expiring(seconds) -> assertTrue(
      seconds > 2592000i64 - 30i64 and seconds <= 2592000i64,
      "exactly 30 days is still relative: " + seconds.toString()
    )
    case _ -> assertTrue(false, "the entry has an expiry")
  }
  ok(MemcachedCache.trySet(store, "eternal", "v", 2147483647), "trySet")
  expectHit(store.get("eternal"), "v", "the longest TTL is not wrapped into the past")
  match ok(Extras.lifetimeOf(store, "eternal"), "lifetimeOf") {
    case Extras.Expiring(seconds) -> assertTrue(seconds > 86400i64 * 365i64, "years to live: " + seconds.toString())
    case _ -> assertTrue(false, "the entry has an expiry")
  }
  // The same number sent as it is expires at once: the mapping is needed.
  val generation = match okReplies(store.client.run(Meta.encodeCommand("mg", Keys.wireKey(Keys.generationKey(store.options.namespace)), "v"), 1), "mg")[0] {
    case Meta.Value(data, _) -> match tryDecodeUtf8(data) {
      case Some(t) -> t
      case None -> panic("not text")
    }
    case _ -> panic("the namespace has a generation")
  }
  val naive = Keys.wireKey(Keys.dataKey(store.options.namespace, generation, "naive"))
  val _ = okReplies(store.client.run(Meta.encodeSet(naive, encodeUtf8("v"), "T" + fortyDays.toString()), 1), "ms")
  expectMiss(store.get("naive"), "3456000 sent unmapped is a day in 1970")
  assertEqualInt(store.errors.errorCount(), 0, "no failure")
  finish(store)
}

test "lifetimeOf and touch report and move memcached's expiry" {
  val store = newStore(freshNamespace())
  store.set("forever", "v", 0)
  store.set("hour", "v", 3600)
  match ok(Extras.lifetimeOf(store, "forever"), "lifetimeOf") {
    case Extras.Permanent -> ()
    case _ -> assertTrue(false, "ttlSeconds = 0 is permanent")
  }
  match ok(Extras.lifetimeOf(store, "hour"), "lifetimeOf") {
    case Extras.Expiring(seconds) -> assertTrue(seconds > 3590i64 and seconds <= 3600i64, "about an hour")
    case _ -> assertTrue(false, "an hour-long entry is expiring")
  }
  match ok(Extras.lifetimeOf(store, "never-set"), "lifetimeOf") {
    case Extras.Absent -> ()
    case _ -> assertTrue(false, "an unset key is absent")
  }
  assertTrue(ok(Extras.touch(store, "hour", 2), "touch"), "the entry exists")
  assertTrue(ok(Extras.touch(store, "forever", 7200), "touch"), "a permanent entry gets an expiry")
  assertFalse(ok(Extras.touch(store, "never-set", 10), "touch"), "an absent key")
  match ok(Extras.lifetimeOf(store, "forever"), "lifetimeOf") {
    case Extras.Expiring(seconds) -> assertTrue(seconds > 7190i64 and seconds <= 7200i64, "two hours now")
    case _ -> assertTrue(false, "touched")
  }
  sleepMillis(3200)
  expectMiss(store.get("hour"), "the shortened entry expired after two real seconds")
  expectHit(store.get("forever"), "v", "the other is alive")
  finish(store)
}

// ─── Values ──────────────────────────────────────────────────────────────────
test "a megabyte and more: the limit is reported, by the client or by the server" {
  val store = newStore(freshNamespace())
  val nearly = Str.repeat("0123456789", 100000)
  ok(MemcachedCache.trySet(store, "big", nearly, 60), "a value of 1,000,000 bytes")
  expectHit(store.get("big"), nearly, "round-trips")
  val over = Str.repeat("x", 1048577)
  val refusedByClient = MemcachedCache.trySet(store, "big", over, 60)
  assertEqual(codeOf(refusedByClient, "trySet"), "VALUE_TOO_LARGE", "1 MiB + 1 byte, stopped by the client")
  assertTrue(Str.contains(messageOf(refusedByClient), "1048577"), "the message has the size")
  expectMiss(store.get("big"), "and the old megabyte is gone, not stale")
  store.client.close()
  // A client told that the server takes 8 MiB: now memcached refuses.
  val generous = Client.withMaxValueBytes(ok(Client.configFromUrl(serverUrl()), "the URL"), 8 * 1048576)
  val trusting = ok(
    MemcachedCache.connectWith(generous, MemcachedCache.options(store.options.namespace)),
    "connectWith"
  )
  trusting.set("big", "old", 60)
  val refusedByServer = MemcachedCache.trySet(trusting, "big", over, 60)
  assertEqual(codeOf(refusedByServer, "trySet"), "VALUE_TOO_LARGE", "1 MiB + 1 byte, refused by memcached")
  assertTrue(Str.contains(messageOf(refusedByServer), "too large"), "with its words: " + messageOf(refusedByServer))
  expectMiss(trusting.get("big"), "memcached dropped the old entry")
  // Exactly 1 MiB of value does not fit either: the item also holds its key
  // and a header.
  assertEqual(
    codeOf(MemcachedCache.trySet(trusting, "big", Str.repeat("x", 1048576), 60), "trySet"),
    "VALUE_TOO_LARGE",
    "1 MiB of value"
  )
  assertEqualInt(trusting.client.connections(), 1, "the connection survived both refusals")
  trusting.set("after", "fine", 60)
  expectHit(trusting.get("after"), "fine", "and works")
  finish(trusting)
}

test "values and keys round-trip verbatim" {
  val store = newStore(freshNamespace())
  val keys: List[String] = newList()
  keys.add("with spaces and\ttabs")
  keys.add("line\r\nbreaks\r\nmn\r\n")
  keys.add("café 世界 🙂")
  keys.add(Str.repeat("k", 250))
  keys.add(Str.repeat("世", 2000))
  keys.add(Str.repeat("k", 5000) + "a")
  keys.add(Str.repeat("k", 5000) + "b")
  var i = 0
  while i < keys.count {
    store.set(keys[i], "value\r\nMN\r\n" + i.toString() + " é世 ", 60)
    i = i + 1
  }
  i = 0
  while i < keys.count {
    expectHit(store.get(keys[i]), "value\r\nMN\r\n" + i.toString() + " é世 ", "key " + i.toString())
    i = i + 1
  }
  store.set("empty", "", 60)
  expectHit(store.get("empty"), "", "the empty value")
  store.delete(keys[5])
  expectMiss(store.get(keys[5]), "a hashed key is deleted")
  expectHit(store.get(keys[6]), "value\r\nMN\r\n6 é世 ", "its neighbour is not")
  assertEqualInt(store.errors.errorCount(), 0, "no failure")
  finish(store)
}

// ─── Namespaces ──────────────────────────────────────────────────────────────
test "clear isolates two stores on one server and leaves foreign items alone" {
  val client = newClient()
  val base = freshNamespace()
  val a = MemcachedCache.fromClient(client, MemcachedCache.options(base + ":a"))
  val b = MemcachedCache.fromClient(client, MemcachedCache.options(base + ":b"))
  val foreign = Keys.wireKey(base + "-not-a-cache-key")
  val _ = okReplies(client.run(Meta.encodeSet(foreign, encodeUtf8("keep"), "T600"), 1), "a foreign item")
  a.set("k", "in a", 0)
  a.set("ttl", "in a", 600)
  b.set("k", "in b", 0)
  a.clear()
  expectMiss(a.get("k"), "the cleared store is empty")
  expectMiss(a.get("ttl"), "the entry with a TTL is gone too")
  expectHit(b.get("k"), "in b", "the other store is untouched")
  a.set("k", "again", 0)
  expectHit(a.get("k"), "again", "the cleared store takes entries again")
  b.clear()
  expectMiss(b.get("k"), "the second store is empty")
  expectHit(a.get("k"), "again", "and the first is untouched by that")
  assertEqual(
    statuses(okReplies(client.run(Meta.encodeCommand("mg", foreign, "v"), 1), "mg")),
    "VA",
    "an item outside every namespace survives both clears"
  )
  val _ = okReplies(client.run(Meta.encodeCommand("md", foreign, ""), 1), "md")
  assertEqualInt(a.errors.errorCount() + b.errors.errorCount(), 0, "no failure")
  a.clear()
  client.close()
}

test "a clear over one connection is seen by the next operation on another" {
  val namespace = freshNamespace()
  val first = newStore(namespace)
  val second = newStore(namespace)
  first.set("k", "v", 60)
  expectHit(second.get("k"), "v", "stores with one namespace share their entries")
  first.clear()
  expectMiss(second.get("k"), "the other connection's very next read misses")
  second.set("k", "new", 60)
  expectHit(first.get("k"), "new", "and what it writes is seen by the first")
  assertEqualInt(first.errors.errorCount() + second.errors.errorCount(), 0, "no failure")
  second.client.close()
  finish(first)
}

test "when the generation item is deleted the namespace is empty and starts over" {
  val store = newStore(freshNamespace())
  store.set("k", "old", 60)
  val generationWire = Keys.wireKey(Keys.generationKey(store.options.namespace))
  val before = okReplies(store.client.run(Meta.encodeCommand("mg", generationWire, "v t"), 1), "mg")
  assertEqual(statuses(before), "VA", "the generation is an item")
  match before[0] {
    case Meta.Value(_, flags) -> match Meta.flagValue(flags, "t") {
      case Some(t) -> assertEqual(t, "-1", "that never expires")
      case None -> assertTrue(false, "t was asked for")
    }
    case _ -> ()
  }
  // What an eviction or a restart does.
  val _ = okReplies(store.client.run(Meta.encodeCommand("md", generationWire, ""), 1), "md")
  expectMiss(store.get("k"), "without a generation nothing can be read")
  store.set("other", "new", 60)
  expectHit(store.get("other"), "new", "the next write starts a new generation")
  expectMiss(store.get("k"), "and the old entry stays out of reach")
  assertEqualInt(store.errors.errorCount(), 0, "none of this is a failure")
  finish(store)
}

// ─── Backend-specific operations ─────────────────────────────────────────────
test "addIfAbsent and compareAndSwap are atomic on the server" {
  val store = newStore(freshNamespace())
  assertTrue(ok(Extras.addIfAbsent(store, "lock", "first", 2), "addIfAbsent"), "the first add stores")
  assertFalse(ok(Extras.addIfAbsent(store, "lock", "second", 2), "addIfAbsent"), "the second does not")
  expectHit(store.get("lock"), "first", "the first value stands")
  store.set("counter", "1", 60)
  val read = match ok(Extras.getVersioned(store, "counter"), "getVersioned") {
    case Some(v) -> v
    case None -> panic("the entry exists")
  }
  assertEqual(read.value, "1", "the value")
  match ok(Extras.compareAndSwap(store, "counter", "2", 60, read.version), "compareAndSwap") {
    case Extras.Swapped -> ()
    case _ -> assertTrue(false, "nobody wrote in between")
  }
  match ok(Extras.compareAndSwap(store, "counter", "3", 60, read.version), "compareAndSwap") {
    case Extras.Changed -> ()
    case _ -> assertTrue(false, "the version is stale now")
  }
  expectHit(store.get("counter"), "2", "only the first swap stored")
  store.delete("counter")
  match ok(Extras.compareAndSwap(store, "counter", "3", 60, read.version), "compareAndSwap") {
    case Extras.Gone -> ()
    case _ -> assertTrue(false, "the entry is gone")
  }
  sleepMillis(3200)
  assertTrue(ok(Extras.addIfAbsent(store, "lock", "third", 60), "addIfAbsent"), "after the expiry the key is free")
  finish(store)
}

test "getMany reads 150 keys in three round trips" {
  val store = newStore(freshNamespace())
  val keys: List[String] = newList()
  var i = 0
  while i < 150 {
    keys.add("key " + i.toString())
    if i % 3 == 0 {
      store.set("key " + i.toString(), "v" + i.toString(), 60)
    }
    i = i + 1
  }
  val found = ok(Extras.getMany(store, keys.toArray()), "getMany")
  assertEqualInt(found.count, 50, "every third key")
  assertEqual(found["key 0"], "v0", "from the first batch")
  assertEqual(found["key 66"], "v66", "from the second")
  assertEqual(found["key 147"], "v147", "from the third")
  assertFalse(found.containsKey("key 1"), "a miss")
  ok(MemcachedCache.ping(store), "the connection is in step after the quiet reads")
  store.clear()
  assertEqualInt(ok(Extras.getMany(store, keys.toArray()), "getMany").count, 0, "nothing after a clear")
  finish(store)
}

// ─── Authentication ──────────────────────────────────────────────────────────
test "a server with an authentication file takes the URL's credentials and refuses others" {
  val url = requiredEnv("LYRIC_MEMCACHED_AUTH_URL")
  val cfg = ok(Client.configFromUrl(url), "LYRIC_MEMCACHED_AUTH_URL")
  if cfg.username.length == 0 {
    panic("LYRIC_MEMCACHED_AUTH_URL must carry credentials: memcached://user:password@host:port")
  }
  val store = ok(MemcachedCache.connectWith(cfg, MemcachedCache.options(freshNamespace())), "connectWith")
  store.set("k", "authenticated", 60)
  expectHit(store.get("k"), "authenticated", "a round trip after authenticating")
  store.client.disconnect()
  expectHit(store.get("k"), "authenticated", "a new connection authenticates again")
  assertEqualInt(store.errors.errorCount(), 0, "no failure")
  val wrong = Client.open(Client.withCredentials(cfg, cfg.username, "not-the-password"))
  assertEqual(codeOf(wrong, "open"), "AUTH_FAILED", "a wrong password")
  val missing = Client.open(Client.withCredentials(cfg, "", ""))
  assertEqual(codeOf(missing, "open"), "AUTH_FAILED", "no credentials")
  assertTrue(Str.contains(messageOf(missing), "requires authentication"), "the message says what is missing")
  finish(store)
}

test "credentials sent to the server without authentication leave nothing readable" {
  val plain = ok(Client.configFromUrl(serverUrl()), "LYRIC_MEMCACHED_URL")
  val client = ok(Client.open(Client.withCredentials(plain, "lyric", "would-be-secret")), "open")
  assertEqual(
    statuses(okReplies(client.run(encodeUtf8("mg auth v\r\n"), 1), "mg auth")),
    "EN",
    "the token was stored already expired"
  )
  client.close()
}

test "flushAll empties the disposable server" {
  val cfg = ok(Client.configFromUrl(requiredEnv("LYRIC_MEMCACHED_AUTH_URL")), "LYRIC_MEMCACHED_AUTH_URL")
  val store = ok(MemcachedCache.connectWith(cfg, MemcachedCache.options(freshNamespace())), "connectWith")
  store.set("k", "v", 0)
  expectHit(store.get("k"), "v", "stored")
  ok(Client.flushAll(store.client), "flush_all")
  expectMiss(store.get("k"), "everything is gone, the generation included")
  store.set("k", "again", 60)
  expectHit(store.get("k"), "again", "and the store starts over")
  assertEqualInt(store.errors.errorCount(), 0, "no failure")
  finish(store)
}
