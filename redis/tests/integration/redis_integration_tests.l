//! tests/integration/redis_integration_tests.l - the client against a real
//! Redis server (6.0 or later, for ACL users and RESP3).
//!
//! Needs `LYRIC_REDIS_URL` (for example `redis://127.0.0.1:16379`) and is
//! compiled only with `--features integration`:
//!
//!   LYRIC_REDIS_URL=redis://127.0.0.1:16379 \
//!     lyric test --manifest redis/lyric.toml --target dotnet --features integration
//!
//! Use a disposable server: the suite creates and deletes an ACL user and
//! runs `FLUSHDB` on database 15. Every other key it writes carries a random
//! prefix and is deleted again.
@cfg(feature = "integration")
@test_module
package Redis.IntegrationTests

import Std.Core
import Std.Collections
import Std.Encoding
import Std.Environment as Env
import Std.String as Str
import Std.Testing
import Std.Time
import Std.Uuid
import Redis
import Redis.Resp
import Net.Tcp.Testing as KT

func serverUrl(): String {
  val url = Env.getVarOrDefault("LYRIC_REDIS_URL", "")
  if url.length == 0 {
    panic(
      "LYRIC_REDIS_URL is not set: the integration tests need a Redis server, for example LYRIC_REDIS_URL=redis://127.0.0.1:16379"
    )
  }
  url
}

func baseConfig(): RedisConfig {
  match configFromUrl(serverUrl()) {
    case Ok(cfg) -> cfg
    case Err(e) -> panic("LYRIC_REDIS_URL is not usable: " + e.message())
  }
}

func openWith(cfg: in RedisConfig): RedisClient {
  match Redis.open(cfg) {
    case Ok(c) -> c
    case Err(e) -> panic("cannot open a client: " + e.message())
  }
}

func client(): RedisClient {
  openWith(baseConfig())
}

val runId: String = uuidToString(newUuid())

func key(name: in String): String {
  "lyric-redis-it:" + runId + ":" + name
}

func ok[T](r: in Result[T, RedisError], what: in String): T {
  match r {
    case Ok(v) -> v
    case Err(e) -> panic(what + " failed: " + e.message())
  }
}

func codeOf[T](r: in Result[T, RedisError], what: in String): String {
  match r {
    case Ok(_) -> {
      assertTrue(false, what + " should have failed")
      ""
    }
    case Err(e) -> e.code()
  }
}

func expectSome(r: in Result[Option[String], RedisError], expected: in String, what: in String): Unit {
  match ok(r, what) {
    case Some(v) -> assertEqual(v, expected, what)
    case None -> assertTrue(false, what + ": expected a value, got none")
  }
}

func expectNone(r: in Result[Option[String], RedisError], what: in String): Unit {
  match ok(r, what) {
    case Some(v) -> assertTrue(false, what + ": expected no value, got " + v)
    case None -> ()
  }
}

// ─── Basics ──────────────────────────────────────────────────────────────────
test "PING answers" {
  val c = client()
  ok(Redis.ping(c), "PING")
  assertTrue(c.isConnected(), "connected after a command")
  c.close()
}

test "SET, GET, EXISTS and DEL round-trip" {
  val c = client()
  val k = key("basic")
  expectNone(Redis.get(c, k), "GET before SET")
  assertFalse(ok(Redis.exists(c, k), "EXISTS"), "absent before SET")
  ok(Redis.set(c, k, "value"), "SET")
  expectSome(Redis.get(c, k), "value", "GET after SET")
  assertTrue(ok(Redis.exists(c, k), "EXISTS"), "present after SET")
  assertEqualLong(ok(Redis.del(c, [k, key("never-set")]), "DEL"), 1i64, "one key deleted")
  expectNone(Redis.get(c, k), "GET after DEL")
  c.close()
}

test "values round-trip verbatim: empty, whitespace, CRLF, non-ASCII" {
  val c = client()
  val k = key("verbatim")
  for v in ["", "  spaced \t and\nnewline  ", "a\r\nb", "café 世界", "*3\r\n$3\r\nSET\r\n"] {
    ok(Redis.set(c, k, v), "SET")
    expectSome(Redis.get(c, k), v, "GET of a verbatim value")
  }
  ok(Redis.set(c, key("spaces and:punct/uation"), "ok"), "SET with a punctuated key")
  expectSome(Redis.get(c, key("spaces and:punct/uation")), "ok", "GET with a punctuated key")
  val _ = ok(Redis.del(c, [k, key("spaces and:punct/uation")]), "DEL")
  c.close()
}

test "binary values round-trip and are refused as text" {
  val c = client()
  val k = key("binary")
  val payload: slice[Byte] = [0.toByte(), 255.toByte(), 13.toByte(), 10.toByte(), 128.toByte()]
  ok(Redis.setBytes(c, k, payload), "SET bytes")
  match ok(Redis.getBytes(c, k), "GET bytes") {
    case None -> assertTrue(false, "the binary value must exist")
    case Some(b) -> {
      assertEqualInt(b.length, 5, "length")
      assertEqualInt(b[1].toInt().and(255), 255, "second byte")
      assertEqualInt(b[4].toInt().and(255), 128, "last byte")
    }
  }
  assertEqual(codeOf(Redis.get(c, k), "GET of invalid UTF-8 as text"), "UNEXPECTED_REPLY", "not text")
  val _ = ok(Redis.del(c, [k]), "DEL")
  c.close()
}

test "a value larger than one read round-trips" {
  val c = client()
  val k = key("large")
  var big = "0123456789abcdef"
  while big.length < 300000 {
    big = big + big
  }
  ok(Redis.set(c, k, big), "SET of a large value")
  match ok(Redis.get(c, k), "GET of a large value") {
    case Some(v) -> {
      assertEqualInt(v.length, big.length, "length of the large value")
      assertTrue(v == big, "content of the large value")
    }
    case None -> assertTrue(false, "the large value must exist")
  }
  val _ = ok(Redis.del(c, [k]), "DEL")
  c.close()
}

// ─── Expiry ──────────────────────────────────────────────────────────────────
test "SET EX / PX, TTL, EXPIRE, PEXPIRE and PERSIST" {
  val c = client()
  val k = key("ttl")
  assertEqualLong(ok(Redis.ttl(c, k), "TTL"), 0i64 - 2i64, "TTL of an absent key")
  ok(Redis.set(c, k, "v"), "SET")
  assertEqualLong(ok(Redis.ttl(c, k), "TTL"), 0i64 - 1i64, "TTL of a key without expiry")
  ok(Redis.setEx(c, k, "v", 100i64), "SET EX")
  val t = ok(Redis.ttl(c, k), "TTL")
  assertTrue(t > 90i64 and t <= 100i64, "TTL after SET EX 100")
  ok(Redis.setPx(c, k, "v", 50000i64), "SET PX")
  val p = ok(Redis.pttl(c, k), "PTTL")
  assertTrue(p > 40000i64 and p <= 50000i64, "PTTL after SET PX 50000")
  assertTrue(ok(Redis.persist(c, k), "PERSIST"), "PERSIST removed an expiry")
  assertEqualLong(ok(Redis.ttl(c, k), "TTL"), 0i64 - 1i64, "TTL after PERSIST")
  assertTrue(ok(Redis.expire(c, k, 200i64), "EXPIRE"), "EXPIRE on an existing key")
  assertTrue(ok(Redis.ttl(c, k), "TTL") > 190i64, "TTL after EXPIRE 200")
  assertTrue(ok(Redis.pexpire(c, k, 30000i64), "PEXPIRE"), "PEXPIRE on an existing key")
  assertTrue(ok(Redis.pttl(c, k), "PTTL") <= 30000i64, "PTTL after PEXPIRE 30000")
  assertFalse(ok(Redis.expire(c, key("never-set"), 10i64), "EXPIRE"), "EXPIRE on an absent key")
  ok(Redis.set(c, k, "plain"), "SET")
  assertEqualLong(ok(Redis.ttl(c, k), "TTL"), 0i64 - 1i64, "a plain SET removes the expiry")
  val _ = ok(Redis.del(c, [k]), "DEL")
  c.close()
}

test "a key with a short expiry really disappears" {
  val c = client()
  val k = key("expires")
  ok(Redis.setPx(c, k, "v", 150i64), "SET PX 150")
  expectSome(Redis.get(c, k), "v", "GET before the expiry")
  sleepMillis(400)
  expectNone(Redis.get(c, k), "GET after the expiry")
  c.close()
}

test "SET NX stores only when the key is absent" {
  val c = client()
  val k = key("nx")
  assertTrue(ok(Redis.setNx(c, k, "first", 0i64), "SET NX"), "stored when absent")
  assertFalse(ok(Redis.setNx(c, k, "second", 60000i64), "SET NX"), "not stored when present")
  expectSome(Redis.get(c, k), "first", "the first value stays")
  val _ = ok(Redis.del(c, [k]), "DEL")
  assertTrue(ok(Redis.setNx(c, k, "third", 60000i64), "SET NX PX"), "stored with an expiry")
  assertTrue(ok(Redis.pttl(c, k), "PTTL") > 0i64, "the expiry is set")
  val _ = ok(Redis.del(c, [k]), "DEL")
  c.close()
}

test "INCRBY counts" {
  val c = client()
  val k = key("counter")
  assertEqualLong(ok(Redis.incrBy(c, k, 5i64), "INCRBY"), 5i64, "first increment")
  assertEqualLong(ok(Redis.incrBy(c, k, 0i64 - 2i64), "INCRBY"), 3i64, "decrement")
  val _ = ok(Redis.del(c, [k]), "DEL")
  c.close()
}

// ─── Hashes ──────────────────────────────────────────────────────────────────
test "HSET, HGET, HDEL and HGETALL" {
  val c = client()
  val k = key("hash")
  assertTrue(ok(Redis.hset(c, k, "a", "1"), "HSET"), "a new field")
  assertFalse(ok(Redis.hset(c, k, "a", "2"), "HSET"), "an existing field")
  assertTrue(ok(Redis.hset(c, k, "b", "café"), "HSET"), "another field")
  expectSome(Redis.hget(c, k, "a"), "2", "HGET")
  expectNone(Redis.hget(c, k, "absent"), "HGET of an absent field")
  val all = ok(Redis.hgetAll(c, k), "HGETALL")
  assertEqualInt(all.count, 2, "two fields")
  assertEqual(all["a"], "2", "field a")
  assertEqual(all["b"], "café", "field b")
  assertEqualLong(ok(Redis.hdel(c, k, ["a", "absent"]), "HDEL"), 1i64, "one field deleted")
  assertEqualInt(ok(Redis.hgetAll(c, key("no-such-hash")), "HGETALL").count, 0, "HGETALL of an absent key")
  val _ = ok(Redis.del(c, [k]), "DEL")
  c.close()
}

// ─── SCAN ────────────────────────────────────────────────────────────────────
test "SCAN MATCH finds exactly the keys under a prefix" {
  val c = client()
  val prefix = key("scan[*]?:")
  var i = 0
  while i < 250 {
    ok(Redis.set(c, prefix + i.toString(), "v"), "SET")
    i = i + 1
  }
  ok(Redis.set(c, key("scanX"), "other"), "SET of a key outside the prefix")
  val found = ok(Redis.scanAll(c, escapeGlob(prefix) + "*"), "SCAN")
  assertEqualInt(found.count, 250, "every key under the prefix, once")
  for k in found {
    assertTrue(k.startsWith(prefix), "a found key is under the prefix")
  }
  val page = ok(Redis.scan(c, "0", escapeGlob(prefix) + "1", 1000), "SCAN of one key")
  assertTrue(page.keys.count <= 1, "a literal pattern matches at most one key")
  assertEqualLong(ok(Redis.del(c, found.toArray()), "DEL"), 250i64, "all deleted")
  val _ = ok(Redis.del(c, [key("scanX")]), "DEL")
  c.close()
}

// ─── Scripts, pipelines, raw commands ────────────────────────────────────────
test "EVAL runs a script with keys and arguments" {
  val c = client()
  val k = key("eval")
  val script = "if redis.call('EXISTS', KEYS[1]) == 1 then return 0 end redis.call('SET', KEYS[1], ARGV[1]) return 1"
  match ok(Redis.eval(c, script, [k], ["v"]), "EVAL") {
    case RInt(n) -> assertEqualLong(n, 1i64, "the script set the key")
    case _ -> assertTrue(false, "EVAL returns an integer")
  }
  match ok(Redis.eval(c, script, [k], ["other"]), "EVAL") {
    case RInt(n) -> assertEqualLong(n, 0i64, "the script saw the key")
    case _ -> assertTrue(false, "EVAL returns an integer")
  }
  expectSome(Redis.get(c, k), "v", "the first value stays")
  assertEqual(
    codeOf(Redis.eval(c, "return redis.call('NOSUCH')", [], []), "a failing script"),
    "SERVER_ERROR",
    "script error"
  )
  val _ = ok(Redis.del(c, [k]), "DEL")
  c.close()
}

test "a pipeline returns one reply per command, errors in place" {
  val c = client()
  val k = key("pipe")
  val commands: slice[slice[slice[Byte]]] = [
    [encodeUtf8("SET"), encodeUtf8(k), encodeUtf8("1")],
    [encodeUtf8("INCR"), encodeUtf8(k)],
    [encodeUtf8("HGET"), encodeUtf8(k), encodeUtf8("f")],
    [encodeUtf8("GET"), encodeUtf8(k)]
  ]
  val replies = ok(c.pipeline(commands), "pipeline")
  assertEqualInt(replies.count, 4, "four replies")
  match replies[1] {
    case RInt(n) -> assertEqualLong(n, 2i64, "INCR")
    case _ -> assertTrue(false, "INCR returns an integer")
  }
  match replies[2] {
    case RError(m) -> assertTrue(m.startsWith("WRONGTYPE"), "HGET on a string is WRONGTYPE")
    case _ -> assertTrue(false, "HGET on a string is an error reply")
  }
  match asText(replies[3]) {
    case Some(t) -> assertEqual(t, "2", "GET")
    case None -> assertTrue(false, "GET returns text")
  }
  expectSome(Redis.get(c, k), "2", "the connection stays in step after a pipeline")
  val _ = ok(Redis.del(c, [k]), "DEL")
  c.close()
}

test "an error reply is a ServerError and the connection stays usable" {
  val c = client()
  val k = key("wrongtype")
  ok(Redis.set(c, k, "text"), "SET")
  match Redis.hget(c, k, "f") {
    case Err(ServerError(m)) -> assertTrue(m.startsWith("WRONGTYPE"), "WRONGTYPE")
    case _ -> assertTrue(false, "HGET on a string must be a ServerError")
  }
  assertEqual(codeOf(Redis.command(c, ["NOSUCHCOMMAND"]), "an unknown command"), "SERVER_ERROR", "unknown command")
  expectSome(Redis.get(c, k), "text", "GET after the errors")
  assertEqualInt(c.connections(), 1, "no reconnect after an error reply")
  val _ = ok(Redis.del(c, [k]), "DEL")
  c.close()
}

// ─── RESP3 ───────────────────────────────────────────────────────────────────
test "RESP3: HELLO 3, map replies and nulls" {
  val c = openWith(withProtocol(baseConfig(), 3))
  val k = key("resp3")
  expectNone(Redis.get(c, k), "GET of an absent key")
  assertTrue(ok(Redis.hset(c, k, "a", "1"), "HSET"), "HSET")
  assertTrue(ok(Redis.hset(c, k, "b", "2"), "HSET"), "HSET")
  match ok(Redis.command(c, ["HGETALL", k]), "HGETALL") {
    case RMap(entries) -> assertEqualInt(entries.count, 4, "a RESP3 map of two pairs")
    case other -> assertTrue(false, "HGETALL under RESP3 is a map, got " + kindOf(other))
  }
  val all = ok(Redis.hgetAll(c, k), "HGETALL")
  assertEqual(all["a"], "1", "field a")
  assertEqual(all["b"], "2", "field b")
  assertEqualInt(ok(Redis.scanAll(c, escapeGlob(k)), "SCAN").count, 1, "SCAN under RESP3")
  val _ = ok(Redis.del(c, [k]), "DEL")
  c.close()
}

// ─── SELECT ──────────────────────────────────────────────────────────────────
test "databases are separate and FLUSHDB empties only the selected one" {
  val c0 = client()
  val c15 = openWith(withDatabase(baseConfig(), 15))
  val k = key("db")
  ok(Redis.set(c0, k, "in-default"), "SET in the default database")
  ok(Redis.set(c15, k, "in-15"), "SET in database 15")
  expectSome(Redis.get(c0, k), "in-default", "GET in the default database")
  expectSome(Redis.get(c15, k), "in-15", "GET in database 15")
  ok(Redis.flushDb(c15), "FLUSHDB in database 15")
  expectNone(Redis.get(c15, k), "database 15 is empty")
  expectSome(Redis.get(c0, k), "in-default", "the default database is untouched")
  val _ = ok(Redis.del(c0, [k]), "DEL")
  c0.close()
  c15.close()
}

test "selecting a database that does not exist is a ServerError at open" {
  assertEqual(codeOf(Redis.open(withDatabase(baseConfig(), 99999)), "open"), "SERVER_ERROR", "SELECT 99999")
}

// ─── AUTH ────────────────────────────────────────────────────────────────────
test "an ACL user authenticates; a wrong password is AuthFailed" {
  val admin = client()
  val user = "lyric-it-" + runId
  val _ = ok(Redis.command(admin, ["ACL", "SETUSER", user, "on", ">correct horse", "~*", "+@all"]), "ACL SETUSER")
  val good = openWith(withCredentials(baseConfig(), user, "correct horse"))
  match ok(Redis.command(good, ["ACL", "WHOAMI"]), "ACL WHOAMI") {
    case RBulk(b) -> assertEqual(textOfBytes(b), user, "authenticated as the ACL user")
    case _ -> assertTrue(false, "ACL WHOAMI returns a bulk string")
  }
  good.close()
  val resp3 = openWith(withProtocol(withCredentials(baseConfig(), user, "correct horse"), 3))
  ok(Redis.ping(resp3), "PING as the ACL user over RESP3")
  resp3.close()
  match Redis.open(withCredentials(baseConfig(), user, "wrong")) {
    case Err(AuthFailed(m)) -> assertFalse(Str.contains(m, "wrong"), "the message does not repeat the password")
    case Err(e) -> assertTrue(false, "expected AuthFailed, got " + e.code())
    case Ok(_) -> assertTrue(false, "a wrong password must not authenticate")
  }
  assertEqual(
    codeOf(Redis.open(withProtocol(withCredentials(baseConfig(), user, "wrong"), 3)), "open over RESP3"),
    "AUTH_FAILED",
    "a wrong password over RESP3"
  )
  assertEqual(
    codeOf(Redis.open(withCredentials(baseConfig(), "no-such-user-" + runId, "pw")), "open as an unknown user"),
    "AUTH_FAILED",
    "an unknown user"
  )
  val _ = ok(Redis.command(admin, ["ACL", "DELUSER", user]), "ACL DELUSER")
  admin.close()
}

func textOfBytes(b: in slice[Byte]): String {
  match tryDecodeUtf8(b) {
    case Some(s) -> s
    case None -> "<binary>"
  }
}

// ─── Broken connections ──────────────────────────────────────────────────────
func clientId(c: in RedisClient): String {
  match ok(Redis.command(c, ["CLIENT", "ID"]), "CLIENT ID") {
    case RInt(n) -> n.toString()
    case _ -> panic("CLIENT ID returns an integer")
  }
}

test "a connection the server killed is replaced and the command repeated" {
  val c = client()
  val admin = client()
  val k = key("reconnect")
  ok(Redis.set(c, k, "before"), "SET")
  val _ = ok(Redis.command(admin, ["CLIENT", "KILL", "ID", clientId(c)]), "CLIENT KILL")
  sleepMillis(100)
  expectSome(Redis.get(c, k), "before", "GET on the killed connection")
  assertEqualInt(c.connections(), 2, "the client reconnected once")
  val _ = ok(Redis.del(c, [k]), "DEL")
  c.close()
  admin.close()
}

test "without the stale retry a killed connection fails once, then recovers" {
  val c = openWith(withStaleRetry(baseConfig(), false))
  val admin = client()
  val _ = ok(Redis.command(admin, ["CLIENT", "KILL", "ID", clientId(c)]), "CLIENT KILL")
  sleepMillis(100)
  assertEqual(codeOf(Redis.ping(c), "PING on the killed connection"), "CONNECTION_LOST", "the first command fails")
  assertFalse(c.isConnected(), "the broken connection was dropped")
  ok(Redis.ping(c), "PING after the failure")
  assertEqualInt(c.connections(), 2, "the client reconnected")
  c.close()
  admin.close()
}

test "a reconnect repeats AUTH and SELECT" {
  val admin = client()
  val user = "lyric-it-re-" + runId
  val _ = ok(Redis.command(admin, ["ACL", "SETUSER", user, "on", ">pw", "~*", "+@all"]), "ACL SETUSER")
  val c = openWith(withDatabase(withCredentials(baseConfig(), user, "pw"), 15))
  val k = key("reselect")
  ok(Redis.set(c, k, "in-15"), "SET in database 15")
  c.disconnect()
  assertFalse(c.isConnected(), "disconnected")
  expectSome(Redis.get(c, k), "in-15", "GET after reconnecting lands in database 15")
  match ok(Redis.command(c, ["ACL", "WHOAMI"]), "ACL WHOAMI") {
    case RBulk(b) -> assertEqual(textOfBytes(b), user, "still the ACL user")
    case _ -> assertTrue(false, "ACL WHOAMI returns a bulk string")
  }
  val _ = ok(Redis.del(c, [k]), "DEL")
  c.close()
  val _ = ok(Redis.command(admin, ["ACL", "DELUSER", user]), "ACL DELUSER")
  admin.close()
}

test "a reply slower than the I/O timeout is ConnectionLost and the client recovers" {
  val c = openWith(withTimeouts(baseConfig(), 5000, 300))
  val started = nowEpochMillis()
  assertEqual(
    codeOf(Redis.command(c, ["BLPOP", key("never-pushed"), "3"]), "a blocking command"),
    "CONNECTION_LOST",
    "the read timed out"
  )
  val waited = nowEpochMillis() - started
  assertTrue(waited < 2500i64, "the timeout fired before the server answered")
  assertFalse(c.isConnected(), "the timed-out connection was dropped")
  ok(Redis.ping(c), "PING after the timeout")
  c.close()
}

test "a closed client refuses commands" {
  val c = client()
  c.close()
  assertEqual(codeOf(Redis.ping(c), "PING on a closed client"), "CLIENT_CLOSED", "closed")
  assertEqual(codeOf(c.ensureConnected(), "ensureConnected on a closed client"), "CLIENT_CLOSED", "closed")
}

// ─── Thread safety ───────────────────────────────────────────────────────────
func incrementMany(c: in RedisClient, k: in String, times: in Int, failures: in List[String]): Unit {
  var i = 0
  while i < times {
    match Redis.incrBy(c, k, 1i64) {
      case Ok(_) -> ()
      case Err(e) -> failures.add(e.message())
    }
    i = i + 1
  }
}

test "one client shared by eight threads loses no command" {
  val c = client()
  val k = key("threads")
  val failures: List[String] = newList()
  val workers: List[KT.Background] = newList()
  var t = 0
  while t < 8 {
    workers.add(KT.startBackground({ -> incrementMany(c, k, 250, failures) }))
    t = t + 1
  }
  for w in workers {
    KT.waitBackground(w)
  }
  assertEqualInt(failures.count, 0, "no command failed")
  expectSome(Redis.get(c, k), "2000", "every increment was applied exactly once")
  assertEqualInt(c.connections(), 1, "one connection served every thread")
  val _ = ok(Redis.del(c, [k]), "DEL")
  c.close()
}
