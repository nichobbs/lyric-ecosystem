//! tests/integration/redis_tls_integration_tests.l - the client over TLS
//! against a real Redis server with TLS enabled.
//!
//! Compiled only with `--features integration`, and needs the variables that
//! `scripts/ci/start-redis-tls.sh` prints or exports:
//!
//!   LYRIC_REDIS_TLS_URL   rediss:// URL of a server whose certificate names the
//!                         URL's host and that does not demand a client certificate
//!   LYRIC_REDIS_MTLS_URL  rediss:// URL of a server that refuses clients without
//!                         a certificate (`tls-auth-clients yes`)
//!   LYRIC_REDIS_TLS_CA    PEM file: the CA that signed both server certificates
//!   LYRIC_REDIS_TLS_CERT  PEM file: a client certificate the servers accept
//!   LYRIC_REDIS_TLS_KEY   PEM file: its private key (PKCS#8)
//!
//!   eval "$(scripts/ci/start-redis-tls.sh)"
//!   lyric test --manifest redis/lyric.toml --target dotnet --features integration
//!
//! Use disposable servers: the suite creates and deletes an ACL user. Every
//! key it writes carries a random prefix and is deleted again.
@cfg(feature = "integration")
@test_module
package Redis.TlsIntegrationTests

import Std.Core
import Std.Collections
import Std.Encoding
import Std.Environment as Env
import Std.String as Str
import Std.Testing
import Std.Time
import Std.Tls as Tls
import Std.Uuid
import Redis
import Redis.Resp
import Net.Tcp.Testing as KT

func requiredEnv(name: in String): String {
  val value = Env.getVarOrDefault(name, "")
  if value.length == 0 {
    panic(
      name +
        " is not set: the TLS integration tests need a Redis server with TLS; run scripts/ci/start-redis-tls.sh, which starts one and prints LYRIC_REDIS_TLS_URL, LYRIC_REDIS_MTLS_URL, LYRIC_REDIS_TLS_CA, LYRIC_REDIS_TLS_CERT and LYRIC_REDIS_TLS_KEY"
    )
  }
  value
}

func ok[T](r: in Result[T, RedisError], what: in String): T {
  match r {
    case Ok(v) -> v
    case Err(e) -> panic(what + " failed: " + e.message())
  }
}

// The server of `urlVariable`, without any trust configured.
func untrusted(urlVariable: in String): RedisConfig {
  val cfg = ok(configFromUrl(requiredEnv(urlVariable)), urlVariable)
  if not cfg.tls.enabled {
    panic(urlVariable + " must be a rediss:// URL")
  }
  cfg
}

func withTestCa(cfg: in RedisConfig): RedisConfig {
  ok(withExclusiveCaCertificateFile(cfg, requiredEnv("LYRIC_REDIS_TLS_CA")), "LYRIC_REDIS_TLS_CA")
}

func withTestIdentity(cfg: in RedisConfig): RedisConfig {
  ok(
    withClientIdentityFiles(cfg, requiredEnv("LYRIC_REDIS_TLS_CERT"), requiredEnv("LYRIC_REDIS_TLS_KEY")),
    "LYRIC_REDIS_TLS_CERT / LYRIC_REDIS_TLS_KEY"
  )
}

// The server that takes clients without a certificate, verified.
func baseConfig(): RedisConfig {
  withTestCa(untrusted("LYRIC_REDIS_TLS_URL"))
}

// The server that requires a client certificate, verified, no certificate yet.
func mutualConfig(): RedisConfig {
  withTestCa(untrusted("LYRIC_REDIS_MTLS_URL"))
}

func openWith(cfg: in RedisConfig): RedisClient {
  match Redis.open(cfg) {
    case Ok(c) -> c
    case Err(e) -> panic("cannot open a TLS client: " + e.message())
  }
}

func client(): RedisClient {
  openWith(baseConfig())
}

val runId: String = uuidToString(newUuid())

func key(name: in String): String {
  "lyric-redis-tls-it:" + runId + ":" + name
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

// "" when a client opens and PING is answered, else the error code of the
// step that failed. A server refuses a client without a certificate only
// after a TLS 1.3 handshake, so that failure can surface on the PING.
func pingOutcome(cfg: in RedisConfig): String {
  match Redis.open(cfg) {
    case Err(e) -> e.code()
    case Ok(c) -> {
      val outcome = match Redis.ping(c) { case Ok(_) -> ""; case Err(e) -> e.code() }
      c.close()
      outcome
    }
  }
}

func expectSome(r: in Result[Option[String], RedisError], expected: in String, what: in String): Unit {
  match ok(r, what) {
    case Some(v) -> assertEqual(v, expected, what)
    case None -> assertTrue(false, what + ": expected a value, got none")
  }
}

// ─── Trust and the server name ───────────────────────────────────────────────
test "a verified TLS connection with the custom CA round-trips commands" {
  val c = client()
  val k = key("basic")
  ok(Redis.ping(c), "PING")
  ok(Redis.set(c, k, "über\r\nTLS"), "SET")
  expectSome(Redis.get(c, k), "über\r\nTLS", "GET")
  assertEqualLong(ok(Redis.del(c, [k]), "DEL"), 1i64, "DEL")
  assertEqualInt(c.connections(), 1, "one connection")
  c.close()
}

test "without the CA the server is not trusted" {
  val cfg = untrusted("LYRIC_REDIS_TLS_URL")
  assertEqual(codeOf(Redis.open(cfg), "the system trust store"), "TLS_FAILED", "the test CA is not a system CA")
  assertEqual(
    codeOf(Redis.connect(requiredEnv("LYRIC_REDIS_TLS_URL")), "connect(url)"),
    "TLS_FAILED",
    "nor through connect"
  )
  val c = Redis.lazy(cfg)
  assertEqual(codeOf(Redis.ping(c), "PING on a lazy client"), "TLS_FAILED", "reported by the command")
  assertFalse(c.isConnected(), "no connection is kept")
  c.close()
}

test "the CA as an additional trust anchor, next to the system trust store" {
  val additional = ok(
    withCaCertificateFile(untrusted("LYRIC_REDIS_TLS_URL"), requiredEnv("LYRIC_REDIS_TLS_CA")),
    "LYRIC_REDIS_TLS_CA"
  )
  val c = openWith(additional)
  ok(Redis.set(c, key("additional-ca"), "trusted"), "SET")
  expectSome(Redis.get(c, key("additional-ca")), "trusted", "GET")
  ok(Redis.del(c, [key("additional-ca")]), "DEL")
  c.close()
}

test "a server name the certificate lacks is refused; the names it carries are accepted" {
  val cfg = baseConfig()
  assertEqual(
    codeOf(Redis.open(withTlsServerName(cfg, "redis.example.invalid")), "another server name"),
    "TLS_FAILED",
    "host name mismatch"
  )
  assertEqual(pingOutcome(withTlsServerName(cfg, "localhost")), "", "the DNS name of the certificate")
  assertEqual(pingOutcome(withTlsServerName(cfg, "127.0.0.1")), "", "the address of the certificate")
}

// ─── Mutual TLS ──────────────────────────────────────────────────────────────
test "mutual TLS: a client certificate is accepted where one is required" {
  val c = openWith(withTestIdentity(mutualConfig()))
  val k = key("mutual")
  ok(Redis.set(c, k, "v"), "SET")
  expectSome(Redis.get(c, k), "v", "GET")
  val _ = ok(Redis.del(c, [k]), "DEL")
  c.close()
  assertEqual(pingOutcome(withTestIdentity(baseConfig())), "", "and where one is optional")
}

test "mutual TLS: no client certificate is refused where one is required" {
  val outcome = pingOutcome(mutualConfig())
  assertTrue(outcome == "TLS_FAILED" or outcome == "CONNECTION_LOST", "refused, got: " + outcome)
  val c = Redis.lazy(mutualConfig())
  val first = codeOf(Redis.ping(c), "PING without a certificate")
  assertTrue(first == "TLS_FAILED" or first == "CONNECTION_LOST", "refused on a lazy client, got: " + first)
  val second = codeOf(Redis.ping(c), "PING again")
  assertTrue(second == "TLS_FAILED" or second == "CONNECTION_LOST", "still refused, got: " + second)
  c.close()
}

// ─── The insecure policy ─────────────────────────────────────────────────────
test "insecure skip verify takes effect only with both keys" {
  val requested = withTlsServerName(withInsecureSkipVerify(untrusted("LYRIC_REDIS_TLS_URL")), "redis.example.invalid")
  Env.setVar("LYRIC_TLS_ALLOW_INSECURE", "")
  assertEqual(codeOf(Redis.open(requested), "the code key alone"), "TLS_FAILED", "verification stays on")
  Env.setVar("LYRIC_TLS_ALLOW_INSECURE", "1")
  assertEqual(
    codeOf(Redis.open(untrusted("LYRIC_REDIS_TLS_URL")), "the variable alone"),
    "TLS_FAILED",
    "verification stays on"
  )
  assertEqual(pingOutcome(requested), "", "both keys: the unknown CA and the wrong name are let through")
  Env.setVar("LYRIC_TLS_ALLOW_INSECURE", "")
  assertEqual(codeOf(Redis.open(requested), "the variable cleared again"), "TLS_FAILED", "verification is back")
}

// ─── The same surface as over plain TCP ──────────────────────────────────────
test "TLS 1.3 can be required" {
  assertEqual(pingOutcome(withMinTlsVersion(baseConfig(), Tls.Tls13)), "", "TLS 1.3 only")
}

test "a value larger than one TLS record round-trips" {
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

test "a pipeline returns one reply per command" {
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
  expectSome(Redis.get(c, k), "2", "the connection stays in step after a pipeline")
  val _ = ok(Redis.del(c, [k]), "DEL")
  c.close()
}

test "RESP3 is negotiated inside TLS" {
  val c = openWith(withProtocol(baseConfig(), 3))
  val k = key("resp3")
  val _ = ok(Redis.hset(c, k, "f", "v"), "HSET")
  val all = ok(Redis.hgetAll(c, k), "HGETALL")
  assertEqualInt(all.count, 1, "one field")
  val _ = ok(Redis.del(c, [k]), "DEL")
  c.close()
}

func textOfBytes(b: in slice[Byte]): String {
  match tryDecodeUtf8(b) {
    case Some(s) -> s
    case None -> "<binary>"
  }
}

test "an ACL user authenticates inside TLS, and a reconnect repeats AUTH and SELECT" {
  val admin = client()
  val user = "lyric-tls-it-" + runId
  val _ = ok(Redis.command(admin, ["ACL", "SETUSER", user, "on", ">pw", "~*", "+@all"]), "ACL SETUSER")
  assertEqual(
    codeOf(Redis.open(withCredentials(baseConfig(), user, "wrong")), "a wrong password"),
    "AUTH_FAILED",
    "AUTH is checked through TLS"
  )
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
  assertEqualInt(c.connections(), 2, "two TLS handshakes")
  val _ = ok(Redis.del(c, [k]), "DEL")
  c.close()
  val _ = ok(Redis.command(admin, ["ACL", "DELUSER", user]), "ACL DELUSER")
  admin.close()
}

func clientId(c: in RedisClient): String {
  match ok(Redis.command(c, ["CLIENT", "ID"]), "CLIENT ID") {
    case RInt(n) -> n.toString()
    case _ -> panic("CLIENT ID returns an integer")
  }
}

test "a TLS connection the server killed is replaced and the command repeated" {
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

test "without the stale retry a killed TLS connection fails once, then recovers" {
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

test "a reply slower than the I/O timeout is ConnectionLost over TLS and the client recovers" {
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

test "one TLS client shared by eight threads loses no command" {
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
