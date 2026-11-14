//! tests/core_tests.l - the static and environment providers, chains, the
//! cache (with an injected clock, and under concurrent use) and timestamps.
@test_module
package Aws.Credentials.CoreTests

import Std.Core
import Std.Collections
import Std.String as Str
import Std.Testing
import Std.Time as Time
import Aws.SigV4
import Aws.Credentials

// Fixture values, not real credentials.
val SECRET: String = "fixture/secret+key/for-tests-only"

val TOKEN: String = "fixture-session-token"

func describe(r: in Result[ResolvedCredentials, CredentialError]): String {
  match r {
    case Ok(c) -> "ok " + c.credentials.accessKeyId + " from " + c.source +
      (if c.credentials.sessionToken.length > 0 { " token" } else { "" }) +
      (match c.expiresAtEpochMs { case Some(at) -> " until " + at.toString(); case None -> "" })
    case Err(e) -> e.code + " " + e.text()
  }
}

func idOf(r: in Result[ResolvedCredentials, CredentialError]): String {
  match r {
    case Ok(c) -> c.credentials.accessKeyId
    case Err(e) -> e.code
  }
}

func envOf(pairs: in slice[String]): (String) -> String {
  val values: Map[String, String] = newMap()
  var i = 0
  while i + 1 < pairs.length {
    values[pairs[i]] = pairs[i + 1]
    i = i + 2
  }
  environmentOf(values)
}

test "static credentials are returned as given, and empty ones are refused" {
  match staticProvider("AKIASTATIC", SECRET, "") {
    case Err(e) -> assertTrue(false, e.text())
    case Ok(p) -> assertEqual(describe(p.resolve()), "ok AKIASTATIC from static", "long-term")
  }
  match staticProvider("AKIASTATIC", SECRET, TOKEN) {
    case Err(e) -> assertTrue(false, e.text())
    case Ok(p) -> assertEqual(describe(p.resolve()), "ok AKIASTATIC from static token", "temporary")
  }
  match staticProvider("", SECRET, "") {
    case Ok(_) -> assertTrue(false, "an empty id was accepted")
    case Err(e) -> {
      assertEqual(e.code, "INVALID_CONFIG", "empty id")
      assertTrue(not Str.contains(e.text(), SECRET), "the secret is not in the message")
    }
  }
  match staticProvider("AKIASTATIC", "", "") {
    case Ok(_) -> assertTrue(false, "an empty secret was accepted")
    case Err(e) -> assertEqual(e.code, "INVALID_CONFIG", "empty secret")
  }
}

test "the environment provider reads the three variables on every call" {
  val values: Map[String, String] = newMap()
  val p = environmentProvider(environmentOf(values))
  assertEqual(
    describe(p.resolve()),
    "NOT_CONFIGURED environment: AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are not set",
    "nothing set"
  )
  values["AWS_ACCESS_KEY_ID"] = "AKIAENV"
  assertEqual(
    describe(p.resolve()),
    "INVALID_CONFIG environment: AWS_ACCESS_KEY_ID is set but AWS_SECRET_ACCESS_KEY is not",
    "half set"
  )
  values["AWS_SECRET_ACCESS_KEY"] = SECRET
  assertEqual(describe(p.resolve()), "ok AKIAENV from environment", "both set")
  values["AWS_SESSION_TOKEN"] = TOKEN
  assertEqual(describe(p.resolve()), "ok AKIAENV from environment token", "with a token")
  values["AWS_ACCESS_KEY_ID"] = ""
  val half = describe(p.resolve())
  assertEqual(
    half,
    "INVALID_CONFIG environment: AWS_SECRET_ACCESS_KEY is set but AWS_ACCESS_KEY_ID is not",
    "other half"
  )
  assertTrue(not Str.contains(half, SECRET), "the secret is not in the message")
}

// A provider that answers from a script and counts its calls.
ref record Scripted {
  name: String
  var calls: Int
  var answer: Result[ResolvedCredentials, CredentialError]
}

impl CredentialProvider for Scripted {
  func resolve(): Result[ResolvedCredentials, CredentialError] {
    self.calls = self.calls + 1
    self.answer
  }
}

func temporary(id: in String, expiresAtMs: in Long): Result[ResolvedCredentials, CredentialError] {
  Ok(
    ResolvedCredentials(
      credentials = Credentials(accessKeyId = id, secretAccessKey = SECRET, sessionToken = TOKEN),
      expiresAtEpochMs = Some(value = expiresAtMs),
      source = "scripted"
    )
  )
}

func longTerm(id: in String): Result[ResolvedCredentials, CredentialError] {
  Ok(
    ResolvedCredentials(
      credentials = Credentials(accessKeyId = id, secretAccessKey = SECRET, sessionToken = ""),
      expiresAtEpochMs = None,
      source = "scripted"
    )
  )
}

func failure(code: in String, provider: in String, message: in String): Result[ResolvedCredentials, CredentialError] {
  Err(CredentialError(code = code, provider = provider, message = message))
}

func scripted(name: in String, answer: in Result[ResolvedCredentials, CredentialError]): Scripted {
  Scripted(name = name, calls = 0, answer = answer)
}

func chainOfThree(a: in Scripted, b: in Scripted, c: in Scripted): CredentialProvider {
  val providers: List[CredentialProvider] = newList()
  providers.add(a)
  providers.add(b)
  providers.add(c)
  chainOf(providers)
}

test "a chain returns the first credentials and does not ask later providers" {
  val a = scripted("a", failure("NOT_CONFIGURED", "a", "nothing here"))
  val b = scripted("b", longTerm("AKIAB"))
  val c = scripted("c", longTerm("AKIAC"))
  assertEqual(describe(chainOfThree(a, b, c).resolve()), "ok AKIAB from scripted", "the second provider answers")
  assertEqualInt(a.calls, 1, "a was asked")
  assertEqualInt(b.calls, 1, "b was asked")
  assertEqualInt(c.calls, 0, "c was not asked")
}

test "a chain passes over NOT_CONFIGURED and UNAVAILABLE, and reports every reason when none answers" {
  val a = scripted("a", failure("NOT_CONFIGURED", "a", "variable unset"))
  val b = scripted("b", failure("UNAVAILABLE", "b", "no route"))
  val c = scripted("c", failure("NOT_CONFIGURED", "c", "no file"))
  assertEqual(
    describe(chainOfThree(a, b, c).resolve()),
    "NO_CREDENTIALS chain: no provider has credentials: a: variable unset; b: no route; c: no file",
    "every reason, in order"
  )
  assertEqualInt(c.calls, 1, "the last provider was asked")
}

test "a chain stops at a provider that is configured but broken" {
  val a = scripted("a", failure("NOT_CONFIGURED", "a", "variable unset"))
  val b = scripted("b", failure("UNSUPPORTED_PROFILE", "b", "the profile uses SSO"))
  val c = scripted("c", longTerm("AKIAC"))
  assertEqual(describe(chainOfThree(a, b, c).resolve()), "UNSUPPORTED_PROFILE b: the profile uses SSO", "b's error")
  assertEqualInt(c.calls, 0, "c was not asked: its credentials would not be the configured ones")
}

// ─── The cache, with an injected clock ───────────────────────────────────────
ref record Clock {
  var nowMs: Long
}

func optionsAt(clock: in Clock): CacheOptions {
  CacheOptions(refreshWindowMs = 300000i64, retryAfterFailureMs = 30000i64, nowMs = { -> clock.nowMs })
}

val T0: Long = 1800000000000i64

val HOUR: Long = 3600000i64

test "long-term credentials are asked for once" {
  val clock = Clock(nowMs = T0)
  val inner = scripted("inner", longTerm("AKIALONG"))
  val cache = cachedWith(inner, optionsAt(clock))
  assertEqual(describe(cache.resolve()), "ok AKIALONG from scripted", "first")
  clock.nowMs = T0 + 1000i64 * HOUR
  assertEqual(describe(cache.resolve()), "ok AKIALONG from scripted", "much later")
  assertEqualInt(inner.calls, 1, "asked once")
  assertEqualInt(cache.stats().refreshes, 1, "one refresh")
}

test "temporary credentials are reused until the refresh window, then replaced" {
  val clock = Clock(nowMs = T0)
  val inner = scripted("inner", temporary("ASIA1", T0 + HOUR))
  val cache = cachedWith(inner, optionsAt(clock))
  assertEqual(describe(cache.resolve()), "ok ASIA1 from scripted token until " + (T0 + HOUR).toString(), "first")
  inner.answer = temporary("ASIA2", T0 + 2i64 * HOUR)
  clock.nowMs = T0 + HOUR - 300001i64
  assertEqual(idOf(cache.resolve()), "ASIA1", "one millisecond before the window")
  assertEqualInt(inner.calls, 1, "not asked again yet")
  clock.nowMs = T0 + HOUR - 300000i64
  assertEqual(idOf(cache.resolve()), "ASIA2", "at the window: refreshed before expiry")
  assertEqualInt(inner.calls, 2, "asked again")
  assertEqual(idOf(cache.resolve()), "ASIA2", "and reused")
  assertEqualInt(inner.calls, 2, "not asked a third time")
}

test "a failed refresh keeps serving unexpired credentials, and is not repeated at once" {
  val clock = Clock(nowMs = T0)
  val inner = scripted("inner", temporary("ASIA1", T0 + HOUR))
  val cache = cachedWith(inner, optionsAt(clock))
  assertEqual(idOf(cache.resolve()), "ASIA1", "first")
  inner.answer = failure("REQUEST_FAILED", "scripted", "the service is down")
  clock.nowMs = T0 + HOUR - 120000i64
  assertEqual(idOf(cache.resolve()), "ASIA1", "refresh failed, old credentials served")
  assertEqualInt(inner.calls, 2, "the refresh was attempted")
  clock.nowMs = clock.nowMs + 29999i64
  assertEqual(idOf(cache.resolve()), "ASIA1", "still served")
  assertEqualInt(inner.calls, 2, "no second attempt within retryAfterFailureMs")
  clock.nowMs = clock.nowMs + 1i64
  assertEqual(idOf(cache.resolve()), "ASIA1", "served after another failed attempt")
  assertEqualInt(inner.calls, 3, "a second attempt after retryAfterFailureMs")
  assertEqualInt(cache.stats().failures, 2, "two failures counted")
  // Once they have expired there is nothing to serve: the error is returned.
  clock.nowMs = T0 + HOUR
  assertEqual(describe(cache.resolve()), "REQUEST_FAILED scripted: the service is down", "expired and no refresh")
  // And a later success replaces them.
  inner.answer = temporary("ASIA3", T0 + 3i64 * HOUR)
  assertEqual(idOf(cache.resolve()), "ASIA3", "recovered")
}

test "an error is not cached when there were never credentials" {
  val clock = Clock(nowMs = T0)
  val inner = scripted("inner", failure("NOT_CONFIGURED", "scripted", "nothing"))
  val cache = cachedWith(inner, optionsAt(clock))
  assertEqual(describe(cache.resolve()), "NOT_CONFIGURED scripted: nothing", "first")
  inner.answer = longTerm("AKIALATE")
  assertEqual(describe(cache.resolve()), "ok AKIALATE from scripted", "asked again at the next call")
}

test "invalidate makes the next resolve ask the provider" {
  val clock = Clock(nowMs = T0)
  val inner = scripted("inner", longTerm("AKIAOLD"))
  val cache = cachedWith(inner, optionsAt(clock))
  assertEqual(idOf(cache.resolve()), "AKIAOLD", "first")
  inner.answer = longTerm("AKIANEW")
  cache.invalidate()
  assertEqual(idOf(cache.resolve()), "AKIANEW", "after invalidate")
  assertEqualInt(inner.calls, 2, "asked twice")
}

// ─── The cache under concurrent use ──────────────────────────────────────────
protected type Tally {
  var calls: Int = 0
  var arrived: Int = 0
  var results: List[String] = newList()
  entry arrive() {
    arrived = arrived + 1
  }
  // Blocks until all four callers are on their way into the cache.
  entry awaitFourArrivals()
    when: arrived >= 4
  {
  }
  entry called(): Int {
    calls = calls + 1
    calls
  }
  entry record(text: in String) {
    results.add(text)
  }
  entry callCount(): Int {
    calls
  }
  entry all(): List[String] {
    val copy: List[String] = newList()
    for r in results {
      copy.add(r)
    }
    copy
  }
}

// A provider that does not answer before all four callers have started, and
// then takes half a second more, so that the callers overlap however late
// the runtime starts a thread (the .NET thread pool can hold a task back for
// longer than any fixed pause).
record Slow {
  tally: Tally
  fails: Bool
}

impl CredentialProvider for Slow {
  func resolve(): Result[ResolvedCredentials, CredentialError] {
    val n = self.tally.called()
    self.tally.awaitFourArrivals()
    Time.sleepMillis(500)
    if self.fails {
      failure("REQUEST_FAILED", "slow", "attempt " + n.toString() + " failed")
    } else {
      longTerm("AKIASLOW" + n.toString())
    }
  }
}

@cfg(target = "dotnet")
extern type Task = "System.Threading.Tasks.Task"

@cfg(target = "dotnet")
@externStatic
@externTarget("System.Threading.Tasks.Task.Run")
func taskRun(work: in () -> Unit): Task = ()

@cfg(target = "dotnet")
@externInstance
@externTarget("System.Threading.Tasks.Task.Wait")
func taskWait(t: in Task): Unit = ()

// A task may not be handed an interface value directly (T0225): the cache
// reaches the four tasks through a protected type, and each calls `resolve`
// outside its lock.
protected type Shared {
  provider: CredentialProvider
  entry get(): CredentialProvider {
    provider
  }
}

func resolveInto(shared: in Shared, tally: in Tally): Unit {
  val cache = shared.get()
  tally.arrive()
  tally.record(describe(cache.resolve()))
}

@cfg(target = "dotnet")
func fourAtOnce(provider: in CredentialProvider, tally: in Tally): Unit {
  val cache = Shared(provider = provider)
  val a = taskRun({ -> resolveInto(cache, tally) })
  val b = taskRun({ -> resolveInto(cache, tally) })
  val c = taskRun({ -> resolveInto(cache, tally) })
  val d = taskRun({ -> resolveInto(cache, tally) })
  taskWait(a)
  taskWait(b)
  taskWait(c)
  taskWait(d)
}

@cfg(target = "jvm")
func fourAtOnce(provider: in CredentialProvider, tally: in Tally): Unit {
  val cache = Shared(provider = provider)
  scope {
    val a = spawn resolveInto(cache, tally)
    val b = spawn resolveInto(cache, tally)
    val c = spawn resolveInto(cache, tally)
    val d = spawn resolveInto(cache, tally)
    await a
    await b
    await c
    await d
  }
}

test "four threads that need credentials at once cause one call of the provider" {
  val tally = Tally()
  val slow: CredentialProvider = Slow(tally = tally, fails = false)
  val cache = cached(slow)
  fourAtOnce(cache, tally)
  val results = tally.all()
  assertEqualInt(results.count, 4, "four results")
  for r in results {
    assertEqual(r, "ok AKIASLOW1 from scripted", "every thread got the one refresh's credentials")
  }
  assertEqualInt(tally.callCount(), 1, "the provider was called once")
}

test "four threads waiting on a refresh that fails all receive its error, from one call" {
  val tally = Tally()
  val slow: CredentialProvider = Slow(tally = tally, fails = true)
  val cache = cached(slow)
  fourAtOnce(cache, tally)
  val results = tally.all()
  assertEqualInt(results.count, 4, "four results")
  for r in results {
    assertEqual(r, "REQUEST_FAILED slow: attempt 1 failed", "every thread got the one refresh's error")
  }
  assertEqualInt(tally.callCount(), 1, "the provider was called once")
}

// ─── Timestamps ──────────────────────────────────────────────────────────────
func stamp(text: in String): String {
  match parseTimestampMs(text) {
    case Some(ms) -> ms.toString()
    case None -> "none"
  }
}

test "expiry timestamps are read as AWS writes them" {
  assertEqual(stamp("1970-01-01T00:00:00Z"), "0", "the epoch")
  assertEqual(stamp("2026-11-13T10:15:00Z"), "1794564900000", "seconds, Z")
  assertEqual(stamp("2026-11-13T10:15:00.250Z"), "1794564900250", "milliseconds")
  assertEqual(stamp("2026-11-13T10:15:00.123456789Z"), "1794564900123", "nanoseconds are cut")
  assertEqual(stamp("2026-11-13T11:15:00+01:00"), "1794564900000", "a positive offset")
  assertEqual(stamp("2026-11-13T05:45:00-04:30"), "1794564900000", "a negative offset")
  assertEqual(stamp("2024-02-29T12:00:00Z"), "1709208000000", "a leap day")
  assertEqual(stamp(" 2026-11-13T10:15:00Z\n"), "1794564900000", "surrounding whitespace")
  assertEqual(stamp("2026-11-13 10:15:00Z"), "none", "no T")
  assertEqual(stamp("2026-11-13T10:15:00"), "none", "no zone")
  assertEqual(stamp("2026-13-13T10:15:00Z"), "none", "month 13")
  assertEqual(stamp("2026-11-13T10:15:00.Z"), "none", "no fraction digits")
  assertEqual(stamp("yesterday"), "none", "not a timestamp")
  assertEqual(stamp(""), "none", "empty")
}
