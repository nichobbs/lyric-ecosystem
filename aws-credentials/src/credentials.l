//! Aws.Credentials - where AWS credentials come from.
//!
//! A `CredentialProvider` answers `resolve()` with credentials and, for
//! temporary ones, the instant they expire. This package holds the interface,
//! the two providers that need nothing but values (`staticProvider`,
//! `environmentProvider`), `chainOf`, which asks providers in order, and
//! `cached`, which keeps what a provider returned until shortly before it
//! expires and is safe to share between threads.
//!
//! The providers that read files or call a service are in
//! `Aws.Credentials.Profile` and `Aws.Credentials.Remote`; the AWS default
//! chain is `Aws.Credentials.Chain`.
//!
//! A `CredentialError` has a `code`:
//!
//!   * `NOT_CONFIGURED`: the provider has nothing to work with (a variable is
//!     unset, a file or profile does not exist). A chain moves on.
//!   * `UNAVAILABLE`: the provider's service cannot be reached at all (the
//!     instance metadata service off EC2). A chain moves on.
//!   * `INVALID_CONFIG`: the provider is configured, but wrongly.
//!   * `UNSUPPORTED_PROFILE`: the profile needs a mechanism this package does
//!     not implement (SSO, `role_arn` with `source_profile`).
//!   * `REQUEST_FAILED`, `MALFORMED_RESPONSE`: the credential service
//!     answered with an error, or with something that is not credentials.
//!   * `PROCESS_FAILED`: a `credential_process` command failed.
//!   * `NO_CREDENTIALS`: no provider of a chain had credentials.
//!
//! No message ever holds a secret access key, a session token, a bearer token
//! or the text of a line of a credentials file.
@runtime_checked
@io
package Aws.Credentials

import Std.Core
import Std.Collections
import Std.Environment as Env
import Std.String as Str
import Std.Time as Time
import Aws.SigV4
import Aws.Credentials.Guard

/// Why a provider has no credentials.
pub record CredentialError {
  /// One of the codes in the package documentation.
  code: String
  /// The provider that failed (`environment`, `profile`, `imds`).
  provider: String
  message: String
}

/// `<provider>: <message>`.
@pure
pub func CredentialError.text(e: in CredentialError): String {
  e.provider + ": " + e.message
}

/// True for the two codes a chain moves on from.
@pure
pub func CredentialError.isSkippable(e: in CredentialError): Bool {
  e.code == "NOT_CONFIGURED" or e.code == "UNAVAILABLE"
}

/// Credentials and when they stop working.
pub record ResolvedCredentials {
  credentials: Credentials
  /// The expiry in milliseconds since the Unix epoch; `None` for long-term
  /// credentials.
  expiresAtEpochMs: Option[Long]
  /// The provider they came from.
  source: String
}

/// True when the credentials have an expiry that is at or before `nowMs`.
@pure
pub func ResolvedCredentials.isExpiredAt(c: in ResolvedCredentials, nowMs: in Long): Bool {
  match c.expiresAtEpochMs {
    case Some(at) -> at <= nowMs
    case None -> false
  }
}

/// A source of AWS credentials.
pub interface CredentialProvider {
  /// The credentials to sign with now. Providers do no caching of their own:
  /// each call reads the variables or files, or calls the service, again.
  /// Wrap a provider in `cached` to share it.
  func resolve(): Result[ResolvedCredentials, CredentialError]
}

/// The current time in milliseconds since the Unix epoch, from the system
/// clock.
pub func systemClockMs(): Long {
  Time.nowEpochMillis()
}

/// A lookup of environment variables in the process environment: the value,
/// or `""` when the variable is unset.
pub func processEnvironment(): (String) -> String {
  { name: String -> Env.getVarOrDefault(name, "") }
}

/// A lookup over `values`, for tests and for applications that keep AWS
/// settings somewhere other than the process environment.
pub func environmentOf(values: in Map[String, String]): (String) -> String {
  { name: String -> match mapGet(values, name) { case Some(v) -> v; case None -> "" } }
}

// ─── Static ──────────────────────────────────────────────────────────────────
record StaticProvider {
  resolved: ResolvedCredentials
}

impl CredentialProvider for StaticProvider {
  func resolve(): Result[ResolvedCredentials, CredentialError] {
    Ok(self.resolved)
  }
}

/// A provider of fixed credentials. `sessionToken` is `""` for long-term
/// credentials. `Err(INVALID_CONFIG)` when the id or the secret is empty.
pub func staticProvider(
  accessKeyId: in String, secretAccessKey: in String, sessionToken: in String
): Result[CredentialProvider, CredentialError] {
  if accessKeyId.length == 0 or secretAccessKey.length == 0 {
    return Err(
      CredentialError(
        code = "INVALID_CONFIG",
        provider = "static",
        message = "the access key id and the secret access key must be non-empty"
      )
    )
  }
  Ok(
    staticProviderOf(
      Credentials(accessKeyId = accessKeyId, secretAccessKey = secretAccessKey, sessionToken = sessionToken)
    )
  )
}

/// A provider of `credentials`, which never expire.
pub func staticProviderOf(credentials: in Credentials): CredentialProvider {
  val p: CredentialProvider = StaticProvider(
    resolved = ResolvedCredentials(
      credentials = credentials,
      expiresAtEpochMs = None,
      source = "static"
    )
  )
  p
}

// ─── Environment ─────────────────────────────────────────────────────────────
record EnvironmentProvider {
  lookup: (String) -> String
}

impl CredentialProvider for EnvironmentProvider {
  func resolve(): Result[ResolvedCredentials, CredentialError] {
    val lookup = self.lookup
    val accessKeyId = lookup("AWS_ACCESS_KEY_ID")
    val secretAccessKey = lookup("AWS_SECRET_ACCESS_KEY")
    if accessKeyId.length == 0 and secretAccessKey.length == 0 {
      return Err(
        CredentialError(
          code = "NOT_CONFIGURED",
          provider = "environment",
          message = "AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are not set"
        )
      )
    }
    if accessKeyId.length == 0 or secretAccessKey.length == 0 {
      val missing = if accessKeyId.length == 0 { "AWS_ACCESS_KEY_ID" } else { "AWS_SECRET_ACCESS_KEY" }
      val present = if accessKeyId.length == 0 { "AWS_SECRET_ACCESS_KEY" } else { "AWS_ACCESS_KEY_ID" }
      return Err(
        CredentialError(
          code = "INVALID_CONFIG",
          provider = "environment",
          message = present + " is set but " + missing + " is not"
        )
      )
    }
    Ok(
      ResolvedCredentials(
        credentials = Credentials(
          accessKeyId = accessKeyId,
          secretAccessKey = secretAccessKey,
          sessionToken = lookup("AWS_SESSION_TOKEN")
        ),
        expiresAtEpochMs = None,
        source = "environment"
      )
    )
  }
}

/// `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and the optional
/// `AWS_SESSION_TOKEN`, read through `lookup` on every `resolve`. Neither
/// variable set is `NOT_CONFIGURED`; exactly one is `INVALID_CONFIG`.
pub func environmentProvider(lookup: in (String) -> String): CredentialProvider {
  val p: CredentialProvider = EnvironmentProvider(lookup = lookup)
  p
}

// ─── Chain ───────────────────────────────────────────────────────────────────
record ChainProvider {
  providers: List[CredentialProvider]
}

impl CredentialProvider for ChainProvider {
  func resolve(): Result[ResolvedCredentials, CredentialError] {
    val skipped: List[String] = newList()
    var i = 0
    while i < self.providers.count {
      match self.providers[i].resolve() {
        case Ok(c) -> return Ok(c)
        case Err(e) -> {
          if not e.isSkippable() {
            return Err(e)
          }
          skipped.add(e.text())
        }
      }
      i = i + 1
    }
    Err(
      CredentialError(
        code = "NO_CREDENTIALS",
        provider = "chain",
        message = "no provider has credentials: " + Str.joinList("; ", skipped)
      )
    )
  }
}

/// A provider that asks `providers` in order and returns the first
/// credentials. A provider that is `NOT_CONFIGURED` or `UNAVAILABLE` is
/// passed over; any other error ends the chain and is returned as it is, so
/// a provider that is configured but broken is never silently replaced by a
/// later one. When every provider was passed over the error is
/// `NO_CREDENTIALS` and its message holds each provider's reason.
pub func chainOf(providers: in List[CredentialProvider]): CredentialProvider {
  val copy: List[CredentialProvider] = newList()
  var i = 0
  while i < providers.count {
    copy.add(providers[i])
    i = i + 1
  }
  val p: CredentialProvider = ChainProvider(providers = copy)
  p
}

// ─── Cache ───────────────────────────────────────────────────────────────────
/// How `cachedWith` keeps credentials.
pub record CacheOptions {
  /// Credentials are refreshed once they are within this many milliseconds
  /// of their expiry.
  refreshWindowMs: Long
  /// After a failed refresh, unexpired credentials are served without
  /// another attempt for this many milliseconds.
  retryAfterFailureMs: Long
  /// The clock: milliseconds since the Unix epoch.
  nowMs: () -> Long
}

/// Refresh five minutes before expiry, retry a failed refresh after thirty
/// seconds, the system clock.
pub func defaultCacheOptions(): CacheOptions {
  CacheOptions(refreshWindowMs = 300000i64, retryAfterFailureMs = 30000i64, nowMs = { -> systemClockMs() })
}

/// What a caller of a cache must do; part of `CacheState`.
pub union Claim {
  case Use(credentials: ResolvedCredentials)
  // The caller refreshes; `fallback` is what to return if that fails.
  case Refresh(fallback: Option[ResolvedCredentials])
  // Another caller is refreshing and there is nothing to serve meanwhile.
  case Wait
}

/// How a refresh another caller made ended; part of `CacheState`.
pub record Settled {
  current: Option[ResolvedCredentials]
  error: Option[CredentialError]
}

/// What a cache has done, for monitoring and tests.
pub record CacheStats {
  /// Calls of the wrapped provider that returned credentials.
  refreshes: Int
  /// Calls of the wrapped provider that failed.
  failures: Int
}

/// The state of a `CachedProvider`. It is public only because a public
/// record holds it; use it through `CachedProvider`.
pub protected type CacheState {
  var current: Option[ResolvedCredentials] = None
  var refreshing: Bool = false
  var lastError: Option[CredentialError] = None
  var lastFailureAtMs: Long = 0i64
  var failedSinceSuccess: Bool = false
  var refreshes: Int = 0
  var failures: Int = 0
  entry claim(nowMs: in Long, windowMs: in Long, retryAfterFailureMs: in Long): Claim {
    match current {
      case Some(c) -> {
        val fresh = match c.expiresAtEpochMs { case None -> true; case Some(at) -> nowMs < at - windowMs }
        if fresh {
          return Use(credentials = c)
        }
        val usable = not c.isExpiredAt(nowMs)
        if refreshing {
          return if usable { Use(credentials = c) } else { Wait }
        }
        if usable and failedSinceSuccess and nowMs - lastFailureAtMs < retryAfterFailureMs {
          return Use(credentials = c)
        }
        refreshing = true
        Refresh(fallback = if usable { Some(value = c) } else { None })
      }
      case None -> {
        if refreshing {
          return Wait
        }
        refreshing = true
        Refresh(fallback = None)
      }
    }
  }
  entry install(fresh: in ResolvedCredentials) {
    current = Some(value = fresh)
    lastError = None
    failedSinceSuccess = false
    refreshing = false
    refreshes = refreshes + 1
  }
  entry failed(e: in CredentialError, nowMs: in Long) {
    lastError = Some(value = e)
    lastFailureAtMs = nowMs
    failedSinceSuccess = true
    refreshing = false
    failures = failures + 1
  }
  // Blocks while another caller refreshes, then reports how that ended.
  entry settled(): Settled
    when: not refreshing
  {
    Settled(current = current, error = lastError)
  }
  entry drop() {
    current = None
  }
  entry stats(): CacheStats {
    CacheStats(refreshes = refreshes, failures = failures)
  }
}

// Where a guarded call leaves its result: a closure cannot return it through
// `guardedCall`.
ref record ResolveSlot {
  var outcome: Option[Result[ResolvedCredentials, CredentialError]]
}

func resolveCatching(
  provider: in CredentialProvider, reportedBy: in String, prefix: in String
): Result[ResolvedCredentials, CredentialError] {
  val slot = ResolveSlot(outcome = None)
  val panicked = guardedCall({ -> slot.outcome = Some(value = provider.resolve()) })
  match panicked {
    case Some(message) -> Err(
      error = CredentialError(
        code = "REQUEST_FAILED",
        provider = reportedBy,
        message = prefix + message
      )
    )
    case None -> match slot.outcome {
      case Some(outcome) -> outcome
      case None -> Err(
        error = CredentialError(
          code = "REQUEST_FAILED",
          provider = reportedBy,
          message = prefix + "no result"
        )
      )
    }
  }
}

/// `provider.resolve()`, for a provider written outside this package. On
/// `dotnet` and the JVM a panic in it is `Err(REQUEST_FAILED)` from provider
/// `provider` with the panic's message. On `native` there is no unwinding
/// (D-N-003 in lyric-lang): a panicking provider ends the process.
pub func resolveGuarded(provider: in CredentialProvider): Result[ResolvedCredentials, CredentialError] {
  resolveCatching(provider, "provider", "")
}

/// A provider that remembers what another provider returned.
pub record CachedProvider {
  inner: CredentialProvider
  options: CacheOptions
  state: CacheState
}

impl CredentialProvider for CachedProvider {
  func resolve(): Result[ResolvedCredentials, CredentialError] {
    val clock = self.options.nowMs
    while true {
      val now = clock()
      match self.state.claim(now, self.options.refreshWindowMs, self.options.retryAfterFailureMs) {
        case Use(c) -> return Ok(c)
        case Refresh(fallback) -> {
          val outcome = resolveCatching(self.inner, "cache", "the provider failed: ")
          match outcome {
            case Ok(fresh) -> {
              self.state.install(fresh)
              return Ok(fresh)
            }
            case Err(e) -> {
              self.state.failed(e, clock())
              match fallback {
                case Some(c) -> return Ok(c)
                case None -> return Err(e)
              }
            }
          }
        }
        case Wait -> {
          val after = self.state.settled()
          match after.current {
            case Some(c) -> if not c.isExpiredAt(clock()) { return Ok(c) }
            case None -> ()
          }
          match after.error {
            case Some(e) -> return Err(e)
            case None -> ()
          }
        }
      }
    }
    Err(CredentialError(code = "NO_CREDENTIALS", provider = "cache", message = "unreachable"))
  }
}

/// Forget the cached credentials, so the next `resolve` asks the wrapped
/// provider (after credentials were rotated, or a service refused them).
pub func CachedProvider.invalidate(p: in CachedProvider): Unit {
  p.state.drop()
}

/// How often the wrapped provider was asked, and how often it failed.
pub func CachedProvider.stats(p: in CachedProvider): CacheStats {
  p.state.stats()
}

/// `p` as a `CredentialProvider`.
pub func CachedProvider.asProvider(p: in CachedProvider): CredentialProvider {
  val provider: CredentialProvider = p
  provider
}

/// `inner` behind a cache with `options`.
///
/// Credentials without an expiry are asked for once. Credentials with one are
/// returned as they are until `refreshWindowMs` before it; the first caller
/// after that asks `inner` again while every other caller is still given the
/// current, unexpired credentials. If that refresh fails the current
/// credentials are served until they expire, and `inner` is not asked again
/// for `retryAfterFailureMs`. Once they have expired a caller either
/// refreshes or waits for the caller that is refreshing, and receives that
/// refresh's outcome; `inner` is never called by two threads at once.
pub func cachedWith(inner: in CredentialProvider, options: in CacheOptions): CachedProvider
  requires: options.refreshWindowMs >= 0i64
  requires: options.retryAfterFailureMs >= 0i64
{
  CachedProvider(inner = inner, options = options, state = CacheState())
}

/// `inner` behind a cache with `defaultCacheOptions()`.
pub func cached(inner: in CredentialProvider): CredentialProvider {
  cachedWith(inner, defaultCacheOptions()).asProvider()
}

// ─── Timestamps ──────────────────────────────────────────────────────────────
func digitsAt(text: in String, start: in Int, count: in Int): Option[Long] {
  if start + count > text.length {
    return None
  }
  var n = 0i64
  var i = 0
  while i < count {
    val u = Str.codeUnitAt(text, start + i)
    if u < 48 or u > 57 {
      return None
    }
    n = n * 10i64 + (u - 48).toLong()
    i = i + 1
  }
  Some(value = n)
}

// Days from 1970-01-01 to the civil date, proleptic Gregorian.
func daysFromCivil(year: in Long, month: in Long, day: in Long): Long {
  val y = if month <= 2i64 { year - 1i64 } else { year }
  val era = (if y >= 0i64 { y } else { y - 399i64 }) / 400i64
  val yoe = y - era * 400i64
  val mp = if month > 2i64 { month - 3i64 } else { month + 9i64 }
  val doy = (153i64 * mp + 2i64) / 5i64 + day - 1i64
  val doe = yoe * 365i64 + yoe / 4i64 - yoe / 100i64 + doy
  era * 146097i64 + doe - 719468i64
}

/// The instant an ISO 8601 / RFC 3339 timestamp names, in milliseconds since
/// the Unix epoch: `2026-11-13T10:15:00Z`, with optional fractional seconds
/// and a `Z` or `+hh:mm` / `-hh:mm` offset. `None` for anything else. This is
/// the form of every `Expiration` AWS returns.
@pure
pub func parseTimestampMs(text: in String): Option[Long] {
  val t = Str.trim(text)
  if t.length < 20 {
    return None
  }
  val sep = Str.charAtOrReplacement(t, 10)
  if Str.charAtOrReplacement(t, 4) != '-' or Str.charAtOrReplacement(t, 7) != '-' or (sep != 'T' and sep != 't') or Str.charAtOrReplacement(t, 13) != ':' or Str.charAtOrReplacement(t, 16) != ':' {
    return None
  }
  val year = match digitsAt(t, 0, 4) { case Some(v) -> v; case None -> return None }
  val month = match digitsAt(t, 5, 2) { case Some(v) -> v; case None -> return None }
  val day = match digitsAt(t, 8, 2) { case Some(v) -> v; case None -> return None }
  val hour = match digitsAt(t, 11, 2) { case Some(v) -> v; case None -> return None }
  val minute = match digitsAt(t, 14, 2) { case Some(v) -> v; case None -> return None }
  val second = match digitsAt(t, 17, 2) { case Some(v) -> v; case None -> return None }
  if month < 1i64 or month > 12i64 or day < 1i64 or day > 31i64 or hour > 23i64 or minute > 59i64 or second > 60i64 {
    return None
  }
  var i = 19
  var millis = 0i64
  if Str.charAtOrReplacement(t, i) == '.' {
    i = i + 1
    var scale = 100i64
    var seen = 0
    while i < t.length and Str.codeUnitAt(t, i) >= 48 and Str.codeUnitAt(t, i) <= 57 {
      millis = millis + (Str.codeUnitAt(t, i) - 48).toLong() * scale
      scale = scale / 10i64
      seen = seen + 1
      i = i + 1
    }
    if seen == 0 {
      return None
    }
  }
  if i >= t.length {
    return None
  }
  var offsetMinutes = 0i64
  val zone = Str.charAtOrReplacement(t, i)
  if zone == 'Z' or zone == 'z' {
    if i + 1 != t.length {
      return None
    }
  } else if zone == '+' or zone == '-' {
    if i + 6 != t.length or Str.charAtOrReplacement(t, i + 3) != ':' {
      return None
    }
    val oh = match digitsAt(t, i + 1, 2) { case Some(v) -> v; case None -> return None }
    val om = match digitsAt(t, i + 4, 2) { case Some(v) -> v; case None -> return None }
    if oh > 23i64 or om > 59i64 {
      return None
    }
    offsetMinutes = oh * 60i64 + om
    if zone == '-' {
      offsetMinutes = 0i64 - offsetMinutes
    }
  } else {
    return None
  }
  val days = daysFromCivil(year, month, day)
  Some(value = ((days * 24i64 + hour) * 60i64 + minute - offsetMinutes) * 60000i64 + second * 1000i64 + millis)
}
