//! Aws.Retry - when and how long to wait before repeating an AWS request,
//! modelled on the "standard" retry mode of the AWS SDKs.
//!
//! A client classifies what happened to an attempt (`FailureKind`), says
//! whether repeating the request is harmless (`idempotent`), and asks
//! `RetryPolicy.decide`. The policy answers `GiveUp` or `RetryAfter(ms)`; the
//! client waits with `RetryPolicy.pause` and sends the request again, signed
//! afresh.
//!
//! What is retried:
//!
//!   * `Throttled` and `ConnectFailed` always: the service said it did not
//!     process the request, or the request never left;
//!   * `Transient`, `NoResponse` and `TimedOut` only for an idempotent
//!     request: the request may have been carried out;
//!   * `ClockSkew` and `NotRetryable` never. A clock skew error is classified
//!     so that a caller can report it; repeating the request with the same
//!     clock would fail the same way, and this package does not correct the
//!     clock.
//!
//! The wait before attempt `n + 1` is drawn uniformly from
//! `[0, min(maxDelayMs, baseDelayMs * 2^(n-1))]` ("full jitter"), and is at
//! least what the service asked for in `Retry-After` or `x-amz-retry-after`,
//! up to `maxDelayMs`. The retry quota (token bucket) of the SDKs' standard
//! mode is not implemented.
@runtime_checked
@io
package Aws.Retry

import Std.Core
import Std.Random as Random
import Std.String as Str
import Std.Time as Time

/// What happened to an attempt that did not succeed.
pub enum FailureKind {
  /// The service refused the request for load reasons and did not process
  /// it: a throttling error code, HTTP 429 or 503.
  case Throttled
  /// The service failed (HTTP 500, 502, 504, `RequestTimeout`,
  /// `InternalError`): the request may or may not have been carried out.
  case Transient
  /// The connection could not be opened: nothing was sent.
  case ConnectFailed
  /// The connection failed after it was opened: the request may have been
  /// carried out.
  case NoResponse
  /// No answer within the deadline: the request may have been carried out.
  case TimedOut
  /// The service rejected the request's timestamp.
  case ClockSkew
  /// Anything else: repeating the request would give the same answer.
  case NotRetryable
}

func isOneOf(code: in String, codes: in slice[String]): Bool {
  for c in codes {
    if c == code {
      return true
    }
  }
  false
}

/// True for the error codes with which AWS services report throttling.
@pure
pub func isThrottlingCode(code: in String): Bool {
  isOneOf(
    code,
    [
      "Throttling",
      "ThrottlingException",
      "ThrottledException",
      "RequestThrottledException",
      "RequestThrottled",
      "TooManyRequestsException",
      "ProvisionedThroughputExceededException",
      "TransactionInProgressException",
      "RequestLimitExceeded",
      "BandwidthLimitExceeded",
      "LimitExceededException",
      "SlowDown",
      "PriorRequestNotComplete",
      "EC2ThrottledException",
      "ServiceUnavailable"
    ]
  )
}

/// True for the error codes of a failure inside the service that says
/// nothing about whether the request was carried out.
@pure
pub func isTransientCode(code: in String): Bool {
  isOneOf(
    code,
    ["RequestTimeout", "RequestTimeoutException", "InternalError", "InternalFailure", "InternalServerError"]
  )
}

/// True for the error codes with which AWS services reject a request whose
/// signing time is too far from theirs.
@pure
pub func isClockSkewCode(code: in String): Bool {
  isOneOf(code, ["RequestTimeTooSkewed", "RequestExpired", "RequestInTheFuture"])
}

/// The kind of a failed response, from its HTTP status and its error code
/// (`""` when the body named none). The code decides when it is a known one;
/// otherwise the status does.
@pure
pub func classifyResponse(status: in Int, errorCode: in String): FailureKind {
  if isThrottlingCode(errorCode) {
    return FailureKind.Throttled
  }
  if isClockSkewCode(errorCode) {
    return FailureKind.ClockSkew
  }
  if isTransientCode(errorCode) {
    return FailureKind.Transient
  }
  if status == 429 or status == 503 {
    return FailureKind.Throttled
  }
  if status == 500 or status == 502 or status == 504 {
    return FailureKind.Transient
  }
  FailureKind.NotRetryable
}

/// True when `message`, the text of a transport failure, says that the
/// connection was never opened (refused, host not found, unreachable), so
/// that nothing can have been sent. The texts are those of the .NET and JDK
/// HTTP clients; a failure this does not recognise must be treated as
/// `NoResponse`.
@pure
pub func isConnectFailure(message: in String): Bool {
  val m = Str.toLower(message)
  Str.contains(m, "connection refused") or Str.contains(m, "connectexception") or
    Str.contains(m, "name or service not known") or
    Str.contains(m, "no such host") or
    Str.contains(m, "nodename nor servname") or
    Str.contains(m, "unknownhostexception") or
    Str.contains(m, "unresolvedaddressexception") or
    Str.contains(m, "network is unreachable") or
    Str.contains(m, "no route to host") or
    Str.contains(m, "actively refused")
}

func parseNonNegative(text: in String): Int {
  val t = Str.trim(text)
  if t.length == 0 or t.length > 9 {
    return -1
  }
  var n = 0
  var i = 0
  while i < t.length {
    val u = Str.codeUnitAt(t, i)
    if u < 48 or u > 57 {
      return -1
    }
    n = n * 10 + (u - 48)
    i = i + 1
  }
  n
}

/// The wait a response asks for, in milliseconds, or -1 when it asks for
/// none: `x-amz-retry-after` (milliseconds) when it is a number, else
/// `Retry-After` when it is a number of seconds. The HTTP-date form of
/// `Retry-After` is not read. Pass `""` for a header that is absent.
@pure
pub func retryAfterMsOf(retryAfter: in String, xAmzRetryAfter: in String): Int {
  val amz = parseNonNegative(xAmzRetryAfter)
  if amz >= 0 {
    return amz
  }
  val seconds = parseNonNegative(retryAfter)
  if seconds < 0 {
    return -1
  }
  if seconds > 2000000 {
    2000000000
  } else {
    seconds * 1000
  }
}

/// How often a request is attempted and how long to wait in between.
pub record RetryPolicy {
  /// Attempts in all, the first included; 1 never retries.
  maxAttempts: Int
  /// The upper bound of the wait before the second attempt; it doubles with
  /// every further attempt.
  baseDelayMs: Int
  /// No wait is longer than this.
  maxDelayMs: Int
  /// Waits for a number of milliseconds.
  sleep: (Int) -> Unit
  /// A number in `[0, 1)`: where in its range a wait falls.
  random: () -> Double
}

/// The standard policy: 3 attempts, waits of up to 100 ms, 200 ms, ...
/// capped at 20 seconds, the system clock and random source.
pub func standardRetry(): RetryPolicy {
  RetryPolicy(
    maxAttempts = 3,
    baseDelayMs = 100,
    maxDelayMs = 20000,
    sleep = { ms: Int -> Time.sleepMillis(ms) },
    random = { -> Random.nextDouble(Random.sharedRandom()) }
  )
}

/// A policy that never retries.
pub func noRetry(): RetryPolicy {
  standardRetry().withMaxAttempts(1)
}

/// `policy` with `maxAttempts` attempts in all.
pub func RetryPolicy.withMaxAttempts(policy: in RetryPolicy, maxAttempts: in Int): RetryPolicy
  requires: maxAttempts >= 1
{
  policy.copy(maxAttempts = maxAttempts)
}

/// `policy` with other backoff bounds.
pub func RetryPolicy.withBackoff(policy: in RetryPolicy, baseDelayMs: in Int, maxDelayMs: in Int): RetryPolicy
  requires: baseDelayMs >= 0
  requires: maxDelayMs >= baseDelayMs
{
  policy.copy(baseDelayMs = baseDelayMs, maxDelayMs = maxDelayMs)
}

/// `policy` waiting with `sleep` (milliseconds); for tests.
pub func RetryPolicy.withSleep(policy: in RetryPolicy, sleep: in (Int) -> Unit): RetryPolicy {
  policy.copy(sleep = sleep)
}

/// `policy` drawing from `random`, which returns numbers in `[0, 1)`; for
/// tests.
pub func RetryPolicy.withRandom(policy: in RetryPolicy, random: in () -> Double): RetryPolicy {
  policy.copy(random = random)
}

/// The largest wait before the attempt that follows attempt `attempt`:
/// `min(maxDelayMs, baseDelayMs * 2^(attempt-1))`.
@pure
pub func RetryPolicy.ceilingMs(policy: in RetryPolicy, attempt: in Int): Int
  requires: attempt >= 1
{
  var ceiling = policy.baseDelayMs
  var i = 1
  while i < attempt and ceiling < policy.maxDelayMs {
    ceiling = if ceiling > policy.maxDelayMs / 2 { policy.maxDelayMs } else { ceiling * 2 }
    i = i + 1
  }
  if ceiling > policy.maxDelayMs {
    policy.maxDelayMs
  } else {
    ceiling
  }
}

/// The wait before the attempt that follows attempt `attempt`: a random
/// point of `[0, ceilingMs(attempt)]`, raised to `retryAfterMs` when the
/// service asked for a longer one (-1 for none), and never above
/// `maxDelayMs`.
pub func RetryPolicy.delayMs(policy: in RetryPolicy, attempt: in Int, retryAfterMs: in Int): Int
  requires: attempt >= 1
{
  val draw = policy.random
  var r = draw()
  if not (r >= 0.0) {
    r = 0.0
  }
  if r > 1.0 {
    r = 1.0
  }
  val ceiling = policy.ceilingMs(attempt)
  var delay = (r * ceiling.toDouble()).toInt()
  if delay > ceiling {
    delay = ceiling
  }
  if retryAfterMs > delay {
    delay = retryAfterMs
  }
  if delay > policy.maxDelayMs {
    policy.maxDelayMs
  } else {
    delay
  }
}

/// What to do after a failed attempt.
pub union Decision {
  case GiveUp
  case RetryAfter(delayMs: Int)
}

/// True when a failure of `kind` may be followed by another attempt of a
/// request that is, or is not, `idempotent` (package documentation).
@pure
pub func isRetryable(kind: in FailureKind, idempotent: in Bool): Bool {
  match kind {
    case Throttled -> true
    case ConnectFailed -> true
    case Transient -> idempotent
    case NoResponse -> idempotent
    case TimedOut -> idempotent
    case ClockSkew -> false
    case NotRetryable -> false
  }
}

/// Whether attempt number `attempt` (1 for the first), which failed with
/// `kind`, is followed by another, and after how long. `retryAfterMs` is
/// what the response asked for (`retryAfterMsOf`), -1 for nothing.
pub func RetryPolicy.decide(
  policy: in RetryPolicy,
  attempt: in Int,
  kind: in FailureKind,
  idempotent: in Bool,
  retryAfterMs: in Int,
): Decision
  requires: attempt >= 1
{
  if attempt >= policy.maxAttempts or not isRetryable(kind, idempotent) {
    return GiveUp
  }
  RetryAfter(delayMs = policy.delayMs(attempt, retryAfterMs))
}

/// Wait `delayMs` with the policy's `sleep`.
pub func RetryPolicy.pause(policy: in RetryPolicy, delayMs: in Int): Unit {
  if delayMs > 0 {
    val sleep = policy.sleep
    sleep(delayMs)
  }
}
