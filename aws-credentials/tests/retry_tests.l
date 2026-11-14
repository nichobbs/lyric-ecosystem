//! tests/retry_tests.l - the retry policy: classification, which failures
//! are retried for which requests, and the waits, with an injected sleep and
//! random source.
@test_module
package Aws.Retry.Tests

import Std.Core
import Std.Collections
import Std.String as Str
import Std.Testing
import Aws.Retry

func kindName(kind: in FailureKind): String {
  match kind {
    case Throttled -> "Throttled"
    case Transient -> "Transient"
    case ConnectFailed -> "ConnectFailed"
    case NoResponse -> "NoResponse"
    case TimedOut -> "TimedOut"
    case ClockSkew -> "ClockSkew"
    case NotRetryable -> "NotRetryable"
  }
}

func classified(status: in Int, code: in String): String {
  kindName(classifyResponse(status, code))
}

test "responses are classified by error code first and status second" {
  assertEqual(classified(400, "Throttling"), "Throttled", "Throttling")
  assertEqual(classified(400, "ThrottlingException"), "Throttled", "ThrottlingException")
  assertEqual(classified(403, "RequestThrottled"), "Throttled", "RequestThrottled")
  assertEqual(classified(503, "SlowDown"), "Throttled", "SlowDown")
  assertEqual(classified(503, "RequestLimitExceeded"), "Throttled", "RequestLimitExceeded")
  assertEqual(classified(400, "TooManyRequestsException"), "Throttled", "TooManyRequestsException")
  assertEqual(classified(400, "RequestTimeout"), "Transient", "RequestTimeout: the service gave up reading")
  assertEqual(classified(500, "InternalError"), "Transient", "InternalError")
  assertEqual(classified(403, "RequestTimeTooSkewed"), "ClockSkew", "RequestTimeTooSkewed")
  assertEqual(classified(400, "RequestExpired"), "ClockSkew", "RequestExpired")
  assertEqual(classified(403, "RequestInTheFuture"), "ClockSkew", "RequestInTheFuture")
  assertEqual(classified(429, ""), "Throttled", "429 without a code")
  assertEqual(classified(503, ""), "Throttled", "503 without a code")
  assertEqual(classified(500, ""), "Transient", "500")
  assertEqual(classified(502, ""), "Transient", "502")
  assertEqual(classified(504, ""), "Transient", "504")
  assertEqual(classified(500, "SomethingElse"), "Transient", "500 with an unknown code")
  assertEqual(classified(403, "AccessDenied"), "NotRetryable", "AccessDenied")
  assertEqual(classified(404, "NoSuchKey"), "NotRetryable", "NoSuchKey")
  assertEqual(classified(403, "SignatureDoesNotMatch"), "NotRetryable", "a wrong signature is not clock skew")
  assertEqual(classified(400, ""), "NotRetryable", "400")
  assertEqual(classified(301, ""), "NotRetryable", "a redirect")
}

test "a transport failure that names a refused or unresolvable connection was never sent" {
  assertTrue(isConnectFailure("Connection refused (127.0.0.1:1)"), ".NET")
  assertTrue(isConnectFailure("java.net.ConnectException"), "JDK, no message")
  assertTrue(isConnectFailure("Name or service not known (nowhere.invalid:443)"), ".NET DNS")
  assertTrue(isConnectFailure("java.nio.channels.UnresolvedAddressException"), "JDK DNS")
  assertFalse(isConnectFailure("Connection reset by peer"), "a reset may follow the request")
  assertFalse(isConnectFailure("The response ended prematurely"), "a cut answer")
  assertFalse(isConnectFailure("request timed out"), "a timeout")
  assertFalse(isConnectFailure(""), "nothing known")
}

test "Retry-After is seconds, x-amz-retry-after is milliseconds and wins" {
  assertEqualInt(retryAfterMsOf("", ""), -1, "neither")
  assertEqualInt(retryAfterMsOf("2", ""), 2000, "Retry-After")
  assertEqualInt(retryAfterMsOf(" 0 ", ""), 0, "zero")
  assertEqualInt(retryAfterMsOf("", "750"), 750, "x-amz-retry-after")
  assertEqualInt(retryAfterMsOf("2", "750"), 750, "both")
  assertEqualInt(retryAfterMsOf("Fri, 13 Nov 2026 10:15:00 GMT", ""), -1, "the date form is not read")
  assertEqualInt(retryAfterMsOf("-1", "soon"), -1, "junk")
  assertEqualInt(retryAfterMsOf("999999999", ""), 2000000000, "a huge value does not overflow")
}

// A policy whose sleeps are recorded instead of slept, and whose random
// source is a fixed fraction.
func recording(sleeps: in List[Int], fraction: in Double): RetryPolicy {
  standardRetry().withSleep({ ms: Int -> sleeps.add(ms) }).withRandom({ -> fraction })
}

func decision(d: in Decision): String {
  match d {
    case GiveUp -> "give up"
    case RetryAfter(ms) -> "retry after " + ms.toString()
  }
}

test "the standard policy: three attempts, full jitter, doubling, capped" {
  val sleeps: List[Int] = newList()
  val top = recording(sleeps, 1.0)
  assertEqualInt(top.maxAttempts, 3, "three attempts")
  assertEqualInt(top.ceilingMs(1), 100, "first wait: up to 100 ms")
  assertEqualInt(top.ceilingMs(2), 200, "second: up to 200 ms")
  assertEqualInt(top.ceilingMs(5), 1600, "fifth: up to 1.6 s")
  assertEqualInt(top.ceilingMs(9), 20000, "capped at 20 s")
  assertEqualInt(top.ceilingMs(1000), 20000, "and no overflow far out")
  assertEqualInt(top.delayMs(2, -1), 200, "random = 1: the ceiling")
  assertEqualInt(recording(sleeps, 0.0).delayMs(2, -1), 0, "random = 0: no wait")
  assertEqualInt(recording(sleeps, 0.5).delayMs(2, -1), 100, "random = 0.5: half the ceiling")
  assertEqualInt(recording(sleeps, 0.25).delayMs(3, -1), 100, "a quarter of 400")
  assertEqualInt(recording(sleeps, 7.0).delayMs(1, -1), 100, "a random source out of range is clamped")
  assertEqualInt(recording(sleeps, -3.0).delayMs(1, -1), 0, "also below")
}

test "the service's Retry-After raises the wait, up to the cap" {
  val sleeps: List[Int] = newList()
  val p = recording(sleeps, 0.5)
  assertEqualInt(p.delayMs(1, 1500), 1500, "longer than the jittered wait")
  assertEqualInt(p.delayMs(1, 10), 50, "shorter than the jittered wait: ignored")
  assertEqualInt(p.delayMs(1, 3600000), 20000, "an hour is cut to maxDelayMs")
}

test "decide: what is retried depends on the failure and on idempotency, up to maxAttempts" {
  val sleeps: List[Int] = newList()
  val p = recording(sleeps, 1.0)
  assertEqual(decision(p.decide(1, FailureKind.Throttled, false, -1)), "retry after 100", "throttled, not idempotent")
  assertEqual(decision(p.decide(2, FailureKind.Throttled, false, -1)), "retry after 200", "second attempt")
  assertEqual(decision(p.decide(3, FailureKind.Throttled, true, -1)), "give up", "the third attempt was the last")
  assertEqual(decision(p.decide(1, FailureKind.ConnectFailed, false, -1)), "retry after 100", "never sent")
  assertEqual(decision(p.decide(1, FailureKind.Transient, true, -1)), "retry after 100", "5xx, idempotent")
  assertEqual(decision(p.decide(1, FailureKind.Transient, false, -1)), "give up", "5xx, not idempotent")
  assertEqual(decision(p.decide(1, FailureKind.NoResponse, true, -1)), "retry after 100", "cut, idempotent")
  assertEqual(decision(p.decide(1, FailureKind.NoResponse, false, -1)), "give up", "cut, not idempotent")
  assertEqual(decision(p.decide(1, FailureKind.TimedOut, true, -1)), "retry after 100", "timeout, idempotent")
  assertEqual(decision(p.decide(1, FailureKind.TimedOut, false, -1)), "give up", "timeout, not idempotent")
  assertEqual(decision(p.decide(1, FailureKind.ClockSkew, true, -1)), "give up", "clock skew")
  assertEqual(decision(p.decide(1, FailureKind.NotRetryable, true, -1)), "give up", "not retryable")
  assertEqual(decision(p.decide(1, FailureKind.Throttled, true, 5000)), "retry after 5000", "Retry-After")
  assertEqual(decision(noRetry().decide(1, FailureKind.Throttled, true, -1)), "give up", "noRetry never retries")
  assertEqual(
    decision(p.withMaxAttempts(5).withBackoff(10, 25).decide(4, FailureKind.Throttled, true, -1)),
    "retry after 25",
    "withMaxAttempts and withBackoff"
  )
}

test "pause uses the injected sleep, and skips a wait of zero" {
  val sleeps: List[Int] = newList()
  val p = recording(sleeps, 1.0)
  p.pause(120)
  p.pause(0)
  p.pause(7)
  assertEqualInt(sleeps.count, 2, "two sleeps")
  assertEqualInt(sleeps[0], 120, "the first")
  assertEqualInt(sleeps[1], 7, "the second")
}

test "the default random source stays inside the ceiling" {
  val p = standardRetry()
  var i = 0
  var highest = 0
  while i < 200 {
    val d = p.delayMs(3, -1)
    assertTrue(d >= 0 and d <= 400, "a wait of " + d.toString() + " ms is within [0, 400]")
    if d > highest {
      highest = d
    }
    i = i + 1
  }
  assertTrue(highest > 0, "the waits are not all zero")
}
