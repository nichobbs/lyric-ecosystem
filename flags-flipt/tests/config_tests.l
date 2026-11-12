//! tests/config_tests.l - configuration and its validation, without a server.
@test_module
package Flags.Flipt.ConfigTests

import Std.Core
import Std.Collections
import Std.Environment as Env
import Std.Testing
import Flags.Flipt

func originOf(raw: in String): String {
  match parseOrigin(raw) {
    case Ok(origin) -> origin
    case Err(message) -> "refused: " + message
  }
}

func openOutcome(cfg: in FliptConfig): String {
  match open(cfg) {
    case Ok(_) -> "ok"
    case Err(e) -> e.code + ": " + e.message
  }
}

test "the server URL: https, or http on loopback only" {
  assertEqual(originOf("https://flipt.example.com"), "https://flipt.example.com", "https")
  assertEqual(originOf("https://Flipt.Example.com:8443/"), "https://flipt.example.com:8443", "port, case, slash")
  assertEqual(originOf("http://127.0.0.1:8080"), "http://127.0.0.1:8080", "loopback IPv4")
  assertEqual(originOf("http://localhost:8080/"), "http://localhost:8080", "localhost")
  assertEqual(originOf("http://[::1]:8080"), "http://[::1]:8080", "loopback IPv6")
  assertTrue(
    originOf("http://flipt.example.com").startsWith("refused: Flipt URL must use https://"),
    "plain http off loopback"
  )
  assertTrue(
    originOf("http://127.0.0.1.example.com").startsWith("refused: Flipt URL must use https://"),
    "a look-alike host"
  )
  assertTrue(
    originOf("http://10.0.0.5:8080").startsWith("refused: Flipt URL must use https://"),
    "a private address is not loopback"
  )
  assertTrue(
    originOf("flipt.example.com").startsWith("refused: Flipt URL must be an absolute http(s) URL"),
    "no scheme"
  )
  assertTrue(originOf("ftp://flipt.example.com").startsWith("refused: Flipt URL must be an absolute"), "another scheme")
  assertTrue(originOf("https://").startsWith("refused: Flipt URL has no host"), "no host")
  assertTrue(
    originOf("https://flipt.example.com/api").startsWith("refused: Flipt URL must be scheme://host[:port] with no path"),
    "a path"
  )
  assertTrue(
    originOf("https://user:pw@flipt.example.com").startsWith("refused: Flipt URL must not carry credentials"),
    "credentials"
  )
  assertTrue(originOf("https://flipt.example.com?x=1").startsWith("refused: Flipt URL must not carry"), "a query")
  assertTrue(isLoopbackAuthority("localhost"), "localhost without a port")
  assertFalse(isLoopbackAuthority("localhost.example.com:80"), "not localhost")
}

test "defaults and builders" {
  val cfg = config("https://flipt.example.com", "production")
  assertEqual(cfg.token, "", "no token")
  assertEqualInt(cfg.requestTimeoutMs, 5000, "request timeout")
  assertEqualInt(cfg.refreshIntervalMs, 10000, "refresh interval")
  assertEqualInt(cfg.maxStaleMs, 0, "no staleness limit")
  assertEqualInt(cfg.pageSize, 100, "page size")
  assertFalse(cfg.evaluates(), "Snapshot mode")
  val ctx: Map[String, String] = newMap()
  ctx["plan"] = "pro"
  val changed = cfg.withToken("t").withNamespace("staging").withRequestTimeout(250).withRefreshInterval(1000).withMaxStale(60000).withPageSize(7).withEvaluation(
    "user-1",
    ctx
  )
  assertEqual(changed.token + "/" + changed.namespace + "/" + changed.entityId, "t/staging/user-1", "strings")
  assertEqualInt(
    changed.requestTimeoutMs + changed.refreshIntervalMs + changed.maxStaleMs + changed.pageSize,
    61257,
    "numbers"
  )
  assertTrue(changed.evaluates(), "Evaluate mode")
  assertEqual(openOutcome(changed), "ok", "a valid configuration opens without a request")
  match open(changed) {
    case Ok(store) -> {
      assertEqual(
        store.namespace + "/" + store.entityId + "/" + store.client.origin,
        "staging/user-1/https://flipt.example.com",
        "the store"
      )
      assertTrue(store.evaluating, "the store's mode")
      assertEqualInt(errorCount(store), 0, "no errors yet")
      assertTrue(isNone(lastError(store)), "no last error")
      assertTrue(isNone(snapshotAgeMs(store)), "nothing loaded")
      assertEqualInt(refreshCount(store), 0, "no refresh")
    }
    case Err(e) -> assertTrue(false, e.message)
  }
}

test "a configuration that cannot be used is INVALID_CONFIG" {
  val cfg = config("https://flipt.example.com", "production")
  assertTrue(
    openOutcome(config("http://flipt.example.com", "x")).startsWith("INVALID_CONFIG: Flipt URL must use https://"),
    "url"
  )
  assertEqual(
    openOutcome(cfg.withNamespace("not a key")),
    "INVALID_CONFIG: namespace 'not a key' is not a Flipt key (letters, digits, '-', '_' and ',')",
    "namespace"
  )
  assertTrue(openOutcome(cfg.withNamespace("")).startsWith("INVALID_CONFIG: namespace ''"), "empty namespace")
  assertEqual(
    openOutcome(cfg.withToken("two words")),
    "INVALID_CONFIG: the Flipt client token holds a character a header value cannot carry",
    "a token with a space"
  )
  assertTrue(
    openOutcome(cfg.withToken("line\nbreak")).startsWith("INVALID_CONFIG: the Flipt client token"),
    "header injection"
  )
  assertEqual(openOutcome(cfg.withRequestTimeout(0)), "INVALID_CONFIG: requestTimeoutMs must be > 0, got 0", "timeout")
  assertEqual(
    openOutcome(cfg.withRefreshInterval(-1)),
    "INVALID_CONFIG: refreshIntervalMs must be >= 0, got -1",
    "interval"
  )
  assertEqual(openOutcome(cfg.withMaxStale(-5)), "INVALID_CONFIG: maxStaleMs must be >= 0, got -5", "staleness")
  assertEqual(openOutcome(cfg.withPageSize(0)), "INVALID_CONFIG: pageSize must be 1 to 1000, got 0", "page size 0")
  assertEqual(
    openOutcome(cfg.withPageSize(1001)),
    "INVALID_CONFIG: pageSize must be 1 to 1000, got 1001",
    "page size 1001"
  )
  assertEqual(openOutcome(cfg.withRefreshInterval(0)), "ok", "interval 0 refreshes on every read")
}

test "configFromEnv reads FLIPT_URL, FLIPT_NAMESPACE and FLIPT_CLIENT_TOKEN" {
  Env.setVar("FLIPT_URL", "")
  Env.setVar("FLIPT_NAMESPACE", "")
  Env.setVar("FLIPT_CLIENT_TOKEN", "")
  match configFromEnv() {
    case Ok(_) -> assertTrue(false, "FLIPT_URL is required")
    case Err(e) -> assertEqual(e.code + ": " + e.message, "INVALID_CONFIG: FLIPT_URL is not set", "no URL")
  }
  match connectFromEnv() {
    case Ok(_) -> assertTrue(false, "FLIPT_URL is required")
    case Err(e) -> assertEqual(e.text(), "flipt: FLIPT_URL is not set", "connectFromEnv")
  }
  Env.setVar("FLIPT_URL", "http://127.0.0.1:8080")
  match configFromEnv() {
    case Ok(cfg) -> assertEqual(
      cfg.url + "|" + cfg.namespace + "|" + cfg.token,
      "http://127.0.0.1:8080|default|",
      "defaults"
    )
    case Err(e) -> assertTrue(false, e.message)
  }
  Env.setVar("FLIPT_NAMESPACE", "staging")
  Env.setVar("FLIPT_CLIENT_TOKEN", "fixture-token")
  match configFromEnv() {
    case Ok(cfg) -> assertEqual(
      cfg.url + "|" + cfg.namespace + "|" + cfg.token,
      "http://127.0.0.1:8080|staging|fixture-token",
      "everything set"
    )
    case Err(e) -> assertTrue(false, e.message)
  }
  Env.setVar("FLIPT_URL", "")
  Env.setVar("FLIPT_NAMESPACE", "")
  Env.setVar("FLIPT_CLIENT_TOKEN", "")
}

test "the errors a failed request is given" {
  val notFound = faultOf("GetFlag", 404, "{\"code\":5,\"message\":\"flag \\\"prod/x\\\" not found\",\"details\":[]}")
  assertEqual(notFound.code + "/" + notFound.status.toString(), "NOT_FOUND/404", "404")
  assertEqual(notFound.text(), "flipt: GetFlag failed: Flipt answered HTTP 404: flag \"prod/x\" not found", "message")
  assertEqual(faultOf("X", 400, "").code, "INVALID_ARGUMENT", "400")
  assertEqual(faultOf("X", 401, "").code, "UNAUTHENTICATED", "401")
  assertEqual(faultOf("X", 403, "").code, "PERMISSION_DENIED", "403")
  assertEqual(faultOf("X", 429, "").code, "HTTP_429", "429")
  assertEqual(faultOf("X", 503, "<html>").message, "X failed: Flipt answered HTTP 503", "no error document")
}
