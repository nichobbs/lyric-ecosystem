//! tests/config_tests.l - configuration: explicit values, named settings (the
//! environment), validation, and how a bucket is addressed.
@test_module
package Storage.Gcs.ConfigTests

import Std.Core
import Std.Collections
import Std.Testing
import Storage
import Storage.Gcs.Client

val GOOG_DATE: String = "20261112T101500Z"

func lookupIn(settings: in Map[String, String]): (String) -> String {
  { name -> match mapGet(settings, name) { case Some(v) -> v; case None -> "" } }
}

func openCode(cfg: in GcsConfig): String {
  match openClient(cfg) {
    case Ok(_) -> "ok"
    case Err(e) -> e.code + ": " + e.message
  }
}

func opened(cfg: in GcsConfig): GcsClient {
  match openClient(cfg) {
    case Ok(c) -> c
    case Err(e) -> panic("openClient failed: " + e.code + " " + e.message)
  }
}

test "gcsConfig is Google Cloud Storage, path-style, signed for 'auto', 30 s per request" {
  val cfg = gcsConfig("my-bucket", "GOOG1EXAMPLE", "secret")
  assertEqual(cfg.bucket, "my-bucket", "bucket")
  assertEqual(cfg.accessId, "GOOG1EXAMPLE", "access ID")
  assertEqual(cfg.secret, "secret", "secret")
  assertEqual(cfg.location, "auto", "location")
  assertEqual(cfg.location, defaultLocation(), "the default location")
  assertEqual(cfg.endpoint, "", "no endpoint override")
  assertTrue(cfg.pathStyle, "path-style")
  assertEqualInt(cfg.requestTimeoutMs, 30000, "timeout")
  assertEqualInt(defaultRequestTimeoutMs(), 30000, "the default timeout")
  assertEqual(googleAuthority(), "storage.googleapis.com", "the XML API host")
}

test "anonymousConfig needs no HMAC key, and withEndpoint points any configuration elsewhere" {
  val cfg = anonymousConfig("gcp-public-data-landsat")
  assertTrue(cfg.anonymous, "anonymous")
  assertEqual(cfg.accessId + cfg.secret, "", "no key")
  assertEqual(openCode(cfg), "ok", "it opens without a key")
  assertFalse(gcsConfig("data", "id", "secret").anonymous, "a configuration with a key signs")
  val elsewhere = cfg.withEndpoint("http://127.0.0.1:19900")
  assertEqual(elsewhere.endpoint, "http://127.0.0.1:19900", "the endpoint")
  assertTrue(elsewhere.anonymous, "still anonymous")
  assertEqual(opened(elsewhere).origin, "http://127.0.0.1:19900", "and the client goes there")
  assertEqual(endpointConfig("http://127.0.0.1:19900", "data", "id", "secret").withEndpoint("").endpoint, "", "back")
}

test "the with* functions change one field each" {
  val cfg = endpointConfig("http://127.0.0.1:19900", "data", "id", "secret").withLocation("europe-west1").withPathStyle(false).withRequestTimeout(
    5000
  )
  assertEqual(cfg.endpoint, "http://127.0.0.1:19900", "endpoint")
  assertEqual(cfg.bucket, "data", "bucket")
  assertEqual(cfg.accessId, "id", "access ID")
  assertEqual(cfg.secret, "secret", "secret")
  assertEqual(cfg.location, "europe-west1", "location")
  assertFalse(cfg.pathStyle, "virtual-hosted")
  assertEqualInt(cfg.requestTimeoutMs, 5000, "timeout")
}

test "named settings: the HMAC key is required, location and endpoint are optional" {
  val settings: Map[String, String] = newMap()
  match configFromLookup("data", lookupIn(settings)) {
    case Ok(_) -> panic("nothing is set")
    case Err(e) -> {
      assertEqual(e.code, "INVALID_CONFIG", "code")
      assertEqual(e.message, "GCS_HMAC_ACCESS_ID is not set", "the first missing setting is named")
    }
  }
  settings["GCS_HMAC_ACCESS_ID"] = "GOOG1EXAMPLE"
  match configFromLookup("data", lookupIn(settings)) {
    case Ok(_) -> panic("the secret is not set")
    case Err(e) -> assertEqual(e.message, "GCS_HMAC_SECRET is not set", "then the secret")
  }
  settings["GCS_HMAC_SECRET"] = "s3cr3t/value+with=symbols"
  match configFromLookup("data", lookupIn(settings)) {
    case Err(e) -> panic(e.message)
    case Ok(cfg) -> {
      assertEqual(cfg.bucket, "data", "bucket")
      assertEqual(cfg.accessId, "GOOG1EXAMPLE", "access ID")
      assertEqual(cfg.secret, "s3cr3t/value+with=symbols", "the secret, verbatim")
      assertEqual(cfg.location, "auto", "location defaults to auto")
      assertEqual(cfg.endpoint, "", "Google Cloud Storage")
      assertTrue(cfg.pathStyle, "path-style")
    }
  }
  settings["GCS_LOCATION"] = "us-central1"
  settings["GCS_ENDPOINT_URL"] = "http://localhost:19900"
  match configFromLookup("data", lookupIn(settings)) {
    case Err(e) -> panic(e.message)
    case Ok(cfg) -> {
      assertEqual(cfg.location, "us-central1", "GCS_LOCATION")
      assertEqual(cfg.endpoint, "http://localhost:19900", "GCS_ENDPOINT_URL")
      assertEqual(openCode(cfg), "ok", "and it opens")
    }
  }
}

test "the environment is read through the same settings" {
  // No assertion on the outcome: it depends on the environment the tests run
  // in. Whatever it holds, the answer is a configuration or INVALID_CONFIG.
  match configFromEnv("data") {
    case Ok(cfg) -> assertEqual(cfg.bucket, "data", "bucket")
    case Err(e) -> assertEqual(e.code, "INVALID_CONFIG", "a missing variable")
  }
}

test "addressing: path-style by default, virtual-hosted on request, on Google or another endpoint" {
  val google = opened(gcsConfig("my-bucket", "GOOG1EXAMPLE", "secret"))
  assertEqual(google.origin, "https://storage.googleapis.com", "origin")
  assertEqual(google.host, "storage.googleapis.com", "the signed host")
  assertEqual(google.objectPath("a/b c.txt"), "/my-bucket/a/b%20c.txt", "the bucket is in the path")
  assertEqual(google.bucketPath(), "/my-bucket", "the bucket's own path")
  val url = google.presign("GET", "a/b c.txt", 60, GOOG_DATE)
  assertTrue(
    url.startsWith("https://storage.googleapis.com/my-bucket/a/b%20c.txt?X-Goog-Algorithm=GOOG4-HMAC-SHA256&"),
    "a path-style signed URL: " + url
  )
  assertTrue(
    url.contains("&X-Goog-Credential=GOOG1EXAMPLE%2F20261112%2Fauto%2Fstorage%2Fgoog4_request&"),
    "the credential scope: " + url
  )
  assertTrue(url.contains("&X-Goog-Date=20261112T101500Z&X-Goog-Expires=60&X-Goog-SignedHeaders=host&"), url)
  val hosted = opened(gcsConfig("my-bucket", "GOOG1EXAMPLE", "secret").withPathStyle(false).withLocation("us"))
  assertEqual(hosted.host, "my-bucket.storage.googleapis.com", "the bucket is in the host")
  assertEqual(hosted.objectPath("k"), "/k", "and not in the path")
  assertEqual(hosted.bucketPath(), "/", "the bucket's own path")
  val hostedUrl = hosted.presign("PUT", "k", 604800, GOOG_DATE)
  assertTrue(hostedUrl.startsWith("https://my-bucket.storage.googleapis.com/k?"), "virtual-hosted: " + hostedUrl)
  assertTrue(hostedUrl.contains("%2F20261112%2Fus%2Fstorage%2Fgoog4_request&"), "the configured location")
  val other = opened(endpointConfig("https://gcs.example.com:8443/", "data", "id", "secret"))
  assertEqual(other.origin, "https://gcs.example.com:8443", "the trailing slash is dropped")
  assertEqual(other.objectPath("k"), "/data/k", "path-style on another endpoint")
  val defaultPort = opened(endpointConfig("https://GCS.Example.com:443", "data", "id", "secret"))
  assertEqual(defaultPort.host, "gcs.example.com", "lowercase, default port dropped: the Host header a client sends")
}

test "configuration is validated before any request" {
  assertEqual(openCode(endpointConfig("http://127.0.0.1:19900", "data", "id", "secret")), "ok", "http on loopback")
  assertEqual(openCode(endpointConfig("http://localhost:19900", "data", "id", "secret")), "ok", "localhost")
  assertEqual(openCode(endpointConfig("http://[::1]:19900", "data", "id", "secret")), "ok", "IPv6 loopback")
  assertTrue(
    openCode(endpointConfig("http://gcs.internal:9000", "data", "id", "secret")).startsWith(
      "INVALID_CONFIG: GCS endpoint must use https://"
    ),
    "plain http to another host would expose the request"
  )
  assertTrue(
    openCode(endpointConfig("http://127.0.0.1.evil.example", "data", "id", "secret")).startsWith(
      "INVALID_CONFIG: GCS endpoint must use https://"
    ),
    "a host that merely starts with a loopback address is not loopback"
  )
  assertTrue(openCode(endpointConfig("ftp://example.com", "data", "id", "secret")).startsWith("INVALID_CONFIG"), "ftp")
  assertTrue(
    openCode(endpointConfig("https://example.com/prefix", "data", "id", "secret")).startsWith("INVALID_CONFIG"),
    "an endpoint with a path"
  )
  assertTrue(openCode(endpointConfig("https://", "data", "id", "secret")).startsWith("INVALID_CONFIG"), "no host")
  val withCreds = openCode(endpointConfig("https://user:hunter2@example.com", "data", "id", "secret"))
  assertTrue(withCreds.startsWith("INVALID_CONFIG"), "an endpoint with credentials")
  assertFalse(withCreds.contains("hunter2"), "whose password is not repeated in the message: " + withCreds)
  assertTrue(openCode(gcsConfig("My_Bucket", "id", "secret")).startsWith("INVALID_CONFIG"), "bucket name case")
  assertTrue(openCode(gcsConfig("ab", "id", "secret")).startsWith("INVALID_CONFIG"), "bucket name too short")
  assertTrue(openCode(gcsConfig("a/b/c", "id", "secret")).startsWith("INVALID_CONFIG"), "bucket name with a slash")
  assertTrue(openCode(gcsConfig("data", "", "secret")).startsWith("INVALID_CONFIG"), "empty access ID")
  assertTrue(openCode(gcsConfig("data", "id", "")).startsWith("INVALID_CONFIG"), "empty secret")
  assertFalse(
    openCode(gcsConfig("da ta", "id", "the-secret-value")).contains("the-secret-value"),
    "the secret is never part of a message"
  )
  assertTrue(
    openCode(gcsConfig("data", "id", "secret").withLocation("us/storage/goog4_request")).startsWith("INVALID_CONFIG"),
    "a location that would alter the credential scope"
  )
  assertTrue(openCode(gcsConfig("data", "id", "secret").withLocation("")).startsWith("INVALID_CONFIG"), "no location")
  assertTrue(
    openCode(gcsConfig("data", "id", "secret").withRequestTimeout(0)).startsWith("INVALID_CONFIG"),
    "a zero timeout"
  )
}

test "bucket names follow Cloud Storage's rules" {
  assertTrue(isValidBucketName("my-bucket"), "plain")
  assertTrue(isValidBucketName("my_bucket.2026"), "underscores and dots")
  assertTrue(isValidBucketName("gcp-public-data-landsat"), "a real public bucket")
  assertTrue(isValidBucketName("abc"), "three characters")
  assertTrue(isValidBucketName("www.example.com"), "a domain-named bucket")
  assertFalse(isValidBucketName("ab"), "too short")
  assertFalse(isValidBucketName("Bucket"), "uppercase")
  assertFalse(isValidBucketName("-bucket"), "starts with a dash")
  assertFalse(isValidBucketName("bucket."), "ends with a dot")
  assertFalse(isValidBucketName("a..b"), "an empty dot-separated part")
  assertFalse(isValidBucketName("a b"), "a space")
  assertFalse(isValidBucketName("bucket/key"), "a slash")
  assertFalse(isValidBucketName("bucket?x=1"), "a query")
  val sixtyThree = "a23456789012345678901234567890123456789012345678901234567890123"
  assertTrue(isValidBucketName(sixtyThree), "63 characters")
  assertFalse(isValidBucketName(sixtyThree + "4"), "64 characters without a dot")
  assertTrue(isValidBucketName(sixtyThree + "." + sixtyThree), "longer with dots, each part at most 63")
  assertFalse(isValidBucketName(sixtyThree + "4." + sixtyThree), "a part of 64")
}

test "locations and loopback authorities" {
  assertTrue(isValidLocation("auto"), "auto")
  assertTrue(isValidLocation("europe-west1"), "a region")
  assertTrue(isValidLocation("nam4"), "a dual-region")
  assertFalse(isValidLocation("US"), "uppercase")
  assertFalse(isValidLocation("us east"), "a space")
  assertTrue(isLoopbackAuthority("127.0.0.1:19900"), "IPv4 with a port")
  assertTrue(isLoopbackAuthority("localhost"), "localhost")
  assertTrue(isLoopbackAuthority("[::1]:8080"), "IPv6 with a port")
  assertFalse(isLoopbackAuthority("127.0.0.1.example.com"), "a look-alike")
  assertFalse(isLoopbackAuthority("example.com:80"), "another host")
  match parseEndpoint("http://LocalHost:80/") {
    case Err(e) -> panic(e.message)
    case Ok(e) -> {
      assertEqual(e.scheme, "http", "scheme")
      assertEqual(e.authority, "localhost", "lowercase, default port dropped")
    }
  }
}

test "user metadata the XML API cannot carry unchanged is described" {
  assertEqual(userMetaProblem("owner", "alice"), "", "a plain entry")
  assertEqual(userMetaProblem("build.note_1-x", "a b, c; d=e"), "", "the allowed key characters, a printable value")
  assertEqual(userMetaProblem("owner", ""), "", "an empty value")
  assertTrue(userMetaProblem("", "v").length > 0, "an empty key")
  assertTrue(userMetaProblem("Owner", "v").length > 0, "an uppercase key")
  assertTrue(userMetaProblem("own er", "v").length > 0, "a key with a space")
  assertTrue(userMetaProblem("owner", "line\nbreak").length > 0, "a control character in a value")
  assertTrue(userMetaProblem("owner", "café").length > 0, "a non-ASCII value")
  assertTrue(userMetaProblem("owner", "trailing ").length > 0, "a trailing space")
  assertTrue(userMetaProblem("owner", " leading").length > 0, "a leading space")
}
