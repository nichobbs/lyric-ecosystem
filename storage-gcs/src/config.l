//! Storage.Gcs.Config - where a bucket is and which HMAC key signs for it.
//!
//! A `GcsConfig` is built from explicit values (`gcsConfig`,
//! `endpointConfig`) or from the environment (`configFromEnv`), and validated
//! by `Storage.Gcs.Client.openClient` before any request is made. Google
//! defines no environment variables for HMAC keys; the names read here are
//! this package's own: `GCS_HMAC_ACCESS_ID`, `GCS_HMAC_SECRET`, and the
//! optional `GCS_LOCATION` and `GCS_ENDPOINT_URL`.
@runtime_checked
@io
package Storage.Gcs.Config

import Std.Core
import Std.Environment as Env
import Std.String as Str
import Storage

// Functions, not `pub val`s: a `pub val` of a package linked as a built
// dependency does not resolve in the dependent package (T0020).
val DEFAULT_REQUEST_TIMEOUT_MS: Int = 30000

val DEFAULT_LOCATION: String = "auto"

val GOOGLE_AUTHORITY: String = "storage.googleapis.com"

/// The default per-request deadline, 30 seconds.
@pure
pub func defaultRequestTimeoutMs(): Int {
  DEFAULT_REQUEST_TIMEOUT_MS
}

/// `auto`: the location a request is signed for unless the configuration
/// names one. Cloud Storage accepts any location in a credential scope.
@pure
pub func defaultLocation(): String {
  DEFAULT_LOCATION
}

/// `storage.googleapis.com`: the XML API's host.
@pure
pub func googleAuthority(): String {
  GOOGLE_AUTHORITY
}

/// Where a bucket is and how to sign for it.
///
/// `accessId` and `secret` are a Cloud Storage HMAC key. `location` is the
/// location named in the credential scope (`auto`, `us-central1`); Cloud
/// Storage accepts any value. `endpoint` is empty for Google Cloud Storage
/// itself, or the base URL of another server that speaks the XML API
/// (`http://127.0.0.1:19900`). It must be `https://` unless the host is
/// loopback: the signature does not hide the access ID, the object or its
/// metadata. `pathStyle` puts the bucket in the path
/// (`https://storage.googleapis.com/<bucket>/<object>`) instead of the host
/// name (`https://<bucket>.storage.googleapis.com/<object>`). `anonymous`
/// sends requests unsigned, for reading a public bucket; the HMAC key is
/// then unused.
pub record GcsConfig {
  bucket: String
  accessId: String
  secret: String
  location: String
  endpoint: String
  pathStyle: Bool
  requestTimeoutMs: Int
  anonymous: Bool
}

/// Configuration for a bucket on Google Cloud Storage, path-style, signed
/// for the location `auto`.
pub func gcsConfig(bucket: in String, accessId: in String, secret: in String): GcsConfig {
  GcsConfig(
    bucket = bucket,
    accessId = accessId,
    secret = secret,
    location = DEFAULT_LOCATION,
    endpoint = "",
    pathStyle = true,
    requestTimeoutMs = DEFAULT_REQUEST_TIMEOUT_MS,
    anonymous = false
  )
}

/// Configuration for a bucket on another server that speaks the XML API, at
/// `endpoint`, path-style.
pub func endpointConfig(endpoint: in String, bucket: in String, accessId: in String, secret: in String): GcsConfig {
  GcsConfig(
    bucket = bucket,
    accessId = accessId,
    secret = secret,
    location = DEFAULT_LOCATION,
    endpoint = endpoint,
    pathStyle = true,
    requestTimeoutMs = DEFAULT_REQUEST_TIMEOUT_MS,
    anonymous = false
  )
}

/// Configuration for reading a public bucket on Google Cloud Storage without
/// an HMAC key: requests are sent unsigned, and Cloud Storage serves what
/// `allUsers` may read. Nothing can be signed, so there are no signed URLs.
pub func anonymousConfig(bucket: in String): GcsConfig {
  GcsConfig(
    bucket = bucket,
    accessId = "",
    secret = "",
    location = DEFAULT_LOCATION,
    endpoint = "",
    pathStyle = true,
    requestTimeoutMs = DEFAULT_REQUEST_TIMEOUT_MS,
    anonymous = true
  )
}

/// `cfg` signing for `location` (the bucket's, such as `europe-west1`).
pub func GcsConfig.withLocation(cfg: in GcsConfig, location: in String): GcsConfig {
  GcsConfig(
    bucket = cfg.bucket,
    accessId = cfg.accessId,
    secret = cfg.secret,
    location = location,
    endpoint = cfg.endpoint,
    pathStyle = cfg.pathStyle,
    requestTimeoutMs = cfg.requestTimeoutMs,
    anonymous = cfg.anonymous
  )
}

/// `cfg` talking to another server that speaks the XML API, at `endpoint`
/// (`""` for Google Cloud Storage itself).
pub func GcsConfig.withEndpoint(cfg: in GcsConfig, endpoint: in String): GcsConfig {
  GcsConfig(
    bucket = cfg.bucket,
    accessId = cfg.accessId,
    secret = cfg.secret,
    location = cfg.location,
    endpoint = endpoint,
    pathStyle = cfg.pathStyle,
    requestTimeoutMs = cfg.requestTimeoutMs,
    anonymous = cfg.anonymous
  )
}

/// `cfg` addressing the bucket in the path (`true`) or the host name.
pub func GcsConfig.withPathStyle(cfg: in GcsConfig, pathStyle: in Bool): GcsConfig {
  GcsConfig(
    bucket = cfg.bucket,
    accessId = cfg.accessId,
    secret = cfg.secret,
    location = cfg.location,
    endpoint = cfg.endpoint,
    pathStyle = pathStyle,
    requestTimeoutMs = cfg.requestTimeoutMs,
    anonymous = cfg.anonymous
  )
}

/// `cfg` with a per-request deadline in milliseconds.
pub func GcsConfig.withRequestTimeout(cfg: in GcsConfig, requestTimeoutMs: in Int): GcsConfig {
  GcsConfig(
    bucket = cfg.bucket,
    accessId = cfg.accessId,
    secret = cfg.secret,
    location = cfg.location,
    endpoint = cfg.endpoint,
    pathStyle = cfg.pathStyle,
    requestTimeoutMs = requestTimeoutMs,
    anonymous = cfg.anonymous
  )
}

func invalidConfig(message: in String): StorageError {
  StorageError(message = message, code = "INVALID_CONFIG")
}

/// Configuration for `bucket` from named settings, looked up with `lookup`
/// (which answers `""` for a setting that is not there): `GCS_HMAC_ACCESS_ID`
/// and `GCS_HMAC_SECRET` are required; `GCS_LOCATION` defaults to `auto`;
/// `GCS_ENDPOINT_URL` selects another server. `Err(INVALID_CONFIG)` names the
/// first required setting that is missing. `configFromEnv` is this with the
/// process environment.
pub func configFromLookup(bucket: in String, lookup: in (String) -> String): Result[GcsConfig, StorageError] {
  val accessId = lookup("GCS_HMAC_ACCESS_ID")
  if accessId.length == 0 {
    return Err(invalidConfig("GCS_HMAC_ACCESS_ID is not set"))
  }
  val secret = lookup("GCS_HMAC_SECRET")
  if secret.length == 0 {
    return Err(invalidConfig("GCS_HMAC_SECRET is not set"))
  }
  val location = lookup("GCS_LOCATION")
  Ok(
    GcsConfig(
      bucket = bucket,
      accessId = accessId,
      secret = secret,
      location = if location.length > 0 { location } else { DEFAULT_LOCATION },
      endpoint = lookup("GCS_ENDPOINT_URL"),
      pathStyle = true,
      requestTimeoutMs = DEFAULT_REQUEST_TIMEOUT_MS,
      anonymous = false
    )
  )
}

/// Configuration for `bucket` from the environment: `GCS_HMAC_ACCESS_ID`,
/// `GCS_HMAC_SECRET`, optional `GCS_LOCATION` and `GCS_ENDPOINT_URL`
/// (`configFromLookup`).
pub func configFromEnv(bucket: in String): Result[GcsConfig, StorageError] {
  configFromLookup(bucket, { name -> Env.getVarOrDefault(name, "") })
}

func isLowerOrDigit(c: in Char): Bool {
  (c >= 'a' and c <= 'z') or (c >= '0' and c <= '9')
}

/// True for a bucket name Cloud Storage accepts: lowercase letters, digits,
/// `-`, `_` and `.`, starting and ending with a letter or digit, 3 to 63
/// characters, or up to 222 when it contains dots, each dot-separated part
/// being at most 63. Checked before the name is built into a URL.
@pure
pub func isValidBucketName(name: in String): Bool {
  if name.length < 3 or name.length > 222 {
    return false
  }
  var partLength = 0
  var dots = 0
  var i = 0
  while i < name.length {
    val c = Str.charAtOrReplacement(name, i)
    if c == '.' {
      if partLength == 0 {
        return false
      }
      partLength = 0
      dots = dots + 1
    } else if isLowerOrDigit(c) or c == '-' or c == '_' {
      partLength = partLength + 1
      if partLength > 63 {
        return false
      }
    } else {
      return false
    }
    i = i + 1
  }
  if dots == 0 and name.length > 63 {
    return false
  }
  isLowerOrDigit(Str.charAtOrReplacement(name, 0)) and isLowerOrDigit(Str.charAtOrReplacement(name, name.length - 1))
}

/// True for a location that can stand in a credential scope: lowercase
/// letters, digits and `-` (`auto`, `us`, `europe-west1`, `nam4`).
@pure
pub func isValidLocation(location: in String): Bool {
  if location.length == 0 {
    return false
  }
  var i = 0
  while i < location.length {
    val c = Str.charAtOrReplacement(location, i)
    if not (isLowerOrDigit(c) or c == '-') {
      return false
    }
    i = i + 1
  }
  true
}

/// True when the `host[:port]` authority names the loopback interface.
@pure
pub func isLoopbackAuthority(authority: in String): Bool {
  var host = authority
  if Str.startsWith(host, "[") {
    val close = Str.lastIndexOfRaw(host, "]")
    host = if close > 0 { host.substring(0, close + 1) } else { host }
  } else {
    val colon = Str.lastIndexOfRaw(host, ":")
    host = if colon >= 0 { host.substring(0, colon) } else { host }
  }
  host == "localhost" or host == "127.0.0.1" or host == "[::1]"
}

func isAuthorityChar(c: in Char): Bool {
  isLowerOrDigit(c) or (c >= 'A' and c <= 'Z') or c == '-' or c == '.' or c == ':' or c == '[' or c == ']'
}

// An endpoint as it may be quoted in an error message: one that embeds
// credentials (`user:password@host`) is not repeated.
func shown(raw: in String): String {
  if Str.contains(raw, "@") {
    "(an endpoint with embedded credentials, not shown)"
  } else {
    raw
  }
}

/// A validated endpoint, split: `scheme` is `http` or `https`, `authority`
/// is `host[:port]` in lowercase with a default port dropped.
pub record Endpoint {
  scheme: String
  authority: String
}

/// Split and validate an endpoint: `http(s)://host[:port]` with nothing after
/// the authority but an optional `/`. The default port is dropped so the
/// signed `host` is the `Host` header an HTTP client sends.
/// `Err(INVALID_CONFIG)` for anything else, and for `http://` to a host that
/// is not loopback.
pub func parseEndpoint(raw: in String): Result[Endpoint, StorageError] {
  var scheme = ""
  var rest = ""
  if Str.startsWith(raw, "https://") {
    scheme = "https"
    rest = raw.substring(8)
  } else if Str.startsWith(raw, "http://") {
    scheme = "http"
    rest = raw.substring(7)
  } else {
    return Err(invalidConfig("GCS endpoint must be an absolute http(s) URL: " + shown(raw)))
  }
  if Str.endsWith(rest, "/") {
    rest = rest.substring(0, rest.length - 1)
  }
  if rest.length == 0 {
    return Err(invalidConfig("GCS endpoint has no host: " + shown(raw)))
  }
  var i = 0
  while i < rest.length {
    if not isAuthorityChar(Str.charAtOrReplacement(rest, i)) {
      return Err(
        invalidConfig("GCS endpoint must be scheme://host[:port] with no path, query or credentials: " + shown(raw))
      )
    }
    i = i + 1
  }
  var authority = Str.toLower(rest)
  if scheme == "https" and Str.endsWith(authority, ":443") {
    authority = authority.substring(0, authority.length - 4)
  } else if scheme == "http" and Str.endsWith(authority, ":80") {
    authority = authority.substring(0, authority.length - 3)
  }
  if scheme == "http" and not isLoopbackAuthority(authority) {
    return Err(
      invalidConfig(
        "GCS endpoint must use https:// (http:// is allowed only for localhost, 127.0.0.1 and [::1]): " + raw
      )
    )
  }
  Ok(Endpoint(scheme = scheme, authority = authority))
}
