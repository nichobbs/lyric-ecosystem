//! Storage.S3.Client - a signed S3 REST client over `Std.Http`.
//!
//! One `S3Client` talks to one bucket. Every request is signed with AWS
//! Signature Version 4 (`Aws.SigV4`, from the `Lyric.Aws.SigV4` package) and sent with `Std.Http`; there is
//! no AWS SDK underneath, so the same code runs on .NET and the JVM.
//!
//! Addressing follows the endpoint: AWS itself is virtual-hosted
//! (`https://<bucket>.s3.<region>.amazonaws.com/<key>`), a custom endpoint
//! (MinIO, LocalStack, Ceph, R2) is path-style
//! (`<endpoint>/<bucket>/<key>`) unless the configuration says otherwise.
//!
//! Failures are `Storage.StorageError` values. `code` is the S3 error code
//! from the response's `Error` document (`AccessDenied`, `NoSuchBucket`,
//! `SignatureDoesNotMatch`), except that `NoSuchKey` and a bodyless 404 are
//! the interface's `NOT_FOUND`; a response with no error document is
//! `HTTP_<status>`, and a request that got no response is `CONNECTION_FAILED`.
//! `CREDENTIALS_UNAVAILABLE` means the configured credential provider had no
//! credentials to sign with.
//!
//! Credentials are static (the access key fields of `S3Config`) or come from
//! an `Aws.Credentials.CredentialProvider` (`withCredentialProvider`), which
//! is asked before every attempt, so temporary credentials are replaced when
//! the provider refreshes them. A request that is throttled, answered with a
//! 5xx, cut off or not answered in time is sent again, signed afresh, as the
//! `Aws.Retry` policy of the configuration says (`withRetry`).
@runtime_checked
@io
package Storage.S3.Client

import Std.Core
import Std.Collections
import Std.Encoding as Enc
import Std.Errors
import Std.File as File
import Std.Hash as Hash
import Std.Http
import Std.String as Str
import Std.Task
import Std.Time as Time
import Std.Tls as Tls
import Aws.Credentials
import Aws.Credentials.Chain
import Aws.Credentials.Profile
import Aws.Retry
import Aws.SigV4
import Aws.SigV4.Env as AwsEnv
import Storage
import Storage.S3.SigV4
import Storage.S3.Wire

// The default per-request deadline, 30 seconds.
val DEFAULT_REQUEST_TIMEOUT_MS: Int = 30000

/// Where a bucket is and how to sign for it.
///
/// `endpoint` is empty for AWS, or the base URL of an S3-compatible server
/// (`https://minio.example.com`, `http://127.0.0.1:9000`). It must be
/// `https://` unless the host is loopback: the signature does not hide the
/// access key id, the object or its metadata. `pathStyle` puts the bucket in
/// the path instead of the host name; custom endpoints usually need it.
/// `sessionToken` is empty for long-term credentials.
///
/// `caCertificatePem` and `additionalCaCertificatePem` are set with
/// `withExclusiveCaCertificate` and `withCaCertificate`; both empty verifies
/// the server certificate against the system trust store.
pub record S3Config {
  bucket: String
  region: String
  accessKeyId: String
  secretAccessKey: String
  sessionToken: String
  endpoint: String
  pathStyle: Bool
  requestTimeoutMs: Int
  /// The only certificate authority the server certificate may chain to (PEM
  /// text); `""` trusts the system trust store and
  /// `additionalCaCertificatePem`.
  caCertificatePem: String = ""
  /// A certificate authority trusted in addition to the system trust store
  /// (PEM text); `""` adds none. Ignored when `caCertificatePem` is set.
  additionalCaCertificatePem: String = ""
  /// Where the credentials come from; `None` uses `accessKeyId`,
  /// `secretAccessKey` and `sessionToken`. Set with `withCredentialProvider`.
  credentialProvider: Option[CredentialProvider] = None
  /// When a failed request is sent again. Set with `withRetry` or
  /// `withoutRetries`.
  retry: RetryPolicy = standardRetry()
}

/// Configuration for a bucket on AWS, virtual-hosted.
pub func awsConfig(bucket: in String, region: in String, accessKeyId: in String, secretAccessKey: in String): S3Config {
  S3Config(
    bucket = bucket,
    region = region,
    accessKeyId = accessKeyId,
    secretAccessKey = secretAccessKey,
    sessionToken = "",
    endpoint = "",
    pathStyle = false,
    requestTimeoutMs = DEFAULT_REQUEST_TIMEOUT_MS
  )
}

/// Configuration for a bucket on an S3-compatible server at `endpoint`,
/// path-style.
pub func endpointConfig(
  endpoint: in String,
  bucket: in String,
  region: in String,
  accessKeyId: in String,
  secretAccessKey: in String,
): S3Config {
  S3Config(
    bucket = bucket,
    region = region,
    accessKeyId = accessKeyId,
    secretAccessKey = secretAccessKey,
    sessionToken = "",
    endpoint = endpoint,
    pathStyle = true,
    requestTimeoutMs = DEFAULT_REQUEST_TIMEOUT_MS
  )
}

/// `cfg` with temporary credentials' session token.
pub func S3Config.withSessionToken(cfg: in S3Config, sessionToken: in String): S3Config {
  cfg.copy(sessionToken = sessionToken)
}

/// `cfg` addressing the bucket in the path (`true`) or the host name.
pub func S3Config.withPathStyle(cfg: in S3Config, pathStyle: in Bool): S3Config {
  cfg.copy(pathStyle = pathStyle)
}

/// `cfg` with a per-request deadline in milliseconds.
pub func S3Config.withRequestTimeout(cfg: in S3Config, requestTimeoutMs: in Int): S3Config {
  cfg.copy(requestTimeoutMs = requestTimeoutMs)
}

/// `cfg` signing with the credentials `provider` returns instead of the
/// access key fields. The provider is asked before every request and every
/// retry: wrap one that calls a service or reads a file in
/// `Aws.Credentials.cached` (`Aws.Credentials.Chain.defaultProvider()` is).
pub func S3Config.withCredentialProvider(cfg: in S3Config, provider: in CredentialProvider): S3Config {
  cfg.copy(credentialProvider = Some(value = provider))
}

/// `cfg` retrying as `policy` says (`Aws.Retry`). The default is
/// `standardRetry()`: three attempts.
pub func S3Config.withRetry(cfg: in S3Config, policy: in RetryPolicy): S3Config {
  cfg.copy(retry = policy)
}

/// `cfg` sending every request once.
pub func S3Config.withoutRetries(cfg: in S3Config): S3Config {
  cfg.copy(retry = noRetry())
}

/// `cfg` trusting the certificate authority `caPem` (PEM text, one
/// certificate) in addition to the system trust store: the additive form, as
/// `Std.Http`'s `withCaCertificate`. For an S3-compatible server whose
/// certificate is signed by a private authority, or is self-signed (pass the
/// certificate itself). `withExclusiveCaCertificate` wins when both are set.
/// An unusable certificate is `INVALID_CONFIG` from `openClient`.
pub func S3Config.withCaCertificate(cfg: in S3Config, caPem: in String): S3Config
  requires: caPem.length > 0
{
  cfg.copy(additionalCaCertificatePem = caPem)
}

/// `cfg` trusting ONLY the certificate authority `caPem` (PEM text) for the
/// server certificate: the system trust store is not consulted. This is the
/// trust-replacing form, as `Std.Http`'s `withExclusiveCaCertificate`;
/// `withCaCertificate` is the additive one. An unusable certificate is
/// `INVALID_CONFIG` from `openClient`.
pub func S3Config.withExclusiveCaCertificate(cfg: in S3Config, caPem: in String): S3Config
  requires: caPem.length > 0
{
  cfg.copy(caCertificatePem = caPem)
}

func readPem(path: in String): Result[String, StorageError] {
  if path.length == 0 {
    return Err(invalidConfig("the path of the CA certificate is empty"))
  }
  match File.readText(path) {
    case Ok(text) -> if text.length > 0 {
      Ok(text)
    } else {
      Err(invalidConfig("the CA certificate file is empty: " + path))
    }
    case Err(_) -> Err(invalidConfig("cannot read the CA certificate: " + path))
  }
}

/// `withCaCertificate` reading the PEM file at `path`. `Err(INVALID_CONFIG)`
/// when the file cannot be read or is empty.
pub func S3Config.withCaCertificateFile(cfg: in S3Config, path: in String): Result[S3Config, StorageError] {
  match readPem(path) {
    case Ok(pem) -> Ok(cfg.withCaCertificate(pem))
    case Err(e) -> Err(e)
  }
}

/// `withExclusiveCaCertificate` reading the PEM file at `path`.
/// `Err(INVALID_CONFIG)` when the file cannot be read or is empty.
pub func S3Config.withExclusiveCaCertificateFile(cfg: in S3Config, path: in String): Result[S3Config, StorageError] {
  match readPem(path) {
    case Ok(pem) -> Ok(cfg.withExclusiveCaCertificate(pem))
    case Err(e) -> Err(e)
  }
}

/// Configuration for `bucket` from the process environment and the AWS
/// shared files: `configFromEnvironment` over
/// `Aws.Credentials.processEnvironment()`.
pub func configFromEnv(bucket: in String): Result[S3Config, StorageError] {
  configFromEnvironment(bucket, processEnvironment())
}

/// Configuration for `bucket` as the AWS SDKs would find it, reading
/// variables through `lookup`:
///
///   * credentials from the default chain (`Aws.Credentials.Chain`): the
///     environment (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, optional
///     `AWS_SESSION_TOKEN`), web identity, the shared config and credentials
///     files (`AWS_PROFILE`), the container endpoint, the EC2 instance
///     metadata service. The chain is asked once here, so that missing
///     credentials are an error now. Credentials from the environment become
///     the static fields of the configuration, as they always did; any other
///     source is kept as a cached credential provider, which refreshes
///     temporary credentials before they expire;
///   * the region from `AWS_REGION`, `AWS_DEFAULT_REGION` or the profile's
///     `region`;
///   * an optional endpoint from `AWS_ENDPOINT_URL_S3`, `AWS_ENDPOINT_URL` or
///     the profile's `endpoint_url` (path-style when set);
///   * an optional certificate authority, trusted in addition to the system
///     trust store, from `AWS_CA_BUNDLE` or the profile's `ca_bundle`.
///
/// `Err(INVALID_CONFIG)` says what is missing; for credentials it holds the
/// reason of every provider of the chain.
pub func configFromEnvironment(bucket: in String, lookup: in (String) -> String): Result[S3Config, StorageError] {
  val provider = cached(defaultChain(lookup))
  val resolved = match provider.resolve() {
    case Ok(c) -> c
    case Err(e) -> return Err(invalidConfig("no AWS credentials: " + e.text()))
  }
  val settings = match settingsFromEnvironment(lookup, "S3") {
    case Ok(s) -> s
    case Err(e) -> return Err(invalidConfig(e.text()))
  }
  if settings.region.length == 0 {
    return Err(
      invalidConfig(
        "no AWS region: neither AWS_REGION nor AWS_DEFAULT_REGION is set, and the profile has no region"
      )
    )
  }
  val fromEnvironment = resolved.source == "environment"
  var cfg = S3Config(
    bucket = bucket,
    region = settings.region,
    accessKeyId = if fromEnvironment { resolved.credentials.accessKeyId } else { "" },
    secretAccessKey = if fromEnvironment { resolved.credentials.secretAccessKey } else { "" },
    sessionToken = if fromEnvironment { resolved.credentials.sessionToken } else { "" },
    endpoint = settings.endpointUrl,
    pathStyle = settings.endpointUrl.length > 0,
    requestTimeoutMs = DEFAULT_REQUEST_TIMEOUT_MS
  )
  if not fromEnvironment {
    cfg = cfg.withCredentialProvider(provider)
  }
  if settings.caBundlePath.length > 0 {
    cfg = match cfg.withCaCertificateFile(settings.caBundlePath) { case Ok(c) -> c; case Err(e) -> return Err(e) }
  }
  Ok(cfg)
}

/// A connection to one bucket. Build one with `openClient`; the fields are
/// derived from a validated `S3Config` and are not meant to be set by hand.
pub record S3Client {
  bucket: String
  region: String
  // Asked for the credentials of every attempt.
  credentials: CredentialProvider
  retry: RetryPolicy
  // `https://host[:port]`, no trailing slash.
  origin: String
  // The authority sent as `Host` and signed: `host[:port]`.
  host: String
  // `/<bucket>` when path-style, else empty.
  basePath: String
  requestTimeoutMs: Int
  http: HttpClient
}

func invalidConfig(message: in String): StorageError {
  StorageError(message = message, code = "INVALID_CONFIG")
}

func isLowerOrDigit(c: in Char): Bool {
  (c >= 'a' and c <= 'z') or (c >= '0' and c <= '9')
}

/// True for a bucket name S3 accepts in a host name: 3 to 63 characters of
/// lowercase letters, digits, `-` and `.`, starting and ending with a letter
/// or digit. Checked before the name is built into a URL.
@pure
pub func isValidBucketName(name: in String): Bool {
  if name.length < 3 or name.length > 63 {
    return false
  }
  var i = 0
  while i < name.length {
    val c = Str.charAtOrReplacement(name, i)
    if not (isLowerOrDigit(c) or c == '-' or c == '.') {
      return false
    }
    i = i + 1
  }
  isLowerOrDigit(Str.charAtOrReplacement(name, 0)) and isLowerOrDigit(Str.charAtOrReplacement(name, name.length - 1)) and
    not Str.contains(name, "..")
}

/// True for a plausible region name (`us-east-1`): lowercase letters, digits
/// and `-`, so it cannot alter the host name it is built into.
@pure
pub func isValidRegion(region: in String): Bool {
  if region.length == 0 {
    return false
  }
  var i = 0
  while i < region.length {
    val c = Str.charAtOrReplacement(region, i)
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

// A validated endpoint, split.
record Endpoint {
  scheme: String
  authority: String
}

// An endpoint is `http(s)://host[:port]` with nothing after the authority but
// an optional `/`. The default port is dropped so the signed `host` is the
// `Host` header an HTTP client sends.
func parseEndpoint(raw: in String): Result[Endpoint, StorageError] {
  var scheme = ""
  var rest = ""
  if Str.startsWith(raw, "https://") {
    scheme = "https"
    rest = raw.substring(8)
  } else if Str.startsWith(raw, "http://") {
    scheme = "http"
    rest = raw.substring(7)
  } else {
    return Err(invalidConfig("S3 endpoint must be an absolute http(s) URL: " + raw))
  }
  if Str.endsWith(rest, "/") {
    rest = rest.substring(0, rest.length - 1)
  }
  if rest.length == 0 {
    return Err(invalidConfig("S3 endpoint has no host: " + raw))
  }
  var i = 0
  while i < rest.length {
    if not isAuthorityChar(Str.charAtOrReplacement(rest, i)) {
      return Err(invalidConfig("S3 endpoint must be scheme://host[:port] with no path, query or credentials: " + raw))
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
        "S3 endpoint must use https:// (http:// is allowed only for localhost, 127.0.0.1 and [::1]): " + raw
      )
    )
  }
  Ok(Endpoint(scheme = scheme, authority = authority))
}

func caCertificate(pem: in String, option: in String): Result[Tls.Certificate, StorageError] {
  match Tls.Certificate.fromPem(pem) {
    case Ok(cert) -> Ok(cert)
    case Err(e) -> Err(
      invalidConfig("the certificate given to " + option + " is not usable: " + Tls.TlsError.message(e))
    )
  }
}

// The HTTP client of a configuration. A redirect (S3 answers 301/307 for a
// bucket in another region) would be followed to a host the signature does
// not cover; it is returned as the error it is instead.
func httpClientFor(cfg: in S3Config): Result[HttpClient, StorageError] {
  if cfg.caCertificatePem.length == 0 and cfg.additionalCaCertificatePem.length == 0 {
    return Ok(clientNoRedirects())
  }
  if not tlsConfigSupported() {
    return Err(invalidConfig("a CA certificate cannot be configured on this target"))
  }
  var builder = HttpClientBuilder.new().withNoRedirects()
  if cfg.caCertificatePem.length > 0 {
    builder = match caCertificate(cfg.caCertificatePem, "withExclusiveCaCertificate") {
      case Err(e) -> return Err(e)
      case Ok(cert) -> builder.withExclusiveCaCertificate(cert)
    }
  } else {
    builder = match caCertificate(cfg.additionalCaCertificatePem, "withCaCertificate") {
      case Err(e) -> return Err(e)
      case Ok(cert) -> builder.withCaCertificate(cert)
    }
  }
  Ok(builder.build())
}

/// Validate `cfg` and open a client for its bucket. No request is made: a
/// wrong credential or a missing bucket shows up on the first operation.
///
/// `Err(INVALID_CONFIG)` for an invalid bucket or region name, empty
/// credentials when there is no credential provider, a malformed endpoint, a plain `http://` endpoint that is not
/// loopback, a non-positive timeout, or a CA certificate that is not a PEM
/// certificate.
pub func openClient(cfg: in S3Config): Result[S3Client, StorageError] {
  if not isValidBucketName(cfg.bucket) {
    return Err(invalidConfig("invalid S3 bucket name: '" + cfg.bucket + "'"))
  }
  if not isValidRegion(cfg.region) {
    return Err(invalidConfig("invalid AWS region: '" + cfg.region + "'"))
  }
  val credentials = match cfg.credentialProvider {
    case Some(provider) -> provider
    case None -> {
      if cfg.accessKeyId.length == 0 or cfg.secretAccessKey.length == 0 {
        return Err(invalidConfig("S3 access key id and secret access key must be non-empty"))
      }
      staticProviderOf(
        Credentials(accessKeyId = cfg.accessKeyId, secretAccessKey = cfg.secretAccessKey, sessionToken = cfg.sessionToken)
      )
    }
  }
  if cfg.retry.maxAttempts < 1 {
    return Err(invalidConfig("retry.maxAttempts must be >= 1, got " + cfg.retry.maxAttempts.toString()))
  }
  if cfg.requestTimeoutMs <= 0 {
    return Err(invalidConfig("requestTimeoutMs must be > 0, got " + cfg.requestTimeoutMs.toString()))
  }
  val endpoint = if cfg.endpoint.length == 0 {
    Endpoint(scheme = "https", authority = "s3." + cfg.region + ".amazonaws.com")
  } else {
    match parseEndpoint(cfg.endpoint) {
      case Err(e) -> return Err(e)
      case Ok(e) -> e
    }
  }
  val host = if cfg.pathStyle { endpoint.authority } else { cfg.bucket + "." + endpoint.authority }
  val http = match httpClientFor(cfg) { case Ok(h) -> h; case Err(e) -> return Err(e) }
  Ok(
    S3Client(
      bucket = cfg.bucket,
      region = cfg.region,
      credentials = credentials,
      retry = cfg.retry,
      origin = endpoint.scheme + "://" + host,
      host = host,
      basePath = if cfg.pathStyle { "/" + cfg.bucket } else { "" },
      requestTimeoutMs = cfg.requestTimeoutMs,
      http = http
    )
  )
}

/// `yyyymmddThhmmssZ` for the current instant: the `x-amz-date` of a request
/// signed now.
pub func amzDateNow(): String {
  AwsEnv.amzDateNow()
}

/// The canonical URI of `key` in this client's bucket (`/<key>` or
/// `/<bucket>/<key>`), as it is signed and requested.
@pure
pub func S3Client.objectPath(client: in S3Client, key: in String): String {
  client.basePath + canonicalKeyPath(key)
}

/// The canonical URI of the bucket itself (`/` or `/<bucket>`).
@pure
pub func S3Client.bucketPath(client: in S3Client): String {
  if client.basePath.length == 0 {
    "/"
  } else {
    client.basePath
  }
}

/// A request body and the `Content-Type` it is sent with.
pub record Payload {
  contentType: String
  bytes: slice[Byte]
}

/// A response to a signed request: the status, the headers the caller needs,
/// and the body.
pub record S3Response {
  status: Int
  body: slice[Byte]
  response: HttpResponse
}

func transportError(message: in String): StorageError {
  StorageError(message = message, code = "CONNECTION_FAILED")
}

/// The credentials to sign with now, from the client's provider.
/// `Err(CREDENTIALS_UNAVAILABLE)` when it has none.
pub func S3Client.currentCredentials(client: in S3Client): Result[Credentials, StorageError] {
  val resolved = resolveGuarded(client.credentials)
  match resolved {
    case Ok(c) -> Ok(c.credentials)
    case Err(e) -> Err(StorageError(message = "no AWS credentials: " + e.text(), code = "CREDENTIALS_UNAVAILABLE"))
  }
}

// What one attempt came to: the result, and for a failure what kind it is
// (`NotRetryable` for a success).
record Attempt {
  outcome: Result[S3Response, StorageError]
  kind: FailureKind
  retryAfterMs: Int
}

func failedAttempt(e: in StorageError, kind: in FailureKind): Attempt {
  Attempt(outcome = Err(e), kind = kind, retryAfterMs = -1)
}

func isTimeoutCause(cause: in String): Bool {
  cause == "request timed out" or cause == "HTTP connect timed out" or Str.contains(cause, "HttpClient.Timeout")
}

// One send, bounded by the request timeout and awaited in place. `Std.Http`
// reports every transport failure as `Err` on all three targets, so nothing
// is caught here.
func sendBounded(client: in S3Client, req: in HttpRequest, url: in String): Attempt {
  val src = makeCancelSourceTimeout(client.requestTimeoutMs)
  val tok = sourceToken(src)
  val sent = await client.http.sendWithCancel(req, tok)
  val deadlinePassed = isCancelled(tok)
  disposeSource(src)
  val resp = match sent {
    case Ok(r) -> r
    case Err(e) -> {
      val timedOut = deadlinePassed or
        match e {
          case ConnectionFailed(_, cause) -> isTimeoutCause(cause)
          case Timeout(_, _) -> true
          case _ -> false
        }
      if timedOut {
        return failedAttempt(
          transportError("S3 request timed out after " + client.requestTimeoutMs.toString() + " ms: " + url),
          FailureKind.TimedOut
        )
      }
      val message = HttpError.message(e)
      return failedAttempt(
        transportError(message),
        if isConnectFailure(message) { FailureKind.ConnectFailed } else { FailureKind.NoResponse }
      )
    }
  }
  match readBody(resp) {
    case Err(e) -> failedAttempt(e, FailureKind.NoResponse)
    case Ok(bytes) -> answered(resp, bytes)
  }
}

func readBody(resp: in HttpResponse): Result[slice[Byte], StorageError] {
  val read = await HttpResponse.bodyBytes(resp)
  match read {
    case Ok(bytes) -> Ok(bytes)
    case Err(e) -> Err(transportError("cannot read S3 response body: " + IOError.message(e)))
  }
}

// The attempt that got `resp`: a response of any status is `Ok`; a failed
// one is classified for the retry policy.
func answered(resp: in HttpResponse, bytes: in slice[Byte]): Attempt {
  val status = HttpResponse.statusCode(resp)
  var kind = FailureKind.NotRetryable
  var retryAfterMs = -1
  if status >= 400 {
    val text = match Enc.tryDecodeUtf8(bytes) { case Some(t) -> t; case None -> "" }
    val code = match parseFault(text) { case Some(fault) -> fault.code; case None -> "" }
    kind = classifyResponse(status, code)
    retryAfterMs = retryAfterMsOf(
      match HttpResponse.header(resp, "Retry-After") { case Some(v) -> v; case None -> "" },
      match HttpResponse.header(resp, "x-amz-retry-after") { case Some(v) -> v; case None -> "" }
    )
  }
  Attempt(
    outcome = Ok(S3Response(status = status, body = bytes, response = resp)),
    kind = kind,
    retryAfterMs = retryAfterMs
  )
}

// One signed attempt.
func attempt(
  client: in S3Client,
  creds: in Credentials,
  method: in HttpMethod,
  methodName: in String,
  canonicalUri: in String,
  query: in List[Pair],
  amzHeaders: in List[Pair],
  body: in Option[Payload],
): Attempt {
  val payloadHash = match body {
    case Some(payload) -> Str.toLower(Hash.sha256OfBytes(payload.bytes))
    case None -> emptyPayloadSha256()
  }
  val signed = signedHeaders(
    methodName,
    canonicalUri,
    query,
    client.host,
    amzHeaders,
    payloadHash,
    true,
    creds,
    client.region,
    "s3",
    amzDateNow()
  )
  val queryString = canonicalQuery(query)
  val url = client.origin + canonicalUri + (if queryString.length > 0 { "?" + queryString } else { "" })
  val parsed = match Url.tryFrom(url) {
    case Ok(u) -> u
    case Err(_) -> return failedAttempt(
      StorageError(message = "cannot build an S3 request URL: " + url, code = "INVALID_ARGUMENT"),
      FailureKind.NotRetryable
    )
  }
  var req = request(method, parsed)
  match body {
    case Some(payload) -> {
      match withBytesBody(req, payload.contentType, payload.bytes) {
        case Ok(r) -> {
          req = r
        }
        case Err(e) -> return failedAttempt(
          StorageError(message = HttpError.message(e), code = "INVALID_ARGUMENT"),
          FailureKind.NotRetryable
        )
      }
    }
    case None -> ()
  }
  // `Host` is set by the HTTP client from the URL; every other signed header
  // is sent as signed, then `Authorization`.
  var k = 0
  while k < signed.headers.count {
    match withHeader(req, signed.headers[k].name, signed.headers[k].value) {
      case Ok(r) -> {
        req = r
      }
      case Err(e) -> return failedAttempt(
        StorageError(message = HttpError.message(e), code = "INVALID_ARGUMENT"),
        FailureKind.NotRetryable
      )
    }
    k = k + 1
  }
  sendBounded(client, req, url)
}

func sendRetrying(
  client: in S3Client,
  method: in HttpMethod,
  methodName: in String,
  canonicalUri: in String,
  query: in List[Pair],
  amzHeaders: in List[Pair],
  body: in Option[Payload],
  idempotent: in Bool,
): Result[S3Response, StorageError] {
  var number = 1
  while true {
    // Asked again for every attempt: a retry is signed with the credentials
    // and the date of its own moment.
    val creds = match client.currentCredentials() { case Ok(c) -> c; case Err(e) -> return Err(e) }
    val made = attempt(client, creds, method, methodName, canonicalUri, query, amzHeaders, body)
    match client.retry.decide(number, made.kind, idempotent, made.retryAfterMs) {
      case GiveUp -> return made.outcome
      case RetryAfter(delayMs) -> client.retry.pause(delayMs)
    }
    number = number + 1
  }
  Err(transportError("unreachable"))
}

/// Sign and send one request to the bucket, again if the client's retry
/// policy says so. The request must be idempotent: after a failure that
/// leaves its outcome unknown (a 5xx, a cut connection, a timeout) it is
/// sent again. Every object operation of S3 is; use `sendUnrepeatable` for
/// one that is not.
///
/// `canonicalUri` is the encoded path (`objectPath`), `query` the unencoded
/// query parameters, `amzHeaders` extra `x-amz-*` headers to sign and send
/// (lowercase names), and `body` the payload, or `None` for a request without
/// one. Any HTTP response is `Ok`, whatever its status (the last one, when
/// the request was retried); `Err` means no response arrived
/// (`CONNECTION_FAILED`), the request could not be built
/// (`INVALID_ARGUMENT`), or there are no credentials
/// (`CREDENTIALS_UNAVAILABLE`).
pub func S3Client.send(
  client: in S3Client,
  method: in HttpMethod,
  methodName: in String,
  canonicalUri: in String,
  query: in List[Pair],
  amzHeaders: in List[Pair],
  body: in Option[Payload],
): Result[S3Response, StorageError] {
  sendRetrying(client, method, methodName, canonicalUri, query, amzHeaders, body, true)
}

/// `send` for a request that must not be carried out twice: it is sent again
/// only when S3 throttled it or the connection could not be opened, never
/// after a 5xx, a cut connection or a timeout.
pub func S3Client.sendUnrepeatable(
  client: in S3Client,
  method: in HttpMethod,
  methodName: in String,
  canonicalUri: in String,
  query: in List[Pair],
  amzHeaders: in List[Pair],
  body: in Option[Payload],
): Result[S3Response, StorageError] {
  sendRetrying(client, method, methodName, canonicalUri, query, amzHeaders, body, false)
}

/// The `StorageError` for a response that is not the success the caller
/// wanted: the S3 error code and message when the body is an `Error` document
/// (`NoSuchKey` as `NOT_FOUND`), `NOT_FOUND` for a bodyless 404, and
/// `HTTP_<status>` otherwise.
pub func faultOf(resp: in S3Response, what: in String): StorageError {
  val text = match Enc.tryDecodeUtf8(resp.body) { case Some(t) -> t; case None -> "" }
  match parseFault(text) {
    case Some(fault) -> {
      val code = if fault.code == "NoSuchKey" { "NOT_FOUND" } else { fault.code }
      StorageError(
        message = what + ": " + fault.code + " (HTTP " + resp.status.toString() + ")" +
          (if fault.message.length > 0 { ": " + fault.message } else { "" }),
        code = code
      )
    }
    case None -> {
      val code = if resp.status == 404 { "NOT_FOUND" } else { "HTTP_" + resp.status.toString() }
      StorageError(message = what + ": S3 answered HTTP " + resp.status.toString(), code = code)
    }
  }
}

func headerOr(resp: in S3Response, name: in String, fallback: in String): String {
  match HttpResponse.header(resp.response, name) {
    case Some(v) -> v
    case None -> fallback
  }
}

/// An object's metadata as S3 reports it in response headers.
pub record ObjectHead {
  contentType: String
  contentLength: Long
  etag: String
  lastModified: String
  userMeta: Map[String, String]
  storageClass: String
}

val META_PREFIX: String = "x-amz-meta-"

func isoOr(httpDate: in String, fallback: in String): String {
  match httpDateToIso(httpDate) {
    case Some(iso) -> iso
    case None -> fallback
  }
}

func headOf(resp: in S3Response, contentLength: in Long): ObjectHead {
  val userMeta: Map[String, String] = newMap()
  val names = HttpResponse.headerNames(resp.response)
  var i = 0
  while i < names.length {
    val lower = Str.toLower(names[i])
    if Str.startsWith(lower, META_PREFIX) and lower.length > META_PREFIX.length {
      userMeta[lower.substring(META_PREFIX.length)] = headerOr(resp, names[i], "")
    }
    i = i + 1
  }
  val lastModified = headerOr(resp, "Last-Modified", "")
  ObjectHead(
    contentType = headerOr(resp, "Content-Type", "application/octet-stream"),
    contentLength = contentLength,
    etag = unquoteEtag(headerOr(resp, "ETag", "")),
    lastModified = isoOr(lastModified, lastModified),
    userMeta = userMeta,
    storageClass = headerOr(resp, "x-amz-storage-class", "STANDARD")
  )
}

func isMetaKeyChar(c: in Char): Bool {
  isLowerOrDigit(c) or c == '-' or c == '_' or c == '.'
}

/// Why S3 cannot store this user metadata entry unchanged, or `""` when it
/// can. S3 carries user metadata in `x-amz-meta-<key>` headers: it lowercases
/// the key, so a key is accepted only in lowercase letters, digits, `-`, `_`
/// and `.`; and a value must be printable ASCII with no leading or trailing
/// space, since a header cannot carry anything else intact.
@pure
pub func userMetaProblem(key: in String, value: in String): String {
  if key.length == 0 {
    return "user metadata key is empty"
  }
  var i = 0
  while i < key.length {
    if not isMetaKeyChar(Str.charAtOrReplacement(key, i)) {
      return "user metadata key '" + key + "' must be lowercase letters, digits, '-', '_' or '.' (S3 lowercases keys)"
    }
    i = i + 1
  }
  var k = 0
  while k < value.length {
    val c = Str.charAtOrReplacement(value, k)
    if c < ' ' or c > '~' {
      return "user metadata value for '" + key + "' must be printable ASCII"
    }
    k = k + 1
  }
  if value.length > 0 and (Str.startsWith(value, " ") or Str.endsWith(value, " ")) {
    return "user metadata value for '" + key + "' must not start or end with a space"
  }
  ""
}

/// What PutObject reported.
pub record PutOutcome {
  etag: String
  // The size of the stored object in bytes.
  contentLength: Long
  // The server's `Date` as ISO 8601, or the local clock when it sent none.
  storedAt: String
}

/// PutObject: store `bytes` under `key`. `amzHeaders` are extra signed
/// `x-amz-*` headers (lowercase names), such as `x-amz-storage-class`.
/// `Err(USER_META_UNSUPPORTED)` for metadata S3 would alter
/// (`userMetaProblem`).
pub func S3Client.putObject(
  client: in S3Client,
  key: in String,
  bytes: in slice[Byte],
  contentType: in String,
  userMeta: in Map[String, String],
  amzHeaders: in List[Pair],
): Result[PutOutcome, StorageError] {
  putPayload(client, key, Payload(contentType = contentType, bytes = bytes), userMeta, amzHeaders)
}

/// PutObject for a payload in the base64 form `StorageBucket.put` takes: the
/// object stored is the decoded bytes. `Err(INVALID_BASE64)` when
/// `dataBase64` is not base64; nothing is sent then.
pub func S3Client.putObjectBase64(
  client: in S3Client,
  key: in String,
  dataBase64: in String,
  contentType: in String,
  userMeta: in Map[String, String],
): Result[PutOutcome, StorageError] {
  val noHeaders: List[Pair] = newList()
  match Enc.tryDecodeBase64(dataBase64) {
    case Some(decoded) -> putPayload(
      client,
      key,
      Payload(contentType = contentType, bytes = decoded),
      userMeta,
      noHeaders
    )
    case None -> Err(StorageError(message = "invalid base64 payload for '" + key + "'", code = "INVALID_BASE64"))
  }
}

func putPayload(
  client: in S3Client,
  key: in String,
  payload: in Payload,
  userMeta: in Map[String, String],
  amzHeaders: in List[Pair],
): Result[PutOutcome, StorageError] {
  val headers: List[Pair] = newList()
  val entries = mapEntries(userMeta)
  for e in entries {
    val problem = userMetaProblem(e.key, e.value)
    if problem.length > 0 {
      return Err(StorageError(message = problem, code = "USER_META_UNSUPPORTED"))
    }
    headers.add(Pair(name = META_PREFIX + e.key, value = e.value))
  }
  var i = 0
  while i < amzHeaders.count {
    headers.add(amzHeaders[i])
    i = i + 1
  }
  val noQuery: List[Pair] = newList()
  match client.send(PUT, "PUT", client.objectPath(key), noQuery, headers, Some(value = payload)) {
    case Err(e) -> Err(e)
    case Ok(resp) -> {
      if resp.status != 200 {
        return Err(faultOf(resp, "cannot put object '" + key + "'"))
      }
      Ok(
        PutOutcome(
          etag = unquoteEtag(headerOr(resp, "ETag", "")),
          contentLength = payload.bytes.length.toLong(),
          storedAt = isoOr(headerOr(resp, "Date", ""), Time.toIsoString(Time.now()))
        )
      )
    }
  }
}

/// A downloaded object.
pub record GotObject {
  bytes: slice[Byte]
  head: ObjectHead
}

/// GetObject: download `key`. `Err(NOT_FOUND)` when there is no such object.
pub func S3Client.getObject(client: in S3Client, key: in String): Result[GotObject, StorageError] {
  val none: List[Pair] = newList()
  match client.send(GET, "GET", client.objectPath(key), none, none, None) {
    case Err(e) -> Err(e)
    case Ok(resp) -> {
      if resp.status != 200 {
        return Err(faultOf(resp, "cannot get object '" + key + "'"))
      }
      Ok(GotObject(bytes = resp.body, head = headOf(resp, resp.body.length.toLong())))
    }
  }
}

/// HeadObject: `Some(head)` when `key` exists, `None` when it does not. No
/// body is transferred.
pub func S3Client.headObject(client: in S3Client, key: in String): Result[Option[ObjectHead], StorageError] {
  val none: List[Pair] = newList()
  match client.send(HEAD, "HEAD", client.objectPath(key), none, none, None) {
    case Err(e) -> Err(e)
    case Ok(resp) -> {
      if resp.status == 404 {
        return Ok(None)
      }
      if resp.status != 200 {
        return Err(faultOf(resp, "cannot head object '" + key + "'"))
      }
      val length = match parseContentLength(headerOr(resp, "Content-Length", "0")) { case Some(n) -> n; case None -> 0i64 }
      Ok(Some(value = headOf(resp, length)))
    }
  }
}

func parseContentLength(text: in String): Option[Long] {
  var n = 0i64
  val t = Str.trim(text)
  if t.length == 0 or t.length > 18 {
    return None
  }
  var i = 0
  while i < t.length {
    val u = Str.codeUnitAt(t, i)
    if u < 48 or u > 57 {
      return None
    }
    n = n * 10i64 + (u - 48).toLong()
    i = i + 1
  }
  Some(value = n)
}

/// DeleteObject. Deleting a key that does not exist succeeds, as S3 defines.
pub func S3Client.deleteObject(client: in S3Client, key: in String): Result[Unit, StorageError] {
  val none: List[Pair] = newList()
  match client.send(DELETE, "DELETE", client.objectPath(key), none, none, None) {
    case Err(e) -> Err(e)
    case Ok(resp) -> {
      if resp.status == 204 or resp.status == 200 {
        return Ok(())
      }
      Err(faultOf(resp, "cannot delete object '" + key + "'"))
    }
  }
}

/// ListObjectsV2: one page of at most `maxKeys` keys beginning with `prefix`,
/// in key order, continuing after `continuationToken` when it is non-empty.
/// Keys are requested url-encoded, so a key holding characters XML cannot
/// carry is still listed.
pub func S3Client.listObjects(
  client: in S3Client, prefix: in String, continuationToken: in String, maxKeys: in Int
): Result[ListPage, StorageError] {
  val query: List[Pair] = newList()
  query.add(Pair(name = "list-type", value = "2"))
  query.add(Pair(name = "encoding-type", value = "url"))
  query.add(Pair(name = "max-keys", value = maxKeys.toString()))
  if prefix.length > 0 {
    query.add(Pair(name = "prefix", value = prefix))
  }
  if continuationToken.length > 0 {
    query.add(Pair(name = "continuation-token", value = continuationToken))
  }
  val none: List[Pair] = newList()
  match client.send(GET, "GET", client.bucketPath(), query, none, None) {
    case Err(e) -> Err(e)
    case Ok(resp) -> {
      if resp.status != 200 {
        return Err(faultOf(resp, "cannot list bucket '" + client.bucket + "'"))
      }
      val text = match Enc.tryDecodeUtf8(resp.body) {
        case Some(t) -> t
        case None -> return Err(StorageError(message = "S3 listing is not UTF-8", code = "LIST_ERROR"))
      }
      match parseListPage(text) {
        case Ok(page) -> Ok(page)
        case Err(message) -> Err(StorageError(message = message, code = "LIST_ERROR"))
      }
    }
  }
}

/// A URL that performs `methodName` (`GET`, `PUT`) on `key` without
/// credentials until `expiresInSeconds` after `amzDate` (`amzDateNow()`).
/// Computed locally with the client's current credentials; S3 is not
/// contacted and the object need not exist. `Err(CREDENTIALS_UNAVAILABLE)`
/// when the credential provider has none.
pub func S3Client.presign(
  client: in S3Client,
  methodName: in String,
  key: in String,
  expiresInSeconds: in Int,
  amzDate: in String,
): Result[String, StorageError]
  requires: expiresInSeconds >= 1 and expiresInSeconds <= 604800
  requires: amzDate.length == 16
{
  val creds = match client.currentCredentials() { case Ok(c) -> c; case Err(e) -> return Err(e) }
  val path = client.objectPath(key)
  val none: List[Pair] = newList()
  Ok(
    client.origin + path + "?" +
      presignQuery(methodName, client.host, path, none, creds, client.region, "s3", amzDate, expiresInSeconds)
  )
}
