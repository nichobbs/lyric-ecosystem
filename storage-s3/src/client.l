//! Storage.S3.Client - a signed S3 REST client over `Std.Http`.
//!
//! One `S3Client` talks to one bucket. Every request is signed with AWS
//! Signature Version 4 (`Storage.S3.SigV4`) and sent with `Std.Http`; there is
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
@runtime_checked
@io
package Storage.S3.Client

import Std.Core
import Std.Collections
import Std.Encoding as Enc
import Std.Environment as Env
import Std.Errors
import Std.Hash as Hash
import Std.Http
import Std.String as Str
import Std.Task
import Std.Time as Time
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
pub record S3Config {
  bucket: String
  region: String
  accessKeyId: String
  secretAccessKey: String
  sessionToken: String
  endpoint: String
  pathStyle: Bool
  requestTimeoutMs: Int
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
  S3Config(
    bucket = cfg.bucket,
    region = cfg.region,
    accessKeyId = cfg.accessKeyId,
    secretAccessKey = cfg.secretAccessKey,
    sessionToken = sessionToken,
    endpoint = cfg.endpoint,
    pathStyle = cfg.pathStyle,
    requestTimeoutMs = cfg.requestTimeoutMs
  )
}

/// `cfg` addressing the bucket in the path (`true`) or the host name.
pub func S3Config.withPathStyle(cfg: in S3Config, pathStyle: in Bool): S3Config {
  S3Config(
    bucket = cfg.bucket,
    region = cfg.region,
    accessKeyId = cfg.accessKeyId,
    secretAccessKey = cfg.secretAccessKey,
    sessionToken = cfg.sessionToken,
    endpoint = cfg.endpoint,
    pathStyle = pathStyle,
    requestTimeoutMs = cfg.requestTimeoutMs
  )
}

/// `cfg` with a per-request deadline in milliseconds.
pub func S3Config.withRequestTimeout(cfg: in S3Config, requestTimeoutMs: in Int): S3Config {
  S3Config(
    bucket = cfg.bucket,
    region = cfg.region,
    accessKeyId = cfg.accessKeyId,
    secretAccessKey = cfg.secretAccessKey,
    sessionToken = cfg.sessionToken,
    endpoint = cfg.endpoint,
    pathStyle = cfg.pathStyle,
    requestTimeoutMs = requestTimeoutMs
  )
}

func envOr(name: in String, fallback: in String): String {
  val v = Env.getVarOrDefault(name, "")
  if v.length > 0 {
    v
  } else {
    fallback
  }
}

/// Configuration for `bucket` from the standard AWS environment variables:
/// `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, optional `AWS_SESSION_TOKEN`,
/// the region from `AWS_REGION` or `AWS_DEFAULT_REGION`, and an optional
/// endpoint from `AWS_ENDPOINT_URL_S3` or `AWS_ENDPOINT_URL` (path-style when
/// set). `Err(INVALID_CONFIG)` names the first variable that is missing.
pub func configFromEnv(bucket: in String): Result[S3Config, StorageError] {
  val accessKeyId = Env.getVarOrDefault("AWS_ACCESS_KEY_ID", "")
  if accessKeyId.length == 0 {
    return Err(StorageError(message = "AWS_ACCESS_KEY_ID is not set", code = "INVALID_CONFIG"))
  }
  val secretAccessKey = Env.getVarOrDefault("AWS_SECRET_ACCESS_KEY", "")
  if secretAccessKey.length == 0 {
    return Err(StorageError(message = "AWS_SECRET_ACCESS_KEY is not set", code = "INVALID_CONFIG"))
  }
  val region = envOr("AWS_REGION", Env.getVarOrDefault("AWS_DEFAULT_REGION", ""))
  if region.length == 0 {
    return Err(StorageError(message = "neither AWS_REGION nor AWS_DEFAULT_REGION is set", code = "INVALID_CONFIG"))
  }
  val endpoint = envOr("AWS_ENDPOINT_URL_S3", Env.getVarOrDefault("AWS_ENDPOINT_URL", ""))
  Ok(
    S3Config(
      bucket = bucket,
      region = region,
      accessKeyId = accessKeyId,
      secretAccessKey = secretAccessKey,
      sessionToken = Env.getVarOrDefault("AWS_SESSION_TOKEN", ""),
      endpoint = endpoint,
      pathStyle = endpoint.length > 0,
      requestTimeoutMs = DEFAULT_REQUEST_TIMEOUT_MS
    )
  )
}

/// A connection to one bucket. Build one with `openClient`; the fields are
/// derived from a validated `S3Config` and are not meant to be set by hand.
pub record S3Client {
  bucket: String
  region: String
  creds: Credentials
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

/// Validate `cfg` and open a client for its bucket. No request is made: a
/// wrong credential or a missing bucket shows up on the first operation.
///
/// `Err(INVALID_CONFIG)` for an invalid bucket or region name, empty
/// credentials, a malformed endpoint, a plain `http://` endpoint that is not
/// loopback, or a non-positive timeout.
pub func openClient(cfg: in S3Config): Result[S3Client, StorageError] {
  if not isValidBucketName(cfg.bucket) {
    return Err(invalidConfig("invalid S3 bucket name: '" + cfg.bucket + "'"))
  }
  if not isValidRegion(cfg.region) {
    return Err(invalidConfig("invalid AWS region: '" + cfg.region + "'"))
  }
  if cfg.accessKeyId.length == 0 or cfg.secretAccessKey.length == 0 {
    return Err(invalidConfig("S3 access key id and secret access key must be non-empty"))
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
  Ok(
    S3Client(
      bucket = cfg.bucket,
      region = cfg.region,
      creds = Credentials(
        accessKeyId = cfg.accessKeyId,
        secretAccessKey = cfg.secretAccessKey,
        sessionToken = cfg.sessionToken
      ),
      origin = endpoint.scheme + "://" + host,
      host = host,
      basePath = if cfg.pathStyle { "/" + cfg.bucket } else { "" },
      requestTimeoutMs = cfg.requestTimeoutMs,
      // A redirect (S3 answers 301/307 for a bucket in another region) would
      // be followed to a host the signature does not cover; it is returned
      // as the error it is instead.
      http = clientNoRedirects()
    )
  )
}

/// `yyyymmddThhmmssZ` for the current instant: the `x-amz-date` of a request
/// signed now.
pub func amzDateNow(): String {
  val iso = Time.toIsoString(Time.now())
  Str.replace(Str.replace(iso.substring(0, 19), "-", ""), ":", "") + "Z"
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

// SHA-256 of the empty string: the payload hash of a request with no body.
val EMPTY_PAYLOAD_SHA256: String = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

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

// Plain (non-`async`) so it may `await` in place and still wrap the send in
// `try` (V0012 forbids `await` inside `try` in an `async func`).
func sendCatching(client: in S3Client, req: in HttpRequest, url: in String): Result[HttpResponse, StorageError] {
  val src = makeCancelSourceTimeout(client.requestTimeoutMs)
  val tok = sourceToken(src)
  val sent = try {
    await client.http.sendWithCancel(req, tok)
  } catch Bug as bug {
    Err(error = ConnectionFailed(url = url, message = bug.message))
  }
  val timedOut = isCancelled(tok)
  disposeSource(src)
  match sent {
    case Ok(resp) -> Ok(resp)
    case Err(e) -> if timedOut {
      Err(transportError("S3 request timed out after " + client.requestTimeoutMs.toString() + " ms: " + url))
    } else {
      Err(transportError(HttpError.message(e)))
    }
  }
}

func readBody(resp: in HttpResponse): Result[slice[Byte], StorageError] {
  val read = try {
    await HttpResponse.bodyBytes(resp)
  } catch Bug as bug {
    return Err(transportError("cannot read S3 response body: " + bug.message))
  }
  match read {
    case Ok(bytes) -> Ok(bytes)
    case Err(e) -> Err(transportError("cannot read S3 response body: " + IOError.message(e)))
  }
}

/// Sign and send one request to the bucket.
///
/// `canonicalUri` is the encoded path (`objectPath`), `query` the unencoded
/// query parameters, `amzHeaders` extra `x-amz-*` headers to sign and send
/// (lowercase names), and `body` the payload, or `None` for a request without
/// one. Any HTTP response is `Ok`, whatever its status;
/// `Err` means no response arrived (`CONNECTION_FAILED`) or the request could
/// not be built (`INVALID_ARGUMENT`).
pub func S3Client.send(
  client: in S3Client,
  method: in HttpMethod,
  methodName: in String,
  canonicalUri: in String,
  query: in List[Pair],
  amzHeaders: in List[Pair],
  body: in Option[Payload],
): Result[S3Response, StorageError] {
  val payloadHash = match body {
    case Some(payload) -> Str.toLower(Hash.sha256OfBytes(payload.bytes))
    case None -> EMPTY_PAYLOAD_SHA256
  }
  val amzDate = amzDateNow()
  val signedHeaders: List[Pair] = newList()
  signedHeaders.add(Pair(name = "host", value = client.host))
  signedHeaders.add(Pair(name = "x-amz-content-sha256", value = payloadHash))
  signedHeaders.add(Pair(name = "x-amz-date", value = amzDate))
  if client.creds.sessionToken.length > 0 {
    signedHeaders.add(Pair(name = "x-amz-security-token", value = client.creds.sessionToken))
  }
  var i = 0
  while i < amzHeaders.count {
    signedHeaders.add(amzHeaders[i])
    i = i + 1
  }
  val signed = signRequest(
    methodName,
    canonicalUri,
    query,
    signedHeaders,
    payloadHash,
    client.creds,
    client.region,
    "s3",
    amzDate
  )
  val queryString = canonicalQuery(query)
  val url = client.origin + canonicalUri + (if queryString.length > 0 { "?" + queryString } else { "" })
  val parsed = match Url.tryFrom(url) {
    case Ok(u) -> u
    case Err(_) -> return Err(StorageError(message = "cannot build an S3 request URL: " + url, code = "INVALID_ARGUMENT"))
  }
  var req = request(method, parsed)
  match body {
    case Some(payload) -> {
      match withBytesBody(req, payload.contentType, payload.bytes) {
        case Ok(r) -> {
          req = r
        }
        case Err(e) -> return Err(StorageError(message = HttpError.message(e), code = "INVALID_ARGUMENT"))
      }
    }
    case None -> ()
  }
  // `Host` is set by the HTTP client from the URL; every other signed header
  // is sent as signed.
  var k = 1
  while k < signedHeaders.count {
    match withHeader(req, signedHeaders[k].name, signedHeaders[k].value) {
      case Ok(r) -> {
        req = r
      }
      case Err(e) -> return Err(StorageError(message = HttpError.message(e), code = "INVALID_ARGUMENT"))
    }
    k = k + 1
  }
  match withHeader(req, "Authorization", signed.authorization) {
    case Ok(r) -> {
      req = r
    }
    case Err(e) -> return Err(StorageError(message = HttpError.message(e), code = "INVALID_ARGUMENT"))
  }
  match sendCatching(client, req, url) {
    case Err(e) -> Err(e)
    case Ok(resp) -> {
      match readBody(resp) {
        case Err(e) -> Err(e)
        case Ok(bytes) -> Ok(S3Response(status = HttpResponse.statusCode(resp), body = bytes, response = resp))
      }
    }
  }
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
/// Computed locally; S3 is not contacted and the object need not exist.
@pure
pub func S3Client.presign(
  client: in S3Client,
  methodName: in String,
  key: in String,
  expiresInSeconds: in Int,
  amzDate: in String,
): String
  requires: expiresInSeconds >= 1 and expiresInSeconds <= 604800
  requires: amzDate.length == 16
{
  val path = client.objectPath(key)
  val none: List[Pair] = newList()
  client.origin + path + "?" +
    presignQuery(methodName, client.host, path, none, client.creds, client.region, "s3", amzDate, expiresInSeconds)
}
