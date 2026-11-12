//! Storage.Gcs.Client - a signed Cloud Storage XML API client over
//! `Std.Http`.
//!
//! One `GcsClient` talks to one bucket. Every request is signed with an HMAC
//! key (`GOOG4-HMAC-SHA256`, `Storage.Gcs.Goog4`) and sent with `Std.Http`;
//! there is no Google SDK underneath, so the same code runs on .NET and the
//! JVM.
//!
//! Addressing is path-style unless the configuration says otherwise:
//! `https://storage.googleapis.com/<bucket>/<object>`, or
//! `<endpoint>/<bucket>/<object>` for another server.
//!
//! Failures are `Storage.StorageError` values. `code` is the Cloud Storage
//! error code from the response's `Error` document (`AccessDenied`,
//! `NoSuchBucket`, `SignatureDoesNotMatch`), except that `NoSuchKey` and a
//! bodyless 404 are the interface's `NOT_FOUND`; a response with no error
//! document is `HTTP_<status>`, and a request that got no response is
//! `CONNECTION_FAILED`.
@runtime_checked
@io
package Storage.Gcs.Client

import Std.Core
import Std.Collections
import Std.Encoding as Enc
import Std.Errors
import Std.Http
import Std.String as Str
import Std.Task
import Std.Time as Time
import Storage
pub use Storage.Gcs.Config
import Storage.Gcs.Goog4
import Storage.Gcs.Wire

/// A connection to one bucket. Build one with `openClient`; the fields are
/// derived from a validated `GcsConfig` and are not meant to be set by hand.
pub record GcsClient {
  bucket: String
  location: String
  key: HmacKey
  // `https://host[:port]`, no trailing slash.
  origin: String
  // The authority sent as `Host` and signed: `host[:port]`.
  host: String
  // `/<bucket>` when path-style, else empty.
  basePath: String
  requestTimeoutMs: Int
  // True for a client that sends its requests unsigned (`anonymousConfig`).
  anonymous: Bool
  http: HttpClient
}

func invalidConfig(message: in String): StorageError {
  StorageError(message = message, code = "INVALID_CONFIG")
}

/// Validate `cfg` and open a client for its bucket. No request is made: a
/// wrong HMAC key or a missing bucket shows up on the first operation.
///
/// `Err(INVALID_CONFIG)` for an invalid bucket name or location, an empty
/// access ID or secret (unless `cfg.anonymous`), a malformed endpoint, a
/// plain `http://` endpoint that is not loopback, or a non-positive timeout.
pub func openClient(cfg: in GcsConfig): Result[GcsClient, StorageError] {
  if not isValidBucketName(cfg.bucket) {
    return Err(invalidConfig("invalid Cloud Storage bucket name: '" + cfg.bucket + "'"))
  }
  if not isValidLocation(cfg.location) {
    return Err(invalidConfig("invalid Cloud Storage location: '" + cfg.location + "'"))
  }
  if not cfg.anonymous and (cfg.accessId.length == 0 or cfg.secret.length == 0) {
    return Err(invalidConfig("the HMAC key's access ID and secret must be non-empty"))
  }
  if cfg.requestTimeoutMs <= 0 {
    return Err(invalidConfig("requestTimeoutMs must be > 0, got " + cfg.requestTimeoutMs.toString()))
  }
  val endpoint = if cfg.endpoint.length == 0 {
    Endpoint(scheme = "https", authority = googleAuthority())
  } else {
    match parseEndpoint(cfg.endpoint) {
      case Err(e) -> return Err(e)
      case Ok(e) -> e
    }
  }
  val host = if cfg.pathStyle { endpoint.authority } else { cfg.bucket + "." + endpoint.authority }
  Ok(
    GcsClient(
      bucket = cfg.bucket,
      location = cfg.location,
      key = HmacKey(accessId = cfg.accessId, secret = cfg.secret),
      origin = endpoint.scheme + "://" + host,
      host = host,
      basePath = if cfg.pathStyle { "/" + cfg.bucket } else { "" },
      requestTimeoutMs = cfg.requestTimeoutMs,
      anonymous = cfg.anonymous,
      // A redirect would be followed to a host the signature does not cover;
      // it is returned as the error it is instead.
      http = clientNoRedirects()
    )
  )
}

/// `yyyymmddThhmmssZ` for the current instant: the `x-goog-date` of a request
/// signed now.
pub func googDateNow(): String {
  amzDateOfIso(Time.toIsoString(Time.now()))
}

/// The canonical resource path of `key` in this client's bucket
/// (`/<bucket>/<key>` or `/<key>`), as it is signed and requested.
@pure
pub func GcsClient.objectPath(client: in GcsClient, key: in String): String {
  client.basePath + canonicalKeyPath(key)
}

/// The canonical resource path of the bucket itself (`/<bucket>` or `/`).
@pure
pub func GcsClient.bucketPath(client: in GcsClient): String {
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

/// A response: the status, the body, the response itself for its headers,
/// and how the request was signed (`None` for an anonymous client). When
/// Cloud Storage answers `SignatureDoesNotMatch`, its error document reports
/// the canonical request and string to sign it derived
/// (`Storage.Gcs.Wire.parseSignatureMismatch`); `signed` holds the ones this
/// client used, to compare.
pub record GcsResponse {
  status: Int
  body: slice[Byte]
  response: HttpResponse
  signed: Option[Signed]
}

func transportError(message: in String): StorageError {
  StorageError(message = message, code = "CONNECTION_FAILED")
}

// Plain (non-`async`) so it may `await` in place and still wrap the send in
// `try` (V0012 forbids `await` inside `try` in an `async func`).
func sendCatching(client: in GcsClient, req: in HttpRequest, url: in String): Result[HttpResponse, StorageError] {
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
      Err(transportError("Cloud Storage request timed out after " + client.requestTimeoutMs.toString() + " ms: " + url))
    } else {
      Err(transportError(HttpError.message(e)))
    }
  }
}

func readBody(resp: in HttpResponse): Result[slice[Byte], StorageError] {
  val read = try {
    await HttpResponse.bodyBytes(resp)
  } catch Bug as bug {
    return Err(transportError("cannot read the Cloud Storage response body: " + bug.message))
  }
  match read {
    case Ok(bytes) -> Ok(bytes)
    case Err(e) -> Err(transportError("cannot read the Cloud Storage response body: " + IOError.message(e)))
  }
}

/// Sign and send one request to the bucket (an anonymous client sends it
/// unsigned).
///
/// `canonicalUri` is the encoded path (`objectPath`, `bucketPath`), `query`
/// the unencoded query parameters, `googHeaders` extra `x-goog-*` headers to
/// sign and send (lowercase names), and `body` the payload, or `None` for a
/// request without one. Any HTTP response is `Ok`, whatever its status; `Err`
/// means no response arrived (`CONNECTION_FAILED`) or the request could not
/// be built (`INVALID_ARGUMENT`).
pub func GcsClient.send(
  client: in GcsClient,
  method: in HttpMethod,
  methodName: in String,
  canonicalUri: in String,
  query: in List[Pair],
  googHeaders: in List[Pair],
  body: in Option[Payload],
): Result[GcsResponse, StorageError] {
  val signed: Option[SignedRequest] = if client.anonymous {
    None
  } else {
    val payloadHash = match body { case Some(payload) -> sha256Hex(payload.bytes); case None -> emptyPayloadSha256() }
    Some(
      value = googSignedHeaders(
        methodName,
        canonicalUri,
        query,
        client.host,
        googHeaders,
        payloadHash,
        client.key,
        client.location,
        googDateNow()
      )
    )
  }
  val queryString = canonicalQuery(query)
  val url = client.origin + canonicalUri + (if queryString.length > 0 { "?" + queryString } else { "" })
  val parsed = match Url.tryFrom(url) {
    case Ok(u) -> u
    case Err(_) -> return Err(StorageError(message = "cannot build a Cloud Storage request URL: " + url, code = "INVALID_ARGUMENT"))
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
  // is sent as signed, then `Authorization`. An anonymous request carries the
  // `x-goog-*` headers as they are.
  val toSend = match signed { case Some(s) -> s.headers; case None -> googHeaders }
  var k = 0
  while k < toSend.count {
    match withHeader(req, toSend[k].name, toSend[k].value) {
      case Ok(r) -> {
        req = r
      }
      case Err(e) -> return Err(StorageError(message = HttpError.message(e), code = "INVALID_ARGUMENT"))
    }
    k = k + 1
  }
  val how: Option[Signed] = match signed { case Some(s) -> Some(value = s.signed); case None -> None }
  match sendCatching(client, req, url) {
    case Err(e) -> Err(e)
    case Ok(resp) -> {
      match readBody(resp) {
        case Err(e) -> Err(e)
        case Ok(bytes) -> Ok(
          GcsResponse(status = HttpResponse.statusCode(resp), body = bytes, response = resp, signed = how)
        )
      }
    }
  }
}

/// The `StorageError` for a response that is not the success the caller
/// wanted: the Cloud Storage error code, message and details when the body
/// is an `Error` document (`NoSuchKey` as `NOT_FOUND`), `NOT_FOUND` for a
/// bodyless 404, and `HTTP_<status>` otherwise.
pub func faultOf(resp: in GcsResponse, what: in String): StorageError {
  val text = match Enc.tryDecodeUtf8(resp.body) { case Some(t) -> t; case None -> "" }
  match parseFault(text) {
    case Some(fault) -> {
      val code = if fault.code == "NoSuchKey" { "NOT_FOUND" } else { fault.code }
      StorageError(
        message = what + ": " + fault.code + " (HTTP " + resp.status.toString() + ")" +
          (if fault.message.length > 0 { ": " + fault.message } else { "" }) +
          (if fault.details.length > 0 { " " + fault.details } else { "" }),
        code = code
      )
    }
    case None -> {
      val code = if resp.status == 404 { "NOT_FOUND" } else { "HTTP_" + resp.status.toString() }
      StorageError(message = what + ": Cloud Storage answered HTTP " + resp.status.toString(), code = code)
    }
  }
}

func headerOr(resp: in GcsResponse, name: in String, fallback: in String): String {
  match HttpResponse.header(resp.response, name) {
    case Some(v) -> v
    case None -> fallback
  }
}

/// An object's metadata as Cloud Storage reports it in response headers.
/// `generation` is empty when the server reports none.
pub record ObjectHead {
  contentType: String
  contentLength: Long
  etag: String
  lastModified: String
  userMeta: Map[String, String]
  storageClass: String
  generation: String
}

val META_PREFIX: String = "x-goog-meta-"

func isoOr(httpDate: in String, fallback: in String): String {
  match httpDateToIso(httpDate) {
    case Some(iso) -> iso
    case None -> fallback
  }
}

func headOf(resp: in GcsResponse, contentLength: in Long): ObjectHead {
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
    storageClass = headerOr(resp, "x-goog-storage-class", "STANDARD"),
    generation = headerOr(resp, "x-goog-generation", "")
  )
}

func isMetaKeyChar(c: in Char): Bool {
  (c >= 'a' and c <= 'z') or (c >= '0' and c <= '9') or c == '-' or c == '_' or c == '.'
}

/// Why Cloud Storage cannot store this user metadata entry unchanged through
/// the XML API, or `""` when it can. User metadata travels in
/// `x-goog-meta-<key>` headers: header names are case-insensitive, so a key
/// is accepted only in lowercase letters, digits, `-`, `_` and `.`; and a
/// value must be printable ASCII with no leading or trailing space, since a
/// header cannot carry anything else intact.
@pure
pub func userMetaProblem(key: in String, value: in String): String {
  if key.length == 0 {
    return "user metadata key is empty"
  }
  var i = 0
  while i < key.length {
    if not isMetaKeyChar(Str.charAtOrReplacement(key, i)) {
      return "user metadata key '" + key +
        "' must be lowercase letters, digits, '-', '_' or '.' (header names are case-insensitive)"
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

/// What an object upload reported.
pub record PutOutcome {
  etag: String
  // The size of the stored object in bytes.
  contentLength: Long
  // The server's `Date` as ISO 8601, or the local clock when it sent none.
  storedAt: String
  // The generation Cloud Storage assigned, or empty when it reported none.
  generation: String
}

/// Upload `bytes` under `key` (PUT Object). `googHeaders` are extra signed
/// `x-goog-*` headers (lowercase names), such as `x-goog-storage-class`.
/// `Err(USER_META_UNSUPPORTED)` for metadata a header would alter
/// (`userMetaProblem`).
pub func GcsClient.putObject(
  client: in GcsClient,
  key: in String,
  bytes: in slice[Byte],
  contentType: in String,
  userMeta: in Map[String, String],
  googHeaders: in List[Pair],
): Result[PutOutcome, StorageError] {
  putPayload(client, key, Payload(contentType = contentType, bytes = bytes), userMeta, googHeaders)
}

/// PUT Object for a payload in the base64 form `StorageBucket.put` takes: the
/// object stored is the decoded bytes. `Err(INVALID_BASE64)` when
/// `dataBase64` is not base64; nothing is sent then.
pub func GcsClient.putObjectBase64(
  client: in GcsClient,
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
  client: in GcsClient,
  key: in String,
  payload: in Payload,
  userMeta: in Map[String, String],
  googHeaders: in List[Pair],
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
  while i < googHeaders.count {
    headers.add(googHeaders[i])
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
          storedAt = isoOr(headerOr(resp, "Date", ""), Time.toIsoString(Time.now())),
          generation = headerOr(resp, "x-goog-generation", "")
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

/// Download `key` (GET Object). `Err(NOT_FOUND)` when there is no such
/// object.
pub func GcsClient.getObject(client: in GcsClient, key: in String): Result[GotObject, StorageError] {
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

func parseLength(text: in String): Option[Long] {
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

/// HEAD Object: `Some(head)` when `key` exists, `None` when it does not. No
/// body is transferred. `contentLength` is the object's stored size
/// (`x-goog-stored-content-length`, else `Content-Length`).
pub func GcsClient.headObject(client: in GcsClient, key: in String): Result[Option[ObjectHead], StorageError] {
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
      val stored = match parseLength(headerOr(resp, "x-goog-stored-content-length", "")) {
        case Some(n) -> Some(value = n)
        case None -> parseLength(headerOr(resp, "Content-Length", "0"))
      }
      val length = match stored { case Some(n) -> n; case None -> 0i64 }
      Ok(Some(value = headOf(resp, length)))
    }
  }
}

/// DELETE Object. Deleting a key that does not exist succeeds: Cloud Storage
/// answers `NoSuchKey` for it, which is not an error for a delete.
pub func GcsClient.deleteObject(client: in GcsClient, key: in String): Result[Unit, StorageError] {
  val none: List[Pair] = newList()
  match client.send(DELETE, "DELETE", client.objectPath(key), none, none, None) {
    case Err(e) -> Err(e)
    case Ok(resp) -> {
      if resp.status == 204 or resp.status == 200 {
        return Ok(())
      }
      // Only an `Error` document that says the object is missing counts: a
      // 404 for a missing bucket (`NoSuchBucket`) stays an error.
      val text = match Enc.tryDecodeUtf8(resp.body) { case Some(t) -> t; case None -> "" }
      val missingObject = match parseFault(text) { case Some(f) -> f.code == "NoSuchKey"; case None -> false }
      if resp.status == 404 and missingObject {
        return Ok(())
      }
      Err(faultOf(resp, "cannot delete object '" + key + "'"))
    }
  }
}

/// One page of at most `maxKeys` object names beginning with `prefix`, in
/// name order, continuing after `continuationToken` when it is non-empty
/// (GET Bucket with `list-type=2`). Names are requested url-encoded, so a
/// name holding characters XML cannot carry is still listed.
pub func GcsClient.listObjects(
  client: in GcsClient, prefix: in String, continuationToken: in String, maxKeys: in Int
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
        case None -> return Err(StorageError(message = "Cloud Storage listing is not UTF-8", code = "LIST_ERROR"))
      }
      match parseListPage(text) {
        case Ok(page) -> Ok(page)
        case Err(message) -> Err(StorageError(message = message, code = "LIST_ERROR"))
      }
    }
  }
}

/// A V4 signed URL that performs `methodName` (`GET`, `PUT`) on `key` without
/// credentials until `expiresInSeconds` after `googDate` (`googDateNow()`).
/// Computed locally; Cloud Storage is not contacted and the object need not
/// exist. An anonymous client has no key to sign with.
@pure
pub func GcsClient.presign(
  client: in GcsClient,
  methodName: in String,
  key: in String,
  expiresInSeconds: in Int,
  googDate: in String,
): String
  requires: expiresInSeconds >= 1 and expiresInSeconds <= 604800
  requires: googDate.length == 16
  requires: not client.anonymous
{
  val path = client.objectPath(key)
  val none: List[Pair] = newList()
  client.origin + path + "?" +
    googPresignQuery(methodName, client.host, path, none, client.key, client.location, googDate, expiresInSeconds)
}
