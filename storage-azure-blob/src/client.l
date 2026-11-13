//! Storage.AzureBlob.Client - a signed Blob REST client over `Std.Http`.
//!
//! One `AzureBlobClient` talks to one container of one storage account. Every
//! request carries `x-ms-date` and `x-ms-version` and is authorized with
//! Shared Key (`Storage.AzureBlob.SharedKey`); there is no Azure SDK
//! underneath, so the same code runs on .NET and the JVM.
//!
//! Addressing follows the endpoint: Azure itself and custom domains put the
//! account in the host name (`https://<account>.blob.core.windows.net/
//! <container>/<blob>`), an emulator puts it first in the path
//! (`http://127.0.0.1:10000/<account>/<container>/<blob>`).
//!
//! Failures are `Storage.StorageError` values. `code` is the Blob service's
//! error code, from the `x-ms-error-code` response header or the `Error`
//! document (`ContainerNotFound`, `AuthenticationFailed`), except that
//! `BlobNotFound` and a 404 without a code are the interface's `NOT_FOUND`; a
//! response with no error code is `HTTP_<status>`, and a request that got no
//! response is `CONNECTION_FAILED`.
@runtime_checked
@io
package Storage.AzureBlob.Client

import Std.Core
import Std.Collections
import Std.Encoding as Enc
import Std.Errors
import Std.Http
import Std.String as Str
import Std.Task
import Std.Time as Time
import Storage
import Storage.AzureBlob.Config
import Storage.AzureBlob.SharedKey
import Storage.AzureBlob.Wire

/// A connection to one container. Build one with `openClient`; the fields are
/// derived from a validated `AzureBlobConfig` and are not meant to be set by
/// hand.
pub record AzureBlobClient {
  account: String
  container: String
  accountKey: String
  // `https://host[:port]`, no trailing slash.
  origin: String
  // `/<account>` for an emulator's path-style endpoint, else empty.
  basePath: String
  // True when `origin` is https: a SAS is then restricted to https.
  secure: Bool
  requestTimeoutMs: Int
  http: HttpClient
}

func invalidConfig(message: in String): StorageError {
  StorageError(message = message, code = "INVALID_CONFIG")
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
  (c >= 'a' and c <= 'z') or (c >= 'A' and c <= 'Z') or (c >= '0' and c <= '9') or c == '-' or c == '.' or c == ':' or
    c == '[' or
    c == ']'
}

// A validated endpoint, split.
record Endpoint {
  scheme: String
  authority: String
  // `/<account>` or empty.
  basePath: String
}

// An endpoint is `http(s)://host[:port]`, optionally followed by
// `/<account>` (the emulators' path style). The default port is dropped.
func parseEndpoint(raw: in String, account: in String): Result[Endpoint, StorageError] {
  var scheme = ""
  var rest = ""
  if Str.startsWith(raw, "https://") {
    scheme = "https"
    rest = raw.substring(8)
  } else if Str.startsWith(raw, "http://") {
    scheme = "http"
    rest = raw.substring(7)
  } else {
    return Err(invalidConfig("Blob endpoint must be an absolute http(s) URL: " + raw))
  }
  if Str.endsWith(rest, "/") {
    rest = rest.substring(0, rest.length - 1)
  }
  val slash = Str.indexOfRaw(rest, "/")
  val hostPart = if slash >= 0 { rest.substring(0, slash) } else { rest }
  val basePath = if slash >= 0 { rest.substring(slash) } else { "" }
  if hostPart.length == 0 {
    return Err(invalidConfig("Blob endpoint has no host: " + raw))
  }
  var i = 0
  while i < hostPart.length {
    if not isAuthorityChar(Str.charAtOrReplacement(hostPart, i)) {
      return Err(invalidConfig("Blob endpoint must be scheme://host[:port][/account] with no query or credentials: " + raw))
    }
    i = i + 1
  }
  if basePath.length > 0 and basePath != "/" + account {
    return Err(
      invalidConfig(
        "Blob endpoint path must be empty or the account name (/" + account + "), as an emulator addresses it: " +
          raw
      )
    )
  }
  var authority = Str.toLower(hostPart)
  if scheme == "https" and Str.endsWith(authority, ":443") {
    authority = authority.substring(0, authority.length - 4)
  } else if scheme == "http" and Str.endsWith(authority, ":80") {
    authority = authority.substring(0, authority.length - 3)
  }
  if scheme == "http" and not isLoopbackAuthority(authority) {
    return Err(
      invalidConfig(
        "Blob endpoint must use https:// (http:// is allowed only for localhost, 127.0.0.1 and [::1]): " + raw
      )
    )
  }
  Ok(Endpoint(scheme = scheme, authority = authority, basePath = basePath))
}

/// Validate `cfg` and open a client for its container. No request is made: a
/// wrong key or a missing container shows up on the first operation.
///
/// `Err(INVALID_CONFIG)` for an invalid account or container name, an
/// account key that is not base64, a malformed endpoint, a plain `http://`
/// endpoint that is not loopback, or a non-positive timeout.
pub func openClient(cfg: in AzureBlobConfig): Result[AzureBlobClient, StorageError] {
  if not isValidAccountName(cfg.accountName) {
    return Err(invalidConfig("invalid storage account name: '" + cfg.accountName + "'"))
  }
  if not isValidContainerName(cfg.container) {
    return Err(invalidConfig("invalid container name: '" + cfg.container + "'"))
  }
  if not isAccountKey(cfg.accountKey) {
    return Err(invalidConfig("the storage account key must be the base64 key Azure issues"))
  }
  if cfg.requestTimeoutMs <= 0 {
    return Err(invalidConfig("requestTimeoutMs must be > 0, got " + cfg.requestTimeoutMs.toString()))
  }
  val endpoint = if cfg.endpoint.length == 0 {
    Endpoint(scheme = "https", authority = cfg.accountName + ".blob.core.windows.net", basePath = "")
  } else {
    match parseEndpoint(cfg.endpoint, cfg.accountName) {
      case Err(e) -> return Err(e)
      case Ok(e) -> e
    }
  }
  Ok(
    AzureBlobClient(
      account = cfg.accountName,
      container = cfg.container,
      accountKey = cfg.accountKey,
      origin = endpoint.scheme + "://" + endpoint.authority,
      basePath = endpoint.basePath,
      secure = endpoint.scheme == "https",
      requestTimeoutMs = cfg.requestTimeoutMs,
      // A redirect would be followed to a host or path the signature does
      // not cover; it is returned as the error it is instead.
      http = clientNoRedirects()
    )
  )
}

/// Seconds since the Unix epoch, now.
pub func nowEpochSeconds(): Long {
  Time.nowEpochMillis() / 1000i64
}

/// The encoded path of the container (`/<container>`, or
/// `/<account>/<container>` for an emulator), as it is signed and requested.
@pure
pub func AzureBlobClient.containerPath(client: in AzureBlobClient): String {
  client.basePath + "/" + client.container
}

/// The encoded path of the blob `key` in this client's container: the name
/// percent-encoded once, `/` kept.
@pure
pub func AzureBlobClient.blobPath(client: in AzureBlobClient, key: in String): String {
  client.containerPath() + "/" + uriEncode(key, true)
}

/// True for a key this backend stores as a blob name: one
/// `Storage.isSafeKey` accepts, of at most 1024 characters (Azure's limit).
@pure
pub func isBlobName(key: in String): Bool {
  isSafeKey(key) and key.length <= 1024
}

/// A request body and the `Content-Type` it is sent with.
pub record Payload {
  contentType: String
  bytes: slice[Byte]
}

/// A response to a signed request: the status, the headers the caller needs,
/// and the body.
pub record BlobResponse {
  status: Int
  body: slice[Byte]
  response: HttpResponse
}

func transportError(message: in String): StorageError {
  StorageError(message = message, code = "CONNECTION_FAILED")
}

// Plain (non-`async`) so it may `await` in place and still wrap the send in
// `try` (V0012 forbids `await` inside `try` in an `async func`).
func sendCatching(client: in AzureBlobClient, req: in HttpRequest, url: in String): Result[HttpResponse, StorageError] {
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
      Err(transportError("Blob request timed out after " + client.requestTimeoutMs.toString() + " ms: " + url))
    } else {
      Err(transportError(HttpError.message(e)))
    }
  }
}

func readBody(resp: in HttpResponse): Result[slice[Byte], StorageError] {
  val read = try {
    await HttpResponse.bodyBytes(resp)
  } catch Bug as bug {
    return Err(transportError("cannot read Blob response body: " + bug.message))
  }
  match read {
    case Ok(bytes) -> Ok(bytes)
    case Err(e) -> Err(transportError("cannot read Blob response body: " + IOError.message(e)))
  }
}

func invalidArgument(message: in String): StorageError {
  StorageError(message = message, code = "INVALID_ARGUMENT")
}

/// Sign and send one request.
///
/// `encodedPath` is the path as requested (`containerPath`, `blobPath`),
/// `query` the unencoded query parameters, `msHeaders` extra `x-ms-*` headers
/// to sign and send (lowercase names; `x-ms-date` and `x-ms-version` are
/// added here), and `body` the payload, or `None` for a request without one.
/// Any HTTP response is `Ok`, whatever its status; `Err` means no response
/// arrived (`CONNECTION_FAILED`) or the request could not be built
/// (`INVALID_ARGUMENT`).
pub func AzureBlobClient.send(
  client: in AzureBlobClient,
  method: in HttpMethod,
  methodName: in String,
  encodedPath: in String,
  query: in List[Pair],
  msHeaders: in List[Pair],
  body: in Option[Payload],
): Result[BlobResponse, StorageError] {
  val sent: List[Pair] = newList()
  sent.add(Pair(name = "x-ms-date", value = httpDateOf(nowEpochSeconds())))
  sent.add(Pair(name = "x-ms-version", value = apiVersion()))
  var i = 0
  while i < msHeaders.count {
    sent.add(msHeaders[i])
    i = i + 1
  }
  // The standard headers the signature covers are the ones the body brings.
  val signedHeaders: List[Pair] = newList()
  var k = 0
  while k < sent.count {
    signedHeaders.add(sent[k])
    k = k + 1
  }
  match body {
    case Some(payload) -> {
      signedHeaders.add(Pair(name = "Content-Length", value = payload.bytes.length.toString()))
      signedHeaders.add(Pair(name = "Content-Type", value = payload.contentType))
    }
    case None -> ()
  }
  val toSign = stringToSign(methodName, signedHeaders, client.account, encodedPath, query)
  val signature = match signatureOf(client.accountKey, toSign) {
    case Some(s) -> s
    case None -> return Err(invalidConfig("the storage account key must be the base64 key Azure issues"))
  }
  val queryString = encodeQuery(query)
  val url = client.origin + encodedPath + (if queryString.length > 0 { "?" + queryString } else { "" })
  val parsed = match Url.tryFrom(url) {
    case Ok(u) -> u
    case Err(_) -> return Err(invalidArgument("cannot build a Blob request URL: " + url))
  }
  var req = request(method, parsed)
  match body {
    case Some(payload) -> {
      match withBytesBody(req, payload.contentType, payload.bytes) {
        case Ok(r) -> {
          req = r
        }
        case Err(e) -> return Err(invalidArgument(HttpError.message(e)))
      }
    }
    case None -> ()
  }
  var h = 0
  while h < sent.count {
    match withHeader(req, sent[h].name, sent[h].value) {
      case Ok(r) -> {
        req = r
      }
      case Err(e) -> return Err(invalidArgument(HttpError.message(e)))
    }
    h = h + 1
  }
  match withHeader(req, "Authorization", authorizationHeader(client.account, signature)) {
    case Ok(r) -> {
      req = r
    }
    case Err(e) -> return Err(invalidArgument(HttpError.message(e)))
  }
  match sendCatching(client, req, url) {
    case Err(e) -> Err(e)
    case Ok(resp) -> {
      match readBody(resp) {
        case Err(e) -> Err(e)
        case Ok(bytes) -> Ok(BlobResponse(status = HttpResponse.statusCode(resp), body = bytes, response = resp))
      }
    }
  }
}

func headerOr(resp: in BlobResponse, name: in String, fallback: in String): String {
  match HttpResponse.header(resp.response, name) {
    case Some(v) -> v
    case None -> fallback
  }
}

/// The Blob service's error code for a response: the `x-ms-error-code`
/// header, else the `Code` of the `Error` document in the body, else empty.
pub func errorCodeOf(resp: in BlobResponse): String {
  val fromHeader = Str.trim(headerOr(resp, "x-ms-error-code", ""))
  if fromHeader.length > 0 {
    return fromHeader
  }
  val text = match Enc.tryDecodeUtf8(resp.body) { case Some(t) -> t; case None -> "" }
  match parseFault(text) {
    case Some(fault) -> fault.code
    case None -> ""
  }
}

/// The `StorageError` for a response that is not the success the caller
/// wanted: the service's error code and message (`BlobNotFound` as
/// `NOT_FOUND`), `NOT_FOUND` for a 404 without a code, and `HTTP_<status>`
/// otherwise.
pub func faultOf(resp: in BlobResponse, what: in String): StorageError {
  val serviceCode = errorCodeOf(resp)
  val text = match Enc.tryDecodeUtf8(resp.body) { case Some(t) -> t; case None -> "" }
  val detail = match parseFault(text) { case Some(fault) -> fault.message; case None -> "" }
  if serviceCode.length > 0 {
    return StorageError(
      message = what + ": " + serviceCode + " (HTTP " + resp.status.toString() + ")" +
        (if detail.length > 0 { ": " + detail } else { "" }),
      code = if serviceCode == "BlobNotFound" { "NOT_FOUND" } else { serviceCode }
    )
  }
  StorageError(
    message = what + ": the Blob service answered HTTP " + resp.status.toString(),
    code = if resp.status == 404 { "NOT_FOUND" } else { "HTTP_" + resp.status.toString() }
  )
}

/// A blob's properties as the service reports them in response headers.
pub record BlobProperties {
  contentType: String
  contentLength: Long
  etag: String
  // ISO 8601 UTC.
  lastModified: String
  userMeta: Map[String, String]
  // `Hot`, `Cool`, `Cold`, `Archive`, or empty when the service reports none
  // (an emulator, or an account kind without tiers).
  accessTier: String
  // `BlockBlob`, `PageBlob` or `AppendBlob`.
  blobType: String
  // ISO 8601 UTC, or empty when the service reports none.
  createdAt: String
}

val META_PREFIX: String = "x-ms-meta-"

func isoOr(httpDate: in String, fallback: in String): String {
  match httpDateToIso(httpDate) {
    case Some(iso) -> iso
    case None -> fallback
  }
}

func propertiesOf(resp: in BlobResponse, contentLength: in Long): BlobProperties {
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
  val created = headerOr(resp, "x-ms-creation-time", "")
  BlobProperties(
    contentType = headerOr(resp, "Content-Type", "application/octet-stream"),
    contentLength = contentLength,
    etag = unquoteEtag(headerOr(resp, "ETag", "")),
    lastModified = isoOr(lastModified, lastModified),
    userMeta = userMeta,
    accessTier = headerOr(resp, "x-ms-access-tier", ""),
    blobType = headerOr(resp, "x-ms-blob-type", "BlockBlob"),
    createdAt = isoOr(created, created)
  )
}

func isMetaNameStart(c: in Char): Bool {
  (c >= 'a' and c <= 'z') or c == '_'
}

/// Why the Blob service cannot store this user metadata entry unchanged, or
/// `""` when it can. Metadata travels in `x-ms-meta-<name>` headers and a
/// name must be a valid C# identifier, which Azure enforces. Header names are
/// case-insensitive and are lowercased on the way (always, over HTTP/2), so a
/// name is accepted only as a lowercase letter or `_` followed by lowercase
/// letters, digits and `_`. A value must be printable ASCII with no leading or
/// trailing space, since a header cannot carry anything else intact.
@pure
pub func userMetaProblem(key: in String, value: in String): String {
  if key.length == 0 {
    return "user metadata name is empty"
  }
  var i = 0
  while i < key.length {
    val c = Str.charAtOrReplacement(key, i)
    val ok = if i == 0 { isMetaNameStart(c) } else { isMetaNameStart(c) or (c >= '0' and c <= '9') }
    if not ok {
      return "user metadata name '" + key +
        "' must be a lowercase letter or '_' followed by lowercase letters, digits or '_' (Azure requires a C# identifier, and header names lose their case)"
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

// A code unit's rank with `_` sorted below the digits, as a culture-aware
// comparison ranks it, instead of between the digits and the letters.
func underscoreFirstRank(unit: in Int): Int {
  if unit == 95 {
    47
  } else {
    unit
  }
}

func lessBy(a: in String, b: in String, underscoreFirst: in Bool): Bool {
  var i = 0
  while i < a.length and i < b.length {
    val x = if underscoreFirst { underscoreFirstRank(Str.codeUnitAt(a, i)) } else { Str.codeUnitAt(a, i) }
    val y = if underscoreFirst { underscoreFirstRank(Str.codeUnitAt(b, i)) } else { Str.codeUnitAt(b, i) }
    if x != y {
      return x < y
    }
    i = i + 1
  }
  a.length < b.length
}

/// Why this set of metadata names cannot be signed unambiguously, or `""`
/// when it can. The Shared Key signature covers the `x-ms-meta-*` headers
/// sorted by name, and implementations of the service do not agree on where
/// `_` sorts against a digit: byte order puts `a1` before `a_`, a
/// culture-aware comparison (which the Azurite emulator uses) puts `a_`
/// first. A set whose order depends on that would be signed correctly for
/// one and answered `AuthenticationFailed` by the other, so it is refused.
@pure
pub func userMetaOrderProblem(names: in List[String]): String {
  var i = 0
  while i < names.count {
    var j = i + 1
    while j < names.count {
      if lessBy(names[i], names[j], false) != lessBy(names[i], names[j], true) {
        return "user metadata names '" + names[i] + "' and '" + names[j] +
          "' sort differently by byte order and by culture ('_' against a digit), so the request cannot be signed for every implementation of the Blob service; rename one"
      }
      j = j + 1
    }
    i = i + 1
  }
  ""
}

/// What Put Blob reported.
pub record PutOutcome {
  etag: String
  // The size of the stored blob in bytes.
  contentLength: Long
  // The blob's `Last-Modified` as ISO 8601, or the local clock when the
  // service sent none.
  storedAt: String
}

func userMetaError(message: in String): StorageError {
  StorageError(message = message, code = "USER_META_UNSUPPORTED")
}

/// Put Blob: store `payload` under `key` as a block blob, replacing any blob
/// already there. `msHeaders` are extra signed `x-ms-*` headers (lowercase
/// names), such as `x-ms-access-tier`. `Err(USER_META_UNSUPPORTED)` for
/// metadata the service would reject or alter (`userMetaProblem`,
/// `userMetaOrderProblem`).
pub func AzureBlobClient.putBlob(
  client: in AzureBlobClient,
  key: in String,
  payload: in Payload,
  userMeta: in Map[String, String],
  msHeaders: in List[Pair],
): Result[PutOutcome, StorageError] {
  val headers: List[Pair] = newList()
  headers.add(Pair(name = "x-ms-blob-type", value = "BlockBlob"))
  val names: List[String] = newList()
  val entries = mapEntries(userMeta)
  for e in entries {
    val problem = userMetaProblem(e.key, e.value)
    if problem.length > 0 {
      return Err(userMetaError(problem))
    }
    names.add(e.key)
    headers.add(Pair(name = META_PREFIX + e.key, value = e.value))
  }
  val orderProblem = userMetaOrderProblem(names)
  if orderProblem.length > 0 {
    return Err(userMetaError(orderProblem))
  }
  var i = 0
  while i < msHeaders.count {
    headers.add(msHeaders[i])
    i = i + 1
  }
  val noQuery: List[Pair] = newList()
  match client.send(PUT, "PUT", client.blobPath(key), noQuery, headers, Some(value = payload)) {
    case Err(e) -> Err(e)
    case Ok(resp) -> {
      if resp.status != 201 {
        return Err(faultOf(resp, "cannot put blob '" + key + "'"))
      }
      Ok(
        PutOutcome(
          etag = unquoteEtag(headerOr(resp, "ETag", "")),
          contentLength = payload.bytes.length.toLong(),
          storedAt = isoOr(headerOr(resp, "Last-Modified", ""), Time.toIsoString(Time.now()))
        )
      )
    }
  }
}

/// Put Blob for a payload in the base64 form `StorageBucket.put` takes: the
/// blob stored is the decoded bytes. `Err(INVALID_BASE64)` when `dataBase64`
/// is not base64; nothing is sent then.
pub func AzureBlobClient.putBlobBase64(
  client: in AzureBlobClient,
  key: in String,
  dataBase64: in String,
  contentType: in String,
  userMeta: in Map[String, String],
): Result[PutOutcome, StorageError] {
  val noHeaders: List[Pair] = newList()
  match Enc.tryDecodeBase64(dataBase64) {
    case Some(decoded) -> client.putBlob(key, Payload(contentType = contentType, bytes = decoded), userMeta, noHeaders)
    case None -> Err(StorageError(message = "invalid base64 payload for '" + key + "'", code = "INVALID_BASE64"))
  }
}

/// A downloaded blob.
pub record GotBlob {
  bytes: slice[Byte]
  properties: BlobProperties
}

/// Get Blob: download `key`. `Err(NOT_FOUND)` when there is no such blob.
pub func AzureBlobClient.getBlob(client: in AzureBlobClient, key: in String): Result[GotBlob, StorageError] {
  val none: List[Pair] = newList()
  match client.send(GET, "GET", client.blobPath(key), none, none, None) {
    case Err(e) -> Err(e)
    case Ok(resp) -> {
      if resp.status != 200 {
        return Err(faultOf(resp, "cannot get blob '" + key + "'"))
      }
      Ok(GotBlob(bytes = resp.body, properties = propertiesOf(resp, resp.body.length.toLong())))
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

/// Get Blob Properties: `Some(properties)` when `key` exists, `None` when it
/// does not. No body is transferred. A container that does not exist is
/// `Err(ContainerNotFound)`, not `None`.
pub func AzureBlobClient.blobProperties(
  client: in AzureBlobClient, key: in String
): Result[Option[BlobProperties], StorageError] {
  val none: List[Pair] = newList()
  match client.send(HEAD, "HEAD", client.blobPath(key), none, none, None) {
    case Err(e) -> Err(e)
    case Ok(resp) -> {
      if resp.status == 200 {
        val length = match parseContentLength(headerOr(resp, "Content-Length", "0")) { case Some(n) -> n; case None -> 0i64 }
        return Ok(Some(value = propertiesOf(resp, length)))
      }
      val fault = faultOf(resp, "cannot get the properties of blob '" + key + "'")
      if fault.code == "NOT_FOUND" {
        return Ok(None)
      }
      Err(fault)
    }
  }
}

/// Delete Blob, with its snapshots. Deleting a key that does not exist
/// succeeds: the service answers `BlobNotFound`, which is the outcome the
/// caller asked for.
pub func AzureBlobClient.deleteBlob(client: in AzureBlobClient, key: in String): Result[Unit, StorageError] {
  val none: List[Pair] = newList()
  val headers: List[Pair] = newList()
  headers.add(Pair(name = "x-ms-delete-snapshots", value = "include"))
  match client.send(DELETE, "DELETE", client.blobPath(key), none, headers, None) {
    case Err(e) -> Err(e)
    case Ok(resp) -> {
      if resp.status == 202 or resp.status == 200 {
        return Ok(())
      }
      val fault = faultOf(resp, "cannot delete blob '" + key + "'")
      if fault.code == "NOT_FOUND" {
        return Ok(())
      }
      Err(fault)
    }
  }
}

/// List Blobs, flat: one page of at most `maxResults` blobs whose names begin
/// with `prefix`, in name order, continuing from `marker` (the `nextMarker`
/// of the page before) when it is non-empty.
pub func AzureBlobClient.listBlobs(
  client: in AzureBlobClient, prefix: in String, marker: in String, maxResults: in Int
): Result[ListPage, StorageError] {
  val query: List[Pair] = newList()
  query.add(Pair(name = "restype", value = "container"))
  query.add(Pair(name = "comp", value = "list"))
  query.add(Pair(name = "maxresults", value = maxResults.toString()))
  if prefix.length > 0 {
    query.add(Pair(name = "prefix", value = prefix))
  }
  if marker.length > 0 {
    query.add(Pair(name = "marker", value = marker))
  }
  val none: List[Pair] = newList()
  match client.send(GET, "GET", client.containerPath(), query, none, None) {
    case Err(e) -> Err(e)
    case Ok(resp) -> {
      if resp.status != 200 {
        return Err(faultOf(resp, "cannot list container '" + client.container + "'"))
      }
      val text = match Enc.tryDecodeUtf8(resp.body) {
        case Some(t) -> t
        case None -> return Err(StorageError(message = "Blob listing is not UTF-8", code = "LIST_ERROR"))
      }
      match parseListPage(text) {
        case Ok(page) -> Ok(page)
        case Err(message) -> Err(StorageError(message = message, code = "LIST_ERROR"))
      }
    }
  }
}

/// A service SAS URL for the blob `key`: whoever holds it may do what
/// `permissions` allows (`r` read, `cw` create and write, ...) until
/// `expiryEpochSeconds`, without credentials. Signed with the account key;
/// the service is not contacted and the blob need not exist. Over https the
/// SAS is restricted to https (`spr=https`). `Err(INVALID_ARGUMENT)` for
/// permissions `isSasPermissions` rejects.
@pure
pub func AzureBlobClient.sasUrl(
  client: in AzureBlobClient,
  key: in String,
  permissions: in String,
  expiryEpochSeconds: in Long,
): Result[String, StorageError]
  requires: expiryEpochSeconds >= 0i64
{
  if not isSasPermissions(permissions) {
    return Err(invalidArgument("not a selection of blob SAS permissions in the order racwdxytmei: '" + permissions + "'"))
  }
  val signed = sasQuery(
    permissions,
    isoSecondsOf(expiryEpochSeconds),
    client.account,
    client.container,
    key,
    if client.secure { "https" } else { "" },
    client.accountKey
  )
  match signed {
    case Some(query) -> Ok(client.origin + client.blobPath(key) + "?" + query)
    case None -> Err(invalidConfig("the storage account key must be the base64 key Azure issues"))
  }
}
