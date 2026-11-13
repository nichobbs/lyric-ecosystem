//! tests/fake_blob_tests.l - the Azure Blob backend over real HTTP, against a
//! Blob service written in Lyric on `Std.HttpServer`.
//!
//! The fake service does what the Blob service does with a request, so
//! nothing here is answered from a canned response:
//!
//!   * it verifies the Shared Key signature of every request with the shared
//!     account key, rebuilding the string to sign from the method, path,
//!     query and headers it actually received, and refuses a request whose
//!     `x-ms-date` is more than 15 minutes off or whose `x-ms-version` is not
//!     the pinned one (`AuthenticationFailed`, `InvalidHeaderValue`);
//!   * a service SAS is verified the same way from its query string,
//!     including its expiry and the permission the method needs
//!     (`AuthenticationFailed`, `AuthorizationPermissionMismatch`);
//!   * containers must be created before use (`ContainerNotFound`); blobs live
//!     in memory with their content type, user metadata and access tier; List
//!     Blobs pages in name order with an opaque marker;
//!   * errors carry the `x-ms-error-code` header and an `Error` document, and
//!     a HEAD carries the header only, as the service does.
//!
//! `Testing.Conformance.Storage` runs against it end to end, then the
//! backend-specific behaviour: binary payloads, metadata, access tiers, error
//! mapping, SAS URLs fetched with a plain HTTP client, containers.
//!
//! The server runs on its own thread: a `Task.Run` worker on `--target dotnet`
//! (`spawn` is synchronous there), a `spawn` inside a `scope` on `--target
//! jvm`. No assertion runs while it is waiting for a request: a scenario
//! returns its findings and ends by asking the server to stop.
//!
//! FIXTURE KEY: `ACCOUNT_KEY` is base64 of a sentence saying so. It is not
//! the key of any storage account.
@test_module
package Storage.AzureBlob.FakeBlobTests

import Std.Core
import Std.Collections
import Std.Encoding as Enc
import Std.Errors
import Std.Hash as Hash
import Std.Http
import Std.HttpServer
import Std.String as Str
import Std.Testing
import Std.Time as Time
import Testing.Conformance
import Testing.Conformance.Storage as StorageConformance
import Storage.AzureBlob
import Storage.AzureBlob.Extras
import Storage.AzureBlob.SharedKey
import Storage.AzureBlob.Wire

// ─── The fake service's state ────────────────────────────────────────────────
val ACCOUNT: String = "fakeaccount"

val ACCOUNT_KEY: String = "ZmFrZS9zZWNyZXQra2V5L2Zvci10ZXN0cy1vbmx5LCBub3QgYSByZWFsIGFjY291bnQga2V5"

// Another well-formed key, for a client that signs with the wrong one:
// base64 of "the wrong key".
val WRONG_KEY: String = "dGhlIHdyb25nIGtleQ=="

val STOP_PATH: String = "/__stop"

// Every stored blob reports this modification time.
val LAST_MODIFIED_HTTP: String = "Thu, 12 Nov 2026 10:15:00 GMT"

val LAST_MODIFIED_ISO: String = "2026-11-12T10:15:00Z"

record StoredBlob {
  bytes: slice[Byte]
  contentType: String
  etag: String
  userMeta: Map[String, String]
  accessTier: String
}

ref record FakeBlobService {
  // Keyed `<container>/<name>`.
  var blobs: Map[String, StoredBlob]
  // Containers made with Create Container. No other container exists.
  var containers: Map[String, Bool]
  var requestCount: Int
}

func newFakeBlobService(): FakeBlobService {
  FakeBlobService(blobs = newMap(), containers = newMap(), requestCount = 0)
}

// ─── Request inspection ──────────────────────────────────────────────────────
func headerPairs(ctx: in HttpContext): List[Pair] {
  val acc: List[Pair] = newList()
  val entries = mapEntries(requestHeaders(ctx))
  for e in entries {
    acc.add(Pair(name = e.key, value = e.value))
  }
  acc
}

func headerValue(ctx: in HttpContext, name: in String): String {
  val wanted = Str.toLower(name)
  val entries = mapEntries(requestHeaders(ctx))
  var found = ""
  for e in entries {
    if Str.toLower(e.key) == wanted {
      found = e.value
    }
  }
  found
}

func queryPairs(rawQuery: in String): List[Pair] {
  val acc: List[Pair] = newList()
  if rawQuery.length == 0 {
    return acc
  }
  val parts = Str.split(rawQuery, "&")
  var i = 0
  while i < parts.length {
    val part = parts[i]
    val eq = Str.indexOfRaw(part, "=")
    if eq >= 0 {
      acc.add(Pair(name = urlDecode(part.substring(0, eq)), value = urlDecode(part.substring(eq + 1))))
    } else {
      acc.add(Pair(name = urlDecode(part), value = ""))
    }
    i = i + 1
  }
  acc
}

func queryValue(query: in List[Pair], name: in String): String {
  var i = 0
  while i < query.count {
    if query[i].name == name {
      return query[i].value
    }
    i = i + 1
  }
  ""
}

func intOf(text: in String): Int {
  var n = 0
  var i = 0
  while i < text.length {
    n = n * 10 + (Str.codeUnitAt(text, i) - 48)
    i = i + 1
  }
  n
}

// ─── Responses ───────────────────────────────────────────────────────────────
func xmlEscape(text: in String): String {
  Str.replace(Str.replace(Str.replace(text, "&", "&amp;"), "<", "&lt;"), ">", "&gt;")
}

func respondEmpty(ctx: in HttpContext, status: in Int, headers: in Map[String, String]): Unit {
  val empty: slice[Byte] = []
  respondBytesWithHeaders(ctx, status, "application/xml", empty, headers)
}

func faultBody(code: in String, message: in String): slice[Byte] {
  Enc.encodeUtf8(
    "<?xml version=\"1.0\" encoding=\"utf-8\"?><Error><Code>" + code + "</Code><Message>" + xmlEscape(message) +
      "\nRequestId:fake0001-0000-0000-0000-000000000000\nTime:2026-11-12T10:15:00.0000000Z</Message></Error>"
  )
}

func respondFault(ctx: in HttpContext, status: in Int, code: in String, message: in String): Unit {
  val headers: Map[String, String] = newMap()
  headers.add("x-ms-error-code", code)
  // As the service does, a HEAD is answered without a body: the error code
  // travels in the header only.
  if requestMethod(ctx) == "HEAD" {
    respondEmpty(ctx, status, headers)
    return
  }
  respondBytesWithHeaders(ctx, status, "application/xml", faultBody(code, message), headers)
}

// ─── Authorization ───────────────────────────────────────────────────────────
// The encoded path the service signs: what it received, each segment decoded
// and encoded again.
func canonicalPathOf(rawPath: in String): String {
  uriEncode(urlDecode(rawPath), true)
}

// Verify a request authorized with Shared Key. Returns "" when the signature
// is the one this service computes, otherwise the error code.
func verifySharedKey(ctx: in HttpContext, query: in List[Pair]): String {
  val authorization = headerValue(ctx, "Authorization")
  if not Str.startsWith(authorization, "SharedKey ") {
    return "AuthenticationFailed"
  }
  val credential = authorization.substring(10)
  val colon = Str.indexOfRaw(credential, ":")
  if colon < 0 or credential.substring(0, colon) != ACCOUNT {
    return "AuthenticationFailed"
  }
  val sentAt = match httpDateToEpochSeconds(headerValue(ctx, "x-ms-date")) {
    case Some(n) -> n
    case None -> return "AuthenticationFailed"
  }
  val now = Time.nowEpochMillis() / 1000i64
  if sentAt > now + 900i64 or sentAt < now - 900i64 {
    return "AuthenticationFailed"
  }
  if headerValue(ctx, "x-ms-version") != apiVersion() {
    return "InvalidHeaderValue"
  }
  val toSign = stringToSign(requestMethod(ctx), headerPairs(ctx), ACCOUNT, canonicalPathOf(requestPath(ctx)), query)
  match signatureOf(ACCOUNT_KEY, toSign) {
    case Some(expected) -> if expected == credential.substring(colon + 1) { "" } else { "AuthenticationFailed" }
    case None -> "AuthenticationFailed"
  }
}

// Verify a request authorized with a service SAS for one blob.
func verifySas(ctx: in HttpContext, query: in List[Pair], container: in String, name: in String): String {
  if queryValue(query, "sv") != apiVersion() or queryValue(query, "sr") != "b" or name.length == 0 {
    return "AuthenticationFailed"
  }
  val permissions = queryValue(query, "sp")
  val expiry = queryValue(query, "se")
  val protocol = queryValue(query, "spr")
  val toSign = sasStringToSign(
    permissions,
    queryValue(query, "st"),
    expiry,
    ACCOUNT,
    container,
    name,
    protocol,
    queryValue(query, "sv")
  )
  val expected = match signatureOf(ACCOUNT_KEY, toSign) { case Some(s) -> s; case None -> "" }
  if expected != queryValue(query, "sig") {
    return "AuthenticationFailed"
  }
  val expiresAt = match isoSecondsToEpoch(expiry) { case Some(n) -> n; case None -> return "AuthenticationFailed" }
  if Time.nowEpochMillis() / 1000i64 > expiresAt {
    return "AuthenticationFailed"
  }
  // This server speaks plain http, which a SAS restricted to https excludes.
  if protocol == "https" {
    return "AuthenticationFailed"
  }
  val method = requestMethod(ctx)
  val allowed = if method == "GET" or method == "HEAD" {
    Str.contains(permissions, "r")
  } else if method == "PUT" {
    Str.contains(permissions, "w") or Str.contains(permissions, "c")
  } else if method == "DELETE" {
    Str.contains(permissions, "d")
  } else {
    false
  }
  if allowed {
    ""
  } else {
    "AuthorizationPermissionMismatch"
  }
}

// ─── Blob operations ─────────────────────────────────────────────────────────
func ordinalLess(a: in String, b: in String): Bool {
  var i = 0
  while i < a.length and i < b.length {
    val x = Str.codeUnitAt(a, i)
    val y = Str.codeUnitAt(b, i)
    if x != y {
      return x < y
    }
    i = i + 1
  }
  a.length < b.length
}

func sortedNamesOf(service: in FakeBlobService, container: in String, prefix: in String): List[String] {
  val acc: List[String] = newList()
  val all = mapKeys(service.blobs)
  val wanted = container + "/" + prefix
  var i = 0
  while i < all.count {
    val full: String = all[i]
    if Str.startsWith(full, wanted) {
      acc.add(full.substring(container.length + 1))
      var j = acc.count - 1
      while j > 0 and ordinalLess(acc[j], acc[j - 1]) {
        val tmp = acc[j]
        acc[j] = acc[j - 1]
        acc[j - 1] = tmp
        j = j - 1
      }
    }
    i = i + 1
  }
  acc
}

func listBlobsFake(service: in FakeBlobService, ctx: in HttpContext, container: in String, query: in List[Pair]): Unit {
  val prefix = queryValue(query, "prefix")
  val maxText = queryValue(query, "maxresults")
  val maxResults = if maxText.length == 0 { 5000 } else { intOf(maxText) }
  val marker = queryValue(query, "marker")
  // The marker is opaque to clients: the hex of the last name of the page
  // before.
  var after = ""
  if marker.length > 0 {
    val decoded = match Enc.tryDecodeHex(marker) { case Some(bytes) -> Enc.tryDecodeUtf8(bytes); case None -> None }
    match decoded {
      case Some(name) -> {
        after = name
      }
      case None -> {
        respondFault(ctx, 400, "OutOfRangeInput", "One of the request inputs is out of range.")
        return
      }
    }
  }
  val names = sortedNamesOf(service, container, prefix)
  val sb = Str.StringBuilder.new()
  sb.append("\u{feff}<?xml version=\"1.0\" encoding=\"utf-8\"?>")
  sb.append(
    "<EnumerationResults ServiceEndpoint=\"http://127.0.0.1/" + ACCOUNT + "\" ContainerName=\"" + container + "\">"
  )
  sb.append("<Prefix>" + xmlEscape(prefix) + "</Prefix>")
  sb.append("<MaxResults>" + maxResults.toString() + "</MaxResults>")
  sb.append("<Blobs>")
  var written = 0
  var last = ""
  var more = false
  var i = 0
  while i < names.count {
    val name = names[i]
    if after.length == 0 or ordinalLess(after, name) {
      if written == maxResults {
        more = true
      } else {
        match mapGet(service.blobs, container + "/" + name) {
          case Some(blob) -> {
            sb.append("<Blob><Name>" + xmlEscape(name) + "</Name><Properties>")
            sb.append("<Creation-Time>" + LAST_MODIFIED_HTTP + "</Creation-Time>")
            sb.append("<Last-Modified>" + LAST_MODIFIED_HTTP + "</Last-Modified>")
            sb.append("<Etag>" + blob.etag + "</Etag>")
            sb.append("<Content-Length>" + blob.bytes.length.toString() + "</Content-Length>")
            sb.append("<Content-Type>" + xmlEscape(blob.contentType) + "</Content-Type>")
            sb.append("<Content-Encoding /><Content-Language /><Content-MD5 /><Cache-Control />")
            sb.append("<BlobType>BlockBlob</BlobType><AccessTier>" + blob.accessTier + "</AccessTier>")
            sb.append("<LeaseStatus>unlocked</LeaseStatus><LeaseState>available</LeaseState>")
            sb.append("<ServerEncrypted>true</ServerEncrypted></Properties></Blob>")
            written = written + 1
            last = name
          }
          case None -> ()
        }
      }
    }
    i = i + 1
  }
  sb.append("</Blobs>")
  if more {
    sb.append("<NextMarker>" + Str.toLower(Enc.encodeHex(Enc.encodeUtf8(last))) + "</NextMarker>")
  } else {
    sb.append("<NextMarker />")
  }
  sb.append("</EnumerationResults>")
  val none: Map[String, String] = newMap()
  respondBytesWithHeaders(ctx, 200, "application/xml", Enc.encodeUtf8(sb.toString()), none)
}

func isTier(name: in String): Bool {
  name == "Hot" or name == "Cool" or name == "Cold" or name == "Archive"
}

func blobHeaders(blob: in StoredBlob): Map[String, String] {
  val headers: Map[String, String] = newMap()
  headers.add("ETag", "\"" + blob.etag + "\"")
  headers.add("Last-Modified", LAST_MODIFIED_HTTP)
  headers.add("x-ms-creation-time", LAST_MODIFIED_HTTP)
  headers.add("x-ms-blob-type", "BlockBlob")
  headers.add("x-ms-access-tier", blob.accessTier)
  val entries = mapEntries(blob.userMeta)
  for e in entries {
    headers.add("x-ms-meta-" + e.key, e.value)
  }
  headers
}

func putBlobFake(service: in FakeBlobService, ctx: in HttpContext, fullName: in String, body: in slice[Byte]): Unit {
  if headerValue(ctx, "x-ms-blob-type") != "BlockBlob" {
    respondFault(ctx, 400, "MissingRequiredHeader", "An HTTP header that's mandatory for this request is not specified.")
    return
  }
  val tier = headerValue(ctx, "x-ms-access-tier")
  if tier.length > 0 and not isTier(tier) {
    respondFault(ctx, 400, "InvalidHeaderValue", "The value for one of the HTTP headers is not in the correct format.")
    return
  }
  val userMeta: Map[String, String] = newMap()
  val entries = mapEntries(requestHeaders(ctx))
  for e in entries {
    val lower = Str.toLower(e.key)
    if Str.startsWith(lower, "x-ms-meta-") {
      userMeta[lower.substring(10)] = e.value
    }
  }
  val contentType = headerValue(ctx, "Content-Type")
  val etag = "0x" + Hash.sha256OfBytes(body).substring(0, 15)
  service.blobs[fullName] = StoredBlob(
    bytes = body,
    contentType = if contentType.length > 0 {
      contentType
    } else {
      "application/octet-stream"
    },
    etag = etag,
    userMeta = userMeta,
    accessTier = if tier.length > 0 { tier } else { "Hot" }
  )
  val headers: Map[String, String] = newMap()
  headers.add("ETag", "\"" + etag + "\"")
  headers.add("Last-Modified", LAST_MODIFIED_HTTP)
  respondEmpty(ctx, 201, headers)
}

func setTierFake(service: in FakeBlobService, ctx: in HttpContext, fullName: in String): Unit {
  val tier = headerValue(ctx, "x-ms-access-tier")
  if not isTier(tier) {
    respondFault(ctx, 400, "InvalidHeaderValue", "The value for one of the HTTP headers is not in the correct format.")
    return
  }
  match mapGet(service.blobs, fullName) {
    case None -> respondFault(ctx, 404, "BlobNotFound", "The specified blob does not exist.")
    case Some(blob) -> {
      service.blobs[fullName] = StoredBlob(
        bytes = blob.bytes,
        contentType = blob.contentType,
        etag = blob.etag,
        userMeta = blob.userMeta,
        accessTier = tier
      )
      val none: Map[String, String] = newMap()
      respondEmpty(ctx, 200, none)
    }
  }
}

// Names that make the service misbehave on purpose. True when one answered.
func misbehave(ctx: in HttpContext, name: in String): Bool {
  val none: Map[String, String] = newMap()
  if name == "boom/internal" {
    respondFault(ctx, 500, "InternalError", "The server encountered an internal error. Please retry the request.")
    return true
  }
  if name == "boom/gateway" {
    respondBytesWithHeaders(ctx, 502, "text/html", Enc.encodeUtf8("<html><body>502 Bad Gateway</body></html>"), none)
    return true
  }
  if name == "boom/moved" {
    val headers: Map[String, String] = newMap()
    headers.add("Location", "http://127.0.0.1:1/elsewhere")
    respondEmpty(ctx, 307, headers)
    return true
  }
  if name == "boom/body-only" {
    // An error whose code is in the document but not in a header.
    respondBytesWithHeaders(
      ctx,
      409,
      "application/xml",
      faultBody("LeaseIdMissing", "There is currently a lease on the blob."),
      none
    )
    return true
  }
  if name == "boom/header-only" {
    // An error whose code is in the header but that has no document.
    val headers: Map[String, String] = newMap()
    headers.add("x-ms-error-code", "ServerBusy")
    respondEmpty(ctx, 503, headers)
    return true
  }
  if name == "boom/bare-404" {
    respondEmpty(ctx, 404, none)
    return true
  }
  false
}

func serveBlob(
  service: in FakeBlobService,
  ctx: in HttpContext,
  container: in String,
  name: in String,
  query: in List[Pair],
  body: in slice[Byte],
): Unit {
  val method = requestMethod(ctx)
  val fullName = container + "/" + name
  if misbehave(ctx, name) {
    return
  }
  if method == "PUT" {
    if queryValue(query, "comp") == "tier" {
      setTierFake(service, ctx, fullName)
    } else {
      putBlobFake(service, ctx, fullName, body)
    }
  } else if method == "DELETE" {
    if service.blobs.containsKey(fullName) {
      val _ = service.blobs.remove(fullName)
      val none: Map[String, String] = newMap()
      respondEmpty(ctx, 202, none)
    } else {
      respondFault(ctx, 404, "BlobNotFound", "The specified blob does not exist.")
    }
  } else if method == "GET" or method == "HEAD" {
    match mapGet(service.blobs, fullName) {
      case None -> {
        respondFault(ctx, 404, "BlobNotFound", "The specified blob does not exist.")
      }
      case Some(blob) -> {
        // `Std.HttpServer` cannot answer a HEAD with an entity's length
        // (nichobbs/lyric-lang#9048), so a HEAD reports Content-Length 0.
        val empty: slice[Byte] = []
        respondBytesWithHeaders(
          ctx,
          200,
          blob.contentType,
          if method == "HEAD" { empty } else { blob.bytes },
          blobHeaders(blob)
        )
      }
    }
  } else {
    respondFault(ctx, 405, "UnsupportedHttpVerb", "The resource doesn't support the specified HTTP verb.")
  }
}

func serveContainer(service: in FakeBlobService, ctx: in HttpContext, container: in String, query: in List[Pair]): Unit {
  val method = requestMethod(ctx)
  val none: Map[String, String] = newMap()
  if queryValue(query, "restype") != "container" {
    respondFault(ctx, 400, "InvalidQueryParameterValue", "Value for one of the query parameters is invalid.")
  } else if method == "PUT" {
    if service.containers.containsKey(container) {
      respondFault(ctx, 409, "ContainerAlreadyExists", "The specified container already exists.")
    } else {
      service.containers.add(container, true)
      respondEmpty(ctx, 201, none)
    }
  } else if not service.containers.containsKey(container) {
    respondFault(ctx, 404, "ContainerNotFound", "The specified container does not exist.")
  } else if method == "GET" and queryValue(query, "comp") == "list" {
    listBlobsFake(service, ctx, container, query)
  } else if method == "GET" or method == "HEAD" {
    respondEmpty(ctx, 200, none)
  } else {
    respondFault(ctx, 405, "UnsupportedHttpVerb", "The resource doesn't support the specified HTTP verb.")
  }
}

func handle(service: in FakeBlobService, ctx: in HttpContext): Unit {
  service.requestCount = service.requestCount + 1
  val body = requestBodyBytes(ctx)
  val query = queryPairs(requestQuery(ctx))
  // Path-style, as an emulator: /<account>/<container>[/<blob>].
  val path = urlDecode(requestPath(ctx))
  val accountPrefix = "/" + ACCOUNT + "/"
  val inAccount = Str.startsWith(path, accountPrefix)
  val rest = if inAccount { path.substring(accountPrefix.length) } else { "" }
  val slash = Str.indexOfRaw(rest, "/")
  val container = if slash >= 0 { rest.substring(0, slash) } else { rest }
  val name = if slash >= 0 { rest.substring(slash + 1) } else { "" }
  val problem = if queryValue(query, "sig").length > 0 {
    verifySas(ctx, query, container, name)
  } else {
    verifySharedKey(ctx, query)
  }
  if problem.length > 0 {
    respondFault(
      ctx,
      if problem == "InvalidHeaderValue" { 400 } else { 403 },
      problem,
      "Server failed to authenticate the request. Make sure the value of Authorization header is formed correctly including the signature."
    )
    return
  }
  if not inAccount or container.length == 0 {
    respondFault(ctx, 400, "InvalidUri", "The requested URI does not represent any resource on the server.")
    return
  }
  if name.length == 0 {
    serveContainer(service, ctx, container, query)
    return
  }
  if not service.containers.containsKey(container) {
    respondFault(ctx, 404, "ContainerNotFound", "The specified container does not exist.")
    return
  }
  serveBlob(service, ctx, container, name, query, body)
}

func serveUntilStopped(l: in HttpListener): Unit {
  val service = newFakeBlobService()
  var running = true
  while running {
    val ctx = nextContext(l)
    if requestPath(ctx) == STOP_PATH {
      respondText(ctx, 200, "served " + service.requestCount.toString())
      running = false
    } else {
      // A bug in the fake must surface as a failed request, not as a server
      // thread that died and a test run that waits for it.
      try {
        handle(service, ctx)
      } catch Bug as b {
        respondFault(ctx, 500, "FakeServerBug", b.message)
      }
    }
  }
}

// ─── Running a scenario against the server ───────────────────────────────────
@cfg(target = "dotnet")
extern type Task = "System.Threading.Tasks.Task"

@cfg(target = "dotnet")
@externStatic
@externTarget("System.Threading.Tasks.Task.Run")
func taskRun(work: in () -> Unit): Task = ()

@cfg(target = "dotnet")
@externInstance
@externTarget("System.Threading.Tasks.Task.Wait")
func taskWait(t: in Task): Unit = ()

func originOf(port: in Int): String {
  "http://127.0.0.1:" + port.toString()
}

@cfg(target = "dotnet")
func withFakeService(port: in Int, scenario: in (String) -> String): String {
  val l = startListener(originOf(port) + "/")
  val server = taskRun({ -> serveUntilStopped(l) })
  val transcript = scenario(originOf(port))
  taskWait(server)
  stopListener(l)
  transcript
}

@cfg(target = "jvm")
func withFakeService(port: in Int, scenario: in (String) -> String): String {
  val l = startListener(originOf(port) + "/")
  var transcript = ""
  scope {
    val server = spawn serveUntilStopped(l)
    transcript = scenario(originOf(port))
    await server
  }
  stopListener(l)
  transcript
}

// Asks the server to stop; the last thing every scenario does. Returns what
// the server said: how many Blob requests it served.
func stopFake(origin: in String): String {
  match await defaultClient().get(origin + STOP_PATH) {
    case Err(e) -> "stop failed: " + HttpError.message(e)
    case Ok(resp) -> match await HttpResponse.bodyText(resp) { case Ok(text) -> text; case Err(_) -> "stop unreadable" }
  }
}

func configFor(origin: in String, container: in String): AzureBlobConfig {
  endpointConfig(origin + "/" + ACCOUNT, ACCOUNT, ACCOUNT_KEY, container).withRequestTimeout(10000)
}

func clientFor(origin: in String, container: in String): AzureBlobClient {
  match openClient(configFor(origin, container)) {
    case Ok(c) -> c
    case Err(e) -> panic("openClient failed: " + e.code + " " + e.message)
  }
}

// A client for a container that exists: created through the backend itself.
func readyClient(origin: in String, container: in String): AzureBlobClient {
  val client = clientFor(origin, container)
  match createContainer(client) {
    case Ok(_) -> client
    case Err(e) -> panic("createContainer failed: " + e.code + " " + e.message)
  }
}

// ─── Conformance ─────────────────────────────────────────────────────────────
ref record ContainerCounter {
  var next: Int
}

// A fresh, empty bucket per conformance case: a new container each time.
func freshBucket(origin: in String, counter: in ContainerCounter): StorageBucket {
  counter.next = counter.next + 1
  bucketOf(readyClient(origin, "conformance-" + counter.next.toString()))
}

// Runs the suite and returns its summary; the tally itself is left in `into`
// for the test to check once the server has stopped.
func conformanceScenario(origin: in String, into: in List[Checks]): String {
  val counter = ContainerCounter(next = 0)
  val checks = StorageConformance.run({ -> freshBucket(origin, counter) })
  into.add(checks)
  summary(checks) + "\n" + stopFake(origin)
}

test "AzureBlobBucket conforms to StorageBucket, over HTTP against the signature-checking fake" {
  val ran: List[Checks] = newList()
  val lines = Str.split(withFakeService(19611, { origin -> conformanceScenario(origin, ran) }), "\n")
  assertEqualInt(ran.count, 1, "the suite ran: " + Str.join(" | ", lines))
  requireConformance(ran[0])
  assertEqualInt(caseCount(ran[0]), 14, "every conformance case ran")
  assertEqualInt(lines.length, 2, "summary and server report: " + Str.join(" | ", lines))
  assertTrue(lines[1].startsWith("served "), "the fake server stopped cleanly: " + lines[1])
  // 14 cases over HTTP: far more requests than cases, so the suite did not
  // pass by never reaching the server.
  assertTrue(lines[1] != "served 0" and lines[1].length >= 9, "the suite's requests reached the server: " + lines[1])
}

// ─── Backend behaviour ───────────────────────────────────────────────────────
func codeOf(r: in Result[String, StorageError]): String {
  match r {
    case Ok(v) -> "ok:" + v
    case Err(e) -> e.code
  }
}

func unitCode(r: in Result[Unit, StorageError]): String {
  match r {
    case Ok(_) -> "ok"
    case Err(e) -> e.code
  }
}

func boolCode(r: in Result[Bool, StorageError]): String {
  match r {
    case Ok(b) -> b.toString()
    case Err(e) -> e.code
  }
}

func everyByte(): slice[Byte] {
  val acc: List[Byte] = newList()
  var i = 0
  while i < 256 {
    acc.add(i.toByte())
    i = i + 1
  }
  acc.toArray()
}

func fakeEtag(bytes: in slice[Byte]): String {
  "0x" + Hash.sha256OfBytes(bytes).substring(0, 15)
}

func binaryScenario(origin: in String): String {
  val client = readyClient(origin, "binary")
  val bucket = bucketOf(client)
  val all = everyByte()
  val noMeta: Map[String, String] = newMap()
  var text = ""
  // Through the interface: base64 in, base64 out, bytes on the wire.
  match bucket.put("bin/all.bin", Enc.encodeBase64(all), "application/octet-stream", noMeta) {
    case Err(e) -> {
      text = text + "put=" + e.code + " " + e.message
    }
    case Ok(meta) -> {
      text = text + "put=" + meta.contentLength.toString() + "/" + meta.etag + "/" + meta.lastModified
    }
  }
  match bucket.get("bin/all.bin") {
    case Err(e) -> {
      text = text + "\nget=" + e.code + " " + e.message
    }
    case Ok(obj) -> {
      text = text + "\nget=" + (if obj.dataBase64 == Enc.encodeBase64(all) { "same" } else { "different" }) + "/" +
        obj.metadata.contentLength.toString() +
        "/" +
        obj.metadata.contentType +
        "/" +
        obj.metadata.etag +
        "/" +
        obj.metadata.lastModified +
        "/" +
        obj.bucket
    }
  }
  // An empty blob: a PUT whose Content-Length is 0, signed as the empty string.
  match bucket.put("bin/empty", "", "application/x-empty", noMeta) {
    case Err(e) -> {
      text = text + "\nempty=" + e.code + " " + e.message
    }
    case Ok(meta) -> match bucket.get("bin/empty") {
      case Err(e) -> {
        text = text + "\nempty=" + e.code
      }
      case Ok(obj) -> {
        text = text + "\nempty=" + meta.contentLength.toString() + "/" + obj.dataBase64.length.toString() + "/" +
          obj.metadata.contentType
      }
    }
  }
  // Raw bytes through the extras, with a name that needs encoding.
  val oddKey = "odd/sp ace+plus&amp=eqé/(1)*'~!$,;@<>.txt"
  text = text + "\nodd=" + codeOf(putBytes(client, oddKey, [1u8, 2u8, 3u8], "text/plain; charset=utf-8", noMeta))
  match getBytes(client, oddKey) {
    case Err(e) -> {
      text = text + "\noddGet=" + e.code + " " + e.message
    }
    case Ok(got) -> {
      text = text + "\noddGet=" + Str.toLower(Enc.encodeHex(got.bytes)) + "/" + got.properties.contentType
    }
  }
  match bucket.list("odd/", "", 10) {
    case Err(e) -> {
      text = text + "\noddList=" + e.code + " " + e.message
    }
    case Ok(r) -> {
      text = text + "\noddList=" + r.entries.length.toString() + ":" +
        (if r.entries.length == 1 and r.entries[0].key == oddKey { "same name" } else { "different name" }) +
        ":" +
        (
          if r.entries.length == 1 {
            r.entries[0].contentLength.toString() + ":" + r.entries[0].etag + ":" + r.entries[0].lastModified
          } else {
            ""
          }
        )
    }
  }
  text = text + "\nbadBase64=" +
    (match bucket.put("k", "not base64!!", "text/plain", noMeta) { case Ok(_) -> "ok"; case Err(e) -> e.code })
  var long = ""
  var i = 0
  while i < 205 {
    long = long + "12345"
    i = i + 1
  }
  text = text + "\ntooLong=" +
    (match bucket.put(long, "", "text/plain", noMeta) { case Ok(_) -> "ok"; case Err(e) -> e.code })
  text + "\n" + stopFake(origin)
}

test "binary payloads, empty blobs and names that need encoding survive the round trip" {
  val lines = Str.split(withFakeService(19612, binaryScenario), "\n")
  assertEqualInt(lines.length, 9, "nine lines: " + Str.join(" | ", lines))
  val etag = fakeEtag(everyByte())
  assertEqual(
    lines[0],
    "put=256/" + etag + "/" + LAST_MODIFIED_ISO,
    "put reports the decoded size, the service's ETag unquoted and its Last-Modified as ISO 8601"
  )
  assertEqual(
    lines[1],
    "get=same/256/application/octet-stream/" + etag + "/" + LAST_MODIFIED_ISO + "/binary",
    "get returns the bytes, and the container as the bucket"
  )
  assertEqual(lines[2], "empty=0/0/application/x-empty", "a zero-length blob")
  assertEqual(
    lines[3],
    "odd=ok:" + fakeEtag([1u8, 2u8, 3u8]),
    "a name with spaces, '+', '&', quotes, angle brackets and non-ASCII"
  )
  assertEqual(lines[4], "oddGet=010203/text/plain; charset=utf-8", "the encoded name is the same blob on get")
  assertEqual(
    lines[5],
    "oddList=1:same name:3:" + fakeEtag([1u8, 2u8, 3u8]) + ":" + LAST_MODIFIED_ISO,
    "the listing returns the name, the size, the ETag and an ISO 8601 date"
  )
  assertEqual(lines[6], "badBase64=INVALID_BASE64", "a payload that is not base64 never reaches the service")
  assertEqual(lines[7], "tooLong=INVALID_KEY", "a name over Azure's 1024 characters is refused locally")
  assertEqual(lines[8], "served 8", "exactly the expected requests reached the server")
}

func statText(r: in Result[Option[BlobProperties], StorageError]): String {
  match r {
    case Err(e) -> e.code
    case Ok(None) -> "absent"
    case Ok(Some(p)) -> {
      val owner = match mapGet(p.userMeta, "owner") { case Some(v) -> v; case None -> "-" }
      val note = match mapGet(p.userMeta, "_build_note1") { case Some(v) -> v; case None -> "-" }
      p.contentType + "/" + p.etag.length.toString() + "/" + p.lastModified + "/" + p.createdAt + "/" + p.accessTier + "/" +
        p.blobType +
        "/" +
        owner +
        "/" +
        note +
        "/" +
        p.userMeta.count.toString()
    }
  }
}

func putMetaCode(bucket: in StorageBucket, key: in String, name: in String, value: in String): String {
  val meta: Map[String, String] = newMap()
  meta[name] = value
  match bucket.put(key, "", "text/plain", meta) {
    case Ok(_) -> "ok"
    case Err(e) -> e.code
  }
}

func metadataScenario(origin: in String): String {
  val client = readyClient(origin, "meta")
  val bucket = bucketOf(client)
  val meta: Map[String, String] = newMap()
  meta["owner"] = "alice"
  meta["_build_note1"] = "two words, one comma"
  var text = "put=" + codeOf(putBytes(client, "m/doc", [7u8], "application/pdf", meta))
  text = text + "\nstat=" + statText(stat(client, "m/doc"))
  text = text + "\nabsent=" + statText(stat(client, "m/none"))
  text = text + "\ntier=" + codeOf(putBytesWithAccessTier(client, "m/cold", [8u8], "text/plain", "Cool"))
  text = text + "\ntierStat=" + statText(stat(client, "m/cold"))
  text = text + "\nsetTier=" + unitCode(setAccessTier(client, "m/cold", "Archive"))
  text = text + "\nsetTierStat=" + statText(stat(client, "m/cold"))
  text = text + "\nsetTierMissing=" + unitCode(setAccessTier(client, "m/none", "Cool"))
  text = text + "\nbadTier=" + codeOf(putBytesWithAccessTier(client, "m/x", [8u8], "text/plain", "Frozen")) + "," +
    unitCode(setAccessTier(client, "m/cold", "hot"))
  // Metadata the service would reject or alter is refused before any request.
  text = text + "\nupperName=" + putMetaCode(bucket, "m/u", "Owner", "alice")
  text = text + "\nhyphenName=" + putMetaCode(bucket, "m/u", "build-note", "x")
  text = text + "\nunicodeValue=" + putMetaCode(bucket, "m/u", "owner", "café")
  text = text + "\npaddedValue=" + putMetaCode(bucket, "m/u", "owner", " alice")
  val ambiguous: Map[String, String] = newMap()
  ambiguous["a_1"] = "x"
  ambiguous["a11"] = "y"
  text = text + "\nambiguousOrder=" +
    (match bucket.put("m/u", "", "text/plain", ambiguous) { case Ok(_) -> "ok"; case Err(e) -> e.code })
  text = text + "\nexists=" + boolCode(bucket.exists("m/u")) + "," + boolCode(bucket.exists("m/doc"))
  text + "\n" + stopFake(origin)
}

test "user metadata travels as x-ms-meta-* headers, and access tiers as x-ms-access-tier" {
  val lines = Str.split(withFakeService(19613, metadataScenario), "\n")
  assertEqualInt(lines.length, 16, "sixteen lines: " + Str.join(" | ", lines))
  assertTrue(lines[0].startsWith("put=ok:0x"), "put with metadata: " + lines[0])
  assertEqual(
    lines[1],
    "stat=application/pdf/17/" + LAST_MODIFIED_ISO + "/" + LAST_MODIFIED_ISO +
      "/Hot/BlockBlob/alice/two words, one comma/2",
    "properties: content type, ETag, dates, tier, blob type and both metadata entries"
  )
  assertEqual(lines[2], "absent=absent", "properties of a missing blob are None, not an error")
  assertTrue(lines[3].startsWith("tier=ok:0x"), "put with an access tier: " + lines[3])
  assertEqual(
    lines[4],
    "tierStat=text/plain/17/" + LAST_MODIFIED_ISO + "/" + LAST_MODIFIED_ISO + "/Cool/BlockBlob/-/-/0",
    "the tier was stored"
  )
  assertEqual(lines[5], "setTier=ok", "Set Blob Tier")
  assertEqual(
    lines[6],
    "setTierStat=text/plain/17/" + LAST_MODIFIED_ISO + "/" + LAST_MODIFIED_ISO + "/Archive/BlockBlob/-/-/0",
    "the tier was changed and nothing else"
  )
  assertEqual(lines[7], "setTierMissing=NOT_FOUND", "Set Blob Tier on a missing blob")
  assertEqual(lines[8], "badTier=INVALID_ARGUMENT,INVALID_ARGUMENT", "an unknown tier is refused locally")
  assertEqual(lines[9], "upperName=USER_META_UNSUPPORTED", "a header name would lose its case")
  assertEqual(lines[10], "hyphenName=USER_META_UNSUPPORTED", "Azure requires a C# identifier")
  assertEqual(lines[11], "unicodeValue=USER_META_UNSUPPORTED", "a header cannot carry non-ASCII intact")
  assertEqual(lines[12], "paddedValue=USER_META_UNSUPPORTED", "a header would lose the leading space")
  assertEqual(lines[13], "ambiguousOrder=USER_META_UNSUPPORTED", "'a_1' and 'a11' have no agreed signing order")
  assertEqual(lines[14], "exists=false,true", "nothing was stored by the refused puts")
  assertEqual(lines[15], "served 11", "the refused requests were never sent")
}

func getCode(bucket: in StorageBucket, key: in String): String {
  match bucket.get(key) {
    case Ok(_) -> "ok"
    case Err(e) -> e.code
  }
}

func bucketWith(cfg: in AzureBlobConfig): StorageBucket {
  match connect(cfg) {
    case Ok(b) -> b
    case Err(e) -> panic("connect failed: " + e.code + " " + e.message)
  }
}

func errorScenario(origin: in String): String {
  val good = bucketOf(readyClient(origin, "errors"))
  val noMeta: Map[String, String] = newMap()
  var text = "missing=" + getCode(good, "no/such/key")
  text = text + "\ninternal=" + getCode(good, "boom/internal")
  text = text + "\ngateway=" + getCode(good, "boom/gateway")
  text = text + "\nmoved=" + getCode(good, "boom/moved")
  text = text + "\nbodyOnly=" + getCode(good, "boom/body-only")
  text = text + "\nheaderOnly=" + getCode(good, "boom/header-only")
  text = text + "\nbare404=" + getCode(good, "boom/bare-404")
  text = text + "\nmessage=" + (match good.get("boom/internal") { case Ok(_) -> "ok"; case Err(e) -> e.message })
  text = text + "\nmissingMessage=" + (match good.get("no/such/key") { case Ok(_) -> "ok"; case Err(e) -> e.message })
  val wrongKey = bucketWith(endpointConfig(origin + "/" + ACCOUNT, ACCOUNT, WRONG_KEY, "errors"))
  text = text + "\nwrongKey=" + getCode(wrongKey, "k")
  text = text + "\nwrongKeyPut=" +
    (match wrongKey.put("k", "aGk=", "text/plain", noMeta) { case Ok(_) -> "ok"; case Err(e) -> e.code })
  text = text + "\nwrongKeyExists=" + boolCode(wrongKey.exists("k"))
  val wrongAccount = bucketWith(endpointConfig(origin + "/otheraccount", "otheraccount", ACCOUNT_KEY, "errors"))
  text = text + "\nwrongAccount=" + getCode(wrongAccount, "k")
  val noContainer = bucketOf(clientFor(origin, "never-created"))
  text = text + "\nnoContainer=" + getCode(noContainer, "k")
  text = text + "\nnoContainerList=" +
    (match noContainer.list("", "", 10) { case Ok(_) -> "ok"; case Err(e) -> e.code })
  text = text + "\nnoContainerExists=" + boolCode(noContainer.exists("k"))
  text = text + "\nnoContainerDelete=" + unitCode(noContainer.delete("k"))
  text = text + "\nbadMarker=" +
    (match good.list("", "zz-not-a-marker", 10) { case Ok(_) -> "ok"; case Err(e) -> e.code })
  text = text + "\ndeleteMissing=" + unitCode(good.delete("no/such/key"))
  text = text + "\nstillWorks=" +
    (match good.put("fine", "aGk=", "text/plain", noMeta) { case Ok(_) -> "ok"; case Err(e) -> e.code })
  text + "\n" + stopFake(origin)
}

test "Blob service error codes map to StorageError codes, and a redirect is not followed" {
  val lines = Str.split(withFakeService(19614, errorScenario), "\n")
  assertEqualInt(lines.length, 21, "twenty-one lines: " + Str.join(" | ", lines))
  assertEqual(lines[0], "missing=NOT_FOUND", "BlobNotFound is the interface's NOT_FOUND")
  assertEqual(lines[1], "internal=InternalError", "a 500 carries the service's code")
  assertEqual(lines[2], "gateway=HTTP_502", "an error page that is not the service's is HTTP_<status>")
  assertEqual(lines[3], "moved=HTTP_307", "a redirect is reported, not followed to another host")
  assertEqual(lines[4], "bodyOnly=LeaseIdMissing", "the code of the Error document when there is no header")
  assertEqual(lines[5], "headerOnly=ServerBusy", "the x-ms-error-code header when there is no document")
  assertEqual(lines[6], "bare404=NOT_FOUND", "a 404 with no code at all")
  assertEqual(
    lines[7],
    "message=cannot get blob 'boom/internal': InternalError (HTTP 500): The server encountered an internal error. Please retry the request.",
    "the message names the operation, the code, the status and the first line of the service's message"
  )
  assertEqual(
    lines[8],
    "missingMessage=cannot get blob 'no/such/key': BlobNotFound (HTTP 404): The specified blob does not exist.",
    "the message keeps the service's own code"
  )
  assertEqual(lines[9], "wrongKey=AuthenticationFailed", "a wrong key fails the server's signature check on GET")
  assertEqual(lines[10], "wrongKeyPut=AuthenticationFailed", "and on PUT")
  assertEqual(lines[11], "wrongKeyExists=AuthenticationFailed", "and on HEAD, where the code is in the header only")
  assertEqual(lines[12], "wrongAccount=AuthenticationFailed", "an account the service does not know")
  assertEqual(lines[13], "noContainer=ContainerNotFound", "a missing container on get")
  assertEqual(lines[14], "noContainerList=ContainerNotFound", "a missing container on list")
  assertEqual(lines[15], "noContainerExists=ContainerNotFound", "a missing container on exists is an error, not false")
  assertEqual(lines[16], "noContainerDelete=ContainerNotFound", "a missing container on delete is an error too")
  assertEqual(lines[17], "badMarker=OutOfRangeInput", "a marker the service does not know")
  assertEqual(lines[18], "deleteMissing=ok", "deleting a missing blob succeeds: BlobNotFound is the wanted outcome")
  assertEqual(lines[19], "stillWorks=ok", "the correctly signed client is unaffected")
  assertEqual(lines[20], "served 21", "every request reached the server")
}

func fetch(url: in String): String {
  match await defaultClient().get(url) {
    case Err(e) -> "transport " + HttpError.message(e)
    case Ok(resp) -> {
      val body = match await HttpResponse.bodyText(resp) { case Ok(t) -> t; case Err(_) -> "(unreadable)" }
      val code = match parseFault(body) { case Some(f) -> f.code; case None -> body }
      HttpResponse.statusCode(resp).toString() + " " + code
    }
  }
}

// One HTTP PUT with no credentials, as the holder of a SAS URL makes it.
func upload(url: in String, bytes: in slice[Byte], blobType: in String): String {
  val parsed = match Url.tryFrom(url) { case Ok(u) -> u; case Err(_) -> return "bad url" }
  var req = match withBytesBody(request(PUT, parsed), "text/plain", bytes) {
    case Ok(r) -> r
    case Err(e) -> return HttpError.message(e)
  }
  if blobType.length > 0 {
    req = match withHeader(req, "x-ms-blob-type", blobType) {
      case Ok(r) -> r
      case Err(e) -> return HttpError.message(e)
    }
  }
  match await sendAsync(req) {
    case Err(e) -> "transport " + HttpError.message(e)
    case Ok(resp) -> {
      val body = match await HttpResponse.bodyText(resp) { case Ok(t) -> t; case Err(_) -> "" }
      val code = match parseFault(body) { case Some(f) -> " " + f.code; case None -> "" }
      HttpResponse.statusCode(resp).toString() + code
    }
  }
}

func sasScenario(origin: in String): String {
  val client = readyClient(origin, "presign")
  val bucket = bucketOf(client)
  val noMeta: Map[String, String] = newMap()
  val key = "shared/q3 report (final).txt"
  var text = "put=" +
    (
      match bucket.put(key, "aGVsbG8gd29ybGQ=", "text/plain", noMeta) {
        case Ok(_) -> "ok"
        case Err(e) -> e.code
      }
    )
  // A read SAS, fetched by a client that has no credentials.
  match bucket.presignedUrl(key, 300) {
    case Err(e) -> {
      text = text + "\nget=" + e.code
    }
    case Ok(url) -> {
      text = text + "\nshape=" +
        (
          if url.startsWith(origin + "/" + ACCOUNT + "/presign/shared/q3%20report%20%28final%29.txt?sv=" + apiVersion() + "&sr=b&sp=r&se=") and not url.contains("spr=") {
            "ok"
          } else {
            url
          }
        )
      text = text + "\nget=" + fetch(url)
      text = text + "\ntampered=" + fetch(Str.replace(url, "sp=r", "sp=rw"))
      text = text + "\notherKey=" + fetch(Str.replace(url, "report", "REPORT"))
      text = text + "\nreadSasPut=" + upload(url, Enc.encodeUtf8("overwritten"), "BlockBlob")
    }
  }
  // A SAS that expired in 2020.
  text = text + "\nexpired=" +
    (match client.sasUrl(key, "r", 1577836860i64) { case Ok(url) -> fetch(url); case Err(e) -> e.code })
  // A write SAS: upload without credentials, then read it back signed.
  match presignedPutUrl(client, "inbox/upload.txt", 300) {
    case Err(e) -> {
      text = text + "\nputUrl=" + e.code
    }
    case Ok(url) -> {
      text = text + "\nputUrlNoType=" + upload(url, Enc.encodeUtf8("uploaded"), "")
      text = text + "\nputUrl=" + upload(url, Enc.encodeUtf8("uploaded"), "BlockBlob")
      // The write SAS does not authorise a read.
      text = text + "\nputUrlAsGet=" + fetch(url)
    }
  }
  text = text + "\nuploaded=" +
    (match bucket.get("inbox/upload.txt") { case Ok(obj) -> obj.dataBase64; case Err(e) -> e.code })
  text = text + "\nstillThere=" + (match bucket.get(key) { case Ok(obj) -> obj.dataBase64; case Err(e) -> e.code })
  text = text + "\nunsafe=" + codeOf(bucket.presignedUrl("../escape", 60)) + "," +
    codeOf(presignedPutUrl(client, "a/../../b", 60)) +
    "," +
    codeOf(presignedPutUrl(client, "k", 0)) +
    "," +
    codeOf(presignedPutUrl(client, "k", 604801))
  text + "\n" + stopFake(origin)
}

test "service SAS URLs work for a client without credentials, until they expire" {
  val lines = Str.split(withFakeService(19615, sasScenario), "\n")
  assertEqualInt(lines.length, 14, "fourteen lines: " + Str.join(" | ", lines))
  assertEqual(lines[0], "put=ok", "the blob is stored")
  assertEqual(lines[1], "shape=ok", "a blob SAS for the pinned version, with no spr over loopback http")
  assertEqual(lines[2], "get=200 hello world", "the SAS URL returns the blob")
  assertEqual(lines[3], "tampered=403 AuthenticationFailed", "changing the permissions invalidates the signature")
  assertEqual(lines[4], "otherKey=403 AuthenticationFailed", "the SAS is for one blob only")
  assertEqual(lines[5], "readSasPut=403 AuthorizationPermissionMismatch", "a read SAS does not allow an upload")
  assertEqual(lines[6], "expired=403 AuthenticationFailed", "an expired SAS is refused")
  assertEqual(
    lines[7],
    "putUrlNoType=400 MissingRequiredHeader",
    "an upload must say x-ms-blob-type, as every Put Blob does"
  )
  assertEqual(lines[8], "putUrl=201", "the write SAS stores an upload")
  assertEqual(lines[9], "putUrlAsGet=403 AuthorizationPermissionMismatch", "a write SAS does not allow a read")
  assertEqual(lines[10], "uploaded=dXBsb2FkZWQ=", "the upload is readable through the bucket")
  assertEqual(lines[11], "stillThere=aGVsbG8gd29ybGQ=", "the refused upload changed nothing")
  assertEqual(
    lines[12],
    "unsafe=INVALID_KEY,INVALID_KEY,INVALID_ARGUMENT,INVALID_ARGUMENT",
    "unsafe keys and lifetimes outside 1 second to 7 days are never signed"
  )
  assertEqual(lines[13], "served 12", "every request reached the server")
}

func containerScenario(origin: in String): String {
  val fresh = clientFor(origin, "fresh-container")
  var text = "before=" + boolCode(containerExists(fresh))
  text = text + "\ncreate=" + boolCode(createContainer(fresh))
  text = text + "\nafter=" + boolCode(containerExists(fresh))
  text = text + "\nagain=" + boolCode(createContainer(fresh))
  val wrongKey = match openClient(endpointConfig(origin + "/" + ACCOUNT, ACCOUNT, WRONG_KEY, "fresh-container")) {
    case Ok(c) -> c
    case Err(e) -> panic("openClient failed: " + e.message)
  }
  text = text + "\nwrongKey=" + boolCode(containerExists(wrongKey)) + "," + boolCode(createContainer(wrongKey))
  text + "\n" + stopFake(origin)
}

test "createContainer tells created from already there, and containerExists from an error" {
  val lines = Str.split(withFakeService(19616, containerScenario), "\n")
  assertEqualInt(lines.length, 6, "six lines: " + Str.join(" | ", lines))
  assertEqual(lines[0], "before=false", "Get Container Properties of a container that does not exist")
  assertEqual(lines[1], "create=true", "created, by a PUT with an empty body")
  assertEqual(lines[2], "after=true", "Get Container Properties afterwards")
  assertEqual(lines[3], "again=false", "ContainerAlreadyExists is not an error")
  assertEqual(lines[4], "wrongKey=AuthenticationFailed,AuthenticationFailed", "a refused request is not 'false'")
  assertEqual(lines[5], "served 6", "every request reached the server")
}

func pagingScenario(origin: in String): String {
  val client = readyClient(origin, "paging")
  val bucket = bucketOf(client)
  val noMeta: Map[String, String] = newMap()
  var text = "puts="
  for key in ["p/e", "other/x", "p/b", "p/a", "p/d", "q", "p/c"] {
    text = text + (match bucket.put(key, "aGk=", "text/plain", noMeta) { case Ok(_) -> "+"; case Err(e) -> e.code })
  }
  // Follow the continuation tokens under a prefix.
  var token = ""
  var pages = 0
  var more = true
  var seen = ""
  var tokens = ""
  while more and pages < 10 {
    match bucket.list("p/", token, 2) {
      case Err(e) -> {
        seen = seen + e.code
        more = false
      }
      case Ok(r) -> {
        pages = pages + 1
        for e in r.entries {
          seen = seen + e.key + " "
        }
        match r.nextContinuationToken {
          case Some(t) -> {
            token = t
            tokens = tokens + (if r.isTruncated { "T" } else { "?" })
          }
          case None -> {
            tokens = tokens + (if r.isTruncated { "?" } else { "." })
            more = false
          }
        }
      }
    }
  }
  text = text + "\npages=" + pages.toString() + " " + tokens + " " + seen
  text = text + "\nall=" +
    (
      match bucket.list("", "", 1000) {
        case Ok(r) -> r.entries.length.toString() + "/" + r.isTruncated.toString()
        case Err(e) -> e.code
      }
    )
  // (`maxKeys` outside 1..1000 is the interface's own `requires:`, a caller
  // bug, and is not exercised here.)
  text = text + "\nunsafePrefix=" + (match bucket.list("../x", "", 10) { case Ok(_) -> "ok"; case Err(e) -> e.code })
  text + "\n" + stopFake(origin)
}

test "list follows NextMarker under a prefix, in name order" {
  val lines = Str.split(withFakeService(19617, pagingScenario), "\n")
  assertEqualInt(lines.length, 5, "five lines: " + Str.join(" | ", lines))
  assertEqual(lines[0], "puts=+++++++", "seven blobs stored")
  assertEqual(lines[1], "pages=3 TT. p/a p/b p/c p/d p/e ", "three pages of at most two, each name once, in order")
  assertEqual(lines[2], "all=7/false", "the empty prefix lists everything on one page")
  assertEqual(lines[3], "unsafePrefix=INVALID_KEY", "the prefix is validated locally")
  assertEqual(lines[4], "served 12", "one request per put and per page")
}

test "a request to a server that is not there is CONNECTION_FAILED, not a panic" {
  val bucket = bucketWith(
    endpointConfig("http://127.0.0.1:1/" + ACCOUNT, ACCOUNT, ACCOUNT_KEY, "data").withRequestTimeout(5000)
  )
  val noMeta: Map[String, String] = newMap()
  assertEqual(getCode(bucket, "k"), "CONNECTION_FAILED", "get")
  match bucket.put("k", "aGk=", "text/plain", noMeta) {
    case Ok(_) -> panic("nothing listens on port 1")
    case Err(e) -> assertEqual(e.code, "CONNECTION_FAILED", "put")
  }
  assertEqual(boolCode(bucket.exists("k")), "CONNECTION_FAILED", "exists is an error, not false")
  match bucket.list("", "", 10) {
    case Ok(_) -> panic("nothing listens on port 1")
    case Err(e) -> assertEqual(e.code, "CONNECTION_FAILED", "list")
  }
  assertEqual(unitCode(bucket.delete("k")), "CONNECTION_FAILED", "delete")
}
