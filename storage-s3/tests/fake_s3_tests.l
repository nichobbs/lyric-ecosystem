//! tests/fake_s3_tests.l - the S3 backend over real HTTP, against an S3 server
//! written in Lyric on `Std.HttpServer`.
//!
//! The fake server does what S3 does with a request, so nothing here is
//! answered from a canned response:
//!
//!   * it verifies the AWS Signature Version 4 of every request with the
//!     shared secret, rebuilding the canonical request from the method, path,
//!     query and signed headers it actually received, and checks that
//!     `x-amz-content-sha256` is the hash of the body it actually received
//!     (`SignatureDoesNotMatch`, `InvalidAccessKeyId`,
//!     `XAmzContentSHA256Mismatch`, `AccessDenied` are its answers otherwise);
//!   * presigned URLs are verified the same way from their query string,
//!     including expiry;
//!   * objects live in memory with their content type, user metadata and
//!     storage class; ListObjectsV2 pages in key order with an opaque
//!     continuation token and `encoding-type=url`.
//!
//! `Testing.Conformance.Storage` runs against it end to end, then the
//! backend-specific behaviour: binary payloads, error mapping, redirects,
//! presigned GET and PUT fetched with a plain HTTP client, storage classes.
//!
//! The server runs on its own thread: a `Task.Run` worker on `--target dotnet`
//! (`spawn` is synchronous there), a `spawn` inside a `scope` on `--target
//! jvm`. No assertion runs while it is waiting for a request: a scenario
//! returns its findings and ends by asking the server to stop.
@test_module
package Storage.S3.FakeS3Tests

import Std.Core
import Std.Collections
import Std.Encoding as Enc
import Std.Errors
import Std.Http
import Std.HttpServer
import Std.String as Str
import Std.Testing
import Std.Time as Time
import Testing.Conformance
import Testing.Conformance.Storage as StorageConformance
import Storage.S3
import Storage.S3.Extras
import Storage.S3.SigV4
import Storage.S3.Wire.{parseFault}

// ─── The fake server's state ─────────────────────────────────────────────────
val ACCESS_KEY: String = "AKIAFAKEFAKEFAKEFAKE"

val SECRET_KEY: String = "fake/secret+key/for-tests-only"

val REGION: String = "eu-test-1"

val STOP_PATH: String = "/__stop"

// Every stored object reports this modification time.
val LAST_MODIFIED_HTTP: String = "Thu, 12 Nov 2026 10:15:00 GMT"

val LAST_MODIFIED_ISO: String = "2026-11-12T10:15:00.000Z"

record StoredObject {
  bytes: slice[Byte]
  contentType: String
  etag: String
  userMeta: Map[String, String]
  storageClass: String
}

ref record FakeS3 {
  // Keyed `<bucket>/<key>`.
  var objects: Map[String, StoredObject]
  // Buckets made with CreateBucket. Any other bucket name simply exists,
  // except the few names the handlers treat specially.
  var created: Map[String, Bool]
  var requestCount: Int
}

func newFakeS3(): FakeS3 {
  FakeS3(objects = newMap(), created = newMap(), requestCount = 0)
}

// ─── Request inspection ──────────────────────────────────────────────────────
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

// ─── Responses ───────────────────────────────────────────────────────────────
func xmlEscape(text: in String): String {
  Str.replace(Str.replace(Str.replace(text, "&", "&amp;"), "<", "&lt;"), ">", "&gt;")
}

func respondEmpty(ctx: in HttpContext, status: in Int, headers: in Map[String, String]): Unit {
  val empty: slice[Byte] = []
  respondBytesWithHeaders(ctx, status, "application/xml", empty, headers)
}

func respondFault(ctx: in HttpContext, status: in Int, code: in String, message: in String): Unit {
  val none: Map[String, String] = newMap()
  // As S3 does, a HEAD is answered without a body: only the status tells the
  // client what went wrong.
  if requestMethod(ctx) == "HEAD" {
    respondEmpty(ctx, status, none)
    return
  }
  val body = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>" + code + "</Code><Message>" +
    xmlEscape(message) +
    "</Message><RequestId>FAKE0001</RequestId></Error>"
  respondBytesWithHeaders(ctx, status, "application/xml", Enc.encodeUtf8(body), none)
}

// ─── Signature verification ──────────────────────────────────────────────────
// Seconds since the Unix epoch for `yyyymmddThhmmssZ` (the civil-from-days
// algorithm, proleptic Gregorian calendar).
func amzDateToEpochSeconds(amzDate: in String): Long {
  val y0 = intAt(amzDate, 0, 4)
  val m = intAt(amzDate, 4, 2)
  val d = intAt(amzDate, 6, 2)
  val y = if m <= 2 { y0 - 1 } else { y0 }
  val era = y / 400
  val yoe = y - era * 400
  val mp = if m > 2 { m - 3 } else { m + 9 }
  val doy = (153 * mp + 2) / 5 + d - 1
  val doe = yoe * 365 + yoe / 4 - yoe / 100 + doy
  val days = (era * 146097 + doe - 719468).toLong()
  days * 86400i64 + (intAt(amzDate, 9, 2) * 3600 + intAt(amzDate, 11, 2) * 60 + intAt(amzDate, 13, 2)).toLong()
}

func intAt(text: in String, start: in Int, count: in Int): Int {
  var n = 0
  var i = 0
  while i < count {
    n = n * 10 + (Str.codeUnitAt(text, start + i) - 48)
    i = i + 1
  }
  n
}

func fakeCreds(): Credentials {
  Credentials(accessKeyId = ACCESS_KEY, secretAccessKey = SECRET_KEY, sessionToken = "")
}

// The value of `name=` in an `Authorization` header, up to the next comma.
func authField(authorization: in String, name: in String): String {
  val at = Str.indexOfRaw(authorization, name + "=")
  if at < 0 {
    return ""
  }
  val rest = authorization.substring(at + name.length + 1)
  val comma = Str.indexOfRaw(rest, ",")
  if comma >= 0 {
    rest.substring(0, comma)
  } else {
    rest
  }
}

// The canonical URI S3 derives from a request path: each segment decoded,
// then encoded again.
func canonicalUriOf(rawPath: in String): String {
  uriEncode(urlDecode(rawPath), true)
}

// Verify a request signed in its `Authorization` header. Returns "" when the
// signature is the one this server computes, otherwise the S3 error code.
func verifyHeaderAuth(ctx: in HttpContext, body: in slice[Byte]): String {
  val authorization = headerValue(ctx, "Authorization")
  if not Str.startsWith(authorization, "AWS4-HMAC-SHA256 ") {
    return "AccessDenied"
  }
  val credential = authField(authorization, "Credential")
  val signedNames = authField(authorization, "SignedHeaders")
  val signature = authField(authorization, "Signature")
  val amzDate = headerValue(ctx, "x-amz-date")
  if amzDate.length != 16 {
    return "AccessDenied"
  }
  if credential != ACCESS_KEY + "/" + credentialScope(amzDate, REGION, "s3") {
    if not Str.startsWith(credential, ACCESS_KEY + "/") {
      return "InvalidAccessKeyId"
    }
    return "AuthorizationHeaderMalformed"
  }
  val payloadHash = headerValue(ctx, "x-amz-content-sha256")
  if payloadHash != sha256Hex(body) {
    return "XAmzContentSHA256Mismatch"
  }
  // The headers S3 requires a SigV4 request to sign.
  val required = ";" + signedNames + ";"
  if not Str.contains(required, ";host;") or not Str.contains(required, ";x-amz-date;") or not Str.contains(required, ";x-amz-content-sha256;") {
    return "AccessDenied"
  }
  val headers: List[Pair] = newList()
  val names = Str.split(signedNames, ";")
  var i = 0
  while i < names.length {
    headers.add(Pair(name = names[i], value = headerValue(ctx, names[i])))
    i = i + 1
  }
  val expected = signRequest(
    requestMethod(ctx),
    canonicalUriOf(requestPath(ctx)),
    queryPairs(requestQuery(ctx)),
    headers,
    payloadHash,
    fakeCreds(),
    REGION,
    "s3",
    amzDate
  )
  if expected.signature == signature {
    ""
  } else {
    "SignatureDoesNotMatch"
  }
}

// Verify a presigned request (signature in the query string).
func verifyQueryAuth(ctx: in HttpContext, query: in List[Pair]): String {
  if queryValue(query, "X-Amz-Algorithm") != "AWS4-HMAC-SHA256" or queryValue(query, "X-Amz-SignedHeaders") != "host" {
    return "AccessDenied"
  }
  val amzDate = queryValue(query, "X-Amz-Date")
  if amzDate.length != 16 {
    return "AccessDenied"
  }
  val scopeText = credentialScope(amzDate, REGION, "s3")
  if queryValue(query, "X-Amz-Credential") != ACCESS_KEY + "/" + scopeText {
    return "InvalidAccessKeyId"
  }
  val expires = intAt(queryValue(query, "X-Amz-Expires"), 0, queryValue(query, "X-Amz-Expires").length)
  val nowSeconds = Time.nowEpochMillis() / 1000i64
  if nowSeconds > amzDateToEpochSeconds(amzDate) + expires.toLong() {
    return "AccessDenied"
  }
  val unsigned: List[Pair] = newList()
  var i = 0
  while i < query.count {
    if query[i].name != "X-Amz-Signature" {
      unsigned.add(query[i])
    }
    i = i + 1
  }
  val headers: List[Pair] = newList()
  headers.add(Pair(name = "host", value = headerValue(ctx, "Host")))
  val cr = canonicalRequest(
    requestMethod(ctx),
    canonicalUriOf(requestPath(ctx)),
    canonicalQuery(unsigned),
    headers,
    "UNSIGNED-PAYLOAD"
  )
  val expected = signatureOf(SECRET_KEY, amzDate, REGION, "s3", stringToSign(amzDate, scopeText, cr))
  if expected == queryValue(query, "X-Amz-Signature") {
    ""
  } else {
    "SignatureDoesNotMatch"
  }
}

// ─── Object operations ───────────────────────────────────────────────────────
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

func sortedKeysOf(s3: in FakeS3, bucket: in String, prefix: in String): List[String] {
  val acc: List[String] = newList()
  val all = mapKeys(s3.objects)
  val wanted = bucket + "/" + prefix
  var i = 0
  while i < all.count {
    val full: String = all[i]
    if Str.startsWith(full, wanted) {
      acc.add(full.substring(bucket.length + 1))
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

// S3's `encoding-type=url` form of a key: percent-encoded, `/` kept, a space
// as `+`.
func listEncoded(key: in String): String {
  Str.replace(uriEncode(key, true), "%20", "+")
}

func listObjects(s3: in FakeS3, ctx: in HttpContext, bucket: in String, query: in List[Pair]): Unit {
  val prefix = queryValue(query, "prefix")
  val maxKeysText = queryValue(query, "max-keys")
  val maxKeys = if maxKeysText.length == 0 { 1000 } else { intAt(maxKeysText, 0, maxKeysText.length) }
  val urlEncoded = queryValue(query, "encoding-type") == "url"
  val token = queryValue(query, "continuation-token")
  // The token is opaque to clients: the hex of the last key of the page before.
  var after = ""
  if token.length > 0 {
    val decoded = match Enc.tryDecodeHex(token) { case Some(bytes) -> Enc.tryDecodeUtf8(bytes); case None -> None }
    match decoded {
      case Some(key) -> {
        after = key
      }
      case None -> {
        respondFault(ctx, 400, "InvalidArgument", "The continuation token provided is incorrect")
        return
      }
    }
  }
  val keys = sortedKeysOf(s3, bucket, prefix)
  val sb = Str.StringBuilder.new()
  sb.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n")
  sb.append("<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">")
  sb.append("<Name>" + bucket + "</Name>")
  sb.append("<Prefix>" + xmlEscape(if urlEncoded { listEncoded(prefix) } else { prefix }) + "</Prefix>")
  sb.append("<MaxKeys>" + maxKeys.toString() + "</MaxKeys>")
  if urlEncoded {
    sb.append("<EncodingType>url</EncodingType>")
  }
  var written = 0
  var last = ""
  var more = false
  var i = 0
  while i < keys.count {
    val key = keys[i]
    if after.length == 0 or ordinalLess(after, key) {
      if written == maxKeys {
        more = true
      } else {
        match mapGet(s3.objects, bucket + "/" + key) {
          case Some(obj) -> {
            sb.append("<Contents><Key>" + xmlEscape(if urlEncoded { listEncoded(key) } else { key }) + "</Key>")
            sb.append("<LastModified>" + LAST_MODIFIED_ISO + "</LastModified>")
            sb.append("<ETag>&quot;" + obj.etag + "&quot;</ETag>")
            sb.append("<Size>" + obj.bytes.length.toString() + "</Size>")
            sb.append("<StorageClass>" + obj.storageClass + "</StorageClass></Contents>")
            written = written + 1
            last = key
          }
          case None -> ()
        }
      }
    }
    i = i + 1
  }
  sb.append("<KeyCount>" + written.toString() + "</KeyCount>")
  sb.append("<IsTruncated>" + (if more { "true" } else { "false" }) + "</IsTruncated>")
  if more {
    sb.append("<NextContinuationToken>" + Str.toLower(Enc.encodeHex(Enc.encodeUtf8(last))) + "</NextContinuationToken>")
  }
  sb.append("</ListBucketResult>")
  val none: Map[String, String] = newMap()
  respondBytesWithHeaders(ctx, 200, "application/xml", Enc.encodeUtf8(sb.toString()), none)
}

func objectHeaders(obj: in StoredObject): Map[String, String] {
  val headers: Map[String, String] = newMap()
  headers.add("ETag", "\"" + obj.etag + "\"")
  headers.add("Last-Modified", LAST_MODIFIED_HTTP)
  if obj.storageClass != "STANDARD" {
    headers.add("x-amz-storage-class", obj.storageClass)
  }
  val entries = mapEntries(obj.userMeta)
  for e in entries {
    headers.add("x-amz-meta-" + e.key, e.value)
  }
  headers
}

func putObject(s3: in FakeS3, ctx: in HttpContext, fullKey: in String, body: in slice[Byte]): Unit {
  val userMeta: Map[String, String] = newMap()
  val entries = mapEntries(requestHeaders(ctx))
  for e in entries {
    val lower = Str.toLower(e.key)
    if Str.startsWith(lower, "x-amz-meta-") {
      userMeta[lower.substring(11)] = e.value
    }
  }
  val storageClass = headerValue(ctx, "x-amz-storage-class")
  val contentType = headerValue(ctx, "Content-Type")
  val etag = sha256Hex(body).substring(0, 32)
  s3.objects[fullKey] = StoredObject(
    bytes = body,
    contentType = if contentType.length > 0 { contentType } else { "binary/octet-stream" },
    etag = etag,
    userMeta = userMeta,
    storageClass = if storageClass.length > 0 { storageClass } else { "STANDARD" }
  )
  val headers: Map[String, String] = newMap()
  headers.add("ETag", "\"" + etag + "\"")
  respondEmpty(ctx, 200, headers)
}

func serveObject(s3: in FakeS3, ctx: in HttpContext, bucket: in String, key: in String, body: in slice[Byte]): Unit {
  val method = requestMethod(ctx)
  val fullKey = bucket + "/" + key
  // Keys that make the server misbehave on purpose.
  if key == "boom/internal" {
    respondFault(ctx, 500, "InternalError", "We encountered an internal error. Please try again.")
    return
  }
  if key == "boom/gateway" {
    val none: Map[String, String] = newMap()
    respondBytesWithHeaders(ctx, 502, "text/html", Enc.encodeUtf8("<html><body>502 Bad Gateway</body></html>"), none)
    return
  }
  if key == "boom/moved" {
    val headers: Map[String, String] = newMap()
    headers.add("Location", "http://127.0.0.1:1/elsewhere")
    val text = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>PermanentRedirect</Code>" +
      "<Message>The bucket you are attempting to access must be addressed using the specified endpoint.</Message></Error>"
    respondBytesWithHeaders(ctx, 301, "application/xml", Enc.encodeUtf8(text), headers)
    return
  }
  if method == "PUT" {
    putObject(s3, ctx, fullKey, body)
  } else if method == "DELETE" {
    val _ = s3.objects.remove(fullKey)
    val none: Map[String, String] = newMap()
    respondEmpty(ctx, 204, none)
  } else if method == "GET" or method == "HEAD" {
    match mapGet(s3.objects, fullKey) {
      case None -> {
        respondFault(ctx, 404, "NoSuchKey", "The specified key does not exist.")
      }
      case Some(obj) -> {
        val empty: slice[Byte] = []
        respondBytesWithHeaders(
          ctx,
          200,
          obj.contentType,
          if method == "HEAD" { empty } else { obj.bytes },
          objectHeaders(obj)
        )
      }
    }
  } else {
    respondFault(ctx, 405, "MethodNotAllowed", "The specified method is not allowed against this resource.")
  }
}

// CreateBucket. Outside us-east-1 the body must name this server's region.
func createBucketFake(s3: in FakeS3, ctx: in HttpContext, bucket: in String, body: in slice[Byte]): Unit {
  val text = match Enc.tryDecodeUtf8(body) { case Some(t) -> t; case None -> "" }
  if not Str.contains(text, "<LocationConstraint>" + REGION + "</LocationConstraint>") {
    respondFault(ctx, 400, "IllegalLocationConstraintException", "The unspecified location constraint is incompatible")
  } else if bucket == "owned-bucket" or s3.created.containsKey(bucket) {
    respondFault(ctx, 409, "BucketAlreadyOwnedByYou", "Your previous request to create the named bucket succeeded")
  } else if bucket == "taken-bucket" {
    respondFault(ctx, 409, "BucketAlreadyExists", "The requested bucket name is not available")
  } else {
    s3.created.add(bucket, true)
    val none: Map[String, String] = newMap()
    respondEmpty(ctx, 200, none)
  }
}

func handle(s3: in FakeS3, ctx: in HttpContext): Unit {
  s3.requestCount = s3.requestCount + 1
  val body = requestBodyBytes(ctx)
  val query = queryPairs(requestQuery(ctx))
  val problem = if queryValue(query, "X-Amz-Signature").length > 0 {
    verifyQueryAuth(ctx, query)
  } else {
    verifyHeaderAuth(ctx, body)
  }
  if problem.length > 0 {
    respondFault(
      ctx,
      if problem == "XAmzContentSHA256Mismatch" { 400 } else { 403 },
      problem,
      "the request was refused: " + problem
    )
    return
  }
  // Path-style: /<bucket>[/<key>].
  val path = urlDecode(requestPath(ctx))
  val rest = path.substring(1)
  val slash = Str.indexOfRaw(rest, "/")
  val bucket = if slash >= 0 { rest.substring(0, slash) } else { rest }
  val key = if slash >= 0 { rest.substring(slash + 1) } else { "" }
  if bucket == "missing-bucket" {
    respondFault(ctx, 404, "NoSuchBucket", "The specified bucket does not exist")
    return
  }
  if key.length == 0 {
    if requestMethod(ctx) == "GET" and queryValue(query, "list-type") == "2" {
      listObjects(s3, ctx, bucket, query)
    } else if requestMethod(ctx) == "HEAD" {
      val none: Map[String, String] = newMap()
      respondEmpty(ctx, if s3.created.containsKey(bucket) or bucket == "owned-bucket" { 200 } else { 404 }, none)
    } else if requestMethod(ctx) == "PUT" {
      createBucketFake(s3, ctx, bucket, body)
    } else {
      respondFault(ctx, 400, "InvalidRequest", "only ListObjectsV2 is served on a bucket")
    }
    return
  }
  serveObject(s3, ctx, bucket, key, body)
}

func serveUntilStopped(l: in HttpListener): Unit {
  val s3 = newFakeS3()
  var running = true
  while running {
    val ctx = nextContext(l)
    if requestPath(ctx) == STOP_PATH {
      respondText(ctx, 200, "served " + s3.requestCount.toString())
      running = false
    } else {
      // A bug in the fake must surface as a failed request, not as a server
      // thread that died and a test run that waits for it.
      try {
        handle(s3, ctx)
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

func endpointOf(port: in Int): String {
  "http://127.0.0.1:" + port.toString()
}

@cfg(target = "dotnet")
func withFakeS3(port: in Int, scenario: in (String) -> String): String {
  val l = startListener(endpointOf(port) + "/")
  val server = taskRun({ -> serveUntilStopped(l) })
  val transcript = scenario(endpointOf(port))
  taskWait(server)
  stopListener(l)
  transcript
}

@cfg(target = "jvm")
func withFakeS3(port: in Int, scenario: in (String) -> String): String {
  val l = startListener(endpointOf(port) + "/")
  var transcript = ""
  scope {
    val server = spawn serveUntilStopped(l)
    transcript = scenario(endpointOf(port))
    await server
  }
  stopListener(l)
  transcript
}

// Asks the server to stop; the last thing every scenario does. Returns what
// the server said: how many S3 requests it served.
func stopFake(endpoint: in String): String {
  match await defaultClient().get(endpoint + STOP_PATH) {
    case Err(e) -> "stop failed: " + HttpError.message(e)
    case Ok(resp) -> match await HttpResponse.bodyText(resp) { case Ok(text) -> text; case Err(_) -> "stop unreadable" }
  }
}

func configFor(endpoint: in String, bucket: in String): S3Config {
  endpointConfig(endpoint, bucket, REGION, ACCESS_KEY, SECRET_KEY).withRequestTimeout(10000)
}

func clientFor(endpoint: in String, bucket: in String): S3Client {
  match openClient(configFor(endpoint, bucket)) {
    case Ok(c) -> c
    case Err(e) -> panic("openClient failed: " + e.code + " " + e.message)
  }
}

// ─── Conformance ─────────────────────────────────────────────────────────────
ref record BucketCounter {
  var next: Int
}

// A fresh, empty bucket per conformance case: a new bucket name each time.
func freshBucket(endpoint: in String, counter: in BucketCounter): StorageBucket {
  counter.next = counter.next + 1
  match connect(configFor(endpoint, "conformance-" + counter.next.toString())) {
    case Ok(b) -> b
    case Err(e) -> panic("connect failed: " + e.code + " " + e.message)
  }
}

// Runs the suite and returns its summary; the tally itself is left in `into`
// for the test to check once the server has stopped.
func conformanceScenario(endpoint: in String, into: in List[Checks]): String {
  val counter = BucketCounter(next = 0)
  val checks = StorageConformance.run({ -> freshBucket(endpoint, counter) })
  into.add(checks)
  summary(checks) + "\n" + stopFake(endpoint)
}

test "S3Bucket conforms to StorageBucket, over HTTP against the signature-checking fake" {
  val ran: List[Checks] = newList()
  val lines = Str.split(withFakeS3(19471, { endpoint -> conformanceScenario(endpoint, ran) }), "\n")
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

func everyByte(): slice[Byte] {
  val acc: List[Byte] = newList()
  var i = 0
  while i < 256 {
    acc.add(i.toByte())
    i = i + 1
  }
  acc.toArray()
}

func binaryScenario(endpoint: in String): String {
  val client = clientFor(endpoint, "binary")
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
      text = text + "put=" + meta.contentLength.toString() + "/" + meta.etag
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
  // An empty object.
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
  // Raw bytes through the extras, with a key that needs encoding.
  val oddKey = "odd/sp ace+plus&amp=eqé/100%/(1)*.txt"
  text = text + "\nodd=" + codeOf(putBytes(client, oddKey, [1u8, 2u8, 3u8], "text/plain", noMeta))
  match getBytes(client, oddKey) {
    case Err(e) -> {
      text = text + "\noddGet=" + e.code + " " + e.message
    }
    case Ok(got) -> {
      text = text + "\noddGet=" + Str.toLower(Enc.encodeHex(got.bytes))
    }
  }
  match bucket.list("odd/", "", 10) {
    case Err(e) -> {
      text = text + "\noddList=" + e.code + " " + e.message
    }
    case Ok(r) -> {
      text = text + "\noddList=" + r.entries.length.toString() + ":" +
        (if r.entries.length == 1 and r.entries[0].key == oddKey { "same key" } else { "different key" }) +
        ":" +
        (
          if r.entries.length == 1 {
            r.entries[0].contentLength.toString() + ":" + r.entries[0].etag.length.toString() + ":" + r.entries[0].lastModified
          } else {
            ""
          }
        )
    }
  }
  text = text + "\nbadBase64=" +
    (match bucket.put("k", "not base64!!", "text/plain", noMeta) { case Ok(_) -> "ok"; case Err(e) -> e.code })
  text + "\n" + stopFake(endpoint)
}

test "binary payloads, empty objects and keys that need encoding survive the round trip" {
  val lines = Str.split(withFakeS3(19472, binaryScenario), "\n")
  assertEqualInt(lines.length, 8, "eight lines: " + Str.join(" | ", lines))
  val etag = sha256Hex(everyByte()).substring(0, 32)
  assertEqual(lines[0], "put=256/" + etag, "put reports the decoded size and the server's ETag, unquoted")
  assertEqual(
    lines[1],
    "get=same/256/application/octet-stream/" + etag + "/2026-11-12T10:15:00Z/binary",
    "get returns the bytes, and Last-Modified as ISO 8601"
  )
  assertEqual(lines[2], "empty=0/0/application/x-empty", "a zero-length object")
  assertEqual(
    lines[3],
    "odd=ok:" + sha256Hex([1u8, 2u8, 3u8]).substring(0, 32),
    "a key with spaces, '+', '&', '%', non-ASCII"
  )
  assertEqual(lines[4], "oddGet=010203", "the encoded key names the same object on get")
  assertEqual(lines[5], "oddList=1:same key:3:32:2026-11-12T10:15:00.000Z", "the listing decodes the key back")
  assertEqual(lines[6], "badBase64=INVALID_BASE64", "a payload that is not base64 never reaches S3")
  assertEqual(lines[7], "served 7", "exactly the expected requests reached the server")
}

func statText(r: in Result[Option[ObjectHead], StorageError]): String {
  match r {
    case Err(e) -> e.code
    case Ok(None) -> "absent"
    case Ok(Some(h)) -> {
      val owner = match mapGet(h.userMeta, "owner") { case Some(v) -> v; case None -> "-" }
      val note = match mapGet(h.userMeta, "build.note_1") { case Some(v) -> v; case None -> "-" }
      h.contentType + "/" + h.etag.length.toString() + "/" + h.lastModified + "/" + h.storageClass + "/" + owner + "/" +
        note +
        "/" +
        h.userMeta.count.toString()
    }
  }
}

func metadataScenario(endpoint: in String): String {
  val client = clientFor(endpoint, "meta")
  val bucket = bucketOf(client)
  val meta: Map[String, String] = newMap()
  meta["owner"] = "alice"
  meta["build.note_1"] = "two words, one comma"
  var text = "put=" + codeOf(putBytes(client, "m/doc", [7u8], "application/pdf", meta))
  text = text + "\nstat=" + statText(stat(client, "m/doc"))
  text = text + "\nabsent=" + statText(stat(client, "m/none"))
  text = text + "\nclass=" + codeOf(putBytesWithStorageClass(client, "m/cold", [8u8], "text/plain", "STANDARD_IA"))
  text = text + "\nclassStat=" + statText(stat(client, "m/cold"))
  text = text + "\nbadClass=" + codeOf(putBytesWithStorageClass(client, "m/x", [8u8], "text/plain", "FROZEN"))
  // Metadata S3 would alter is refused before any request.
  val upper: Map[String, String] = newMap()
  upper["Owner"] = "alice"
  text = text + "\nupperKey=" +
    (match bucket.put("m/u", "", "text/plain", upper) { case Ok(_) -> "ok"; case Err(e) -> e.code })
  val unicode: Map[String, String] = newMap()
  unicode["owner"] = "café"
  text = text + "\nunicodeValue=" +
    (match bucket.put("m/v", "", "text/plain", unicode) { case Ok(_) -> "ok"; case Err(e) -> e.code })
  val padded: Map[String, String] = newMap()
  padded["owner"] = " alice"
  text = text + "\npaddedValue=" +
    (match bucket.put("m/p", "", "text/plain", padded) { case Ok(_) -> "ok"; case Err(e) -> e.code })
  text = text + "\nexists=" + (match bucket.exists("m/u") { case Ok(b) -> b.toString(); case Err(e) -> e.code })
  text + "\n" + stopFake(endpoint)
}

test "user metadata travels as x-amz-meta-* headers, and storage classes as x-amz-storage-class" {
  val lines = Str.split(withFakeS3(19473, metadataScenario), "\n")
  assertEqualInt(lines.length, 11, "eleven lines: " + Str.join(" | ", lines))
  assertTrue(lines[0].startsWith("put=ok:"), "put with metadata: " + lines[0])
  assertEqual(
    lines[1],
    "stat=application/pdf/32/2026-11-12T10:15:00Z/STANDARD/alice/two words, one comma/2",
    "HEAD reports the content type, ETag, date, class and both metadata entries"
  )
  assertEqual(lines[2], "absent=absent", "HEAD of a missing key is None, not an error")
  assertTrue(lines[3].startsWith("class=ok:"), "put with a storage class: " + lines[3])
  assertEqual(lines[4], "classStat=text/plain/32/2026-11-12T10:15:00Z/STANDARD_IA/-/-/0", "the class was stored")
  assertEqual(lines[5], "badClass=INVALID_ARGUMENT", "an unknown storage class is refused locally")
  assertEqual(lines[6], "upperKey=USER_META_UNSUPPORTED", "S3 would lowercase the key")
  assertEqual(lines[7], "unicodeValue=USER_META_UNSUPPORTED", "a header cannot carry non-ASCII intact")
  assertEqual(lines[8], "paddedValue=USER_META_UNSUPPORTED", "a header would lose the leading space")
  assertEqual(lines[9], "exists=false", "nothing was stored by the refused puts")
  assertEqual(lines[10], "served 6", "the refused puts made no request")
}

func getCode(bucket: in StorageBucket, key: in String): String {
  match bucket.get(key) {
    case Ok(_) -> "ok"
    case Err(e) -> e.code
  }
}

func errorScenario(endpoint: in String): String {
  val good = bucketOf(clientFor(endpoint, "errors"))
  val noMeta: Map[String, String] = newMap()
  var text = "missing=" + getCode(good, "no/such/key")
  text = text + "\ninternal=" + getCode(good, "boom/internal")
  text = text + "\ngateway=" + getCode(good, "boom/gateway")
  text = text + "\nmoved=" + getCode(good, "boom/moved")
  text = text + "\nmessage=" + (match good.get("boom/internal") { case Ok(_) -> "ok"; case Err(e) -> e.message })
  val wrongSecret = match connect(endpointConfig(endpoint, "errors", REGION, ACCESS_KEY, "not-the-secret")) {
    case Ok(b) -> b
    case Err(e) -> panic("connect failed: " + e.message)
  }
  text = text + "\nwrongSecret=" + getCode(wrongSecret, "k")
  text = text + "\nwrongSecretPut=" +
    (match wrongSecret.put("k", "aGk=", "text/plain", noMeta) { case Ok(_) -> "ok"; case Err(e) -> e.code })
  text = text + "\nwrongSecretExists=" +
    (match wrongSecret.exists("k") { case Ok(b) -> b.toString(); case Err(e) -> e.code })
  val wrongKeyId = match connect(endpointConfig(endpoint, "errors", REGION, "AKIANOTKNOWN", SECRET_KEY)) {
    case Ok(b) -> b
    case Err(e) -> panic("connect failed: " + e.message)
  }
  text = text + "\nwrongKeyId=" + getCode(wrongKeyId, "k")
  val wrongRegion = match connect(endpointConfig(endpoint, "errors", "us-east-1", ACCESS_KEY, SECRET_KEY)) {
    case Ok(b) -> b
    case Err(e) -> panic("connect failed: " + e.message)
  }
  text = text + "\nwrongRegion=" + getCode(wrongRegion, "k")
  val noBucket = bucketOf(clientFor(endpoint, "missing-bucket"))
  text = text + "\nnoBucket=" + getCode(noBucket, "k")
  text = text + "\nnoBucketList=" + (match noBucket.list("", "", 10) { case Ok(_) -> "ok"; case Err(e) -> e.code })
  text = text + "\nbadToken=" +
    (match good.list("", "zz-not-a-token", 10) { case Ok(_) -> "ok"; case Err(e) -> e.code })
  text = text + "\nstillWorks=" +
    (match good.put("fine", "aGk=", "text/plain", noMeta) { case Ok(_) -> "ok"; case Err(e) -> e.code })
  text + "\n" + stopFake(endpoint)
}

test "S3 error documents map to StorageError codes, and a redirect is not followed" {
  val lines = Str.split(withFakeS3(19474, errorScenario), "\n")
  assertEqualInt(lines.length, 15, "fifteen lines: " + Str.join(" | ", lines))
  assertEqual(lines[0], "missing=NOT_FOUND", "NoSuchKey is the interface's NOT_FOUND")
  assertEqual(lines[1], "internal=InternalError", "a 500 with an Error document carries its Code")
  assertEqual(lines[2], "gateway=HTTP_502", "a non-S3 error page is HTTP_<status>")
  assertEqual(lines[3], "moved=PermanentRedirect", "a 301 is reported, not followed to another host")
  assertEqual(
    lines[4],
    "message=cannot get object 'boom/internal': InternalError (HTTP 500): We encountered an internal error. Please try again.",
    "the message names the operation, the code, the status and S3's own message"
  )
  assertEqual(lines[5], "wrongSecret=SignatureDoesNotMatch", "a wrong secret fails the server's signature check on GET")
  assertEqual(lines[6], "wrongSecretPut=SignatureDoesNotMatch", "and on PUT")
  assertEqual(lines[7], "wrongSecretExists=HTTP_403", "HEAD has no error body, so only the status is known")
  assertEqual(lines[8], "wrongKeyId=InvalidAccessKeyId", "an unknown access key id")
  assertEqual(lines[9], "wrongRegion=AuthorizationHeaderMalformed", "a credential scope for another region")
  assertEqual(lines[10], "noBucket=NoSuchBucket", "a missing bucket on get")
  assertEqual(lines[11], "noBucketList=NoSuchBucket", "a missing bucket on list")
  assertEqual(lines[12], "badToken=InvalidArgument", "a continuation token the server does not know")
  assertEqual(lines[13], "stillWorks=ok", "the correctly signed client is unaffected")
  assertEqual(lines[14], "served 14", "every request reached the server")
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

func upload(url: in String, bytes: in slice[Byte]): String {
  val parsed = match Url.tryFrom(url) { case Ok(u) -> u; case Err(_) -> return "bad url" }
  val req = match withBytesBody(request(PUT, parsed), "text/plain", bytes) {
    case Ok(r) -> r
    case Err(e) -> return HttpError.message(e)
  }
  match await sendAsync(req) {
    case Err(e) -> "transport " + HttpError.message(e)
    case Ok(resp) -> HttpResponse.statusCode(resp).toString()
  }
}

func presignScenario(endpoint: in String): String {
  val client = clientFor(endpoint, "presign")
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
  // A presigned GET, fetched by a client that has no credentials.
  match bucket.presignedUrl(key, 300) {
    case Err(e) -> {
      text = text + "\nget=" + e.code
    }
    case Ok(url) -> {
      text = text + "\nget=" + fetch(url)
      text = text + "\ntampered=" + fetch(Str.replace(url, "X-Amz-Expires=300", "X-Amz-Expires=3000"))
      text = text + "\notherKey=" + fetch(Str.replace(url, "report", "REPORT"))
    }
  }
  // A URL signed years ago for one minute has expired.
  text = text + "\nexpired=" + fetch(client.presign("GET", key, 60, "20200101T000000Z"))
  // A presigned PUT: upload without credentials, then read it back signed.
  match presignedPutUrl(client, "inbox/upload.txt", 300) {
    case Err(e) -> {
      text = text + "\nputUrl=" + e.code
    }
    case Ok(url) -> {
      text = text + "\nputUrl=" + upload(url, Enc.encodeUtf8("uploaded"))
      // The PUT URL does not authorise a GET.
      text = text + "\nputUrlAsGet=" + fetch(url)
    }
  }
  text = text + "\nuploaded=" +
    (match bucket.get("inbox/upload.txt") { case Ok(obj) -> obj.dataBase64; case Err(e) -> e.code })
  text = text + "\nunsafe=" + codeOf(bucket.presignedUrl("../escape", 60)) + "," +
    codeOf(presignedPutUrl(client, "a/../../b", 60))
  text + "\n" + stopFake(endpoint)
}

test "presigned GET and PUT URLs work for a client without credentials, until they expire" {
  val lines = Str.split(withFakeS3(19475, presignScenario), "\n")
  assertEqualInt(lines.length, 10, "ten lines: " + Str.join(" | ", lines))
  assertEqual(lines[0], "put=ok", "the object is stored")
  assertEqual(lines[1], "get=200 hello world", "the presigned URL returns the object")
  assertEqual(lines[2], "tampered=403 SignatureDoesNotMatch", "changing the expiry invalidates the signature")
  assertEqual(lines[3], "otherKey=403 SignatureDoesNotMatch", "the URL is for one key only")
  assertEqual(lines[4], "expired=403 AccessDenied", "an expired URL is refused")
  assertEqual(lines[5], "putUrl=200", "the presigned PUT stores an upload")
  assertEqual(lines[6], "putUrlAsGet=403 SignatureDoesNotMatch", "a PUT URL is signed for PUT only")
  assertEqual(lines[7], "uploaded=dXBsb2FkZWQ=", "the upload is readable through the bucket")
  assertEqual(lines[8], "unsafe=INVALID_KEY,INVALID_KEY", "unsafe keys are never presigned")
  assertEqual(lines[9], "served 8", "every request reached the server")
}

func boolCode(r: in Result[Bool, StorageError]): String {
  match r {
    case Ok(b) -> b.toString()
    case Err(e) -> e.code
  }
}

func bucketScenario(endpoint: in String): String {
  val fresh = clientFor(endpoint, "fresh-bucket")
  var text = "before=" + boolCode(bucketExists(fresh))
  text = text + "\ncreate=" + boolCode(createBucket(fresh))
  text = text + "\nafter=" + boolCode(bucketExists(fresh))
  text = text + "\nagain=" + boolCode(createBucket(fresh))
  text = text + "\ntaken=" + boolCode(createBucket(clientFor(endpoint, "taken-bucket")))
  text + "\n" + stopFake(endpoint)
}

test "createBucket names the region, and tells created from already owned" {
  val lines = Str.split(withFakeS3(19476, bucketScenario), "\n")
  assertEqualInt(lines.length, 6, "six lines: " + Str.join(" | ", lines))
  assertEqual(lines[0], "before=false", "HeadBucket of a bucket that does not exist")
  assertEqual(lines[1], "create=true", "created, with the LocationConstraint the server requires")
  assertEqual(lines[2], "after=true", "HeadBucket afterwards")
  assertEqual(lines[3], "again=false", "BucketAlreadyOwnedByYou is not an error")
  assertEqual(lines[4], "taken=BucketAlreadyExists", "a name owned by someone else is")
  assertEqual(lines[5], "served 5", "every request reached the server")
}

// ─── Configuration (no server) ───────────────────────────────────────────────
func openCode(cfg: in S3Config): String {
  match openClient(cfg) {
    case Ok(_) -> "ok"
    case Err(e) -> e.code + ": " + e.message
  }
}

test "addressing: AWS is virtual-hosted, a custom endpoint is path-style" {
  val aws = match openClient(awsConfig("my-bucket", "eu-west-1", "AKIAEXAMPLE", "secret")) {
    case Ok(c) -> c
    case Err(e) -> panic(e.message)
  }
  val awsUrl = aws.presign("GET", "a/b c.txt", 60, "20261112T101500Z")
  assertTrue(
    awsUrl.startsWith("https://my-bucket.s3.eu-west-1.amazonaws.com/a/b%20c.txt?X-Amz-Algorithm=AWS4-HMAC-SHA256&"),
    "virtual-hosted AWS URL: " + awsUrl
  )
  assertTrue(
    awsUrl.contains("&X-Amz-Credential=AKIAEXAMPLE%2F20261112%2Feu-west-1%2Fs3%2Faws4_request&"),
    "the credential scope names the region: " + awsUrl
  )
  val awsPath = match openClient(awsConfig("my.dotted.bucket", "us-east-1", "AKIAEXAMPLE", "secret").withPathStyle(true)) {
    case Ok(c) -> c
    case Err(e) -> panic(e.message)
  }
  assertTrue(
    awsPath.presign("GET", "k", 60, "20261112T101500Z").startsWith(
      "https://s3.us-east-1.amazonaws.com/my.dotted.bucket/k?"
    ),
    "path-style on AWS, for a bucket name a TLS wildcard cannot cover"
  )
  val minio = match openClient(endpointConfig("https://minio.example.com:9000/", "data", "us-east-1", "minio", "secret")) {
    case Ok(c) -> c
    case Err(e) -> panic(e.message)
  }
  assertTrue(
    minio.presign("PUT", "k", 60, "20261112T101500Z").startsWith("https://minio.example.com:9000/data/k?"),
    "a custom endpoint is path-style, trailing slash dropped"
  )
  val hosted = match openClient(endpointConfig("https://objects.example.com:443", "data", "auto", "id", "secret").withPathStyle(false)) {
    case Ok(c) -> c
    case Err(e) -> panic(e.message)
  }
  assertTrue(
    hosted.presign("GET", "k", 60, "20261112T101500Z").startsWith("https://data.objects.example.com/k?"),
    "virtual-hosted on a custom endpoint, default port dropped so the signed host is the Host header"
  )
  val token = match openClient(awsConfig("my-bucket", "eu-west-1", "ASIAEXAMPLE", "secret").withSessionToken("tok")) {
    case Ok(c) -> c
    case Err(e) -> panic(e.message)
  }
  assertTrue(
    token.presign("GET", "k", 60, "20261112T101500Z").contains("&X-Amz-Security-Token=tok&"),
    "temporary credentials put the session token in a presigned URL"
  )
}

test "configuration is validated before any request" {
  val ok = endpointConfig("http://127.0.0.1:9000", "data", "us-east-1", "id", "secret")
  assertEqual(openCode(ok), "ok", "plain http is accepted for loopback")
  assertEqual(openCode(endpointConfig("http://localhost:9000", "data", "us-east-1", "id", "secret")), "ok", "localhost")
  assertEqual(openCode(endpointConfig("http://[::1]:9000", "data", "us-east-1", "id", "secret")), "ok", "IPv6 loopback")
  assertTrue(
    openCode(endpointConfig("http://minio.internal:9000", "data", "us-east-1", "id", "secret")).startsWith(
      "INVALID_CONFIG: S3 endpoint must use https://"
    ),
    "plain http to another host would expose the request"
  )
  assertTrue(
    openCode(endpointConfig("http://127.0.0.1.evil.example", "data", "us-east-1", "id", "secret")).startsWith(
      "INVALID_CONFIG: S3 endpoint must use https://"
    ),
    "a host that merely starts with a loopback address is not loopback"
  )
  assertTrue(
    openCode(endpointConfig("ftp://example.com", "data", "us-east-1", "id", "secret")).startsWith("INVALID_CONFIG"),
    "not http(s)"
  )
  assertTrue(
    openCode(endpointConfig("https://example.com/prefix", "data", "us-east-1", "id", "secret")).startsWith(
      "INVALID_CONFIG"
    ),
    "an endpoint with a path"
  )
  assertTrue(
    openCode(endpointConfig("https://user:pw@example.com", "data", "us-east-1", "id", "secret")).startsWith(
      "INVALID_CONFIG"
    ),
    "an endpoint with credentials"
  )
  assertTrue(
    openCode(awsConfig("My_Bucket", "us-east-1", "id", "secret")).startsWith("INVALID_CONFIG"),
    "bucket name case"
  )
  assertTrue(
    openCode(awsConfig("ab", "us-east-1", "id", "secret")).startsWith("INVALID_CONFIG"),
    "bucket name too short"
  )
  assertTrue(
    openCode(awsConfig("a/b/c", "us-east-1", "id", "secret")).startsWith("INVALID_CONFIG"),
    "bucket name with a slash"
  )
  assertTrue(
    openCode(awsConfig("bucket", "us-east-1.evil.example/", "id", "secret")).startsWith("INVALID_CONFIG"),
    "region"
  )
  assertTrue(
    openCode(awsConfig("bucket", "us-east-1", "", "secret")).startsWith("INVALID_CONFIG"),
    "empty access key id"
  )
  assertTrue(openCode(awsConfig("bucket", "us-east-1", "id", "")).startsWith("INVALID_CONFIG"), "empty secret")
  assertTrue(
    openCode(awsConfig("bucket", "us-east-1", "id", "secret").withRequestTimeout(0)).startsWith("INVALID_CONFIG"),
    "a zero timeout"
  )
}

test "a request to a server that is not there is CONNECTION_FAILED, not a panic" {
  val bucket = match connect(endpointConfig("http://127.0.0.1:1", "data", "us-east-1", "id", "secret").withRequestTimeout(5000)) {
    case Ok(b) -> b
    case Err(e) -> panic(e.message)
  }
  val noMeta: Map[String, String] = newMap()
  assertEqual(getCode(bucket, "k"), "CONNECTION_FAILED", "get")
  match bucket.put("k", "aGk=", "text/plain", noMeta) {
    case Ok(_) -> panic("nothing listens on port 1")
    case Err(e) -> assertEqual(e.code, "CONNECTION_FAILED", "put")
  }
  match bucket.exists("k") {
    case Ok(_) -> panic("nothing listens on port 1")
    case Err(e) -> assertEqual(e.code, "CONNECTION_FAILED", "exists is an error, not false")
  }
  match bucket.list("", "", 10) {
    case Ok(_) -> panic("nothing listens on port 1")
    case Err(e) -> assertEqual(e.code, "CONNECTION_FAILED", "list")
  }
  match bucket.delete("k") {
    case Ok(_) -> panic("nothing listens on port 1")
    case Err(e) -> assertEqual(e.code, "CONNECTION_FAILED", "delete")
  }
}

test "user metadata S3 cannot store unchanged is described" {
  assertEqual(userMetaProblem("owner", "alice"), "", "a plain entry")
  assertEqual(userMetaProblem("build.note_1-x", "a b, c; d=e"), "", "the allowed key characters and a printable value")
  assertEqual(userMetaProblem("owner", ""), "", "an empty value")
  assertTrue(userMetaProblem("", "v").length > 0, "an empty key")
  assertTrue(userMetaProblem("Owner", "v").length > 0, "an uppercase key")
  assertTrue(userMetaProblem("own er", "v").length > 0, "a key with a space")
  assertTrue(userMetaProblem("owner", "line\nbreak").length > 0, "a control character in a value")
  assertTrue(userMetaProblem("owner", "trailing ").length > 0, "a trailing space")
}
