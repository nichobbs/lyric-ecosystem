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
//! presigned GET and PUT fetched with a plain HTTP client, storage classes,
//! throttling and failures that are retried or not, credentials that are
//! replaced while a client is in use (the server accepts a second,
//! temporary key and reports which key signed the last request),
//! and the same server over HTTPS (`startListenerTls`) with a fixture
//! certificate from a private CA, for the CA certificate options.
//!
//! The server runs on its own thread: a `Task.Run` worker on `--target dotnet`
//! (`spawn` is synchronous there), a `spawn` inside a `scope` on `--target
//! jvm`, a `Std.Task` scope thread on `--target native`. No assertion runs while it is waiting for a request: a scenario
//! returns its findings and ends by asking the server to stop.
@test_module
package Storage.S3.FakeS3Tests

import Std.Core
import Std.Collections
import Std.Encoding as Enc
import Std.Environment as Env
import Std.Errors
import Std.File as File
import Std.Http
import Std.HttpServer
import Std.String as Str
import Std.Task as StdTask
import Std.Testing
import Std.Time as Time
import Std.Tls as Tls
import Testing.Conformance
import Testing.Conformance.Storage as StorageConformance
import Aws.Credentials
import Aws.Retry
import Storage.S3
import Storage.S3.Extras
import Storage.S3.SigV4
import Storage.S3.Wire.{parseFault}

// ─── The fake server's state ─────────────────────────────────────────────────
val ACCESS_KEY: String = "AKIAFAKEFAKEFAKEFAKE"

val SECRET_KEY: String = "fake/secret+key/for-tests-only"

val REGION: String = "eu-test-1"

// A second, temporary key the server accepts, with its session token.
val ROTATED_KEY: String = "ASIAROTATEDROTATED00"

val ROTATED_SECRET: String = "rotated/secret+key/for-tests-only"

val ROTATED_TOKEN: String = "fixture-rotated-session-token"

val STOP_PATH: String = "/__stop"

// `GET /__attempts/<bucket>/<key>`: how many requests a misbehaving key or
// bucket has received.
val ATTEMPTS_PATH: String = "/__attempts/"

// `GET /__lastkey`: the access key id that signed the last accepted request.
val LAST_KEY_PATH: String = "/__lastkey"

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
  // Requests received per misbehaving key (`<bucket>/<key>`) or bucket.
  var attempts: Map[String, Int]
  var lastKeyId: String
}

func newFakeS3(): FakeS3 {
  FakeS3(objects = newMap(), created = newMap(), requestCount = 0, attempts = newMap(), lastKeyId = "")
}

// How many requests a misbehaving name has had.
func attemptsOn(s3: in FakeS3, name: in String): Int {
  if s3.attempts.containsKey(name) {
    val n: Int = s3.attempts[name]
    n
  } else {
    0
  }
}

// Counts a request to a misbehaving name and returns how many it has had.
func countAttempt(s3: in FakeS3, name: in String): Int {
  val n = attemptsOn(s3, name) + 1
  s3.attempts[name] = n
  n
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
  val rotated = Str.startsWith(credential, ROTATED_KEY + "/")
  val keyId = if rotated { ROTATED_KEY } else { ACCESS_KEY }
  if credential != keyId + "/" + credentialScope(amzDate, REGION, "s3") {
    if not Str.startsWith(credential, keyId + "/") {
      return "InvalidAccessKeyId"
    }
    return "AuthorizationHeaderMalformed"
  }
  // Temporary credentials: the session token must be sent, and signed.
  if rotated and (headerValue(ctx, "x-amz-security-token") != ROTATED_TOKEN or not Str.contains(";" + authField(authorization, "SignedHeaders") + ";", ";x-amz-security-token;")) {
    return "InvalidToken"
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
    if rotated {
      Credentials(accessKeyId = ROTATED_KEY, secretAccessKey = ROTATED_SECRET, sessionToken = ROTATED_TOKEN)
    } else {
      fakeCreds()
    },
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
  if key == "boom/denied" {
    val _ = countAttempt(s3, fullKey)
    respondFault(ctx, 403, "AccessDenied", "Access Denied")
    return
  }
  // Throttled twice, then served.
  if key == "flaky/slowdown" and countAttempt(s3, fullKey) <= 2 {
    respondFault(ctx, 503, "SlowDown", "Please reduce your request rate.")
    return
  }
  // Throttled once, with the wait the server wants.
  if key == "flaky/retry-after" and countAttempt(s3, fullKey) == 1 {
    val headers: Map[String, String] = newMap()
    headers.add("Retry-After", "1")
    val text = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>SlowDown</Code><Message>Please reduce your request rate.</Message></Error>"
    respondBytesWithHeaders(ctx, 503, "application/xml", Enc.encodeUtf8(text), headers)
    return
  }
  // Fails once inside the server, then served.
  if key == "flaky/internal-once" and countAttempt(s3, fullKey) == 1 {
    respondFault(ctx, 500, "InternalError", "We encountered an internal error. Please try again.")
    return
  }
  if key == "flaky/always" {
    val _ = countAttempt(s3, fullKey)
    respondFault(ctx, 503, "SlowDown", "Please reduce your request rate.")
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
  val authorization = headerValue(ctx, "Authorization")
  if authorization.length > 0 {
    s3.lastKeyId = if Str.contains(authorization, "Credential=" + ROTATED_KEY + "/") { ROTATED_KEY } else { ACCESS_KEY }
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
    } else if requestMethod(ctx) == "PUT" and bucket == "create-fails-once" and countAttempt(s3, bucket) == 1 {
      respondFault(ctx, 500, "InternalError", "We encountered an internal error. Please try again.")
    } else if requestMethod(ctx) == "PUT" and bucket == "create-throttled-once" and countAttempt(s3, bucket) == 1 {
      respondFault(ctx, 503, "SlowDown", "Please reduce your request rate.")
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
    } else if Str.startsWith(requestPath(ctx), ATTEMPTS_PATH) {
      val name = urlDecode(requestPath(ctx)).substring(ATTEMPTS_PATH.length)
      respondText(ctx, 200, attemptsOn(s3, name).toString())
    } else if requestPath(ctx) == LAST_KEY_PATH {
      respondText(ctx, 200, s3.lastKeyId)
    } else {
      handleGuarded(s3, ctx)
    }
  }
}

// A bug in the fake must surface as a failed request, not as a server thread
// that died and a test run that waits for it. Native has no unwinding
// (D-N-003): there a bug in the fake ends the test process with its message.
@cfg(not(target = "native"))
func handleGuarded(s3: in FakeS3, ctx: in HttpContext): Unit {
  try {
    handle(s3, ctx)
  } catch Bug as b {
    respondFault(ctx, 500, "FakeServerBug", b.message)
  }
}

@cfg(target = "native")
func handleGuarded(s3: in FakeS3, ctx: in HttpContext): Unit {
  handle(s3, ctx)
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

@cfg(target = "native")
func withFakeS3(port: in Int, scenario: in (String) -> String): String {
  val l = startListener(endpointOf(port) + "/")
  val server = StdTask.makeScope()
  StdTask.scopeSpawn(server, { -> serveUntilStopped(l) })
  val transcript = scenario(endpointOf(port))
  val _ = StdTask.awaitAll(server)
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

// `S3Client.presign` as text: the URL, or the error code.
func presigned(client: in S3Client, methodName: in String, key: in String, seconds: in Int, amzDate: in String): String {
  match client.presign(methodName, key, seconds, amzDate) {
    case Ok(url) -> url
    case Err(e) -> e.code + " " + e.message
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
  // The two GETs of boom/internal and the one of boom/gateway are idempotent
  // and answered 5xx: each is attempted three times.
  assertEqual(lines[14], "served 20", "every request reached the server, the 5xx answers three times each")
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
  text = text + "\nexpired=" + fetch(presigned(client, "GET", key, 60, "20200101T000000Z"))
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
  val awsUrl = presigned(aws, "GET", "a/b c.txt", 60, "20261112T101500Z")
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
    presigned(awsPath, "GET", "k", 60, "20261112T101500Z").startsWith(
      "https://s3.us-east-1.amazonaws.com/my.dotted.bucket/k?"
    ),
    "path-style on AWS, for a bucket name a TLS wildcard cannot cover"
  )
  val minio = match openClient(endpointConfig("https://minio.example.com:9000/", "data", "us-east-1", "minio", "secret")) {
    case Ok(c) -> c
    case Err(e) -> panic(e.message)
  }
  assertTrue(
    presigned(minio, "PUT", "k", 60, "20261112T101500Z").startsWith("https://minio.example.com:9000/data/k?"),
    "a custom endpoint is path-style, trailing slash dropped"
  )
  val hosted = match openClient(endpointConfig("https://objects.example.com:443", "data", "auto", "id", "secret").withPathStyle(false)) {
    case Ok(c) -> c
    case Err(e) -> panic(e.message)
  }
  assertTrue(
    presigned(hosted, "GET", "k", 60, "20261112T101500Z").startsWith("https://data.objects.example.com/k?"),
    "virtual-hosted on a custom endpoint, default port dropped so the signed host is the Host header"
  )
  val token = match openClient(awsConfig("my-bucket", "eu-west-1", "ASIAEXAMPLE", "secret").withSessionToken("tok")) {
    case Ok(c) -> c
    case Err(e) -> panic(e.message)
  }
  assertTrue(
    presigned(token, "GET", "k", 60, "20261112T101500Z").contains("&X-Amz-Security-Token=tok&"),
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

// ─── TLS ─────────────────────────────────────────────────────────────────────
// TEST FIXTURES, NOT SECRETS. The certificates and the private key below exist
// only for the hermetic TLS tests of the AWS backends in this repository (the
// same four are in mq-sqs/tests/fake_sqs_tests.l): a throwaway CA, a server
// certificate for `localhost` / `127.0.0.1` signed by it, and a second CA that
// signed nothing. They protect nothing and are valid until 2126. Made with:
//
//   openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out X.key
//   openssl req -x509 -new -key ca.key -sha256 -days 36500 \
//     -subj "/O=Lyric AWS backends test fixture/CN=test fixture CA (not a secret)" \
//     -addext "basicConstraints=critical,CA:TRUE" -addext "keyUsage=critical,keyCertSign,cRLSign" -out ca.crt
//   openssl req -new -key server.key -subj "/O=Lyric AWS backends test fixture/CN=localhost" -out server.csr
//   openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial -sha256 -days 36500 \
//     -extfile server.ext -out server.crt      (subjectAltName=DNS:localhost,IP:127.0.0.1)
val FIXTURE_CA_PEM: String = "-----BEGIN CERTIFICATE-----\nMIICDjCCAbOgAwIBAgIUehV33rhTgQ0MIpdI/2vALz16WHIwCgYIKoZIzj0EAwIw\nUzEoMCYGA1UECgwfTHlyaWMgQVdTIGJhY2tlbmRzIHRlc3QgZml4dHVyZTEnMCUG\nA1UEAwwedGVzdCBmaXh0dXJlIENBIChub3QgYSBzZWNyZXQpMCAXDTI2MTExMzA3\nNDkzNFoYDzIxMjYxMDIwMDc0OTM0WjBTMSgwJgYDVQQKDB9MeXJpYyBBV1MgYmFj\na2VuZHMgdGVzdCBmaXh0dXJlMScwJQYDVQQDDB50ZXN0IGZpeHR1cmUgQ0EgKG5v\ndCBhIHNlY3JldCkwWTATBgcqhkjOPQIBBggqhkjOPQMBBwNCAASlTjPdK05iaHbC\nP8/DRkaOkAbSHlo/5usXvOpQK4ebMwgH+6+sjVY+4srGM/5gY1Ad27A2BJVQ4IrQ\njFAm9zMYo2MwYTAdBgNVHQ4EFgQUJhRJqHEzNL474DOA1wtcBP8LM94wHwYDVR0j\nBBgwFoAUJhRJqHEzNL474DOA1wtcBP8LM94wDwYDVR0TAQH/BAUwAwEB/zAOBgNV\nHQ8BAf8EBAMCAQYwCgYIKoZIzj0EAwIDSQAwRgIhAKru9e/hbUoMCQ9/Nz11sj+1\nWfzr4m0/s1z1FMKPKGB7AiEA+lt6MMiDNToff6ngfv7m8ztBYUsHlQoF5Kq7izgn\nOdg=\n-----END CERTIFICATE-----\n"

val FIXTURE_SERVER_CERT_PEM: String = "-----BEGIN CERTIFICATE-----\nMIICJTCCAcugAwIBAgIUby5WyizAQTgdThfE+RT04AjHDyMwCgYIKoZIzj0EAwIw\nUzEoMCYGA1UECgwfTHlyaWMgQVdTIGJhY2tlbmRzIHRlc3QgZml4dHVyZTEnMCUG\nA1UEAwwedGVzdCBmaXh0dXJlIENBIChub3QgYSBzZWNyZXQpMCAXDTI2MTExMzA3\nNDkzNFoYDzIxMjYxMDIwMDc0OTM0WjA+MSgwJgYDVQQKDB9MeXJpYyBBV1MgYmFj\na2VuZHMgdGVzdCBmaXh0dXJlMRIwEAYDVQQDDAlsb2NhbGhvc3QwWTATBgcqhkjO\nPQIBBggqhkjOPQMBBwNCAASqrA4/CynXzAIAQGdvnp7GPHGthl3P6qJ2qZfxM/2p\nHzQlu8UAdbIRSPdi6tD0454iL9e5ldRxVJ3e1Pig3W1Vo4GPMIGMMAkGA1UdEwQC\nMAAwDgYDVR0PAQH/BAQDAgeAMBMGA1UdJQQMMAoGCCsGAQUFBwMBMBoGA1UdEQQT\nMBGCCWxvY2FsaG9zdIcEfwAAATAdBgNVHQ4EFgQU/je8BKSYH9HeLVMBa18JsP8y\nhLAwHwYDVR0jBBgwFoAUJhRJqHEzNL474DOA1wtcBP8LM94wCgYIKoZIzj0EAwID\nSAAwRQIhAM5g73n49syQr/I2s+6Vsfs0Ojw2kwxAnv8zsswLxIw+AiAcN2KJd/i/\nxMz8T83NmZfFivdfE10h/SkK51VsHIttfg==\n-----END CERTIFICATE-----\n"

val FIXTURE_SERVER_KEY_PEM: String = "-----BEGIN PRIVATE KEY-----\nMIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgtKwkAL49aCgKU8Be\nefwIxJ9fk9Vp3UQnXex9b9hiCwyhRANCAASqrA4/CynXzAIAQGdvnp7GPHGthl3P\n6qJ2qZfxM/2pHzQlu8UAdbIRSPdi6tD0454iL9e5ldRxVJ3e1Pig3W1V\n-----END PRIVATE KEY-----\n"

// A CA that did not sign the server certificate.
val FIXTURE_OTHER_CA_PEM: String = "-----BEGIN CERTIFICATE-----\nMIICHTCCAcOgAwIBAgIUARJtxHD1seTCMo8Yy05/b8YDT2EwCgYIKoZIzj0EAwIw\nWzEoMCYGA1UECgwfTHlyaWMgQVdTIGJhY2tlbmRzIHRlc3QgZml4dHVyZTEvMC0G\nA1UEAwwmYW5vdGhlciB0ZXN0IGZpeHR1cmUgQ0EgKG5vdCBhIHNlY3JldCkwIBcN\nMjYxMTEzMDc0OTM0WhgPMjEyNjEwMjAwNzQ5MzRaMFsxKDAmBgNVBAoMH0x5cmlj\nIEFXUyBiYWNrZW5kcyB0ZXN0IGZpeHR1cmUxLzAtBgNVBAMMJmFub3RoZXIgdGVz\ndCBmaXh0dXJlIENBIChub3QgYSBzZWNyZXQpMFkwEwYHKoZIzj0CAQYIKoZIzj0D\nAQcDQgAEj31puhUVhDm9h62ooQv3xD+A+VxMIWBBPMjPG2QLQXiPCf9hSPIVAxV8\nq4VC09dnHKzwuYtonrrvVnsr52vOm6NjMGEwHQYDVR0OBBYEFJmzXefGlc3B9qWW\nae54NCLQy6LHMB8GA1UdIwQYMBaAFJmzXefGlc3B9qWWae54NCLQy6LHMA8GA1Ud\nEwEB/wQFMAMBAf8wDgYDVR0PAQH/BAQDAgEGMAoGCCqGSM49BAMCA0gAMEUCIQDU\nauGTenHbZvZ5bCf8/JkpOShiIvdCuqhdF6seQWKltQIgGdUI0OPFU9Y599Q3sX0Y\nS4bDv5i7emOh6NvklppE0fo=\n-----END CERTIFICATE-----\n"

func tlsEndpointOf(port: in Int): String {
  "https://127.0.0.1:" + port.toString()
}

func startFakeTls(port: in Int): HttpListener {
  val identity = match Tls.Identity.fromPem(FIXTURE_SERVER_CERT_PEM, FIXTURE_SERVER_KEY_PEM) {
    case Ok(i) -> i
    case Err(e) -> panic("fixture identity: " + Tls.TlsError.message(e))
  }
  match startListenerTls("127.0.0.1", port, Tls.TlsServerConfig(identity = identity)) {
    case Ok(l) -> l
    case Err(_) -> panic("cannot start the HTTPS fake on port " + port.toString())
  }
}

@cfg(target = "dotnet")
func withFakeS3Tls(port: in Int, scenario: in (String) -> String): String {
  val l = startFakeTls(port)
  val server = taskRun({ -> serveUntilStopped(l) })
  val transcript = scenario(tlsEndpointOf(port))
  taskWait(server)
  stopListener(l)
  transcript
}

@cfg(target = "native")
func withFakeS3Tls(port: in Int, scenario: in (String) -> String): String {
  val l = startFakeTls(port)
  val server = StdTask.makeScope()
  StdTask.scopeSpawn(server, { -> serveUntilStopped(l) })
  val transcript = scenario(tlsEndpointOf(port))
  val _ = StdTask.awaitAll(server)
  stopListener(l)
  transcript
}

@cfg(target = "jvm")
func withFakeS3Tls(port: in Int, scenario: in (String) -> String): String {
  val l = startFakeTls(port)
  var transcript = ""
  scope {
    val server = spawn serveUntilStopped(l)
    transcript = scenario(tlsEndpointOf(port))
    await server
  }
  stopListener(l)
  transcript
}

// `stopFake` for the HTTPS fake: a plain client that trusts the fixture CA.
func stopFakeTls(endpoint: in String): String {
  val ca = match Tls.Certificate.fromPem(FIXTURE_CA_PEM) {
    case Ok(c) -> c
    case Err(e) -> return "stop failed: " + Tls.TlsError.message(e)
  }
  match await HttpClientBuilder.new().withCaCertificate(ca).build().get(endpoint + STOP_PATH) {
    case Err(e) -> "stop failed: " + HttpError.message(e)
    case Ok(resp) -> match await HttpResponse.bodyText(resp) { case Ok(text) -> text; case Err(_) -> "stop unreadable" }
  }
}

// Stores an object and reads it back through `cfg`: `ok`, or the error.
func roundTrip(cfg: in S3Config, key: in String): String {
  val bucket = match connect(cfg) { case Ok(b) -> b; case Err(e) -> return e.code + " " + e.message }
  val noMeta: Map[String, String] = newMap()
  match bucket.put(key, "aGk=", "text/plain", noMeta) {
    case Err(e) -> return e.code + " " + e.message
    case Ok(_) -> ()
  }
  match bucket.get(key) {
    case Err(e) -> e.code + " " + e.message
    case Ok(obj) -> if obj.dataBase64 == "aGk=" { "ok" } else { "different" }
  }
}

// `Std.Tls` reads the first certificate of a PEM text on dotnet and the JVM and
// all of them on native (OpenSSL).
@cfg(not(target = "native"))
func wholePemTextIsTrusted(): Bool {
  false
}

@cfg(target = "native")
func wholePemTextIsTrusted(): Bool {
  true
}

func tlsScenario(endpoint: in String): String {
  val base = configFor(endpoint, "tls")
  var text = "additive=" + roundTrip(base.withCaCertificate(FIXTURE_CA_PEM), "a")
  text = text + "\nexclusive=" + roundTrip(base.withExclusiveCaCertificate(FIXTURE_CA_PEM), "b")
  text = text + "\nexclusiveWins=" +
    roundTrip(base.withCaCertificate(FIXTURE_OTHER_CA_PEM).withExclusiveCaCertificate(FIXTURE_CA_PEM), "c")
  text = text + "\nsystemOnly=" + roundTrip(base, "d")
  text = text + "\notherAdditive=" + roundTrip(base.withCaCertificate(FIXTURE_OTHER_CA_PEM), "e")
  text = text + "\notherExclusive=" + roundTrip(base.withExclusiveCaCertificate(FIXTURE_OTHER_CA_PEM), "f")
  // The refused handshakes did not break the server or reach it.
  text = text + "\nafter=" + roundTrip(base.withCaCertificate(FIXTURE_CA_PEM), "g")
  // A PEM text with several certificates: the first one is the one trusted
  // on dotnet and the JVM, every one of them on native.
  text = text + "\nbundleSecond=" + roundTrip(base.withCaCertificate(FIXTURE_OTHER_CA_PEM + FIXTURE_CA_PEM), "h")
  text = text + "\nbundleFirst=" + roundTrip(base.withCaCertificate(FIXTURE_CA_PEM + FIXTURE_OTHER_CA_PEM), "i")
  text + "\n" + stopFakeTls(endpoint)
}

test "an HTTPS server with a private CA is reachable with withCaCertificate, and refused without it" {
  val lines = Str.split(withFakeS3Tls(19477, tlsScenario), "\n")
  assertEqualInt(lines.length, 10, "ten lines: " + Str.join(" | ", lines))
  assertEqual(lines[0], "additive=ok", "the CA added to the system trust store")
  assertEqual(lines[1], "exclusive=ok", "the CA as the only trusted authority")
  assertEqual(lines[2], "exclusiveWins=ok", "withExclusiveCaCertificate wins over withCaCertificate")
  assertTrue(lines[3].startsWith("systemOnly=CONNECTION_FAILED "), "the system trust store alone: " + lines[3])
  assertTrue(lines[4].startsWith("otherAdditive=CONNECTION_FAILED "), "a CA that did not sign it: " + lines[4])
  assertTrue(lines[5].startsWith("otherExclusive=CONNECTION_FAILED "), "only a CA that did not sign it: " + lines[5])
  assertEqual(lines[6], "after=ok", "the server still serves after refused handshakes")
  if wholePemTextIsTrusted() {
    assertEqual(lines[7], "bundleSecond=ok", "native trusts every certificate of the PEM text")
  } else {
    assertTrue(
      lines[7].startsWith("bundleSecond=CONNECTION_FAILED "),
      "the CA as the second certificate of the PEM text is not read: " + lines[7]
    )
  }
  assertEqual(lines[8], "bundleFirst=ok", "the first certificate of the PEM text is trusted")
  // A round trip is two signed requests; nothing but the trusted clients'
  // requests got through.
  assertEqual(
    lines[9],
    if wholePemTextIsTrusted() { "served 12" } else { "served 10" },
    "only the trusted clients' requests reached the server"
  )
}

test "a CA certificate that is not a PEM certificate is INVALID_CONFIG before any request" {
  val base = endpointConfig("https://127.0.0.1:1", "data", "us-east-1", "id", "secret")
  val notPem = openCode(base.withCaCertificate("not a certificate"))
  assertTrue(notPem.startsWith("INVALID_CONFIG"), "additive, not PEM: " + notPem)
  assertTrue(Str.contains(notPem, "withCaCertificate"), "the message names the option: " + notPem)
  val exclusive = openCode(
    base.withExclusiveCaCertificate("-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n")
  )
  assertTrue(exclusive.startsWith("INVALID_CONFIG"), "exclusive, not a certificate: " + exclusive)
  assertTrue(Str.contains(exclusive, "withExclusiveCaCertificate"), "the message names the option: " + exclusive)
  assertTrue(
    openCode(base.withCaCertificate(FIXTURE_SERVER_KEY_PEM)).startsWith("INVALID_CONFIG"),
    "a private key is not a certificate"
  )
  assertEqual(openCode(base.withCaCertificate(FIXTURE_CA_PEM)), "ok", "a real certificate opens")
  // On a loopback http:// endpoint the option has nothing to verify, but is
  // still validated.
  val plain = endpointConfig("http://127.0.0.1:1", "data", "us-east-1", "id", "secret")
  assertTrue(openCode(plain.withCaCertificate("nope")).startsWith("INVALID_CONFIG"), "validated on http:// too")
  assertEqual(openCode(plain.withCaCertificate(FIXTURE_CA_PEM)), "ok", "and accepted there")
}

func fileCode(r: in Result[S3Config, StorageError]): String {
  match r {
    case Ok(cfg) -> "ok " + cfg.additionalCaCertificatePem.length.toString() + "/" +
      cfg.caCertificatePem.length.toString()
    case Err(e) -> e.code + " " + e.message
  }
}

test "withCaCertificateFile reads a PEM file, and a missing or empty one is INVALID_CONFIG" {
  val base = endpointConfig("https://127.0.0.1:1", "data", "us-east-1", "id", "secret")
  val dir = Env.getVarOrDefault("TMPDIR", "/tmp")
  val path = dir + "/lyric-s3-fixture-ca-" + Time.nowEpochMillis().toString() + ".pem"
  match File.writeText(path, FIXTURE_CA_PEM) {
    case Ok(_) -> ()
    case Err(_) -> panic("cannot write " + path)
  }
  val n = FIXTURE_CA_PEM.length.toString()
  assertEqual(fileCode(base.withCaCertificateFile(path)), "ok " + n + "/0", "the additive form")
  assertEqual(fileCode(base.withExclusiveCaCertificateFile(path)), "ok 0/" + n, "the exclusive form")
  match File.writeText(path, "") {
    case Ok(_) -> ()
    case Err(_) -> panic("cannot write " + path)
  }
  assertEqual(
    fileCode(base.withCaCertificateFile(path)),
    "INVALID_CONFIG the CA certificate file is empty: " + path,
    "an empty file"
  )
  val _ = File.deleteFile(path)
  assertEqual(
    fileCode(base.withCaCertificateFile(path)),
    "INVALID_CONFIG cannot read the CA certificate: " + path,
    "a missing file"
  )
  assertEqual(
    fileCode(base.withExclusiveCaCertificateFile("")),
    "INVALID_CONFIG the path of the CA certificate is empty",
    "an empty path"
  )
}

// ─── Retries ─────────────────────────────────────────────────────────────────
// The standard policy with its waits recorded instead of slept, and the
// random source at the top of its range: the waits are 100 ms, then 200 ms.
func recordingRetry(sleeps: in List[Int]): RetryPolicy {
  standardRetry().withSleep({ ms: Int -> sleeps.add(ms) }).withRandom({ -> 1.0 })
}

func control(endpoint: in String, path: in String): String {
  match await defaultClient().get(endpoint + path) {
    case Err(e) -> "failed: " + HttpError.message(e)
    case Ok(resp) -> match await HttpResponse.bodyText(resp) { case Ok(text) -> text; case Err(_) -> "unreadable" }
  }
}

func attemptsOf(endpoint: in String, name: in String): String {
  control(endpoint, ATTEMPTS_PATH + name)
}

func sleepsText(sleeps: in List[Int]): String {
  val acc: List[String] = newList()
  for ms in sleeps {
    acc.add(ms.toString())
  }
  Str.joinList(",", acc)
}

func retryScenario(endpoint: in String): String {
  val acc: List[String] = newList()
  val sleeps: List[Int] = newList()
  val noMeta: Map[String, String] = newMap()
  val cfg = configFor(endpoint, "retries").withRetry(recordingRetry(sleeps))
  val client = match openClient(cfg) { case Ok(c) -> c; case Err(e) -> panic(e.message) }
  val bucket = bucketOf(client)
  // Throttled twice, then stored: the caller sees success.
  acc.add(
    "slowdown=" +
      (match bucket.put("flaky/slowdown", "aGk=", "text/plain", noMeta) { case Ok(_) -> "ok"; case Err(e) -> e.code }) +
      " attempts=" +
      attemptsOf(endpoint, "retries/flaky/slowdown") +
      " waits=" +
      sleepsText(sleeps)
  )
  acc.add("stored=" + (match bucket.get("flaky/slowdown") { case Ok(o) -> o.dataBase64; case Err(e) -> e.code }))
  sleeps.clear()
  // The server names the wait: it replaces a shorter backoff.
  acc.add(
    "retryAfter=" + getCode(bucket, "flaky/retry-after") + " attempts=" +
      attemptsOf(endpoint, "retries/flaky/retry-after") +
      " waits=" +
      sleepsText(sleeps)
  )
  sleeps.clear()
  // A 500 on an idempotent request is retried.
  acc.add(
    "internalOnce=" + getCode(bucket, "flaky/internal-once") + " attempts=" +
      attemptsOf(endpoint, "retries/flaky/internal-once") +
      " waits=" +
      sleepsText(sleeps)
  )
  sleeps.clear()
  // Throttled every time: three attempts, then the error.
  acc.add(
    "always=" + getCode(bucket, "flaky/always") + " attempts=" + attemptsOf(endpoint, "retries/flaky/always") +
      " waits=" +
      sleepsText(sleeps)
  )
  sleeps.clear()
  // An error that would be the same next time is not retried.
  acc.add(
    "denied=" + getCode(bucket, "boom/denied") + " attempts=" + attemptsOf(endpoint, "retries/boom/denied") + " waits=" +
      sleepsText(sleeps)
  )
  acc.add("missing=" + getCode(bucket, "no/such/key") + " waits=" + sleepsText(sleeps))
  // Retries switched off: one attempt.
  val once = match connect(configFor(endpoint, "once").withoutRetries()) {
    case Ok(b) -> b
    case Err(e) -> panic(e.message)
  }
  acc.add("withoutRetries=" + getCode(once, "flaky/always") + " attempts=" + attemptsOf(endpoint, "once/flaky/always"))
  // More attempts than the default.
  val five = match connect(configFor(endpoint, "five").withRetry(recordingRetry(sleeps).withMaxAttempts(5))) {
    case Ok(b) -> b
    case Err(e) -> panic(e.message)
  }
  acc.add(
    "five=" + getCode(five, "flaky/always") + " attempts=" + attemptsOf(endpoint, "five/flaky/always") + " waits=" +
      sleepsText(sleeps)
  )
  sleeps.clear()
  // CreateBucket is not idempotent: a 500 leaves its outcome unknown and is
  // returned; throttling means it was not carried out and is retried.
  val failing = match openClient(configFor(endpoint, "create-fails-once").withRetry(recordingRetry(sleeps))) {
    case Ok(c) -> c
    case Err(e) -> panic(e.message)
  }
  acc.add(
    "createAfter500=" + boolCode(createBucket(failing)) + " attempts=" + attemptsOf(endpoint, "create-fails-once") +
      " waits=" +
      sleepsText(sleeps)
  )
  val throttled = match openClient(configFor(endpoint, "create-throttled-once").withRetry(recordingRetry(sleeps))) {
    case Ok(c) -> c
    case Err(e) -> panic(e.message)
  }
  acc.add(
    "createThrottled=" + boolCode(createBucket(throttled)) + " attempts=" +
      attemptsOf(endpoint, "create-throttled-once") +
      " waits=" +
      sleepsText(sleeps)
  )
  acc.add(stopFake(endpoint))
  Str.joinList("\n", acc)
}

test "throttling and server errors are retried with backoff; other errors and unrepeatable requests are not" {
  val lines = Str.split(withFakeS3(19531, retryScenario), "\n")
  assertEqualInt(lines.length, 12, "twelve lines: " + Str.join(" | ", lines))
  assertEqual(
    lines[0],
    "slowdown=ok attempts=3 waits=100,200",
    "SlowDown twice, then success: three attempts, two waits"
  )
  assertEqual(lines[1], "stored=aGk=", "the object the third attempt stored")
  assertEqual(lines[2], "retryAfter=NOT_FOUND attempts=2 waits=1000", "Retry-After: 1 is a wait of 1000 ms, not 100")
  assertEqual(
    lines[3],
    "internalOnce=NOT_FOUND attempts=2 waits=100",
    "a 500 on GET is retried, and the second answer is the result"
  )
  assertEqual(lines[4], "always=SlowDown attempts=3 waits=100,200", "three attempts, then the last error")
  assertEqual(lines[5], "denied=AccessDenied attempts=1 waits=", "AccessDenied is not retried")
  assertEqual(lines[6], "missing=NOT_FOUND waits=", "nor is a missing key")
  assertEqual(lines[7], "withoutRetries=SlowDown attempts=1", "withoutRetries sends once")
  assertEqual(lines[8], "five=SlowDown attempts=5 waits=100,200,400,800", "withMaxAttempts(5), the waits doubling")
  assertEqual(lines[9], "createAfter500=InternalError attempts=1 waits=", "CreateBucket is not repeated after a 500")
  assertEqual(lines[10], "createThrottled=true attempts=2 waits=100", "but it is after throttling")
  assertTrue(lines[11].startsWith("served "), "the fake server stopped cleanly: " + lines[11])
}

test "a connection that cannot be opened is retried for every request, since nothing was sent" {
  val sleeps: List[Int] = newList()
  val cfg = endpointConfig("http://127.0.0.1:1", "data", "us-east-1", "id", "secret").withRetry(recordingRetry(sleeps))
  val client = match openClient(cfg) { case Ok(c) -> c; case Err(e) -> panic(e.message) }
  assertEqual(getCode(bucketOf(client), "k"), "CONNECTION_FAILED", "get")
  assertEqual(sleepsText(sleeps), "100,200", "an idempotent request: three attempts")
  sleeps.clear()
  assertEqual(boolCode(createBucket(client)), "CONNECTION_FAILED", "createBucket")
  assertEqual(sleepsText(sleeps), "100,200", "an unrepeatable request too: the connection was refused")
}

// ─── Credential providers ────────────────────────────────────────────────────
ref record Clock {
  var nowMs: Long
}

// A provider that hands out the long-term test key until `switchAtMs`, with
// an expiry, and the rotated temporary key after it.
ref record Rotating {
  clock: Clock
  switchAtMs: Long
  var calls: Int
  var broken: Bool
}

impl CredentialProvider for Rotating {
  func resolve(): Result[ResolvedCredentials, CredentialError] {
    self.calls = self.calls + 1
    if self.broken {
      return Err(
        CredentialError(code = "REQUEST_FAILED", provider = "rotating", message = "the credential service is down")
      )
    }
    if self.clock.nowMs < self.switchAtMs {
      Ok(
        ResolvedCredentials(
          credentials = fakeCreds(),
          expiresAtEpochMs = Some(value = self.switchAtMs + 300000i64),
          source = "rotating"
        )
      )
    } else {
      Ok(
        ResolvedCredentials(
          credentials = Credentials(
            accessKeyId = ROTATED_KEY,
            secretAccessKey = ROTATED_SECRET,
            sessionToken = ROTATED_TOKEN
          ),
          expiresAtEpochMs = Some(value = self.switchAtMs + 7200000i64),
          source = "rotating"
        )
      )
    }
  }
}

func rotationScenario(endpoint: in String): String {
  val acc: List[String] = newList()
  val noMeta: Map[String, String] = newMap()
  val t0 = 1800000000000i64
  val clock = Clock(nowMs = t0)
  // The first credentials expire five minutes after `switchAtMs`; the cache
  // refreshes five minutes before expiry, which is at `switchAtMs`.
  val inner = Rotating(clock = clock, switchAtMs = t0 + 3600000i64, calls = 0, broken = false)
  val cache = cachedWith(
    inner,
    CacheOptions(refreshWindowMs = 300000i64, retryAfterFailureMs = 30000i64, nowMs = { -> clock.nowMs })
  )
  // Empty static keys: the provider is the only source.
  val cfg = endpointConfig(endpoint, "rotation", REGION, "", "").withCredentialProvider(cache.asProvider()).withRequestTimeout(
    10000
  )
  val client = match openClient(cfg) { case Ok(c) -> c; case Err(e) -> panic(e.code + " " + e.message) }
  val bucket = bucketOf(client)
  acc.add("put=" + (match bucket.put("a", "aGk=", "text/plain", noMeta) { case Ok(_) -> "ok"; case Err(e) -> e.code }))
  acc.add("signedBy=" + control(endpoint, LAST_KEY_PATH) + " calls=" + inner.calls.toString())
  clock.nowMs = t0 + 3599999i64
  acc.add("get=" + (match bucket.get("a") { case Ok(o) -> o.dataBase64; case Err(e) -> e.code }))
  acc.add("signedBy=" + control(endpoint, LAST_KEY_PATH) + " calls=" + inner.calls.toString())
  // The clock reaches the refresh window: the next request is signed with
  // the new key and its session token, which the server verifies.
  clock.nowMs = t0 + 3600000i64
  acc.add("get=" + (match bucket.get("a") { case Ok(o) -> o.dataBase64; case Err(e) -> e.code + " " + e.message }))
  acc.add("signedBy=" + control(endpoint, LAST_KEY_PATH) + " calls=" + inner.calls.toString())
  acc.add(
    "presigned=" +
      (
        if presigned(client, "GET", "a", 60, "20261112T101500Z").contains("X-Amz-Credential=" + ROTATED_KEY + "%2F") and presigned(client, "GET", "a", 60, "20261112T101500Z").contains("&X-Amz-Security-Token=" + ROTATED_TOKEN + "&") {
          "rotated key and token"
        } else {
          "other"
        }
      )
  )
  // A provider with nothing to give: the operation fails without a request.
  inner.broken = true
  cache.invalidate()
  acc.add("noCredentials=" + (match bucket.get("a") { case Ok(_) -> "ok"; case Err(e) -> e.code + ": " + e.message }))
  acc.add("presignNoCredentials=" + codeOf(bucket.presignedUrl("a", 60)))
  acc.add(stopFake(endpoint))
  Str.joinList("\n", acc)
}

test "credentials from a provider are refreshed mid-run, and the next request is signed with the new ones" {
  val lines = Str.split(withFakeS3(19532, rotationScenario), "\n")
  assertEqualInt(lines.length, 10, "ten lines: " + Str.join(" | ", lines))
  assertEqual(lines[0], "put=ok", "the first credentials work")
  assertEqual(lines[1], "signedBy=" + ACCESS_KEY + " calls=1", "signed with the first key; the provider asked once")
  assertEqual(lines[2], "get=aGk=", "still valid a millisecond before the refresh window")
  assertEqual(lines[3], "signedBy=" + ACCESS_KEY + " calls=1", "and the provider was not asked again")
  assertEqual(lines[4], "get=aGk=", "the request after the refresh succeeds")
  assertEqual(lines[5], "signedBy=" + ROTATED_KEY + " calls=2", "signed with the new key; one refresh")
  assertEqual(lines[6], "presigned=rotated key and token", "presigned URLs use the current credentials")
  assertEqual(
    lines[7],
    "noCredentials=CREDENTIALS_UNAVAILABLE: no AWS credentials: rotating: the credential service is down",
    "no credentials is an error of the operation"
  )
  assertEqual(lines[8], "presignNoCredentials=CREDENTIALS_UNAVAILABLE", "and of presigning")
  // put, get, get (control requests are not counted): the get without
  // credentials sent nothing.
  assertEqual(lines[9], "served 3", "the operation without credentials sent no request")
}

// ─── Configuration from the environment and the shared files ────────────────
func envConfigText(lookup: in (String) -> String): String {
  match configFromEnvironment("data", lookup) {
    case Err(e) -> e.code + ": " + e.message
    case Ok(cfg) -> "key=" + cfg.accessKeyId + " secret=" + (if cfg.secretAccessKey.length > 0 { "set" } else { "" }) +
      " token=" +
      cfg.sessionToken +
      " provider=" +
      (if isSome(cfg.credentialProvider) { "yes" } else { "no" }) +
      " region=" +
      cfg.region +
      " endpoint=" +
      cfg.endpoint +
      " pathStyle=" +
      cfg.pathStyle.toString() +
      " ca=" +
      cfg.additionalCaCertificatePem.length.toString()
  }
}

func writeFile(path: in String, text: in String): Unit {
  match File.writeText(path, text) {
    case Ok(_) -> ()
    case Err(_) -> panic("cannot write " + path)
  }
}

func scratchDir(label: in String): String {
  val dir = Env.getVarOrDefault("TMPDIR", "/tmp") + "/lyric-s3-" + label + "-" + Time.nowEpochMillis().toString()
  match File.createDir(dir) {
    case Ok(_) -> dir
    case Err(_) -> panic("cannot create " + dir)
  }
}

// An environment in which no provider of the default chain has anything,
// and none looks outside `dir`.
func isolated(dir: in String): Map[String, String] {
  val values: Map[String, String] = newMap()
  values["AWS_CONFIG_FILE"] = dir + "/config"
  values["AWS_SHARED_CREDENTIALS_FILE"] = dir + "/credentials"
  values["AWS_EC2_METADATA_DISABLED"] = "true"
  values
}

test "configFromEnvironment: environment credentials stay static fields, as before" {
  val dir = scratchDir("env")
  val values = isolated(dir)
  val lookup = environmentOf(values)
  assertEqual(
    envConfigText(lookup),
    "INVALID_CONFIG: no AWS credentials: chain: no provider has credentials: environment: AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are not set; web-identity: AWS_WEB_IDENTITY_TOKEN_FILE and AWS_ROLE_ARN are not set; profile: there is no profile 'default' in " +
      dir +
      "/config or " +
      dir +
      "/credentials; container: neither AWS_CONTAINER_CREDENTIALS_RELATIVE_URI nor AWS_CONTAINER_CREDENTIALS_FULL_URI is set; imds: AWS_EC2_METADATA_DISABLED is true",
    "nothing anywhere: every provider's reason"
  )
  values["AWS_ACCESS_KEY_ID"] = "AKIDEXAMPLE"
  assertEqual(
    envConfigText(lookup),
    "INVALID_CONFIG: no AWS credentials: environment: AWS_ACCESS_KEY_ID is set but AWS_SECRET_ACCESS_KEY is not",
    "half a key pair"
  )
  values["AWS_SECRET_ACCESS_KEY"] = SECRET_KEY
  assertEqual(
    envConfigText(lookup),
    "INVALID_CONFIG: no AWS region: neither AWS_REGION nor AWS_DEFAULT_REGION is set, and the profile has no region",
    "no region"
  )
  values["AWS_DEFAULT_REGION"] = "eu-west-2"
  assertEqual(
    envConfigText(lookup),
    "key=AKIDEXAMPLE secret=set token= provider=no region=eu-west-2 endpoint= pathStyle=false ca=0",
    "keys and region from the environment: a static configuration"
  )
  values["AWS_SESSION_TOKEN"] = "tok"
  values["AWS_REGION"] = "eu-west-3"
  values["AWS_ENDPOINT_URL"] = "http://127.0.0.1:4566"
  assertEqual(
    envConfigText(lookup),
    "key=AKIDEXAMPLE secret=set token=tok provider=no region=eu-west-3 endpoint=http://127.0.0.1:4566 pathStyle=true ca=0",
    "token, AWS_REGION, AWS_ENDPOINT_URL"
  )
  values["AWS_ENDPOINT_URL_S3"] = "http://127.0.0.1:9000"
  assertTrue(envConfigText(lookup).contains(" endpoint=http://127.0.0.1:9000 "), "AWS_ENDPOINT_URL_S3 wins")
  val _ = File.deleteDirRecursive(dir)
}

test "configFromEnvironment: a profile gives a credential provider, region, endpoint and CA" {
  val dir = scratchDir("profile")
  val values = isolated(dir)
  val lookup = environmentOf(values)
  writeFile(
    dir + "/credentials",
    "[default]\naws_access_key_id = " + ACCESS_KEY + "\naws_secret_access_key = " + SECRET_KEY + "\n"
  )
  writeFile(
    dir + "/config",
    "[default]\nregion = " + REGION + "\nendpoint_url = https://127.0.0.1:9297\nca_bundle = " + dir +
      "/ca.pem\ns3 =\n  signature_version = s3v4\n"
  )
  assertEqual(
    envConfigText(lookup),
    "INVALID_CONFIG: cannot read the CA certificate: " + dir + "/ca.pem",
    "a ca_bundle that is not there"
  )
  writeFile(dir + "/ca.pem", FIXTURE_CA_PEM)
  assertEqual(
    envConfigText(lookup),
    "key= secret= token= provider=yes region=" + REGION + " endpoint=https://127.0.0.1:9297 pathStyle=true ca=" +
      FIXTURE_CA_PEM.length.toString(),
    "everything from the two files; the keys are not copied into the configuration"
  )
  values["AWS_CA_BUNDLE"] = dir + "/missing.pem"
  assertEqual(
    envConfigText(lookup),
    "INVALID_CONFIG: cannot read the CA certificate: " + dir + "/missing.pem",
    "AWS_CA_BUNDLE wins over the profile"
  )
  writeFile(dir + "/config", "[default]\nsso_session = corp\nregion = " + REGION + "\n")
  writeFile(dir + "/credentials", "")
  assertEqual(
    envConfigText(lookup),
    "INVALID_CONFIG: no AWS credentials: profile: the profile 'default' sets sso_session: IAM Identity Center (SSO) and console login sessions are not supported",
    "a profile this package cannot use is named, not skipped"
  )
  val _ = File.deleteDirRecursive(dir)
}

func sharedFilesScenario(endpoint: in String): String {
  val dir = scratchDir("shared")
  val values = isolated(dir)
  writeFile(
    dir + "/credentials",
    "[ci]\naws_access_key_id = " + ACCESS_KEY + "\naws_secret_access_key = " + SECRET_KEY + "\n"
  )
  writeFile(dir + "/config", "[profile ci]\nregion = " + REGION + "\nendpoint_url = " + endpoint + "\n")
  values["AWS_PROFILE"] = "ci"
  val noMeta: Map[String, String] = newMap()
  var text = match configFromEnvironment("shared-files", environmentOf(values)) {
    case Err(e) -> "config=" + e.code + " " + e.message
    case Ok(cfg) -> match connect(cfg) {
      case Err(e) -> "connect=" + e.code + " " + e.message
      case Ok(bucket) -> "put=" +
        (
          match bucket.put("k", "aGk=", "text/plain", noMeta) {
            case Ok(_) -> "ok"
            case Err(e) -> e.code + " " + e.message
          }
        ) +
        "\nget=" +
        (
          match bucket.get("k") {
            case Ok(o) -> o.dataBase64
            case Err(e) -> e.code
          }
        )
    }
  }
  val _ = File.deleteDirRecursive(dir)
  text + "\n" + stopFake(endpoint)
}

test "a bucket configured from a profile of the shared files signs requests the server accepts" {
  val lines = Str.split(withFakeS3(19533, sharedFilesScenario), "\n")
  assertEqualInt(lines.length, 3, "three lines: " + Str.join(" | ", lines))
  assertEqual(lines[0], "put=ok", "signed with the profile's keys")
  assertEqual(lines[1], "get=aGk=", "and read back")
  assertEqual(lines[2], "served 2", "both requests reached the server")
}
