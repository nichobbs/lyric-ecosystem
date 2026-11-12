//! tests/fake_gcs_tests.l - the Cloud Storage backend over real HTTP, against
//! an XML API endpoint written in Lyric on `Std.HttpServer`.
//!
//! The fake server does what Cloud Storage does with a request, so nothing
//! here is answered from a canned response:
//!
//!   * it verifies the GOOG4-HMAC-SHA256 signature of every request with the
//!     shared secret, rebuilding the canonical request from the method, path,
//!     query and signed headers it actually received; it checks that
//!     `x-goog-content-sha256` is the hash of the body it actually received,
//!     that every `x-goog-*` header of the request is signed, and that
//!     `x-goog-date` is within 15 minutes of its own clock;
//!   * V4 signed URLs are verified the same way from their query string,
//!     including expiry;
//!   * a request with no signature at all is an anonymous caller, who may
//!     read the bucket named `public-bucket` and nothing else;
//!   * objects live in memory with their content type, user metadata, storage
//!     class and generation; `list-type=2` listings page in name order with
//!     an opaque continuation token and `encoding-type=url`.
//!
//! Its answers are the ones storage.googleapis.com was observed to give
//! (2026-11-12) wherever that could be observed without an HMAC key: the
//! `Error` documents for `NoSuchKey` (also for DELETE of a missing object),
//! `NoSuchBucket`, `SignatureDoesNotMatch` (also for an access ID that does
//! not exist), `InvalidSecurity`, `MalformedSecurityHeader`,
//! `MissingSecurityHeader`, `AccessDenied` for an anonymous caller and an
//! invalid page token, and the object headers of GET and HEAD. `ExpiredToken`,
//! `RequestTimeTooSkewed`, `PermanentRedirect` and `InternalError` are the
//! codes the XML API reference documents; the status and code for a payload
//! that does not match `x-goog-content-sha256`, or for an unsigned `x-goog-*`
//! header, are this fake's own choice.
//!
//! `Testing.Conformance.Storage` runs against it end to end, then the
//! backend-specific behaviour: binary payloads, error mapping, redirects,
//! signed GET and PUT URLs fetched with a plain HTTP client, storage classes.
//!
//! The server runs on its own thread: a `Task.Run` worker on `--target dotnet`
//! (`spawn` is synchronous there), a `spawn` inside a `scope` on `--target
//! jvm`. No assertion runs while it is waiting for a request: a scenario
//! returns its findings and ends by asking the server to stop.
@test_module
package Storage.Gcs.FakeGcsTests

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
import Storage.Gcs
import Storage.Gcs.Extras
import Storage.Gcs.Goog4
import Storage.Gcs.Wire.{parseFault}

// ─── The fake server's state ─────────────────────────────────────────────────
// Fixtures, not a real HMAC key.
val ACCESS_ID: String = "GOOG1EFAKEFAKEFAKEFAKEFAKEFAKEFAKEFAKEFAKEFAKEFAKEFAKEFAKEFA"

val SECRET: String = "fake/secret+key/for-tests-only"

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
  generation: Long
}

ref record FakeGcs {
  // Keyed `<bucket>/<name>`.
  var objects: Map[String, StoredObject]
  var requestCount: Int
  var nextGeneration: Long
}

func newFakeGcs(): FakeGcs {
  FakeGcs(objects = newMap(), requestCount = 0, nextGeneration = 1762942500000000i64)
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
  respondBytesWithHeaders(ctx, status, "application/xml; charset=UTF-8", empty, headers)
}

func respondFault(ctx: in HttpContext, status: in Int, code: in String, message: in String, details: in String): Unit {
  val none: Map[String, String] = newMap()
  // As Cloud Storage does, a HEAD is answered without a body: only the status
  // tells the client what went wrong.
  if requestMethod(ctx) == "HEAD" {
    respondEmpty(ctx, status, none)
    return
  }
  val body = "<?xml version='1.0' encoding='UTF-8'?><Error><Code>" + code + "</Code><Message>" + xmlEscape(message) +
    "</Message>" +
    (if details.length > 0 { "<Details>" + xmlEscape(details) + "</Details>" } else { "" }) +
    "</Error>"
  respondBytesWithHeaders(ctx, status, "application/xml; charset=UTF-8", Enc.encodeUtf8(body), none)
}

// ─── Signature verification ──────────────────────────────────────────────────
func intAt(text: in String, start: in Int, count: in Int): Int {
  var n = 0
  var i = 0
  while i < count {
    n = n * 10 + (Str.codeUnitAt(text, start + i) - 48)
    i = i + 1
  }
  n
}

func isDigitsOnly(text: in String): Bool {
  var i = 0
  while i < text.length {
    val u = Str.codeUnitAt(text, i)
    if u < 48 or u > 57 {
      return false
    }
    i = i + 1
  }
  text.length > 0
}

// Seconds since the Unix epoch for `yyyymmddThhmmssZ` (the civil-from-days
// algorithm, proleptic Gregorian calendar).
func googDateToEpochSeconds(googDate: in String): Long {
  val y0 = intAt(googDate, 0, 4)
  val m = intAt(googDate, 4, 2)
  val d = intAt(googDate, 6, 2)
  val y = if m <= 2 { y0 - 1 } else { y0 }
  val era = y / 400
  val yoe = y - era * 400
  val mp = if m > 2 { m - 3 } else { m + 9 }
  val doy = (153 * mp + 2) / 5 + d - 1
  val doe = yoe * 365 + yoe / 4 - yoe / 100 + doy
  val days = (era * 146097 + doe - 719468).toLong()
  days * 86400i64 + (intAt(googDate, 9, 2) * 3600 + intAt(googDate, 11, 2) * 60 + intAt(googDate, 13, 2)).toLong()
}

func isGoogDate(text: in String): Bool {
  text.length == 16 and isDigitsOnly(text.substring(0, 8)) and text.substring(8, 1) == "T" and
    isDigitsOnly(text.substring(9, 6)) and
    text.substring(15, 1) == "Z"
}

func fakeKey(): HmacKey {
  HmacKey(accessId = ACCESS_ID, secret = SECRET)
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

// The canonical resource path derived from a request path: each segment
// decoded, then encoded again.
func canonicalUriOf(rawPath: in String): String {
  uriEncode(urlDecode(rawPath), true)
}

// A credential `<access id>/<yyyymmdd>/<location>/storage/goog4_request`
// split; `ok` is false when it does not have that shape or names another day
// than `googDate`. Any location is accepted, as Cloud Storage accepts any.
record Scope {
  ok: Bool
  accessId: String
  location: String
}

func scopeOf(credential: in String, googDate: in String): Scope {
  val parts = Str.split(credential, "/")
  if parts.length != 5 or parts[3] != "storage" or parts[4] != "goog4_request" or parts[1] != googDate.substring(0, 8) or parts[2].length == 0 {
    return Scope(ok = false, accessId = "", location = "")
  }
  Scope(ok = true, accessId = parts[0], location = parts[2])
}

// What the server answers to a request it refuses; `status` 0 means the
// request is accepted.
record Refusal {
  status: Int
  code: String
  message: String
  details: String
}

func accepted(): Refusal {
  Refusal(status = 0, code = "", message = "", details = "")
}

func signatureMismatch(): Refusal {
  Refusal(
    status = 403,
    code = "SignatureDoesNotMatch",
    message = "Access denied.",
    details = "The request signature we calculated does not match the signature you provided. Check your Google secret key and signing method."
  )
}

func invalidSecurity(): Refusal {
  Refusal(
    status = 403,
    code = "InvalidSecurity",
    message = "The provided security credentials are not valid.",
    details = "Request was not signed or contained a malformed signature"
  )
}

func absDiff(a: in Long, b: in Long): Long {
  if a > b {
    a - b
  } else {
    b - a
  }
}

// Verify a request signed in its `Authorization` header.
func verifyHeaderAuth(ctx: in HttpContext, body: in slice[Byte]): Refusal {
  val authorization = headerValue(ctx, "Authorization")
  if not Str.startsWith(authorization, "GOOG4-HMAC-SHA256 ") {
    return invalidSecurity()
  }
  val credential = authField(authorization, "Credential")
  val signedNames = authField(authorization, "SignedHeaders")
  val signature = authField(authorization, "Signature")
  val googDate = headerValue(ctx, "x-goog-date")
  if credential.length == 0 or signedNames.length == 0 or signature.length == 0 or not isGoogDate(googDate) {
    return invalidSecurity()
  }
  val credScope = scopeOf(credential, googDate)
  if not credScope.ok {
    return invalidSecurity()
  }
  // A header named in SignedHeaders must be in the request.
  val names = Str.split(signedNames, ";")
  val signedSet = ";" + signedNames + ";"
  var i = 0
  while i < names.length {
    if names[i] != "host" and headerValue(ctx, names[i]).length == 0 {
      return Refusal(
        status = 400,
        code = "MalformedSecurityHeader",
        message = "Invalid argument.",
        details = "Your request has a malformed header. Header was included in signedheaders, but not in the request."
      )
    }
    i = i + 1
  }
  // `host`, the date and the payload hash must be signed, and so must every
  // other `x-goog-*` header the request carries.
  var allSigned = Str.contains(signedSet, ";host;") and Str.contains(signedSet, ";x-goog-date;") and
    Str.contains(signedSet, ";x-goog-content-sha256;")
  val sent = mapEntries(requestHeaders(ctx))
  for e in sent {
    val lower = Str.toLower(e.key)
    if Str.startsWith(lower, "x-goog-") and not Str.contains(signedSet, ";" + lower + ";") {
      allSigned = false
    }
  }
  if not allSigned {
    return Refusal(
      status = 400,
      code = "MalformedSecurityHeader",
      message = "Invalid argument.",
      details = "Your request has a malformed header. An x-goog- header of the request is not in signedheaders."
    )
  }
  val payloadHash = headerValue(ctx, "x-goog-content-sha256")
  val headers: List[Pair] = newList()
  var k = 0
  while k < names.length {
    headers.add(Pair(name = names[k], value = headerValue(ctx, names[k])))
    k = k + 1
  }
  val expected = signGoogRequest(
    requestMethod(ctx),
    canonicalUriOf(requestPath(ctx)),
    queryPairs(requestQuery(ctx)),
    headers,
    payloadHash,
    fakeKey(),
    credScope.location,
    googDate
  )
  // An access ID the service does not know is a signature mismatch too.
  if credScope.accessId != ACCESS_ID or expected.signature != signature {
    return signatureMismatch()
  }
  if absDiff(Time.nowEpochMillis() / 1000i64, googDateToEpochSeconds(googDate)) > 900i64 {
    return Refusal(
      status = 403,
      code = "RequestTimeTooSkewed",
      message = "The difference between the request time and the server's time is too large.",
      details = ""
    )
  }
  if payloadHash != sha256Hex(body) {
    return Refusal(
      status = 400,
      code = "ContentSha256Mismatch",
      message = "The x-goog-content-sha256 you specified did not match what we received.",
      details = ""
    )
  }
  accepted()
}

// Verify a V4 signed URL (signature in the query string).
func verifyQueryAuth(ctx: in HttpContext, query: in List[Pair]): Refusal {
  if queryValue(query, "X-Goog-Signature").length == 0 {
    return Refusal(
      status = 400,
      code = "MissingSecurityHeader",
      message = "Invalid argument.",
      details = "Your request was missing a required header. Missing headers: x-goog-signature"
    )
  }
  val googDate = queryValue(query, "X-Goog-Date")
  val expiresText = queryValue(query, "X-Goog-Expires")
  if queryValue(query, "X-Goog-Algorithm") != "GOOG4-HMAC-SHA256" or queryValue(query, "X-Goog-SignedHeaders") != "host" or not isGoogDate(googDate) or not isDigitsOnly(expiresText) or expiresText.length > 6 {
    return invalidSecurity()
  }
  val credScope = scopeOf(queryValue(query, "X-Goog-Credential"), googDate)
  if not credScope.ok {
    return invalidSecurity()
  }
  val unsigned: List[Pair] = newList()
  var i = 0
  while i < query.count {
    if query[i].name != "X-Goog-Signature" {
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
  val expected = googSignatureOf(
    SECRET,
    googDate,
    credScope.location,
    googStringToSign(googDate, googCredentialScope(googDate, credScope.location), cr)
  )
  if credScope.accessId != ACCESS_ID or expected != queryValue(query, "X-Goog-Signature") {
    return signatureMismatch()
  }
  val expires = intAt(expiresText, 0, expiresText.length)
  if expires < 1 or expires > 604800 {
    return Refusal(
      status = 400,
      code = "InvalidArgument",
      message = "Invalid argument.",
      details = "X-Goog-Expires must be between 1 and 604800 seconds"
    )
  }
  val nowSeconds = Time.nowEpochMillis() / 1000i64
  val activeFrom = googDateToEpochSeconds(googDate)
  if nowSeconds > activeFrom + expires.toLong() or nowSeconds < activeFrom - 900i64 {
    return Refusal(status = 400, code = "ExpiredToken", message = "The provided token has expired.", details = "")
  }
  accepted()
}

// A request without a signature: the anonymous caller may read
// `public-bucket` (`allUsers` have the object viewer role there) and nothing
// else. Cloud Storage does not say whether what was asked for exists.
func anonymousAccess(method: in String, bucket: in String, key: in String): Refusal {
  if bucket == "public-bucket" and (method == "GET" or method == "HEAD") {
    return accepted()
  }
  val permission = if method == "PUT" {
    "storage.objects.create"
  } else if method == "DELETE" {
    "storage.objects.delete"
  } else if key.length == 0 {
    "storage.objects.list"
  } else {
    "storage.objects.get"
  }
  Refusal(
    status = 403,
    code = "AccessDenied",
    message = "Access denied.",
    details = "Anonymous caller does not have " + permission + " access to the Google Cloud Storage " +
      (if key.length == 0 { "bucket" } else { "object" }) +
      ". Permission '" +
      permission +
      "' denied on resource (or it may not exist)."
  )
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

func sortedKeysOf(gcs: in FakeGcs, bucket: in String, prefix: in String): List[String] {
  val acc: List[String] = newList()
  val all = mapKeys(gcs.objects)
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

// The `encoding-type=url` form of a name, as Cloud Storage writes it:
// percent-encoded, `/` kept, a space as `+`.
func listEncoded(key: in String): String {
  Str.replace(uriEncode(key, true), "%20", "+")
}

func listObjects(gcs: in FakeGcs, ctx: in HttpContext, bucket: in String, query: in List[Pair]): Unit {
  val prefix = queryValue(query, "prefix")
  val maxKeysText = queryValue(query, "max-keys")
  val maxKeys = if maxKeysText.length == 0 { 1000 } else { intAt(maxKeysText, 0, maxKeysText.length) }
  val urlEncoded = queryValue(query, "encoding-type") == "url"
  val token = queryValue(query, "continuation-token")
  // The token is opaque to clients: base64 of the last name of the page
  // before.
  var after = ""
  if token.length > 0 {
    val decoded = match Enc.tryDecodeBase64(token) { case Some(bytes) -> Enc.tryDecodeUtf8(bytes); case None -> None }
    match decoded {
      case Some(key) -> {
        after = key
      }
      case None -> {
        respondFault(ctx, 400, "InvalidArgument", "Invalid argument.", "Invalid page token.")
        return
      }
    }
  }
  val keys = sortedKeysOf(gcs, bucket, prefix)
  val entries = Str.StringBuilder.new()
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
        match mapGet(gcs.objects, bucket + "/" + key) {
          case Some(obj) -> {
            entries.append("<Contents><Key>" + xmlEscape(if urlEncoded { listEncoded(key) } else { key }) + "</Key>")
            entries.append("<Generation>" + obj.generation.toString() + "</Generation>")
            entries.append("<MetaGeneration>1</MetaGeneration>")
            entries.append("<LastModified>" + LAST_MODIFIED_ISO + "</LastModified>")
            entries.append("<ETag>\"" + obj.etag + "\"</ETag>")
            entries.append("<Size>" + obj.bytes.length.toString() + "</Size></Contents>")
            written = written + 1
            last = key
          }
          case None -> ()
        }
      }
    }
    i = i + 1
  }
  // The element order is Cloud Storage's: the token before the entries.
  val sb = Str.StringBuilder.new()
  sb.append("<?xml version='1.0' encoding='UTF-8'?>")
  sb.append("<ListBucketResult xmlns='http://doc.s3.amazonaws.com/2006-03-01'>")
  sb.append("<Name>" + bucket + "</Name>")
  sb.append("<Prefix>" + xmlEscape(if urlEncoded { listEncoded(prefix) } else { prefix }) + "</Prefix>")
  if more {
    sb.append("<NextContinuationToken>" + Enc.encodeBase64(Enc.encodeUtf8(last)) + "</NextContinuationToken>")
  }
  sb.append("<KeyCount>" + written.toString() + "</KeyCount>")
  sb.append("<MaxKeys>" + maxKeys.toString() + "</MaxKeys>")
  sb.append("<IsTruncated>" + (if more { "true" } else { "false" }) + "</IsTruncated>")
  if urlEncoded {
    sb.append("<EncodingType>url</EncodingType>")
  }
  sb.append(entries.toString())
  sb.append("</ListBucketResult>")
  val none: Map[String, String] = newMap()
  respondBytesWithHeaders(ctx, 200, "application/xml; charset=UTF-8", Enc.encodeUtf8(sb.toString()), none)
}

// The headers Cloud Storage sends with an object, on GET and on HEAD.
// `x-goog-stored-content-length` is how a HEAD reports the size here:
// `Std.HttpServer` cannot send a HEAD response whose `Content-Length` is an
// entity's length (nichobbs/lyric-lang#9048), and the client reads this
// header first, as it does against Cloud Storage.
func objectHeaders(obj: in StoredObject): Map[String, String] {
  val headers: Map[String, String] = newMap()
  headers.add("ETag", "\"" + obj.etag + "\"")
  headers.add("Last-Modified", LAST_MODIFIED_HTTP)
  headers.add("x-goog-generation", obj.generation.toString())
  headers.add("x-goog-metageneration", "1")
  headers.add("x-goog-stored-content-encoding", "identity")
  headers.add("x-goog-stored-content-length", obj.bytes.length.toString())
  headers.add("x-goog-storage-class", obj.storageClass)
  val entries = mapEntries(obj.userMeta)
  for e in entries {
    headers.add("x-goog-meta-" + e.key, e.value)
  }
  headers
}

func putObject(gcs: in FakeGcs, ctx: in HttpContext, fullKey: in String, body: in slice[Byte]): Unit {
  val userMeta: Map[String, String] = newMap()
  val entries = mapEntries(requestHeaders(ctx))
  for e in entries {
    val lower = Str.toLower(e.key)
    if Str.startsWith(lower, "x-goog-meta-") {
      userMeta[lower.substring(12)] = e.value
    }
  }
  val storageClass = headerValue(ctx, "x-goog-storage-class")
  val contentType = headerValue(ctx, "Content-Type")
  val etag = sha256Hex(body).substring(0, 32)
  gcs.nextGeneration = gcs.nextGeneration + 1i64
  gcs.objects[fullKey] = StoredObject(
    bytes = body,
    contentType = if contentType.length > 0 { contentType } else { "application/octet-stream" },
    etag = etag,
    userMeta = userMeta,
    storageClass = if storageClass.length > 0 { storageClass } else { "STANDARD" },
    generation = gcs.nextGeneration
  )
  val headers: Map[String, String] = newMap()
  headers.add("ETag", "\"" + etag + "\"")
  headers.add("x-goog-generation", gcs.nextGeneration.toString())
  respondEmpty(ctx, 200, headers)
}

func noSuchKey(ctx: in HttpContext, bucket: in String, key: in String): Unit {
  respondFault(ctx, 404, "NoSuchKey", "The specified key does not exist.", "No such object: " + bucket + "/" + key)
}

func serveObject(gcs: in FakeGcs, ctx: in HttpContext, bucket: in String, key: in String, body: in slice[Byte]): Unit {
  val method = requestMethod(ctx)
  val fullKey = bucket + "/" + key
  // Names that make the server misbehave on purpose.
  if key == "boom/internal" {
    respondFault(ctx, 500, "InternalError", "We encountered an internal error. Please try again.", "")
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
    val text = "<?xml version='1.0' encoding='UTF-8'?><Error><Code>PermanentRedirect</Code>" +
      "<Message>The bucket you are attempting to access must be addressed using the specified endpoint.</Message></Error>"
    respondBytesWithHeaders(ctx, 301, "application/xml; charset=UTF-8", Enc.encodeUtf8(text), headers)
    return
  }
  if method == "PUT" {
    putObject(gcs, ctx, fullKey, body)
  } else if method == "DELETE" {
    // Unlike S3, Cloud Storage answers 404 NoSuchKey when there was nothing
    // to delete.
    if gcs.objects.containsKey(fullKey) {
      val _ = gcs.objects.remove(fullKey)
      val none: Map[String, String] = newMap()
      respondEmpty(ctx, 204, none)
    } else {
      noSuchKey(ctx, bucket, key)
    }
  } else if method == "GET" or method == "HEAD" {
    match mapGet(gcs.objects, fullKey) {
      case None -> {
        noSuchKey(ctx, bucket, key)
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
    respondFault(ctx, 405, "MethodNotAllowed", "The specified method is not allowed against this resource.", "")
  }
}

func handle(gcs: in FakeGcs, ctx: in HttpContext): Unit {
  gcs.requestCount = gcs.requestCount + 1
  val body = requestBodyBytes(ctx)
  val query = queryPairs(requestQuery(ctx))
  // Path-style: /<bucket>[/<name>].
  val path = urlDecode(requestPath(ctx))
  val rest = path.substring(1)
  val slash = Str.indexOfRaw(rest, "/")
  val bucket = if slash >= 0 { rest.substring(0, slash) } else { rest }
  val key = if slash >= 0 { rest.substring(slash + 1) } else { "" }
  val refusal = if queryValue(query, "X-Goog-Algorithm").length > 0 or queryValue(query, "X-Goog-Signature").length > 0 {
    verifyQueryAuth(ctx, query)
  } else if headerValue(ctx, "Authorization").length > 0 {
    verifyHeaderAuth(ctx, body)
  } else {
    anonymousAccess(requestMethod(ctx), bucket, key)
  }
  if refusal.status != 0 {
    respondFault(ctx, refusal.status, refusal.code, refusal.message, refusal.details)
    return
  }
  if bucket == "missing-bucket" {
    respondFault(ctx, 404, "NoSuchBucket", "The specified bucket does not exist.", "")
    return
  }
  if key.length == 0 {
    if requestMethod(ctx) == "GET" and queryValue(query, "list-type") == "2" {
      listObjects(gcs, ctx, bucket, query)
    } else if requestMethod(ctx) == "HEAD" {
      val none: Map[String, String] = newMap()
      respondEmpty(ctx, 200, none)
    } else {
      respondFault(ctx, 400, "InvalidArgument", "Invalid argument.", "only list-type=2 listings are served on a bucket")
    }
    return
  }
  serveObject(gcs, ctx, bucket, key, body)
}

func serveUntilStopped(l: in HttpListener): Unit {
  val gcs = newFakeGcs()
  var running = true
  while running {
    val ctx = nextContext(l)
    if requestPath(ctx) == STOP_PATH {
      respondText(ctx, 200, "served " + gcs.requestCount.toString())
      running = false
    } else {
      // A bug in the fake must surface as a failed request, not as a server
      // thread that died and a test run that waits for it.
      try {
        handle(gcs, ctx)
      } catch Bug as b {
        respondFault(ctx, 500, "FakeServerBug", b.message, "")
      }
    }
  }
}

// ─── Running a scenario against the server ───────────────────────────────────
@cfg(target = "dotnet")
extern type WorkerTask = "System.Threading.Tasks.Task"

@cfg(target = "dotnet")
@externStatic
@externTarget("System.Threading.Tasks.Task.Run")
func taskRun(work: in () -> Unit): WorkerTask = ()

@cfg(target = "dotnet")
@externInstance
@externTarget("System.Threading.Tasks.Task.Wait")
func taskWait(t: in WorkerTask): Unit = ()

func endpointOf(port: in Int): String {
  "http://127.0.0.1:" + port.toString()
}

@cfg(target = "dotnet")
func withFakeGcs(port: in Int, scenario: in (String) -> String): String {
  val l = startListener(endpointOf(port) + "/")
  val server = taskRun({ -> serveUntilStopped(l) })
  val transcript = scenario(endpointOf(port))
  taskWait(server)
  stopListener(l)
  transcript
}

@cfg(target = "jvm")
func withFakeGcs(port: in Int, scenario: in (String) -> String): String {
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
// the server said: how many requests it served.
func stopFake(endpoint: in String): String {
  match await defaultClient().get(endpoint + STOP_PATH) {
    case Err(e) -> "stop failed: " + HttpError.message(e)
    case Ok(resp) -> match await HttpResponse.bodyText(resp) { case Ok(text) -> text; case Err(_) -> "stop unreadable" }
  }
}

func configFor(endpoint: in String, bucket: in String): GcsConfig {
  endpointConfig(endpoint, bucket, ACCESS_ID, SECRET).withRequestTimeout(10000)
}

func clientFor(endpoint: in String, bucket: in String): GcsClient {
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

test "GcsBucket conforms to StorageBucket, over HTTP against the signature-checking fake" {
  val ran: List[Checks] = newList()
  val lines = Str.split(withFakeGcs(19901, { endpoint -> conformanceScenario(endpoint, ran) }), "\n")
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
  // Raw bytes through the extras, with a name that needs encoding.
  val oddKey = "odd/sp ace+plus&amp=eqé/100%/(1)*~!$'@,;.txt"
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

test "binary payloads, empty objects and names that need encoding survive the round trip" {
  val lines = Str.split(withFakeGcs(19902, binaryScenario), "\n")
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
    "a name with spaces, '+', '&', '%', non-ASCII and reserved punctuation"
  )
  assertEqual(lines[4], "oddGet=010203", "the encoded name is the same object on get")
  assertEqual(lines[5], "oddList=1:same key:3:32:2026-11-12T10:15:00.000Z", "the listing decodes the name back")
  assertEqual(lines[6], "badBase64=INVALID_BASE64", "a payload that is not base64 never reaches the server")
  assertEqual(lines[7], "served 7", "exactly the expected requests reached the server")
}

func statText(r: in Result[Option[ObjectHead], StorageError]): String {
  match r {
    case Err(e) -> e.code
    case Ok(None) -> "absent"
    case Ok(Some(h)) -> {
      val owner = match mapGet(h.userMeta, "owner") { case Some(v) -> v; case None -> "-" }
      val note = match mapGet(h.userMeta, "build.note_1") { case Some(v) -> v; case None -> "-" }
      h.contentType + "/" + h.contentLength.toString() + "/" + h.etag.length.toString() + "/" + h.lastModified + "/" +
        h.storageClass +
        "/" +
        owner +
        "/" +
        note +
        "/" +
        h.userMeta.count.toString() +
        "/" +
        h.generation.length.toString()
    }
  }
}

func metadataScenario(endpoint: in String): String {
  val client = clientFor(endpoint, "meta")
  val bucket = bucketOf(client)
  val meta: Map[String, String] = newMap()
  meta["owner"] = "alice"
  meta["build.note_1"] = "two  words, one comma"
  var text = "put=" + codeOf(putBytes(client, "m/doc", [7u8, 7u8, 7u8, 7u8, 7u8], "application/pdf", meta))
  text = text + "\nstat=" + statText(stat(client, "m/doc"))
  text = text + "\nabsent=" + statText(stat(client, "m/none"))
  text = text + "\nclass=" + codeOf(putBytesWithStorageClass(client, "m/cold", [8u8], "text/plain", "NEARLINE"))
  text = text + "\nclassStat=" + statText(stat(client, "m/cold"))
  text = text + "\nbadClass=" + codeOf(putBytesWithStorageClass(client, "m/x", [8u8], "text/plain", "GLACIER"))
  // Metadata a header would alter is refused before any request.
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
  // The generation changes when an object is replaced.
  val first = match stat(client, "m/doc") { case Ok(Some(h)) -> h.generation; case _ -> "?" }
  val noMeta: Map[String, String] = newMap()
  text = text + "\nreplace=" + codeOf(putBytes(client, "m/doc", [9u8], "text/plain", noMeta))
  val second = match stat(client, "m/doc") { case Ok(Some(h)) -> h.generation; case _ -> "?" }
  text = text + "\ngeneration=" + (if first != second and first != "?" and second != "?" { "changed" } else { "same" })
  text + "\n" + stopFake(endpoint)
}

test "user metadata travels as x-goog-meta-* headers, storage classes as x-goog-storage-class, and HEAD reports the stored size" {
  val lines = Str.split(withFakeGcs(19903, metadataScenario), "\n")
  assertEqualInt(lines.length, 13, "thirteen lines: " + Str.join(" | ", lines))
  assertTrue(lines[0].startsWith("put=ok:"), "put with metadata: " + lines[0])
  assertEqual(
    lines[1],
    "stat=application/pdf/5/32/2026-11-12T10:15:00Z/STANDARD/alice/two  words, one comma/2/16",
    "HEAD reports the content type, stored size, ETag, date, class, both metadata entries and the generation"
  )
  assertEqual(lines[2], "absent=absent", "HEAD of a missing object is None, not an error")
  assertTrue(lines[3].startsWith("class=ok:"), "put with a storage class: " + lines[3])
  assertEqual(lines[4], "classStat=text/plain/1/32/2026-11-12T10:15:00Z/NEARLINE/-/-/0/16", "the class was stored")
  assertEqual(lines[5], "badClass=INVALID_ARGUMENT", "an S3 class name is not a Cloud Storage class; refused locally")
  assertEqual(lines[6], "upperKey=USER_META_UNSUPPORTED", "header names are case-insensitive")
  assertEqual(lines[7], "unicodeValue=USER_META_UNSUPPORTED", "a header cannot carry non-ASCII intact")
  assertEqual(lines[8], "paddedValue=USER_META_UNSUPPORTED", "a header would lose the leading space")
  assertEqual(lines[9], "exists=false", "nothing was stored by the refused puts")
  assertTrue(lines[10].startsWith("replace=ok:"), "replacing an object: " + lines[10])
  assertEqual(lines[11], "generation=changed", "a replaced object has a new generation")
  assertEqual(lines[12], "served 9", "the refused puts made no request")
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
  text = text + "\nmissingMessage=" + (match good.get("no/such/key") { case Ok(_) -> "ok"; case Err(e) -> e.message })
  text = text + "\ninternal=" + getCode(good, "boom/internal")
  text = text + "\ngateway=" + getCode(good, "boom/gateway")
  text = text + "\nmoved=" + getCode(good, "boom/moved")
  val wrongSecret = match connect(endpointConfig(endpoint, "errors", ACCESS_ID, "not-the-secret")) {
    case Ok(b) -> b
    case Err(e) -> panic("connect failed: " + e.message)
  }
  text = text + "\nwrongSecret=" + getCode(wrongSecret, "k")
  text = text + "\nwrongSecretPut=" +
    (match wrongSecret.put("k", "aGk=", "text/plain", noMeta) { case Ok(_) -> "ok"; case Err(e) -> e.code })
  text = text + "\nwrongSecretExists=" +
    (match wrongSecret.exists("k") { case Ok(b) -> b.toString(); case Err(e) -> e.code })
  text = text + "\nwrongSecretDelete=" + (match wrongSecret.delete("k") { case Ok(_) -> "ok"; case Err(e) -> e.code })
  val wrongAccessId = match connect(endpointConfig(endpoint, "errors", "GOOG1ENOTKNOWN", SECRET)) {
    case Ok(b) -> b
    case Err(e) -> panic("connect failed: " + e.message)
  }
  text = text + "\nwrongAccessId=" + getCode(wrongAccessId, "k")
  // Any location signs: the server derives the key for the location named.
  val otherLocation = match connect(configFor(endpoint, "errors").withLocation("europe-west1")) {
    case Ok(b) -> b
    case Err(e) -> panic("connect failed: " + e.message)
  }
  text = text + "\notherLocation=" + getCode(otherLocation, "no/such/key")
  val noBucket = bucketOf(clientFor(endpoint, "missing-bucket"))
  text = text + "\nnoBucket=" + getCode(noBucket, "k")
  text = text + "\nnoBucketList=" + (match noBucket.list("", "", 10) { case Ok(_) -> "ok"; case Err(e) -> e.code })
  text = text + "\nnoBucketDelete=" + (match noBucket.delete("k") { case Ok(_) -> "ok"; case Err(e) -> e.code })
  text = text + "\nbadToken=" +
    (match good.list("", "zz-not-a-token", 10) { case Ok(_) -> "ok"; case Err(e) -> e.code })
  text = text + "\nstillWorks=" +
    (match good.put("fine", "aGk=", "text/plain", noMeta) { case Ok(_) -> "ok"; case Err(e) -> e.code })
  text + "\n" + stopFake(endpoint)
}

test "Cloud Storage error documents map to StorageError codes, and a redirect is not followed" {
  val lines = Str.split(withFakeGcs(19904, errorScenario), "\n")
  assertEqualInt(lines.length, 17, "seventeen lines: " + Str.join(" | ", lines))
  assertEqual(lines[0], "missing=NOT_FOUND", "NoSuchKey is the interface's NOT_FOUND")
  assertEqual(
    lines[1],
    "missingMessage=cannot get object 'no/such/key': NoSuchKey (HTTP 404): The specified key does not exist. No such object: errors/no/such/key",
    "the message names the operation, the code, the status, and the service's message and details"
  )
  assertEqual(lines[2], "internal=InternalError", "a 500 with an Error document carries its Code")
  assertEqual(lines[3], "gateway=HTTP_502", "an error page that is not Cloud Storage's is HTTP_<status>")
  assertEqual(lines[4], "moved=PermanentRedirect", "a 301 is reported, not followed to another host")
  assertEqual(lines[5], "wrongSecret=SignatureDoesNotMatch", "a wrong secret fails the server's signature check on GET")
  assertEqual(lines[6], "wrongSecretPut=SignatureDoesNotMatch", "and on PUT")
  assertEqual(lines[7], "wrongSecretExists=HTTP_403", "HEAD has no error body, so only the status is known")
  assertEqual(lines[8], "wrongSecretDelete=SignatureDoesNotMatch", "a refused delete is not 'already gone'")
  assertEqual(lines[9], "wrongAccessId=SignatureDoesNotMatch", "an unknown access ID is a signature mismatch too")
  assertEqual(lines[10], "otherLocation=NOT_FOUND", "a request signed for another location is accepted")
  assertEqual(lines[11], "noBucket=NoSuchBucket", "a missing bucket on get")
  assertEqual(lines[12], "noBucketList=NoSuchBucket", "a missing bucket on list")
  assertEqual(
    lines[13],
    "noBucketDelete=NoSuchBucket",
    "a missing bucket on delete is an error, unlike a missing object"
  )
  assertEqual(lines[14], "badToken=InvalidArgument", "a continuation token the server does not know")
  assertEqual(lines[15], "stillWorks=ok", "the correctly signed client is unaffected")
  assertEqual(lines[16], "served 16", "every request reached the server")
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

func fetchWith(url: in String, headerName: in String, headerValueText: in String): String {
  val parsed = match Url.tryFrom(url) { case Ok(u) -> u; case Err(_) -> return "bad url" }
  val req = match withHeader(request(GET, parsed), headerName, headerValueText) {
    case Ok(r) -> r
    case Err(e) -> return HttpError.message(e)
  }
  match await sendAsync(req) {
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
  // A signed GET URL, fetched by a client that has no credentials.
  match bucket.presignedUrl(key, 300) {
    case Err(e) -> {
      text = text + "\nget=" + e.code
    }
    case Ok(url) -> {
      text = text + "\nget=" + fetch(url)
      text = text + "\ntampered=" + fetch(Str.replace(url, "X-Goog-Expires=300", "X-Goog-Expires=3000"))
      text = text + "\notherKey=" + fetch(Str.replace(url, "report", "REPORT"))
      text = text + "\nnoSignature=" + fetch(url.substring(0, Str.indexOfRaw(url, "&X-Goog-Signature=")))
    }
  }
  // A URL signed years ago for one minute has expired.
  text = text + "\nexpired=" + fetch(client.presign("GET", key, 60, "20200101T000000Z"))
  // A signed PUT URL: upload without credentials, then read it back signed.
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

test "signed GET and PUT URLs work for a client without credentials, until they expire" {
  val lines = Str.split(withFakeGcs(19905, presignScenario), "\n")
  assertEqualInt(lines.length, 11, "eleven lines: " + Str.join(" | ", lines))
  assertEqual(lines[0], "put=ok", "the object is stored")
  assertEqual(lines[1], "get=200 hello world", "the signed URL returns the object")
  assertEqual(lines[2], "tampered=403 SignatureDoesNotMatch", "changing the expiry invalidates the signature")
  assertEqual(lines[3], "otherKey=403 SignatureDoesNotMatch", "the URL is for one object only")
  assertEqual(lines[4], "noSignature=400 MissingSecurityHeader", "a signed URL without its signature")
  assertEqual(lines[5], "expired=400 ExpiredToken", "an expired URL is refused")
  assertEqual(lines[6], "putUrl=200", "the signed PUT URL stores an upload")
  assertEqual(lines[7], "putUrlAsGet=403 SignatureDoesNotMatch", "a PUT URL is signed for PUT only")
  assertEqual(lines[8], "uploaded=dXBsb2FkZWQ=", "the upload is readable through the bucket")
  assertEqual(lines[9], "unsafe=INVALID_KEY,INVALID_KEY", "unsafe keys are never signed")
  assertEqual(lines[10], "served 9", "every request reached the server")
}

// One request built by hand, so the server can be shown a request this
// backend never sends: `signedBody` is what the signature covers, `sentBody`
// what travels.
func rawPut(
  endpoint: in String,
  path: in String,
  signedBody: in String,
  sentBody: in String,
  googDate: in String,
  unsignedHeader: in String,
): String {
  val host = endpoint.substring(7)
  val none: List[Pair] = newList()
  val signed = googSignedHeaders(
    "PUT",
    path,
    none,
    host,
    none,
    sha256Hex(Enc.encodeUtf8(signedBody)),
    fakeKey(),
    "auto",
    googDate
  )
  val parsed = match Url.tryFrom(endpoint + path) { case Ok(u) -> u; case Err(_) -> return "bad url" }
  var req = match withBytesBody(request(PUT, parsed), "text/plain", Enc.encodeUtf8(sentBody)) {
    case Ok(r) -> r
    case Err(e) -> return HttpError.message(e)
  }
  var k = 0
  while k < signed.headers.count {
    req = match withHeader(req, signed.headers[k].name, signed.headers[k].value) {
      case Ok(r) -> r
      case Err(e) -> return HttpError.message(e)
    }
    k = k + 1
  }
  if unsignedHeader.length > 0 {
    req = match withHeader(req, unsignedHeader, "smuggled") {
      case Ok(r) -> r
      case Err(e) -> return HttpError.message(e)
    }
  }
  match await sendAsync(req) {
    case Err(e) -> "transport " + HttpError.message(e)
    case Ok(resp) -> {
      val body = match await HttpResponse.bodyText(resp) { case Ok(t) -> t; case Err(_) -> "(unreadable)" }
      val code = match parseFault(body) { case Some(f) -> f.code; case None -> "-" }
      HttpResponse.statusCode(resp).toString() + " " + code
    }
  }
}

func strictScenario(endpoint: in String): String {
  val client = clientFor(endpoint, "strict")
  val now = googDateNow()
  var text = "honest=" + rawPut(endpoint, "/strict/a.txt", "hello", "hello", now, "")
  text = text + "\nswappedBody=" + rawPut(endpoint, "/strict/b.txt", "hello", "HELLO", now, "")
  text = text + "\nstaleDate=" + rawPut(endpoint, "/strict/c.txt", "hello", "hello", "20200101T000000Z", "")
  text = text + "\nunsignedMeta=" + rawPut(endpoint, "/strict/d.txt", "hello", "hello", now, "x-goog-meta-owner")
  text = text + "\nanonymous=" + fetch(endpoint + "/strict/a.txt")
  text = text + "\notherScheme=" + fetchWith(endpoint + "/strict/a.txt", "Authorization", "AWS4-HMAC-SHA256 nonsense")
  text = text + "\nstored=" +
    (match bucketOf(client).list("", "", 10) { case Ok(r) -> r.entries.length.toString(); case Err(e) -> e.code })
  text = text + "\nbucket=" + (match bucketExists(client) { case Ok(b) -> b.toString(); case Err(e) -> e.code })
  text = text + "\nnoBucket=" +
    (
      match bucketExists(clientFor(endpoint, "missing-bucket")) {
        case Ok(b) -> b.toString()
        case Err(e) -> e.code
      }
    )
  text + "\n" + stopFake(endpoint)
}

test "the fake refuses what Cloud Storage refuses: a swapped body, a stale date, an unsigned x-goog header, no signature" {
  val lines = Str.split(withFakeGcs(19906, strictScenario), "\n")
  assertEqualInt(lines.length, 10, "ten lines: " + Str.join(" | ", lines))
  assertEqual(lines[0], "honest=200 -", "a hand-signed request that is what it says is accepted")
  assertEqual(lines[1], "swappedBody=400 ContentSha256Mismatch", "the body is not the one whose hash was signed")
  assertEqual(lines[2], "staleDate=403 RequestTimeTooSkewed", "a request signed years ago cannot be replayed")
  assertEqual(lines[3], "unsignedMeta=400 MalformedSecurityHeader", "an x-goog header outside the signature")
  assertEqual(lines[4], "anonymous=403 AccessDenied", "an unsigned request is an anonymous caller, refused")
  assertEqual(lines[5], "otherScheme=403 InvalidSecurity", "an Authorization header that is not a V4 signature")
  assertEqual(lines[6], "stored=1", "only the honest request stored an object")
  assertEqual(lines[7], "bucket=true", "HEAD Bucket of a bucket that exists")
  assertEqual(lines[8], "noBucket=false", "and of one that does not")
  assertEqual(lines[9], "served 9", "every request reached the server")
}

func anonymousScenario(endpoint: in String): String {
  val owner = clientFor(endpoint, "public-bucket")
  val noMeta: Map[String, String] = newMap()
  var text = "seed=" +
    codeOf(putBytes(owner, "pub/a b.txt", Enc.encodeUtf8("public"), "text/plain", noMeta)).substring(0, 3) +
    "," +
    codeOf(putBytes(owner, "pub/c.txt", Enc.encodeUtf8("also public"), "text/plain", noMeta)).substring(0, 3)
  val visitor = match openClient(anonymousConfig("public-bucket").withEndpoint(endpoint).withRequestTimeout(10000)) {
    case Ok(c) -> c
    case Err(e) -> panic("openClient failed: " + e.code + " " + e.message)
  }
  val bucket = bucketOf(visitor)
  text = text + "\nlist=" +
    (
      match bucket.list("pub/", "", 10) {
        case Ok(r) -> r.entries.length.toString() + ":" +
          (if r.entries.length > 0 { r.entries[0].key } else { "" })
        case Err(e) -> e.code
      }
    )
  text = text + "\nget=" + (match bucket.get("pub/a b.txt") { case Ok(obj) -> obj.dataBase64; case Err(e) -> e.code })
  text = text + "\nstat=" +
    (
      match stat(visitor, "pub/c.txt") {
        case Ok(Some(h)) -> h.contentLength.toString()
        case Ok(None) -> "absent"
        case Err(e) -> e.code
      }
    )
  text = text + "\nexists=" + (match bucket.exists("pub/none") { case Ok(b) -> b.toString(); case Err(e) -> e.code })
  text = text + "\nput=" +
    (match bucket.put("pub/mine.txt", "aGk=", "text/plain", noMeta) { case Ok(_) -> "ok"; case Err(e) -> e.code })
  text = text + "\ndelete=" + (match bucket.delete("pub/c.txt") { case Ok(_) -> "ok"; case Err(e) -> e.code })
  text = text + "\nsigned=" + codeOf(bucket.presignedUrl("pub/c.txt", 60)) + "," +
    codeOf(presignedPutUrl(visitor, "pub/c.txt", 60))
  val elsewhere = match openClient(anonymousConfig("private-bucket").withEndpoint(endpoint).withRequestTimeout(10000)) {
    case Ok(c) -> c
    case Err(e) -> panic("openClient failed: " + e.code + " " + e.message)
  }
  text = text + "\nprivateGet=" + getCode(bucketOf(elsewhere), "k")
  text = text + "\nprivateList=" +
    (match bucketOf(elsewhere).list("", "", 10) { case Ok(_) -> "ok"; case Err(e) -> e.code })
  text = text + "\nprivateMessage=" +
    (match bucketOf(elsewhere).get("k") { case Ok(_) -> "ok"; case Err(e) -> e.message })
  text + "\n" + stopFake(endpoint)
}

test "an anonymous client reads a public bucket unsigned, and can neither write nor sign" {
  val lines = Str.split(withFakeGcs(19907, anonymousScenario), "\n")
  assertEqualInt(lines.length, 12, "twelve lines: " + Str.join(" | ", lines))
  assertEqual(lines[0], "seed=ok:,ok:", "the owner stores two objects, signed")
  assertEqual(lines[1], "list=2:pub/a b.txt", "the anonymous listing")
  assertEqual(lines[2], "get=cHVibGlj", "the anonymous download")
  assertEqual(lines[3], "stat=11", "the anonymous HEAD")
  assertEqual(lines[4], "exists=false", "a missing object in a public bucket is simply absent")
  assertEqual(lines[5], "put=AccessDenied", "an anonymous caller may not write")
  assertEqual(lines[6], "delete=AccessDenied", "or delete; a refusal is not 'already gone'")
  assertEqual(lines[7], "signed=NOT_SUPPORTED,NOT_SUPPORTED", "without a key there is nothing to sign a URL with")
  assertEqual(lines[8], "privateGet=AccessDenied", "another bucket is not public")
  assertEqual(lines[9], "privateList=AccessDenied", "nor listable")
  assertEqual(
    lines[10],
    "privateMessage=cannot get object 'k': AccessDenied (HTTP 403): Access denied. Anonymous caller does not have storage.objects.get access to the Google Cloud Storage object. Permission 'storage.objects.get' denied on resource (or it may not exist).",
    "the service's details say which permission is missing"
  )
  assertEqual(lines[11], "served 11", "every request reached the server; the refused signing made none")
}

test "a request to a server that is not there is CONNECTION_FAILED, not a panic" {
  val bucket = match connect(endpointConfig("http://127.0.0.1:1", "data", "id", "secret").withRequestTimeout(5000)) {
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

// What a call that violates one of the interface's `requires:` clauses does.
func outOfRange(bucket: in StorageBucket): String {
  val listed = try {
    match bucket.list("", "", 0) {
      case Ok(_) -> "ok"
      case Err(e) -> e.code
    }
  } catch Bug as _ {
    "panic"
  }
  val signed = try {
    match bucket.presignedUrl("k", 604801) {
      case Ok(_) -> "ok"
      case Err(e) -> e.code
    }
  } catch Bug as _ {
    "panic"
  }
  listed + "," + signed
}

// The interface's `requires:` clauses are enforced on a cross-package
// implementation on the JVM and not on .NET, where the call reaches the
// backend's own range checks (see the README's "Surprises").
@cfg(target = "dotnet")
func outOfRangeExpected(): String {
  "INVALID_ARGUMENT,INVALID_ARGUMENT"
}

@cfg(target = "jvm")
func outOfRangeExpected(): String {
  "panic,panic"
}

test "out-of-range maxKeys and expiresInSeconds make no request on either target" {
  val bucket = match connect(endpointConfig("http://127.0.0.1:1", "data", "id", "secret").withRequestTimeout(5000)) {
    case Ok(b) -> b
    case Err(e) -> panic(e.message)
  }
  assertEqual(outOfRange(bucket), outOfRangeExpected(), "refused before anything is sent")
}
