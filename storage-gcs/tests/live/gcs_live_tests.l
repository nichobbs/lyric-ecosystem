//! tests/live/gcs_live_tests.l - the backend against Google Cloud Storage
//! itself (storage.googleapis.com), without credentials.
//!
//! Compiled only with `--features live`. It needs the network and nothing
//! else: no bucket of your own and no HMAC key.
//!
//!   lyric test --manifest storage-gcs/lyric.toml --target dotnet --features live
//!
//! Two things can be learnt from Cloud Storage without a key, and both are
//! checked here:
//!
//!   * How it reads a signed request. A request signed with an HMAC key that
//!     does not exist is refused with `SignatureDoesNotMatch`, and the error
//!     document carries the canonical request and the string to sign the
//!     service derived from what it received. They must be, byte for byte,
//!     the ones this client signed: then the only thing a real key would
//!     change is the HMAC over that same string. Nothing is stored and no
//!     bucket is touched: the signature is checked before the bucket is
//!     looked up, so the probe bucket need not exist.
//!   * What it answers. Public buckets are read through this client with
//!     `anonymousConfig`: listings (paged, url-encoded names), GET, HEAD
//!     (stored size, storage class, generation, ETag, dates) and the error
//!     documents for a missing object and a missing bucket.
//!
//! What this cannot show: that Cloud Storage accepts a request signed with a
//! real HMAC key, and what it answers to an authorized PUT or DELETE. That
//! is tests/integration/, which needs a bucket and a key.
//!
//! The public buckets are Google's own (`gcp-public-data-landsat`, the
//! Landsat public dataset; `cloud-samples-data`, the samples the product
//! documentation uses). Set `LYRIC_GCS_LIVE_BUCKET` and
//! `LYRIC_GCS_LIVE_PREFIX` to read another public bucket.
@cfg(feature = "live")
@test_module
package Storage.Gcs.LiveTests

import Std.Core
import Std.Collections
import Std.Encoding as Enc
import Std.Environment as Env
import Std.Errors
import Std.Http
import Std.String as Str
import Std.Testing
import Storage.Gcs
import Storage.Gcs.Extras
import Storage.Gcs.Goog4
import Storage.Gcs.Wire

// Fixtures: an access ID in the shape Cloud Storage issues and a secret,
// neither of which exists.
val PROBE_ACCESS_ID: String = "GOOG1EXAMPLEEXAMPLEEXAMPLEEXAMPLEEXAMPLEEXAMPLEEXAMPLEEXAMPL"

val PROBE_SECRET: String = "not-a-real-secret/for-signature-probes-only"

// A bucket name nobody needs to own: the signature is refused first.
val PROBE_BUCKET: String = "lyric-ecosystem-signature-probe"

func publicBucket(): String {
  Env.getVarOrDefault("LYRIC_GCS_LIVE_BUCKET", "gcp-public-data-landsat")
}

func publicPrefix(): String {
  Env.getVarOrDefault("LYRIC_GCS_LIVE_PREFIX", "LC08/01/001/002/")
}

func probeClient(pathStyle: in Bool, location: in String): GcsClient {
  val cfg = gcsConfig(PROBE_BUCKET, PROBE_ACCESS_ID, PROBE_SECRET).withPathStyle(pathStyle).withLocation(location)
  match openClient(cfg) {
    case Ok(c) -> c
    case Err(e) -> panic("openClient failed: " + e.code + " " + e.message)
  }
}

func anonymousClient(bucket: in String): GcsClient {
  match openClient(anonymousConfig(bucket)) {
    case Ok(c) -> c
    case Err(e) -> panic("openClient failed: " + e.code + " " + e.message)
  }
}

// Cloud Storage must refuse `sent` as a signature mismatch, and report the
// canonical request and string to sign this client signed.
func assertSameReading(sent: in Result[GcsResponse, StorageError], what: in String): Unit {
  val resp = match sent {
    case Ok(r) -> r
    case Err(e) -> panic(what + ": no response from storage.googleapis.com: " + e.code + " " + e.message)
  }
  val text = match Enc.tryDecodeUtf8(resp.body) { case Some(t) -> t; case None -> "" }
  assertEqualInt(resp.status, 403, what + ": refused, body: " + text)
  match parseFault(text) {
    case Some(f) -> assertEqual(f.code, "SignatureDoesNotMatch", what + ": the refusal is about the signature")
    case None -> panic(what + ": the answer is not an Error document: " + text)
  }
  val ours = match resp.signed { case Some(s) -> s; case None -> panic(what + ": the request was not signed") }
  match parseSignatureMismatch(text) {
    case None -> panic(what + ": the Error document reports no string to sign: " + text)
    case Some(theirs) -> {
      assertEqual(theirs.canonicalRequest, ours.canonicalRequest, what + ": the canonical request Cloud Storage derived")
      assertEqual(theirs.stringToSign, ours.stringToSign, what + ": the string to sign Cloud Storage derived")
      // So the signature this client sent is the HMAC of exactly the string
      // Cloud Storage would have signed with the key's real secret.
      val googDate = Str.split(theirs.stringToSign, "\n")[1]
      val location = Str.split(Str.split(theirs.stringToSign, "\n")[2], "/")[1]
      assertEqual(
        ours.signature,
        googSignatureOf(PROBE_SECRET, googDate, location, theirs.stringToSign),
        what + ": the signature sent is the HMAC of the service's string to sign"
      )
    }
  }
}

test "Cloud Storage reads a GET of a name full of reserved characters as this client signed it" {
  val client = probeClient(true, "auto")
  val none: List[Pair] = newList()
  val key = "a b+c/é(1)*~!$&'=@,;.txt/日本 100%"
  assertSameReading(client.send(GET, "GET", client.objectPath(key), none, none, None), "GET")
}

test "Cloud Storage reads a PUT with a body, user metadata and a storage class as this client signed it" {
  val client = probeClient(true, "europe-west1")
  val none: List[Pair] = newList()
  val headers: List[Pair] = newList()
  headers.add(Pair(name = "x-goog-meta-owner", value = "alice"))
  headers.add(Pair(name = "x-goog-meta-build.note_1", value = "two  words, one comma; a=b"))
  headers.add(Pair(name = "x-goog-storage-class", value = "NEARLINE"))
  val body = Payload(contentType = "text/plain", bytes = Enc.encodeUtf8("hello, Cloud Storage"))
  assertSameReading(
    client.send(PUT, "PUT", client.objectPath("reports/q3 (final).txt"), none, headers, Some(value = body)),
    "PUT"
  )
}

test "Cloud Storage reads a DELETE as this client signed it" {
  val client = probeClient(true, "auto")
  val none: List[Pair] = newList()
  assertSameReading(client.send(DELETE, "DELETE", client.objectPath("reports/q3.txt"), none, none, None), "DELETE")
}

test "Cloud Storage reads a list-type=2 listing's query string as this client signed it" {
  val client = probeClient(true, "auto")
  val none: List[Pair] = newList()
  val query: List[Pair] = newList()
  query.add(Pair(name = "list-type", value = "2"))
  query.add(Pair(name = "encoding-type", value = "url"))
  query.add(Pair(name = "max-keys", value = "1000"))
  query.add(Pair(name = "prefix", value = "a b+c/é&=?#"))
  query.add(Pair(name = "continuation-token", value = "CmhMQzA4LzAx/+=="))
  assertSameReading(client.send(GET, "GET", client.bucketPath(), query, none, None), "list")
}

test "Cloud Storage reads a virtual-hosted request as this client signed it" {
  val client = probeClient(false, "us")
  val none: List[Pair] = newList()
  assertEqual(client.host, PROBE_BUCKET + ".storage.googleapis.com", "the bucket is in the host")
  assertSameReading(client.send(GET, "GET", client.objectPath("a/b c.txt"), none, none, None), "virtual-hosted GET")
}

test "Cloud Storage reads a V4 signed URL as this client signed it" {
  val client = probeClient(true, "auto")
  val googDate = googDateNow()
  val url = client.presign("GET", "shared/q3 report (final).txt", 600, googDate)
  val marker = "&X-Goog-Signature="
  val at = Str.indexOfRaw(url, marker)
  assertTrue(at > 0, "the URL carries its signature last: " + url)
  val signature = url.substring(at + marker.length)
  val resp = match await defaultClient().get(url) {
    case Ok(r) -> r
    case Err(e) -> panic("no response from storage.googleapis.com: " + HttpError.message(e))
  }
  val text = match await HttpResponse.bodyText(resp) { case Ok(t) -> t; case Err(_) -> "" }
  assertEqualInt(HttpResponse.statusCode(resp), 403, "refused: " + text)
  match parseSignatureMismatch(text) {
    case None -> panic("the Error document reports no string to sign: " + text)
    case Some(theirs) -> {
      val lines = Str.split(theirs.canonicalRequest, "\n")
      assertEqual(lines[0], "GET", "the method")
      assertEqual(
        lines[1],
        "/" + PROBE_BUCKET + "/shared/q3%20report%20%28final%29.txt",
        "the resource path, encoded as this client encoded it"
      )
      assertEqual(
        lines[2],
        url.substring(Str.indexOfRaw(url, "?") + 1, at - Str.indexOfRaw(url, "?") - 1),
        "the canonical query string is the URL's, without the signature"
      )
      assertEqual(lines[lines.length - 1], "UNSIGNED-PAYLOAD", "the payload of a signed URL")
      assertEqual(
        theirs.stringToSign,
        googStringToSign(googDate, googCredentialScope(googDate, "auto"), theirs.canonicalRequest),
        "the string to sign is built from that canonical request as this package builds it"
      )
      assertEqual(
        signature,
        googSignatureOf(PROBE_SECRET, googDate, "auto", theirs.stringToSign),
        "the signature in the URL is the HMAC of the service's string to sign"
      )
    }
  }
}

test "a public bucket is listed anonymously: pages, order, sizes, ETags, the continuation token" {
  val client = anonymousClient(publicBucket())
  val first = match client.listObjects(publicPrefix(), "", 3) {
    case Ok(p) -> p
    case Err(e) -> panic("list failed: " + e.code + " " + e.message)
  }
  assertEqualInt(first.objects.count, 3, "a page of max-keys entries")
  assertTrue(first.isTruncated, "there is more")
  assertTrue(first.nextToken.length > 0, "and a token to continue with")
  val second = match client.listObjects(publicPrefix(), first.nextToken, 3) {
    case Ok(p) -> p
    case Err(e) -> panic("list of the second page failed: " + e.code + " " + e.message)
  }
  assertEqualInt(second.objects.count, 3, "the second page")
  var i = 0
  while i < 3 {
    val a = first.objects[i]
    assertTrue(a.key.startsWith(publicPrefix()), "under the prefix: " + a.key)
    assertTrue(a.etag.length > 0 and not a.etag.contains("\""), "an unquoted ETag: " + a.etag)
    assertTrue(a.lastModified.length >= 20 and a.lastModified.endsWith("Z"), "an ISO 8601 date: " + a.lastModified)
    assertTrue(a.generation.length > 0, "a generation")
    assertTrue(a.size >= 0i64, "a size")
    i = i + 1
  }
  assertTrue(first.objects[0].key < first.objects[1].key and first.objects[1].key < first.objects[2].key, "in order")
  assertTrue(first.objects[2].key < second.objects[0].key, "the second page continues after the first")
  // Through the interface.
  match bucketOf(client).list(publicPrefix(), "", 2) {
    case Err(e) -> panic("StorageBucket.list failed: " + e.code + " " + e.message)
    case Ok(r) -> {
      assertEqualInt(r.entries.length, 2, "two entries")
      assertTrue(r.isTruncated and isSome(r.nextContinuationToken), "truncated, with a token")
      assertEqual(r.entries[0].key, first.objects[0].key, "the same first object")
    }
  }
  match client.listObjects(publicPrefix(), "zz-not-a-token", 3) {
    case Ok(_) -> panic("an invalid token was accepted")
    case Err(e) -> assertEqual(e.code, "InvalidArgument", "an invalid continuation token: " + e.message)
  }
}

test "a public object is read anonymously: HEAD reports what the listing and GET report" {
  val client = anonymousClient(publicBucket())
  val page = match client.listObjects(publicPrefix(), "", 50) {
    case Ok(p) -> p
    case Err(e) -> panic("list failed: " + e.code + " " + e.message)
  }
  // The smallest object of the page, so the download stays small.
  var pick = page.objects[0]
  var i = 1
  while i < page.objects.count {
    if page.objects[i].size < pick.size {
      pick = page.objects[i]
    }
    i = i + 1
  }
  assertTrue(pick.size < 5000000i64, "a small object to download: " + pick.key + " " + pick.size.toString())
  val head = match stat(client, pick.key) {
    case Ok(Some(h)) -> h
    case Ok(None) -> panic("HEAD says the listed object is absent: " + pick.key)
    case Err(e) -> panic("HEAD failed: " + e.code + " " + e.message)
  }
  assertEqualLong(head.contentLength, pick.size, "HEAD's stored size is the listed size")
  assertEqual(head.etag, pick.etag, "HEAD's ETag is the listed ETag")
  assertEqual(head.generation, pick.generation, "HEAD's generation is the listed generation")
  assertTrue(head.storageClass.length > 0, "a storage class: " + head.storageClass)
  assertEqual(head.lastModified.substring(0, 19), pick.lastModified.substring(0, 19), "the same instant, to the second")
  assertTrue(head.lastModified.endsWith("Z") and head.lastModified.length == 20, "Last-Modified as ISO 8601")
  val got = match getBytes(client, pick.key) {
    case Ok(g) -> g
    case Err(e) -> panic("GET failed: " + e.code + " " + e.message)
  }
  assertEqualLong(got.bytes.length.toLong(), pick.size, "GET returns the listed number of bytes")
  assertEqual(got.head.etag, pick.etag, "GET's ETag")
  assertEqual(got.head.contentType, head.contentType, "GET's content type is HEAD's")
  match bucketOf(client).exists(pick.key) {
    case Ok(b) -> assertTrue(b, "exists")
    case Err(e) -> panic("exists failed: " + e.code)
  }
  match bucketOf(client).get(pick.key) {
    case Ok(obj) -> assertEqual(obj.dataBase64, Enc.encodeBase64(got.bytes), "StorageBucket.get, as base64")
    case Err(e) -> panic("StorageBucket.get failed: " + e.code)
  }
}

test "names with spaces and non-ASCII characters are listed decoded and can be read back" {
  // Google's samples bucket holds PDFs named after page titles: spaces and
  // U+00A0 in the names.
  val client = anonymousClient("cloud-samples-data")
  val page = match client.listObjects("adk-samples/data-science/bqml/The", "", 5) {
    case Ok(p) -> p
    case Err(e) -> panic("list failed: " + e.code + " " + e.message)
  }
  assertTrue(page.objects.count > 0, "the samples are there")
  val name = page.objects[0].key
  assertTrue(name.contains(" ") and not name.contains("+") and not name.contains("%"), "decoded: " + name)
  match stat(client, name) {
    case Ok(Some(h)) -> assertEqualLong(h.contentLength, page.objects[0].size, "HEAD finds the object under that name")
    case Ok(None) -> panic("the decoded name does not name the object: " + name)
    case Err(e) -> panic("HEAD failed: " + e.code + " " + e.message)
  }
}

test "a missing object and a missing bucket are told apart, as Cloud Storage reports them" {
  val client = anonymousClient(publicBucket())
  match client.getObject("lyric/no/such/object") {
    case Ok(_) -> panic("the object exists")
    case Err(e) -> {
      assertEqual(e.code, "NOT_FOUND", "NoSuchKey is NOT_FOUND")
      assertTrue(e.message.contains("NoSuchKey (HTTP 404)"), "the message keeps Cloud Storage's code: " + e.message)
    }
  }
  match stat(client, "lyric/no/such/object") {
    case Ok(None) -> ()
    case Ok(Some(_)) -> panic("the object exists")
    case Err(e) -> panic("HEAD failed: " + e.code)
  }
  match bucketExists(client) {
    case Ok(b) -> assertTrue(b, "the public bucket exists")
    case Err(e) -> panic("HEAD Bucket failed: " + e.code)
  }
  val missing = anonymousClient("lyric-ecosystem-no-such-bucket-zz91x")
  match missing.getObject("k") {
    case Ok(_) -> panic("the bucket exists")
    case Err(e) -> assertEqual(e.code, "NoSuchBucket", "a missing bucket on GET")
  }
  match missing.listObjects("", "", 1) {
    case Ok(_) -> panic("the bucket exists")
    case Err(e) -> assertEqual(e.code, "NoSuchBucket", "a missing bucket on list")
  }
  match bucketExists(missing) {
    case Ok(b) -> assertFalse(b, "HEAD Bucket of a missing bucket")
    case Err(e) -> panic("HEAD Bucket failed: " + e.code)
  }
  // An anonymous caller may not write to or delete from a public bucket.
  val noMeta: Map[String, String] = newMap()
  match putBytes(client, "lyric/probe.txt", Enc.encodeUtf8("x"), "text/plain", noMeta) {
    case Ok(_) -> panic("an anonymous PUT was accepted")
    case Err(e) -> assertEqual(e.code, "AccessDenied", "PUT is refused: " + e.message)
  }
  match bucketOf(client).presignedUrl("k", 60) {
    case Ok(_) -> panic("an anonymous client cannot sign")
    case Err(e) -> assertEqual(e.code, "NOT_SUPPORTED", "no key, no signed URL")
  }
}
