//! tests/integration/gcs_integration_tests.l - the backend against a real
//! bucket on Google Cloud Storage, with a real HMAC key.
//!
//! Compiled only with `--features credentials`, and configured from the
//! environment:
//!
//!   LYRIC_GCS_BUCKET          a disposable bucket the key may read and write
//!   LYRIC_GCS_HMAC_ACCESS_ID  the HMAC key's access ID (`GOOG1E...`)
//!   LYRIC_GCS_HMAC_SECRET     its secret
//!   LYRIC_GCS_ENDPOINT        optional: another server that speaks the XML
//!                             API with HMAC signing; unset or `google` is
//!                             https://storage.googleapis.com
//!   LYRIC_GCS_LOCATION        optional: the location to sign for (`auto`)
//!
//!   LYRIC_GCS_BUCKET=my-test-bucket LYRIC_GCS_HMAC_ACCESS_ID=GOOG1E... \
//!   LYRIC_GCS_HMAC_SECRET=... \
//!     lyric test --manifest storage-gcs/lyric.toml --target dotnet --features credentials
//!
//! No emulator speaks this API: fake-gcs-server (v1.56.1) serves the JSON API
//! and answers an XML API PUT Object with `invalid uploadType`, and it checks
//! no signature. So this suite needs Google Cloud Storage itself, and a key
//! (`gcloud storage hmac create <service account>`).
//!
//! Use a disposable bucket: the conformance suite needs an empty bucket for
//! every case, so EVERY OBJECT IN THE BUCKET IS DELETED, repeatedly. The
//! bucket is not created by the suite (creating one needs a project).
@cfg(feature = "credentials")
@test_module
package Storage.Gcs.IntegrationTests

import Std.Core
import Std.Collections
import Std.Encoding as Enc
import Std.Environment as Env
import Std.Errors
import Std.Http
import Std.String as Str
import Std.Testing
import Testing.Conformance
import Testing.Conformance.Storage as StorageConformance
import Storage.Gcs
import Storage.Gcs.Extras
import Storage.Gcs.Wire.{parseFault}

func required(name: in String): String {
  val v = Env.getVarOrDefault(name, "")
  if v.length == 0 {
    panic(
      name + " is not set: the Cloud Storage integration tests need LYRIC_GCS_BUCKET, " +
        "LYRIC_GCS_HMAC_ACCESS_ID and LYRIC_GCS_HMAC_SECRET (see tests/integration/gcs_integration_tests.l)"
    )
  }
  v
}

func liveConfig(): GcsConfig {
  val bucket = required("LYRIC_GCS_BUCKET")
  val accessId = required("LYRIC_GCS_HMAC_ACCESS_ID")
  val secret = required("LYRIC_GCS_HMAC_SECRET")
  val endpoint = Env.getVarOrDefault("LYRIC_GCS_ENDPOINT", "")
  val location = Env.getVarOrDefault("LYRIC_GCS_LOCATION", "")
  val base = gcsConfig(bucket, accessId, secret).withEndpoint(if endpoint == "google" { "" } else { endpoint })
  if location.length > 0 {
    base.withLocation(location)
  } else {
    base
  }
}

func liveClient(): GcsClient {
  val client = match openClient(liveConfig()) {
    case Ok(c) -> c
    case Err(e) -> panic("cannot open the Cloud Storage client: " + e.code + " " + e.message)
  }
  match bucketExists(client) {
    case Ok(true) -> ()
    case Ok(false) -> panic("bucket '" + client.bucket + "' does not exist; create a disposable bucket for this suite")
    case Err(e) -> panic("cannot reach bucket '" + client.bucket + "': " + e.code + " " + e.message)
  }
  client
}

// Delete every object, so the next conformance case starts on an empty
// bucket.
func emptied(client: in GcsClient): StorageBucket {
  var token = ""
  var more = true
  while more {
    match client.listObjects("", token, 1000) {
      case Err(e) -> panic("cannot list the bucket to empty it: " + e.code + " " + e.message)
      case Ok(page) -> {
        var i = 0
        while i < page.objects.count {
          match client.deleteObject(page.objects[i].key) {
            case Ok(_) -> ()
            case Err(e) -> panic("cannot delete '" + page.objects[i].key + "': " + e.code + " " + e.message)
          }
          i = i + 1
        }
        token = page.nextToken
        more = page.isTruncated
      }
    }
  }
  bucketOf(client)
}

test "GcsBucket conforms to StorageBucket against Google Cloud Storage" {
  val client = liveClient()
  val checks = StorageConformance.run({ -> emptied(client) })
  requireConformance(checks)
  assertEqualInt(caseCount(checks), 14, "every conformance case ran")
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

test "binary objects, names that need encoding, user metadata and HEAD" {
  val client = liveClient()
  val bucket = emptied(client)
  val all = everyByte()
  val meta: Map[String, String] = newMap()
  meta["owner"] = "alice"
  meta["build.note_1"] = "two words, one comma"
  val key = "odd/sp ace+plus&amp=eqé/100%/(1)*~!$'@,;.txt"
  match putBytes(client, key, all, "application/x-lyric-test", meta) {
    case Err(e) -> panic("put failed: " + e.code + " " + e.message)
    case Ok(etag) -> assertTrue(etag.length > 0, "an ETag")
  }
  match getBytes(client, key) {
    case Err(e) -> panic("get failed: " + e.code + " " + e.message)
    case Ok(got) -> {
      assertEqual(Enc.encodeHex(got.bytes), Enc.encodeHex(all), "every byte value survives")
      assertEqual(got.head.contentType, "application/x-lyric-test", "content type")
      assertEqual(unwrapOr(mapGet(got.head.userMeta, "owner"), "-"), "alice", "user metadata")
      assertEqual(unwrapOr(mapGet(got.head.userMeta, "build.note_1"), "-"), "two words, one comma", "user metadata")
    }
  }
  match stat(client, key) {
    case Err(e) -> panic("stat failed: " + e.code + " " + e.message)
    case Ok(None) -> panic("stat says the object is absent")
    case Ok(Some(h)) -> {
      assertEqualLong(h.contentLength, 256i64, "HEAD reports the stored size")
      assertEqual(h.contentType, "application/x-lyric-test", "content type")
      assertEqualInt(h.userMeta.count, 2, "both metadata entries")
      assertTrue(h.storageClass.length > 0, "a storage class: " + h.storageClass)
      assertTrue(h.generation.length > 0, "a generation")
      assertTrue(h.lastModified.endsWith("Z") and h.lastModified.length == 20, "ISO 8601: " + h.lastModified)
    }
  }
  match bucket.list("odd/", "", 10) {
    case Err(e) -> panic("list failed: " + e.code + " " + e.message)
    case Ok(r) -> {
      assertEqualInt(r.entries.length, 1, "one entry")
      assertEqual(r.entries[0].key, key, "the listing decodes the name back")
      assertEqualLong(r.entries[0].contentLength, 256i64, "its size")
    }
  }
  match stat(client, "odd/none") {
    case Ok(None) -> ()
    case Ok(Some(_)) -> panic("an object that was never stored")
    case Err(e) -> panic("stat of a missing object failed: " + e.code)
  }
}

test "a storage class other than the bucket's default is stored" {
  val client = liveClient()
  val _ = emptied(client)
  match putBytesWithStorageClass(client, "cold/object.txt", Enc.encodeUtf8("cold"), "text/plain", "NEARLINE") {
    case Err(e) -> panic("put failed: " + e.code + " " + e.message)
    case Ok(_) -> ()
  }
  match stat(client, "cold/object.txt") {
    case Ok(Some(h)) -> assertEqual(h.storageClass, "NEARLINE", "the class of the object")
    case Ok(None) -> panic("absent")
    case Err(e) -> panic("stat failed: " + e.code)
  }
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

test "signed GET and PUT URLs work for a client without credentials" {
  val client = liveClient()
  val bucket = emptied(client)
  val noMeta: Map[String, String] = newMap()
  val key = "shared/q3 report (final).txt"
  match bucket.put(key, "aGVsbG8gd29ybGQ=", "text/plain", noMeta) {
    case Err(e) -> panic("put failed: " + e.code + " " + e.message)
    case Ok(_) -> ()
  }
  val url = match bucket.presignedUrl(key, 300) {
    case Ok(u) -> u
    case Err(e) -> panic("presignedUrl failed: " + e.code)
  }
  assertEqual(fetch(url), "200 hello world", "the signed URL returns the object")
  assertEqual(
    fetch(Str.replace(url, "X-Goog-Expires=300", "X-Goog-Expires=3000")),
    "403 SignatureDoesNotMatch",
    "changing the expiry invalidates the signature"
  )
  assertEqual(fetch(Str.replace(url, "report", "REPORT")), "403 SignatureDoesNotMatch", "the URL is for one object")
  val expired = fetch(client.presign("GET", key, 60, "20200101T000000Z"))
  assertTrue(expired.startsWith("400 ") or expired.startsWith("403 "), "an expired URL is refused: " + expired)
  val putUrl = match presignedPutUrl(client, "inbox/upload.txt", 300) {
    case Ok(u) -> u
    case Err(e) -> panic("presignedPutUrl failed: " + e.code)
  }
  assertEqual(upload(putUrl, Enc.encodeUtf8("uploaded")), "200", "the signed PUT URL stores an upload")
  match bucket.get("inbox/upload.txt") {
    case Ok(obj) -> assertEqual(obj.dataBase64, "dXBsb2FkZWQ=", "the upload is readable through the bucket")
    case Err(e) -> panic("get of the upload failed: " + e.code)
  }
}

test "Cloud Storage's own error codes reach the caller" {
  val client = liveClient()
  val bucket = emptied(client)
  match bucket.get("no/such/object") {
    case Ok(_) -> panic("the object exists")
    case Err(e) -> assertEqual(e.code, "NOT_FOUND", "NoSuchKey is NOT_FOUND")
  }
  match bucket.delete("no/such/object") {
    case Ok(_) -> ()
    case Err(e) -> panic("deleting a missing object must succeed: " + e.code + " " + e.message)
  }
  match bucket.list("", "zz-not-a-token", 10) {
    case Ok(_) -> panic("an invalid token was accepted")
    case Err(e) -> assertEqual(e.code, "InvalidArgument", "an invalid continuation token")
  }
  val cfg = liveConfig()
  val wrongSecret = match connect(gcsConfig(cfg.bucket, cfg.accessId, "not-the-secret").withEndpoint(cfg.endpoint)) {
    case Ok(b) -> b
    case Err(e) -> panic("connect failed: " + e.message)
  }
  match wrongSecret.get("k") {
    case Ok(_) -> panic("a wrong secret was accepted")
    case Err(e) -> assertEqual(e.code, "SignatureDoesNotMatch", "a wrong secret")
  }
  match wrongSecret.exists("k") {
    case Ok(_) -> panic("a wrong secret was accepted on HEAD")
    case Err(e) -> assertEqual(e.code, "HTTP_403", "HEAD has no error body")
  }
  val _ = emptied(client)
}
