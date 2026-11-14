//! tests/integration/s3_integration_tests.l - the backend against a real
//! S3-compatible server (MinIO in CI; AWS S3 works too).
//!
//! Compiled only with `--features integration`, and configured from the
//! environment:
//!
//!   LYRIC_S3_ENDPOINT       base URL of the server, e.g. http://127.0.0.1:9000
//!                           (set it to `aws` to use AWS S3 itself)
//!   LYRIC_S3_BUCKET         a bucket for this run; created when it is missing
//!   LYRIC_S3_REGION         the region to sign for, e.g. us-east-1
//!   AWS_ACCESS_KEY_ID       credentials allowed to create and use the bucket
//!   AWS_SECRET_ACCESS_KEY
//!   AWS_SESSION_TOKEN       optional, for temporary credentials
//!   LYRIC_S3_CA_FILE        optional: a PEM file with a certificate authority
//!                           to trust in addition to the system trust store
//!                           (`withCaCertificateFile`), for a server whose
//!                           certificate is private (AWS Emulator in CI)
//!
//!   LYRIC_S3_ENDPOINT=http://127.0.0.1:9000 LYRIC_S3_BUCKET=lyric-it \
//!   LYRIC_S3_REGION=us-east-1 AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... \
//!     lyric test --manifest storage-s3/lyric.toml --target dotnet --features integration
//!
//! Use a disposable bucket: the conformance suite needs an empty bucket for
//! every case, so EVERY OBJECT IN THE BUCKET IS DELETED, repeatedly.
@cfg(feature = "integration")
@test_module
package Storage.S3.IntegrationTests

import Std.Core
import Std.Collections
import Std.Encoding as Enc
import Std.Environment as Env
import Std.Errors
import Std.Http
import Std.String as Str
import Std.Testing
import Std.Tls as Tls
import Testing.Conformance
import Testing.Conformance.Storage as StorageConformance
import Storage.S3
import Storage.S3.Extras

func required(name: in String): String {
  val v = Env.getVarOrDefault(name, "")
  if v.length == 0 {
    panic(
      name + " is not set: the S3 integration tests need LYRIC_S3_ENDPOINT, LYRIC_S3_BUCKET, LYRIC_S3_REGION, " +
        "AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY (see tests/integration/s3_integration_tests.l)"
    )
  }
  v
}

func liveConfig(): S3Config {
  val endpoint = required("LYRIC_S3_ENDPOINT")
  val bucket = required("LYRIC_S3_BUCKET")
  val region = required("LYRIC_S3_REGION")
  val keyId = required("AWS_ACCESS_KEY_ID")
  val secret = required("AWS_SECRET_ACCESS_KEY")
  val base = if endpoint == "aws" {
    awsConfig(bucket, region, keyId, secret)
  } else {
    endpointConfig(endpoint, bucket, region, keyId, secret)
  }
  val withToken = base.withSessionToken(Env.getVarOrDefault("AWS_SESSION_TOKEN", ""))
  val caFile = Env.getVarOrDefault("LYRIC_S3_CA_FILE", "")
  if caFile.length == 0 {
    return withToken
  }
  match withToken.withCaCertificateFile(caFile) {
    case Ok(cfg) -> cfg
    case Err(e) -> panic("LYRIC_S3_CA_FILE: " + e.code + " " + e.message)
  }
}

func liveClient(): S3Client {
  match openClient(liveConfig()) {
    case Ok(c) -> c
    case Err(e) -> panic("cannot open the S3 client: " + e.code + " " + e.message)
  }
}

// The bucket, created on first use.
func readyClient(): S3Client {
  val client = liveClient()
  match bucketExists(client) {
    case Ok(true) -> ()
    case Ok(false) -> {
      match createBucket(client) {
        case Ok(_) -> ()
        case Err(e) -> panic("cannot create bucket '" + client.bucket + "': " + e.code + " " + e.message)
      }
    }
    case Err(e) -> panic("cannot reach bucket '" + client.bucket + "': " + e.code + " " + e.message)
  }
  client
}

// Delete every object, so the next conformance case starts on an empty bucket.
func emptied(client: in S3Client): StorageBucket {
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

test "S3Bucket conforms to StorageBucket against a real S3 server" {
  val client = readyClient()
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

test "binary objects, encoded keys, metadata and HEAD against a real S3 server" {
  val client = readyClient()
  val bucket = emptied(client)
  val key = "it/sp ace+plus&amp=eqé/100%/(1)*.bin"
  val meta: Map[String, String] = newMap()
  meta["owner"] = "alice"
  meta["build.note_1"] = "two words, one comma"
  val all = everyByte()
  val etag = match putBytes(client, key, all, "application/x-lyric-test", meta) {
    case Ok(e) -> e
    case Err(e) -> panic("putBytes failed: " + e.code + " " + e.message)
  }
  assertTrue(etag.length > 0 and not etag.startsWith("\""), "put returns an unquoted ETag: " + etag)
  match getBytes(client, key) {
    case Err(e) -> panic("getBytes failed: " + e.code + " " + e.message)
    case Ok(got) -> {
      assertEqual(Enc.encodeHex(got.bytes), Enc.encodeHex(all), "all 256 byte values come back unchanged")
      assertEqual(got.head.contentType, "application/x-lyric-test", "content type")
      assertEqual(got.head.etag, etag, "the same ETag on get")
      assertTrue(
        got.head.lastModified.endsWith("Z") and got.head.lastModified.contains("T"),
        "Last-Modified is ISO 8601: " + got.head.lastModified
      )
    }
  }
  match stat(client, key) {
    case Err(e) -> panic("stat failed: " + e.code + " " + e.message)
    case Ok(None) -> panic("stat says the object is absent")
    case Ok(Some(head)) -> {
      assertEqualLong(head.contentLength, 256i64, "HEAD reports the object's size")
      assertEqual(head.contentType, "application/x-lyric-test", "HEAD content type")
      assertEqual(head.etag, etag, "HEAD ETag")
      assertEqualInt(head.userMeta.count, 2, "both metadata entries")
      match mapGet(head.userMeta, "owner") {
        case Some(v) -> assertEqual(v, "alice", "metadata owner")
        case None -> panic("metadata 'owner' is missing")
      }
      match mapGet(head.userMeta, "build.note_1") {
        case Some(v) -> assertEqual(v, "two words, one comma", "metadata with punctuation in key and value")
        case None -> panic("metadata 'build.note_1' is missing")
      }
    }
  }
  match stat(client, "it/never-stored") {
    case Ok(None) -> ()
    case Ok(Some(_)) -> panic("stat found an object that was never stored")
    case Err(e) -> panic("stat of a missing key failed: " + e.code + " " + e.message)
  }
  match bucket.list("it/", "", 10) {
    case Err(e) -> panic("list failed: " + e.code + " " + e.message)
    case Ok(r) -> {
      assertEqualInt(r.entries.length, 1, "one object under the prefix")
      assertEqual(r.entries[0].key, key, "the listing returns the key exactly")
      assertEqualLong(r.entries[0].contentLength, 256i64, "the listing reports the size")
      assertEqual(r.entries[0].etag, etag, "the listing reports the ETag")
    }
  }
  val noMeta: Map[String, String] = newMap()
  match bucket.put("it/empty", "", "text/plain", noMeta) {
    case Err(e) -> panic("put of an empty object failed: " + e.code + " " + e.message)
    case Ok(m) -> assertEqualLong(m.contentLength, 0i64, "an empty object")
  }
  match bucket.get("it/empty") {
    case Err(e) -> panic("get of an empty object failed: " + e.code + " " + e.message)
    case Ok(obj) -> assertEqual(obj.dataBase64, "", "an empty object reads back empty")
  }
}

// A client without credentials, as whoever is handed a presigned URL has. It
// trusts LYRIC_S3_CA_FILE when that is set, as the backend under test does.
func plainClient(): HttpClient {
  val caFile = Env.getVarOrDefault("LYRIC_S3_CA_FILE", "")
  if caFile.length == 0 {
    return defaultClient()
  }
  match Tls.Certificate.fromPemFile(caFile) {
    case Ok(ca) -> HttpClientBuilder.new().withCaCertificate(ca).build()
    case Err(e) -> panic("LYRIC_S3_CA_FILE: " + Tls.TlsError.message(e))
  }
}

func fetchText(url: in String): String {
  match await plainClient().get(url) {
    case Err(e) -> "transport " + HttpError.message(e)
    case Ok(resp) -> {
      val body = match await HttpResponse.bodyText(resp) { case Ok(t) -> t; case Err(_) -> "(unreadable)" }
      HttpResponse.statusCode(resp).toString() + " " + body
    }
  }
}

test "presigned GET and PUT URLs against a real S3 server" {
  val client = readyClient()
  val bucket = emptied(client)
  val noMeta: Map[String, String] = newMap()
  val key = "shared/q3 report (final).txt"
  match bucket.put(key, "aGVsbG8gd29ybGQ=", "text/plain", noMeta) {
    case Ok(_) -> ()
    case Err(e) -> panic("put failed: " + e.code + " " + e.message)
  }
  val url = match bucket.presignedUrl(key, 300) {
    case Ok(u) -> u
    case Err(e) -> panic("presignedUrl failed: " + e.code + " " + e.message)
  }
  assertEqual(fetchText(url), "200 hello world", "the presigned URL returns the object to a client without credentials")
  assertTrue(
    fetchText(Str.replace(url, "X-Amz-Expires=300", "X-Amz-Expires=3000")).startsWith("403 "),
    "a tampered URL is refused"
  )
  assertTrue(
    fetchText(client.presign("GET", key, 60, "20200101T000000Z")).startsWith("403 "),
    "an expired URL is refused"
  )
  val putUrl = match presignedPutUrl(client, "inbox/upload.txt", 300) {
    case Ok(u) -> u
    case Err(e) -> panic("presignedPutUrl failed: " + e.code + " " + e.message)
  }
  val parsed = match Url.tryFrom(putUrl) {
    case Ok(u) -> u
    case Err(e) -> panic("the presigned PUT URL is not a URL: " + HttpError.message(e))
  }
  val upload = match withBytesBody(request(PUT, parsed), "text/plain", Enc.encodeUtf8("uploaded")) {
    case Ok(r) -> r
    case Err(e) -> panic(HttpError.message(e))
  }
  match await plainClient().send(upload) {
    case Err(e) -> panic("the presigned PUT failed: " + HttpError.message(e))
    case Ok(resp) -> assertEqualInt(HttpResponse.statusCode(resp), 200, "the presigned PUT is accepted")
  }
  match bucket.get("inbox/upload.txt") {
    case Err(e) -> panic("get of the upload failed: " + e.code + " " + e.message)
    case Ok(obj) -> assertEqual(obj.dataBase64, "dXBsb2FkZWQ=", "the upload is stored")
  }
}

test "errors from a real S3 server carry its codes" {
  val client = readyClient()
  val bucket = emptied(client)
  match bucket.get("no/such/key") {
    case Ok(_) -> panic("a missing key was found")
    case Err(e) -> assertEqual(e.code, "NOT_FOUND", "NoSuchKey is NOT_FOUND")
  }
  val cfg = liveConfig()
  val wrongSecret = cfg.copy(secretAccessKey = cfg.secretAccessKey + "x")
  match connect(wrongSecret) {
    case Err(e) -> panic("connect failed: " + e.message)
    case Ok(b) -> match b.get("k") {
      case Ok(_) -> panic("a wrong secret was accepted")
      case Err(e) -> assertEqual(e.code, "SignatureDoesNotMatch", "a wrong secret: " + e.message)
    }
  }
  val otherBucket = cfg.copy(bucket = cfg.bucket + "-absent-" + cfg.region)
  match connect(otherBucket) {
    case Err(e) -> panic("connect failed: " + e.message)
    case Ok(b) -> match b.list("", "", 10) {
      case Ok(_) -> panic("a bucket that does not exist was listed")
      case Err(e) -> assertEqual(e.code, "NoSuchBucket", "a missing bucket: " + e.message)
    }
  }
  match createBucket(client) {
    // Amazon S3 (and AWS Emulator) answer 200 for this in us-east-1; MinIO
    // and every other region answer BucketAlreadyOwnedByYou.
    case Ok(created) -> assertTrue(
      not created or cfg.region == "us-east-1",
      "creating the bucket again reports it as already owned"
    )
    case Err(e) -> panic("createBucket of an existing bucket failed: " + e.code + " " + e.message)
  }
}
