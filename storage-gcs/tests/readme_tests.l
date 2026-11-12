//! tests/readme_tests.l - the usage examples of README.md, verbatim, so the
//! README cannot drift from what compiles. Nothing here makes a request: the
//! examples are compiled, and the ones that need no server are run.
@test_module
package Storage.Gcs.ReadmeTests

import Std.Core
import Std.Collections
import Std.Encoding as Enc
import Std.Http
import Std.Testing
import Storage.Gcs
import Storage.Gcs.Extras
import Storage.Gcs.Goog4
import Storage.Gcs.Wire

func example(accessId: in String, secret: in String): Result[String, StorageError] {
  val bucket = connect(gcsConfig("my-bucket", accessId, secret))?
  val stored = put(bucket, "reports/q3.txt", Enc.encodeBase64(Enc.encodeUtf8("hello")), "text/plain")?
  println(stored.etag)
  val object = get(bucket, "reports/q3.txt")?
  println(object.metadata.contentType + " " + object.dataBase64)
  val page = bucket.list("reports/", "", 100)?
  for entry in page.entries {
    println(entry.key)
  }
  if exists(bucket, "reports/old.txt")? {
    delete(bucket, "reports/old.txt")?
  }
  bucket.presignedUrl("reports/q3.txt", 3600)
}

func fromEnvironment(): Result[StorageBucket, StorageError] {
  connectFromEnv("my-bucket")
}

func adjusted(accessId: in String, secret: in String): Result[StorageBucket, StorageError] {
  val cfg = gcsConfig("my-bucket", accessId, secret).withLocation("europe-west1").withPathStyle(false).withRequestTimeout(
    10000
  )
  connect(cfg)
}

func publicData(): Result[Int, StorageError] {
  val bucket = connect(anonymousConfig("gcp-public-data-landsat"))?
  val page = bucket.list("LC08/01/001/002/", "", 10)?
  Ok(page.entries.length)
}

func otherServer(accessId: in String, secret: in String): Result[StorageBucket, StorageError] {
  connect(endpointConfig("https://gcs.example.com", "data", accessId, secret))
}

func withMetadata(bucket: in StorageBucket, pdfBase64: in String): Result[StorageMetadata, StorageError] {
  val meta: Map[String, String] = newMap()
  meta["owner"] = "alice"
  bucket.put("reports/q3.pdf", pdfBase64, "application/pdf", meta)
}

func everyKey(bucket: in StorageBucket, prefix: in String): Result[List[String], StorageError] {
  val keys: List[String] = newList()
  var token = ""
  var more = true
  while more {
    val page = bucket.list(prefix, token, 1000)?
    for entry in page.entries {
      keys.add(entry.key)
    }
    more = page.isTruncated
    token = match page.nextContinuationToken { case Some(t) -> t; case None -> "" }
  }
  Ok(keys)
}

func gcsOnly(cfg: in GcsConfig, bytes: in slice[Byte]): Result[String, StorageError] {
  val client = openClient(cfg)?
  if not bucketExists(client)? {
    return Err(StorageError(message = "no such bucket: " + client.bucket, code = "NoSuchBucket"))
  }
  val noMeta: Map[String, String] = newMap()
  val etag = putBytes(client, "raw/data.bin", bytes, "application/octet-stream", noMeta)?
  val cold = putBytesWithStorageClass(client, "raw/archive.bin", bytes, "application/octet-stream", "COLDLINE")?
  val got = getBytes(client, "raw/data.bin")?
  println(etag + cold + got.bytes.length.toString())
  match stat(client, "raw/data.bin")? {
    case Some(head) -> println(head.contentLength.toString() + " " + head.storageClass + " " + head.generation)
    case None -> println("absent")
  }
  presignedPutUrl(client, "inbox/upload.bin", 900)
}

func whyRefused(client: in GcsClient, key: in String): String {
  val none: List[Pair] = newList()
  match client.send(GET, "GET", client.objectPath(key), none, none, None) {
    case Err(e) -> e.message
    case Ok(resp) -> {
      val body = match Enc.tryDecodeUtf8(resp.body) { case Some(t) -> t; case None -> "" }
      match parseSignatureMismatch(body) {
        case Some(theirs) -> match resp.signed {
          case Some(ours) -> if theirs.canonicalRequest == ours.canonicalRequest {
            "same canonical request: the HMAC key's secret is wrong"
          } else {
            "Cloud Storage read the request differently:\n" + theirs.canonicalRequest
          }
          case None -> "the request was not signed"
        }
        case None -> "HTTP " + resp.status.toString()
      }
    }
  }
}

func codeOf(r: in Result[String, StorageError]): String {
  match r {
    case Ok(v) -> "ok:" + v
    case Err(e) -> e.code
  }
}

test "the README examples compile, and the ones that need no server behave as described" {
  // Nothing listens on port 1: every example that makes a request fails with
  // CONNECTION_FAILED, which shows it got as far as sending one.
  val unreachable = endpointConfig("http://127.0.0.1:1", "data", "GOOG1EXAMPLE", "secret").withRequestTimeout(5000)
  assertEqual(codeOf(gcsOnly(unreachable, [1u8, 2u8])), "CONNECTION_FAILED", "gcsOnly")
  val bucket = match connect(unreachable) { case Ok(b) -> b; case Err(e) -> panic(e.message) }
  match withMetadata(bucket, "aGk=") {
    case Ok(_) -> panic("nothing listens on port 1")
    case Err(e) -> assertEqual(e.code, "CONNECTION_FAILED", "withMetadata")
  }
  match everyKey(bucket, "reports/") {
    case Ok(_) -> panic("nothing listens on port 1")
    case Err(e) -> assertEqual(e.code, "CONNECTION_FAILED", "everyKey")
  }
  val client = match openClient(unreachable) { case Ok(c) -> c; case Err(e) -> panic(e.message) }
  assertTrue(whyRefused(client, "k").length > 0, "whyRefused reports the transport error")
  assertTrue(isOk(adjusted("GOOG1EXAMPLE", "secret")), "adjusted: connect makes no request")
  assertTrue(isOk(otherServer("GOOG1EXAMPLE", "secret")), "otherServer: connect makes no request")
  match fromEnvironment() {
    case Ok(_) -> ()
    case Err(e) -> assertEqual(e.code, "INVALID_CONFIG", "no HMAC key in the environment")
  }
  // `example` and `publicData` are compiled, not called: they would reach
  // storage.googleapis.com (tests/live runs `publicData`'s calls).
}
