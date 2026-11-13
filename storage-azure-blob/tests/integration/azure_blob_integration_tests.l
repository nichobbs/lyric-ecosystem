//! tests/integration/azure_blob_integration_tests.l - the backend against a
//! real Blob service (Azurite, the official emulator, in CI; an Azure storage
//! account works too).
//!
//! Compiled only with `--features integration`, and configured from the
//! environment:
//!
//!   LYRIC_AZURE_BLOB_ENDPOINT   the Blob endpoint, e.g.
//!                               http://127.0.0.1:10000/devstoreaccount1
//!                               (set it to `azure` to use
//!                               https://<account>.blob.core.windows.net)
//!   LYRIC_AZURE_BLOB_ACCOUNT    the storage account name
//!   LYRIC_AZURE_BLOB_KEY        the account key, base64
//!   LYRIC_AZURE_BLOB_CONTAINER  a container for this run; created when it is
//!                               missing
//!
//!   LYRIC_AZURE_BLOB_ENDPOINT=http://127.0.0.1:10000/devstoreaccount1 \
//!   LYRIC_AZURE_BLOB_ACCOUNT=devstoreaccount1 LYRIC_AZURE_BLOB_KEY=... \
//!   LYRIC_AZURE_BLOB_CONTAINER=lyric-it \
//!     lyric test --manifest storage-azure-blob/lyric.toml --target dotnet --features integration
//!
//! Use a disposable container: the conformance suite needs an empty container
//! for every case, so EVERY BLOB IN THE CONTAINER IS DELETED, repeatedly.
@cfg(feature = "integration")
@test_module
package Storage.AzureBlob.IntegrationTests

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
import Storage.AzureBlob
import Storage.AzureBlob.Extras

func required(name: in String): String {
  val v = Env.getVarOrDefault(name, "")
  if v.length == 0 {
    panic(
      name + " is not set: the Azure Blob integration tests need LYRIC_AZURE_BLOB_ENDPOINT, " +
        "LYRIC_AZURE_BLOB_ACCOUNT, LYRIC_AZURE_BLOB_KEY and LYRIC_AZURE_BLOB_CONTAINER " +
        "(see tests/integration/azure_blob_integration_tests.l)"
    )
  }
  v
}

func liveConfig(): AzureBlobConfig {
  val endpoint = required("LYRIC_AZURE_BLOB_ENDPOINT")
  val account = required("LYRIC_AZURE_BLOB_ACCOUNT")
  val key = required("LYRIC_AZURE_BLOB_KEY")
  val container = required("LYRIC_AZURE_BLOB_CONTAINER")
  if endpoint == "azure" {
    azureConfig(account, key, container)
  } else {
    endpointConfig(endpoint, account, key, container)
  }
}

func liveClient(): AzureBlobClient {
  match openClient(liveConfig()) {
    case Ok(c) -> c
    case Err(e) -> panic("cannot open the Blob client: " + e.code + " " + e.message)
  }
}

// The container, created on first use.
func readyClient(): AzureBlobClient {
  val client = liveClient()
  match containerExists(client) {
    case Ok(true) -> ()
    case Ok(false) -> {
      match createContainer(client) {
        case Ok(_) -> ()
        case Err(e) -> panic("cannot create container '" + client.container + "': " + e.code + " " + e.message)
      }
    }
    case Err(e) -> panic("cannot reach container '" + client.container + "': " + e.code + " " + e.message)
  }
  client
}

// Delete every blob, so the next conformance case starts on an empty
// container.
func emptied(client: in AzureBlobClient): StorageBucket {
  var marker = ""
  var more = true
  while more {
    match client.listBlobs("", marker, 1000) {
      case Err(e) -> panic("cannot list the container to empty it: " + e.code + " " + e.message)
      case Ok(page) -> {
        var i = 0
        while i < page.blobs.count {
          match client.deleteBlob(page.blobs[i].name) {
            case Ok(_) -> ()
            case Err(e) -> panic("cannot delete '" + page.blobs[i].name + "': " + e.code + " " + e.message)
          }
          i = i + 1
        }
        marker = page.nextMarker
        more = marker.length > 0
      }
    }
  }
  bucketOf(client)
}

test "AzureBlobBucket conforms to StorageBucket against a real Blob service" {
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

test "binary blobs, encoded names, metadata and properties against a real Blob service" {
  val client = readyClient()
  val bucket = emptied(client)
  val key = "it/sp ace+plus&amp=eqé/(1)*'~!$,;@.bin"
  val meta: Map[String, String] = newMap()
  meta["owner"] = "alice"
  meta["_build_note"] = "two words, one comma"
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
      assertEqual(got.properties.contentType, "application/x-lyric-test", "content type")
      assertEqual(got.properties.etag, etag, "the same ETag on get")
      assertEqual(got.properties.blobType, "BlockBlob", "a block blob")
      assertTrue(
        got.properties.lastModified.endsWith("Z") and got.properties.lastModified.contains("T"),
        "Last-Modified is ISO 8601: " + got.properties.lastModified
      )
    }
  }
  match stat(client, key) {
    case Err(e) -> panic("stat failed: " + e.code + " " + e.message)
    case Ok(None) -> panic("stat says the blob is absent")
    case Ok(Some(properties)) -> {
      assertEqualLong(properties.contentLength, 256i64, "Get Blob Properties reports the blob's size")
      assertEqual(properties.contentType, "application/x-lyric-test", "properties content type")
      assertEqual(properties.etag, etag, "properties ETag")
      assertTrue(properties.createdAt.endsWith("Z"), "creation time is ISO 8601: " + properties.createdAt)
      assertEqualInt(properties.userMeta.count, 2, "both metadata entries")
      match mapGet(properties.userMeta, "owner") {
        case Some(v) -> assertEqual(v, "alice", "metadata owner")
        case None -> panic("metadata 'owner' is missing")
      }
      match mapGet(properties.userMeta, "_build_note") {
        case Some(v) -> assertEqual(
          v,
          "two words, one comma",
          "metadata with underscores in the name and punctuation in the value"
        )
        case None -> panic("metadata '_build_note' is missing")
      }
    }
  }
  match stat(client, "it/never-stored") {
    case Ok(None) -> ()
    case Ok(Some(_)) -> panic("stat found a blob that was never stored")
    case Err(e) -> panic("stat of a missing key failed: " + e.code + " " + e.message)
  }
  match bucket.list("it/", "", 10) {
    case Err(e) -> panic("list failed: " + e.code + " " + e.message)
    case Ok(r) -> {
      assertEqualInt(r.entries.length, 1, "one blob under the prefix")
      assertEqual(r.entries[0].key, key, "the listing returns the name exactly")
      assertEqualLong(r.entries[0].contentLength, 256i64, "the listing reports the size")
      assertEqual(r.entries[0].etag, etag, "the listing reports the ETag")
      assertTrue(r.entries[0].lastModified.endsWith("Z"), "the listing's date is ISO 8601: " + r.entries[0].lastModified)
    }
  }
  val noMeta: Map[String, String] = newMap()
  match bucket.put("it/empty", "", "text/plain", noMeta) {
    case Err(e) -> panic("put of an empty blob failed: " + e.code + " " + e.message)
    case Ok(m) -> assertEqualLong(m.contentLength, 0i64, "an empty blob")
  }
  match bucket.get("it/empty") {
    case Err(e) -> panic("get of an empty blob failed: " + e.code + " " + e.message)
    case Ok(obj) -> assertEqual(obj.dataBase64, "", "an empty blob reads back empty")
  }
  // Metadata the service would reject or alter never reaches it.
  val bad: Map[String, String] = newMap()
  bad["build-note"] = "x"
  match bucket.put("it/bad-meta", "", "text/plain", bad) {
    case Ok(_) -> panic("a metadata name that is not a C# identifier was accepted")
    case Err(e) -> assertEqual(e.code, "USER_META_UNSUPPORTED", "a hyphen in a metadata name")
  }
}

test "access tiers against a real Blob service" {
  val client = readyClient()
  val _ = emptied(client)
  match putBytesWithAccessTier(client, "tier/cool.txt", [1u8, 2u8], "text/plain", "Cool") {
    case Ok(_) -> ()
    case Err(e) -> panic("put with an access tier failed: " + e.code + " " + e.message)
  }
  match stat(client, "tier/cool.txt") {
    case Ok(Some(properties)) -> assertEqual(properties.accessTier, "Cool", "the tier given at upload")
    case Ok(None) -> panic("the blob is absent")
    case Err(e) -> panic("stat failed: " + e.code + " " + e.message)
  }
  match setAccessTier(client, "tier/cool.txt", "Hot") {
    case Ok(_) -> ()
    case Err(e) -> panic("setAccessTier failed: " + e.code + " " + e.message)
  }
  match stat(client, "tier/cool.txt") {
    case Ok(Some(properties)) -> assertEqual(properties.accessTier, "Hot", "the tier after Set Blob Tier")
    case Ok(None) -> panic("the blob is absent")
    case Err(e) -> panic("stat failed: " + e.code + " " + e.message)
  }
  match setAccessTier(client, "tier/never-stored", "Cool") {
    case Ok(_) -> panic("the tier of a missing blob was set")
    case Err(e) -> assertEqual(e.code, "NOT_FOUND", "Set Blob Tier on a missing blob")
  }
}

func fetchText(url: in String): String {
  match await defaultClient().get(url) {
    case Err(e) -> "transport " + HttpError.message(e)
    case Ok(resp) -> {
      val body = match await HttpResponse.bodyText(resp) { case Ok(t) -> t; case Err(_) -> "(unreadable)" }
      HttpResponse.statusCode(resp).toString() + " " + body
    }
  }
}

func uploadTo(url: in String, text: in String): Int {
  val parsed = match Url.tryFrom(url) {
    case Ok(u) -> u
    case Err(e) -> panic("the SAS URL is not a URL: " + HttpError.message(e))
  }
  val withBody = match withBytesBody(request(PUT, parsed), "text/plain", Enc.encodeUtf8(text)) {
    case Ok(r) -> r
    case Err(e) -> panic(HttpError.message(e))
  }
  val upload = match withHeader(withBody, "x-ms-blob-type", "BlockBlob") {
    case Ok(r) -> r
    case Err(e) -> panic(HttpError.message(e))
  }
  match await sendAsync(upload) {
    case Err(e) -> panic("the SAS upload failed: " + HttpError.message(e))
    case Ok(resp) -> HttpResponse.statusCode(resp)
  }
}

test "service SAS URLs for reading and writing against a real Blob service" {
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
  assertEqual(fetchText(url), "200 hello world", "the SAS URL returns the blob to a client without credentials")
  assertTrue(fetchText(Str.replace(url, "sp=r", "sp=rw")).startsWith("403 "), "a tampered URL is refused")
  assertTrue(fetchText(Str.replace(url, "report", "REPORT")).startsWith("403 "), "the SAS is for one blob only")
  assertEqualInt(uploadTo(url, "overwritten"), 403, "a read SAS does not allow an upload")
  val expired = match client.sasUrl(key, "r", 1577836860i64) {
    case Ok(u) -> u
    case Err(e) -> panic("sasUrl failed: " + e.code + " " + e.message)
  }
  assertTrue(fetchText(expired).startsWith("403 "), "a SAS that expired in 2020 is refused")
  val putUrl = match presignedPutUrl(client, "inbox/upload.txt", 300) {
    case Ok(u) -> u
    case Err(e) -> panic("presignedPutUrl failed: " + e.code + " " + e.message)
  }
  assertEqualInt(uploadTo(putUrl, "uploaded"), 201, "the write SAS accepts an upload")
  assertTrue(fetchText(putUrl).startsWith("403 "), "a write SAS does not allow a read")
  match bucket.get("inbox/upload.txt") {
    case Err(e) -> panic("get of the upload failed: " + e.code + " " + e.message)
    case Ok(obj) -> assertEqual(obj.dataBase64, "dXBsb2FkZWQ=", "the upload is stored")
  }
}

// Azure answers a signature it cannot verify with `AuthenticationFailed`;
// Azurite answers `AuthorizationFailure`.
func isAuthFailure(code: in String): Bool {
  code == "AuthenticationFailed" or code == "AuthorizationFailure"
}

test "errors from a real Blob service carry its codes" {
  val client = readyClient()
  val bucket = emptied(client)
  match bucket.get("no/such/key") {
    case Ok(_) -> panic("a missing key was found")
    case Err(e) -> {
      assertEqual(e.code, "NOT_FOUND", "BlobNotFound is NOT_FOUND")
      assertTrue(e.message.contains("BlobNotFound"), "the message keeps the service's code: " + e.message)
    }
  }
  val cfg = liveConfig()
  // A different, well-formed key: 64 zero bytes in base64. FIXTURE, opens nothing.
  val zeros: List[Byte] = newList()
  var i = 0
  while i < 64 {
    zeros.add(0u8)
    i = i + 1
  }
  val wrongKey = AzureBlobConfig(
    accountName = cfg.accountName,
    accountKey = Enc.encodeBase64(zeros.toArray()),
    container = cfg.container,
    endpoint = cfg.endpoint,
    requestTimeoutMs = cfg.requestTimeoutMs
  )
  match connect(wrongKey) {
    case Err(e) -> panic("connect failed: " + e.message)
    case Ok(b) -> {
      match b.get("k") {
        case Ok(_) -> panic("a wrong key was accepted")
        case Err(e) -> assertTrue(isAuthFailure(e.code), "a wrong key on GET: " + e.code + " " + e.message)
      }
      match b.exists("k") {
        case Ok(_) -> panic("a wrong key was accepted")
        case Err(e) -> assertTrue(
          isAuthFailure(e.code),
          "and on HEAD, from the x-ms-error-code header: " + e.code + " " + e.message
        )
      }
    }
  }
  val otherContainer = AzureBlobConfig(
    accountName = cfg.accountName,
    accountKey = cfg.accountKey,
    container = cfg.container + "-absent",
    endpoint = cfg.endpoint,
    requestTimeoutMs = cfg.requestTimeoutMs
  )
  match openClient(otherContainer) {
    case Err(e) -> panic("openClient failed: " + e.message)
    case Ok(absent) -> {
      val b = bucketOf(absent)
      match b.list("", "", 10) {
        case Ok(_) -> panic("a container that does not exist was listed")
        case Err(e) -> assertEqual(e.code, "ContainerNotFound", "a missing container on list: " + e.message)
      }
      match b.get("k") {
        case Ok(_) -> panic("a blob was found in a container that does not exist")
        case Err(e) -> assertEqual(e.code, "ContainerNotFound", "a missing container on get: " + e.message)
      }
      match b.exists("k") {
        case Ok(found) -> panic("exists answered " + found.toString() + " for a container that does not exist")
        case Err(e) -> assertEqual(e.code, "ContainerNotFound", "a missing container on exists is an error, not false")
      }
      match containerExists(absent) {
        case Ok(found) -> assertFalse(found, "containerExists of a missing container")
        case Err(e) -> panic("containerExists failed: " + e.code + " " + e.message)
      }
    }
  }
  match createContainer(client) {
    case Ok(created) -> assertFalse(created, "creating the container again reports it as existing")
    case Err(e) -> panic("createContainer of an existing container failed: " + e.code + " " + e.message)
  }
}
