//! Storage.AzureBlob - Azure Blob Storage backend for `Storage.StorageBucket`.
//!
//! Pure Lyric: the Blob REST API over `Std.Http`, authorized with Shared Key.
//! No Azure SDK, so the backend is the same code on .NET and the JVM.
//!
//! Usage:
//!
//!   import Storage.AzureBlob
//!
//!   val bucket = connect(azureConfig("myaccount", accountKey, "reports"))?
//!   put(bucket, "2026/q3.pdf", base64Data, "application/pdf")?
//!   val url = bucket.presignedUrl("2026/q3.pdf", 3600)?
//!
//! A bucket is one container. `connectWithConnectionString(text, container)`
//! takes an Azure Storage connection string, `connectFromEnv(container)`
//! reads the `AZURE_STORAGE_*` variables, and `endpointConfig(...)` addresses
//! an emulator or a sovereign cloud. Take the account key from the
//! application's `config { }` block or the environment, never from source.
//!
//! Keys are validated with `Storage.isSafeKey` exactly as the local-filesystem
//! backend does (`INVALID_KEY`), so an application behaves the same on either.
//! Other error codes are described in `Storage.AzureBlob.Client`.
//!
//! Blob-only operations (raw bytes, properties, access tiers, creating the
//! container, a SAS that allows an upload) are in `Storage.AzureBlob.Extras`,
//! over the `AzureBlobClient` this package also re-exports.
@runtime_checked
@io
package Storage.AzureBlob

pub use Storage
pub use Storage.AzureBlob.Client
pub use Storage.AzureBlob.Config
import Std.Core
import Std.Collections
import Std.Encoding as Enc
import Storage.AzureBlob.Wire

// The `StorageBucket` view of a client. Not `pub`: build one with `connect`
// or `bucketOf`, which only accept a validated client.
record AzureBlobBucket {
  client: AzureBlobClient
}

func invalidKey(key: in String): StorageError {
  StorageError(message = "invalid key '" + key + "'", code = "INVALID_KEY")
}

impl StorageBucket for AzureBlobBucket {
  func put(
    key: in String, dataBase64: in String, contentType: in String, userMeta: in Map[String, String]
  ): Result[StorageMetadata, StorageError] {
    if not isBlobName(key) {
      return Err(invalidKey(key))
    }
    match self.client.putBlobBase64(key, dataBase64, contentType, userMeta) {
      case Err(e) -> Err(e)
      case Ok(stored) -> Ok(
        StorageMetadata(
          contentType = contentType,
          contentLength = stored.contentLength,
          etag = stored.etag,
          lastModified = stored.storedAt,
          userMeta = userMeta
        )
      )
    }
  }

  func get(key: in String): Result[StorageObject, StorageError] {
    if not isBlobName(key) {
      return Err(invalidKey(key))
    }
    match self.client.getBlob(key) {
      case Err(e) -> Err(e)
      case Ok(got) -> Ok(
        StorageObject(
          bucket = self.client.container,
          key = key,
          metadata = StorageMetadata(
            contentType = got.properties.contentType,
            contentLength = got.properties.contentLength,
            etag = got.properties.etag,
            lastModified = got.properties.lastModified,
            userMeta = got.properties.userMeta
          ),
          dataBase64 = Enc.encodeBase64(got.bytes)
        )
      )
    }
  }

  func delete(key: in String): Result[Unit, StorageError] {
    if not isBlobName(key) {
      return Err(invalidKey(key))
    }
    self.client.deleteBlob(key)
  }

  func list(prefix: in String, continuationToken: in String, maxKeys: in Int): Result[ListResult, StorageError] {
    if maxKeys < 1 or maxKeys > 1000 {
      return Err(StorageError(message = "maxKeys must be in [1, 1000]", code = "INVALID_ARGUMENT"))
    }
    if not isSafePrefix(prefix) {
      return Err(StorageError(message = "invalid key prefix '" + prefix + "'", code = "INVALID_KEY"))
    }
    match self.client.listBlobs(prefix, continuationToken, maxKeys) {
      case Err(e) -> Err(e)
      case Ok(page) -> {
        val entries: List[ListEntry] = newList()
        var i = 0
        while i < page.blobs.count {
          val b = page.blobs[i]
          entries.add(ListEntry(key = b.name, contentLength = b.size, lastModified = b.lastModified, etag = b.etag))
          i = i + 1
        }
        val truncated = page.nextMarker.length > 0
        Ok(
          ListResult(
            entries = entries.toArray(),
            nextContinuationToken = if truncated { Some(value = page.nextMarker) } else { None },
            isTruncated = truncated
          )
        )
      }
    }
  }

  func presignedUrl(key: in String, expiresInSeconds: in Int): Result[String, StorageError] {
    if not isBlobName(key) {
      return Err(invalidKey(key))
    }
    if expiresInSeconds < 1 or expiresInSeconds > 604800 {
      return Err(StorageError(message = "expiresInSeconds must be in [1, 604800]", code = "INVALID_ARGUMENT"))
    }
    self.client.sasUrl(key, "r", nowEpochSeconds() + expiresInSeconds.toLong())
  }

  func exists(key: in String): Result[Bool, StorageError] {
    if not isBlobName(key) {
      return Err(invalidKey(key))
    }
    match self.client.blobProperties(key) {
      case Err(e) -> Err(e)
      case Ok(properties) -> Ok(isSome(properties))
    }
  }
}

/// The `StorageBucket` for an open client.
pub func bucketOf(client: in AzureBlobClient): StorageBucket {
  val bucket: StorageBucket = AzureBlobBucket(client = client)
  bucket
}

/// Open the container `cfg` describes as a `StorageBucket`. No request is
/// made; see `openClient` for what is validated (`INVALID_CONFIG`).
pub func connect(cfg: in AzureBlobConfig): Result[StorageBucket, StorageError] {
  match openClient(cfg) {
    case Err(e) -> Err(e)
    case Ok(client) -> Ok(bucketOf(client))
  }
}

/// Open `container` of the account an Azure Storage connection string names
/// (`parseConnectionString`).
pub func connectWithConnectionString(
  connectionString: in String, container: in String
): Result[StorageBucket, StorageError] {
  match parseConnectionString(connectionString, container) {
    case Err(e) -> Err(e)
    case Ok(cfg) -> connect(cfg)
  }
}

/// Open `container` with the account in the `AZURE_STORAGE_*` environment
/// variables (`configFromEnv`).
pub func connectFromEnv(container: in String): Result[StorageBucket, StorageError] {
  match configFromEnv(container) {
    case Err(e) -> Err(e)
    case Ok(cfg) -> connect(cfg)
  }
}
