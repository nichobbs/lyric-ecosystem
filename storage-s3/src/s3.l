//! Storage.S3 - Amazon S3 (and S3-compatible) backend for
//! `Storage.StorageBucket`.
//!
//! Pure Lyric: the S3 REST API over `Std.Http`, signed with AWS Signature
//! Version 4. No AWS SDK, so the backend is the same code on .NET and the JVM.
//!
//! Usage:
//!
//!   import Storage.S3
//!
//!   val bucket = connect(awsConfig("my-bucket", "eu-west-1", keyId, secret))?
//!   put(bucket, "reports/q3.pdf", base64Data, "application/pdf")?
//!   val url = bucket.presignedUrl("reports/q3.pdf", 3600)?
//!
//! `connectFromEnv(bucket)` uses the AWS default credential chain and the
//! standard `AWS_*` variables instead, `withCredentialProvider` any
//! `Aws.Credentials` provider, and
//! `endpointConfig(...)` addresses MinIO, LocalStack and other compatible
//! servers path-style. Take credentials from the application's `config { }`
//! block or the environment, never from source.
//!
//! Keys are validated with `Storage.isSafeKey` exactly as the local-filesystem
//! backend does (`INVALID_KEY`), so an application behaves the same on either.
//! Other error codes are described in `Storage.S3.Client`.
//!
//! S3-only operations (raw bytes, HEAD metadata, storage classes, presigned
//! PUT) are in `Storage.S3.Extras`, over the `S3Client` this package also
//! re-exports.
@runtime_checked
@io
package Storage.S3

pub use Storage
pub use Storage.S3.Client
import Std.Core
import Std.Collections
import Std.Encoding as Enc
import Storage.S3.SigV4
import Storage.S3.Wire

// The `StorageBucket` view of a client. Not `pub`: build one with `connect`
// or `bucketOf`, which only accept a validated client.
record S3Bucket {
  client: S3Client
}

func invalidKey(key: in String): StorageError {
  StorageError(message = "invalid key '" + key + "'", code = "INVALID_KEY")
}

impl StorageBucket for S3Bucket {
  func put(
    key: in String, dataBase64: in String, contentType: in String, userMeta: in Map[String, String]
  ): Result[StorageMetadata, StorageError] {
    if not isSafeKey(key) {
      return Err(invalidKey(key))
    }
    match self.client.putObjectBase64(key, dataBase64, contentType, userMeta) {
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
    if not isSafeKey(key) {
      return Err(invalidKey(key))
    }
    match self.client.getObject(key) {
      case Err(e) -> Err(e)
      case Ok(got) -> Ok(
        StorageObject(
          bucket = self.client.bucket,
          key = key,
          metadata = StorageMetadata(
            contentType = got.head.contentType,
            contentLength = got.head.contentLength,
            etag = got.head.etag,
            lastModified = got.head.lastModified,
            userMeta = got.head.userMeta
          ),
          dataBase64 = Enc.encodeBase64(got.bytes)
        )
      )
    }
  }

  func delete(key: in String): Result[Unit, StorageError] {
    if not isSafeKey(key) {
      return Err(invalidKey(key))
    }
    self.client.deleteObject(key)
  }

  func list(prefix: in String, continuationToken: in String, maxKeys: in Int): Result[ListResult, StorageError] {
    if maxKeys < 1 or maxKeys > 1000 {
      return Err(StorageError(message = "maxKeys must be in [1, 1000]", code = "INVALID_ARGUMENT"))
    }
    if not isSafePrefix(prefix) {
      return Err(StorageError(message = "invalid key prefix '" + prefix + "'", code = "INVALID_KEY"))
    }
    match self.client.listObjects(prefix, continuationToken, maxKeys) {
      case Err(e) -> Err(e)
      case Ok(page) -> {
        val entries: List[ListEntry] = newList()
        var i = 0
        while i < page.objects.count {
          val o = page.objects[i]
          entries.add(ListEntry(key = o.key, contentLength = o.size, lastModified = o.lastModified, etag = o.etag))
          i = i + 1
        }
        Ok(
          ListResult(
            entries = entries.toArray(),
            nextContinuationToken = if page.isTruncated { Some(value = page.nextToken) } else { None },
            isTruncated = page.isTruncated
          )
        )
      }
    }
  }

  func presignedUrl(key: in String, expiresInSeconds: in Int): Result[String, StorageError] {
    if not isSafeKey(key) {
      return Err(invalidKey(key))
    }
    if expiresInSeconds < 1 or expiresInSeconds > 604800 {
      return Err(StorageError(message = "expiresInSeconds must be in [1, 604800]", code = "INVALID_ARGUMENT"))
    }
    self.client.presign("GET", key, expiresInSeconds, amzDateNow())
  }

  func exists(key: in String): Result[Bool, StorageError] {
    if not isSafeKey(key) {
      return Err(invalidKey(key))
    }
    match self.client.headObject(key) {
      case Err(e) -> Err(e)
      case Ok(head) -> Ok(isSome(head))
    }
  }
}

/// The `StorageBucket` for an open client.
pub func bucketOf(client: in S3Client): StorageBucket {
  val bucket: StorageBucket = S3Bucket(client = client)
  bucket
}

/// Open the bucket `cfg` describes as a `StorageBucket`. No request is made;
/// see `openClient` for what is validated (`INVALID_CONFIG`).
pub func connect(cfg: in S3Config): Result[StorageBucket, StorageError] {
  match openClient(cfg) {
    case Err(e) -> Err(e)
    case Ok(client) -> Ok(bucketOf(client))
  }
}

/// Open `bucket` with the credentials, region, optional endpoint and
/// optional certificate authority the environment and the AWS shared files
/// give (`configFromEnv`): the default credential chain.
pub func connectFromEnv(bucket: in String): Result[StorageBucket, StorageError] {
  match configFromEnv(bucket) {
    case Err(e) -> Err(e)
    case Ok(cfg) -> connect(cfg)
  }
}
