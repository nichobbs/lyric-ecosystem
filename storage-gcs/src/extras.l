//! Storage.Gcs.Extras - Cloud Storage features the `StorageBucket` interface
//! does not have.
//!
//! These work on a `GcsClient` (`openClient`), not on the interface: an
//! application that calls them is choosing Cloud Storage. Objects travel as
//! raw bytes here, with no base64 step.
@runtime_checked
@io
package Storage.Gcs.Extras

import Std.Core
import Std.Collections
import Std.Http
import Storage
import Storage.Gcs.Client
import Storage.Gcs.Goog4

func invalidKey(key: in String): StorageError {
  StorageError(message = "invalid key '" + key + "'", code = "INVALID_KEY")
}

func unsignable(): StorageError {
  StorageError(message = "an anonymous client has no HMAC key to sign a URL with", code = "NOT_SUPPORTED")
}

/// True for a storage class name the XML API documents for
/// `x-goog-storage-class`.
@pure
pub func isStorageClass(name: in String): Bool {
  name == "STANDARD" or name == "NEARLINE" or name == "COLDLINE" or name == "ARCHIVE" or name == "MULTI_REGIONAL" or
    name == "REGIONAL"
}

/// Store `bytes` under `key` without a base64 round trip. Returns the ETag.
pub func putBytes(
  client: in GcsClient,
  key: in String,
  bytes: in slice[Byte],
  contentType: in String,
  userMeta: in Map[String, String],
): Result[String, StorageError]
  requires: contentType.length > 0
{
  if not isSafeKey(key) {
    return Err(invalidKey(key))
  }
  val noHeaders: List[Pair] = newList()
  match client.putObject(key, bytes, contentType, userMeta, noHeaders) {
    case Err(e) -> Err(e)
    case Ok(stored) -> Ok(stored.etag)
  }
}

/// Store `bytes` under `key` in `storageClass` (`NEARLINE`, `COLDLINE`,
/// `ARCHIVE`, ...) instead of the bucket's default class.
/// `Err(INVALID_ARGUMENT)` for a name `isStorageClass` rejects. Returns the
/// ETag.
pub func putBytesWithStorageClass(
  client: in GcsClient,
  key: in String,
  bytes: in slice[Byte],
  contentType: in String,
  storageClass: in String,
): Result[String, StorageError]
  requires: contentType.length > 0
{
  if not isSafeKey(key) {
    return Err(invalidKey(key))
  }
  if not isStorageClass(storageClass) {
    return Err(
      StorageError(
        message = "unknown Cloud Storage storage class '" + storageClass + "'",
        code = "INVALID_ARGUMENT"
      )
    )
  }
  val headers: List[Pair] = newList()
  headers.add(Pair(name = "x-goog-storage-class", value = storageClass))
  val noMeta: Map[String, String] = newMap()
  match client.putObject(key, bytes, contentType, noMeta, headers) {
    case Err(e) -> Err(e)
    case Ok(stored) -> Ok(stored.etag)
  }
}

/// Download `key` as raw bytes with its metadata.
pub func getBytes(client: in GcsClient, key: in String): Result[GotObject, StorageError] {
  if not isSafeKey(key) {
    return Err(invalidKey(key))
  }
  client.getObject(key)
}

/// The metadata of `key` (size, content type, ETag, user metadata, storage
/// class, generation) without downloading it; `None` when there is no such
/// object.
pub func stat(client: in GcsClient, key: in String): Result[Option[ObjectHead], StorageError] {
  if not isSafeKey(key) {
    return Err(invalidKey(key))
  }
  client.headObject(key)
}

/// A V4 signed URL that lets its holder upload an object to `key` with one
/// HTTP `PUT`, without credentials, for `expiresInSeconds` (at most 604800,
/// 7 days).
pub func presignedPutUrl(client: in GcsClient, key: in String, expiresInSeconds: in Int): Result[String, StorageError] {
  if not isSafeKey(key) {
    return Err(invalidKey(key))
  }
  if expiresInSeconds < 1 or expiresInSeconds > 604800 {
    return Err(StorageError(message = "expiresInSeconds must be in [1, 604800]", code = "INVALID_ARGUMENT"))
  }
  if client.anonymous {
    return Err(unsignable())
  }
  Ok(client.presign("PUT", key, expiresInSeconds, googDateNow()))
}

/// True when the client's bucket exists and this HMAC key may use it (HEAD
/// Bucket); false when there is no such bucket.
pub func bucketExists(client: in GcsClient): Result[Bool, StorageError] {
  val none: List[Pair] = newList()
  match client.send(HEAD, "HEAD", client.bucketPath(), none, none, None) {
    case Err(e) -> Err(e)
    case Ok(resp) -> {
      if resp.status == 200 {
        return Ok(true)
      }
      if resp.status == 404 {
        return Ok(false)
      }
      Err(faultOf(resp, "cannot head bucket '" + client.bucket + "'"))
    }
  }
}
