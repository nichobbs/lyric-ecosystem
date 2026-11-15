//! Storage.S3.Extras - S3 features the `StorageBucket` interface does not
//! have.
//!
//! These work on an `S3Client` (`openClient`), not on the interface: an
//! application that calls them is choosing S3. Objects travel as raw bytes
//! here, with no base64 step.
@runtime_checked
@io
package Storage.S3.Extras

import Std.Core
import Std.Collections
import Std.Encoding as Enc
import Storage
import Storage.S3.Client
import Std.Http
import Storage.S3.SigV4

func invalidKey(key: in String): StorageError {
  StorageError(message = "invalid key '" + key + "'", code = "INVALID_KEY")
}

/// True for a storage class name S3 documents for PutObject.
@pure
pub func isStorageClass(name: in String): Bool {
  name == "STANDARD" or name == "REDUCED_REDUNDANCY" or name == "STANDARD_IA" or name == "ONEZONE_IA" or
    name == "INTELLIGENT_TIERING" or
    name == "GLACIER" or
    name == "DEEP_ARCHIVE" or
    name == "GLACIER_IR" or
    name == "EXPRESS_ONEZONE"
}

/// Store `bytes` under `key` without a base64 round trip. Returns the ETag.
pub func putBytes(
  client: in S3Client,
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

/// Store `bytes` under `key` in `storageClass` (`STANDARD_IA`, `GLACIER_IR`,
/// ...). `Err(INVALID_ARGUMENT)` for a name `isStorageClass` rejects. Returns
/// the ETag.
pub func putBytesWithStorageClass(
  client: in S3Client,
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
    return Err(StorageError(message = "unknown S3 storage class '" + storageClass + "'", code = "INVALID_ARGUMENT"))
  }
  val headers: List[Pair] = newList()
  headers.add(Pair(name = "x-amz-storage-class", value = storageClass))
  val noMeta: Map[String, String] = newMap()
  match client.putObject(key, bytes, contentType, noMeta, headers) {
    case Err(e) -> Err(e)
    case Ok(stored) -> Ok(stored.etag)
  }
}

/// Download `key` as raw bytes with its metadata.
pub func getBytes(client: in S3Client, key: in String): Result[GotObject, StorageError] {
  if not isSafeKey(key) {
    return Err(invalidKey(key))
  }
  client.getObject(key)
}

/// The metadata of `key` (size, content type, ETag, user metadata, storage
/// class) without downloading it; `None` when there is no such object.
pub func stat(client: in S3Client, key: in String): Result[Option[ObjectHead], StorageError] {
  if not isSafeKey(key) {
    return Err(invalidKey(key))
  }
  client.headObject(key)
}

/// A URL that lets its holder upload an object to `key` with one HTTP `PUT`,
/// without credentials, for `expiresInSeconds` (at most 604800, 7 days).
pub func presignedPutUrl(client: in S3Client, key: in String, expiresInSeconds: in Int): Result[String, StorageError] {
  if not isSafeKey(key) {
    return Err(invalidKey(key))
  }
  if expiresInSeconds < 1 or expiresInSeconds > 604800 {
    return Err(StorageError(message = "expiresInSeconds must be in [1, 604800]", code = "INVALID_ARGUMENT"))
  }
  client.presign("PUT", key, expiresInSeconds, amzDateNow())
}

/// Create the client's bucket in the client's region. `Ok(true)` when it was
/// created, `Ok(false)` when this account already owns it; a name taken by
/// another account is `Err(BucketAlreadyExists)`. Amazon S3 answers 200 for a
/// bucket the account already owns in `us-east-1`, so there the result is
/// `Ok(true)` either way.
pub func createBucket(client: in S3Client): Result[Bool, StorageError] {
  val none: List[Pair] = newList()
  // us-east-1 is the default location and must not be named; every other
  // region must be.
  val body = if client.region == "us-east-1" {
    ""
  } else {
    "<CreateBucketConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><LocationConstraint>" + client.region +
      "</LocationConstraint></CreateBucketConfiguration>"
  }
  // Not repeated after an unknown outcome: a second CreateBucket would report
  // "already owned" for the bucket the first one created.
  val sent = client.sendUnrepeatable(
    PUT,
    "PUT",
    client.bucketPath(),
    none,
    none,
    Some(value = Payload(contentType = "application/xml", bytes = Enc.encodeUtf8(body)))
  )
  match sent {
    case Err(e) -> Err(e)
    case Ok(resp) -> {
      if resp.status == 200 {
        return Ok(true)
      }
      val fault = faultOf(resp, "cannot create bucket '" + client.bucket + "'")
      if fault.code == "BucketAlreadyOwnedByYou" {
        return Ok(false)
      }
      Err(fault)
    }
  }
}

/// True when the client's bucket exists and these credentials may use it
/// (HeadBucket); false when there is no such bucket.
pub func bucketExists(client: in S3Client): Result[Bool, StorageError] {
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
