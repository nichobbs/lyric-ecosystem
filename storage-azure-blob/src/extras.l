//! Storage.AzureBlob.Extras - Blob service features the `StorageBucket`
//! interface does not have.
//!
//! These work on an `AzureBlobClient` (`openClient`), not on the interface:
//! an application that calls them is choosing Azure Blob Storage. Blobs
//! travel as raw bytes here, with no base64 step.
@runtime_checked
@io
package Storage.AzureBlob.Extras

import Std.Core
import Std.Collections
import Std.Encoding as Enc
import Std.Http
import Storage
import Storage.AzureBlob.Client
import Storage.AzureBlob.SharedKey

func invalidKey(key: in String): StorageError {
  StorageError(message = "invalid key '" + key + "'", code = "INVALID_KEY")
}

/// True for an access tier a block blob can be given: `Hot`, `Cool`, `Cold`
/// or `Archive`.
@pure
pub func isAccessTier(name: in String): Bool {
  name == "Hot" or name == "Cool" or name == "Cold" or name == "Archive"
}

/// Store `bytes` under `key` without a base64 round trip. Returns the ETag.
pub func putBytes(
  client: in AzureBlobClient,
  key: in String,
  bytes: in slice[Byte],
  contentType: in String,
  userMeta: in Map[String, String],
): Result[String, StorageError]
  requires: contentType.length > 0
{
  if not isBlobName(key) {
    return Err(invalidKey(key))
  }
  val noHeaders: List[Pair] = newList()
  match client.putBlob(key, Payload(contentType = contentType, bytes = bytes), userMeta, noHeaders) {
    case Err(e) -> Err(e)
    case Ok(stored) -> Ok(stored.etag)
  }
}

/// Store `bytes` under `key` in the access tier `tier` (`Cool`, `Archive`,
/// ...). `Err(INVALID_ARGUMENT)` for a name `isAccessTier` rejects. Returns
/// the ETag.
pub func putBytesWithAccessTier(
  client: in AzureBlobClient,
  key: in String,
  bytes: in slice[Byte],
  contentType: in String,
  tier: in String,
): Result[String, StorageError]
  requires: contentType.length > 0
{
  if not isBlobName(key) {
    return Err(invalidKey(key))
  }
  if not isAccessTier(tier) {
    return Err(StorageError(message = "unknown blob access tier '" + tier + "'", code = "INVALID_ARGUMENT"))
  }
  val headers: List[Pair] = newList()
  headers.add(Pair(name = "x-ms-access-tier", value = tier))
  val noMeta: Map[String, String] = newMap()
  match client.putBlob(key, Payload(contentType = contentType, bytes = bytes), noMeta, headers) {
    case Err(e) -> Err(e)
    case Ok(stored) -> Ok(stored.etag)
  }
}

// A request that carries no entity still declares one of zero bytes, so the
// service gets the `Content-Length: 0` it requires of a PUT.
func emptyBody(): Option[Payload] {
  Some(value = Payload(contentType = "application/octet-stream", bytes = Enc.encodeUtf8("")))
}

/// Set Blob Tier: move the existing blob `key` to the access tier `tier`.
/// `Err(NOT_FOUND)` when there is no such blob, `Err(INVALID_ARGUMENT)` for a
/// name `isAccessTier` rejects. Moving a blob out of `Archive` is accepted
/// and completes later.
pub func setAccessTier(client: in AzureBlobClient, key: in String, tier: in String): Result[Unit, StorageError] {
  if not isBlobName(key) {
    return Err(invalidKey(key))
  }
  if not isAccessTier(tier) {
    return Err(StorageError(message = "unknown blob access tier '" + tier + "'", code = "INVALID_ARGUMENT"))
  }
  val query: List[Pair] = newList()
  query.add(Pair(name = "comp", value = "tier"))
  val headers: List[Pair] = newList()
  headers.add(Pair(name = "x-ms-access-tier", value = tier))
  match client.send(PUT, "PUT", client.blobPath(key), query, headers, emptyBody()) {
    case Err(e) -> Err(e)
    case Ok(resp) -> {
      if resp.status == 200 or resp.status == 202 {
        return Ok(())
      }
      Err(faultOf(resp, "cannot set the access tier of blob '" + key + "'"))
    }
  }
}

/// Download `key` as raw bytes with its properties.
pub func getBytes(client: in AzureBlobClient, key: in String): Result[GotBlob, StorageError] {
  if not isBlobName(key) {
    return Err(invalidKey(key))
  }
  client.getBlob(key)
}

/// The properties of `key` (size, content type, ETag, user metadata, access
/// tier, blob type, creation time) without downloading it; `None` when there
/// is no such blob.
pub func stat(client: in AzureBlobClient, key: in String): Result[Option[BlobProperties], StorageError] {
  if not isBlobName(key) {
    return Err(invalidKey(key))
  }
  client.blobProperties(key)
}

/// A service SAS URL that lets its holder upload a blob to `key` with one
/// HTTP `PUT`, without credentials, for `expiresInSeconds` (at most 604800,
/// 7 days). The upload must carry the header `x-ms-blob-type: BlockBlob`,
/// as every Put Blob does. The SAS grants create and write (`sp=cw`), not
/// read.
pub func presignedPutUrl(
  client: in AzureBlobClient, key: in String, expiresInSeconds: in Int
): Result[String, StorageError] {
  if not isBlobName(key) {
    return Err(invalidKey(key))
  }
  if expiresInSeconds < 1 or expiresInSeconds > 604800 {
    return Err(StorageError(message = "expiresInSeconds must be in [1, 604800]", code = "INVALID_ARGUMENT"))
  }
  client.sasUrl(key, "cw", nowEpochSeconds() + expiresInSeconds.toLong())
}

/// Create the client's container, private (no anonymous access). `Ok(true)`
/// when it was created, `Ok(false)` when it already exists.
pub func createContainer(client: in AzureBlobClient): Result[Bool, StorageError] {
  val query: List[Pair] = newList()
  query.add(Pair(name = "restype", value = "container"))
  val none: List[Pair] = newList()
  match client.send(PUT, "PUT", client.containerPath(), query, none, emptyBody()) {
    case Err(e) -> Err(e)
    case Ok(resp) -> {
      if resp.status == 201 {
        return Ok(true)
      }
      val fault = faultOf(resp, "cannot create container '" + client.container + "'")
      if fault.code == "ContainerAlreadyExists" {
        return Ok(false)
      }
      Err(fault)
    }
  }
}

/// True when the client's container exists and this account key may use it
/// (Get Container Properties); false when there is no such container.
pub func containerExists(client: in AzureBlobClient): Result[Bool, StorageError] {
  val query: List[Pair] = newList()
  query.add(Pair(name = "restype", value = "container"))
  val none: List[Pair] = newList()
  match client.send(HEAD, "HEAD", client.containerPath(), query, none, None) {
    case Err(e) -> Err(e)
    case Ok(resp) -> {
      if resp.status == 200 {
        return Ok(true)
      }
      val fault = faultOf(resp, "cannot get the properties of container '" + client.container + "'")
      if fault.code == "ContainerNotFound" or (resp.status == 404 and errorCodeOf(resp).length == 0) {
        return Ok(false)
      }
      Err(fault)
    }
  }
}
