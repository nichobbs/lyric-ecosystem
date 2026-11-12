# lyric-storage-azure-blob

Azure Blob Storage, and the Azurite emulator, as a backend for the `Storage.StorageBucket` interface
of `lyric-storage`. A bucket is one container of one storage account.

The backend is pure Lyric: the Blob REST API over `Std.Http`, authorized with Shared Key
(HMAC-SHA256 under the account key). There is no Azure SDK underneath and no extern declaration in
the package, so the same code runs on both targets and there is no `_kernel_<target>/` directory.

| | |
|---|---|
| Package | `Lyric.Storage.AzureBlob` (Lyric packages `Storage.AzureBlob`, `Storage.AzureBlob.Client`, `Storage.AzureBlob.Config`, `Storage.AzureBlob.Extras`, `Storage.AzureBlob.SharedKey`, `Storage.AzureBlob.Wire`) |
| Targets | `dotnet`, `jvm` |
| Stability | `experimental` (0.1.0) |
| Interface | `Storage.StorageBucket` from `Lyric.Storage` 0.1.0, re-exported with `pub use Storage` |
| SDK / native dependencies | none |
| Protocol | Blob REST API, `x-ms-version: 2021-12-02`; Shared Key authorization; service SAS `sv=2021-12-02` |
| Tested against | Azurite 3.35.0 (`azurite` npm package), both targets |

## Requirements

`Std.Http.withBytesBody`, `HttpResponse.headerNames` and `Std.HttpServer.requestBodyBytes` (the
last for the tests only), added in nichobbs/lyric-lang#9047. They are not in a release yet, so the
package does not build with an older standard library; `.lyric-lang-rev` pins a commit that has
them.

## Usage

```lyric
import Std.Core
import Storage.AzureBlob

func example(accountKey: in String, pdfBase64: in String): Result[String, StorageError] {
  val bucket = connect(azureConfig("myaccount", accountKey, "reports"))?
  put(bucket, "2026/q3.pdf", pdfBase64, "application/pdf")?
  bucket.presignedUrl("2026/q3.pdf", 3600)
}
```

`import Storage.AzureBlob` also brings the names of `Storage` into scope bare (`put`, `get`,
`StorageBucket`, `StorageError`), because the package re-exports it. `connect` returns the
interface, so the rest of an application names only `StorageBucket` and can be given the
local-filesystem backend or a mock instead. The container must exist; `createContainer` in
`Storage.AzureBlob.Extras` makes it.

### Configuration

Build an `AzureBlobConfig` and pass it to `connect` (or `openClient`, for the Blob-only
operations):

| Constructor | Addressing |
|---|---|
| `azureConfig(accountName, accountKey, container)` | Azure: `https://<account>.blob.core.windows.net/<container>/<blob>` |
| `endpointConfig(endpoint, accountName, accountKey, container)` | another Blob endpoint. `https://<host>[:port]` for a sovereign cloud or a custom domain; `http(s)://<host>[:port]/<account>` for an emulator, which puts the account in the path |
| `emulatorConfig(container)` | Azurite on its default port, `http://127.0.0.1:10000/devstoreaccount1`, with the emulators' development account |
| `parseConnectionString(text, container)` / `connectWithConnectionString(text, container)` | an Azure Storage connection string, see below |
| `configFromEnv(container)` / `connectFromEnv(container)` | from the environment, see below |

Every field of `AzureBlobConfig`:

| Field | |
|---|---|
| `accountName` | the storage account, 3 to 24 lowercase letters and digits |
| `accountKey` | the account key as Azure issues it, base64 |
| `container` | the container: 3 to 63 lowercase letters, digits and `-`. The system containers (`$root`, `$web`, `$logs`) are not accepted |
| `endpoint` | empty for Azure, or the Blob endpoint as above |
| `requestTimeoutMs` | per-request deadline, default 30000; set with `.withRequestTimeout(ms)` |

A connection string is `Name=Value` settings separated by `;`, names in any case:

```
DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=...;EndpointSuffix=core.windows.net
AccountName=devstoreaccount1;AccountKey=...;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1
UseDevelopmentStorage=true
```

`AccountName` and `AccountKey` are required unless `UseDevelopmentStorage=true`. `BlobEndpoint`
wins over the endpoint built from `DefaultEndpointsProtocol` (default `https`), the account name
and `EndpointSuffix` (default `core.windows.net`). `DevelopmentStorageProxyUri` moves the
development account to another host. `QueueEndpoint`, `TableEndpoint` and `FileEndpoint` are
ignored. A `SharedAccessSignature`, an unknown setting or a repeated setting is `INVALID_CONFIG`;
the message names the setting and never repeats a value.

`configFromEnv` reads the variables the Azure CLI reads:

| Variable | |
|---|---|
| `AZURE_STORAGE_CONNECTION_STRING` | a connection string; when set, the others are not read |
| `AZURE_STORAGE_ACCOUNT` | the account name |
| `AZURE_STORAGE_KEY` | the account key |
| `AZURE_STORAGE_SERVICE_ENDPOINT` | optional Blob endpoint; Azure when unset |

Take the account key from the application's `config { }` block or the environment, never from
source. `devAccountName()`, `devAccountKey()` and `devBlobEndpoint()` are the emulators'
development account: public constants Microsoft documents, which open nothing but an emulator.

`openClient` validates the configuration before any request and answers `INVALID_CONFIG` for an
account or container name that could alter the host name or path, an account key that is not
base64, a malformed endpoint, an endpoint path that is not `/<account>`, or an `http://` endpoint
whose host is not `localhost`, `127.0.0.1` or `[::1]`. Plain HTTP would expose blob names,
metadata and payloads, and let anyone on the path replay a request for the 15 minutes its
signature is valid.

### Behaviour of the interface methods

| Method | Blob operation | Notes |
|---|---|---|
| `put` | Put Blob (`BlockBlob`) | The base64 payload is decoded and sent as bytes in one request. User metadata travels as `x-ms-meta-<name>` headers. `lastModified` is the blob's `Last-Modified`. |
| `get` | Get Blob | `lastModified` is the `Last-Modified` header as ISO 8601. `userMeta` holds every `x-ms-meta-*` header. `bucket` is the container. |
| `delete` | Delete Blob | Idempotent: the service's `BlobNotFound` is success. Snapshots of the blob are deleted with it. |
| `exists` | Get Blob Properties | `BlobNotFound` is `Ok(false)`; any other failure, a missing container included, is an `Err`, not `false`. |
| `list` | List Blobs, flat | `prefix`, `maxresults`, `marker`; the continuation token is the service's `NextMarker`. |
| `presignedUrl` | service SAS | Read permission on the one blob (`sr=b`, `sp=r`), signed locally with the account key; the blob need not exist. At most 604800 seconds. |

### Error codes

`StorageError.code` is:

| Code | Meaning |
|---|---|
| `INVALID_KEY`, `INVALID_ARGUMENT`, `INVALID_BASE64`, `USER_META_UNSUPPORTED`, `INVALID_CONFIG` | refused locally, nothing was sent |
| `NOT_FOUND` | the service answered `BlobNotFound`, or 404 without an error code |
| any other Blob service code, verbatim (`ContainerNotFound`, `AuthenticationFailed`, `AuthorizationFailure`, `ContainerAlreadyExists`, `ServerBusy`, ...) | the `x-ms-error-code` response header, or the `<Code>` of the `Error` document when there is no header |
| `HTTP_<status>` | a response without an error code (a proxy's error page, a redirect) |
| `CONNECTION_FAILED` | no response: connection refused, TLS failure, timeout |
| `LIST_ERROR` | a listing that is not a valid `EnumerationResults` document |

The message keeps the service's own code, the HTTP status and the first line of its message, for
example `cannot get blob 'a/b': BlobNotFound (HTTP 404): The specified blob does not exist.`

## Blob-only operations: `Storage.AzureBlob.Extras`

These take an `AzureBlobClient` (`openClient(cfg)`; `bucketOf(client)` gives the `StorageBucket`
for the same client) and are not part of the interface:

| Function | |
|---|---|
| `putBytes(client, key, bytes, contentType, userMeta)` | Put Blob from a `slice[Byte]`, no base64 step; returns the ETag |
| `putBytesWithAccessTier(client, key, bytes, contentType, tier)` | Put Blob with `x-ms-access-tier` (`Hot`, `Cool`, `Cold`, `Archive`) |
| `setAccessTier(client, key, tier)` | Set Blob Tier on an existing blob |
| `getBytes(client, key)` | Get Blob as bytes plus `BlobProperties` |
| `stat(client, key)` | Get Blob Properties: size, content type, ETag, last-modified and creation times, user metadata, access tier, blob type; `None` when absent |
| `presignedPutUrl(client, key, expiresInSeconds)` | a service SAS with create and write permission (`sp=cw`): one upload without credentials |
| `createContainer(client)` / `containerExists(client)` | Create Container (private) and Get Container Properties |

`Storage.AzureBlob.Client` also exposes the signed-request primitive (`AzureBlobClient.send`) and
`AzureBlobClient.sasUrl(key, permissions, expiryEpochSeconds)` for other SAS permissions, and
`Storage.AzureBlob.SharedKey` the signing functions, for an operation this package does not wrap.

## Surprises

- **Keys** are validated with `Storage.isSafeKey` (prefixes with `isSafePrefix`) exactly as the
  local-filesystem backend validates them, so `..` segments, a `.meta.json` suffix, `:`, `%25`
  and the rest are `INVALID_KEY` here too although Azure would accept them, and no request is
  made. A key longer than 1024 characters, Azure's limit, is `INVALID_KEY` as well.
- **User metadata names are narrower than on S3.** Azure requires a metadata name to be a valid C#
  identifier, and header names lose their case on the way, so a name must be a lowercase letter or
  `_` followed by lowercase letters, digits and `_`. `content-kind`, `build.note` and `Owner` are
  `USER_META_UNSUPPORTED`. A value must be printable ASCII with no leading or trailing space.
- **Some sets of metadata names are refused although each name is valid.** The signature covers
  the `x-ms-meta-*` headers sorted by name, and implementations of the service disagree on where
  `_` sorts against a digit: byte order puts `a11` before `a_1`, Azurite's comparison puts `a_1`
  first (verified against Azurite 3.35.0, which answers 403 to the byte-order signature). A put
  whose names would sort differently under the two rules (`a_1` together with `a11`) is
  `USER_META_UNSUPPORTED`; rename one.
- **`delete` of a missing blob succeeds, `delete` in a missing container does not**
  (`ContainerNotFound`). The same holds for `exists`: a missing container is an error, not `false`.
- **A wrong account key is `AuthenticationFailed` on Azure and `AuthorizationFailure` on
  Azurite.** The code is the service's, verbatim.
- **ETags** are Azure's (`0x8DCF0A1B2C3D4E5`), not a hash of the content, and change on every
  write.
- **The container is not created for you.** Operations on a container that does not exist answer
  `ContainerNotFound`.
- **A presigned URL is a SAS**, so over https it carries `spr=https` and is refused over plain
  http. It grants read on that blob for as long as the account key is not rotated; it cannot be
  revoked on its own.
- **An upload through `presignedPutUrl` must send `x-ms-blob-type: BlockBlob`**, as every Put Blob
  must; a bare `PUT` is `MissingRequiredHeader`.
- **Redirects are not followed.** A redirect would send the request to a host or path the
  signature does not cover, so it is reported as `HTTP_<status>`.
- **`list`** returns blobs only, flat, in the service's name order; a page can hold fewer than
  `maxKeys` entries and still be truncated, as the service allows. Soft-deleted blobs, snapshots
  and versions are not listed.
- **`put` replaces** an existing blob unconditionally, including its metadata and access tier.
- **The clock matters.** `x-ms-date` is the local clock; the service refuses a request more than
  15 minutes off (`AuthenticationFailed`). A SAS expiry is computed from the same clock.
- **Target difference (compiler):** calling `list` with `maxKeys` outside 1..1000 violates the
  interface's own `requires:`. On the JVM that is a `PreconditionViolated` panic; on .NET the
  inherited contract is not enforced and the call returns `Err(INVALID_ARGUMENT)`. Do not rely on
  either. No other behaviour differs between the targets.

## Tests

```sh
lyric test --manifest storage-azure-blob/lyric.toml --target dotnet
lyric test --manifest storage-azure-blob/lyric.toml --target jvm
```

The default run is hermetic and needs no network:

- `tests/shared_key_tests.l`: Shared Key strings to sign and signatures, including the
  documentation's Get Container Metadata example (query parameters), a Put Blob with metadata
  headers, a request whose `Content-Length` is 0 (signed as the empty string), an emulator's
  path-style resource and a repeated query parameter; and service SAS strings and query strings
  for read and write. Each expected signature was computed independently with Python's `hmac`,
  `hashlib` and `base64` from the documented string-to-sign rules.
- `tests/wire_tests.l`: `EnumerationResults` and `Error` documents, encoded names, ETags, RFC 1123
  and ISO 8601 dates (expected values from Python's `email.utils` and `time`).
- `tests/config_tests.l`: connection strings, configuration validation, addressing, metadata
  rules.
- `tests/fake_blob_tests.l`: `Testing.Conformance.Storage` and the backend's own behaviour over
  real HTTP against a Blob service written in Lyric on `Std.HttpServer` inside the test. The
  server verifies the Shared Key signature of every request from what it received (and its
  `x-ms-date` and `x-ms-version`), verifies every SAS including expiry and permission, requires
  containers to be created, keeps blobs in memory, and pages List Blobs with an opaque marker.

Integration tests run against a real Blob service and are compiled only with
`--features integration`:

```sh
eval "$(bash scripts/ci/start-azurite.sh)"      # Azurite on 127.0.0.1:10000, needs node and npm
lyric test --manifest storage-azure-blob/lyric.toml --target dotnet --features integration
lyric test --manifest storage-azure-blob/lyric.toml --target jvm --features integration
```

| Variable | |
|---|---|
| `LYRIC_AZURE_BLOB_ENDPOINT` | the Blob endpoint, e.g. `http://127.0.0.1:10000/devstoreaccount1`; `azure` selects `https://<account>.blob.core.windows.net` |
| `LYRIC_AZURE_BLOB_ACCOUNT` | the storage account name (`devstoreaccount1` for Azurite) |
| `LYRIC_AZURE_BLOB_KEY` | the account key (Azurite's is the public development key) |
| `LYRIC_AZURE_BLOB_CONTAINER` | a disposable container; created when missing. **Every blob in it is deleted**, repeatedly |

A missing variable fails every integration test with a message naming it. The suite runs the
conformance suite, binary blobs with encoded names and metadata, Get Blob Properties, access
tiers, read and write SAS URLs fetched by a client without credentials (tampered, wrong blob,
wrong permission, expired), and the service's own error codes. It passes on both targets against
Azurite 3.35.0. `scripts/ci/start-azurite.sh` installs that version with npm (set
`AZURITE_BLOB_PORT` for another port, `AZURITE_DIR` for the install directory). It has not been
run against an Azure storage account.

## Known gaps

- Authorization is Shared Key only: no Microsoft Entra ID (OAuth) tokens, no managed identity, no
  SAS-token credentials (a `SharedAccessSignature` connection string is refused), no user
  delegation SAS, no stored access policies.
- Blobs are held in memory whole and uploaded with one Put Blob, as the interface's base64
  payloads require: no block upload (Put Block / Put Block List) of large blobs, no streaming, no
  range reads. `Std.Http` buffers bodies too. Put Blob accepts at most 5000 MiB.
- Block blobs only: no append blobs or page blobs. `get`, `exists`, `delete` and `list` work on
  one that already exists; `put` over it replaces it with a block blob.
- No snapshots, versioning, soft delete, leases, tags, immutability policies, customer-provided
  keys or encryption scopes. A blob under an active lease cannot be written or deleted
  (`LeaseIdMissing`).
- No hierarchical (delimiter) listing, and no container deletion or access-level changes.
- Requests are not retried. `ServerBusy` and 5xx answers reach the caller as errors.
- Not verified against Azure itself, only against Azurite: in particular the access tier of a
  real account, `spr=https`, and Azure's ordering of metadata headers.
- In the hermetic tests the fake server answers HEAD with `Content-Length: 0`, because
  `Std.HttpServer` cannot send a HEAD response that reports an entity's length
  (nichobbs/lyric-lang#9048). `stat`'s `contentLength` is therefore asserted only by the
  integration tests.
- Compiler: an inherited interface `requires:` is enforced on a cross-package implementation on
  the JVM but not on .NET (see "Surprises"; related to nichobbs/lyric-lang#9043, no issue number
  of its own yet).
- Compiler: a `pub val` of a library package is not visible to a test module that links the
  library's shared build (`T0020`; `lyric test` falls back to compiling with the library). The
  package exposes its constants as functions (`apiVersion()`, `devAccountKey()`) instead. No issue
  number yet.
- As in `storage-s3`, a request body is carried in a `Payload` record rather than a bare
  `slice[Byte]` parameter (nichobbs/lyric-lang#9039, #9049).
