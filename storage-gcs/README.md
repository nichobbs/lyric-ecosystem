# lyric-storage-gcs

Google Cloud Storage as a backend for the `Storage.StorageBucket` interface of `lyric-storage`
(`nichobbs/lyric-lang`). It runs on `dotnet` and `jvm` and is `experimental` (0.1.0). The backend is
pure Lyric: the Cloud Storage XML API over `Std.Http`, signed with an HMAC key
(`GOOG4-HMAC-SHA256`). There is no Google SDK underneath and no extern declaration in the package,
so the same code runs on both targets and there is no `_kernel_<target>/` directory.

**Not verified against Google Cloud Storage with a real HMAC key.** No authenticated request of
this package has ever been accepted by Google Cloud Storage: the integration suite needs a bucket
and a key, and has not been run. What has been checked against `storage.googleapis.com` itself,
without a key, is how it reads this client's signed requests and what it answers to anonymous
reads; see [Verification status](#verification-status) before relying on the package.

| | |
|---|---|
| Package | `Lyric.Storage.Gcs` (Lyric packages `Storage.Gcs`, `Storage.Gcs.Client`, `Storage.Gcs.Config`, `Storage.Gcs.Extras`, `Storage.Gcs.Goog4`, `Storage.Gcs.Wire`) |
| Targets | `dotnet`, `jvm` |
| Stability | `experimental` (0.1.0) |
| Interface | `Storage.StorageBucket` from `Lyric.Storage`, re-exported with `pub use Storage` |
| Dependencies | `Lyric.Aws.SigV4` (`aws-sigv4/` in this repository) for the canonical request, a `{ workspace = true }` dependency. No SDK and no native dependency |
| Protocol | Cloud Storage XML API; V4 signing with an HMAC key (`GOOG4-HMAC-SHA256`, `x-goog-*` headers) |

## Why the XML API and HMAC keys

Cloud Storage has two APIs, and the choice between them is a choice of authentication:

- The **JSON API** is authorized with OAuth 2.0 access tokens. A server obtains one from a
  service-account key by signing a JWT with RS256 (RSA-SHA256 with the key's PEM private key). The
  Lyric standard library cannot do that on either target: `Std.Hash` has SHA-1/256/512 and
  HMAC-SHA256, `Std.Tls` loads PEM keys only to hand them to a TLS stack, and `lyric-auth` verifies
  JWTs but does not sign them. Implementing RSA signing would need an extern kernel per target or a
  host shim, which this repository does not allow for something the compiler and standard library
  should provide.
- The **XML API** also accepts requests signed with an **HMAC key** (an access ID and a secret,
  created for a service account). The V4 signing process needs HMAC-SHA256 and SHA-256 only, which
  `Std.Hash` has on both targets. It is the S3-compatible surface of Cloud Storage: the same
  canonical request, with `GOOG4-HMAC-SHA256`, `x-goog-*` headers and a credential scope ending in
  `storage/goog4_request`.

So this backend is the XML API with HMAC keys, in pure Lyric on both targets. The consequence for
an application: it authenticates with an HMAC key, not with a service-account JSON key file,
Application Default Credentials, workload identity or the metadata server.

The signing reuses `Lyric.Aws.SigV4` for everything the two schemes share (`canonicalRequest`,
`canonicalQuery`, `canonicalUri`, `uriEncode`, `signedHeaderNames`, `sha256Hex`, the `Pair`,
`Signed` and `SignedRequest` records) and implements only what differs in `Storage.Gcs.Goog4`:
the algorithm name, the `GOOG4` key prefix, the scope, and the `x-goog-*` header and `X-Goog-*`
query parameter names.

## Usage

Inside this workspace an application declares `"Lyric.Storage.Gcs" = { workspace = true }` under
`[dependencies]`. Create an HMAC key for a service account that may use the bucket
(`gcloud storage hmac create SERVICE_ACCOUNT_EMAIL`) and pass its access ID and secret from the
application's `config { }` block or the environment, never from source.

Every example below is compiled by `tests/readme_tests.l`.

```lyric
import Std.Core
import Std.Encoding as Enc
import Storage.Gcs

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
```

Every function and every `StorageBucket` method returns `Result[_, StorageError]`; `StorageError`
has a `code` (table under Configuration) and a `message`.

`import Storage.Gcs` also brings the names of `Storage`, `Storage.Gcs.Client` and
`Storage.Gcs.Config` into scope bare (`put`, `get`, `StorageBucket`, `StorageError`, `gcsConfig`,
`openClient`), because the package re-exports them. `connect` returns the interface, so the rest of
an application names only `StorageBucket` and can be given the local-filesystem backend or a mock
instead.

Variations:

- **The HMAC key from the environment; another location, virtual-hosted addressing, another
  timeout; another server that speaks the XML API:**

  ```lyric
  func fromEnvironment(): Result[StorageBucket, StorageError] {
    connectFromEnv("my-bucket")
  }

  func adjusted(accessId: in String, secret: in String): Result[StorageBucket, StorageError] {
    val cfg = gcsConfig("my-bucket", accessId, secret)
      .withLocation("europe-west1")
      .withPathStyle(false)
      .withRequestTimeout(10000)
    connect(cfg)
  }

  func otherServer(accessId: in String, secret: in String): Result[StorageBucket, StorageError] {
    connect(endpointConfig("https://gcs.example.com", "data", accessId, secret))
  }
  ```

- **A public bucket, without a key.** Requests are sent unsigned; Cloud Storage serves what
  `allUsers` may read:

  ```lyric
  func publicData(): Result[Int, StorageError] {
    val bucket = connect(anonymousConfig("gcp-public-data-landsat"))?
    val page = bucket.list("LC08/01/001/002/", "", 10)?
    Ok(page.entries.length)
  }
  ```

- **User metadata and paging** (`import Std.Collections`):

  ```lyric
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
      token = match page.nextContinuationToken {
        case Some(t) -> t
        case None -> ""
      }
    }
    Ok(keys)
  }
  ```

- **Cloud Storage-only operations** on a `GcsClient` (`import Storage.Gcs.Extras`);
  `bucketOf(client)` gives the `StorageBucket` for the same client:

  ```lyric
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
  ```

- **Diagnosing a refused signature** (`import Std.Http`, `Storage.Gcs.Goog4`, `Storage.Gcs.Wire`).
  Cloud Storage reports the canonical request it derived; the response of `GcsClient.send` carries
  the one this client signed:

  ```lyric
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
  ```

How the interface methods map to the XML API:

| Method | XML API operation | Notes |
|---|---|---|
| `put` | PUT Object | The base64 payload is decoded and sent as bytes in one request. User metadata travels as `x-goog-meta-<key>` headers. |
| `get` | GET Object | `lastModified` is the `Last-Modified` header as ISO 8601. `userMeta` holds every `x-goog-meta-*` header. |
| `delete` | DELETE Object | Cloud Storage answers 404 `NoSuchKey` for an object that is not there; that is `Ok`. |
| `exists` | HEAD Object | 404 is `Ok(false)`; any other failure is an `Err`, not `false`. |
| `list` | GET Bucket, `list-type=2` | `prefix`, `continuation-token`, `max-keys`; names requested with `encoding-type=url`, so a name XML cannot carry is still listed. |
| `presignedUrl` | V4 signed URL for GET | Computed locally; the object need not exist. At most 604800 seconds. `NOT_SUPPORTED` for an anonymous client. |

## Configuration

Build a `GcsConfig` and pass it to `connect` (or `openClient`, for the Cloud Storage-only
operations).

| Constructor | |
|---|---|
| `gcsConfig(bucket, accessId, secret)` | Google Cloud Storage, path-style: `https://storage.googleapis.com/<bucket>/<object>` |
| `endpointConfig(endpoint, bucket, accessId, secret)` | another server that speaks the XML API, path-style: `<endpoint>/<bucket>/<object>` |
| `anonymousConfig(bucket)` | Google Cloud Storage, unsigned requests, for a public bucket |
| `configFromEnv(bucket)` / `connectFromEnv(bucket)` | from the environment, see below |
| `configFromLookup(bucket, lookup)` | the same settings from any `(String) -> String` source |

| `GcsConfig` field | Set with | Default | Meaning |
|---|---|---|---|
| `bucket` | constructor | (required) | Lowercase letters, digits, `-`, `_` and `.`, starting and ending with a letter or digit; 3 to 63 characters, or up to 222 with dots, each dot-separated part at most 63 |
| `accessId`, `secret` | constructor | (required unless `anonymous`) | The HMAC key. Not empty |
| `location` | `.withLocation(location)` | `auto` | The location named in the credential scope: lowercase letters, digits and `-`. Cloud Storage accepts any value; Google recommends the bucket's location |
| `endpoint` | `endpointConfig`, `.withEndpoint(endpoint)` | `""` (Google) | `http(s)://host[:port]`, nothing after the authority but an optional `/`. `https://` unless the host is `localhost`, `127.0.0.1` or `[::1]` |
| `pathStyle` | `.withPathStyle(flag)` | true | Put the bucket in the path (`true`) or in the host name (`https://<bucket>.storage.googleapis.com/<object>`) |
| `requestTimeoutMs` | `.withRequestTimeout(ms)` | 30000 | Deadline of each request; must be positive |
| `anonymous` | `anonymousConfig` | false | Send requests unsigned |

`configFromEnv` and `connectFromEnv` read these variables. Google defines no environment variables
for HMAC keys, so the names are this package's own:

| Variable | |
|---|---|
| `GCS_HMAC_ACCESS_ID`, `GCS_HMAC_SECRET` | Required: the HMAC key |
| `GCS_LOCATION` | Optional; `auto` when unset |
| `GCS_ENDPOINT_URL` | Optional; another server, path-style |

A missing required variable is `INVALID_CONFIG` naming it. Nothing else is read from the
environment: not `GOOGLE_APPLICATION_CREDENTIALS`, not a `.boto` file, not the metadata server.

`openClient` (and so `connect`) validates the configuration before any request and answers
`INVALID_CONFIG` for a bucket name or location that could alter the URL or the credential scope, an
empty access ID or secret, a malformed endpoint, a non-positive timeout, or an `http://` endpoint
whose host is not loopback. Plain HTTP would expose the access ID, object names, metadata and
payloads, and let anyone on the path replay a request for the 15 minutes its signature is valid. An
endpoint that embeds credentials (`https://user:password@host`) is refused without repeating it in
the message.

`StorageError.code` is:

| Code | Meaning |
|---|---|
| `INVALID_KEY`, `INVALID_ARGUMENT`, `INVALID_BASE64`, `USER_META_UNSUPPORTED`, `INVALID_CONFIG`, `NOT_SUPPORTED` | refused locally, nothing was sent |
| `NOT_FOUND` | Cloud Storage answered `NoSuchKey`, or 404 without an error document |
| any other Cloud Storage code, verbatim (`AccessDenied`, `NoSuchBucket`, `SignatureDoesNotMatch`, `InvalidArgument`, `RequestTimeTooSkewed`, `SlowDown`, `PermanentRedirect`, ...) | the `<Code>` of the response's `Error` document; the message carries its `<Message>` and `<Details>` |
| `HTTP_<status>` | a response without a Cloud Storage error document (a HEAD, or a proxy's error page) |
| `CONNECTION_FAILED` | no response: connection refused, TLS failure, timeout |
| `LIST_ERROR` | a listing that is not a valid `ListBucketResult` |

## Surprises

Authentication:

- **The credential is an HMAC key, not a service account key file.** There is no OAuth, no
  Application Default Credentials, no workload identity. Create the key for a service account that
  has access to the bucket (Google recommends service-account keys over the user-account keys of
  the console's Interoperability tab).
- **An unknown access ID is `SignatureDoesNotMatch`,** not a code of its own: Cloud Storage does
  not say whether an access ID exists. A deleted or deactivated key looks the same.
- **Without permission, existence is not revealed.** `AccessDenied` "applies to any object you
  don't have access to, regardless of whether or not the object actually exists". A `get` or
  `exists` with a key that may not read the bucket is `AccessDenied` / `HTTP_403`, never
  `NOT_FOUND` / `false`.
- **The location is not checked.** Any `location` signs successfully; `auto` is the default. It is
  part of the signature all the same, so it must be a plain location name.
- **`anonymousConfig` sends nothing secret and can sign nothing.** `presignedUrl` and
  `presignedPutUrl` answer `NOT_SUPPORTED`; writes are `AccessDenied`.

Connecting and addressing:

- **`connect` makes no request.** A wrong key or a missing bucket shows up on the first operation,
  not at start-up. Call `bucketExists` (Extras) to check early.
- **Path-style is the default,** unlike `storage-s3` on AWS: `https://storage.googleapis.com/<bucket>/...`
  works for every bucket name, dotted ones included. `.withPathStyle(false)` selects
  `https://<bucket>.storage.googleapis.com/...`.
- **Redirects are not followed.** Following one would send the request to a host the signature
  does not cover, so it is reported as `PermanentRedirect` or `TemporaryRedirect`.
- **An endpoint with a path is refused** (a gateway mounted under a prefix).
- **Credentials are static.** There is no refresh: when an HMAC key is rotated, build a new client.

Keys:

- **Keys are restricted to what every backend can store.** They are validated with
  `Storage.isSafeKey` (prefixes with `isSafePrefix`) exactly as the local-filesystem backend
  validates them, and a refused key is `INVALID_KEY` with no request made: an empty key, a leading
  `/`, `..`, `.` and empty segments (`a//b`, a trailing `/`), a `.meta.json` suffix, control
  characters, `:`, `%25`, a segment ending in `.` or a space, and Windows device names (`CON`,
  `nul.txt`). Cloud Storage itself would accept most of these.
- **`list` can return names that `get` refuses.** Listing passes on whatever the bucket holds; an
  object written by another tool under a name `isSafeKey` rejects is listed but cannot be read,
  deleted or signed through this backend. `Storage.Gcs.Client`'s methods do not validate names.
- **`list` is flat.** There is no delimiter: a prefix matches every name beneath it at any depth,
  in Cloud Storage's name order, at most 1000 per page. Only live object versions are listed.
- **The continuation token is opaque** and belongs to the bucket and prefix it came from; an
  invalid one is `InvalidArgument`.

Metadata:

- **User metadata a header would alter is refused, not stored differently.** A key must be
  lowercase letters, digits, `-`, `_` or `.` (header names are case-insensitive), and a value
  printable ASCII without leading or trailing spaces; otherwise `put` is `USER_META_UNSUPPORTED`
  and nothing is sent. Cloud Storage's JSON API allows more; this API does not carry it intact.
- **`put` does not read the object back.** The `StorageMetadata` it returns repeats the content
  type and user metadata it was given; `lastModified` is the server's `Date` header at the time of
  the upload (the local clock when there is none), which can differ from the `Last-Modified` a
  later `get` reports.
- **`lastModified` has two forms.** `get` and `stat` convert the `Last-Modified` header to ISO 8601
  with second resolution (`2017-08-10T18:50:44Z`); `list` passes on the listing's `LastModified`
  as Cloud Storage wrote it, with milliseconds (`2017-08-10T18:50:44.626Z`). Do not compare them
  as text.
- **The ETag is Cloud Storage's.** It is the MD5 of the content, in hex, for an object uploaded in
  one request (what the local-filesystem backend also computes), and something else for a
  composite object or one encrypted with a customer-supplied key.
- **`stat` reports the stored size.** It reads `x-goog-stored-content-length` (falling back to
  `Content-Length`). For an object stored with `Content-Encoding: gzip`, which Cloud Storage may
  serve decompressed, `get`'s `contentLength` is the number of bytes received and can differ. This
  backend never writes such an object.
- **An object without a `Content-Type` reads as `application/octet-stream`.**
- **Storage classes are Cloud Storage's** (`NEARLINE`, `COLDLINE`, `ARCHIVE`), not S3's.

Errors:

- **Error codes come in two spellings.** Refusals made locally are upper snake case
  (`INVALID_KEY`); Cloud Storage's own codes are passed on verbatim (`AccessDenied`). Only
  `NoSuchKey` is mapped, to the interface's `NOT_FOUND`.
- **`delete` of a missing object is `Ok`, of a missing bucket is not.** Cloud Storage answers 404
  `NoSuchKey` when there was nothing to delete (S3 answers 204); the backend treats exactly that
  answer as success. `NoSuchBucket` and `AccessDenied` stay errors.
- **HEAD has no error body, so `exists` and `stat` know only the status.** A wrong secret is
  `HTTP_403` there, where `get` says `SignatureDoesNotMatch`. A 404 is "no such object" whatever
  the cause, so `exists` on a bucket that does not exist is `Ok(false)`, not `NoSuchBucket`.
- **`exists` is an `Err` when it cannot tell**, never `false`: a refused connection or a 403 is not
  "absent".
- **Out-of-range arguments differ between targets.** An empty content type, `maxKeys` outside 1 to
  1000 and `expiresInSeconds` outside 1 to 604800 violate the interface's `requires:` clauses. On
  `jvm` a call through `StorageBucket` panics with `PreconditionViolated`; on `dotnet` the same
  call reaches the backend, which answers `Err(INVALID_ARGUMENT)` for the two ranges and does not
  check the content type (`tests/fake_gcs_tests.l` asserts both behaviours). Validate before
  calling.
- **Requests are not retried, and a timeout does not undo the request.** `SlowDown` (429, 503),
  5xx answers and timeouts reach the caller as errors. A `put` that is `CONNECTION_FAILED` after
  `requestTimeoutMs` may still have been stored.

Signed URLs:

- **A signed URL is computed locally.** No request is made, the object need not exist, and a wrong
  key produces a URL that Cloud Storage will refuse. The local-filesystem backend answers
  `NOT_SUPPORTED` instead.
- **It is signed with the local clock**, as every request is. Cloud Storage accepts a request
  whose `x-goog-date` is within 15 minutes of its own clock (`RequestTimeTooSkewed` otherwise), and
  a signed URL from 15 minutes before its date until it expires.
- **It stops working when the HMAC key is deleted or deactivated,** whatever `expiresInSeconds`
  says.
- **Only the host is signed.** A signed PUT URL accepts any content type and any size.

Memory and transport:

- **Objects are held in memory whole, more than once.** `put` decodes the base64 payload to bytes
  and hashes them (the payload is signed with its SHA-256 in `x-goog-content-sha256`, not
  `UNSIGNED-PAYLOAD`); `get` downloads the bytes and encodes them to base64. Use `putBytes` /
  `getBytes` (Extras) to skip the base64 step. There is no streaming, resumable upload or range
  read.
- **Every `x-goog-*` header is signed, `Content-Type` is not.** Cloud Storage requires the former.
- **A client is a value.** `GcsClient` has no mutable state of its own; every request is signed and
  sent independently through one `Std.Http` client, so it can be shared between threads.

## Backend-specific module

`Storage.Gcs.Extras` holds what Cloud Storage offers beyond the interface. Its functions take a
`GcsClient` (`openClient(cfg)`), validate keys like the interface methods, and move objects as raw
bytes:

| Function | |
|---|---|
| `putBytes(client, key, bytes, contentType, userMeta)` | PUT Object from a `slice[Byte]`, no base64 step; returns the ETag |
| `putBytesWithStorageClass(client, key, bytes, contentType, storageClass)` | PUT Object with `x-goog-storage-class`. The name must be one `isStorageClass` knows (`STANDARD`, `NEARLINE`, `COLDLINE`, `ARCHIVE`, and the legacy `MULTI_REGIONAL`, `REGIONAL`), else `INVALID_ARGUMENT`. Takes no user metadata |
| `getBytes(client, key)` | GET Object as bytes plus `ObjectHead` |
| `stat(client, key)` | HEAD Object: stored size, content type, ETag, user metadata, storage class, generation; `None` when absent |
| `presignedPutUrl(client, key, expiresInSeconds)` | A V4 signed URL that accepts one upload without credentials |
| `bucketExists(client)` | HEAD Bucket: `Ok(false)` for 404, `Err(HTTP_403)` when the key may not use it |

The other packages are public for an operation this package does not wrap:

- `Storage.Gcs.Client`: `GcsClient` and its methods (`putObject`, `getObject`, `headObject`,
  `deleteObject`, `listObjects`, `presign`), the signed-request primitive `GcsClient.send` (whose
  response reports how the request was signed), `faultOf`, `userMetaProblem`, `googDateNow`. These
  methods do not validate keys.
- `Storage.Gcs.Config`: `GcsConfig`, its constructors and `with*` functions, `configFromLookup`,
  and the validators (`isValidBucketName`, `isValidLocation`, `isLoopbackAuthority`,
  `parseEndpoint`).
- `Storage.Gcs.Goog4`: `HmacKey`, `googCredentialScope`, `googStringToSign`, `googSigningKey`,
  `googSignatureOf`, `signGoogRequest`, `googSignedHeaders`, `googPresignQuery`,
  `canonicalKeyPath`. It re-exports `Aws.SigV4`, so `Pair`, `canonicalRequest` and `uriEncode` are
  in scope too; so are that package's own signing functions (`signRequest`, `presignQuery`), which
  sign with `AWS4-HMAC-SHA256`, not for this backend.
- `Storage.Gcs.Wire`: parsing of `ListBucketResult` and `Error` documents (with `Details`, and the
  string to sign of a `SignatureDoesNotMatch`), ETags, HTTP dates and url-encoded names.

## Verification status

| What | Checked against | |
|---|---|---|
| The canonical request and string to sign of GET, PUT (body, user metadata, storage class), DELETE, a `list-type=2` listing, a virtual-hosted request and a V4 signed URL | **Google Cloud Storage** (`tests/live`, both targets) | Cloud Storage reports, in its `SignatureDoesNotMatch` error, the canonical request and string to sign it derived; they equal this client's byte for byte, and the signature sent is the HMAC of that string |
| The signing key derivation and the final HMAC | An independent implementation (Python `hmac`/`hashlib`, from Google's "Signatures" page) | `tests/goog4_tests.l` |
| Listing with `list-type=2`, `encoding-type=url`, `max-keys`, `prefix`, `continuation-token`; GET; HEAD (`x-goog-stored-content-length`, storage class, generation, ETag); HEAD Bucket; the `Error` documents for `NoSuchKey`, `NoSuchBucket`, `AccessDenied`, an invalid token | **Google Cloud Storage**, anonymously, on Google's public buckets (`tests/live`, both targets) | The requests are unsigned, so this shows the request and response handling, not the authentication |
| The `StorageBucket` conformance suite, PUT, DELETE, user metadata and storage class round trips, signed URLs being honoured and expiring, every error mapping | **Only the in-process fake** (`tests/fake_gcs_tests.l`, both targets) | The fake checks every signature with the algorithm above and answers as Cloud Storage was observed or is documented to answer |
| **A request signed with a real HMAC key being accepted by Google Cloud Storage; the response of an authorized PUT and DELETE; a signed URL being honoured** | **Nothing** | `tests/integration` is written for it and has never been run: it needs a bucket and an HMAC key |

No emulator closes that gap. `fake-gcs-server` (v1.56.1 was built and tried) serves the JSON API:
an XML API `PUT /<bucket>/<object>` is routed to its JSON upload handler and answered
`invalid uploadType`, its errors are JSON, its XML listing ignores `max-keys` and
`continuation-token`, and it checks no signature. S3-compatible servers such as MinIO implement
`AWS4-HMAC-SHA256` with `x-amz-*` headers; none is known to accept `GOOG4-HMAC-SHA256` (not tried
here). So there is no `scripts/ci/start-*.sh` for this package.

From the XML API documentation and not observed: that `ExpiredToken` is the code of an expired
signed URL (Cloud Storage checks the signature first, so it cannot be seen without a key), the
status of a successful PUT (200) and DELETE (204), and that an upload's response carries `ETag`
and `x-goog-generation`.

## Tests

Run from the repository root:

```sh
lyric test --manifest storage-gcs/lyric.toml --target dotnet
lyric test --manifest storage-gcs/lyric.toml --target jvm
```

The default run is hermetic and needs no network:

- `tests/goog4_tests.l`: the signing, step by step. The canonical requests and strings to sign are
  the ones Google Cloud Storage reported for these requests; the signing key and signatures were
  computed independently with Python.
- `tests/wire_tests.l`: `ListBucketResult` and `Error` documents as Cloud Storage sent them, ETags,
  HTTP dates, url-encoded names.
- `tests/config_tests.l`: constructors, named settings, validation, addressing.
- `tests/readme_tests.l`: the examples of this README.
- `tests/fake_gcs_tests.l`: `Testing.Conformance.Storage` and the backend's own behaviour over
  real HTTP against an XML API endpoint written in Lyric on `Std.HttpServer` inside the test. The
  server verifies the `GOOG4-HMAC-SHA256` signature of every request (header and signed-URL forms,
  including expiry and clock skew) with the shared secret, the hash of the body it received, and
  that every `x-goog-*` header is signed; keeps objects in memory; pages `list-type=2` listings;
  and serves an anonymous caller one public bucket. It listens on the loopback ports 19901 to
  19907, which must be free.

The live suite talks to `storage.googleapis.com` and needs the network but no credentials:

```sh
lyric test --manifest storage-gcs/lyric.toml --target dotnet --features live
```

It sends about ten requests signed with an HMAC key that does not exist (all refused; nothing is
stored, and the probe bucket need not exist because the signature is checked first) and reads a
few small objects from Google's public buckets `gcp-public-data-landsat` and `cloud-samples-data`.
`LYRIC_GCS_LIVE_BUCKET` and `LYRIC_GCS_LIVE_PREFIX` select another public bucket for the listing
and object tests.

The integration suite runs against a real bucket and is compiled only with
`--features credentials`:

```sh
LYRIC_GCS_BUCKET=my-test-bucket LYRIC_GCS_HMAC_ACCESS_ID=GOOG1E... LYRIC_GCS_HMAC_SECRET=... \
  lyric test --manifest storage-gcs/lyric.toml --target dotnet --features credentials
```

| Variable | |
|---|---|
| `LYRIC_GCS_BUCKET` | a disposable bucket that already exists. **Every object in it is deleted**, repeatedly |
| `LYRIC_GCS_HMAC_ACCESS_ID`, `LYRIC_GCS_HMAC_SECRET` | an HMAC key that may read, write and delete in the bucket |
| `LYRIC_GCS_ENDPOINT` | optional; unset or `google` is `https://storage.googleapis.com` |
| `LYRIC_GCS_LOCATION` | optional; the location to sign for (`auto`) |

A missing variable fails every integration test with a message naming it. The suite runs the
conformance suite, binary objects with encoded names and metadata, HEAD, a storage class, signed
GET and PUT URLs fetched by a client without credentials, and Cloud Storage's own error codes. It
has not been run (see Verification status), and it cannot run in CI without a bucket and a key
supplied as secrets.

## Pinned versions

| | |
|---|---|
| SDK / native libraries | none |
| `Lyric.Aws.SigV4` | the version in this repository (`aws-sigv4/`) |
| `Lyric.Storage`, `Lyric.Conformance`, standard library | the `lyric-lang` commit in `.lyric-lang-rev`, through the root `[workspace.overrides]`. The package needs `Std.Http.withBytesBody`, `HttpResponse.headerNames` and (for the tests) `Std.HttpServer.requestBodyBytes` (nichobbs/lyric-lang#9047) |
| Cloud Storage XML API | as documented and as served on 2026-11-12 |
| Emulator | none (see Verification status) |

## Known gaps

- Not verified against Google Cloud Storage with a real HMAC key (see Verification status).
- Authentication is HMAC keys only: no OAuth 2.0, service-account key files, Application Default
  Credentials, workload identity or metadata server. They need RSA-SHA256 signing, which the Lyric
  standard library does not have on either target. No lyric-lang issue number yet.
- Objects are held in memory whole and uploaded with one PUT, as the interface's base64 payloads
  require: no resumable or multipart upload, no compose, no streaming, no range reads. `Std.Http`
  buffers bodies too.
- No bucket creation or deletion (the XML API needs a project for it), no object versions or
  generation preconditions (`x-goog-if-generation-match`), no ACLs, no customer-supplied or
  customer-managed encryption keys, no requester-pays (`x-goog-user-project`), no object holds or
  retention, no batch delete. `list` has no delimiter.
- Requests are not retried. `SlowDown` and 5xx answers reach the caller as errors.
- The interface's `requires:` clauses are enforced on `jvm` and not on `dotnet` for this backend
  (see Surprises; nichobbs/lyric-lang#9043).
- `Std.HttpServer` cannot send a HEAD response that reports an entity's length
  (nichobbs/lyric-lang#9048). The hermetic tests are not affected: Cloud Storage reports an
  object's size in `x-goog-stored-content-length`, the client reads that header first, and the fake
  sends it, so `stat`'s `contentLength` is asserted hermetically as well as live.
- The signing shares `Lyric.Aws.SigV4`'s canonical request but repeats the shape of its signing
  functions with other names (`signGoogRequest`, `googSignedHeaders`, `googPresignQuery`), because
  the algorithm name, key prefix, scope terminator and header and query prefixes are constants
  there. Parameterising them in `aws-sigv4/` would remove the repetition. Likewise
  `Storage.Gcs.Wire` parses the same `ListBucketResult` document as `Storage.S3.Wire`.
- A `pub val` of a library package is not visible to a dependent package that links the library's
  shared build (`T0020`), so constants are exposed as functions (`googAlgorithm()`,
  `defaultLocation()`). As in `storage-s3`, a request body is carried in a `Payload` record rather
  than a bare `slice[Byte]` parameter (nichobbs/lyric-lang#9039, #9049).
