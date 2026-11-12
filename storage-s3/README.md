# lyric-storage-s3

Amazon S3, and S3-compatible servers (MinIO, LocalStack, Ceph RGW, Cloudflare R2), as a backend for
the `Storage.StorageBucket` interface of `lyric-storage`.

The backend is pure Lyric: the S3 REST API over `Std.Http`, signed with AWS Signature Version 4.
There is no AWS SDK underneath and no extern declaration in the package, so the same code runs on
both targets and there is no `_kernel_<target>/` directory.

| | |
|---|---|
| Package | `Lyric.Storage.S3` (Lyric packages `Storage.S3`, `Storage.S3.Client`, `Storage.S3.Extras`, `Storage.S3.SigV4`, `Storage.S3.Wire`) |
| Targets | `dotnet`, `jvm` |
| Stability | `experimental` (0.1.0) |
| Interface | `Storage.StorageBucket` from `Lyric.Storage` 0.1.0, re-exported with `pub use Storage` |
| SDK / native dependencies | none |
| Protocol | S3 REST API, 2006-03-01; AWS Signature Version 4 (`AWS4-HMAC-SHA256`) |

## Requirements

`Std.Http.withBytesBody`, `HttpResponse.headerNames` and `Std.HttpServer.requestBodyBytes` (the
last for the tests only), added in nichobbs/lyric-lang#9047. They are not in a release yet, so the
package does not build with an older standard library; `.lyric-lang-rev` pins a commit that has
them.

## Usage

```lyric
import Std.Core
import Storage.S3

func example(keyId: in String, secret: in String, pdfBase64: in String): Result[String, StorageError] {
  val bucket = connect(awsConfig("my-bucket", "eu-west-1", keyId, secret))?
  put(bucket, "reports/q3.pdf", pdfBase64, "application/pdf")?
  bucket.presignedUrl("reports/q3.pdf", 3600)
}
```

`import Storage.S3` also brings the names of `Storage` into scope bare (`put`, `get`,
`StorageBucket`, `StorageError`), because the package re-exports it. `connect` returns the interface, so the rest of an application names only `StorageBucket` and can
be given the local-filesystem backend or a mock instead.

### Configuration

Build an `S3Config` and pass it to `connect` (or `openClient`, for the S3-only operations):

| Constructor | Addressing |
|---|---|
| `awsConfig(bucket, region, accessKeyId, secretAccessKey)` | AWS, virtual-hosted: `https://<bucket>.s3.<region>.amazonaws.com/<key>` |
| `endpointConfig(endpoint, bucket, region, accessKeyId, secretAccessKey)` | a custom endpoint, path-style: `<endpoint>/<bucket>/<key>` |
| `configFromEnv(bucket)` / `connectFromEnv(bucket)` | from the environment, see below |

Adjust with `.withSessionToken(token)` (temporary credentials), `.withPathStyle(true|false)`
(for example path-style on AWS for a bucket name containing dots, which a TLS wildcard certificate
does not cover) and `.withRequestTimeout(ms)` (default 30000).

`configFromEnv` reads the standard variables: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`,
`AWS_SESSION_TOKEN` (optional), `AWS_REGION` or `AWS_DEFAULT_REGION`, and `AWS_ENDPOINT_URL_S3` or
`AWS_ENDPOINT_URL` (optional; path-style when set).

Take credentials from the application's `config { }` block or the environment, never from source.

`openClient` validates the configuration before any request and answers `INVALID_CONFIG` for a
bucket or region name that could alter the host name, empty credentials, a malformed endpoint, or
an `http://` endpoint whose host is not `localhost`, `127.0.0.1` or `[::1]`. Plain HTTP would
expose the access key id, object keys, metadata and payloads, and let anyone on the path replay a
request for the 15 minutes its signature is valid.

### Behaviour of the interface methods

| Method | S3 operation | Notes |
|---|---|---|
| `put` | PutObject | The base64 payload is decoded and sent as bytes. User metadata travels as `x-amz-meta-<key>` headers. |
| `get` | GetObject | `lastModified` is the `Last-Modified` header as ISO 8601. `userMeta` holds every `x-amz-meta-*` header. |
| `delete` | DeleteObject | Idempotent, as S3 defines. |
| `exists` | HeadObject | 404 is `Ok(false)`; any other failure is an `Err`, not `false`. |
| `list` | ListObjectsV2 | `prefix`, `continuation-token`, `max-keys`; keys requested with `encoding-type=url`, so a key XML cannot carry is still listed. |
| `presignedUrl` | presigned GET | Computed locally; the object need not exist. At most 604800 seconds. |

Keys are validated with `Storage.isSafeKey` (prefixes with `isSafePrefix`) exactly as the
local-filesystem backend validates them, so `..` segments, a `.meta.json` suffix and the rest are
`INVALID_KEY` here too, and no request is made. A valid key is percent-encoded into the path with
`/` kept, as SigV4 requires for S3.

User metadata that S3 would alter is refused with `USER_META_UNSUPPORTED` rather than stored
differently: a key must be lowercase letters, digits, `-`, `_` or `.` (S3 lowercases keys), and a
value printable ASCII without leading or trailing spaces (a header cannot carry anything else
intact).

### Error codes

`StorageError.code` is:

| Code | Meaning |
|---|---|
| `INVALID_KEY`, `INVALID_ARGUMENT`, `INVALID_BASE64`, `USER_META_UNSUPPORTED`, `INVALID_CONFIG` | refused locally, nothing was sent |
| `NOT_FOUND` | S3 answered `NoSuchKey`, or 404 without an error document |
| any other S3 code, verbatim (`AccessDenied`, `NoSuchBucket`, `SignatureDoesNotMatch`, `InvalidAccessKeyId`, `PermanentRedirect`, `SlowDown`, ...) | the `<Code>` of S3's `Error` document |
| `HTTP_<status>` | a response without an S3 error document (a HEAD, or a proxy's error page) |
| `CONNECTION_FAILED` | no response: connection refused, TLS failure, timeout |
| `LIST_ERROR` | a listing that is not a valid `ListBucketResult` |

Redirects are not followed. S3 answers 301 or 307 for a bucket in another region; following it
would send the request to a host the signature does not cover, so it is reported as
`PermanentRedirect` or `TemporaryRedirect`.

## S3-only operations: `Storage.S3.Extras`

These take an `S3Client` (`openClient(cfg)`; `bucketOf(client)` gives the `StorageBucket` for the
same client) and are not part of the interface:

| Function | |
|---|---|
| `putBytes(client, key, bytes, contentType, userMeta)` | PutObject from a `slice[Byte]`, no base64 step; returns the ETag |
| `putBytesWithStorageClass(client, key, bytes, contentType, storageClass)` | PutObject with `x-amz-storage-class` (`STANDARD_IA`, `GLACIER_IR`, ...) |
| `getBytes(client, key)` | GetObject as bytes plus `ObjectHead` |
| `stat(client, key)` | HeadObject: size, content type, ETag, user metadata, storage class; `None` when absent |
| `presignedPutUrl(client, key, expiresInSeconds)` | a URL that accepts one upload without credentials |
| `createBucket(client)` / `bucketExists(client)` | CreateBucket (naming the region outside `us-east-1`) and HeadBucket |

`Storage.S3.Client` also exposes the signed-request primitive (`S3Client.send`) and
`Storage.S3.SigV4` the signing functions, for an operation this package does not wrap.

## Tests

```sh
lyric test --manifest storage-s3/lyric.toml --target dotnet
lyric test --manifest storage-s3/lyric.toml --target jvm
```

The default run is hermetic and needs no network:

- `tests/sigv4_tests.l`: the worked examples of the Amazon S3 API reference (GET Object, PUT
  Object, GET Bucket Lifecycle, List Objects, and the presigned URL example), checking the
  canonical request, the string to sign, the signature and the `Authorization` header, plus key
  and query encoding.
- `tests/wire_tests.l`: `ListBucketResult` and `Error` documents, ETags, HTTP dates, url-encoded
  listing keys.
- `tests/fake_s3_tests.l`: `Testing.Conformance.Storage` and the backend's own behaviour over real
  HTTP against an S3 server written in Lyric on `Std.HttpServer` inside the test. The server
  verifies the SigV4 signature of every request (header and presigned-URL forms) with the shared
  secret and the hash of the body it received, keeps objects in memory, and pages ListObjectsV2.

Integration tests run against a real S3-compatible server and are compiled only with
`--features integration`:

```sh
LYRIC_S3_ENDPOINT=http://127.0.0.1:9000 LYRIC_S3_BUCKET=lyric-it LYRIC_S3_REGION=us-east-1 \
AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... \
  lyric test --manifest storage-s3/lyric.toml --target dotnet --features integration
```

| Variable | |
|---|---|
| `LYRIC_S3_ENDPOINT` | base URL of the server; `aws` selects AWS S3 itself, virtual-hosted |
| `LYRIC_S3_BUCKET` | a disposable bucket; created when missing. **Every object in it is deleted**, repeatedly |
| `LYRIC_S3_REGION` | the region to sign for (`us-east-1` for a default MinIO) |
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` | credentials that may create and use the bucket |
| `AWS_SESSION_TOKEN` | optional |

A missing variable fails every integration test with a message naming it. The suite runs the
conformance suite, binary objects with encoded keys and metadata, HEAD, presigned GET and PUT
fetched by a client without credentials, and S3's own error codes. It passes on both targets
against MinIO (built from source, `github.com/minio/minio@latest`, single node).

## Known gaps

- Objects are held in memory whole, as the interface's base64 payloads require: no multipart
  upload, no streaming, no range reads. `Std.Http` buffers bodies too.
- No server-side encryption headers, object tagging, versioning, ACLs, or batch delete.
- Credentials are static: there is no IMDS, ECS, SSO or `~/.aws/credentials` provider chain and no
  refresh of temporary credentials. Build a new client when a session token is renewed.
- Requests are not retried. S3's `SlowDown` and 5xx answers reach the caller as errors.
- A presigned URL is signed with the local clock, as every request is; a clock more than 15
  minutes off is `RequestTimeTooSkewed`.
- In the hermetic tests the fake server answers HEAD with `Content-Length: 0`, because
  `Std.HttpServer` cannot send a HEAD response that reports an entity's length. `stat`'s
  `contentLength` is therefore asserted only by the integration tests.
- JVM compiler defect found while writing the backend (not worked around by a conversion in
  library code; the request body is carried in a `Payload` record instead of a bare parameter):
  in a library package, a `slice[Byte]` returned by a stdlib call and passed straight to another
  Lyric function's `in slice[Byte]` parameter fails class verification
  (`'[B' is not assignable to '[Ljava/lang/Object;'`). Described in
  `lyric-lang/docs/progress/2026-11-12-http-bytes-body.md`; no issue number yet.
