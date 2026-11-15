# lyric-storage-s3

Amazon S3, and S3-compatible servers (MinIO, LocalStack, Ceph RGW, Cloudflare R2), as a backend for
the `Storage.StorageBucket` interface of `lyric-storage` (`nichobbs/lyric-lang`). It runs on `dotnet`,
`jvm` and `native` and is `experimental` (0.1.0). The backend is pure Lyric: the S3 REST API over
`Std.Http`, signed with AWS Signature Version 4. There is no AWS SDK underneath and no extern
declaration in the package, so the same code runs on every target and there is no
`_kernel_<target>/` directory.

| | |
|---|---|
| Package | `Lyric.Storage.S3` (Lyric packages `Storage.S3`, `Storage.S3.Client`, `Storage.S3.Extras`, `Storage.S3.SigV4`, `Storage.S3.Wire`) |
| Targets | `dotnet`, `jvm`, `native` (Linux x86-64; clang and lyric-rt to build, OpenSSL 3 at run time for HTTPS) |
| Stability | `experimental` (0.1.0) |
| Interface | `Storage.StorageBucket` from `Lyric.Storage`, re-exported with `pub use Storage` |
| Dependencies | `Lyric.Aws.SigV4` (`aws-sigv4/`, the signing) and `Lyric.Aws.Credentials` (`aws-credentials/`, credential providers and the retry policy), both shared with the other AWS backends of this repository, as `{ workspace = true }` dependencies. No SDK and no native dependency |
| Protocol | S3 REST API, 2006-03-01; AWS Signature Version 4 (`AWS4-HMAC-SHA256`) |

## Usage

Inside this workspace an application declares `"Lyric.Storage.S3" = { workspace = true }` under
`[dependencies]`.

```lyric
import Std.Core
import Std.Encoding as Enc
import Storage.S3

func example(keyId: in String, secret: in String): Result[String, StorageError] {
  val bucket = connect(awsConfig("my-bucket", "eu-west-1", keyId, secret))?
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

Every function and every `StorageBucket` method returns `Result[_, StorageError]`; `StorageError` has a
`code` (table under Configuration) and a `message`.

`import Storage.S3` also brings the names of `Storage` and of `Storage.S3.Client` into scope bare
(`put`, `get`, `StorageBucket`, `StorageError`, `awsConfig`, `openClient`), because the package
re-exports both. `connect` returns the interface, so the rest of an application names only
`StorageBucket` and can be given the local-filesystem backend or a mock instead.

Variations:

- **Credentials from the environment**, and an S3-compatible server:

  ```lyric
  func fromEnvironment(): Result[StorageBucket, StorageError] {
    connectFromEnv("my-bucket")
  }

  func compatibleServer(keyId: in String, secret: in String): Result[StorageBucket, StorageError] {
    connect(endpointConfig("https://minio.example.com:9000", "data", "us-east-1", keyId, secret))
  }
  ```

- **A credential provider and a retry policy** (`Aws.Credentials`, `Aws.Retry`, from
  `aws-credentials/`): credentials that are refreshed before they expire, and five attempts instead
  of three. Compiled by `tests/readme_tests.l`.

  ```lyric
  import Aws.Credentials.Chain
  import Aws.Retry

  func withDefaultChain(bucket: in String, region: in String): Result[StorageBucket, StorageError] {
    val cfg = awsConfig(bucket, region, "", "")
      .withCredentialProvider(defaultProvider())
      .withRetry(standardRetry().withMaxAttempts(5))
    connect(cfg)
  }

  func sentOnce(keyId: in String, secret: in String): Result[StorageBucket, StorageError] {
    connect(awsConfig("my-bucket", "eu-west-1", keyId, secret).withoutRetries())
  }
  ```

- **Temporary credentials, path-style addressing, another timeout:**

  ```lyric
  func adjusted(keyId: in String, secret: in String, token: in String): Result[StorageBucket, StorageError] {
    val cfg = awsConfig("my.dotted.bucket", "us-east-1", keyId, secret)
      .withSessionToken(token)
      .withPathStyle(true)
      .withRequestTimeout(10000)
    connect(cfg)
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

- **S3-only operations** on an `S3Client` (`import Storage.S3.Extras`); `bucketOf(client)` gives the
  `StorageBucket` for the same client:

  ```lyric
  func s3Only(cfg: in S3Config, bytes: in slice[Byte]): Result[String, StorageError] {
    val client = openClient(cfg)?
    if not bucketExists(client)? {
      val created = createBucket(client)?
      println(created.toString())
    }
    val noMeta: Map[String, String] = newMap()
    val etag = putBytes(client, "raw/data.bin", bytes, "application/octet-stream", noMeta)?
    val got = getBytes(client, "raw/data.bin")?
    println(etag + got.bytes.length.toString())
    match stat(client, "raw/data.bin")? {
      case Some(head) -> println(head.contentLength.toString() + " " + head.storageClass)
      case None -> println("absent")
    }
    presignedPutUrl(client, "inbox/upload.bin", 900)
  }
  ```

How the interface methods map to S3:

| Method | S3 operation | Notes |
|---|---|---|
| `put` | PutObject | The base64 payload is decoded and sent as bytes. User metadata travels as `x-amz-meta-<key>` headers. |
| `get` | GetObject | `lastModified` is the `Last-Modified` header as ISO 8601. `userMeta` holds every `x-amz-meta-*` header. |
| `delete` | DeleteObject | Idempotent, as S3 defines. |
| `exists` | HeadObject | 404 is `Ok(false)`; any other failure is an `Err`, not `false`. |
| `list` | ListObjectsV2 | `prefix`, `continuation-token`, `max-keys`; keys requested with `encoding-type=url`, so a key XML cannot carry is still listed. |
| `presignedUrl` | presigned GET | Computed locally; the object need not exist. At most 604800 seconds. |

## Configuration

Build an `S3Config` and pass it to `connect` (or `openClient`, for the S3-only operations). Take
credentials from the application's `config { }` block or the environment, never from source.

| Constructor | Addressing |
|---|---|
| `awsConfig(bucket, region, accessKeyId, secretAccessKey)` | AWS, virtual-hosted: `https://<bucket>.s3.<region>.amazonaws.com/<key>` |
| `endpointConfig(endpoint, bucket, region, accessKeyId, secretAccessKey)` | a custom endpoint, path-style: `<endpoint>/<bucket>/<key>` |
| `configFromEnv(bucket)` / `connectFromEnv(bucket)` | from the environment and the AWS shared files, see below |
| `configFromEnvironment(bucket, lookup)` | the same, reading variables through `lookup` (`Aws.Credentials.environmentOf(map)`) instead of the process environment |

| `S3Config` field | Set with | Default | Meaning |
|---|---|---|---|
| `bucket` | constructor | (required) | 3 to 63 characters of lowercase letters, digits, `-` and `.`, starting and ending with a letter or digit, no `..` |
| `region` | constructor | (required) | The region to sign for: lowercase letters, digits and `-` (`us-east-1` for a default MinIO, `auto` for R2) |
| `accessKeyId`, `secretAccessKey` | constructor | (required unless `credentialProvider` is set) | Not empty; ignored when there is a credential provider |
| `sessionToken` | `.withSessionToken(token)` | `""` | The token of temporary credentials |
| `endpoint` | `endpointConfig` | `""` (AWS) | `http(s)://host[:port]`, nothing after the authority but an optional `/`. `https://` unless the host is `localhost`, `127.0.0.1` or `[::1]` |
| `pathStyle` | `.withPathStyle(flag)` | false for `awsConfig`, true for `endpointConfig` and for an endpoint from the environment | Put the bucket in the path instead of the host name |
| `requestTimeoutMs` | `.withRequestTimeout(ms)` | 30000 | Deadline of each attempt; must be positive |
| `credentialProvider` | `.withCredentialProvider(provider)` | `None` | An `Aws.Credentials.CredentialProvider` asked for the credentials of every attempt, instead of the three fields above. Wrap a provider that calls a service or reads a file in `Aws.Credentials.cached` |
| `retry` | `.withRetry(policy)`, `.withoutRetries()` | `Aws.Retry.standardRetry()`: 3 attempts, waits of up to 100 ms and 200 ms | When a failed request is sent again; `maxAttempts` must be at least 1 |
| `additionalCaCertificatePem` | `.withCaCertificate(pem)`, `.withCaCertificateFile(path)` | `""` | A certificate authority (PEM text, one certificate) trusted in addition to the system trust store |
| `caCertificatePem` | `.withExclusiveCaCertificate(pem)`, `.withExclusiveCaCertificateFile(path)` | `""` | The only certificate authority the server certificate may chain to; the system trust store is not consulted. Wins over `additionalCaCertificatePem` |

TLS: an `https://` endpoint's certificate is verified against the system trust store and must carry
the host name. For an S3-compatible server whose certificate comes from a private authority, or is
self-signed, give the authority's certificate (or the self-signed certificate itself) to
`withCaCertificate`, which adds it to the system trust store, or to `withExclusiveCaCertificate`,
which trusts nothing else. These are `Std.Http`'s options of the same names, and the names and
meaning are those of `Lyric.Redis`. The `...File` forms read a PEM file and return a `Result`: a
file that is missing or empty is `INVALID_CONFIG`. There is no option to skip certificate
verification.

```lyric
val base = endpointConfig("https://minio.internal.example:9000", "reports", "us-east-1", keyId, secret)
val cfg = match base.withCaCertificateFile("/etc/ssl/private-ca.pem") { case Ok(c) -> c; case Err(e) -> return Err(e) }
val bucket = connect(cfg)?
```

`configFromEnv` and `connectFromEnv` configure the bucket the way the AWS SDKs would find it
(`Aws.Credentials`, from `aws-credentials/`; its README has the details of every provider):

| What | From, in this order |
|---|---|
| Credentials | the default chain: `AWS_ACCESS_KEY_ID` + `AWS_SECRET_ACCESS_KEY` (+ `AWS_SESSION_TOKEN`); web identity (`AWS_WEB_IDENTITY_TOKEN_FILE` + `AWS_ROLE_ARN`); the profile `AWS_PROFILE` (else `default`) of `AWS_CONFIG_FILE` / `AWS_SHARED_CREDENTIALS_FILE` (else `~/.aws/config`, `~/.aws/credentials`), static keys or `credential_process`; the container endpoint (`AWS_CONTAINER_CREDENTIALS_RELATIVE_URI`, `AWS_CONTAINER_CREDENTIALS_FULL_URI`); the EC2 instance metadata service (IMDSv2; `AWS_EC2_METADATA_DISABLED=true` skips it) |
| Region (required) | `AWS_REGION`, `AWS_DEFAULT_REGION`, the profile's `region` |
| Endpoint (optional; path-style when set) | `AWS_ENDPOINT_URL_S3`, `AWS_ENDPOINT_URL`, the profile's `endpoint_url`; none of them when `AWS_IGNORE_CONFIGURED_ENDPOINT_URLS=true` |
| CA certificate (optional, additive) | `AWS_CA_BUNDLE`, the profile's `ca_bundle`: a PEM file, as `withCaCertificateFile` |

The chain is asked once, in `configFromEnv`, so missing credentials are `INVALID_CONFIG` there, with
the reason of every provider. Credentials from the environment variables become the static fields
of the `S3Config`, exactly as before this package had providers. Credentials from any other source
are not copied into the configuration: it carries the cached chain as its `credentialProvider`, and
temporary credentials are refreshed five minutes before they expire.

**Changed from earlier versions of this package**, which read the three credential variables and
the region and endpoint variables only: (1) when the credential variables are unset, the shared
files, the container endpoint and the instance metadata service are now consulted, where
`configFromEnv` used to fail at once; off EC2 that costs up to a second for the metadata service
unless `AWS_EC2_METADATA_DISABLED=true`; (2) the error text for missing credentials is the chain's,
not `AWS_ACCESS_KEY_ID is not set`; (3) the region, the endpoint and a CA bundle are also taken
from the profile, and `AWS_CA_BUNDLE` is honoured; (4) only one of the two credential variables set
is `INVALID_CONFIG` naming both, and the chain does not go on to another source.

`openClient` (and so `connect`) validates the configuration before any request and answers
`INVALID_CONFIG` for a bucket or region name that could alter the host name, empty credentials
when there is no credential provider, a malformed endpoint, a non-positive timeout, a CA certificate that is not a PEM certificate, or an
`http://` endpoint whose host is not loopback. Plain
HTTP would expose the access key id, object keys, metadata and payloads, and let anyone on the path
replay a request for the 15 minutes its signature is valid.

`StorageError.code` is:

| Code | Meaning |
|---|---|
| `INVALID_KEY`, `INVALID_ARGUMENT`, `INVALID_BASE64`, `USER_META_UNSUPPORTED`, `INVALID_CONFIG` | refused locally, nothing was sent |
| `NOT_FOUND` | S3 answered `NoSuchKey`, or 404 without an error document |
| any other S3 code, verbatim (`AccessDenied`, `NoSuchBucket`, `SignatureDoesNotMatch`, `InvalidAccessKeyId`, `PermanentRedirect`, `SlowDown`, ...) | the `<Code>` of S3's `Error` document |
| `HTTP_<status>` | a response without an S3 error document (a HEAD, or a proxy's error page) |
| `CONNECTION_FAILED` | no response: connection refused, TLS failure, timeout |
| `CREDENTIALS_UNAVAILABLE` | the credential provider had no credentials; nothing was sent |
| `LIST_ERROR` | a listing that is not a valid `ListBucketResult` |

## Surprises

Connecting and addressing:

- **`connect` makes no request.** A wrong credential, a wrong region or a missing bucket shows up on
  the first operation, not at start-up. Call `bucketExists` (Extras) to check early.
- **Redirects are not followed.** S3 answers 301 or 307 for a bucket in another region; following it
  would send the request to a host the signature does not cover, so it is reported as
  `PermanentRedirect` or `TemporaryRedirect`. Fix the region in the configuration.
- **A custom endpoint is path-style, AWS is virtual-hosted.** A bucket name containing dots is not
  covered by AWS's TLS wildcard certificate; address it with `.withPathStyle(true)`. An endpoint with
  a path (a gateway mounted under a prefix) is refused.
- **A CA certificate applies to the client's own requests only.** A presigned URL is fetched by
  whoever receives it, with their own trust store. On an `http://` (loopback) endpoint the option is
  validated and then has nothing to verify.
- **Only the first certificate of the PEM text is read on `dotnet` and `jvm`; `native` reads all of
  them.** `withCaCertificate` and `withExclusiveCaCertificate` take one certificate authority, not
  a bundle: in a file with several certificates the managed targets ignore the others, and
  `native` (OpenSSL) trusts every one of them. The difference is the standard library's
  (`Std.Tls.Certificate.fromPem`); there is no lyric-lang issue for it yet. Pass one certificate,
  the authority that signed the server certificate (for a self-signed server, its own
  certificate), and the three targets agree.
- **An untrusted server certificate is `CONNECTION_FAILED`, and the message differs by target.** On
  the JVM it names the cause (`PKIX path building failed: ... unable to find valid certification path
  to requested target`); on dotnet it is only `The SSL connection could not be established, see inner
  exception`; on native it is OpenSSL's (`TLS handshake failed: ... certificate verify failed`). The
  code is the same, and so are the CA options: every target accepts the same PEM, and with a PEM of
  one certificate they behave the same in the hermetic and integration suites.
- **Bucket names are validated to today's S3 rules**, so a legacy bucket with uppercase letters or
  underscores cannot be opened.

Keys:

- **Keys are restricted to what every backend can store.** They are validated with `Storage.isSafeKey`
  (prefixes with `isSafePrefix`) exactly as the local-filesystem backend validates them, and a refused
  key is `INVALID_KEY` with no request made: an empty key, a leading `/`, `..`, `.` and empty segments
  (`a//b`, a trailing `/`), a `.meta.json` suffix, control characters, `:`, `%25`, a segment ending in
  `.` or a space, and Windows device names (`CON`, `nul.txt`). S3 itself would accept most of these.
- **`list` can return keys that `get` refuses.** Listing passes on whatever the bucket holds; an object
  written by another tool under a key `isSafeKey` rejects is listed but cannot be read, deleted or
  presigned through this backend.
- **`list` is flat.** There is no delimiter: a prefix matches every key beneath it at any depth, in
  S3's key order, at most 1000 per page.

Metadata:

- **User metadata that S3 would alter is refused, not stored differently.** A key must be lowercase
  letters, digits, `-`, `_` or `.` (S3 lowercases keys), and a value printable ASCII without leading or
  trailing spaces (a header cannot carry anything else intact); otherwise `put` is
  `USER_META_UNSUPPORTED` and nothing is sent.
- **`put` does not read the object back.** The `StorageMetadata` it returns repeats the content type
  and user metadata it was given; `lastModified` is the server's `Date` header at the time of the
  upload (the local clock when there is none), which can differ from the `Last-Modified` a later `get`
  reports.
- **`lastModified` has two forms.** `get` and `stat` convert the `Last-Modified` header to ISO 8601
  with second resolution (`2026-11-12T10:15:00Z`); `list` passes on the listing's `LastModified` as S3
  wrote it, usually with milliseconds (`2026-11-12T10:15:00.000Z`). Do not compare them as text.
- **The ETag is S3's, not always an MD5.** It is the MD5 of the content for a plain single-part upload
  (what the local-filesystem backend also computes) but something else under SSE-KMS or on some
  compatible servers.
- **An object without a `Content-Type` reads as `application/octet-stream`.**

Errors:

- **Error codes come in two spellings.** Refusals made locally are upper snake case (`INVALID_KEY`);
  S3's own codes are passed on verbatim in S3's spelling (`AccessDenied`). Only `NoSuchKey` is mapped,
  to the interface's `NOT_FOUND`.
- **HEAD has no error body, so `exists` and `stat` know only the status.** Wrong credentials are
  `HTTP_403` there, where `get` says `SignatureDoesNotMatch` or `AccessDenied`. A 404 is "no such
  object" whatever the cause, so `exists` on a bucket that does not exist is `Ok(false)`, not
  `NoSuchBucket`.
- **`exists` is an `Err` when it cannot tell**, never `false`: a refused connection or a 403 is not
  "absent".
- **`delete` of a missing key is `Ok`**, as S3 defines.
- **Out-of-range arguments differ between targets.** An empty content type, `maxKeys` outside 1 to
  1000 and `expiresInSeconds` outside 1 to 604800 violate the interface's `requires:` clauses. On `jvm`
  and `native` a call through `StorageBucket` panics with `PreconditionViolated`; on `dotnet` the
  same call reaches the backend, which answers `Err(INVALID_ARGUMENT)` for the two ranges and does
  not check the content type. On `native` a panic cannot be caught (there is no unwinding): it aborts
  the process after printing the message.
  Validate before calling.
- **Failed requests are sent again, up to three attempts.** Throttling (`SlowDown`,
  `RequestLimitExceeded`, HTTP 429 and 503), server failures (HTTP 500, 502, 504, `InternalError`,
  `RequestTimeout`), a refused or cut connection and a timeout are retried; every other answer
  (`AccessDenied`, `NoSuchKey`, a 301) is returned at once. Put, get, head, delete and list are
  idempotent, so repeating one after an unknown outcome changes nothing. The error returned is the
  last attempt's. Each attempt has its own `requestTimeoutMs`, is signed again with the current
  date and the current credentials, and is preceded by a wait: up to 100 ms, then up to 200 ms
  (full jitter, doubling, 20 s at most), or what `Retry-After` / `x-amz-retry-after` asks for when
  that is longer. The worst case of one call is therefore three timeouts plus the waits.
  `withRetry` changes the policy and `withoutRetries()` restores one attempt.
- **`createBucket` is the exception**: a second CreateBucket would report "already owned" for the
  bucket the first one created, so it is sent again only after throttling or a connection that
  could not be opened, never after a 5xx, a cut connection or a timeout (`S3Client.sendUnrepeatable`).
- **A timeout does not undo the request.** A `put` that is `CONNECTION_FAILED` after its attempts
  timed out may still have been stored.
- **Clock skew is not corrected.** `RequestTimeTooSkewed` is returned, not retried.
- **A credential provider is asked before every attempt**, on the calling thread. An uncached
  provider that calls a service makes every request pay for that call; `Aws.Credentials.cached`
  avoids it. When the provider has nothing, the operation is `CREDENTIALS_UNAVAILABLE` and no
  request is made.
- **On `native` a credential provider that panics ends the process.** `dotnet` and `jvm` turn a
  panic in a provider into an error (`aws-credentials/README.md`); native has no unwinding. The
  providers of `Aws.Credentials` do not panic; this is about a `CredentialProvider` an application
  writes.
- **A request is not retried because its credentials expired.** `ExpiredToken` is returned; the
  cache replaces credentials by the clock, five minutes ahead of their expiry.

Presigned URLs:

- **A presigned URL is computed locally.** No request is made, the object need not exist, and wrong
  credentials produce a URL that S3 will refuse. The local-filesystem backend answers `NOT_SUPPORTED`
  instead.
- **It is signed with the local clock**, as every request is; a clock more than 15 minutes off is
  `RequestTimeTooSkewed`.
- **With temporary credentials the session token is part of the URL**, and the URL stops working when
  those credentials expire, whatever `expiresInSeconds` says. With a credential provider the URL is
  signed with the credentials current at that moment.
- **`S3Client.presign` returns a `Result`** (it was a plain `String`): resolving the credentials can
  fail (`CREDENTIALS_UNAVAILABLE`).
- **Only the host is signed.** A presigned PUT accepts any content type and any size.

Memory and transport:

- **Objects are held in memory whole, more than once.** `put` decodes the base64 payload to bytes and
  hashes them (the payload is signed with its SHA-256, not `UNSIGNED-PAYLOAD`); `get` downloads the
  bytes and encodes them to base64. Use `putBytes` / `getBytes` (Extras) to skip the base64 step. There
  is no streaming, multipart upload or range read.
- **A client is a value.** `S3Client` has no mutable state of its own; every request is signed and sent
  independently through one `Std.Http` client, so it can be shared between threads.

## Backend-specific module

`Storage.S3.Extras` holds what S3 offers beyond the interface. Its functions take an `S3Client`
(`openClient(cfg)`), validate keys like the interface methods, and move objects as raw bytes:

| Function | |
|---|---|
| `putBytes(client, key, bytes, contentType, userMeta)` | PutObject from a `slice[Byte]`, no base64 step; returns the ETag |
| `putBytesWithStorageClass(client, key, bytes, contentType, storageClass)` | PutObject with `x-amz-storage-class`. The name must be one `isStorageClass` knows (`STANDARD`, `REDUCED_REDUNDANCY`, `STANDARD_IA`, `ONEZONE_IA`, `INTELLIGENT_TIERING`, `GLACIER`, `DEEP_ARCHIVE`, `GLACIER_IR`, `EXPRESS_ONEZONE`), else `INVALID_ARGUMENT`. Takes no user metadata |
| `getBytes(client, key)` | GetObject as bytes plus `ObjectHead` |
| `stat(client, key)` | HeadObject: size, content type, ETag, user metadata, storage class (`STANDARD` when S3 names none); `None` when absent |
| `presignedPutUrl(client, key, expiresInSeconds)` | A URL that accepts one upload without credentials |
| `createBucket(client)` | CreateBucket, naming the region outside `us-east-1`. `Ok(true)` when created, `Ok(false)` when this account already owns it (AWS answers 200 for that in `us-east-1`, so it is `Ok(true)` there) |
| `bucketExists(client)` | HeadBucket: `Ok(false)` for 404, `Err(HTTP_403)` when the credentials may not use it |

The other packages are public for an operation this package does not wrap:

- `Storage.S3.Client`: `S3Config`, `S3Client` and its methods (`putObject`, `getObject`, `headObject`,
  `deleteObject`, `listObjects`, `presign`, `currentCredentials`), the signed-request primitives
  `S3Client.send` (retried as an idempotent request) and `S3Client.sendUnrepeatable`, `faultOf`,
  and the validators (`isValidBucketName`, `isValidRegion`, `isLoopbackAuthority`, `userMetaProblem`).
  These methods do not validate keys.
- `Storage.S3.SigV4`: re-exports `Aws.SigV4` (`Pair`, `Credentials`, `signRequest`, `presignQuery`,
  ...) from the `Lyric.Aws.SigV4` package and adds what is S3's own, `canonicalKeyPath`: an object key
  percent-encoded once, with `/` kept and no path normalisation.
- `Storage.S3.Wire`: parsing of `ListBucketResult` and `Error` documents, ETags and HTTP dates.

## Tests

Run from the repository root:

```sh
lyric test --manifest storage-s3/lyric.toml --target dotnet
lyric test --manifest storage-s3/lyric.toml --target jvm
lyric test --manifest storage-s3/lyric.toml --target native
```

The native run needs clang and lyric-rt (`make -C ../lyric-lang/lyric-rt`), compiles the package and
its dependencies from source into one executable per test module, and has nothing to restore;
`bash scripts/ci/test-native.sh` does it for every package that claims `native`.

The default run is hermetic and needs no network (31 tests on every target):

- `tests/sigv4_tests.l`: what is S3's own in the signing: an object key encoded once with `/` kept,
  and two worked examples of the Amazon S3 API reference (PUT Object with a storage class and an
  encoded key, a presigned GET URL). The signing algorithm itself is tested in `aws-sigv4/`.
- `tests/wire_tests.l`: `ListBucketResult` and `Error` documents, ETags, HTTP dates, url-encoded
  listing keys.
- `tests/fake_s3_tests.l`: `Testing.Conformance.Storage` and the backend's own behaviour over real
  HTTP against an S3 server written in Lyric on `Std.HttpServer` inside the test. The server
  verifies the SigV4 signature of every request (header and presigned-URL forms) with the shared
  secret and the hash of the body it received, keeps objects in memory, and pages ListObjectsV2.
  The same server is also started over HTTPS (`Std.HttpServer.startListenerTls`, every target) with
  a fixture certificate from a private CA: a client given that CA with `withCaCertificate` or
  `withExclusiveCaCertificate` stores and reads an object, and a client without it, or with another
  CA, gets `CONNECTION_FAILED` and never reaches the server. The certificates and the key in the
  file are labelled test fixtures. It listens on the loopback ports 19471 to 19477, which must be
  free.
  Retries and credentials (ports 19531 to 19533): keys that answer `SlowDown` twice and then
  succeed, once with `Retry-After`, a 500 once, `SlowDown` always, `AccessDenied`; the server counts
  the attempts, and the policy's sleep and random source are injected, so the waits are asserted
  exactly (100 ms, 200 ms; 1000 ms for `Retry-After: 1`) and nothing sleeps. CreateBucket is shown
  not to be repeated after a 500 and to be repeated after throttling. A provider whose credentials
  change when an injected clock reaches the refresh window: the server accepts a second, temporary
  key and reports which key signed the last request, so the test sees the request after the
  refresh signed with the new key and its session token. `configFromEnvironment` with an
  environment made of a map: environment keys, a profile's keys, region, endpoint and CA bundle,
  an SSO profile refused; and a bucket configured from a profile storing and reading an object.
- `tests/readme_tests.l`: the credential provider and retry examples of this README.

Integration tests run against a real S3-compatible server and are compiled only with
`--features integration`:

```sh
eval "$(bash scripts/ci/start-minio.sh)" && export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY
LYRIC_S3_ENDPOINT=http://127.0.0.1:9000 LYRIC_S3_BUCKET=lyric-it LYRIC_S3_REGION=us-east-1 \
  lyric test --manifest storage-s3/lyric.toml --target dotnet --features integration
```

`scripts/ci/start-minio.sh` builds the pinned MinIO with the Go toolchain into `$MINIO_BIN_DIR`
(default `~/.cache/lyric-ci/minio`, cached by CI), starts a single node on 127.0.0.1:9000 with a
random secret, and prints `AWS_ACCESS_KEY_ID=...` and `AWS_SECRET_ACCESS_KEY=...` lines, or appends
them to `$GITHUB_ENV` under GitHub Actions.

| Variable | |
|---|---|
| `LYRIC_S3_ENDPOINT` | base URL of the server; `aws` selects AWS S3 itself, virtual-hosted |
| `LYRIC_S3_BUCKET` | a disposable bucket; created when missing. **Every object in it is deleted**, repeatedly |
| `LYRIC_S3_REGION` | the region to sign for (`us-east-1` for a default MinIO) |
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` | credentials that may create and use the bucket |
| `AWS_SESSION_TOKEN` | optional |
| `LYRIC_S3_CA_FILE` | optional: a PEM file with a certificate authority, passed to `withCaCertificateFile` (and trusted by the test's own client for presigned URLs) |

A missing variable fails every integration test with a message naming it. The suite runs the
conformance suite, binary objects with encoded keys and metadata, HEAD, presigned GET and PUT
fetched by a client without credentials, and S3's own error codes, on every target. On `native`,
replace `--target dotnet` with `--target native`, or run `bash scripts/ci/test-native.sh
--integration` with the same variables (it also runs the other packages that claim `native`, so
it needs their services too).

## Pinned versions

| | |
|---|---|
| SDK / native libraries | none |
| `Lyric.Aws.SigV4`, `Lyric.Aws.Credentials` | the versions in this repository (`aws-sigv4/`, `aws-credentials/`) |
| `Lyric.Storage`, `Lyric.Conformance`, standard library | the `lyric-lang` commit in `.lyric-lang-rev`, through the root `[workspace.overrides]`. The package needs `Std.Http.withBytesBody`, `HttpResponse.headerNames` and (for the tests) `Std.HttpServer.requestBodyBytes`, added in nichobbs/lyric-lang#9047 and not in a release yet |
| MinIO (integration suite) | `github.com/minio/minio@v0.0.0-20260212201848-7aac2a2c5b7c`, single node, built from that module version by `scripts/ci/start-minio.sh` |

The suite also runs against [AWS Emulator](https://github.com/aws/aws-emulator) 1.0.341, AWS's own
local emulator, in the `aws-emulator` CI job (all four integration tests, every target):

```sh
set -a; eval "$(bash scripts/ci/start-aws-emulator.sh)"; set +a
bash scripts/ci/test-aws-emulator.sh dotnet     # or jvm, native; runs aws-credentials, mq-sqs and storage-s3
```

It verifies signatures against a key generated at start, takes the bucket in the path only, and
follows Amazon where MinIO differs: creating a bucket the account already owns is a 200 in
`us-east-1`, so `createBucket` returns `Ok(true)` there. It serves HTTPS with a certificate made at
start, from its own CA. `test-aws-emulator.sh` passes that CA as `LYRIC_S3_CA_FILE`, so the suite
trusts it through `withCaCertificateFile` and nothing process-wide is changed (no `SSL_CERT_FILE`,
no JVM trust store): this run is the check of the CA option against a real HTTPS server with a
private certificate, on every target.

The suite has not been run against AWS S3 itself; `LYRIC_S3_ENDPOINT=aws` is there to do so by
hand.

## Known gaps

- **`native` differs from the managed targets in three places**, all described under Surprises: a
  panic ends the process, whether it comes from a credential provider an application wrote or from
  a violated `requires:` clause of the interface; `withCaCertificate` /
  `withExclusiveCaCertificate` (and so `AWS_CA_BUNDLE` / `ca_bundle`) trust every certificate of a
  PEM text with several, where `dotnet` and `jvm` trust the first only (a difference in
  `Std.Tls`, nichobbs/lyric-lang#9146); and the text of a `CONNECTION_FAILED` message is the
  standard library's and OpenSSL's. `native` has been built and run on Linux x86-64 only, and a
  native program that talks HTTPS needs OpenSSL 3 (`libssl.so.3`) at run time. It needs a
  `lyric-lang` with the fix of nichobbs/lyric-lang#9136, which the commit in `.lyric-lang-rev` has.
- Objects are held in memory whole, as the interface's base64 payloads require: no multipart
  upload, no streaming, no range reads. `Std.Http` buffers bodies too.
- No server-side encryption headers, object tagging, versioning, ACLs, or batch delete. `list` has no
  delimiter.
- No IAM Identity Center (SSO) credentials and no `role_arn` profiles; see the Known gaps of
  `aws-credentials/README.md`. The container, instance metadata and web identity providers have
  been run against in-process fakes only, not on ECS, EKS or EC2.
- Retries have no retry quota and no clock-skew correction, and a request refused with
  `ExpiredToken` is not sent again with fresh credentials.
- A `ca_bundle` / `AWS_CA_BUNDLE` file holding more than one certificate is untested through
  `configFromEnv`; the hermetic suite covers the same PEM text given to `withCaCertificate`.
- A presigned URL is signed with the local clock, as every request is; a clock more than 15
  minutes off is `RequestTimeTooSkewed`.
- The interface's `requires:` clauses are enforced on `jvm` and `native` and not on `dotnet` for
  this backend (see Surprises). No lyric-lang issue number yet.
- In the hermetic tests the fake server answers HEAD with `Content-Length: 0`, because
  `Std.HttpServer` cannot send a HEAD response that reports an entity's length. `stat`'s
  `contentLength` is therefore asserted only by the integration tests.
- JVM compiler defect found while writing the backend (not worked around by a conversion in
  library code; the request body is carried in a `Payload` record instead of a bare parameter):
  in a library package, a `slice[Byte]` returned by a stdlib call and passed straight to another
  Lyric function's `in slice[Byte]` parameter fails class verification
  (`'[B' is not assignable to '[Ljava/lang/Object;'`). Described in
  `lyric-lang/docs/progress/2026-11-12-http-bytes-body.md` (nichobbs/lyric-lang#9049).
