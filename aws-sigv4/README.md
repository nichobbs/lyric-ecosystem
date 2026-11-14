# lyric-aws-sigv4

AWS Signature Version 4 in pure Lyric, for `dotnet`, `jvm` and `native`, `experimental` (0.1.0). It is the
signing code the AWS backends of this repository share (`storage-s3`, `mq-sqs`); it implements no
library interface of its own. It depends on the standard library only: SHA-256 and HMAC-SHA256 come
from `Std.Hash`, and there is no AWS SDK and no extern declaration.

| | |
|---|---|
| Package | `Lyric.Aws.SigV4` (Lyric packages `Aws.SigV4`, `Aws.SigV4.Env`) |
| Interface | none: a shared building block |
| Targets | `dotnet`, `jvm`, `native` |
| Stability | `experimental` (0.1.0) |
| Algorithm | `AWS4-HMAC-SHA256`, header signing and query-string presigning |
| Dependencies | the standard library. No SDK, no `[nuget]`, no `[maven]`, no native dependency |

## Usage

Inside this workspace a package declares `"Lyric.Aws.SigV4" = { workspace = true }` under
`[dependencies]`.

Signing a request for a JSON-protocol service (this is what `mq-sqs` does for every call):

```lyric
import Std.Core
import Std.Collections
import Std.Encoding
import Aws.SigV4
import Aws.SigV4.Env

func signedSendMessage(body: in String): Result[SignedRequest, String] {
  val creds = credentialsFromEnv()?
  val region = regionFromEnv()?
  val extra: List[Pair] = newList()
  extra.add(Pair(name = "content-type", value = "application/x-amz-json-1.0"))
  extra.add(Pair(name = "x-amz-target", value = "AmazonSQS.SendMessage"))
  val noQuery: List[Pair] = newList()
  Ok(
    signedHeaders(
      "POST",
      canonicalUri("/", false),
      noQuery,
      "sqs." + region + ".amazonaws.com",
      extra,
      sha256Hex(encodeUtf8(body)),
      false,
      creds,
      region,
      "sqs",
      amzDateNow()
    )
  )
}
```

`SignedRequest.headers` is every header to set on the request, in order, `Authorization` last;
`SignedRequest.signed` holds the canonical request, the string to sign and the signature, for
logging or tests.

A presigned URL (S3):

```lyric
func presignedGet(creds: in Credentials, bucket: in String, key: in String): String {
  val host = bucket + ".s3.eu-west-1.amazonaws.com"
  val path = canonicalUri("/" + key, true)
  val noQuery: List[Pair] = newList()
  "https://" + host + path + "?" + presignQuery("GET", host, path, noQuery, creds, "eu-west-1", "s3", amzDateNow(), 900)
}
```

Both examples are compiled and run by `tests/readme_tests.l`.

## What is in it

`Aws.SigV4` is pure (`@pure` functions over strings and bytes; nothing reads the clock, the
environment or the network):

| Function | What it is |
|---|---|
| `uriEncode(text, keepSlash)` | RFC 3986 percent-encoding as SigV4 wants it: everything but `A-Z a-z 0-9 - . _ ~`, uppercase hex, space as `%20`; `/` kept when asked |
| `canonicalUri(path, s3Style)` | the canonical URI of an unencoded absolute path. `s3Style = true`: encoded once, `/` kept, nothing normalised. `false`: `.`, `..` and empty segments removed, each segment encoded twice |
| `encodedPath(path)` | the path as a request line carries it: encoded once, `/` kept |
| `canonicalQuery(params)` | the canonical query string: encoded, sorted by name then value |
| `signedHeaderNames(headers)` | the `SignedHeaders` value |
| `canonicalRequest(...)` | task 1 |
| `credentialScope(amzDate, region, service)` | `<yyyymmdd>/<region>/<service>/aws4_request` |
| `stringToSign(...)` | task 2 |
| `signingKey(secret, amzDate, region, service)` | the derived key |
| `signatureOf(...)` | task 3: the lowercase hex signature |
| `signRequest(...)` | all three for a request signed in its `Authorization` header; the caller supplies exactly the headers to sign |
| `signedHeaders(...)` | `signRequest` plus the header list: `host`, `x-amz-date`, `x-amz-content-sha256` when asked (S3), `x-amz-security-token` when the credentials hold a session token, and the caller's extra headers |
| `presignQuery(...)` | the query string of a presigned URL, `X-Amz-Signature` included; signs `host` only, payload `UNSIGNED-PAYLOAD`, session token included when there is one |
| `sha256Hex(bytes)`, `emptyPayloadSha256()`, `unsignedPayload()`, `algorithm()` | payload hashes and constants |
| `amzDateOfIso(iso)` | `2026-11-12T10:15:00.000Z` to `20261112T101500Z` |

Records: `Pair` (a header or query parameter), `Credentials`, `Signed`, `SignedRequest`.

`Aws.SigV4.Env` (`@io`) is what a signer takes from its surroundings:

| Function | What it reads |
|---|---|
| `credentialsFromEnv()` | `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, optional `AWS_SESSION_TOKEN`; `Err("<variable> is not set")` for the first required one that is missing |
| `credentialsOf(id, secret, token)` | explicit values, refused when the id or the secret is empty |
| `regionFromEnv()` | `AWS_REGION`, else `AWS_DEFAULT_REGION` |
| `endpointFromEnv(service)` | `AWS_ENDPOINT_URL_<SERVICE>`, else `AWS_ENDPOINT_URL`, else `""` |
| `firstSet(names)` | the first variable of a list that is set and non-empty |
| `amzDateNow()` | the current instant as `yyyymmddThhmmssZ` |

## Configuration

There is nothing to configure in the package itself. The environment variables `Aws.SigV4.Env`
reads are the ones in the table above; a variable that is set to the empty string counts as unset.

## Supported targets

`dotnet`, `jvm` and `native`. All three run the same code; there is no per-target kernel.

## Surprises

- **The canonical URI of a non-S3 service is encoded twice.** `canonicalUri("/a b", false)` is
  `/a%2520b`: the request line carries `/a%20b` (`encodedPath`) and the signature covers the
  encoding of that. For S3 (`s3Style = true`) both are `/a%20b`. Passing an already-encoded path
  encodes it again; pass the unencoded path.
- **Only S3 leaves the path alone.** `canonicalUri("/a//b/../c", false)` is `/a/c`. With
  `s3Style = true` it is `/a//b/../c`, because `a//b` and `a/../b` are distinct object keys.
- **`signedHeaders` does not return a `Host` header.** `host` is signed, but an HTTP client sets
  `Host` from the URL; what it sends must be the `host` you passed (no default port, lowercase).
- **`x-amz-content-sha256` is sent only when asked.** S3 requires it; other services sign the
  payload hash without sending it.
- **Header names are lowercased, values trimmed and inner runs of spaces collapsed** in the
  canonical request, as the specification says. The header you send must still carry a value the
  server canonicalises to the same thing.
- **Credentials come from the environment or from explicit values only.** No shared credentials
  file, profile, SSO, container or instance metadata endpoint, and no `AssumeRole`.
- **The secret is never part of an error message**; `Credentials` is a plain record, though, and
  printing one prints the secret.
- **Constants are functions** (`unsignedPayload()`, `algorithm()`, `emptyPayloadSha256()`), not
  `pub val`s; see "Known gaps".
- **Chunked (streaming) payload signing and SigV4A are not implemented.**

There is no difference between the targets.

## Tests

```sh
lyric test --manifest aws-sigv4/lyric.toml --target dotnet
lyric test --manifest aws-sigv4/lyric.toml --target jvm
```

`lyric test --manifest` does not run on `--target native` yet (nichobbs/lyric-lang#6815): there the
same three test modules are built as executables, each with the package's own packages
(`lyric build --manifest ... --target native`), and run, by `bash scripts/ci/test-native.sh` here and
in the `native` CI job; all 25 tests pass that way.

Everything is hermetic; there is no integration suite, because nothing here talks to a service
(the packages that use it sign real requests: `storage-s3` and `mq-sqs` each run an in-process
server that verifies the signature of every request, and their integration suites).

- `tests/sigv4_tests.l` (19 tests): the worked examples Amazon publishes. S3: GET Object with a
  `Range` header, PUT Object, GET Bucket Lifecycle, List Objects and the presigned URL of the S3 API
  reference. Other services: the `GET iam ListUsers` walk-through of the IAM User Guide (canonical
  request, its hash, the signing key and the signature), five cases of the AWS Signature Version 4
  test suite (`get-vanilla`, `post-vanilla`, `get-vanilla-query-order-key-case`,
  `post-x-www-form-urlencoded`, `get-header-value-trim`, `post-sts-header-after` for the session
  token) and its path normalisation cases, and an Amazon SQS JSON 1.0 `SendMessage` POST with
  long-term and temporary credentials. Amazon publishes no signature for the SQS request; its
  expected values, and every other expected value, were recomputed with an independent
  implementation (Python's `hmac` and `hashlib`) before they were written into the test.
- `tests/env_tests.l` (5 tests): credentials, region and endpoint from the environment.
- `tests/readme_tests.l` (1 test): the examples above.

## Pinned versions

| Component | Version |
|---|---|
| SDK or library dependencies | none |
| Compiler | the `lyric-lang` commit in `.lyric-lang-rev` |

## Known gaps

- lyric-lang#9043: a `pub val` of a package that is linked as a built workspace dependency does not
  resolve in the dependent package (`T0020: unknown name`), so the constants are `pub func`s. A
  minimal reproduction is two packages, one with `pub val GREETING: String = "hello"` and one that
  imports it and names `GREETING`.
- Streaming (`aws-chunked`) payload signing, SigV4A (multi-region) signing and event-stream signing
  are not implemented; no issue is filed, nothing in this repository needs them yet.
- Credential sources other than the environment and explicit values (profiles, IMDS, ECS, SSO,
  STS) are not implemented; no issue is filed.
