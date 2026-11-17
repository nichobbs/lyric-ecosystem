# lyric-aws-secrets

AWS Secrets Manager and Parameter Store integration for [Lyric](https://github.com/nichobbs/lyric-lang), for `dotnet`, `jvm` and `native`, `experimental` (0.1.0). Fetches secrets at application startup and injects them into `config { }` blocks, with TTL-based caching and an environment-variable override for local development.

The client is written in Lyric: the AWS JSON 1.1 protocol over `Std.Http`, signed with AWS Signature Version 4 by `Lyric.Aws.SigV4`, with credentials from `Lyric.Aws.Credentials` (environment, web identity, shared files, container endpoint, instance metadata). There is no AWS SDK, no `[nuget]`/`[maven]` dependency and no extern declaration. The `@secretsManager` and `@parameterStore` annotations and `Std.AnnotationMeta` stay in `lyric-lang`.

| | |
|---|---|
| Package | `Lyric.AwsSecrets` (Lyric packages `AwsSecrets`, `AwsSecrets.Client`, `AwsSecrets.Fetch`) |
| Targets | `dotnet`, `jvm`, `native` (Linux x86-64; clang and lyric-rt to build, OpenSSL 3 at run time for HTTPS). The annotation path (`init()`) needs annotation metadata, which native does not embed (lyric-lang#8524): see "Targets" |
| Stability | `experimental` (0.1.0) |
| Services | Secrets Manager (`GetSecretValue`), Systems Manager Parameter Store (`GetParameter`) |
| Dependencies | `Lyric.Aws.SigV4`, `Lyric.Aws.Credentials` (`{ workspace = true }`) |

## Installation

Inside this workspace a package declares `"Lyric.AwsSecrets" = { workspace = true }` under `[dependencies]`.

## Targets

| Target | Explicit API (`getSecret`, ...) | `init()` and the annotations |
|---|---|---|
| `dotnet` | yes | yes |
| `jvm` | yes | yes |
| `native` | yes | no: `init()` returns `InvalidConfig` (field `Lyric.AnnotationMeta`), because the native target embeds no annotation metadata (lyric-lang#8524). `registerSecrets` and `registerAnnotated` are not declared there |

## Configuration

Read as the AWS SDKs read it, through `Aws.Credentials`:

| Setting | Source |
|---|---|
| credentials | the default chain: `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`/`AWS_SESSION_TOKEN`, web identity (`AWS_WEB_IDENTITY_TOKEN_FILE`), the shared files (`AWS_PROFILE`), the container endpoint, the EC2 instance metadata service |
| region | `AWS_REGION`, `AWS_DEFAULT_REGION`, the profile's `region` |
| endpoint | `AWS_ENDPOINT_URL_SECRETS_MANAGER` / `AWS_ENDPOINT_URL_SSM`, `AWS_ENDPOINT_URL`, the profile's `endpoint_url`; `https://` unless the host is loopback |
| CA bundle | `AWS_CA_BUNDLE` or the profile's `ca_bundle` (trusted in addition to the system store) |
| cache TTL | `LYRIC_CONFIG_AWSSECRETS_SECRETCACHE_TTLSECONDS` (default 300) |

No credentials or region are needed until a value has to be fetched: a field whose `LYRIC_CONFIG_...` variable is set is not fetched. When they are missing the error is `InvalidConfig` naming the secret.

A request is signed afresh for each attempt and retried (`Aws.Retry`, three attempts) on throttling, 5xx and transport failures. Error classification uses the service's error code (`ResourceNotFoundException`, `ParameterNotFound`, `AccessDeniedException`, `DecryptionFailure`, `InvalidKeyId`, ...), not message text.

## Quick start

```lyric
import Std.Core
import AwsSecrets

config Database {
  host: String = "localhost"
  @sensitive
  @secretsManager("my-service/prod", key = "dbPassword")
  password: String
}

func connectionPassword(): Result[String, AwsSecrets.SecretsError] {
  AwsSecrets.init()?
  Ok(Database.password)
}

func oneOff(): Result[String, AwsSecrets.SecretsError] {
  AwsSecrets.getParameter("/my-service/signing-key")
}
```

`init()` must run before the first read of any annotated config block: a block is initialised once, on first access, and a value registered afterwards is not seen by it. This example is compiled by `tests/readme_tests.l`.

## Annotations

### `@secretsManager(name)`

Fetch an entire secret value as a plain string:

```lyric
@secretsManager("my-service/api-key")
apiKey: String
```

Fetches the secret named `"my-service/api-key"` and stores it in `apiKey`.

### `@secretsManager(name, key = "field")`

Extract a JSON field from a structured secret:

```lyric
@secretsManager("my-service/prod", key = "dbPassword")
password: String
```

Fetches the secret `"my-service/prod"`, parses it as JSON, extracts the field `"dbPassword"`, and stores it.

### `@parameterStore("/path")`

Fetch a Parameter Store String or SecureString:

```lyric
@parameterStore("/my-service/signing-key")
signingKey: String
```

Fetches the parameter at `/my-service/signing-key`. SecureString parameters are automatically decrypted by the service.

## Caching

Fetched values are cached in process memory with a configurable TTL (default 300 seconds = 5 minutes).

On a warm Lambda invocation or repeated requests, the cached value is returned without a request.

### Cache configuration

```bash
export LYRIC_CONFIG_AWSSECRETS_SECRETCACHE_TTLSECONDS=300  # default
```

| Config field | Type | Default | Description |
|---|---|---|---|
| `ttlSeconds` | `Int` | `300` | Cache time-to-live in seconds; `0` = disable caching |

### Cache rotation strategy

For secrets that rotate, choose `ttlSeconds < rotationPeriodSeconds / 6`:

```
If rotation period = 30 days (2,592,000 seconds):
  ttlSeconds should be < 432,000 seconds (5 days)
  Recommended: ttlSeconds = 300 (5 minutes) — safe margin
```

This ensures stale values are refreshed well before the rotation window closes.

## Environment variable override

If the corresponding `LYRIC_CONFIG_<PACKAGE>_<BLOCK>_<FIELD>` env var is set, `init()` skips the AWS fetch and uses the env var value instead.

A fetched value is held in `Std.ConfigOverride`, not in the process environment,
so child processes do not inherit it.

This enables **local development without AWS credentials**:

```bash
# Local development: no requests
export LYRIC_CONFIG_DATABASE_PASSWORD="local-password"
export LYRIC_CONFIG_AUTH_JWTSECRET="local-secret"
./my-service
```

Missing env vars that don't have annotations produce a config error at startup, which surfaces incomplete local setups explicitly.

## Explicit fetch API

For one-off secret retrievals outside config blocks:

### `getSecret(name)`

Fetch an entire secret value:

```lyric
import AwsSecrets

match AwsSecrets.getSecret("my-service/api-key") {
  case Ok(value)     -> // use value
  case Err(SecretsError.NotFound) -> // secret not found
  case Err(SecretsError.AccessDenied) -> // IAM permission denied
  case Err(err)      -> // other error
}
```

### `getSecretField(name, field)`

Extract a JSON field from a structured secret:

```lyric
import AwsSecrets

match AwsSecrets.getSecretField("my-service/prod", "apiKey") {
  case Ok(value)     -> // use value
  case Err(err)      -> // error
}
```

### `getParameter(path)`

Fetch a Parameter Store value:

```lyric
import AwsSecrets

match AwsSecrets.getParameter("/my-service/signing-key") {
  case Ok(value)     -> // use value
  case Err(err)      -> // error
}
```

## Error handling

### `SecretsError`

```lyric
union SecretsError {
  case NotFound(name: String)
  case AccessDenied(name: String, message: String)
  case DecryptionError(name: String, message: String)
  case ParseError(name: String, key: String, message: String)
  case NetworkError(name: String, message: String)
  case BinaryValueUnsupported(name: String)
  case InvalidConfig(field: String, message: String)
}
```

| Error | Meaning | Action |
|---|---|---|
| `NotFound` | Secret or parameter does not exist, or is not accessible with the current IAM role | Check the name/path |
| `AccessDenied` | IAM role lacks permission | Grant `secretsmanager:GetSecretValue` or `ssm:GetParameter` |
| `DecryptionError` | KMS decryption of a SecureString/secret failed | Verify KMS permissions |
| `ParseError` | Secret is JSON but the requested key is absent or the value is not valid JSON | Check the secret's JSON shape and key name |
| `NetworkError` | A transient network/service error (throttling, a 5xx, a refused connection), or any service error code that is not one of the others | Retry, or read `errorMessage` for detail |
| `InvalidConfig` | `SecretCache.ttlSeconds` (from `LYRIC_CONFIG_AWSSECRETS_SECRETCACHE_TTLSECONDS`) is outside 0..86400, returned by `init()` and every get call without fetching; or, from `init()`, an annotation with no secret name/path, a field with both annotations, or an unregisterable value | Fix the env var or the annotation |
| `BinaryValueUnsupported` | The secret is stored as raw binary (Secrets Manager's `SecretBinary`) rather than a string (`SecretString`) — this API only supports string-valued secrets | Store the value as a `SecretString` instead, or base64-encode it as one |

Classification is by the service's error code (`AwsSecrets.Fetch.classifySecretsFault` and `classifyParameterFault`). Throttling, server faults, an unrecognised code and transport failures are `NetworkError`; a request the service rejects (`InvalidRequestException`, for example a secret marked for deletion) and missing credentials or region are `InvalidConfig`.

### `errorMessage(err)`

Get a human-readable error message:

```lyric
import AwsSecrets

match AwsSecrets.init() {
  case Err(err) -> {
    Std.Core.log("ERROR", AwsSecrets.errorMessage(err))
  }
  case Ok(_)    -> {}
}
```

## IAM permissions

The Lambda execution role must have the following permissions:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "secretsmanager:GetSecretValue"
      ],
      "Resource": [
        "arn:aws:secretsmanager:REGION:ACCOUNT_ID:secret:my-service/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "ssm:GetParameter"
      ],
      "Resource": [
        "arn:aws:ssm:REGION:ACCOUNT_ID:parameter/my-service/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "kms:Decrypt"
      ],
      "Resource": [
        "arn:aws:kms:REGION:ACCOUNT_ID:key/*"
      ]
    }
  ]
}
```

## Known gaps

- The annotation path is not available on native (lyric-lang#8524).
- Only string-valued secrets: a binary secret (`SecretBinary`) is `BinaryValueUnsupported`.
- `GetSecretValue` always asks for the current version (`AWSCURRENT`); there is no version-stage or version-id argument.
- The environment-driven functions build a client (and read the shared files when a setting needs them) on each uncached fetch; a refreshing credential provider is not kept between fetches. Environment credentials, the Lambda case, are unaffected. Use `AwsSecrets.Fetch.fetchSecretWith` with one `AwsClient` to share a provider.
- The two services are tested against the AWS Emulator (1.0.341), not against AWS itself.

## Migration from the lyric-lang copy

- Same package (`Lyric.AwsSecrets`), imports (`AwsSecrets`) and public API. The `local`, `aws` and `jvm` features are gone: there is one implementation. The default build used to be `local`, where `init()` did nothing and every fetch was `NotFound`; set the `LYRIC_CONFIG_...` variable of a field to skip its fetch.
- `AwsSecrets.Kernel.Net` is now `AwsSecrets.Fetch` (internal). Its message-substring `classifySecretsError` and `classifyParameterError` are replaced by `classifySecretsFault` and `classifyParameterFault`, which take the service error code.
- `init()` without credentials is `InvalidConfig` where the SDK versions returned `NetworkError`.
- `Std.Json` is no longer used (`Std.JsonValue` is), so the explicit API works on native.

## Testing

Hermetic, on each target (`dotnet`, `jvm`, `native`):

```sh
lyric test --manifest aws-secrets/lyric.toml --target dotnet
```

- `tests/secrets_tests.l`: errors, TTL check, classification by code, field extraction, cache keys.
- `tests/annotation_tests.l`: `registerSecrets` and `init()`'s skip path (not on native).
- `tests/client_tests.l`: endpoint, region, error documents, configuration validation.
- `tests/fake_aws_tests.l`: the package over real HTTP against an in-process Secrets Manager and Parameter Store on `Std.HttpServer` that verifies every SigV4 signature, with the real services' error documents, retries on throttling, the cache, the environment-driven API and `init()` end to end.

Against the AWS Emulator (verifies signatures; `tests/integration/secrets_integration_tests.l`, feature `emulator`):

```sh
bash scripts/ci/start-aws-emulator.sh          # prints the variables below
LYRIC_SECRETS_ENDPOINT=https://127.0.0.1:9297 LYRIC_SECRETS_REGION=us-east-1 \
AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... LYRIC_SECRETS_CA_FILE=~/.aws/emulator/9297/ca.pem \
  lyric test --manifest aws-secrets/lyric.toml --target jvm --features emulator
```

CI runs it with `bash scripts/ci/test-aws-emulator.sh <target>`. The emulator supports Secrets Manager `CreateSecret` (with a `ClientRequestToken`), `GetSecretValue`, `DeleteSecret` and Parameter Store `PutParameter`, `GetParameter`, `DeleteParameter`.

## Package layout

```
aws-secrets/
  lyric.toml
  README.md
  src/
    secrets.l   AwsSecrets         annotations, init, explicit fetch API, errors
    client.l    AwsSecrets.Client  signed AWS JSON 1.1 client over Std.Http
    fetch.l     AwsSecrets.Fetch   fetching, classification, TTL cache
  tests/
```

## See also

- `aws-lambda` -- Lambda runtime adapter; use with `AwsSecrets.init()`
- `aws-xray` -- AWS X-Ray active tracing
- `aws-credentials`, `aws-sigv4` -- the credential providers and signing this package uses
