# lyric-aws-credentials

Where AWS credentials come from, and when a failed AWS request is sent again, in pure Lyric, for
`dotnet` and `jvm`, `experimental` (0.1.0). It is what the AWS backends of this repository share
(`storage-s3`, `mq-sqs`) on top of `aws-sigv4`; it implements no library interface of `lyric-lang`.
There is no AWS SDK and no extern declaration: files are read with `Std.File`, services are called
with `Std.Http`, commands are run with `Std.Process`.

| | |
|---|---|
| Package | `Lyric.Aws.Credentials` (Lyric packages `Aws.Credentials`, `Aws.Credentials.Ini`, `Aws.Credentials.Profile`, `Aws.Credentials.Remote`, `Aws.Credentials.Chain`, `Aws.Retry`) |
| Interface | none: a shared building block. It defines `CredentialProvider` |
| Targets | `dotnet`, `jvm` |
| Stability | `experimental` (0.1.0) |
| Dependencies | `Lyric.Aws.SigV4` and the standard library. No SDK, no `[nuget]`, no `[maven]`, no native dependency |

## Usage

Inside this workspace a package declares `"Lyric.Aws.Credentials" = { workspace = true }` under
`[dependencies]`.

The credentials an AWS client signs with, from the default chain behind a cache:

```lyric
import Std.Core
import Std.Collections
import Aws.SigV4
import Aws.Credentials
import Aws.Credentials.Chain
import Aws.Credentials.Profile
import Aws.Credentials.Remote
import Aws.Retry

func signingCredentials(provider: in CredentialProvider): Result[Credentials, String] {
  match provider.resolve() {
    case Ok(resolved) -> Ok(resolved.credentials)
    case Err(e) -> Err(e.code + ": " + e.text())
  }
}

func example(): Result[Credentials, String] {
  val provider = defaultProvider()
  signingCredentials(provider)
}
```

Keep the provider and call `resolve()` before every request: the cache answers from memory until
the credentials are five minutes from expiry, and then refreshes them.

A chain of your own:

```lyric
func profileThenInstance(profile: in String): CredentialProvider {
  val files = sharedFilesFromEnvironment(processEnvironment()).copy(profile = profile, profileIsExplicit = true)
  val providers: List[CredentialProvider] = newList()
  providers.add(profileProviderOf(files))
  providers.add(imdsProvider(processEnvironment()))
  cached(chainOf(providers))
}
```

Retrying a request:

```lyric
func withRetries(send: in () -> Int, idempotent: in Bool): Int {
  val policy = standardRetry()
  var attempt = 1
  while true {
    val status = send()
    match policy.decide(attempt, classifyResponse(status, ""), idempotent, -1) {
      case GiveUp -> return status
      case RetryAfter(delayMs) -> policy.pause(delayMs)
    }
    attempt = attempt + 1
  }
  0
}
```

The three examples are compiled and run by `tests/readme_tests.l`. `storage-s3` and `mq-sqs` take a
provider with `withCredentialProvider` and a policy with `withRetry`.

## Credential providers

`CredentialProvider` has one method, `resolve(): Result[ResolvedCredentials, CredentialError]`.
`ResolvedCredentials` is the `Aws.SigV4.Credentials`, `expiresAtEpochMs` (`None` for long-term
credentials) and `source`, the name of the provider. A provider does no caching: every `resolve`
reads the variables or files, or calls the service, again.

Every provider that reads the environment takes a lookup, `(String) -> String`, that returns `""`
for an unset variable: `processEnvironment()` for the real one, `environmentOf(map)` for a test or
for an application that keeps its AWS settings elsewhere.

| Provider | Constructor | `source` | What it reads |
|---|---|---|---|
| Static | `staticProvider(id, secret, token)`, `staticProviderOf(credentials)` | `static` | the values given |
| Environment | `environmentProvider(lookup)` | `environment` | `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN` |
| Shared files | `profileProvider(lookup)`, `profileProviderOf(files)` | `profile`, `credential_process` | the profile of `~/.aws/config` and `~/.aws/credentials` |
| Container | `containerProvider(lookup)`, `containerProviderOf(cfg)` | `container` | the ECS or EKS Pod Identity credential endpoint |
| Instance metadata | `imdsProvider(lookup)`, `imdsProviderOf(cfg)` | `imds` | the EC2 instance role, by IMDSv2 |
| Web identity | `webIdentityProvider(lookup)`, `webIdentityProviderOf(cfg)` | `web-identity` | STS `AssumeRoleWithWebIdentity` (EKS IRSA, GitHub OIDC) |
| Chain | `chainOf(providers)` | the answering provider's | the providers in order |
| Default chain | `defaultChain(lookup)`, `defaultProvider()` | the answering provider's | environment, web identity, shared files, container, instance metadata |
| Cache | `cached(provider)`, `cachedWith(provider, options)` | the wrapped provider's | what the wrapped provider returned |

### Shared files (`Aws.Credentials.Profile`, `Aws.Credentials.Ini`)

- The config file is `AWS_CONFIG_FILE`, else `~/.aws/config`; the credentials file is
  `AWS_SHARED_CREDENTIALS_FILE`, else `~/.aws/credentials`; the profile is `AWS_PROFILE`, else
  `default`. The home directory is `HOME`, else `USERPROFILE`; a leading `~/` in either variable is
  expanded.
- A profile is `[default]` or `[profile <name>]` in the config file and `[<name>]` in the
  credentials file. The two are merged; the credentials file wins, and `[profile default]` wins
  over `[default]`.
- The INI rules are the AWS SDKs': `#` and `;` comment lines, a trailing comment after whitespace,
  case-insensitive keys, repeated sections merged, an indented line continuing the value before it,
  and sub-property blocks (`s3 =` followed by indented lines), which are kept apart from the
  profile's own properties (`IniSection.subProperties`).
- Credentials are the static keys of the profile (`aws_access_key_id`, `aws_secret_access_key`,
  optional `aws_session_token`), or the JSON its `credential_process` command prints (`Version` 1,
  `AccessKeyId`, `SecretAccessKey`, optional `SessionToken` and `Expiration`).
- `settingsFromEnvironment(lookup, service)` returns what is not a credential: the region
  (`AWS_REGION`, `AWS_DEFAULT_REGION`, the profile's `region`), the endpoint override
  (`AWS_ENDPOINT_URL_<SERVICE>`, `AWS_ENDPOINT_URL`, the profile's `endpoint_url`, none of them when
  `AWS_IGNORE_CONFIGURED_ENDPOINT_URLS` is `true`), and the CA bundle (`AWS_CA_BUNDLE`, the
  profile's `ca_bundle`).

### Container, instance metadata, web identity (`Aws.Credentials.Remote`)

| Provider | Variables | Request |
|---|---|---|
| Container | `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI` (on `http://169.254.170.2`, wins), `AWS_CONTAINER_CREDENTIALS_FULL_URI`, `AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE` (wins, read for every request), `AWS_CONTAINER_AUTHORIZATION_TOKEN` | `GET`, the token as `Authorization`, 5 s |
| Instance metadata | `AWS_EC2_METADATA_DISABLED`, `AWS_EC2_METADATA_SERVICE_ENDPOINT`, `AWS_EC2_METADATA_SERVICE_ENDPOINT_MODE` (`IPv6`) | `PUT /latest/api/token` with a TTL of 21600 s, then the role name and its credentials with the token; 1 s each |
| Web identity | `AWS_WEB_IDENTITY_TOKEN_FILE`, `AWS_ROLE_ARN`, `AWS_ROLE_SESSION_NAME` (else `lyric-<time>`); the STS endpoint from `AWS_ENDPOINT_URL_STS`, `AWS_ENDPOINT_URL`, the profile's `endpoint_url`, else `https://sts.<region>.amazonaws.com`, else `https://sts.amazonaws.com`; `AWS_CA_BUNDLE` | an unsigned form `POST` of `AssumeRoleWithWebIdentity`, 30 s |

A full container URI is refused unless it is `https://`, a loopback address (`127.0.0.0/8`, `::1`,
`localhost`), the ECS agent (`169.254.170.2`) or the EKS Pod Identity agent (`169.254.170.23`,
`[fd00:ec2::23]`). The STS endpoint must be `https://` unless its host is loopback.

### The cache

`cachedWith(provider, CacheOptions(refreshWindowMs, retryAfterFailureMs, nowMs))` returns a
`CachedProvider`; `cached(provider)` uses `defaultCacheOptions()`: a window of five minutes, thirty
seconds after a failure, the system clock. `nowMs` is the clock, a `() -> Long` of milliseconds
since the epoch, so a test can move time.

- Credentials without an expiry are asked for once.
- Credentials with one are served until `refreshWindowMs` before it. The first caller after that
  refreshes; every other caller is served the current credentials meanwhile.
- If the refresh fails, the current credentials are served until they expire, and the wrapped
  provider is not asked again for `retryAfterFailureMs`.
- When there is nothing to serve, one caller asks the provider and the others wait for its outcome;
  the provider is never called by two threads at once. The state is a protected type.
- `invalidate()` forgets the credentials; `stats()` counts refreshes and failures.

## Retries (`Aws.Retry`)

`RetryPolicy` is modelled on the AWS SDKs' "standard" mode. `standardRetry()` is three attempts with
waits drawn uniformly from `[0, min(20 s, 100 ms * 2^(attempt-1))]`; `noRetry()` is one attempt;
`withMaxAttempts`, `withBackoff`, `withSleep` and `withRandom` change a policy (the last two inject
the sleep and the random source).

| Function | What it is |
|---|---|
| `classifyResponse(status, errorCode)` | the `FailureKind` of a failed response: the error code decides when it is a known one, else the status |
| `isThrottlingCode`, `isTransientCode`, `isClockSkewCode` | the code lists (`Throttling`, `ThrottlingException`, `RequestThrottled`, `SlowDown`, `RequestLimitExceeded`, ...; `RequestTimeout`, `InternalError`, ...; `RequestTimeTooSkewed`, `RequestExpired`, `RequestInTheFuture`) |
| `isConnectFailure(message)` | true when a transport error text says the connection was never opened |
| `retryAfterMsOf(retryAfter, xAmzRetryAfter)` | the wait a response asks for |
| `policy.decide(attempt, kind, idempotent, retryAfterMs)` | `GiveUp` or `RetryAfter(delayMs)` |
| `policy.pause(delayMs)` | waits with the policy's sleep |

| `FailureKind` | From | Retried |
|---|---|---|
| `Throttled` | a throttling code, HTTP 429, 503 | always |
| `ConnectFailed` | a connection that could not be opened | always |
| `Transient` | HTTP 500, 502, 504, `RequestTimeout`, `InternalError` | only when the request is idempotent |
| `NoResponse` | a connection cut after it was opened | only when the request is idempotent |
| `TimedOut` | no answer within the deadline | only when the request is idempotent |
| `ClockSkew` | `RequestTimeTooSkewed`, `RequestExpired`, `RequestInTheFuture` | never |
| `NotRetryable` | anything else | never |

The caller signs each attempt again; the policy only decides and waits.

## Configuration

There is nothing to configure in the package itself. The environment variables are those in the
tables above; a variable set to the empty string counts as unset.

## Supported targets

`dotnet` and `jvm`. Both run the same code; there is no per-target kernel. `native` has not been
tried.

## Surprises

- **A chain stops at a provider that is configured but broken.** Only `NOT_CONFIGURED` and
  `UNAVAILABLE` are passed over. Half a key pair in the environment, a web identity token file that
  cannot be read, a profile that needs SSO: each ends the chain with its own error, so the
  credentials of a later provider are never used in place of the ones that were configured. The AWS
  SDKs differ among themselves here; some move on.
- **`NO_CREDENTIALS` holds every provider's reason**, in chain order, in one message.
- **A profile that needs SSO or an assumed role is refused.** `role_arn`, `source_profile`,
  `credential_source`, `web_identity_token_file`, the `sso_*` settings and `login_session` make a
  profile `UNSUPPORTED_PROFILE`, even next to static keys. Web identity works through the
  environment variables, not through a profile.
- **A profile named with `AWS_PROFILE` must exist** (`INVALID_CONFIG`); a missing `default` profile
  is `NOT_CONFIGURED`. `AWS_DEFAULT_PROFILE` is not read.
- **`credential_process` is run without a shell.** The command line is split on whitespace with
  `"..."` and `'...'` quoting; there is no variable expansion, globbing or redirection. It has 60
  seconds. Neither its output nor its error stream is ever quoted in an error.
- **A malformed line of a shared file is an error with its line number**, never skipped, and never
  quoted: it may hold a secret. An indented line directly after a section header is such an error.
- **Instance metadata is IMDSv2 only.** There is no fallback to IMDSv1. Off EC2 the provider gives
  up after one second (`UNAVAILABLE`), which is what the default chain costs when nothing else has
  credentials; set `AWS_EC2_METADATA_DISABLED=true` to skip it.
- **The container host is not resolved.** A full URI over plain `http://` must name an allowed
  address literally; a host name other than `localhost` needs `https://`.
- **`ca_bundle` and `AWS_CA_BUNDLE` are trusted in addition to the system trust store**, through
  `Std.Tls.Certificate.fromPem`, which is documented for a file of one certificate. A bundle of
  several certificates has not been tried.
- **The cache serves credentials that are inside the refresh window while a refresh runs or after
  it failed**, until they expire. An error is never cached when there are no credentials at all:
  the next `resolve` asks again.
- **A wait is never longer than `maxDelayMs`**, also when `Retry-After` asks for more. The HTTP-date
  form of `Retry-After` is not read.
- **Clock skew errors are classified, not retried.** Nothing corrects the clock.
- **`isConnectFailure` reads error texts** of the .NET and JDK HTTP clients, because `Std.Http`
  reports every transport failure as one kind. A text it does not know is `NoResponse`, which is
  retried less, never more.
- **No message holds a secret.** `ResolvedCredentials` and `Credentials` are plain records, though:
  printing one prints the secret.

There is no difference between the targets.

## Tests

```sh
lyric test --manifest aws-credentials/lyric.toml --target dotnet
lyric test --manifest aws-credentials/lyric.toml --target jvm
```

Hermetic, 52 tests in six modules:

- `tests/ini_tests.l` (9): the INI dialect, malformed lines included.
- `tests/core_tests.l` (13): static and environment providers, chains, the cache with an injected
  clock (reuse, refresh at the window, a failed refresh, invalidation) and under four concurrent
  callers, expiry timestamps.
- `tests/profile_tests.l` (12): file and profile selection, merging, section naming, sub-property
  blocks, refused profiles, real files, `credential_process` with a real command (`/bin/sh` on a
  script the test writes), settings.
- `tests/remote_tests.l` (9): in-process fakes of the three services on `Std.HttpServer`. The fake
  metadata service answers 401 without the session token it issued and issues one only to a `PUT`
  with the TTL header; the fake container endpoint checks `Authorization`; the fake STS checks the
  form and answers with STS's XML, success and errors. The order of the default chain is tested
  against all of them at once.
- `tests/retry_tests.l` (8): classification, decisions and waits with an injected sleep and random
  source.
- `tests/readme_tests.l` (1): the examples above.

The fakes listen on `127.0.0.1` ports 19521 to 19525.

Integration, against the AWS Emulator, 4 tests:

```sh
scripts/ci/start-aws-emulator.sh          # exports AWS_EMULATOR_CREDENTIALS_FILE and AWS_EMULATOR_CONFIG_FILE
scripts/ci/test-aws-emulator.sh dotnet    # runs this package, mq-sqs and storage-s3
```

| Variable | Meaning |
|---|---|
| `AWS_EMULATOR_CREDENTIALS_FILE` | the INI credentials file the emulator wrote (`[default]`, the key it generated) |
| `AWS_EMULATOR_CONFIG_FILE` | the INI config file the emulator wrote (region, `endpoint_url`, `ca_bundle`, an `s3 =` block) |

`tests/integration/emulator_integration_tests.l` reads both files through
`AWS_SHARED_CREDENTIALS_FILE` and `AWS_CONFIG_FILE` in a lookup that holds nothing else, and calls
STS `GetCallerIdentity` on the emulator, signed with what it read. The emulator verifies SigV4: the
call returns account `000000000000`, and the same call with another secret is refused with 403.
Under `--features integration` an unset variable is a failure that names it.

Not covered by an integration test: the container endpoint, the instance metadata service and STS
`AssumeRoleWithWebIdentity` have only been run against the in-process fakes, never against ECS,
EKS, EC2 or AWS STS.

## Pinned versions

| What | Version |
|---|---|
| AWS Emulator (integration) | 1.0.341, pinned in `scripts/ci/start-aws-emulator.sh` |
| STS API | `2011-06-15` |
| `credential_process` output | `Version` 1 |

## Known gaps

No issue has been filed for these; each is a feature that is absent, not one that misbehaves.

- No IAM Identity Center (SSO) or console login session profiles, and no `role_arn` profiles
  (`source_profile`, `credential_source`): such a profile is `UNSUPPORTED_PROFILE`. No
  `AssumeRole` with signed credentials.
- No `[services]` sections and no per-service `endpoint_url` in a sub-property block.
- A CA bundle of more than one certificate is untested.
- No retry quota (the token bucket of the standard mode), no adaptive mode, no clock-skew
  correction.
- No IMDSv1, no `AWS_METADATA_SERVICE_TIMEOUT` or `AWS_METADATA_SERVICE_NUM_ATTEMPTS`.
- A provider value cannot be handed to a spawned task directly (interface values are not sendable,
  T0225); share it through a protected type, as `tests/core_tests.l` does.
