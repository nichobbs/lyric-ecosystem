# lyric-ecosystem

Out-of-tree libraries for [Lyric](https://github.com/nichobbs/lyric-lang): backends that bind a
Lyric library interface to an external service or SDK. The interfaces (`JobScheduler`,
`MessageQueue`, `StorageBucket`, `CacheStore`, `SessionStore`, `FlagStore`) and their conformance suites live in
`lyric-lang`; each package here implements one interface against one backend and passes that
interface's conformance suite on every target it claims.

## Packages

| Package | Directory | Interface | Backend | Targets | Implementation |
|---|---|---|---|---|---|
| `Lyric.Storage.S3` | `storage-s3/` | `Storage.StorageBucket` | Amazon S3 and S3-compatible stores | dotnet, jvm | pure Lyric (SigV4 over `Std.Http`) |
| `Lyric.Storage.Gcs` | `storage-gcs/` | `Storage.StorageBucket` | Google Cloud Storage | dotnet, jvm | pure Lyric (XML API, GOOG4-HMAC-SHA256 over `Std.Http`); not yet verified with a real key |
| `Lyric.Storage.AzureBlob` | `storage-azure-blob/` | `Storage.StorageBucket` | Azure Blob Storage | dotnet, jvm | pure Lyric (Shared Key over `Std.Http`) |
| `Lyric.Aws.SigV4` | `aws-sigv4/` | (shared signing) | AWS Signature V4 | dotnet, jvm | pure Lyric |
| `Lyric.Jobs.Quartz` | `jobs-quartz/` | `Jobs.JobScheduler` | Quartz | jvm | Quartz via Maven |
| `Lyric.Jobs.QuartzNet` | `jobs-quartznet/` | `Jobs.JobScheduler` | Quartz.NET | dotnet | Quartz.NET via NuGet |
| `Lyric.Net` | `net/` | (shared TCP and TLS client connections, TLS options, URL authority parsing) | host sockets and TLS | dotnet, jvm | pure Lyric over one per-target socket kernel; used by `Lyric.Redis`, `Lyric.Mq.RabbitMq` and `Lyric.Mq.ActiveMq` |
| `Lyric.Redis` | `redis/` | (client) | Redis | dotnet, jvm | pure Lyric (RESP over TCP) |
| `Lyric.Cache.Redis` | `cache-redis/` | `Cache.CacheStore` | Redis | dotnet, jvm | on `Lyric.Redis` |
| `Lyric.Cache.Memcached` | `cache-memcached/` | `Cache.CacheStore` | memcached 1.6.10 and later | dotnet, jvm | pure Lyric (meta protocol on `Lyric.Net`) |
| `Lyric.Session.Redis` | `session-redis/` | `Session.SessionStore` | Redis | dotnet, jvm | on `Lyric.Redis` |
| `Lyric.Flags.Flipt` | `flags-flipt/` | `Flags.FlagStore` | Flipt | dotnet, jvm | pure Lyric (Flipt v1 REST over `Std.Http`) |
| `Lyric.Mq.RabbitMq` | `mq-rabbitmq/` | `Mq.MessageQueue`, `Mq.QueueConsumer` | RabbitMQ | dotnet, jvm | pure Lyric (AMQP 0-9-1 over TCP) |
| `Lyric.Mq.Sqs` | `mq-sqs/` | `Mq.MessageQueue`, `Mq.QueueConsumer` | Amazon SQS | dotnet, jvm | pure Lyric (JSON 1.0 over `Std.Http`, SigV4) |
| `Lyric.Mq.ActiveMq` | `mq-activemq/` | `Mq.MessageQueue`, `Mq.QueueConsumer` | Apache ActiveMQ | dotnet, jvm | pure Lyric (STOMP 1.2 over TCP) |

All packages are `0.x` and `experimental`. Each package README lists its configuration, pinned
library versions and known gaps.

## Building and testing

The packages resolve the `lyric-lang` interface packages through `[workspace.overrides]` in the
root `lyric.toml`, against a `lyric-lang` checkout that sits next to this repository, at the commit
recorded in `.lyric-lang-rev`:

```sh
git clone https://github.com/nichobbs/lyric-lang ../lyric-lang
git -C ../lyric-lang checkout "$(cat .lyric-lang-rev)"
make -C ../lyric-lang lyric maven-resolver      # compiler built from the same commit
export LYRIC=$PWD/../lyric-lang/bin/lyric
export LYRIC_MAVEN_RESOLVER=$PWD/../lyric-lang/resolver/target/lyric-resolver.jar

bash scripts/ci/test-packages.sh dotnet         # hermetic suites, every package claiming dotnet
bash scripts/ci/test-packages.sh jvm
"$LYRIC" test --manifest storage-s3/lyric.toml --target jvm   # one package
```

The compiler is built from the pinned commit because the latest release (0.7.12) predates the
conformance suites and `[package] targets`. Once a release contains them, CI installs that release
and the interface dependencies become registry version ranges.

Suites that need a real service live in `<package>/tests/integration/` and run with
`bash scripts/ci/test-packages.sh <target> --integration` once the service is reachable; the
package README names the environment variables. CI runs them against service containers.

## Continuous integration

- `CI` (`.github/workflows/ci.yml`): formatting, the hermetic suites of every package on each
  target it claims, and the integration suites against service containers.
- `Claude Code Review` (`.github/workflows/claude-code-review.yml`): the same structured review as
  `lyric-lang`. REQUIRED findings become issues labelled `pr-<n>` and fail the check. It needs the
  `CLAUDE_CODE_OAUTH_TOKEN` repository secret.
- `Claude Code` (`.github/workflows/claude.yml`): `@claude` mentions from collaborators.

Conventions for contributors and agents are in `CLAUDE.md`.
