# Lyric.Redis

A Redis client written in Lyric, for `dotnet` and `jvm`, `experimental` (0.1.0). It implements no
`lyric-lang` interface: it is the client that `Lyric.Cache.Redis` (`Cache.CacheStore`) and
`Lyric.Session.Redis` (`Session.SessionStore`) in this repository are built on, and it can be used
directly. The protocol (RESP2 and RESP3 encoder, incremental decoder), the URL parser and the client
are plain Lyric; the TCP connection and the TLS layer over it are `Net.Tcp`'s, from the
`Lyric.Net` package of this repository (`net/`), which holds the only extern declarations. There is no
third-party SDK.

| | |
|---|---|
| Lyric packages | `Redis` (client), `Redis.Resp` (protocol), `Redis.Url` (URL parser) |
| Depends on | `Lyric.Net` (`net/`): TCP and TLS connection, TLS option checks, URL authority parsing |
| Targets | `dotnet`, `jvm` |
| Stability | experimental (0.1.0) |
| Third-party libraries | none |
| Server requirement | Redis 6.0 or later for ACL users (`AUTH user password`), RESP3 and TLS; older servers work with RESP2 and a `requirepass` password |
| TLS | `rediss://`, TLS 1.2 and 1.3, through the platform's own stack (`SslStream` on dotnet, JSSE on jvm) |

## Usage

Inside this workspace a package declares `"Lyric.Redis" = { workspace = true }` under
`[dependencies]`.

```lyric
import Std.Core
import Redis

func example(url: in String): Result[Option[String], RedisError] {
  val client = Redis.connect(url)?
  Redis.setEx(client, "greeting", "hello", 60i64)?
  val value = Redis.get(client, "greeting")?
  client.close()
  Ok(value)
}
```

`example("redis://app:secret@cache.internal:6379/2")` connects, authenticates as the ACL user `app`,
selects database 2, and answers `Ok(Some("hello"))`.

Every operation returns `Result[_, RedisError]`; nothing throws. `RedisError` is a union with
`message()` and a stable `code()`:

| Case | `code()` | Meaning |
|---|---|---|
| `InvalidConfig` | `INVALID_CONFIG` | Unusable URL, configuration, certificate or key; nothing was dialled |
| `ConnectFailed` | `CONNECT_FAILED` | The TCP connection could not be opened (refused, unresolved, connect timeout) |
| `TlsFailed` | `TLS_FAILED` | The TLS handshake failed |
| `AuthFailed` | `AUTH_FAILED` | The server refused the credentials |
| `ConnectionLost` | `CONNECTION_LOST` | The connection broke or timed out during a command, which may or may not have run |
| `ProtocolViolation` | `PROTOCOL_VIOLATION` | The server sent bytes that are not RESP |
| `ServerError` | `SERVER_ERROR` | The server answered with an error reply (`-ERR ...`, `WRONGTYPE ...`) |
| `UnexpectedReply` | `UNEXPECTED_REPLY` | The reply has a type the command does not produce |
| `ClientClosed` | `CLIENT_CLOSED` | The client was closed with `close()` |

No message contains the password or a private key.

Variations:

- **An explicit configuration** instead of a URL, connecting at once (`open`) or on the first command
  (`lazy`):

  ```lyric
  func configured(password: in String): Result[RedisClient, RedisError] {
    val base = Redis.configFor("cache.internal", 6379)
    val cfg = Redis.withTimeouts(
      Redis.withProtocol(Redis.withDatabase(Redis.withCredentials(base, "app", password), 2), 3),
      2000,
      1000
    )
    Redis.open(cfg)
  }

  func onFirstUse(url: in String): Result[RedisClient, RedisError] {
    val cfg = Redis.configFromUrl(url)?
    Ok(Redis.lazy(Redis.withStaleRetry(cfg, false)))
  }
  ```

- **Typed commands**: `ping`, `get`, `getBytes`, `set`, `setBytes`, `setEx`, `setPx`, `setNx`, `del`,
  `exists`, `expire`, `pexpire`, `persist`, `ttl`, `pttl`, `incrBy`, `hget`, `hset`, `hdel`, `hgetAll`,
  `scan`, `scanAll` (with `escapeGlob` for a literal `MATCH` pattern), `flushDb`, `eval`.

  ```lyric
  func typed(client: in RedisClient): Result[Long, RedisError] {
    val stored = Redis.setNx(client, "lock:job", "owner-1", 30000i64)?
    val user = Redis.hgetAll(client, "user:42")?
    val keys = Redis.scanAll(client, Redis.escapeGlob("user:") + "*")?
    println(stored.toString() + user.count.toString() + keys.count.toString())
    Redis.setBytes(client, "blob", [0u8, 255u8])?
    Redis.incrBy(client, "hits", 1i64)
  }
  ```

- **Any other command** with `Redis.command(client, args)`, which returns the decoded
  `Redis.Resp.RespValue`; `client.execute(args)` takes binary arguments. `client.pipeline(commands)`
  sends several commands in one write and returns one reply per command.

  ```lyric
  import Std.Encoding
  import Redis.Resp

  func raw(client: in RedisClient): Result[Unit, RedisError] {
    val reply = Redis.command(client, ["LPUSH", "queue", "job-1"])?
    match reply {
      case RInt(n) -> println(n.toString())
      case other -> println(Resp.kindOf(other))
    }
    val commands: slice[slice[slice[Byte]]] = [
      [encodeUtf8("SET"), encodeUtf8("a"), encodeUtf8("1")],
      [encodeUtf8("INCR"), encodeUtf8("a")]
    ]
    val replies = client.pipeline(commands)?
    for r in replies {
      match r {
        case RError(m) -> println("failed: " + m)
        case other -> println(Resp.kindOf(other))
      }
    }
    Ok(())
  }
  ```

- **Client state**: `client.close()` (final), `client.disconnect()` (the next command dials again),
  `client.isConnected()`, `client.connections()` (how many connections the client has opened) and
  `client.ensureConnected()`.
- **TLS**: a `rediss://` URL uses the system trust store and the URL's host as the server name. A
  private CA, a client certificate (mutual TLS) and the other options are set on the configuration:

  ```lyric
  func tlsPublic(): Result[RedisClient, RedisError] {
    Redis.connect("rediss://app:secret@redis.example.com:6380")
  }

  func tlsPrivate(caPem: in String, certPem: in String, keyPem: in String): Result[RedisClient, RedisError] {
    val base = Redis.configFromUrl("rediss://cache.internal:6380/1")?
    val cfg = Redis.withClientIdentity(Redis.withExclusiveCaCertificate(base, caPem), certPem, keyPem)
    Redis.checkTls(cfg)?
    Redis.open(cfg)
  }

  func tlsFiles(): Result[RedisClient, RedisError] {
    val base = Redis.withTls(Redis.configFor("10.0.0.5", 6380))
    val trusted = Redis.withExclusiveCaCertificateFile(base, "/etc/app/redis-ca.pem")?
    val identified = Redis.withClientIdentityFiles(trusted, "/etc/app/client.pem", "/etc/app/client.key")?
    Redis.open(Redis.withMinTlsVersion(Redis.withTlsServerName(identified, "redis.internal"), Tls.Tls13))
  }
  ```

  (`tlsFiles` needs `import Std.Tls as Tls`.) Certificates and keys are PEM text; a private key must be
  an unencrypted PKCS#8 block (`-----BEGIN PRIVATE KEY-----`; convert with
  `openssl pkcs8 -topk8 -nocrypt`).

## Configuration

Connection settings are explicit values. An application keeps the URL in its own `config { }` block and
passes it in.

The URL, for `Redis.connect(url)` and `Redis.configFromUrl(url)`:

```
redis://[username[:password]@]host[:port][/database][?db=N&protocol=2|3]
rediss://...                      the same, over TLS
```

| Part | Default | Notes |
|---|---|---|
| scheme | (required) | `redis` or `rediss`; anything else is `InvalidConfig` |
| `username`, `password` | none | Percent-decoded. `redis://:secret@host` is a password without a user name. A user name without a password is refused |
| `host` | (required) | An IPv6 address is written `[::1]`; without the brackets it is refused |
| `port` | 6379 | For `rediss://` too |
| `/database` or `?db=N` | 0 | `?db=` wins when both are given |
| `?protocol=` | 2 | 2 or 3 |

`RedisConfig`, from `Redis.configFor(host, port)` or `Redis.configFromUrl(url)`, adjusted with the
`with...` functions:

| Field | Set with | Default | Meaning |
|---|---|---|---|
| `host`, `port` | `configFor`, URL | (required), 6379 | The server |
| `username`, `password` | `withCredentials`, URL | `""`, `""` | ACL user (`""` is the default user) and password (`""` sends no credentials) |
| `database` | `withDatabase`, URL | 0 | Selected with `SELECT` after connecting and after every reconnect |
| `protocol` | `withProtocol`, URL | 2 | RESP version; 3 negotiates with `HELLO 3` |
| `connectTimeoutMs` | `withTimeouts` | 5000 | Longest wait for the TCP connection; 0 is no limit |
| `ioTimeoutMs` | `withTimeouts` | 5000 | Longest wait for one read or write, the TLS handshake included; 0 is no limit |
| `retryStaleConnection` | `withStaleRetry` | true | Repeat a command once when the server had closed the idle connection |
| `tls` | the functions below | off | `RedisTlsOptions` |

TLS options (`RedisConfig.tls`). The function names are those of `Std.Http`'s `HttpClientBuilder`, the
types `Std.Tls`'s:

| Function | Default without it | Effect |
|---|---|---|
| `withTls(cfg)` | plain TCP | Connect over TLS (what `rediss://` does) |
| `withCaCertificate(cfg, caPem)`, `withCaCertificateFile(cfg, path)` | the system trust store (the JDK's on `jvm`) | Trust this CA in addition to the system trust store |
| `withExclusiveCaCertificate(cfg, caPem)`, `withExclusiveCaCertificateFile(cfg, path)` | the system trust store (the JDK's on `jvm`) | Trust only this CA for the server certificate. Wins over `withCaCertificate` |
| `withClientIdentity(cfg, certPem, keyPem)`, `withClientIdentityFiles(cfg, certPath, keyPath)` | no client certificate | Present a client certificate (mutual TLS) |
| `withTlsServerName(cfg, name)` | `host` | Send `name` as SNI and check the certificate against it |
| `withMinTlsVersion(cfg, Std.Tls.Tls13)` | TLS 1.2 | Refuse TLS 1.2. Nothing older than 1.2 can be configured |
| `withInsecureSkipVerify(cfg)` | verification on | One of the two keys that switch verification off (below) |
| `checkTls(cfg)` | | Validate the TLS part without connecting. `open` and the first command of a `lazy` client run the same check |

Environment variables:

| Variable | Read | Effect |
|---|---|---|
| `LYRIC_TLS_ALLOW_INSECURE` | when a TLS client first connects | With the exact value `1`, and only for a client whose configuration has `withInsecureSkipVerify`, neither the certificate chain nor the server name is checked. Any other value counts as unset |

It is the only variable the library reads. This is the standard library's dual-key policy (lyric-lang
`docs/61-https-tls-http-versions.md` section 4): the variable alone does nothing; the call alone leaves
verification on and writes a warning to standard error. Never use it in production.

## Surprises

Connection model:

- **One client is one connection, and commands are serialized.** `RedisClient` is a `protected type`:
  a command, or a pipeline, holds the client's lock from its first byte written to the last byte of its
  reply. A client can be shared between threads, but there is no pool and no multiplexing, so one slow
  command makes every other thread wait. A blocking command (`BLPOP`) holds the client for as long as
  it blocks.
- **A sequence of calls is not atomic.** Another thread's command can run between two of yours. Use
  `eval`.
- **Connection state set with a raw command does not survive a reconnect.** A reconnect repeats
  `AUTH`/`HELLO` and the configured `SELECT`, nothing else. A `SELECT`, `CLIENT SETNAME` or `MULTI`
  sent through `Redis.command` is lost when the connection is replaced, and nothing reports it. Choose
  the database in the configuration.
- **Push messages are discarded.** RESP3 out-of-band messages that arrive between replies are skipped
  and attributes are dropped, so `SUBSCRIBE` and client-side caching invalidation cannot be used
  through this client.
- **`lazy` reports nothing until the first command.** An unreachable server, a failed handshake or a
  wrong password is an `Err` from `connect`/`open`, but from the first command of a `lazy` client, and
  again from every later command, each of which dials anew.

Reconnects, retries and timeouts:

- **A command is repeated only when it provably did not run.** A broken connection is closed and the
  next command dials again. The failing command itself is repeated once, on a fresh connection, only
  when the write failed or the server had already closed the idle connection (end of stream before any
  reply byte). A timeout or a reset inside a reply is `ConnectionLost` and is never retried, since the
  command may have run. `withStaleRetry(cfg, false)` turns the repeat off. A pipeline is never retried.
- **A reply slower than `ioTimeoutMs` is `ConnectionLost`, even if the server is healthy.** The
  connection is dropped so that a late reply cannot be mistaken for the next one. A script or a
  blocking command that takes longer than the timeout (5 s by default) therefore fails on the client
  while it may still complete on the server.
- **The I/O timeout is per read, not per command.** A large reply that keeps arriving does not time
  out, however long it takes in total.
- **On `jvm` the I/O timeout bounds reads only**: `SO_TIMEOUT` does not bound a blocked write. On
  `dotnet` it bounds reads and writes.
- **An error reply does not break the connection.** `ServerError` leaves the connection in use; only
  `ConnectionLost` and `ProtocolViolation` drop it.

Commands:

- **`get`, `hget` and `hgetAll` are text.** A value that is not UTF-8 is `UnexpectedReply`, not a
  lossy string; read it with `getBytes` or `command`.
- **`set` removes an existing expiry**, as Redis defines for `SET`. `setEx` and `setPx` take a
  positive lifetime; zero or less is a contract violation.
- **`ttl` and `pttl` return Redis's sentinels**: `-1` for a key without an expiry, `-2` for a key that
  does not exist.
- **A pipeline answers `Ok` when a command in it failed.** Error replies come back in place as `RError`
  values in the list; `Err` means the connection broke, and then it is unknown which commands ran. A
  pipeline is one write, not a transaction.
- **`scanAll` may miss or include keys written during the scan**, as `SCAN` does; it removes the
  duplicates the server returns. Its pattern is a glob: escape literal text with `escapeGlob`.
- **`eval` sends the whole script on every call.** There is no `EVALSHA` cache.
- **`flushDb` deletes every key of the selected database**, not only keys this client wrote.
- **Argument limits are `requires:` clauses and record invariants.** An empty command, and in
  `configFor` and the `with...` functions a port outside 1 to 65535, a user name without a password, a
  negative timeout or database index, panic with a contract violation; they are not `InvalidConfig`.
  The same mistakes in a URL are `InvalidConfig`.
- **Replies are bounded.** A bulk string over 512 MiB, an aggregate over 16777216 elements or nesting
  deeper than 128 is `ProtocolViolation`. Every reply is held in memory whole.

Handshake:

- **RESP2 sends no `HELLO`.** With a password it sends `AUTH password` or `AUTH user password`. With
  RESP3 the credentials travel inside `HELLO 3 AUTH user password`, and a password without a user
  name authenticates as `default`.
- **A server that refuses `HELLO 3` is a `ServerError` from `open`.** The client does not fall back to
  RESP2.
- **A refused `SELECT` is a `ServerError` from `open`**, not `InvalidConfig`.
- **`rediss://` keeps the default port 6379.** The port commonly used for TLS, 6380, must be written.
- **Unknown URL parameters are refused** (`InvalidConfig`), and TLS options are not URL parameters.

TLS:

- **A custom CA is additive or exclusive.** `withCaCertificate` trusts that CA and the system trust
  store; `withExclusiveCaCertificate` trusts only that CA, and wins when both are set. The PEM holds
  one certificate, not a bundle. In `RedisTlsOptions` the exclusive CA is `caCertificatePem` and the
  additional one `additionalCaCertificatePem`.
- **On `jvm` the default trust store is the JDK's, not the operating system's.** A CA installed in the
  OS store is trusted on `dotnet` and not on `jvm` unless it is also in the JDK's `cacerts`.
- **TLS options on a plain connection are an error.** A CA, a client certificate, a server name, a
  minimum version or `withInsecureSkipVerify` on a configuration that is not TLS is `InvalidConfig`,
  before anything is dialled, so a `redis://` URL cannot silently ignore them.
- **Certificates are checked when the client first connects, once per client, and loaded again for
  every handshake.** The `...File` variants
  read the file once, when they are called, and return `Result`; the others take PEM text and cannot
  fail. An unusable certificate, an unsupported key format or a key that does not belong to the
  certificate is `InvalidConfig` from `open`, `checkTls` or the first command of a `lazy` client. A
  certificate renewed on disk is not picked up by an existing client.
- **`withInsecureSkipVerify` alone changes nothing** except a warning on standard error; see the
  variable above. `LYRIC_TLS_ALLOW_INSECURE` is read when the client first connects, so changing it
  later does not affect an existing client.
- **A missing client certificate can surface after `open`.** With TLS 1.3 a server checks the client
  certificate after the client's side of the handshake has finished. The refusal is reported by
  whatever reads next: `open` when it sends `AUTH`/`HELLO`/`SELECT`, otherwise the first command, as
  `TlsFailed` or `ConnectionLost`.
- **Every reconnect is a full handshake**, with the same PEM text, bounded by the same timeouts as
  over plain TCP. The I/O timeout bounds the handshake as a whole, not each of its steps.
- **A timeout reads "timed out: the server sent nothing for N ms"**, as `ConnectionLost`. The
  connection is closed and the command is not repeated.
- **On `dotnet` a connection that has been read from owns one thread** until it is closed, and the
  client does not use the runtime's shared thread pool (see the `Lyric.Net` README). Always `close`
  a client you are done with.
- **A TLS client on a plain port fails within the I/O timeout** with `TlsFailed`; a plain client on a
  TLS port gets an error, never a reply.
- **On `dotnet` a client identity given as PEM text passes through a private temporary directory**
  (mode 700, deleted at once) inside `Std.TlsHost`, because .NET's in-memory PEM pairing takes spans,
  which auto-FFI cannot pass.

## Backend-specific module

There is no interface, so there is no separate module of extras; the package is three Lyric packages:

- `Redis`: the client, the configuration and the typed commands above.
- `Redis.Resp`: the protocol with no I/O. `encodeCommand` / `encodeCommandBytes`, `decode` (one reply
  out of a buffer, `Incomplete` when the buffer ends first), `RespDecoder` / `feed` for a stream in
  arbitrary chunks, and the `RespValue` union (`RSimple`, `RError`, `RInt`, `RBulk`, `RNull`, `RArray`,
  and the RESP3 `RDouble`, `RBool`, `RBigNumber`, `RVerbatim`, `RMap`, `RSet`, `RPush`) with `asText`
  and `kindOf`. The RESP3 streamed forms (`$?`, `*?`) are rejected: no Redis release emits them.
- `Redis.Url`: `parseRedisUrl`. Its error text never contains the URL's password.

The socket and TLS boundary is not part of this package: it is `Net.Tcp` in `Lyric.Net` (`net/`),
shared with the message-queue clients of this repository. `RedisConfig` carries PEM text and not
`Std.Tls.Certificate` / `Identity` values because those are opaque outside the standard library
(lyric-lang#9064); `checkTls` validates the PEM with the `Std.Tls` loaders. The in-process fake
servers of the test suites, here and in `cache-redis` and `session-redis`, use `Net.Tcp.Testing`.

## Tests

```sh
# Hermetic (no Redis needed): RESP, URL parsing, the client against an in-process fake RESP server,
# and the client over TLS against an in-process TLS server
lyric test --manifest redis/lyric.toml --target dotnet
lyric test --manifest redis/lyric.toml --target jvm

# Integration, against real servers: one plain, two with TLS
redis-server --port 16379 --save "" --appendonly no --daemonize yes
export LYRIC_REDIS_URL=redis://127.0.0.1:16379
eval "$(scripts/ci/start-redis-tls.sh)"
lyric test --manifest redis/lyric.toml --target dotnet --features integration
lyric test --manifest redis/lyric.toml --target jvm --features integration
```

Run from the repository root. The hermetic TLS suite (`tests/tls_tests.l`) runs real handshakes on both
targets against a fake server built on `Net.Tcp.Testing`. Its certificates and keys are test
fixtures embedded in the file (a throwaway CA, a server certificate for `localhost` / `127.0.0.1`, a
client certificate, a second CA); they are not secrets and protect nothing.

The integration modules (`tests/integration/`) are compiled only with `--features integration` and fail
with a message naming the variable when one is unset. Use disposable servers: the suites create and
delete ACL users and run `FLUSHDB` on database 15. All other keys carry a random prefix and are deleted
again.

| Variable | Meaning |
|---|---|
| `LYRIC_REDIS_URL` | `redis://` URL of a plain server |
| `LYRIC_REDIS_TLS_URL` | `rediss://` URL of a TLS server that does not demand a client certificate; its certificate must name the URL's host, `localhost` and `127.0.0.1` |
| `LYRIC_REDIS_MTLS_URL` | `rediss://` URL of a TLS server that refuses clients without a certificate (`tls-auth-clients yes`) |
| `LYRIC_REDIS_TLS_CA` | PEM file: the CA that signed the server certificates and the client certificate |
| `LYRIC_REDIS_TLS_CERT`, `LYRIC_REDIS_TLS_KEY` | PEM files: a client certificate and its PKCS#8 key |

`scripts/ci/start-redis-tls.sh` installs `redis-server` if it is missing, generates a CA, a server
certificate (SAN `localhost`, `127.0.0.1`) and a client certificate with `openssl` into a temporary
directory, starts the two TLS servers on 127.0.0.1:16380 and 127.0.0.1:16381 (no plain port; override
with `LYRIC_REDIS_TLS_PORT` and `LYRIC_REDIS_MTLS_PORT`), and prints the five TLS variables as `export`
lines, or appends them to `$GITHUB_ENV` under GitHub Actions. The hermetic TLS suite sets and clears
`LYRIC_TLS_ALLOW_INSECURE` itself.

## Pinned versions

There is no third-party library. The servers and runtimes the suites run against:

| | |
|---|---|
| Redis, plain (CI) | the `redis:7.4` service container |
| Redis, TLS (CI) | the `redis-server` package of the `ubuntu-latest` runner, started by `scripts/ci/start-redis-tls.sh` (7.0.15 on Ubuntu 24.04) |
| Redis (development) | 7.0.15, plain and TLS |
| Runtimes | .NET 10; Temurin 22 in CI, OpenJDK 21 in development |

## Known gaps

- **TLS.** No CA bundles: one additional CA, one exclusive CA, or the system trust store. No PKCS#12,
  PKCS#1/SEC1 or encrypted keys (a `Std.Tls` limit). No certificate revocation checking. No certificate pinning, no ALPN, no session resumption
  control.
- The connect timeout has no automated test: it needs an address that drops packets, which the
  hermetic suite cannot create portably.
- The gaps of `Lyric.Net` apply (lyric-lang#9042, lyric-lang#9064); see its README.
- **One connection per client**: no pool, no multiplexing, no pub/sub API, no `MULTI`/`EXEC` helper (use
  `eval` or a pipeline), no Cluster or Sentinel support.
- `EVAL` sends the script text each time; there is no `EVALSHA` cache.
