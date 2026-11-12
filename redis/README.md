# Lyric.Redis

A Redis client written in Lyric. The protocol (RESP2 and RESP3 encoder, incremental decoder), the URL
parser and the client are plain Lyric; only the TCP socket and the TLS layer over it are reached through
a per-target kernel. No third-party SDK.

| | |
|---|---|
| Lyric packages | `Redis` (client), `Redis.Resp` (protocol), `Redis.Url` (URL parser), `Redis.Kernel` (socket and TLS boundary) |
| Targets | `dotnet`, `jvm` |
| Stability | experimental (0.1.0) |
| Third-party libraries | none |
| Tested against | Redis 7.0.15 (plain and TLS), .NET 10, OpenJDK 21 |
| Server requirement | Redis 6.0 or later for ACL users (`AUTH user password`), RESP3 and TLS; older servers work with RESP2 and a `requirepass` password |
| TLS | `rediss://`, TLS 1.2 and 1.3, through the platform's own stack (`SslStream` on dotnet, JSSE on jvm) |

It is the base of `Lyric.Cache.Redis` and `Lyric.Session.Redis` in this repository.

## Use

```lyric
import Redis

match Redis.connect("redis://app:secret@cache.internal:6379/2") {
  case Err(e) -> println(e.message())
  case Ok(client) -> {
    val _ = Redis.setEx(client, "greeting", "hello", 60i64)
    match Redis.get(client, "greeting") {
      case Ok(Some(v)) -> println(v)
      case Ok(None) -> println("expired")
      case Err(e) -> println(e.message())
    }
    client.close()
  }
}
```

Every operation returns `Result[_, RedisError]`; nothing throws. `RedisError` is a union
(`InvalidConfig`, `ConnectFailed`, `AuthFailed`, `ConnectionLost`, `ProtocolViolation`, `ServerError`,
`TlsFailed`, `UnexpectedReply`, `ClientClosed`) with `message()` and a stable `code()`. No message
contains the password or a private key.

Commands: `ping`, `get`, `getBytes`, `set`, `setBytes`, `setEx`, `setPx`, `setNx`, `del`, `exists`,
`expire`, `pexpire`, `persist`, `ttl`, `pttl`, `incrBy`, `hget`, `hset`, `hdel`, `hgetAll`, `scan`,
`scanAll` (with `escapeGlob` for literal `MATCH` patterns), `flushDb`, `eval`. Anything else goes through
`Redis.command(client, ["XADD", ...])`, which returns the decoded `Redis.Resp.RespValue`, or
`client.execute(args)` for binary arguments. `client.pipeline(commands)` sends several commands in one
write.

## Configuration

Connection settings are explicit values. An application keeps the URL in its own `config { }` block and
passes it in. The library reads one environment variable, `LYRIC_TLS_ALLOW_INSECURE`, and only as the
second key of the insecure policy described under TLS.

- `Redis.connect(url)` with `redis://[username[:password]@]host[:port][/database][?db=N&protocol=2|3]`,
  or `rediss://...` for the same over TLS. `redis://:secret@host` is a password without a user name; user
  name and password are percent-decoded; an IPv6 host is written `[::1]`.
- `Redis.configFor(host, port)` or `Redis.configFromUrl(url)`, adjusted with `withCredentials`,
  `withDatabase`, `withProtocol` (2 is the default; 3 negotiates with `HELLO 3`), `withTimeouts` (connect
  and I/O, milliseconds, both 5000 by default), `withStaleRetry` and the TLS functions below, then
  `Redis.open(cfg)` (connects and authenticates at once) or `Redis.lazy(cfg)` (connects on the first
  command).

## TLS

```lyric
import Redis

// The system trust store, the URL's host as the server name:
val public = Redis.connect("rediss://app:secret@redis.example.com:6380")

// A private CA and a client certificate (mutual TLS):
match Redis.configFromUrl("rediss://cache.internal:6380/1") {
  case Err(e) -> println(e.message())
  case Ok(base) -> {
    val cfg = Redis.withClientIdentity(Redis.withExclusiveCaCertificate(base, caPem), certPem, keyPem)
    val client = Redis.open(cfg)
  }
}
```

A `rediss://` URL, or `Redis.withTls(cfg)`, connects over TLS. By default the server certificate must
chain to the system trust store (the JDK's trust store on `jvm`) and must carry the host name: both
checks are on, on both targets. The options live in `RedisConfig.tls` (`RedisTlsOptions`) and are set
with functions named after `Std.Http`'s `HttpClientBuilder`; the types are `Std.Tls`'s. Certificates and
keys are PEM text; `Std.Tls` loads no other format, and a private key must be an unencrypted PKCS#8 block
(`-----BEGIN PRIVATE KEY-----`; convert with `openssl pkcs8 -topk8 -nocrypt`).

| Function | Effect |
|---|---|
| `withTls(cfg)` | Connect over TLS (what `rediss://` does). |
| `withExclusiveCaCertificate(cfg, caPem)`, `withExclusiveCaCertificateFile(cfg, path)` | Trust only this CA for the server certificate. The system trust store is then not consulted. |
| `withClientIdentity(cfg, certPem, keyPem)`, `withClientIdentityFiles(cfg, certPath, keyPath)` | Present a client certificate (mutual TLS). |
| `withTlsServerName(cfg, name)` | Send `name` as SNI and check the certificate against it instead of against `host`. |
| `withMinTlsVersion(cfg, Std.Tls.Tls13)` | Refuse TLS 1.2. The default floor is TLS 1.2; nothing older can be configured. |
| `withInsecureSkipVerify(cfg)` | One of the two keys that switch verification off (below). |
| `checkTls(cfg)` | Validate the TLS part without connecting. `open` and the first command of a `lazy` client run the same check. |

- The `...File` variants read the file once, when they are called, and return `Result`; the others take
  the PEM text and cannot fail. Certificates and keys are parsed when the client first connects, and once
  per client. An unusable certificate, an unsupported key format, a key that does not belong to the
  certificate, or TLS options on a configuration that is not TLS are `InvalidConfig`, before anything is
  dialled.
- A failed handshake (untrusted certificate, name mismatch, no common protocol version, a server that
  is not TLS) is `TlsFailed` (`TLS_FAILED`) with the endpoint and the platform's reason.
- **Mutual TLS.** With TLS 1.3 a server checks the client certificate after the client's side of the
  handshake has finished. A missing or untrusted client certificate is therefore reported by whatever
  reads next: `open` when it sends `AUTH`/`HELLO`/`SELECT`, otherwise the first command, as `TlsFailed`
  or `ConnectionLost`.
- **Timeouts and reconnects** are the same as over plain TCP. The connect timeout bounds the TCP
  connection and the I/O timeout bounds each read and write of the handshake and of every later command.
  Every reconnect runs a new handshake with the same parsed certificates.
- **`withInsecureSkipVerify` is the standard library's dual-key policy** (lyric-lang
  `docs/61-https-tls-http-versions.md` section 4). Verification is switched off, for that client only,
  when the configuration asks for it and the environment variable `LYRIC_TLS_ALLOW_INSECURE` is exactly
  `1` when the client first connects. The variable alone does nothing. The call alone leaves verification
  on and writes a warning to standard error. With both, neither the certificate chain nor the server name
  is checked, on both targets. Never use it in production.
- There is no additive form (system trust store plus one more CA, `Std.Http`'s `withCaCertificate`): on
  `dotnet` it needs a certificate validation callback on `SslStream`, which this package does not bind.
  A CA bundle is not supported either: the PEM holds one certificate.

## Behaviour

- **Thread safety.** `RedisClient` is a `protected type` holding one connection. A command, or a
  pipeline, runs under the client's lock from its first byte written to the last byte of its reply, so a
  client can be shared between threads; commands are serialized, not multiplexed, and there is no
  connection pool in 0.1. A sequence of separate calls is not atomic: use `eval`.
- **Reconnect.** A connection that breaks is closed, and the next command dials again and repeats
  `AUTH`/`HELLO` and `SELECT`. A command is repeated once on a fresh connection only when it provably did
  not run: the write failed, or the server had already closed the idle connection (end of stream before
  any reply byte). A timeout or a reset inside a reply is `ConnectionLost` and is never retried, since the
  command may have run. `withStaleRetry(cfg, false)` turns the repeat off. A pipeline is never retried.
- **Timeouts.** A connection that is not established within the connect timeout is `ConnectFailed`, on
  both targets. A reply that does not arrive within the I/O timeout is `ConnectionLost`, and the
  connection is dropped so that a late reply cannot be mistaken for the next one. On `jvm` the I/O timeout
  bounds reads only: `SO_TIMEOUT` does not bound a blocked write.
- **RESP3.** Every RESP3 type is decoded; push messages that arrive between replies are skipped;
  attributes are dropped. Map replies (`HGETALL`) are handled by the typed commands under both versions.

## Kernels

Both kernels declare the package `Redis.Kernel` with the same surface (`sockConnect`, `tlsPrepare`,
`sockStartTls`, `sockRead`, `sockWrite`, `sockClose`, plus `serverListen` / `serverListenTls` /
`serverAccept` and `startBackground` / `waitBackground`, which the test suites use for the in-process
fake servers and for threads), and a file-level `@cfg(target = ...)` leaves one of them in a build. All
externs are `extern type` / `@externTarget` declarations bound by auto-FFI; there is no host shim.

- `src/_kernel_dotnet/redis_kernel.l` binds `System.Net.Sockets.TcpClient` and, for TLS,
  `System.Net.Security.SslStream` with `SslClientAuthenticationOptions`. `Std.TcpHost` is not used for
  the client side: it dials plain connections only, has no connect timeout, and its connection type is
  opaque, so a client-side TLS upgrade cannot be layered on it. Verification is `SslStream`'s own, with
  no callback: a configured CA is an `X509ChainPolicy` in `CustomRootTrust` mode, and the insecure mode
  is an `X509ChainPolicy` with `X509VerificationFlags.AllFlags`. The listening side used by the tests is
  `Std.TcpHost`, including its server-side TLS.
- `src/_kernel_jvm/redis_kernel.l` binds `java.net.Socket` and, for TLS, JSSE: an `SSLContext` per
  client, an `SSLSocket` layered over the connected socket, and `SSLParameters` with the endpoint
  identification algorithm `HTTPS`. A configured CA is a `KeyStore` behind a `TrustManagerFactory`, a
  client identity a `KeyStore` behind a `KeyManagerFactory`. The JSSE and `KeyStore` calls that take
  `null`, a `char[]` or a typed array go through `java.lang.reflect.Method.invoke`, as in the standard
  library's JVM HTTP kernel.
- PEM parsing is the standard library's, on both targets: `Std.Tls` validates (`Certificate.fromPem`,
  `Identity.fromPem`), and `Std.TlsHost`, its kernel, yields the platform objects (`X509Certificate2`;
  `java.security.cert.Certificate` and `PrivateKey`). `Std.Tls.Certificate` and `Identity` are opaque and
  their handle accessor is `internal` to the standard library, which is why `RedisConfig` carries PEM
  text and not those types.

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

The hermetic TLS suite (`tests/tls_tests.l`) runs real handshakes on both targets against a fake server
built on `Kernel.serverListenTls`. Its certificates and keys are test fixtures embedded in the file (a
throwaway CA, a server certificate for `localhost` / `127.0.0.1`, a client certificate, a second CA);
they are not secrets and protect nothing.

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
directory, starts the two TLS servers on 127.0.0.1:16380 and 127.0.0.1:16381 (no plain port), and prints
the five TLS variables as `export` lines, or appends them to `$GITHUB_ENV` under GitHub Actions.

## Known gaps

- **TLS.** No additive CA (system trust store plus a custom CA) and no CA bundles: one exclusive CA or
  the system trust store. No PKCS#12, PKCS#1/SEC1 or encrypted keys (a `Std.Tls` limit). No certificate
  revocation checking. No certificate pinning, no ALPN, no session
  resumption control. On `dotnet` a client identity given as PEM text passes through a private temporary
  directory (mode 700, deleted at once) inside `Std.TlsHost`, because .NET's in-memory PEM pairing takes
  spans, which auto-FFI cannot pass.
- The connect timeout on `dotnet` (`Task.Wait` on `TcpClient.ConnectAsync`) has no automated test: it
  needs an address that drops packets, which the hermetic suite cannot create portably. It was checked
  by hand against a listener with a full backlog.
- **One connection per client**: no pool, no multiplexing, no pub/sub API, no `MULTI`/`EXEC` helper (use
  `eval` or a pipeline), no Cluster or Sentinel support.
- `EVAL` sends the script text each time; there is no `EVALSHA` cache.
