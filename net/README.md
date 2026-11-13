# Lyric.Net

TCP and TLS client connections for protocol clients written in Lyric, for `dotnet` and `jvm`,
`experimental` (0.1.0). It implements no `lyric-lang` interface: it is the layer under the pure-Lyric
clients of this repository (`Lyric.Redis`, `Lyric.Mq.ActiveMq`, `Lyric.Mq.RabbitMq`), which used to
carry a copy each, and it can be used directly by any client that speaks its own protocol over a
socket.

It holds three things:

- `Net.Tcp`: a connection with a connect timeout, a read with a time limit that tells "nothing
  arrived" from "the peer closed" from a failure, a write that gives up when the peer accepts no
  data, a TLS client upgrade with a handshake limit, a reentrant lock and a monotonic clock.
- `Net.Tls`: the TLS options of a client (`TlsOptions`), with the function names of `Std.Http`'s
  `HttpClientBuilder`, their validation, the two-key policy for switching verification off, and a
  reader of the options from environment variables.
- `Net.Url`: the authority part of a connection URL (`scheme://[user[:password]@]host[:port]`):
  schemes with default ports, percent-decoding, user information, IPv6 literals.

| | |
|---|---|
| Lyric packages | `Net.Tcp`, `Net.Tls`, `Net.Url`; `Net.Tcp.Testing` (test support); `Net.Tcp.Kernel` (the externs, not an API) |
| Targets | `dotnet`, `jvm` |
| Stability | experimental (0.1.0) |
| Third-party libraries | none (`[nuget]` and `[maven]` are empty) |
| Depends on | the standard library only |
| TLS | TLS 1.2 and 1.3 through the platform's own stack (`SslStream` on dotnet, JSSE on jvm) |

## Usage

Inside this workspace a package declares `"Lyric.Net" = { workspace = true }` under `[dependencies]`.

A request and its reply over TLS:

```lyric
import Std.Core
import Std.Encoding
import Net.Tcp as Tcp
import Net.Tls

func askOverTls(host: in String, port: in Int, caPem: in String, request: in String): Result[String, String] {
  val tls = plainTcp().withTls().withCaCertificate(caPem)
  match validateTlsOptions(tls) {
    case Ok(_) -> ()
    case Err(problem) -> return Err(tlsProblemText(problem, "a tcps:// URL"))
  }
  val plain = Tcp.sockConnect(host, port, 10000)?
  val sock = Tcp.sockStartTls(plain, tls, host, skipsVerification(tls), 10000)?
  val answer = match Tcp.sockWriteWithin(sock, encodeUtf8(request), 5000) {
    case Err(e) -> Err(e)
    case Ok(_) -> match Tcp.sockRead(sock, 4096, 5000) {
      case Ok(Data(bytes)) -> match tryDecodeUtf8(bytes) {
        case Some(text) -> Ok(text)
        case None -> Err("the reply is not UTF-8")
      }
      case Ok(TimedOut) -> Err("no reply within five seconds")
      case Ok(Eof) -> Err("the server closed the connection")
      case Err(e) -> Err(e)
    }
  }
  Tcp.sockClose(sock)
  answer
}
```

A connection URL:

```lyric
import Std.Core
import Net.Url as Url

record Endpoint {
  host: String
  port: Int
  user: String
  password: String
  tls: Bool
}

func schemes(): slice[Url.Scheme] {
  [Url.Scheme(name = "tcp", defaultPort = 9000, tls = false), Url.Scheme(name = "tcps", defaultPort = 9443, tls = true)]
}

func parseEndpoint(url: in String): Result[Endpoint, String] {
  val found = match Url.matchScheme(url, schemes(), true) {
    case Ok(m) -> m
    case Err(problem) -> return Err(Url.schemeProblemText(problem, schemes()))
  }
  val userInfo = match Url.splitUserInfo(found.rest) {
    case Ok(u) -> u
    case Err(problem) -> return Err(Url.authorityProblemText(problem, "user", "password", found.scheme))
  }
  match Url.parseHostPort(userInfo.hostPort, found.scheme.defaultPort) {
    case Ok(address) -> Ok(
      Endpoint(
        host = address.host,
        port = address.port,
        user = userInfo.user,
        password = userInfo.password,
        tls = found.scheme.tls
      )
    )
    case Err(problem) -> Err(Url.authorityProblemText(problem, "user", "password", found.scheme))
  }
}
```

Both examples are compiled and run by `tests/readme_tests.l`.

Imports: `Net.Tls` is imported whole, because its `with...` functions are methods on `TlsOptions` and
method syntax does not work through an import alias (`T0113`). `Net.Tcp`, `Net.Tcp.Testing` and
`Net.Url` have plain functions and can be imported under an alias.

## API

### `Net.Tcp`

| | |
|---|---|
| `sockConnect(host, port, connectTimeoutMs): Result[Sock, String]` | Open a connection; `0` is the operating system's limit. `TCP_NODELAY` is set |
| `sockStartTls(sock, tls, host, skipVerify, timeoutMs): Result[Sock, String]` | Client side of a TLS handshake. `tls` is a `Net.Tls.TlsOptions`; the name checked is `tls.serverName`, or `host`. Pass `skipsVerification(tls)` for `skipVerify`. On `Err` the connection is closed |
| `sockRead(sock, maxBytes, timeoutMs): Result[ReadOutcome, String]` | `Data(bytes)` (1 to `maxBytes` bytes), `TimedOut` (nothing within the wait; the connection stays usable; `0` polls), or `Eof` (the peer closed). `Err` is a failure. Bytes that arrived before a close or a failure are returned first, and the `Eof` or `Err` repeats |
| `sockWriteWithin(sock, bytes, timeoutMs): Result[Unit, String]` | Write everything and flush; `Err("the write timed out: ...")` when the peer accepts no data for `timeoutMs` (`0` is no limit). The connection is then of no further use |
| `sockWrite(sock, bytes)` | `sockWriteWithin` with no limit |
| `sockClose(sock)` | Close; twice is harmless |
| `newLock()`, `lockEnter(l)`, `lockTryEnter(l, timeoutMs): Bool`, `lockExit(l)` | A reentrant lock. Each lock is its own instance |
| `monotonicMillis(): Long`, `sleepMs(ms)` | A clock that never goes backwards; block the calling thread |

A `Sock` is read by one thread at a time and written by one thread at a time; the two may differ.

### `Net.Tls`

`TlsOptions` is a record of PEM text and switches; an empty string is "not configured". Start from
`plainTcp()`:

| Function on `TlsOptions` | Effect |
|---|---|
| `withTls()` | TLS with the defaults: the platform trust store, the name checked against the host |
| `withCaCertificate(pem)` | trust this CA certificate in addition to the platform trust store |
| `withExclusiveCaCertificate(pem)` | trust only this CA certificate; wins over `withCaCertificate` |
| `withClientIdentity(certificatePem, keyPem)` | present a client certificate (mutual TLS) |
| `withTlsServerName(name)` | send `name` as SNI and check the certificate against it instead of the host |
| `withMinTlsVersion(Std.Tls.TlsVersion.Tls13)` | TLS 1.3 only; the default `Tls12` accepts 1.2 and 1.3 |
| `withInsecureSkipVerify()` | one of the two keys that switch verification off (below) |

| Function | |
|---|---|
| `validateTlsOptions(t): Result[Unit, TlsProblem]` | Check without connecting: options set while TLS is off, PEM that the `Std.Tls` loaders refuse, half a client identity, a key that is not the certificate's |
| `tlsProblemText(problem, howToEnable)` | A wording of the problem; `howToEnable` completes "use ... or withTls()" |
| `hasTlsSettings(t)`, `requiresTls13(t)` | Whether anything but `enabled` is set; whether only TLS 1.3 is accepted |
| `insecureAllowEnvName()` | `"LYRIC_TLS_ALLOW_INSECURE"` |
| `insecureAllowedByEnvironment()` | Whether that variable is `1` now |
| `skipsVerification(t)` | Whether a connection opened with `t` now skips verification: TLS on, the option set, and the variable `1` |
| `insecureIgnoredWarning(t, component)` | The warning to print when the option is set and the variable is not; `""` otherwise |
| `tlsOptionsFromEnv(prefix, base): Result[TlsOptions, String]` | `base` with the settings of the `<prefix>TLS...` variables (below) |

### `Net.Url`

| | |
|---|---|
| `Scheme(name, defaultPort, tls)` | A scheme a client accepts |
| `matchScheme(url, schemes, ignoreCase): Result[SchemeMatch, SchemeProblem]` | The scheme and everything after `://`. `NoScheme` or `UnsupportedScheme(name)` otherwise |
| `percentDecode(text): Result[String, EscapeProblem]` | `%XX` escapes read as UTF-8 bytes |
| `splitUserInfo(authority): Result[UserInfo, AuthorityProblem]` | Cut at the last `@`, split at the first `:` before it, both halves percent-decoded. `present` is false without an `@` |
| `parseHostPort(text, defaultPort): Result[HostPort, AuthorityProblem]` | `host`, `host:port`, `[v6]`, `[v6]:port`; ports 1 to 65535 |
| `schemeProblemText`, `escapeProblemText`, `authorityProblemText` | A wording of each problem. A client that words its own errors matches on the problem instead |

The path and the query of a URL are the client's own: cut them off before `splitUserInfo`.

### `Net.Tcp.Testing` (test support)

`serverListen(host, port)`, `serverPort`, `serverAccept` (a `Net.Tcp.Sock`), `serverClose`,
`sockAcceptTls(sock, ServerTls(certPem, keyPem, clientCaPem), timeoutMs)` (the server side of a
handshake; a non-empty `clientCaPem` requires a client certificate), `startBackground(work)`,
`waitBackground(b)` and `startPoolWork(work)` (the shared thread pool on dotnet). They exist for the
in-process fake servers that the hermetic suites of protocol clients run against. They are not a
server for an application: use `Std.HttpServer` or `Std.TcpHost` for that.

## Configuration

The package reads one variable on its own:

| Variable | Read | Effect |
|---|---|---|
| `LYRIC_TLS_ALLOW_INSECURE` | by `insecureAllowedByEnvironment()` and `skipsVerification(t)`, when they are called | With the exact value `1`, and only for options that have `withInsecureSkipVerify()`, `skipsVerification` is true. Any other value counts as unset |

This is the standard library's two-key policy (lyric-lang `docs/61-https-tls-http-versions.md`
section 4): the variable alone does nothing; the option alone leaves verification on, and
`insecureIgnoredWarning` gives the text to warn with. Never use it in production.

`tlsOptionsFromEnv(prefix, base)` reads, for the prefix its caller chooses (`LYRIC_CONFIG_MQ_ACTIVEMQ_`
and `LYRIC_CONFIG_MQ_RABBITMQ_` in this repository); a variable that is empty counts as unset:

| Variable | Effect |
|---|---|
| `<prefix>TLSCAFILE` | a PEM file with a CA certificate to trust in addition to the platform trust store |
| `<prefix>TLSCAEXCLUSIVE` | `true` to trust only that CA; `false` (default). Without `TLSCAFILE`, `true` is an error |
| `<prefix>TLSCERTFILE`, `<prefix>TLSKEYFILE` | PEM files with the client certificate and its PKCS8 private key |
| `<prefix>TLSSERVERNAME` | the name to check the server certificate against |
| `<prefix>TLSMINVERSION` | `1.2` (default) or `1.3` |

It neither switches TLS on (the caller does, usually from the URL scheme) nor verification off (that
takes `withInsecureSkipVerify()` in code).

## Surprises

Connections:

- **On `dotnet` a connection that has been read from owns one thread until it is closed.** The first
  `sockRead` starts a reader thread that does blocking reads and hands each chunk over; `sockRead`
  waits for the hand-over with a limit. That makes a read timeout exact over TLS as well as plain
  TCP, and independent of the runtime's shared thread pool: the connect, the handshake and the
  reader run on threads of their own, and nothing completes on a pool thread. An application that
  blocks all its pool threads (calling a synchronous client from many tasks) therefore still gets
  its replies on time. The price: a connection dropped without `sockClose` keeps its thread and its
  socket. Always close. On `jvm` reads are the socket's own with `SO_TIMEOUT`, and there is no extra
  thread.
- **A read timeout is not an error.** `TimedOut` leaves the connection usable and loses nothing:
  bytes that arrive later are returned by a later read. `timeoutMs = 0` returns only what has
  already arrived (it waits one millisecond on dotnet, so a chunk being handed over is not missed).
- **A write timeout is the end of the connection.** `sockWriteWithin` gives up when the peer accepts
  no data for the limit, not when the whole write takes longer than it; how much was sent is unknown,
  so the only thing left to do is `sockClose`. On `jvm` a socket has no write timeout: the write runs
  on a thread of its own and the socket is closed when no 64 KiB chunk gets through in time.
- **`Eof` and a read failure repeat.** Every read after the peer's close returns `Eof` again; bytes
  that arrived before it are returned first.
- **`0` means different things.** For `sockConnect` and `sockStartTls` it is "no limit of ours"; for
  `sockRead` it is "do not wait"; for `sockWriteWithin` it is "no limit".
- **The lock is fair on `jvm` and not on `dotnet`** (a `ReentrantLock(true)` and a monitor). A
  thread that releases the lock and takes it again at once can keep winning on dotnet.
- **Failure texts are the platform's.** An `Err` carries the .NET or JDK exception message; only
  "the write timed out", "connect to HOST:PORT timed out after N ms" and the handshake's "timed out
  after N ms" (dotnet) are worded here. Match on those, not on platform text.

TLS:

- **Certificates are PEM text, not `Std.Tls` values.** `Std.Tls.Certificate` and `Std.Tls.Identity`
  cannot leave the standard library: their handles are `internal` (nichobbs/lyric-lang#9064). So
  `TlsOptions` carries PEM text, `validateTlsOptions` checks it with the `Std.Tls` loaders, and the
  kernel loads it again for every handshake. A certificate renewed in a file is picked up only if
  the caller reads the file again.
- **A custom CA can be additive or exclusive.** `withCaCertificate` adds one CA to the platform trust
  store; `withExclusiveCaCertificate` replaces the store with it, and wins when both are set. Each
  takes one certificate, not a bundle.
- **On `jvm` the platform trust store is the JDK's**, not the operating system's. A CA installed in
  the OS store is trusted on `dotnet` and not on `jvm` unless it is also in the JDK's `cacerts`.
- **The server name is always checked** against `withTlsServerName`, or the host given to
  `sockStartTls`, unless verification is skipped. An IP address is checked against the certificate's
  IP subject alternative names.
- **`sockStartTls` does not validate and does not apply the insecure policy.** It does what its
  arguments say. Call `validateTlsOptions` first (a malformed PEM otherwise fails the handshake with
  a loader message, after the TCP connection was made), and pass `skipsVerification(tls)`, never a
  constant `true`, as `skipVerify`.
- **TLS options on a plain connection are a problem, not a silent downgrade**: `validateTlsOptions`
  returns `TlsNotEnabled` when anything is set and `enabled` is not.
- **`withInsecureSkipVerify()` alone changes nothing.** See "Configuration".
- **A missing client certificate can surface after the handshake.** With TLS 1.3 a server checks the
  client certificate after the client's side has finished; the refusal is reported by the next read
  or write.
- **The handshake limit is the whole handshake on `dotnet` and each read of it on `jvm`.**
- **Private keys are unencrypted PKCS8** (`-----BEGIN PRIVATE KEY-----`; convert with
  `openssl pkcs8 -topk8 -nocrypt`), the only form `Std.Tls` loads.
- **On `dotnet` a client identity given as PEM passes through a private temporary directory** (mode
  700, deleted at once) inside `Std.TlsHost`, because .NET's in-memory PEM pairing takes spans, which
  auto-FFI cannot pass.

URLs:

- **An IPv6 literal without brackets is refused**, not guessed at: `::1` would otherwise read as host
  `::` and port 1. Write `[::1]` or `[::1]:6379`.
- **The user information ends at the last `@`** and splits at its first `:`, so a password may hold a
  literal `@` or `:`; `/`, `?` and `#` in it must be percent-encoded, because the caller cuts the path
  and the query off first.
- **An empty user or password is not a problem here.** `splitUserInfo(":secret@host")` is a present
  user information with an empty user: whether that is allowed is the client's rule.
- **No problem value or text carries the password.**

Differences between the targets, collected:

| | `dotnet` | `jvm` |
|---|---|---|
| Socket | `System.Net.Sockets.TcpClient` | `java.net.Socket` |
| TLS | `SslStream.AuthenticateAsClient`, trust through `CertificateChainPolicy` | `SSLSocket` over the connected socket, `SSLContext` from `KeyStore`-backed factories, endpoint identification `HTTPS` |
| Platform trust store | operating system root stores | JDK trust store |
| Reads | a reader thread per connection that has been read from; no shared thread pool | `SO_TIMEOUT` on the socket |
| Write limit | `SendTimeout` on the socket | the write runs on its own thread; the socket is closed when no 64 KiB chunk gets through in time |
| TLS handshake limit | whole handshake | each read |
| Skip-verify | chain policy with `X509VerificationFlags.AllFlags` | a trust manager that accepts everything, and no endpoint identification |
| Lock | monitor, not fair | fair `ReentrantLock` |
| `startPoolWork` (test support) | the shared thread pool | a thread of its own: there is no shared pool to keep busy |
| Platform part of error texts | .NET exception messages | JDK exception messages |

## Layout

| File | Package | Content |
|---|---|---|
| `src/tcp.l` | `Net.Tcp` | the connection API |
| `src/tcp_testing.l` | `Net.Tcp.Testing` | listener, server-side TLS and background threads for tests |
| `src/tls.l` | `Net.Tls` | `TlsOptions`, validation, the insecure policy, the environment reader |
| `src/url.l` | `Net.Url` | schemes, percent-decoding, user information, host and port |
| `src/_kernel_dotnet/tcp_kernel.l` | `Net.Tcp.Kernel` | `System.Net.Sockets`, `System.Net.Security.SslStream`, monitor lock, clock, dedicated threads |
| `src/_kernel_jvm/tcp_kernel.l` | `Net.Tcp.Kernel` | `java.net.Socket`, `javax.net.ssl.SSLSocket`, `ReentrantLock`, clock, threads |

The two kernel files declare the same package and surface; a file-level `@cfg(target = ...)` keeps one
per build. They are the only files with extern declarations (`extern type` and `@externTarget`, bound
by auto-FFI; no host shim). `Net.Tcp.Kernel` is public because `Net.Tcp` and `Net.Tcp.Testing` are
separate Lyric packages; it is not an API.

How the kernels do TLS:

- dotnet: `SslStream.AuthenticateAsClient` with `SslClientAuthenticationOptions`. Trust is a
  `CertificateChainPolicy` (a custom root store; the platform's root stores are added to it for
  `withCaCertificate`), and skip-verify is that policy with `X509VerificationFlags.AllFlags`. No
  validation callback is involved: the callback is a named delegate, which a Lyric lambda cannot
  become (lyric-lang docs/66 gap 2). An earlier design kept a pending `Stream.ReadAsync` per
  connection; its completion needs a pool thread, so with the pool busy a message that had arrived
  was seen seconds late. The reader thread replaced it.
- jvm: an `SSLContext` from a `KeyStore`-backed `TrustManagerFactory` and `KeyManagerFactory`, an
  `SSLSocket` layered over the connected socket, and
  `SSLParameters.setEndpointIdentificationAlgorithm("HTTPS")`. The JSSE and `KeyStore` calls that
  take `null` or a typed array go through `java.lang.reflect`, as in the standard library's
  `_kernel_jvm/http_host.l`.
- Both load certificates and keys through `Std.TlsHost`, the standard library's PEM loader.

## Tests

Run from the repository root. There is no service to integrate with, so there is no integration
suite; every test is hermetic and runs real sockets and real TLS handshakes on the loopback
interface.

```sh
lyric test --manifest net/lyric.toml --target dotnet
lyric test --manifest net/lyric.toml --target jvm
```

| Module | Content |
|---|---|
| `tests/tcp_tests.l` | echo and end of stream, reads that never return more than asked, a refused connection, the lock (reentrancy, identity, `lockTryEnter`), the clock, the write timeout against a peer that reads nothing, independence from a busy thread pool, bytes arriving in pieces across read timeouts (plain and TLS), and the TLS handshake: exclusive and additional CA, an untrusted CA, the name check and the server name override, skip-verify, mutual TLS, TLS 1.3 only, a handshake that times out |
| `tests/tls_tests.l` | the `with...` functions, validation, the two-key policy and its warning, `tlsOptionsFromEnv` |
| `tests/url_tests.l` | schemes, percent-decoding, user information, hosts and ports, IPv6 with and without brackets, no password in problem texts |
| `tests/readme_tests.l` | the two examples above, compiled and run |

The certificates and keys in the test files are constants named `TEST_ONLY_...`: generated for
tests with `openssl`, valid until 2126, and worthless as secrets. `tests/tls_tests.l` sets and clears
`LYRIC_TLS_ALLOW_INSECURE` and `LYRIC_NET_TLS_TESTS_*` itself and writes three PEM files named
`lyric-net-test-only-*` into `$TMPDIR` (or `/tmp`).

The clients built on this package test it again from above: `redis`, `mq-activemq` and `mq-rabbitmq`
run their hermetic suites against in-process servers on `Net.Tcp.Testing`, and their integration
suites against real servers over plain TCP and TLS.

## Pinned versions

There is no third-party library. The runtimes the suites run against: .NET 10; Temurin 22 in CI,
OpenJDK 21 in development.

## Known gaps

- No server API: `Net.Tcp.Testing` is for tests only.
- No connection pool, no asynchronous API, no half-close, no Unix-domain sockets, no proxy support.
- The connect timeout has no automated test: it needs an address that drops packets, which a hermetic
  suite cannot create portably.
- TLS: one CA certificate per option, not a bundle; no PKCS12, PKCS1/SEC1 or encrypted keys (a
  `Std.Tls` limit); no revocation checks; no client certificate chain (intermediates between the
  client certificate and the server's CA are not sent); no ALPN, no certificate pinning, no session
  resumption control.
- On dotnet, skip-verify relies on `SslStream` honouring `X509VerificationFlags.IgnoreInvalidName`
  from the chain policy for the name check. That is what .NET 10 does (the tests cover it); a runtime
  that does not would refuse the connection rather than accept it.
- The kernels import `Std.TlsHost`, the standard library's kernel package for PEM loading. It is
  public but not marked `@stable`.
- On `dotnet` every connection that has been read from holds one thread until it is closed.
- Standard library and compiler issues behind the design, in `nichobbs/lyric-lang`:
  - [#9042](https://github.com/nichobbs/lyric-lang/issues/9042): `Std.TcpHost` has no JVM kernel and
    cannot tell a read timeout from a failure, so this package binds the host socket API itself.
  - [#9064](https://github.com/nichobbs/lyric-lang/issues/9064): the standard library has no TLS
    client for a raw socket, and `Std.Tls.Certificate.rawHandle` / `Identity.rawHandle` are
    `internal`, so the kernels bind the platform's TLS themselves and the options take PEM text.
  - [#4775](https://github.com/nichobbs/lyric-lang/issues/4775): the JVM auto-FFI cannot pass `null`
    as an argument. Nor can it pass a typed reference array (`TrustManager[]`, `Certificate[]`,
    `String[]`), and a record cannot implement `X509TrustManager` (neither filed). The JVM kernel
    makes those calls through `java.lang.reflect`.
  - A Lyric lambda cannot become a named .NET delegate (`RemoteCertificateValidationCallback`;
    docs/66 gap 2), so the dotnet kernel configures trust through `CertificateChainPolicy` only.
  - `@externTarget("System.Threading.Tasks.Task`1[System.Int32].get_Result")` on an `extern type`
    for the closed generic emits a member reference the runtime rejects (nichobbs/lyric-lang#9068).
  - Extension methods of a package imported under an alias cannot be called with method syntax
    (`T0113`), which is why `Net.Tls` is imported whole (nichobbs/lyric-lang#9071).
  - A `[project.tests]` module cannot import a non-test package from `tests/` (`T0215`), so the test
    certificates are repeated in the test files that need them.
