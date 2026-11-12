//! Redis - a Redis client written in Lyric.
//!
//! The protocol (`Redis.Resp`), the URL parser (`Redis.Url`) and the client
//! are plain Lyric; only the TCP socket and the TLS layer over it are reached
//! through a per-target kernel (`Redis.Kernel`). Every operation returns
//! `Result[_, RedisError]`; nothing throws.
//!
//! ```
//! import Redis
//!
//! match Redis.connect("redis://app:secret@cache.internal:6379/2") {
//!   case Err(e) -> println(e.message())
//!   case Ok(client) -> {
//!     val _ = Redis.setEx(client, "greeting", "hello", 60)
//!     match Redis.get(client, "greeting") {
//!       case Ok(Some(v)) -> println(v)
//!       case Ok(None) -> println("expired")
//!       case Err(e) -> println(e.message())
//!     }
//!     client.close()
//!   }
//! }
//! ```
//!
//! Thread safety: `RedisClient` is a `protected type` holding ONE connection.
//! Each command (and each `pipeline`) runs under the client's lock from the
//! first byte written to the last byte of the reply, so a client may be shared
//! between threads; commands from different threads are serialized, not
//! multiplexed. A sequence of separate calls is not atomic: use `eval` for
//! that.
//!
//! TLS: a `rediss://` URL (or `withTls`) connects over TLS, verifying the
//! server certificate against the system trust store and its name against
//! the host. `withExclusiveCaCertificate`, `withClientIdentity`,
//! `withMinTlsVersion`, `withTlsServerName` and `withInsecureSkipVerify`
//! adjust that; the names and the insecure policy are `Std.Http`'s
//! (`HttpClientBuilder`), the types `Std.Tls`'s.
//!
//! Reconnection: a connection that breaks is closed and the next command
//! dials again (and repeats AUTH / SELECT). A command is repeated once on a
//! fresh connection only when it provably did not run: the write failed, or
//! the server had already closed the previously idle connection (end of
//! stream before any reply byte). A timeout or a reset in the middle of a
//! reply is returned as `ConnectionLost` and never retried.
@runtime_checked
@io
package Redis

import Std.Core
import Std.Collections
import Std.Console as Console
import Std.Encoding
import Std.Environment as Env
import Std.File as File
import Std.Http as Http
import Std.Parse
import Std.String as Str
import Std.Tls as Tls
import Redis.Resp as Resp
import Redis.Url as Url
import Redis.Kernel as Kernel

// ─── Errors ──────────────────────────────────────────────────────────────────
/// Why a Redis operation failed.
@experimental
pub union RedisError {
  /// The configuration or URL is unusable.
  case InvalidConfig(message: String)
  /// The TCP connection could not be opened.
  case ConnectFailed(message: String)
  /// The TLS handshake failed: the server certificate is not trusted, does
  /// not carry the server name, the server refused the client certificate,
  /// or the two sides share no protocol version.
  case TlsFailed(message: String)
  /// The server refused the credentials (`AUTH` / `HELLO`).
  case AuthFailed(message: String)
  /// The connection broke (closed, reset or timed out) during a command. The
  /// command may or may not have run.
  case ConnectionLost(message: String)
  /// The server sent bytes that are not RESP.
  case ProtocolViolation(message: String)
  /// The server answered the command with an error reply (`-ERR ...`).
  case ServerError(message: String)
  /// The reply has a type the command does not produce.
  case UnexpectedReply(message: String)
  /// The client was closed with `close()`.
  case ClientClosed
}

/// The error as text. Never contains the password.
@experimental
pub func RedisError.message(e: in RedisError): String {
  match e {
    case InvalidConfig(m) -> "invalid Redis configuration: " + m
    case ConnectFailed(m) -> "cannot connect to Redis: " + m
    case TlsFailed(m) -> "Redis TLS handshake failed: " + m
    case AuthFailed(m) -> "Redis authentication failed: " + m
    case ConnectionLost(m) -> "Redis connection lost: " + m
    case ProtocolViolation(m) -> "Redis protocol violation: " + m
    case ServerError(m) -> "Redis error reply: " + m
    case UnexpectedReply(m) -> "unexpected Redis reply: " + m
    case ClientClosed -> "the Redis client is closed"
  }
}

/// A stable identifier for the kind of error.
@experimental
pub func RedisError.code(e: in RedisError): String {
  match e {
    case InvalidConfig(_) -> "INVALID_CONFIG"
    case ConnectFailed(_) -> "CONNECT_FAILED"
    case TlsFailed(_) -> "TLS_FAILED"
    case AuthFailed(_) -> "AUTH_FAILED"
    case ConnectionLost(_) -> "CONNECTION_LOST"
    case ProtocolViolation(_) -> "PROTOCOL_VIOLATION"
    case ServerError(_) -> "SERVER_ERROR"
    case UnexpectedReply(_) -> "UNEXPECTED_REPLY"
    case ClientClosed -> "CLIENT_CLOSED"
  }
}

// ─── Configuration ───────────────────────────────────────────────────────────
/// The TLS side of a `RedisConfig`. Certificates and keys are PEM text, the
/// only format `Std.Tls` loads; a private key is an unencrypted PKCS#8 block
/// (`-----BEGIN PRIVATE KEY-----`). Build it with `withTls` and its siblings.
@experimental
pub record RedisTlsOptions {
  /// Connect over TLS. The other fields require it.
  enabled: Bool
  /// The name sent as SNI and checked against the server certificate; `""`
  /// uses the configuration's `host`.
  serverName: String
  /// The only certificate authority the server certificate may chain to;
  /// `""` trusts the system trust store.
  caCertificatePem: String
  /// The client certificate presented for mutual TLS; `""` presents none.
  clientCertificatePem: String
  /// The private key of `clientCertificatePem`.
  clientKeyPem: String
  /// The lowest TLS version accepted. TLS 1.2 is the floor.
  minVersion: Tls.TlsVersion
  /// One of the two keys of the insecure policy (see `withInsecureSkipVerify`).
  insecureSkipVerify: Bool
  invariant: (clientCertificatePem.length == 0) == (clientKeyPem.length == 0)
}

func noTls(): RedisTlsOptions {
  RedisTlsOptions(
    enabled = false,
    serverName = "",
    caCertificatePem = "",
    clientCertificatePem = "",
    clientKeyPem = "",
    minVersion = Tls.Tls12,
    insecureSkipVerify = false
  )
}

/// How to reach and talk to one Redis server.
@experimental
pub record RedisConfig {
  host: String
  port: Int
  /// ACL user name; `""` authenticates as the default user.
  username: String
  /// Password; `""` sends no credentials.
  password: String
  /// Database index selected after connecting.
  database: Int
  /// RESP version: 2, or 3 (negotiated with `HELLO 3`, Redis 6 and later).
  protocol: Int
  /// Longest wait for the TCP connection, in milliseconds (`0` = no limit).
  connectTimeoutMs: Int
  /// Longest wait for one read or write, in milliseconds (`0` = no limit).
  /// It also bounds each step of the TLS handshake.
  ioTimeoutMs: Int
  /// Repeat a command once on a fresh connection when the server had closed
  /// the idle one before the command ran.
  retryStaleConnection: Bool
  /// TLS: off unless the URL is `rediss://` or `withTls` was applied.
  tls: RedisTlsOptions
  invariant: host.length > 0
  invariant: port >= 1 and port <= 65535
  invariant: database >= 0
  invariant: protocol == 2 or protocol == 3
  invariant: connectTimeoutMs >= 0 and ioTimeoutMs >= 0
  invariant: username.length == 0 or password.length > 0
}

/// A configuration for `host:port` with no credentials, database 0, RESP2,
/// five-second timeouts and no TLS.
@experimental
pub func configFor(host: in String, port: in Int): RedisConfig
  requires: host.length > 0
  requires: port >= 1 and port <= 65535
{
  RedisConfig(
    host = host,
    port = port,
    username = "",
    password = "",
    database = 0,
    protocol = 2,
    connectTimeoutMs = 5000,
    ioTimeoutMs = 5000,
    retryStaleConnection = true,
    tls = noTls()
  )
}

/// The configuration a `redis://[user:password@]host[:port][/db]` URL names
/// (see `Redis.Url`), with five-second timeouts. A `rediss://` URL switches
/// TLS on with the defaults: the system trust store and the URL's host as
/// the server name.
@experimental
pub func configFromUrl(url: in String): Result[RedisConfig, RedisError] {
  match Url.parseRedisUrl(url) {
    case Err(e) -> Err(InvalidConfig(message = e))
    case Ok(u) -> Ok(
      RedisConfig(
        host = u.host,
        port = u.port,
        username = u.username,
        password = u.password,
        database = u.database,
        protocol = if u.protocol == 3 { 3 } else { 2 },
        connectTimeoutMs = 5000,
        ioTimeoutMs = 5000,
        retryStaleConnection = true,
        tls = noTls().copy(enabled = u.tls)
      )
    )
  }
}

/// `cfg` with other timeouts (milliseconds, `0` = no limit).
@experimental
pub func withTimeouts(cfg: in RedisConfig, connectTimeoutMs: in Int, ioTimeoutMs: in Int): RedisConfig
  requires: connectTimeoutMs >= 0 and ioTimeoutMs >= 0
{
  cfg.copy(connectTimeoutMs = connectTimeoutMs, ioTimeoutMs = ioTimeoutMs)
}

/// `cfg` with other credentials. `username = ""` authenticates as the default
/// user; `password = ""` sends none.
@experimental
pub func withCredentials(cfg: in RedisConfig, username: in String, password: in String): RedisConfig
  requires: username.length == 0 or password.length > 0
{
  cfg.copy(username = username, password = password)
}

/// `cfg` selecting another database index.
@experimental
pub func withDatabase(cfg: in RedisConfig, database: in Int): RedisConfig
  requires: database >= 0
{
  cfg.copy(database = database)
}

/// `cfg` speaking RESP `protocol` (2 or 3).
@experimental
pub func withProtocol(cfg: in RedisConfig, protocol: in Int): RedisConfig
  requires: protocol == 2 or protocol == 3
{
  cfg.copy(protocol = protocol)
}

/// `cfg` with the stale-connection retry switched on or off.
@experimental
pub func withStaleRetry(cfg: in RedisConfig, retry: in Bool): RedisConfig {
  cfg.copy(retryStaleConnection = retry)
}

// ─── TLS configuration ───────────────────────────────────────────────────────
/// `cfg` connecting over TLS (what a `rediss://` URL does): the server
/// certificate is verified against the system trust store and must carry the
/// host name.
@experimental
pub func withTls(cfg: in RedisConfig): RedisConfig {
  cfg.copy(tls = cfg.tls.copy(enabled = true))
}

/// `cfg` trusting ONLY the certificate authority `caPem` (PEM text) for the
/// server certificate: the system trust store is not consulted. This is the
/// trust-replacing form, as `Std.Http`'s `withExclusiveCaCertificate`; there
/// is no additive form. The PEM is parsed when the client first connects; an
/// unusable certificate is `InvalidConfig` there.
@experimental
pub func withExclusiveCaCertificate(cfg: in RedisConfig, caPem: in String): RedisConfig
  requires: caPem.length > 0
{
  cfg.copy(tls = cfg.tls.copy(caCertificatePem = caPem))
}

func readPem(what: in String, path: in String): Result[String, RedisError] {
  if path.length == 0 {
    return Err(InvalidConfig(message = "the path of the " + what + " is empty"))
  }
  match File.readText(path) {
    case Ok(text) -> if text.length > 0 {
      Ok(text)
    } else {
      Err(InvalidConfig(message = "the " + what + " file is empty: " + path))
    }
    case Err(_) -> Err(InvalidConfig(message = "cannot read the " + what + ": " + path))
  }
}

/// `withExclusiveCaCertificate` reading the PEM file at `path`.
@experimental
pub func withExclusiveCaCertificateFile(cfg: in RedisConfig, path: in String): Result[RedisConfig, RedisError] {
  match readPem("CA certificate", path) {
    case Ok(pem) -> Ok(withExclusiveCaCertificate(cfg, pem))
    case Err(e) -> Err(e)
  }
}

/// `cfg` presenting a client certificate for mutual TLS: `certPem` and its
/// private key `keyPem` (PEM text; the key an unencrypted PKCS#8 block). Both
/// are parsed when the client first connects; an unusable pair is
/// `InvalidConfig` there.
@experimental
pub func withClientIdentity(cfg: in RedisConfig, certPem: in String, keyPem: in String): RedisConfig
  requires: certPem.length > 0 and keyPem.length > 0
{
  cfg.copy(tls = cfg.tls.copy(clientCertificatePem = certPem, clientKeyPem = keyPem))
}

/// `withClientIdentity` reading the two PEM files.
@experimental
pub func withClientIdentityFiles(
  cfg: in RedisConfig, certPath: in String, keyPath: in String
): Result[RedisConfig, RedisError] {
  match readPem("client certificate", certPath) {
    case Err(e) -> Err(e)
    case Ok(certPem) -> match readPem("client private key", keyPath) {
      case Err(e) -> Err(e)
      case Ok(keyPem) -> Ok(withClientIdentity(cfg, certPem, keyPem))
    }
  }
}

/// `cfg` accepting no TLS version below `version` (`Std.Tls.TlsVersion`:
/// `Tls12`, the default and the floor, or `Tls13`).
@experimental
pub func withMinTlsVersion(cfg: in RedisConfig, version: in Tls.TlsVersion): RedisConfig {
  cfg.copy(tls = cfg.tls.copy(minVersion = version))
}

/// `cfg` sending `serverName` as SNI and checking the server certificate
/// against it instead of against `host`: for a server reached by address, or
/// through a tunnel, whose certificate names something else.
@experimental
pub func withTlsServerName(cfg: in RedisConfig, serverName: in String): RedisConfig
  requires: serverName.length > 0
{
  cfg.copy(tls = cfg.tls.copy(serverName = serverName))
}

/// Request that the server certificate is not verified at all: neither its
/// chain nor its name. This alone does NOT disable verification. As for
/// `Std.Http`'s `withInsecureSkipVerify` (lyric-lang docs/61 section 4), the
/// environment variable `LYRIC_TLS_ALLOW_INSECURE=1` must also be set when
/// the client first connects:
///
/// | This called? | Variable set? | Effect |
/// |---|---|---|
/// | no | no | verification on |
/// | no | yes | verification on: the variable alone does nothing |
/// | yes | no | verification on, and a warning on standard error |
/// | yes | yes | verification off for this client |
///
/// Never use it in production.
@experimental
pub func withInsecureSkipVerify(cfg: in RedisConfig): RedisConfig {
  cfg.copy(tls = cfg.tls.copy(insecureSkipVerify = true))
}

val insecureAllowEnvVar: String = "LYRIC_TLS_ALLOW_INSECURE"

func hasTlsDetails(t: in RedisTlsOptions): Bool {
  val pinned = match t.minVersion { case Tls13 -> true; case Tls12 -> false }
  t.serverName.length > 0 or t.caCertificatePem.length > 0 or t.clientCertificatePem.length > 0 or
    t.clientKeyPem.length > 0 or
    t.insecureSkipVerify or
    pinned
}

func checkCaPem(pem: in String): Result[Unit, RedisError] {
  match Tls.Certificate.fromPem(pem) {
    case Ok(_) -> Ok(())
    case Err(e) -> Err(InvalidConfig(message = "TLS CA certificate: " + e.message()))
  }
}

func checkIdentityPem(certPem: in String, keyPem: in String): Result[Unit, RedisError] {
  match Tls.Identity.fromPem(certPem, keyPem) {
    case Ok(_) -> Ok(())
    case Err(e) -> Err(InvalidConfig(message = "TLS client certificate and key: " + e.message()))
  }
}

/// Check the TLS part of `cfg` without connecting: TLS options on a
/// configuration that does not enable TLS, a CA or client certificate that
/// does not parse, a private key in an unsupported format (`Std.Tls` loads
/// unencrypted PKCS#8 only), or a key that does not belong to the
/// certificate are `InvalidConfig`. `open` and the first command of a `lazy`
/// client run the same check.
@experimental
pub func checkTls(cfg: in RedisConfig): Result[Unit, RedisError] {
  val t = cfg.tls
  if not t.enabled {
    if hasTlsDetails(t) {
      return Err(
        InvalidConfig(message = "TLS options are set but the connection is not TLS: use a rediss:// URL or withTls")
      )
    }
    return Ok(())
  }
  if t.caCertificatePem.length > 0 {
    match checkCaPem(t.caCertificatePem) {
      case Err(e) -> return Err(e)
      case Ok(_) -> ()
    }
  }
  if t.clientCertificatePem.length > 0 {
    match checkIdentityPem(t.clientCertificatePem, t.clientKeyPem) {
      case Err(e) -> return Err(e)
      case Ok(_) -> ()
    }
  }
  Ok(())
}

/// Whether verification is really off for a client of `cfg`, given whether
/// the environment allows it: the dual-key policy of `withInsecureSkipVerify`
/// (`Std.Http.resolveInsecureVerifyPolicy`).
@pure
@experimental
pub func insecurePolicyFor(cfg: in RedisConfig, envAllows: in Bool): Http.InsecureVerifyPolicy {
  Http.resolveInsecureVerifyPolicy(cfg.tls.insecureSkipVerify, envAllows)
}

// The kernel's TLS state for `cfg`, built once per client; `None` for a plain
// connection.
func prepareTls(cfg: in RedisConfig): Result[Option[Kernel.TlsClient], RedisError] {
  match checkTls(cfg) {
    case Err(e) -> return Err(e)
    case Ok(_) -> ()
  }
  val t = cfg.tls
  if not t.enabled {
    return Ok(None)
  }
  val skipVerify = match insecurePolicyFor(cfg, Env.getVarOrDefault(insecureAllowEnvVar, "") == "1") {
    case VerificationDisabled -> true
    case VerificationEnabled -> false
    case VerificationEnabledWarn -> {
      Console.error(
        "[Redis] warning: withInsecureSkipVerify() requested but " + insecureAllowEnvVar +
          " is not set; TLS certificate verification remains ENABLED."
      )
      false
    }
  }
  val settings = Kernel.TlsSettings(
    caPem = t.caCertificatePem,
    certPem = t.clientCertificatePem,
    keyPem = t.clientKeyPem,
    tls13Only = match t.minVersion { case Tls13 -> true; case Tls12 -> false },
    skipVerify = skipVerify
  )
  match Kernel.tlsPrepare(settings) {
    case Ok(client) -> Ok(Some(value = client))
    case Err(m) -> Err(InvalidConfig(message = "TLS: " + m))
  }
}

func tlsServerName(cfg: in RedisConfig): String {
  if cfg.tls.serverName.length > 0 {
    cfg.tls.serverName
  } else {
    cfg.host
  }
}

// ─── Wire exchange ───────────────────────────────────────────────────────────
val readChunkBytes: Int = 16384

// One round trip on an open socket. `Replied` carries the bytes received
// after the reply; `Broken.notExecuted` is true only when the command cannot
// have run.
union Exchange {
  case Replied(value: Resp.RespValue, rest: slice[Byte])
  case Broken(error: RedisError, notExecuted: Bool)
}

// Read one reply, skipping RESP3 push messages, which may arrive at any time.
func readReply(sock: in Kernel.Sock, pending: in slice[Byte]): Exchange {
  var buf = pending
  var received = false
  while true {
    match Resp.decode(buf, 0) {
      case Decoded(value, next) -> {
        val rest: slice[Byte] = buf.slice(next, buf.length)
        match value {
          case RPush(_) -> {
            buf = rest
          }
          case _ -> return Replied(value = value, rest = rest)
        }
      }
      case Malformed(m) -> return Broken(error = ProtocolViolation(message = m), notExecuted = false)
      case Incomplete -> {
        match Kernel.sockRead(sock, readChunkBytes) {
          case Err(m) -> return Broken(error = ConnectionLost(message = m), notExecuted = false)
          case Ok(chunk) -> {
            if chunk.length == 0 {
              return Broken(
                error = ConnectionLost(message = "the server closed the connection"),
                notExecuted = not received and buf.length == 0
              )
            }
            received = true
            buf = buf.concat(chunk)
          }
        }
      }
    }
  }
  Broken(error = ConnectionLost(message = "unreachable"), notExecuted = false)
}

func exchange(sock: in Kernel.Sock, pending: in slice[Byte], request: in slice[Byte]): Exchange {
  match Kernel.sockWrite(sock, request) {
    case Err(m) -> Broken(error = ConnectionLost(message = m), notExecuted = true)
    case Ok(_) -> readReply(sock, pending)
  }
}

func bytesOfArgs(args: in slice[String]): slice[slice[Byte]] {
  val acc: List[slice[Byte]] = newList()
  for a in args {
    acc.add(encodeUtf8(a))
  }
  acc.toArray()
}

// AUTH / HELLO and SELECT on a new connection. Anything but success closes it.
func handshake(sock: in Kernel.Sock, cfg: in RedisConfig): Result[slice[Byte], RedisError] {
  var pending: slice[Byte] = []
  val steps: List[slice[String]] = newList()
  val hasPassword = cfg.password.length > 0
  val user = if cfg.username.length > 0 { cfg.username } else { "default" }
  if cfg.protocol == 3 {
    if hasPassword {
      steps.add(["HELLO", "3", "AUTH", user, cfg.password])
    } else {
      steps.add(["HELLO", "3"])
    }
  } else if hasPassword {
    if cfg.username.length > 0 {
      steps.add(["AUTH", cfg.username, cfg.password])
    } else {
      steps.add(["AUTH", cfg.password])
    }
  }
  if cfg.database != 0 {
    steps.add(["SELECT", cfg.database.toString()])
  }
  for step in steps {
    val name: String = step[0]
    match exchange(sock, pending, Resp.encodeCommand(step)) {
      case Broken(e, _) -> return Err(e)
      case Replied(value, rest) -> {
        pending = rest
        match value {
          case RError(m) -> {
            if name == "SELECT" {
              return Err(ServerError(message = m))
            }
            if name == "HELLO" and not isAuthError(m) {
              return Err(ServerError(message = "the server refused RESP3 (HELLO 3): " + m))
            }
            return Err(AuthFailed(message = m))
          }
          case _ -> ()
        }
      }
    }
  }
  Ok(pending)
}

func isAuthError(m: in String): Bool {
  m.startsWith("WRONGPASS") or m.startsWith("NOAUTH") or m.startsWith("NOPERM") or Str.contains(m, "invalid password") or
    Str.contains(m, "invalid username-password") or
    Str.contains(m, "without any password configured")
}

// Dial, and run the TLS handshake when `tls` is set.
func dial(cfg: in RedisConfig, tls: in Option[Kernel.TlsClient]): Result[Kernel.Sock, RedisError] {
  val endpoint = cfg.host + ":" + cfg.port.toString() + ": "
  match Kernel.sockConnect(cfg.host, cfg.port, cfg.connectTimeoutMs, cfg.ioTimeoutMs) {
    case Err(m) -> Err(ConnectFailed(message = endpoint + m))
    case Ok(plain) -> match tls {
      case None -> Ok(plain)
      case Some(t) -> match Kernel.sockStartTls(plain, t, tlsServerName(cfg), cfg.port) {
        case Ok(encrypted) -> Ok(encrypted)
        case Err(m) -> Err(TlsFailed(message = endpoint + m))
      }
    }
  }
}

func openLink(cfg: in RedisConfig, tls: in Option[Kernel.TlsClient]): Result[Link, RedisError] {
  match dial(cfg, tls) {
    case Err(e) -> Err(e)
    case Ok(sock) -> match handshake(sock, cfg) {
      case Ok(pending) -> Ok(Link(sock = sock, pending = pending))
      case Err(e) -> {
        Kernel.sockClose(sock)
        Err(e)
      }
    }
  }
}

record Link {
  sock: Kernel.Sock
  pending: slice[Byte]
}

func replyResult(value: in Resp.RespValue): Result[Resp.RespValue, RedisError] {
  match value {
    case RError(m) -> Err(ServerError(message = m))
    case _ -> Ok(value)
  }
}

// ─── Client ──────────────────────────────────────────────────────────────────
/// A connection to one Redis server, safe to share between threads (see the
/// package header). Build one with `connect`, `open` or `lazy`.
@experimental
pub protected type RedisClient {
  config: RedisConfig
  var link: Option[Link] = None
  var closed: Bool = false
  var connectCount: Int = 0
  // The parsed TLS state, built by the first connection and kept for the
  // client's life.
  var tlsReady: Bool = false
  var tls: Option[Kernel.TlsClient] = None
  func dropLink(): Unit {
    match link {
      case Some(l) -> Kernel.sockClose(l.sock)
      case None -> ()
    }
    link = None
  }
  func currentLink(): Result[Link, RedisError] {
    if not tlsReady {
      match prepareTls(config) {
        case Err(e) -> return Err(e)
        case Ok(prepared) -> {
          tls = prepared
          tlsReady = true
        }
      }
    }
    match link {
      case Some(l) -> Ok(l)
      case None -> match openLink(config, tls) {
        case Err(e) -> Err(e)
        case Ok(l) -> {
          link = Some(value = l)
          connectCount = connectCount + 1
          Ok(l)
        }
      }
    }
  }
  /// Open the connection now if it is not open (commands do this themselves).
  entry ensureConnected(): Result[Unit, RedisError] {
    if closed {
      return Err(ClientClosed)
    }
    match currentLink() {
      case Ok(_) -> Ok(())
      case Err(e) -> Err(e)
    }
  }
  /// Send one command given as binary-safe arguments and return its reply.
  /// An error reply is `Err(ServerError)`.
  entry execute(args: in slice[slice[Byte]]): Result[Resp.RespValue, RedisError]
    requires: args.length > 0
  {
    if closed {
      return Err(ClientClosed)
    }
    val request = Resp.encodeCommandBytes(args)
    var retried = false
    while true {
      val reused = match link { case Some(_) -> true; case None -> false }
      match currentLink() {
        case Err(e) -> return Err(e)
        case Ok(l) -> match exchange(l.sock, l.pending, request) {
          case Replied(value, rest) -> {
            link = Some(value = Link(sock = l.sock, pending = rest))
            return replyResult(value)
          }
          case Broken(e, notExecuted) -> {
            dropLink()
            if notExecuted and reused and not retried and config.retryStaleConnection {
              retried = true
            } else {
              return Err(e)
            }
          }
        }
      }
    }
    Err(ClientClosed)
  }
  /// Send several commands in one write and return their replies in order.
  /// Error replies are returned in place as `RError` values. The batch is not
  /// a transaction, and it is never retried.
  entry pipeline(commands: in slice[slice[slice[Byte]]]): Result[List[Resp.RespValue], RedisError]
    requires: commands.length > 0
  {
    if closed {
      return Err(ClientClosed)
    }
    var request: slice[Byte] = []
    for command in commands {
      if command.length == 0 {
        return Err(InvalidConfig(message = "a pipelined command needs at least its name"))
      }
      request = request.concat(Resp.encodeCommandBytes(command))
    }
    match currentLink() {
      case Err(e) -> return Err(e)
      case Ok(l) -> {
        match Kernel.sockWrite(l.sock, request) {
          case Err(m) -> {
            dropLink()
            return Err(ConnectionLost(message = m))
          }
          case Ok(_) -> ()
        }
        val replies: List[Resp.RespValue] = newList()
        var pending: slice[Byte] = l.pending
        var i = 0
        while i < commands.length {
          match readReply(l.sock, pending) {
            case Replied(value, rest) -> {
              replies.add(value)
              pending = rest
            }
            case Broken(e, _) -> {
              dropLink()
              return Err(e)
            }
          }
          i = i + 1
        }
        link = Some(value = Link(sock = l.sock, pending = pending))
        Ok(replies)
      }
    }
  }
  /// Close the connection without closing the client: the next command dials
  /// again.
  entry disconnect(): Unit {
    dropLink()
  }
  /// Close the client for good. Later commands return `Err(ClientClosed)`.
  entry close(): Unit {
    dropLink()
    closed = true
  }
  /// True while a connection is open.
  func isConnected(): Bool {
    match link {
      case Some(_) -> true
      case None -> false
    }
  }
  /// How many times this client has opened a connection.
  func connections(): Int {
    connectCount
  }
}

/// A client for `cfg`, connected and authenticated before it is returned, so
/// an unreachable server, a failed TLS handshake or a wrong password is
/// reported here.
@experimental
pub func open(cfg: in RedisConfig): Result[RedisClient, RedisError] {
  val client = RedisClient(config = cfg)
  match client.ensureConnected() {
    case Ok(_) -> Ok(client)
    case Err(e) -> Err(e)
  }
}

/// A client for `cfg` that connects on its first command.
@experimental
pub func lazy(cfg: in RedisConfig): RedisClient {
  RedisClient(config = cfg)
}

/// `open` for a `redis://` or `rediss://` URL.
@experimental
pub func connect(url: in String): Result[RedisClient, RedisError] {
  match configFromUrl(url) {
    case Err(e) -> Err(e)
    case Ok(cfg) -> open(cfg)
  }
}

// ─── Reply conversion ────────────────────────────────────────────────────────
func unexpected(command: in String, wanted: in String, got: in Resp.RespValue): RedisError {
  UnexpectedReply(message = command + " expected " + wanted + ", got " + Resp.kindOf(got))
}

func textOfBytes(command: in String, bytes: in slice[Byte]): Result[String, RedisError] {
  match tryDecodeUtf8(bytes) {
    case Some(s) -> Ok(s)
    case None -> Err(UnexpectedReply(message = command + " returned a value that is not UTF-8 text"))
  }
}

func toOptText(command: in String, value: in Resp.RespValue): Result[Option[String], RedisError] {
  match value {
    case RNull -> Ok(None)
    case RBulk(b) -> match textOfBytes(command, b) { case Ok(s) -> Ok(Some(value = s)); case Err(e) -> Err(e) }
    case RVerbatim(_, b) -> match textOfBytes(command, b) { case Ok(s) -> Ok(Some(value = s)); case Err(e) -> Err(e) }
    case RSimple(t) -> Ok(Some(value = t))
    case other -> Err(unexpected(command, "a string or null", other))
  }
}

func toText(command: in String, value: in Resp.RespValue): Result[String, RedisError] {
  match toOptText(command, value) {
    case Ok(Some(s)) -> Ok(s)
    case Ok(None) -> Err(unexpected(command, "a string", value))
    case Err(e) -> Err(e)
  }
}

func toLong(command: in String, value: in Resp.RespValue): Result[Long, RedisError] {
  match value {
    case RInt(n) -> Ok(n)
    case other -> Err(unexpected(command, "an integer", other))
  }
}

func toBool(command: in String, value: in Resp.RespValue): Result[Bool, RedisError] {
  match value {
    case RInt(n) -> Ok(n != 0i64)
    case RBool(b) -> Ok(b)
    case other -> Err(unexpected(command, "an integer", other))
  }
}

func toUnit(command: in String, value: in Resp.RespValue): Result[Unit, RedisError] {
  match value {
    case RSimple(_) -> Ok(())
    case other -> Err(unexpected(command, "a status", other))
  }
}

func itemsOf(command: in String, value: in Resp.RespValue): Result[List[Resp.RespValue], RedisError] {
  match value {
    case RArray(items) -> Ok(items)
    case RSet(items) -> Ok(items)
    case RMap(entries) -> Ok(entries)
    case other -> Err(unexpected(command, "an array", other))
  }
}

func toTextList(command: in String, value: in Resp.RespValue): Result[List[String], RedisError] {
  match itemsOf(command, value) {
    case Err(e) -> Err(e)
    case Ok(items) -> {
      val acc: List[String] = newList()
      for item in items {
        match toText(command, item) {
          case Ok(s) -> acc.add(s)
          case Err(e) -> return Err(e)
        }
      }
      Ok(acc)
    }
  }
}

// ─── Commands ────────────────────────────────────────────────────────────────
/// Send any command, arguments as text (UTF-8).
@experimental
pub func command(client: in RedisClient, args: in slice[String]): Result[Resp.RespValue, RedisError]
  requires: args.length > 0
{
  client.execute(bytesOfArgs(args))
}

/// `PING`: `Ok` when the server answers `PONG`.
@experimental
pub func ping(client: in RedisClient): Result[Unit, RedisError] {
  match command(client, ["PING"]) {
    case Err(e) -> Err(e)
    case Ok(v) -> match toText("PING", v) {
      case Ok(t) -> if t == "PONG" {
        Ok(())
      } else {
        Err(UnexpectedReply(message = "PING answered " + t))
      }
      case Err(e) -> Err(e)
    }
  }
}

/// `GET key`: the value as text, `None` when the key does not exist.
@experimental
pub func get(client: in RedisClient, key: in String): Result[Option[String], RedisError] {
  match command(client, ["GET", key]) {
    case Err(e) -> Err(e)
    case Ok(v) -> toOptText("GET", v)
  }
}

/// `GET key` for a binary value.
@experimental
pub func getBytes(client: in RedisClient, key: in String): Result[Option[slice[Byte]], RedisError] {
  match command(client, ["GET", key]) {
    case Err(e) -> Err(e)
    case Ok(RNull) -> Ok(None)
    case Ok(RBulk(b)) -> Ok(Some(value = b))
    case Ok(other) -> Err(unexpected("GET", "a bulk string or null", other))
  }
}

/// `SET key value`: store without an expiry (an existing expiry is removed).
@experimental
pub func set(client: in RedisClient, key: in String, value: in String): Result[Unit, RedisError] {
  match command(client, ["SET", key, value]) {
    case Err(e) -> Err(e)
    case Ok(v) -> toUnit("SET", v)
  }
}

/// `SET key value` for a binary value.
@experimental
pub func setBytes(client: in RedisClient, key: in String, value: in slice[Byte]): Result[Unit, RedisError] {
  val args: slice[slice[Byte]] = [encodeUtf8("SET"), encodeUtf8(key), value]
  match client.execute(args) {
    case Err(e) -> Err(e)
    case Ok(v) -> toUnit("SET", v)
  }
}

/// `SET key value EX seconds`.
@experimental
pub func setEx(client: in RedisClient, key: in String, value: in String, seconds: in Long): Result[Unit, RedisError]
  requires: seconds > 0i64
{
  match command(client, ["SET", key, value, "EX", seconds.toString()]) {
    case Err(e) -> Err(e)
    case Ok(v) -> toUnit("SET", v)
  }
}

/// `SET key value PX milliseconds`.
@experimental
pub func setPx(client: in RedisClient, key: in String, value: in String, millis: in Long): Result[Unit, RedisError]
  requires: millis > 0i64
{
  match command(client, ["SET", key, value, "PX", millis.toString()]) {
    case Err(e) -> Err(e)
    case Ok(v) -> toUnit("SET", v)
  }
}

/// `SET key value NX [PX milliseconds]`: store only when the key does not
/// exist. `ttlMillis = 0` stores without an expiry. `Ok(true)` when stored.
@experimental
pub func setNx(client: in RedisClient, key: in String, value: in String, ttlMillis: in Long): Result[Bool, RedisError]
  requires: ttlMillis >= 0i64
{
  val args: slice[String] = if ttlMillis > 0i64 {
    ["SET", key, value, "NX", "PX", ttlMillis.toString()]
  } else {
    ["SET", key, value, "NX"]
  }
  match command(client, args) {
    case Err(e) -> Err(e)
    case Ok(RNull) -> Ok(false)
    case Ok(RSimple(_)) -> Ok(true)
    case Ok(other) -> Err(unexpected("SET NX", "a status or null", other))
  }
}

/// `DEL key...`: how many of the keys existed.
@experimental
pub func del(client: in RedisClient, keys: in slice[String]): Result[Long, RedisError]
  requires: keys.length > 0
{
  val args: List[String] = newList()
  args.add("DEL")
  for k in keys {
    args.add(k)
  }
  match command(client, args.toArray()) {
    case Err(e) -> Err(e)
    case Ok(v) -> toLong("DEL", v)
  }
}

/// `EXISTS key`.
@experimental
pub func exists(client: in RedisClient, key: in String): Result[Bool, RedisError] {
  match command(client, ["EXISTS", key]) {
    case Err(e) -> Err(e)
    case Ok(v) -> toBool("EXISTS", v)
  }
}

/// `EXPIRE key seconds`: `Ok(false)` when the key does not exist.
@experimental
pub func expire(client: in RedisClient, key: in String, seconds: in Long): Result[Bool, RedisError] {
  match command(client, ["EXPIRE", key, seconds.toString()]) {
    case Err(e) -> Err(e)
    case Ok(v) -> toBool("EXPIRE", v)
  }
}

/// `PEXPIRE key milliseconds`: `Ok(false)` when the key does not exist.
@experimental
pub func pexpire(client: in RedisClient, key: in String, millis: in Long): Result[Bool, RedisError] {
  match command(client, ["PEXPIRE", key, millis.toString()]) {
    case Err(e) -> Err(e)
    case Ok(v) -> toBool("PEXPIRE", v)
  }
}

/// `PERSIST key`: remove the expiry. `Ok(false)` when there was none.
@experimental
pub func persist(client: in RedisClient, key: in String): Result[Bool, RedisError] {
  match command(client, ["PERSIST", key]) {
    case Err(e) -> Err(e)
    case Ok(v) -> toBool("PERSIST", v)
  }
}

/// `TTL key`: seconds to live; `-1` for a key without an expiry, `-2` for a
/// key that does not exist.
@experimental
pub func ttl(client: in RedisClient, key: in String): Result[Long, RedisError] {
  match command(client, ["TTL", key]) {
    case Err(e) -> Err(e)
    case Ok(v) -> toLong("TTL", v)
  }
}

/// `PTTL key`: `ttl` in milliseconds.
@experimental
pub func pttl(client: in RedisClient, key: in String): Result[Long, RedisError] {
  match command(client, ["PTTL", key]) {
    case Err(e) -> Err(e)
    case Ok(v) -> toLong("PTTL", v)
  }
}

/// `INCRBY key delta`: the value after the increment.
@experimental
pub func incrBy(client: in RedisClient, key: in String, delta: in Long): Result[Long, RedisError] {
  match command(client, ["INCRBY", key, delta.toString()]) {
    case Err(e) -> Err(e)
    case Ok(v) -> toLong("INCRBY", v)
  }
}

/// `HGET key field`.
@experimental
pub func hget(client: in RedisClient, key: in String, field: in String): Result[Option[String], RedisError] {
  match command(client, ["HGET", key, field]) {
    case Err(e) -> Err(e)
    case Ok(v) -> toOptText("HGET", v)
  }
}

/// `HSET key field value`: `Ok(true)` when the field is new.
@experimental
pub func hset(client: in RedisClient, key: in String, field: in String, value: in String): Result[Bool, RedisError] {
  match command(client, ["HSET", key, field, value]) {
    case Err(e) -> Err(e)
    case Ok(v) -> toBool("HSET", v)
  }
}

/// `HDEL key field...`: how many of the fields existed.
@experimental
pub func hdel(client: in RedisClient, key: in String, fields: in slice[String]): Result[Long, RedisError]
  requires: fields.length > 0
{
  val args: List[String] = newList()
  args.add("HDEL")
  args.add(key)
  for f in fields {
    args.add(f)
  }
  match command(client, args.toArray()) {
    case Err(e) -> Err(e)
    case Ok(v) -> toLong("HDEL", v)
  }
}

/// `HGETALL key`: every field and value; empty when the key does not exist.
@experimental
pub func hgetAll(client: in RedisClient, key: in String): Result[Map[String, String], RedisError] {
  match command(client, ["HGETALL", key]) {
    case Err(e) -> Err(e)
    case Ok(v) -> match toTextList("HGETALL", v) {
      case Err(e) -> Err(e)
      case Ok(flat) -> {
        if flat.count % 2 != 0 {
          return Err(UnexpectedReply(message = "HGETALL returned an odd number of items"))
        }
        val acc: Map[String, String] = newMap()
        var i = 0
        while i < flat.count {
          acc[flat[i]] = flat[i + 1]
          i = i + 2
        }
        Ok(acc)
      }
    }
  }
}

/// One page of a `SCAN`: the keys found and the cursor for the next page
/// (`"0"` when the iteration is complete).
@experimental
pub record ScanPage {
  cursor: String
  keys: List[String]
}

/// `SCAN cursor [MATCH pattern] [COUNT count]`. Start with cursor `"0"`;
/// `pattern = ""` matches every key; `count = 0` leaves the server's default.
@experimental
pub func scan(
  client: in RedisClient, cursor: in String, pattern: in String, count: in Int
): Result[ScanPage, RedisError]
  requires: count >= 0
{
  val args: List[String] = newList()
  args.add("SCAN")
  args.add(cursor)
  if pattern.length > 0 {
    args.add("MATCH")
    args.add(pattern)
  }
  if count > 0 {
    args.add("COUNT")
    args.add(count.toString())
  }
  match command(client, args.toArray()) {
    case Err(e) -> Err(e)
    case Ok(v) -> match itemsOf("SCAN", v) {
      case Err(e) -> Err(e)
      case Ok(parts) -> {
        if parts.count != 2 {
          return Err(UnexpectedReply(message = "SCAN returned " + parts.count.toString() + " items"))
        }
        match toText("SCAN", parts[0]) {
          case Err(e) -> Err(e)
          case Ok(next) -> match toTextList("SCAN", parts[1]) {
            case Err(e) -> Err(e)
            case Ok(keys) -> Ok(ScanPage(cursor = next, keys = keys))
          }
        }
      }
    }
  }
}

/// Every key matching `pattern`, collected over a complete `SCAN` iteration.
/// A key may be reported once even when the keyspace changes during the scan;
/// duplicates the server returns are removed.
@experimental
pub func scanAll(client: in RedisClient, pattern: in String): Result[List[String], RedisError] {
  val seen: Map[String, Bool] = newMap()
  val acc: List[String] = newList()
  var cursor = "0"
  var done = false
  while not done {
    match scan(client, cursor, pattern, 500) {
      case Err(e) -> return Err(e)
      case Ok(page) -> {
        for k in page.keys {
          if not seen.containsKey(k) {
            seen[k] = true
            acc.add(k)
          }
        }
        cursor = page.cursor
        done = cursor == "0"
      }
    }
  }
  Ok(acc)
}

/// The literal `text` as a glob pattern for `SCAN MATCH` / `KEYS`: the glob
/// metacharacters are escaped.
@pure
@experimental
pub func escapeGlob(text: in String): String {
  var acc = ""
  var i = 0
  while i < text.length {
    val c = text.substring(i, 1)
    if c == "*" or c == "?" or c == "[" or c == "]" or c == "\\" {
      acc = acc + "\\"
    }
    acc = acc + c
    i = i + 1
  }
  acc
}

/// `FLUSHDB`: delete every key of the selected database.
@experimental
pub func flushDb(client: in RedisClient): Result[Unit, RedisError] {
  match command(client, ["FLUSHDB"]) {
    case Err(e) -> Err(e)
    case Ok(v) -> toUnit("FLUSHDB", v)
  }
}

/// `EVAL script numkeys key... arg...`: run a Lua script atomically.
@experimental
pub func eval(
  client: in RedisClient, script: in String, keys: in slice[String], args: in slice[String]
): Result[Resp.RespValue, RedisError] {
  val all: List[String] = newList()
  all.add("EVAL")
  all.add(script)
  all.add(keys.length.toString())
  for k in keys {
    all.add(k)
  }
  for a in args {
    all.add(a)
  }
  command(client, all.toArray())
}
