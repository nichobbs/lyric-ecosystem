//! Cache.Memcached.Client - a memcached client written in Lyric, speaking the
//! meta text protocol (`Cache.Memcached.Meta`) over `Net.Tcp`.
//!
//! It is the connection under `Cache.Memcached`: one server, one connection,
//! requests made of meta commands and their replies. Every operation returns
//! `Result[_, MemcachedError]`; nothing throws.
//!
//! ```
//! import Cache.Memcached.Client as Memcached
//!
//! match Memcached.connect("memcached://cache.internal:11211") {
//!   case Err(e) -> println(e.message())
//!   case Ok(client) -> {
//!     match Memcached.ping(client) {
//!       case Ok(_) -> println("alive")
//!       case Err(e) -> println(e.message())
//!     }
//!     client.close()
//!   }
//! }
//! ```
//!
//! Server: memcached 1.6.10 or later (the first release with the stable meta
//! protocol and base64 keys). The version is read when a connection is
//! opened, and an older server is `UnsupportedServer`.
//!
//! Thread safety: `MemcachedClient` is a `protected type` holding ONE
//! connection. Each request runs under the client's lock from the first byte
//! written to the last byte of the last reply, so a client may be shared
//! between threads; requests from different threads are serialized.
//!
//! TLS: a `memcached+tls://` URL connects over TLS (a server started with
//! `-Z`), verifying the server certificate against the system trust store
//! and its name against the host; `withTlsOptions` takes a
//! `Net.Tls.TlsOptions` for everything else.
//!
//! Authentication: `user:password` in the URL (or `withCredentials`) are
//! sent as memcached's ASCII authentication, for a server started with
//! `-Y <file>`. SASL (`-S`) belongs to the binary protocol and is not
//! supported.
//!
//! Reconnection: a connection that breaks is closed and the next request
//! dials again. A request is repeated once on a fresh connection only when
//! it did not run: the write failed, or the server had already closed the
//! previously idle connection (end of stream before any reply byte). A
//! timeout or a reset in the middle of the replies is `ConnectionLost` and
//! never retried. A `CLIENT_ERROR` or `ERROR` reply also closes the
//! connection, because memcached may then read the rest of the request as
//! commands of its own.
@runtime_checked
@io
package Cache.Memcached.Client

import Std.Core
import Std.Collections
import Std.Console as Console
import Std.Parse
import Std.String as Str
import Cache.Memcached.Meta as Meta
import Cache.Memcached.Url as Url
import Net.Tcp as Tcp
import Net.Tls as NetTls

// ─── Errors ──────────────────────────────────────────────────────────────────
/// Why a memcached operation failed.
@experimental
pub union MemcachedError {
  /// The configuration, URL or namespace is unusable.
  case InvalidConfig(message: String)
  /// The TCP connection could not be opened.
  case ConnectFailed(message: String)
  /// The TLS handshake failed.
  case TlsFailed(message: String)
  /// The server refused the credentials, or requires some and got none.
  case AuthFailed(message: String)
  /// The server is older than memcached 1.6.10 or does not speak the meta
  /// protocol.
  case UnsupportedServer(message: String)
  /// The connection broke (closed, reset or timed out) during a request. The
  /// request may or may not have run.
  case ConnectionLost(message: String)
  /// The server sent bytes that are not a memcached reply.
  case ProtocolViolation(message: String)
  /// The server refused the request as malformed (`CLIENT_ERROR`, `ERROR`).
  case RequestRefused(message: String)
  /// The server could not carry the request out (`SERVER_ERROR`), for
  /// example because it is out of memory.
  case ServerFailure(message: String)
  /// The value is larger than an item may be (1 MiB unless the server was
  /// started with `-I`). Nothing was stored.
  case ValueTooLarge(message: String)
  /// The reply is not one the command produces, or a stored value is not
  /// UTF-8 text.
  case UnexpectedReply(message: String)
  /// The namespace was cleared again and again while one operation ran.
  case NamespaceContended(message: String)
  /// The client was closed with `close()`.
  case ClientClosed
}

/// The error as text. Never contains the password.
@experimental
pub func MemcachedError.message(e: in MemcachedError): String {
  match e {
    case InvalidConfig(m) -> "invalid memcached configuration: " + m
    case ConnectFailed(m) -> "cannot connect to memcached: " + m
    case TlsFailed(m) -> "memcached TLS handshake failed: " + m
    case AuthFailed(m) -> "memcached authentication failed: " + m
    case UnsupportedServer(m) -> "unsupported memcached server: " + m
    case ConnectionLost(m) -> "memcached connection lost: " + m
    case ProtocolViolation(m) -> "memcached protocol violation: " + m
    case RequestRefused(m) -> "memcached refused the request: " + m
    case ServerFailure(m) -> "memcached server error: " + m
    case ValueTooLarge(m) -> "value too large for memcached: " + m
    case UnexpectedReply(m) -> "unexpected memcached reply: " + m
    case NamespaceContended(m) -> "memcached namespace contended: " + m
    case ClientClosed -> "the memcached client is closed"
  }
}

/// A stable identifier for the kind of error.
@experimental
pub func MemcachedError.code(e: in MemcachedError): String {
  match e {
    case InvalidConfig(_) -> "INVALID_CONFIG"
    case ConnectFailed(_) -> "CONNECT_FAILED"
    case TlsFailed(_) -> "TLS_FAILED"
    case AuthFailed(_) -> "AUTH_FAILED"
    case UnsupportedServer(_) -> "UNSUPPORTED_SERVER"
    case ConnectionLost(_) -> "CONNECTION_LOST"
    case ProtocolViolation(_) -> "PROTOCOL_VIOLATION"
    case RequestRefused(_) -> "REQUEST_REFUSED"
    case ServerFailure(_) -> "SERVER_FAILURE"
    case ValueTooLarge(_) -> "VALUE_TOO_LARGE"
    case UnexpectedReply(_) -> "UNEXPECTED_REPLY"
    case NamespaceContended(_) -> "NAMESPACE_CONTENDED"
    case ClientClosed -> "CLIENT_CLOSED"
  }
}

// ─── Configuration ───────────────────────────────────────────────────────────
/// How to reach and talk to one memcached server.
@experimental
pub record MemcachedConfig {
  host: String
  port: Int
  /// User name of the ASCII authentication; `""` sends no credentials.
  username: String
  /// Its password.
  password: String
  /// Longest wait for the TCP connection, in milliseconds (`0` = no limit).
  connectTimeoutMs: Int
  /// Longest wait for one read or write, in milliseconds (`0` = no limit).
  /// It also bounds the TLS handshake as a whole.
  ioTimeoutMs: Int
  /// Repeat a request once on a fresh connection when the server had closed
  /// the idle one before the request ran.
  retryStaleConnection: Bool
  /// The largest value, in bytes, that is sent to the server at all: the
  /// server's item size limit (`-I`, 1 MiB by default). A larger value is
  /// `ValueTooLarge` without a round trip.
  maxValueBytes: Int
  /// TLS: off unless the URL is `memcached+tls://` or the options enable it.
  tls: NetTls.TlsOptions
  invariant: host.length > 0
  invariant: port >= 1 and port <= 65535
  invariant: connectTimeoutMs >= 0 and ioTimeoutMs >= 0
  invariant: maxValueBytes >= 1
  invariant: (username.length == 0) == (password.length == 0)
}

/// memcached's default item size limit: 1 MiB.
@experimental
pub func defaultMaxValueBytes(): Int {
  1048576
}

/// A configuration for `host:port` with no credentials, five-second
/// timeouts, the 1 MiB value limit and no TLS.
@experimental
pub func configFor(host: in String, port: in Int): MemcachedConfig
  requires: host.length > 0
  requires: port >= 1 and port <= 65535
{
  MemcachedConfig(
    host = host,
    port = port,
    username = "",
    password = "",
    connectTimeoutMs = 5000,
    ioTimeoutMs = 5000,
    retryStaleConnection = true,
    maxValueBytes = defaultMaxValueBytes(),
    tls = NetTls.plainTcp()
  )
}

/// The configuration a `memcached://[user:password@]host[:port]` URL names
/// (see `Cache.Memcached.Url`). A `memcached+tls://` URL switches TLS on
/// with the defaults: the system trust store and the URL's host as the
/// server name.
@experimental
pub func configFromUrl(url: in String): Result[MemcachedConfig, MemcachedError] {
  match Url.parseMemcachedUrl(url) {
    case Err(e) -> Err(InvalidConfig(message = e))
    case Ok(u) -> {
      val base = configFor(u.host, u.port)
      val tls = if u.tls { NetTls.plainTcp().withTls() } else { NetTls.plainTcp() }
      Ok(base.copy(username = u.username, password = u.password, tls = tls))
    }
  }
}

/// `cfg` with other timeouts (milliseconds, `0` = no limit).
@experimental
pub func withTimeouts(cfg: in MemcachedConfig, connectTimeoutMs: in Int, ioTimeoutMs: in Int): MemcachedConfig
  requires: connectTimeoutMs >= 0 and ioTimeoutMs >= 0
{
  cfg.copy(connectTimeoutMs = connectTimeoutMs, ioTimeoutMs = ioTimeoutMs)
}

/// `cfg` with the credentials of the ASCII authentication; two empty strings
/// send none. What memcached accepts in them is checked by `checkConfig`.
@experimental
pub func withCredentials(cfg: in MemcachedConfig, username: in String, password: in String): MemcachedConfig
  requires: (username.length == 0) == (password.length == 0)
{
  cfg.copy(username = username, password = password)
}

/// `cfg` with the stale-connection retry switched on or off.
@experimental
pub func withStaleRetry(cfg: in MemcachedConfig, retry: in Bool): MemcachedConfig {
  cfg.copy(retryStaleConnection = retry)
}

/// `cfg` for a server whose item size limit is `maxValueBytes`
/// (`memcached -I`).
@experimental
pub func withMaxValueBytes(cfg: in MemcachedConfig, maxValueBytes: in Int): MemcachedConfig
  requires: maxValueBytes >= 1
{
  cfg.copy(maxValueBytes = maxValueBytes)
}

/// `cfg` with the TLS options `tls`: build them from `Net.Tls.plainTcp()`
/// with `withTls()`, `withCaCertificate`, `withExclusiveCaCertificate`,
/// `withClientIdentity`, `withTlsServerName`, `withMinTlsVersion` and
/// `withInsecureSkipVerify`, or read them with `Net.Tls.tlsOptionsFromEnv`.
@experimental
pub func withTlsOptions(cfg: in MemcachedConfig, tls: in NetTls.TlsOptions): MemcachedConfig {
  cfg.copy(tls = tls)
}

// memcached reads the authentication token as `<user> <password>` and its
// file as `user:password` lines.
func credentialProblem(what: in String, text: in String, colonAllowed: in Bool): String {
  var i = 0
  while i < text.length {
    val unit = text.codeUnitAt(i)
    if unit <= 32 or unit == 127 {
      return "the " + what + " contains a space or a control character, which memcached cannot accept"
    }
    if unit == 58 and not colonAllowed {
      return "the " + what + " contains ':', which memcached cannot accept"
    }
    i = i + 1
  }
  ""
}

/// Check `cfg` without connecting: TLS options on a configuration that does
/// not enable TLS, a certificate or key that does not load, or credentials
/// memcached cannot accept are `InvalidConfig`. `open` and the first request
/// of a `lazy` client run the same check.
@experimental
pub func checkConfig(cfg: in MemcachedConfig): Result[Unit, MemcachedError] {
  match NetTls.validateTlsOptions(cfg.tls) {
    case Ok(_) -> ()
    case Err(problem) -> return Err(InvalidConfig(message = NetTls.tlsProblemText(problem, "a memcached+tls:// URL")))
  }
  val userProblem = credentialProblem("user name", cfg.username, false)
  if userProblem.length > 0 {
    return Err(InvalidConfig(message = userProblem))
  }
  val passwordProblem = credentialProblem("password", cfg.password, true)
  if passwordProblem.length > 0 {
    return Err(InvalidConfig(message = passwordProblem))
  }
  Ok(())
}

/// The oldest memcached this client talks to, as `major.minor.patch`.
@experimental
pub func minimumServerVersion(): String {
  "1.6.10"
}

func leadingNumber(text: in String): Option[Int] {
  var stop = 0
  while stop < text.length and text.codeUnitAt(stop) >= 48 and text.codeUnitAt(stop) <= 57 {
    stop = stop + 1
  }
  if stop == 0 or stop > 6 {
    return None
  }
  parseOptInt(Str.substring(text, 0, stop))
}

/// Whether a server that reports `version` is recent enough. A version text
/// that does not start with `major.minor.patch` numbers cannot be judged and
/// is accepted.
@pure
@experimental
pub func versionSupported(version: in String): Bool {
  val parts = version.split(".")
  if parts.length < 3 {
    return true
  }
  val major = match leadingNumber(parts[0]) { case Some(n) -> n; case None -> return true }
  val minor = match leadingNumber(parts[1]) { case Some(n) -> n; case None -> return true }
  val patch = match leadingNumber(parts[2]) { case Some(n) -> n; case None -> return true }
  if major != 1 {
    return major > 1
  }
  if minor != 6 {
    return minor > 6
  }
  patch >= 10
}

// ─── Wire exchange ───────────────────────────────────────────────────────────
val readChunkBytes: Int = 65536

// How long one read waits when the configuration sets no limit; the read is
// then repeated.
val unboundedReadSliceMs: Int = 60000

// What reading the replies of one request produced. `Broken.notExecuted` is
// true only when the request cannot have run.
union Exchange {
  case Replied(replies: List[Meta.MetaReply], rest: slice[Byte])
  case Refused(error: MemcachedError)
  case Broken(error: MemcachedError, notExecuted: Bool)
}

// The next bytes from the server; an empty slice means it closed the
// connection.
func readChunk(sock: in Tcp.Sock, ioTimeoutMs: in Int): Result[slice[Byte], String] {
  val wait = if ioTimeoutMs > 0 { ioTimeoutMs } else { unboundedReadSliceMs }
  while true {
    match Tcp.sockRead(sock, readChunkBytes, wait) {
      case Err(m) -> return Err(m)
      case Ok(Tcp.Data(bytes)) -> return Ok(bytes)
      case Ok(Tcp.Eof) -> {
        val closed: slice[Byte] = []
        return Ok(closed)
      }
      case Ok(Tcp.TimedOut) -> {
        if ioTimeoutMs > 0 {
          return Err("timed out: the server sent nothing for " + ioTimeoutMs.toString() + " ms")
        }
      }
    }
  }
  Err("unreachable")
}

func refusal(message: in String): MemcachedError {
  if message == "unauthenticated" {
    AuthFailed(message = "the server requires authentication and the client has no credentials")
  } else {
    RequestRefused(message = message)
  }
}

// Read `expected` replies, or, with `untilNoOp`, the replies up to an `MN`
// (which is not returned).
func readReplies(
  sock: in Tcp.Sock, pending: in slice[Byte], expected: in Int, untilNoOp: in Bool, ioTimeoutMs: in Int
): Exchange {
  var buf = pending
  var received = false
  val replies: List[Meta.MetaReply] = newList()
  while untilNoOp or replies.count < expected {
    match Meta.decode(buf, 0) {
      case Meta.Decoded(reply, next) -> {
        buf = buf.slice(next, buf.length)
        match reply {
          case Meta.ClientError(m) -> return Refused(error = refusal(m))
          case Meta.UnknownCommand -> return Refused(error = RequestRefused(message = "the server does not know the command"))
          case Meta.NoOp -> {
            if untilNoOp {
              return Replied(replies = replies, rest = buf)
            }
            replies.add(reply)
          }
          case _ -> replies.add(reply)
        }
      }
      case Meta.Malformed(m) -> return Broken(error = ProtocolViolation(message = m), notExecuted = false)
      case Meta.Incomplete -> {
        match readChunk(sock, ioTimeoutMs) {
          case Err(m) -> return Broken(error = ConnectionLost(message = m), notExecuted = false)
          case Ok(chunk) -> {
            if chunk.length == 0 {
              return Broken(
                error = ConnectionLost(message = "the server closed the connection"),
                notExecuted = not received and buf.length == 0 and replies.count == 0
              )
            }
            received = true
            buf = buf.concat(chunk)
          }
        }
      }
    }
  }
  Replied(replies = replies, rest = buf)
}

func exchange(
  sock: in Tcp.Sock,
  pending: in slice[Byte],
  request: in slice[Byte],
  expected: in Int,
  untilNoOp: in Bool,
  ioTimeoutMs: in Int,
): Exchange {
  match Tcp.sockWriteWithin(sock, request, ioTimeoutMs) {
    case Err(m) -> Broken(error = ConnectionLost(message = m), notExecuted = true)
    case Ok(_) -> readReplies(sock, pending, expected, untilNoOp, ioTimeoutMs)
  }
}

record Link {
  sock: Tcp.Sock
  pending: slice[Byte]
  version: String
}

// The ASCII authentication, when credentials are configured, and the version
// check. Anything but success closes the connection.
func handshake(sock: in Tcp.Sock, cfg: in MemcachedConfig): Result[Link, MemcachedError] {
  var pending: slice[Byte] = []
  if cfg.username.length > 0 {
    match exchange(sock, pending, Meta.encodeAuth(cfg.username, cfg.password), 1, false, cfg.ioTimeoutMs) {
      case Broken(e, _) -> return Err(e)
      case Refused(_) -> return Err(AuthFailed(message = "the server refused the user name and password"))
      case Replied(replies, rest) -> {
        pending = rest
        match replies[0] {
          case Meta.Stored -> ()
          case other -> return Err(UnexpectedReply(message = "the authentication was answered with " + Meta.statusOf(other)))
        }
      }
    }
  }
  match exchange(sock, pending, Meta.encodeVersion(), 1, false, cfg.ioTimeoutMs) {
    case Broken(e, _) -> Err(e)
    case Refused(e) -> match e {
      case AuthFailed(_) -> Err(e)
      case _ -> Err(UnsupportedServer(message = "the server does not answer the version command"))
    }
    case Replied(replies, rest) -> match replies[0] {
      case Meta.Version(text) -> if versionSupported(text) {
        Ok(Link(sock = sock, pending = rest, version = text))
      } else {
        Err(
          UnsupportedServer(
            message = "the server is memcached " + text + "; the meta protocol with base64 keys needs " +
              minimumServerVersion() +
              " or later"
          )
        )
      }
      case other -> Err(
        UnexpectedReply(
          message = "version was answered with " + Meta.statusOf(other)
        )
      )
    }
  }
}

// What a client's TLS handshakes use, decided once per client.
record TlsPlan {
  options: NetTls.TlsOptions
  // The insecure policy as it stood when the client first connected.
  skipVerify: Bool
}

func prepareTls(cfg: in MemcachedConfig): Result[Option[TlsPlan], MemcachedError] {
  match checkConfig(cfg) {
    case Err(e) -> return Err(e)
    case Ok(_) -> ()
  }
  if not cfg.tls.enabled {
    return Ok(None)
  }
  val warning = NetTls.insecureIgnoredWarning(cfg.tls, "Cache.Memcached")
  if warning.length > 0 {
    Console.error(warning)
  }
  Ok(Some(value = TlsPlan(options = cfg.tls, skipVerify = NetTls.skipsVerification(cfg.tls))))
}

// Dial, and run the TLS handshake when `tls` is set.
func dial(cfg: in MemcachedConfig, tls: in Option[TlsPlan]): Result[Tcp.Sock, MemcachedError] {
  val endpoint = cfg.host + ":" + cfg.port.toString() + ": "
  match Tcp.sockConnect(cfg.host, cfg.port, cfg.connectTimeoutMs) {
    case Err(m) -> Err(ConnectFailed(message = endpoint + m))
    case Ok(plain) -> match tls {
      case None -> Ok(plain)
      case Some(plan) -> match Tcp.sockStartTls(plain, plan.options, cfg.host, plan.skipVerify, cfg.ioTimeoutMs) {
        case Ok(encrypted) -> Ok(encrypted)
        case Err(m) -> Err(TlsFailed(message = endpoint + m))
      }
    }
  }
}

func openLink(cfg: in MemcachedConfig, tls: in Option[TlsPlan]): Result[Link, MemcachedError] {
  match dial(cfg, tls) {
    case Err(e) -> Err(e)
    case Ok(sock) -> match handshake(sock, cfg) {
      case Ok(link) -> Ok(link)
      case Err(e) -> {
        Tcp.sockClose(sock)
        Err(e)
      }
    }
  }
}

// ─── Client ──────────────────────────────────────────────────────────────────
/// A connection to one memcached server, safe to share between threads (see
/// the package header). Build one with `connect`, `open` or `lazy`.
@experimental
pub protected type MemcachedClient {
  config: MemcachedConfig
  var link: Option[Link] = None
  var closed: Bool = false
  var connectCount: Int = 0
  var lastVersion: String = ""
  // The TLS plan, made by the first connection and kept for the client's
  // life.
  var tlsReady: Bool = false
  var tls: Option[TlsPlan] = None
  func dropLink(): Unit {
    match link {
      case Some(l) -> Tcp.sockClose(l.sock)
      case None -> ()
    }
    link = None
  }
  func currentLink(): Result[Link, MemcachedError] {
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
          lastVersion = l.version
          Ok(l)
        }
      }
    }
  }
  func roundTrip(
    request: in slice[Byte], expected: in Int, untilNoOp: in Bool
  ): Result[List[Meta.MetaReply], MemcachedError] {
    if closed {
      return Err(ClientClosed)
    }
    var retried = false
    while true {
      val reused = match link { case Some(_) -> true; case None -> false }
      match currentLink() {
        case Err(e) -> return Err(e)
        case Ok(l) -> match exchange(l.sock, l.pending, request, expected, untilNoOp, config.ioTimeoutMs) {
          case Replied(replies, rest) -> {
            link = Some(value = Link(sock = l.sock, pending = rest, version = l.version))
            return Ok(replies)
          }
          case Refused(e) -> {
            dropLink()
            return Err(e)
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
  /// Open the connection now if it is not open (requests do this themselves).
  entry ensureConnected(): Result[Unit, MemcachedError] {
    if closed {
      return Err(ClientClosed)
    }
    match currentLink() {
      case Ok(_) -> Ok(())
      case Err(e) -> Err(e)
    }
  }
  /// Send `request`, one or more encoded commands (`Cache.Memcached.Meta`),
  /// in one write and return their `expected` replies in order. A
  /// `SERVER_ERROR` is returned in place as a reply; a `CLIENT_ERROR` or
  /// `ERROR` is `Err(RequestRefused)` and closes the connection. Only the
  /// last command of a request may change anything, because the request is
  /// repeated when the connection turns out to be stale.
  entry run(request: in slice[Byte], expected: in Int): Result[List[Meta.MetaReply], MemcachedError]
    requires: request.length > 0
    requires: expected >= 1
  {
    roundTrip(request, expected, false)
  }
  /// Send `request`, quiet commands (`q` flag) followed by an `mn`, in one
  /// write and return the replies that precede the `MN`.
  entry runUntilNoOp(request: in slice[Byte]): Result[List[Meta.MetaReply], MemcachedError]
    requires: request.length > 0
  {
    roundTrip(request, 0, true)
  }
  /// Close the connection without closing the client: the next request dials
  /// again.
  entry disconnect(): Unit {
    dropLink()
  }
  /// Close the client for good. Later requests return `Err(ClientClosed)`.
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
  /// The version the server reported when the connection was last opened;
  /// `""` before the first connection.
  func serverVersion(): String {
    lastVersion
  }
  /// The largest value this client sends (`MemcachedConfig.maxValueBytes`).
  func maxValueBytes(): Int {
    config.maxValueBytes
  }
}

/// A client for `cfg`, connected, authenticated and checked for the server
/// version before it is returned, so an unreachable server, a failed TLS
/// handshake, a wrong password or an old server is reported here.
@experimental
pub func open(cfg: in MemcachedConfig): Result[MemcachedClient, MemcachedError] {
  val client = MemcachedClient(config = cfg)
  match client.ensureConnected() {
    case Ok(_) -> Ok(client)
    case Err(e) -> Err(e)
  }
}

/// A client for `cfg` that connects on its first request.
@experimental
pub func lazy(cfg: in MemcachedConfig): MemcachedClient {
  MemcachedClient(config = cfg)
}

/// `open` for a `memcached://` or `memcached+tls://` URL.
@experimental
pub func connect(url: in String): Result[MemcachedClient, MemcachedError] {
  match configFromUrl(url) {
    case Err(e) -> Err(e)
    case Ok(cfg) -> open(cfg)
  }
}

func unexpected(command: in String, reply: in Meta.MetaReply): MemcachedError {
  match reply {
    case Meta.ServerError(m) -> ServerFailure(message = m)
    case other -> UnexpectedReply(message = command + " was answered with " + Meta.statusOf(other))
  }
}

/// A liveness check: one `mn` round trip.
@experimental
pub func ping(client: in MemcachedClient): Result[Unit, MemcachedError] {
  match client.run(Meta.encodeNoOp(), 1) {
    case Err(e) -> Err(e)
    case Ok(replies) -> match replies[0] { case Meta.NoOp -> Ok(()); case other -> Err(unexpected("mn", other)) }
  }
}

/// Ask the server for its version (`version`).
@experimental
pub func serverVersion(client: in MemcachedClient): Result[String, MemcachedError] {
  match client.run(Meta.encodeVersion(), 1) {
    case Err(e) -> Err(e)
    case Ok(replies) -> match replies[0] {
      case Meta.Version(text) -> Ok(text)
      case other -> Err(unexpected("version", other))
    }
  }
}

/// Empty the WHOLE server (`flush_all`): every namespace and every other
/// application's entries. For test suites on a disposable server only; a
/// cache store is emptied with `CacheStore.clear()`, which touches nothing
/// but its own namespace.
@experimental
pub func flushAll(client: in MemcachedClient): Result[Unit, MemcachedError] {
  match client.run(Meta.encodeFlushAll(), 1) {
    case Err(e) -> Err(e)
    case Ok(replies) -> match replies[0] { case Meta.Done -> Ok(()); case other -> Err(unexpected("flush_all", other)) }
  }
}
