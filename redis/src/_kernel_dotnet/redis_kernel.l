//! Redis.Kernel (dotnet) - the socket boundary of the Redis client on
//! `--target dotnet`.
//!
//! A client connection is a `System.Net.Sockets.TcpClient` and its stream,
//! bound here through `extern type` / `@externTarget`, so that the stream can
//! be wrapped in a `System.Net.Security.SslStream` for TLS: `Std.TcpHost`
//! dials plain connections only and its `Conn` is opaque, so a client-side TLS
//! upgrade cannot be layered on it. The listening side (used by the in-process
//! fake servers of the test suites) is `Std.TcpHost`, including its
//! server-side TLS termination. Every operation returns a `Result` with the
//! failure text; nothing throws.
//!
//! TLS (`tlsPrepare`, `sockStartTls`). Certificates are parsed by
//! `Std.TlsHost`, the standard library's certificate kernel. Verification is
//! `SslStream`'s own, with no validation callback: the default is the system
//! trust store and a host-name check against the server name; a configured CA
//! becomes an `X509ChainPolicy` in `CustomRootTrust` mode holding exactly that
//! CA (the system store is then not consulted); `skipVerify` (already resolved
//! through the dual-key policy by `Redis`) is an `X509ChainPolicy` with
//! `X509VerificationFlags.AllFlags`, which ignores every chain error and,
//! through `IgnoreInvalidName`, the host-name check.
//!
//! The JVM twin (`src/_kernel_jvm/redis_kernel.l`) declares the same package
//! with the same public surface; a file-level `@cfg(target = ...)` leaves
//! exactly one of them in a build.
@axiom("the host socket, TLS and thread APIs (System.Net.Sockets.TcpClient, System.Net.Security.SslStream, System.Security.Cryptography.X509Certificates, Std.TcpHost, Std.TlsHost and System.Threading.Tasks.Task on dotnet; java.net.Socket, java.net.ServerSocket, javax.net.ssl, java.security.KeyStore, java.io streams and java.lang.Thread on the JVM) conform to their documented contracts")
@cfg(target = "dotnet")
@io
package Redis.Kernel

import Std.Core
import Std.Encoding
import Std.String as Str
import Std.TcpHost as Tcp
import Std.Tls as Tls
import Std.TlsHost as TlsHost

extern type TcpClient = "System.Net.Sockets.TcpClient"

extern type Stream = "System.IO.Stream"

extern type MemStream = "System.IO.MemoryStream"

extern type SslClientOptions = "System.Net.Security.SslClientAuthenticationOptions"

extern type SslProtocols = "System.Security.Authentication.SslProtocols"

extern type X509ChainPolicy = "System.Security.Cryptography.X509Certificates.X509ChainPolicy"

extern type X509Certificate2Collection = "System.Security.Cryptography.X509Certificates.X509Certificate2Collection"

extern type X509CertificateCollection = "System.Security.Cryptography.X509Certificates.X509CertificateCollection"

extern type X509ChainTrustMode = "System.Security.Cryptography.X509Certificates.X509ChainTrustMode"

extern type X509RevocationMode = "System.Security.Cryptography.X509Certificates.X509RevocationMode"

extern type X509VerificationFlags = "System.Security.Cryptography.X509Certificates.X509VerificationFlags"

/// A unit of work running on its own thread.
pub extern type Background = "System.Threading.Tasks.Task"

/// One open TCP connection, plain or TLS.
pub union Sock {
  /// A connection this process dialled; `stream` is the `NetworkStream`, or
  /// the `SslStream` over it after `sockStartTls`. `readBuf` is the reused
  /// read buffer: a connection is read from one thread at a time.
  case Dialed(client: TcpClient, stream: Stream, readBuf: MemStream)
  /// A connection a `ServerSock` accepted.
  case Accepted(conn: Tcp.Conn)
}

/// A bound, listening TCP socket (used by the in-process fake servers of the
/// test suites; the client itself never listens). With `tls`, every accepted
/// connection is terminated as TLS before `serverAccept` returns it.
pub record ServerSock {
  listener: Tcp.Listener
  tls: Option[Tls.TlsServerConfig]
}

/// What a TLS client connection verifies and presents. PEM text; `""` leaves
/// the item out.
pub record TlsSettings {
  /// The only CA the server certificate may chain to; `""` = the system
  /// trust store.
  caPem: String
  /// The client certificate and its PKCS#8 private key (mutual TLS); both or
  /// neither.
  certPem: String
  keyPem: String
  /// Refuse TLS 1.2 (the floor is TLS 1.2 otherwise).
  tls13Only: Bool
  /// Verify neither the certificate chain nor the host name.
  skipVerify: Bool
}

/// `TlsSettings` parsed once, reused by every connection of a client.
pub record TlsClient {
  ca: Option[TlsHost.CertHandle]
  identity: Option[TlsHost.CertHandle]
  tls13Only: Bool
  skipVerify: Bool
}

@externStatic
@externTarget("System.Threading.Tasks.Task.Run")
func taskRun(work: in () -> Unit): Background = ()

@externInstance
@externTarget("System.Threading.Tasks.Task.Wait")
func taskWait(t: in Background): Unit = ()

@externInstance
@externTarget("System.Threading.Tasks.Task.Wait")
func taskWaitMillis(t: in Background, millis: in Int): Bool = ()

@externTarget("System.Net.Sockets.TcpClient..ctor")
func newTcpClient(): TcpClient = ()

@externInstance
@externTarget("System.Net.Sockets.TcpClient.ConnectAsync")
func clientConnectAsync(c: in TcpClient, host: in String, port: in Int): Background = ()

@externInstance
@externTarget("System.Net.Sockets.TcpClient.GetStream")
func clientStream(c: in TcpClient): Stream = ()

@externInstance
@externTarget("System.Net.Sockets.TcpClient.Close")
func clientClose(c: in TcpClient): Unit = ()

@externInstance
@externTarget("System.Net.Sockets.TcpClient.set_NoDelay")
func clientSetNoDelay(c: in TcpClient, on: in Bool): Unit = ()

// Per-operation socket timeouts in milliseconds (0 = wait forever). They
// bound a `NetworkStream` read or write, and so the TLS handshake and every
// record read or write of an `SslStream` layered over it.
@externInstance
@externTarget("System.Net.Sockets.TcpClient.set_ReceiveTimeout")
func clientSetReceiveTimeout(c: in TcpClient, ms: in Int): Unit = ()

@externInstance
@externTarget("System.Net.Sockets.TcpClient.set_SendTimeout")
func clientSetSendTimeout(c: in TcpClient, ms: in Int): Unit = ()

@externInstance
@externTarget("System.IO.Stream.Read")
func streamReadInto(s: in Stream, buffer: in slice[Byte], offset: in Int, count: in Int): Int = ()

@externInstance
@externTarget("System.IO.Stream.Write")
func streamWriteFrom(s: in Stream, buffer: in slice[Byte], offset: in Int, count: in Int): Unit = ()

@externInstance
@externTarget("System.IO.Stream.Flush")
func streamFlush(s: in Stream): Unit = ()

@externInstance
@externTarget("System.IO.Stream.Dispose")
func streamDispose(s: in Stream): Unit = ()

// `MemoryStream(int capacity)` owns a `byte[capacity]` that `GetBuffer()`
// hands out: the read buffer `Stream.Read` fills in place (the idiom of
// `Std.TcpHost`). The array is only ever passed as an extern argument; the
// bytes a read returns leave as a fresh slice.
@externTarget("System.IO.MemoryStream..ctor")
func newMemStreamCap(capacity: in Int): MemStream = ()

@externInstance
@externTarget("System.IO.MemoryStream.GetBuffer")
func memGetBuffer(m: in MemStream): slice[Byte] = ()

// The declared return type is the base `Stream`, so a TLS connection and a
// plain one share `Sock.Dialed.stream`; the `SslStream` members below reach
// the object by `@externTarget` name.
@externTarget("System.Net.Security.SslStream..ctor")
func newSslStream(inner: in Stream, leaveInnerStreamOpen: in Bool): Stream = ()

@externInstance
@externTarget("System.Net.Security.SslStream.AuthenticateAsClient")
func sslAuthenticateAsClient(s: in Stream, options: in SslClientOptions): Unit = ()

@externTarget("System.Net.Security.SslClientAuthenticationOptions..ctor")
func newClientOptions(): SslClientOptions = ()

@externInstance
@externTarget("System.Net.Security.SslClientAuthenticationOptions.set_TargetHost")
func optionsSetTargetHost(o: in SslClientOptions, host: in String): Unit = ()

@externInstance
@externTarget("System.Net.Security.SslClientAuthenticationOptions.set_CertificateChainPolicy")
func optionsSetChainPolicy(o: in SslClientOptions, p: in X509ChainPolicy): Unit = ()

@externInstance
@externTarget("System.Net.Security.SslClientAuthenticationOptions.set_ClientCertificates")
func optionsSetClientCertificates(o: in SslClientOptions, c: in X509CertificateCollection): Unit = ()

@externInstance
@externTarget("System.Net.Security.SslClientAuthenticationOptions.set_EnabledSslProtocols")
func optionsSetProtocols(o: in SslClientOptions, p: in SslProtocols): Unit = ()

// The only version pinned explicitly. Without it `EnabledSslProtocols` stays
// `None`, the platform default, which is TLS 1.2 and 1.3 on .NET 10.
@externTarget("System.Security.Authentication.SslProtocols.Tls13")
func sslProtocolsTls13(): SslProtocols = ()

@externTarget("System.Security.Cryptography.X509Certificates.X509CertificateCollection..ctor")
func newCertCollection(): X509CertificateCollection = ()

@externInstance
@externTarget("System.Security.Cryptography.X509Certificates.X509CertificateCollection.Add")
func certCollectionAdd(c: in X509CertificateCollection, cert: in TlsHost.CertHandle): Int = ()

@externTarget("System.Security.Cryptography.X509Certificates.X509ChainPolicy..ctor")
func newChainPolicy(): X509ChainPolicy = ()

@externInstance
@externTarget("System.Security.Cryptography.X509Certificates.X509ChainPolicy.set_TrustMode")
func chainPolicySetTrustMode(p: in X509ChainPolicy, m: in X509ChainTrustMode): Unit = ()

@externTarget("System.Security.Cryptography.X509Certificates.X509ChainTrustMode.CustomRootTrust")
func chainTrustModeCustomRoot(): X509ChainTrustMode = ()

@externInstance
@externTarget("System.Security.Cryptography.X509Certificates.X509ChainPolicy.get_CustomTrustStore")
func chainPolicyCustomTrustStore(p: in X509ChainPolicy): X509Certificate2Collection = ()

@externInstance
@externTarget("System.Security.Cryptography.X509Certificates.X509Certificate2Collection.Add")
func trustStoreAdd(c: in X509Certificate2Collection, cert: in TlsHost.CertHandle): Int = ()

@externInstance
@externTarget("System.Security.Cryptography.X509Certificates.X509ChainPolicy.set_RevocationMode")
func chainPolicySetRevocationMode(p: in X509ChainPolicy, m: in X509RevocationMode): Unit = ()

@externTarget("System.Security.Cryptography.X509Certificates.X509RevocationMode.NoCheck")
func revocationModeNoCheck(): X509RevocationMode = ()

@externInstance
@externTarget("System.Security.Cryptography.X509Certificates.X509ChainPolicy.set_VerificationFlags")
func chainPolicySetVerificationFlags(p: in X509ChainPolicy, f: in X509VerificationFlags): Unit = ()

@externTarget("System.Security.Cryptography.X509Certificates.X509VerificationFlags.AllFlags")
func verificationFlagsAll(): X509VerificationFlags = ()

val readBufferSize: Int = 16384

func closeClientQuietly(client: in TcpClient): Unit {
  try {
    clientClose(client)
  } catch Bug as _ {
    ()
  }
}

func disposeQuietly(stream: in Stream): Unit {
  try {
    streamDispose(stream)
  } catch Bug as _ {
    ()
  }
}

// `Task.Wait` reports a failed connect as an AggregateException, whose text
// is "One or more errors occurred. (<the socket error>)".
func innerMessage(message: in String): String {
  val prefix = "One or more errors occurred. ("
  if message.startsWith(prefix) and message.endsWith(")") {
    message.substring(prefix.length, message.length - prefix.length - 1)
  } else {
    message
  }
}

/// Open a TCP connection to `host:port`, waiting at most `connectTimeoutMs`
/// milliseconds for it (`0` = no limit), and bound every later read and write
/// to `ioTimeoutMs` milliseconds of inactivity (`0` = no limit).
pub func sockConnect(host: in String, port: in Int, connectTimeoutMs: in Int, ioTimeoutMs: in Int): Result[Sock, String]
  requires: connectTimeoutMs >= 0
  requires: ioTimeoutMs >= 0
{
  val client = try {
    newTcpClient()
  } catch Bug as b {
    return Err(b.message)
  }
  try {
    val dialling = clientConnectAsync(client, host, port)
    if connectTimeoutMs == 0 {
      taskWait(dialling)
    } else if not taskWaitMillis(dialling, connectTimeoutMs) {
      // Closing the client abandons the pending connect.
      closeClientQuietly(client)
      return Err("no connection within " + connectTimeoutMs.toString() + " ms")
    }
    clientSetNoDelay(client, true)
    clientSetReceiveTimeout(client, ioTimeoutMs)
    clientSetSendTimeout(client, ioTimeoutMs)
    Ok(Dialed(client = client, stream = clientStream(client), readBuf = newMemStreamCap(readBufferSize)))
  } catch Bug as b {
    closeClientQuietly(client)
    Err(innerMessage(b.message))
  }
}

func pemBytes(pem: in String): slice[Byte] {
  encodeUtf8(pem)
}

func loadMessage(e: in TlsHost.HostLoadError): String {
  match e {
    case TlsHost.HostMalformed(m) -> m
    case TlsHost.HostMismatch(m) -> m
  }
}

/// Parse `settings` for use by `sockStartTls`. An unusable certificate or key
/// is `Err` with the reason.
pub func tlsPrepare(settings: in TlsSettings): Result[TlsClient, String]
  requires: (settings.certPem.length == 0) == (settings.keyPem.length == 0)
{
  var ca: Option[TlsHost.CertHandle] = None
  if settings.caPem.length > 0 {
    match TlsHost.hostCertFromPemBytes(pemBytes(settings.caPem)) {
      case Ok(handle) -> {
        ca = Some(value = handle)
      }
      case Err(e) -> return Err("CA certificate: " + loadMessage(e))
    }
  }
  var identity: Option[TlsHost.CertHandle] = None
  if settings.certPem.length > 0 {
    match TlsHost.hostIdentityFromPemBytes(pemBytes(settings.certPem), pemBytes(settings.keyPem)) {
      case Ok(handle) -> {
        identity = Some(value = handle)
      }
      case Err(e) -> return Err("client certificate and key: " + loadMessage(e))
    }
  }
  Ok(TlsClient(ca = ca, identity = identity, tls13Only = settings.tls13Only, skipVerify = settings.skipVerify))
}

func clientOptions(tls: in TlsClient, serverName: in String): SslClientOptions {
  val opts = newClientOptions()
  optionsSetTargetHost(opts, serverName)
  if tls.tls13Only {
    optionsSetProtocols(opts, sslProtocolsTls13())
  }
  match tls.identity {
    case Some(identity) -> {
      val certs = newCertCollection()
      val _ = certCollectionAdd(certs, identity)
      optionsSetClientCertificates(opts, certs)
    }
    case None -> ()
  }
  if tls.skipVerify {
    val policy = newChainPolicy()
    chainPolicySetRevocationMode(policy, revocationModeNoCheck())
    chainPolicySetVerificationFlags(policy, verificationFlagsAll())
    optionsSetChainPolicy(opts, policy)
  } else {
    match tls.ca {
      case Some(ca) -> {
        val policy = newChainPolicy()
        chainPolicySetTrustMode(policy, chainTrustModeCustomRoot())
        chainPolicySetRevocationMode(policy, revocationModeNoCheck())
        val _ = trustStoreAdd(chainPolicyCustomTrustStore(policy), ca)
        optionsSetChainPolicy(opts, policy)
      }
      case None -> ()
    }
  }
  opts
}

/// Run the TLS handshake as a client on the freshly dialled `s` and return
/// the encrypted connection. `serverName` is the name sent as SNI and the
/// name the server certificate must carry. The handshake is bounded by the
/// connection's I/O timeout. On `Err` the connection has been closed.
pub func sockStartTls(s: in Sock, tls: in TlsClient, serverName: in String, port: in Int): Result[Sock, String]
  requires: serverName.length > 0
{
  match s {
    case Accepted(conn) -> {
      Tcp.hostClose(conn)
      Err("only a dialled connection can start TLS as a client")
    }
    case Dialed(client, stream, readBuf) -> {
      val ssl = try {
        newSslStream(stream, false)
      } catch Bug as b {
        closeClientQuietly(client)
        return Err(b.message)
      }
      try {
        sslAuthenticateAsClient(ssl, clientOptions(tls, serverName))
        Ok(Dialed(client = client, stream = ssl, readBuf = readBuf))
      } catch Bug as b {
        disposeQuietly(ssl)
        closeClientQuietly(client)
        Err(b.message)
      }
    }
  }
}

/// Read up to `maxBytes` bytes, blocking until at least one arrives. An empty
/// slice means the peer closed the connection.
pub func sockRead(s: in Sock, maxBytes: in Int): Result[slice[Byte], String]
  requires: maxBytes > 0
{
  match s {
    case Accepted(conn) -> match Tcp.hostRead(conn, maxBytes) {
      case Ok(bytes) -> Ok(bytes)
      case Err(e) -> Err(e.message())
    }
    case Dialed(_, stream, readBuf) -> {
      try {
        val want = if maxBytes < readBufferSize { maxBytes } else { readBufferSize }
        val n = streamReadInto(stream, memGetBuffer(readBuf), 0, want)
        Ok(memGetBuffer(readBuf).slice(0, n))
      } catch Bug as b {
        Err(b.message)
      }
    }
  }
}

/// Write all of `bytes` and flush.
pub func sockWrite(s: in Sock, bytes: in slice[Byte]): Result[Unit, String] {
  match s {
    case Accepted(conn) -> match Tcp.hostWrite(conn, bytes) { case Ok(_) -> Ok(()); case Err(e) -> Err(e.message()) }
    case Dialed(_, stream, _) -> {
      try {
        streamWriteFrom(stream, bytes, 0, bytes.length)
        streamFlush(stream)
        Ok(())
      } catch Bug as b {
        Err(b.message)
      }
    }
  }
}

/// Close the connection. Closing twice is harmless.
pub func sockClose(s: in Sock): Unit {
  match s {
    case Accepted(conn) -> Tcp.hostClose(conn)
    case Dialed(client, stream, _) -> {
      disposeQuietly(stream)
      closeClientQuietly(client)
    }
  }
}

/// Listen on `host:port` (`port = 0` picks a free port).
pub func serverListen(host: in String, port: in Int): Result[ServerSock, String] {
  match Tcp.hostListen(host, port) {
    case Ok(l) -> Ok(ServerSock(listener = l, tls = None))
    case Err(e) -> Err(e.message())
  }
}

/// `serverListen` for a TLS server presenting `certPem` / `keyPem` (PEM text,
/// PKCS#8 key). With `clientCaPem` not empty the server asks for a client
/// certificate signed by that CA, and with `requireClientCert` refuses a
/// client that presents none.
pub func serverListenTls(
  host: in String,
  port: in Int,
  certPem: in String,
  keyPem: in String,
  clientCaPem: in String,
  requireClientCert: in Bool,
): Result[ServerSock, String]
  requires: not requireClientCert or clientCaPem.length > 0
{
  val identity = match Tls.Identity.fromPem(certPem, keyPem) {
    case Ok(id) -> id
    case Err(e) -> return Err(e.message())
  }
  var clientCa: Option[Tls.Certificate] = None
  if clientCaPem.length > 0 {
    match Tls.Certificate.fromPem(clientCaPem) {
      case Ok(ca) -> {
        clientCa = Some(value = ca)
      }
      case Err(e) -> return Err(e.message())
    }
  }
  val config = Tls.TlsServerConfig(
    identity = identity,
    minVersion = Tls.Tls12,
    clientCa = clientCa,
    requireClientCert = requireClientCert
  )
  match Tcp.hostListen(host, port) {
    case Ok(l) -> Ok(ServerSock(listener = l, tls = Some(value = config)))
    case Err(e) -> Err(e.message())
  }
}

/// The port `server` is bound to.
pub func serverPort(server: in ServerSock): Int {
  Tcp.hostLocalPort(server.listener)
}

/// Block until a client connects. On a TLS server the handshake has run when
/// this returns; a client whose handshake fails is an `Err`.
pub func serverAccept(server: in ServerSock): Result[Sock, String] {
  match server.tls {
    case Some(config) -> match Tcp.hostAcceptTls(server.listener, config) {
      case Ok(conn) -> Ok(Accepted(conn = conn))
      case Err(e) -> Err(e.message())
    }
    case None -> match Tcp.hostAccept(server.listener) {
      case Ok(conn) -> Ok(Accepted(conn = conn))
      case Err(e) -> Err(e.message())
    }
  }
}

/// Stop listening.
pub func serverClose(server: in ServerSock): Unit {
  Tcp.hostStopListener(server.listener)
}

/// Run `work` on another thread.
pub func startBackground(work: in () -> Unit): Background {
  taskRun(work)
}

/// Block until `b` has finished.
pub func waitBackground(b: in Background): Unit {
  try {
    taskWait(b)
  } catch Bug as _ {
    ()
  }
}
