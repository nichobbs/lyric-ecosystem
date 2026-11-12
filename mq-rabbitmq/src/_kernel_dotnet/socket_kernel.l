//! Mq.RabbitMq.Kernel (dotnet) - the socket boundary of the AMQP client on
//! `--target dotnet`.
//!
//! `System.Net.Sockets.TcpClient` / `TcpListener`, `System.Net.Security.SslStream`
//! for TLS, a monitor lock, a monotonic clock and a background task. Every
//! operation returns a `Result` with the failure text; nothing throws.
//!
//! `Std.TcpHost` is not used: an AMQP consumer has to tell "nothing arrived
//! within the wait" from "the connection failed", and `Std.TcpHost` reports a
//! receive timeout as the same `TransportFailed` as a reset. Here a read is a
//! `Stream.ReadAsync` whose task the connection keeps: `sockRead` waits for it
//! with a limit and, when the limit passes, leaves it pending for the next
//! call. That is exact for a plain socket and for an `SslStream` alike, where
//! polling the socket would be wrong: bytes on the wire may be a handshake
//! message or part of a record (no plaintext yet), and plaintext may already
//! sit decrypted in the stream with nothing left on the wire.
//!
//! TLS: the client side is `SslStream.AuthenticateAsClient` with an
//! `SslClientAuthenticationOptions`. Trust is configured through
//! `CertificateChainPolicy` (a custom root store, with the platform roots added
//! for additive trust), never through a validation callback: the callback is
//! the named delegate `RemoteCertificateValidationCallback`, which a Lyric
//! lambda cannot become (docs/66 gap 2 in lyric-lang). The host name is always
//! checked against `TargetHost` by `SslStream` itself. Skipping verification
//! is the chain policy with `X509VerificationFlags.AllFlags`, which also
//! carries `IgnoreInvalidName`. Certificates and keys are loaded from PEM by
//! the standard library's `Std.TlsHost`. The server side
//! (`sockAcceptTls`) exists for the in-process broker of the hermetic tests.
//!
//! The JVM twin (`src/_kernel_jvm/socket_kernel.l`) declares the same package
//! with the same public surface; a file-level `@cfg(target = ...)` leaves
//! exactly one of them in a build.
@axiom("the host socket, thread, lock and clock primitives conform to their documented platform contracts")
@cfg(target = "dotnet")
@io
package Mq.RabbitMq.Kernel

import Std.Core
import Std.Encoding
import Std.String as Str
import Std.Parse as Parse
import Std.TlsHost as TlsHost

extern type TcpClient = "System.Net.Sockets.TcpClient"

extern type TcpListener = "System.Net.Sockets.TcpListener"

extern type Stream = "System.IO.Stream"

extern type MemStream = "System.IO.MemoryStream"

extern type IPAddress = "System.Net.IPAddress"

extern type EndPoint = "System.Net.EndPoint"

extern type BclTask = "System.Threading.Tasks.Task"

extern type SslClientOptions = "System.Net.Security.SslClientAuthenticationOptions"

extern type SslServerOptions = "System.Net.Security.SslServerAuthenticationOptions"

extern type SslCertContext = "System.Net.Security.SslStreamCertificateContext"

extern type SslProtocols = "System.Security.Authentication.SslProtocols"

extern type X509CertificateCollection = "System.Security.Cryptography.X509Certificates.X509CertificateCollection"

extern type X509Certificate2Collection = "System.Security.Cryptography.X509Certificates.X509Certificate2Collection"

extern type X509ChainPolicy = "System.Security.Cryptography.X509Certificates.X509ChainPolicy"

extern type X509ChainTrustMode = "System.Security.Cryptography.X509Certificates.X509ChainTrustMode"

extern type X509RevocationMode = "System.Security.Cryptography.X509Certificates.X509RevocationMode"

extern type X509VerificationFlags = "System.Security.Cryptography.X509Certificates.X509VerificationFlags"

extern type X509Store = "System.Security.Cryptography.X509Certificates.X509Store"

extern type StoreName = "System.Security.Cryptography.X509Certificates.StoreName"

extern type StoreLocation = "System.Security.Cryptography.X509Certificates.StoreLocation"

extern type OpenFlags = "System.Security.Cryptography.X509Certificates.OpenFlags"

@externTarget("System.Net.Sockets.TcpClient..ctor")
func newTcpClient(): TcpClient = ()

@externInstance
@externTarget("System.Net.Sockets.TcpClient.ConnectAsync")
func clientConnectAsync(c: in TcpClient, host: in String, port: in Int): BclTask = ()

@externInstance
@externTarget("System.Threading.Tasks.Task.Wait")
func taskWaitFor(t: in BclTask, timeoutMs: in Int): Bool = ()

@externInstance
@externTarget("System.Threading.Tasks.Task.Wait")
func taskWait(t: in BclTask): Unit = ()

@externStatic
@externTarget("System.Threading.Tasks.Task.Run")
func taskRun(work: in () -> Unit): BclTask = ()

@externInstance
@externTarget("System.Threading.Tasks.Task.Wait")
func readWaitFor(t: in Task[Int], timeoutMs: in Int): Bool = ()

@externInstance
@externTarget("System.Threading.Tasks.Task`1.get_Result")
func readResult[T](t: in Task[T]): T = ()

@externInstance
@externTarget("System.Net.Sockets.TcpClient.GetStream")
func clientStream(c: in TcpClient): Stream = ()

@externInstance
@externTarget("System.Net.Sockets.TcpClient.Close")
func clientClose(c: in TcpClient): Unit = ()

@externInstance
@externTarget("System.Net.Sockets.TcpClient.set_NoDelay")
func clientSetNoDelay(c: in TcpClient, on: in Bool): Unit = ()

// The live `Task<int>` of one read; `sockRead` waits for it with a limit.
func streamReadAsync(s: in Stream, buffer: in slice[Byte], offset: in Int, count: in Int): Task[Int] {
  s.ReadAsync(buffer, offset, count)
}

@externInstance
@externTarget("System.IO.Stream.Write")
func streamWriteFrom(s: in Stream, buffer: in slice[Byte], offset: in Int, count: in Int): Unit = ()

@externInstance
@externTarget("System.IO.Stream.Flush")
func streamFlush(s: in Stream): Unit = ()

@externInstance
@externTarget("System.IO.Stream.Close")
func streamClose(s: in Stream): Unit = ()

// `MemoryStream(capacity).GetBuffer()` is the `byte[]` a read fills in place;
// it is never bound as a Lyric slice, and the bytes a read returns leave as a
// fresh slice (the idiom of `Std.TcpHost`).
@externTarget("System.IO.MemoryStream..ctor")
func newMemStreamCap(capacity: in Int): MemStream = ()

@externInstance
@externTarget("System.IO.MemoryStream.GetBuffer")
func memGetBuffer(m: in MemStream): slice[Byte] = ()

@externTarget("System.Net.IPAddress.Parse")
func ipParse(host: in String): IPAddress = ()

@externTarget("System.Net.Sockets.TcpListener..ctor")
func newTcpListener(addr: in IPAddress, port: in Int): TcpListener = ()

@externInstance
@externTarget("System.Net.Sockets.TcpListener.Start")
func listenerStart(l: in TcpListener): Unit = ()

@externInstance
@externTarget("System.Net.Sockets.TcpListener.Stop")
func listenerStop(l: in TcpListener): Unit = ()

@externInstance
@externTarget("System.Net.Sockets.TcpListener.AcceptTcpClient")
func listenerAccept(l: in TcpListener): TcpClient = ()

@externTarget("System.Net.Sockets.TcpListener.get_LocalEndpoint")
func listenerLocalEndPoint(l: in TcpListener): EndPoint = ()

@externTarget("System.Object.ToString")
func endPointToString(e: in EndPoint): String = ()

@externStatic
@externTarget("System.Threading.Monitor.Enter")
func monitorEnter[T](obj: in T): Unit = ()

@externStatic
@externTarget("System.Threading.Monitor.Exit")
func monitorExit[T](obj: in T): Unit = ()

@externStatic
@externTarget("System.Threading.Thread.Sleep")
func threadSleep(ms: in Int): Unit = ()

@externStatic
@externTarget("System.Environment.get_TickCount64")
func tickCount64(): Long = ()

// ─── TLS externs ─────────────────────────────────────────────────────────────
// The constructor's declared return type is the base `Stream`, so a TLS
// connection and a plain one share `Sock.stream`; the `SslStream` members
// below take that base-typed receiver (the idiom of `Std.TcpHost`).
@externTarget("System.Net.Security.SslStream..ctor")
func newSslStream(inner: in Stream, leaveInnerStreamOpen: in Bool): Stream = ()

@externInstance
@externTarget("System.Net.Security.SslStream.AuthenticateAsClient")
func sslAuthenticateAsClient(s: in Stream, options: in SslClientOptions): Unit = ()

@externInstance
@externTarget("System.Net.Security.SslStream.AuthenticateAsServer")
func sslAuthenticateAsServer(s: in Stream, options: in SslServerOptions): Unit = ()

@externTarget("System.Net.Security.SslClientAuthenticationOptions..ctor")
func newClientOptions(): SslClientOptions = ()

@externInstance
@externTarget("System.Net.Security.SslClientAuthenticationOptions.set_TargetHost")
func clientOptionsSetTargetHost(o: in SslClientOptions, host: in String): Unit = ()

@externInstance
@externTarget("System.Net.Security.SslClientAuthenticationOptions.set_EnabledSslProtocols")
func clientOptionsSetProtocols(o: in SslClientOptions, p: in SslProtocols): Unit = ()

@externInstance
@externTarget("System.Net.Security.SslClientAuthenticationOptions.set_ClientCertificates")
func clientOptionsSetCertificates(o: in SslClientOptions, c: in X509CertificateCollection): Unit = ()

@externInstance
@externTarget("System.Net.Security.SslClientAuthenticationOptions.set_CertificateChainPolicy")
func clientOptionsSetChainPolicy(o: in SslClientOptions, p: in X509ChainPolicy): Unit = ()

@externTarget("System.Net.Security.SslServerAuthenticationOptions..ctor")
func newServerOptions(): SslServerOptions = ()

@externInstance
@externTarget("System.Net.Security.SslServerAuthenticationOptions.set_ServerCertificateContext")
func serverOptionsSetCertificateContext(o: in SslServerOptions, c: in SslCertContext): Unit = ()

@externInstance
@externTarget("System.Net.Security.SslServerAuthenticationOptions.set_ClientCertificateRequired")
func serverOptionsSetClientCertificateRequired(o: in SslServerOptions, v: in Bool): Unit = ()

@externInstance
@externTarget("System.Net.Security.SslServerAuthenticationOptions.set_CertificateChainPolicy")
func serverOptionsSetChainPolicy(o: in SslServerOptions, p: in X509ChainPolicy): Unit = ()

@externTarget("System.Net.Security.SslStreamCertificateContext.Create")
func newCertContext(
  target: in TlsHost.CertHandle, additional: in X509Certificate2Collection, offline: in Bool
): SslCertContext = ()

// `SslProtocols` is a flags enum with no callable OR, so only the exact
// TLS 1.3 pin is ever set; the TLS 1.2 floor is the platform default.
@externTarget("System.Security.Authentication.SslProtocols.Tls13")
func sslProtocolsTls13(): SslProtocols = ()

@externTarget("System.Security.Cryptography.X509Certificates.X509CertificateCollection..ctor")
func newCertificateCollection(): X509CertificateCollection = ()

func certificateCollectionAdd(c: in X509CertificateCollection, cert: in TlsHost.CertHandle): Int {
  c.Add(cert)
}

@externTarget("System.Security.Cryptography.X509Certificates.X509Certificate2Collection..ctor")
func newCertificate2Collection(): X509Certificate2Collection = ()

@externInstance
@externTarget("System.Security.Cryptography.X509Certificates.X509Certificate2Collection.Add")
func certificate2CollectionAdd(c: in X509Certificate2Collection, cert: in TlsHost.CertHandle): Int = ()

@externInstance
@externTarget("System.Security.Cryptography.X509Certificates.X509Certificate2Collection.AddRange")
func certificate2CollectionAddRange(c: in X509Certificate2Collection, more: in X509Certificate2Collection): Unit = ()

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
@externTarget("System.Security.Cryptography.X509Certificates.X509ChainPolicy.set_RevocationMode")
func chainPolicySetRevocationMode(p: in X509ChainPolicy, m: in X509RevocationMode): Unit = ()

@externTarget("System.Security.Cryptography.X509Certificates.X509RevocationMode.NoCheck")
func revocationModeNoCheck(): X509RevocationMode = ()

@externInstance
@externTarget("System.Security.Cryptography.X509Certificates.X509ChainPolicy.set_VerificationFlags")
func chainPolicySetVerificationFlags(p: in X509ChainPolicy, f: in X509VerificationFlags): Unit = ()

@externTarget("System.Security.Cryptography.X509Certificates.X509VerificationFlags.AllFlags")
func verificationFlagsAll(): X509VerificationFlags = ()

@externTarget("System.Security.Cryptography.X509Certificates.X509Store..ctor")
func newStore(name: in StoreName, location: in StoreLocation): X509Store = ()

@externTarget("System.Security.Cryptography.X509Certificates.StoreName.Root")
func storeNameRoot(): StoreName = ()

@externTarget("System.Security.Cryptography.X509Certificates.StoreLocation.LocalMachine")
func storeLocationMachine(): StoreLocation = ()

@externTarget("System.Security.Cryptography.X509Certificates.StoreLocation.CurrentUser")
func storeLocationUser(): StoreLocation = ()

@externTarget("System.Security.Cryptography.X509Certificates.OpenFlags.ReadOnly")
func openFlagsReadOnly(): OpenFlags = ()

@externInstance
@externTarget("System.Security.Cryptography.X509Certificates.X509Store.Open")
func storeOpen(s: in X509Store, flags: in OpenFlags): Unit = ()

@externInstance
@externTarget("System.Security.Cryptography.X509Certificates.X509Store.get_Certificates")
func storeCertificates(s: in X509Store): X509Certificate2Collection = ()

@externInstance
@externTarget("System.Security.Cryptography.X509Certificates.X509Store.Close")
func storeClose(s: in X509Store): Unit = ()

val readBufferSize: Int = 16384

/// The read in progress on a connection, and bytes a read returned beyond
/// what its caller asked for.
pub ref record ReadState {
  var pending: Option[Task[Int]]
  var carry: slice[Byte]
}

/// One open connection: plain TCP, or TLS over it.
pub record Sock {
  client: TcpClient
  stream: Stream
  readBuf: MemStream
  reader: ReadState
}

/// How a client secures a connection (`sockStartTls`). PEM text; an empty
/// string means "not configured".
pub record ClientTls {
  /// The name sent as SNI and checked against the server certificate.
  serverName: String
  /// A trust anchor in addition to the platform's, or instead of them when
  /// `exclusiveCa` is set.
  caPem: String
  exclusiveCa: Bool
  /// The client certificate and its PKCS8 private key (mutual TLS).
  certPem: String
  keyPem: String
  /// TLS 1.3 only; otherwise TLS 1.2 or later.
  tls13Only: Bool
  /// Accept any server certificate and any name. The caller decides whether
  /// that is allowed; the kernel only does what it is told.
  skipVerify: Bool
}

/// How a server secures an accepted connection (`sockAcceptTls`).
pub record ServerTls {
  certPem: String
  keyPem: String
  /// When not empty, a client certificate issued by this CA is required.
  clientCaPem: String
}

/// What one read produced.
pub union ReadOutcome {
  case Data(bytes: slice[Byte])
  case TimedOut
  case Eof
}

/// A bound, listening TCP socket (the loopback AMQP peer of the hermetic
/// tests; the client itself never listens).
pub record ServerSock {
  listener: TcpListener
}

/// A unit of work running on its own thread.
pub record Background {
  task: BclTask
}

/// A reentrant mutual-exclusion lock.
pub record Lock {
}

func closeQuietly(client: in TcpClient): Unit {
  try {
    clientClose(client)
  } catch Bug as _ {
    ()
  }
}

func noBytes(): slice[Byte] {
  val empty: slice[Byte] = []
  empty
}

func sockOver(client: in TcpClient, stream: in Stream): Sock {
  Sock(
    client = client,
    stream = stream,
    readBuf = newMemStreamCap(readBufferSize),
    reader = ReadState(pending = None, carry = noBytes())
  )
}

func wrapClient(client: in TcpClient): Sock {
  clientSetNoDelay(client, true)
  sockOver(client, clientStream(client))
}

// A failed task reports through `AggregateException`, whose message wraps the
// cause as "One or more errors occurred. (cause)".
func causeOf(message: in String): String {
  val prefix = "One or more errors occurred. ("
  if Str.startsWith(message, prefix) and Str.endsWith(message, ")") {
    Str.substring(message, prefix.length, message.length - prefix.length - 1)
  } else {
    message
  }
}

/// Open a TCP connection to `host:port`, waiting at most `connectTimeoutMs`
/// milliseconds for it (`0` = the operating system's limit).
pub func sockConnect(host: in String, port: in Int, connectTimeoutMs: in Int): Result[Sock, String]
  requires: connectTimeoutMs >= 0
{
  val client = newTcpClient()
  try {
    val pending = clientConnectAsync(client, host, port)
    if connectTimeoutMs == 0 {
      taskWait(pending)
    } else if not taskWaitFor(pending, connectTimeoutMs) {
      closeQuietly(client)
      return Err("connect to " + host + ":" + port.toString() + " timed out after " + connectTimeoutMs.toString() + " ms")
    }
    Ok(wrapClient(client))
  } catch Bug as b {
    closeQuietly(client)
    Err(b.message)
  }
}

func takeCarry(state: in ReadState, maxBytes: in Int): slice[Byte] {
  val held = state.carry
  if held.length <= maxBytes {
    state.carry = noBytes()
    return held
  }
  state.carry = held.slice(maxBytes, held.length)
  held.slice(0, maxBytes)
}

/// Read up to `maxBytes` bytes, waiting at most `timeoutMs` milliseconds for
/// the first one (`0` = only what has already arrived).
pub func sockRead(s: in Sock, maxBytes: in Int, timeoutMs: in Int): Result[ReadOutcome, String]
  requires: maxBytes > 0
  requires: timeoutMs >= 0
{
  val state = s.reader
  if state.carry.length > 0 {
    return Ok(Data(bytes = takeCarry(state, maxBytes)))
  }
  try {
    val read = match state.pending {
      case Some(t) -> t
      case None -> {
        val t = streamReadAsync(s.stream, memGetBuffer(s.readBuf), 0, readBufferSize)
        state.pending = Some(value = t)
        t
      }
    }
    // A wait of 0 would miss a read that is completing on another thread.
    if not readWaitFor(read, if timeoutMs < 1 { 1 } else { timeoutMs }) {
      return Ok(TimedOut)
    }
    state.pending = None
    val n = readResult(read)
    if n <= 0 {
      return Ok(Eof)
    }
    state.carry = memGetBuffer(s.readBuf).slice(0, n)
    Ok(Data(bytes = takeCarry(state, maxBytes)))
  } catch Bug as b {
    Err(causeOf(b.message))
  }
}

/// Write all of `bytes` and flush.
pub func sockWrite(s: in Sock, bytes: in slice[Byte]): Result[Unit, String] {
  try {
    streamWriteFrom(s.stream, bytes, 0, bytes.length)
    streamFlush(s.stream)
    Ok(())
  } catch Bug as b {
    Err(b.message)
  }
}

/// Close the connection. Closing twice is harmless.
pub func sockClose(s: in Sock): Unit {
  try {
    streamClose(s.stream)
  } catch Bug as _ {
    ()
  }
  closeQuietly(s.client)
}

// ─── TLS ─────────────────────────────────────────────────────────────────────
func loadFailure(what: in String, e: in TlsHost.HostLoadError): String {
  match e {
    case TlsHost.HostMalformed(m) -> what + " could not be loaded: " + m
    case TlsHost.HostMismatch(m) -> what + " could not be loaded: " + m
  }
}

func loadCertificate(what: in String, pem: in String): Result[TlsHost.CertHandle, String] {
  match TlsHost.hostCertFromPemBytes(encodeUtf8(pem)) {
    case Ok(h) -> Ok(h)
    case Err(e) -> Err(loadFailure(what, e))
  }
}

func loadIdentity(what: in String, certPem: in String, keyPem: in String): Result[TlsHost.CertHandle, String] {
  match TlsHost.hostIdentityFromPemBytes(encodeUtf8(certPem), encodeUtf8(keyPem)) {
    case Ok(h) -> Ok(h)
    case Err(e) -> Err(loadFailure(what, e))
  }
}

func addPlatformRoots(into: in X509Certificate2Collection, location: in StoreLocation): Unit {
  val store = newStore(storeNameRoot(), location)
  storeOpen(store, openFlagsReadOnly())
  certificate2CollectionAddRange(into, storeCertificates(store))
  storeClose(store)
}

// Trust exactly `ca`, plus the platform's roots unless `exclusive`.
func customRootPolicy(ca: in TlsHost.CertHandle, exclusive: in Bool): X509ChainPolicy {
  val policy = newChainPolicy()
  chainPolicySetTrustMode(policy, chainTrustModeCustomRoot())
  // What `SslStream` does without a policy; a policy object alone would
  // switch on online revocation checks.
  chainPolicySetRevocationMode(policy, revocationModeNoCheck())
  val roots = chainPolicyCustomTrustStore(policy)
  val _ = certificate2CollectionAdd(roots, ca)
  if not exclusive {
    addPlatformRoots(roots, storeLocationMachine())
    addPlatformRoots(roots, storeLocationUser())
  }
  policy
}

func clientOptions(tls: in ClientTls): Result[SslClientOptions, String] {
  val options = newClientOptions()
  clientOptionsSetTargetHost(options, tls.serverName)
  if tls.tls13Only {
    clientOptionsSetProtocols(options, sslProtocolsTls13())
  }
  if tls.certPem.length > 0 {
    val identity = match loadIdentity("the client certificate and key", tls.certPem, tls.keyPem) {
      case Ok(h) -> h
      case Err(e) -> return Err(e)
    }
    val certificates = newCertificateCollection()
    val _ = certificateCollectionAdd(certificates, identity)
    clientOptionsSetCertificates(options, certificates)
  }
  if tls.skipVerify {
    val policy = newChainPolicy()
    chainPolicySetRevocationMode(policy, revocationModeNoCheck())
    chainPolicySetVerificationFlags(policy, verificationFlagsAll())
    clientOptionsSetChainPolicy(options, policy)
  } else if tls.caPem.length > 0 {
    val ca = match loadCertificate("the CA certificate", tls.caPem) { case Ok(h) -> h; case Err(e) -> return Err(e) }
    clientOptionsSetChainPolicy(options, customRootPolicy(ca, tls.exclusiveCa))
  }
  Ok(options)
}

func serverOptions(tls: in ServerTls): Result[SslServerOptions, String] {
  val identity = match loadIdentity("the server certificate and key", tls.certPem, tls.keyPem) {
    case Ok(h) -> h
    case Err(e) -> return Err(e)
  }
  val options = newServerOptions()
  serverOptionsSetCertificateContext(options, newCertContext(identity, newCertificate2Collection(), true))
  if tls.clientCaPem.length > 0 {
    val ca = match loadCertificate("the client CA certificate", tls.clientCaPem) {
      case Ok(h) -> h
      case Err(e) -> return Err(e)
    }
    serverOptionsSetClientCertificateRequired(options, true)
    serverOptionsSetChainPolicy(options, customRootPolicy(ca, true))
  }
  Ok(options)
}

ref record Handshake {
  var failure: String
}

func runGuarded(outcome: in Handshake, work: in () -> Unit): Unit {
  try {
    work()
  } catch Bug as b {
    outcome.failure = if b.message.length > 0 { b.message } else { "the handshake failed" }
  }
}

// Run a blocking handshake on another thread so it can be abandoned: the
// caller closes the socket on `Err`, which ends a handshake still in flight.
func handshakeWithin(work: in () -> Unit, timeoutMs: in Int): Result[Unit, String] {
  val outcome = Handshake(failure = "")
  val running = taskRun({ -> runGuarded(outcome, work) })
  if timeoutMs == 0 {
    taskWait(running)
  } else if not taskWaitFor(running, timeoutMs) {
    return Err("timed out after " + timeoutMs.toString() + " ms")
  }
  if outcome.failure.length > 0 {
    Err(outcome.failure)
  } else {
    Ok(())
  }
}

/// Run the client side of a TLS handshake over `s` and return the secured
/// connection. It waits at most `timeoutMs` milliseconds (`0` = no limit).
/// On `Err` the connection is closed.
pub func sockStartTls(s: in Sock, tls: in ClientTls, timeoutMs: in Int): Result[Sock, String]
  requires: timeoutMs >= 0
{
  try {
    val options = match clientOptions(tls) { case Ok(o) -> o; case Err(e) -> { closeQuietly(s.client); return Err(e) } }
    val secured = newSslStream(s.stream, false)
    match handshakeWithin({ -> sslAuthenticateAsClient(secured, options) }, timeoutMs) {
      case Ok(_) -> Ok(sockOver(s.client, secured))
      case Err(e) -> {
        closeQuietly(s.client)
        Err(e)
      }
    }
  } catch Bug as b {
    closeQuietly(s.client)
    Err(b.message)
  }
}

/// Run the server side of a TLS handshake over an accepted connection. On
/// `Err` the connection is closed.
pub func sockAcceptTls(s: in Sock, tls: in ServerTls, timeoutMs: in Int): Result[Sock, String]
  requires: timeoutMs >= 0
{
  try {
    val options = match serverOptions(tls) { case Ok(o) -> o; case Err(e) -> { closeQuietly(s.client); return Err(e) } }
    val secured = newSslStream(s.stream, false)
    match handshakeWithin({ -> sslAuthenticateAsServer(secured, options) }, timeoutMs) {
      case Ok(_) -> Ok(sockOver(s.client, secured))
      case Err(e) -> {
        closeQuietly(s.client)
        Err(e)
      }
    }
  } catch Bug as b {
    closeQuietly(s.client)
    Err(b.message)
  }
}

/// Listen on the IP literal `host` and `port` (`port = 0` picks a free port).
pub func serverListen(host: in String, port: in Int): Result[ServerSock, String] {
  try {
    val l = newTcpListener(ipParse(host), port)
    listenerStart(l)
    Ok(ServerSock(listener = l))
  } catch Bug as b {
    Err(b.message)
  }
}

/// The port `server` is bound to, `0` when it cannot be read.
pub func serverPort(server: in ServerSock): Int {
  try {
    val text = endPointToString(listenerLocalEndPoint(server.listener))
    val colon = Str.lastIndexOfRaw(text, ":")
    if colon < 0 {
      0
    } else {
      match Parse.tryParseInt(Str.substring(text, colon + 1)) {
        case Ok(n) -> n
        case Err(_) -> 0
      }
    }
  } catch Bug as _ {
    0
  }
}

/// Block until a client connects.
pub func serverAccept(server: in ServerSock): Result[Sock, String] {
  try {
    Ok(wrapClient(listenerAccept(server.listener)))
  } catch Bug as b {
    Err(b.message)
  }
}

/// Stop listening.
pub func serverClose(server: in ServerSock): Unit {
  try {
    listenerStop(server.listener)
  } catch Bug as _ {
    ()
  }
}

/// Run `work` on another thread.
pub func startBackground(work: in () -> Unit): Background {
  Background(task = taskRun(work))
}

/// Block until `b` has finished.
pub func waitBackground(b: in Background): Unit {
  try {
    taskWait(b.task)
  } catch Bug as _ {
    ()
  }
}

/// Block the calling thread for `ms` milliseconds.
pub func sleepMs(ms: in Int): Unit
  requires: ms >= 0
{
  threadSleep(ms)
}

/// Milliseconds on a clock that never goes backwards.
pub func monotonicMillis(): Long {
  tickCount64()
}

pub func newLock(): Lock {
  Lock()
}

pub func lockEnter(l: in Lock): Unit {
  monitorEnter(l)
}

pub func lockExit(l: in Lock): Unit {
  monitorExit(l)
}
