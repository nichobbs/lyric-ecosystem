//! Net.Tcp.Kernel (dotnet) - the socket boundary of `Net.Tcp` on
//! `--target dotnet`. Use `Net.Tcp`, not this package.
//!
//! `System.Net.Sockets.TcpClient` / `TcpListener`, `System.Net.Security.SslStream`
//! for TLS, a monitor lock, a monotonic clock and a background thread. Every
//! operation returns a `Result` with the failure text; nothing throws.
//!
//! `Std.TcpHost` is not used: a protocol client has to tell "nothing arrived
//! within the wait" from "the connection failed", and `Std.TcpHost` reports a
//! receive timeout as the same `TransportFailed` as a reset. Here each
//! connection that is read from has a reader thread of its own, which does
//! blocking `Stream.Read` calls and hands each chunk to `sockRead` through a
//! pair of semaphores; `sockRead` waits for the hand-over with a limit. That
//! is exact for a plain socket and for an `SslStream` alike, where polling the
//! socket would be wrong: bytes on the wire may be a handshake message or part
//! of a record (no plaintext yet), and plaintext may already sit decrypted in
//! the stream with nothing left on the wire. The reader reads the next chunk
//! only after the previous one was taken, and the end of the stream or a read
//! failure is handed over after every chunk read before it, so nothing that
//! arrived is lost or reordered.
//!
//! Nothing here runs on, or waits for, the runtime's shared thread pool. The
//! connect, the TLS handshake, the reader and `startBackground` each run on a
//! thread of their own (`TaskCreationOptions.LongRunning`), and reads and
//! writes are the synchronous stream calls. The asynchronous calls
//! (`ConnectAsync`, `ReadAsync`) complete on pool threads, so an application
//! whose pool threads are all blocked, as they are when it calls this
//! synchronous client from many tasks at once, would see its reads complete
//! seconds late: a consume would time out with the message already received.
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
//! (`sockAcceptTls`) exists for the in-process servers of hermetic tests
//! (`Net.Tcp.Testing`).
//!
//! The JVM twin (`src/_kernel_jvm/tcp_kernel.l`) declares the same package
//! with the same public surface; a file-level `@cfg(target = ...)` leaves
//! exactly one of them in a build.
@axiom("the host socket, thread, lock and clock primitives conform to their documented platform contracts")
@cfg(target = "dotnet")
@io
package Net.Tcp.Kernel

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

extern type TaskFactory = "System.Threading.Tasks.TaskFactory"

extern type TaskCreationOptions = "System.Threading.Tasks.TaskCreationOptions"

extern type Semaphore = "System.Threading.SemaphoreSlim"

extern type NetObject = "System.Object"

extern type NetMonitor = "System.Threading.Monitor"

@externTarget("System.Net.Sockets.TcpClient..ctor")
func newTcpClient(): TcpClient = ()

@externInstance
@externTarget("System.Net.Sockets.TcpClient.Connect")
func clientConnect(c: in TcpClient, host: in String, port: in Int): Unit = ()

@externInstance
@externTarget("System.Net.Sockets.TcpClient.set_SendTimeout")
func clientSetSendTimeout(c: in TcpClient, ms: in Int): Unit = ()

@externStatic
@externTarget("System.Threading.Tasks.Task.get_Factory")
func taskFactory(): TaskFactory = ()

@externInstance
@externTarget("System.Threading.Tasks.TaskFactory.StartNew")
func factoryStartNew(f: in TaskFactory, work: in () -> Unit, options: in TaskCreationOptions): BclTask = ()

@externTarget("System.Threading.Tasks.TaskCreationOptions.LongRunning")
func longRunning(): TaskCreationOptions = ()

@externTarget("System.Threading.SemaphoreSlim..ctor")
func newSemaphore(initialCount: in Int): Semaphore = ()

@externInstance
@externTarget("System.Threading.SemaphoreSlim.Wait")
func semWait(s: in Semaphore): Unit = ()

@externInstance
@externTarget("System.Threading.SemaphoreSlim.Wait")
func semWaitFor(s: in Semaphore, timeoutMs: in Int): Bool = ()

@externInstance
@externTarget("System.Threading.SemaphoreSlim.Release")
func semRelease(s: in Semaphore): Int = ()

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
@externTarget("System.Net.Sockets.TcpClient.GetStream")
func clientStream(c: in TcpClient): Stream = ()

@externInstance
@externTarget("System.Net.Sockets.TcpClient.Close")
func clientClose(c: in TcpClient): Unit = ()

@externInstance
@externTarget("System.Net.Sockets.TcpClient.set_NoDelay")
func clientSetNoDelay(c: in TcpClient, on: in Bool): Unit = ()

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

/// What a connection's reader thread hands to `sockRead`, and bytes a read
/// returned beyond what its caller asked for.
pub ref record ReadState {
  var carry: slice[Byte]
  // Whether the reader thread has been started (by the first `sockRead`).
  var started: Bool
  // Written by the reader before it signals `ready`, read by `sockRead` after
  // it has taken `ready`: one chunk, or the end of the stream (`ended`, with
  // `failure` when the read failed).
  var chunk: slice[Byte]
  var ended: Bool
  var failure: String
  // Signalled once per hand-over, and left signalled at the end of the stream.
  ready: Semaphore
  // Signalled when `sockRead` has taken a chunk: the reader may read again.
  taken: Semaphore
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

/// A bound, listening TCP socket (the loopback peer of hermetic tests; a
/// client never listens).
pub record ServerSock {
  listener: TcpListener
}

/// A unit of work running on its own thread.
pub record Background {
  task: BclTask
}

/// A reentrant mutual-exclusion lock. A `ref record`, so each lock is its own
/// instance; the monitor is that of the `System.Object` it holds.
pub ref record Lock {
  gate: NetObject
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
    reader = ReadState(
      carry = noBytes(),
      started = false,
      chunk = noBytes(),
      ended = false,
      failure = "",
      ready = newSemaphore(0),
      taken = newSemaphore(0)
    )
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

ref record Attempt {
  var failure: String
}

func runGuarded(outcome: in Attempt, fallback: in String, work: in () -> Unit): Unit {
  try {
    work()
  } catch Bug as b {
    outcome.failure = if b.message.length > 0 { causeOf(b.message) } else { fallback }
  }
}

// Start `work` on a thread of its own, not one of the shared pool's.
func runDedicated(work: in () -> Unit): BclTask {
  factoryStartNew(taskFactory(), work, longRunning())
}

// Run a blocking call on its own thread so it can be abandoned: `Ok(false)`
// when `timeoutMs` (0 = no limit) passed first. The caller then closes the
// socket, which ends a call still in flight.
func runWithin(work: in () -> Unit, timeoutMs: in Int, fallback: in String): Result[Bool, String] {
  val outcome = Attempt(failure = "")
  val running = runDedicated({ -> runGuarded(outcome, fallback, work) })
  if timeoutMs == 0 {
    taskWait(running)
  } else if not taskWaitFor(running, timeoutMs) {
    return Ok(false)
  }
  if outcome.failure.length > 0 {
    Err(outcome.failure)
  } else {
    Ok(true)
  }
}

/// Open a TCP connection to `host:port`, waiting at most `connectTimeoutMs`
/// milliseconds for it (`0` = the operating system's limit).
pub func sockConnect(host: in String, port: in Int, connectTimeoutMs: in Int): Result[Sock, String]
  requires: connectTimeoutMs >= 0
{
  val client = newTcpClient()
  try {
    match runWithin({ -> clientConnect(client, host, port) }, connectTimeoutMs, "the connect failed") {
      case Ok(true) -> Ok(wrapClient(client))
      case Ok(false) -> {
        closeQuietly(client)
        Err("connect to " + host + ":" + port.toString() + " timed out after " + connectTimeoutMs.toString() + " ms")
      }
      case Err(e) -> {
        closeQuietly(client)
        Err(e)
      }
    }
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

// The reader thread of one connection: read a chunk, hand it over, wait until
// it was taken. It ends with the stream (the peer's close, a failure, or
// `sockClose`, which fails the read in flight).
func readLoop(s: in Sock): Unit {
  val state = s.reader
  var open = true
  while open {
    try {
      val n = streamReadInto(s.stream, memGetBuffer(s.readBuf), 0, readBufferSize)
      if n <= 0 {
        state.ended = true
        open = false
      } else {
        state.chunk = memGetBuffer(s.readBuf).slice(0, n)
      }
    } catch Bug as b {
      state.failure = if b.message.length > 0 { causeOf(b.message) } else { "the read failed" }
      state.ended = true
      open = false
    }
    val _ = semRelease(state.ready)
    if open {
      semWait(state.taken)
    }
  }
}

/// Read up to `maxBytes` bytes, waiting at most `timeoutMs` milliseconds for
/// the first one (`0` = only what has already arrived). Bytes that arrived
/// before the stream ended or failed are returned before the `Eof` or `Err`,
/// which every later call then repeats.
pub func sockRead(s: in Sock, maxBytes: in Int, timeoutMs: in Int): Result[ReadOutcome, String]
  requires: maxBytes > 0
  requires: timeoutMs >= 0
{
  val state = s.reader
  if state.carry.length > 0 {
    return Ok(Data(bytes = takeCarry(state, maxBytes)))
  }
  try {
    if not state.started {
      state.started = true
      val _ = runDedicated({ -> readLoop(s) })
    }
    // A wait of 0 would miss a chunk the reader is handing over right now.
    if not semWaitFor(state.ready, if timeoutMs < 1 { 1 } else { timeoutMs }) {
      return Ok(TimedOut)
    }
    if state.ended {
      val _ = semRelease(state.ready)
      if state.failure.length > 0 {
        return Err(state.failure)
      }
      return Ok(Eof)
    }
    state.carry = state.chunk
    state.chunk = noBytes()
    val _ = semRelease(state.taken)
    Ok(Data(bytes = takeCarry(state, maxBytes)))
  } catch Bug as b {
    Err(causeOf(b.message))
  }
}

/// Write all of `bytes` and flush. The write is given up, with an `Err`, when
/// the peer accepts no data for `timeoutMs` milliseconds (`0` = no limit);
/// how much was written by then is unknown, so the connection is of no further
/// use.
pub func sockWriteWithin(s: in Sock, bytes: in slice[Byte], timeoutMs: in Int): Result[Unit, String]
  requires: timeoutMs >= 0
{
  val started = tickCount64()
  try {
    clientSetSendTimeout(s.client, timeoutMs)
    streamWriteFrom(s.stream, bytes, 0, bytes.length)
    streamFlush(s.stream)
    Ok(())
  } catch Bug as b {
    if timeoutMs > 0 and tickCount64() - started >= timeoutMs.toLong() {
      Err("the write timed out: the peer accepted no data for " + timeoutMs.toString() + " ms")
    } else {
      Err(b.message)
    }
  }
}

/// Write all of `bytes` and flush, however long the peer takes to accept them.
pub func sockWrite(s: in Sock, bytes: in slice[Byte]): Result[Unit, String] {
  sockWriteWithin(s, bytes, 0)
}

/// Close the connection. Closing twice is harmless.
pub func sockClose(s: in Sock): Unit {
  try {
    streamClose(s.stream)
  } catch Bug as _ {
    ()
  }
  closeQuietly(s.client)
  // A reader waiting for its last chunk to be taken goes on to a read that
  // now fails, and ends.
  try {
    val _ = semRelease(s.reader.taken)
  } catch Bug as _ {
    ()
  }
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

// A handshake that fails or outlasts `timeoutMs` (0 = no limit) is an `Err`.
func handshakeWithin(work: in () -> Unit, timeoutMs: in Int): Result[Unit, String] {
  match runWithin(work, timeoutMs, "the handshake failed") {
    case Ok(true) -> Ok(())
    case Ok(false) -> Err("timed out after " + timeoutMs.toString() + " ms")
    case Err(e) -> Err(e)
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

/// Run `work` on a thread of its own.
pub func startBackground(work: in () -> Unit): Background {
  Background(task = runDedicated(work))
}

/// Queue `work` to the runtime's shared thread pool, which the rest of this
/// kernel stays away from. For tests that keep the pool busy.
pub func startPoolWork(work: in () -> Unit): Background {
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
  Lock(gate = NetObject.new())
}

pub func lockEnter(l: in Lock): Unit {
  NetMonitor.Enter(l.gate)
}

/// Take the lock if it becomes free within `timeoutMs` milliseconds (`0` =
/// only if it is free now). False when it did not.
pub func lockTryEnter(l: in Lock, timeoutMs: in Int): Bool
  requires: timeoutMs >= 0
{
  NetMonitor.TryEnter(l.gate, timeoutMs)
}

pub func lockExit(l: in Lock): Unit {
  NetMonitor.Exit(l.gate)
}
