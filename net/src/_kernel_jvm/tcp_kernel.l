//! Net.Tcp.Kernel (jvm) - the socket boundary of `Net.Tcp` on
//! `--target jvm`. Use `Net.Tcp`, not this package.
//!
//! `java.net.Socket` / `java.net.ServerSocket` and their streams,
//! `javax.net.ssl.SSLSocket` for TLS, a `ReentrantLock`, `System.nanoTime` and
//! a daemon thread, all through the JVM auto-FFI (`extern type` +
//! `T.method(args)`); no Java shim. Every operation returns a `Result` with
//! the failure text; nothing throws.
//!
//! TLS: an `SSLContext` built from a `KeyStore`-backed `TrustManagerFactory`
//! (the configured CA, plus the platform's accepted issuers for additive
//! trust) and a `KeyManagerFactory` (client identity), whose socket factory
//! layers an `SSLSocket` over the connected socket. The host name is checked
//! by the JDK (`SSLParameters.setEndpointIdentificationAlgorithm("HTTPS")`).
//! An `SSLSocket` is a `Socket`: reads, read timeouts and writes are the same
//! code as for plain TCP. A socket has no write timeout, so a bounded write
//! (`sockWriteWithin`) runs on a thread of its own, which the caller watches
//! and, when no chunk gets through in time, ends by closing the socket.
//! Certificates and keys are loaded from PEM by the standard library's
//! `Std.TlsHost`. The server side (`sockAcceptTls`) exists for the in-process
//! servers of hermetic tests (`Net.Tcp.Testing`).
//!
//! A few JDK calls go through `java.lang.reflect` (the idiom of the standard
//! library's `_kernel_jvm/http_host.l`): the auto-FFI cannot pass a typed
//! reference array (`TrustManager[]`, `Certificate[]`, `String[]`) or `null`
//! (lyric-lang#4775) as an argument, and `KeyStore.load`, `KeyStore.setKeyEntry`,
//! `TrustManagerFactory.init`, `KeyManagerFactory.init`, `SSLContext.init` and
//! `SSLParameters.setProtocols` need one or the other.
//!
//! The dotnet twin (`src/_kernel_dotnet/tcp_kernel.l`) declares the same
//! package with the same public surface; a file-level `@cfg(target = ...)`
//! leaves exactly one of them in a build.
@axiom("the host socket, thread, lock and clock primitives conform to their documented platform contracts")
@cfg(target = "jvm")
@io
package Net.Tcp.Kernel

import Std.Core
import Std.Collections
import Std.Encoding
import Std.TlsHost as TlsHost

extern type JSocket = "java.net.Socket"

extern type JServerSocket = "java.net.ServerSocket"

extern type JInetSocketAddress = "java.net.InetSocketAddress"

extern type JInetAddress = "java.net.InetAddress"

extern type JInputStream = "java.io.InputStream"

extern type JOutputStream = "java.io.OutputStream"

extern type JBAOS = "java.io.ByteArrayOutputStream"

extern type JRunnable = "java.lang.Runnable"

extern type JThread = "java.lang.Thread"

extern type JSystem = "java.lang.System"

extern type JReentrantLock = "java.util.concurrent.locks.ReentrantLock"

extern type JTimeUnit = "java.util.concurrent.TimeUnit"

extern type JAtomicLong = "java.util.concurrent.atomic.AtomicLong"

extern type JObject = "java.lang.Object"

extern type JArrayList = "java.util.ArrayList"

extern type JClass = "java.lang.Class"

extern type JMethod = "java.lang.reflect.Method"

extern type JReflectArray = "java.lang.reflect.Array"

extern type JInvocationHandler = "java.lang.reflect.InvocationHandler"

extern type JClassLoader = "java.lang.ClassLoader"

extern type JCharacterCls = "java.lang.Character"

extern type JKeyStore = "java.security.KeyStore"

extern type JTrustManagerFactory = "javax.net.ssl.TrustManagerFactory"

extern type JKeyManagerFactory = "javax.net.ssl.KeyManagerFactory"

extern type JTrustManager = "javax.net.ssl.TrustManager"

extern type JSSLContext = "javax.net.ssl.SSLContext"

extern type JSSLParameters = "javax.net.ssl.SSLParameters"

extern type JSSLSocketFactory = "javax.net.ssl.SSLSocketFactory"

extern type JSSLSocket = "javax.net.ssl.SSLSocket"

/// One open connection: plain TCP, or TLS over it.
pub record Sock {
  socket: JSocket
  input: JInputStream
  output: JOutputStream
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
  server: JServerSocket
}

/// A unit of work running on its own thread.
pub record Background {
  thread: JThread
}

/// A reentrant mutual-exclusion lock. A `ref record`, so each lock is its own
/// instance.
pub ref record Lock {
  inner: JReentrantLock
}

record BackgroundJob {
  work: () -> Unit
}

impl JRunnable for BackgroundJob {
  func run(): Unit {
    val work = self.work
    work()
  }
}

func wrapSocket(socket: in JSocket): Sock {
  socket.setTcpNoDelay(true)
  val input: JInputStream = socket.getInputStream()
  val output: JOutputStream = socket.getOutputStream()
  Sock(socket = socket, input = input, output = output)
}

func closeQuietly(socket: in JSocket): Unit {
  try {
    socket.close()
  } catch Bug as _ {
    ()
  }
}

/// Open a TCP connection to `host:port`, waiting at most `connectTimeoutMs`
/// milliseconds for it (`0` = the operating system's limit).
pub func sockConnect(host: in String, port: in Int, connectTimeoutMs: in Int): Result[Sock, String]
  requires: connectTimeoutMs >= 0
{
  val socket: JSocket = JSocket.new()
  try {
    val address: JInetSocketAddress = JInetSocketAddress.new(host, port)
    socket.connect(address, connectTimeoutMs)
    Ok(wrapSocket(socket))
  } catch Bug as b {
    closeQuietly(socket)
    Err(b.message)
  }
}

func baosWriteByte(acc: in JBAOS, b: in Int): Unit {
  acc.write(b)
}

func baosWriteBytes(acc: in JBAOS, bytes: in slice[Byte]): Unit {
  acc.write(bytes)
}

// The first byte blocks (`read()`, bounded by the socket timeout), the rest is
// whatever has already arrived. The arrays come from the JDK (`readNBytes`,
// `toByteArray`), never from a caller-owned buffer filled through
// `read(byte[], int, int)`: the auto-FFI array marshalling does not propagate
// such writes back.
func readChunk(s: in Sock, maxBytes: in Int, timeoutMs: in Int): ReadOutcome {
  // A socket timeout of 0 means "wait forever", so a poll waits 1 ms.
  val wait = if timeoutMs < 1 { 1 } else { timeoutMs }
  s.socket.setSoTimeout(wait)
  val first = s.input.read()
  if first < 0 {
    return Eof
  }
  val acc: JBAOS = JBAOS.new()
  baosWriteByte(acc, first)
  val avail = s.input.available()
  if avail > 0 and maxBytes > 1 {
    val want = if avail < maxBytes - 1 { avail } else { maxBytes - 1 }
    baosWriteBytes(acc, s.input.readNBytes(want))
  }
  Data(bytes = acc.toByteArray())
}

/// Read up to `maxBytes` bytes, waiting at most `timeoutMs` milliseconds for
/// the first one (`0` = only what has already arrived).
pub func sockRead(s: in Sock, maxBytes: in Int, timeoutMs: in Int): Result[ReadOutcome, String]
  requires: maxBytes > 0
  requires: timeoutMs >= 0
{
  try {
    Ok(readChunk(s, maxBytes, timeoutMs))
  } catch Bug as b {
    // `typeName` is the simple class name on this target.
    if b.typeName == "SocketTimeoutException" or b.typeName == "java.net.SocketTimeoutException" {
      Ok(TimedOut)
    } else {
      Err(b.message)
    }
  }
}

func writeAll(output: in JOutputStream, bytes: in slice[Byte]): Unit {
  output.write(bytes)
  output.flush()
}

val writeChunkBytes: Int = 65536

// One bounded write, on its own thread. `progress` counts the chunks written,
// so the caller can tell a slow peer from one that accepts nothing.
ref record WriteJob {
  output: JOutputStream
  bytes: slice[Byte]
  progress: JAtomicLong
  var failure: String
}

func writeChunks(job: in WriteJob): Unit {
  val total = job.bytes.length
  if total <= writeChunkBytes {
    job.output.write(job.bytes)
  } else {
    var offset = 0
    while offset < total {
      val stop = if total - offset < writeChunkBytes { total } else { offset + writeChunkBytes }
      job.output.write(job.bytes.slice(offset, stop))
      val _ = job.progress.incrementAndGet()
      offset = stop
    }
  }
  job.output.flush()
}

impl JRunnable for WriteJob {
  func run(): Unit {
    try {
      writeChunks(self)
    } catch Bug as b {
      self.failure = if b.message.length > 0 { b.message } else { "the write failed" }
    }
  }
}

// Wait for the writer; false when a whole `timeoutMs` passed without a chunk
// getting through.
func writerFinished(writer: in JThread, job: in WriteJob, timeoutMs: in Int): Bool {
  var seen = 0i64
  while true {
    writer.join(timeoutMs.toLong())
    if not writer.isAlive() {
      return true
    }
    val now = job.progress.get()
    if now == seen {
      return false
    }
    seen = now
  }
  true
}

/// Write all of `bytes` and flush. The write is given up, with an `Err`, when
/// the peer accepts no data for `timeoutMs` milliseconds (`0` = no limit);
/// the socket is closed then, because how much was written is unknown.
pub func sockWriteWithin(s: in Sock, bytes: in slice[Byte], timeoutMs: in Int): Result[Unit, String]
  requires: timeoutMs >= 0
{
  try {
    if timeoutMs == 0 {
      writeAll(s.output, bytes)
      return Ok(())
    }
    val job = WriteJob(output = s.output, bytes = bytes, progress = JAtomicLong.new(), failure = "")
    val writer: JThread = JThread.startVirtualThread(job)
    if not writerFinished(writer, job, timeoutMs) {
      closeQuietly(s.socket)
      return Err("the write timed out: the peer accepted no data for " + timeoutMs.toString() + " ms")
    }
    if job.failure.length > 0 {
      Err(job.failure)
    } else {
      Ok(())
    }
  } catch Bug as b {
    Err(b.message)
  }
}

/// Write all of `bytes` and flush, however long the peer takes to accept them.
pub func sockWrite(s: in Sock, bytes: in slice[Byte]): Result[Unit, String] {
  sockWriteWithin(s, bytes, 0)
}

/// Close the connection. Closing twice is harmless.
pub func sockClose(s: in Sock): Unit {
  closeQuietly(s.socket)
}

// ─── TLS ─────────────────────────────────────────────────────────────────────
// The first public method of `className` called `name` with `arity`
// parameters, the first of them of type `firstParam` when that is not empty.
func findMethod(className: in String, name: in String, arity: in Int, firstParam: in String): JMethod {
  val cls: JClass = JClass.forName(className)
  val methods: slice[JMethod] = cls.getMethods()
  var i = 0
  while i < methods.length {
    val m = methods[i]
    if m.getName() == name and m.getParameterCount() == arity {
      if firstParam.length == 0 {
        return m
      }
      val parameters: slice[JClass] = m.getParameterTypes()
      if parameters[0].getName() == firstParam {
        return m
      }
    }
    i = i + 1
  }
  panic("no method " + className + "." + name + " with " + arity.toString() + " parameters")
}

// A `null` reference: the element of a fresh one-element array.
func nullRef(): JObject {
  val objectClass: JClass = JClass.forName("java.lang.Object")
  val holder: JObject = JReflectArray.newInstance(objectClass, 1)
  JReflectArray.get(holder, 0)
}

// An empty `char[]`: the "no password" of an in-memory key store.
func noPassword(): JObject {
  val charClass: JClass = JCharacterCls.TYPE
  JReflectArray.newInstance(charClass, 0)
}

func certificateArray(certificate: in TlsHost.JCertificate): JObject {
  val elementClass: JClass = JClass.forName("java.security.cert.Certificate")
  val array: JObject = JReflectArray.newInstance(elementClass, 1)
  JReflectArray.set(array, 0, certificate)
  array
}

func emptyKeyStore(): JKeyStore {
  val store: JKeyStore = JKeyStore.getInstance("PKCS12")
  val arguments: JArrayList = JArrayList.new()
  arguments.add(nullRef())
  arguments.add(noPassword())
  val _ = findMethod("java.security.KeyStore", "load", 2, "java.io.InputStream").invoke(store, arguments.toArray())
  store
}

// A trust manager factory over `store`; a `null` store is the platform's.
func trustFactory(store: in JObject): JTrustManagerFactory {
  val factory: JTrustManagerFactory = JTrustManagerFactory.getInstance(JTrustManagerFactory.getDefaultAlgorithm())
  val arguments: JArrayList = JArrayList.new()
  arguments.add(store)
  val _ = findMethod("javax.net.ssl.TrustManagerFactory", "init", 1, "java.security.KeyStore").invoke(
    factory,
    arguments.toArray()
  )
  factory
}

func loadFailure(what: in String, e: in TlsHost.HostLoadError): String {
  match e {
    case TlsHost.HostMalformed(m) -> what + " could not be loaded: " + m
    case TlsHost.HostMismatch(m) -> what + " could not be loaded: " + m
  }
}

func loadCertificate(what: in String, pem: in String): TlsHost.JCertificate {
  match TlsHost.hostCertFromPemBytes(encodeUtf8(pem)) {
    case Ok(h) -> h.cert
    case Err(e) -> panic(loadFailure(what, e))
  }
}

// The platform's trust anchors, as the `X509Certificate[]` its default trust
// manager reports. The method is looked up on the public interface: the
// implementing class is not exported from `java.base`.
func platformAnchors(): JObject {
  val managers: slice[JTrustManager] = trustFactory(nullRef()).getTrustManagers()
  val none: JArrayList = JArrayList.new()
  findMethod("javax.net.ssl.X509TrustManager", "getAcceptedIssuers", 0, "").invoke(managers[0], none.toArray())
}

// `TrustManager[]` that trusts `caPem`, plus the platform's anchors unless
// `exclusive`.
func trustManagersFor(what: in String, caPem: in String, exclusive: in Bool): JObject {
  val store = emptyKeyStore()
  store.setCertificateEntry("lyric-ca", loadCertificate(what, caPem))
  if not exclusive {
    val anchors = platformAnchors()
    val count = JReflectArray.getLength(anchors)
    var i = 0
    while i < count {
      val anchor: TlsHost.JCertificate = JReflectArray.get(anchors, i)
      store.setCertificateEntry("lyric-platform-" + i.toString(), anchor)
      i = i + 1
    }
  }
  val managers: JObject = trustFactory(store).getTrustManagers()
  managers
}

// `KeyManager[]` presenting the certificate and key.
func keyManagersFor(what: in String, certPem: in String, keyPem: in String): JObject {
  val handle = match TlsHost.hostIdentityFromPemBytes(encodeUtf8(certPem), encodeUtf8(keyPem)) {
    case Ok(h) -> h
    case Err(e) -> panic(loadFailure(what, e))
  }
  val key: TlsHost.JPrivateKey = match handle.key {
    case Some(k) -> k
    case None -> panic(what + " could not be loaded: no private key")
  }
  val store = emptyKeyStore()
  val entry: JArrayList = JArrayList.new()
  entry.add("lyric-identity")
  entry.add(key)
  entry.add(noPassword())
  entry.add(certificateArray(handle.cert))
  val _ = findMethod("java.security.KeyStore", "setKeyEntry", 4, "").invoke(store, entry.toArray())
  val factory: JKeyManagerFactory = JKeyManagerFactory.getInstance(JKeyManagerFactory.getDefaultAlgorithm())
  val arguments: JArrayList = JArrayList.new()
  arguments.add(store)
  arguments.add(noPassword())
  val _ = findMethod("javax.net.ssl.KeyManagerFactory", "init", 2, "java.security.KeyStore").invoke(
    factory,
    arguments.toArray()
  )
  val managers: JObject = factory.getKeyManagers()
  managers
}

// The trust manager behind `skipVerify`: a `java.lang.reflect.Proxy` for
// `X509TrustManager` whose checks return without throwing. A Lyric record
// cannot implement the interface directly, because its methods take
// `X509Certificate[]`.
record TrustEverything {
  label: String
}

func boxed[T](value: in T): JObject {
  val objectClass: JClass = JClass.forName("java.lang.Object")
  val holder: JObject = JReflectArray.newInstance(objectClass, 1)
  JReflectArray.set(holder, 0, value)
  JReflectArray.get(holder, 0)
}

impl JInvocationHandler for TrustEverything {
  func invoke(proxy: in JObject, method: in JMethod, args: in slice[JObject]): JObject {
    val name: String = method.getName()
    if name == "getAcceptedIssuers" {
      val certificateClass: JClass = JClass.forName("java.security.cert.X509Certificate")
      val none: JObject = JReflectArray.newInstance(certificateClass, 0)
      none
    } else if name == "toString" {
      val label: String = self.label
      boxed(label)
    } else if name == "hashCode" {
      val hash: Int = JSystem.identityHashCode(proxy)
      boxed(hash)
    } else if name == "equals" {
      val never: Bool = false
      boxed(never)
    } else {
      // `checkClientTrusted` / `checkServerTrusted` are `void`.
      nullRef()
    }
  }
}

@unsafe_ffi
func trustEverythingManagers(): JObject {
  val loader: JClassLoader = JClassLoader.getSystemClassLoader()
  val managerClass: JClass = JClass.forName("javax.net.ssl.X509TrustManager")
  val classClass: JClass = JClass.forName("java.lang.Class")
  val interfaces: JObject = JReflectArray.newInstance(classClass, 1)
  JReflectArray.set(interfaces, 0, managerClass)
  val handler: JInvocationHandler = TrustEverything(label = "Net.Tcp.Kernel.TrustEverything")
  val arguments: List[JObject] = newList()
  arguments.add(externAsObject(loader))
  arguments.add(interfaces)
  arguments.add(externAsObject(handler))
  val manager: JTrustManager = findMethod("java.lang.reflect.Proxy", "newProxyInstance", 3, "").invoke(
    nullRef(),
    arguments.toArray()
  )
  val managerArrayClass: JClass = JClass.forName("javax.net.ssl.TrustManager")
  val managers: JObject = JReflectArray.newInstance(managerArrayClass, 1)
  JReflectArray.set(managers, 0, manager)
  managers
}

// `keyManagers` / `trustManagers`: the arrays, or `null` for the defaults.
func sslContext(keyManagers: in JObject, trustManagers: in JObject): JSSLContext {
  val context: JSSLContext = JSSLContext.getInstance("TLS")
  val arguments: JArrayList = JArrayList.new()
  arguments.add(keyManagers)
  arguments.add(trustManagers)
  arguments.add(nullRef())
  val _ = findMethod("javax.net.ssl.SSLContext", "init", 3, "").invoke(context, arguments.toArray())
  context
}

func protocolArray(tls13Only: in Bool): JObject {
  val stringClass: JClass = JClass.forName("java.lang.String")
  val protocols: JObject = JReflectArray.newInstance(stringClass, if tls13Only { 1 } else { 2 })
  JReflectArray.set(protocols, 0, "TLSv1.3")
  if not tls13Only {
    JReflectArray.set(protocols, 1, "TLSv1.2")
  }
  protocols
}

// Layer an `SSLSocket` over `plain`. `peerName` is what the JDK sends as
// SNI and checks the peer certificate against.
func layerTls(context: in JSSLContext, plain: in JSocket, peerName: in String): JSocket {
  val factory: JSSLSocketFactory = context.getSocketFactory()
  factory.createSocket(plain, peerName, plain.getPort(), true)
}

func asSslSocket(layered: in JSocket): JSSLSocket {
  val sslClass: JClass = JClass.forName("javax.net.ssl.SSLSocket")
  val ssl: JSSLSocket = sslClass.cast(layered)
  ssl
}

func setProtocols(parameters: in JSSLParameters, tls13Only: in Bool): Unit {
  val arguments: JArrayList = JArrayList.new()
  arguments.add(protocolArray(tls13Only))
  val _ = findMethod("javax.net.ssl.SSLParameters", "setProtocols", 1, "").invoke(parameters, arguments.toArray())
}

func clientHandshake(plain: in JSocket, tls: in ClientTls, timeoutMs: in Int): Sock {
  val keyManagers = if tls.certPem.length > 0 {
    keyManagersFor("the client certificate and key", tls.certPem, tls.keyPem)
  } else {
    nullRef()
  }
  val trustManagers = if tls.skipVerify {
    trustEverythingManagers()
  } else if tls.caPem.length > 0 {
    trustManagersFor("the CA certificate", tls.caPem, tls.exclusiveCa)
  } else {
    nullRef()
  }
  val layered = layerTls(sslContext(keyManagers, trustManagers), plain, tls.serverName)
  val ssl = asSslSocket(layered)
  ssl.setUseClientMode(true)
  val parameters: JSSLParameters = ssl.getSSLParameters()
  if not tls.skipVerify {
    parameters.setEndpointIdentificationAlgorithm("HTTPS")
  }
  setProtocols(parameters, tls.tls13Only)
  ssl.setSSLParameters(parameters)
  layered.setSoTimeout(timeoutMs)
  ssl.startHandshake()
  wrapSocket(layered)
}

func serverHandshake(plain: in JSocket, tls: in ServerTls, timeoutMs: in Int): Sock {
  val keyManagers = keyManagersFor("the server certificate and key", tls.certPem, tls.keyPem)
  val needClient = tls.clientCaPem.length > 0
  val trustManagers = if needClient {
    trustManagersFor("the client CA certificate", tls.clientCaPem, true)
  } else {
    nullRef()
  }
  val layered = layerTls(sslContext(keyManagers, trustManagers), plain, "localhost")
  val ssl = asSslSocket(layered)
  ssl.setUseClientMode(false)
  ssl.setNeedClientAuth(needClient)
  layered.setSoTimeout(timeoutMs)
  ssl.startHandshake()
  wrapSocket(layered)
}

func handshakeFailure(typeName: in String, message: in String, timeoutMs: in Int): String {
  if typeName == "SocketTimeoutException" or typeName == "java.net.SocketTimeoutException" {
    "timed out after " + timeoutMs.toString() + " ms"
  } else {
    message
  }
}

/// Run the client side of a TLS handshake over `s` and return the secured
/// connection. It waits at most `timeoutMs` milliseconds (`0` = no limit).
/// On `Err` the connection is closed.
pub func sockStartTls(s: in Sock, tls: in ClientTls, timeoutMs: in Int): Result[Sock, String]
  requires: timeoutMs >= 0
{
  try {
    Ok(clientHandshake(s.socket, tls, timeoutMs))
  } catch Bug as b {
    closeQuietly(s.socket)
    Err(handshakeFailure(b.typeName, b.message, timeoutMs))
  }
}

/// Run the server side of a TLS handshake over an accepted connection. On
/// `Err` the connection is closed.
pub func sockAcceptTls(s: in Sock, tls: in ServerTls, timeoutMs: in Int): Result[Sock, String]
  requires: timeoutMs >= 0
{
  try {
    Ok(serverHandshake(s.socket, tls, timeoutMs))
  } catch Bug as b {
    closeQuietly(s.socket)
    Err(handshakeFailure(b.typeName, b.message, timeoutMs))
  }
}

/// Listen on the IP literal `host` and `port` (`port = 0` picks a free port).
pub func serverListen(host: in String, port: in Int): Result[ServerSock, String] {
  try {
    val address: JInetAddress = JInetAddress.getByName(host)
    val server: JServerSocket = JServerSocket.new(port, 50, address)
    Ok(ServerSock(server = server))
  } catch Bug as b {
    Err(b.message)
  }
}

/// The port `server` is bound to.
pub func serverPort(server: in ServerSock): Int {
  server.server.getLocalPort()
}

/// Block until a client connects.
pub func serverAccept(server: in ServerSock): Result[Sock, String] {
  try {
    val socket: JSocket = server.server.accept()
    Ok(wrapSocket(socket))
  } catch Bug as b {
    Err(b.message)
  }
}

/// Stop listening.
pub func serverClose(server: in ServerSock): Unit {
  try {
    server.server.close()
  } catch Bug as _ {
    ()
  }
}

func startThread(t: in JThread): Unit {
  t.setDaemon(true)
  t.start()
}

/// Run `work` on a thread of its own.
pub func startBackground(work: in () -> Unit): Background {
  val t: JThread = JThread.new(BackgroundJob(work = work))
  startThread(t)
  Background(thread = t)
}

/// On dotnet this queues `work` to the runtime's shared thread pool, for tests
/// that keep the pool busy. The JVM kernel uses no shared pool, so here it is
/// `startBackground`.
pub func startPoolWork(work: in () -> Unit): Background {
  startBackground(work)
}

/// Block until `b` has finished.
pub func waitBackground(b: in Background): Unit {
  try {
    b.thread.join()
  } catch Bug as _ {
    ()
  }
}

/// Block the calling thread for `ms` milliseconds.
pub func sleepMs(ms: in Int): Unit
  requires: ms >= 0
{
  try {
    JThread.sleep(ms.toLong())
  } catch Bug as _ {
    ()
  }
}

/// Milliseconds on a clock that never goes backwards.
pub func monotonicMillis(): Long {
  JSystem.nanoTime() / 1000000i64
}

// Fair: a consumer waiting for a message gives the lock up every 100 ms and
// takes it again at once. With the default (barging) policy it usually wins
// that race against a publisher parked on the lock, which can then starve
// for the whole of the wait.
pub func newLock(): Lock {
  Lock(inner = JReentrantLock.new(true))
}

pub func lockEnter(l: in Lock): Unit {
  l.inner.lock()
}

/// Take the lock if it becomes free within `timeoutMs` milliseconds (`0` =
/// only if it is free now). False when it did not.
pub func lockTryEnter(l: in Lock, timeoutMs: in Int): Bool
  requires: timeoutMs >= 0
{
  try {
    l.inner.tryLock(timeoutMs.toLong(), JTimeUnit.MILLISECONDS)
  } catch Bug as _ {
    false
  }
}

pub func lockExit(l: in Lock): Unit {
  l.inner.unlock()
}
