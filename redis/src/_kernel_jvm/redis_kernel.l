//! Redis.Kernel (jvm) - the socket boundary of the Redis client on
//! `--target jvm`.
//!
//! `java.net.Socket` / `java.net.ServerSocket` and their streams through the
//! JVM auto-FFI (`extern type` + `T.method(args)`); no Java shim. Every
//! operation returns a `Result` with the failure text; nothing throws.
//!
//! TLS (`tlsPrepare`, `sockStartTls`) is JSSE: an `SSLContext` built once per
//! client, an `SSLSocket` layered over the connected socket, and
//! `SSLParameters` with the endpoint identification algorithm `HTTPS`, which
//! makes the handshake check the server name against the certificate.
//! Certificates and keys are parsed by `Std.TlsHost`, the standard library's
//! certificate kernel. The default trust is the JDK's own trust store; a
//! configured CA becomes a `KeyStore` holding exactly that certificate (the
//! JDK store is then not consulted); a client identity is a `KeyStore` entry
//! behind a `KeyManagerFactory`. `skipVerify` (already resolved through the
//! dual-key policy by `Redis`) installs a trust manager that accepts every
//! chain and clears the endpoint identification algorithm.
//!
//! Some JSSE and `KeyStore` calls take `null`, a `char[]` or a typed object
//! array, which the auto-FFI cannot express as arguments; those calls go
//! through `java.lang.reflect.Method.invoke` (bound by auto-FFI like every
//! other JDK member here), the technique of the standard library's JVM
//! HTTP kernel (`std/_kernel_jvm/http_host.l`).
//!
//! The dotnet twin (`src/_kernel_dotnet/redis_kernel.l`) declares the same
//! package with the same public surface; a file-level `@cfg(target = ...)`
//! leaves exactly one of them in a build.
@axiom("the host socket, TLS and thread APIs (System.Net.Sockets.TcpClient, System.Net.Security.SslStream, System.Security.Cryptography.X509Certificates, Std.TcpHost, Std.TlsHost and System.Threading.Tasks.Task on dotnet; java.net.Socket, java.net.ServerSocket, javax.net.ssl, java.security.KeyStore, java.io streams and java.lang.Thread on the JVM) conform to their documented contracts")
@cfg(target = "jvm")
@io
package Redis.Kernel

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

extern type JObject = "java.lang.Object"

// Takes any value, `null` included, through the extern boundary and yields a
// genuine `Object[]` for a reflective call.
extern type JArrayList = "java.util.ArrayList"

extern type JClass = "java.lang.Class"

extern type JMethod = "java.lang.reflect.Method"

extern type JReflectArray = "java.lang.reflect.Array"

extern type JInvocationHandler = "java.lang.reflect.InvocationHandler"

extern type JClassLoader = "java.lang.ClassLoader"

extern type JCharacterCls = "java.lang.Character"

extern type JSystem = "java.lang.System"

extern type JKeyStore = "java.security.KeyStore"

extern type JSecureRandom = "java.security.SecureRandom"

extern type JTrustManagerFactory = "javax.net.ssl.TrustManagerFactory"

extern type JKeyManagerFactory = "javax.net.ssl.KeyManagerFactory"

extern type JTrustManager = "javax.net.ssl.TrustManager"

extern type JSSLContext = "javax.net.ssl.SSLContext"

extern type JSSLSocketFactory = "javax.net.ssl.SSLSocketFactory"

extern type JSSLServerSocketFactory = "javax.net.ssl.SSLServerSocketFactory"

extern type JSSLSocket = "javax.net.ssl.SSLSocket"

extern type JSSLServerSocket = "javax.net.ssl.SSLServerSocket"

extern type JSSLParameters = "javax.net.ssl.SSLParameters"

/// One open TCP connection, plain or TLS (`socket` is then the `SSLSocket`).
pub record Sock {
  socket: JSocket
  input: JInputStream
  output: JOutputStream
}

/// A bound, listening TCP socket (used by the in-process fake servers of the
/// test suites; the client itself never listens). A TLS server's `server` is
/// an `SSLServerSocket`.
pub record ServerSock {
  server: JServerSocket
}

/// What a TLS client connection verifies and presents. PEM text; `""` leaves
/// the item out.
pub record TlsSettings {
  /// The only CA the server certificate may chain to; `""` = the JDK's
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
  context: JSSLContext
  tls13Only: Bool
  skipVerify: Bool
}

/// A unit of work running on its own thread.
pub record Background {
  thread: JThread
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
/// milliseconds for it (`0` = no limit), and bound every later read to
/// `ioTimeoutMs` milliseconds of inactivity (`0` = no limit).
pub func sockConnect(host: in String, port: in Int, connectTimeoutMs: in Int, ioTimeoutMs: in Int): Result[Sock, String]
  requires: connectTimeoutMs >= 0
  requires: ioTimeoutMs >= 0
{
  val socket: JSocket = JSocket.new()
  try {
    val address: JInetSocketAddress = JInetSocketAddress.new(host, port)
    socket.connect(address, connectTimeoutMs)
    socket.setSoTimeout(ioTimeoutMs)
    Ok(wrapSocket(socket))
  } catch Bug as b {
    closeQuietly(socket)
    Err(b.message)
  }
}

// ─── Reflection helpers ──────────────────────────────────────────────────────
func findMethod(cls: in JClass, name: in String, arity: in Int, firstParamClass: in String): JMethod {
  val methods: slice[JMethod] = cls.getMethods()
  var i = 0
  var found: Option[JMethod] = None
  var matches = 0
  while i < methods.length {
    val m = methods[i]
    if m.getName() == name and m.getParameterCount() == arity {
      var wanted = true
      if firstParamClass.length > 0 {
        val ptypes: slice[JClass] = m.getParameterTypes()
        wanted = ptypes[0].getName() == firstParamClass
      }
      if wanted {
        found = Some(value = m)
        matches = matches + 1
      }
    }
    i = i + 1
  }
  if matches != 1 {
    panic(
      "Redis.Kernel: reflective lookup of " + name + "/" + arity.toString() + " matched " + matches.toString() +
        " methods"
    )
  }
  match found {
    case Some(m) -> m
    case None -> panic("Redis.Kernel: reflective lookup of " + name + " failed")
  }
}

// A genuine `null`: the class only shapes the scratch array it is read from.
func nullOfClass(className: in String): JObject {
  val c: JClass = JClass.forName(className)
  val arr: JObject = JReflectArray.newInstance(c, 1)
  JReflectArray.get(arr, 0)
}

// An empty `char[]`: the "no password" argument of the KeyStore calls.
func emptyCharArray(): JObject {
  val charClass: JClass = JCharacterCls.TYPE
  JReflectArray.newInstance(charClass, 0)
}

// A one-element array whose component type is `elemClassName`.
func typedArrayOf[T](elemClassName: in String, v: in T): JObject {
  val elemClass: JClass = JClass.forName(elemClassName)
  val arr: JObject = JReflectArray.newInstance(elemClass, 1)
  JReflectArray.set(arr, 0, v)
  arr
}

func stringArrayOf(values: in List[String]): JObject {
  val elemClass: JClass = JClass.forName("java.lang.String")
  val arr: JObject = JReflectArray.newInstance(elemClass, values.count)
  var i = 0
  while i < values.count {
    JReflectArray.set(arr, i, values[i])
    i = i + 1
  }
  arr
}

func invoke1[A, B](target: in A, methodName: in String, firstParamClass: in String, arg0: in B): JObject {
  val cls: JClass = target.getClass()
  val m = findMethod(cls, methodName, 1, firstParamClass)
  val args: JArrayList = JArrayList.new()
  args.add(arg0)
  m.invoke(target, args.toArray())
}

func invoke2[A, B, C](target: in A, methodName: in String, arg0: in B, arg1: in C): JObject {
  val cls: JClass = target.getClass()
  val m = findMethod(cls, methodName, 2, "")
  val args: JArrayList = JArrayList.new()
  args.add(arg0)
  args.add(arg1)
  m.invoke(target, args.toArray())
}

func invoke3[A, B, C, D](target: in A, methodName: in String, arg0: in B, arg1: in C, arg2: in D): JObject {
  val cls: JClass = target.getClass()
  val m = findMethod(cls, methodName, 3, "")
  val args: JArrayList = JArrayList.new()
  args.add(arg0)
  args.add(arg1)
  args.add(arg2)
  m.invoke(target, args.toArray())
}

func invoke4[A, B, C, D, E](
  target: in A, methodName: in String, arg0: in B, arg1: in C, arg2: in D, arg3: in E
): JObject {
  val cls: JClass = target.getClass()
  val m = findMethod(cls, methodName, 4, "")
  val args: JArrayList = JArrayList.new()
  args.add(arg0)
  args.add(arg1)
  args.add(arg2)
  args.add(arg3)
  m.invoke(target, args.toArray())
}

// A value as a `java.lang.Object`, boxed if it is a primitive.
func boxAsObject[T](v: in T): JObject {
  val cls: JClass = JClass.forName("java.lang.Object")
  val arr: JObject = JReflectArray.newInstance(cls, 1)
  JReflectArray.set(arr, 0, v)
  JReflectArray.get(arr, 0)
}

// ─── TLS ─────────────────────────────────────────────────────────────────────
func emptyKeyStore(): JKeyStore {
  val ks: JKeyStore = JKeyStore.getInstance("PKCS12")
  val _ = invoke2(ks, "load", nullOfClass("java.io.InputStream"), emptyCharArray())
  ks
}

// Trust managers that accept only chains ending in `ca`.
func trustManagersFor(ca: in TlsHost.JCertificate): JObject {
  val ks = emptyKeyStore()
  ks.setCertificateEntry("lyric-redis-ca", ca)
  val algorithm: String = JTrustManagerFactory.getDefaultAlgorithm()
  val tmf: JTrustManagerFactory = JTrustManagerFactory.getInstance(algorithm)
  val _ = invoke1(tmf, "init", "java.security.KeyStore", ks)
  val managers: JObject = tmf.getTrustManagers()
  managers
}

// Key managers presenting `identity` (certificate and private key).
func keyManagersFor(identity: in TlsHost.CertHandle): JObject {
  val key: TlsHost.JPrivateKey = match identity.key {
    case Some(k) -> k
    case None -> panic("Redis.Kernel: the identity has no private key")
  }
  val ks = emptyKeyStore()
  val cert: TlsHost.JCertificate = identity.cert
  val chain = typedArrayOf("java.security.cert.Certificate", cert)
  val _ = invoke4(ks, "setKeyEntry", "lyric-redis-identity", key, emptyCharArray(), chain)
  val algorithm: String = JKeyManagerFactory.getDefaultAlgorithm()
  val kmf: JKeyManagerFactory = JKeyManagerFactory.getInstance(algorithm)
  val _ = invoke2(kmf, "init", ks, emptyCharArray())
  val managers: JObject = kmf.getKeyManagers()
  managers
}

// The trust manager behind `skipVerify`: an `X509TrustManager` whose check
// methods return normally for every chain. It is a `java.lang.reflect.Proxy`
// because `X509TrustManager`'s methods take and return `X509Certificate[]`,
// which a Lyric `impl` of the interface cannot declare; `InvocationHandler`
// has only `Object` and `Object[]` in its one method.
record TrustEveryChain {
  marker: Bool
}

impl JInvocationHandler for TrustEveryChain {
  func invoke(proxy: in JObject, method: in JMethod, args: in slice[JObject]): JObject {
    val name: String = method.getName()
    if name == "getAcceptedIssuers" {
      val cls: JClass = JClass.forName("java.security.cert.X509Certificate")
      val none: JObject = JReflectArray.newInstance(cls, 0)
      none
    } else if name == "toString" {
      val text: String = "Redis.Kernel trust manager without verification"
      boxAsObject(text)
    } else if name == "hashCode" {
      val hash: Int = JSystem.identityHashCode(proxy)
      boxAsObject(hash)
    } else if name == "equals" {
      // Never equal to anything: JSSE does not compare trust managers.
      val no: Bool = false
      boxAsObject(no)
    } else {
      // checkClientTrusted / checkServerTrusted are void: returning without
      // an exception accepts the chain.
      nullOfClass("java.lang.Object")
    }
  }
}

@unsafe_ffi
func trustManagersWithoutVerification(): JObject {
  val loader: JClassLoader = JClassLoader.getSystemClassLoader()
  val tmClass: JClass = JClass.forName("javax.net.ssl.X509TrustManager")
  val interfaces = typedArrayOf("java.lang.Class", tmClass)
  val handler: TrustEveryChain = TrustEveryChain(marker = true)
  val asHandler: JInvocationHandler = handler
  val proxyClass: JClass = JClass.forName("java.lang.reflect.Proxy")
  val newProxy = findMethod(proxyClass, "newProxyInstance", 3, "")
  val args: List[JObject] = newList()
  args.add(externAsObject(loader))
  args.add(interfaces)
  args.add(externAsObject(asHandler))
  val manager: JTrustManager = newProxy.invoke(nullOfClass("java.lang.Object"), args.toArray())
  typedArrayOf("javax.net.ssl.TrustManager", manager)
}

// `SSLContext.init(null, ...)` / `(..., null, ...)` selects the JDK's default
// key or trust managers.
func newContext(keyManagers: in JObject, trustManagers: in JObject): JSSLContext {
  val ctx: JSSLContext = JSSLContext.getInstance("TLS")
  val random: JSecureRandom = JSecureRandom.new()
  val _ = invoke3(ctx, "init", keyManagers, trustManagers, random)
  ctx
}

func loadMessage(e: in TlsHost.HostLoadError): String {
  match e {
    case TlsHost.HostMalformed(m) -> m
    case TlsHost.HostMismatch(m) -> m
  }
}

func buildClientContext(settings: in TlsSettings): Result[JSSLContext, String] {
  var trustManagers: JObject = nullOfClass("java.lang.Object")
  if settings.skipVerify {
    trustManagers = trustManagersWithoutVerification()
  } else if settings.caPem.length > 0 {
    match TlsHost.hostCertFromPemBytes(encodeUtf8(settings.caPem)) {
      case Ok(handle) -> {
        trustManagers = trustManagersFor(handle.cert)
      }
      case Err(e) -> return Err("CA certificate: " + loadMessage(e))
    }
  }
  var keyManagers: JObject = nullOfClass("java.lang.Object")
  if settings.certPem.length > 0 {
    match TlsHost.hostIdentityFromPemBytes(encodeUtf8(settings.certPem), encodeUtf8(settings.keyPem)) {
      case Ok(handle) -> {
        keyManagers = keyManagersFor(handle)
      }
      case Err(e) -> return Err("client certificate and key: " + loadMessage(e))
    }
  }
  Ok(newContext(keyManagers, trustManagers))
}

/// Parse `settings` for use by `sockStartTls`. An unusable certificate or key
/// is `Err` with the reason.
pub func tlsPrepare(settings: in TlsSettings): Result[TlsClient, String]
  requires: (settings.certPem.length == 0) == (settings.keyPem.length == 0)
{
  try {
    match buildClientContext(settings) {
      case Ok(ctx) -> Ok(TlsClient(context = ctx, tls13Only = settings.tls13Only, skipVerify = settings.skipVerify))
      case Err(m) -> Err(m)
    }
  } catch Bug as b {
    Err(b.message)
  }
}

func protocolsFor(tls13Only: in Bool): JObject {
  val names: List[String] = newList()
  if not tls13Only {
    names.add("TLSv1.2")
  }
  names.add("TLSv1.3")
  stringArrayOf(names)
}

func handshake(s: in Sock, tls: in TlsClient, serverName: in String, port: in Int): Sock {
  val factory: JSSLSocketFactory = tls.context.getSocketFactory()
  // Layered over the connected socket, so the connect timeout and SO_TIMEOUT
  // set by `sockConnect` stay in force; `serverName` is the SNI name (when it
  // is not an IP literal) and the name the certificate is checked against.
  val ssl: JSSLSocket = factory.createSocket(s.socket, serverName, port, true)
  val params: JSSLParameters = ssl.getSSLParameters()
  val _ = invoke1(params, "setProtocols", "", protocolsFor(tls.tls13Only))
  if tls.skipVerify {
    params.setEndpointIdentificationAlgorithm("")
  } else {
    params.setEndpointIdentificationAlgorithm("HTTPS")
  }
  ssl.setSSLParameters(params)
  ssl.startHandshake()
  val input: JInputStream = ssl.getInputStream()
  val output: JOutputStream = ssl.getOutputStream()
  val asSocket: JSocket = ssl
  Sock(socket = asSocket, input = input, output = output)
}

/// Run the TLS handshake as a client on the freshly dialled `s` and return
/// the encrypted connection. `serverName` is the name sent as SNI and the
/// name the server certificate must carry. The handshake is bounded by the
/// connection's I/O timeout. On `Err` the connection has been closed.
pub func sockStartTls(s: in Sock, tls: in TlsClient, serverName: in String, port: in Int): Result[Sock, String]
  requires: serverName.length > 0
{
  try {
    Ok(handshake(s, tls, serverName, port))
  } catch Bug as b {
    closeQuietly(s.socket)
    Err(b.message)
  }
}

func baosWriteByte(acc: in JBAOS, b: in Int): Unit {
  acc.write(b)
}

func baosWriteBytes(acc: in JBAOS, bytes: in slice[Byte]): Unit {
  acc.write(bytes)
}

// The first byte blocks (`read()`), the rest is whatever has already arrived.
// The arrays come from the JDK (`readNBytes`, `toByteArray`), never from a
// caller-owned buffer filled through `read(byte[], int, int)`: the auto-FFI
// array marshalling does not propagate such writes back.
func readChunk(input: in JInputStream, maxBytes: in Int): slice[Byte] {
  val first = input.read()
  if first < 0 {
    val empty: slice[Byte] = []
    return empty
  }
  val acc: JBAOS = JBAOS.new()
  baosWriteByte(acc, first)
  val avail = input.available()
  if avail > 0 and maxBytes > 1 {
    val want = if avail < maxBytes - 1 { avail } else { maxBytes - 1 }
    baosWriteBytes(acc, input.readNBytes(want))
  }
  acc.toByteArray()
}

/// Read up to `maxBytes` bytes, blocking until at least one arrives. An empty
/// slice means the peer closed the connection.
pub func sockRead(s: in Sock, maxBytes: in Int): Result[slice[Byte], String]
  requires: maxBytes > 0
{
  try {
    Ok(readChunk(s.input, maxBytes))
  } catch Bug as b {
    Err(b.message)
  }
}

func writeAll(output: in JOutputStream, bytes: in slice[Byte]): Unit {
  output.write(bytes)
  output.flush()
}

/// Write all of `bytes` and flush.
pub func sockWrite(s: in Sock, bytes: in slice[Byte]): Result[Unit, String] {
  try {
    writeAll(s.output, bytes)
    Ok(())
  } catch Bug as b {
    Err(b.message)
  }
}

/// Close the connection. Closing twice is harmless.
pub func sockClose(s: in Sock): Unit {
  closeQuietly(s.socket)
}

/// Listen on `host:port` (`port = 0` picks a free port).
pub func serverListen(host: in String, port: in Int): Result[ServerSock, String] {
  try {
    val address: JInetAddress = JInetAddress.getByName(host)
    val server: JServerSocket = JServerSocket.new(port, 50, address)
    Ok(ServerSock(server = server))
  } catch Bug as b {
    Err(b.message)
  }
}

func listenTls(
  host: in String,
  port: in Int,
  certPem: in String,
  keyPem: in String,
  clientCaPem: in String,
  requireClientCert: in Bool,
): Result[ServerSock, String] {
  var keyManagers: JObject = nullOfClass("java.lang.Object")
  match TlsHost.hostIdentityFromPemBytes(encodeUtf8(certPem), encodeUtf8(keyPem)) {
    case Ok(handle) -> {
      keyManagers = keyManagersFor(handle)
    }
    case Err(e) -> return Err("server certificate and key: " + loadMessage(e))
  }
  var trustManagers: JObject = nullOfClass("java.lang.Object")
  if clientCaPem.length > 0 {
    match TlsHost.hostCertFromPemBytes(encodeUtf8(clientCaPem)) {
      case Ok(handle) -> {
        trustManagers = trustManagersFor(handle.cert)
      }
      case Err(e) -> return Err("client CA certificate: " + loadMessage(e))
    }
  }
  val factory: JSSLServerSocketFactory = newContext(keyManagers, trustManagers).getServerSocketFactory()
  val address: JInetAddress = JInetAddress.getByName(host)
  val server: JSSLServerSocket = factory.createServerSocket(port, 50, address)
  if requireClientCert {
    server.setNeedClientAuth(true)
  } else if clientCaPem.length > 0 {
    server.setWantClientAuth(true)
  }
  val asServer: JServerSocket = server
  Ok(ServerSock(server = asServer))
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
  try {
    listenTls(host, port, certPem, keyPem, clientCaPem, requireClientCert)
  } catch Bug as b {
    Err(b.message)
  }
}

/// The port `server` is bound to.
pub func serverPort(server: in ServerSock): Int {
  server.server.getLocalPort()
}

/// Block until a client connects. On a TLS server the handshake runs on the
/// connection's first read or write, where a client whose handshake fails is
/// reported.
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

/// Run `work` on another thread.
pub func startBackground(work: in () -> Unit): Background {
  val t: JThread = JThread.new(BackgroundJob(work = work))
  startThread(t)
  Background(thread = t)
}

/// Block until `b` has finished.
pub func waitBackground(b: in Background): Unit {
  try {
    b.thread.join()
  } catch Bug as _ {
    ()
  }
}
