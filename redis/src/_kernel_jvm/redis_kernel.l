//! Redis.Kernel (jvm) - the socket boundary of the Redis client on
//! `--target jvm`.
//!
//! `java.net.Socket` / `java.net.ServerSocket` and their streams through the
//! JVM auto-FFI (`extern type` + `T.method(args)`); no Java shim. Every
//! operation returns a `Result` with the failure text; nothing throws.
//!
//! The dotnet twin (`src/_kernel_dotnet/redis_kernel.l`) declares the same
//! package with the same public surface; a file-level `@cfg(target = ...)`
//! leaves exactly one of them in a build.
@axiom("the host socket and thread APIs (Std.TcpHost and System.Threading.Tasks.Task on dotnet; java.net.Socket, java.net.ServerSocket, java.io streams and java.lang.Thread on the JVM) conform to their documented contracts")
@cfg(target = "jvm")
@io
package Redis.Kernel

import Std.Core

extern type JSocket = "java.net.Socket"

extern type JServerSocket = "java.net.ServerSocket"

extern type JInetSocketAddress = "java.net.InetSocketAddress"

extern type JInetAddress = "java.net.InetAddress"

extern type JInputStream = "java.io.InputStream"

extern type JOutputStream = "java.io.OutputStream"

extern type JBAOS = "java.io.ByteArrayOutputStream"

extern type JRunnable = "java.lang.Runnable"

extern type JThread = "java.lang.Thread"

/// One open TCP connection.
pub record Sock {
  socket: JSocket
  input: JInputStream
  output: JOutputStream
}

/// A bound, listening TCP socket (used by the in-process fake server of the
/// test suite; the client itself never listens).
pub record ServerSock {
  server: JServerSocket
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
