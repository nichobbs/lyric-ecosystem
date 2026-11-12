//! Redis.Kernel (dotnet) - the socket boundary of the Redis client on
//! `--target dotnet`.
//!
//! The transport is `Std.TcpHost` (`System.Net.Sockets.TcpClient` /
//! `TcpListener`), the standard library's audited TCP kernel, so the only
//! externs declared here are the two `System.Threading.Tasks.Task` members
//! behind `startBackground` / `waitBackground`. Every operation returns a
//! `Result` with the failure text; nothing throws.
//!
//! The JVM twin (`src/_kernel_jvm/redis_kernel.l`) declares the same package
//! with the same public surface; a file-level `@cfg(target = ...)` leaves
//! exactly one of them in a build.
//!
//! `connectTimeoutMs` is not applied on this target: `Std.TcpHost.hostConnect`
//! dials with the operating system's connect timeout (see the README's known
//! gaps). The read/write timeout is applied on both targets.
@axiom("the host socket and thread APIs (Std.TcpHost and System.Threading.Tasks.Task on dotnet; java.net.Socket, java.net.ServerSocket, java.io streams and java.lang.Thread on the JVM) conform to their documented contracts")
@cfg(target = "dotnet")
@io
package Redis.Kernel

import Std.Core
import Std.TcpHost as Tcp

/// One open TCP connection.
pub record Sock {
  conn: Tcp.Conn
}

/// A bound, listening TCP socket (used by the in-process fake server of the
/// test suite; the client itself never listens).
pub record ServerSock {
  listener: Tcp.Listener
}

/// A unit of work running on its own thread.
pub extern type Background = "System.Threading.Tasks.Task"

@externStatic
@externTarget("System.Threading.Tasks.Task.Run")
func taskRun(work: in () -> Unit): Background = ()

@externInstance
@externTarget("System.Threading.Tasks.Task.Wait")
func taskWait(t: in Background): Unit = ()

/// Open a TCP connection to `host:port` and bound every later read and write
/// to `ioTimeoutMs` milliseconds of inactivity (`0` = no limit).
pub func sockConnect(host: in String, port: in Int, connectTimeoutMs: in Int, ioTimeoutMs: in Int): Result[Sock, String]
  requires: connectTimeoutMs >= 0
  requires: ioTimeoutMs >= 0
{
  match Tcp.hostConnect(host, port) {
    case Err(e) -> Err(e.message())
    case Ok(conn) -> {
      Tcp.hostSetIoTimeout(conn, ioTimeoutMs)
      Ok(Sock(conn = conn))
    }
  }
}

/// Read up to `maxBytes` bytes, blocking until at least one arrives. An empty
/// slice means the peer closed the connection.
pub func sockRead(s: in Sock, maxBytes: in Int): Result[slice[Byte], String]
  requires: maxBytes > 0
{
  match Tcp.hostRead(s.conn, maxBytes) {
    case Ok(bytes) -> Ok(bytes)
    case Err(e) -> Err(e.message())
  }
}

/// Write all of `bytes` and flush.
pub func sockWrite(s: in Sock, bytes: in slice[Byte]): Result[Unit, String] {
  match Tcp.hostWrite(s.conn, bytes) {
    case Ok(_) -> Ok(())
    case Err(e) -> Err(e.message())
  }
}

/// Close the connection. Closing twice is harmless.
pub func sockClose(s: in Sock): Unit {
  Tcp.hostClose(s.conn)
}

/// Listen on `host:port` (`port = 0` picks a free port).
pub func serverListen(host: in String, port: in Int): Result[ServerSock, String] {
  match Tcp.hostListen(host, port) {
    case Ok(l) -> Ok(ServerSock(listener = l))
    case Err(e) -> Err(e.message())
  }
}

/// The port `server` is bound to.
pub func serverPort(server: in ServerSock): Int {
  Tcp.hostLocalPort(server.listener)
}

/// Block until a client connects.
pub func serverAccept(server: in ServerSock): Result[Sock, String] {
  match Tcp.hostAccept(server.listener) {
    case Ok(conn) -> Ok(Sock(conn = conn))
    case Err(e) -> Err(e.message())
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
