//! Net.Tcp.Testing - test support: a listening socket, the server side of a
//! TLS handshake and background threads, for the in-process servers that the
//! hermetic test suites of protocol clients run against.
//!
//! Nothing here is meant for an application: a server for production use is
//! the standard library's (`Std.HttpServer`, `Std.TcpHost`). The connections
//! `serverAccept` returns are `Net.Tcp.Sock`s, read and written with
//! `Net.Tcp`.
@runtime_checked
@io
package Net.Tcp.Testing

import Std.Core
import Net.Tcp.Kernel as Kernel
import Net.Tcp as Tcp

/// A bound, listening TCP socket.
pub record ServerSock {
  inner: Kernel.ServerSock
}

/// How a test server secures an accepted connection (`sockAcceptTls`). PEM
/// text.
pub record ServerTls {
  certPem: String
  keyPem: String
  /// When not empty, a client certificate issued by this CA is required.
  clientCaPem: String
}

/// A unit of work running on its own thread.
pub record Background {
  inner: Kernel.Background
}

/// Listen on the IP literal `host` and `port` (`port = 0` picks a free port).
pub func serverListen(host: in String, port: in Int): Result[ServerSock, String] {
  match Kernel.serverListen(host, port) {
    case Ok(s) -> Ok(ServerSock(inner = s))
    case Err(e) -> Err(e)
  }
}

/// The port `server` is bound to, `0` when it cannot be read.
pub func serverPort(server: in ServerSock): Int {
  Kernel.serverPort(server.inner)
}

/// Block until a client connects.
pub func serverAccept(server: in ServerSock): Result[Tcp.Sock, String] {
  match Kernel.serverAccept(server.inner) {
    case Ok(s) -> Ok(Tcp.Sock(inner = s))
    case Err(e) -> Err(e)
  }
}

/// Stop listening.
pub func serverClose(server: in ServerSock): Unit {
  Kernel.serverClose(server.inner)
}

/// Run the server side of a TLS handshake over an accepted connection,
/// waiting at most `timeoutMs` milliseconds (`0` = no limit). On `Err` the
/// connection is closed.
pub func sockAcceptTls(s: in Tcp.Sock, tls: in ServerTls, timeoutMs: in Int): Result[Tcp.Sock, String]
  requires: timeoutMs >= 0
{
  val settings = Kernel.ServerTls(certPem = tls.certPem, keyPem = tls.keyPem, clientCaPem = tls.clientCaPem)
  match Kernel.sockAcceptTls(s.inner, settings, timeoutMs) {
    case Ok(secured) -> Ok(Tcp.Sock(inner = secured))
    case Err(e) -> Err(e)
  }
}

/// Run `work` on a thread of its own.
pub func startBackground(work: in () -> Unit): Background {
  Background(inner = Kernel.startBackground(work))
}

/// Queue `work` to the runtime's shared thread pool (dotnet), which `Net.Tcp`
/// stays away from; on the JVM, which has no such pool, a thread of its own.
/// For tests that keep the pool busy.
pub func startPoolWork(work: in () -> Unit): Background {
  Background(inner = Kernel.startPoolWork(work))
}

/// Block until `b` has finished.
pub func waitBackground(b: in Background): Unit {
  Kernel.waitBackground(b.inner)
}
