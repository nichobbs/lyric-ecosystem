//! Net.Tcp - a TCP client connection, plain or TLS, for protocol clients
//! written in Lyric.
//!
//! What it adds to a socket is what a client of a request/reply or streaming
//! protocol needs and `Std.TcpHost` does not give: a read with a time limit
//! that tells "nothing arrived yet" (`TimedOut`) from "the peer closed" (`Eof`)
//! and from a failure (`Err`), a write that gives up when the peer accepts no
//! data, a connect and a TLS handshake with a time limit, and a TLS client
//! upgrade configured by `Net.Tls.TlsOptions`. Every operation returns a
//! `Result` with the failure text; nothing throws.
//!
//! ```
//! import Net.Tcp as Tcp
//! import Net.Tls
//!
//! val tls = plainTcp().withTls()
//! match Tcp.sockConnect("broker.internal", 5671, 10000) {
//!   case Err(e) -> println(e)
//!   case Ok(plain) -> match Tcp.sockStartTls(plain, tls, "broker.internal", skipsVerification(tls), 10000) {
//!     case Err(e) -> println(e)
//!     case Ok(sock) -> {
//!       val _ = Tcp.sockWriteWithin(sock, encodeUtf8("PING\r\n"), 5000)
//!       match Tcp.sockRead(sock, 4096, 5000) {
//!         case Ok(Data(bytes)) -> println(bytes.length.toString())
//!         case Ok(TimedOut) -> println("nothing within five seconds")
//!         case Ok(Eof) -> println("closed")
//!         case Err(e) -> println(e)
//!       }
//!       Tcp.sockClose(sock)
//!     }
//!   }
//! }
//! ```
//!
//! Threads: a `Sock` is read by one thread at a time and written by one thread
//! at a time (a reader and a writer may be different threads). Use a `Lock` to
//! serialize a whole exchange. On dotnet a connection that has been read from
//! owns one thread until it is closed, and nothing here waits for the shared
//! thread pool (see `Net.Tcp.Kernel`).
//!
//! The sockets, threads, locks and the clock are reached through the
//! per-target kernel `Net.Tcp.Kernel`; listening sockets and background
//! threads, which only the in-process servers of test suites need, are in
//! `Net.Tcp.Testing`.
@runtime_checked
@io
package Net.Tcp

import Std.Core
import Net.Tcp.Kernel as Kernel
import Net.Tls as NetTls

/// One open connection: plain TCP, or TLS over it.
pub record Sock {
  inner: Kernel.Sock
}

/// What one read produced.
pub union ReadOutcome {
  /// At least one byte, at most as many as were asked for.
  case Data(bytes: slice[Byte])
  /// Nothing arrived within the wait. The connection is still usable.
  case TimedOut
  /// The peer closed the connection.
  case Eof
}

/// A reentrant mutual-exclusion lock. Each lock is its own instance: two
/// locks are never equal.
pub record Lock {
  inner: Kernel.Lock
}

/// Open a TCP connection to `host:port`, waiting at most `connectTimeoutMs`
/// milliseconds for it (`0` = the operating system's limit).
pub func sockConnect(host: in String, port: in Int, connectTimeoutMs: in Int): Result[Sock, String]
  requires: connectTimeoutMs >= 0
{
  match Kernel.sockConnect(host, port, connectTimeoutMs) {
    case Ok(s) -> Ok(Sock(inner = s))
    case Err(e) -> Err(e)
  }
}

/// Run the client side of a TLS handshake over the freshly connected `s` and
/// return the secured connection. `tls` says what to trust and present
/// (validate it first with `Net.Tls.validateTlsOptions`); the name sent as
/// SNI and checked against the server certificate is `tls.serverName`, or
/// `host` when that is empty. `skipVerify` switches certificate and name
/// verification off: pass `Net.Tls.skipsVerification(tls)`, which applies the
/// two-key policy, never a constant `true`. The handshake waits at most
/// `timeoutMs` milliseconds (`0` = no limit). On `Err` the connection is
/// closed.
pub func sockStartTls(
  s: in Sock, tls: in NetTls.TlsOptions, host: in String, skipVerify: in Bool, timeoutMs: in Int
): Result[Sock, String]
  requires: timeoutMs >= 0
{
  val exclusive = tls.exclusiveCaCertificatePem.length > 0
  val settings = Kernel.ClientTls(
    serverName = if tls.serverName.length > 0 { tls.serverName } else { host },
    caPem = if exclusive { tls.exclusiveCaCertificatePem } else { tls.caCertificatePem },
    exclusiveCa = exclusive,
    certPem = tls.clientCertificatePem,
    keyPem = tls.clientKeyPem,
    tls13Only = NetTls.requiresTls13(tls),
    skipVerify = skipVerify
  )
  match Kernel.sockStartTls(s.inner, settings, timeoutMs) {
    case Ok(secured) -> Ok(Sock(inner = secured))
    case Err(e) -> Err(e)
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
  match Kernel.sockRead(s.inner, maxBytes, timeoutMs) {
    case Ok(Kernel.Data(bytes)) -> Ok(Data(bytes = bytes))
    case Ok(Kernel.TimedOut) -> Ok(TimedOut)
    case Ok(Kernel.Eof) -> Ok(Eof)
    case Err(e) -> Err(e)
  }
}

/// Write all of `bytes` and flush. The write is given up, with an `Err`
/// whose text starts with "the write timed out", when the peer accepts no
/// data for `timeoutMs` milliseconds (`0` = no limit); how much was written
/// by then is unknown, so the connection is of no further use.
pub func sockWriteWithin(s: in Sock, bytes: in slice[Byte], timeoutMs: in Int): Result[Unit, String]
  requires: timeoutMs >= 0
{
  Kernel.sockWriteWithin(s.inner, bytes, timeoutMs)
}

/// Write all of `bytes` and flush, however long the peer takes to accept them.
pub func sockWrite(s: in Sock, bytes: in slice[Byte]): Result[Unit, String] {
  Kernel.sockWrite(s.inner, bytes)
}

/// Close the connection. Closing twice is harmless.
pub func sockClose(s: in Sock): Unit {
  Kernel.sockClose(s.inner)
}

/// A new lock, free.
pub func newLock(): Lock {
  Lock(inner = Kernel.newLock())
}

/// Take the lock, waiting for it as long as it takes.
pub func lockEnter(l: in Lock): Unit {
  Kernel.lockEnter(l.inner)
}

/// Take the lock if it becomes free within `timeoutMs` milliseconds (`0` =
/// only if it is free now). False when it did not.
pub func lockTryEnter(l: in Lock, timeoutMs: in Int): Bool
  requires: timeoutMs >= 0
{
  Kernel.lockTryEnter(l.inner, timeoutMs)
}

/// Release the lock once.
pub func lockExit(l: in Lock): Unit {
  Kernel.lockExit(l.inner)
}

/// Milliseconds on a clock that never goes backwards.
pub func monotonicMillis(): Long {
  Kernel.monotonicMillis()
}

/// Block the calling thread for `ms` milliseconds.
pub func sleepMs(ms: in Int): Unit
  requires: ms >= 0
{
  Kernel.sleepMs(ms)
}
