//! Net.Tcp.Kernel (native) - the socket boundary of `Net.Tcp` on
//! `--target native`. Use `Net.Tcp`, not this package.
//!
//! Sockets and TLS are the standard library's native transport
//! (`Std.TcpHost` over lyric-rt's POSIX sockets and OpenSSL), threads are
//! `Std.Task`'s, the clock is `Std.Time`'s, and shared state is in `protected`
//! types. The one C symbol bound here is `pthread_self`, for the owner of the
//! reentrant lock. Every operation returns a `Result` with the failure text;
//! native has no exceptions to throw.
//!
//! Reads. A timed read first waits for the socket to become readable
//! (`Std.IoWait.waitFd`, a `poll`): nothing readable within the wait is
//! `TimedOut`, and no thread is held by a connection. For plain TCP the read
//! that follows cannot block. For TLS a readable socket does not mean
//! plaintext: the bytes may be part of a record or a handshake message (a
//! TLS 1.3 session ticket). So a TLS read runs under a short socket timeout
//! (`tlsReadSliceMs`), and when the timeout ends it the kernel goes back to
//! waiting on the socket with what is left of the caller's wait; OpenSSL
//! keeps what it has read of the record. Every TLS read asks for a whole
//! record's worth (`readChunkBytes`), so OpenSSL never holds plaintext the
//! socket cannot announce; what the caller did not ask for is kept in the
//! connection (`carry`) and returned first by the next read.
//!
//! One thread at a time uses a TLS connection: OpenSSL does not allow a read
//! and a write of one connection to run at once, so both take the
//! connection's lock (`io`). A reader holds it only while bytes are on the
//! wire, for at most `tlsReadSliceMs`; a writer holds it for its write. Plain
//! TCP needs no lock.
//!
//! Writes. `Std.TcpHost.hostSetIoTimeout` bounds each write by the time the
//! peer accepts no data (`SO_SNDTIMEO`); the reads above do not depend on the
//! receive timeout it sets with it.
//!
//! `Std.TcpHost` reports a read or write ended by its timeout as
//! `TransportFailed` with a fixed text, like any other failure (the reason
//! the dotnet twin does not use its `Std.TcpHost`). `endedByTimeout` compares
//! against those texts; the read and write timeout tests of `tests/tcp_tests.l`
//! fail when they change.
//!
//! The connect takes its limit from `Std.TcpHost.hostConnect`, which reports an
//! expired one as `TransportFailed` with a fixed text that
//! `connectEndedByTimeout` compares against (`sockConnect` turns it into the
//! text this package documents). The TLS client handshake runs on a thread of
//! its own, which the caller waits for with a limit (`Std.Task.runActionWithin`):
//! a handshake ended by the socket timeout carries no text that says so. A
//! handshake that is given up closes its socket when it ends.
//!
//! Closing. `sockClose` from one thread while another is inside a read or a
//! write shuts the socket down, which ends that call, and the last thread out
//! releases the connection; with no call in progress it is released at once
//! (TLS sends its close notification).
//!
//! POSIX only (Linux, macOS): `pthread_self`.
//!
//! The dotnet and JVM twins (`src/_kernel_dotnet/tcp_kernel.l`,
//! `src/_kernel_jvm/tcp_kernel.l`) declare the same package with the same
//! public surface; a file-level `@cfg(target = ...)` leaves exactly one of
//! them in a build.
@axiom("the host socket, thread, lock and clock primitives conform to their documented platform contracts")
@cfg(target = "native")
@io
package Net.Tcp.Kernel

import Std.Core
import Std.Task as Task
import Std.TcpHost as TcpHost
import Std.IoWait as IoWait
import Std.Time as Time
import Std.Tls as Tls

// The calling thread, never 0.
extern func pthreadSelf(): Long = "pthread_self"

// The most plaintext one TLS record holds.
val readChunkBytes: Int = 16384

// How long a TLS read may hold the connection while a record completes.
val tlsReadSliceMs: Int = 20

// The longest single wait on an event; waits without a limit repeat it.
val longWaitMs: Int = 3600000

val closedText: String = "the connection is closed"

// ─── Events and locks ────────────────────────────────────────────────────────
// A one-shot event is a cancellation source: `Task.cancelSource` sets it and
// `Task.delayWithCancel` waits for it with a limit.
func eventSet(ev: in Task.CancellationTokenSource): Unit {
  Task.cancelSource(ev)
}

// Wait for `ev` at most `ms` milliseconds; true when it is set.
func eventWait(ev: in Task.CancellationTokenSource, ms: in Int): Bool {
  val token = Task.sourceToken(ev)
  if ms > 0 and not Task.isCancelled(token) {
    val _ = Task.delayWithCancel(ms, token)
  }
  Task.isCancelled(token)
}

func eventWaitForever(ev: in Task.CancellationTokenSource): Unit {
  while not eventWait(ev, longWaitMs) {
    ()
  }
}

// What `LockState.tryTake` answers: taken, or the event the holder sets when
// it lets go.
pub union TakeOutcome {
  case LockTaken
  case LockHeld(freed: Task.CancellationTokenSource)
}

pub protected type LockState {
  var owner: Long = 0i64
  var depth: Int = 0
  var freed: Task.CancellationTokenSource = Task.makeCancelSource()
  entry tryTake(me: in Long): TakeOutcome {
    if depth == 0 {
      owner = me
      depth = 1
      return LockTaken
    }
    if owner == me {
      depth = depth + 1
      return LockTaken
    }
    LockHeld(freed = freed)
  }
  // The event to set when the lock became free, if it did.
  entry release(me: in Long): Option[Task.CancellationTokenSource] {
    if depth == 0 or owner != me {
      return None
    }
    depth = depth - 1
    if depth > 0 {
      return None
    }
    owner = 0i64
    val ev = freed
    freed = Task.makeCancelSource()
    Some(ev)
  }
}

/// A reentrant mutual-exclusion lock. A `ref record`, so each lock is its own
/// instance.
pub ref record Lock {
  state: LockState
}

pub func newLock(): Lock {
  Lock(state = LockState())
}

pub func lockEnter(l: in Lock): Unit {
  val me = pthreadSelf()
  while true {
    match l.state.tryTake(me) {
      case LockTaken -> {
        return
      }
      case LockHeld(freed) -> eventWaitForever(freed)
    }
  }
}

/// Take the lock if it becomes free within `timeoutMs` milliseconds (`0` =
/// only if it is free now).
pub func lockTryEnter(l: in Lock, timeoutMs: in Int): Bool
  requires: timeoutMs >= 0
{
  val me = pthreadSelf()
  val deadline = monotonicMillis() + timeoutMs.toLong()
  while true {
    match l.state.tryTake(me) {
      case LockTaken -> {
        return true
      }
      case LockHeld(freed) -> {
        val left = millisUntil(deadline)
        if left == 0 {
          return false
        }
        val _ = eventWait(freed, left)
      }
    }
  }
  false
}

pub func lockExit(l: in Lock): Unit {
  match l.state.release(pthreadSelf()) {
    case Some(ev) -> eventSet(ev)
    case None -> ()
  }
}

// ─── The clock ───────────────────────────────────────────────────────────────
/// Milliseconds on a clock that never goes backwards.
pub func monotonicMillis(): Long {
  Time.monotonicNanos() / 1000000i64
}

/// Block the calling thread for `ms` milliseconds.
pub func sleepMs(ms: in Int): Unit
  requires: ms >= 0
{
  if ms > 0 {
    Time.sleepMillis(ms)
  }
}

// The milliseconds from now to `deadline`, 0 when it has passed.
func millisUntil(deadline: in Long): Int {
  val left = deadline - monotonicMillis()
  if left <= 0i64 {
    0
  } else if left > longWaitMs.toLong() {
    longWaitMs
  } else {
    left.toInt()
  }
}

// ─── Connections ─────────────────────────────────────────────────────────────
// What the connection holds for the next read.
pub union Buffered {
  case CarryBytes(bytes: slice[Byte])
  case CarryEnd
  case CarryFailed(message: String)
  case CarryNone
}

// How `sockClose` is to go on.
pub union CloseStep {
  case CloseDone
  // No call is in progress: release the connection.
  case CloseRelease
  // Calls are in progress: shut the socket down; the last one out releases.
  case CloseShutDown
}

// The state the threads using one connection share.
pub protected type ConnState {
  // `sockClose` was called, or the socket went to another `Sock`.
  var closed: Bool = false
  // The socket was released, or is no longer this `Sock`'s to release.
  var released: Bool = false
  // The calls in progress on the connection.
  var users: Int = 0
  // Bytes read beyond what the reader asked for.
  var carry: slice[Byte] = []
  // 0 while the stream is open, 1 at its end, 2 when a read failed.
  var ended: Int = 0
  var failure: String = ""
  entry enter(): Bool {
    if closed {
      return false
    }
    users = users + 1
    true
  }
  // True when the caller is the last one out of a closed connection and has
  // to release it.
  entry leave(): Bool {
    users = users - 1
    if closed and users == 0 and not released {
      released = true
      return true
    }
    false
  }
  entry beginClose(): CloseStep {
    if closed {
      return CloseDone
    }
    closed = true
    if users == 0 {
      released = true
      return CloseRelease
    }
    users = users + 1
    CloseShutDown
  }
  // The socket is someone else's now: the TLS connection made over it, or
  // `Std.TcpHost`, which closes it when a client handshake fails. False when
  // the connection was closed before.
  entry disown(): Bool {
    if closed {
      return false
    }
    closed = true
    released = true
    true
  }
  entry buffered(maxBytes: in Int): Buffered {
    if carry.length > 0 {
      if carry.length <= maxBytes {
        val all = carry
        carry = []
        return CarryBytes(bytes = all)
      }
      val head = carry.slice(0, maxBytes)
      carry = carry.slice(maxBytes, carry.length)
      return CarryBytes(bytes = head)
    }
    if ended == 1 {
      return CarryEnd
    }
    if ended == 2 {
      return CarryFailed(message = failure)
    }
    CarryNone
  }
  entry keep(rest: in slice[Byte]): Unit {
    carry = rest
  }
  entry endRead(code: in Int, message: in String): Unit {
    if ended == 0 {
      ended = code
      failure = message
    }
  }
  func isClosed(): Bool {
    closed
  }
}

/// One open connection: plain TCP, or TLS over it.
pub record Sock {
  conn: TcpHost.Conn
  secured: Bool
  state: ConnState
  // Serializes the reads and writes of a TLS connection (module header).
  io: Lock
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

func sockOver(conn: in TcpHost.Conn, secured: in Bool): Sock {
  Sock(conn = conn, secured = secured, state = ConnState(), io = newLock())
}

// Whether `Std.TcpHost` ended a read or a write because the socket timeout
// set with `hostSetIoTimeout` passed. It has no error case for that (module
// header), so this is the text lyric-rt gives the failure.
func endedByTimeout(e: in TcpHost.TcpError): Bool {
  match e {
    case TcpHost.TransportFailed(m) -> m == "read: timed out" or m == "TLS read: timed out" or m == "write: timed out" or
      m == "TLS write: timed out"
    case _ -> false
  }
}

// Leave the connection, releasing it when `sockClose` ran meanwhile.
func leave(s: in Sock): Unit {
  if s.state.leave() {
    TcpHost.hostClose(s.conn)
  }
}

// ─── Connect ─────────────────────────────────────────────────────────────────
// What a connect or a handshake on its own thread hands to the caller that
// waits for it with a limit.
protected type Pending {
  var finished: Bool = false
  var abandoned: Bool = false
  var conn: Option[TcpHost.Conn] = None
  var failure: String = ""
  // False when the caller gave up: the connection is the worker's to close.
  entry succeed(c: in TcpHost.Conn): Bool {
    if abandoned {
      return false
    }
    conn = Some(c)
    finished = true
    true
  }
  entry fail(message: in String): Unit {
    failure = message
    finished = true
  }
  // True when the worker has not finished: the caller gives up.
  entry abandon(): Bool {
    if finished {
      return false
    }
    abandoned = true
    true
  }
  func connection(): Option[TcpHost.Conn] {
    conn
  }
  func failureText(): String {
    failure
  }
}

// Whether `Std.TcpHost` ended a connect because the limit given to `hostConnect`
// passed. It has no error case for that (module header), so this is the text it
// gives the failure.
func connectEndedByTimeout(e: in TcpHost.TcpError, limitMs: in Int): Bool {
  match e {
    case TcpHost.TransportFailed(m) -> m == "connect timed out after " + limitMs.toString() + " ms"
    case _ -> false
  }
}

/// Open a TCP connection to `host:port`, waiting at most `connectTimeoutMs`
/// milliseconds for it (`0` = the operating system's limit).
pub func sockConnect(host: in String, port: in Int, connectTimeoutMs: in Int): Result[Sock, String]
  requires: connectTimeoutMs >= 0
{
  match TcpHost.hostConnect(host, port, connectTimeoutMs) {
    case Ok(c) -> Ok(sockOver(c, false))
    case Err(e) -> {
      if connectTimeoutMs > 0 and connectEndedByTimeout(e, connectTimeoutMs) {
        Err("connect to " + host + ":" + port.toString() + " timed out after " + connectTimeoutMs.toString() + " ms")
      } else {
        Err(TcpHost.TcpError.message(e))
      }
    }
  }
}

// ─── Reads ───────────────────────────────────────────────────────────────────
// What one read of the wire gave.
union WireRead {
  case WireGot(outcome: ReadOutcome)
  // The read ended on its socket timeout, or another thread holds the
  // connection: wait again.
  case WireNotYet
  case WireBroken(message: String)
}

func readOnce(s: in Sock, maxBytes: in Int, left: in Int): WireRead {
  if s.secured {
    if not lockTryEnter(s.io, left) {
      return WireNotYet
    }
    TcpHost.hostSetIoTimeout(s.conn, tlsReadSliceMs)
  }
  val read = TcpHost.hostRead(s.conn, readChunkBytes)
  if s.secured {
    lockExit(s.io)
  }
  match read {
    case Ok(bytes) -> {
      if bytes.length == 0 {
        s.state.endRead(1, "")
        return WireGot(outcome = Eof)
      }
      if bytes.length <= maxBytes {
        return WireGot(outcome = Data(bytes = bytes))
      }
      s.state.keep(bytes.slice(maxBytes, bytes.length))
      WireGot(outcome = Data(bytes = bytes.slice(0, maxBytes)))
    }
    case Err(e) -> {
      if endedByTimeout(e) {
        return WireNotYet
      }
      val message = TcpHost.TcpError.message(e)
      s.state.endRead(2, message)
      WireBroken(message = message)
    }
  }
}

func readWire(s: in Sock, maxBytes: in Int, timeoutMs: in Int): Result[ReadOutcome, String] {
  val deadline = monotonicMillis() + timeoutMs.toLong()
  while true {
    val left = millisUntil(deadline)
    if IoWait.waitFd(TcpHost.hostFd(s.conn), IoWait.ioRead, left) == 0 {
      return Ok(TimedOut)
    }
    match readOnce(s, maxBytes, left) {
      case WireGot(outcome) -> {
        return Ok(outcome)
      }
      case WireBroken(message) -> {
        return Err(message)
      }
      case WireNotYet -> {
        if left == 0 {
          return Ok(TimedOut)
        }
      }
    }
  }
  Ok(TimedOut)
}

/// Read up to `maxBytes` bytes, waiting at most `timeoutMs` milliseconds for
/// the first one (`0` = only what has already arrived).
pub func sockRead(s: in Sock, maxBytes: in Int, timeoutMs: in Int): Result[ReadOutcome, String]
  requires: maxBytes > 0
  requires: timeoutMs >= 0
{
  match s.state.buffered(maxBytes) {
    case CarryBytes(bytes) -> {
      return Ok(Data(bytes = bytes))
    }
    case CarryEnd -> {
      return Ok(Eof)
    }
    case CarryFailed(message) -> {
      return Err(message)
    }
    case CarryNone -> ()
  }
  if not s.state.enter() {
    return Err(closedText)
  }
  val outcome = readWire(s, maxBytes, timeoutMs)
  val closedMeanwhile = s.state.isClosed()
  leave(s)
  if closedMeanwhile {
    return Err(closedText)
  }
  outcome
}

// ─── Writes ──────────────────────────────────────────────────────────────────
/// Write all of `bytes`. The write is given up, with an `Err`, when the peer
/// accepts no data for `timeoutMs` milliseconds (`0` = no limit).
pub func sockWriteWithin(s: in Sock, bytes: in slice[Byte], timeoutMs: in Int): Result[Unit, String]
  requires: timeoutMs >= 0
{
  if not s.state.enter() {
    return Err(closedText)
  }
  if s.secured {
    lockEnter(s.io)
  }
  TcpHost.hostSetIoTimeout(s.conn, timeoutMs)
  val written = TcpHost.hostWrite(s.conn, bytes)
  if s.secured {
    lockExit(s.io)
  }
  leave(s)
  match written {
    case Ok(_) -> Ok(())
    case Err(e) -> {
      if timeoutMs > 0 and endedByTimeout(e) {
        Err("the write timed out: the peer accepted no data for " + timeoutMs.toString() + " ms")
      } else {
        Err(TcpHost.TcpError.message(e))
      }
    }
  }
}

/// Write all of `bytes`, however long the peer takes to accept them.
pub func sockWrite(s: in Sock, bytes: in slice[Byte]): Result[Unit, String] {
  sockWriteWithin(s, bytes, 0)
}

/// Close the connection. Closing twice is harmless.
pub func sockClose(s: in Sock): Unit {
  match s.state.beginClose() {
    case CloseDone -> ()
    case CloseRelease -> TcpHost.hostClose(s.conn)
    case CloseShutDown -> {
      TcpHost.hostShutdown(s.conn)
      leave(s)
    }
  }
}

// ─── TLS ─────────────────────────────────────────────────────────────────────
func clientTrust(tls: in ClientTls): TcpHost.TlsClientTrust {
  if tls.skipVerify {
    TcpHost.TlsTrustInsecure
  } else if tls.caPem.length == 0 {
    TcpHost.TlsTrustSystemDefault
  } else if tls.exclusiveCa {
    TcpHost.TlsTrustExclusive(caPem = tls.caPem)
  } else {
    TcpHost.TlsTrustAdditive(caPem = tls.caPem)
  }
}

func clientIdentity(tls: in ClientTls): Result[Option[Tls.Identity], String] {
  if tls.certPem.length == 0 and tls.keyPem.length == 0 {
    return Ok(None)
  }
  match Tls.Identity.fromPem(tls.certPem, tls.keyPem) {
    case Ok(identity) -> Ok(Some(identity))
    case Err(e) -> Err("the client certificate and key: " + Tls.TlsError.message(e))
  }
}

func minVersion(tls13Only: in Bool): Tls.TlsVersion {
  if tls13Only {
    Tls.TlsVersion.Tls13
  } else {
    Tls.TlsVersion.Tls12
  }
}

// `Std.TcpHost` closes the socket itself when the handshake fails.
func handshakeInto(
  pending: in Pending,
  plain: in TcpHost.Conn,
  tls: in ClientTls,
  identity: in Option[Tls.Identity],
  timeoutMs: in Int,
): Unit {
  // Ends a handshake the caller gave up on, with a peer that stays silent.
  TcpHost.hostSetIoTimeout(plain, timeoutMs)
  match TcpHost.hostUpgradeClientTls(plain, tls.serverName, clientTrust(tls), identity, minVersion(tls.tls13Only), "") {
    case Ok(c) -> {
      TcpHost.hostSetIoTimeout(c, 0)
      if not pending.succeed(c) {
        TcpHost.hostClose(c)
      }
    }
    case Err(e) -> pending.fail(TcpHost.TcpError.message(e))
  }
}

/// Run the client side of a TLS handshake over the freshly connected `s`,
/// waiting at most `timeoutMs` milliseconds (`0` = no limit). On `Err` the
/// connection is closed.
pub func sockStartTls(s: in Sock, tls: in ClientTls, timeoutMs: in Int): Result[Sock, String]
  requires: timeoutMs >= 0
{
  val identity = match clientIdentity(tls) { case Ok(found) -> found; case Err(e) -> { sockClose(s); return Err(e) } }
  // From here the socket is the handshake's: it becomes the TLS connection
  // or is closed by `Std.TcpHost`.
  if not s.state.disown() {
    return Err(closedText)
  }
  val pending = Pending()
  if timeoutMs == 0 {
    handshakeInto(pending, s.conn, tls, identity, 0)
  } else {
    val inTime = Task.runActionWithin({ -> handshakeInto(pending, s.conn, tls, identity, timeoutMs) }, timeoutMs)
    if not inTime and pending.abandon() {
      return Err("the TLS handshake timed out after " + timeoutMs.toString() + " ms")
    }
  }
  match pending.connection() {
    case Some(c) -> Ok(sockOver(c, true))
    case None -> Err(pending.failureText())
  }
}

func serverConfig(tls: in ServerTls): Result[Tls.TlsServerConfig, String] {
  val identity = match Tls.Identity.fromPem(tls.certPem, tls.keyPem) {
    case Ok(found) -> found
    case Err(e) -> {
      return Err("the server certificate and key: " + Tls.TlsError.message(e))
    }
  }
  if tls.clientCaPem.length == 0 {
    return Ok(Tls.TlsServerConfig(identity = identity))
  }
  match Tls.Certificate.fromPem(tls.clientCaPem) {
    case Ok(ca) -> Ok(Tls.TlsServerConfig(identity = identity, clientCa = Some(ca), requireClientCert = true))
    case Err(e) -> Err("the client CA: " + Tls.TlsError.message(e))
  }
}

/// Run the server side of a TLS handshake over an accepted connection,
/// waiting at most `timeoutMs` milliseconds (`0` = no limit). On `Err` the
/// connection is closed.
pub func sockAcceptTls(s: in Sock, tls: in ServerTls, timeoutMs: in Int): Result[Sock, String]
  requires: timeoutMs >= 0
{
  val prepared = match serverConfig(tls) {
    case Err(e) -> Err(e)
    case Ok(config) -> match TcpHost.hostPrepareServerTls(config) {
      case Ok(found) -> Ok(found)
      case Err(e) -> Err(TcpHost.TcpError.message(e))
    }
  }
  val context = match prepared { case Ok(found) -> found; case Err(e) -> { sockClose(s); return Err(e) } }
  if not s.state.enter() {
    TcpHost.hostReleaseServerTls(context)
    return Err(closedText)
  }
  val upgraded = TcpHost.hostUpgradeServerTlsPrepared(s.conn, context, timeoutMs)
  TcpHost.hostReleaseServerTls(context)
  match upgraded {
    case Ok(c) -> {
      val _ = s.state.disown()
      Ok(sockOver(c, true))
    }
    case Err(e) -> {
      leave(s)
      sockClose(s)
      Err(TcpHost.TcpError.message(e))
    }
  }
}

// ─── Listening (test support) ────────────────────────────────────────────────
// The accepts in progress on a listener, so that it is released after them.
pub protected type ListenState {
  var closed: Bool = false
  var accepting: Int = 0
  var idle: Task.CancellationTokenSource = Task.makeCancelSource()
  entry enter(): Bool {
    if closed {
      return false
    }
    accepting = accepting + 1
    true
  }
  // The event to set when the last accept left a closed listener.
  entry leave(): Option[Task.CancellationTokenSource] {
    accepting = accepting - 1
    if closed and accepting == 0 {
      return Some(idle)
    }
    None
  }
  // `None` when the listener was already closed; otherwise the event to wait
  // for before releasing it, already set when no accept is in progress.
  entry beginClose(): Option[Task.CancellationTokenSource] {
    if closed {
      return None
    }
    closed = true
    if accepting == 0 {
      Task.cancelSource(idle)
    }
    Some(idle)
  }
}

/// A bound, listening TCP socket (the loopback peer of hermetic tests; a
/// client never listens).
pub record ServerSock {
  listener: TcpHost.Listener
  state: ListenState
}

/// Listen on the IP literal `host` and `port` (`port = 0` picks a free port).
pub func serverListen(host: in String, port: in Int): Result[ServerSock, String] {
  match TcpHost.hostListen(host, port) {
    case Ok(l) -> Ok(ServerSock(listener = l, state = ListenState()))
    case Err(e) -> Err(TcpHost.TcpError.message(e))
  }
}

/// The port `server` is bound to, `0` when it cannot be read.
pub func serverPort(server: in ServerSock): Int {
  val port = TcpHost.hostLocalPort(server.listener)
  if port < 0 {
    0
  } else {
    port
  }
}

/// Block until a client connects.
pub func serverAccept(server: in ServerSock): Result[Sock, String] {
  if not server.state.enter() {
    return Err("the listener is closed")
  }
  val accepted = TcpHost.hostAccept(server.listener)
  match server.state.leave() {
    case Some(idle) -> eventSet(idle)
    case None -> ()
  }
  match accepted {
    case Ok(c) -> Ok(sockOver(c, false))
    case Err(e) -> Err(TcpHost.TcpError.message(e))
  }
}

/// Stop listening. An accept in progress on another thread ends with an
/// `Err`; the listener is released once it has.
pub func serverClose(server: in ServerSock): Unit {
  match server.state.beginClose() {
    case None -> ()
    case Some(idle) -> {
      TcpHost.hostStopListener(server.listener)
      eventWaitForever(idle)
      TcpHost.hostCloseListener(server.listener)
    }
  }
}

// ─── Background work (test support) ──────────────────────────────────────────
/// A unit of work running on its own thread.
pub record Background {
  work: Task.Scope
}

/// Run `work` on a thread of its own.
pub func startBackground(work: in () -> Unit): Background {
  val started = Task.makeScope()
  Task.scopeSpawn(started, work)
  Background(work = started)
}

/// A thread of its own: native has no shared thread pool to keep busy.
pub func startPoolWork(work: in () -> Unit): Background {
  startBackground(work)
}

/// Block until `b` has finished.
pub func waitBackground(b: in Background): Unit {
  val _ = Task.awaitAll(b.work)
}
