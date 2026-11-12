//! Mq.ActiveMq.Kernel (dotnet) - the socket boundary of the STOMP client on
//! `--target dotnet`.
//!
//! `System.Net.Sockets.TcpClient` / `TcpListener` / `Socket`, a monitor lock,
//! a monotonic clock and a background task. Every operation returns a
//! `Result` with the failure text; nothing throws.
//!
//! `Std.TcpHost` is not used: a STOMP consumer has to tell "nothing arrived
//! within the wait" from "the connection failed", and `Std.TcpHost` reports a
//! receive timeout as the same `TransportFailed` as a reset. Here a read waits
//! with `Socket.Poll`, which answers that question without an exception.
//!
//! The JVM twin (`src/_kernel_jvm/socket_kernel.l`) declares the same package
//! with the same public surface; a file-level `@cfg(target = ...)` leaves
//! exactly one of them in a build.
@axiom("the host socket, thread, lock and clock primitives conform to their documented platform contracts")
@cfg(target = "dotnet")
@io
package Mq.ActiveMq.Kernel

import Std.Core
import Std.String as Str
import Std.Parse as Parse

extern type TcpClient = "System.Net.Sockets.TcpClient"

extern type TcpListener = "System.Net.Sockets.TcpListener"

extern type NetSocket = "System.Net.Sockets.Socket"

extern type SelectMode = "System.Net.Sockets.SelectMode"

extern type Stream = "System.IO.Stream"

extern type MemStream = "System.IO.MemoryStream"

extern type IPAddress = "System.Net.IPAddress"

extern type EndPoint = "System.Net.EndPoint"

extern type Task = "System.Threading.Tasks.Task"

@externTarget("System.Net.Sockets.TcpClient..ctor")
func newTcpClient(): TcpClient = ()

@externInstance
@externTarget("System.Net.Sockets.TcpClient.ConnectAsync")
func clientConnectAsync(c: in TcpClient, host: in String, port: in Int): Task = ()

@externInstance
@externTarget("System.Threading.Tasks.Task.Wait")
func taskWaitFor(t: in Task, timeoutMs: in Int): Bool = ()

@externInstance
@externTarget("System.Threading.Tasks.Task.Wait")
func taskWait(t: in Task): Unit = ()

@externStatic
@externTarget("System.Threading.Tasks.Task.Run")
func taskRun(work: in () -> Unit): Task = ()

@externInstance
@externTarget("System.Net.Sockets.TcpClient.GetStream")
func clientStream(c: in TcpClient): Stream = ()

@externInstance
@externTarget("System.Net.Sockets.TcpClient.Close")
func clientClose(c: in TcpClient): Unit = ()

@externInstance
@externTarget("System.Net.Sockets.TcpClient.set_NoDelay")
func clientSetNoDelay(c: in TcpClient, on: in Bool): Unit = ()

@externTarget("System.Net.Sockets.TcpClient.get_Client")
func clientSocket(c: in TcpClient): NetSocket = ()

@externTarget("System.Net.Sockets.SelectMode.SelectRead")
func selectRead(): SelectMode = ()

// `Poll` answers true when a read would not block: bytes are waiting, or the
// peer closed (the read then returns 0).
@externInstance
@externTarget("System.Net.Sockets.Socket.Poll")
func socketPoll(s: in NetSocket, microSeconds: in Int, mode: in SelectMode): Bool = ()

@externInstance
@externTarget("System.IO.Stream.Read")
func streamReadInto(s: in Stream, buffer: in slice[Byte], offset: in Int, count: in Int): Int = ()

@externInstance
@externTarget("System.IO.Stream.Write")
func streamWriteFrom(s: in Stream, buffer: in slice[Byte], offset: in Int, count: in Int): Unit = ()

@externInstance
@externTarget("System.IO.Stream.Flush")
func streamFlush(s: in Stream): Unit = ()

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

val readBufferSize: Int = 16384

/// One open TCP connection.
pub record Sock {
  client: TcpClient
  stream: Stream
  readBuf: MemStream
}

/// What one read produced.
pub union ReadOutcome {
  case Data(bytes: slice[Byte])
  case TimedOut
  case Eof
}

/// A bound, listening TCP socket (the loopback STOMP peer of the hermetic
/// tests; the client itself never listens).
pub record ServerSock {
  listener: TcpListener
}

/// A unit of work running on its own thread.
pub record Background {
  task: Task
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

func wrapClient(client: in TcpClient): Sock {
  clientSetNoDelay(client, true)
  Sock(client = client, stream = clientStream(client), readBuf = newMemStreamCap(readBufferSize))
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

/// Read up to `maxBytes` bytes, waiting at most `timeoutMs` milliseconds for
/// the first one (`0` = only what has already arrived).
pub func sockRead(s: in Sock, maxBytes: in Int, timeoutMs: in Int): Result[ReadOutcome, String]
  requires: maxBytes > 0
  requires: timeoutMs >= 0
{
  try {
    // Poll takes microseconds in an Int; cap the wait so it cannot overflow.
    val waitMs = if timeoutMs > 2000000 { 2000000 } else { timeoutMs }
    if not socketPoll(clientSocket(s.client), waitMs * 1000, selectRead()) {
      return Ok(TimedOut)
    }
    val want = if maxBytes < readBufferSize { maxBytes } else { readBufferSize }
    val n = streamReadInto(s.stream, memGetBuffer(s.readBuf), 0, want)
    if n <= 0 {
      return Ok(Eof)
    }
    Ok(Data(bytes = memGetBuffer(s.readBuf).slice(0, n)))
  } catch Bug as b {
    Err(b.message)
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
  closeQuietly(s.client)
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
