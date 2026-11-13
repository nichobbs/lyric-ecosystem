//! tests/kernel_tests.l - the socket kernel against a loopback peer: connect,
//! timed reads, writes and their timeout, end of stream, a refused connection,
//! the lock, the clock, and independence from the shared thread pool.
@test_module
package Mq.ActiveMq.KernelTests

import Std.Core
import Std.Testing
import Std.Encoding
import Std.String as Str
import Mq.ActiveMq.Kernel as K

func unwrapServer(): K.ServerSock {
  match K.serverListen("127.0.0.1", 0) {
    case Ok(s) -> s
    case Err(e) -> panic("listen failed: " + e)
  }
}

func echoOnce(server: in K.ServerSock): Unit {
  match K.serverAccept(server) {
    case Err(_) -> ()
    case Ok(peer) -> {
      match K.sockRead(peer, 1024, 2000) {
        case Ok(Data(bytes)) -> {
          val _ = K.sockWrite(peer, bytes)
        }
        case _ -> ()
      }
      K.sockClose(peer)
    }
  }
}

test "a write is echoed back, then the peer's close reads as Eof" {
  val server = unwrapServer()
  val port = K.serverPort(server)
  assertTrue(port > 0, "an ephemeral port was assigned")
  val bg = K.startBackground({ -> echoOnce(server) })
  match K.sockConnect("127.0.0.1", port, 2000) {
    case Err(e) -> assertTrue(false, "connect failed: " + e)
    case Ok(sock) -> {
      match K.sockRead(sock, 1024, 0) {
        case Ok(TimedOut) -> assertTrue(true, "a poll with nothing waiting times out")
        case Err(e) -> assertTrue(false, "ERR " + e)
        case _ -> assertTrue(false, "expected TimedOut from an immediate poll")
      }
      match K.sockRead(sock, 1024, 40) {
        case Ok(TimedOut) -> assertTrue(true, "a short wait with nothing waiting times out")
        case _ -> assertTrue(false, "expected TimedOut from a short wait")
      }
      match K.sockWrite(sock, encodeUtf8("ping é")) {
        case Ok(_) -> ()
        case Err(e) -> assertTrue(false, "write failed: " + e)
      }
      match K.sockRead(sock, 1024, 2000) {
        case Ok(Data(bytes)) -> {
          match tryDecodeUtf8(bytes) {
            case Some(s) -> assertEqual(s, "ping é", "the echoed bytes")
            case None -> assertTrue(false, "echo was not UTF-8")
          }
        }
        case _ -> assertTrue(false, "expected the echo")
      }
      match K.sockRead(sock, 1024, 2000) {
        case Ok(Eof) -> assertTrue(true, "the peer closed")
        case _ -> assertTrue(false, "expected Eof after the peer closed")
      }
      K.sockClose(sock)
      K.sockClose(sock)
    }
  }
  K.waitBackground(bg)
  K.serverClose(server)
}

func writeThenClose(server: in K.ServerSock, text: in String): Unit {
  match K.serverAccept(server) {
    case Err(_) -> ()
    case Ok(peer) -> {
      val _ = K.sockWrite(peer, encodeUtf8(text))
      K.sockClose(peer)
    }
  }
}

func readText(sock: in K.Sock, maxBytes: in Int): String {
  match K.sockRead(sock, maxBytes, 2000) {
    case Ok(Data(bytes)) -> match tryDecodeUtf8(bytes) { case Some(s) -> s; case None -> "<not UTF-8>" }
    case Ok(TimedOut) -> "<timed out>"
    case Ok(Eof) -> "<eof>"
    case Err(e) -> "<error: " + e + ">"
  }
}

test "a read never returns more than it was asked for, and loses nothing" {
  val server = unwrapServer()
  val bg = K.startBackground({ -> writeThenClose(server, "0123456789") })
  match K.sockConnect("127.0.0.1", K.serverPort(server), 2000) {
    case Err(e) -> assertTrue(false, "connect failed: " + e)
    case Ok(sock) -> {
      K.waitBackground(bg)
      var all = ""
      var rounds = 0
      while all.length < 10 and rounds < 10 {
        val part = readText(sock, 4)
        assertTrue(part.length >= 1 and part.length <= 4, "at most 4 bytes: " + part)
        all = all + part
        rounds += 1
      }
      assertEqual(all, "0123456789", "every byte, in order")
      assertEqual(readText(sock, 4), "<eof>", "then the close")
      K.sockClose(sock)
    }
  }
  K.serverClose(server)
}

test "a refused connection is an Err" {
  val server = unwrapServer()
  val port = K.serverPort(server)
  K.serverClose(server)
  match K.sockConnect("127.0.0.1", port, 2000) {
    case Ok(sock) -> {
      K.sockClose(sock)
      assertTrue(false, "nothing listens on the port any more")
    }
    case Err(e) -> assertTrue(e.length > 0, "the failure has a message")
  }
}

test "the lock is reentrant and the clock does not go backwards" {
  val l = K.newLock()
  K.lockEnter(l)
  K.lockEnter(l)
  K.lockExit(l)
  K.lockExit(l)
  val before = K.monotonicMillis()
  K.sleepMs(20)
  val after = K.monotonicMillis()
  assertTrue(after >= before + 10i64, "sleep advanced the clock")
}

test "two locks are two locks" {
  assertTrue(K.newLock() != K.newLock(), "a lock has identity: two locks are never the same lock")
}

func holdBriefly(l: in K.Lock, held: in Held): Unit {
  K.lockEnter(l)
  held.since = K.monotonicMillis()
  K.sleepMs(600)
  K.lockExit(l)
}

ref record Held {
  var since: Long
}

test "lockTryEnter gives up after its limit and succeeds on a free lock" {
  val l = K.newLock()
  assertTrue(K.lockTryEnter(l, 0), "a free lock is taken at once")
  assertTrue(K.lockTryEnter(l, 0), "and again by its holder: it is reentrant")
  K.lockExit(l)
  K.lockExit(l)
  val held = Held(since = 0i64)
  val holder = K.startBackground({ -> holdBriefly(l, held) })
  while held.since == 0i64 {
    K.sleepMs(5)
  }
  assertTrue(not K.lockTryEnter(l, 0), "a lock another thread holds is not taken")
  assertTrue(not K.lockTryEnter(l, 50), "nor within a short wait")
  assertTrue(K.lockTryEnter(l, 10000), "but once the holder lets go")
  K.lockExit(l)
  K.waitBackground(holder)
  val other = K.newLock()
  K.lockEnter(l)
  assertTrue(K.lockTryEnter(other, 0), "holding one lock does not hold another")
  K.lockExit(other)
  K.lockExit(l)
}

func acceptAndIgnore(server: in K.ServerSock, done: in Held): Unit {
  match K.serverAccept(server) {
    case Err(_) -> ()
    case Ok(peer) -> {
      while done.since == 0i64 {
        K.sleepMs(20)
      }
      K.sockClose(peer)
    }
  }
}

test "a write to a peer that reads nothing ends with its timeout" {
  val server = unwrapServer()
  val done = Held(since = 0i64)
  val bg = K.startBackground({ -> acceptAndIgnore(server, done) })
  match K.sockConnect("127.0.0.1", K.serverPort(server), 2000) {
    case Err(e) -> assertTrue(false, "connect failed: " + e)
    case Ok(sock) -> {
      match K.sockWriteWithin(sock, encodeUtf8("small"), 500) {
        case Ok(_) -> ()
        case Err(e) -> assertTrue(false, "a write the buffers take succeeds: " + e)
      }
      var megabyte = "0123456789abcdef"
      while megabyte.length < 1048576 {
        megabyte = megabyte + megabyte
      }
      val chunk = encodeUtf8(megabyte)
      val started = K.monotonicMillis()
      var failure = ""
      var written = 0
      while failure.length == 0 and written < 256 {
        match K.sockWriteWithin(sock, chunk, 500) {
          case Ok(_) -> {
            written += 1
          }
          case Err(e) -> {
            failure = e
          }
        }
      }
      val elapsed = K.monotonicMillis() - started
      assertTrue(Str.contains(failure, "the write timed out"), "after " + written.toString() + " MiB: " + failure)
      assertTrue(elapsed < 60000i64, "the stalled write returned after " + elapsed.toString() + " ms")
      K.sockClose(sock)
    }
  }
  done.since = 1i64
  K.waitBackground(bg)
  K.serverClose(server)
}

func idleWhile(done: in Held): Unit {
  while done.since == 0i64 {
    K.sleepMs(20)
  }
}

test "connects, reads and background work do not wait for the shared thread pool" {
  val done = Held(since = 0i64)
  var n = 0
  while n < 64 {
    val _ = K.startPoolWork({ -> idleWhile(done) })
    n += 1
  }
  K.sleepMs(200)
  val server = unwrapServer()
  val bg = K.startBackground({ -> echoOnce(server) })
  val started = K.monotonicMillis()
  match K.sockConnect("127.0.0.1", K.serverPort(server), 5000) {
    case Err(e) -> assertTrue(false, "connect failed: " + e)
    case Ok(sock) -> {
      val _ = K.sockWrite(sock, encodeUtf8("busy"))
      match K.sockRead(sock, 1024, 1000) {
        case Ok(Data(bytes)) -> assertEqualInt(bytes.length, 4, "the echo")
        case _ -> assertTrue(false, "the echo did not arrive within a second")
      }
      K.sockClose(sock)
    }
  }
  val elapsed = K.monotonicMillis() - started
  assertTrue(elapsed < 3000i64, "the round trip took " + elapsed.toString() + " ms")
  done.since = 1i64
  K.waitBackground(bg)
  K.serverClose(server)
}
