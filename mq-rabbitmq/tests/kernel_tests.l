//! tests/kernel_tests.l - the socket kernel against a loopback peer: connect,
//! timed reads, writes, end of stream, a refused connection, the lock and the
//! clock.
@test_module
package Mq.RabbitMq.KernelTests

import Std.Core
import Std.Testing
import Std.Encoding
import Mq.RabbitMq.Kernel as K

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
