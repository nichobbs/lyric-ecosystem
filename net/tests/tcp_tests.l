//! tests/tcp_tests.l - `Net.Tcp` against a loopback peer: connect, timed
//! reads, writes and their timeout, end of stream, a refused connection, the
//! lock, the clock, independence from the shared thread pool, reads split
//! across timeouts, and the TLS client handshake (trust, names, client
//! identity, versions).
@test_module
package Net.Tcp.Tests

import Std.Core
import Std.Collections
import Std.Testing
import Std.Encoding
import Std.String as Str
import Std.Tls as Tls
import Net.Tls
import Net.Tcp as K
import Net.Tcp.Testing as KT

func unwrapServer(): KT.ServerSock {
  match KT.serverListen("127.0.0.1", 0) {
    case Ok(s) -> s
    case Err(e) -> panic("listen failed: " + e)
  }
}

func echoOnce(server: in KT.ServerSock): Unit {
  match KT.serverAccept(server) {
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
  val port = KT.serverPort(server)
  assertTrue(port > 0, "an ephemeral port was assigned")
  val bg = KT.startBackground({ -> echoOnce(server) })
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
  KT.waitBackground(bg)
  KT.serverClose(server)
}

func writeThenClose(server: in KT.ServerSock, text: in String): Unit {
  match KT.serverAccept(server) {
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
  val bg = KT.startBackground({ -> writeThenClose(server, "0123456789") })
  match K.sockConnect("127.0.0.1", KT.serverPort(server), 2000) {
    case Err(e) -> assertTrue(false, "connect failed: " + e)
    case Ok(sock) -> {
      KT.waitBackground(bg)
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
  KT.serverClose(server)
}

test "a refused connection is an Err" {
  val server = unwrapServer()
  val port = KT.serverPort(server)
  KT.serverClose(server)
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
  val holder = KT.startBackground({ -> holdBriefly(l, held) })
  while held.since == 0i64 {
    K.sleepMs(5)
  }
  assertTrue(not K.lockTryEnter(l, 0), "a lock another thread holds is not taken")
  assertTrue(not K.lockTryEnter(l, 50), "nor within a short wait")
  assertTrue(K.lockTryEnter(l, 10000), "but once the holder lets go")
  K.lockExit(l)
  KT.waitBackground(holder)
  val other = K.newLock()
  K.lockEnter(l)
  assertTrue(K.lockTryEnter(other, 0), "holding one lock does not hold another")
  K.lockExit(other)
  K.lockExit(l)
}

func acceptAndIgnore(server: in KT.ServerSock, done: in Held): Unit {
  match KT.serverAccept(server) {
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
  val bg = KT.startBackground({ -> acceptAndIgnore(server, done) })
  match K.sockConnect("127.0.0.1", KT.serverPort(server), 2000) {
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
  KT.waitBackground(bg)
  KT.serverClose(server)
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
    val _ = KT.startPoolWork({ -> idleWhile(done) })
    n += 1
  }
  K.sleepMs(200)
  val server = unwrapServer()
  val bg = KT.startBackground({ -> echoOnce(server) })
  val started = K.monotonicMillis()
  match K.sockConnect("127.0.0.1", KT.serverPort(server), 5000) {
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
  KT.waitBackground(bg)
  KT.serverClose(server)
}

// ─── Reads split across timeouts, and TLS ────────────────────────────────────
func filler(count: in Int): String {
  var text = "x"
  while text.length < count {
    text = text + text
  }
  Str.substring(text, 0, count)
}

func serverTls(clientCaPem: in String): KT.ServerTls {
  KT.ServerTls(certPem = TEST_ONLY_SERVER_CERT, keyPem = TEST_ONLY_SERVER_KEY, clientCaPem = clientCaPem)
}

func trusting(): TlsOptions {
  plainTcp().withTls().withExclusiveCaCertificate(TEST_ONLY_CA)
}

// A peer that sends `text` in `pieces` writes, `pauseMs` apart.
func dribble(server: in KT.ServerSock, secured: in Bool, text: in String, pieces: in Int, pauseMs: in Int): Unit {
  val accepted = match KT.serverAccept(server) { case Ok(s) -> s; case Err(_) -> return }
  val sock = if secured {
    match KT.sockAcceptTls(accepted, serverTls(""), 5000) {
      case Ok(s) -> s
      case Err(_) -> return
    }
  } else {
    accepted
  }
  val bytes = encodeUtf8(text)
  val step = bytes.length / pieces
  var n = 0
  while n < pieces {
    val stop = if n == pieces - 1 { bytes.length } else { (n + 1) * step }
    val _ = K.sockWrite(sock, bytes.slice(n * step, stop))
    K.sleepMs(pauseMs)
    n += 1
  }
  K.sockClose(sock)
}

// Read with `waitMs` per call until the peer closes; every byte must come
// through, in order, whatever the read timeouts cut across.
func readDribble(secured: in Bool, waitMs: in Int, pauseMs: in Int, label: in String): Unit {
  val polling = waitMs < pauseMs / 2
  val server = match KT.serverListen("127.0.0.1", 0) { case Ok(s) -> s; case Err(e) -> panic(e) }
  val text = filler(40) + "|" + filler(700) + "|end"
  val peer = KT.startBackground({ -> dribble(server, secured, text, 12, pauseMs) })
  val plain = match K.sockConnect("127.0.0.1", KT.serverPort(server), 5000) { case Ok(s) -> s; case Err(e) -> panic(e) }
  val sock = if secured {
    match K.sockStartTls(plain, trusting(), "localhost", false, 5000) {
      case Ok(s) -> s
      case Err(e) -> panic(label + ": handshake: " + e)
    }
  } else {
    plain
  }
  val got: List[Byte] = newList()
  var timeouts = 0
  var open = true
  val giveUp = K.monotonicMillis() + 30000i64
  while open and K.monotonicMillis() < giveUp {
    match K.sockRead(sock, 7, waitMs) {
      case Ok(Data(bytes)) -> {
        for b in bytes {
          got.add(b)
        }
      }
      case Ok(TimedOut) -> {
        timeouts += 1
      }
      case Ok(Eof) -> {
        open = false
      }
      case Err(e) -> {
        assertTrue(false, label + ": read failed: " + e)
        open = false
      }
    }
  }
  assertTrue(not open, label + ": the peer's close was seen")
  val received = match tryDecodeUtf8(got.toArray()) { case Some(t) -> t; case None -> "<not UTF-8>" }
  assertTrue(
    received == text,
    label + ": " + got.count.toString() + " bytes after " + timeouts.toString() + " timeouts"
  )
  if polling {
    assertTrue(timeouts > 0, label + ": the reads did time out between the pieces")
  }
  match K.sockRead(sock, 7, waitMs) {
    case Ok(Eof) -> ()
    case _ -> assertTrue(false, label + ": the end of the stream is reported again")
  }
  K.sockClose(sock)
  KT.waitBackground(peer)
  KT.serverClose(server)
}

test "bytes sent in pieces across read timeouts all arrive, in order, over plain TCP and TLS" {
  // Waits shorter than, equal to and longer than the pause between pieces,
  // so reads expire before, while and after a piece arrives.
  readDribble(false, 1, 30, "plain, polling")
  readDribble(false, 30, 30, "plain, wait equal to the pause")
  readDribble(false, 45, 30, "plain, wait longer than the pause")
  readDribble(true, 1, 30, "TLS, polling")
  readDribble(true, 30, 30, "TLS, wait equal to the pause")
  readDribble(true, 45, 30, "TLS, wait longer than the pause")
}

// A TLS peer that echoes one read, or gives up when the handshake fails.
func tlsEchoOnce(server: in KT.ServerSock, tls: in KT.ServerTls): Unit {
  val accepted = match KT.serverAccept(server) { case Ok(s) -> s; case Err(_) -> return }
  match KT.sockAcceptTls(accepted, tls, 5000) {
    case Err(_) -> ()
    case Ok(peer) -> {
      match K.sockRead(peer, 1024, 3000) {
        case Ok(Data(bytes)) -> {
          val _ = K.sockWrite(peer, bytes)
        }
        case _ -> ()
      }
      K.sockClose(peer)
    }
  }
}

// One connection to a TLS peer: "" when a write was echoed back over it,
// otherwise what went wrong.
func tlsRoundTrip(server: in KT.ServerTls, client: in TlsOptions, host: in String, skipVerify: in Bool): String {
  val listener = unwrapServer()
  val peer = KT.startBackground({ -> tlsEchoOnce(listener, server) })
  val plain = match K.sockConnect("127.0.0.1", KT.serverPort(listener), 5000) {
    case Ok(s) -> s
    case Err(e) -> panic(e)
  }
  val outcome = match K.sockStartTls(plain, client, host, skipVerify, 5000) {
    case Err(e) -> "handshake: " + e
    case Ok(sock) -> {
      val echoed = match K.sockWriteWithin(sock, encodeUtf8("over TLS"), 2000) {
        case Err(e) -> "write: " + e
        case Ok(_) -> match K.sockRead(sock, 1024, 3000) {
          case Ok(Data(bytes)) -> if bytes.length == 8 { "" } else { "a short echo" }
          case Ok(TimedOut) -> "read: timed out"
          case Ok(Eof) -> "read: the peer closed"
          case Err(e) -> "read: " + e
        }
      }
      K.sockClose(sock)
      echoed
    }
  }
  KT.waitBackground(peer)
  KT.serverClose(listener)
  outcome
}

func expectHandshakeFailure(outcome: in String, label: in String): Unit {
  assertTrue(Str.startsWith(outcome, "handshake: "), label + ": " + outcome)
}

test "a server certificate is trusted through an exclusive or an additional CA, and not without one" {
  assertEqual(tlsRoundTrip(serverTls(""), trusting(), "localhost", false), "", "exclusive CA")
  assertEqual(
    tlsRoundTrip(serverTls(""), plainTcp().withTls().withCaCertificate(TEST_ONLY_CA), "localhost", false),
    "",
    "additional CA"
  )
  expectHandshakeFailure(tlsRoundTrip(serverTls(""), plainTcp().withTls(), "localhost", false), "platform trust only")
  expectHandshakeFailure(
    tlsRoundTrip(serverTls(""), plainTcp().withTls().withExclusiveCaCertificate(TEST_ONLY_OTHER_CA), "localhost", false),
    "another CA, exclusive"
  )
  expectHandshakeFailure(
    tlsRoundTrip(serverTls(""), plainTcp().withTls().withCaCertificate(TEST_ONLY_OTHER_CA), "localhost", false),
    "another CA, additional"
  )
  // The exclusive CA wins when both are set.
  expectHandshakeFailure(
    tlsRoundTrip(
      serverTls(""),
      plainTcp().withTls().withCaCertificate(TEST_ONLY_CA).withExclusiveCaCertificate(TEST_ONLY_OTHER_CA),
      "localhost",
      false
    ),
    "the exclusive CA wins"
  )
}

test "the server name is the host unless the options name another, and is always checked" {
  assertEqual(tlsRoundTrip(serverTls(""), trusting(), "127.0.0.1", false), "", "the certificate names the address too")
  expectHandshakeFailure(
    tlsRoundTrip(serverTls(""), trusting(), "broker.invalid", false),
    "a host the certificate lacks"
  )
  assertEqual(
    tlsRoundTrip(serverTls(""), trusting().withTlsServerName("localhost"), "broker.invalid", false),
    "",
    "withTlsServerName replaces the host"
  )
  expectHandshakeFailure(
    tlsRoundTrip(serverTls(""), trusting().withTlsServerName("broker.invalid"), "localhost", false),
    "a server name the certificate lacks"
  )
}

test "skipVerify accepts an untrusted certificate and a wrong name; the option alone does not" {
  val insecure = plainTcp().withTls().withTlsServerName("broker.invalid").withInsecureSkipVerify()
  assertEqual(tlsRoundTrip(serverTls(""), insecure, "localhost", true), "", "verification off")
  expectHandshakeFailure(tlsRoundTrip(serverTls(""), insecure, "localhost", false), "the option without the decision")
}

test "a client identity is presented to a server that requires one" {
  val mutual = serverTls(TEST_ONLY_CA)
  assertEqual(
    tlsRoundTrip(mutual, trusting().withClientIdentity(TEST_ONLY_CLIENT_CERT, TEST_ONLY_CLIENT_KEY), "localhost", false),
    "",
    "mutual TLS"
  )
  // TLS 1.3 reports a refused client certificate after the client's
  // handshake is done, so the failure may be the handshake or the first use.
  assertTrue(tlsRoundTrip(mutual, trusting(), "localhost", false).length > 0, "no client certificate")
  assertTrue(
    tlsRoundTrip(mutual, trusting().withClientIdentity(TEST_ONLY_STRAY_CERT, TEST_ONLY_STRAY_KEY), "localhost", false).length >
      0,
    "a client certificate from another CA"
  )
  val unusable = tlsRoundTrip(
    serverTls(""),
    trusting().withClientIdentity(TEST_ONLY_CLIENT_CERT, TEST_ONLY_STRAY_KEY),
    "localhost",
    false
  )
  expectHandshakeFailure(unusable, "a key that is not the certificate's")
}

test "TLS 1.3 can be required" {
  assertEqual(
    tlsRoundTrip(serverTls(""), trusting().withMinTlsVersion(Tls.TlsVersion.Tls13), "localhost", false),
    "",
    "1.3"
  )
}

test "a handshake with a peer that never answers ends with its timeout" {
  val listener = unwrapServer()
  val done = Held(since = 0i64)
  val peer = KT.startBackground({ -> acceptAndIgnore(listener, done) })
  val plain = match K.sockConnect("127.0.0.1", KT.serverPort(listener), 5000) {
    case Ok(s) -> s
    case Err(e) -> panic(e)
  }
  val started = K.monotonicMillis()
  match K.sockStartTls(plain, trusting(), "localhost", false, 300) {
    case Ok(sock) -> {
      K.sockClose(sock)
      assertTrue(false, "a peer that is not TLS")
    }
    case Err(e) -> assertTrue(Str.contains(e, "timed out"), e)
  }
  assertTrue(K.monotonicMillis() - started < 3000i64, "the handshake was given up in time")
  done.since = 1i64
  KT.waitBackground(peer)
  KT.serverClose(listener)
}

// ─── TEST-ONLY certificates and keys ─────────────────────────────────────────
// Everything below was generated for hermetic tests with `openssl` (EC P-256,
// valid until 2126); the same fixtures are in mq-activemq's client tests. The
// private keys are public by being here: never trust these certificates
// outside a test.
//   TEST_ONLY_CA            self-signed CA
//   TEST_ONLY_SERVER_*      issued by the CA; SAN DNS:localhost, IP:127.0.0.1
//   TEST_ONLY_CLIENT_*      issued by the CA; clientAuth
//   TEST_ONLY_OTHER_CA      a second, unrelated self-signed CA
//   TEST_ONLY_STRAY_*       a client certificate issued by the other CA
// test-only
val TEST_ONLY_CA: String = "-----BEGIN CERTIFICATE-----\n" + "MIIB4TCCAYegAwIBAgIUMdZcqE1iGtInxR0FPbqibzyXeHYwCgYIKoZIzj0EAwIw\n" + "PTEkMCIGA1UECgwbbHlyaWMtbXEtYWN0aXZlbXEgVEVTVCBPTkxZMRUwEwYDVQQD\n" + "DAx0ZXN0LW9ubHkgQ0EwIBcNMjYxMTEyMjA0MzQyWhgPMjEyNjEwMTkyMDQzNDJa\n" + "MD0xJDAiBgNVBAoMG2x5cmljLW1xLWFjdGl2ZW1xIFRFU1QgT05MWTEVMBMGA1UE\n" + "AwwMdGVzdC1vbmx5IENBMFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEYAYCGXrA\n" + "I2ewAr+wvyoRAfbXD36+mTug7dD9P79g9FPZ4pb/G1LPreTm3JJpvL0rPMcjyZ6i\n" + "atNgUShoEQtM1KNjMGEwHQYDVR0OBBYEFBO96Lq71dvpw7aQAdoxUqxZdTZiMB8G\n" + "A1UdIwQYMBaAFBO96Lq71dvpw7aQAdoxUqxZdTZiMA8GA1UdEwEB/wQFMAMBAf8w\n" + "DgYDVR0PAQH/BAQDAgEGMAoGCCqGSM49BAMCA0gAMEUCIEPshpyn3+k8gCVsrkjG\n" + "hpyEoRE8+JDdAiWTbTZM17iqAiEAzarnIdQWuq0m+22jQ8ljnODgMXejV5pJ30E3\n" + "C5N+2hs=\n" + "-----END CERTIFICATE-----\n"

// test-only
val TEST_ONLY_SERVER_CERT: String = "-----BEGIN CERTIFICATE-----\n" + "MIICATCCAaagAwIBAgIUCKVnu3yl92vqNWhjbOod6MrPh4gwCgYIKoZIzj0EAwIw\n" + "PTEkMCIGA1UECgwbbHlyaWMtbXEtYWN0aXZlbXEgVEVTVCBPTkxZMRUwEwYDVQQD\n" + "DAx0ZXN0LW9ubHkgQ0EwIBcNMjYxMTEyMjA0MzQyWhgPMjEyNjEwMTkyMDQzNDJa\n" + "MDoxJDAiBgNVBAoMG2x5cmljLW1xLWFjdGl2ZW1xIFRFU1QgT05MWTESMBAGA1UE\n" + "AwwJbG9jYWxob3N0MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAE4UqMwZLtPgfr\n" + "qsc8bdkA+Ma4as92fSn9nFyRnSlkW2qbsPx1fsPpbzdnx8noQeU9ng2B7JZdGFT/\n" + "N41Ri4aJnqOBhDCBgTAaBgNVHREEEzARgglsb2NhbGhvc3SHBH8AAAEwEwYDVR0l\n" + "BAwwCgYIKwYBBQUHAwEwDgYDVR0PAQH/BAQDAgeAMB0GA1UdDgQWBBQwtXKELMB8\n" + "diGydfIRw6aYVl0dVzAfBgNVHSMEGDAWgBQTvei6u9Xb6cO2kAHaMVKsWXU2YjAK\n" + "BggqhkjOPQQDAgNJADBGAiEA9nWrmJBPYADaQCWfcLkxFIe+5NJtvPZWY4DzWJoz\n" + "cV0CIQD4sNqVPsbSEbuQuHAh+m82dFOxaOzuTU+Ptb+s6uKrzw==\n" + "-----END CERTIFICATE-----\n"

// test-only private key
val TEST_ONLY_SERVER_KEY: String = "-----BEGIN PRIVATE KEY-----\n" + "MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQg0iMsh8kO7i1GbAKc\n" + "gI7OOrGRS/k+bbNUtMsYmBA68X+hRANCAAThSozBku0+B+uqxzxt2QD4xrhqz3Z9\n" + "Kf2cXJGdKWRbapuw/HV+w+lvN2fHyehB5T2eDYHsll0YVP83jVGLhome\n" + "-----END PRIVATE KEY-----\n"

// test-only
val TEST_ONLY_CLIENT_CERT: String = "-----BEGIN CERTIFICATE-----\n" + "MIIB6jCCAY+gAwIBAgIUCKVnu3yl92vqNWhjbOod6MrPh4kwCgYIKoZIzj0EAwIw\n" + "PTEkMCIGA1UECgwbbHlyaWMtbXEtYWN0aXZlbXEgVEVTVCBPTkxZMRUwEwYDVQQD\n" + "DAx0ZXN0LW9ubHkgQ0EwIBcNMjYxMTEyMjA0MzQyWhgPMjEyNjEwMTkyMDQzNDJa\n" + "MEExJDAiBgNVBAoMG2x5cmljLW1xLWFjdGl2ZW1xIFRFU1QgT05MWTEZMBcGA1UE\n" + "AwwQdGVzdC1vbmx5IGNsaWVudDBZMBMGByqGSM49AgEGCCqGSM49AwEHA0IABBPG\n" + "NQcnPleid2CUSj3TcqY/7T2mH54mNqqRP89ugZtmCmPrKeguQejvY8i04nToDPGg\n" + "9Lno+WXA7UeDh7aEGYijZzBlMBMGA1UdJQQMMAoGCCsGAQUFBwMCMA4GA1UdDwEB\n" + "/wQEAwIHgDAdBgNVHQ4EFgQULwRAoPjdU+MFvJYdA4EeB9XhWDUwHwYDVR0jBBgw\n" + "FoAUE73ourvV2+nDtpAB2jFSrFl1NmIwCgYIKoZIzj0EAwIDSQAwRgIhAPHfud/j\n" + "lfy9BqSFYumS5MW7ljCZC+hBeO9fXZiiSbgDAiEAx+DPE3IVzjYVclCL4k/TT2+y\n" + "C9kbvqMHtdvi2dWjbJc=\n" + "-----END CERTIFICATE-----\n"

// test-only private key
val TEST_ONLY_CLIENT_KEY: String = "-----BEGIN PRIVATE KEY-----\n" + "MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgn3/zm5+BLXEqOfVa\n" + "81O7AX4r9UiOMVTJpfWUi9mn5CChRANCAAQTxjUHJz5XondglEo903KmP+09ph+e\n" + "JjaqkT/PboGbZgpj6ynoLkHo72PItOJ06AzxoPS56PllwO1Hg4e2hBmI\n" + "-----END PRIVATE KEY-----\n"

// test-only
val TEST_ONLY_OTHER_CA: String = "-----BEGIN CERTIFICATE-----\n" + "MIIB7DCCAZOgAwIBAgIUIff2SUzilDgPnfhpZuCjgCv0cuswCgYIKoZIzj0EAwIw\n" + "QzEkMCIGA1UECgwbbHlyaWMtbXEtYWN0aXZlbXEgVEVTVCBPTkxZMRswGQYDVQQD\n" + "DBJ0ZXN0LW9ubHkgb3RoZXIgQ0EwIBcNMjYxMTEyMjA0MzQyWhgPMjEyNjEwMTky\n" + "MDQzNDJaMEMxJDAiBgNVBAoMG2x5cmljLW1xLWFjdGl2ZW1xIFRFU1QgT05MWTEb\n" + "MBkGA1UEAwwSdGVzdC1vbmx5IG90aGVyIENBMFkwEwYHKoZIzj0CAQYIKoZIzj0D\n" + "AQcDQgAEUR1JFo0CD3Njte9zvDQRwZpfqkN99Cr/tWDv/py3OxILiIUZxPIAJjKZ\n" + "IwaEIBHAjfKazW3H6DwAKRDyLRMuKaNjMGEwHQYDVR0OBBYEFOc3kuRypGZLrgq+\n" + "ULOmQ53rPQc7MB8GA1UdIwQYMBaAFOc3kuRypGZLrgq+ULOmQ53rPQc7MA8GA1Ud\n" + "EwEB/wQFMAMBAf8wDgYDVR0PAQH/BAQDAgEGMAoGCCqGSM49BAMCA0cAMEQCIFDY\n" + "fnOZLZvo50DDnsjqmvU8QERfKI4BuZKAF1URYAMgAiA+BnCvnTP5byJjxU558h8x\n" + "lTB/jAfGEwVj5amWPzTAww==\n" + "-----END CERTIFICATE-----\n"

// test-only
val TEST_ONLY_STRAY_CERT: String = "-----BEGIN CERTIFICATE-----\n" + "MIIB9DCCAZugAwIBAgIUW4CJBIZ52Zh1nR8hK5SdBEl3Uh0wCgYIKoZIzj0EAwIw\n" + "QzEkMCIGA1UECgwbbHlyaWMtbXEtYWN0aXZlbXEgVEVTVCBPTkxZMRswGQYDVQQD\n" + "DBJ0ZXN0LW9ubHkgb3RoZXIgQ0EwIBcNMjYxMTEyMjA0MzQyWhgPMjEyNjEwMTky\n" + "MDQzNDJaMEcxJDAiBgNVBAoMG2x5cmljLW1xLWFjdGl2ZW1xIFRFU1QgT05MWTEf\n" + "MB0GA1UEAwwWdGVzdC1vbmx5IHN0cmF5IGNsaWVudDBZMBMGByqGSM49AgEGCCqG\n" + "SM49AwEHA0IABIkfflimVwpiL9W8mKqin/dEkFuBwKThiXNqht9NVJ/5ONO4xJgo\n" + "JKcroGDsJK0HsEL5vjBGzRxk6sdGUbh/p+2jZzBlMBMGA1UdJQQMMAoGCCsGAQUF\n" + "BwMCMA4GA1UdDwEB/wQEAwIHgDAdBgNVHQ4EFgQU8hV86rSPyWSoYYNIY19fk/G0\n" + "ptIwHwYDVR0jBBgwFoAU5zeS5HKkZkuuCr5Qs6ZDnes9BzswCgYIKoZIzj0EAwID\n" + "RwAwRAIgZDqq5uIxulT/iFszvxubUpoYw/9zr/0EhsEkzkX2ErkCIH9j9qmllwSG\n" + "ce8eoOWDafL1iEcoqdAufQP53uCudIrc\n" + "-----END CERTIFICATE-----\n"

// test-only private key
val TEST_ONLY_STRAY_KEY: String = "-----BEGIN PRIVATE KEY-----\n" + "MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQg/irIMjHltlSLzkqe\n" + "AitdTtpknt7mMQqZFfyCqWygP3ihRANCAASJH35YplcKYi/VvJiqop/3RJBbgcCk\n" + "4YlzaobfTVSf+TjTuMSYKCSnK6Bg7CStB7BC+b4wRs0cZOrHRlG4f6ft\n" + "-----END PRIVATE KEY-----\n"
