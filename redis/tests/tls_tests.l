//! tests/tls_tests.l - the client over TLS against an in-process TLS server,
//! and the TLS configuration checks that need no server.
//!
//! The fake server listens on a free loopback port with `Kernel.serverListenTls`
//! (an `SslStream` server through `Std.TcpHost` on dotnet, an `SSLServerSocket`
//! on the JVM) and answers a few commands, so the hermetic suite runs real
//! handshakes on both targets: trust, the server name, mutual TLS, the
//! insecure policy, the TLS version floor, and reconnects and timeouts over
//! TLS.
//!
//! TEST FIXTURES, NOT SECRETS. The certificates and private keys below exist
//! only for this file: a throwaway CA, a server certificate for `localhost` /
//! `127.0.0.1`, a client certificate, and a second CA with a client
//! certificate of its own. They protect nothing and are valid until 2126.
//! They were made with:
//!
//!   openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out X.key
//!   openssl req -x509 -new -key ca.key -sha256 -days 36500 -subj "/O=Lyric.Redis test fixture/CN=..." \
//!     -addext "basicConstraints=critical,CA:TRUE" -addext "keyUsage=critical,keyCertSign,cRLSign" -out ca.crt
//!   openssl req -new -key X.key -subj "/O=Lyric.Redis test fixture/CN=..." -out X.csr
//!   openssl x509 -req -in X.csr -CA ca.crt -CAkey ca.key -CAcreateserial -sha256 -days 36500 \
//!     -extfile X.ext -out X.crt      (server: subjectAltName=DNS:localhost,IP:127.0.0.1)
//!   openssl ec -in client.key -out client.sec1.key
@test_module
package Redis.TlsTests

import Std.Core
import Std.Collections
import Std.Encoding
import Std.Environment as Env
import Std.File as File
import Std.Http as Http
import Std.String as Str
import Std.Testing
import Std.Time
import Std.Tls as Tls
import Std.Uuid
import Redis
import Redis.Resp
import Redis.Kernel as Kernel

// ─── Test fixtures (not secrets, see the header) ─────────────────────────────
// The CA that signed the server and the client certificate.
val caPem: String = "-----BEGIN CERTIFICATE-----\nMIICADCCAaWgAwIBAgIUcdyX600z6cYJ1OFtBpJhnDMVnTEwCgYIKoZIzj0EAwIw\nTDEhMB8GA1UECgwYTHlyaWMuUmVkaXMgdGVzdCBmaXh0dXJlMScwJQYDVQQDDB50\nZXN0IGZpeHR1cmUgQ0EgKG5vdCBhIHNlY3JldCkwIBcNMjYxMTEyMjA0MzMyWhgP\nMjEyNjEwMTkyMDQzMzJaMEwxITAfBgNVBAoMGEx5cmljLlJlZGlzIHRlc3QgZml4\ndHVyZTEnMCUGA1UEAwwedGVzdCBmaXh0dXJlIENBIChub3QgYSBzZWNyZXQpMFkw\nEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEOgXwgst6N+0k44HeUyvYfLO6phc3xlGA\n3yd6B5bS2m5WohZcMc/XoDtFPNKo8ABfa0nv9P6pWtT03A+g31l/PqNjMGEwHQYD\nVR0OBBYEFJoTVciJsCCJgLlu0Z3bn0BvQXfFMB8GA1UdIwQYMBaAFJoTVciJsCCJ\ngLlu0Z3bn0BvQXfFMA8GA1UdEwEB/wQFMAMBAf8wDgYDVR0PAQH/BAQDAgEGMAoG\nCCqGSM49BAMCA0kAMEYCIQDepJlbChF3EJR4ophs6EadOiIfbiNE/nEY/FUmYKrb\nNgIhAKzlFopWBf/sJ18grKOZ2NYGVYWICajfoWC/KhsdN+xy\n-----END CERTIFICATE-----\n"

// The fake server's certificate: SAN `localhost` and `127.0.0.1`.
val serverCertPem: String = "-----BEGIN CERTIFICATE-----\nMIICFjCCAb2gAwIBAgIULVFmbTDKmIn/CnDN0q6A4NUoflwwCgYIKoZIzj0EAwIw\nTDEhMB8GA1UECgwYTHlyaWMuUmVkaXMgdGVzdCBmaXh0dXJlMScwJQYDVQQDDB50\nZXN0IGZpeHR1cmUgQ0EgKG5vdCBhIHNlY3JldCkwIBcNMjYxMTEyMjA0MzMyWhgP\nMjEyNjEwMTkyMDQzMzJaMDcxITAfBgNVBAoMGEx5cmljLlJlZGlzIHRlc3QgZml4\ndHVyZTESMBAGA1UEAwwJbG9jYWxob3N0MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcD\nQgAEsju6Yqcgs4wqbIyuDVFrxHg2hIhn+Gnr3NERGns0cTIdH9/e/HSMjVHVj2M6\nQtOrmDu+DUiS3mO3dK18CT5pEaOBjzCBjDAJBgNVHRMEAjAAMA4GA1UdDwEB/wQE\nAwIHgDAaBgNVHREEEzARgglsb2NhbGhvc3SHBH8AAAEwEwYDVR0lBAwwCgYIKwYB\nBQUHAwEwHQYDVR0OBBYEFMBUfdPpc3g1ae04RuQI5X/7/6TWMB8GA1UdIwQYMBaA\nFJoTVciJsCCJgLlu0Z3bn0BvQXfFMAoGCCqGSM49BAMCA0cAMEQCIHLD3H2/Rm4h\ndMPqbgLUkWraf5Px9YQ+VEsvK3LvJvDRAiBnL/3eAfr/i4Dlsy+Q37CtMCFx6igx\n0rpVJDGPyqOUig==\n-----END CERTIFICATE-----\n"

val serverKeyPem: String = "-----BEGIN PRIVATE KEY-----\nMIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQg0XxBx94m+bTOBj2U\nc172oqWEQ65Y6vPC6cBnR9HbjRWhRANCAASyO7pipyCzjCpsjK4NUWvEeDaEiGf4\naevc0REaezRxMh0f3978dIyNUdWPYzpC06uYO74NSJLeY7d0rXwJPmkR\n-----END PRIVATE KEY-----\n"

// A client certificate signed by the CA.
val clientCertPem: String = "-----BEGIN CERTIFICATE-----\nMIICBzCCAa2gAwIBAgIULVFmbTDKmIn/CnDN0q6A4NUofl0wCgYIKoZIzj0EAwIw\nTDEhMB8GA1UECgwYTHlyaWMuUmVkaXMgdGVzdCBmaXh0dXJlMScwJQYDVQQDDB50\nZXN0IGZpeHR1cmUgQ0EgKG5vdCBhIHNlY3JldCkwIBcNMjYxMTEyMjA0MzMyWhgP\nMjEyNjEwMTkyMDQzMzJaMEUxITAfBgNVBAoMGEx5cmljLlJlZGlzIHRlc3QgZml4\ndHVyZTEgMB4GA1UEAwwXbHlyaWMtcmVkaXMtdGVzdC1jbGllbnQwWTATBgcqhkjO\nPQIBBggqhkjOPQMBBwNCAAR4oVdze7HnihaUq/Sf4I6Ddncix0NeQS+k7xZC+Lg7\nUnM7vzFyqYdNb2cUQfv6egidJX0sdVI5ZasWSzj3e/Pbo3IwcDAJBgNVHRMEAjAA\nMA4GA1UdDwEB/wQEAwIHgDATBgNVHSUEDDAKBggrBgEFBQcDAjAdBgNVHQ4EFgQU\n+wwkziHaFDb/VeR3BB2JOMlr854wHwYDVR0jBBgwFoAUmhNVyImwIImAuW7Rnduf\nQG9Bd8UwCgYIKoZIzj0EAwIDSAAwRQIhAOFkq0YWRsMGpZlbMdnuiA6FuTem4Q9i\n5rKzxr6NpZwQAiB14a10p5it684BpqhtzlaaLlhj+2IfFyTaw6pB0BEfKA==\n-----END CERTIFICATE-----\n"

val clientKeyPem: String = "-----BEGIN PRIVATE KEY-----\nMIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgPx2HVYJy+Z5v2EK/\npwiyyVKaq+4w02AbVQe0P4E1QaShRANCAAR4oVdze7HnihaUq/Sf4I6Ddncix0Ne\nQS+k7xZC+Lg7UnM7vzFyqYdNb2cUQfv6egidJX0sdVI5ZasWSzj3e/Pb\n-----END PRIVATE KEY-----\n"

// The client key again as a SEC1 block, a format `Std.Tls` does not load.
val clientSec1KeyPem: String = "-----BEGIN EC PRIVATE KEY-----\nMHcCAQEEID8dh1WCcvmeb9hCv6cIsslSmqvuMNNgG1UHtD+BNUGkoAoGCCqGSM49\nAwEHoUQDQgAEeKFXc3ux54oWlKv0n+COg3Z3IsdDXkEvpO8WQvi4O1JzO78xcqmH\nTW9nFEH7+noInSV9LHVSOWWrFks493vz2w==\n-----END EC PRIVATE KEY-----\n"

// A second CA that signed neither side.
val otherCaPem: String = "-----BEGIN CERTIFICATE-----\nMIICCzCCAbGgAwIBAgIUSbvog3B7ySjXPYCC9uCF9D3FOMgwCgYIKoZIzj0EAwIw\nUjEhMB8GA1UECgwYTHlyaWMuUmVkaXMgdGVzdCBmaXh0dXJlMS0wKwYDVQQDDCR0\nZXN0IGZpeHR1cmUgb3RoZXIgQ0EgKG5vdCBhIHNlY3JldCkwIBcNMjYxMTEyMjA0\nMzMyWhgPMjEyNjEwMTkyMDQzMzJaMFIxITAfBgNVBAoMGEx5cmljLlJlZGlzIHRl\nc3QgZml4dHVyZTEtMCsGA1UEAwwkdGVzdCBmaXh0dXJlIG90aGVyIENBIChub3Qg\nYSBzZWNyZXQpMFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEJgDKmA7sR32z/AaM\njQ6O7JyLwVFA1Oxj+ogUQ7CSlqtGBipYKCkb6265mFMvwdaEWl5LdSMZXw1jesUq\noo/Xb6NjMGEwHQYDVR0OBBYEFPhYszoBBK+kfgdQIc2pqB4oFDShMB8GA1UdIwQY\nMBaAFPhYszoBBK+kfgdQIc2pqB4oFDShMA8GA1UdEwEB/wQFMAMBAf8wDgYDVR0P\nAQH/BAQDAgEGMAoGCCqGSM49BAMCA0gAMEUCIQCH4/eeRDiw0uwkI2zia5bIzGt+\nqe8u++BjI2FGnxHTEAIgTz0HPXV6cP/ZFll8luiaAXjUOjqD6jZKndAiVx8PdjQ=\n-----END CERTIFICATE-----\n"

// A client certificate signed by the second CA.
val strangerCertPem: String = "-----BEGIN CERTIFICATE-----\nMIICDjCCAbWgAwIBAgIUBXm5vDBXg+szVU+C2Lan6vNJRuIwCgYIKoZIzj0EAwIw\nUjEhMB8GA1UECgwYTHlyaWMuUmVkaXMgdGVzdCBmaXh0dXJlMS0wKwYDVQQDDCR0\nZXN0IGZpeHR1cmUgb3RoZXIgQ0EgKG5vdCBhIHNlY3JldCkwIBcNMjYxMTEyMjA0\nMzMyWhgPMjEyNjEwMTkyMDQzMzJaMEcxITAfBgNVBAoMGEx5cmljLlJlZGlzIHRl\nc3QgZml4dHVyZTEiMCAGA1UEAwwZbHlyaWMtcmVkaXMtdGVzdC1zdHJhbmdlcjBZ\nMBMGByqGSM49AgEGCCqGSM49AwEHA0IABAzFJ9WDcIpeQqiiovxWz1JS4qRzXNU2\nlGMfyzyzJxY3V1bZCk8REpvFKLZNa+3bR85+ave987pZ80Fy0/U02Q+jcjBwMAkG\nA1UdEwQCMAAwDgYDVR0PAQH/BAQDAgeAMBMGA1UdJQQMMAoGCCsGAQUFBwMCMB0G\nA1UdDgQWBBTS6eFTb65l+PHWYk9f/MrsnVV6qTAfBgNVHSMEGDAWgBT4WLM6AQSv\npH4HUCHNqageKBQ0oTAKBggqhkjOPQQDAgNHADBEAiBBBNNE/UgrFNCd3KmccsUf\nXOV47ZZN4PsJhT/NSLW3HwIgPUcjDQYQGoUjh9MJoesm8puabg3qyIgJvCvyTAoO\nclM=\n-----END CERTIFICATE-----\n"

val strangerKeyPem: String = "-----BEGIN PRIVATE KEY-----\nMIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgxO0oUlYJARpINAAg\np/0yBEuft8bFQyWge9QjZ/KAhiehRANCAAQMxSfVg3CKXkKooqL8Vs9SUuKkc1zV\nNpRjH8s8sycWN1dW2QpPERKbxSi2TWvt20fOfmr3vfO6WfNBctP1NNkP\n-----END PRIVATE KEY-----\n"

// ─── The fake TLS server ─────────────────────────────────────────────────────
union Action {
  /// Write `reply` and keep the connection.
  case Send(reply: String)
  /// Write `reply`, then close the connection.
  case SendThenClose(reply: String)
  /// Write nothing for `millis` milliseconds, then close the connection.
  case Hang(millis: Int)
}

record Fake {
  port: Int
  log: List[String]
  worker: Kernel.Background
  /// A configuration the fake accepts, used to stop it.
  trusted: RedisConfig
}

func commandWords(v: in RespValue): List[String] {
  val words: List[String] = newList()
  match v {
    case RArray(items) -> {
      for item in items {
        match asText(item) {
          case Some(t) -> words.add(t)
          case None -> words.add("<binary>")
        }
      }
    }
    case _ -> words.add("<not-a-command>")
  }
  words
}

func writeWire(sock: in Kernel.Sock, text: in String): Unit {
  val _ = Kernel.sockWrite(sock, encodeUtf8(text))
}

// Serve one connection. False once FAKE.STOP was received. A client whose
// handshake fails shows up here as a failed first read on the JVM.
func serveConnection(
  sock: in Kernel.Sock,
  index: in Int,
  handler: in (List[String], Int) -> Action,
  log: in List[String],
): Bool {
  var decoder = newDecoder()
  var open = true
  var keepServing = true
  while open {
    match Kernel.sockRead(sock, 4096) {
      case Err(_) -> {
        open = false
      }
      case Ok(chunk) -> {
        if chunk.length == 0 {
          open = false
        } else {
          val fed = feed(decoder, chunk)
          decoder = fed.decoder
          for v in fed.values {
            if open {
              val words = commandWords(v)
              if words.count > 0 and words[0] == "FAKE.STOP" {
                writeWire(sock, "+OK\r\n")
                keepServing = false
                open = false
              } else {
                log.add(Str.joinList(" ", words))
                match handler(words, index) {
                  case Send(reply) -> writeWire(sock, reply)
                  case SendThenClose(reply) -> {
                    writeWire(sock, reply)
                    open = false
                  }
                  case Hang(millis) -> {
                    sleepMillis(millis)
                    open = false
                  }
                }
              }
            }
          }
        }
      }
    }
  }
  Kernel.sockClose(sock)
  keepServing
}

// `index` counts the connections whose handshake succeeded. A failed
// handshake is a failed accept on dotnet; the server keeps listening, up to a
// bound that only a broken listener reaches.
func serve(server: in Kernel.ServerSock, handler: in (List[String], Int) -> Action, log: in List[String]): Unit {
  var running = true
  var index = 0
  var refused = 0
  while running {
    match Kernel.serverAccept(server) {
      case Err(_) -> {
        refused = refused + 1
        running = refused < 100
      }
      case Ok(sock) -> {
        val before = log.count
        running = serveConnection(sock, index, handler, log)
        if log.count > before {
          index = index + 1
        }
      }
    }
  }
  Kernel.serverClose(server)
}

func trustedConfig(port: in Int): RedisConfig {
  Redis.withExclusiveCaCertificate(Redis.withTls(Redis.configFor("127.0.0.1", port)), caPem)
}

func startFakeWith(requireClientCert: in Bool, handler: in (List[String], Int) -> Action): Fake {
  val clientCa = if requireClientCert { caPem } else { "" }
  match Kernel.serverListenTls("127.0.0.1", 0, serverCertPem, serverKeyPem, clientCa, requireClientCert) {
    case Err(m) -> panic("the fake TLS server cannot listen: " + m)
    case Ok(server) -> {
      val log: List[String] = newList()
      val port = Kernel.serverPort(server)
      val worker = Kernel.startBackground({ -> serve(server, handler, log) })
      val base = trustedConfig(port)
      val trusted = if requireClientCert { Redis.withClientIdentity(base, clientCertPem, clientKeyPem) } else { base }
      Fake(port = port, log = log, worker = worker, trusted = trusted)
    }
  }
}

func stopFake(fake: in Fake): Unit {
  val stopper = Redis.lazy(fake.trusted)
  match Redis.command(stopper, ["FAKE.STOP"]) {
    case Ok(_) -> ()
    case Err(e) -> panic("the fake TLS server did not take FAKE.STOP: " + e.message())
  }
  stopper.close()
  Kernel.waitBackground(fake.worker)
}

func bulk(text: in String): String {
  "$" + encodeUtf8(text).length.toString() + "\r\n" + text + "\r\n"
}

// PING, SET, GET, SELECT, and AUTH against the password "sesame".
func miniHandle(store: in Map[String, String], words: in List[String]): Action {
  val name = words[0]
  if name == "PING" {
    return Send(reply = "+PONG\r\n")
  }
  if name == "AUTH" {
    if words[words.count - 1] == "sesame" {
      return Send(reply = "+OK\r\n")
    }
    return Send(reply = "-WRONGPASS invalid username-password pair or user is disabled.\r\n")
  }
  if name == "SELECT" {
    return Send(reply = "+OK\r\n")
  }
  if name == "SET" {
    store[words[1]] = words[2]
    return Send(reply = "+OK\r\n")
  }
  if name == "GET" {
    if store.containsKey(words[1]) {
      return Send(reply = bulk(store[words[1]]))
    }
    return Send(reply = "$-1\r\n")
  }
  Send(reply = "-ERR unknown command '" + name + "'\r\n")
}

func startMini(requireClientCert: in Bool): Fake {
  val store: Map[String, String] = newMap()
  startFakeWith(requireClientCert, { words, conn -> miniHandle(store, words) })
}

func ok[T](r: in Result[T, RedisError], what: in String): T {
  match r {
    case Ok(v) -> v
    case Err(e) -> panic(what + " failed: " + e.message())
  }
}

func codeOf[T](r: in Result[T, RedisError], what: in String): String {
  match r {
    case Ok(_) -> {
      assertTrue(false, what + " should have failed")
      ""
    }
    case Err(e) -> e.code()
  }
}

func messageOf[T](r: in Result[T, RedisError], what: in String): String {
  match r {
    case Ok(_) -> {
      assertTrue(false, what + " should have failed")
      ""
    }
    case Err(e) -> e.message()
  }
}

// The outcome of opening a client and sending PING: "" on success, else the
// error code of whichever step failed. A server that refuses a client only
// after the handshake (a missing client certificate under TLS 1.3) fails the
// PING, not the open.
func pingOutcome(cfg: in RedisConfig): String {
  match Redis.open(cfg) {
    case Err(e) -> e.code()
    case Ok(c) -> {
      val outcome = match Redis.ping(c) { case Ok(_) -> ""; case Err(e) -> e.code() }
      c.close()
      outcome
    }
  }
}

func tlsOn(port: in Int): RedisConfig {
  Redis.withTls(Redis.configFor("127.0.0.1", port))
}

// ─── Trust ───────────────────────────────────────────────────────────────────
test "a TLS client verifies the server against the configured CA and round-trips commands" {
  val fake = startMini(false)
  val c = ok(Redis.open(trustedConfig(fake.port)), "open over TLS")
  ok(Redis.ping(c), "PING")
  ok(Redis.set(c, "k", "café\r\nline"), "SET")
  match ok(Redis.get(c, "k"), "GET") {
    case Some(v) -> assertEqual(v, "café\r\nline", "the value came back through TLS")
    case None -> assertTrue(false, "the key was set")
  }
  assertEqual(codeOf(Redis.command(c, ["NOSUCH"]), "an unknown command"), "SERVER_ERROR", "error reply")
  assertEqualInt(c.connections(), 1, "one connection")
  c.close()
  stopFake(fake)
  assertEqual(fake.log[0], "PING", "the first command the server decrypted")
}

test "a rediss URL with the CA connects, and AUTH and SELECT run inside TLS" {
  val fake = startMini(false)
  val url = "rediss://:sesame@127.0.0.1:" + fake.port.toString() + "/3"
  val cfg = Redis.withExclusiveCaCertificate(ok(Redis.configFromUrl(url), "the URL"), caPem)
  assertTrue(cfg.tls.enabled, "rediss:// switches TLS on")
  val c = ok(Redis.open(cfg), "open")
  ok(Redis.ping(c), "PING")
  c.close()
  val wrong = Redis.withCredentials(cfg, "", "wrong")
  assertEqual(codeOf(Redis.open(wrong), "a wrong password"), "AUTH_FAILED", "AUTH is answered through TLS")
  stopFake(fake)
  assertEqual(fake.log[0], "AUTH sesame", "AUTH first")
  assertEqual(fake.log[1], "SELECT 3", "then SELECT")
  assertEqual(fake.log[2], "PING", "then the command")
}

test "without the CA the server certificate is not trusted" {
  val fake = startMini(false)
  val url = "rediss://127.0.0.1:" + fake.port.toString()
  assertEqual(codeOf(Redis.connect(url), "the system trust store"), "TLS_FAILED", "an unknown CA is refused")
  val lazyClient = Redis.lazy(tlsOn(fake.port))
  assertEqual(codeOf(Redis.ping(lazyClient), "PING"), "TLS_FAILED", "a lazy client reports it on the command")
  assertEqual(codeOf(Redis.ping(lazyClient), "PING again"), "TLS_FAILED", "and again, as a value")
  assertFalse(lazyClient.isConnected(), "no connection was kept")
  val m = messageOf(Redis.open(tlsOn(fake.port)), "open")
  assertTrue(Str.contains(m, "127.0.0.1:" + fake.port.toString()), "the message names the endpoint")
  lazyClient.close()
  stopFake(fake)
  assertEqualInt(fake.log.count, 0, "no command reached the server")
}

test "another CA does not vouch for the server" {
  val fake = startMini(false)
  val cfg = Redis.withExclusiveCaCertificate(tlsOn(fake.port), otherCaPem)
  assertEqual(codeOf(Redis.open(cfg), "the wrong CA"), "TLS_FAILED", "the chain does not end in the configured CA")
  stopFake(fake)
}

// ─── Server name ─────────────────────────────────────────────────────────────
test "the certificate must carry the server name" {
  val fake = startMini(false)
  val byAddress = trustedConfig(fake.port)
  assertEqual(pingOutcome(byAddress), "", "127.0.0.1 is in the certificate")
  assertEqual(pingOutcome(Redis.withTlsServerName(byAddress, "localhost")), "", "localhost is in the certificate")
  val other = Redis.withTlsServerName(byAddress, "redis.example.invalid")
  assertEqual(codeOf(Redis.open(other), "another name"), "TLS_FAILED", "a name the certificate lacks is refused")
  stopFake(fake)
}

// ─── Mutual TLS ──────────────────────────────────────────────────────────────
test "mutual TLS: the client certificate is presented and required" {
  val fake = startMini(true)
  val base = trustedConfig(fake.port)
  assertEqual(pingOutcome(Redis.withClientIdentity(base, clientCertPem, clientKeyPem)), "", "with the certificate")
  val without = pingOutcome(base)
  assertTrue(without == "TLS_FAILED" or without == "CONNECTION_LOST", "without a certificate: " + without)
  val stranger = pingOutcome(Redis.withClientIdentity(base, strangerCertPem, strangerKeyPem))
  assertTrue(stranger == "TLS_FAILED" or stranger == "CONNECTION_LOST", "a certificate of another CA: " + stranger)
  stopFake(fake)
  assertEqualInt(fake.log.count, 1, "only the trusted client's PING was served")
}

// ─── The insecure policy ─────────────────────────────────────────────────────
test "insecure skip verify needs both keys" {
  val fake = startMini(false)
  val insecure = Redis.withTlsServerName(Redis.withInsecureSkipVerify(tlsOn(fake.port)), "redis.example.invalid")
  Env.setVar("LYRIC_TLS_ALLOW_INSECURE", "")
  assertEqual(codeOf(Redis.open(insecure), "the code key alone"), "TLS_FAILED", "verification stays on")
  Env.setVar("LYRIC_TLS_ALLOW_INSECURE", "1")
  assertEqual(
    codeOf(Redis.open(tlsOn(fake.port)), "the variable alone"),
    "TLS_FAILED",
    "the variable alone does nothing"
  )
  assertEqual(pingOutcome(insecure), "", "both keys: neither the chain nor the name is checked")
  Env.setVar("LYRIC_TLS_ALLOW_INSECURE", "true")
  assertEqual(codeOf(Redis.open(insecure), "a value other than 1"), "TLS_FAILED", "only the value 1 counts")
  Env.setVar("LYRIC_TLS_ALLOW_INSECURE", "")
  stopFake(fake)
}

test "the insecure policy is the standard library's matrix" {
  val plain = tlsOn(6379)
  val requested = Redis.withInsecureSkipVerify(plain)
  assertTrue(Redis.insecurePolicyFor(plain, false) == Http.VerificationEnabled, "no, no")
  assertTrue(Redis.insecurePolicyFor(plain, true) == Http.VerificationEnabled, "the variable alone")
  assertTrue(Redis.insecurePolicyFor(requested, false) == Http.VerificationEnabledWarn, "the code key alone")
  assertTrue(Redis.insecurePolicyFor(requested, true) == Http.VerificationDisabled, "both")
}

// ─── Versions, reconnects, timeouts ──────────────────────────────────────────
test "TLS 1.3 can be required" {
  val fake = startMini(false)
  assertEqual(pingOutcome(Redis.withMinTlsVersion(trustedConfig(fake.port), Tls.Tls13)), "", "TLS 1.3 only")
  assertEqual(pingOutcome(Redis.withMinTlsVersion(trustedConfig(fake.port), Tls.Tls12)), "", "TLS 1.2 or later")
  stopFake(fake)
}

test "a connection the server closed is replaced by a new TLS connection" {
  val fake = startFakeWith(
    false,
    { words, conn -> if conn == 0 { SendThenClose(reply = "+PONG\r\n") } else { Send(reply = "+PONG\r\n") } }
  )
  val c = ok(Redis.open(trustedConfig(fake.port)), "open")
  ok(Redis.ping(c), "PING on the first connection")
  sleepMillis(200)
  ok(Redis.ping(c), "PING after the server closed the connection")
  assertEqualInt(c.connections(), 2, "a second handshake happened")
  c.disconnect()
  ok(Redis.ping(c), "PING after disconnect")
  assertEqualInt(c.connections(), 3, "disconnect, then a third handshake")
  c.close()
  assertEqual(codeOf(Redis.ping(c), "PING on a closed client"), "CLIENT_CLOSED", "closed")
  stopFake(fake)
}

test "a silent TLS server trips the I/O timeout" {
  val fake = startFakeWith(
    false,
    { words, conn -> if conn == 0 { Hang(millis = 1500) } else { Send(reply = "+PONG\r\n") } }
  )
  val c = ok(Redis.open(Redis.withTimeouts(trustedConfig(fake.port), 2000, 300)), "open")
  val started = nowEpochMillis()
  assertEqual(codeOf(Redis.ping(c), "PING with no answer"), "CONNECTION_LOST", "timed out")
  assertTrue(nowEpochMillis() - started < 1300i64, "the timeout fired before the server gave up")
  assertFalse(c.isConnected(), "the timed-out connection was dropped")
  c.close()
  stopFake(fake)
}

test "a TLS client on a plain port fails the handshake within the I/O timeout" {
  match Kernel.serverListen("127.0.0.1", 0) {
    case Err(m) -> panic("cannot listen: " + m)
    case Ok(server) -> {
      val port = Kernel.serverPort(server)
      // Accepts one connection and never answers: a server that is not TLS.
      val worker = Kernel.startBackground(
        { ->
          match Kernel.serverAccept(server) {
            case Ok(sock) -> {
              sleepMillis(1500)
              Kernel.sockClose(sock)
            }
            case Err(_) -> ()
          }
          Kernel.serverClose(server)
        }
      )
      val started = nowEpochMillis()
      val cfg = Redis.withTimeouts(trustedConfig(port), 2000, 300)
      assertEqual(codeOf(Redis.open(cfg), "TLS to a plain server"), "TLS_FAILED", "no handshake")
      assertTrue(nowEpochMillis() - started < 1300i64, "the handshake is bounded by the I/O timeout")
      Kernel.waitBackground(worker)
    }
  }
}

test "a plain client on a TLS port gets an error, not a reply" {
  val fake = startMini(false)
  val plain = Redis.withTimeouts(Redis.configFor("127.0.0.1", fake.port), 2000, 1000)
  val outcome = pingOutcome(plain)
  assertTrue(outcome.length > 0, "a plain PING is not answered by a TLS server")
  stopFake(fake)
  assertEqualInt(fake.log.count, 0, "the server decoded no command")
}

// ─── Configuration checks (no server) ────────────────────────────────────────
test "the URL scheme chooses TLS" {
  assertTrue(ok(Redis.configFromUrl("rediss://cache.internal:6380/1"), "rediss").tls.enabled, "rediss:// is TLS")
  assertFalse(ok(Redis.configFromUrl("redis://cache.internal"), "redis").tls.enabled, "redis:// is not")
  assertFalse(Redis.configFor("cache.internal", 6379).tls.enabled, "configFor is not")
  val on = Redis.withTls(Redis.configFor("cache.internal", 6379))
  assertTrue(on.tls.enabled, "withTls")
  assertEqual(on.tls.serverName, "", "the server name defaults to the host")
  ok(Redis.checkTls(on), "TLS with the defaults")
  ok(Redis.checkTls(Redis.configFor("cache.internal", 6379)), "no TLS at all")
}

test "TLS options on a plain connection are InvalidConfig" {
  val plain = Redis.configFor("127.0.0.1", 6379)
  val withCa = Redis.withExclusiveCaCertificate(plain, caPem)
  assertEqual(codeOf(Redis.checkTls(withCa), "a CA without TLS"), "INVALID_CONFIG", "CA")
  assertEqual(codeOf(Redis.open(withCa), "open"), "INVALID_CONFIG", "open refuses before dialling")
  assertEqual(codeOf(Redis.ping(Redis.lazy(withCa)), "a lazy client"), "INVALID_CONFIG", "a lazy client refuses")
  assertTrue(Str.contains(messageOf(Redis.checkTls(withCa), "the message"), "rediss://"), "the message says how")
  val identity = Redis.withClientIdentity(plain, clientCertPem, clientKeyPem)
  assertEqual(codeOf(Redis.checkTls(identity), "an identity without TLS"), "INVALID_CONFIG", "identity")
  assertEqual(
    codeOf(Redis.checkTls(Redis.withInsecureSkipVerify(plain)), "skip verify without TLS"),
    "INVALID_CONFIG",
    "insecure"
  )
  assertEqual(
    codeOf(Redis.checkTls(Redis.withTlsServerName(plain, "x")), "a server name without TLS"),
    "INVALID_CONFIG",
    "server name"
  )
  assertEqual(
    codeOf(Redis.checkTls(Redis.withMinTlsVersion(plain, Tls.Tls13)), "a version without TLS"),
    "INVALID_CONFIG",
    "version"
  )
  ok(Redis.checkTls(Redis.withTls(withCa)), "the same options with TLS on")
}

test "unusable certificates and keys are InvalidConfig" {
  val tls = Redis.withTls(Redis.configFor("127.0.0.1", 6379))
  val badCa = Redis.withExclusiveCaCertificate(tls, "not a certificate")
  assertEqual(codeOf(Redis.checkTls(badCa), "a CA that is not PEM"), "INVALID_CONFIG", "CA")
  assertEqual(codeOf(Redis.open(badCa), "open"), "INVALID_CONFIG", "open refuses before dialling")
  val sec1 = Redis.withClientIdentity(tls, clientCertPem, clientSec1KeyPem)
  val sec1Message = messageOf(Redis.checkTls(sec1), "a SEC1 key")
  assertTrue(Str.contains(sec1Message, "PKCS8"), "the message names the supported key format: " + sec1Message)
  assertFalse(Str.contains(sec1Message, "BEGIN"), "the message does not repeat the key")
  val swapped = Redis.withClientIdentity(tls, clientKeyPem, clientCertPem)
  assertEqual(codeOf(Redis.checkTls(swapped), "certificate and key swapped"), "INVALID_CONFIG", "swapped")
  val noCert = Redis.withClientIdentity(tls, "not a certificate", clientKeyPem)
  assertEqual(codeOf(Redis.checkTls(noCert), "a certificate that is not PEM"), "INVALID_CONFIG", "certificate")
  ok(
    Redis.checkTls(Redis.withClientIdentity(Redis.withExclusiveCaCertificate(tls, caPem), clientCertPem, clientKeyPem)),
    "good"
  )
}

test "certificates and keys can be read from files" {
  val dir = Env.getVarOrDefault("TMPDIR", "/tmp")
  val stem = dir + "/lyric-redis-tls-" + uuidToString(newUuid())
  val caPath = stem + "-ca.pem"
  val certPath = stem + "-cert.pem"
  val keyPath = stem + "-key.pem"
  match File.writeText(caPath, caPem) {
    case Ok(_) -> ()
    case Err(_) -> panic("cannot write " + caPath)
  }
  match File.writeText(certPath, clientCertPem) {
    case Ok(_) -> ()
    case Err(_) -> panic("cannot write " + certPath)
  }
  match File.writeText(keyPath, clientKeyPem) {
    case Ok(_) -> ()
    case Err(_) -> panic("cannot write " + keyPath)
  }
  val tls = Redis.withTls(Redis.configFor("127.0.0.1", 6379))
  val withCa = ok(Redis.withExclusiveCaCertificateFile(tls, caPath), "the CA file")
  assertEqual(withCa.tls.caCertificatePem, caPem, "the CA text")
  val full = ok(Redis.withClientIdentityFiles(withCa, certPath, keyPath), "the identity files")
  assertEqual(full.tls.clientCertificatePem, clientCertPem, "the certificate text")
  assertEqual(full.tls.clientKeyPem, clientKeyPem, "the key text")
  ok(Redis.checkTls(full), "the files hold a usable configuration")
  val missing = stem + "-missing.pem"
  assertEqual(codeOf(Redis.withExclusiveCaCertificateFile(tls, missing), "a missing CA file"), "INVALID_CONFIG", "CA")
  assertEqual(
    codeOf(Redis.withClientIdentityFiles(tls, certPath, missing), "a missing key file"),
    "INVALID_CONFIG",
    "key"
  )
  assertEqual(
    codeOf(Redis.withClientIdentityFiles(tls, missing, keyPath), "a missing certificate file"),
    "INVALID_CONFIG",
    "certificate"
  )
  assertEqual(codeOf(Redis.withExclusiveCaCertificateFile(tls, ""), "an empty path"), "INVALID_CONFIG", "empty path")
  val _ = File.deleteFile(caPath)
  val _ = File.deleteFile(certPath)
  val _ = File.deleteFile(keyPath)
}
