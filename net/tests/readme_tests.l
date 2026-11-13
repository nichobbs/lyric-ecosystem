//! tests/readme_tests.l - the two examples of the README, as written there,
//! compiled and run: a request and its reply over TLS against a loopback
//! server, and a connection URL.
@test_module
package Net.ReadmeTests

import Std.Core
import Std.Testing
import Std.Encoding
import Net.Tcp as Tcp
import Net.Tcp.Testing as KT
import Net.Tls
import Net.Url as Url

// ─── README: a request and its reply over TLS ────────────────────────────────
func askOverTls(host: in String, port: in Int, caPem: in String, request: in String): Result[String, String] {
  val tls = plainTcp().withTls().withCaCertificate(caPem)
  match validateTlsOptions(tls) {
    case Ok(_) -> ()
    case Err(problem) -> return Err(tlsProblemText(problem, "a tcps:// URL"))
  }
  val plain = Tcp.sockConnect(host, port, 10000)?
  val sock = Tcp.sockStartTls(plain, tls, host, skipsVerification(tls), 10000)?
  val answer = match Tcp.sockWriteWithin(sock, encodeUtf8(request), 5000) {
    case Err(e) -> Err(e)
    case Ok(_) -> match Tcp.sockRead(sock, 4096, 5000) {
      case Ok(Data(bytes)) -> match tryDecodeUtf8(bytes) {
        case Some(text) -> Ok(text)
        case None -> Err("the reply is not UTF-8")
      }
      case Ok(TimedOut) -> Err("no reply within five seconds")
      case Ok(Eof) -> Err("the server closed the connection")
      case Err(e) -> Err(e)
    }
  }
  Tcp.sockClose(sock)
  answer
}

// ─── README: a connection URL ────────────────────────────────────────────────
record Endpoint {
  host: String
  port: Int
  user: String
  password: String
  tls: Bool
}

func schemes(): slice[Url.Scheme] {
  [Url.Scheme(name = "tcp", defaultPort = 9000, tls = false), Url.Scheme(name = "tcps", defaultPort = 9443, tls = true)]
}

func parseEndpoint(url: in String): Result[Endpoint, String] {
  val found = match Url.matchScheme(url, schemes(), true) {
    case Ok(m) -> m
    case Err(problem) -> return Err(Url.schemeProblemText(problem, schemes()))
  }
  val userInfo = match Url.splitUserInfo(found.rest) {
    case Ok(u) -> u
    case Err(problem) -> return Err(Url.authorityProblemText(problem, "user", "password", found.scheme))
  }
  match Url.parseHostPort(userInfo.hostPort, found.scheme.defaultPort) {
    case Ok(address) -> Ok(
      Endpoint(
        host = address.host,
        port = address.port,
        user = userInfo.user,
        password = userInfo.password,
        tls = found.scheme.tls
      )
    )
    case Err(problem) -> Err(Url.authorityProblemText(problem, "user", "password", found.scheme))
  }
}

// ─── The tests ───────────────────────────────────────────────────────────────
func upperEchoOnce(server: in KT.ServerSock): Unit {
  val accepted = match KT.serverAccept(server) { case Ok(s) -> s; case Err(_) -> return }
  val tls = KT.ServerTls(certPem = TEST_ONLY_SERVER_CERT, keyPem = TEST_ONLY_SERVER_KEY, clientCaPem = "")
  match KT.sockAcceptTls(accepted, tls, 5000) {
    case Err(_) -> ()
    case Ok(peer) -> {
      match Tcp.sockRead(peer, 1024, 3000) {
        case Ok(Data(bytes)) -> {
          val _ = Tcp.sockWrite(peer, encodeUtf8("re: "))
          val _ = Tcp.sockWrite(peer, bytes)
        }
        case _ -> ()
      }
      Tcp.sockClose(peer)
    }
  }
}

test "the TLS example asks and gets its answer" {
  val server = match KT.serverListen("127.0.0.1", 0) { case Ok(s) -> s; case Err(e) -> panic(e) }
  val peer = KT.startBackground({ -> upperEchoOnce(server) })
  match askOverTls("localhost", KT.serverPort(server), TEST_ONLY_CA, "ping") {
    case Ok(answer) -> assertTrue(answer == "re: " or answer == "re: ping", "the reply, or its first piece: " + answer)
    case Err(e) -> assertTrue(false, e)
  }
  KT.waitBackground(peer)
  KT.serverClose(server)
  match askOverTls("localhost", 1, "not a certificate", "ping") {
    case Ok(_) -> assertTrue(false, "an unusable CA")
    case Err(e) -> assertTrue(e.startsWith("TLS CA certificate:"), e)
  }
}

test "the URL example parses an endpoint" {
  match parseEndpoint("tcps://app:s%40cret@[::1]") {
    case Err(e) -> assertTrue(false, e)
    case Ok(endpoint) -> {
      assertEqual(endpoint.host, "::1", "host")
      assertEqualInt(endpoint.port, 9443, "the scheme's default port")
      assertEqual(endpoint.user, "app", "user")
      assertEqual(endpoint.password, "s@cret", "password")
      assertTrue(endpoint.tls, "tcps is TLS")
    }
  }
  match parseEndpoint("http://host") {
    case Ok(_) -> assertTrue(false, "another scheme")
    case Err(e) -> assertEqual(e, "unsupported scheme 'http', expected tcp or tcps", "the scheme problem")
  }
  match parseEndpoint("tcp://::1") {
    case Ok(_) -> assertTrue(false, "an IPv6 literal without brackets")
    case Err(e) -> assertEqual(e, "an IPv6 address must be written in brackets, as in tcp://[::1]:9000", "the hint")
  }
}

// ─── TEST-ONLY certificates and keys ─────────────────────────────────────────
// Generated for hermetic tests with `openssl` (EC P-256, valid until 2126);
// the same fixtures as in tests/tcp_tests.l. The private key is public by
// being here: never trust these certificates outside a test.
// test-only
val TEST_ONLY_CA: String = "-----BEGIN CERTIFICATE-----\n" + "MIIB4TCCAYegAwIBAgIUMdZcqE1iGtInxR0FPbqibzyXeHYwCgYIKoZIzj0EAwIw\n" + "PTEkMCIGA1UECgwbbHlyaWMtbXEtYWN0aXZlbXEgVEVTVCBPTkxZMRUwEwYDVQQD\n" + "DAx0ZXN0LW9ubHkgQ0EwIBcNMjYxMTEyMjA0MzQyWhgPMjEyNjEwMTkyMDQzNDJa\n" + "MD0xJDAiBgNVBAoMG2x5cmljLW1xLWFjdGl2ZW1xIFRFU1QgT05MWTEVMBMGA1UE\n" + "AwwMdGVzdC1vbmx5IENBMFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEYAYCGXrA\n" + "I2ewAr+wvyoRAfbXD36+mTug7dD9P79g9FPZ4pb/G1LPreTm3JJpvL0rPMcjyZ6i\n" + "atNgUShoEQtM1KNjMGEwHQYDVR0OBBYEFBO96Lq71dvpw7aQAdoxUqxZdTZiMB8G\n" + "A1UdIwQYMBaAFBO96Lq71dvpw7aQAdoxUqxZdTZiMA8GA1UdEwEB/wQFMAMBAf8w\n" + "DgYDVR0PAQH/BAQDAgEGMAoGCCqGSM49BAMCA0gAMEUCIEPshpyn3+k8gCVsrkjG\n" + "hpyEoRE8+JDdAiWTbTZM17iqAiEAzarnIdQWuq0m+22jQ8ljnODgMXejV5pJ30E3\n" + "C5N+2hs=\n" + "-----END CERTIFICATE-----\n"

// test-only
val TEST_ONLY_SERVER_CERT: String = "-----BEGIN CERTIFICATE-----\n" + "MIICATCCAaagAwIBAgIUCKVnu3yl92vqNWhjbOod6MrPh4gwCgYIKoZIzj0EAwIw\n" + "PTEkMCIGA1UECgwbbHlyaWMtbXEtYWN0aXZlbXEgVEVTVCBPTkxZMRUwEwYDVQQD\n" + "DAx0ZXN0LW9ubHkgQ0EwIBcNMjYxMTEyMjA0MzQyWhgPMjEyNjEwMTkyMDQzNDJa\n" + "MDoxJDAiBgNVBAoMG2x5cmljLW1xLWFjdGl2ZW1xIFRFU1QgT05MWTESMBAGA1UE\n" + "AwwJbG9jYWxob3N0MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAE4UqMwZLtPgfr\n" + "qsc8bdkA+Ma4as92fSn9nFyRnSlkW2qbsPx1fsPpbzdnx8noQeU9ng2B7JZdGFT/\n" + "N41Ri4aJnqOBhDCBgTAaBgNVHREEEzARgglsb2NhbGhvc3SHBH8AAAEwEwYDVR0l\n" + "BAwwCgYIKwYBBQUHAwEwDgYDVR0PAQH/BAQDAgeAMB0GA1UdDgQWBBQwtXKELMB8\n" + "diGydfIRw6aYVl0dVzAfBgNVHSMEGDAWgBQTvei6u9Xb6cO2kAHaMVKsWXU2YjAK\n" + "BggqhkjOPQQDAgNJADBGAiEA9nWrmJBPYADaQCWfcLkxFIe+5NJtvPZWY4DzWJoz\n" + "cV0CIQD4sNqVPsbSEbuQuHAh+m82dFOxaOzuTU+Ptb+s6uKrzw==\n" + "-----END CERTIFICATE-----\n"

// test-only private key
val TEST_ONLY_SERVER_KEY: String = "-----BEGIN PRIVATE KEY-----\n" + "MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQg0iMsh8kO7i1GbAKc\n" + "gI7OOrGRS/k+bbNUtMsYmBA68X+hRANCAAThSozBku0+B+uqxzxt2QD4xrhqz3Z9\n" + "Kf2cXJGdKWRbapuw/HV+w+lvN2fHyehB5T2eDYHsll0YVP83jVGLhome\n" + "-----END PRIVATE KEY-----\n"
