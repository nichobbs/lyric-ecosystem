//! tests/tls_tests.l - `Net.Tls`: the options and the functions that build
//! them, validation with the `Std.Tls` loaders, the two-key insecure policy
//! and the environment reader.
@test_module
package Net.Tls.Tests

import Std.Core
import Std.Testing
import Std.Environment as Env
import Std.File as File
import Std.String as Str
import Std.Tls as Tls
import Net.Tls

func problemOf(t: in TlsOptions): String {
  match validateTlsOptions(t) {
    case Ok(_) -> "valid"
    case Err(p) -> tlsProblemText(p, "a tcps:// URL")
  }
}

func expectContains(text: in String, part: in String, label: in String): Unit {
  assertTrue(Str.contains(text, part), label + ": " + text)
}

test "plainTcp is no TLS and nothing set" {
  val t = plainTcp()
  assertTrue(not t.enabled, "off")
  assertTrue(not hasTlsSettings(t), "nothing set")
  assertTrue(not requiresTls13(t), "TLS 1.2 is the floor")
  assertEqual(problemOf(t), "valid", "plain TCP is valid")
  assertTrue(not skipsVerification(t), "nothing to skip")
}

test "each with... function sets its option and nothing else" {
  val t = plainTcp().withTls()
  assertTrue(t.enabled, "withTls")
  assertTrue(not hasTlsSettings(t), "withTls sets nothing else")
  assertEqual(t.withTlsServerName("db.internal").serverName, "db.internal", "withTlsServerName")
  val additive = t.withCaCertificate(TEST_ONLY_CA)
  assertEqual(additive.caCertificatePem, TEST_ONLY_CA, "withCaCertificate")
  assertEqual(additive.exclusiveCaCertificatePem, "", "the additive CA is not exclusive")
  val exclusive = t.withExclusiveCaCertificate(TEST_ONLY_CA)
  assertEqual(exclusive.exclusiveCaCertificatePem, TEST_ONLY_CA, "withExclusiveCaCertificate")
  assertEqual(exclusive.caCertificatePem, "", "the exclusive CA is not additive")
  val mutual = t.withClientIdentity(TEST_ONLY_CLIENT_CERT, TEST_ONLY_CLIENT_KEY)
  assertEqual(mutual.clientCertificatePem, TEST_ONLY_CLIENT_CERT, "the certificate")
  assertEqual(mutual.clientKeyPem, TEST_ONLY_CLIENT_KEY, "the key")
  assertTrue(requiresTls13(t.withMinTlsVersion(Tls.TlsVersion.Tls13)), "withMinTlsVersion")
  assertTrue(t.withInsecureSkipVerify().insecureSkipVerify, "withInsecureSkipVerify")
  assertTrue(not plainTcp().withCaCertificate(TEST_ONLY_CA).enabled, "an option does not switch TLS on")
}

test "options without TLS are refused" {
  val plain = plainTcp()
  expectContains(problemOf(plain.withCaCertificate(TEST_ONLY_CA)), "the connection is not TLS", "a CA")
  expectContains(
    problemOf(plain.withExclusiveCaCertificate(TEST_ONLY_CA)),
    "the connection is not TLS",
    "an exclusive CA"
  )
  expectContains(
    problemOf(plain.withClientIdentity(TEST_ONLY_CLIENT_CERT, TEST_ONLY_CLIENT_KEY)),
    "the connection is not TLS",
    "an identity"
  )
  expectContains(problemOf(plain.withTlsServerName("h")), "the connection is not TLS", "a server name")
  expectContains(problemOf(plain.withMinTlsVersion(Tls.TlsVersion.Tls13)), "the connection is not TLS", "a version")
  expectContains(problemOf(plain.withInsecureSkipVerify()), "the connection is not TLS", "skip-verify")
  expectContains(problemOf(plain.withInsecureSkipVerify()), "use a tcps:// URL or withTls()", "the hint")
  match validateTlsOptions(plain.withTlsServerName("h")) {
    case Err(TlsNotEnabled) -> ()
    case _ -> assertTrue(false, "the problem is TlsNotEnabled")
  }
}

test "certificates and keys are checked with the Std.Tls loaders" {
  val tls = plainTcp().withTls()
  assertEqual(problemOf(tls.withCaCertificate(TEST_ONLY_CA)), "valid", "a CA")
  assertEqual(problemOf(tls.withExclusiveCaCertificate(TEST_ONLY_CA)), "valid", "an exclusive CA")
  assertEqual(
    problemOf(tls.withCaCertificate(TEST_ONLY_CA).withClientIdentity(TEST_ONLY_CLIENT_CERT, TEST_ONLY_CLIENT_KEY)),
    "valid",
    "a CA and an identity"
  )
  expectContains(problemOf(tls.withCaCertificate("not a certificate")), "TLS CA certificate:", "a malformed CA")
  expectContains(
    problemOf(tls.withExclusiveCaCertificate("-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n")),
    "TLS exclusive CA certificate:",
    "a malformed exclusive CA"
  )
  match validateTlsOptions(tls.withClientIdentity(TEST_ONLY_CLIENT_CERT, "")) {
    case Err(ClientCertificateWithoutKey) -> ()
    case _ -> assertTrue(false, "a certificate alone")
  }
  match validateTlsOptions(tls.withClientIdentity("", TEST_ONLY_CLIENT_KEY)) {
    case Err(ClientKeyWithoutCertificate) -> ()
    case _ -> assertTrue(false, "a key alone")
  }
  expectContains(problemOf(tls.withClientIdentity(TEST_ONLY_CLIENT_CERT, "")), "without its private key", "the text")
  expectContains(problemOf(tls.withClientIdentity("", TEST_ONLY_CLIENT_KEY)), "without its certificate", "the text")
  expectContains(
    problemOf(tls.withClientIdentity(TEST_ONLY_CLIENT_CERT, "not a key")),
    "TLS client identity:",
    "a malformed key"
  )
  expectContains(
    problemOf(tls.withClientIdentity("not a certificate", TEST_ONLY_CLIENT_KEY)),
    "TLS client identity:",
    "a malformed certificate"
  )
  val sec1 = "-----BEGIN EC PRIVATE KEY-----\nAAAA\n-----END EC PRIVATE KEY-----\n"
  expectContains(problemOf(tls.withClientIdentity(TEST_ONLY_CLIENT_CERT, sec1)), "PKCS8", "a SEC1 key is refused")
  match validateTlsOptions(tls.withClientIdentity(TEST_ONLY_CLIENT_CERT, TEST_ONLY_OTHER_KEY)) {
    case Err(ClientIdentityUnusable(_)) -> ()
    case _ -> assertTrue(false, "a key that belongs to another certificate")
  }
}

test "verification is skipped only with both keys of the insecure policy" {
  assertEqual(insecureAllowEnvName(), "LYRIC_TLS_ALLOW_INSECURE", "the variable")
  val secure = plainTcp().withTls()
  val insecure = secure.withInsecureSkipVerify()
  Env.setVar(insecureAllowEnvName(), "")
  assertTrue(not insecureAllowedByEnvironment(), "the environment does not allow it")
  assertTrue(not skipsVerification(insecure), "the option alone")
  expectContains(
    insecureIgnoredWarning(insecure, "Net.Tls.Tests"),
    "[Net.Tls.Tests] warning:",
    "the option alone warns"
  )
  expectContains(insecureIgnoredWarning(insecure, "Net.Tls.Tests"), "remains ENABLED", "and says what happens")
  assertEqual(insecureIgnoredWarning(secure, "Net.Tls.Tests"), "", "nothing to warn about")
  Env.setVar(insecureAllowEnvName(), "true")
  assertTrue(not insecureAllowedByEnvironment(), "only 1 allows it")
  Env.setVar(insecureAllowEnvName(), "1")
  assertTrue(insecureAllowedByEnvironment(), "the environment allows it")
  assertTrue(not skipsVerification(secure), "the variable alone")
  assertTrue(skipsVerification(insecure), "both keys")
  assertEqual(insecureIgnoredWarning(insecure, "Net.Tls.Tests"), "", "no warning when it takes effect")
  assertTrue(not skipsVerification(plainTcp().withInsecureSkipVerify()), "never without TLS")
  Env.setVar(insecureAllowEnvName(), "")
}

func writeTestOnlyFile(name: in String, text: in String): String {
  val path = Env.getVarOrDefault("TMPDIR", "/tmp") + "/lyric-net-test-only-" + name
  match File.writeText(path, text) {
    case Ok(_) -> path
    case Err(_) -> panic("cannot write " + path)
  }
}

func fromEnv(prefix: in String): TlsOptions {
  match tlsOptionsFromEnv(prefix, plainTcp().withTls()) {
    case Ok(t) -> t
    case Err(e) -> panic("tlsOptionsFromEnv: " + e)
  }
}

func envError(prefix: in String): String {
  match tlsOptionsFromEnv(prefix, plainTcp().withTls()) {
    case Ok(_) -> "accepted"
    case Err(e) -> e
  }
}

test "tlsOptionsFromEnv reads the variables under its prefix" {
  val prefix = "LYRIC_NET_TLS_TESTS_"
  val names = ["TLSCAFILE", "TLSCAEXCLUSIVE", "TLSCERTFILE", "TLSKEYFILE", "TLSSERVERNAME", "TLSMINVERSION"]
  for n in names {
    Env.setVar(prefix + n, "")
  }
  val untouched = fromEnv(prefix)
  assertTrue(untouched.enabled, "the base is kept")
  assertTrue(not hasTlsSettings(untouched), "no variable, no setting")
  val caPath = writeTestOnlyFile("ca.pem", TEST_ONLY_CA)
  val certPath = writeTestOnlyFile("client.pem", TEST_ONLY_CLIENT_CERT)
  val keyPath = writeTestOnlyFile("client.key", TEST_ONLY_CLIENT_KEY)
  Env.setVar(prefix + "TLSCAFILE", caPath + ".missing")
  expectContains(envError(prefix), "cannot read the file named by LYRIC_NET_TLS_TESTS_TLSCAFILE", "an unreadable file")
  Env.setVar(prefix + "TLSCAFILE", caPath)
  assertEqual(fromEnv(prefix).caCertificatePem, TEST_ONLY_CA, "the CA is additional by default")
  assertEqual(fromEnv(prefix).exclusiveCaCertificatePem, "", "and not exclusive")
  Env.setVar(prefix + "TLSCAEXCLUSIVE", "TRUE")
  assertEqual(fromEnv(prefix).exclusiveCaCertificatePem, TEST_ONLY_CA, "TLSCAEXCLUSIVE makes it exclusive")
  assertEqual(fromEnv(prefix).caCertificatePem, "", "and not additional")
  Env.setVar(prefix + "TLSCAEXCLUSIVE", "maybe")
  expectContains(envError(prefix), "LYRIC_NET_TLS_TESTS_TLSCAEXCLUSIVE must be true or false", "a bad switch")
  Env.setVar(prefix + "TLSCAEXCLUSIVE", "true")
  Env.setVar(prefix + "TLSCAFILE", "")
  expectContains(
    envError(prefix),
    "LYRIC_NET_TLS_TESTS_TLSCAEXCLUSIVE is set without LYRIC_NET_TLS_TESTS_TLSCAFILE",
    "exclusive without a CA"
  )
  Env.setVar(prefix + "TLSCAEXCLUSIVE", "")
  Env.setVar(prefix + "TLSCERTFILE", certPath)
  match validateTlsOptions(fromEnv(prefix)) {
    case Err(ClientCertificateWithoutKey) -> ()
    case _ -> assertTrue(false, "a certificate file alone is kept, for validation to refuse")
  }
  Env.setVar(prefix + "TLSKEYFILE", keyPath)
  val mutual = fromEnv(prefix)
  assertEqual(mutual.clientCertificatePem, TEST_ONLY_CLIENT_CERT, "the certificate")
  assertEqual(mutual.clientKeyPem, TEST_ONLY_CLIENT_KEY, "the key")
  Env.setVar(prefix + "TLSSERVERNAME", "db.internal")
  assertEqual(fromEnv(prefix).serverName, "db.internal", "the server name")
  Env.setVar(prefix + "TLSMINVERSION", "1.3")
  assertTrue(requiresTls13(fromEnv(prefix)), "1.3")
  Env.setVar(prefix + "TLSMINVERSION", "1.2")
  assertTrue(not requiresTls13(fromEnv(prefix)), "1.2")
  Env.setVar(prefix + "TLSMINVERSION", "1.1")
  expectContains(envError(prefix), "LYRIC_NET_TLS_TESTS_TLSMINVERSION must be 1.2 or 1.3", "an old version")
  assertTrue(not fromEnv("LYRIC_NET_TLS_TESTS_OTHER_").insecureSkipVerify, "verification cannot be switched off here")
  for n in names {
    Env.setVar(prefix + n, "")
  }
}

// ─── TEST-ONLY certificates and keys ─────────────────────────────────────────
// Generated for hermetic tests with `openssl` (EC P-256, valid until 2126);
// the same fixtures as in tests/tcp_tests.l. The private keys are public by
// being here: never trust these certificates outside a test.
// test-only
val TEST_ONLY_CA: String = "-----BEGIN CERTIFICATE-----\n" + "MIIB4TCCAYegAwIBAgIUMdZcqE1iGtInxR0FPbqibzyXeHYwCgYIKoZIzj0EAwIw\n" + "PTEkMCIGA1UECgwbbHlyaWMtbXEtYWN0aXZlbXEgVEVTVCBPTkxZMRUwEwYDVQQD\n" + "DAx0ZXN0LW9ubHkgQ0EwIBcNMjYxMTEyMjA0MzQyWhgPMjEyNjEwMTkyMDQzNDJa\n" + "MD0xJDAiBgNVBAoMG2x5cmljLW1xLWFjdGl2ZW1xIFRFU1QgT05MWTEVMBMGA1UE\n" + "AwwMdGVzdC1vbmx5IENBMFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEYAYCGXrA\n" + "I2ewAr+wvyoRAfbXD36+mTug7dD9P79g9FPZ4pb/G1LPreTm3JJpvL0rPMcjyZ6i\n" + "atNgUShoEQtM1KNjMGEwHQYDVR0OBBYEFBO96Lq71dvpw7aQAdoxUqxZdTZiMB8G\n" + "A1UdIwQYMBaAFBO96Lq71dvpw7aQAdoxUqxZdTZiMA8GA1UdEwEB/wQFMAMBAf8w\n" + "DgYDVR0PAQH/BAQDAgEGMAoGCCqGSM49BAMCA0gAMEUCIEPshpyn3+k8gCVsrkjG\n" + "hpyEoRE8+JDdAiWTbTZM17iqAiEAzarnIdQWuq0m+22jQ8ljnODgMXejV5pJ30E3\n" + "C5N+2hs=\n" + "-----END CERTIFICATE-----\n"

// test-only
val TEST_ONLY_CLIENT_CERT: String = "-----BEGIN CERTIFICATE-----\n" + "MIIB6jCCAY+gAwIBAgIUCKVnu3yl92vqNWhjbOod6MrPh4kwCgYIKoZIzj0EAwIw\n" + "PTEkMCIGA1UECgwbbHlyaWMtbXEtYWN0aXZlbXEgVEVTVCBPTkxZMRUwEwYDVQQD\n" + "DAx0ZXN0LW9ubHkgQ0EwIBcNMjYxMTEyMjA0MzQyWhgPMjEyNjEwMTkyMDQzNDJa\n" + "MEExJDAiBgNVBAoMG2x5cmljLW1xLWFjdGl2ZW1xIFRFU1QgT05MWTEZMBcGA1UE\n" + "AwwQdGVzdC1vbmx5IGNsaWVudDBZMBMGByqGSM49AgEGCCqGSM49AwEHA0IABBPG\n" + "NQcnPleid2CUSj3TcqY/7T2mH54mNqqRP89ugZtmCmPrKeguQejvY8i04nToDPGg\n" + "9Lno+WXA7UeDh7aEGYijZzBlMBMGA1UdJQQMMAoGCCsGAQUFBwMCMA4GA1UdDwEB\n" + "/wQEAwIHgDAdBgNVHQ4EFgQULwRAoPjdU+MFvJYdA4EeB9XhWDUwHwYDVR0jBBgw\n" + "FoAUE73ourvV2+nDtpAB2jFSrFl1NmIwCgYIKoZIzj0EAwIDSQAwRgIhAPHfud/j\n" + "lfy9BqSFYumS5MW7ljCZC+hBeO9fXZiiSbgDAiEAx+DPE3IVzjYVclCL4k/TT2+y\n" + "C9kbvqMHtdvi2dWjbJc=\n" + "-----END CERTIFICATE-----\n"

// test-only private key
val TEST_ONLY_CLIENT_KEY: String = "-----BEGIN PRIVATE KEY-----\n" + "MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgn3/zm5+BLXEqOfVa\n" + "81O7AX4r9UiOMVTJpfWUi9mn5CChRANCAAQTxjUHJz5XondglEo903KmP+09ph+e\n" + "JjaqkT/PboGbZgpj6ynoLkHo72PItOJ06AzxoPS56PllwO1Hg4e2hBmI\n" + "-----END PRIVATE KEY-----\n"

// test-only
val TEST_ONLY_OTHER_KEY: String = "-----BEGIN PRIVATE KEY-----\n" + "MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQg/irIMjHltlSLzkqe\n" + "AitdTtpknt7mMQqZFfyCqWygP3ihRANCAASJH35YplcKYi/VvJiqop/3RJBbgcCk\n" + "4YlzaobfTVSf+TjTuMSYKCSnK6Bg7CStB7BC+b4wRs0cZOrHRlG4f6ft\n" + "-----END PRIVATE KEY-----\n"
