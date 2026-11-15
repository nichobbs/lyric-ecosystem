//! tests/integration/emulator_integration_tests.l - the shared-files provider
//! against the files a real tool wrote, and a signed call a real service
//! checks.
//!
//! The AWS Emulator (scripts/ci/start-aws-emulator.sh) writes an INI
//! credentials file with the access key it generated for the run, and an INI
//! config file with its region, `endpoint_url`, `ca_bundle` and an `s3 =`
//! sub-property block. These tests read both through
//! `AWS_SHARED_CREDENTIALS_FILE` and `AWS_CONFIG_FILE`, exactly as an
//! application would, and call STS `GetCallerIdentity` on the emulator, which
//! verifies the SigV4 signature: the call succeeds only if the key, the
//! secret, the region, the endpoint and the CA were all read correctly.
//!
//! Variables (set by scripts/ci/start-aws-emulator.sh):
//!
//!   AWS_EMULATOR_CREDENTIALS_FILE   the emulator's `emulator_credentials`
//!   AWS_EMULATOR_CONFIG_FILE        the emulator's `config`
//!
//! The lookup the providers are given holds only those two files: the
//! process environment's own `AWS_*` variables are not consulted, so nothing
//! but the files can supply the credentials.
@cfg(feature = "emulator")
@test_module
package Aws.Credentials.IntegrationTests

import Std.Core
import Std.Collections
import Std.Encoding as Enc
import Std.Environment as Env
import Std.Errors
import Std.File as File
import Std.Http
import Std.String as Str
import Std.Testing
import Std.Tls as Tls
import Std.Xml
import Aws.SigV4
import Aws.SigV4.Env as AwsEnv
import Aws.Credentials
import Aws.Credentials.Chain
import Aws.Credentials.Profile

func required(name: in String): String {
  val v = Env.getVarOrDefault(name, "")
  if v.length == 0 {
    panic(
      name + " is not set: the aws-credentials integration tests need AWS_EMULATOR_CREDENTIALS_FILE and " +
        "AWS_EMULATOR_CONFIG_FILE (see tests/integration/emulator_integration_tests.l)"
    )
  }
  v
}

// An environment that holds the emulator's two files and nothing else.
func emulatorEnvironment(): (String) -> String {
  val values: Map[String, String] = newMap()
  values["AWS_SHARED_CREDENTIALS_FILE"] = required("AWS_EMULATOR_CREDENTIALS_FILE")
  values["AWS_CONFIG_FILE"] = required("AWS_EMULATOR_CONFIG_FILE")
  values["AWS_EC2_METADATA_DISABLED"] = "true"
  environmentOf(values)
}

func settings(): AwsSettings {
  match settingsFromEnvironment(emulatorEnvironment(), "STS") {
    case Ok(s) -> s
    case Err(e) -> panic("cannot read the emulator's settings: " + e.text())
  }
}

// STS GetCallerIdentity, signed with `creds`: `<status> <account or error code>`.
func callerIdentity(creds: in Credentials, s: in AwsSettings): String {
  val pem = match File.readText(s.caBundlePath) {
    case Ok(t) -> t
    case Err(_) -> panic("cannot read " + s.caBundlePath)
  }
  val ca = match Tls.Certificate.fromPem(pem) {
    case Ok(c) -> c
    case Err(e) -> panic("the emulator's CA is not usable: " + Tls.TlsError.message(e))
  }
  val http = HttpClientBuilder.new().withNoRedirects().withCaCertificate(ca).build()
  val host = s.endpointUrl.substring(8)
  val contentType = "application/x-www-form-urlencoded; charset=utf-8"
  val body = Enc.encodeUtf8("Action=GetCallerIdentity&Version=2011-06-15")
  val extra: List[Pair] = newList()
  extra.add(Pair(name = "content-type", value = contentType))
  val noQuery: List[Pair] = newList()
  val signed = signedHeaders(
    "POST",
    canonicalUri("/", false),
    noQuery,
    host,
    extra,
    sha256Hex(body),
    false,
    creds,
    s.region,
    "sts",
    AwsEnv.amzDateNow()
  )
  val url = match Url.tryFrom(s.endpointUrl + "/") {
    case Ok(u) -> u
    case Err(_) -> panic("not a URL: " + s.endpointUrl)
  }
  var req = match withBytesBody(request(POST, url), contentType, body) {
    case Ok(r) -> r
    case Err(e) -> panic(HttpError.message(e))
  }
  var k = 0
  while k < signed.headers.count {
    if signed.headers[k].name != "content-type" {
      req = match withHeader(req, signed.headers[k].name, signed.headers[k].value) {
        case Ok(r) -> r
        case Err(e) -> panic(HttpError.message(e))
      }
    }
    k = k + 1
  }
  val resp = match await http.send(req) {
    case Ok(r) -> r
    case Err(e) -> return "no response: " + HttpError.message(e)
  }
  val text = match await HttpResponse.bodyText(resp) { case Ok(t) -> t; case Err(_) -> "" }
  val status = HttpResponse.statusCode(resp).toString()
  match parseXml(text) {
    case Err(_) -> status + " (not XML)"
    case Ok(doc) -> match findFirst(documentRoot(doc), "Account") {
      case Some(account) -> status + " " + Str.trim(textContent(account))
      case None -> match findFirst(documentRoot(doc), "Code") {
        case Some(code) -> status + " " + Str.trim(textContent(code))
        case None -> status + " (no Account, no Code)"
      }
    }
  }
}

test "the emulator's config file gives the region, the endpoint and the CA bundle; its s3 block is skipped" {
  val s = settings()
  assertEqual(s.region, "us-east-1", "region")
  assertTrue(Str.startsWith(s.endpointUrl, "https://127.0.0.1:"), "endpoint_url: " + s.endpointUrl)
  assertTrue(Str.endsWith(s.caBundlePath, "/ca.pem"), "ca_bundle: " + s.caBundlePath)
  match loadProfile(sharedFilesFromEnvironment(emulatorEnvironment())) {
    case Err(e) -> assertTrue(false, e.text())
    case Ok(profile) -> {
      assertTrue(profile.found, "the default profile exists")
      assertEqual(profile.get("s3"), "", "the s3 property opens a block and has no value")
      assertEqual(profile.get("signature_version"), "", "the block's signature_version is not a profile property")
    }
  }
}

test "credentials from the emulator's credentials file sign a call the emulator accepts" {
  val resolved = match defaultChain(emulatorEnvironment()).resolve() {
    case Ok(c) -> c
    case Err(e) -> panic("the default chain found no credentials: " + e.text())
  }
  assertEqual(resolved.source, "profile", "they come from the shared files, not the process environment")
  assertTrue(not isSome(resolved.expiresAtEpochMs), "static keys do not expire")
  assertEqual(
    callerIdentity(resolved.credentials, settings()),
    "200 000000000000",
    "GetCallerIdentity: the emulator's account"
  )
}

test "the emulator refuses the same call signed with another secret" {
  val resolved = match profileProvider(emulatorEnvironment()).resolve() {
    case Ok(c) -> c
    case Err(e) -> panic(e.text())
  }
  val wrong = Credentials(
    accessKeyId = resolved.credentials.accessKeyId,
    secretAccessKey = "not-the-secret-the-emulator-generated",
    sessionToken = ""
  )
  val answer = callerIdentity(wrong, settings())
  assertTrue(
    Str.startsWith(answer, "403 "),
    "a wrong secret is refused, so the accepted call proved the secret: " + answer
  )
}

test "the cached default chain serves the same credentials without reading the files again" {
  val cache = cachedWith(defaultChain(emulatorEnvironment()), defaultCacheOptions())
  val first = match cache.resolve() { case Ok(c) -> c; case Err(e) -> panic(e.text()) }
  val second = match cache.resolve() { case Ok(c) -> c; case Err(e) -> panic(e.text()) }
  assertEqual(second.credentials.accessKeyId, first.credentials.accessKeyId, "the same key")
  assertEqualInt(cache.stats().refreshes, 1, "one walk of the chain")
  assertEqual(callerIdentity(second.credentials, settings()), "200 000000000000", "and they sign")
}
