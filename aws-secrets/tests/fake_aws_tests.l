//! tests/fake_aws_tests.l - AwsSecrets over real HTTP, against a Secrets Manager
//! and Parameter Store written in Lyric on `Std.HttpServer`.
//!
//! The fake server does what the services do with a request, so nothing here is
//! answered from a canned response:
//!
//!   * it verifies the AWS Signature Version 4 of every request with the shared
//!     secret, rebuilding the canonical request from the method, path and signed
//!     headers it actually received and the SHA-256 of the body it actually
//!     received (`InvalidSignatureException`, `UnrecognizedClientException`,
//!     `MissingAuthenticationTokenException` otherwise), and wants the session
//!     token of temporary credentials sent and signed;
//!   * it looks the secret or parameter up in what it holds, with the error
//!     documents and HTTP statuses of the real services: `ResourceNotFoundException`,
//!     `ParameterNotFound`, `AccessDeniedException` (also naming `kms:Decrypt`),
//!     `InvalidRequestException` for a secret marked for deletion, a binary secret
//!     with no `SecretString`, a SecureString that is returned decrypted only when
//!     `WithDecryption` is true;
//!   * names beginning `throttle-` fail twice with `ThrottlingException` and then
//!     succeed, `fail-` names answer HTTP 502 with an HTML page.
//!
//! The server runs on its own thread: a `Task.Run` worker on `--target dotnet`
//! (`spawn` is synchronous there), a `spawn` inside a `scope` on `--target jvm`, a
//! `Std.Task` scope thread on `--target native`. No assertion runs while it is
//! waiting for a request: a scenario returns its findings and ends by asking the
//! server to stop.
@test_module
package AwsSecrets.FakeAwsTests

import Std.Core
import Std.Collections
import Std.Encoding as Enc
import Std.Environment as Env
import Std.Errors
import Std.Http
import Std.HttpServer
import Std.JsonValue
import Std.String as Str
import Std.Task as StdTask
import Std.Testing
import Std.Time as Time
import Aws.SigV4
import Aws.Retry
import AwsSecrets
import AwsSecrets.Client
import AwsSecrets.Fetch

val ACCESS_KEY: String = "AKIAFAKEFAKEFAKEFAKE"

val SECRET_KEY: String = "fake/secret+key/for-tests-only"

val TEMP_ACCESS_KEY: String = "ASIAFAKETEMPORARYKEY"

val TEMP_SECRET_KEY: String = "fake/temporary+secret/for-tests-only"

val TEMP_TOKEN: String = "FakeSessionToken//for+tests/only=="

val REGION: String = "eu-test-1"

val STOP_PATH: String = "/__stop"

// A plain GET returning how many signed requests the server has accepted.
val COUNT_PATH: String = "/__count"

ref record FakeAws {
  var requestCount: Int
  var attempts: Map[String, Int]
}

func headerValue(ctx: in HttpContext, name: in String): String {
  val wanted = Str.toLower(name)
  var found = ""
  for e in mapEntries(requestHeaders(ctx)) {
    if Str.toLower(e.key) == wanted {
      found = e.value
    }
  }
  found
}

func digits(text: in String): Int {
  var n = 0
  var i = 0
  while i < text.length {
    n = n * 10 + (Str.codeUnitAt(text, i) - 48)
    i = i + 1
  }
  n
}

func amzDateToEpochSeconds(amzDate: in String): Long {
  val y0 = digits(amzDate.substring(0, 4))
  val m = digits(amzDate.substring(4, 2))
  val d = digits(amzDate.substring(6, 2))
  val y = if m <= 2 { y0 - 1 } else { y0 }
  val era = y / 400
  val yoe = y - era * 400
  val mp = if m > 2 { m - 3 } else { m + 9 }
  val doy = (153 * mp + 2) / 5 + d - 1
  val doe = yoe * 365 + yoe / 4 - yoe / 100 + doy
  val days = (era * 146097 + doe - 719468).toLong()
  days * 86400i64 +
    (digits(amzDate.substring(9, 2)) * 3600 + digits(amzDate.substring(11, 2)) * 60 + digits(amzDate.substring(13, 2))).toLong()
}

func authField(authorization: in String, name: in String): String {
  val at = Str.indexOfRaw(authorization, name + "=")
  if at < 0 {
    return ""
  }
  val rest = authorization.substring(at + name.length + 1)
  val comma = Str.indexOfRaw(rest, ",")
  if comma >= 0 {
    rest.substring(0, comma)
  } else {
    rest
  }
}

func jstr(v: in String): JsonValue {
  Std.JsonValue.JString(value = v)
}

func jsonObject(fields: in List[JsonField]): JsonValue {
  Std.JsonValue.JObject(fields = fields)
}

func respondDoc(ctx: in HttpContext, status: in Int, fields: in List[JsonField]): Unit {
  val none: Map[String, String] = newMap()
  respondBytesWithHeaders(
    ctx,
    status,
    "application/x-amz-json-1.1",
    Enc.encodeUtf8(writeValue(jsonObject(fields))),
    none
  )
}

func respondFault(ctx: in HttpContext, status: in Int, kind: in String, message: in String): Unit {
  respondDoc(
    ctx,
    status,
    [JsonField(name = "__type", value = jstr(kind)), JsonField(name = "message", value = jstr(message))]
  )
}

// `None` when the signature is the one this server computes for the service
// named in the credential scope.
func verifyAuth(ctx: in HttpContext, body: in slice[Byte], service: in String): Option[String] {
  val authorization = headerValue(ctx, "Authorization")
  if not Str.startsWith(authorization, "AWS4-HMAC-SHA256 ") {
    return Some(value = "MissingAuthenticationTokenException")
  }
  val credential = authField(authorization, "Credential")
  val signedNames = authField(authorization, "SignedHeaders")
  val signature = authField(authorization, "Signature")
  val amzDate = headerValue(ctx, "x-amz-date")
  if amzDate.length != 16 {
    return Some(value = "IncompleteSignature")
  }
  var creds = Credentials(accessKeyId = ACCESS_KEY, secretAccessKey = SECRET_KEY, sessionToken = "")
  if Str.startsWith(credential, TEMP_ACCESS_KEY + "/") {
    creds = Credentials(accessKeyId = TEMP_ACCESS_KEY, secretAccessKey = TEMP_SECRET_KEY, sessionToken = TEMP_TOKEN)
  } else if not Str.startsWith(credential, ACCESS_KEY + "/") {
    return Some(value = "UnrecognizedClientException")
  }
  if credential != creds.accessKeyId + "/" + credentialScope(amzDate, REGION, service) {
    return Some(value = "InvalidSignatureException")
  }
  val skew = Time.nowEpochMillis() / 1000i64 - amzDateToEpochSeconds(amzDate)
  if skew > 900i64 or skew < -900i64 {
    return Some(value = "InvalidSignatureException")
  }
  val required = ";" + signedNames + ";"
  if not Str.contains(required, ";host;") or not Str.contains(required, ";x-amz-date;") or not Str.contains(required, ";x-amz-target;") {
    return Some(value = "InvalidSignatureException")
  }
  if creds.sessionToken.length > 0 {
    if headerValue(ctx, "x-amz-security-token") != creds.sessionToken {
      return Some(value = "UnrecognizedClientException")
    }
    if not Str.contains(required, ";x-amz-security-token;") {
      return Some(value = "InvalidSignatureException")
    }
  }
  val headers: List[Pair] = newList()
  val names = Str.split(signedNames, ";")
  var i = 0
  while i < names.length {
    headers.add(Pair(name = names[i], value = headerValue(ctx, names[i])))
    i = i + 1
  }
  val noQuery: List[Pair] = newList()
  val expected = signRequest(
    requestMethod(ctx),
    canonicalUri(urlDecode(requestPath(ctx)), false),
    noQuery,
    headers,
    sha256Hex(body),
    creds,
    REGION,
    service,
    amzDate
  )
  if expected.signature == signature {
    None
  } else {
    Some(value = "InvalidSignatureException")
  }
}

func countAttempt(fake: in FakeAws, name: in String): Int {
  var n = 1
  if fake.attempts.containsKey(name) {
    val before: Int = fake.attempts[name]
    n = before + 1
  }
  fake.attempts[name] = n
  n
}

func field(name: in String, value: in String): JsonField {
  JsonField(name = name, value = jstr(value))
}

func handleSecret(fake: in FakeAws, ctx: in HttpContext, doc: in JsonValue): Unit {
  val id = match getString(doc, "SecretId") { case Some(s) -> s; case None -> "" }
  if Str.startsWith(id, "fail-") {
    val none: Map[String, String] = newMap()
    respondBytesWithHeaders(ctx, 502, "text/html", Enc.encodeUtf8("<html>Bad Gateway</html>"), none)
  } else if Str.startsWith(id, "throttle-") and countAttempt(fake, id) <= 2 {
    respondFault(ctx, 400, "com.amazonaws.secretsmanager#ThrottlingException", "Rate exceeded")
  } else if id == "app/db" {
    respondDoc(ctx, 200, [field("Name", id), field("SecretString", "{\"user\":\"admin\",\"password\":\"s3cret\"}")])
  } else if id == "app/plain" or Str.startsWith(id, "throttle-") {
    respondDoc(ctx, 200, [field("Name", id), field("SecretString", "plain-value")])
  } else if id == "app/binary" {
    respondDoc(ctx, 200, [field("Name", id), field("SecretBinary", "AQID")])
  } else if id == "app/denied" {
    respondFault(
      ctx,
      400,
      "com.amazonaws.secretsmanager#AccessDeniedException",
      "User is not authorized to perform: secretsmanager:GetSecretValue"
    )
  } else if id == "app/deleted" {
    respondFault(
      ctx,
      400,
      "com.amazonaws.secretsmanager#InvalidRequestException",
      "You can't perform this operation on the secret because it was marked for deletion."
    )
  } else if id == "app/kms" {
    respondFault(
      ctx,
      400,
      "com.amazonaws.secretsmanager#DecryptionFailure",
      "Secrets Manager can't decrypt the protected secret text"
    )
  } else {
    respondFault(
      ctx,
      400,
      "com.amazonaws.secretsmanager#ResourceNotFoundException",
      "Secrets Manager can't find the specified secret."
    )
  }
}

func handleParameter(ctx: in HttpContext, doc: in JsonValue): Unit {
  val name = match getString(doc, "Name") { case Some(s) -> s; case None -> "" }
  val decrypt = match getBool(doc, "WithDecryption") { case Some(b) -> b; case None -> false }
  if name == "/app/key" {
    respondDoc(
      ctx,
      200,
      [
        JsonField(
          name = "Parameter",
          value = jsonObject([field("Name", name), field("Type", "String"), field("Value", "param-value")])
        )
      ]
    )
  } else if name == "/app/secure" {
    val value = if decrypt { "decrypted-value" } else { "AQICAHg-encrypted-blob" }
    respondDoc(
      ctx,
      200,
      [
        JsonField(
          name = "Parameter",
          value = jsonObject([field("Name", name), field("Type", "SecureString"), field("Value", value)])
        )
      ]
    )
  } else if name == "/app/kmsdenied" {
    respondFault(
      ctx,
      400,
      "com.amazonaws.ssm#AccessDeniedException",
      "User is not authorized to perform: kms:Decrypt on the key"
    )
  } else if name == "/app/denied" {
    respondFault(
      ctx,
      400,
      "com.amazonaws.ssm#AccessDeniedException",
      "User is not authorized to perform: ssm:GetParameter"
    )
  } else {
    respondFault(ctx, 400, "com.amazonaws.ssm#ParameterNotFound", "")
  }
}

func handle(fake: in FakeAws, ctx: in HttpContext): Unit {
  val body = requestBodyBytes(ctx)
  val target = headerValue(ctx, "X-Amz-Target")
  val service = if Str.startsWith(target, "secretsmanager.") { "secretsmanager" } else { "ssm" }
  match verifyAuth(ctx, body, service) {
    case Some(kind) -> {
      respondFault(ctx, 403, "com.amazon.coral.service#" + kind, "request refused: " + kind)
      return
    }
    case None -> ()
  }
  fake.requestCount = fake.requestCount + 1
  if requestMethod(ctx) != "POST" or requestPath(ctx) != "/" {
    respondFault(ctx, 404, "com.amazon.coral.service#UnknownOperationException", "POST / is the only route")
    return
  }
  if headerValue(ctx, "Content-Type") != "application/x-amz-json-1.1" {
    respondFault(
      ctx,
      400,
      "com.amazon.coral.service#SerializationException",
      "Content-Type must be application/x-amz-json-1.1"
    )
    return
  }
  val text = match Enc.tryDecodeUtf8(body) { case Some(t) -> t; case None -> "" }
  val doc = match parseValue(text) {
    case Ok(d) -> d
    case Err(_) -> {
      respondFault(ctx, 400, "com.amazon.coral.service#SerializationException", "the request body is not JSON")
      return
    }
  }
  if target == "secretsmanager.GetSecretValue" {
    handleSecret(fake, ctx, doc)
  } else if target == "AmazonSSM.GetParameter" {
    handleParameter(ctx, doc)
  } else {
    respondFault(ctx, 400, "com.amazon.coral.service#UnknownOperationException", "unknown target " + target)
  }
}

func serveUntilStopped(l: in HttpListener): Unit {
  val fake = FakeAws(requestCount = 0, attempts = newMap())
  var running = true
  while running {
    val ctx = nextContext(l)
    if requestPath(ctx) == STOP_PATH {
      respondText(ctx, 200, "stopped")
      running = false
    } else if requestPath(ctx) == COUNT_PATH {
      respondText(ctx, 200, fake.requestCount.toString())
    } else {
      handleGuarded(fake, ctx)
    }
  }
}

@cfg(not(target = "native"))
func handleGuarded(fake: in FakeAws, ctx: in HttpContext): Unit {
  try {
    handle(fake, ctx)
  } catch Bug as b {
    respondFault(ctx, 500, "FakeServerBug", b.message)
  }
}

@cfg(target = "native")
func handleGuarded(fake: in FakeAws, ctx: in HttpContext): Unit {
  handle(fake, ctx)
}

@cfg(target = "dotnet")
extern type WorkerTask = "System.Threading.Tasks.Task"

@cfg(target = "dotnet")
@externStatic
@externTarget("System.Threading.Tasks.Task.Run")
func taskRun(work: in () -> Unit): WorkerTask = ()

@cfg(target = "dotnet")
@externInstance
@externTarget("System.Threading.Tasks.Task.Wait")
func taskWait(t: in WorkerTask): Unit = ()

func endpointOf(port: in Int): String {
  "http://127.0.0.1:" + port.toString()
}

@cfg(target = "dotnet")
func withFake(port: in Int, scenario: in (String) -> String): String {
  val l = startListener(endpointOf(port) + "/")
  val server = taskRun({ -> serveUntilStopped(l) })
  val result = scenario(endpointOf(port))
  taskWait(server)
  stopListener(l)
  result
}

@cfg(target = "native")
func withFake(port: in Int, scenario: in (String) -> String): String {
  val l = startListener(endpointOf(port) + "/")
  val server = StdTask.makeScope()
  StdTask.scopeSpawn(server, { -> serveUntilStopped(l) })
  val result = scenario(endpointOf(port))
  val _ = StdTask.awaitAll(server)
  stopListener(l)
  result
}

@cfg(target = "jvm")
func withFake(port: in Int, scenario: in (String) -> String): String {
  val l = startListener(endpointOf(port) + "/")
  var result = ""
  scope {
    val server = spawn serveUntilStopped(l)
    result = scenario(endpointOf(port))
    await server
  }
  stopListener(l)
  result
}

func plainGet(url: in String): String {
  match await defaultClient().get(url) {
    case Err(e) -> "request failed: " + HttpError.message(e)
    case Ok(resp) -> match await HttpResponse.bodyText(resp) { case Ok(text) -> text; case Err(_) -> "unreadable" }
  }
}

func served(endpoint: in String): Int {
  digits(plainGet(endpoint + COUNT_PATH))
}

// A scenario's failures, one per line; empty when every check held.
ref record Findings {
  var lines: List[String]
}

func check(f: in Findings, ok: in Bool, label: in String): Unit {
  if not ok {
    f.lines.add(label)
  }
}

func finish(f: in Findings, endpoint: in String): String {
  val _ = plainGet(endpoint + STOP_PATH)
  Str.joinList("\n", f.lines)
}

func noPause(delayMs: in Int): Unit {
}

func staticClient(endpoint: in String, service: in Service, retry: in RetryPolicy): AwsClient {
  val cfg = staticConfig(REGION, endpoint, ACCESS_KEY, SECRET_KEY).withRetry(retry)
  match openClient(service, cfg) {
    case Ok(c) -> c
    case Err(e) -> {
      assertTrue(false, "client must open: " + e.message)
      match openClient(service, staticConfig(REGION, "", "a", "b")) {
        case Ok(c) -> c
        case Err(_) -> panic("unreachable")
      }
    }
  }
}

func fastRetry(): RetryPolicy {
  standardRetry().withSleep(noPause)
}

func secretOf(r: in Result[String, SecretsError]): String {
  match r {
    case Ok(v) -> "ok:" + v
    case Err(e) -> "err:" + errorMessage(e)
  }
}

func kindOf(r: in Result[String, SecretsError]): String {
  match r {
    case Ok(_) -> "Ok"
    case Err(NotFound(_)) -> "NotFound"
    case Err(AccessDenied(_, _)) -> "AccessDenied"
    case Err(DecryptionError(_, _)) -> "DecryptionError"
    case Err(ParseError(_, _, _)) -> "ParseError"
    case Err(NetworkError(_, _)) -> "NetworkError"
    case Err(BinaryValueUnsupported(_)) -> "Binary"
    case Err(InvalidConfig(_, _)) -> "InvalidConfig"
  }
}

func secretsScenario(endpoint: in String): String {
  val f = Findings(lines = newList())
  val client = staticClient(endpoint, secretsManagerService(), fastRetry())
  check(f, secretOf(fetchSecretWith(client, "app/plain", None, 0)) == "ok:plain-value", "whole string secret")
  check(f, secretOf(fetchSecretWith(client, "app/db", Some("password"), 0)) == "ok:s3cret", "JSON field")
  check(f, kindOf(fetchSecretWith(client, "app/db", Some("missing"), 0)) == "ParseError", "absent JSON field")
  check(f, kindOf(fetchSecretWith(client, "app/plain", Some("k"), 0)) == "ParseError", "field of a non-JSON secret")
  check(f, kindOf(fetchSecretWith(client, "app/binary", None, 0)) == "Binary", "binary secret")
  check(f, kindOf(fetchSecretWith(client, "app/none", None, 0)) == "NotFound", "missing secret")
  check(f, kindOf(fetchSecretWith(client, "app/denied", None, 0)) == "AccessDenied", "access denied")
  check(f, kindOf(fetchSecretWith(client, "app/kms", None, 0)) == "DecryptionError", "decryption failure")
  check(f, kindOf(fetchSecretWith(client, "app/deleted", None, 0)) == "InvalidConfig", "marked for deletion")
  finish(f, endpoint)
}

test "Secrets Manager: values, JSON fields and every error kind, over signed HTTP" {
  assertEqual(withFake(19461, secretsScenario), "", "secrets scenario findings")
}

func parametersScenario(endpoint: in String): String {
  val f = Findings(lines = newList())
  val client = staticClient(endpoint, parameterStoreService(), fastRetry())
  check(f, secretOf(fetchParameterWith(client, "/app/key", true, 0)) == "ok:param-value", "String parameter")
  check(
    f,
    secretOf(fetchParameterWith(client, "/app/secure", true, 0)) == "ok:decrypted-value",
    "SecureString decrypted"
  )
  check(
    f,
    secretOf(fetchParameterWith(client, "/app/secure", false, 0)) == "ok:AQICAHg-encrypted-blob",
    "SecureString raw"
  )
  check(f, kindOf(fetchParameterWith(client, "/app/missing", true, 0)) == "NotFound", "missing parameter")
  check(f, kindOf(fetchParameterWith(client, "/app/denied", true, 0)) == "AccessDenied", "access denied")
  check(f, kindOf(fetchParameterWith(client, "/app/kmsdenied", true, 0)) == "DecryptionError", "kms:Decrypt denied")
  finish(f, endpoint)
}

test "Parameter Store: String and SecureString parameters and every error kind, over signed HTTP" {
  assertEqual(withFake(19462, parametersScenario), "", "parameters scenario findings")
}

func credentialsScenario(endpoint: in String): String {
  val f = Findings(lines = newList())
  val svc = secretsManagerService()
  val wrongSecret = match openClient(svc, staticConfig(REGION, endpoint, ACCESS_KEY, "not-the-secret").withRetry(noRetry())) {
    case Ok(c) -> kindOf(fetchSecretWith(c, "app/plain", None, 0))
    case Err(e) -> "open failed " + e.message
  }
  check(f, wrongSecret == "AccessDenied", "a wrong secret key is refused, got " + wrongSecret)
  val unknownKey = match openClient(svc, staticConfig(REGION, endpoint, "AKIAUNKNOWNUNKNOWNUN", SECRET_KEY).withRetry(noRetry())) {
    case Ok(c) -> kindOf(fetchSecretWith(c, "app/plain", None, 0))
    case Err(e) -> "open failed " + e.message
  }
  check(f, unknownKey == "AccessDenied", "an unknown key id is refused, got " + unknownKey)
  val wrongRegion = match openClient(svc, staticConfig("eu-other-9", endpoint, ACCESS_KEY, SECRET_KEY).withRetry(noRetry())) {
    case Ok(c) -> kindOf(fetchSecretWith(c, "app/plain", None, 0))
    case Err(e) -> "open failed " + e.message
  }
  check(f, wrongRegion == "AccessDenied", "a wrong region in the scope is refused, got " + wrongRegion)
  var temp = staticConfig(REGION, endpoint, TEMP_ACCESS_KEY, TEMP_SECRET_KEY)
  temp = temp.copy(sessionToken = TEMP_TOKEN)
  val withToken = match openClient(svc, temp) {
    case Ok(c) -> secretOf(fetchSecretWith(c, "app/plain", None, 0))
    case Err(e) -> "open failed " + e.message
  }
  check(f, withToken == "ok:plain-value", "temporary credentials with their session token, got " + withToken)
  val noToken = match openClient(svc, staticConfig(REGION, endpoint, TEMP_ACCESS_KEY, TEMP_SECRET_KEY).withRetry(noRetry())) {
    case Ok(c) -> kindOf(fetchSecretWith(c, "app/plain", None, 0))
    case Err(e) -> "open failed " + e.message
  }
  check(f, noToken == "AccessDenied", "temporary credentials without their token are refused, got " + noToken)
  finish(f, endpoint)
}

test "credentials: a wrong secret, key id, region or missing session token is AccessDenied" {
  assertEqual(withFake(19463, credentialsScenario), "", "credentials scenario findings")
}

func failuresScenario(endpoint: in String): String {
  val f = Findings(lines = newList())
  val svc = secretsManagerService()
  val retrying = staticClient(endpoint, svc, fastRetry())
  check(
    f,
    secretOf(fetchSecretWith(retrying, "throttle-a", None, 0)) == "ok:plain-value",
    "throttling is retried until it succeeds"
  )
  val once = staticClient(endpoint, svc, noRetry())
  check(
    f,
    kindOf(fetchSecretWith(once, "throttle-b", None, 0)) == "NetworkError",
    "throttling without retries is NetworkError"
  )
  check(f, kindOf(fetchSecretWith(retrying, "fail-gateway", None, 0)) == "NetworkError", "an HTML 502 is NetworkError")
  val unreachable = staticClient("http://127.0.0.1:19499", svc, noRetry())
  check(
    f,
    kindOf(fetchSecretWith(unreachable, "app/plain", None, 0)) == "NetworkError",
    "a refused connection is NetworkError"
  )
  finish(f, endpoint)
}

test "failures: throttling is retried, an HTML 502 and a refused connection are NetworkError" {
  assertEqual(withFake(19464, failuresScenario), "", "failures scenario findings")
}

func cacheScenario(endpoint: in String): String {
  val f = Findings(lines = newList())
  clearCache()
  val client = staticClient(endpoint, secretsManagerService(), fastRetry())
  val before = served(endpoint)
  check(f, secretOf(fetchSecretWith(client, "app/plain", None, 60)) == "ok:plain-value", "first fetch")
  check(f, secretOf(fetchSecretWith(client, "app/plain", None, 60)) == "ok:plain-value", "second fetch")
  check(f, served(endpoint) - before == 1, "two fetches with a TTL make one request")
  val mid = served(endpoint)
  val _a = fetchSecretWith(client, "app/plain", None, 0)
  val _b = fetchSecretWith(client, "app/plain", None, 0)
  check(f, served(endpoint) - mid == 2, "a TTL of 0 asks every time")
  val keyed = served(endpoint)
  val _c = fetchSecretWith(client, "app/db", Some("user"), 60)
  val _d = fetchSecretWith(client, "app/db", Some("password"), 60)
  check(f, served(endpoint) - keyed == 2, "the key is part of the cache key")
  val failedBefore = served(endpoint)
  val _e = fetchSecretWith(client, "app/none", None, 60)
  val _g = fetchSecretWith(client, "app/none", None, 60)
  check(f, served(endpoint) - failedBefore == 2, "an error is not cached")
  clearCache()
  finish(f, endpoint)
}

test "cache: a TTL serves the second fetch locally, a TTL of 0 and an error do not" {
  assertEqual(withFake(19465, cacheScenario), "", "cache scenario findings")
}

// ─── The environment-driven path: getSecret / init() ─────────────────────────
@cfg(not(target = "native"))
config Wired {
  @sensitive
  @secretsManager("app/db", key = "password")
  dbPassword: String
  @sensitive
  @parameterStore("/app/secure")
  signingKey: String
  @secretsManager("app/plain")
  plain: String
}

func setEnvironmentFor(endpoint: in String): Unit {
  Env.setVar("AWS_ACCESS_KEY_ID", ACCESS_KEY)
  Env.setVar("AWS_SECRET_ACCESS_KEY", SECRET_KEY)
  Env.setVar("AWS_REGION", REGION)
  Env.setVar("AWS_ENDPOINT_URL", endpoint)
}

func environmentScenario(endpoint: in String): String {
  val f = Findings(lines = newList())
  setEnvironmentFor(endpoint)
  clearCache()
  check(f, secretOf(AwsSecrets.getSecret("app/plain")) == "ok:plain-value", "getSecret from the environment")
  check(f, secretOf(AwsSecrets.getSecretField("app/db", "user")) == "ok:admin", "getSecretField")
  check(f, secretOf(AwsSecrets.getParameter("/app/secure")) == "ok:decrypted-value", "getParameter decrypts")
  check(
    f,
    secretOf(AwsSecrets.getParameterRaw("/app/secure")) == "ok:AQICAHg-encrypted-blob",
    "getParameterRaw does not"
  )
  check(f, kindOf(AwsSecrets.getSecret("app/none")) == "NotFound", "a missing secret")
  clearCache()
  finish(f, endpoint)
}

test "getSecret, getSecretField, getParameter and getParameterRaw read their client from the environment" {
  assertEqual(withFake(19466, environmentScenario), "", "environment scenario findings")
}

@cfg(not(target = "native"))
func initScenario(endpoint: in String): String {
  val f = Findings(lines = newList())
  setEnvironmentFor(endpoint)
  clearCache()
  match AwsSecrets.init() {
    case Ok(_) -> ()
    case Err(e) -> check(f, false, "init() failed: " + errorMessage(e))
  }
  check(f, Wired.dbPassword == "s3cret", "the JSON field reached the config block, got " + Wired.dbPassword)
  check(f, Wired.signingKey == "decrypted-value", "the SecureString reached the config block")
  check(f, Wired.plain == "plain-value", "the plain secret reached the config block")
  clearCache()
  finish(f, endpoint)
}

@cfg(not(target = "native"))
test "init(): annotated config fields are fetched over signed HTTP and read back from the config block" {
  assertEqual(withFake(19467, initScenario), "", "init scenario findings")
}
