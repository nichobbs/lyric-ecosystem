//! tests/remote_tests.l - the container, instance metadata and web identity
//! providers, and the order of the default chain, against in-process fakes
//! of the three services on `Std.HttpServer`.
//!
//! The fakes enforce what the real services enforce: the metadata service
//! answers 401 to a request without the session token it issued, and issues
//! one only to a `PUT` that carries the TTL header; the container endpoint
//! answers 401 without the expected `Authorization`; STS checks the form of
//! `AssumeRoleWithWebIdentity` and answers with the XML documents STS sends.
//!
//! The server runs on its own thread: a `Task.Run` worker on `--target
//! dotnet`, a `spawn` inside a `scope` on `--target jvm`. A scenario returns
//! its findings as lines and ends by asking the server to stop.
@test_module
package Aws.Credentials.RemoteTests

import Std.Core
import Std.Collections
import Std.Encoding as Enc
import Std.Environment as Env
import Std.Errors
import Std.File as File
import Std.Http
import Std.HttpServer
import Std.String as Str
import Std.Testing
import Std.Time as Time
import Aws.Credentials
import Aws.Credentials.Chain
import Aws.Credentials.Remote

// Fixture values, not real credentials.
val SECRET: String = "fixture/secret+key/for-tests-only"

val SESSION_TOKEN: String = "fixture-session-token"

val CONTAINER_AUTH: String = "Bearer fixture-container-authorization"

val OIDC_TOKEN: String = "fixture.oidc.token-with+chars/that=need&encoding"

val ROLE_ARN: String = "arn:aws:iam::123456789012:role/lyric-test"

val EXPIRATION: String = "2026-11-13T10:15:00Z"

val EXPIRATION_MS: String = "1794564900000"

val STOP_PATH: String = "/__stop"

// ─── The fake services ───────────────────────────────────────────────────────
ref record Fake {
  var mode: String
  var imdsTokens: Int
  var lastImdsToken: String
  var requests: Int
}

func headerValue(ctx: in HttpContext, name: in String): String {
  val wanted = Str.toLower(name)
  val entries = mapEntries(requestHeaders(ctx))
  var found = ""
  for e in entries {
    if Str.toLower(e.key) == wanted {
      found = e.value
    }
  }
  found
}

func credentialsJson(id: in String): String {
  "{\"Code\": \"Success\", \"Type\": \"AWS-HMAC\", \"AccessKeyId\": \"" + id + "\", \"SecretAccessKey\": \"" + SECRET +
    "\", \"Token\": \"" +
    SESSION_TOKEN +
    "\", \"Expiration\": \"" +
    EXPIRATION +
    "\"}"
}

func serveImds(fake: in Fake, ctx: in HttpContext, path: in String): Unit {
  if path == "/latest/api/token" {
    if fake.mode == "imds-forbidden" {
      respondText(ctx, 403, "")
    } else if requestMethod(ctx) != "PUT" {
      respondText(ctx, 405, "")
    } else if headerValue(ctx, "x-aws-ec2-metadata-token-ttl-seconds") != "21600" {
      respondText(ctx, 400, "")
    } else {
      fake.imdsTokens = fake.imdsTokens + 1
      fake.lastImdsToken = "fixture-imds-session-" + fake.imdsTokens.toString()
      respondText(ctx, 200, fake.lastImdsToken)
    }
    return
  }
  // Everything else needs the token: this is IMDSv2 only.
  val token = headerValue(ctx, "x-aws-ec2-metadata-token")
  if token.length == 0 or token != fake.lastImdsToken {
    respondText(ctx, 401, "")
    return
  }
  val roles = "/latest/meta-data/iam/security-credentials/"
  if path == roles {
    if fake.mode == "imds-no-role" {
      respondText(ctx, 404, "")
    } else {
      respondText(ctx, 200, "lyric-instance-role\n")
    }
  } else if path == roles + "lyric-instance-role" {
    if fake.mode == "imds-bad-json" {
      respondJson(ctx, 200, "{\"Code\": \"Success\", \"AccessKeyId\": \"ASIAIMDS\"}")
    } else {
      respondJson(ctx, 200, credentialsJson("ASIAIMDS"))
    }
  } else {
    respondText(ctx, 404, "")
  }
}

func serveContainer(ctx: in HttpContext, path: in String): Unit {
  if path == "/v2/credentials/open" {
    respondJson(ctx, 200, credentialsJson("ASIAOPEN"))
  } else if headerValue(ctx, "Authorization") != CONTAINER_AUTH {
    respondJson(ctx, 401, "{\"message\": \"unauthorized\"}")
  } else if path == "/v2/credentials/task" {
    respondJson(ctx, 200, credentialsJson("ASIACONTAINER"))
  } else if path == "/v2/credentials/garbage" {
    respondText(ctx, 200, "<html>" + SECRET + "</html>")
  } else {
    respondJson(ctx, 404, "{}")
  }
}

func formValue(body: in String, name: in String): String {
  val parts = Str.split(body, "&")
  var i = 0
  while i < parts.length {
    val eq = Str.indexOfRaw(parts[i], "=")
    if eq > 0 and parts[i].substring(0, eq) == name {
      return urlDecode(parts[i].substring(eq + 1))
    }
    i = i + 1
  }
  ""
}

func stsError(ctx: in HttpContext, status: in Int, code: in String, message: in String): Unit {
  val headers: Map[String, String] = newMap()
  respondBytesWithHeaders(
    ctx,
    status,
    "text/xml",
    Enc.encodeUtf8(
      "<ErrorResponse xmlns=\"https://sts.amazonaws.com/doc/2011-06-15/\"><Error><Type>Sender</Type><Code>" + code +
        "</Code><Message>" +
        message +
        "</Message></Error><RequestId>fixture-request</RequestId></ErrorResponse>"
    ),
    headers
  )
}

func serveSts(ctx: in HttpContext): Unit {
  val body = requestBody(ctx)
  if requestMethod(ctx) != "POST" or not Str.startsWith(headerValue(ctx, "Content-Type"), "application/x-www-form-urlencoded") {
    stsError(ctx, 400, "InvalidRequest", "expected a form POST")
    return
  }
  if headerValue(ctx, "Authorization").length > 0 {
    stsError(ctx, 400, "InvalidRequest", "AssumeRoleWithWebIdentity is not signed")
    return
  }
  if formValue(body, "Action") != "AssumeRoleWithWebIdentity" or formValue(body, "Version") != "2011-06-15" {
    stsError(ctx, 400, "InvalidAction", "unknown action or version")
    return
  }
  if formValue(body, "RoleArn") != ROLE_ARN {
    stsError(ctx, 403, "AccessDenied", "Not authorized to perform sts:AssumeRoleWithWebIdentity")
    return
  }
  val token = formValue(body, "WebIdentityToken")
  if token == "fixture-expired-oidc" {
    stsError(
      ctx,
      400,
      "ExpiredTokenException",
      "Token expired: current date/time must be before the expiration date/time"
    )
    return
  }
  if token == "echoed" {
    stsError(ctx, 400, "InvalidIdentityToken", "the token echoed is not valid")
    return
  }
  if token != OIDC_TOKEN {
    stsError(ctx, 400, "InvalidIdentityToken", "The ID Token provided is not a valid JWT")
    return
  }
  val headers: Map[String, String] = newMap()
  respondBytesWithHeaders(
    ctx,
    200,
    "text/xml",
    Enc.encodeUtf8(
      "<AssumeRoleWithWebIdentityResponse xmlns=\"https://sts.amazonaws.com/doc/2011-06-15/\">\n  <AssumeRoleWithWebIdentityResult>\n    <SubjectFromWebIdentityToken>fixture-subject</SubjectFromWebIdentityToken>\n    <AssumedRoleUser><Arn>arn:aws:sts::123456789012:assumed-role/lyric-test/" +
        formValue(body, "RoleSessionName") +
        "</Arn><AssumedRoleId>AROAFIXTURE:session</AssumedRoleId></AssumedRoleUser>\n    <Credentials>\n      <AccessKeyId>ASIAWEB-" +
        formValue(body, "RoleSessionName") +
        "</AccessKeyId>\n      <SecretAccessKey>" +
        SECRET +
        "</SecretAccessKey>\n      <SessionToken>" +
        SESSION_TOKEN +
        "</SessionToken>\n      <Expiration>" +
        EXPIRATION +
        "</Expiration>\n    </Credentials>\n  </AssumeRoleWithWebIdentityResult>\n  <ResponseMetadata><RequestId>fixture-request</RequestId></ResponseMetadata>\n</AssumeRoleWithWebIdentityResponse>"
    ),
    headers
  )
}

func handle(fake: in Fake, ctx: in HttpContext): Unit {
  fake.requests = fake.requests + 1
  val path = requestPath(ctx)
  if Str.startsWith(path, "/__mode/") {
    fake.mode = path.substring(8)
    respondText(ctx, 200, fake.mode)
  } else if Str.startsWith(path, "/latest/") {
    serveImds(fake, ctx, path)
  } else if Str.startsWith(path, "/v2/credentials/") {
    serveContainer(ctx, path)
  } else if path == "/sts/" {
    serveSts(ctx)
  } else {
    respondText(ctx, 404, "")
  }
}

func serveUntilStopped(l: in HttpListener): Unit {
  val fake = Fake(mode = "", imdsTokens = 0, lastImdsToken = "", requests = 0)
  var running = true
  while running {
    val ctx = nextContext(l)
    if requestPath(ctx) == STOP_PATH {
      respondText(ctx, 200, "served " + fake.requests.toString())
      running = false
    } else {
      try {
        handle(fake, ctx)
      } catch Bug as b {
        respondText(ctx, 500, "fake server bug: " + b.message)
      }
    }
  }
}

@cfg(target = "dotnet")
extern type Worker = "System.Threading.Tasks.Task"

@cfg(target = "dotnet")
@externStatic
@externTarget("System.Threading.Tasks.Task.Run")
func taskRun(work: in () -> Unit): Worker = ()

@cfg(target = "dotnet")
@externInstance
@externTarget("System.Threading.Tasks.Task.Wait")
func taskWait(t: in Worker): Unit = ()

func endpointOf(port: in Int): String {
  "http://127.0.0.1:" + port.toString()
}

@cfg(target = "dotnet")
func withFake(port: in Int, scenario: in (String) -> String): String {
  val l = startListener(endpointOf(port) + "/")
  val server = taskRun({ -> serveUntilStopped(l) })
  val transcript = scenario(endpointOf(port))
  taskWait(server)
  stopListener(l)
  transcript
}

@cfg(target = "jvm")
func withFake(port: in Int, scenario: in (String) -> String): String {
  val l = startListener(endpointOf(port) + "/")
  var transcript = ""
  scope {
    val server = spawn serveUntilStopped(l)
    transcript = scenario(endpointOf(port))
    await server
  }
  stopListener(l)
  transcript
}

func fetch(url: in String): String {
  match await defaultClient().get(url) {
    case Err(e) -> "failed: " + HttpError.message(e)
    case Ok(resp) -> match await HttpResponse.bodyText(resp) {
      case Ok(text) -> HttpResponse.statusCode(resp).toString() + " " + text
      case Err(_) -> "unreadable"
    }
  }
}

func setMode(endpoint: in String, mode: in String): Unit {
  val _ = fetch(endpoint + "/__mode/" + mode)
}

func stopFake(endpoint: in String): String {
  fetch(endpoint + STOP_PATH)
}

func describe(r: in Result[ResolvedCredentials, CredentialError]): String {
  match r {
    case Ok(c) -> "ok " + c.credentials.accessKeyId + " from " + c.source +
      (if c.credentials.secretAccessKey == SECRET { "" } else { " WRONG-SECRET" }) +
      (if c.credentials.sessionToken == SESSION_TOKEN { "" } else { " WRONG-TOKEN" }) +
      (match c.expiresAtEpochMs { case Some(at) -> " until " + at.toString(); case None -> " no expiry" })
    case Err(e) -> e.code + " " + e.text()
  }
}

func scratchDir(label: in String): String {
  val dir = Env.getVarOrDefault("TMPDIR", "/tmp") + "/lyric-aws-remote-" + label + "-" +
    Time.nowEpochMillis().toString()
  match File.createDir(dir) {
    case Ok(_) -> dir
    case Err(_) -> panic("cannot create " + dir)
  }
}

func write(path: in String, text: in String): Unit {
  match File.writeText(path, text) {
    case Ok(_) -> ()
    case Err(_) -> panic("cannot write " + path)
  }
}

func check(lines: in slice[String], index: in Int, expected: in String, label: in String): Unit {
  assertTrue(index < lines.length, label + ": the scenario produced line " + index.toString())
  assertEqual(lines[index], expected, label)
}

// ─── Instance metadata ───────────────────────────────────────────────────────
func imdsScenario(endpoint: in String): String {
  val acc: List[String] = newList()
  val cfg = ImdsConfig(endpoint = endpoint, disabled = false)
  // The fake is IMDSv2 only: no token, no answer.
  acc.add(fetch(endpoint + "/latest/meta-data/iam/security-credentials/"))
  acc.add(describe(imdsCredentials(cfg)))
  // The same through the environment variable, with a trailing slash.
  val values: Map[String, String] = newMap()
  values["AWS_EC2_METADATA_SERVICE_ENDPOINT"] = endpoint + "/"
  acc.add(describe(imdsProvider(environmentOf(values)).resolve()))
  values["AWS_EC2_METADATA_DISABLED"] = "TRUE"
  acc.add(describe(imdsProvider(environmentOf(values)).resolve()))
  setMode(endpoint, "imds-no-role")
  acc.add(describe(imdsCredentials(cfg)))
  setMode(endpoint, "imds-forbidden")
  acc.add(describe(imdsCredentials(cfg)))
  setMode(endpoint, "imds-bad-json")
  acc.add(describe(imdsCredentials(cfg)))
  acc.add(stopFake(endpoint))
  Str.joinList("\n", acc)
}

test "IMDSv2: token, role name, credentials; and every way it says no" {
  val port = 19521
  val lines = Str.split(withFake(port, { e -> imdsScenario(e) }), "\n")
  check(lines, 0, "401 ", "the fake refuses a request without a session token")
  check(lines, 1, "ok ASIAIMDS from imds until " + EXPIRATION_MS, "the three-step flow")
  check(lines, 2, "ok ASIAIMDS from imds until " + EXPIRATION_MS, "AWS_EC2_METADATA_SERVICE_ENDPOINT")
  check(lines, 3, "NOT_CONFIGURED imds: AWS_EC2_METADATA_DISABLED is true", "AWS_EC2_METADATA_DISABLED, any case")
  check(lines, 4, "NOT_CONFIGURED imds: the instance has no IAM role", "an instance without a role")
  check(
    lines,
    5,
    "UNAVAILABLE imds: the instance metadata service gave no session token: HTTP 403",
    "IMDS refuses tokens"
  )
  check(lines, 6, "MALFORMED_RESPONSE imds: the answer has no SecretAccessKey", "credentials that are not credentials")
  assertTrue(lines.length == 8 and Str.startsWith(lines[7], "200 served "), "the fake stopped cleanly")
}

test "a metadata service that accepts the connection and never answers is UNAVAILABLE after the deadline" {
  // A listener nobody serves: the connection is accepted, the request is
  // never answered.
  val endpoint = endpointOf(19525)
  val l = startListener(endpoint + "/")
  val before = Time.nowEpochMillis()
  val text = describe(imdsCredentials(ImdsConfig(endpoint = endpoint, disabled = false, timeoutMs = 300)))
  val took = Time.nowEpochMillis() - before
  stopListener(l)
  assertEqual(
    text,
    "UNAVAILABLE imds: the instance metadata service is not reachable: no answer from " + endpoint +
      "/latest/api/token within 300 ms",
    "no answer within the deadline"
  )
  assertTrue(took >= 250i64 and took < 3000i64, "the deadline is kept: " + took.toString() + " ms")
}

test "IMDS that is not there is UNAVAILABLE at once" {
  val before = Time.nowEpochMillis()
  val text = describe(imdsCredentials(ImdsConfig(endpoint = "http://127.0.0.1:1", disabled = false)))
  val took = Time.nowEpochMillis() - before
  assertTrue(
    Str.startsWith(
      text,
      "UNAVAILABLE imds: the instance metadata service is not reachable: no answer from http://127.0.0.1:1/latest/api/token"
    ),
    text
  )
  assertTrue(took < 5000i64, "a refused connection took " + took.toString() + " ms")
  assertEqual(
    describe(imdsCredentials(ImdsConfig(endpoint = "ftp://169.254.169.254", disabled = false))),
    "INVALID_CONFIG imds: the metadata endpoint is not an absolute http(s) URL: ftp://169.254.169.254",
    "an endpoint that is not http"
  )
  val values: Map[String, String] = newMap()
  assertEqual(
    imdsConfigFromEnvironment(environmentOf(values)).endpoint,
    "http://169.254.169.254",
    "the default endpoint"
  )
  values["AWS_EC2_METADATA_SERVICE_ENDPOINT_MODE"] = "IPv6"
  assertEqual(imdsConfigFromEnvironment(environmentOf(values)).endpoint, "http://[fd00:ec2::254]", "the IPv6 endpoint")
}

// ─── Container credentials ───────────────────────────────────────────────────
func containerScenario(endpoint: in String): String {
  val acc: List[String] = newList()
  val dir = scratchDir("container")
  val tokenFile = dir + "/token"
  val values: Map[String, String] = newMap()
  val provider = containerProvider(environmentOf(values))
  acc.add(describe(provider.resolve()))
  values["AWS_CONTAINER_CREDENTIALS_FULL_URI"] = endpoint + "/v2/credentials/task"
  acc.add(describe(provider.resolve()))
  values["AWS_CONTAINER_AUTHORIZATION_TOKEN"] = CONTAINER_AUTH
  acc.add(describe(provider.resolve()))
  // The token file wins over the variable, and is read on every resolve.
  write(tokenFile, "Bearer wrong\n")
  values["AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE"] = tokenFile
  acc.add(describe(provider.resolve()))
  write(tokenFile, CONTAINER_AUTH + "\n")
  values["AWS_CONTAINER_AUTHORIZATION_TOKEN"] = ""
  acc.add(describe(provider.resolve()))
  values["AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE"] = dir + "/missing"
  acc.add(describe(provider.resolve()))
  // A relative URI is served by the ECS agent; here the fake stands in.
  val relative = ContainerConfig(
    relativeUri = "/v2/credentials/open",
    fullUri = endpoint + "/v2/credentials/task",
    authorizationToken = "",
    authorizationTokenFile = "",
    relativeBase = endpoint + "/"
  )
  acc.add(describe(containerProviderOf(relative).resolve()))
  acc.add(describe(containerCredentials(relative.copy(relativeUri = "v2/credentials/open"))))
  val garbage = describe(
    containerCredentials(
      relative.copy(
        relativeUri = "",
        fullUri = endpoint + "/v2/credentials/garbage",
        authorizationToken = CONTAINER_AUTH
      )
    )
  )
  acc.add(garbage)
  acc.add(if Str.contains(garbage, SECRET) { "leaked" } else { "no secret" })
  acc.add(
    describe(
      containerCredentials(
        relative.copy(relativeUri = "", fullUri = endpoint + "/v2/credentials/task", authorizationToken = "a\nb")
      )
    )
  )
  val _ = File.deleteDirRecursive(dir)
  acc.add(stopFake(endpoint))
  Str.joinList("\n", acc)
}

test "container credentials: full and relative URIs, the authorization token and its file" {
  val lines = Str.split(withFake(19522, { e -> containerScenario(e) }), "\n")
  check(
    lines,
    0,
    "NOT_CONFIGURED container: neither AWS_CONTAINER_CREDENTIALS_RELATIVE_URI nor AWS_CONTAINER_CREDENTIALS_FULL_URI is set",
    "nothing set"
  )
  check(
    lines,
    1,
    "REQUEST_FAILED container: the container credential endpoint answered HTTP 401",
    "no Authorization: the fake refuses"
  )
  check(lines, 2, "ok ASIACONTAINER from container until " + EXPIRATION_MS, "AWS_CONTAINER_AUTHORIZATION_TOKEN")
  check(
    lines,
    3,
    "REQUEST_FAILED container: the container credential endpoint answered HTTP 401",
    "the token file wins, and it is wrong"
  )
  check(lines, 4, "ok ASIACONTAINER from container until " + EXPIRATION_MS, "the token file, rewritten")
  assertTrue(
    lines.length > 5 and Str.startsWith(lines[5], "INVALID_CONFIG container: cannot read the authorization token file "),
    "a token file that does not exist"
  )
  check(lines, 6, "ok ASIAOPEN from container until " + EXPIRATION_MS, "the relative URI wins over the full one")
  check(
    lines,
    7,
    "INVALID_CONFIG container: AWS_CONTAINER_CREDENTIALS_RELATIVE_URI must start with '/'",
    "a relative URI without /"
  )
  check(lines, 8, "MALFORMED_RESPONSE container: the answer is not JSON", "an answer that is not JSON")
  check(lines, 9, "no secret", "and is not quoted")
  check(
    lines,
    10,
    "INVALID_CONFIG container: the authorization token holds a line break",
    "a token that would split a header"
  )
  assertTrue(lines.length == 12 and Str.startsWith(lines[11], "200 served "), "the fake stopped cleanly")
}

test "a full container URI is refused unless it is https, loopback, or an ECS or EKS agent address" {
  assertEqual(containerUriProblem("https://credentials.example.com/creds"), "", "https anywhere")
  assertEqual(containerUriProblem("http://127.0.0.1:8080/creds"), "", "127.0.0.1")
  assertEqual(containerUriProblem("http://127.8.9.10/creds"), "", "all of 127/8")
  assertEqual(containerUriProblem("http://localhost/creds"), "", "localhost")
  assertEqual(containerUriProblem("http://[::1]:8080/creds"), "", "IPv6 loopback")
  assertEqual(containerUriProblem("http://169.254.170.2/v2/credentials/x"), "", "the ECS agent")
  assertEqual(containerUriProblem("http://169.254.170.23/v1/credentials"), "", "the EKS Pod Identity agent")
  assertEqual(containerUriProblem("http://[fd00:ec2::23]/v1/credentials"), "", "the EKS Pod Identity agent, IPv6")
  val refused = "plain http:// is allowed only to a loopback address, 169.254.170.2, 169.254.170.23 and [fd00:ec2::23]"
  assertEqual(containerUriProblem("http://169.254.169.254/latest"), refused, "the instance metadata address")
  assertEqual(containerUriProblem("http://credentials.example.com/creds"), refused, "a name is not resolved")
  assertEqual(containerUriProblem("http://127.0.0.1.example.com/creds"), refused, "a name that starts like loopback")
  assertEqual(containerUriProblem("http://1270.0.0.1/creds"), refused, "not an address in 127/8")
  assertEqual(containerUriProblem("http://user@127.0.0.1/creds"), "it is not an absolute http(s) URL", "userinfo")
  assertEqual(containerUriProblem("file:///etc/passwd"), "it is not an absolute http(s) URL", "another scheme")
  assertEqual(
    describe(
      containerCredentials(
        ContainerConfig(
          relativeUri = "",
          fullUri = "http://10.0.0.5/creds",
          authorizationToken = "",
          authorizationTokenFile = ""
        )
      )
    ),
    "INVALID_CONFIG container: AWS_CONTAINER_CREDENTIALS_FULL_URI is refused: " + refused,
    "the provider refuses before any request"
  )
}

// ─── Web identity ────────────────────────────────────────────────────────────
func webIdentityScenario(endpoint: in String): String {
  val acc: List[String] = newList()
  val dir = scratchDir("web-identity")
  val tokenFile = dir + "/token"
  write(tokenFile, OIDC_TOKEN + "\n")
  val values: Map[String, String] = newMap()
  values["AWS_CONFIG_FILE"] = dir + "/no-config"
  values["AWS_SHARED_CREDENTIALS_FILE"] = dir + "/no-credentials"
  val provider = webIdentityProvider(environmentOf(values))
  acc.add(describe(provider.resolve()))
  values["AWS_ROLE_ARN"] = ROLE_ARN
  acc.add(describe(provider.resolve()))
  values["AWS_WEB_IDENTITY_TOKEN_FILE"] = tokenFile
  values["AWS_ROLE_SESSION_NAME"] = "ci-run-7"
  values["AWS_ENDPOINT_URL_STS"] = endpoint + "/sts"
  acc.add(describe(provider.resolve()))
  // AWS_ENDPOINT_URL is the fallback of AWS_ENDPOINT_URL_STS.
  values["AWS_ENDPOINT_URL_STS"] = ""
  values["AWS_ENDPOINT_URL"] = endpoint + "/sts/"
  acc.add(describe(provider.resolve()))
  // The platform rotates the token file; it is read for every request.
  write(tokenFile, "fixture-expired-oidc")
  acc.add(describe(provider.resolve()))
  write(tokenFile, "not-the-token")
  acc.add(describe(provider.resolve()))
  write(tokenFile, "echoed")
  acc.add(describe(provider.resolve()))
  write(tokenFile, OIDC_TOKEN)
  values["AWS_ROLE_ARN"] = "arn:aws:iam::123456789012:role/other"
  acc.add(describe(provider.resolve()))
  write(tokenFile, "\n")
  acc.add(describe(provider.resolve()))
  values["AWS_WEB_IDENTITY_TOKEN_FILE"] = dir + "/missing"
  acc.add(describe(provider.resolve()))
  // An explicit configuration, with a generated session name left out.
  write(tokenFile, OIDC_TOKEN)
  acc.add(
    describe(
      webIdentityProviderOf(WebIdentityConfig(tokenFile = tokenFile, roleArn = ROLE_ARN, roleSessionName = "explicit", stsEndpoint = endpoint + "/sts")).resolve()
    )
  )
  val _ = File.deleteDirRecursive(dir)
  acc.add(stopFake(endpoint))
  Str.joinList("\n", acc)
}

test "web identity: AssumeRoleWithWebIdentity with the token file, and the errors of STS" {
  val lines = Str.split(withFake(19523, { e -> webIdentityScenario(e) }), "\n")
  check(
    lines,
    0,
    "NOT_CONFIGURED web-identity: AWS_WEB_IDENTITY_TOKEN_FILE and AWS_ROLE_ARN are not set",
    "nothing set"
  )
  check(lines, 1, "INVALID_CONFIG web-identity: AWS_ROLE_ARN is set but AWS_WEB_IDENTITY_TOKEN_FILE is not", "half set")
  check(
    lines,
    2,
    "ok ASIAWEB-ci-run-7 from web-identity until " + EXPIRATION_MS,
    "AWS_ENDPOINT_URL_STS, AWS_ROLE_SESSION_NAME"
  )
  check(lines, 3, "ok ASIAWEB-ci-run-7 from web-identity until " + EXPIRATION_MS, "AWS_ENDPOINT_URL")
  check(
    lines,
    4,
    "REQUEST_FAILED web-identity: AssumeRoleWithWebIdentity was answered with HTTP 400: ExpiredTokenException: Token expired: current date/time must be before the expiration date/time",
    "an expired token: the STS code and message"
  )
  check(
    lines,
    5,
    "REQUEST_FAILED web-identity: AssumeRoleWithWebIdentity was answered with HTTP 400: InvalidIdentityToken: The ID Token provided is not a valid JWT",
    "a token STS does not accept"
  )
  check(
    lines,
    6,
    "REQUEST_FAILED web-identity: AssumeRoleWithWebIdentity was answered with HTTP 400",
    "an error text that repeats the token is not passed on"
  )
  check(
    lines,
    7,
    "REQUEST_FAILED web-identity: AssumeRoleWithWebIdentity was answered with HTTP 403: AccessDenied: Not authorized to perform sts:AssumeRoleWithWebIdentity",
    "a role that may not be assumed"
  )
  assertTrue(
    lines.length > 9 and Str.startsWith(lines[8], "INVALID_CONFIG web-identity: the web identity token file is empty: "),
    "an empty token file"
  )
  assertTrue(
    Str.startsWith(lines[9], "INVALID_CONFIG web-identity: cannot read the web identity token file "),
    "a token file that does not exist"
  )
  check(lines, 10, "ok ASIAWEB-explicit from web-identity until " + EXPIRATION_MS, "an explicit configuration")
  assertTrue(lines.length == 12 and Str.startsWith(lines[11], "200 served "), "the fake stopped cleanly")
}

test "the STS endpoint comes from the region when nothing overrides it, and must be https" {
  val values: Map[String, String] = newMap()
  values["AWS_CONFIG_FILE"] = "/nonexistent/lyric/config"
  values["AWS_SHARED_CREDENTIALS_FILE"] = "/nonexistent/lyric/credentials"
  values["AWS_WEB_IDENTITY_TOKEN_FILE"] = "/var/run/token"
  values["AWS_ROLE_ARN"] = ROLE_ARN
  match webIdentityConfigFromEnvironment(environmentOf(values)) {
    case Err(e) -> assertTrue(false, e.text())
    case Ok(cfg) -> {
      assertEqual(cfg.stsEndpoint, "https://sts.amazonaws.com", "no region: the global endpoint")
      assertTrue(Str.startsWith(cfg.roleSessionName, "lyric-"), "a generated session name")
    }
  }
  values["AWS_DEFAULT_REGION"] = "eu-west-1"
  match webIdentityConfigFromEnvironment(environmentOf(values)) {
    case Err(e) -> assertTrue(false, e.text())
    case Ok(cfg) -> assertEqual(cfg.stsEndpoint, "https://sts.eu-west-1.amazonaws.com", "the regional endpoint")
  }
  assertEqual(
    describe(
      webIdentityCredentials(
        WebIdentityConfig(
          tokenFile = "/var/run/token",
          roleArn = ROLE_ARN,
          roleSessionName = "s",
          stsEndpoint = "http://sts.example.com"
        )
      )
    ),
    "INVALID_CONFIG web-identity: the STS endpoint must use https:// (http:// is allowed only to a loopback address): http://sts.example.com",
    "the token is not sent over plain http"
  )
}

test "an STS answer is read for its Credentials element, and never quoted" {
  match credentialsOfStsXml("<R><Credentials><AccessKeyId>A</AccessKeyId><SecretAccessKey>" + SECRET + "</SecretAccessKey></Credentials></R>") {
    case Ok(_) -> assertTrue(false, "accepted without a session token")
    case Err(message) -> {
      assertEqual(message, "the Credentials element lacks AccessKeyId, SecretAccessKey or SessionToken", "no token")
      assertFalse(Str.contains(message, SECRET), "no secret")
    }
  }
  match credentialsOfStsXml("<R><Credentials><AccessKeyId>A</AccessKeyId><SecretAccessKey>S</SecretAccessKey><SessionToken>T</SessionToken><Expiration>later</Expiration></Credentials></R>") {
    case Ok(_) -> assertTrue(false, "accepted without an expiry")
    case Err(message) -> assertEqual(message, "the Credentials element has no ISO 8601 Expiration", "a bad expiry")
  }
  match credentialsOfStsXml("{\"AccessKeyId\": \"A\"}") {
    case Ok(_) -> assertTrue(false, "accepted JSON")
    case Err(message) -> assertEqual(message, "the answer is not XML", "not XML")
  }
  match credentialsOfStsXml("<R><Other/></R>") {
    case Ok(_) -> assertTrue(false, "accepted without credentials")
    case Err(message) -> assertEqual(message, "the answer has no Credentials element", "no Credentials")
  }
}

// ─── The default chain ───────────────────────────────────────────────────────
func chainScenario(endpoint: in String): String {
  val acc: List[String] = newList()
  val dir = scratchDir("chain")
  val tokenFile = dir + "/token"
  write(tokenFile, OIDC_TOKEN)
  val credentialsPath = dir + "/credentials"
  write(
    credentialsPath,
    "[default]\naws_access_key_id = AKIAPROFILE\naws_secret_access_key = " + SECRET + "\naws_session_token = " +
      SESSION_TOKEN +
      "\n"
  )
  val values: Map[String, String] = newMap()
  // Everything is configured: each provider has credentials to give.
  values["AWS_ACCESS_KEY_ID"] = "AKIAENV"
  values["AWS_SECRET_ACCESS_KEY"] = SECRET
  values["AWS_SESSION_TOKEN"] = SESSION_TOKEN
  values["AWS_WEB_IDENTITY_TOKEN_FILE"] = tokenFile
  values["AWS_ROLE_ARN"] = ROLE_ARN
  values["AWS_ROLE_SESSION_NAME"] = "chain"
  values["AWS_ENDPOINT_URL_STS"] = endpoint + "/sts"
  values["AWS_CONFIG_FILE"] = dir + "/config"
  values["AWS_SHARED_CREDENTIALS_FILE"] = credentialsPath
  values["AWS_CONTAINER_CREDENTIALS_FULL_URI"] = endpoint + "/v2/credentials/open"
  values["AWS_EC2_METADATA_SERVICE_ENDPOINT"] = endpoint
  val chain = defaultChain(environmentOf(values))
  acc.add(describe(chain.resolve()))
  values["AWS_ACCESS_KEY_ID"] = ""
  values["AWS_SECRET_ACCESS_KEY"] = ""
  acc.add(describe(chain.resolve()))
  values["AWS_WEB_IDENTITY_TOKEN_FILE"] = ""
  values["AWS_ROLE_ARN"] = ""
  acc.add(describe(chain.resolve()))
  val _ = File.deleteFile(credentialsPath)
  acc.add(describe(chain.resolve()))
  values["AWS_CONTAINER_CREDENTIALS_FULL_URI"] = ""
  acc.add(describe(chain.resolve()))
  // Nothing left: every provider's reason, in order.
  values["AWS_EC2_METADATA_DISABLED"] = "true"
  acc.add(describe(chain.resolve()))
  // A provider that is configured but fails ends the chain: the container
  // endpoint's credentials are not used in place of the web identity's.
  values["AWS_WEB_IDENTITY_TOKEN_FILE"] = dir + "/missing-token"
  values["AWS_ROLE_ARN"] = ROLE_ARN
  values["AWS_CONTAINER_CREDENTIALS_FULL_URI"] = endpoint + "/v2/credentials/open"
  acc.add(describe(chain.resolve()))
  val _ = File.deleteDirRecursive(dir)
  acc.add(dir)
  acc.add(stopFake(endpoint))
  Str.joinList("\n", acc)
}

test "the default chain asks the environment, web identity, the profile, the container endpoint and IMDS, in that order" {
  val lines = Str.split(withFake(19524, { e -> chainScenario(e) }), "\n")
  assertEqualInt(lines.length, 9, "the scenario's lines")
  val dir = lines[7]
  check(lines, 0, "ok AKIAENV from environment no expiry", "1: the environment")
  check(lines, 1, "ok ASIAWEB-chain from web-identity until " + EXPIRATION_MS, "2: web identity")
  check(lines, 2, "ok AKIAPROFILE from profile no expiry", "3: the shared files")
  check(lines, 3, "ok ASIAOPEN from container until " + EXPIRATION_MS, "4: the container endpoint")
  check(lines, 4, "ok ASIAIMDS from imds until " + EXPIRATION_MS, "5: instance metadata")
  check(
    lines,
    5,
    "NO_CREDENTIALS chain: no provider has credentials: environment: AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are not set; web-identity: AWS_WEB_IDENTITY_TOKEN_FILE and AWS_ROLE_ARN are not set; profile: there is no profile 'default' in " +
      dir +
      "/config or " +
      dir +
      "/credentials; container: neither AWS_CONTAINER_CREDENTIALS_RELATIVE_URI nor AWS_CONTAINER_CREDENTIALS_FULL_URI is set; imds: AWS_EC2_METADATA_DISABLED is true",
    "none: all five reasons"
  )
  check(
    lines,
    6,
    "INVALID_CONFIG web-identity: cannot read the web identity token file " + dir + "/missing-token",
    "a broken provider is not skipped"
  )
  assertTrue(Str.startsWith(lines[8], "200 served "), "the fake stopped cleanly")
}
