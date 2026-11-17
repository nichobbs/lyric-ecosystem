//! AwsSecrets.Client - a signed AWS JSON 1.1 client over `Std.Http`, for the
//! two services this package reads: Secrets Manager and Systems Manager
//! Parameter Store.
//!
//! Every action is a `POST /` with `X-Amz-Target: <prefix>.<Action>` and a JSON
//! body, signed with AWS Signature Version 4 (`Aws.SigV4`). There is no AWS SDK
//! underneath, so the same code runs on .NET, the JVM and native.
//!
//! Failures are `ServiceError` values. `code` is the service's error code
//! (`ResourceNotFoundException`, `ParameterNotFound`, `AccessDeniedException`);
//! a failed response without an error document is `HTTP_<status>`; a request
//! that was not answered in time is `TIMEOUT`, one that failed before a response
//! arrived (refused, reset, TLS) `CONNECTION_FAILED`, a configuration that cannot
//! be used `INVALID_CONFIG`, no credentials `CREDENTIALS_UNAVAILABLE`, and a
//! successful response that is not a JSON object `MALFORMED_RESPONSE`.
//!
//! The configuration is the AWS SDKs' own (`clientFromEnvironment`):
//! credentials from the default chain (`Aws.Credentials.Chain`: environment,
//! web identity, shared files, container endpoint, instance metadata), the
//! region from `AWS_REGION`, `AWS_DEFAULT_REGION` or the profile, an optional
//! endpoint from `AWS_ENDPOINT_URL_<SERVICE>` or `AWS_ENDPOINT_URL`, and an
//! optional certificate authority from `AWS_CA_BUNDLE`.
//!
//! A failed request is sent again, signed afresh, as the `Aws.Retry` policy says
//! (`withRetry`). Both actions this package sends are reads, so every failure the
//! policy treats as transient is retried.
@runtime_checked
@io
package AwsSecrets.Client

import Std.Core
import Std.Collections
import Std.Encoding as Enc
import Std.Errors
import Std.File as File
import Std.Http
import Std.JsonValue
import Std.String as Str
import Std.Task
import Std.Tls as Tls
import Aws.Credentials
import Aws.Credentials.Chain
import Aws.Credentials.Profile
import Aws.Retry
import Aws.SigV4
import Aws.SigV4.Env as AwsEnv

/// The default per-request deadline, 30 seconds.
val DEFAULT_REQUEST_TIMEOUT_MS: Int = 30000

/// A failed AWS call.
pub record ServiceError {
  /// The service's error code, or one of this client's own (module
  /// documentation).
  code: String
  message: String
  /// The HTTP status of the response, 0 when there was none.
  status: Int
}

/// The error as one line of text.
@pure
pub func ServiceError.text(e: in ServiceError): String {
  e.message
}

/// The facts about a service this client needs: how it is signed, named in a
/// host name and the environment, and addressed in `X-Amz-Target`.
pub record Service {
  /// The SigV4 signing name: `secretsmanager`, `ssm`.
  signingName: String
  /// The first label of the AWS host name: `secretsmanager`, `ssm`.
  hostPrefix: String
  /// The service identifier of `AWS_ENDPOINT_URL_<ID>`: `SECRETS_MANAGER`, `SSM`.
  envId: String
  /// The `X-Amz-Target` prefix: `secretsmanager`, `AmazonSSM`.
  targetPrefix: String
}

/// AWS Secrets Manager.
pub func secretsManagerService(): Service {
  Service(
    signingName = "secretsmanager",
    hostPrefix = "secretsmanager",
    envId = "SECRETS_MANAGER",
    targetPrefix = "secretsmanager"
  )
}

/// AWS Systems Manager (Parameter Store).
pub func parameterStoreService(): Service {
  Service(signingName = "ssm", hostPrefix = "ssm", envId = "SSM", targetPrefix = "AmazonSSM")
}

/// Where a client talks to and how it signs. `endpoint` is empty for the
/// region's AWS endpoint, or the base URL of a compatible server; it must be
/// `https://` unless the host is loopback.
pub record ClientConfig {
  region: String
  endpoint: String
  accessKeyId: String
  secretAccessKey: String
  sessionToken: String
  /// Where the credentials come from; `None` uses the three fields above.
  credentialProvider: Option[CredentialProvider] = None
  requestTimeoutMs: Int = DEFAULT_REQUEST_TIMEOUT_MS
  retry: RetryPolicy = standardRetry()
  /// A certificate authority trusted in addition to the system trust store
  /// (PEM text); `""` adds none.
  caCertificatePem: String = ""
}

/// Configuration with static credentials for `region`, and `endpoint` unless it
/// is empty.
pub func staticConfig(
  region: in String, endpoint: in String, accessKeyId: in String, secretAccessKey: in String
): ClientConfig {
  ClientConfig(
    region = region,
    endpoint = endpoint,
    accessKeyId = accessKeyId,
    secretAccessKey = secretAccessKey,
    sessionToken = ""
  )
}

/// `cfg` signing with the credentials `provider` returns, asked before every
/// attempt.
pub func ClientConfig.withCredentialProvider(cfg: in ClientConfig, provider: in CredentialProvider): ClientConfig {
  cfg.copy(credentialProvider = Some(value = provider))
}

/// `cfg` retrying as `policy` says.
pub func ClientConfig.withRetry(cfg: in ClientConfig, policy: in RetryPolicy): ClientConfig {
  cfg.copy(retry = policy)
}

/// `cfg` with a per-request deadline in milliseconds.
pub func ClientConfig.withRequestTimeout(cfg: in ClientConfig, requestTimeoutMs: in Int): ClientConfig {
  cfg.copy(requestTimeoutMs = requestTimeoutMs)
}

/// `cfg` trusting the certificate authority `caPem` (PEM text) in addition to
/// the system trust store.
pub func ClientConfig.withCaCertificate(cfg: in ClientConfig, caPem: in String): ClientConfig {
  cfg.copy(caCertificatePem = caPem)
}

func invalidConfig(message: in String): ServiceError {
  ServiceError(code = "INVALID_CONFIG", message = message, status = 0)
}

func transportError(message: in String): ServiceError {
  ServiceError(code = "CONNECTION_FAILED", message = message, status = 0)
}

/// A connection to one service endpoint. Build one with `openClient` or
/// `clientFromEnvironment`.
pub record AwsClient {
  service: Service
  region: String
  credentials: CredentialProvider
  retry: RetryPolicy
  /// `https://host[:port]`, no trailing slash.
  origin: String
  /// The authority sent as `Host` and signed.
  host: String
  requestTimeoutMs: Int
  http: HttpClient
}

func isLowerOrDigit(u: in Int): Bool {
  (u >= 97 and u <= 122) or (u >= 48 and u <= 57)
}

/// True for a plausible region name (`us-east-1`): lowercase letters, digits and
/// `-`, so it cannot alter the host name or credential scope it is built into.
@pure
pub func isValidRegion(region: in String): Bool {
  if region.length == 0 {
    return false
  }
  var i = 0
  while i < region.length {
    val u = Str.codeUnitAt(region, i)
    if not (isLowerOrDigit(u) or u == 45) {
      return false
    }
    i = i + 1
  }
  true
}

func isAuthorityUnit(u: in Int): Bool {
  isLowerOrDigit(u) or (u >= 65 and u <= 90) or u == 45 or u == 46 or u == 58 or u == 91 or u == 93
}

/// An endpoint split into `scheme` and `authority`; the default port is dropped
/// so the signed host is the `Host` header an HTTP client sends.
pub record Endpoint {
  scheme: String
  authority: String
}

/// True when the `host[:port]` authority names the loopback interface.
@pure
pub func isLoopbackAuthority(authority: in String): Bool {
  var host = authority
  if Str.startsWith(host, "[") {
    val close = Str.lastIndexOfRaw(host, "]")
    host = if close > 0 { host.substring(0, close + 1) } else { host }
  } else {
    val colon = Str.lastIndexOfRaw(host, ":")
    host = if colon >= 0 { host.substring(0, colon) } else { host }
  }
  host == "localhost" or host == "127.0.0.1" or host == "[::1]"
}

/// Parse an endpoint: `http(s)://host[:port]` with nothing after the authority
/// but an optional `/`. `http://` is accepted only for a loopback host. `Err`
/// carries the reason.
@pure
pub func parseEndpoint(raw: in String): Result[Endpoint, String] {
  var scheme = ""
  var rest = ""
  if Str.startsWith(raw, "https://") {
    scheme = "https"
    rest = raw.substring(8)
  } else if Str.startsWith(raw, "http://") {
    scheme = "http"
    rest = raw.substring(7)
  } else {
    return Err("the endpoint must be an absolute http(s) URL: " + raw)
  }
  if Str.endsWith(rest, "/") {
    rest = rest.substring(0, rest.length - 1)
  }
  if rest.length == 0 {
    return Err("the endpoint has no host: " + raw)
  }
  var i = 0
  while i < rest.length {
    if not isAuthorityUnit(Str.codeUnitAt(rest, i)) {
      return Err("the endpoint must be scheme://host[:port], with no path, credentials, query or fragment: " + raw)
    }
    i = i + 1
  }
  var authority = Str.toLower(rest)
  if scheme == "https" and Str.endsWith(authority, ":443") {
    authority = authority.substring(0, authority.length - 4)
  } else if scheme == "http" and Str.endsWith(authority, ":80") {
    authority = authority.substring(0, authority.length - 3)
  }
  if scheme == "http" and not isLoopbackAuthority(authority) {
    return Err("the endpoint must use https:// (http:// is allowed only for localhost, 127.0.0.1 and [::1]): " + raw)
  }
  Ok(Endpoint(scheme = scheme, authority = authority))
}

func httpClientFor(cfg: in ClientConfig): Result[HttpClient, ServiceError] {
  if cfg.caCertificatePem.length == 0 {
    return Ok(clientNoRedirects())
  }
  if not tlsConfigSupported() {
    return Err(invalidConfig("a CA certificate cannot be configured on this target"))
  }
  match Tls.Certificate.fromPem(cfg.caCertificatePem) {
    case Err(e) -> Err(invalidConfig("the CA certificate is not usable: " + Tls.TlsError.message(e)))
    case Ok(cert) -> Ok(HttpClientBuilder.new().withNoRedirects().withCaCertificate(cert).build())
  }
}

/// Validate `cfg` and open a client for `service`. No request is made: a wrong
/// credential shows up on the first call.
///
/// `Err(INVALID_CONFIG)` for missing credentials when there is no provider, a
/// malformed or plain-`http://` non-loopback endpoint, a missing or invalid
/// region, a non-positive timeout, or an unusable CA certificate.
pub func openClient(service: in Service, cfg: in ClientConfig): Result[AwsClient, ServiceError] {
  val credentials = match cfg.credentialProvider {
    case Some(provider) -> provider
    case None -> {
      if cfg.accessKeyId.length == 0 or cfg.secretAccessKey.length == 0 {
        return Err(invalidConfig("the AWS access key id and secret access key must be non-empty"))
      }
      staticProviderOf(
        Credentials(accessKeyId = cfg.accessKeyId, secretAccessKey = cfg.secretAccessKey, sessionToken = cfg.sessionToken)
      )
    }
  }
  if cfg.retry.maxAttempts < 1 {
    return Err(invalidConfig("retry.maxAttempts must be >= 1, got " + cfg.retry.maxAttempts.toString()))
  }
  if cfg.requestTimeoutMs <= 0 {
    return Err(invalidConfig("requestTimeoutMs must be > 0, got " + cfg.requestTimeoutMs.toString()))
  }
  if not isValidRegion(cfg.region) {
    return Err(
      invalidConfig(
        if cfg.region.length == 0 {
          "no AWS region: set AWS_REGION or AWS_DEFAULT_REGION, or `region` in the profile"
        } else {
          "invalid AWS region: '" + cfg.region + "'"
        }
      )
    )
  }
  var endpoint = Endpoint(scheme = "https", authority = service.hostPrefix + "." + cfg.region + ".amazonaws.com")
  if cfg.endpoint.length > 0 {
    endpoint = match parseEndpoint(cfg.endpoint) {
      case Err(message) -> return Err(invalidConfig(message))
      case Ok(e) -> e
    }
  }
  val http = match httpClientFor(cfg) { case Ok(h) -> h; case Err(e) -> return Err(e) }
  Ok(
    AwsClient(
      service = service,
      region = cfg.region,
      credentials = credentials,
      retry = cfg.retry,
      origin = endpoint.scheme + "://" + endpoint.authority,
      host = endpoint.authority,
      requestTimeoutMs = cfg.requestTimeoutMs,
      http = http
    )
  )
}

func readPem(path: in String): Result[String, ServiceError] {
  match File.readText(path) {
    case Ok(text) -> if text.length > 0 { Ok(text) } else { Err(invalidConfig("the CA bundle is empty: " + path)) }
    case Err(_) -> Err(invalidConfig("cannot read the CA bundle: " + path))
  }
}

/// A client for `service` as the AWS SDKs would configure it, reading variables
/// through `lookup`: credentials from the default chain, the region, an
/// optional endpoint and an optional CA bundle (module documentation).
/// Credentials from the environment are static; any other source is kept as a
/// cached provider, which refreshes temporary credentials before they expire.
///
/// `Err(INVALID_CONFIG)` says what is missing; for credentials it holds the
/// reason of every provider of the chain.
pub func clientFromEnvironment(service: in Service, lookup: in (String) -> String): Result[AwsClient, ServiceError] {
  val provider = cached(defaultChain(lookup))
  val resolved = match provider.resolve() {
    case Ok(c) -> c
    case Err(e) -> return Err(invalidConfig("no AWS credentials: " + e.text()))
  }
  val settings = match settingsFromEnvironment(lookup, service.envId) {
    case Ok(s) -> s
    case Err(e) -> return Err(invalidConfig(e.text()))
  }
  var cfg = ClientConfig(
    region = settings.region,
    endpoint = settings.endpointUrl,
    accessKeyId = "",
    secretAccessKey = "",
    sessionToken = ""
  )
  cfg = cfg.withCredentialProvider(provider)
  if settings.caBundlePath.length > 0 {
    val pem = match readPem(settings.caBundlePath) { case Ok(p) -> p; case Err(e) -> return Err(e) }
    cfg = cfg.withCaCertificate(pem)
  }
  openClient(service, cfg)
}

/// True when `cause`, the message of an `HttpError.ConnectionFailed`, is one of
/// the texts the host HTTP client uses for a request that ran out of time.
@pure
pub func isTimeoutCause(cause: in String): Bool {
  cause == "request timed out" or cause == "HTTP connect timed out" or Str.contains(cause, "HttpClient.Timeout")
}

// What one attempt came to.
record Attempt {
  outcome: Result[JsonValue, ServiceError]
  kind: FailureKind
  retryAfterMs: Int
}

func failedAttempt(e: in ServiceError, kind: in FailureKind): Attempt {
  Attempt(outcome = Err(e), kind = kind, retryAfterMs = -1)
}

union Sent {
  case Answered(response: HttpResponse)
  case Unanswered(failed: Attempt)
}

func sendBounded(client: in AwsClient, req: in HttpRequest, action: in String): Sent {
  val src = makeCancelSourceTimeout(client.requestTimeoutMs)
  val tok = sourceToken(src)
  val sent = await client.http.sendWithCancel(req, tok)
  val deadlinePassed = isCancelled(tok)
  disposeSource(src)
  match sent {
    case Ok(resp) -> Answered(response = resp)
    case Err(e) -> {
      val timedOut = deadlinePassed or
        match e {
          case ConnectionFailed(_, cause) -> isTimeoutCause(cause)
          case Timeout(_, _) -> true
          case _ -> false
        }
      if timedOut {
        Unanswered(
          failed = failedAttempt(
            ServiceError(
              code = "TIMEOUT",
              message = action + " timed out after " + client.requestTimeoutMs.toString() + " ms at " + client.origin,
              status = 0
            ),
            FailureKind.TimedOut
          )
        )
      } else {
        val cause = HttpError.message(e)
        Unanswered(
          failed = failedAttempt(
            transportError(action + " got no response from " + client.origin + ": " + cause),
            if isConnectFailure(cause) { FailureKind.ConnectFailed } else { FailureKind.NoResponse }
          )
        )
      }
    }
  }
}

/// The error code and message of a failed response's JSON body, or `None` when
/// it is not an error document. The code is what follows the last `#` of
/// `__type` (`com.amazonaws.ssm#ParameterNotFound`).
@pure
pub func parseFault(body: in String): Option[ServiceError] {
  val doc = match parseValue(body) { case Ok(v) -> v; case Err(_) -> return None }
  val kind = match getString(doc, "__type") {
    case Some(t) -> t
    case None -> match getString(doc, "code") { case Some(c) -> c; case None -> return None }
  }
  val hash = Str.lastIndexOfRaw(kind, "#")
  val code = if hash >= 0 { kind.substring(hash + 1) } else { kind }
  if code.length == 0 {
    return None
  }
  val message = match getString(doc, "message") {
    case Some(m) -> m
    case None -> match getString(doc, "Message") { case Some(m) -> m; case None -> "" }
  }
  Some(value = ServiceError(code = code, message = message, status = 0))
}

/// The `ServiceError` for a response that is not a 200: the service's error
/// code and message when the body is an error document, `HTTP_<status>`
/// otherwise.
@pure
pub func faultOf(action: in String, status: in Int, body: in String): ServiceError {
  match parseFault(body) {
    case Some(fault) -> ServiceError(
      code = fault.code,
      message = action + " failed: " + fault.code + " (HTTP " + status.toString() + ")" +
        (if fault.message.length > 0 { ": " + fault.message } else { "" }),
      status = status
    )
    case None -> ServiceError(
      code = "HTTP_" + status.toString(),
      message = action + " failed: the service answered HTTP " + status.toString(),
      status = status
    )
  }
}

func attempt(client: in AwsClient, creds: in Credentials, action: in String, bytes: in slice[Byte]): Attempt {
  val contentType = "application/x-amz-json-1.1"
  val extra: List[Pair] = newList()
  extra.add(Pair(name = "content-type", value = contentType))
  extra.add(Pair(name = "x-amz-target", value = client.service.targetPrefix + "." + action))
  val noQuery: List[Pair] = newList()
  val signed = signedHeaders(
    "POST",
    canonicalUri("/", false),
    noQuery,
    client.host,
    extra,
    sha256Hex(bytes),
    false,
    creds,
    client.region,
    client.service.signingName,
    AwsEnv.amzDateNow()
  )
  val url = client.origin + "/"
  val parsed = match Url.tryFrom(url) {
    case Ok(u) -> u
    case Err(_) -> return failedAttempt(invalidConfig("cannot build a request URL from " + url), FailureKind.NotRetryable)
  }
  var req = match withBytesBody(request(POST, parsed), contentType, bytes) {
    case Ok(r) -> r
    case Err(e) -> return failedAttempt(invalidConfig(HttpError.message(e)), FailureKind.NotRetryable)
  }
  // `Host` is set by the HTTP client from the URL and `Content-Type` by the
  // body; every other signed header is sent as signed, then `Authorization`.
  var k = 0
  while k < signed.headers.count {
    if signed.headers[k].name != "content-type" {
      match withHeader(req, signed.headers[k].name, signed.headers[k].value) {
        case Ok(r) -> {
          req = r
        }
        case Err(e) -> return failedAttempt(invalidConfig(HttpError.message(e)), FailureKind.NotRetryable)
      }
    }
    k = k + 1
  }
  val resp = match sendBounded(client, req, action) { case Answered(r) -> r; case Unanswered(failed) -> return failed }
  val read = await HttpResponse.bodyBytes(resp)
  val responseBytes = match read {
    case Ok(b) -> b
    case Err(e) -> return failedAttempt(
      transportError(action + ": cannot read the response body: " + IOError.message(e)),
      FailureKind.NoResponse
    )
  }
  val status = HttpResponse.statusCode(resp)
  val responseText = match Enc.tryDecodeUtf8(responseBytes) { case Some(t) -> t; case None -> "" }
  if status != 200 {
    val fault = faultOf(action, status, responseText)
    return Attempt(
      outcome = Err(fault),
      kind = classifyResponse(status, if Str.startsWith(fault.code, "HTTP_") { "" } else { fault.code }),
      retryAfterMs = retryAfterMsOf(
        match HttpResponse.header(resp, "Retry-After") { case Some(v) -> v; case None -> "" },
        match HttpResponse.header(resp, "x-amz-retry-after") { case Some(v) -> v; case None -> "" }
      )
    )
  }
  match parseValue(responseText) {
    case Ok(doc) -> match asObject(doc) {
      case Some(_) -> Attempt(outcome = Ok(doc), kind = FailureKind.NotRetryable, retryAfterMs = -1)
      case None -> failedAttempt(
        ServiceError(
          code = "MALFORMED_RESPONSE",
          message = action + ": the response is not a JSON object",
          status = status
        ),
        FailureKind.NotRetryable
      )
    }
    case Err(e) -> failedAttempt(
      ServiceError(
        code = "MALFORMED_RESPONSE",
        message = action + ": the response is not JSON: " + e.message(),
        status = status
      ),
      FailureKind.NotRetryable
    )
  }
}

/// The credentials to sign with now, from the client's provider.
/// `Err(CREDENTIALS_UNAVAILABLE)` when it has none.
pub func AwsClient.currentCredentials(client: in AwsClient): Result[Credentials, ServiceError] {
  val resolved = resolveGuarded(client.credentials)
  match resolved {
    case Ok(c) -> Ok(c.credentials)
    case Err(e) -> Err(
      ServiceError(
        code = "CREDENTIALS_UNAVAILABLE",
        message = "no AWS credentials: " + e.text(),
        status = 0
      )
    )
  }
}

/// Sign and send one read action and return the response document, sending it
/// again as the client's retry policy says. `action` is the action name
/// (`GetSecretValue`), `body` the JSON request. The error of the last attempt
/// is returned.
pub func AwsClient.call(client: in AwsClient, action: in String, body: in String): Result[JsonValue, ServiceError] {
  val bytes = Enc.encodeUtf8(body)
  var number = 1
  while true {
    // Asked again for every attempt: a retry is signed with the credentials
    // and the date of its own moment.
    val creds = match client.currentCredentials() { case Ok(c) -> c; case Err(e) -> return Err(e) }
    val made = attempt(client, creds, action, bytes)
    match client.retry.decide(number, made.kind, true, made.retryAfterMs) {
      case GiveUp -> return made.outcome
      case RetryAfter(delayMs) -> client.retry.pause(delayMs)
    }
    number = number + 1
  }
  Err(invalidConfig("unreachable"))
}
