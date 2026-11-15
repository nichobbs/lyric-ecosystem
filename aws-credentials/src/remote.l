//! Aws.Credentials.Remote - the providers that ask a service for temporary
//! credentials: the container credential endpoint (ECS, EKS Pod Identity),
//! the EC2 instance metadata service (IMDSv2) and STS
//! `AssumeRoleWithWebIdentity` (EKS IRSA, GitHub OIDC).
//!
//! Each provider is configured by a record; `...FromEnvironment` builds the
//! record from the variables the AWS SDKs define, on every `resolve`. The
//! requests are made with `Std.Http`; none is signed, and none follows a
//! redirect.
//!
//! No error message holds a credential, an authorization token or a web
//! identity token.
@runtime_checked
@io
package Aws.Credentials.Remote

import Std.Core
import Std.Collections
import Std.Encoding as Enc
import Std.Errors
import Std.File as File
import Std.Http
import Std.String as Str
import Std.Task
import Std.Tls as Tls
import Std.Xml
import Aws.SigV4
import Aws.Credentials
import Aws.Credentials.Profile

func failure(provider: in String, code: in String, message: in String): CredentialError {
  CredentialError(code = code, provider = provider, message = message)
}

// ─── HTTP ────────────────────────────────────────────────────────────────────
record Reply {
  status: Int
  body: String
}

union CallError {
  // No response: refused, unreachable, or out of time.
  case NoResponse(message: String)
  // The request could not be built.
  case BadRequest(message: String)
}

func isTimeoutCause(cause: in String): Bool {
  cause == "request timed out" or cause == "HTTP connect timed out" or Str.contains(cause, "HttpClient.Timeout")
}

// One request, awaited in place. `Std.Http` reports every transport failure
// as `Err` on all three targets, so nothing is caught here.
func call(
  http: in HttpClient,
  method: in HttpMethod,
  url: in String,
  headers: in List[Pair],
  form: in Option[String],
  timeoutMs: in Int,
): Result[Reply, CallError] {
  val parsed = match Url.tryFrom(url) {
    case Ok(u) -> u
    case Err(_) -> return Err(BadRequest(message = "not a usable URL: " + url))
  }
  var req = request(method, parsed)
  match form {
    case Some(text) -> {
      req = match withBytesBody(req, "application/x-www-form-urlencoded", Enc.encodeUtf8(text)) {
        case Ok(r) -> r
        case Err(_) -> return Err(BadRequest(message = "the request body cannot be sent"))
      }
    }
    case None -> ()
  }
  var k = 0
  while k < headers.count {
    req = match withHeader(req, headers[k].name, headers[k].value) {
      case Ok(r) -> r
      case Err(_) -> return Err(BadRequest(message = "the header " + headers[k].name + " cannot be sent"))
    }
    k = k + 1
  }
  val src = makeCancelSourceTimeout(timeoutMs)
  val tok = sourceToken(src)
  val sent = await http.sendWithCancel(req, tok)
  val deadlinePassed = isCancelled(tok)
  disposeSource(src)
  val resp = match sent {
    case Ok(r) -> r
    case Err(e) -> {
      val timedOut = deadlinePassed or
        match e {
          case ConnectionFailed(_, cause) -> isTimeoutCause(cause)
          case Timeout(_, _) -> true
          case _ -> false
        }
      return Err(
        NoResponse(
          message = if timedOut {
            "no answer from " + url + " within " + timeoutMs.toString() + " ms"
          } else {
            "no answer from " + url + ": " + HttpError.message(e)
          }
        )
      )
    }
  }
  val read = await HttpResponse.bodyText(resp)
  match read {
    case Ok(text) -> Ok(Reply(status = HttpResponse.statusCode(resp), body = text))
    case Err(e) -> Err(NoResponse(message = "the answer of " + url + " could not be read: " + IOError.message(e)))
  }
}

// `scheme://authority` split from the rest; the authority lowercased.
record Target {
  scheme: String
  // Host without port or brackets, lowercase.
  host: String
}

func targetOf(url: in String): Option[Target] {
  var scheme = ""
  var rest = ""
  if Str.startsWith(url, "https://") {
    scheme = "https"
    rest = url.substring(8)
  } else if Str.startsWith(url, "http://") {
    scheme = "http"
    rest = url.substring(7)
  } else {
    return None
  }
  var stop = rest.length
  var i = 0
  while i < rest.length {
    val c = Str.charAtOrReplacement(rest, i)
    if c == '/' or c == '?' or c == '#' {
      stop = i
      break
    }
    i = i + 1
  }
  var authority = Str.toLower(rest.substring(0, stop))
  if authority.length == 0 or Str.contains(authority, "@") {
    return None
  }
  var host = authority
  if Str.startsWith(authority, "[") {
    val close = Str.indexOfRaw(authority, "]")
    if close < 0 {
      return None
    }
    host = authority.substring(1, close - 1)
  } else {
    val colon = Str.lastIndexOfRaw(authority, ":")
    if colon >= 0 {
      host = authority.substring(0, colon)
    }
  }
  if host.length == 0 {
    return None
  }
  Some(value = Target(scheme = scheme, host = host))
}

/// True for a host that names the loopback interface: `localhost`, an IPv4
/// address in `127.0.0.0/8`, or `::1`.
@pure
pub func isLoopbackHost(host: in String): Bool {
  val h = Str.toLower(host)
  if h == "localhost" or h == "::1" or h == "0:0:0:0:0:0:0:1" {
    return true
  }
  val parts = Str.split(h, ".")
  if parts.length != 4 or parts[0] != "127" {
    return false
  }
  var i = 1
  while i < 4 {
    val p = parts[i]
    if p.length == 0 or p.length > 3 {
      return false
    }
    var n = 0
    var k = 0
    while k < p.length {
      val u = Str.codeUnitAt(p, k)
      if u < 48 or u > 57 {
        return false
      }
      n = n * 10 + (u - 48)
      k = k + 1
    }
    if n > 255 {
      return false
    }
    i = i + 1
  }
  true
}

func trimTrailingSlash(url: in String): String {
  if Str.endsWith(url, "/") {
    url.substring(0, url.length - 1)
  } else {
    url
  }
}

// ─── Container credentials ───────────────────────────────────────────────────
val CONTAINER: String = "container"

/// How to reach a container credential endpoint.
pub record ContainerConfig {
  /// `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI`: a path on `relativeBase`. Wins
  /// over `fullUri`.
  relativeUri: String
  /// `AWS_CONTAINER_CREDENTIALS_FULL_URI`.
  fullUri: String
  /// `AWS_CONTAINER_AUTHORIZATION_TOKEN`: sent as `Authorization`.
  authorizationToken: String
  /// `AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE`: a file whose content is sent
  /// as `Authorization`, read for every request. Wins over
  /// `authorizationToken`.
  authorizationTokenFile: String
  /// Where a relative URI is served: the ECS agent.
  relativeBase: String = "http://169.254.170.2"
  timeoutMs: Int = 5000
}

/// The configuration the environment names; all four fields empty when the
/// variables are unset.
pub func containerConfigFromEnvironment(lookup: in (String) -> String): ContainerConfig {
  ContainerConfig(
    relativeUri = lookup("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI"),
    fullUri = lookup("AWS_CONTAINER_CREDENTIALS_FULL_URI"),
    authorizationToken = lookup("AWS_CONTAINER_AUTHORIZATION_TOKEN"),
    authorizationTokenFile = lookup("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE")
  )
}

/// Why a full URI may not be asked for credentials, or `""` when it may. The
/// AWS SDKs' rule: `https://` anywhere; `http://` only to a loopback address,
/// the ECS agent (`169.254.170.2`) or the EKS Pod Identity agent
/// (`169.254.170.23`, `fd00:ec2::23`). The host is not resolved: a name other
/// than `localhost` needs `https://`.
@pure
pub func containerUriProblem(fullUri: in String): String {
  match targetOf(fullUri) {
    case None -> "it is not an absolute http(s) URL"
    case Some(t) -> if t.scheme == "https" or isLoopbackHost(t.host) or t.host == "169.254.170.2" or t.host == "169.254.170.23" or t.host == "fd00:ec2::23" {
      ""
    } else {
      "plain http:// is allowed only to a loopback address, 169.254.170.2, 169.254.170.23 and [fd00:ec2::23]"
    }
  }
}

/// The credentials of the container endpoint `cfg` names. `NOT_CONFIGURED`
/// when neither URI is set.
pub func containerCredentials(cfg: in ContainerConfig): Result[ResolvedCredentials, CredentialError] {
  var url = ""
  if cfg.relativeUri.length > 0 {
    if not Str.startsWith(cfg.relativeUri, "/") {
      return Err(failure(CONTAINER, "INVALID_CONFIG", "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI must start with '/'"))
    }
    url = trimTrailingSlash(cfg.relativeBase) + cfg.relativeUri
  } else if cfg.fullUri.length > 0 {
    val problem = containerUriProblem(cfg.fullUri)
    if problem.length > 0 {
      return Err(failure(CONTAINER, "INVALID_CONFIG", "AWS_CONTAINER_CREDENTIALS_FULL_URI is refused: " + problem))
    }
    url = cfg.fullUri
  } else {
    return Err(
      failure(
        CONTAINER,
        "NOT_CONFIGURED",
        "neither AWS_CONTAINER_CREDENTIALS_RELATIVE_URI nor AWS_CONTAINER_CREDENTIALS_FULL_URI is set"
      )
    )
  }
  var token = cfg.authorizationToken
  if cfg.authorizationTokenFile.length > 0 {
    token = match File.readText(cfg.authorizationTokenFile) {
      case Ok(text) -> Str.trim(text)
      case Err(_) -> return Err(
        failure(
          CONTAINER,
          "INVALID_CONFIG",
          "cannot read the authorization token file " + cfg.authorizationTokenFile
        )
      )
    }
  }
  if Str.contains(token, "\r") or Str.contains(token, "\n") {
    return Err(failure(CONTAINER, "INVALID_CONFIG", "the authorization token holds a line break"))
  }
  val headers: List[Pair] = newList()
  if token.length > 0 {
    headers.add(Pair(name = "Authorization", value = token))
  }
  val reply = match call(clientNoRedirects(), GET, url, headers, None, cfg.timeoutMs) {
    case Ok(r) -> r
    case Err(NoResponse(message)) -> return Err(failure(CONTAINER, "REQUEST_FAILED", message))
    case Err(BadRequest(message)) -> return Err(failure(CONTAINER, "INVALID_CONFIG", message))
  }
  if reply.status != 200 {
    return Err(
      failure(
        CONTAINER,
        "REQUEST_FAILED",
        "the container credential endpoint answered HTTP " + reply.status.toString()
      )
    )
  }
  match credentialsOfJson(reply.body, "Token", CONTAINER) {
    case Ok(c) -> Ok(c)
    case Err(message) -> Err(failure(CONTAINER, "MALFORMED_RESPONSE", Str.replace(message, "the output", "the answer")))
  }
}

record ContainerProvider {
  lookup: (String) -> String
}

impl CredentialProvider for ContainerProvider {
  func resolve(): Result[ResolvedCredentials, CredentialError] {
    containerCredentials(containerConfigFromEnvironment(self.lookup))
  }
}

record ContainerConfigProvider {
  cfg: ContainerConfig
}

impl CredentialProvider for ContainerConfigProvider {
  func resolve(): Result[ResolvedCredentials, CredentialError] {
    containerCredentials(self.cfg)
  }
}

/// The container endpoint the environment names.
pub func containerProvider(lookup: in (String) -> String): CredentialProvider {
  val p: CredentialProvider = ContainerProvider(lookup = lookup)
  p
}

/// The container endpoint `cfg` names.
pub func containerProviderOf(cfg: in ContainerConfig): CredentialProvider {
  val p: CredentialProvider = ContainerConfigProvider(cfg = cfg)
  p
}

// ─── EC2 instance metadata (IMDSv2) ──────────────────────────────────────────
val IMDS: String = "imds"

/// How to reach the instance metadata service.
pub record ImdsConfig {
  /// `http://169.254.169.254`, or `AWS_EC2_METADATA_SERVICE_ENDPOINT`.
  endpoint: String
  /// `AWS_EC2_METADATA_DISABLED` is `true`.
  disabled: Bool
  /// The deadline of each of the three requests. Short, so that a machine
  /// that is not an EC2 instance finds out quickly.
  timeoutMs: Int = 1000
  /// The lifetime asked for the session token, 1 to 21600 seconds.
  tokenTtlSeconds: Int = 21600
}

/// The configuration the environment names: `AWS_EC2_METADATA_DISABLED`,
/// `AWS_EC2_METADATA_SERVICE_ENDPOINT`, else the IPv6 endpoint when
/// `AWS_EC2_METADATA_SERVICE_ENDPOINT_MODE` is `IPv6`, else
/// `http://169.254.169.254`.
pub func imdsConfigFromEnvironment(lookup: in (String) -> String): ImdsConfig {
  var endpoint = lookup("AWS_EC2_METADATA_SERVICE_ENDPOINT")
  if endpoint.length == 0 {
    endpoint = if Str.toLower(lookup("AWS_EC2_METADATA_SERVICE_ENDPOINT_MODE")) == "ipv6" {
      "http://[fd00:ec2::254]"
    } else {
      "http://169.254.169.254"
    }
  }
  ImdsConfig(endpoint = endpoint, disabled = Str.toLower(lookup("AWS_EC2_METADATA_DISABLED")) == "true")
}

/// The credentials of the instance's role, by IMDSv2 only: a session token
/// is requested with `PUT /latest/api/token`, and the role name and its
/// credentials are read with it. There is no fallback to IMDSv1.
///
/// `NOT_CONFIGURED` when disabled or when the instance has no role;
/// `UNAVAILABLE` when the token request gets no answer or no token (not an
/// EC2 instance, or the metadata service is switched off); `REQUEST_FAILED`
/// when a later request is refused.
pub func imdsCredentials(cfg: in ImdsConfig): Result[ResolvedCredentials, CredentialError]
  requires: cfg.tokenTtlSeconds >= 1 and cfg.tokenTtlSeconds <= 21600
{
  if cfg.disabled {
    return Err(failure(IMDS, "NOT_CONFIGURED", "AWS_EC2_METADATA_DISABLED is true"))
  }
  if not isSome(targetOf(cfg.endpoint)) {
    return Err(failure(IMDS, "INVALID_CONFIG", "the metadata endpoint is not an absolute http(s) URL: " + cfg.endpoint))
  }
  val base = trimTrailingSlash(cfg.endpoint)
  val http = clientNoRedirects()
  val ttl: List[Pair] = newList()
  ttl.add(Pair(name = "x-aws-ec2-metadata-token-ttl-seconds", value = cfg.tokenTtlSeconds.toString()))
  val tokenReply = match call(http, PUT, base + "/latest/api/token", ttl, None, cfg.timeoutMs) {
    case Ok(r) -> r
    case Err(NoResponse(message)) -> return Err(failure(IMDS, "UNAVAILABLE", "the instance metadata service is not reachable: " + message))
    case Err(BadRequest(message)) -> return Err(failure(IMDS, "INVALID_CONFIG", message))
  }
  // Anything but a token (403 where the metadata service is switched off, an
  // answer from something that is not the metadata service at all) means
  // there is no IMDSv2 here.
  if tokenReply.status != 200 {
    return Err(
      failure(
        IMDS,
        "UNAVAILABLE",
        "the instance metadata service gave no session token: HTTP " + tokenReply.status.toString()
      )
    )
  }
  val token = Str.trim(tokenReply.body)
  if token.length == 0 or Str.contains(token, "\n") or Str.contains(token, "\r") {
    return Err(failure(IMDS, "MALFORMED_RESPONSE", "the IMDSv2 token request returned no usable token"))
  }
  val auth: List[Pair] = newList()
  auth.add(Pair(name = "x-aws-ec2-metadata-token", value = token))
  val rolesUrl = base + "/latest/meta-data/iam/security-credentials/"
  val rolesReply = match call(http, GET, rolesUrl, auth, None, cfg.timeoutMs) {
    case Ok(r) -> r
    case Err(NoResponse(message)) -> return Err(failure(IMDS, "REQUEST_FAILED", message))
    case Err(BadRequest(message)) -> return Err(failure(IMDS, "INVALID_CONFIG", message))
  }
  if rolesReply.status == 404 {
    return Err(failure(IMDS, "NOT_CONFIGURED", "the instance has no IAM role"))
  }
  if rolesReply.status != 200 {
    return Err(
      failure(
        IMDS,
        "REQUEST_FAILED",
        "the role name request was answered with HTTP " + rolesReply.status.toString()
      )
    )
  }
  val role = Str.trim(Str.split(Str.replace(rolesReply.body, "\r", ""), "\n")[0])
  if role.length == 0 {
    return Err(failure(IMDS, "NOT_CONFIGURED", "the instance has no IAM role"))
  }
  val credsReply = match call(http, GET, rolesUrl + uriEncode(role, false), auth, None, cfg.timeoutMs) {
    case Ok(r) -> r
    case Err(NoResponse(message)) -> return Err(failure(IMDS, "REQUEST_FAILED", message))
    case Err(BadRequest(message)) -> return Err(failure(IMDS, "INVALID_CONFIG", message))
  }
  if credsReply.status != 200 {
    return Err(
      failure(
        IMDS,
        "REQUEST_FAILED",
        "the credentials request for role '" + role + "' was answered with HTTP " + credsReply.status.toString()
      )
    )
  }
  match credentialsOfJson(credsReply.body, "Token", IMDS) {
    case Ok(c) -> Ok(c)
    case Err(message) -> Err(failure(IMDS, "MALFORMED_RESPONSE", Str.replace(message, "the output", "the answer")))
  }
}

record ImdsProvider {
  lookup: (String) -> String
}

impl CredentialProvider for ImdsProvider {
  func resolve(): Result[ResolvedCredentials, CredentialError] {
    imdsCredentials(imdsConfigFromEnvironment(self.lookup))
  }
}

record ImdsConfigProvider {
  cfg: ImdsConfig
}

impl CredentialProvider for ImdsConfigProvider {
  func resolve(): Result[ResolvedCredentials, CredentialError] {
    imdsCredentials(self.cfg)
  }
}

/// The instance metadata service, as the environment configures it.
pub func imdsProvider(lookup: in (String) -> String): CredentialProvider {
  val p: CredentialProvider = ImdsProvider(lookup = lookup)
  p
}

/// The instance metadata service at `cfg`.
pub func imdsProviderOf(cfg: in ImdsConfig): CredentialProvider {
  val p: CredentialProvider = ImdsConfigProvider(cfg = cfg)
  p
}

// ─── Web identity (STS AssumeRoleWithWebIdentity) ────────────────────────────
val WEB_IDENTITY: String = "web-identity"

/// What `AssumeRoleWithWebIdentity` needs.
pub record WebIdentityConfig {
  /// `AWS_WEB_IDENTITY_TOKEN_FILE`: a file holding the OIDC token, read for
  /// every request (the platform rotates it).
  tokenFile: String
  /// `AWS_ROLE_ARN`.
  roleArn: String
  /// `AWS_ROLE_SESSION_NAME`, or a generated name.
  roleSessionName: String
  /// The STS endpoint, `https://` unless the host is loopback.
  stsEndpoint: String
  /// A PEM file of a certificate authority to trust for the STS endpoint in
  /// addition to the system trust store, or `""`.
  caBundlePath: String = ""
  timeoutMs: Int = 30000
}

func webIdentityConfigOf(lookup: in (String) -> String): Result[WebIdentityConfig, CredentialError] {
  val tokenFile = lookup("AWS_WEB_IDENTITY_TOKEN_FILE")
  val roleArn = lookup("AWS_ROLE_ARN")
  if tokenFile.length == 0 and roleArn.length == 0 {
    return Err(failure(WEB_IDENTITY, "NOT_CONFIGURED", "AWS_WEB_IDENTITY_TOKEN_FILE and AWS_ROLE_ARN are not set"))
  }
  if tokenFile.length == 0 {
    return Err(failure(WEB_IDENTITY, "INVALID_CONFIG", "AWS_ROLE_ARN is set but AWS_WEB_IDENTITY_TOKEN_FILE is not"))
  }
  if roleArn.length == 0 {
    return Err(failure(WEB_IDENTITY, "INVALID_CONFIG", "AWS_WEB_IDENTITY_TOKEN_FILE is set but AWS_ROLE_ARN is not"))
  }
  val settings = match settingsFromEnvironment(lookup, "STS") {
    case Ok(s) -> s
    case Err(e) -> return Err(failure(WEB_IDENTITY, e.code, e.message))
  }
  val endpoint = if settings.endpointUrl.length > 0 {
    settings.endpointUrl
  } else if settings.region.length > 0 {
    "https://sts." + settings.region + ".amazonaws.com"
  } else {
    "https://sts.amazonaws.com"
  }
  val named = lookup("AWS_ROLE_SESSION_NAME")
  Ok(
    WebIdentityConfig(
      tokenFile = tokenFile,
      roleArn = roleArn,
      roleSessionName = if named.length > 0 { named } else { "lyric-" + systemClockMs().toString() },
      stsEndpoint = endpoint,
      caBundlePath = settings.caBundlePath
    )
  )
}

/// The configuration the environment names: `AWS_WEB_IDENTITY_TOKEN_FILE`,
/// `AWS_ROLE_ARN`, `AWS_ROLE_SESSION_NAME` (else `lyric-<time>`), and the STS
/// endpoint from `AWS_ENDPOINT_URL_STS`, `AWS_ENDPOINT_URL` or the profile's
/// `endpoint_url`, else the regional endpoint of `AWS_REGION` /
/// `AWS_DEFAULT_REGION` / the profile's `region`, else
/// `https://sts.amazonaws.com`. `AWS_CA_BUNDLE` or the profile's `ca_bundle`
/// is trusted for it. `NOT_CONFIGURED` when neither of the first two is set,
/// `INVALID_CONFIG` when only one is.
pub func webIdentityConfigFromEnvironment(lookup: in (String) -> String): Result[WebIdentityConfig, CredentialError] {
  webIdentityConfigOf(lookup)
}

func childText(node: in XmlNode, tag: in String): String {
  match findFirst(node, tag) {
    case Some(n) -> Str.trim(textContent(n))
    case None -> ""
  }
}

/// The credentials in an `AssumeRoleWithWebIdentityResponse` (or any STS
/// response with a `Credentials` element). `Err` says what is missing and
/// never quotes the document.
pub func credentialsOfStsXml(text: in String): Result[ResolvedCredentials, String] {
  val doc = match parseXml(text) { case Ok(d) -> d; case Err(_) -> return Err("the answer is not XML") }
  val credentials = match findFirst(documentRoot(doc), "Credentials") {
    case Some(n) -> n
    case None -> return Err("the answer has no Credentials element")
  }
  val accessKeyId = childText(credentials, "AccessKeyId")
  val secretAccessKey = childText(credentials, "SecretAccessKey")
  val sessionToken = childText(credentials, "SessionToken")
  if accessKeyId.length == 0 or secretAccessKey.length == 0 or sessionToken.length == 0 {
    return Err("the Credentials element lacks AccessKeyId, SecretAccessKey or SessionToken")
  }
  val expiresAt = match parseTimestampMs(childText(credentials, "Expiration")) {
    case Some(ms) -> ms
    case None -> return Err("the Credentials element has no ISO 8601 Expiration")
  }
  Ok(
    ResolvedCredentials(
      credentials = Credentials(
        accessKeyId = accessKeyId,
        secretAccessKey = secretAccessKey,
        sessionToken = sessionToken
      ),
      expiresAtEpochMs = Some(value = expiresAt),
      source = WEB_IDENTITY
    )
  )
}

// `<Code>: <Message>` of an STS `ErrorResponse`, or `""`.
func stsFault(text: in String): String {
  match parseXml(text) {
    case Err(_) -> ""
    case Ok(doc) -> match findFirst(documentRoot(doc), "Error") {
      case None -> ""
      case Some(error) -> {
        val code = childText(error, "Code")
        val message = childText(error, "Message")
        if code.length == 0 {
          ""
        } else if message.length == 0 {
          code
        } else {
          code + ": " + message
        }
      }
    }
  }
}

func stsClient(cfg: in WebIdentityConfig): Result[HttpClient, CredentialError] {
  if cfg.caBundlePath.length == 0 {
    return Ok(clientNoRedirects())
  }
  if not tlsConfigSupported() {
    return Err(failure(WEB_IDENTITY, "INVALID_CONFIG", "a CA bundle cannot be configured on this target"))
  }
  val pem = match File.readText(cfg.caBundlePath) {
    case Ok(text) -> text
    case Err(_) -> return Err(failure(WEB_IDENTITY, "INVALID_CONFIG", "cannot read the CA bundle " + cfg.caBundlePath))
  }
  match Tls.Certificate.fromPem(pem) {
    case Ok(cert) -> Ok(HttpClientBuilder.new().withNoRedirects().withCaCertificate(cert).build())
    case Err(e) -> Err(
      failure(
        WEB_IDENTITY,
        "INVALID_CONFIG",
        "the CA bundle " + cfg.caBundlePath + " is not usable: " + Tls.TlsError.message(e)
      )
    )
  }
}

/// Exchange the OIDC token in `cfg.tokenFile` for the credentials of
/// `cfg.roleArn`: STS `AssumeRoleWithWebIdentity`, an unsigned query-protocol
/// call. `REQUEST_FAILED` holds the STS error code and message
/// (`InvalidIdentityToken`, `AccessDenied`, `ExpiredTokenException`).
pub func webIdentityCredentials(cfg: in WebIdentityConfig): Result[ResolvedCredentials, CredentialError] {
  if cfg.roleArn.length == 0 or cfg.roleSessionName.length == 0 {
    return Err(failure(WEB_IDENTITY, "INVALID_CONFIG", "the role ARN and the role session name must be non-empty"))
  }
  match targetOf(cfg.stsEndpoint) {
    case None -> return Err(
      failure(
        WEB_IDENTITY,
        "INVALID_CONFIG",
        "the STS endpoint is not an absolute http(s) URL: " + cfg.stsEndpoint
      )
    )
    case Some(t) -> if t.scheme != "https" and not isLoopbackHost(t.host) {
      return Err(
        failure(
          WEB_IDENTITY,
          "INVALID_CONFIG",
          "the STS endpoint must use https:// (http:// is allowed only to a loopback address): " + cfg.stsEndpoint
        )
      )
    }
  }
  val token = match File.readText(cfg.tokenFile) {
    case Ok(text) -> Str.trim(text)
    case Err(_) -> return Err(failure(WEB_IDENTITY, "INVALID_CONFIG", "cannot read the web identity token file " + cfg.tokenFile))
  }
  if token.length == 0 {
    return Err(failure(WEB_IDENTITY, "INVALID_CONFIG", "the web identity token file is empty: " + cfg.tokenFile))
  }
  val http = match stsClient(cfg) { case Ok(h) -> h; case Err(e) -> return Err(e) }
  val form = "Action=AssumeRoleWithWebIdentity&Version=2011-06-15&RoleArn=" + uriEncode(cfg.roleArn, false) +
    "&RoleSessionName=" +
    uriEncode(cfg.roleSessionName, false) +
    "&WebIdentityToken=" +
    uriEncode(token, false)
  val none: List[Pair] = newList()
  val url = trimTrailingSlash(cfg.stsEndpoint) + "/"
  val reply = match call(http, POST, url, none, Some(value = form), cfg.timeoutMs) {
    case Ok(r) -> r
    case Err(NoResponse(message)) -> return Err(failure(WEB_IDENTITY, "REQUEST_FAILED", message))
    case Err(BadRequest(message)) -> return Err(failure(WEB_IDENTITY, "INVALID_CONFIG", message))
  }
  if reply.status != 200 {
    val fault = stsFault(reply.body)
    // An error text that repeats the token is not passed on.
    val shown = if fault.length > 0 and not Str.contains(fault, token) { ": " + fault } else { "" }
    return Err(
      failure(
        WEB_IDENTITY,
        "REQUEST_FAILED",
        "AssumeRoleWithWebIdentity was answered with HTTP " + reply.status.toString() + shown
      )
    )
  }
  match credentialsOfStsXml(reply.body) {
    case Ok(c) -> Ok(c)
    case Err(message) -> Err(failure(WEB_IDENTITY, "MALFORMED_RESPONSE", message))
  }
}

record WebIdentityProvider {
  lookup: (String) -> String
}

impl CredentialProvider for WebIdentityProvider {
  func resolve(): Result[ResolvedCredentials, CredentialError] {
    match webIdentityConfigOf(self.lookup) {
      case Ok(cfg) -> webIdentityCredentials(cfg)
      case Err(e) -> Err(e)
    }
  }
}

record WebIdentityConfigProvider {
  cfg: WebIdentityConfig
}

impl CredentialProvider for WebIdentityConfigProvider {
  func resolve(): Result[ResolvedCredentials, CredentialError] {
    webIdentityCredentials(self.cfg)
  }
}

/// Web identity, as the environment configures it.
pub func webIdentityProvider(lookup: in (String) -> String): CredentialProvider {
  val p: CredentialProvider = WebIdentityProvider(lookup = lookup)
  p
}

/// Web identity with `cfg`.
pub func webIdentityProviderOf(cfg: in WebIdentityConfig): CredentialProvider {
  val p: CredentialProvider = WebIdentityConfigProvider(cfg = cfg)
  p
}
