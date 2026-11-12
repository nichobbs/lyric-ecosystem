//! Flags.Flipt.Client - a client of Flipt's v1 REST API over `Std.Http`.
//!
//! One `FliptClient` talks to one Flipt server: the evaluation API
//! (`POST /evaluate/v1/boolean`, `/evaluate/v1/variant`, `/evaluate/v1/batch`)
//! and the part of the management API a flag store needs
//! (`/api/v1/namespaces/...`). Requests are JSON over HTTP with an optional
//! `Authorization: Bearer <client token>`. There is no Flipt SDK underneath,
//! so the same code runs on .NET and the JVM.
//!
//! Failures are `FliptError` values. `code` is:
//!
//!   * `UNAUTHENTICATED` (401), `PERMISSION_DENIED` (403), `NOT_FOUND` (404),
//!     `INVALID_ARGUMENT` (400, and a request this client refused to send);
//!   * `HTTP_<status>` for any other response that is not a 200, a redirect
//!     included (redirects are not followed: the token would go with them);
//!   * `TIMEOUT` when the request deadline passed, `CONNECTION_FAILED` when
//!     there was no response for another reason;
//!   * `INVALID_CONFIG` for a configuration that cannot be used;
//!   * `MALFORMED_RESPONSE` for a 200 this client cannot read.
//!
//! Requests are not retried.
@runtime_checked
@io
package Flags.Flipt.Client

import Std.Core
import Std.Collections
import Std.Encoding as Enc
import Std.Environment as Env
import Std.Errors
import Std.Http
import Std.JsonValue
import Std.String as Str
import Std.Task
import Flags.Flipt.Wire

// The default per-request deadline.
val DEFAULT_REQUEST_TIMEOUT_MS: Int = 5000

// How old a snapshot may get before a read refreshes it.
val DEFAULT_REFRESH_INTERVAL_MS: Int = 10000

// Flags per `ListFlags` page.
val DEFAULT_PAGE_SIZE: Int = 100

// A listing that is still paging after this many pages is not one this
// client keeps following.
val MAX_PAGES: Int = 1000

/// A failed Flipt operation.
pub record FliptError {
  /// One of the codes in the module documentation.
  code: String
  message: String
  /// The HTTP status of the response, 0 when there was none.
  status: Int
}

/// The error as one line: `flipt: <message>`.
@pure
pub func FliptError.text(e: in FliptError): String {
  "flipt: " + e.message
}

func invalidConfig(message: in String): FliptError {
  FliptError(code = "INVALID_CONFIG", message = message, status = 0)
}

func invalidArgument(message: in String): FliptError {
  FliptError(code = "INVALID_ARGUMENT", message = message, status = 0)
}

func malformed(action: in String, message: in String): FliptError {
  FliptError(code = "MALFORMED_RESPONSE", message = action + ": " + message, status = 200)
}

/// How a store answers a read.
pub enum ReadMode {
  /// From a snapshot of the namespace's flags, refreshed by polling. No
  /// targeting: a flag has its default value.
  case Snapshot
  /// By evaluating the flag on the server for the configured entity and
  /// context, one request per read.
  case Evaluate
}

/// Where the Flipt server is and how a store reads from it.
pub record FliptConfig {
  /// `http(s)://host[:port]` of the Flipt server. `https://` unless the host
  /// is loopback.
  url: String
  /// The namespace the flags are read from.
  namespace: String
  /// A Flipt client token, sent as `Authorization: Bearer`; empty for a
  /// server without authentication.
  token: String
  /// The deadline of one request in milliseconds.
  requestTimeoutMs: Int
  /// `Snapshot` mode: a read refreshes the snapshot when the last attempt is
  /// at least this old. `Evaluate` mode: the same for `listFlags`.
  refreshIntervalMs: Int
  /// How long the last value read from the server is served while the server
  /// cannot be reached, in milliseconds; after that a read has no value and
  /// the caller's default applies. 0 serves it for as long as the outage
  /// lasts.
  maxStaleMs: Int
  mode: ReadMode
  /// `Evaluate` mode: the entity every read is evaluated for.
  entityId: String
  /// `Evaluate` mode: the context every read is evaluated with.
  context: Map[String, String]
  /// Flags per `ListFlags` request, 1 to 1000.
  pageSize: Int
}

/// Configuration for the namespace `namespace` of the server at `url`, in
/// `Snapshot` mode, without a token.
pub func config(url: in String, namespace: in String): FliptConfig {
  FliptConfig(
    url = url,
    namespace = namespace,
    token = "",
    requestTimeoutMs = DEFAULT_REQUEST_TIMEOUT_MS,
    refreshIntervalMs = DEFAULT_REFRESH_INTERVAL_MS,
    maxStaleMs = 0,
    mode = ReadMode.Snapshot,
    entityId = "",
    context = newMap(),
    pageSize = DEFAULT_PAGE_SIZE
  )
}

/// Configuration from the environment: `FLIPT_URL` (required),
/// `FLIPT_NAMESPACE` (`default` when unset or empty) and
/// `FLIPT_CLIENT_TOKEN` (optional). `Err(INVALID_CONFIG)` when `FLIPT_URL` is
/// not set.
pub func configFromEnv(): Result[FliptConfig, FliptError] {
  val url = Env.getVarOrDefault("FLIPT_URL", "")
  if url.length == 0 {
    return Err(invalidConfig("FLIPT_URL is not set"))
  }
  // A variable set to the empty string counts as unset.
  val named = Env.getVarOrDefault("FLIPT_NAMESPACE", "")
  val namespace = if named.length > 0 { named } else { "default" }
  Ok(config(url, namespace).copy(token = Env.getVarOrDefault("FLIPT_CLIENT_TOKEN", "")))
}

/// `cfg` with a client token.
pub func FliptConfig.withToken(cfg: in FliptConfig, token: in String): FliptConfig {
  cfg.copy(token = token)
}

/// `cfg` reading another namespace.
pub func FliptConfig.withNamespace(cfg: in FliptConfig, namespace: in String): FliptConfig {
  cfg.copy(namespace = namespace)
}

/// `cfg` with a per-request deadline in milliseconds.
pub func FliptConfig.withRequestTimeout(cfg: in FliptConfig, requestTimeoutMs: in Int): FliptConfig {
  cfg.copy(requestTimeoutMs = requestTimeoutMs)
}

/// `cfg` refreshing every `refreshIntervalMs` milliseconds. 0 refreshes on
/// every read.
pub func FliptConfig.withRefreshInterval(cfg: in FliptConfig, refreshIntervalMs: in Int): FliptConfig {
  cfg.copy(refreshIntervalMs = refreshIntervalMs)
}

/// `cfg` serving the last known values for at most `maxStaleMs` milliseconds
/// of outage; 0 for no limit.
pub func FliptConfig.withMaxStale(cfg: in FliptConfig, maxStaleMs: in Int): FliptConfig {
  cfg.copy(maxStaleMs = maxStaleMs)
}

/// `cfg` in `Evaluate` mode: every read is evaluated on the server for
/// `entityId` with `context`.
pub func FliptConfig.withEvaluation(
  cfg: in FliptConfig, entityId: in String, context: in Map[String, String]
): FliptConfig {
  cfg.copy(mode = ReadMode.Evaluate, entityId = entityId, context = context)
}

/// True when `cfg` is in `Evaluate` mode.
@pure
pub func FliptConfig.evaluates(cfg: in FliptConfig): Bool {
  cfg.mode == ReadMode.Evaluate
}

/// `cfg` listing `pageSize` flags per request.
pub func FliptConfig.withPageSize(cfg: in FliptConfig, pageSize: in Int): FliptConfig {
  cfg.copy(pageSize = pageSize)
}

// ─── The server address ──────────────────────────────────────────────────────
func isAuthorityUnit(u: in Int): Bool {
  (u >= 97 and u <= 122) or (u >= 65 and u <= 90) or (u >= 48 and u <= 57) or u == 45 or u == 46 or u == 58 or u == 91 or
    u == 93
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

/// The origin (`scheme://host[:port]`, lower case, no trailing slash) of a
/// Flipt server URL. `Err` carries the reason: not an absolute http(s) URL, a
/// path, a query, credentials, or plain `http://` to a host that is not
/// loopback (the token and the flags would cross the network in clear).
@pure
pub func parseOrigin(raw: in String): Result[String, String] {
  var scheme = ""
  var rest = ""
  if Str.startsWith(raw, "https://") {
    scheme = "https"
    rest = raw.substring(8)
  } else if Str.startsWith(raw, "http://") {
    scheme = "http"
    rest = raw.substring(7)
  } else {
    return Err("Flipt URL must be an absolute http(s) URL: " + raw)
  }
  if Str.endsWith(rest, "/") {
    rest = rest.substring(0, rest.length - 1)
  }
  if rest.length == 0 {
    return Err("Flipt URL has no host: " + raw)
  }
  if Str.contains(rest, "/") {
    return Err("Flipt URL must be scheme://host[:port] with no path: " + raw)
  }
  var i = 0
  while i < rest.length {
    if not isAuthorityUnit(Str.codeUnitAt(rest, i)) {
      return Err("Flipt URL must not carry credentials, a query or a fragment: " + raw)
    }
    i = i + 1
  }
  val authority = Str.toLower(rest)
  if scheme == "http" and not isLoopbackAuthority(authority) {
    return Err("Flipt URL must use https:// (http:// is allowed only for localhost, 127.0.0.1 and [::1]): " + raw)
  }
  Ok(scheme + "://" + authority)
}

// ─── The client ──────────────────────────────────────────────────────────────
/// A connection to one Flipt server. Build one with `openClient`; the fields
/// are derived from a validated `FliptConfig` and are not meant to be set by
/// hand.
pub record FliptClient {
  // `scheme://host[:port]`, no trailing slash.
  origin: String
  token: String
  requestTimeoutMs: Int
  pageSize: Int
  http: HttpClient
}

func isTokenSafe(token: in String): Bool {
  var i = 0
  while i < token.length {
    val u = Str.codeUnitAt(token, i)
    if u <= 32 or u >= 127 {
      return false
    }
    i = i + 1
  }
  true
}

/// Validate the connection part of `cfg` and open a client. No request is
/// made: a wrong token shows up on the first operation.
///
/// `Err(INVALID_CONFIG)` for a URL `parseOrigin` refuses, a token with a
/// space, a control character or a character outside ASCII, a non-positive
/// timeout, or a page size outside 1 to 1000.
pub func openClient(cfg: in FliptConfig): Result[FliptClient, FliptError] {
  val origin = match parseOrigin(cfg.url) { case Ok(o) -> o; case Err(message) -> return Err(invalidConfig(message)) }
  if not isTokenSafe(cfg.token) {
    return Err(invalidConfig("the Flipt client token holds a character a header value cannot carry"))
  }
  if cfg.requestTimeoutMs <= 0 {
    return Err(invalidConfig("requestTimeoutMs must be > 0, got " + cfg.requestTimeoutMs.toString()))
  }
  if cfg.pageSize < 1 or cfg.pageSize > 1000 {
    return Err(invalidConfig("pageSize must be 1 to 1000, got " + cfg.pageSize.toString()))
  }
  Ok(
    FliptClient(
      origin = origin,
      token = cfg.token,
      requestTimeoutMs = cfg.requestTimeoutMs,
      pageSize = cfg.pageSize,
      // A redirect would carry the token to another host; it is returned as
      // the error it is instead.
      http = clientNoRedirects()
    )
  )
}

// Plain (non-`async`) so it may `await` in place and still wrap the send in
// `try` (V0012 forbids `await` inside `try` in an `async func`).
func sendCatching(client: in FliptClient, req: in HttpRequest, action: in String): Result[HttpResponse, FliptError] {
  val src = makeCancelSourceTimeout(client.requestTimeoutMs)
  val tok = sourceToken(src)
  val sent = try {
    await client.http.sendWithCancel(req, tok)
  } catch Bug as bug {
    Err(error = ConnectionFailed(url = client.origin, message = bug.message))
  }
  val timedOut = isCancelled(tok)
  disposeSource(src)
  match sent {
    case Ok(resp) -> Ok(resp)
    case Err(e) -> if timedOut {
      Err(
        FliptError(
          code = "TIMEOUT",
          message = action + " timed out after " + client.requestTimeoutMs.toString() + " ms at " + client.origin,
          status = 0
        )
      )
    } else {
      Err(
        FliptError(
          code = "CONNECTION_FAILED",
          message = action + " got no response from " + client.origin + ": " + HttpError.message(e),
          status = 0
        )
      )
    }
  }
}

func readBody(resp: in HttpResponse, action: in String): Result[slice[Byte], FliptError] {
  val read = try {
    await HttpResponse.bodyBytes(resp)
  } catch Bug as bug {
    return Err(
      FliptError(
        code = "CONNECTION_FAILED",
        message = action + ": cannot read the response body: " + bug.message,
        status = 0
      )
    )
  }
  match read {
    case Ok(bytes) -> Ok(bytes)
    case Err(e) -> Err(
      FliptError(
        code = "CONNECTION_FAILED",
        message = action + ": cannot read the response body: " + IOError.message(e),
        status = 0
      )
    )
  }
}

func codeOfStatus(status: in Int): String {
  if status == 400 {
    "INVALID_ARGUMENT"
  } else if status == 401 {
    "UNAUTHENTICATED"
  } else if status == 403 {
    "PERMISSION_DENIED"
  } else if status == 404 {
    "NOT_FOUND"
  } else {
    "HTTP_" + status.toString()
  }
}

/// The `FliptError` for a response that is not a 200: the code of the status
/// and Flipt's message when the body is an error document.
@pure
pub func faultOf(action: in String, status: in Int, body: in String): FliptError {
  val detail = match parseFault(body) {
    case Some(fault) -> if fault.message.length > 0 { ": " + fault.message } else { "" }
    case None -> ""
  }
  FliptError(
    code = codeOfStatus(status),
    message = action + " failed: Flipt answered HTTP " + status.toString() + detail,
    status = status
  )
}

/// Send one request and return the response document.
///
/// `action` names the operation in errors; `pathAndQuery` starts with `/`;
/// `body` is the JSON request body, empty for none.
pub func FliptClient.call(
  client: in FliptClient,
  action: in String,
  method: in HttpMethod,
  pathAndQuery: in String,
  body: in String,
): Result[JsonValue, FliptError] {
  val url = client.origin + pathAndQuery
  val parsed = match Url.tryFrom(url) {
    case Ok(u) -> u
    case Err(_) -> return Err(invalidArgument("cannot build a Flipt request URL: " + url))
  }
  var req = request(method, parsed)
  if body.length > 0 {
    req = match withBytesBody(req, "application/json", Enc.encodeUtf8(body)) {
      case Ok(r) -> r
      case Err(e) -> return Err(invalidArgument(HttpError.message(e)))
    }
  }
  req = match withHeader(req, "Accept", "application/json") {
    case Ok(r) -> r
    case Err(e) -> return Err(invalidArgument(HttpError.message(e)))
  }
  if client.token.length > 0 {
    req = match withHeader(req, "Authorization", "Bearer " + client.token) {
      case Ok(r) -> r
      case Err(e) -> return Err(invalidArgument(HttpError.message(e)))
    }
  }
  val resp = match sendCatching(client, req, action) { case Ok(r) -> r; case Err(e) -> return Err(e) }
  val responseBytes = match readBody(resp, action) { case Ok(b) -> b; case Err(e) -> return Err(e) }
  val status = HttpResponse.statusCode(resp)
  val responseText = match Enc.tryDecodeUtf8(responseBytes) { case Some(t) -> t; case None -> "" }
  if status != 200 {
    return Err(faultOf(action, status, responseText))
  }
  match parseDocument(responseText) {
    case Ok(doc) -> Ok(doc)
    case Err(message) -> Err(malformed(action, message))
  }
}

func checkedKey(what: in String, key: in String): Result[Unit, FliptError] {
  if isValidKey(key) {
    Ok(())
  } else {
    Err(invalidArgument(what + " '" + key + "' is not a Flipt key (letters, digits, '-', '_' and ',')"))
  }
}

func flagsPath(namespaceKey: in String): String {
  "/api/v1/namespaces/" + namespaceKey + "/flags"
}

// ─── Reading ─────────────────────────────────────────────────────────────────
/// GetNamespace: `Ok(true)` when the namespace exists, `Ok(false)` when
/// Flipt answers 404.
pub func FliptClient.namespaceExists(client: in FliptClient, namespaceKey: in String): Result[Bool, FliptError] {
  match checkedKey("namespace", namespaceKey) {
    case Err(e) -> return Err(e)
    case Ok(_) -> ()
  }
  match client.call("GetNamespace", GET, "/api/v1/namespaces/" + namespaceKey, "") {
    case Ok(_) -> Ok(true)
    case Err(e) -> if e.code == "NOT_FOUND" { Ok(false) } else { Err(e) }
  }
}

/// ListFlags: one page of the flags of a namespace. `pageToken` is empty for
/// the first page. Flipt answers an empty list for a namespace that does not
/// exist.
pub func FliptClient.listFlagsPage(
  client: in FliptClient, namespaceKey: in String, pageToken: in String
): Result[FlagPage, FliptError] {
  match checkedKey("namespace", namespaceKey) {
    case Err(e) -> return Err(e)
    case Ok(_) -> ()
  }
  var path = flagsPath(namespaceKey) + "?limit=" + client.pageSize.toString()
  if pageToken.length > 0 {
    // Page tokens are base64url text; anything else is not one of Flipt's.
    var i = 0
    while i < pageToken.length {
      val u = Str.codeUnitAt(pageToken, i)
      val ok = (u >= 97 and u <= 122) or (u >= 65 and u <= 90) or (u >= 48 and u <= 57) or u == 45 or u == 95 or u == 61
      if not ok {
        return Err(malformed("ListFlags", "the page token is not base64url text"))
      }
      i = i + 1
    }
    path = path + "&pageToken=" + pageToken
  }
  match client.call("ListFlags", GET, path, "") {
    case Err(e) -> Err(e)
    case Ok(doc) -> match parseFlagPage(doc) {
      case Ok(page) -> Ok(page)
      case Err(message) -> Err(malformed("ListFlags", message))
    }
  }
}

/// Every flag of a namespace, in Flipt's order, following the pages.
pub func FliptClient.listFlags(client: in FliptClient, namespaceKey: in String): Result[List[FliptFlag], FliptError] {
  val all: List[FliptFlag] = newList()
  var token = ""
  var pages = 0
  var done = false
  while not done {
    val page = match client.listFlagsPage(namespaceKey, token) { case Ok(p) -> p; case Err(e) -> return Err(e) }
    for f in page.flags {
      all.add(f)
    }
    pages = pages + 1
    if page.nextPageToken.length == 0 {
      done = true
    } else if page.nextPageToken == token or pages >= MAX_PAGES {
      return Err(malformed("ListFlags", "the listing does not end"))
    } else {
      token = page.nextPageToken
    }
  }
  Ok(all)
}

/// GetFlag: one flag with its variants.
pub func FliptClient.getFlag(
  client: in FliptClient, namespaceKey: in String, flagKey: in String
): Result[FliptFlag, FliptError] {
  match checkedKey("namespace", namespaceKey) {
    case Err(e) -> return Err(e)
    case Ok(_) -> ()
  }
  match checkedKey("flag", flagKey) {
    case Err(e) -> return Err(e)
    case Ok(_) -> ()
  }
  match client.call("GetFlag", GET, flagsPath(namespaceKey) + "/" + flagKey, "") {
    case Err(e) -> Err(e)
    case Ok(doc) -> match parseFlag(doc) {
      case Ok(flag) -> Ok(flag)
      case Err(message) -> Err(malformed("GetFlag", message))
    }
  }
}

// ─── Evaluation ──────────────────────────────────────────────────────────────
/// Evaluate a boolean flag for an entity. `NOT_FOUND` for a flag that does
/// not exist, `INVALID_ARGUMENT` for a variant flag.
pub func FliptClient.evaluateBoolean(
  client: in FliptClient, namespaceKey: in String, input: in EvaluationInput
): Result[BooleanResult, FliptError] {
  match checkedKey("namespace", namespaceKey) {
    case Err(e) -> return Err(e)
    case Ok(_) -> ()
  }
  match checkedKey("flag", input.flagKey) {
    case Err(e) -> return Err(e)
    case Ok(_) -> ()
  }
  match client.call("EvaluateBoolean", POST, "/evaluate/v1/boolean", evaluationRequest(namespaceKey, input)) {
    case Err(e) -> Err(e)
    case Ok(doc) -> match parseBooleanResult(doc) {
      case Ok(r) -> Ok(r)
      case Err(message) -> Err(malformed("EvaluateBoolean", message))
    }
  }
}

/// Evaluate a variant flag for an entity. `NOT_FOUND` for a flag that does
/// not exist, `INVALID_ARGUMENT` for a boolean flag.
pub func FliptClient.evaluateVariant(
  client: in FliptClient, namespaceKey: in String, input: in EvaluationInput
): Result[VariantResult, FliptError] {
  match checkedKey("namespace", namespaceKey) {
    case Err(e) -> return Err(e)
    case Ok(_) -> ()
  }
  match checkedKey("flag", input.flagKey) {
    case Err(e) -> return Err(e)
    case Ok(_) -> ()
  }
  match client.call("EvaluateVariant", POST, "/evaluate/v1/variant", evaluationRequest(namespaceKey, input)) {
    case Err(e) -> Err(e)
    case Ok(doc) -> match parseVariantResult(doc) {
      case Ok(r) -> Ok(r)
      case Err(message) -> Err(malformed("EvaluateVariant", message))
    }
  }
}

/// Evaluate several flags of either type in one request. The answers are in
/// request order; a flag that does not exist is a `FlagNotFound`, not an
/// error. An empty `inputs` makes no request.
pub func FliptClient.evaluateBatch(
  client: in FliptClient, namespaceKey: in String, inputs: in List[EvaluationInput]
): Result[List[Evaluation], FliptError] {
  match checkedKey("namespace", namespaceKey) {
    case Err(e) -> return Err(e)
    case Ok(_) -> ()
  }
  for input in inputs {
    match checkedKey("flag", input.flagKey) {
      case Err(e) -> return Err(e)
      case Ok(_) -> ()
    }
  }
  if inputs.count == 0 {
    val none: List[Evaluation] = newList()
    return Ok(none)
  }
  match client.call("EvaluateBatch", POST, "/evaluate/v1/batch", batchRequest(namespaceKey, inputs)) {
    case Err(e) -> Err(e)
    case Ok(doc) -> match parseBatchResponse(doc, inputs.count) {
      case Ok(answers) -> Ok(answers)
      case Err(message) -> Err(malformed("EvaluateBatch", message))
    }
  }
}

// ─── Management ──────────────────────────────────────────────────────────────
/// CreateNamespace. `INVALID_ARGUMENT` when the key is taken.
pub func FliptClient.createNamespace(
  client: in FliptClient, namespaceKey: in String, description: in String
): Result[Unit, FliptError] {
  match checkedKey("namespace", namespaceKey) {
    case Err(e) -> return Err(e)
    case Ok(_) -> ()
  }
  val body = createNamespaceRequest(namespaceKey, namespaceKey, description)
  match client.call("CreateNamespace", POST, "/api/v1/namespaces", body) {
    case Err(e) -> Err(e)
    case Ok(_) -> Ok(())
  }
}

/// DeleteNamespace. Flipt refuses (`INVALID_ARGUMENT`) while the namespace
/// still has flags, and answers 200 for a namespace it has no flags for.
pub func FliptClient.deleteNamespace(client: in FliptClient, namespaceKey: in String): Result[Unit, FliptError] {
  match checkedKey("namespace", namespaceKey) {
    case Err(e) -> return Err(e)
    case Ok(_) -> ()
  }
  match client.call("DeleteNamespace", DELETE, "/api/v1/namespaces/" + namespaceKey, "") {
    case Err(e) -> Err(e)
    case Ok(_) -> Ok(())
  }
}

func createFlag(
  client: in FliptClient,
  namespaceKey: in String,
  flagKey: in String,
  description: in String,
  enabled: in Bool,
  kind: in FlagKind,
): Result[Unit, FliptError] {
  match checkedKey("namespace", namespaceKey) {
    case Err(e) -> return Err(e)
    case Ok(_) -> ()
  }
  match checkedKey("flag", flagKey) {
    case Err(e) -> return Err(e)
    case Ok(_) -> ()
  }
  val body = createFlagRequest(flagKey, description, enabled, kind)
  match client.call("CreateFlag", POST, flagsPath(namespaceKey), body) {
    case Err(e) -> Err(e)
    case Ok(_) -> Ok(())
  }
}

/// CreateFlag: a boolean flag whose value is `enabled`. `INVALID_ARGUMENT`
/// when the key is taken.
pub func FliptClient.createBooleanFlag(
  client: in FliptClient,
  namespaceKey: in String,
  flagKey: in String,
  description: in String,
  enabled: in Bool,
): Result[Unit, FliptError] {
  createFlag(client, namespaceKey, flagKey, description, enabled, FlagKind.Boolean)
}

/// CreateFlag: a variant flag without variants. `INVALID_ARGUMENT` when the
/// key is taken.
pub func FliptClient.createVariantFlag(
  client: in FliptClient,
  namespaceKey: in String,
  flagKey: in String,
  description: in String,
  enabled: in Bool,
): Result[Unit, FliptError] {
  createFlag(client, namespaceKey, flagKey, description, enabled, FlagKind.Variant)
}

/// CreateVariant: returns the new variant with the id Flipt assigned.
pub func FliptClient.createVariant(
  client: in FliptClient,
  namespaceKey: in String,
  flagKey: in String,
  variantKey: in String,
  attachment: in String,
): Result[FliptVariant, FliptError] {
  match checkedKey("namespace", namespaceKey) {
    case Err(e) -> return Err(e)
    case Ok(_) -> ()
  }
  match checkedKey("flag", flagKey) {
    case Err(e) -> return Err(e)
    case Ok(_) -> ()
  }
  if variantKey.length == 0 {
    return Err(invalidArgument("a variant key must be non-empty"))
  }
  val path = flagsPath(namespaceKey) + "/" + flagKey + "/variants"
  match client.call("CreateVariant", POST, path, createVariantRequest(variantKey, attachment)) {
    case Err(e) -> Err(e)
    case Ok(doc) -> match parseVariant(doc) {
      case Ok(v) -> Ok(v)
      case Err(message) -> Err(malformed("CreateVariant", message))
    }
  }
}

/// UpdateFlag: replace the flag's description, `enabled` and default variant
/// (`defaultVariantId` empty for none). The display name stays the key.
pub func FliptClient.updateFlag(
  client: in FliptClient,
  namespaceKey: in String,
  flagKey: in String,
  description: in String,
  enabled: in Bool,
  defaultVariantId: in String,
): Result[Unit, FliptError] {
  match checkedKey("namespace", namespaceKey) {
    case Err(e) -> return Err(e)
    case Ok(_) -> ()
  }
  match checkedKey("flag", flagKey) {
    case Err(e) -> return Err(e)
    case Ok(_) -> ()
  }
  val body = updateFlagRequest(flagKey, description, enabled, defaultVariantId)
  match client.call("UpdateFlag", PUT, flagsPath(namespaceKey) + "/" + flagKey, body) {
    case Err(e) -> Err(e)
    case Ok(_) -> Ok(())
  }
}

/// DeleteFlag. Flipt answers 200 for a flag that does not exist.
pub func FliptClient.deleteFlag(
  client: in FliptClient, namespaceKey: in String, flagKey: in String
): Result[Unit, FliptError] {
  match checkedKey("namespace", namespaceKey) {
    case Err(e) -> return Err(e)
    case Ok(_) -> ()
  }
  match checkedKey("flag", flagKey) {
    case Err(e) -> return Err(e)
    case Ok(_) -> ()
  }
  match client.call("DeleteFlag", DELETE, flagsPath(namespaceKey) + "/" + flagKey, "") {
    case Err(e) -> Err(e)
    case Ok(_) -> Ok(())
  }
}
