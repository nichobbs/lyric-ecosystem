//! Mq.Sqs.Address - endpoints and queue URLs.
//!
//! Pure functions: an endpoint (`https://sqs.eu-west-1.amazonaws.com`,
//! `http://127.0.0.1:9324`) and a queue URL
//! (`https://sqs.eu-west-1.amazonaws.com/123456789012/orders.fifo`) are
//! validated and split here before anything is signed or sent. Plain `http://`
//! is accepted only for the loopback interface: a signature does not hide the
//! access key id, the queue or the messages.
@runtime_checked
package Mq.Sqs.Address

import Std.Core
import Std.Collections
import Std.String as Str

/// A validated endpoint: `scheme://authority`, the default port dropped so
/// the signed `host` is the `Host` header an HTTP client sends.
pub record Endpoint {
  scheme: String
  authority: String
}

/// `scheme://authority`, no trailing slash.
@pure
pub func Endpoint.origin(e: in Endpoint): String {
  e.scheme + "://" + e.authority
}

/// A validated queue URL, split.
pub record QueueUrl {
  /// The URL as it is sent in `QueueUrl`: scheme, authority and path.
  url: String
  endpoint: Endpoint
  /// `/<account>/<name>`.
  path: String
  /// The queue name: the last path segment.
  name: String
  /// True for a FIFO queue, which SQS marks with the `.fifo` suffix.
  fifo: Bool
  /// The region in an AWS host name (`sqs.<region>.amazonaws.com`), or `""`
  /// for a host that does not name one.
  region: String
}

func isLowerOrDigit(u: in Int): Bool {
  (u >= 97 and u <= 122) or (u >= 48 and u <= 57)
}

func isAlnum(u: in Int): Bool {
  isLowerOrDigit(u) or (u >= 65 and u <= 90)
}

// `-`, `.`, `:`, `[`, `]` besides letters and digits.
func isAuthorityUnit(u: in Int): Bool {
  isAlnum(u) or u == 45 or u == 46 or u == 58 or u == 91 or u == 93
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

/// True for a plausible region name (`us-east-1`, `elasticmq`): lowercase
/// letters, digits and `-`, so it cannot alter a host name or a credential
/// scope it is built into.
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

/// True for a queue name SQS accepts: 1 to 80 characters of letters, digits,
/// `-` and `_`, with an optional `.fifo` suffix (counted in the 80).
@pure
pub func isValidQueueName(name: in String): Bool {
  if name.length == 0 or name.length > 80 {
    return false
  }
  val stem = if Str.endsWith(name, ".fifo") { name.substring(0, name.length - 5) } else { name }
  if stem.length == 0 {
    return false
  }
  var i = 0
  while i < stem.length {
    val u = Str.codeUnitAt(stem, i)
    if not (isAlnum(u) or u == 45 or u == 95) {
      return false
    }
    i = i + 1
  }
  true
}

// Splits `scheme://rest`; `Err` for anything but http and https.
func splitScheme(raw: in String, what: in String): Result[Endpoint, String] {
  if Str.startsWith(raw, "https://") {
    return Ok(Endpoint(scheme = "https", authority = raw.substring(8)))
  }
  if Str.startsWith(raw, "http://") {
    return Ok(Endpoint(scheme = "http", authority = raw.substring(7)))
  }
  Err(what + " must be an absolute http(s) URL: " + raw)
}

func checkedAuthority(
  scheme: in String, rawAuthority: in String, what: in String, raw: in String
): Result[Endpoint, String] {
  if rawAuthority.length == 0 {
    return Err(what + " has no host: " + raw)
  }
  var i = 0
  while i < rawAuthority.length {
    if not isAuthorityUnit(Str.codeUnitAt(rawAuthority, i)) {
      return Err(what + " must not carry credentials, a query or a fragment: " + raw)
    }
    i = i + 1
  }
  var authority = Str.toLower(rawAuthority)
  if scheme == "https" and Str.endsWith(authority, ":443") {
    authority = authority.substring(0, authority.length - 4)
  } else if scheme == "http" and Str.endsWith(authority, ":80") {
    authority = authority.substring(0, authority.length - 3)
  }
  if scheme == "http" and not isLoopbackAuthority(authority) {
    return Err(what + " must use https:// (http:// is allowed only for localhost, 127.0.0.1 and [::1]): " + raw)
  }
  Ok(Endpoint(scheme = scheme, authority = authority))
}

/// Parse an endpoint: `http(s)://host[:port]` with nothing after the
/// authority but an optional `/`. `Err` carries the reason.
@pure
pub func parseEndpoint(raw: in String): Result[Endpoint, String] {
  val split = match splitScheme(raw, "SQS endpoint") { case Ok(s) -> s; case Err(e) -> return Err(e) }
  var rest = split.authority
  if Str.endsWith(rest, "/") {
    rest = rest.substring(0, rest.length - 1)
  }
  if Str.contains(rest, "/") {
    return Err("SQS endpoint must be scheme://host[:port] with no path: " + raw)
  }
  checkedAuthority(split.scheme, rest, "SQS endpoint", raw)
}

/// The region an AWS SQS host name carries, or `""`:
/// `sqs.<region>.amazonaws.com`, `sqs-fips.<region>.amazonaws.com`,
/// `sqs.<region>.amazonaws.com.cn`, `sqs.<region>.api.aws` and the legacy
/// `<region>.queue.amazonaws.com`.
@pure
pub func regionOfHost(authority: in String): String {
  val colon = Str.lastIndexOfRaw(authority, ":")
  val host = if colon >= 0 and not Str.startsWith(authority, "[") { authority.substring(0, colon) } else { authority }
  val labels = Str.split(host, ".")
  if labels.length < 4 {
    return ""
  }
  val isAws = Str.endsWith(host, ".amazonaws.com") or Str.endsWith(host, ".amazonaws.com.cn") or
    Str.endsWith(host, ".api.aws")
  if not isAws {
    return ""
  }
  if (labels[0] == "sqs" or labels[0] == "sqs-fips") and isValidRegion(labels[1]) {
    return labels[1]
  }
  if labels[1] == "queue" and isValidRegion(labels[0]) {
    return labels[0]
  }
  ""
}

/// Parse a queue URL: `http(s)://host[:port]/<account>/<name>`. The path is
/// letters, digits, `-`, `_`, `.` and `/`; the last segment is the queue
/// name. `Err` carries the reason.
@pure
pub func parseQueueUrl(raw: in String): Result[QueueUrl, String] {
  val split = match splitScheme(raw, "SQS queue URL") { case Ok(s) -> s; case Err(e) -> return Err(e) }
  val slash = Str.indexOfRaw(split.authority, "/")
  if slash < 0 {
    return Err("SQS queue URL has no queue path: " + raw)
  }
  val endpoint = match checkedAuthority(split.scheme, split.authority.substring(0, slash), "SQS queue URL", raw) {
    case Ok(e) -> e
    case Err(e) -> return Err(e)
  }
  var path = split.authority.substring(slash)
  if path.length > 1 and Str.endsWith(path, "/") {
    path = path.substring(0, path.length - 1)
  }
  var i = 0
  while i < path.length {
    val u = Str.codeUnitAt(path, i)
    if not (isAlnum(u) or u == 45 or u == 95 or u == 46 or u == 47) {
      return Err("SQS queue URL must not carry a query, a fragment or encoded characters: " + raw)
    }
    i = i + 1
  }
  val name = path.substring(Str.lastIndexOfRaw(path, "/") + 1)
  if not isValidQueueName(name) {
    return Err("SQS queue URL does not end in a queue name: " + raw)
  }
  Ok(
    QueueUrl(
      url = endpoint.origin() + path,
      endpoint = endpoint,
      path = path,
      name = name,
      fifo = Str.endsWith(name, ".fifo"),
      region = regionOfHost(endpoint.authority)
    )
  )
}
