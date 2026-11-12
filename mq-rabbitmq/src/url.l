//! Mq.RabbitMq.Url - broker URLs in the RabbitMQ URI scheme:
//! `amqp://[user[:password]@]host[:port][/vhost][?parameters]` (plain TCP,
//! port 5672) and `amqps://...` (TLS, port 5671).
//!
//! The user, password and virtual host are percent-decoded, so a password may
//! hold `@`, `:` or `/` written as `%40`, `%3A`, `%2F`. An IPv6 literal is
//! written in brackets (`amqp://[::1]:5672`).
//!
//! The virtual host follows the URI specification RabbitMQ publishes:
//!
//!   amqp://host            the default virtual host `/`
//!   amqp://host/%2f        the virtual host `/`, spelled out
//!   amqp://host/orders     the virtual host `orders`
//!   amqp://host/           the virtual host with the EMPTY name
//!   amqp://host/a/b        refused: write `a%2Fb` for a name with a slash
//!
//! Without user information the user and password are `guest` / `guest`,
//! which a stock RabbitMQ accepts from the loopback address only.
//!
//! Query parameters: `heartbeat` (seconds), `connection_timeout`
//! (milliseconds) and `frame_max` (bytes). Any other parameter is refused,
//! because a parameter that is silently ignored (`verify=verify_none`, say)
//! is worse than an error.
@runtime_checked
package Mq.RabbitMq.Url

import Std.Core
import Std.Collections
import Std.Encoding
import Std.Parse
import Std.String as Str

/// The AMQP port of a stock RabbitMQ broker.
pub val DEFAULT_PORT: Int = 5672

/// The AMQP-over-TLS port of a stock RabbitMQ broker.
pub val DEFAULT_TLS_PORT: Int = 5671

/// The virtual host used when a URL names none.
pub val DEFAULT_VHOST: String = "/"

/// The user and password used when a URL names none.
pub val DEFAULT_USER: String = "guest"

/// Where a broker listens, who connects to it and to which virtual host.
pub record BrokerAddress {
  host: String
  port: Int
  user: String
  password: String
  virtualHost: String
  /// `true` for an `amqps://` URL.
  tls: Bool
  /// The `heartbeat` parameter in seconds; -1 when absent.
  heartbeatSeconds: Int
  /// The `connection_timeout` parameter in milliseconds; -1 when absent.
  connectTimeoutMs: Int
  /// The `frame_max` parameter in bytes; -1 when absent.
  frameMax: Int
}

func hexValue(unit: in Int): Int {
  if unit >= 48 and unit <= 57 {
    unit - 48
  } else if unit >= 65 and unit <= 70 {
    unit - 55
  } else if unit >= 97 and unit <= 102 {
    unit - 87
  } else {
    -1
  }
}

/// `text` with every `%XX` replaced by the byte it names, read as UTF-8.
pub func percentDecode(text: in String): Result[String, String] {
  if not Str.contains(text, "%") {
    return Ok(text)
  }
  val bytes: List[Byte] = newList()
  var i = 0
  while i < text.length {
    val unit = text.codeUnitAt(i)
    if unit == 37 {
      if i + 2 > text.length - 1 {
        return Err("a '%' is not followed by two hexadecimal digits")
      }
      val hi = hexValue(text.codeUnitAt(i + 1))
      val lo = hexValue(text.codeUnitAt(i + 2))
      if hi < 0 or lo < 0 {
        return Err("a '%' is not followed by two hexadecimal digits")
      }
      bytes.add((hi * 16 + lo).toByte())
      i += 3
    } else {
      var j = i
      while j < text.length and text.codeUnitAt(j) != 37 {
        j += 1
      }
      for b in encodeUtf8(Str.substring(text, i, j - i)) {
        bytes.add(b)
      }
      i = j
    }
  }
  match tryDecodeUtf8(bytes.toArray()) {
    case Some(s) -> Ok(s)
    case None -> Err("percent-encoded bytes are not valid UTF-8")
  }
}

func invalid(reason: in String): String {
  "rabbitmq: invalid broker URL: " + reason
}

func parsePort(text: in String): Result[Int, String] {
  match parseOptInt(text) {
    case Some(p) -> if p >= 1 and p <= 65535 { Ok(p) } else { Err(invalid("port " + text + " is out of range")) }
    case None -> Err(invalid("port '" + text + "' is not a number"))
  }
}

func parseCount(name: in String, text: in String): Result[Int, String] {
  match parseOptInt(text) {
    case Some(n) -> if n >= 0 { Ok(n) } else { Err(invalid("parameter " + name + " must not be negative")) }
    case None -> Err(invalid("parameter " + name + " is not a number"))
  }
}

/// Parse an `amqp://` or `amqps://` broker URL. The password never appears
/// in an error.
pub func parseUrl(url: in String): Result[BrokerAddress, String] {
  val schemeEnd = Str.indexOfRaw(url, "://")
  if schemeEnd < 0 {
    return Err(invalid("expected amqp://host[:port][/vhost] or amqps://host[:port][/vhost]"))
  }
  val scheme = Str.toLower(Str.substring(url, 0, schemeEnd))
  val tls = scheme == "amqps"
  if not tls and scheme != "amqp" {
    return Err(invalid("unsupported scheme '" + scheme + "', expected amqp or amqps"))
  }
  var rest = Str.substring(url, schemeEnd + 3)
  if Str.contains(rest, "#") {
    return Err(invalid("a fragment is not allowed"))
  }
  var heartbeat = -1
  var connectTimeout = -1
  var frameMax = -1
  val question = Str.indexOfRaw(rest, "?")
  if question >= 0 {
    val query = Str.substring(rest, question + 1)
    rest = Str.substring(rest, 0, question)
    for pair in Str.split(query, "&") {
      if pair.length > 0 {
        val eq = Str.indexOfRaw(pair, "=")
        val name = if eq < 0 { pair } else { Str.substring(pair, 0, eq) }
        val value = if eq < 0 { "" } else { Str.substring(pair, eq + 1) }
        if name == "heartbeat" {
          heartbeat = match parseCount(name, value) { case Ok(n) -> n; case Err(e) -> return Err(e) }
          if heartbeat > 65535 {
            return Err(invalid("parameter heartbeat is out of range"))
          }
        } else if name == "connection_timeout" {
          connectTimeout = match parseCount(name, value) { case Ok(n) -> n; case Err(e) -> return Err(e) }
        } else if name == "frame_max" {
          frameMax = match parseCount(name, value) { case Ok(n) -> n; case Err(e) -> return Err(e) }
        } else {
          // Only the name: a value could be a secret.
          return Err(invalid("unsupported parameter '" + name + "' (supported: heartbeat, connection_timeout, frame_max)"))
        }
      }
    }
  }
  // The authority ends at the first slash; a password with a slash is
  // percent-encoded.
  var virtualHost = DEFAULT_VHOST
  val slash = Str.indexOfRaw(rest, "/")
  if slash >= 0 {
    val rawVhost = Str.substring(rest, slash + 1)
    rest = Str.substring(rest, 0, slash)
    if Str.contains(rawVhost, "/") {
      return Err(invalid("the path holds more than the virtual host; write a '/' in its name as %2F"))
    }
    virtualHost = match percentDecode(rawVhost) {
      case Ok(v) -> v
      case Err(e) -> return Err(invalid("virtual host: " + e))
    }
  }
  var user = DEFAULT_USER
  var password = DEFAULT_USER
  val at = Str.lastIndexOfRaw(rest, "@")
  if at >= 0 {
    val userInfo = Str.substring(rest, 0, at)
    rest = Str.substring(rest, at + 1)
    val colon = Str.indexOfRaw(userInfo, ":")
    val rawUser = if colon < 0 { userInfo } else { Str.substring(userInfo, 0, colon) }
    val rawPassword = if colon < 0 { "" } else { Str.substring(userInfo, colon + 1) }
    user = match percentDecode(rawUser) { case Ok(v) -> v; case Err(e) -> return Err(invalid("user: " + e)) }
    password = match percentDecode(rawPassword) { case Ok(v) -> v; case Err(e) -> return Err(invalid("password: " + e)) }
    if user.length == 0 {
      return Err(invalid("the user before '@' is empty"))
    }
  }
  var host = rest
  var port = if tls { DEFAULT_TLS_PORT } else { DEFAULT_PORT }
  if Str.startsWith(rest, "[") {
    val close = Str.indexOfRaw(rest, "]")
    if close < 0 {
      return Err(invalid("an IPv6 address is missing its closing bracket"))
    }
    host = Str.substring(rest, 1, close - 1)
    val after = Str.substring(rest, close + 1)
    if after.length > 0 {
      if not Str.startsWith(after, ":") {
        return Err(invalid("unexpected text after the IPv6 address"))
      }
      port = match parsePort(Str.substring(after, 1)) { case Ok(p) -> p; case Err(e) -> return Err(e) }
    }
  } else {
    val colon = Str.lastIndexOfRaw(rest, ":")
    if colon >= 0 {
      host = Str.substring(rest, 0, colon)
      port = match parsePort(Str.substring(rest, colon + 1)) { case Ok(p) -> p; case Err(e) -> return Err(e) }
    }
  }
  if host.length == 0 {
    return Err(invalid("the host is empty"))
  }
  Ok(
    BrokerAddress(
      host = host,
      port = port,
      user = user,
      password = password,
      virtualHost = virtualHost,
      tls = tls,
      heartbeatSeconds = heartbeat,
      connectTimeoutMs = connectTimeout,
      frameMax = frameMax
    )
  )
}
