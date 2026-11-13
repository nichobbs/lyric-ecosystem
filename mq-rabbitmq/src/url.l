//! Mq.RabbitMq.Url - broker URLs in the RabbitMQ URI scheme:
//! `amqp://[user[:password]@]host[:port][/vhost][?parameters]` (plain TCP,
//! port 5672) and `amqps://...` (TLS, port 5671).
//!
//! The user, password and virtual host are percent-decoded, so a password may
//! hold `@`, `:` or `/` written as `%40`, `%3A`, `%2F`. An IPv6 literal is
//! written in brackets (`amqp://[::1]:5672`); without them it is refused,
//! because its last group cannot be told from a port. The authority is parsed
//! by `Net.Url` (`Lyric.Net`).
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
import Std.Parse
import Std.String as Str
import Net.Url as NetUrl

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

/// `text` with every `%XX` replaced by the byte it names, read as UTF-8.
pub func percentDecode(text: in String): Result[String, String] {
  match NetUrl.percentDecode(text) {
    case Ok(s) -> Ok(s)
    case Err(problem) -> Err(NetUrl.escapeProblemText(problem))
  }
}

func invalid(reason: in String): String {
  "rabbitmq: invalid broker URL: " + reason
}

func schemes(): slice[NetUrl.Scheme] {
  [
    NetUrl.Scheme(name = "amqp", defaultPort = DEFAULT_PORT, tls = false),
    NetUrl.Scheme(name = "amqps", defaultPort = DEFAULT_TLS_PORT, tls = true)
  ]
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
  val found = match NetUrl.matchScheme(url, schemes(), true) {
    case Ok(m) -> m
    case Err(NoScheme) -> return Err(invalid("expected amqp://host[:port][/vhost] or amqps://host[:port][/vhost]"))
    case Err(problem) -> return Err(invalid(NetUrl.schemeProblemText(problem, schemes())))
  }
  var rest = found.rest
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
  val userInfo = match NetUrl.splitUserInfo(rest) {
    case Ok(u) -> u
    case Err(problem) -> return Err(invalid(NetUrl.authorityProblemText(problem, "user", "password", found.scheme)))
  }
  if userInfo.present and userInfo.user.length == 0 {
    return Err(invalid("the user before '@' is empty"))
  }
  val user = if userInfo.present { userInfo.user } else { DEFAULT_USER }
  val password = if userInfo.present { userInfo.password } else { DEFAULT_USER }
  val address = match NetUrl.parseHostPort(userInfo.hostPort, found.scheme.defaultPort) {
    case Ok(hp) -> hp
    case Err(problem) -> return Err(invalid(NetUrl.authorityProblemText(problem, "user", "password", found.scheme)))
  }
  Ok(
    BrokerAddress(
      host = address.host,
      port = address.port,
      user = user,
      password = password,
      virtualHost = virtualHost,
      tls = found.scheme.tls,
      heartbeatSeconds = heartbeat,
      connectTimeoutMs = connectTimeout,
      frameMax = frameMax
    )
  )
}
