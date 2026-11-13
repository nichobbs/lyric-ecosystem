//! Mq.ActiveMq.Url - broker URLs of the form
//! `stomp://[login[:passcode]@]host[:port]` (plain TCP) and
//! `stomp+ssl://[login[:passcode]@]host[:port]` (TLS).
//!
//! The login and passcode are percent-decoded, so a passcode may hold `@`,
//! `:` or `/` written as `%40`, `%3A`, `%2F`. An IPv6 literal is written in
//! brackets (`stomp://[::1]:61613`); without them it is refused, because its
//! last group cannot be told from a port. The port defaults to 61613 for `stomp`
//! and 61614 for `stomp+ssl`, the ports of a stock ActiveMQ. Other schemes
//! are refused. The authority is parsed by `Net.Url` (`Lyric.Net`).
@runtime_checked
package Mq.ActiveMq.Url

import Std.Core
import Std.String as Str
import Net.Url as NetUrl

/// The STOMP port of a stock ActiveMQ broker.
pub val DEFAULT_PORT: Int = 61613

/// The STOMP-over-TLS port of a stock ActiveMQ broker.
pub val DEFAULT_TLS_PORT: Int = 61614

/// Where a broker listens and who connects to it.
pub record BrokerAddress {
  host: String
  port: Int
  login: String
  passcode: String
  /// `true` for a `stomp+ssl://` URL.
  tls: Bool
}

/// `text` with every `%XX` replaced by the byte it names, read as UTF-8.
pub func percentDecode(text: in String): Result[String, String] {
  match NetUrl.percentDecode(text) {
    case Ok(s) -> Ok(s)
    case Err(problem) -> Err(NetUrl.escapeProblemText(problem))
  }
}

func invalid(reason: in String): String {
  "activemq: invalid broker URL: " + reason
}

func schemes(): slice[NetUrl.Scheme] {
  [
    NetUrl.Scheme(name = "stomp", defaultPort = DEFAULT_PORT, tls = false),
    NetUrl.Scheme(name = "stomp+ssl", defaultPort = DEFAULT_TLS_PORT, tls = true)
  ]
}

/// Parse a `stomp://` or `stomp+ssl://` broker URL. The passcode never
/// appears in an error.
pub func parseUrl(url: in String): Result[BrokerAddress, String] {
  val found = match NetUrl.matchScheme(url, schemes(), true) {
    case Ok(m) -> m
    case Err(NoScheme) -> return Err(invalid("expected stomp://host[:port] or stomp+ssl://host[:port]"))
    case Err(problem) -> return Err(invalid(NetUrl.schemeProblemText(problem, schemes())))
  }
  var rest = found.rest
  if Str.endsWith(rest, "/") {
    rest = Str.substring(rest, 0, rest.length - 1)
  }
  if Str.contains(rest, "/") or Str.contains(rest, "?") or Str.contains(rest, "#") {
    return Err(invalid("a path, query or fragment is not allowed"))
  }
  val userInfo = match NetUrl.splitUserInfo(rest) {
    case Ok(u) -> u
    case Err(problem) -> return Err(invalid(NetUrl.authorityProblemText(problem, "login", "passcode", found.scheme)))
  }
  if userInfo.present and userInfo.user.length == 0 {
    return Err(invalid("the login before '@' is empty"))
  }
  match NetUrl.parseHostPort(userInfo.hostPort, found.scheme.defaultPort) {
    case Ok(hp) -> Ok(
      BrokerAddress(
        host = hp.host,
        port = hp.port,
        login = userInfo.user,
        passcode = userInfo.password,
        tls = found.scheme.tls
      )
    )
    case Err(problem) -> Err(invalid(NetUrl.authorityProblemText(problem, "login", "passcode", found.scheme)))
  }
}
