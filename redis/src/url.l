//! Redis.Url - parsing of `redis://` and `rediss://` connection URLs.
//!
//!   redis://[username[:password]@]host[:port][/database][?db=N&protocol=2|3]
//!   rediss://...   the same, over TLS
//!
//! `redis://:secret@host` carries a password and no user name (the classic
//! `requirepass` form); `redis://app:secret@host` an ACL user and its
//! password. User name and password are percent-decoded. An IPv6 host is
//! written in brackets (`redis://[::1]:6379`); without them it is refused. The
//! authority is parsed by `Net.Url` (`Lyric.Net`). The `rediss` scheme only switches
//! TLS on; certificates and the other TLS options are not URL parameters (see
//! `Redis.withExclusiveCaCertificate` and its siblings).
@runtime_checked
@pure
package Redis.Url

import Std.Core
import Std.Parse
import Std.String as Str
import Net.Url as NetUrl

/// The parts of a connection URL.
@experimental
pub record RedisUrl {
  host: String
  port: Int
  /// ACL user name; `""` when the URL names none.
  username: String
  /// Password; `""` when the URL carries none.
  password: String
  database: Int
  /// 2 or 3; `0` when the URL does not choose.
  protocol: Int
  /// True for `rediss://`: the connection is made over TLS.
  tls: Bool
}

func parseDatabase(text: in String): Result[Int, String] {
  match parseOptInt(text) {
    case Some(n) -> if n >= 0 { Ok(n) } else { Err("database index must not be negative") }
    case None -> Err("database index is not a number: " + text)
  }
}

func schemes(): slice[NetUrl.Scheme] {
  [
    NetUrl.Scheme(name = "redis", defaultPort = 6379, tls = false),
    NetUrl.Scheme(name = "rediss", defaultPort = 6379, tls = true)
  ]
}

func escapeText(problem: in NetUrl.EscapeProblem): String {
  match problem {
    case NetUrl.TruncatedEscape -> "truncated percent escape"
    case NetUrl.InvalidEscape -> "invalid percent escape"
    case NetUrl.EscapeNotUtf8 -> "percent escapes do not decode to UTF-8"
  }
}

func authorityText(problem: in NetUrl.AuthorityProblem): String {
  match problem {
    case NetUrl.UserEscape(p) -> "user name: " + escapeText(p)
    case NetUrl.PasswordEscape(p) -> "password: " + escapeText(p)
    case NetUrl.UnclosedBracket -> "unterminated IPv6 host"
    case NetUrl.TextAfterBracket -> "unexpected text after the IPv6 host"
    case NetUrl.UnbracketedIpv6 -> "an IPv6 host must be written in brackets, as in redis://[::1]:6379"
    case NetUrl.EmptyHost -> "empty host"
    case NetUrl.PortNotNumber(text) -> "port is not a number: " + text
    case NetUrl.PortOutOfRange(text) -> "port out of range: " + text
  }
}

/// Parse a `redis://` or `rediss://` URL. The error text never contains the
/// URL's password.
@experimental
pub func parseRedisUrl(url: in String): Result[RedisUrl, String] {
  // The scheme is matched as written: `REDIS://` is refused.
  val found = match NetUrl.matchScheme(url, schemes(), false) {
    case Ok(m) -> m
    case Err(_) -> return Err("a Redis URL starts with redis:// or rediss://")
  }
  var rest = found.rest
  val hash = Str.indexOfRaw(rest, "#")
  if hash >= 0 {
    rest = rest.substring(0, hash)
  }
  var query = ""
  val question = Str.indexOfRaw(rest, "?")
  if question >= 0 {
    query = rest.substring(question + 1)
    rest = rest.substring(0, question)
  }
  var path = ""
  val slash = Str.indexOfRaw(rest, "/")
  if slash >= 0 {
    path = rest.substring(slash + 1)
    rest = rest.substring(0, slash)
  }
  val userInfo = match NetUrl.splitUserInfo(rest) { case Ok(u) -> u; case Err(p) -> return Err(authorityText(p)) }
  if userInfo.user.length > 0 and userInfo.password.length == 0 {
    return Err("a user name needs a password (redis://user:password@host)")
  }
  val address = match NetUrl.parseHostPort(userInfo.hostPort, found.scheme.defaultPort) {
    case Ok(hp) -> hp
    case Err(p) -> return Err(authorityText(p))
  }
  var database = 0
  if path.length > 0 {
    match parseDatabase(path) {
      case Ok(n) -> {
        database = n
      }
      case Err(e) -> return Err(e)
    }
  }
  var protocol = 0
  if query.length > 0 {
    for pair in query.split("&") {
      val eq = Str.indexOfRaw(pair, "=")
      val name = if eq >= 0 { pair.substring(0, eq) } else { pair }
      val value = if eq >= 0 { pair.substring(eq + 1) } else { "" }
      if name == "db" {
        match parseDatabase(value) {
          case Ok(n) -> {
            database = n
          }
          case Err(e) -> return Err(e)
        }
      } else if name == "protocol" {
        if value == "2" {
          protocol = 2
        } else if value == "3" {
          protocol = 3
        } else {
          return Err("protocol must be 2 or 3")
        }
      } else if name.length > 0 {
        return Err("unknown URL parameter: " + name)
      }
    }
  }
  Ok(
    RedisUrl(
      host = address.host,
      port = address.port,
      username = userInfo.user,
      password = userInfo.password,
      database = database,
      protocol = protocol,
      tls = found.scheme.tls
    )
  )
}
