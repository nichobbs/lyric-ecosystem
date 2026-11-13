//! Redis.Url - parsing of `redis://` and `rediss://` connection URLs.
//!
//!   redis://[username[:password]@]host[:port][/database][?db=N&protocol=2|3]
//!   rediss://...   the same, over TLS
//!
//! `redis://:secret@host` carries a password and no user name (the classic
//! `requirepass` form); `redis://app:secret@host` an ACL user and its
//! password. User name and password are percent-decoded. An IPv6 host is
//! written in brackets (`redis://[::1]:6379`). The `rediss` scheme only switches
//! TLS on; certificates and the other TLS options are not URL parameters (see
//! `Redis.withExclusiveCaCertificate` and its siblings).
@runtime_checked
@pure
package Redis.Url

import Std.Core
import Std.Collections
import Std.Encoding
import Std.Parse
import Std.String as Str

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

func hexValue(unit: in Int): Int {
  if unit >= 48 and unit <= 57 {
    unit - 48
  } else if unit >= 65 and unit <= 70 {
    unit - 55
  } else if unit >= 97 and unit <= 102 {
    unit - 87
  } else {
    0 - 1
  }
}

// Percent-decoding over the UTF-8 bytes, so `%C3%A9` decodes to one character.
func percentDecode(s: in String): Result[String, String] {
  if not Str.contains(s, "%") {
    return Ok(s)
  }
  val raw = encodeUtf8(s)
  val acc: List[Byte] = newList()
  var i = 0
  while i < raw.length {
    val b = raw[i].toInt().and(255)
    if b == 37 {
      if i + 2 >= raw.length {
        return Err("truncated percent escape")
      }
      val hi = hexValue(raw[i + 1].toInt().and(255))
      val lo = hexValue(raw[i + 2].toInt().and(255))
      if hi < 0 or lo < 0 {
        return Err("invalid percent escape")
      }
      acc.add((hi * 16 + lo).toByte())
      i = i + 3
    } else {
      acc.add(raw[i])
      i = i + 1
    }
  }
  match tryDecodeUtf8(acc.toArray()) {
    case Some(decoded) -> Ok(decoded)
    case None -> Err("percent escapes do not decode to UTF-8")
  }
}

func parseDatabase(text: in String): Result[Int, String] {
  match parseOptInt(text) {
    case Some(n) -> if n >= 0 { Ok(n) } else { Err("database index must not be negative") }
    case None -> Err("database index is not a number: " + text)
  }
}

record HostPort {
  host: String
  port: Int
}

func parsePort(text: in String): Result[Int, String] {
  match parseOptInt(text) {
    case Some(p) -> if p >= 1 and p <= 65535 { Ok(p) } else { Err("port out of range: " + text) }
    case None -> Err("port is not a number: " + text)
  }
}

func parseHostPort(text: in String): Result[HostPort, String] {
  if text.startsWith("[") {
    val close = Str.indexOfRaw(text, "]")
    if close < 0 {
      return Err("unterminated IPv6 host")
    }
    val host = text.substring(1, close - 1)
    val rest = text.substring(close + 1)
    if host.length == 0 {
      return Err("empty host")
    }
    if rest.length == 0 {
      return Ok(HostPort(host = host, port = 6379))
    }
    if not rest.startsWith(":") {
      return Err("unexpected text after the IPv6 host")
    }
    return match parsePort(rest.substring(1)) { case Ok(p) -> Ok(HostPort(host = host, port = p)); case Err(e) -> Err(e) }
  }
  val colon = Str.lastIndexOfRaw(text, ":")
  if colon < 0 {
    if text.length == 0 {
      return Err("empty host")
    }
    return Ok(HostPort(host = text, port = 6379))
  }
  val host = text.substring(0, colon)
  if host.length == 0 {
    return Err("empty host")
  }
  match parsePort(text.substring(colon + 1)) {
    case Ok(p) -> Ok(HostPort(host = host, port = p))
    case Err(e) -> Err(e)
  }
}

/// Parse a `redis://` or `rediss://` URL. The error text never contains the
/// URL's password.
@experimental
pub func parseRedisUrl(url: in String): Result[RedisUrl, String] {
  val tls = url.startsWith("rediss://")
  if not tls and not url.startsWith("redis://") {
    return Err("a Redis URL starts with redis:// or rediss://")
  }
  var rest = if tls { url.substring(9) } else { url.substring(8) }
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
  var username = ""
  var password = ""
  val at = Str.lastIndexOfRaw(rest, "@")
  if at >= 0 {
    val userinfo = rest.substring(0, at)
    rest = rest.substring(at + 1)
    val colon = Str.indexOfRaw(userinfo, ":")
    val rawUser = if colon >= 0 { userinfo.substring(0, colon) } else { userinfo }
    val rawPassword = if colon >= 0 { userinfo.substring(colon + 1) } else { "" }
    match percentDecode(rawUser) {
      case Ok(u) -> {
        username = u
      }
      case Err(e) -> return Err("user name: " + e)
    }
    match percentDecode(rawPassword) {
      case Ok(p) -> {
        password = p
      }
      case Err(e) -> return Err("password: " + e)
    }
    if username.length > 0 and password.length == 0 {
      return Err("a user name needs a password (redis://user:password@host)")
    }
  }
  var host = ""
  var port = 6379
  match parseHostPort(rest) {
    case Ok(hp) -> {
      host = hp.host
      port = hp.port
    }
    case Err(e) -> return Err(e)
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
      host = host,
      port = port,
      username = username,
      password = password,
      database = database,
      protocol = protocol,
      tls = tls
    )
  )
}
