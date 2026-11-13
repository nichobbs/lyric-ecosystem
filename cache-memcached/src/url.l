//! Cache.Memcached.Url - parsing of memcached connection URLs.
//!
//!   memcached://[user:password@]host[:port]
//!   memcached+tls://...   the same, over TLS
//!
//! The port defaults to 11211. `user:password` are the credentials of
//! memcached's ASCII authentication (`memcached -Y <file>`), percent-decoded.
//! An IPv6 host is written in brackets (`memcached://[::1]:11211`). The URL
//! names one server and has no path and no parameters; certificates and the
//! other TLS options are not URL parameters (see `Client.withTlsOptions`).
//!
//! memcached has no registered URL scheme. `memcached` is what most clients
//! accept; `+tls` follows the `<protocol>+<transport>` form this repository
//! already uses (`stomp+ssl`) and says what it does: the same protocol inside
//! TLS, as a server started with `-Z` speaks it.
@runtime_checked
@pure
package Cache.Memcached.Url

import Std.Core
import Std.String as Str
import Net.Url as NetUrl

/// The parts of a connection URL.
@experimental
pub record MemcachedUrl {
  host: String
  port: Int
  /// User name; `""` when the URL carries no credentials.
  username: String
  /// Password; `""` when the URL carries no credentials.
  password: String
  /// True for `memcached+tls://`: the connection is made over TLS.
  tls: Bool
}

func schemes(): slice[NetUrl.Scheme] {
  [
    NetUrl.Scheme(name = "memcached", defaultPort = 11211, tls = false),
    NetUrl.Scheme(name = "memcached+tls", defaultPort = 11211, tls = true)
  ]
}

/// Parse a `memcached://` or `memcached+tls://` URL. The scheme is matched
/// without regard to case. The error text never contains the URL's password.
@experimental
pub func parseMemcachedUrl(url: in String): Result[MemcachedUrl, String] {
  val found = match NetUrl.matchScheme(url, schemes(), true) {
    case Ok(m) -> m
    case Err(p) -> return Err(NetUrl.schemeProblemText(p, schemes()))
  }
  var rest = found.rest
  if Str.indexOfRaw(rest, "?") >= 0 or Str.indexOfRaw(rest, "#") >= 0 {
    return Err("a memcached URL has no parameters")
  }
  val slash = Str.indexOfRaw(rest, "/")
  if slash >= 0 {
    if slash != rest.length - 1 {
      return Err("a memcached URL has no path")
    }
    rest = Str.substring(rest, 0, slash)
  }
  if Str.indexOfRaw(rest, ",") >= 0 {
    return Err("a memcached URL names one server; a server list is not supported")
  }
  val userInfo = match NetUrl.splitUserInfo(rest) {
    case Ok(u) -> u
    case Err(p) -> return Err(NetUrl.authorityProblemText(p, "user name", "password", found.scheme))
  }
  if userInfo.present and (userInfo.user.length == 0 or userInfo.password.length == 0) {
    return Err("credentials need a user name and a password (memcached://user:password@host)")
  }
  val address = match NetUrl.parseHostPort(userInfo.hostPort, found.scheme.defaultPort) {
    case Ok(hp) -> hp
    case Err(p) -> return Err(NetUrl.authorityProblemText(p, "user name", "password", found.scheme))
  }
  Ok(
    MemcachedUrl(
      host = address.host,
      port = address.port,
      username = userInfo.user,
      password = userInfo.password,
      tls = found.scheme.tls
    )
  )
}
