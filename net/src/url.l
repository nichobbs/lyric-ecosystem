//! Net.Url - the authority part of a connection URL,
//! `scheme://[user[:password]@]host[:port]`, for clients whose URL grammar
//! is their own (`redis://host/2`, `amqp://host/vhost?heartbeat=30`,
//! `stomp+ssl://host`).
//!
//! A client names its schemes with their default ports (`matchScheme`), cuts
//! its own path and query off the rest, and hands the authority to
//! `splitUserInfo` and `parseHostPort`. Problems come back as values, so each
//! client words its own errors; `authorityProblemText` and
//! `schemeProblemText` are a ready wording.
//!
//! ```
//! import Net.Url as Url
//!
//! val schemes = [Url.Scheme(name = "tcp", defaultPort = 9000, tls = false),
//!                Url.Scheme(name = "tcps", defaultPort = 9443, tls = true)]
//! match Url.matchScheme("tcps://app:s%40cret@[::1]", schemes, true) {
//!   case Err(p) -> println(Url.schemeProblemText(p, schemes))
//!   case Ok(m) -> match Url.splitUserInfo(m.rest) {
//!     case Err(p) -> println(Url.authorityProblemText(p, "user", "password", m.scheme))
//!     case Ok(u) -> match Url.parseHostPort(u.hostPort, m.scheme.defaultPort) {
//!       case Err(p) -> println(Url.authorityProblemText(p, "user", "password", m.scheme))
//!       case Ok(hp) -> println(hp.host + " " + hp.port.toString())   // ::1 9443
//!     }
//!   }
//! }
//! ```
//!
//! The user and password are percent-decoded, so a password may hold `@`,
//! `:` or `/` written as `%40`, `%3A`, `%2F`. An IPv6 literal is written in
//! brackets (`[::1]:6379`); without them it is refused, because its last
//! group cannot be told from a port (`::1` would read as host `::`, port 1).
//! No problem value carries the password.
@runtime_checked
@pure
package Net.Url

import Std.Core
import Std.Collections
import Std.Encoding
import Std.Parse
import Std.String as Str

/// A URL scheme a client accepts.
pub record Scheme {
  /// The scheme without `://`, in lower case: `"amqps"`.
  name: String
  /// The port used when the URL names none.
  defaultPort: Int
  /// Whether the scheme means TLS.
  tls: Bool
}

/// A URL whose scheme was recognized.
pub record SchemeMatch {
  scheme: Scheme
  /// Everything after `://`.
  rest: String
}

/// Why `matchScheme` refused a URL.
pub union SchemeProblem {
  /// The text has no `://`.
  case NoScheme
  /// The scheme is none of the accepted ones. `name` is the scheme as
  /// compared (in lower case when case is ignored).
  case UnsupportedScheme(name: String)
}

/// Find the scheme of `url` among `schemes`. With `ignoreCase` the URL's
/// scheme is compared in lower case (`AMQP://` is `amqp`).
pub func matchScheme(url: in String, schemes: in slice[Scheme], ignoreCase: in Bool): Result[SchemeMatch, SchemeProblem] {
  val schemeEnd = Str.indexOfRaw(url, "://")
  if schemeEnd < 0 {
    return Err(NoScheme)
  }
  val written = Str.substring(url, 0, schemeEnd)
  val name = if ignoreCase { Str.toLower(written) } else { written }
  for s in schemes {
    if s.name == name {
      return Ok(SchemeMatch(scheme = s, rest = Str.substring(url, schemeEnd + 3)))
    }
  }
  Err(UnsupportedScheme(name = name))
}

func schemeNames(schemes: in slice[Scheme]): String {
  var names = ""
  var i = 0
  while i < schemes.length {
    if i > 0 {
      names = names + (if i == schemes.length - 1 { " or " } else { ", " })
    }
    names = names + schemes[i].name
    i += 1
  }
  names
}

/// `problem` as text: "unsupported scheme 'http', expected amqp or amqps".
pub func schemeProblemText(problem: in SchemeProblem, schemes: in slice[Scheme]): String {
  match problem {
    case NoScheme -> "no scheme, expected " + schemeNames(schemes)
    case UnsupportedScheme(name) -> "unsupported scheme '" + name + "', expected " + schemeNames(schemes)
  }
}

/// Why a percent-encoded text does not decode.
pub union EscapeProblem {
  /// A `%` with fewer than two characters after it.
  case TruncatedEscape
  /// A `%` followed by something other than two hexadecimal digits.
  case InvalidEscape
  /// The decoded bytes are not UTF-8.
  case EscapeNotUtf8
}

/// `problem` as text.
pub func escapeProblemText(problem: in EscapeProblem): String {
  match problem {
    case TruncatedEscape -> "a '%' is not followed by two hexadecimal digits"
    case InvalidEscape -> "a '%' is not followed by two hexadecimal digits"
    case EscapeNotUtf8 -> "percent-encoded bytes are not valid UTF-8"
  }
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

/// `text` with every `%XX` replaced by the byte it names, read as UTF-8, so
/// `%C3%A9` decodes to one character.
pub func percentDecode(text: in String): Result[String, EscapeProblem] {
  if not Str.contains(text, "%") {
    return Ok(text)
  }
  val bytes: List[Byte] = newList()
  var i = 0
  while i < text.length {
    val unit = text.codeUnitAt(i)
    if unit == 37 {
      if i + 2 > text.length - 1 {
        return Err(TruncatedEscape)
      }
      val hi = hexValue(text.codeUnitAt(i + 1))
      val lo = hexValue(text.codeUnitAt(i + 2))
      if hi < 0 or lo < 0 {
        return Err(InvalidEscape)
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
    case None -> Err(EscapeNotUtf8)
  }
}

/// Why an authority (`[user[:password]@]host[:port]`) was refused.
pub union AuthorityProblem {
  case UserEscape(problem: EscapeProblem)
  case PasswordEscape(problem: EscapeProblem)
  /// `[` without `]`.
  case UnclosedBracket
  /// Something other than `:port` after `]`.
  case TextAfterBracket
  /// More than one `:` outside brackets: an IPv6 literal without them.
  case UnbracketedIpv6
  case EmptyHost
  /// `text` is what stood where the port should.
  case PortNotNumber(text: String)
  case PortOutOfRange(text: String)
}

/// `problem` as text. `userWord` and `passwordWord` are the client's names
/// for the two halves of the user information (`"login"`, `"passcode"`);
/// `scheme` words the example in the IPv6 hint.
pub func authorityProblemText(
  problem: in AuthorityProblem,
  userWord: in String,
  passwordWord: in String,
  scheme: in Scheme,
): String {
  match problem {
    case UserEscape(p) -> userWord + ": " + escapeProblemText(p)
    case PasswordEscape(p) -> passwordWord + ": " + escapeProblemText(p)
    case UnclosedBracket -> "an IPv6 address is missing its closing bracket"
    case TextAfterBracket -> "unexpected text after the IPv6 address"
    case UnbracketedIpv6 -> "an IPv6 address must be written in brackets, as in " + scheme.name + "://[::1]:" +
      scheme.defaultPort.toString()
    case EmptyHost -> "the host is empty"
    case PortNotNumber(text) -> "port '" + text + "' is not a number"
    case PortOutOfRange(text) -> "port " + text + " is out of range"
  }
}

/// An authority cut at its last `@`.
pub record UserInfo {
  /// Whether there was an `@`. Without one, `user` and `password` are empty.
  present: Bool
  /// The text before the first `:` of the user information, decoded.
  user: String
  /// The text after it, decoded; empty when there is no `:`.
  password: String
  /// What follows the `@`: `host[:port]`.
  hostPort: String
}

/// Cut the user information off `authority`. The `@` that ends it is the
/// last one, so a password with a literal `@` still parses; the `:` that
/// splits it is the first one.
pub func splitUserInfo(authority: in String): Result[UserInfo, AuthorityProblem] {
  val at = Str.lastIndexOfRaw(authority, "@")
  if at < 0 {
    return Ok(UserInfo(present = false, user = "", password = "", hostPort = authority))
  }
  val userInfo = Str.substring(authority, 0, at)
  val colon = Str.indexOfRaw(userInfo, ":")
  val rawUser = if colon < 0 { userInfo } else { Str.substring(userInfo, 0, colon) }
  val rawPassword = if colon < 0 { "" } else { Str.substring(userInfo, colon + 1) }
  val user = match percentDecode(rawUser) { case Ok(v) -> v; case Err(p) -> return Err(UserEscape(problem = p)) }
  val password = match percentDecode(rawPassword) {
    case Ok(v) -> v
    case Err(p) -> return Err(PasswordEscape(problem = p))
  }
  Ok(UserInfo(present = true, user = user, password = password, hostPort = Str.substring(authority, at + 1)))
}

/// A host and a port.
pub record HostPort {
  /// A name, an IPv4 address, or an IPv6 address without its brackets.
  host: String
  port: Int
}

func parsePort(text: in String): Result[Int, AuthorityProblem] {
  match parseOptInt(text) {
    case Some(p) -> if p >= 1 and p <= 65535 { Ok(p) } else { Err(PortOutOfRange(text = text)) }
    case None -> Err(PortNotNumber(text = text))
  }
}

/// Parse `host`, `host:port`, `[v6]` or `[v6]:port`; `defaultPort` is used
/// when no port is written.
pub func parseHostPort(text: in String, defaultPort: in Int): Result[HostPort, AuthorityProblem] {
  var host = text
  var port = defaultPort
  if Str.startsWith(text, "[") {
    val close = Str.indexOfRaw(text, "]")
    if close < 0 {
      return Err(UnclosedBracket)
    }
    host = Str.substring(text, 1, close - 1)
    val after = Str.substring(text, close + 1)
    if after.length > 0 {
      if not Str.startsWith(after, ":") {
        return Err(TextAfterBracket)
      }
      port = match parsePort(Str.substring(after, 1)) { case Ok(p) -> p; case Err(e) -> return Err(e) }
    }
  } else {
    val colon = Str.lastIndexOfRaw(text, ":")
    if colon >= 0 and Str.indexOfRaw(text, ":") != colon {
      return Err(UnbracketedIpv6)
    }
    if colon >= 0 {
      host = Str.substring(text, 0, colon)
      port = match parsePort(Str.substring(text, colon + 1)) { case Ok(p) -> p; case Err(e) -> return Err(e) }
    }
  }
  if host.length == 0 {
    return Err(EmptyHost)
  }
  Ok(HostPort(host = host, port = port))
}
