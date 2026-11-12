//! Mq.ActiveMq.Url - broker URLs of the form
//! `stomp://[login[:passcode]@]host[:port]`.
//!
//! The login and passcode are percent-decoded, so a passcode may hold `@`,
//! `:` or `/` written as `%40`, `%3A`, `%2F`. An IPv6 literal is written in
//! brackets (`stomp://[::1]:61613`). The port defaults to 61613, the STOMP
//! port of a stock ActiveMQ. `stomp+ssl`, `stomps` and `ssl` URLs are refused:
//! this client speaks plain TCP only.
@runtime_checked
package Mq.ActiveMq.Url

import Std.Core
import Std.Collections
import Std.Encoding
import Std.Parse
import Std.String as Str

/// The STOMP port of a stock ActiveMQ broker.
pub val DEFAULT_PORT: Int = 61613

/// Where a broker listens and who connects to it.
pub record BrokerAddress {
  host: String
  port: Int
  login: String
  passcode: String
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
  "activemq: invalid broker URL: " + reason
}

func parsePort(text: in String): Result[Int, String] {
  match parseOptInt(text) {
    case Some(p) -> if p >= 1 and p <= 65535 { Ok(p) } else { Err(invalid("port " + text + " is out of range")) }
    case None -> Err(invalid("port '" + text + "' is not a number"))
  }
}

/// Parse a `stomp://` broker URL. The passcode never appears in an error.
pub func parseUrl(url: in String): Result[BrokerAddress, String] {
  val schemeEnd = Str.indexOfRaw(url, "://")
  if schemeEnd < 0 {
    return Err(invalid("expected stomp://host[:port]"))
  }
  val scheme = Str.toLower(Str.substring(url, 0, schemeEnd))
  if scheme == "stomp+ssl" or scheme == "stomps" or scheme == "ssl" or scheme == "stomp+nio+ssl" {
    return Err(invalid("TLS is not supported by this client; use a stomp:// connector"))
  }
  if scheme != "stomp" {
    return Err(invalid("unsupported scheme '" + scheme + "', expected stomp"))
  }
  var rest = Str.substring(url, schemeEnd + 3)
  if Str.endsWith(rest, "/") {
    rest = Str.substring(rest, 0, rest.length - 1)
  }
  if Str.contains(rest, "/") or Str.contains(rest, "?") or Str.contains(rest, "#") {
    return Err(invalid("a path, query or fragment is not allowed"))
  }
  var login = ""
  var passcode = ""
  val at = Str.lastIndexOfRaw(rest, "@")
  if at >= 0 {
    val userInfo = Str.substring(rest, 0, at)
    rest = Str.substring(rest, at + 1)
    val colon = Str.indexOfRaw(userInfo, ":")
    val rawLogin = if colon < 0 { userInfo } else { Str.substring(userInfo, 0, colon) }
    val rawPasscode = if colon < 0 { "" } else { Str.substring(userInfo, colon + 1) }
    login = match percentDecode(rawLogin) { case Ok(v) -> v; case Err(e) -> return Err(invalid("login: " + e)) }
    passcode = match percentDecode(rawPasscode) { case Ok(v) -> v; case Err(e) -> return Err(invalid("passcode: " + e)) }
    if login.length == 0 {
      return Err(invalid("the login before '@' is empty"))
    }
  }
  var host = rest
  var port = DEFAULT_PORT
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
  Ok(BrokerAddress(host = host, port = port, login = login, passcode = passcode))
}
