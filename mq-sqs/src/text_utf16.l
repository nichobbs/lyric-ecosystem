//! Mq.Sqs.Text, managed targets (`dotnet`, `jvm`) - the two functions of the
//! wire format that look at the characters of a string. A `String` is UTF-16
//! code units here; the native twin (`text_utf8.l`, same package) reads UTF-8
//! bytes.
//!
//! Internal: only `Mq.Sqs.Wire` should import this.
@cfg(not(target = "native"))
package Mq.Sqs.Text

import Std.Core
import Std.String as Str

func isSurrogate(u: in Int): Bool {
  u >= 0xD800 and u <= 0xDFFF
}

/// True when every character of `text` is one SQS accepts in a body or a
/// string attribute: tab, line feed, carriage return, U+0020 to U+D7FF,
/// U+E000 to U+FFFD, and well-formed surrogate pairs.
@pure
pub func sqsText(text: in String): Bool {
  var i = 0
  while i < text.length {
    val u = Str.codeUnitAt(text, i)
    if u >= 0xD800 and u <= 0xDBFF {
      if i + 1 >= text.length {
        return false
      }
      val low = Str.codeUnitAt(text, i + 1)
      if low < 0xDC00 or low > 0xDFFF {
        return false
      }
      i = i + 2
    } else {
      val ok = u == 9 or u == 10 or u == 13 or (u >= 0x20 and u <= 0xD7FF) or (u >= 0xE000 and u <= 0xFFFD)
      if not ok {
        return false
      }
      i = i + 1
    }
  }
  true
}

func hex4(u: in Int): String {
  val digits = "0123456789abcdef"
  digits.substring((u / 4096) % 16, 1) + digits.substring((u / 256) % 16, 1) + digits.substring((u / 16) % 16, 1) +
    digits.substring(u % 16, 1)
}

/// `text` as a JSON string literal in which every character SQS would refuse
/// in an attribute value is a `\uXXXX` escape, so the literal is always SQS
/// text whatever `text` holds.
@pure
pub func jsonLiteral(text: in String): String {
  val sb = Str.StringBuilder.new()
  sb.append("\"")
  var i = 0
  while i < text.length {
    val u = Str.codeUnitAt(text, i)
    if u == 34 {
      sb.append("\\\"")
    } else if u == 92 {
      sb.append("\\\\")
    } else if u == 10 {
      sb.append("\\n")
    } else if u == 13 {
      sb.append("\\r")
    } else if u == 9 {
      sb.append("\\t")
    } else if u < 0x20 or isSurrogate(u) or u >= 0xFFFE {
      sb.append("\\u")
      sb.append(hex4(u))
    } else {
      sb.append(text.substring(i, 1))
    }
    i = i + 1
  }
  sb.append("\"")
  sb.toString()
}
