//! Mq.Sqs.Text on `--target native`, the twin of `text_utf16.l` in the same
//! package. A `String` is UTF-8 bytes here (`length` counts bytes), so the
//! characters are read as code points; the results are the ones the managed
//! twin gives for the same text.
//!
//! Internal: only `Mq.Sqs.Wire` should import this.
@cfg(target = "native")
package Mq.Sqs.Text

import Std.Core
import Std.Encoding as Enc
import Std.String as Str

func utf8Length(cp: in Int): Int {
  if cp < 0x80 {
    1
  } else if cp < 0x800 {
    2
  } else if cp < 0x10000 {
    3
  } else {
    4
  }
}

/// True when every character of `text` is one SQS accepts in a body or a
/// string attribute: tab, line feed, carriage return, U+0020 to U+D7FF,
/// U+E000 to U+FFFD, and the characters above U+FFFF. Bytes that are not
/// well-formed UTF-8 are refused, as an unpaired surrogate is on the managed
/// targets.
@pure
pub func sqsText(text: in String): Bool {
  // `codePoints` reads each ill-formed byte as U+FFFD, three bytes for one:
  // the text is well formed exactly when its code points encode to its length.
  var bytes = 0
  for cp in Str.codePoints(text) {
    val ok = cp == 9 or cp == 10 or cp == 13 or (cp >= 0x20 and cp <= 0xD7FF) or (cp >= 0xE000 and cp <= 0xFFFD) or
      cp >= 0x10000
    if not ok {
      return false
    }
    bytes = bytes + utf8Length(cp)
  }
  bytes == text.length
}

func hex4(u: in Int): String {
  val digits = "0123456789abcdef"
  digits.substring((u / 4096) % 16, 1) + digits.substring((u / 256) % 16, 1) + digits.substring((u / 16) % 16, 1) +
    digits.substring(u % 16, 1)
}

/// `text` as a JSON string literal in which every character SQS would refuse
/// in an attribute value is a `\uXXXX` escape, so the literal is always SQS
/// text whatever `text` holds. A character above U+FFFF is written as the
/// escapes of its surrogate pair, as on the managed targets.
@pure
pub func jsonLiteral(text: in String): String {
  val sb = Str.StringBuilder.new()
  sb.append("\"")
  for cp in Str.codePoints(text) {
    if cp == 34 {
      sb.append("\\\"")
    } else if cp == 92 {
      sb.append("\\\\")
    } else if cp == 10 {
      sb.append("\\n")
    } else if cp == 13 {
      sb.append("\\r")
    } else if cp == 9 {
      sb.append("\\t")
    } else if cp < 0x20 or cp == 0xFFFE or cp == 0xFFFF {
      sb.append("\\u")
      sb.append(hex4(cp))
    } else if cp >= 0x10000 {
      val v = cp - 0x10000
      sb.append("\\u")
      sb.append(hex4(0xD800 + v / 0x400))
      sb.append("\\u")
      sb.append(hex4(0xDC00 + v % 0x400))
    } else {
      sb.append(Enc.codepointToString(cp))
    }
  }
  sb.append("\"")
  sb.toString()
}
