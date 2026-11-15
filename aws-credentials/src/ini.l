//! Aws.Credentials.Ini - the INI dialect of the AWS shared files
//! (`~/.aws/config`, `~/.aws/credentials`).
//!
//! The rules are the AWS SDKs':
//!
//!   * a line whose first non-blank character is `#` or `;` is a comment, and
//!     so is a blank line;
//!   * `[name]` opens a section; text after `]` must be blank or a comment;
//!   * `key = value` sets a property of the open section. Keys are
//!     case-insensitive (stored lowercase); the value is trimmed, and ` #` or
//!     ` ;` (a comment character after whitespace) starts a trailing comment.
//!     `key=value;x` keeps `;x`;
//!   * a line that starts with whitespace continues the property before it.
//!     After a property whose value is empty (`s3 =`) it is a sub-property
//!     (`  addressing_style = path`), kept apart from the section's own
//!     properties; after a property with a value it is a continuation of that
//!     value, joined with a newline;
//!   * the same section may appear several times: its properties are merged,
//!     and a later value replaces an earlier one.
//!
//! A line that is none of these is an error that names the line number and
//! never quotes the line: it may hold a secret.
@runtime_checked
package Aws.Credentials.Ini

import Std.Core
import Std.Collections
import Std.String as Str

/// One section: its properties, and the sub-property blocks keyed by the
/// property that opens them.
pub record IniSection {
  name: String
  properties: Map[String, String]
  subProperties: Map[String, Map[String, String]]
}

/// A parsed file: sections in the order they first appear.
pub record IniFile {
  sections: List[IniSection]
}

func isBlank(c: in Char): Bool {
  c == ' ' or c == '\t'
}

func isCommentStart(c: in Char): Bool {
  c == '#' or c == ';'
}

// The value with a trailing comment (whitespace, then `#` or `;`) removed.
func stripTrailingComment(raw: in String): String {
  val value = Str.trim(raw)
  var i = 1
  while i < value.length {
    if isCommentStart(Str.charAtOrReplacement(value, i)) and isBlank(Str.charAtOrReplacement(value, i - 1)) {
      return Str.trim(value.substring(0, i))
    }
    i = i + 1
  }
  value
}

func sectionNamed(file: in IniFile, name: in String): Option[IniSection] {
  var i = 0
  while i < file.sections.count {
    if file.sections[i].name == name {
      return Some(value = file.sections[i])
    }
    i = i + 1
  }
  None
}

/// The section `name`, when the file has one.
@pure
pub func IniFile.section(file: in IniFile, name: in String): Option[IniSection] {
  sectionNamed(file, name)
}

/// The value of `key` (lowercase) in `section`, or `""`.
@pure
pub func IniSection.get(section: in IniSection, key: in String): String {
  match mapGet(section.properties, key) {
    case Some(v) -> v
    case None -> ""
  }
}

/// True when `section` has a property `key` (lowercase), even an empty one.
@pure
pub func IniSection.has(section: in IniSection, key: in String): Bool {
  section.properties.containsKey(key)
}

func problem(source: in String, lineNumber: in Int, what: in String): String {
  source + " line " + lineNumber.toString() + ": " + what
}

/// Parse `text`. `source` names the file in error messages. `Err` names the
/// line number of the first line that is not valid; the line itself is never
/// quoted.
pub func parseIni(text: in String, source: in String): Result[IniFile, String] {
  val file = IniFile(sections = newList())
  val lines = Str.split(Str.replace(Str.replace(text, "\r\n", "\n"), "\r", "\n"), "\n")
  var current: Option[IniSection] = None
  // The property the last property line set, `""` when a continuation line
  // may not follow.
  var lastKey = ""
  var lastWasEmpty = false
  var n = 0
  while n < lines.length {
    val raw = lines[n]
    val lineNumber = n + 1
    n = n + 1
    val trimmed = Str.trim(raw)
    if trimmed.length == 0 {
      continue
    }
    val firstChar = Str.charAtOrReplacement(raw, 0)
    if isCommentStart(firstChar) {
      continue
    }
    if isBlank(firstChar) {
      if isCommentStart(Str.charAtOrReplacement(trimmed, 0)) {
        continue
      }
      val section = match current {
        case Some(s) -> s
        case None -> return Err(problem(source, lineNumber, "an indented line before any section"))
      }
      if lastKey.length == 0 {
        return Err(problem(source, lineNumber, "an indented line that continues no property"))
      }
      if lastWasEmpty {
        val eq = Str.indexOfRaw(trimmed, "=")
        if eq <= 0 {
          return Err(problem(source, lineNumber, "a sub-property must be 'name = value'"))
        }
        val subKey = Str.toLower(Str.trim(trimmed.substring(0, eq)))
        val subValue = stripTrailingComment(trimmed.substring(eq + 1))
        val block: Map[String, String] = match mapGet(section.subProperties, lastKey) {
          case Some(b) -> b
          case None -> newMap()
        }
        block[subKey] = subValue
        section.subProperties[lastKey] = block
      } else {
        section.properties[lastKey] = section.get(lastKey) + "\n" + stripTrailingComment(trimmed)
      }
      continue
    }
    if firstChar == '[' {
      val close = Str.indexOfRaw(trimmed, "]")
      if close < 0 {
        return Err(problem(source, lineNumber, "a section header without ']'"))
      }
      val after = Str.trim(trimmed.substring(close + 1))
      if after.length > 0 and not isCommentStart(Str.charAtOrReplacement(after, 0)) {
        return Err(problem(source, lineNumber, "text after a section header"))
      }
      val name = Str.trim(trimmed.substring(1, close - 1))
      if name.length == 0 {
        return Err(problem(source, lineNumber, "a section header with no name"))
      }
      current = match sectionNamed(file, name) {
        case Some(existing) -> Some(value = existing)
        case None -> {
          val fresh = IniSection(name = name, properties = newMap(), subProperties = newMap())
          file.sections.add(fresh)
          Some(value = fresh)
        }
      }
      lastKey = ""
      lastWasEmpty = false
      continue
    }
    val section = match current {
      case Some(s) -> s
      case None -> return Err(problem(source, lineNumber, "a property before any section"))
    }
    val eq = Str.indexOfRaw(trimmed, "=")
    if eq <= 0 {
      return Err(problem(source, lineNumber, "expected '[section]' or 'name = value'"))
    }
    val key = Str.toLower(Str.trim(trimmed.substring(0, eq)))
    val value = stripTrailingComment(trimmed.substring(eq + 1))
    section.properties[key] = value
    if value.length == 0 {
      // A later `key =` block replaces an earlier one of the same name.
      val empty: Map[String, String] = newMap()
      section.subProperties[key] = empty
    } else if section.subProperties.containsKey(key) {
      section.subProperties.remove(key)
    }
    lastKey = key
    lastWasEmpty = value.length == 0
  }
  Ok(file)
}
