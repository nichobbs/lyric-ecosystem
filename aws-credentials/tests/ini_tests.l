//! tests/ini_tests.l - the INI dialect of the AWS shared files.
@test_module
package Aws.Credentials.IniTests

import Std.Core
import Std.Collections
import Std.String as Str
import Std.Testing
import Aws.Credentials.Ini

func parsed(text: in String): IniFile {
  match parseIni(text, "test") {
    case Ok(f) -> f
    case Err(message) -> panic("parseIni failed: " + message)
  }
}

func errorOf(text: in String): String {
  match parseIni(text, "test") {
    case Ok(_) -> "parsed"
    case Err(message) -> message
  }
}

func valueOf(file: in IniFile, section: in String, key: in String): String {
  match file.section(section) {
    case Some(s) -> if s.has(key) { s.get(key) } else { "<no key>" }
    case None -> "<no section>"
  }
}

test "sections and properties, with whitespace everywhere it is allowed" {
  val f = parsed("[default]\naws_access_key_id=AKIA1\n  \n[ profile dev ]\t\nregion =   eu-west-1  \nOUTPUT\t=\tjson\n")
  assertEqualInt(f.sections.count, 2, "two sections")
  assertEqual(valueOf(f, "default", "aws_access_key_id"), "AKIA1", "no spaces around =")
  assertEqual(valueOf(f, "profile dev", "region"), "eu-west-1", "the section name and the value are trimmed")
  assertEqual(valueOf(f, "profile dev", "output"), "json", "keys are lowercased, tabs are whitespace")
  assertEqual(valueOf(f, "profile dev", "missing"), "<no key>", "a missing key")
  assertEqual(valueOf(f, "nope", "region"), "<no section>", "a missing section")
}

test "comments: whole lines, after a section header, and after whitespace in a value" {
  val text = "# leading\n; also\n[default] # the default\nregion = us-east-1 # trailing\na = b;c\nb = x ;y\n  # indented comment\nc = #only\n"
  val f = parsed(text)
  assertEqual(valueOf(f, "default", "region"), "us-east-1", "a trailing # comment is removed")
  assertEqual(valueOf(f, "default", "a"), "b;c", "; without whitespace before it is part of the value")
  assertEqual(valueOf(f, "default", "b"), "x", "a trailing ; comment is removed")
  assertEqual(valueOf(f, "default", "c"), "#only", "a value that starts with # is a value")
}

test "CRLF and bare CR line endings, and a file without a final newline" {
  val f = parsed("[default]\r\nregion = us-east-1\r\n[other]\rkey = v")
  assertEqual(valueOf(f, "default", "region"), "us-east-1", "CRLF")
  assertEqual(valueOf(f, "other", "key"), "v", "CR, no final newline")
}

test "a sub-property block is kept apart from the section's own properties" {
  val text = "[default]\nregion = us-east-1\ns3 =\n  signature_version = s3v4\n  addressing_style = path\n\taws_access_key_id = NOT-A-KEY\nendpoint_url = https://x\n"
  val f = parsed(text)
  assertEqual(valueOf(f, "default", "s3"), "", "the opening property is empty")
  assertEqual(valueOf(f, "default", "signature_version"), "<no key>", "a sub-property is not a property")
  assertEqual(valueOf(f, "default", "aws_access_key_id"), "<no key>", "even when it is named like a credential")
  assertEqual(valueOf(f, "default", "endpoint_url"), "https://x", "the property after the block is read")
  match f.section("default") {
    case None -> assertTrue(false, "no default section")
    case Some(s) -> match mapGet(s.subProperties, "s3") {
      case None -> assertTrue(false, "no s3 block")
      case Some(block) -> {
        assertEqualInt(mapSize(block), 3, "three sub-properties")
        assertEqual(
          match mapGet(block, "addressing_style") { case Some(v) -> v; case None -> "" },
          "path",
          "a sub-property value"
        )
      }
    }
  }
}

test "an indented line after a property with a value continues the value" {
  val f = parsed("[default]\ncredential_process = /bin/tool\n  --flag\nregion = r\n")
  assertEqual(valueOf(f, "default", "credential_process"), "/bin/tool\n--flag", "joined with a newline")
  assertEqual(valueOf(f, "default", "region"), "r", "the next property is its own")
}

test "a repeated section is merged, and a later value wins" {
  val f = parsed("[a]\nx = 1\ny = 1\n[b]\nx = 9\n[a]\ny = 2\nz = 3\n")
  assertEqualInt(f.sections.count, 2, "two sections")
  assertEqual(valueOf(f, "a", "x"), "1", "kept")
  assertEqual(valueOf(f, "a", "y"), "2", "replaced")
  assertEqual(valueOf(f, "a", "z"), "3", "added")
}

test "a value may hold = and brackets" {
  val f = parsed("[default]\naws_session_token = abc==\nweird = [x]=y\n")
  assertEqual(valueOf(f, "default", "aws_session_token"), "abc==", "base64 padding")
  assertEqual(valueOf(f, "default", "weird"), "[x]=y", "brackets in a value")
}

test "an empty file and a file of comments have no sections" {
  assertEqualInt(parsed("").sections.count, 0, "empty")
  assertEqualInt(parsed("# nothing\n\n; here\n").sections.count, 0, "comments only")
}

test "malformed lines are errors that name the line and never quote it" {
  val secret = "wJalrXUtnFEMI/K7MDENG"
  val e1 = errorOf("[default]\n" + secret + "\n")
  assertEqual(e1, "test line 2: expected '[section]' or 'name = value'", "a line without =")
  assertTrue(not Str.contains(e1, secret), "the line is not quoted")
  assertEqual(errorOf("region = x\n"), "test line 1: a property before any section", "no section")
  assertEqual(errorOf("[default\nregion = x\n"), "test line 1: a section header without ']'", "no ]")
  assertEqual(errorOf("[default] trailing\n"), "test line 1: text after a section header", "text after ]")
  assertEqual(errorOf("[]\n"), "test line 1: a section header with no name", "empty name")
  assertEqual(errorOf("[a]\n= v\n"), "test line 2: expected '[section]' or 'name = value'", "empty key")
  assertEqual(
    errorOf("[a]\n  x = 1\n"),
    "test line 2: an indented line that continues no property",
    "nothing to continue"
  )
  assertEqual(errorOf("  x = 1\n"), "test line 1: an indented line before any section", "indented first line")
  assertEqual(
    errorOf("[a]\ns3 =\n  " + secret + "\n"),
    "test line 3: a sub-property must be 'name = value'",
    "bad sub-property"
  )
}
