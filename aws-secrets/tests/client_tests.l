//! tests/client_tests.l - the client's pure parts: endpoint parsing, region
//! validation, error documents and the validation `openClient` does.
@test_module
package AwsSecrets.ClientTests

import Std.Core
import Std.Testing
import AwsSecrets.Client

test "parseEndpoint: https, loopback http and the dropped default port" {
  match parseEndpoint("https://Secrets.Example.com:443/") {
    case Ok(e) -> {
      assertEqual(e.scheme, "https", "scheme")
      assertEqual(e.authority, "secrets.example.com", "default port dropped, host lowercased")
    }
    case Err(m) -> assertTrue(false, "unexpected error: " + m)
  }
  match parseEndpoint("http://127.0.0.1:9297") {
    case Ok(e) -> assertEqual(e.authority, "127.0.0.1:9297", "loopback http keeps its port")
    case Err(m) -> assertTrue(false, "unexpected error: " + m)
  }
}

test "parseEndpoint: refuses plain http to a remote host, paths and credentials" {
  assertTrue(parseEndpoint("http://example.com").isErr(), "http to a remote host")
  assertTrue(parseEndpoint("https://example.com/path").isErr(), "a path")
  assertTrue(parseEndpoint("https://user:pw@example.com").isErr(), "credentials")
  assertTrue(parseEndpoint("ftp://example.com").isErr(), "another scheme")
  assertTrue(parseEndpoint("https://").isErr(), "no host")
}

test "isValidRegion accepts region names and nothing that could alter a host name" {
  assertTrue(isValidRegion("us-east-1"), "us-east-1")
  assertFalse(isValidRegion(""), "empty")
  assertFalse(isValidRegion("us.east"), "a dot")
  assertFalse(isValidRegion("US-EAST-1"), "uppercase")
  assertFalse(isValidRegion("a/b"), "a slash")
}

test "parseFault reads the code after # and either spelling of the message" {
  match parseFault("{\"__type\":\"com.amazonaws.ssm#ParameterNotFound\",\"message\":\"gone\"}") {
    case Some(f) -> {
      assertEqual(f.code, "ParameterNotFound", "code")
      assertEqual(f.message, "gone", "message")
    }
    case None -> assertTrue(false, "expected a fault")
  }
  match parseFault("{\"__type\":\"ResourceNotFoundException\",\"Message\":\"Secrets Manager can't find it\"}") {
    case Some(f) -> assertEqual(f.message, "Secrets Manager can't find it", "capitalised Message")
    case None -> assertTrue(false, "expected a fault")
  }
  assertTrue(parseFault("<html>bad gateway</html>").isNone(), "not JSON")
  assertTrue(parseFault("{\"x\":1}").isNone(), "no type")
}

test "faultOf: the code of an error document, HTTP_<status> otherwise" {
  val known = faultOf("GetSecretValue", 400, "{\"__type\":\"ResourceNotFoundException\",\"message\":\"no\"}")
  assertEqual(known.code, "ResourceNotFoundException", "code")
  assertEqualInt(known.status, 400, "status")
  val unknown = faultOf("GetSecretValue", 502, "<html/>")
  assertEqual(unknown.code, "HTTP_502", "no document")
}

func openCode(cfg: in ClientConfig): String {
  match openClient(secretsManagerService(), cfg) {
    case Ok(_) -> "OK"
    case Err(e) -> e.code
  }
}

test "openClient validates the configuration" {
  assertEqual(openCode(staticConfig("us-east-1", "", "AKIDEXAMPLE", "secret")), "OK", "a valid configuration")
  assertEqual(openCode(staticConfig("", "", "AKIDEXAMPLE", "secret")), "INVALID_CONFIG", "no region")
  assertEqual(openCode(staticConfig("us-east-1", "", "", "")), "INVALID_CONFIG", "no credentials")
  assertEqual(openCode(staticConfig("us-east-1", "http://example.com", "a", "b")), "INVALID_CONFIG", "plain http")
  assertEqual(openCode(staticConfig("us-east-1", "", "a", "b").withRequestTimeout(0)), "INVALID_CONFIG", "timeout")
}
