//! tests/env_tests.l - `Aws.SigV4.Env`: credentials, region and endpoint from
//! the standard AWS environment variables. The tests set the variables they
//! read (to documentation values, never a real credential) and restore what
//! was there.
@test_module
package Aws.SigV4.EnvTests

import Std.Core
import Std.Environment as Env
import Std.Testing
import Aws.SigV4
import Aws.SigV4.Env

val NAMES: slice[String] = ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_REGION", "AWS_DEFAULT_REGION", "AWS_ENDPOINT_URL", "AWS_ENDPOINT_URL_SQS"]

func saved(): slice[String] {
  [
    Env.getVarOrDefault(NAMES[0], ""),
    Env.getVarOrDefault(NAMES[1], ""),
    Env.getVarOrDefault(NAMES[2], ""),
    Env.getVarOrDefault(NAMES[3], ""),
    Env.getVarOrDefault(NAMES[4], ""),
    Env.getVarOrDefault(NAMES[5], ""),
    Env.getVarOrDefault(NAMES[6], "")
  ]
}

func restore(values: in slice[String]): Unit {
  var i = 0
  while i < NAMES.length {
    Env.setVar(NAMES[i], values[i])
    i = i + 1
  }
}

func clearAll(): Unit {
  for name in NAMES {
    Env.setVar(name, "")
  }
}

func errOf(r: in Result[Credentials, String]): String {
  match r {
    case Ok(_) -> "ok"
    case Err(e) -> e
  }
}

test "credentialsFromEnv names the first missing variable and never the secret" {
  val before = saved()
  clearAll()
  assertEqual(errOf(credentialsFromEnv()), "AWS_ACCESS_KEY_ID is not set", "nothing set")
  Env.setVar("AWS_ACCESS_KEY_ID", "AKIDEXAMPLE")
  assertEqual(errOf(credentialsFromEnv()), "AWS_SECRET_ACCESS_KEY is not set", "no secret")
  Env.setVar("AWS_SECRET_ACCESS_KEY", "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY")
  match credentialsFromEnv() {
    case Err(e) -> assertTrue(false, "both set: " + e)
    case Ok(c) -> {
      assertEqual(c.accessKeyId, "AKIDEXAMPLE", "access key id")
      assertEqual(c.secretAccessKey, "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY", "secret")
      assertEqual(c.sessionToken, "", "long-term credentials have no token")
    }
  }
  Env.setVar("AWS_SESSION_TOKEN", "tok/en+1=")
  match credentialsFromEnv() {
    case Err(e) -> assertTrue(false, "with a token: " + e)
    case Ok(c) -> assertEqual(c.sessionToken, "tok/en+1=", "the session token")
  }
  restore(before)
}

test "credentialsOf refuses empty values" {
  assertEqual(errOf(credentialsOf("", "s", "")), "the access key id is empty", "no id")
  assertEqual(errOf(credentialsOf("id", "", "")), "the secret access key is empty", "no secret")
  assertEqual(errOf(credentialsOf("id", "s", "")), "ok", "a token is optional")
}

func textOf(r: in Result[String, String]): String {
  match r {
    case Ok(v) -> "ok:" + v
    case Err(e) -> e
  }
}

test "the region is AWS_REGION, else AWS_DEFAULT_REGION" {
  val before = saved()
  clearAll()
  assertEqual(textOf(regionFromEnv()), "neither AWS_REGION nor AWS_DEFAULT_REGION is set", "nothing set")
  Env.setVar("AWS_DEFAULT_REGION", "eu-west-1")
  assertEqual(textOf(regionFromEnv()), "ok:eu-west-1", "the default region")
  Env.setVar("AWS_REGION", "us-east-2")
  assertEqual(textOf(regionFromEnv()), "ok:us-east-2", "AWS_REGION wins")
  restore(before)
}

test "the endpoint override is service-specific first" {
  val before = saved()
  clearAll()
  assertEqual(endpointFromEnv("SQS"), "", "nothing set")
  Env.setVar("AWS_ENDPOINT_URL", "http://127.0.0.1:4566")
  assertEqual(endpointFromEnv("SQS"), "http://127.0.0.1:4566", "the global override")
  Env.setVar("AWS_ENDPOINT_URL_SQS", "http://127.0.0.1:9324")
  assertEqual(endpointFromEnv("SQS"), "http://127.0.0.1:9324", "the service override wins")
  assertEqual(endpointFromEnv("S3"), "http://127.0.0.1:4566", "another service falls back")
  restore(before)
}

test "amzDateNow has the x-amz-date shape" {
  val d = amzDateNow()
  assertEqualInt(d.length, 16, "sixteen characters: " + d)
  assertEqual(d.substring(8, 1), "T", "a T after the day")
  assertEqual(d.substring(15, 1), "Z", "UTC")
}
