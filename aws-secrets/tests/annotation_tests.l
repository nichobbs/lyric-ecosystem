//! tests/annotation_tests.l - the annotation path: `registerSecrets` driven
//! through fetch functions of the test's own, and `init()` skipping fields whose
//! environment variable is set. It needs the annotation metadata the compiler
//! embeds (Std.AnnotationMeta), which the native target does not have
//! (lyric-lang#8524), so the module is absent there.
@cfg(not(target = "native"))
@test_module
package AwsSecrets.AnnotationTests

import Std.Core
import Std.Collections
import Std.Testing
import Std.Environment as Env
import Std.AnnotationMeta as Meta
import Std.ConfigOverride as Override
import AwsSecrets

// ─── Annotation-driven registration (D230, #7181) ──────────────────────────
// Each fixture config block below is read by at most one test: a block is
// initialised once, on first access, so a block shared between tests would
// carry whatever the first reader registered.  Blocks that hold a required
// field are never read unless a value was registered first.
config Db {
  host: String = "localhost"
  @sensitive
  @secretsManager("svc/prod", key = "dbPassword")
  password: String
}

config Keys {
  @sensitive
  @parameterStore("/svc/signing-key")
  signingKey: String
}

config EnvWins {
  @sensitive
  @secretsManager("svc/env-wins")
  token: String
}

config Partial1 {
  @secretsManager("svc/partial-one")
  one: String
}

config Partial2 {
  @secretsManager("svc/partial-two")
  two: String
}

config NoName {
  @secretsManager(key = "k")
  value: String
}

config EmptyValue {
  @secretsManager("svc/empty")
  value: String
}

func fieldsOf(configName: in String): List[Meta.AnnotatedField] {
  val acc: List[Meta.AnnotatedField] = newList()
  match Meta.allAnnotatedFields() {
    case Err(e) -> assertTrue(false, "annotation metadata unavailable: " + Meta.errorMessage(e))
    case Ok(all) -> {
      var i = 0
      while i < all.count {
        if all[i].configName == configName and (all[i].annotationName == "secretsManager" or all[i].annotationName == "parameterStore") {
          acc.add(all[i])
        }
        i = i + 1
      }
    }
  }
  acc
}

func fieldsOf2(a: in String, b: in String): List[Meta.AnnotatedField] {
  val acc = fieldsOf(a)
  val more = fieldsOf(b)
  var i = 0
  while i < more.count {
    acc.add(more[i])
    i = i + 1
  }
  acc
}

func assertOverride(key: in String, expected: in Option[String], label: in String): Unit {
  match Override.getConfigOverride(key) {
    case Some(v) -> match expected {
      case Some(e) -> assertEqual(v, e, label)
      case None -> assertTrue(false, label + " (expected none, got " + v + ")")
    }
    case None -> match expected {
      case Some(e) -> assertTrue(false, label + " (expected " + e + ", got none)")
      case None -> ()
    }
  }
}

func echoSecret(name: in String, key: in Option[String]): Result[String, SecretsError] {
  val k = match key { case Some(x) -> x; case None -> "-" }
  Ok(value = name + "|" + k)
}

func echoParameter(path: in String): Result[String, SecretsError] {
  Ok(value = "param:" + path)
}

func neverSecret(name: in String, key: in Option[String]): Result[String, SecretsError] {
  Err(error = NotFound(name = "unexpected secret fetch: " + name))
}

func neverParameter(path: in String): Result[String, SecretsError] {
  Err(error = NotFound(name = "unexpected parameter fetch: " + path))
}

test "registerSecrets: fetched values reach the config fields on first read" {
  val fields = fieldsOf2("Db", "Keys")
  assertEqualInt(fields.count, 2, "two annotated fixture fields")
  match AwsSecrets.registerSecrets(fields, echoSecret, echoParameter) {
    case Ok(_) -> ()
    case Err(e) -> assertTrue(false, "registerSecrets failed: " + AwsSecrets.errorMessage(e))
  }
  assertEqual(Db.password, "svc/prod|dbPassword", "secret with a JSON key")
  assertEqual(Db.host, "localhost", "an unannotated field keeps its default")
  assertEqual(Keys.signingKey, "param:/svc/signing-key", "parameter")
}

test "registerSecrets: an env var wins and the fetch is skipped" {
  val fields = fieldsOf("EnvWins")
  assertEqualInt(fields.count, 1, "one annotated fixture field")
  Env.setVar(fields[0].envVar, "from-env")
  match AwsSecrets.registerSecrets(fields, neverSecret, neverParameter) {
    case Ok(_) -> ()
    case Err(e) -> assertTrue(false, "the fetch must not run when the env var is set: " + AwsSecrets.errorMessage(e))
  }
  assertEqual(EnvWins.token, "from-env", "env var value")
}

test "registerSecrets: a fetch error is returned and earlier values stay registered" {
  val fields = fieldsOf2("Partial1", "Partial2")
  assertEqualInt(fields.count, 2, "two annotated fixture fields")
  match AwsSecrets.registerSecrets(fields, { n: String, k: Option[String] -> if n == "svc/partial-two" { Err(error = AccessDenied(name = n, message = "denied")) } else { echoSecret(n, k) } }, echoParameter) {
    case Err(AccessDenied(n, _)) -> assertEqual(n, "svc/partial-two", "the failing secret is named")
    case Err(e) -> assertTrue(false, "wrong error: " + AwsSecrets.errorMessage(e))
    case Ok(_) -> assertTrue(false, "the second fetch fails, so the call must fail")
  }
  assertOverride(fields[0].envVar, Some(value = "svc/partial-one|-"), "first value registered")
  assertOverride(fields[1].envVar, None, "failed value not registered")
}

test "registerSecrets: an annotation without its name or path is InvalidConfig" {
  match AwsSecrets.registerSecrets(fieldsOf("NoName"), neverSecret, neverParameter) {
    case Err(InvalidConfig(field, _)) -> assertEqual(field, "NoName.value", "the field is named")
    case _ -> assertTrue(false, "expected InvalidConfig")
  }
}

test "registerSecrets: two secret annotations on one field are InvalidConfig" {
  val json = "[{\"package\":\"P\",\"config\":\"Both\",\"field\":\"value\",\"annotation\":\"secretsManager\",\"envVar\":\"LYRIC_CONFIG_P_BOTH_VALUE\",\"args\":[{\"name\":\"\",\"kind\":\"string\",\"value\":\"svc/both\"}]}," +
    "{\"package\":\"P\",\"config\":\"Both\",\"field\":\"value\",\"annotation\":\"parameterStore\",\"envVar\":\"LYRIC_CONFIG_P_BOTH_VALUE\",\"args\":[{\"name\":\"\",\"kind\":\"string\",\"value\":\"/svc/both\"}]}]"
  val fields = match Meta.parseAnnotationMeta(json) {
    case Ok(fs) -> fs
    case Err(e) -> {
      assertTrue(false, "fixture JSON must decode: " + Meta.errorMessage(e))
      newList()
    }
  }
  assertEqualInt(fields.count, 2, "the field is listed once per annotation")
  match AwsSecrets.registerSecrets(fields, echoSecret, echoParameter) {
    case Err(InvalidConfig(field, _)) -> assertEqual(field, "Both.value", "the field is named")
    case _ -> assertTrue(false, "expected InvalidConfig")
  }
}

test "registerSecrets: an empty fetched value is ParseError, not a silent clear" {
  match AwsSecrets.registerSecrets(fieldsOf("EmptyValue"), { n: String, k: Option[String] -> Ok(value = "") }, echoParameter) {
    case Err(ParseError(name, _, _)) -> assertEqual(name, "EmptyValue.value", "the field is named")
    case _ -> assertTrue(false, "expected ParseError")
  }
}

// Runs last on purpose: it sets the env var of every annotated fixture so
// `init()` takes its skip path (no AWS call, no credentials needed) on all
// three features, which would otherwise change what the tests above see.
test "init(): with every annotated field already set, succeeds without an AWS call" {
  match Meta.allAnnotatedFields() {
    case Err(e) -> assertTrue(false, "annotation metadata unavailable: " + Meta.errorMessage(e))
    case Ok(all) -> {
      var i = 0
      while i < all.count {
        Env.setVar(all[i].envVar, "preset")
        i = i + 1
      }
    }
  }
  match AwsSecrets.init() {
    case Ok(_) -> ()
    case Err(e) -> assertTrue(false, "init() must succeed when no fetch is needed: " + AwsSecrets.errorMessage(e))
  }
}
