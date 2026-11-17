//! tests/integration/secrets_integration_tests.l - the package against a real
//! Secrets Manager and Parameter Store endpoint (AWS Emulator in CI; AWS works
//! too, with real credentials and no `LYRIC_SECRETS_ENDPOINT`).
//!
//! Needs these variables and is compiled only with `--features emulator`:
//!
//!   LYRIC_SECRETS_ENDPOINT=https://127.0.0.1:9297 LYRIC_SECRETS_REGION=us-east-1 \
//!   AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... \
//!     lyric test --manifest aws-secrets/lyric.toml --target dotnet --features emulator
//!
//! `scripts/ci/start-aws-emulator.sh` starts the emulator and prints them.
//!
//! `LYRIC_SECRETS_CA_FILE` is optional: a PEM file with a certificate authority
//! to trust in addition to the system trust store, for an endpoint whose
//! certificate is private (the emulator's). The tests hand it to the package the
//! way an application would, through `AWS_CA_BUNDLE`, and the endpoint and
//! region through `AWS_ENDPOINT_URL` and `AWS_REGION`, so the whole
//! environment-driven path (default credential chain, endpoint override, CA
//! bundle) is what runs.
//!
//! Each test creates the secrets and parameters it reads under a name of its own
//! (`lyric-it-<run>/...`) and deletes them again.
@cfg(feature = "emulator")
@test_module
package AwsSecrets.IntegrationTests

import Std.Core
import Std.Collections
import Std.Environment as Env
import Std.JsonValue
import Std.Testing
import Std.Uuid as Uuid
import Aws.Credentials
import AwsSecrets
import AwsSecrets.Client
import AwsSecrets.Fetch

func required(name: in String, example: in String): String {
  val v = Env.getVarOrDefault(name, "")
  if v.length == 0 {
    panic(name + " is not set: the integration tests need an endpoint, for example " + name + "=" + example)
  }
  v
}

// What an application sets: the endpoint, the region and the CA bundle.
func configureEnvironment(): Unit {
  Env.setVar("AWS_ENDPOINT_URL", required("LYRIC_SECRETS_ENDPOINT", "https://127.0.0.1:9297"))
  Env.setVar("AWS_REGION", required("LYRIC_SECRETS_REGION", "us-east-1"))
  val ca = Env.getVarOrDefault("LYRIC_SECRETS_CA_FILE", "")
  if ca.length > 0 {
    Env.setVar("AWS_CA_BUNDLE", ca)
  }
  clearCache()
}

func clientOf(service: in Service): AwsClient {
  configureEnvironment()
  match clientFromEnvironment(service, processEnvironment()) {
    case Ok(c) -> c
    case Err(e) -> panic("cannot open a client: " + e.message)
  }
}

val runId: String = Uuid.uuidToString(Uuid.newUuid()).substring(0, 8)

func jstr(v: in String): JsonValue {
  Std.JsonValue.JString(value = v)
}

func obj(fields: in List[JsonField]): String {
  writeValue(Std.JsonValue.JObject(fields = fields))
}

func put(client: in AwsClient, action: in String, fields: in List[JsonField]): Unit {
  match client.call(action, obj(fields)) {
    case Ok(_) -> ()
    case Err(e) -> panic(action + " failed: " + e.message)
  }
}

func createSecret(name: in String, value: in String): Unit {
  put(
    clientOf(secretsManagerService()),
    "CreateSecret",
    [
      JsonField(name = "Name", value = jstr(name)),
      JsonField(name = "SecretString", value = jstr(value)),
      JsonField(name = "ClientRequestToken", value = jstr(Uuid.uuidToString(Uuid.newUuid())))
    ]
  )
}

func deleteSecret(name: in String): Unit {
  val _ = clientOf(secretsManagerService()).call(
    "DeleteSecret",
    obj(
      [
        JsonField(name = "SecretId", value = jstr(name)),
        JsonField(name = "ForceDeleteWithoutRecovery", value = Std.JsonValue.JBool(value = true))
      ]
    )
  )
}

func putParameter(name: in String, value: in String, kind: in String): Unit {
  put(
    clientOf(parameterStoreService()),
    "PutParameter",
    [
      JsonField(name = "Name", value = jstr(name)),
      JsonField(name = "Value", value = jstr(value)),
      JsonField(name = "Type", value = jstr(kind)),
      JsonField(name = "Overwrite", value = Std.JsonValue.JBool(value = true))
    ]
  )
}

func deleteParameter(name: in String): Unit {
  val _ = clientOf(parameterStoreService()).call("DeleteParameter", obj([JsonField(name = "Name", value = jstr(name))]))
}

func text(r: in Result[String, SecretsError]): String {
  match r {
    case Ok(v) -> v
    case Err(e) -> "ERROR " + errorMessage(e)
  }
}

func isNotFound(r: in Result[String, SecretsError]): Bool {
  match r {
    case Err(NotFound(_)) -> true
    case _ -> false
  }
}

test "getSecret and getSecretField read a secret the service holds" {
  configureEnvironment()
  val name = "lyric-it-" + runId + "/db"
  createSecret(name, "{\"user\":\"admin\",\"password\":\"s3cret\"}")
  assertEqual(text(AwsSecrets.getSecret(name)), "{\"user\":\"admin\",\"password\":\"s3cret\"}", "the whole secret")
  assertEqual(text(AwsSecrets.getSecretField(name, "password")), "s3cret", "one JSON field")
  match AwsSecrets.getSecretField(name, "nope") {
    case Err(ParseError(_, _, _)) -> ()
    case _ -> assertTrue(false, "an absent field is ParseError")
  }
  deleteSecret(name)
}

test "getSecret of a secret that does not exist is NotFound" {
  configureEnvironment()
  assertTrue(isNotFound(AwsSecrets.getSecret("lyric-it-" + runId + "/absent")), "NotFound")
}

test "getParameter and getParameterRaw read String and SecureString parameters" {
  configureEnvironment()
  val plain = "/lyric-it-" + runId + "/plain"
  val secure = "/lyric-it-" + runId + "/secure"
  putParameter(plain, "param-value", "String")
  putParameter(secure, "decrypted-value", "SecureString")
  assertEqual(text(AwsSecrets.getParameter(plain)), "param-value", "String parameter")
  assertEqual(text(AwsSecrets.getParameter(secure)), "decrypted-value", "SecureString, decrypted")
  assertTrue(text(AwsSecrets.getParameterRaw(secure)) != "decrypted-value", "SecureString, not decrypted")
  deleteParameter(plain)
  deleteParameter(secure)
}

test "getParameter of a parameter that does not exist is NotFound" {
  configureEnvironment()
  assertTrue(isNotFound(AwsSecrets.getParameter("/lyric-it-" + runId + "/absent")), "NotFound")
}

test "a wrong secret access key is AccessDenied: the endpoint verifies the signature" {
  configureEnvironment()
  val real = Env.getVarOrDefault("AWS_SECRET_ACCESS_KEY", "")
  Env.setVar("AWS_SECRET_ACCESS_KEY", "not-the-secret-key")
  val result = AwsSecrets.getSecret("lyric-it-" + runId + "/any")
  Env.setVar("AWS_SECRET_ACCESS_KEY", real)
  match result {
    case Err(AccessDenied(_, _)) -> ()
    case Err(e) -> assertTrue(false, "expected AccessDenied, got " + errorMessage(e))
    case Ok(_) -> assertTrue(false, "a wrong key must not be served")
  }
}
