//! tests/secrets_tests.l - the pure surface of AwsSecrets: the error union and
//! its messages, the cache TTL check, the error classification by service
//! error code, JSON-field extraction and cache keys. The annotation path
//! (`registerSecrets`, `init()`) is in `annotation_tests.l`, the fetches over
//! real HTTP in `fake_aws_tests.l`.
@test_module
package AwsSecrets.SecretsTests

import Std.Core
import Std.Collections
import Std.Testing
import AwsSecrets
import AwsSecrets.Fetch as SecretsKernel

// ─── SecretsError union ───────────────────────────────────────────────────────
test "SecretsError.NotFound: carries secret name" {
  val e = NotFound(name = "my-service/prod")
  match e {
    case NotFound(n) -> assertEqual(n, "my-service/prod", "secret name")
    case _ -> assertTrue(false, "expected NotFound")
  }
}

test "SecretsError.AccessDenied: carries name and message" {
  val e = AccessDenied(name = "my-secret", message = "not authorized")
  match e {
    case AccessDenied(n, m) -> {
      assertEqual(n, "my-secret", "name field")
      assertEqual(m, "not authorized", "message field")
    }
    case _ -> assertTrue(false, "expected AccessDenied")
  }
}

test "SecretsError.DecryptionError: carries name and message" {
  val e = DecryptionError(name = "/my/param", message = "KMS key not found")
  match e {
    case DecryptionError(n, m) -> {
      assertEqual(n, "/my/param", "name field")
      assertEqual(m, "KMS key not found", "message field")
    }
    case _ -> assertTrue(false, "expected DecryptionError")
  }
}

test "SecretsError.ParseError: carries name, key, and message" {
  val e = ParseError(name = "my-json-secret", key = "dbPassword", message = "key absent")
  match e {
    case ParseError(n, k, m) -> {
      assertEqual(n, "my-json-secret", "name field")
      assertEqual(k, "dbPassword", "key field")
      assertEqual(m, "key absent", "message field")
    }
    case _ -> assertTrue(false, "expected ParseError")
  }
}

test "SecretsError.BinaryValueUnsupported: carries secret name" {
  val e = BinaryValueUnsupported(name = "my-binary-secret")
  match e {
    case BinaryValueUnsupported(n) -> assertEqual(n, "my-binary-secret", "secret name")
    case _ -> assertTrue(false, "expected BinaryValueUnsupported")
  }
}

// ─── errorMessage ─────────────────────────────────────────────────────────────
test "errorMessage: NotFound includes secret name" {
  val e = NotFound(name = "app/prod/jwt-secret")
  val msg = AwsSecrets.errorMessage(e)
  assertTrue(msg.contains("app/prod/jwt-secret"), "error message must include the secret name")
  assertTrue(msg.length > 0, "error message must be non-empty")
}

test "errorMessage: AccessDenied includes name and inner message" {
  val e = AccessDenied(name = "my-secret", message = "iam:GetSecretValue denied")
  val msg = AwsSecrets.errorMessage(e)
  assertTrue(msg.contains("my-secret"), "message includes secret name")
  assertTrue(msg.contains("iam:GetSecretValue denied"), "message includes inner error")
}

test "errorMessage: DecryptionError includes name" {
  val e = DecryptionError(name = "/secure/key", message = "KMS unavailable")
  val msg = AwsSecrets.errorMessage(e)
  assertTrue(msg.contains("/secure/key"), "message includes parameter name")
}

test "errorMessage: ParseError includes both name and key" {
  val e = ParseError(name = "structured-secret", key = "missingField", message = "key absent")
  val msg = AwsSecrets.errorMessage(e)
  assertTrue(msg.contains("structured-secret"), "message includes secret name")
  assertTrue(msg.contains("missingField"), "message includes missing key")
}

test "errorMessage: BinaryValueUnsupported includes secret name" {
  val e = BinaryValueUnsupported(name = "my-binary-secret")
  val msg = AwsSecrets.errorMessage(e)
  assertTrue(msg.contains("my-binary-secret"), "message includes secret name")
  assertTrue(msg.length > 0, "error message must be non-empty")
}

func netExtractSecretValueScenario(): String {
  val noneKeyBinary = SecretsKernel.extractSecretValue("my-binary-secret", None, None)
  match noneKeyBinary {
    case Ok(v) -> return "expected BinaryValueUnsupported for a binary secret, got Ok: " + v
    case Err(BinaryValueUnsupported(n)) -> if n != "my-binary-secret" {
      return "BinaryValueUnsupported carried the wrong name: " + n
    } else {
      ()
    }
    case Err(e) -> return "expected BinaryValueUnsupported, got a different error: " + AwsSecrets.errorMessage(e)
  }

  val someKeyBinary = SecretsKernel.extractSecretValue("my-binary-secret", Some("field"), None)
  match someKeyBinary {
    case Ok(v) -> return "expected BinaryValueUnsupported for a binary secret with a key, got Ok: " + v
    case Err(BinaryValueUnsupported(_)) -> ()
    case Err(e) -> return "expected BinaryValueUnsupported, got a different error: " + AwsSecrets.errorMessage(e)
  }

  val stringSecret = SecretsKernel.extractSecretValue("my-secret", None, Some("plain-value"))
  match stringSecret {
    case Ok(v) -> if v != "plain-value" { "expected the raw secret string, got: " + v } else { "OK" }
    case Err(e) -> "expected Ok for a string secret, got Err: " + AwsSecrets.errorMessage(e)
  }
}

test "extractSecretValue converts a binary (null SecretString) secret into BinaryValueUnsupported, never Ok(null)" {
  assertEqual(
    netExtractSecretValueScenario(),
    "OK",
    "extractSecretValue must convert a null .NET SecretString into a typed BinaryValueUnsupported"
  )
}

// ─── secretCacheKeyFor / parameterCacheKeyFor (aws kernel) ────────────────────
// Pure cache-key-construction logic, independent of any network/client
// call
// #7046 regression above.
func netCacheKeyCollisionScenario(): String {
  val noneKey = SecretsKernel.secretCacheKeyFor("foo bar", None)
  val someKey = SecretsKernel.secretCacheKeyFor("foo", Some("bar"))
  if noneKey == someKey {
    return "getSecret(\"foo bar\") and getSecretField(\"foo\", \"bar\") must not share a cache key, both produced: " +
      noneKey
  }

  val split1 = SecretsKernel.secretCacheKeyFor("ab", Some("cd"))
  val split2 = SecretsKernel.secretCacheKeyFor("abc", Some("d"))
  if split1 == split2 {
    return "differently-split (name, key) pairs must not share a cache key, both produced: " + split1
  }

  if SecretsKernel.secretCacheKeyFor("my-secret", Some("field")) != SecretsKernel.secretCacheKeyFor("my-secret", Some("field")) {
    return "identical (name, key) pairs must produce identical cache keys"
  }
  if SecretsKernel.secretCacheKeyFor("my-secret", None) != SecretsKernel.secretCacheKeyFor("my-secret", None) {
    return "identical (name, None) pairs must produce identical cache keys"
  }

  val plainSecret = SecretsKernel.secretCacheKeyFor("my-secret", None)
  val fieldOfSameName = SecretsKernel.secretCacheKeyFor("my-secret", Some(""))
  if plainSecret == fieldOfSameName {
    return "a None-keyed and a Some-keyed cache entry must never collide, both produced: " + plainSecret
  }

  val paramKey1 = SecretsKernel.parameterCacheKeyFor("my-param", true)
  val paramKey2 = SecretsKernel.parameterCacheKeyFor("my-param", false)
  if paramKey1 == paramKey2 {
    return "different decrypt flags must not share a parameter cache key, both produced: " + paramKey1
  }
  if SecretsKernel.parameterCacheKeyFor("/app/db/password", true) != SecretsKernel.parameterCacheKeyFor("/app/db/password", true) {
    return "identical (path, decrypt) pairs must produce identical parameter cache keys"
  }

  "OK"
}

test "secretCacheKeyFor/parameterCacheKeyFor never collide across differently-split inputs" {
  assertEqual(
    netCacheKeyCollisionScenario(),
    "OK",
    "a wrong-secret-returned cache-key collision must be impossible regardless of what characters name/key/path contain"
  )
}

// ─── extractSecretField (jvm kernel) ───────────────────────────────────────
// Pure JSON-field-extraction logic, independent of any network/client call
// — exercises the panic-vs-Err fix for a non-string field value without
// needing a live or mocked AWS endpoint.
test "checkCacheTtl: accepts 0, the default and the one-day maximum" {
  assertTrue(isValidCacheTtl(0), "0 disables caching")
  assertTrue(isValidCacheTtl(300), "default TTL")
  assertTrue(isValidCacheTtl(maxCacheTtlSeconds()), "maximum TTL")
  match checkCacheTtl(300) {
    case Ok(t) -> assertTrue(t == 300, "Ok carries the TTL")
    case Err(e) -> assertTrue(false, "300 must be accepted: " + errorMessage(e))
  }
}

test "checkCacheTtl: rejects negative and over-long TTLs with InvalidConfig" {
  assertTrue(not isValidCacheTtl(-1), "negative TTL")
  assertTrue(not isValidCacheTtl(maxCacheTtlSeconds() + 1), "TTL past one day")
  match checkCacheTtl(-5) {
    case Err(InvalidConfig(field, _)) -> assertEqual(field, "SecretCache.ttlSeconds", "names the field")
    case _ -> assertTrue(false, "negative TTL must be InvalidConfig")
  }
  match checkCacheTtl(1000000) {
    case Err(InvalidConfig(_, msg)) -> assertTrue(msg.contains("1000000"), "message carries the value")
    case _ -> assertTrue(false, "huge TTL must be InvalidConfig")
  }
}

test "SecretsError.InvalidConfig: errorMessage names the field" {
  assertTrue(errorMessage(InvalidConfig(field = "f", message = "m")).contains("f"), "InvalidConfig message")
}

// ─── Classification by service error code ────────────────────────────────────
func classifySecretsScenario(): String {
  match SecretsKernel.classifySecretsFault("s", "ResourceNotFoundException", "m") {
    case NotFound(n) -> if n != "s" { return "NotFound carried " + n } else { () }
    case _ -> return "ResourceNotFoundException must be NotFound"
  }
  match SecretsKernel.classifySecretsFault("s", "AccessDeniedException", "m") {
    case AccessDenied(n, m) -> if n != "s" or m != "m" { return "AccessDenied carried the wrong fields" } else { () }
    case _ -> return "AccessDeniedException must be AccessDenied"
  }
  match SecretsKernel.classifySecretsFault("s", "UnrecognizedClientException", "m") {
    case AccessDenied(_, _) -> ()
    case _ -> return "UnrecognizedClientException must be AccessDenied"
  }
  match SecretsKernel.classifySecretsFault("s", "DecryptionFailure", "m") {
    case DecryptionError(_, _) -> ()
    case _ -> return "DecryptionFailure must be DecryptionError"
  }
  match SecretsKernel.classifySecretsFault("s", "InvalidRequestException", "marked for deletion") {
    case InvalidConfig(f, m) -> if f != "s" or m != "marked for deletion" {
      return "InvalidConfig carried the wrong fields"
    } else {
      ()
    }
    case _ -> return "InvalidRequestException must be InvalidConfig"
  }
  match SecretsKernel.classifySecretsFault("s", "ThrottlingException", "slow down") {
    case NetworkError(n, m) -> if n == "s" and m == "slow down" {
      "OK"
    } else {
      "NetworkError carried the wrong fields"
    }
    case _ -> "ThrottlingException must be NetworkError"
  }
}

test "classifySecretsFault maps Secrets Manager error codes to SecretsError" {
  assertEqual(classifySecretsScenario(), "OK", "Secrets Manager classification")
}

func classifyParameterScenario(): String {
  match SecretsKernel.classifyParameterFault("/p", "ParameterNotFound", "m") {
    case NotFound(n) -> if n != "/p" { return "NotFound carried " + n } else { () }
    case _ -> return "ParameterNotFound must be NotFound"
  }
  match SecretsKernel.classifyParameterFault("/p", "AccessDeniedException", "not authorized to perform: ssm:GetParameter") {
    case AccessDenied(_, _) -> ()
    case _ -> return "AccessDeniedException must be AccessDenied"
  }
  match SecretsKernel.classifyParameterFault("/p", "AccessDeniedException", "no kms:Decrypt on the key") {
    case DecryptionError(_, _) -> ()
    case _ -> return "an access failure naming kms:Decrypt must be DecryptionError"
  }
  match SecretsKernel.classifyParameterFault("/p", "InvalidKeyId", "m") {
    case DecryptionError(_, _) -> ()
    case _ -> return "InvalidKeyId must be DecryptionError"
  }
  match SecretsKernel.classifyParameterFault("/p", "InternalServerError", "boom") {
    case NetworkError(_, _) -> "OK"
    case _ -> "InternalServerError must be NetworkError"
  }
}

test "classifyParameterFault maps Parameter Store error codes to SecretsError" {
  assertEqual(classifyParameterScenario(), "OK", "Parameter Store classification")
}

func extractFieldScenario(): String {
  match SecretsKernel.extractSecretField("s", Some("k"), "{\"k\":\"v\",\"n\":3}") {
    case Ok(v) -> if v != "v" { return "expected v, got " + v } else { () }
    case Err(e) -> return "unexpected " + AwsSecrets.errorMessage(e)
  }
  match SecretsKernel.extractSecretField("s", Some("n"), "{\"k\":\"v\",\"n\":3}") {
    case Err(ParseError(_, _, _)) -> ()
    case _ -> return "a non-string field must be ParseError"
  }
  match SecretsKernel.extractSecretField("s", Some("k"), "not json") {
    case Err(ParseError(_, _, _)) -> "OK"
    case _ -> "a secret that is not JSON must be ParseError"
  }
}

test "extractSecretField reads string fields of a JSON secret" {
  assertEqual(extractFieldScenario(), "OK", "field extraction")
}

// On native there is no annotation metadata (lyric-lang#8524): init() says so
// instead of succeeding without having fetched anything.
@cfg(target = "native")
func initOnNativeScenario(): String {
  match AwsSecrets.init() {
    case Err(InvalidConfig(field, _)) -> if field == "Lyric.AnnotationMeta" { "OK" } else { "wrong field " + field }
    case _ -> "init() must be InvalidConfig on native"
  }
}

@cfg(not(target = "native"))
func initOnNativeScenario(): String {
  "OK"
}

test "init(): on native it reports that annotation metadata is unavailable" {
  assertEqual(initOnNativeScenario(), "OK", "init() on native")
}
