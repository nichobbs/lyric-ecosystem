//! AwsSecrets.Fetch - fetching, error classification and the TTL cache behind
//! `AwsSecrets`'s public functions.
//!
//! A fetch is a signed request through `AwsSecrets.Client`: `GetSecretValue`
//! to Secrets Manager, `GetParameter` to Parameter Store. The `...With`
//! functions take the client, so a program (or a test) can point them at any
//! endpoint; the others build one from the process environment
//! (`clientFromEnvironment`). Failures are `SecretsError` values chosen from the
//! service's error code, never from message text.
//!
//! Values are cached in process memory for `ttlSeconds` under a key that
//! cannot collide across differently split names and keys.
@runtime_checked
@io
package AwsSecrets.Fetch

import Std.Core
import Std.Collections
import Std.JsonValue
import Std.Time
import Std.String as Str
import Aws.Credentials
import AwsSecrets
import AwsSecrets.Client

/// A cached value and when it stops being served.
pub record CacheEntry {
  value: String
  expiresAtMs: Long
}

/// The process-wide cache. Public only because a public value holds it.
pub protected type ValueCache {
  var entries: Map[String, CacheEntry] = newMap()
  entry lookup(key: in String, nowMs: in Long): Option[String] {
    if entries.containsKey(key) {
      val e: CacheEntry = entries[key]
      if nowMs < e.expiresAtMs {
        return Some(value = e.value)
      }
      entries.remove(key)
    }
    None
  }
  entry store(key: in String, value: in String, expiresAtMs: in Long) {
    entries[key] = CacheEntry(value = value, expiresAtMs = expiresAtMs)
  }
  entry clear() {
    entries = newMap()
  }
}

val cache: ValueCache = ValueCache()

/// Forget every cached secret and parameter.
pub func clearCache(): Unit {
  cache.clear()
}

/// The service error `code` of a Secrets Manager failure as a `SecretsError`.
/// `name` is the secret. Codes: `ResourceNotFoundException` is `NotFound`; an
/// authorisation failure (`AccessDeniedException`, a bad signature or key) is
/// `AccessDenied`; `DecryptionFailure` or a KMS failure is `DecryptionError`; a
/// request the service rejects (`InvalidRequestException`,
/// `InvalidParameterException`, `INVALID_CONFIG`, `CREDENTIALS_UNAVAILABLE`) is
/// `InvalidConfig`; anything else (throttling, a server fault, a transport
/// failure) is `NetworkError`.
@pure
pub func classifySecretsFault(name: in String, code: in String, message: in String): SecretsError {
  if code == "ResourceNotFoundException" {
    NotFound(name = name)
  } else if isAuthorisationCode(code) {
    AccessDenied(name = name, message = message)
  } else if code == "DecryptionFailure" or Str.startsWith(code, "KMS") {
    DecryptionError(name = name, message = message)
  } else if isRequestCode(code) {
    InvalidConfig(field = name, message = message)
  } else {
    NetworkError(name = name, message = message)
  }
}

/// `classifySecretsFault` for a Parameter Store failure: `ParameterNotFound` and
/// `ParameterVersionNotFound` are `NotFound`, `InvalidKeyId` or an access
/// failure that names `kms:Decrypt` is `DecryptionError`.
@pure
pub func classifyParameterFault(path: in String, code: in String, message: in String): SecretsError {
  if code == "ParameterNotFound" or code == "ParameterVersionNotFound" {
    NotFound(name = path)
  } else if code == "InvalidKeyId" or (isAuthorisationCode(code) and Str.contains(message, "kms:Decrypt")) {
    DecryptionError(name = path, message = message)
  } else if isAuthorisationCode(code) {
    AccessDenied(name = path, message = message)
  } else if isRequestCode(code) {
    InvalidConfig(field = path, message = message)
  } else {
    NetworkError(name = path, message = message)
  }
}

@pure
func isAuthorisationCode(code: in String): Bool {
  code == "AccessDeniedException" or code == "AccessDenied" or code == "UnrecognizedClientException" or
    code == "InvalidSignatureException" or
    code == "ExpiredTokenException" or
    code == "InvalidClientTokenId" or
    code == "IncompleteSignature" or
    code == "MissingAuthenticationTokenException" or
    code == "SignatureDoesNotMatch"
}

@pure
func isRequestCode(code: in String): Bool {
  code == "InvalidRequestException" or code == "InvalidParameterException" or code == "ValidationException" or
    code == "InvalidParameters" or
    code == "INVALID_CONFIG" or
    code == "CREDENTIALS_UNAVAILABLE"
}

/// The value of `key` (or the whole string when `key` is `None`) in
/// `secretString`. A key needs a JSON object whose field is a string.
pub func extractSecretField(
  name: in String, key: in Option[String], secretString: in String
): Result[String, SecretsError] {
  match key {
    case None -> Ok(value = secretString)
    case Some(k) -> match parseValue(secretString) {
      case Err(e) -> Err(error = ParseError(name = name, key = k, message = e.message()))
      case Ok(doc) -> match getField(doc, k) {
        case None -> Err(
          error = ParseError(
            name = name,
            key = k,
            message = "field '" + k +
              "' not present in secret JSON"
          )
        )
        case Some(field) -> match asString(field) {
          case Some(s) -> Ok(value = s)
          case None -> Err(
            error = ParseError(
              name = name,
              key = k,
              message = "field '" + k +
                "' is not a string"
            )
          )
        }
      }
    }
  }
}

/// `extractSecretField` for a response that may hold no `SecretString` (a
/// binary secret has `SecretBinary` instead): `BinaryValueUnsupported`.
pub func extractSecretValue(
  name: in String, key: in Option[String], secretStringOpt: in Option[String]
): Result[String, SecretsError] {
  match secretStringOpt {
    case None -> Err(error = BinaryValueUnsupported(name = name))
    case Some(secretString) -> extractSecretField(name, key, secretString)
  }
}

/// The cache key of a secret: length-prefixed and tagged, so that
/// `getSecret("foo bar")` and `getSecretField("foo", "bar")` never share one.
@pure
pub func secretCacheKeyFor(name: in String, key: in Option[String]): String {
  match key {
    case None -> "N:" + name
    case Some(k) -> "S" + name.length.toString() + ":" + name + ":" + k
  }
}

/// The cache key of a parameter.
@pure
pub func parameterCacheKeyFor(path: in String, decrypt: in Bool): String {
  "P" + path.length.toString() + ":" + path + ":" + decrypt.toString()
}

func clientFor(service: in Service, name: in String): Result[AwsClient, SecretsError] {
  match clientFromEnvironment(service, processEnvironment()) {
    case Ok(c) -> Ok(c)
    case Err(e) -> Err(error = InvalidConfig(field = name, message = e.message))
  }
}

func remember(key: in String, ttlSeconds: in Int, result: in Result[String, SecretsError]): Unit {
  if ttlSeconds > 0 {
    match result {
      case Ok(v) -> cache.store(key, v, nowEpochMillis() + ttlSeconds.toLong() * 1000i64)
      case Err(_) -> ()
    }
  }
}

/// Fetch a secret through `client`, cached for `ttlSeconds`.
pub func fetchSecretWith(
  client: in AwsClient, name: in String, key: in Option[String], ttlSeconds: in Int
): Result[String, SecretsError]
  requires: name.length > 0
  requires: ttlSeconds >= 0
{
  val cacheKey = secretCacheKeyFor(name, key)
  if ttlSeconds > 0 {
    match cache.lookup(cacheKey, nowEpochMillis()) {
      case Some(v) -> return Ok(value = v)
      case None -> ()
    }
  }
  val body = writeValue(Std.JsonValue.JObject(fields = [JsonField(name = "SecretId", value = JString(value = name))]))
  val result: Result[String, SecretsError] = match client.call("GetSecretValue", body) {
    case Err(e) -> Err(error = classifySecretsFault(name, e.code, e.message))
    case Ok(doc) -> extractSecretValue(
      name,
      key,
      getString(doc, "SecretString")
    )
  }
  remember(cacheKey, ttlSeconds, result)
  result
}

/// Fetch a parameter through `client`, cached for `ttlSeconds`.
pub func fetchParameterWith(
  client: in AwsClient, path: in String, decrypt: in Bool, ttlSeconds: in Int
): Result[String, SecretsError]
  requires: path.length > 0
  requires: ttlSeconds >= 0
{
  val cacheKey = parameterCacheKeyFor(path, decrypt)
  if ttlSeconds > 0 {
    match cache.lookup(cacheKey, nowEpochMillis()) {
      case Some(v) -> return Ok(value = v)
      case None -> ()
    }
  }
  val body = writeValue(
    Std.JsonValue.JObject(
      fields = [
        JsonField(name = "Name", value = JString(value = path)),
        JsonField(name = "WithDecryption", value = JBool(value = decrypt))
      ]
    )
  )
  val result: Result[String, SecretsError] = match client.call("GetParameter", body) {
    case Err(e) -> Err(
      error = classifyParameterFault(path, e.code, e.message)
    )
    case Ok(doc) -> match getField(doc, "Parameter") {
      case None -> Err(
        error = ParseError(
          name = path,
          key = "Parameter",
          message = "the response has no Parameter"
        )
      )
      case Some(p) -> match getString(p, "Value") {
        case Some(v) -> Ok(value = v)
        case None -> Err(
          error = ParseError(
            name = path,
            key = "Value",
            message = "the parameter has no Value"
          )
        )
      }
    }
  }
  remember(cacheKey, ttlSeconds, result)
  result
}

/// Fetch a secret with the client the environment configures.
pub func fetchSecret(name: in String, key: in Option[String], ttlSeconds: in Int): Result[String, SecretsError]
  requires: name.length > 0
  requires: ttlSeconds >= 0
{
  if ttlSeconds > 0 {
    match cache.lookup(secretCacheKeyFor(name, key), nowEpochMillis()) {
      case Some(v) -> return Ok(value = v)
      case None -> ()
    }
  }
  match clientFor(secretsManagerService(), name) {
    case Err(e) -> Err(error = e)
    case Ok(client) -> fetchSecretWith(client, name, key, ttlSeconds)
  }
}

/// Fetch a parameter with the client the environment configures.
pub func fetchParameter(path: in String, decrypt: in Bool, ttlSeconds: in Int): Result[String, SecretsError]
  requires: path.length > 0
  requires: ttlSeconds >= 0
{
  if ttlSeconds > 0 {
    match cache.lookup(parameterCacheKeyFor(path, decrypt), nowEpochMillis()) {
      case Some(v) -> return Ok(value = v)
      case None -> ()
    }
  }
  match clientFor(parameterStoreService(), path) {
    case Err(e) -> Err(error = e)
    case Ok(client) -> fetchParameterWith(client, path, decrypt, ttlSeconds)
  }
}

/// Fetch every `@secretsManager` / `@parameterStore` config-block field and
/// register its value with Std.ConfigOverride (D230); see
/// `AwsSecrets.registerAnnotated`.
@cfg(not(target = "native"))
pub func initFromAnnotations(ttlSeconds: in Int): Result[Unit, SecretsError]
  requires: ttlSeconds >= 0
{
  AwsSecrets.registerAnnotated(ttlSeconds)
}

/// The native target embeds no annotation metadata (lyric-lang#8524), so the
/// annotated fields cannot be found: an error, not a silent no-op.
@cfg(target = "native")
pub func initFromAnnotations(ttlSeconds: in Int): Result[Unit, SecretsError]
  requires: ttlSeconds >= 0
{
  Err(
    error = InvalidConfig(
      field = "Lyric.AnnotationMeta",
      message = "annotation metadata is not available on native (lyric-lang#8524); use getSecret and getParameter"
    )
  )
}
