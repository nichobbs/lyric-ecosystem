//! tests/readme_tests.l - the usage examples of README.md, verbatim, so the
//! README cannot drift from what compiles.
@test_module
package Aws.Credentials.ReadmeTests

import Std.Core
import Std.Collections
import Std.Testing
import Aws.SigV4
import Aws.Credentials
import Aws.Credentials.Chain
import Aws.Credentials.Profile
import Aws.Credentials.Remote
import Aws.Retry

// The credentials an AWS client signs with: the default chain, cached.
func signingCredentials(provider: in CredentialProvider): Result[Credentials, String] {
  match provider.resolve() {
    case Ok(resolved) -> Ok(resolved.credentials)
    case Err(e) -> Err(e.code + ": " + e.text())
  }
}

func example(): Result[Credentials, String] {
  val provider = defaultProvider()
  signingCredentials(provider)
}

// A chain of your own: a named profile first, then the instance role.
func profileThenInstance(profile: in String): CredentialProvider {
  val files = sharedFilesFromEnvironment(processEnvironment()).copy(profile = profile, profileIsExplicit = true)
  val providers: List[CredentialProvider] = newList()
  providers.add(profileProviderOf(files))
  providers.add(imdsProvider(processEnvironment()))
  cached(chainOf(providers))
}

// One request, sent again as the standard policy says.
func withRetries(send: in () -> Int, idempotent: in Bool): Int {
  val policy = standardRetry()
  var attempt = 1
  while true {
    val status = send()
    match policy.decide(attempt, classifyResponse(status, ""), idempotent, -1) {
      case GiveUp -> return status
      case RetryAfter(delayMs) -> policy.pause(delayMs)
    }
    attempt = attempt + 1
  }
  0
}

ref record Counter {
  var calls: Int
}

test "the README examples compile, and behave as the README says" {
  val values: Map[String, String] = newMap()
  values["AWS_ACCESS_KEY_ID"] = "AKIAEXAMPLE"
  values["AWS_SECRET_ACCESS_KEY"] = "fixture-secret-not-a-real-one"
  match signingCredentials(cached(defaultChain(environmentOf(values)))) {
    case Ok(creds) -> assertEqual(creds.accessKeyId, "AKIAEXAMPLE", "the environment is first in the chain")
    case Err(message) -> assertTrue(false, message)
  }
  val counter = Counter(calls = 0)
  val status = withRetries({ -> counter.calls = counter.calls + 1; if counter.calls < 3 { 503 } else { 200 } }, false)
  assertEqualInt(status, 200, "throttled twice, then served")
  assertEqualInt(counter.calls, 3, "three attempts")
}
