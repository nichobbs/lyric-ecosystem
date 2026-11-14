//! tests/readme_tests.l - the credential provider and retry examples of
//! README.md, verbatim, so they cannot drift from what compiles.
@test_module
package Storage.S3.ReadmeTests

import Std.Core
import Std.Testing
import Aws.Credentials.Chain
import Aws.Retry
import Storage.S3

func withDefaultChain(bucket: in String, region: in String): Result[StorageBucket, StorageError] {
  val cfg = awsConfig(bucket, region, "", "").withCredentialProvider(defaultProvider()).withRetry(
    standardRetry().withMaxAttempts(5)
  )
  connect(cfg)
}

func sentOnce(keyId: in String, secret: in String): Result[StorageBucket, StorageError] {
  connect(awsConfig("my-bucket", "eu-west-1", keyId, secret).withoutRetries())
}

test "the README examples compile, and open a bucket without making a request" {
  // `connect` asks the provider for nothing: with the default chain the
  // credentials are resolved by the first operation.
  assertTrue(isOk(withDefaultChain("my-bucket", "eu-west-1")), "a bucket with the default chain")
  assertTrue(isOk(sentOnce("id", "secret")), "a bucket without retries")
  match withDefaultChain("Not A Bucket", "eu-west-1") {
    case Ok(_) -> assertTrue(false, "an invalid bucket name was accepted")
    case Err(e) -> assertEqual(e.code, "INVALID_CONFIG", "the configuration is still validated")
  }
}
