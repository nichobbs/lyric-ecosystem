//! tests/readme_tests.l - the README's usage example, compiled (not on native, which
//! checks the required fields of every config block at start-up). Nothing is
//! fetched: the functions are only referenced.
@cfg(not(target = "native"))
@test_module
package AwsSecrets.ReadmeTests

import Std.Core
import Std.Testing
import AwsSecrets

config Database {
  host: String = "localhost"
  @sensitive
  @secretsManager("my-service/prod", key = "dbPassword")
  password: String
}

func connectionPassword(): Result[String, AwsSecrets.SecretsError] {
  AwsSecrets.init()?
  Ok(Database.password)
}

func oneOff(): Result[String, AwsSecrets.SecretsError] {
  AwsSecrets.getParameter("/my-service/signing-key")
}

test "the README example compiles and its functions are callable values" {
  val a: () -> Result[String, AwsSecrets.SecretsError] = connectionPassword
  val b: () -> Result[String, AwsSecrets.SecretsError] = oneOff
  assertTrue(true, "both examples type-check")
}
