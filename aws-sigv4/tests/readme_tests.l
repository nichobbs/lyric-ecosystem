//! tests/readme_tests.l - the usage examples of README.md, verbatim, so the
//! README cannot drift from what compiles.
@test_module
package Aws.SigV4.ReadmeTests

import Std.Core
import Std.Collections
import Std.Encoding
import Std.Testing
import Aws.SigV4
import Aws.SigV4.Env

func signedSendMessage(body: in String): Result[SignedRequest, String] {
  val creds = credentialsFromEnv()?
  val region = regionFromEnv()?
  val extra: List[Pair] = newList()
  extra.add(Pair(name = "content-type", value = "application/x-amz-json-1.0"))
  extra.add(Pair(name = "x-amz-target", value = "AmazonSQS.SendMessage"))
  val noQuery: List[Pair] = newList()
  Ok(
    signedHeaders(
      "POST",
      canonicalUri("/", false),
      noQuery,
      "sqs." + region + ".amazonaws.com",
      extra,
      sha256Hex(encodeUtf8(body)),
      false,
      creds,
      region,
      "sqs",
      amzDateNow()
    )
  )
}

func presignedGet(creds: in Credentials, bucket: in String, key: in String): String {
  val host = bucket + ".s3.eu-west-1.amazonaws.com"
  val path = canonicalUri("/" + key, true)
  val noQuery: List[Pair] = newList()
  "https://" + host + path + "?" + presignQuery("GET", host, path, noQuery, creds, "eu-west-1", "s3", amzDateNow(), 900)
}

test "the README examples compile" {
  val creds = Credentials(accessKeyId = "AKIDEXAMPLE", secretAccessKey = "secret", sessionToken = "")
  assertTrue(presignedGet(creds, "b", "a b.txt").contains("/a%20b.txt?X-Amz-Algorithm=AWS4-HMAC-SHA256"), "presign")
  match signedSendMessage("{}") {
    case Ok(r) -> assertEqual(r.headers[r.headers.count - 1].name, "Authorization", "signed")
    case Err(e) -> assertTrue(e.length > 0, "no credentials in the environment")
  }
}
