//! tests/sigv4_tests.l - what `Storage.S3.SigV4` adds to `Aws.SigV4`, and that
//! the shared signer is still reachable through it.
//!
//! The service-neutral tests (canonical request, string to sign, signing key,
//! the published examples of S3 and of other services) are in the
//! `Lyric.Aws.SigV4` package (`aws-sigv4/tests/sigv4_tests.l`). Here: the
//! canonical URI of an object key, and two examples of the Amazon S3 API
//! reference signed end to end through this package's names ("Authenticating
//! Requests: Using the Authorization Header", PUT Object; "Authenticating
//! Requests: Using Query Parameters", the presigned URL), with the
//! documentation credentials, bucket `examplebucket`, region `us-east-1` and
//! the instant `20130524T000000Z`.
@test_module
package Storage.S3.SigV4Tests

import Std.Core
import Std.Collections
import Std.Encoding
import Std.Testing
import Storage.S3.SigV4

val AMZ_DATE: String = "20130524T000000Z"

val HOST: String = "examplebucket.s3.amazonaws.com"

func docCreds(): Credentials {
  Credentials(
    accessKeyId = "AKIAIOSFODNN7EXAMPLE",
    secretAccessKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
    sessionToken = ""
  )
}

func noPairs(): List[Pair] {
  newList()
}

test "an object key is encoded once, with '/' kept and nothing normalised" {
  assertEqual(canonicalKeyPath("test$file.text"), "/test%24file.text", "the key is percent-encoded")
  assertEqual(
    canonicalKeyPath("photos/2026/my photo (1).jpg"),
    "/photos/2026/my%20photo%20%281%29.jpg",
    "an object key"
  )
  assertEqual(canonicalKeyPath("100%/a"), "/100%25/a", "a literal percent is encoded once")
  assertEqual(
    canonicalKeyPath("a//b/./c/../d"),
    "/a//b/./c/../d",
    "dot segments and empty segments are part of the key"
  )
  assertEqual(canonicalKeyPath("/leading"), "//leading", "a key may begin with '/'")
  assertEqual(canonicalKeyPath("café/日本"), "/caf%C3%A9/%E6%97%A5%E6%9C%AC", "non-ASCII is UTF-8 bytes")
  assertEqual(canonicalKeyPath(""), "/", "the empty key is the bucket root")
}

test "AWS example: PUT Object with a storage class and an encoded key" {
  val payloadHash = sha256Hex(encodeUtf8("Welcome to Amazon S3."))
  val headers: List[Pair] = newList()
  headers.add(Pair(name = "Date", value = "Fri, 24 May 2013 00:00:00 GMT"))
  headers.add(Pair(name = "Host", value = HOST))
  headers.add(Pair(name = "x-amz-date", value = AMZ_DATE))
  headers.add(Pair(name = "x-amz-storage-class", value = "REDUCED_REDUNDANCY"))
  headers.add(Pair(name = "x-amz-content-sha256", value = payloadHash))
  val signed = signRequest(
    "PUT",
    canonicalKeyPath("test$file.text"),
    noPairs(),
    headers,
    payloadHash,
    docCreds(),
    "us-east-1",
    "s3",
    AMZ_DATE
  )
  assertEqual(signed.signature, "98ad721746da40c64f1a55b78f14c238d841ea1380cd77a1b5971af0ece108bd", "signature")
}

test "AWS example: presigned GET URL valid for 24 hours" {
  val query = presignQuery(
    "GET",
    HOST,
    canonicalKeyPath("test.txt"),
    noPairs(),
    docCreds(),
    "us-east-1",
    "s3",
    AMZ_DATE,
    86400
  )
  assertEqual(
    "https://" + HOST + "/test.txt?" + query,
    "https://examplebucket.s3.amazonaws.com/test.txt?X-Amz-Algorithm=AWS4-HMAC-SHA256" +
      "&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20130524%2Fus-east-1%2Fs3%2Faws4_request" +
      "&X-Amz-Date=20130524T000000Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host" +
      "&X-Amz-Signature=aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404",
    "the presigned URL of the documentation"
  )
}
