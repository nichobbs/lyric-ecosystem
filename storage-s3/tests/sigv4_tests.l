//! tests/sigv4_tests.l - `Storage.S3.SigV4` against the worked examples
//! Amazon publishes.
//!
//! Every expected value is copied from the Amazon S3 API reference,
//! "Authenticating Requests: Using the Authorization Header (AWS Signature
//! Version 4)" (examples: GET Object, PUT Object, GET Bucket Lifecycle, Get
//! Bucket (List Objects)) and "Authenticating Requests: Using Query
//! Parameters" (the presigned URL example). They all use the documentation
//! credentials below, bucket `examplebucket`, region `us-east-1` and the
//! instant `20130524T000000Z`.
@test_module
package Storage.S3.SigV4Tests

import Std.Core
import Std.Collections
import Std.Encoding
import Std.Testing
import Storage.S3.SigV4

val AMZ_DATE: String = "20130524T000000Z"

val HOST: String = "examplebucket.s3.amazonaws.com"

val EMPTY_SHA: String = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

func docCreds(): Credentials {
  Credentials(
    accessKeyId = "AKIAIOSFODNN7EXAMPLE",
    secretAccessKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
    sessionToken = ""
  )
}

func pairs(flat: in slice[String]): List[Pair] {
  val acc: List[Pair] = newList()
  var i = 0
  while i + 1 < flat.length {
    acc.add(Pair(name = flat[i], value = flat[i + 1]))
    i = i + 2
  }
  acc
}

func noPairs(): List[Pair] {
  newList()
}

test "AWS example: GET Object with a Range header" {
  val headers = pairs(["Host", HOST, "Range", "bytes=0-9", "x-amz-content-sha256", EMPTY_SHA, "x-amz-date", AMZ_DATE])
  val signed = signRequest("GET", "/test.txt", noPairs(), headers, EMPTY_SHA, docCreds(), "us-east-1", "s3", AMZ_DATE)
  assertEqual(
    signed.canonicalRequest,
    "GET\n/test.txt\n\nhost:examplebucket.s3.amazonaws.com\nrange:bytes=0-9\n" +
      "x-amz-content-sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\n" +
      "x-amz-date:20130524T000000Z\n\nhost;range;x-amz-content-sha256;x-amz-date\n" +
      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
    "canonical request"
  )
  assertEqual(
    signed.stringToSign,
    "AWS4-HMAC-SHA256\n20130524T000000Z\n20130524/us-east-1/s3/aws4_request\n" +
      "7344ae5b7ee6c3e7e6b0fe0640412a37625d1fbfff95c48bbb2dc43964946972",
    "string to sign"
  )
  assertEqual(signed.signature, "f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41", "signature")
  assertEqual(
    signed.authorization,
    "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, " +
      "SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, " +
      "Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41",
    "Authorization header"
  )
}

test "AWS example: PUT Object with a storage class and an encoded key" {
  val payloadHash = sha256Hex(encodeUtf8("Welcome to Amazon S3."))
  assertEqual(payloadHash, "44ce7dd67c959e0d3524ffac1771dfbba87d2b6b4b4e99e42034a8b803f8b072", "payload hash")
  assertEqual(canonicalKeyPath("test$file.text"), "/test%24file.text", "the key is percent-encoded")
  val headers = pairs(
    [
      "Date",
      "Fri, 24 May 2013 00:00:00 GMT",
      "Host",
      HOST,
      "x-amz-date",
      AMZ_DATE,
      "x-amz-storage-class",
      "REDUCED_REDUNDANCY",
      "x-amz-content-sha256",
      payloadHash
    ]
  )
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
  assertEqual(
    signed.signedHeaders,
    "date;host;x-amz-content-sha256;x-amz-date;x-amz-storage-class",
    "signed headers are sorted"
  )
  assertEqual(signed.signature, "98ad721746da40c64f1a55b78f14c238d841ea1380cd77a1b5971af0ece108bd", "signature")
}

test "AWS example: GET Bucket Lifecycle (a query parameter without a value)" {
  val headers = pairs(["host", HOST, "x-amz-content-sha256", EMPTY_SHA, "x-amz-date", AMZ_DATE])
  val query = pairs(["lifecycle", ""])
  assertEqual(canonicalQuery(query), "lifecycle=", "a valueless parameter keeps its '='")
  val signed = signRequest("GET", "/", query, headers, EMPTY_SHA, docCreds(), "us-east-1", "s3", AMZ_DATE)
  assertEqual(signed.signature, "fea454ca298b7da1c68078a5d1bdbfbbe0d65c699e0f91ac7a200a0136783543", "signature")
}

test "AWS example: Get Bucket (List Objects) with two query parameters" {
  val headers = pairs(["host", HOST, "x-amz-content-sha256", EMPTY_SHA, "x-amz-date", AMZ_DATE])
  // Given out of order: the canonical query sorts them.
  val query = pairs(["prefix", "J", "max-keys", "2"])
  assertEqual(canonicalQuery(query), "max-keys=2&prefix=J", "parameters are sorted by name")
  val signed = signRequest("GET", "/", query, headers, EMPTY_SHA, docCreds(), "us-east-1", "s3", AMZ_DATE)
  assertEqual(signed.signature, "34b48302e7b5fa45bde8084f4b7868a86f0a534bc59db6670ed5711ef69dc6f7", "signature")
}

test "AWS example: presigned GET URL valid for 24 hours" {
  val query = presignQuery("GET", HOST, "/test.txt", noPairs(), docCreds(), "us-east-1", "s3", AMZ_DATE, 86400)
  assertEqual(
    "https://" + HOST + "/test.txt?" + query,
    "https://examplebucket.s3.amazonaws.com/test.txt?X-Amz-Algorithm=AWS4-HMAC-SHA256" +
      "&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20130524%2Fus-east-1%2Fs3%2Faws4_request" +
      "&X-Amz-Date=20130524T000000Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host" +
      "&X-Amz-Signature=aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404",
    "the presigned URL of the documentation"
  )
}

test "a presigned URL with temporary credentials carries and signs the session token" {
  val temp = Credentials(accessKeyId = "ASIAEXAMPLE", secretAccessKey = "secret", sessionToken = "tok/en+1=")
  val query = presignQuery("GET", HOST, "/k", noPairs(), temp, "eu-west-1", "s3", AMZ_DATE, 60)
  assertTrue(
    query.contains("&X-Amz-Security-Token=tok%2Fen%2B1%3D&X-Amz-SignedHeaders=host&X-Amz-Signature="),
    "the token is encoded and sorted before SignedHeaders: " + query
  )
  val withoutToken = presignQuery(
    "GET",
    HOST,
    "/k",
    noPairs(),
    Credentials(accessKeyId = "ASIAEXAMPLE", secretAccessKey = "secret", sessionToken = ""),
    "eu-west-1",
    "s3",
    AMZ_DATE,
    60
  )
  assertTrue(
    query.substring(query.length - 64) != withoutToken.substring(withoutToken.length - 64),
    "the token changes the signature"
  )
}

test "uriEncode leaves only unreserved characters, and '/' when asked" {
  assertEqual(uriEncode("AZaz09-._~", false), "AZaz09-._~", "unreserved characters are untouched")
  assertEqual(uriEncode("a b+c=d&e", false), "a%20b%2Bc%3Dd%26e", "space is %20, never '+'")
  assertEqual(uriEncode("a/b", false), "a%2Fb", "'/' is encoded in a query value")
  assertEqual(uriEncode("a/b", true), "a/b", "'/' is kept in a path")
  assertEqual(uriEncode("!*'()", true), "%21%2A%27%28%29", "sub-delimiters are encoded, uppercase hex")
  assertEqual(uriEncode("café/日本", true), "caf%C3%A9/%E6%97%A5%E6%9C%AC", "non-ASCII is UTF-8 bytes")
  assertEqual(uriEncode("😀", true), "%F0%9F%98%80", "a supplementary-plane character is four bytes")
  assertEqual(
    canonicalKeyPath("photos/2026/my photo (1).jpg"),
    "/photos/2026/my%20photo%20%281%29.jpg",
    "an object key"
  )
  assertEqual(canonicalKeyPath("100%/a"), "/100%25/a", "a literal percent is encoded once")
}

test "canonical query sorts by encoded name, then value" {
  val query = pairs(["b", "2", "a", "z", "a", "b", "A", "1", "list-type", "2", "continuation-token", "a/b=="])
  assertEqual(
    canonicalQuery(query),
    "A=1&a=b&a=z&b=2&continuation-token=a%2Fb%3D%3D&list-type=2",
    "uppercase sorts before lowercase, values break ties"
  )
  assertEqual(canonicalQuery(noPairs()), "", "no parameters is the empty string")
}

test "header values are trimmed and inner runs of spaces collapsed" {
  val headers = pairs(["Host", "h", "X-Amz-Meta-Note", "  two   spaces  here "])
  val cr = canonicalRequest("PUT", "/k", "", headers, "UNSIGNED-PAYLOAD")
  assertEqual(
    cr,
    "PUT\n/k\n\nhost:h\nx-amz-meta-note:two spaces here\n\nhost;x-amz-meta-note\nUNSIGNED-PAYLOAD",
    "canonical headers"
  )
}

test "the signature depends on the day, the region and the service" {
  val base = signatureOf("secret", AMZ_DATE, "us-east-1", "s3", "x")
  assertEqualInt(base.length, 64, "a signature is 64 hex digits")
  assertTrue(base != signatureOf("secret", "20130525T000000Z", "us-east-1", "s3", "x"), "another day")
  assertTrue(base != signatureOf("secret", AMZ_DATE, "us-west-2", "s3", "x"), "another region")
  assertTrue(base != signatureOf("secret", AMZ_DATE, "us-east-1", "ses", "x"), "another service")
  assertEqual(
    base,
    signatureOf("secret", "20130524T235959Z", "us-east-1", "s3", "x"),
    "the time of day is not in the key"
  )
  assertEqual(credentialScope(AMZ_DATE, "us-east-1", "s3"), "20130524/us-east-1/s3/aws4_request", "credential scope")
}
