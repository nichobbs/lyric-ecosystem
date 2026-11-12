//! tests/sigv4_tests.l - `Aws.SigV4` against the worked examples Amazon
//! publishes.
//!
//! The S3 values are copied from the Amazon S3 API reference, "Authenticating
//! Requests: Using the Authorization Header (AWS Signature Version 4)"
//! (examples: GET Object, PUT Object, GET Bucket Lifecycle, Get Bucket (List
//! Objects)) and "Authenticating Requests: Using Query Parameters" (the
//! presigned URL example): documentation credentials, bucket `examplebucket`,
//! region `us-east-1`, instant `20130524T000000Z`.
//!
//! The non-S3 values are those of the IAM User Guide's "Create a signed AWS
//! API request" walk-through (`GET iam ListUsers`, with its published
//! canonical request hash, signing key and signature) and of the AWS
//! Signature Version 4 test suite (`get-vanilla`, `post-vanilla`,
//! `post-x-www-form-urlencoded`, `get-header-value-trim`, `post-sts-token`):
//! credentials `AKIDEXAMPLE`, instant `20150830T123600Z`. The Amazon SQS JSON
//! request is the `SendMessage` example of the SQS Developer Guide, for which
//! Amazon publishes no signature; its expected values, like every value
//! above, were recomputed with an independent implementation (Python's `hmac`
//! and `hashlib`) before being written here.
@test_module
package Aws.SigV4.Tests

import Std.Core
import Std.Collections
import Std.Encoding
import Std.Testing
import Aws.SigV4

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
  assertEqual(canonicalUri("/test$file.text", true), "/test%24file.text", "the key is percent-encoded")
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
    canonicalUri("/test$file.text", true),
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
    canonicalUri("/photos/2026/my photo (1).jpg", true),
    "/photos/2026/my%20photo%20%281%29.jpg",
    "an object key"
  )
  assertEqual(canonicalUri("/100%/a", true), "/100%25/a", "a literal percent is encoded once")
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

// ─── Services other than S3 ──────────────────────────────────────────────────
val V4_DATE: String = "20150830T123600Z"

func suiteCreds(): Credentials {
  Credentials(
    accessKeyId = "AKIDEXAMPLE",
    secretAccessKey = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
    sessionToken = ""
  )
}

test "AWS example: GET iam ListUsers" {
  val headers = pairs(
    [
      "Content-Type",
      "application/x-www-form-urlencoded; charset=utf-8",
      "Host",
      "iam.amazonaws.com",
      "X-Amz-Date",
      V4_DATE
    ]
  )
  val query = pairs(["Version", "2010-05-08", "Action", "ListUsers"])
  val signed = signRequest(
    "GET",
    canonicalUri("/", false),
    query,
    headers,
    EMPTY_SHA,
    suiteCreds(),
    "us-east-1",
    "iam",
    V4_DATE
  )
  assertEqual(
    signed.canonicalRequest,
    "GET\n/\nAction=ListUsers&Version=2010-05-08\n" +
      "content-type:application/x-www-form-urlencoded; charset=utf-8\nhost:iam.amazonaws.com\n" +
      "x-amz-date:20150830T123600Z\n\ncontent-type;host;x-amz-date\n" +
      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
    "canonical request"
  )
  assertEqual(
    signed.stringToSign,
    "AWS4-HMAC-SHA256\n20150830T123600Z\n20150830/us-east-1/iam/aws4_request\n" +
      "f536975d06c0309214f805bb90ccff089219ecd68b2577efef23edd43b7e1a59",
    "string to sign"
  )
  assertEqual(signed.signature, "5d672d79c15b13162d9279b0855cfba6789a8edb4c82c400e06b5924a6f2b5d7", "signature")
  assertEqual(
    signed.authorization,
    "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/iam/aws4_request, " +
      "SignedHeaders=content-type;host;x-amz-date, " +
      "Signature=5d672d79c15b13162d9279b0855cfba6789a8edb4c82c400e06b5924a6f2b5d7",
    "Authorization header"
  )
}

test "AWS example: the signing key of 20150830/us-east-1/iam" {
  assertEqual(
    encodeHex(signingKey("wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY", V4_DATE, "us-east-1", "iam")).toLower(),
    "c4afb1cc5771d871763a393e44b703571b55cc28424d1a5e86da6ed3c154a4b9",
    "kSigning"
  )
}

func suiteSignature(method: in String, query: in List[Pair], headers: in List[Pair], body: in String): String {
  signRequest(method, canonicalUri("/", false), query, headers, sha256Hex(encodeUtf8(body)), suiteCreds(), "us-east-1", "service", V4_DATE).signature
}

test "AWS Signature Version 4 test suite: vanilla GET and POST, a form body, trimmed header values" {
  val plain = pairs(["Host", "example.amazonaws.com", "X-Amz-Date", V4_DATE])
  assertEqual(
    suiteSignature("GET", noPairs(), plain, ""),
    "5fa00fa31553b73ebf1942676e86291e8372ff2a2260956d9b8aae1d763fbf31",
    "get-vanilla"
  )
  assertEqual(
    suiteSignature("POST", noPairs(), plain, ""),
    "5da7c1a2acd57cee7505fc6676e4e544621c30862966e37dddb68e92efbe5d6b",
    "post-vanilla"
  )
  assertEqual(
    suiteSignature("GET", pairs(["Param2", "value2", "Param1", "value1"]), plain, ""),
    "b97d918cfa904a5beff61c982a1b6f458b799221646efd99d3219ec94cdf2500",
    "get-vanilla-query-order-key-case"
  )
  val form = pairs(
    [
      "Content-Type",
      "application/x-www-form-urlencoded",
      "Host",
      "example.amazonaws.com",
      "X-Amz-Date",
      V4_DATE
    ]
  )
  assertEqual(
    suiteSignature("POST", noPairs(), form, "Param1=value1"),
    "ff11897932ad3f4e8b18135d722051e5ac45fc38421b1da7b9d196a0fe09473a",
    "post-x-www-form-urlencoded"
  )
  val spaced = pairs(
    [
      "Host",
      "example.amazonaws.com",
      "My-Header1",
      " value1",
      "My-Header2",
      " \"a   b   c\"",
      "X-Amz-Date",
      V4_DATE
    ]
  )
  assertEqual(
    suiteSignature("GET", noPairs(), spaced, ""),
    "acc3ed3afb60bb290fc8d2dd0098b9911fcaa05412b367055dee359757a9c736",
    "get-header-value-trim"
  )
}

val SUITE_TOKEN: String = "AQoDYXdzEPT//////////wEXAMPLEtc764bNrC9SAPBSM22wDOk4x4HIZ8j4FZTwdQWLWsKWHGBuFqwAeMicRXmxfpSPfIeoIYRqTflfKD8YUuwthAx7mSEI/qkPpKPi/kMcGdQrmGdeehM4IC1NtBmUpp2wUE8phUZampKsburEDy0KPkyQDYwT7WZ0wq5VSXDvp75YU9HFvlRd8Tx6q6fE8YQcHNVXAkiY9q6d+xo0rKwT38xVqr7ZD0u0iPPkUL64lIZbqBAz+scqKmlzm8FDrypNC9Yjc8fPOLn9FX9KSYvKTr4rvx3iSIlTJabIQwj2ICCR/oLxBA=="

test "AWS Signature Version 4 test suite: a session token is sent and signed (post-sts-header-after)" {
  val temp = Credentials(
    accessKeyId = "AKIDEXAMPLE",
    secretAccessKey = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
    sessionToken = SUITE_TOKEN
  )
  val request = signedHeaders(
    "POST",
    canonicalUri("/", false),
    noPairs(),
    "example.amazonaws.com",
    noPairs(),
    EMPTY_SHA,
    false,
    temp,
    "us-east-1",
    "service",
    V4_DATE
  )
  assertEqual(request.signed.signedHeaders, "host;x-amz-date;x-amz-security-token", "signed headers")
  assertEqual(request.signed.signature, "85d96828115b5dc0cfc3bd16ad9e210dd772bbebba041836c64533a82be05ead", "signature")
  assertEqualInt(request.headers.count, 3, "x-amz-date, the token and Authorization are sent")
  assertEqual(request.headers[0].name, "x-amz-date", "first header")
  assertEqual(request.headers[1].name, "x-amz-security-token", "second header")
  assertEqual(request.headers[1].value, SUITE_TOKEN, "the token is sent as given")
  assertEqual(request.headers[2].name, "Authorization", "Authorization is last")
  assertEqual(request.headers[2].value, request.signed.authorization, "Authorization value")
}

val SQS_BODY: String = "{\"QueueUrl\":\"https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue\",\"MessageBody\":\"This is a test message\"}"

func sqsHeaders(): List[Pair] {
  pairs(["content-type", "application/x-amz-json-1.0", "x-amz-target", "AmazonSQS.SendMessage"])
}

test "Amazon SQS JSON 1.0: POST SendMessage" {
  val payloadHash = sha256Hex(encodeUtf8(SQS_BODY))
  assertEqual(payloadHash, "535d4dc42deadd62be2669b76c79f67a7c9c4d468a9801202ad3f230531fd6d0", "payload hash")
  val request = signedHeaders(
    "POST",
    canonicalUri("/", false),
    noPairs(),
    "sqs.us-east-1.amazonaws.com",
    sqsHeaders(),
    payloadHash,
    false,
    suiteCreds(),
    "us-east-1",
    "sqs",
    V4_DATE
  )
  assertEqual(
    request.signed.canonicalRequest,
    "POST\n/\n\ncontent-type:application/x-amz-json-1.0\nhost:sqs.us-east-1.amazonaws.com\n" +
      "x-amz-date:20150830T123600Z\nx-amz-target:AmazonSQS.SendMessage\n\n" +
      "content-type;host;x-amz-date;x-amz-target\n" +
      "535d4dc42deadd62be2669b76c79f67a7c9c4d468a9801202ad3f230531fd6d0",
    "canonical request"
  )
  assertEqual(
    request.signed.stringToSign,
    "AWS4-HMAC-SHA256\n20150830T123600Z\n20150830/us-east-1/sqs/aws4_request\n" +
      "db82930953d5bfdacfb1a8ffc3c1c5344dbb2782dac6d86d3e6028733adf599c",
    "string to sign"
  )
  assertEqual(request.signed.signature, "72e065d2e22b25e7f5ca8a3347d8f40cb99a93ec7f258c7c51272a82e7ee9823", "signature")
  // No x-amz-content-sha256: only S3 wants the payload hash as a header.
  assertEqualInt(request.headers.count, 4, "x-amz-date, content-type, x-amz-target, Authorization")
  assertEqual(request.headers[0].name, "x-amz-date", "x-amz-date first")
  assertEqual(request.headers[1].name, "content-type", "then the extra headers, in the order given")
  assertEqual(request.headers[2].name, "x-amz-target", "x-amz-target")
  assertEqual(
    request.headers[3].value,
    "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/sqs/aws4_request, " +
      "SignedHeaders=content-type;host;x-amz-date;x-amz-target, " +
      "Signature=72e065d2e22b25e7f5ca8a3347d8f40cb99a93ec7f258c7c51272a82e7ee9823",
    "Authorization"
  )
}

test "Amazon SQS JSON 1.0: temporary credentials" {
  val temp = Credentials(
    accessKeyId = "AKIDEXAMPLE",
    secretAccessKey = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
    sessionToken = "tok/en+1="
  )
  val request = signedHeaders(
    "POST",
    canonicalUri("/", false),
    noPairs(),
    "sqs.us-east-1.amazonaws.com",
    sqsHeaders(),
    sha256Hex(encodeUtf8(SQS_BODY)),
    false,
    temp,
    "us-east-1",
    "sqs",
    V4_DATE
  )
  assertEqual(
    request.signed.signedHeaders,
    "content-type;host;x-amz-date;x-amz-security-token;x-amz-target",
    "the token header is signed"
  )
  assertEqual(request.signed.signature, "496d1521a1914696e469630f29a657fa9ef7a0d6a3ead1ba6008f26882603ed0", "signature")
}

test "signedHeaders for S3 sends and signs x-amz-content-sha256" {
  val request = signedHeaders(
    "GET",
    "/test.txt",
    noPairs(),
    HOST,
    pairs(["range", "bytes=0-9"]),
    EMPTY_SHA,
    true,
    docCreds(),
    "us-east-1",
    "s3",
    AMZ_DATE
  )
  assertEqual(
    request.signed.signature,
    "f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41",
    "the signature of the GET Object example"
  )
  assertEqual(request.headers[0].name, "x-amz-content-sha256", "the payload hash is sent")
  assertEqual(request.headers[0].value, EMPTY_SHA, "its value")
  assertEqual(emptyPayloadSha256(), sha256Hex(encodeUtf8("")), "the empty payload hash")
}

test "the canonical URI: S3 keeps the path, other services normalise it and encode twice" {
  assertEqual(canonicalUri("", false), "/", "the empty path")
  assertEqual(canonicalUri("/", false), "/", "the root")
  assertEqual(canonicalUri("/", true), "/", "the root, S3")
  assertEqual(canonicalUri("/example/..", false), "/", "get-relative")
  assertEqual(canonicalUri("/example1/example2/../..", false), "/", "get-relative-relative")
  assertEqual(canonicalUri("//", false), "/", "get-slash")
  assertEqual(canonicalUri("/./", false), "/", "get-slash-dot-slash")
  assertEqual(canonicalUri("/./example", false), "/example", "get-slash-pointless-dot")
  assertEqual(canonicalUri("//example//", false), "/example/", "get-slashes")
  assertEqual(canonicalUri("/123456789012/MyQueue", false), "/123456789012/MyQueue", "a queue path")
  assertEqual(canonicalUri("/a b/c", false), "/a%2520b/c", "encoded twice")
  assertEqual(encodedPath("/a b/c"), "/a%20b/c", "the request line carries it encoded once")
  assertEqual(canonicalUri("/ሴ", false), "/%25E1%2588%25B4", "non-ASCII, encoded twice")
  assertEqual(canonicalUri("/a//b/./c/../d", true), "/a//b/./c/../d", "S3 never normalises")
  assertEqual(canonicalUri("a b", true), "/a%20b", "a leading slash is supplied")
}

test "amzDateOfIso is the basic ISO 8601 form" {
  assertEqual(amzDateOfIso("2026-11-12T10:15:00.123Z"), "20261112T101500Z", "with milliseconds")
  assertEqual(amzDateOfIso("2013-05-24T00:00:00Z"), "20130524T000000Z", "without")
}
