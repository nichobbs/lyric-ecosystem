//! tests/goog4_tests.l - GOOG4-HMAC-SHA256 signing, step by step.
//!
//! Where the expected values come from:
//!
//!   * every canonical request and string to sign below was reported back,
//!     byte for byte, by Google Cloud Storage itself (storage.googleapis.com,
//!     2026-11-12): a request signed with an HMAC key that does not exist is
//!     refused with `SignatureDoesNotMatch`, and the error document carries
//!     the `CanonicalRequest` and `StringToSign` the service derived from the
//!     request it received;
//!   * every signature and the signing key were computed independently with
//!     Python's `hmac` and `hashlib` from Google's description of the signing
//!     key derivation ("Signatures", cloud.google.com/storage/docs), not with
//!     this package.
//!
//! The access ID and secret are fixtures; they are not a real HMAC key.
@test_module
package Storage.Gcs.Goog4Tests

import Std.Core
import Std.Collections
import Std.Encoding
import Std.Testing
import Storage.Gcs.Goog4

val GOOG_DATE: String = "20261112T224228Z"

val HOST: String = "storage.googleapis.com"

val EMPTY_SHA256: String = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

func fixtureKey(): HmacKey {
  HmacKey(
    accessId = "GOOG1EXAMPLEEXAMPLEEXAMPLEEXAMPLEEXAMPLEEXAMPLEEXAMPLEEXAMPL",
    secret = "fixture/secret+for-tests-only"
  )
}

func noPairs(): List[Pair] {
  newList()
}

test "the algorithm, credential scope and string to sign are Google's" {
  assertEqual(googAlgorithm(), "GOOG4-HMAC-SHA256", "the algorithm of an HMAC key with x-goog-* headers")
  assertEqual(googCredentialScope(GOOG_DATE, "auto"), "20261112/auto/storage/goog4_request", "the credential scope")
  assertEqual(
    googCredentialScope("20191201T190859Z", "us-central1"),
    "20191201/us-central1/storage/goog4_request",
    "the credential scope of the documentation's example"
  )
  assertEqual(
    googStringToSign(GOOG_DATE, "20261112/auto/storage/goog4_request", "GET\n/\n\nhost:h\n\nhost\nUNSIGNED-PAYLOAD"),
    "GOOG4-HMAC-SHA256\n20261112T224228Z\n20261112/auto/storage/goog4_request\n" +
      sha256Hex(encodeUtf8("GET\n/\n\nhost:h\n\nhost\nUNSIGNED-PAYLOAD")),
    "algorithm, active datetime, scope, hash of the canonical request"
  )
}

test "the signing key is derived from GOOG4<secret>, the day, the location, storage, goog4_request" {
  assertEqual(
    encodeHex(googSigningKey("fixture/secret+for-tests-only", GOOG_DATE, "auto")).toLower(),
    "5b20d65c657770898ab26c8de7a896dc81b9d75ebf9812f7d643f0a7abf28ca0",
    "the derived key, computed independently"
  )
  assertTrue(
    encodeHex(googSigningKey("fixture/secret+for-tests-only", GOOG_DATE, "us")) !=
      encodeHex(googSigningKey("fixture/secret+for-tests-only", GOOG_DATE, "auto")),
    "another location is another key"
  )
  assertTrue(
    encodeHex(googSigningKey("fixture/secret+for-tests-only", "20261113T000000Z", "auto")) !=
      encodeHex(googSigningKey("fixture/secret+for-tests-only", GOOG_DATE, "auto")),
    "another day is another key"
  )
}

test "an object key is encoded once, with '/' kept and nothing normalised" {
  assertEqual(canonicalKeyPath("photos/2026/my photo (1).jpg"), "/photos/2026/my%20photo%20%281%29.jpg", "a key")
  assertEqual(canonicalKeyPath("100%/a"), "/100%25/a", "a literal percent is encoded once")
  assertEqual(canonicalKeyPath("a//b/./c/../d"), "/a//b/./c/../d", "dot and empty segments are part of the name")
  assertEqual(canonicalKeyPath("café/日本"), "/caf%C3%A9/%E6%97%A5%E6%9C%AC", "non-ASCII is UTF-8 bytes")
}

test "GET of a key that needs encoding: canonical request, string to sign and signature" {
  val headers: List[Pair] = newList()
  headers.add(Pair(name = "host", value = HOST))
  headers.add(Pair(name = "x-goog-content-sha256", value = EMPTY_SHA256))
  headers.add(Pair(name = "x-goog-date", value = GOOG_DATE))
  val signed = signGoogRequest(
    "GET",
    "/lyric-signature-probe" + canonicalKeyPath("a b+c/é(1)*~!$&'=@,;.txt"),
    noPairs(),
    headers,
    EMPTY_SHA256,
    fixtureKey(),
    "auto",
    GOOG_DATE
  )
  assertEqual(
    signed.canonicalRequest,
    "GET\n/lyric-signature-probe/a%20b%2Bc/%C3%A9%281%29%2A~%21%24%26%27%3D%40%2C%3B.txt\n\n" +
      "host:storage.googleapis.com\n" +
      "x-goog-content-sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\n" +
      "x-goog-date:20261112T224228Z\n\n" +
      "host;x-goog-content-sha256;x-goog-date\n" +
      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
    "the canonical request Cloud Storage derived from this request"
  )
  assertEqual(
    signed.stringToSign,
    "GOOG4-HMAC-SHA256\n20261112T224228Z\n20261112/auto/storage/goog4_request\n" +
      "8a9ea3a3b550e9a6daa14e5d705c9a1cb311e8871a6c9f3aed160d13831f2f54",
    "the string to sign Cloud Storage derived from this request"
  )
  assertEqual(signed.signature, "e35486fa4b5f25e3d63c1faa8de7594cd36d395abaf30f85a5f77e4140686c66", "signature")
  assertEqual(
    signed.authorization,
    "GOOG4-HMAC-SHA256 Credential=GOOG1EXAMPLEEXAMPLEEXAMPLEEXAMPLEEXAMPLEEXAMPLEEXAMPLEEXAMPL" +
      "/20261112/auto/storage/goog4_request, SignedHeaders=host;x-goog-content-sha256;x-goog-date, " +
      "Signature=e35486fa4b5f25e3d63c1faa8de7594cd36d395abaf30f85a5f77e4140686c66",
    "the Authorization header"
  )
}

test "a listing: the query string is sorted and encoded" {
  val query: List[Pair] = newList()
  query.add(Pair(name = "list-type", value = "2"))
  query.add(Pair(name = "encoding-type", value = "url"))
  query.add(Pair(name = "max-keys", value = "2"))
  query.add(Pair(name = "prefix", value = "a b+c/é&="))
  query.add(Pair(name = "continuation-token", value = "ab/+="))
  val signed = googSignedHeaders(
    "GET",
    "/lyric-signature-probe",
    query,
    HOST,
    noPairs(),
    EMPTY_SHA256,
    fixtureKey(),
    "auto",
    GOOG_DATE
  )
  assertEqual(
    signed.signed.canonicalRequest,
    "GET\n/lyric-signature-probe\n" +
      "continuation-token=ab%2F%2B%3D&encoding-type=url&list-type=2&max-keys=2&prefix=a%20b%2Bc%2F%C3%A9%26%3D\n" +
      "host:storage.googleapis.com\n" +
      "x-goog-content-sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\n" +
      "x-goog-date:20261112T224228Z\n\n" +
      "host;x-goog-content-sha256;x-goog-date\n" +
      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
    "the canonical request Cloud Storage derived from this request"
  )
  assertEqual(
    signed.signed.stringToSign,
    "GOOG4-HMAC-SHA256\n20261112T224228Z\n20261112/auto/storage/goog4_request\n" +
      "8e33cac5dc2af73d1ae26762fab1304834f742c25741037161c224e99636c2f1",
    "the string to sign Cloud Storage derived from this request"
  )
  assertEqual(signed.signed.signature, "08a8253dd2f7476f99db7a6281d7b5e970e574a46d682e09e541a915ff8eefe7", "signature")
  // The headers to send: everything signed but `host`, then `Authorization`.
  assertEqualInt(signed.headers.count, 3, "three headers to set")
  assertEqual(signed.headers[0].name, "x-goog-content-sha256", "the payload hash is sent")
  assertEqual(signed.headers[1].name, "x-goog-date", "the date is sent")
  assertEqual(signed.headers[1].value, GOOG_DATE, "as signed")
  assertEqual(signed.headers[2].name, "Authorization", "Authorization last")
  assertEqual(signed.headers[2].value, signed.signed.authorization, "the signature")
}

test "PUT with user metadata and a storage class: every x-goog-* header is signed" {
  val extra: List[Pair] = newList()
  extra.add(Pair(name = "x-goog-meta-owner", value = "alice"))
  extra.add(Pair(name = "x-goog-meta-note", value = "two  words, one comma"))
  extra.add(Pair(name = "x-goog-storage-class", value = "NEARLINE"))
  val payloadHash = sha256Hex(encodeUtf8("hello"))
  val signed = googSignedHeaders(
    "PUT",
    "/lyric-signature-probe" + canonicalKeyPath("reports/q3.txt"),
    noPairs(),
    HOST,
    extra,
    payloadHash,
    fixtureKey(),
    "europe-west1",
    GOOG_DATE
  )
  assertEqual(
    signed.signed.canonicalRequest,
    "PUT\n/lyric-signature-probe/reports/q3.txt\n\n" + "host:storage.googleapis.com\n" +
      "x-goog-content-sha256:2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824\n" +
      "x-goog-date:20261112T224228Z\n" +
      "x-goog-meta-note:two words, one comma\n" +
      "x-goog-meta-owner:alice\n" +
      "x-goog-storage-class:NEARLINE\n\n" +
      "host;x-goog-content-sha256;x-goog-date;x-goog-meta-note;x-goog-meta-owner;x-goog-storage-class\n" +
      "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
    "the canonical request Cloud Storage derived: headers sorted, a run of spaces collapsed"
  )
  assertEqual(
    signed.signed.stringToSign,
    "GOOG4-HMAC-SHA256\n20261112T224228Z\n20261112/europe-west1/storage/goog4_request\n" +
      "c715e0351960974ad90c8cf978c8fbf5406da139bda822211c87495c4ae00dc6",
    "the string to sign Cloud Storage derived, scoped to the configured location"
  )
  assertEqual(signed.signed.signature, "24cf51d9b216054171a5cecabaae71850ee0bac042e3fbf2269c9631950cf3bc", "signature")
  assertEqualInt(signed.headers.count, 6, "payload hash, date, three x-goog-* headers, Authorization")
  assertEqual(signed.headers[3].value, "two  words, one comma", "the header is sent as given, not as canonicalised")
}

test "DELETE addressed virtual-hosted: the bucket is in the signed host, not the path" {
  val signed = googSignedHeaders(
    "DELETE",
    canonicalKeyPath("reports/q3.txt"),
    noPairs(),
    "lyric-signature-probe.storage.googleapis.com",
    noPairs(),
    EMPTY_SHA256,
    fixtureKey(),
    "auto",
    GOOG_DATE
  )
  assertEqual(
    signed.signed.stringToSign,
    "GOOG4-HMAC-SHA256\n20261112T224228Z\n20261112/auto/storage/goog4_request\n" +
      "cd2fb4b467cf1e1045c0231fa637a710a0f18e9ea0198aa2d36a672137d775f4",
    "the string to sign Cloud Storage derived from this request"
  )
  assertEqual(signed.signed.signature, "83b336a50d81769ca5a914f73c6c4392662fdf9970f15164a999417d7005b871", "signature")
}

test "a V4 signed URL: X-Goog-* parameters, only the host signed, UNSIGNED-PAYLOAD" {
  val getQuery = googPresignQuery(
    "GET",
    HOST,
    "/lyric-signature-probe" + canonicalKeyPath("shared/q3 report (final).txt"),
    noPairs(),
    fixtureKey(),
    "auto",
    GOOG_DATE,
    3600
  )
  assertEqual(
    getQuery,
    "X-Goog-Algorithm=GOOG4-HMAC-SHA256" +
      "&X-Goog-Credential=GOOG1EXAMPLEEXAMPLEEXAMPLEEXAMPLEEXAMPLEEXAMPLEEXAMPLEEXAMPL%2F20261112%2Fauto%2Fstorage%2Fgoog4_request" +
      "&X-Goog-Date=20261112T224228Z&X-Goog-Expires=3600&X-Goog-SignedHeaders=host" +
      "&X-Goog-Signature=d47070125abca88219bd7608ef92cf331fad140b68195ae5eddc5abe85777865",
    "a signed GET URL's query string, computed independently"
  )
  val putQuery = googPresignQuery(
    "PUT",
    HOST,
    "/lyric-signature-probe" + canonicalKeyPath("inbox/upload.bin"),
    noPairs(),
    fixtureKey(),
    "us-central1",
    GOOG_DATE,
    604800
  )
  assertEqual(
    putQuery,
    "X-Goog-Algorithm=GOOG4-HMAC-SHA256" +
      "&X-Goog-Credential=GOOG1EXAMPLEEXAMPLEEXAMPLEEXAMPLEEXAMPLEEXAMPLEEXAMPLEEXAMPL%2F20261112%2Fus-central1%2Fstorage%2Fgoog4_request" +
      "&X-Goog-Date=20261112T224228Z&X-Goog-Expires=604800&X-Goog-SignedHeaders=host" +
      "&X-Goog-Signature=efbb8ae20566a39dd840896e8a0d9bd6533597679dbe0144ecad20215f8f412f",
    "a signed PUT URL for the longest lifetime, in another location"
  )
}

test "a different secret, method or date gives a different signature" {
  val sts = "GOOG4-HMAC-SHA256\n20261112T224228Z\n20261112/auto/storage/goog4_request\nabc"
  val reference = googSignatureOf("fixture/secret+for-tests-only", GOOG_DATE, "auto", sts)
  assertEqualInt(reference.length, 64, "a hex SHA-256 HMAC")
  assertEqual(reference, reference.toLower(), "lowercase hex")
  assertTrue(googSignatureOf("another-secret", GOOG_DATE, "auto", sts) != reference, "the secret matters")
  assertTrue(
    googSignatureOf("fixture/secret+for-tests-only", GOOG_DATE, "auto", sts + "d") != reference,
    "the string to sign matters"
  )
}
