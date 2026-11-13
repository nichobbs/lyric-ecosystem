//! Storage.Gcs.Goog4 - Google Cloud Storage V4 signing with an HMAC key
//! (`GOOG4-HMAC-SHA256`).
//!
//! Google's V4 signing process is AWS Signature Version 4 with four names
//! changed: the algorithm is `GOOG4-HMAC-SHA256`, the signing key is derived
//! from `GOOG4<secret>`, the credential scope ends in `storage/goog4_request`,
//! and the request carries `x-goog-date` and `x-goog-content-sha256` (or the
//! `X-Goog-*` query parameters of a signed URL). The canonical request, the
//! percent-encoding and the canonical query string are identical, so they
//! are `Aws.SigV4`'s (package `Lyric.Aws.SigV4`), re-exported here: `Pair`,
//! `uriEncode`, `canonicalUri`, `canonicalQuery`, `canonicalRequest`,
//! `signedHeaderNames`, `sha256Hex`, `Signed`, `SignedRequest`.
//!
//! The re-export also brings `Aws.SigV4`'s own signing functions into scope
//! (`signRequest`, `presignQuery`, ...). Those sign with `AWS4-HMAC-SHA256`;
//! everything that signs for Cloud Storage here has `goog` in its name.
//!
//! Pure functions over strings and bytes: nothing reads the clock, the
//! environment or the network, so each step is checked in
//! `tests/goog4_tests.l` against values computed independently and against
//! the canonical requests Google Cloud Storage itself reported.
@runtime_checked
package Storage.Gcs.Goog4

import Std.Core
import Std.Collections
import Std.Encoding as Enc
import Std.Hash as Hash
import Std.String as Str
pub use Aws.SigV4

// Functions, not `pub val`s: a `pub val` of a package linked as a built
// dependency does not resolve in the dependent package (T0020).
val GOOG_ALGORITHM: String = "GOOG4-HMAC-SHA256"

val GOOG_SERVICE: String = "storage"

val GOOG_REQUEST_TYPE: String = "goog4_request"

/// `GOOG4-HMAC-SHA256`: the algorithm of an HMAC key signing with `x-goog-*`
/// headers.
@pure
pub func googAlgorithm(): String {
  GOOG_ALGORITHM
}

/// A Cloud Storage HMAC key: the access ID (`GOOG1E...`) and its secret.
pub record HmacKey {
  accessId: String
  secret: String
}

/// The canonical resource path of an object key: `/` followed by the key,
/// percent-encoded once with `/` kept and nothing normalised.
@pure
pub func canonicalKeyPath(key: in String): String {
  canonicalUri("/" + key, true)
}

/// `<yyyymmdd>/<location>/storage/goog4_request` for the day of `googDate`
/// (`yyyymmddThhmmssZ`).
@pure
pub func googCredentialScope(googDate: in String, location: in String): String
  requires: googDate.length == 16
{
  googDate.substring(0, 8) + "/" + location + "/" + GOOG_SERVICE + "/" + GOOG_REQUEST_TYPE
}

/// The string to sign: the algorithm, the active datetime, the credential
/// scope and the SHA-256 of the canonical request.
@pure
pub func googStringToSign(googDate: in String, credScope: in String, canonicalRequestText: in String): String {
  GOOG_ALGORITHM + "\n" + googDate + "\n" + credScope + "\n" + sha256Hex(Enc.encodeUtf8(canonicalRequestText))
}

func hmac(key: in slice[Byte], message: in String): slice[Byte] {
  Hash.hmacSha256(key, Enc.encodeUtf8(message))
}

/// The signing key for a day and a location, derived from the HMAC key's
/// secret; the secret itself never signs a request.
@pure
pub func googSigningKey(secret: in String, googDate: in String, location: in String): slice[Byte]
  requires: googDate.length == 16
{
  val kDate = hmac(Enc.encodeUtf8("GOOG4" + secret), googDate.substring(0, 8))
  val kLocation = hmac(kDate, location)
  val kService = hmac(kLocation, GOOG_SERVICE)
  hmac(kService, GOOG_REQUEST_TYPE)
}

/// The lowercase hex signature of `toSign` under the derived signing key.
@pure
pub func googSignatureOf(secret: in String, googDate: in String, location: in String, toSign: in String): String
  requires: googDate.length == 16
{
  Str.toLower(Enc.encodeHex(hmac(googSigningKey(secret, googDate, location), toSign)))
}

/// Sign a request whose signature travels in the `Authorization` header.
/// `headers` are exactly the headers that are signed and must be sent as
/// given: `host`, `x-goog-date`, `x-goog-content-sha256` and every other
/// `x-goog-*` header of the request (Cloud Storage requires all of them to be
/// signed). `googSignedHeaders` builds that list. `canonicalUriText` is
/// already encoded (`canonicalKeyPath`); `query` is not.
@pure
pub func signGoogRequest(
  method: in String,
  canonicalUriText: in String,
  query: in List[Pair],
  headers: in List[Pair],
  payloadHash: in String,
  key: in HmacKey,
  location: in String,
  googDate: in String,
): Signed
  requires: googDate.length == 16
{
  val cr = canonicalRequest(method, canonicalUriText, canonicalQuery(query), headers, payloadHash)
  val credScope = googCredentialScope(googDate, location)
  val sts = googStringToSign(googDate, credScope, cr)
  val sig = googSignatureOf(key.secret, googDate, location, sts)
  val names = signedHeaderNames(headers)
  Signed(
    canonicalRequest = cr,
    stringToSign = sts,
    signedHeaders = names,
    signature = sig,
    authorization = GOOG_ALGORITHM + " Credential=" + key.accessId + "/" + credScope + ", SignedHeaders=" + names +
      ", Signature=" +
      sig
  )
}

/// Sign a request and assemble the headers it must carry.
///
/// Signed are `host`, `x-goog-content-sha256` (`payloadHash`: `sha256Hex` of
/// the body), `x-goog-date`, and every header in `extraHeaders` (lowercase
/// names: `x-goog-meta-*`, `x-goog-storage-class`). The result's `headers`
/// are the ones to set on the request, `Authorization` last; `Host` is not
/// among them, the HTTP client sets it from the URL.
@pure
pub func googSignedHeaders(
  method: in String,
  canonicalUriText: in String,
  query: in List[Pair],
  host: in String,
  extraHeaders: in List[Pair],
  payloadHash: in String,
  key: in HmacKey,
  location: in String,
  googDate: in String,
): SignedRequest
  requires: googDate.length == 16
{
  val toSign: List[Pair] = newList()
  toSign.add(Pair(name = "host", value = host))
  toSign.add(Pair(name = "x-goog-content-sha256", value = payloadHash))
  toSign.add(Pair(name = "x-goog-date", value = googDate))
  var i = 0
  while i < extraHeaders.count {
    toSign.add(extraHeaders[i])
    i = i + 1
  }
  val signed = signGoogRequest(method, canonicalUriText, query, toSign, payloadHash, key, location, googDate)
  val toSend: List[Pair] = newList()
  var k = 1
  while k < toSign.count {
    toSend.add(toSign[k])
    k = k + 1
  }
  toSend.add(Pair(name = "Authorization", value = signed.authorization))
  SignedRequest(headers = toSend, signed = signed)
}

/// The query string of a V4 signed URL, signature included: the `X-Goog-*`
/// authentication parameters, any `extraQuery` parameters, and
/// `X-Goog-Signature` last. Only `host` is signed and the payload is
/// `UNSIGNED-PAYLOAD`, so the holder of the URL needs nothing but the URL.
/// `host` is the authority the URL will be requested from.
@pure
pub func googPresignQuery(
  method: in String,
  host: in String,
  canonicalUriText: in String,
  extraQuery: in List[Pair],
  key: in HmacKey,
  location: in String,
  googDate: in String,
  expiresInSeconds: in Int,
): String
  requires: googDate.length == 16
  requires: expiresInSeconds >= 1 and expiresInSeconds <= 604800
{
  val credScope = googCredentialScope(googDate, location)
  val query: List[Pair] = newList()
  var i = 0
  while i < extraQuery.count {
    query.add(extraQuery[i])
    i = i + 1
  }
  query.add(Pair(name = "X-Goog-Algorithm", value = GOOG_ALGORITHM))
  query.add(Pair(name = "X-Goog-Credential", value = key.accessId + "/" + credScope))
  query.add(Pair(name = "X-Goog-Date", value = googDate))
  query.add(Pair(name = "X-Goog-Expires", value = expiresInSeconds.toString()))
  query.add(Pair(name = "X-Goog-SignedHeaders", value = "host"))
  val headers: List[Pair] = newList()
  headers.add(Pair(name = "host", value = host))
  val canonicalQueryString = canonicalQuery(query)
  val cr = canonicalRequest(method, canonicalUriText, canonicalQueryString, headers, unsignedPayload())
  val sts = googStringToSign(googDate, credScope, cr)
  canonicalQueryString + "&X-Goog-Signature=" + googSignatureOf(key.secret, googDate, location, sts)
}
