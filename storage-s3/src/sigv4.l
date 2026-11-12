//! Storage.S3.SigV4 - AWS Signature Version 4 for the S3 REST API.
//!
//! Pure functions over strings and bytes: nothing here reads the clock, the
//! environment or the network, so every step can be checked against the worked
//! examples in the Amazon S3 API reference ("Signature Calculations for the
//! Authorization Header" and "Authenticating Requests: Using Query
//! Parameters"), which `tests/sigv4_tests.l` does.
//!
//! S3 differs from other AWS services in three ways this module handles:
//! the payload hash is also sent and signed as `x-amz-content-sha256`; the
//! canonical URI is the object key percent-encoded once, with `/` left alone;
//! and a presigned URL carries the signature in the query string with the
//! literal payload `UNSIGNED-PAYLOAD`.
@runtime_checked
package Storage.S3.SigV4

import Std.Core
import Std.Collections
import Std.Encoding as Enc
import Std.Hash as Hash
import Std.String as Str

/// The payload hash of a presigned URL, and of a request whose body is not
/// signed.
val UNSIGNED_PAYLOAD: String = "UNSIGNED-PAYLOAD"

/// The only algorithm this module signs with.
val ALGORITHM: String = "AWS4-HMAC-SHA256"

/// A name and value: a header to sign, or a query parameter.
pub record Pair {
  name: String
  value: String
}

/// The credentials a request is signed with. `sessionToken` is empty for
/// long-term credentials.
pub record Credentials {
  accessKeyId: String
  secretAccessKey: String
  sessionToken: String
}

/// A signed request, step by step, so a caller (or a test) can see each
/// intermediate value the specification names.
pub record Signed {
  canonicalRequest: String
  stringToSign: String
  signedHeaders: String
  signature: String
  authorization: String
}

func isUnreserved(b: in Int): Bool {
  (b >= 65 and b <= 90) or (b >= 97 and b <= 122) or (b >= 48 and b <= 57) or b == 45 or b == 46 or b == 95 or b == 126
}

/// Percent-encode `text` as SigV4 requires: every UTF-8 byte except
/// `A-Z a-z 0-9 - . _ ~` becomes `%XX` with uppercase hex digits. `/` is
/// encoded too unless `keepSlash`, which is how an object key is encoded into
/// a path.
@pure
pub func uriEncode(text: in String, keepSlash: in Bool): String {
  val digits = "0123456789ABCDEF"
  val bytes = Enc.encodeUtf8(text)
  val sb = Str.StringBuilder.new()
  var i = 0
  while i < bytes.length {
    val b = bytes[i].toInt()
    if isUnreserved(b) or (keepSlash and b == 47) {
      sb.append(Enc.codepointToString(b))
    } else {
      sb.append("%")
      sb.append(digits.substring(b / 16, 1))
      sb.append(digits.substring(b % 16, 1))
    }
    i = i + 1
  }
  sb.toString()
}

/// The canonical URI of an object: `/` followed by the key, encoded once with
/// `/` kept.
@pure
pub func canonicalKeyPath(key: in String): String {
  "/" + uriEncode(key, true)
}

// Ordinal comparison by UTF-16 code unit. Both inputs are ASCII wherever this
// is used (encoded parameters, lowercase header names), so it is byte order.
func ordinalLess(a: in String, b: in String): Bool {
  var i = 0
  while i < a.length and i < b.length {
    val x = Str.codeUnitAt(a, i)
    val y = Str.codeUnitAt(b, i)
    if x != y {
      return x < y
    }
    i = i + 1
  }
  a.length < b.length
}

func pairLess(a: in Pair, b: in Pair): Bool {
  if a.name == b.name {
    return ordinalLess(a.value, b.value)
  }
  ordinalLess(a.name, b.name)
}

// Insertion sort: the lists are a handful of headers or parameters.
func sortedPairs(xs: in List[Pair]): List[Pair] {
  val acc: List[Pair] = newList()
  var i = 0
  while i < xs.count {
    acc.add(xs[i])
    var j = acc.count - 1
    while j > 0 and pairLess(acc[j], acc[j - 1]) {
      val tmp = acc[j]
      acc[j] = acc[j - 1]
      acc[j - 1] = tmp
      j = j - 1
    }
    i = i + 1
  }
  acc
}

/// The canonical query string: each name and value percent-encoded, sorted by
/// encoded name then value, joined as `name=value` with `&`. A parameter
/// without a value is `name=`.
@pure
pub func canonicalQuery(params: in List[Pair]): String {
  val encoded: List[Pair] = newList()
  var i = 0
  while i < params.count {
    encoded.add(Pair(name = uriEncode(params[i].name, false), value = uriEncode(params[i].value, false)))
    i = i + 1
  }
  val sorted = sortedPairs(encoded)
  val sb = Str.StringBuilder.new()
  var k = 0
  while k < sorted.count {
    if k > 0 {
      sb.append("&")
    }
    sb.append(sorted[k].name)
    sb.append("=")
    sb.append(sorted[k].value)
    k = k + 1
  }
  sb.toString()
}

// Trim, and collapse each run of spaces to one, as the canonical headers
// require of a header value.
func canonicalHeaderValue(value: in String): String {
  val trimmed = Str.trim(value)
  val sb = Str.StringBuilder.new()
  var prevSpace = false
  var i = 0
  while i < trimmed.length {
    val c = Str.charAtOrReplacement(trimmed, i)
    if c == ' ' {
      if not prevSpace {
        sb.append(" ")
      }
      prevSpace = true
    } else {
      sb.appendChar(c)
      prevSpace = false
    }
    i = i + 1
  }
  sb.toString()
}

func canonicalHeaderPairs(headers: in List[Pair]): List[Pair] {
  val lowered: List[Pair] = newList()
  var i = 0
  while i < headers.count {
    lowered.add(Pair(name = Str.toLower(headers[i].name), value = canonicalHeaderValue(headers[i].value)))
    i = i + 1
  }
  sortedPairs(lowered)
}

/// The `SignedHeaders` value for `headers`: lowercase names, sorted, joined
/// with `;`.
@pure
pub func signedHeaderNames(headers: in List[Pair]): String {
  val sorted = canonicalHeaderPairs(headers)
  val sb = Str.StringBuilder.new()
  var i = 0
  while i < sorted.count {
    if i > 0 {
      sb.append(";")
    }
    sb.append(sorted[i].name)
    i = i + 1
  }
  sb.toString()
}

/// The canonical request (task 1 of the specification). `canonicalUri` and
/// `canonicalQueryString` are already encoded (`canonicalKeyPath`,
/// `canonicalQuery`); `headers` are the headers to sign, in any order and
/// case, and must include `host`.
@pure
pub func canonicalRequest(
  method: in String,
  canonicalUri: in String,
  canonicalQueryString: in String,
  headers: in List[Pair],
  payloadHash: in String,
): String {
  val sorted = canonicalHeaderPairs(headers)
  val sb = Str.StringBuilder.new()
  sb.append(method)
  sb.append("\n")
  sb.append(canonicalUri)
  sb.append("\n")
  sb.append(canonicalQueryString)
  sb.append("\n")
  var i = 0
  while i < sorted.count {
    sb.append(sorted[i].name)
    sb.append(":")
    sb.append(sorted[i].value)
    sb.append("\n")
    i = i + 1
  }
  sb.append("\n")
  sb.append(signedHeaderNames(headers))
  sb.append("\n")
  sb.append(payloadHash)
  sb.toString()
}

/// Lowercase hex SHA-256 of `bytes`: the payload hash of a request body.
@pure
pub func sha256Hex(bytes: in slice[Byte]): String {
  Str.toLower(Hash.sha256OfBytes(bytes))
}

/// `<yyyymmdd>/<region>/<service>/aws4_request` for the day of `amzDate`
/// (`yyyymmddThhmmssZ`).
@pure
pub func credentialScope(amzDate: in String, region: in String, service: in String): String
  requires: amzDate.length == 16
{
  amzDate.substring(0, 8) + "/" + region + "/" + service + "/aws4_request"
}

/// The string to sign (task 2).
@pure
pub func stringToSign(amzDate: in String, credScope: in String, canonicalRequestText: in String): String {
  ALGORITHM + "\n" + amzDate + "\n" + credScope + "\n" + sha256Hex(Enc.encodeUtf8(canonicalRequestText))
}

func hmac(key: in slice[Byte], message: in String): slice[Byte] {
  Hash.hmacSha256(key, Enc.encodeUtf8(message))
}

/// The lowercase hex signature of `toSign` (task 3) under the key derived
/// from the secret, the day, the region and the service.
@pure
pub func signatureOf(
  secretAccessKey: in String,
  amzDate: in String,
  region: in String,
  service: in String,
  toSign: in String,
): String
  requires: amzDate.length == 16
{
  val kDate = hmac(Enc.encodeUtf8("AWS4" + secretAccessKey), amzDate.substring(0, 8))
  val kRegion = hmac(kDate, region)
  val kService = hmac(kRegion, service)
  val kSigning = hmac(kService, "aws4_request")
  Str.toLower(Enc.encodeHex(hmac(kSigning, toSign)))
}

/// Sign a request whose signature travels in the `Authorization` header.
/// `headers` are exactly the headers that are signed and must be sent as
/// given: `host`, `x-amz-date`, `x-amz-content-sha256`, and
/// `x-amz-security-token` for temporary credentials, plus any other header
/// the caller wants covered.
@pure
pub func signRequest(
  method: in String,
  canonicalUri: in String,
  query: in List[Pair],
  headers: in List[Pair],
  payloadHash: in String,
  creds: in Credentials,
  region: in String,
  service: in String,
  amzDate: in String,
): Signed
  requires: amzDate.length == 16
{
  val cr = canonicalRequest(method, canonicalUri, canonicalQuery(query), headers, payloadHash)
  val credScope = credentialScope(amzDate, region, service)
  val sts = stringToSign(amzDate, credScope, cr)
  val sig = signatureOf(creds.secretAccessKey, amzDate, region, service, sts)
  val names = signedHeaderNames(headers)
  Signed(
    canonicalRequest = cr,
    stringToSign = sts,
    signedHeaders = names,
    signature = sig,
    authorization = ALGORITHM + " Credential=" + creds.accessKeyId + "/" + credScope + ", SignedHeaders=" + names +
      ", Signature=" +
      sig
  )
}

/// The query string of a presigned request, signature included: the
/// `X-Amz-*` authentication parameters, any `extraQuery` parameters, and
/// `X-Amz-Signature` last. Only `host` is signed and the payload is
/// `UNSIGNED-PAYLOAD`, so the holder of the URL needs nothing but the URL.
/// `host` is the authority the URL will be requested from.
@pure
pub func presignQuery(
  method: in String,
  host: in String,
  canonicalUri: in String,
  extraQuery: in List[Pair],
  creds: in Credentials,
  region: in String,
  service: in String,
  amzDate: in String,
  expiresInSeconds: in Int,
): String
  requires: amzDate.length == 16
  requires: expiresInSeconds >= 1 and expiresInSeconds <= 604800
{
  val credScope = credentialScope(amzDate, region, service)
  val query: List[Pair] = newList()
  var i = 0
  while i < extraQuery.count {
    query.add(extraQuery[i])
    i = i + 1
  }
  query.add(Pair(name = "X-Amz-Algorithm", value = ALGORITHM))
  query.add(Pair(name = "X-Amz-Credential", value = creds.accessKeyId + "/" + credScope))
  query.add(Pair(name = "X-Amz-Date", value = amzDate))
  query.add(Pair(name = "X-Amz-Expires", value = expiresInSeconds.toString()))
  query.add(Pair(name = "X-Amz-SignedHeaders", value = "host"))
  if creds.sessionToken.length > 0 {
    query.add(Pair(name = "X-Amz-Security-Token", value = creds.sessionToken))
  }
  val headers: List[Pair] = newList()
  headers.add(Pair(name = "host", value = host))
  val canonicalQueryString = canonicalQuery(query)
  val cr = canonicalRequest(method, canonicalUri, canonicalQueryString, headers, UNSIGNED_PAYLOAD)
  val sts = stringToSign(amzDate, credScope, cr)
  canonicalQueryString + "&X-Amz-Signature=" + signatureOf(creds.secretAccessKey, amzDate, region, service, sts)
}
