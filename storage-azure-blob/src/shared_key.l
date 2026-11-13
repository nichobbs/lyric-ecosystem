//! Storage.AzureBlob.SharedKey - Shared Key authorization and service SAS
//! signing for the Azure Blob REST API.
//!
//! Pure functions over strings: nothing here reads the clock, the environment
//! or the network, so every step can be checked against the rules of the
//! Azure Storage documentation ("Authorize with Shared Key" and "Create a
//! service SAS"), which `tests/shared_key_tests.l` does.
//!
//! A request is authorized by `Authorization: SharedKey <account>:<signature>`
//! where the signature is the base64 HMAC-SHA256, under the base64-decoded
//! account key, of a string built from the verb, eleven standard headers, the
//! `x-ms-*` headers and the resource. A service SAS is the same HMAC over a
//! different string, carried in the query string of a URL.
@runtime_checked
package Storage.AzureBlob.SharedKey

import Std.Core
import Std.Collections
import Std.Encoding as Enc
import Std.Hash as Hash
import Std.String as Str

val API_VERSION: String = "2021-12-02"

/// The REST API version every request is sent with (`x-ms-version`) and every
/// SAS is signed for (`sv`): `2021-12-02`. The string-to-sign formats in this
/// module are the ones that version defines.
@pure
pub func apiVersion(): String {
  API_VERSION
}

/// A name and value: a header, or a query parameter.
pub record Pair {
  name: String
  value: String
}

func isUnreserved(b: in Int): Bool {
  (b >= 65 and b <= 90) or (b >= 97 and b <= 122) or (b >= 48 and b <= 57) or b == 45 or b == 46 or b == 95 or b == 126
}

/// Percent-encode `text`: every UTF-8 byte except `A-Z a-z 0-9 - . _ ~`
/// becomes `%XX` with uppercase hex digits. `/` is encoded too unless
/// `keepSlash`, which is how a blob name is encoded into a path.
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

// Ordinal comparison by UTF-16 code unit.
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

/// The query string of a URL: each name and value percent-encoded, in the
/// order given, joined as `name=value` with `&`.
@pure
pub func encodeQuery(params: in List[Pair]): String {
  val sb = Str.StringBuilder.new()
  var i = 0
  while i < params.count {
    if i > 0 {
      sb.append("&")
    }
    sb.append(uriEncode(params[i].name, false))
    sb.append("=")
    sb.append(uriEncode(params[i].value, false))
    i = i + 1
  }
  sb.toString()
}

// Trim, and collapse each run of spaces and tabs to one space, as the
// canonicalized headers require of a header value.
func canonicalHeaderValue(value: in String): String {
  val trimmed = Str.trim(value)
  val sb = Str.StringBuilder.new()
  var prevSpace = false
  var i = 0
  while i < trimmed.length {
    val c = Str.charAtOrReplacement(trimmed, i)
    if c == ' ' or c == '\t' {
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

/// The CanonicalizedHeaders string: every header of `headers` whose name
/// begins with `x-ms-`, name lowercased, sorted by name, each as
/// `name:value\n` with the value trimmed and its inner whitespace collapsed.
/// Headers that are not `x-ms-*` are ignored. A name must appear once.
@pure
pub func canonicalizedHeaders(headers: in List[Pair]): String {
  val ms: List[Pair] = newList()
  var i = 0
  while i < headers.count {
    val lower = Str.toLower(Str.trim(headers[i].name))
    if Str.startsWith(lower, "x-ms-") {
      ms.add(Pair(name = lower, value = canonicalHeaderValue(headers[i].value)))
    }
    i = i + 1
  }
  val sorted = sortedPairs(ms)
  val sb = Str.StringBuilder.new()
  var k = 0
  while k < sorted.count {
    sb.append(sorted[k].name)
    sb.append(":")
    sb.append(sorted[k].value)
    sb.append("\n")
    k = k + 1
  }
  sb.toString()
}

/// The CanonicalizedResource string: `/<account>` followed by the request's
/// encoded URI path, then every query parameter as `\n<name>:<value>` with the
/// name lowercased, the parameters sorted by name, and the value not
/// percent-encoded. The values of a repeated parameter are sorted and joined
/// with commas.
///
/// `encodedPath` is the path exactly as it is requested, so for an emulator
/// or other path-style endpoint, where the path itself begins with the
/// account name, the account appears twice.
@pure
pub func canonicalizedResource(account: in String, encodedPath: in String, query: in List[Pair]): String {
  val lowered: List[Pair] = newList()
  var i = 0
  while i < query.count {
    lowered.add(Pair(name = Str.toLower(query[i].name), value = query[i].value))
    i = i + 1
  }
  val sorted = sortedPairs(lowered)
  val sb = Str.StringBuilder.new()
  sb.append("/")
  sb.append(account)
  sb.append(encodedPath)
  var k = 0
  while k < sorted.count {
    if k > 0 and sorted[k].name == sorted[k - 1].name {
      sb.append(",")
    } else {
      sb.append("\n")
      sb.append(sorted[k].name)
      sb.append(":")
    }
    sb.append(sorted[k].value)
    k = k + 1
  }
  sb.toString()
}

// The value of the standard header `name` among `headers`, or "".
func standardHeader(headers: in List[Pair], name: in String): String {
  var i = 0
  while i < headers.count {
    if Str.toLower(Str.trim(headers[i].name)) == name {
      return Str.trim(headers[i].value)
    }
    i = i + 1
  }
  ""
}

/// The string a Shared Key signature is computed over, for the Blob service:
///
///   VERB, Content-Encoding, Content-Language, Content-Length, Content-MD5,
///   Content-Type, Date, If-Modified-Since, If-Match, If-None-Match,
///   If-Unmodified-Since, Range
///
/// each followed by a newline, then `canonicalizedHeaders` and
/// `canonicalizedResource`. `headers` are the request's headers in any order
/// and case: the standard ones are picked out by name, an absent one is the
/// empty string, and a `Content-Length` of `0` is the empty string too (as
/// every version since 2015-02-21 requires). `query` holds the unencoded
/// query parameters.
@pure
pub func stringToSign(
  method: in String,
  headers: in List[Pair],
  account: in String,
  encodedPath: in String,
  query: in List[Pair],
): String {
  val names = [
    "content-encoding",
    "content-language",
    "content-length",
    "content-md5",
    "content-type",
    "date",
    "if-modified-since",
    "if-match",
    "if-none-match",
    "if-unmodified-since",
    "range"
  ]
  val sb = Str.StringBuilder.new()
  sb.append(method)
  sb.append("\n")
  var i = 0
  while i < names.length {
    val value = standardHeader(headers, names[i])
    if not (names[i] == "content-length" and value == "0") {
      sb.append(value)
    }
    sb.append("\n")
    i = i + 1
  }
  sb.append(canonicalizedHeaders(headers))
  sb.append(canonicalizedResource(account, encodedPath, query))
  sb.toString()
}

/// The base64 HMAC-SHA256 of `toSign` (as UTF-8) under the account key, which
/// Azure issues base64-encoded. `None` when `accountKeyBase64` is not base64.
@pure
pub func signatureOf(accountKeyBase64: in String, toSign: in String): Option[String] {
  match Enc.tryDecodeBase64(accountKeyBase64) {
    case None -> None
    case Some(key) -> Some(value = Enc.encodeBase64(Hash.hmacSha256(key, Enc.encodeUtf8(toSign))))
  }
}

/// True for an account key Azure could have issued: base64 of at least one
/// byte.
@pure
pub func isAccountKey(accountKeyBase64: in String): Bool {
  if accountKeyBase64.length == 0 {
    return false
  }
  match Enc.tryDecodeBase64(accountKeyBase64) {
    case None -> false
    case Some(key) -> key.length > 0
  }
}

/// The `Authorization` header value for a signature.
@pure
pub func authorizationHeader(account: in String, signature: in String): String {
  "SharedKey " + account + ":" + signature
}

/// True when `permissions` is a non-empty selection of the service SAS
/// permission letters for a blob, in the order Azure requires them:
/// `r` read, `a` add, `c` create, `w` write, `d` delete, `x` delete version,
/// `y` permanent delete, `t` tags, `m` move, `e` execute, `i` immutability.
@pure
pub func isSasPermissions(permissions: in String): Bool {
  val order = "racwdxytmei"
  if permissions.length == 0 {
    return false
  }
  var at = 0
  var i = 0
  while i < permissions.length {
    val unit = Str.codeUnitAt(permissions, i)
    var found = false
    while at < order.length and not found {
      if Str.codeUnitAt(order, at) == unit {
        found = true
      }
      at = at + 1
    }
    if not found {
      return false
    }
    i = i + 1
  }
  true
}

/// The canonicalized resource of a service SAS for one blob:
/// `/blob/<account>/<container>/<blob>`, the blob name not percent-encoded.
/// The account appears once, whatever the endpoint's addressing style.
@pure
pub func sasCanonicalizedResource(account: in String, container: in String, blob: in String): String {
  "/blob/" + account + "/" + container + "/" + blob
}

/// The string a service SAS for one blob (`sr=b`) is signed over, in the
/// format of version 2020-12-06 and later:
///
///   signedPermissions, signedStart, signedExpiry, canonicalizedResource,
///   signedIdentifier, signedIP, signedProtocol, signedVersion,
///   signedResource, signedSnapshotTime, signedEncryptionScope, rscc, rscd,
///   rsce, rscl, rsct
///
/// joined by newlines. This module signs no stored access policy, IP range,
/// snapshot, encryption scope or response-header override, so those fields
/// are empty; `start` and `protocol` are empty when the SAS carries no `st`
/// or `spr`.
@pure
pub func sasStringToSign(
  permissions: in String,
  start: in String,
  expiry: in String,
  account: in String,
  container: in String,
  blob: in String,
  protocol: in String,
  version: in String,
): String {
  permissions + "\n" + start + "\n" + expiry + "\n" + sasCanonicalizedResource(account, container, blob) + "\n" + "\n" +
    "\n" +
    protocol +
    "\n" +
    version +
    "\n" +
    "b" +
    "\n" +
    "\n" +
    "\n" +
    "\n" +
    "\n" +
    "\n" +
    "\n"
}

/// The query string of a service SAS for one blob: `sv`, `sr=b`, `sp`, `se`,
/// `spr` when `protocol` is not empty, and `sig` last, each value
/// percent-encoded. `expiry` is ISO 8601 UTC to the second
/// (`2026-11-12T10:15:00Z`); `protocol` is `https`, `https,http` or empty.
/// `None` when the account key is not base64.
@pure
pub func sasQuery(
  permissions: in String,
  expiry: in String,
  account: in String,
  container: in String,
  blob: in String,
  protocol: in String,
  accountKeyBase64: in String,
): Option[String]
  requires: isSasPermissions(permissions)
{
  val toSign = sasStringToSign(permissions, "", expiry, account, container, blob, protocol, API_VERSION)
  match signatureOf(accountKeyBase64, toSign) {
    case None -> None
    case Some(signature) -> {
      val query: List[Pair] = newList()
      query.add(Pair(name = "sv", value = API_VERSION))
      query.add(Pair(name = "sr", value = "b"))
      query.add(Pair(name = "sp", value = permissions))
      query.add(Pair(name = "se", value = expiry))
      if protocol.length > 0 {
        query.add(Pair(name = "spr", value = protocol))
      }
      query.add(Pair(name = "sig", value = signature))
      Some(value = encodeQuery(query))
    }
  }
}
