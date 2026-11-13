//! Storage.Gcs.Wire - the response formats of the Cloud Storage XML API.
//!
//! Parses what Cloud Storage sends back: the `ListBucketResult` document of a
//! `list-type=2` listing, the `Error` document of a failed request (with the
//! `Details` Cloud Storage adds, and the string to sign it reports for a
//! signature mismatch), quoted ETags, `Last-Modified` HTTP dates, and the
//! `encoding-type=url` form of listed object names. Pure functions, no I/O.
@runtime_checked
package Storage.Gcs.Wire

import Std.Core
import Std.Collections
import Std.Encoding as Enc
import Std.Parse
import Std.String as Str
import Std.Xml

/// The `Code`, `Message` and `Details` of a Cloud Storage `Error` document.
/// `details` is empty when the document has none.
pub record GcsFault {
  code: String
  message: String
  details: String
}

/// What Cloud Storage derived from a request whose signature it refused
/// (`SignatureDoesNotMatch`): its own string to sign and canonical request.
/// Comparing them with the client's shows which part of the request the two
/// sides read differently.
pub record SignatureMismatch {
  stringToSign: String
  canonicalRequest: String
}

/// One `Contents` element of a listing. `generation` is empty when the
/// server reports none.
pub record ListedObject {
  key: String
  size: Long
  lastModified: String
  etag: String
  generation: String
}

/// A page of a `list-type=2` listing. `nextToken` is empty unless
/// `isTruncated`.
pub record ListPage {
  objects: List[ListedObject]
  isTruncated: Bool
  nextToken: String
}

func childText(node: in XmlNode, tag: in String): String {
  val children = elementChildren(node)
  var i = 0
  while i < children.count {
    match elementTag(children[i]) {
      case Some(t) -> {
        if t == tag {
          return textContent(children[i])
        }
      }
      case None -> ()
    }
    i = i + 1
  }
  ""
}

// The root element of `body` when it is an `Error` document.
func errorRoot(body: in String): Option[XmlNode] {
  if Str.trim(body).length == 0 {
    return None
  }
  match parseXml(body) {
    case Err(_) -> None
    case Ok(doc) -> {
      val root = documentRoot(doc)
      match elementTag(root) {
        case Some(tag) -> if tag == "Error" { Some(value = root) } else { None }
        case None -> None
      }
    }
  }
}

/// The fault in a Cloud Storage error body, or `None` when `body` is not an
/// `Error` document (an empty body, as HEAD returns, or a proxy's HTML page).
@pure
pub func parseFault(body: in String): Option[GcsFault] {
  match errorRoot(body) {
    case None -> None
    case Some(root) -> {
      val code = Str.trim(childText(root, "Code"))
      if code.length == 0 {
        return None
      }
      Some(
        value = GcsFault(
          code = code,
          message = Str.trim(childText(root, "Message")),
          details = Str.trim(childText(root, "Details"))
        )
      )
    }
  }
}

/// The string to sign and canonical request Cloud Storage reports in a
/// `SignatureDoesNotMatch` error body, verbatim; `None` when `body` carries
/// neither.
@pure
pub func parseSignatureMismatch(body: in String): Option[SignatureMismatch] {
  match errorRoot(body) {
    case None -> None
    case Some(root) -> {
      val sts = childText(root, "StringToSign")
      val cr = childText(root, "CanonicalRequest")
      if sts.length == 0 and cr.length == 0 {
        return None
      }
      Some(value = SignatureMismatch(stringToSign = sts, canonicalRequest = cr))
    }
  }
}

/// An ETag without its surrounding double quotes.
@pure
pub func unquoteEtag(etag: in String): String {
  val t = Str.trim(etag)
  if t.length >= 2 and Str.startsWith(t, "\"") and Str.endsWith(t, "\"") {
    return t.substring(1, t.length - 2)
  }
  t
}

func hexValue(unit: in Int): Int {
  if unit >= 48 and unit <= 57 {
    return unit - 48
  }
  if unit >= 65 and unit <= 70 {
    return unit - 55
  }
  if unit >= 97 and unit <= 102 {
    return unit - 87
  }
  0 - 1
}

/// Decode a value Cloud Storage encoded for `encoding-type=url`: `%XX` is a
/// byte and `+` a space, and the bytes are UTF-8. `None` for a malformed
/// escape or bytes that are not UTF-8.
@pure
pub func urlDecode(text: in String): Option[String] {
  val raw = Enc.encodeUtf8(text)
  val acc: List[Byte] = newList()
  var i = 0
  while i < raw.length {
    val b = raw[i].toInt()
    if b == 37 {
      if i + 2 >= raw.length {
        return None
      }
      val hi = hexValue(raw[i + 1].toInt())
      val lo = hexValue(raw[i + 2].toInt())
      if hi < 0 or lo < 0 {
        return None
      }
      acc.add((hi * 16 + lo).toByte())
      i = i + 3
    } else if b == 43 {
      acc.add(32u8)
      i = i + 1
    } else {
      acc.add(raw[i])
      i = i + 1
    }
  }
  Enc.tryDecodeUtf8(acc.toArray())
}

func listedObject(node: in XmlNode, urlEncoded: in Bool): Result[ListedObject, String] {
  val rawKey = childText(node, "Key")
  val key = if urlEncoded {
    match urlDecode(rawKey) {
      case Some(k) -> k
      case None -> return Err(error = "listing holds an object name that is not valid url-encoded UTF-8: " + rawKey)
    }
  } else {
    rawKey
  }
  val sizeText = Str.trim(childText(node, "Size"))
  val size = match parseOptLong(sizeText) {
    case Some(n) -> n
    case None -> return Err(error = "listing holds a Size that is not a number: " + sizeText)
  }
  Ok(
    value = ListedObject(
      key = key,
      size = size,
      lastModified = Str.trim(childText(node, "LastModified")),
      etag = unquoteEtag(childText(node, "ETag")),
      generation = Str.trim(childText(node, "Generation"))
    )
  )
}

/// Parse a `list-type=2` listing. Object names are url-decoded when the
/// document says `EncodingType` is `url`.
@pure
pub func parseListPage(body: in String): Result[ListPage, String] {
  match parseXml(body) {
    case Err(e) -> Err(error = "listing is not XML: " + XmlError.message(e))
    case Ok(doc) -> {
      val root = documentRoot(doc)
      val tag = match elementTag(root) { case Some(t) -> t; case None -> "" }
      if tag != "ListBucketResult" {
        return Err(error = "expected a ListBucketResult document, got <" + tag + ">")
      }
      val urlEncoded = Str.trim(childText(root, "EncodingType")) == "url"
      val objects: List[ListedObject] = newList()
      val children = elementChildren(root)
      var i = 0
      while i < children.count {
        val isContents = match elementTag(children[i]) { case Some(t) -> t == "Contents"; case None -> false }
        if isContents {
          match listedObject(children[i], urlEncoded) {
            case Err(message) -> return Err(error = message)
            case Ok(o) -> objects.add(o)
          }
        }
        i = i + 1
      }
      val truncated = Str.toLower(Str.trim(childText(root, "IsTruncated"))) == "true"
      val token = Str.trim(childText(root, "NextContinuationToken"))
      if truncated and token.length == 0 {
        return Err(error = "listing is truncated but carries no NextContinuationToken")
      }
      Ok(value = ListPage(objects = objects, isTruncated = truncated, nextToken = if truncated { token } else { "" }))
    }
  }
}

func monthNumber(name: in String): Int {
  val months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
  var i = 0
  while i < months.length {
    if months[i] == name {
      return i + 1
    }
    i = i + 1
  }
  0
}

func isDigits(text: in String, count: in Int): Bool {
  if text.length != count {
    return false
  }
  var i = 0
  while i < text.length {
    val u = Str.codeUnitAt(text, i)
    if u < 48 or u > 57 {
      return false
    }
    i = i + 1
  }
  true
}

/// An HTTP date in the IMF-fixdate form Cloud Storage uses for
/// `Last-Modified` and `Date` (`Thu, 10 Aug 2017 18:50:44 GMT`) as ISO 8601
/// UTC (`2017-08-10T18:50:44Z`), the form `StorageMetadata.lastModified`
/// carries. `None` when `httpDate` is not in that form.
@pure
pub func httpDateToIso(httpDate: in String): Option[String] {
  val parts = Str.split(Str.trim(httpDate), " ")
  if parts.length != 6 or parts[5] != "GMT" {
    return None
  }
  val day = parts[1]
  val month = monthNumber(parts[2])
  val year = parts[3]
  val clock = Str.split(parts[4], ":")
  if month == 0 or not isDigits(day, 2) or not isDigits(year, 4) or clock.length != 3 {
    return None
  }
  if not isDigits(clock[0], 2) or not isDigits(clock[1], 2) or not isDigits(clock[2], 2) {
    return None
  }
  val monthText = if month < 10 { "0" + month.toString() } else { month.toString() }
  Some(value = year + "-" + monthText + "-" + day + "T" + clock[0] + ":" + clock[1] + ":" + clock[2] + "Z")
}
