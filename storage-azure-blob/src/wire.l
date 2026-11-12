//! Storage.AzureBlob.Wire - the Blob REST API's formats.
//!
//! Parses what the Blob service sends back: the `EnumerationResults` document
//! of List Blobs, the `Error` document of a failed request, quoted ETags and
//! RFC 1123 dates; and formats the dates a request carries (`x-ms-date`, the
//! `se` of a SAS). Pure functions, no I/O.
@runtime_checked
package Storage.AzureBlob.Wire

import Std.Core
import Std.Collections
import Std.Encoding as Enc
import Std.Parse
import Std.String as Str
import Std.Xml

/// The `Code` and `Message` of a Blob service `Error` document.
pub record BlobFault {
  code: String
  message: String
}

/// One `Blob` element of a listing.
pub record ListedBlob {
  name: String
  size: Long
  // ISO 8601 UTC, or the service's text when it is not an RFC 1123 date.
  lastModified: String
  etag: String
  contentType: String
  // `Hot`, `Cool`, `Cold`, `Archive`, or empty when the service reports none.
  accessTier: String
}

/// A page of List Blobs. `nextMarker` is empty on the last page.
pub record ListPage {
  blobs: List[ListedBlob]
  nextMarker: String
}

func childNode(node: in XmlNode, tag: in String): Option[XmlNode] {
  val children = elementChildren(node)
  var i = 0
  while i < children.count {
    match elementTag(children[i]) {
      case Some(t) -> {
        if t == tag {
          return Some(value = children[i])
        }
      }
      case None -> ()
    }
    i = i + 1
  }
  None
}

func childText(node: in XmlNode, tag: in String): String {
  match childNode(node, tag) {
    case Some(child) -> textContent(child)
    case None -> ""
  }
}

// The service's message is followed by `RequestId:` and `Time:` lines; only
// the first line says what went wrong.
func firstLine(text: in String): String {
  val t = Str.trim(text)
  val nl = Str.indexOfRaw(t, "\n")
  if nl >= 0 {
    Str.trim(t.substring(0, nl))
  } else {
    t
  }
}

/// The fault in a Blob service error body, or `None` when `body` is not an
/// `Error` document (an empty body, as HEAD returns, or a proxy's HTML page).
/// `message` is the first line of the service's `Message`.
@pure
pub func parseFault(body: in String): Option[BlobFault] {
  // A UTF-8 byte order mark, which the service sends, is not part of the XML.
  val text = if body.length > 0 and Str.codeUnitAt(body, 0) == 65279 { body.substring(1) } else { body }
  if Str.trim(text).length == 0 {
    return None
  }
  match parseXml(text) {
    case Err(_) -> None
    case Ok(doc) -> {
      val root = documentRoot(doc)
      match elementTag(root) {
        case Some(tag) -> {
          if tag != "Error" {
            return None
          }
          val code = Str.trim(childText(root, "Code"))
          if code.length == 0 {
            return None
          }
          Some(value = BlobFault(code = code, message = firstLine(childText(root, "Message"))))
        }
        case None -> None
      }
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

/// Decode a percent-encoded value: `%XX` is a byte, and the bytes are UTF-8.
/// A `+` stays a `+`. `None` for a malformed escape or bytes that are not
/// UTF-8.
@pure
pub func percentDecode(text: in String): Option[String] {
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
    } else {
      acc.add(raw[i])
      i = i + 1
    }
  }
  Enc.tryDecodeUtf8(acc.toArray())
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

func digitsValue(text: in String): Int {
  var n = 0
  var i = 0
  while i < text.length {
    n = n * 10 + (Str.codeUnitAt(text, i) - 48)
    i = i + 1
  }
  n
}

// Days since 1970-01-01 of a proleptic Gregorian date.
func daysFromCivil(year: in Int, month: in Int, day: in Int): Long {
  val y = if month <= 2 { year - 1 } else { year }
  val era = y / 400
  val yoe = y - era * 400
  val mp = if month > 2 { month - 3 } else { month + 9 }
  val doy = (153 * mp + 2) / 5 + day - 1
  val doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
  (era * 146097 + doe - 719468).toLong()
}

// A UTC date and time; `weekday` 0 is Sunday.
record Civil {
  year: Int
  month: Int
  day: Int
  hour: Int
  minute: Int
  second: Int
  weekday: Int
}

// The fields of an RFC 1123 date, validated. The weekday is not checked and
// is reported as 0.
func httpDateFields(httpDate: in String): Option[Civil] {
  val parts = Str.split(Str.trim(httpDate), " ")
  if parts.length != 6 or parts[5] != "GMT" {
    return None
  }
  val month = monthNumber(parts[2])
  val clock = Str.split(parts[4], ":")
  if month == 0 or not isDigits(parts[1], 2) or not isDigits(parts[3], 4) or clock.length != 3 {
    return None
  }
  if not isDigits(clock[0], 2) or not isDigits(clock[1], 2) or not isDigits(clock[2], 2) {
    return None
  }
  Some(
    value = Civil(
      year = digitsValue(parts[3]),
      month = month,
      day = digitsValue(parts[1]),
      hour = digitsValue(clock[0]),
      minute = digitsValue(clock[1]),
      second = digitsValue(clock[2]),
      weekday = 0
    )
  )
}

func pad2(n: in Int): String {
  if n < 10 {
    "0" + n.toString()
  } else {
    n.toString()
  }
}

func isoOfCivil(f: in Civil): String {
  pad4(f.year) + "-" + pad2(f.month) + "-" + pad2(f.day) + "T" + pad2(f.hour) + ":" + pad2(f.minute) + ":" +
    pad2(f.second) +
    "Z"
}

func pad4(n: in Int): String {
  var text = n.toString()
  while text.length < 4 {
    text = "0" + text
  }
  text
}

/// An RFC 1123 date, the form the Blob service uses for `Last-Modified` and
/// expects in `x-ms-date` (`Wed, 12 Oct 2009 17:50:00 GMT`), as ISO 8601 UTC
/// (`2009-10-12T17:50:00Z`), the form `StorageMetadata.lastModified` carries.
/// `None` when `httpDate` is not in that form.
@pure
pub func httpDateToIso(httpDate: in String): Option[String] {
  match httpDateFields(httpDate) {
    case None -> None
    case Some(f) -> Some(value = isoOfCivil(f))
  }
}

/// Seconds since the Unix epoch of an RFC 1123 date; `None` when `httpDate`
/// is not in that form.
@pure
pub func httpDateToEpochSeconds(httpDate: in String): Option[Long] {
  match httpDateFields(httpDate) {
    case None -> None
    case Some(f) -> Some(
      value = daysFromCivil(f.year, f.month, f.day) * 86400i64 +
        (f.hour * 3600 + f.minute * 60 + f.second).toLong()
    )
  }
}

func civilFields(epochSeconds: in Long): Civil {
  val days = epochSeconds / 86400i64
  val secondOfDay = (epochSeconds - days * 86400i64).toInt()
  val z = days.toInt() + 719468
  val era = z / 146097
  val doe = z - era * 146097
  val yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365
  val doy = doe - (365 * yoe + yoe / 4 - yoe / 100)
  val mp = (5 * doy + 2) / 153
  val day = doy - (153 * mp + 2) / 5 + 1
  val month = if mp < 10 { mp + 3 } else { mp - 9 }
  val year = yoe + era * 400 + (if month <= 2 { 1 } else { 0 })
  Civil(
    year = year,
    month = month,
    day = day,
    hour = secondOfDay / 3600,
    minute = (secondOfDay % 3600) / 60,
    second = secondOfDay % 60,
    weekday = (days.toInt() + 4) % 7
  )
}

/// The RFC 1123 date of an instant given in seconds since the Unix epoch
/// (`Thu, 12 Nov 2026 10:15:00 GMT`): the `x-ms-date` of a request.
@pure
pub func httpDateOf(epochSeconds: in Long): String
  requires: epochSeconds >= 0i64
{
  val weekdays = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]
  val months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
  val f = civilFields(epochSeconds)
  weekdays[f.weekday] + ", " + pad2(f.day) + " " + months[f.month - 1] + " " + pad4(f.year) + " " + pad2(f.hour) + ":" +
    pad2(f.minute) +
    ":" +
    pad2(f.second) +
    " GMT"
}

/// ISO 8601 UTC to the second (`2026-11-12T10:15:00Z`) of an instant given in
/// seconds since the Unix epoch: the `se` of a SAS.
@pure
pub func isoSecondsOf(epochSeconds: in Long): String
  requires: epochSeconds >= 0i64
{
  isoOfCivil(civilFields(epochSeconds))
}

/// Seconds since the Unix epoch of an ISO 8601 UTC instant to the second
/// (`2026-11-12T10:15:00Z`); `None` for any other form.
@pure
pub func isoSecondsToEpoch(iso: in String): Option[Long] {
  if iso.length != 20 or iso.substring(4, 1) != "-" or iso.substring(7, 1) != "-" or iso.substring(10, 1) != "T" {
    return None
  }
  if iso.substring(13, 1) != ":" or iso.substring(16, 1) != ":" or iso.substring(19, 1) != "Z" {
    return None
  }
  val year = iso.substring(0, 4)
  val month = iso.substring(5, 2)
  val day = iso.substring(8, 2)
  val hour = iso.substring(11, 2)
  val minute = iso.substring(14, 2)
  val second = iso.substring(17, 2)
  if not isDigits(year, 4) or not isDigits(month, 2) or not isDigits(day, 2) or not isDigits(hour, 2) {
    return None
  }
  if not isDigits(minute, 2) or not isDigits(second, 2) {
    return None
  }
  Some(
    value = daysFromCivil(digitsValue(year), digitsValue(month), digitsValue(day)) * 86400i64 +
      (digitsValue(hour) * 3600 + digitsValue(minute) * 60 + digitsValue(second)).toLong()
  )
}

func isoOr(httpDate: in String): String {
  match httpDateToIso(httpDate) {
    case Some(iso) -> iso
    case None -> Str.trim(httpDate)
  }
}

func listedBlob(node: in XmlNode): Result[ListedBlob, String] {
  val nameNode = match childNode(node, "Name") {
    case Some(n) -> n
    case None -> return Err(error = "listing holds a Blob without a Name")
  }
  val rawName = textContent(nameNode)
  // Since version 2021-02-12 a name XML cannot carry is percent-encoded and
  // marked `Encoded="true"`.
  val encoded = match getAttribute(nameNode, "Encoded") { case Some(v) -> v == "true"; case None -> false }
  val name = if encoded {
    match percentDecode(rawName) {
      case Some(n) -> n
      case None -> return Err(error = "listing holds a name that is not valid percent-encoded UTF-8: " + rawName)
    }
  } else {
    rawName
  }
  match childNode(node, "Properties") {
    case None -> Err(error = "listing holds no Properties for blob '" + name + "'")
    case Some(props) -> {
      val sizeText = Str.trim(childText(props, "Content-Length"))
      val size = match parseOptLong(sizeText) {
        case Some(n) -> n
        case None -> return Err(error = "listing holds a Content-Length that is not a number: " + sizeText)
      }
      Ok(
        value = ListedBlob(
          name = name,
          size = size,
          lastModified = isoOr(childText(props, "Last-Modified")),
          etag = unquoteEtag(childText(props, "Etag")),
          contentType = Str.trim(childText(props, "Content-Type")),
          accessTier = Str.trim(childText(props, "AccessTier"))
        )
      )
    }
  }
}

/// Parse a List Blobs response body (`EnumerationResults`). Only `Blob`
/// elements are returned; a `BlobPrefix`, which a flat listing does not
/// produce, is ignored.
@pure
pub func parseListPage(body: in String): Result[ListPage, String] {
  val text = if body.length > 0 and Str.codeUnitAt(body, 0) == 65279 { body.substring(1) } else { body }
  match parseXml(text) {
    case Err(e) -> Err(error = "listing is not XML: " + XmlError.message(e))
    case Ok(doc) -> {
      val root = documentRoot(doc)
      val tag = match elementTag(root) { case Some(t) -> t; case None -> "" }
      if tag != "EnumerationResults" {
        return Err(error = "expected an EnumerationResults document, got <" + tag + ">")
      }
      val blobs: List[ListedBlob] = newList()
      match childNode(root, "Blobs") {
        case None -> ()
        case Some(container) -> {
          val children = elementChildren(container)
          var i = 0
          while i < children.count {
            val isBlob = match elementTag(children[i]) { case Some(t) -> t == "Blob"; case None -> false }
            if isBlob {
              match listedBlob(children[i]) {
                case Err(message) -> return Err(error = message)
                case Ok(b) -> blobs.add(b)
              }
            }
            i = i + 1
          }
        }
      }
      Ok(value = ListPage(blobs = blobs, nextMarker = Str.trim(childText(root, "NextMarker"))))
    }
  }
}
