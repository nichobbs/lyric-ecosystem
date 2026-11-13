//! tests/wire_tests.l - `Storage.AzureBlob.Wire`: the Blob service's XML
//! documents, ETags and dates.
@test_module
package Storage.AzureBlob.WireTests

import Std.Core
import Std.Collections
import Std.Testing
import Storage.AzureBlob.Wire

// A List Blobs response in the shape the Blob service documents, with a
// second page to come.
val LIST_XML: String = "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n" + "<EnumerationResults ServiceEndpoint=\"https://myaccount.blob.core.windows.net/\" ContainerName=\"mycontainer\">\n" + "  <Prefix>reports/</Prefix>\n" + "  <MaxResults>2</MaxResults>\n" + "  <Blobs>\n" + "    <Blob>\n" + "      <Name>reports/q3 &amp; q4.pdf</Name>\n" + "      <Properties>\n" + "        <Creation-Time>Wed, 09 Sep 2009 09:20:02 GMT</Creation-Time>\n" + "        <Last-Modified>Wed, 09 Sep 2009 09:20:03 GMT</Last-Modified>\n" + "        <Etag>0x8CBFF45D8A29A19</Etag>\n" + "        <Content-Length>4192256</Content-Length>\n" + "        <Content-Type>application/pdf</Content-Type>\n" + "        <Content-Encoding />\n" + "        <Content-MD5 />\n" + "        <BlobType>BlockBlob</BlobType>\n" + "        <AccessTier>Cool</AccessTier>\n" + "        <LeaseStatus>unlocked</LeaseStatus>\n" + "      </Properties>\n" + "    </Blob>\n" + "    <Blob>\n" + "      <Name Encoded=\"true\">reports/odd%EF%BF%BEname%2Bplus</Name>\n" + "      <Properties>\n" + "        <Last-Modified>Thu, 12 Nov 2026 10:15:00 GMT</Last-Modified>\n" + "        <Etag>\"0x8DCF0A1B2C3D4E5\"</Etag>\n" + "        <Content-Length>0</Content-Length>\n" + "        <Content-Type>text/plain</Content-Type>\n" + "        <BlobType>BlockBlob</BlobType>\n" + "      </Properties>\n" + "    </Blob>\n" + "    <BlobPrefix><Name>reports/archive/</Name></BlobPrefix>\n" + "  </Blobs>\n" + "  <NextMarker>2!92!MDAwMDIzIXJlcG9ydHMvcTQucGRmITAwMDAyOCE5OTk5LTEyLTMxVDIzOjU5OjU5Ljk5OTk5OTlaIQ--</NextMarker>\n" + "</EnumerationResults>"

test "a List Blobs page: names, sizes, dates, ETags, tiers and the next marker" {
  match parseListPage(LIST_XML) {
    case Err(message) -> panic("parseListPage failed: " + message)
    case Ok(page) -> {
      assertEqualInt(page.blobs.count, 2, "two Blob elements; a BlobPrefix is not a blob")
      assertEqual(
        page.nextMarker,
        "2!92!MDAwMDIzIXJlcG9ydHMvcTQucGRmITAwMDAyOCE5OTk5LTEyLTMxVDIzOjU5OjU5Ljk5OTk5OTlaIQ--",
        "NextMarker, opaque"
      )
      assertEqual(page.blobs[0].name, "reports/q3 & q4.pdf", "XML entities in a name are decoded")
      assertEqualLong(page.blobs[0].size, 4192256i64, "first size")
      assertEqual(page.blobs[0].lastModified, "2009-09-09T09:20:03Z", "Last-Modified as ISO 8601")
      assertEqual(page.blobs[0].etag, "0x8CBFF45D8A29A19", "an unquoted ETag")
      assertEqual(page.blobs[0].contentType, "application/pdf", "content type")
      assertEqual(page.blobs[0].accessTier, "Cool", "access tier")
      assertEqual(page.blobs[1].name, "reports/odd￾name+plus", "an Encoded name is percent-decoded")
      assertEqualLong(page.blobs[1].size, 0i64, "an empty blob")
      assertEqual(page.blobs[1].etag, "0x8DCF0A1B2C3D4E5", "a quoted ETag is unquoted")
      assertEqual(page.blobs[1].accessTier, "", "no AccessTier element")
    }
  }
}

test "the last page has an empty NextMarker; an empty container has no blobs" {
  val last = "<EnumerationResults ContainerName=\"c\"><Blobs><Blob><Name>only</Name><Properties>" +
    "<Last-Modified>not a date</Last-Modified><Etag>e</Etag><Content-Length>3</Content-Length></Properties></Blob>" +
    "</Blobs><NextMarker /></EnumerationResults>"
  match parseListPage(last) {
    case Err(message) -> panic("parseListPage failed: " + message)
    case Ok(page) -> {
      assertEqualInt(page.blobs.count, 1, "one blob")
      assertEqual(page.nextMarker, "", "no further page")
      assertEqual(page.blobs[0].lastModified, "not a date", "a date in another form is passed through")
    }
  }
  // The service sends a byte order mark before the declaration.
  val empty = "\u{feff}<?xml version=\"1.0\" encoding=\"utf-8\"?><EnumerationResults ContainerName=\"c\"><Blobs />" +
    "<NextMarker /></EnumerationResults>"
  match parseListPage(empty) {
    case Err(message) -> panic("parseListPage failed: " + message)
    case Ok(page) -> {
      assertEqualInt(page.blobs.count, 0, "no blobs")
      assertEqual(page.nextMarker, "", "no further page")
    }
  }
}

func listError(xml: in String): String {
  match parseListPage(xml) {
    case Ok(_) -> "ok"
    case Err(message) -> message
  }
}

test "a listing that is not an EnumerationResults document is an error" {
  assertTrue(listError("<html>nope").startsWith("listing is not XML"), "not XML")
  assertEqual(
    listError("<Error><Code>X</Code></Error>"),
    "expected an EnumerationResults document, got <Error>",
    "another document"
  )
  assertTrue(
    listError("<EnumerationResults><Blobs><Blob><Name>a</Name><Properties><Content-Length>big</Content-Length></Properties></Blob></Blobs></EnumerationResults>").contains(
      "Content-Length"
    ),
    "a size that is not a number"
  )
  assertTrue(
    listError("<EnumerationResults><Blobs><Blob><Name Encoded=\"true\">%ZZ</Name><Properties><Content-Length>1</Content-Length></Properties></Blob></Blobs></EnumerationResults>").contains(
      "percent-encoded"
    ),
    "a name with a malformed escape"
  )
  assertTrue(
    listError("<EnumerationResults><Blobs><Blob><Name>a</Name></Blob></Blobs></EnumerationResults>").contains(
      "Properties"
    ),
    "a blob without properties"
  )
}

test "an Error document gives its code and the first line of its message" {
  val xml = "\u{feff}<?xml version=\"1.0\" encoding=\"utf-8\"?><Error><Code>BlobNotFound</Code>" +
    "<Message>The specified blob does not exist.\nRequestId:4b2d2f1a-801e-0045-0b1c-35a0a0000000\n" +
    "Time:2026-11-12T10:15:00.0000000Z</Message></Error>"
  match parseFault(xml) {
    case None -> panic("an Error document was not recognised")
    case Some(fault) -> {
      assertEqual(fault.code, "BlobNotFound", "Code")
      assertEqual(fault.message, "The specified blob does not exist.", "the first line of Message")
    }
  }
  assertTrue(isNone(parseFault("")), "an empty body")
  assertTrue(isNone(parseFault("<html><body>502 Bad Gateway</body></html>")), "an HTML page")
  assertTrue(isNone(parseFault("<Error><Message>no code</Message></Error>")), "an Error without a Code")
  assertTrue(isNone(parseFault("<EnumerationResults />")), "another document")
}

test "ETags are unquoted" {
  assertEqual(unquoteEtag("\"0x8DCF0A1B2C3D4E5\""), "0x8DCF0A1B2C3D4E5", "quoted")
  assertEqual(unquoteEtag("0x8DCF0A1B2C3D4E5"), "0x8DCF0A1B2C3D4E5", "bare")
  assertEqual(unquoteEtag(""), "", "empty")
}

func isoOf(httpDate: in String): String {
  match httpDateToIso(httpDate) {
    case Some(iso) -> iso
    case None -> "(none)"
  }
}

func epochOf(httpDate: in String): Long {
  match httpDateToEpochSeconds(httpDate) {
    case Some(n) -> n
    case None -> 0i64 - 1i64
  }
}

// Expected values from Python's `email.utils.formatdate(t, usegmt=True)` and
// `time.gmtime(t)`.
test "RFC 1123 dates, ISO 8601 instants and epoch seconds convert both ways" {
  assertEqual(httpDateOf(0i64), "Thu, 01 Jan 1970 00:00:00 GMT", "the epoch")
  assertEqual(httpDateOf(951782400i64), "Tue, 29 Feb 2000 00:00:00 GMT", "a leap day in a century year")
  assertEqual(httpDateOf(1794478500i64), "Thu, 12 Nov 2026 10:15:00 GMT", "a day in 2026")
  assertEqual(httpDateOf(1709251199i64), "Thu, 29 Feb 2024 23:59:59 GMT", "the last second of a leap day")
  assertEqual(httpDateOf(4102444799i64), "Thu, 31 Dec 2099 23:59:59 GMT", "the last second of 2099")
  assertEqual(isoSecondsOf(0i64), "1970-01-01T00:00:00Z", "the epoch as ISO 8601")
  assertEqual(isoSecondsOf(951782400i64), "2000-02-29T00:00:00Z", "a leap day as ISO 8601")
  assertEqual(isoSecondsOf(1794478500i64), "2026-11-12T10:15:00Z", "a day in 2026 as ISO 8601")
  assertEqual(isoSecondsOf(4102444799i64), "2099-12-31T23:59:59Z", "the last second of 2099 as ISO 8601")
  assertEqual(isoOf("Thu, 12 Nov 2026 10:15:00 GMT"), "2026-11-12T10:15:00Z", "RFC 1123 to ISO 8601")
  assertEqualLong(epochOf("Thu, 12 Nov 2026 10:15:00 GMT"), 1794478500i64, "RFC 1123 to epoch seconds")
  assertEqualLong(epochOf("Tue, 29 Feb 2000 00:00:00 GMT"), 951782400i64, "a leap day to epoch seconds")
  assertEqualLong(epochOf("Thu, 01 Jan 1970 00:00:00 GMT"), 0i64, "the epoch to epoch seconds")
  assertEqual(isoOf("2026-11-12T10:15:00Z"), "(none)", "an ISO date is not an RFC 1123 date")
  assertEqual(isoOf("Thu, 12 Foo 2026 10:15:00 GMT"), "(none)", "an unknown month")
  assertEqual(isoOf("Thu, 12 Nov 2026 10:15:00 UTC"), "(none)", "not GMT")
  assertEqual(isoOf(""), "(none)", "empty")
  match isoSecondsToEpoch("2026-11-12T10:15:00Z") {
    case Some(n) -> assertEqualLong(n, 1794478500i64, "ISO 8601 to epoch seconds")
    case None -> panic("an ISO 8601 instant was not parsed")
  }
  assertTrue(isNone(isoSecondsToEpoch("2026-11-12T10:15:00.000Z")), "fractional seconds are another form")
  assertTrue(isNone(isoSecondsToEpoch("2026-11-12 10:15:00Z")), "a space for the T")
  assertTrue(isNone(isoSecondsToEpoch("")), "empty")
}

test "percent-decoding keeps '+' and rejects malformed input" {
  match percentDecode("a%20b+c%2Fd%C3%A9") {
    case Some(text) -> assertEqual(text, "a b+c/dé", "escapes are bytes of UTF-8, '+' is itself")
    case None -> panic("a valid value was rejected")
  }
  assertTrue(isNone(percentDecode("%")), "a lone percent sign")
  assertTrue(isNone(percentDecode("%4")), "a short escape")
  assertTrue(isNone(percentDecode("%GG")), "not hex")
  assertTrue(isNone(percentDecode("%FF")), "not UTF-8")
}
