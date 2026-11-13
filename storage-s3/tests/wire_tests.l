//! tests/wire_tests.l - `Storage.S3.Wire`: S3's XML documents, ETags, dates
//! and url-encoded listing keys.
@test_module
package Storage.S3.WireTests

import Std.Core
import Std.Collections
import Std.Testing
import Storage.S3.Wire

// The ListObjectsV2 response example of the Amazon S3 API reference, with a
// continuation token.
val LIST_XML: String = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" + "<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n" + "  <Name>bucket</Name>\n" + "  <Prefix></Prefix>\n" + "  <NextContinuationToken>1ueGcxLPRx1Tr/XYExHnhbYLgveDs2J/wm36Hy4vbOwM=</NextContinuationToken>\n" + "  <KeyCount>2</KeyCount>\n" + "  <MaxKeys>2</MaxKeys>\n" + "  <IsTruncated>true</IsTruncated>\n" + "  <Contents>\n" + "    <Key>happyface.jpg</Key>\n" + "    <LastModified>2014-11-21T19:40:05.000Z</LastModified>\n" + "    <ETag>&quot;70ee1738b6b21e2c8a43f3a5ab0eee71&quot;</ETag>\n" + "    <Size>11</Size>\n" + "    <StorageClass>STANDARD</StorageClass>\n" + "  </Contents>\n" + "  <Contents>\n" + "    <Key>reports/q3 &amp; q4.pdf</Key>\n" + "    <LastModified>2014-11-21T19:41:00.000Z</LastModified>\n" + "    <ETag>\"becf17f89c30367a9a44495d62ed521a-1\"</ETag>\n" + "    <Size>4192256</Size>\n" + "    <StorageClass>STANDARD_IA</StorageClass>\n" + "  </Contents>\n" + "</ListBucketResult>"

test "a ListObjectsV2 page: keys, sizes, dates, unquoted ETags and the continuation token" {
  match parseListPage(LIST_XML) {
    case Err(message) -> panic("parseListPage failed: " + message)
    case Ok(page) -> {
      assertEqualInt(page.objects.count, 2, "two Contents elements")
      assertTrue(page.isTruncated, "IsTruncated")
      assertEqual(page.nextToken, "1ueGcxLPRx1Tr/XYExHnhbYLgveDs2J/wm36Hy4vbOwM=", "NextContinuationToken")
      assertEqual(page.objects[0].key, "happyface.jpg", "first key")
      assertEqualLong(page.objects[0].size, 11i64, "first size")
      assertEqual(page.objects[0].lastModified, "2014-11-21T19:40:05.000Z", "first LastModified")
      assertEqual(page.objects[0].etag, "70ee1738b6b21e2c8a43f3a5ab0eee71", "an entity-quoted ETag is unquoted")
      assertEqual(page.objects[1].key, "reports/q3 & q4.pdf", "XML entities in a key are decoded")
      assertEqualLong(page.objects[1].size, 4192256i64, "second size")
      assertEqual(page.objects[1].etag, "becf17f89c30367a9a44495d62ed521a-1", "a multipart ETag")
    }
  }
}

test "an empty, final page" {
  val xml = "<ListBucketResult><Name>b</Name><KeyCount>0</KeyCount><IsTruncated>false</IsTruncated></ListBucketResult>"
  match parseListPage(xml) {
    case Err(message) -> panic("parseListPage failed: " + message)
    case Ok(page) -> {
      assertEqualInt(page.objects.count, 0, "no objects")
      assertFalse(page.isTruncated, "not truncated")
      assertEqual(page.nextToken, "", "no token")
    }
  }
}

test "url-encoded keys are decoded when the document says EncodingType url" {
  val xml = "<ListBucketResult><EncodingType>url</EncodingType><IsTruncated>false</IsTruncated>" +
    "<Contents><Key>a+b/caf%C3%A9%2B1/%E6%97%A5.txt</Key><LastModified>2026-01-02T03:04:05.000Z</LastModified>" +
    "<ETag>\"e\"</ETag><Size>1</Size></Contents></ListBucketResult>"
  match parseListPage(xml) {
    case Err(message) -> panic("parseListPage failed: " + message)
    case Ok(page) -> assertEqual(page.objects[0].key, "a b/café+1/日.txt", "'+' is a space, %XX are UTF-8 bytes")
  }
  val plain = "<ListBucketResult><IsTruncated>false</IsTruncated>" +
    "<Contents><Key>a+b%20c</Key><LastModified>x</LastModified><ETag>e</ETag><Size>1</Size></Contents></ListBucketResult>"
  match parseListPage(plain) {
    case Err(message) -> panic("parseListPage failed: " + message)
    case Ok(page) -> assertEqual(page.objects[0].key, "a+b%20c", "without EncodingType a key is taken literally")
  }
}

func listError(xml: in String): String {
  match parseListPage(xml) {
    case Err(message) -> message
    case Ok(_) -> "(Ok)"
  }
}

test "malformed listings are errors, not empty pages" {
  assertTrue(listError("not xml").startsWith("listing is not XML"), "not XML: " + listError("not xml"))
  assertEqual(
    listError("<Error><Code>AccessDenied</Code></Error>"),
    "expected a ListBucketResult document, got <Error>",
    "a different document"
  )
  assertEqual(
    listError("<ListBucketResult><IsTruncated>true</IsTruncated></ListBucketResult>"),
    "listing is truncated but carries no NextContinuationToken",
    "a truncated page must carry a token"
  )
  assertTrue(
    listError("<ListBucketResult><Contents><Key>k</Key><Size>big</Size></Contents></ListBucketResult>").startsWith(
      "listing holds a Size that is not a number"
    ),
    "a non-numeric Size"
  )
  assertTrue(
    listError("<ListBucketResult><EncodingType>url</EncodingType><Contents><Key>%ZZ</Key><Size>1</Size></Contents></ListBucketResult>").startsWith(
      "listing holds a key that is not valid url-encoded UTF-8"
    ),
    "a malformed escape in a key"
  )
}

test "an S3 Error document yields its Code and Message" {
  val xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>NoSuchKey</Code>" +
    "<Message>The resource you requested does not exist</Message><Resource>/mybucket/myfoto.jpg</Resource>" +
    "<RequestId>4442587FB7D0A2F9</RequestId></Error>"
  match parseFault(xml) {
    case None -> panic("expected a fault")
    case Some(f) -> {
      assertEqual(f.code, "NoSuchKey", "Code")
      assertEqual(f.message, "The resource you requested does not exist", "Message")
    }
  }
  assertTrue(isNone(parseFault("")), "an empty body (HEAD) is no fault")
  assertTrue(isNone(parseFault("<html><body>502 Bad Gateway</body></html>")), "a proxy's HTML page is no fault")
  assertTrue(isNone(parseFault("<Error><Message>no code</Message></Error>")), "an Error without a Code is no fault")
  assertTrue(isNone(parseFault("<Error><Code>Trunc")), "truncated XML is no fault")
}

test "ETags lose their quotes" {
  assertEqual(unquoteEtag("\"d41d8cd98f00b204e9800998ecf8427e\""), "d41d8cd98f00b204e9800998ecf8427e", "quoted")
  assertEqual(unquoteEtag("abc"), "abc", "already bare")
  assertEqual(unquoteEtag(" \"x\" "), "x", "surrounding whitespace")
  assertEqual(unquoteEtag("\""), "\"", "a lone quote is left alone")
  assertEqual(unquoteEtag(""), "", "empty")
}

func iso(httpDate: in String): String {
  match httpDateToIso(httpDate) {
    case Some(s) -> s
    case None -> "(none)"
  }
}

test "HTTP dates become ISO 8601 UTC" {
  assertEqual(iso("Wed, 12 Oct 2009 17:50:00 GMT"), "2009-10-12T17:50:00Z", "the S3 documentation's Last-Modified")
  assertEqual(iso("Sun, 01 Feb 2026 03:04:05 GMT"), "2026-02-01T03:04:05Z", "a single-digit month is padded")
  assertEqual(iso("Thu, 31 Dec 2026 23:59:59 GMT"), "2026-12-31T23:59:59Z", "December")
  assertEqual(iso("2026-02-01T03:04:05Z"), "(none)", "an ISO date is not an HTTP date")
  assertEqual(iso("Sun, 01 Foo 2026 03:04:05 GMT"), "(none)", "an unknown month")
  assertEqual(iso("Sun, 1 Feb 2026 03:04:05 GMT"), "(none)", "a one-digit day is not IMF-fixdate")
  assertEqual(iso("Sun, 01 Feb 2026 03:04:05 PST"), "(none)", "only GMT")
  assertEqual(iso(""), "(none)", "empty")
}

test "urlDecode rejects malformed input" {
  assertTrue(isNone(urlDecode("%")), "a lone percent")
  assertTrue(isNone(urlDecode("%4")), "a short escape")
  assertTrue(isNone(urlDecode("%FF")), "a byte that is not UTF-8")
  match urlDecode("plain-text_1.~") {
    case Some(s) -> assertEqual(s, "plain-text_1.~", "unreserved text is unchanged")
    case None -> panic("plain text must decode")
  }
}
