//! tests/wire_tests.l - parsing what the Cloud Storage XML API sends back.
//!
//! The documents marked "as Cloud Storage sent it" are responses of
//! storage.googleapis.com itself, recorded on 2026-11-12 from public buckets
//! (`gcp-public-data-landsat`, `cloud-samples-data`) and from requests signed
//! with an HMAC key that does not exist; long values are shortened.
@test_module
package Storage.Gcs.WireTests

import Std.Core
import Std.Collections
import Std.Testing
import Storage.Gcs.Wire

// A `list-type=2&encoding-type=url&max-keys=2` listing in the form Cloud
// Storage sent it (single quotes in the declaration and the namespace,
// `Generation` and `MetaGeneration` in each entry, the token before the
// entries). The second entry's name is one `cloud-samples-data` listed, put
// here to have a name with `+` and a percent-escape in the same document.
val REAL_LISTING: String = "<?xml version='1.0' encoding='UTF-8'?>" + "<ListBucketResult xmlns='http://doc.s3.amazonaws.com/2006-03-01'><Name>gcp-public-data-landsat</Name>" + "<Prefix></Prefix><NextContinuationToken>CmhMQzA4LzAxLzAwMS8wMDIvTEMwOF9CMS5USUY=</NextContinuationToken>" + "<KeyCount>2</KeyCount><MaxKeys>2</MaxKeys><IsTruncated>true</IsTruncated><EncodingType>url</EncodingType>" + "<Contents><Key>LC08/01/001/002/LC08_L1GT_001002_20160817_20170322_01_T2/LC08_ANG.txt</Key>" + "<Generation>1502391044760665</Generation><MetaGeneration>1</MetaGeneration>" + "<LastModified>2017-08-10T18:50:44.626Z</LastModified><ETag>\"355b2e48e44378a932ae2b494013269c\"</ETag>" + "<Size>117255</Size></Contents>" + "<Contents><Key>adk-samples/data-science/bqml/The+ALTER+MODEL+statement+%C2%A0_%C2%A0+BigQuery.pdf</Key>" + "<Generation>1502391047022358</Generation><MetaGeneration>1</MetaGeneration>" + "<LastModified>2017-08-10T18:50:46.888Z</LastModified><ETag>\"1b0e1ef4b9ae3b5c1b79ef4f8b0a7d0c\"</ETag>" + "<Size>64273840</Size></Contents></ListBucketResult>"

// The last page of a listing, as Cloud Storage sent it.
val REAL_EMPTY_LISTING: String = "<?xml version='1.0' encoding='UTF-8'?>" + "<ListBucketResult xmlns='http://doc.s3.amazonaws.com/2006-03-01'><Name>gcp-public-data-landsat</Name>" + "<Prefix>zzzz-nothing</Prefix><KeyCount>0</KeyCount><MaxKeys>1</MaxKeys><IsTruncated>false</IsTruncated>" + "</ListBucketResult>"

// GET of a missing object, as Cloud Storage sent it.
val REAL_NO_SUCH_KEY: String = "<?xml version='1.0' encoding='UTF-8'?><Error><Code>NoSuchKey</Code>" + "<Message>The specified key does not exist.</Message>" + "<Details>No such object: gcp-public-data-landsat/no/such/key</Details></Error>"

// A missing bucket: no `Details` element.
val REAL_NO_SUCH_BUCKET: String = "<?xml version='1.0' encoding='UTF-8'?><Error><Code>NoSuchBucket</Code>" + "<Message>The specified bucket does not exist.</Message></Error>"

// A refused signature on a signed URL, as Cloud Storage sent it: the string
// to sign and the canonical request span lines and escape `&`.
val REAL_SIGNATURE_MISMATCH: String = "<?xml version='1.0' encoding='UTF-8'?><Error>" + "<Code>SignatureDoesNotMatch</Code><Message>Access denied.</Message>" + "<Details>The request signature we calculated does not match the signature you provided. " + "Check your Google secret key and signing method.</Details>" + "<StringToSign>GOOG4-HMAC-SHA256\n20261112T223933Z\n20261112/auto/storage/goog4_request\n" + "7b4fa6b7ddee4a234c66c9d24f0091e278c2576220f09180c803180e903e74d0</StringToSign>" + "<CanonicalRequest>GET\n/gcp-public-data-landsat/no/such/key\n" + "X-Goog-Algorithm=GOOG4-HMAC-SHA256&amp;X-Goog-Credential=GOOG1EXAMPLE%2F20261112%2Fauto%2Fstorage%2Fgoog4_request" + "&amp;X-Goog-Date=20261112T223933Z&amp;X-Goog-Expires=60&amp;X-Goog-SignedHeaders=host\n" + "host:storage.googleapis.com\n\nhost\nUNSIGNED-PAYLOAD</CanonicalRequest></Error>"

test "a real list-type=2 page: url-encoded names, sizes, ETags, generations, the token" {
  match parseListPage(REAL_LISTING) {
    case Err(message) -> panic(message)
    case Ok(page) -> {
      assertEqualInt(page.objects.count, 2, "two entries")
      assertTrue(page.isTruncated, "truncated")
      assertEqual(page.nextToken, "CmhMQzA4LzAxLzAwMS8wMDIvTEMwOF9CMS5USUY=", "the continuation token, verbatim")
      val first = page.objects[0]
      assertEqual(first.key, "LC08/01/001/002/LC08_L1GT_001002_20160817_20170322_01_T2/LC08_ANG.txt", "name")
      assertTrue(first.size == 117255i64, "size")
      assertEqual(first.lastModified, "2017-08-10T18:50:44.626Z", "LastModified as sent")
      assertEqual(first.etag, "355b2e48e44378a932ae2b494013269c", "the ETag without quotes")
      assertEqual(first.generation, "1502391044760665", "generation")
      assertEqual(
        page.objects[1].key,
        "adk-samples/data-science/bqml/The ALTER MODEL statement \u{00a0}_\u{00a0} BigQuery.pdf",
        "'+' is a space and %C2%A0 a no-break space"
      )
      assertTrue(page.objects[1].size == 64273840i64, "a larger size")
    }
  }
}

test "the last page of a listing is not truncated and has no token" {
  match parseListPage(REAL_EMPTY_LISTING) {
    case Err(message) -> panic(message)
    case Ok(page) -> {
      assertEqualInt(page.objects.count, 0, "no entries")
      assertFalse(page.isTruncated, "not truncated")
      assertEqual(page.nextToken, "", "no token")
    }
  }
}

test "a listing without EncodingType is taken literally; odd documents are errors" {
  val plain = "<ListBucketResult><IsTruncated>False</IsTruncated><Contents><Key>a+b%20c</Key><Size>3</Size>" +
    "<ETag>\"e\"</ETag><LastModified>2026-11-12T10:15:00.000Z</LastModified></Contents></ListBucketResult>"
  match parseListPage(plain) {
    case Err(message) -> panic(message)
    case Ok(page) -> {
      assertEqual(page.objects[0].key, "a+b%20c", "not decoded without EncodingType=url")
      assertEqual(page.objects[0].generation, "", "no generation reported")
    }
  }
  val capital = "<ListBucketResult><IsTruncated>True</IsTruncated><NextContinuationToken>t</NextContinuationToken>" +
    "</ListBucketResult>"
  match parseListPage(capital) {
    case Err(message) -> panic(message)
    case Ok(page) -> assertTrue(page.isTruncated, "IsTruncated is read in either capitalisation")
  }
  assertTrue(isErr(parseListPage("not xml at all")), "not XML")
  assertTrue(isErr(parseListPage("<ListAllMyBucketsResult/>")), "another document")
  assertTrue(
    isErr(parseListPage("<ListBucketResult><IsTruncated>true</IsTruncated></ListBucketResult>")),
    "truncated without a token"
  )
  assertTrue(
    isErr(parseListPage("<ListBucketResult><Contents><Key>k</Key><Size>big</Size></Contents></ListBucketResult>")),
    "a size that is not a number"
  )
  assertTrue(
    isErr(
      parseListPage(
        "<ListBucketResult><EncodingType>url</EncodingType><Contents><Key>%ZZ</Key><Size>1</Size></Contents>" +
          "</ListBucketResult>"
      )
    ),
    "a malformed escape in a url-encoded name"
  )
}

test "real Error documents: code, message and details" {
  match parseFault(REAL_NO_SUCH_KEY) {
    case None -> panic("NoSuchKey was not parsed")
    case Some(f) -> {
      assertEqual(f.code, "NoSuchKey", "code")
      assertEqual(f.message, "The specified key does not exist.", "message")
      assertEqual(f.details, "No such object: gcp-public-data-landsat/no/such/key", "details")
    }
  }
  match parseFault(REAL_NO_SUCH_BUCKET) {
    case None -> panic("NoSuchBucket was not parsed")
    case Some(f) -> {
      assertEqual(f.code, "NoSuchBucket", "code")
      assertEqual(f.details, "", "no details")
    }
  }
  assertTrue(isNone(parseFault("")), "an empty body, as HEAD returns")
  assertTrue(isNone(parseFault("   \n")), "whitespace")
  assertTrue(isNone(parseFault("<html><body>502 Bad Gateway</body></html>")), "a proxy's error page")
  assertTrue(isNone(parseFault("<Error><Message>no code</Message></Error>")), "an Error without a Code")
  assertTrue(isNone(parseFault("{\"error\":{\"code\":404}}")), "a JSON API error is not an XML API fault")
}

test "a refused signature reports the service's string to sign and canonical request" {
  match parseFault(REAL_SIGNATURE_MISMATCH) {
    case None -> panic("SignatureDoesNotMatch was not parsed")
    case Some(f) -> {
      assertEqual(f.code, "SignatureDoesNotMatch", "code")
      assertEqual(f.message, "Access denied.", "message")
    }
  }
  match parseSignatureMismatch(REAL_SIGNATURE_MISMATCH) {
    case None -> panic("the mismatch was not parsed")
    case Some(m) -> {
      assertEqual(
        m.stringToSign,
        "GOOG4-HMAC-SHA256\n20261112T223933Z\n20261112/auto/storage/goog4_request\n" +
          "7b4fa6b7ddee4a234c66c9d24f0091e278c2576220f09180c803180e903e74d0",
        "the string to sign, line breaks kept"
      )
      assertEqual(
        m.canonicalRequest,
        "GET\n/gcp-public-data-landsat/no/such/key\n" +
          "X-Goog-Algorithm=GOOG4-HMAC-SHA256&X-Goog-Credential=GOOG1EXAMPLE%2F20261112%2Fauto%2Fstorage%2Fgoog4_request" +
          "&X-Goog-Date=20261112T223933Z&X-Goog-Expires=60&X-Goog-SignedHeaders=host\n" +
          "host:storage.googleapis.com\n\nhost\nUNSIGNED-PAYLOAD",
        "the canonical request, '&amp;' unescaped, the blank line kept"
      )
    }
  }
  assertTrue(isNone(parseSignatureMismatch(REAL_NO_SUCH_KEY)), "another error has neither")
  assertTrue(isNone(parseSignatureMismatch("")), "an empty body")
}

test "ETags are unquoted" {
  assertEqual(unquoteEtag("\"355b2e48e44378a932ae2b494013269c\""), "355b2e48e44378a932ae2b494013269c", "quoted")
  assertEqual(unquoteEtag("abc"), "abc", "already bare")
  assertEqual(unquoteEtag(" \"x\" "), "x", "surrounding space")
  assertEqual(unquoteEtag("\""), "\"", "a lone quote is left alone")
  assertEqual(unquoteEtag(""), "", "empty")
}

test "url-encoded names decode to UTF-8, with '+' as a space" {
  assertEqual(unwrapOr(urlDecode("a+b%20c%2Bd"), "?"), "a b c+d", "'+', %20 and %2B")
  assertEqual(unwrapOr(urlDecode("caf%C3%A9/%E6%97%A5%E6%9C%AC"), "?"), "café/日本", "UTF-8 sequences")
  assertEqual(unwrapOr(urlDecode("plain/key.txt"), "?"), "plain/key.txt", "nothing to decode")
  assertEqual(unwrapOr(urlDecode("100%25"), "?"), "100%", "an encoded percent")
  assertEqual(unwrapOr(urlDecode("%2f%2F"), "?"), "//", "hex digits in either case")
  assertTrue(isNone(urlDecode("%")), "a lone percent")
  assertTrue(isNone(urlDecode("%4")), "a truncated escape")
  assertTrue(isNone(urlDecode("%GG")), "not hex")
  assertTrue(isNone(urlDecode("%FF")), "not UTF-8")
}

test "HTTP dates become ISO 8601" {
  assertEqual(unwrapOr(httpDateToIso("Thu, 10 Aug 2017 18:50:44 GMT"), "?"), "2017-08-10T18:50:44Z", "Last-Modified")
  assertEqual(unwrapOr(httpDateToIso(" Thu, 12 Nov 2026 22:39:12 GMT "), "?"), "2026-11-12T22:39:12Z", "trimmed")
  assertEqual(unwrapOr(httpDateToIso("Fri, 01 Jan 2027 00:00:00 GMT"), "?"), "2027-01-01T00:00:00Z", "January")
  assertTrue(isNone(httpDateToIso("2026-11-12T22:39:12Z")), "already ISO")
  assertTrue(isNone(httpDateToIso("Thu, 12 Foo 2026 22:39:12 GMT")), "an unknown month")
  assertTrue(isNone(httpDateToIso("Thu, 12 Nov 2026 22:39 GMT")), "no seconds")
  assertTrue(isNone(httpDateToIso("Thu, 12 Nov 2026 22:39:12 UTC")), "not GMT")
  assertTrue(isNone(httpDateToIso("")), "empty")
}
