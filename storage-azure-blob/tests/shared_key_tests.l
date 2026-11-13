//! tests/shared_key_tests.l - `Storage.AzureBlob.SharedKey` against strings
//! and signatures worked out independently of this package.
//!
//! The strings to sign follow the Azure Storage documentation ("Authorize
//! with Shared Key", Blob service format for version 2009-09-19 and later,
//! and "Create a service SAS", version 2020-12-06 and later); the first is
//! the documentation's own example, Get Container Metadata. Each expected
//! signature was computed with Python's `hmac`, `hashlib` and `base64` over
//! the expected string, with the key below, and not with this package.
//!
//! FIXTURE KEY: the account key of the Azurite emulator, a constant
//! Microsoft publishes. It opens nothing but an emulator.
@test_module
package Storage.AzureBlob.SharedKeyTests

import Std.Core
import Std.Collections
import Std.Testing
import Storage.AzureBlob.SharedKey

val FIXTURE_KEY: String = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="

val DATE: String = "Thu, 12 Nov 2026 10:15:00 GMT"

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

func signed(toSign: in String): String {
  match signatureOf(FIXTURE_KEY, toSign) {
    case Some(s) -> s
    case None -> "(key is not base64)"
  }
}

test "the documentation's example: Get Container Metadata, with query parameters" {
  val headers = pairs(["x-ms-version", "2015-02-21", "x-ms-date", "Fri, 26 Jun 2015 23:39:12 GMT"])
  val query = pairs(["restype", "container", "comp", "metadata", "timeout", "20"])
  val toSign = stringToSign("GET", headers, "myaccount", "/mycontainer", query)
  assertEqual(
    toSign,
    "GET\n\n\n\n\n\n\n\n\n\n\n\nx-ms-date:Fri, 26 Jun 2015 23:39:12 GMT\nx-ms-version:2015-02-21\n" +
      "/myaccount/mycontainer\ncomp:metadata\nrestype:container\ntimeout:20",
    "string to sign"
  )
  assertEqual(signed(toSign), "1u9lui2jDxj0+fpbHjQ5m5NnastJRSYM+PSmfi8TXx4=", "signature")
  assertEqual(
    authorizationHeader("myaccount", signed(toSign)),
    "SharedKey myaccount:1u9lui2jDxj0+fpbHjQ5m5NnastJRSYM+PSmfi8TXx4=",
    "Authorization header"
  )
}

test "Put Blob with metadata headers, a content length and a content type" {
  // Given out of order and in mixed case, with untidy whitespace in a value.
  val headers = pairs(
    [
      "X-Ms-Meta-Purpose",
      "  two   words ",
      "Content-Type",
      "text/plain; charset=UTF-8",
      "x-ms-version",
      "2021-12-02",
      "x-ms-meta-owner",
      "alice",
      "Content-Length",
      "11",
      "x-ms-date",
      DATE,
      "x-ms-blob-type",
      "BlockBlob",
      "Accept",
      "*/*"
    ]
  )
  val toSign = stringToSign("PUT", headers, "myaccount", "/mycontainer/docs/hello%20world.txt", noPairs())
  assertEqual(
    toSign,
    "PUT\n\n\n11\n\ntext/plain; charset=UTF-8\n\n\n\n\n\n\nx-ms-blob-type:BlockBlob\n" +
      "x-ms-date:Thu, 12 Nov 2026 10:15:00 GMT\nx-ms-meta-owner:alice\nx-ms-meta-purpose:two words\n" +
      "x-ms-version:2021-12-02\n/myaccount/mycontainer/docs/hello%20world.txt",
    "string to sign"
  )
  assertEqual(signed(toSign), "SneT62RKaaxGuXZiBZOCsVhBQgdSiFQY9ZCOJG/HYxg=", "signature")
}

test "a Content-Length of zero is signed as the empty string" {
  val headers = pairs(
    [
      "Content-Length",
      "0",
      "Content-Type",
      "application/octet-stream",
      "x-ms-date",
      DATE,
      "x-ms-version",
      "2021-12-02"
    ]
  )
  val toSign = stringToSign("PUT", headers, "myaccount", "/mycontainer", pairs(["restype", "container"]))
  assertEqual(
    toSign,
    "PUT\n\n\n\n\napplication/octet-stream\n\n\n\n\n\n\nx-ms-date:Thu, 12 Nov 2026 10:15:00 GMT\n" +
      "x-ms-version:2021-12-02\n/myaccount/mycontainer\nrestype:container",
    "string to sign"
  )
  assertEqual(signed(toSign), "6Gb5VNFKghOChrGXh8dZQvNVaU0mgVcyNy10O5YqA8U=", "signature")
  // With no Content-Length header at all the string, and so the signature,
  // is the same.
  val without = pairs(["Content-Type", "application/octet-stream", "x-ms-date", DATE, "x-ms-version", "2021-12-02"])
  assertEqual(
    stringToSign("PUT", without, "myaccount", "/mycontainer", pairs(["restype", "container"])),
    toSign,
    "an absent Content-Length"
  )
}

test "an emulator's path-style resource names the account twice; parameters are sorted and not encoded" {
  val headers = pairs(["x-ms-date", DATE, "x-ms-version", "2021-12-02"])
  val query = pairs(
    ["restype", "container", "comp", "list", "maxresults", "2", "prefix", "logs/2026/", "marker", "a b/c+d"]
  )
  val toSign = stringToSign("GET", headers, "devstoreaccount1", "/devstoreaccount1/lyric-it", query)
  assertEqual(
    toSign,
    "GET\n\n\n\n\n\n\n\n\n\n\n\nx-ms-date:Thu, 12 Nov 2026 10:15:00 GMT\nx-ms-version:2021-12-02\n" +
      "/devstoreaccount1/devstoreaccount1/lyric-it\ncomp:list\nmarker:a b/c+d\nmaxresults:2\nprefix:logs/2026/\n" +
      "restype:container",
    "string to sign"
  )
  assertEqual(signed(toSign), "UFrJh6H99uZF1OXXBjazvUHY0aKXFbJh9hyLgcGA3Gw=", "signature")
  assertEqual(
    encodeQuery(query),
    "restype=container&comp=list&maxresults=2&prefix=logs%2F2026%2F&marker=a%20b%2Fc%2Bd",
    "the same parameters as they are sent"
  )
}

test "a repeated parameter's values are sorted and joined with commas, and names are lowercased" {
  val headers = pairs(["x-ms-date", DATE, "x-ms-version", "2021-12-02"])
  val query = pairs(
    [
      "Include",
      "uncommittedblobs",
      "restype",
      "container",
      "include",
      "metadata",
      "COMP",
      "list",
      "include",
      "snapshots"
    ]
  )
  val toSign = stringToSign("GET", headers, "myaccount", "/mycontainer", query)
  assertEqual(
    toSign,
    "GET\n\n\n\n\n\n\n\n\n\n\n\nx-ms-date:Thu, 12 Nov 2026 10:15:00 GMT\nx-ms-version:2021-12-02\n" +
      "/myaccount/mycontainer\ncomp:list\ninclude:metadata,snapshots,uncommittedblobs\nrestype:container",
    "string to sign"
  )
  assertEqual(signed(toSign), "UNOVj7USUButOTee+kImvZSdjaJT146l64OnBuVzxwY=", "signature")
}

test "the other standard headers take their places in the string" {
  val headers = pairs(
    [
      "Range",
      "bytes=0-9",
      "If-None-Match",
      "\"0x8D1\"",
      "Content-MD5",
      "XrY7u+Ae7tCTyyK7j1rNww==",
      "Content-Encoding",
      "gzip",
      "Content-Language",
      "en",
      "Date",
      DATE,
      "If-Modified-Since",
      "a",
      "If-Match",
      "b",
      "If-Unmodified-Since",
      "c"
    ]
  )
  assertEqual(
    stringToSign("GET", headers, "acct", "/c/b", noPairs()),
    "GET\ngzip\nen\n\nXrY7u+Ae7tCTyyK7j1rNww==\n\nThu, 12 Nov 2026 10:15:00 GMT\na\nb\n\"0x8D1\"\nc\nbytes=0-9\n/acct/c/b",
    "all eleven standard headers, in the documented order"
  )
}

test "a service SAS for reading one blob over https" {
  val toSign = sasStringToSign(
    "r",
    "",
    "2026-11-12T11:15:00Z",
    "myaccount",
    "mycontainer",
    "shared/q3 report (final).txt",
    "https",
    "2021-12-02"
  )
  assertEqual(
    toSign,
    "r\n\n2026-11-12T11:15:00Z\n/blob/myaccount/mycontainer/shared/q3 report (final).txt\n\n\nhttps\n2021-12-02\nb\n" +
      "\n\n\n\n\n\n",
    "string to sign: sixteen fields, the blob name not encoded"
  )
  assertEqual(signed(toSign), "+1i2DJdy5guZ5oXZMl0ISaYtNOjNBqhhKoh2h/9P04s=", "signature")
  val query = sasQuery(
    "r",
    "2026-11-12T11:15:00Z",
    "myaccount",
    "mycontainer",
    "shared/q3 report (final).txt",
    "https",
    FIXTURE_KEY
  )
  match query {
    case None -> panic("the fixture key is base64")
    case Some(q) -> assertEqual(
      q,
      "sv=2021-12-02&sr=b&sp=r&se=2026-11-12T11%3A15%3A00Z&spr=https&sig=%2B1i2DJdy5guZ5oXZMl0ISaYtNOjNBqhhKoh2h%2F9P04s%3D",
      "the SAS query string, signature percent-encoded"
    )
  }
}

test "a service SAS for writing one blob, with no protocol restriction" {
  val toSign = sasStringToSign(
    "cw",
    "",
    "2026-11-12T11:15:00Z",
    "devstoreaccount1",
    "lyric-it",
    "inbox/upload.txt",
    "",
    "2021-12-02"
  )
  assertEqual(
    toSign,
    "cw\n\n2026-11-12T11:15:00Z\n/blob/devstoreaccount1/lyric-it/inbox/upload.txt\n\n\n\n2021-12-02\nb\n\n\n\n\n\n\n",
    "string to sign: the account once, even for an emulator"
  )
  assertEqual(signed(toSign), "rtGCWjYUUy/Zrtnc5g5HSKONb9hlptb8+cgxHnIxX9c=", "signature")
  val query = sasQuery(
    "cw",
    "2026-11-12T11:15:00Z",
    "devstoreaccount1",
    "lyric-it",
    "inbox/upload.txt",
    "",
    FIXTURE_KEY
  )
  match query {
    case None -> panic("the fixture key is base64")
    case Some(q) -> assertEqual(
      q,
      "sv=2021-12-02&sr=b&sp=cw&se=2026-11-12T11%3A15%3A00Z&sig=rtGCWjYUUy%2FZrtnc5g5HSKONb9hlptb8%2BcgxHnIxX9c%3D",
      "no spr parameter"
    )
  }
}

test "an account key that is not base64 signs nothing" {
  assertTrue(isNone(signatureOf("not base64!!", "x")), "signatureOf")
  assertTrue(isNone(sasQuery("r", "2026-11-12T11:15:00Z", "a", "c", "b", "", "not base64!!")), "sasQuery")
  assertFalse(isAccountKey("not base64!!"), "isAccountKey")
  assertFalse(isAccountKey(""), "an empty key")
  assertTrue(isAccountKey(FIXTURE_KEY), "the fixture key")
}

test "SAS permissions must be in the documented order" {
  assertTrue(isSasPermissions("r"), "read")
  assertTrue(isSasPermissions("cw"), "create and write")
  assertTrue(isSasPermissions("racwd"), "the common five")
  assertFalse(isSasPermissions(""), "empty")
  assertFalse(isSasPermissions("wr"), "out of order")
  assertFalse(isSasPermissions("rr"), "repeated")
  assertFalse(isSasPermissions("rz"), "an unknown letter")
}

test "percent-encoding of blob names and query values" {
  assertEqual(uriEncode("docs/hello world.txt", true), "docs/hello%20world.txt", "a path keeps its slashes")
  assertEqual(uriEncode("a/b", false), "a%2Fb", "a query value does not")
  assertEqual(uriEncode("AZaz09-._~", false), "AZaz09-._~", "the unreserved characters")
  assertEqual(
    uriEncode("sp ace+plus&amp=eqé/100%/(1)*", true),
    "sp%20ace%2Bplus%26amp%3Deq%C3%A9/100%25/%281%29%2A",
    "the rest, as UTF-8"
  )
}
