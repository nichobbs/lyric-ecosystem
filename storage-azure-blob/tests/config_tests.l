//! tests/config_tests.l - connection strings, configuration validation and
//! addressing. No server is involved.
//!
//! FIXTURE KEYS: `KEY` below is base64 of the text "fixture-key-for-tests";
//! the emulator key is the public constant `devAccountKey()`. Neither opens a
//! real storage account.
@test_module
package Storage.AzureBlob.ConfigTests

import Std.Core
import Std.Collections
import Std.Testing
import Storage.AzureBlob

val KEY: String = "Zml4dHVyZS1rZXktZm9yLXRlc3Rz"

func parsed(connectionString: in String): AzureBlobConfig {
  match parseConnectionString(connectionString, "data") {
    case Ok(cfg) -> cfg
    case Err(e) -> panic("parseConnectionString failed: " + e.code + " " + e.message)
  }
}

func parseError(connectionString: in String): String {
  match parseConnectionString(connectionString, "data") {
    case Ok(_) -> "ok"
    case Err(e) -> e.code + ": " + e.message
  }
}

test "a standard connection string names the account, the key and Azure's endpoint" {
  val cfg = parsed(
    "DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=" + devAccountKey() +
      ";EndpointSuffix=core.windows.net"
  )
  assertEqual(cfg.accountName, "myaccount", "AccountName")
  assertEqual(cfg.accountKey, devAccountKey(), "AccountKey keeps its '=' padding")
  assertEqual(cfg.container, "data", "the container is the caller's")
  assertEqual(cfg.endpoint, "", "Azure's own endpoint is the default")
}

test "a connection string with a BlobEndpoint, another suffix, or other spellings" {
  val emulator = parsed(
    "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=" + devAccountKey() +
      ";BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;"
  )
  assertEqual(emulator.endpoint, "http://127.0.0.1:10000/devstoreaccount1", "BlobEndpoint wins over the protocol")
  assertEqual(emulator.accountName, "devstoreaccount1", "the emulator account")
  val china = parsed("AccountName=myaccount;AccountKey=" + KEY + ";EndpointSuffix=core.chinacloudapi.cn")
  assertEqual(
    china.endpoint,
    "https://myaccount.blob.core.chinacloudapi.cn",
    "a sovereign cloud's suffix, https by default"
  )
  val spaced = parsed(" accountname = myaccount ;; ACCOUNTKEY=" + KEY + " ; defaultendpointsprotocol=HTTPS ")
  assertEqual(spaced.accountName, "myaccount", "names in any case, whitespace and empty segments ignored")
  assertEqual(spaced.accountKey, KEY, "the key, trimmed")
  assertEqual(spaced.endpoint, "", "https to core.windows.net is Azure itself")
  val plain = parsed("DefaultEndpointsProtocol=http;AccountName=myaccount;AccountKey=" + KEY)
  assertEqual(plain.endpoint, "http://myaccount.blob.core.windows.net", "http is kept, for openClient to refuse")
  match openClient(plain) {
    case Ok(_) -> panic("plain http to Azure was accepted")
    case Err(e) -> assertEqual(e.code, "INVALID_CONFIG", "plain http to a host that is not loopback")
  }
}

test "UseDevelopmentStorage=true is the emulators' development account" {
  val dev = parsed("UseDevelopmentStorage=true")
  assertEqual(dev.accountName, devAccountName(), "account")
  assertEqual(dev.accountKey, devAccountKey(), "the well-known key")
  assertEqual(dev.endpoint, "http://127.0.0.1:10000/devstoreaccount1", "the default Blob port")
  val proxied = parsed("UseDevelopmentStorage=true;DevelopmentStorageProxyUri=http://localhost/")
  assertEqual(proxied.endpoint, "http://localhost:10000/devstoreaccount1", "the proxy's host")
  val same = emulatorConfig("data")
  assertEqual(same.endpoint, dev.endpoint, "emulatorConfig is the same account")
  assertEqual(same.accountKey, dev.accountKey, "and key")
}

test "a connection string this backend cannot use is refused without repeating a value" {
  assertEqual(parseError(""), "INVALID_CONFIG: connection string has no AccountName", "empty")
  assertEqual(parseError("AccountName=myaccount"), "INVALID_CONFIG: connection string has no AccountKey", "no key")
  assertEqual(
    parseError("AccountName=myaccount;AccountKey=" + KEY + ";Colour=blue"),
    "INVALID_CONFIG: connection string holds an unknown setting 'Colour'",
    "an unknown setting"
  )
  assertEqual(
    parseError("AccountName=a;AccountName=b;AccountKey=" + KEY),
    "INVALID_CONFIG: connection string sets 'AccountName' more than once",
    "a repeated setting"
  )
  assertEqual(
    parseError("AccountName=myaccount;" + KEY),
    "INVALID_CONFIG: connection string holds a setting that is not Name=Value",
    "a segment without '=' is not echoed: it may be a key"
  )
  assertTrue(
    parseError("BlobEndpoint=https://myaccount.blob.core.windows.net;SharedAccessSignature=sv=2021&sig=abc").startsWith(
      "INVALID_CONFIG: connection string carries a SharedAccessSignature"
    ),
    "a SAS connection string"
  )
  assertEqual(
    parseError("DefaultEndpointsProtocol=ftp;AccountName=myaccount;AccountKey=" + KEY),
    "INVALID_CONFIG: connection string DefaultEndpointsProtocol must be https or http",
    "another protocol"
  )
}

func opened(cfg: in AzureBlobConfig): AzureBlobClient {
  match openClient(cfg) {
    case Ok(c) -> c
    case Err(e) -> panic("openClient failed: " + e.code + " " + e.message)
  }
}

func sasOf(client: in AzureBlobClient, key: in String, permissions: in String): String {
  match client.sasUrl(key, permissions, 1794482100i64) {
    case Ok(url) -> url
    case Err(e) -> e.code
  }
}

test "addressing: Azure puts the account in the host, an emulator puts it in the path" {
  val azure = opened(azureConfig("myaccount", devAccountKey(), "mycontainer"))
  assertEqual(azure.containerPath(), "/mycontainer", "container path on Azure")
  assertEqual(azure.blobPath("a/b c.txt"), "/mycontainer/a/b%20c.txt", "blob path, encoded once with '/' kept")
  // The SAS signed for exactly this string in tests/shared_key_tests.l.
  assertEqual(
    sasOf(azure, "shared/q3 report (final).txt", "r"),
    "https://myaccount.blob.core.windows.net/mycontainer/shared/q3%20report%20%28final%29.txt" +
      "?sv=2021-12-02&sr=b&sp=r&se=2026-11-12T11%3A15%3A00Z&spr=https&sig=%2B1i2DJdy5guZ5oXZMl0ISaYtNOjNBqhhKoh2h%2F9P04s%3D",
    "a read SAS on Azure is restricted to https"
  )
  val emulator = opened(
    endpointConfig(
      "http://127.0.0.1:10000/devstoreaccount1/",
      "devstoreaccount1",
      devAccountKey(),
      "lyric-it"
    )
  )
  assertEqual(emulator.containerPath(), "/devstoreaccount1/lyric-it", "container path on an emulator")
  assertEqual(
    sasOf(emulator, "inbox/upload.txt", "cw"),
    "http://127.0.0.1:10000/devstoreaccount1/lyric-it/inbox/upload.txt" +
      "?sv=2021-12-02&sr=b&sp=cw&se=2026-11-12T11%3A15%3A00Z&sig=rtGCWjYUUy%2FZrtnc5g5HSKONb9hlptb8%2BcgxHnIxX9c%3D",
    "a write SAS on a loopback http endpoint carries no spr"
  )
  assertEqual(sasOf(emulator, "k", "wr"), "INVALID_ARGUMENT", "permissions out of order")
  val custom = opened(endpointConfig("https://Blobs.Example.com:443", "myaccount", KEY, "data"))
  assertTrue(
    sasOf(custom, "k", "r").startsWith("https://blobs.example.com/data/k?sv="),
    "a custom domain: host lowercased, default port dropped, no account in the path"
  )
}

func openCode(cfg: in AzureBlobConfig): String {
  match openClient(cfg) {
    case Ok(_) -> "ok"
    case Err(e) -> e.code + ": " + e.message
  }
}

test "configuration is validated before any request" {
  assertEqual(
    openCode(endpointConfig("http://127.0.0.1:10000/devstoreaccount1", "devstoreaccount1", KEY, "data")),
    "ok",
    "plain http is accepted for loopback"
  )
  assertEqual(
    openCode(endpointConfig("http://localhost:10000/devstoreaccount1", "devstoreaccount1", KEY, "data")),
    "ok",
    "localhost"
  )
  assertEqual(
    openCode(endpointConfig("http://[::1]:10000/devstoreaccount1", "devstoreaccount1", KEY, "data")),
    "ok",
    "IPv6 loopback"
  )
  assertTrue(
    openCode(endpointConfig("http://azurite.internal:10000/devstoreaccount1", "devstoreaccount1", KEY, "data")).startsWith(
      "INVALID_CONFIG: Blob endpoint must use https://"
    ),
    "plain http to another host would expose the request"
  )
  assertTrue(
    openCode(endpointConfig("http://127.0.0.1.evil.example", "myaccount", KEY, "data")).startsWith(
      "INVALID_CONFIG: Blob endpoint must use https://"
    ),
    "a host that merely starts with a loopback address is not loopback"
  )
  assertTrue(
    openCode(endpointConfig("ftp://example.com", "myaccount", KEY, "data")).startsWith("INVALID_CONFIG"),
    "not http(s)"
  )
  assertTrue(
    openCode(endpointConfig("https://example.com/otheraccount", "myaccount", KEY, "data")).startsWith(
      "INVALID_CONFIG: Blob endpoint path must be empty or the account name"
    ),
    "an endpoint path that is not the account"
  )
  assertTrue(
    openCode(endpointConfig("https://example.com/myaccount/extra", "myaccount", KEY, "data")).startsWith(
      "INVALID_CONFIG"
    ),
    "a longer path"
  )
  assertTrue(
    openCode(endpointConfig("https://user:pw@example.com", "myaccount", KEY, "data")).startsWith("INVALID_CONFIG"),
    "an endpoint with credentials"
  )
  assertTrue(
    openCode(endpointConfig("https://example.com?x=1", "myaccount", KEY, "data")).startsWith("INVALID_CONFIG"),
    "an endpoint with a query"
  )
  assertTrue(openCode(azureConfig("My_Account", KEY, "data")).startsWith("INVALID_CONFIG"), "account name characters")
  assertTrue(openCode(azureConfig("ab", KEY, "data")).startsWith("INVALID_CONFIG"), "account name too short")
  assertTrue(
    openCode(azureConfig("evil.example/", KEY, "data")).startsWith("INVALID_CONFIG"),
    "an account name that would alter the host"
  )
  assertTrue(openCode(azureConfig("myaccount", KEY, "Data")).startsWith("INVALID_CONFIG"), "container name case")
  assertTrue(openCode(azureConfig("myaccount", KEY, "ab")).startsWith("INVALID_CONFIG"), "container name too short")
  assertTrue(openCode(azureConfig("myaccount", KEY, "a--b")).startsWith("INVALID_CONFIG"), "two hyphens in a row")
  assertTrue(
    openCode(azureConfig("myaccount", KEY, "a/b")).startsWith("INVALID_CONFIG"),
    "a container name with a slash"
  )
  assertTrue(openCode(azureConfig("myaccount", KEY, "$root")).startsWith("INVALID_CONFIG"), "a system container")
  assertEqual(
    openCode(azureConfig("myaccount", "not base64!!", "data")),
    "INVALID_CONFIG: the storage account key must be the base64 key Azure issues",
    "a key that is not base64, not echoed"
  )
  assertTrue(openCode(azureConfig("myaccount", "", "data")).startsWith("INVALID_CONFIG"), "an empty key")
  assertTrue(
    openCode(azureConfig("myaccount", KEY, "data").withRequestTimeout(0)).startsWith("INVALID_CONFIG"),
    "a zero timeout"
  )
}

test "user metadata the Blob service cannot store unchanged is described" {
  assertEqual(userMetaProblem("owner", "alice"), "", "a plain entry")
  assertEqual(userMetaProblem("_build_note1", "a b, c; d=e"), "", "the allowed name characters and a printable value")
  assertEqual(userMetaProblem("owner", ""), "", "an empty value")
  assertTrue(userMetaProblem("", "v").length > 0, "an empty name")
  assertTrue(userMetaProblem("Owner", "v").length > 0, "an uppercase name")
  assertTrue(userMetaProblem("1st", "v").length > 0, "a name starting with a digit is not a C# identifier")
  assertTrue(userMetaProblem("build-note", "v").length > 0, "a hyphen is not allowed in a C# identifier")
  assertTrue(userMetaProblem("build.note", "v").length > 0, "nor a dot")
  assertTrue(userMetaProblem("own er", "v").length > 0, "a name with a space")
  assertTrue(userMetaProblem("owner", "line\nbreak").length > 0, "a control character in a value")
  assertTrue(userMetaProblem("owner", "café").length > 0, "a non-ASCII value")
  assertTrue(userMetaProblem("owner", "trailing ").length > 0, "a trailing space")
}

func names(flat: in slice[String]): List[String] {
  val acc: List[String] = newList()
  var i = 0
  while i < flat.length {
    acc.add(flat[i])
    i = i + 1
  }
  acc
}

test "metadata names whose signing order is ambiguous are described" {
  assertEqual(userMetaOrderProblem(names(["owner", "purpose", "a_b", "a_c", "ab1"])), "", "no '_' meets a digit")
  assertEqual(userMetaOrderProblem(names(["a_b"])), "", "a single name")
  assertTrue(userMetaOrderProblem(names(["a", "a_", "a1"])).length > 0, "'a_' against 'a1'")
  assertTrue(
    userMetaOrderProblem(names(["build1", "owner", "build_2"])).contains("'build1' and 'build_2'"),
    "the message names the pair"
  )
}

test "blob names are the interface's safe keys, up to Azure's length limit" {
  assertTrue(isBlobName("a/b/c.txt"), "a nested key")
  assertFalse(isBlobName("../escape"), "a traversal")
  assertFalse(isBlobName("photo.jpg.meta.json"), "the reserved sidecar suffix")
  assertFalse(isBlobName(""), "empty")
  var long = ""
  var i = 0
  while i < 128 {
    long = long + "12345678"
    i = i + 1
  }
  assertTrue(isBlobName(long), "1024 characters")
  assertFalse(isBlobName(long + "x"), "1025 characters")
}
