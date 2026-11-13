//! tests/fake_server_tests.l - the client, the store, the backend-specific
//! operations and the `CacheStore` conformance suite against an in-process
//! fake memcached, so the hermetic suite covers real sockets on both targets.
//!
//! The fake listens on a free loopback port (`Net.Tcp.Testing`) and serves
//! each connection on a thread of its own. It implements what the client
//! speaks the way memcached 1.6.24 answers it (every reply below was taken
//! from a real server): `mg`, `ms`, `md`, `ma`, `mn`, `version`, `flush_all`
//! and the authentication `set`; base64 keys, the 250-byte key limit, the
//! item size limit, add mode, compare-and-swap, quiet mode, opaque tokens;
//! `CLIENT_ERROR`, `SERVER_ERROR` and `ERROR`, including memcached's habit
//! of reading the data block of a refused `ms` as a command of its own. The
//! `ms` modes append, prepend and replace, which the client never sends, are
//! answered with a `SERVER_ERROR` naming the fake.
//!
//! Expiry is the fake's own, with memcached's rules: 0 is never, a negative
//! expiry is already over, up to 30 days it is relative, above it an
//! absolute timestamp, and the number is cut to 32 bits first. Its clock is
//! the real clock plus an offset that `advance` moves, so the conformance
//! suite and the TTL tests run without sleeping; nothing in the client
//! decides whether an entry has expired.
//!
//! A test can also script the next replies (`Fault`): close without
//! answering, answer with other bytes, hang, answer and close, answer one
//! byte at a time.
//!
//! TEST FIXTURES, NOT SECRETS. The certificates and the private key below
//! exist only for this file: a throwaway CA and a server certificate for
//! `localhost` / `127.0.0.1`. They protect nothing and are valid until 2126.
//! They were made with:
//!
//!   openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out X.key
//!   openssl req -x509 -new -key ca.key -sha256 -days 36500 \
//!     -subj "/O=Lyric.Cache.Memcached test fixture/CN=test fixture CA (not a secret)" \
//!     -addext "basicConstraints=critical,CA:TRUE" -addext "keyUsage=critical,keyCertSign,cRLSign" -out ca.crt
//!   openssl req -new -key server.key -subj "/O=Lyric.Cache.Memcached test fixture/CN=localhost" -out server.csr
//!   openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial -sha256 -days 36500 \
//!     -extfile server.ext -out server.crt      (subjectAltName=DNS:localhost,IP:127.0.0.1)
@test_module
package Cache.Memcached.FakeServerTests

import Std.Core
import Std.Collections
import Std.Encoding
import Std.Parse
import Std.String as Str
import Std.Testing
import Std.Time
import Std.Uuid
import Cache
import Cache.Memcached as MemcachedCache
import Cache.Memcached.Client as Client
import Cache.Memcached.Extras as Extras
import Cache.Memcached.Keys as Keys
import Cache.Memcached.Meta as Meta
import Net.Tcp as Tcp
import Net.Tcp.Testing as KT
import Net.Tls as NetTls
import Testing.Conformance
import Testing.Conformance.Cache as CacheConformance

// Fixture: the throwaway CA.
val caPem: String = "-----BEGIN CERTIFICATE-----\nMIICFDCCAbmgAwIBAgIUZrW6FyNzLLwDVhOpHrBAyZ56X0wwCgYIKoZIzj0EAwIw\nVjErMCkGA1UECgwiTHlyaWMuQ2FjaGUuTWVtY2FjaGVkIHRlc3QgZml4dHVyZTEn\nMCUGA1UEAwwedGVzdCBmaXh0dXJlIENBIChub3QgYSBzZWNyZXQpMCAXDTI2MTEx\nMzAxMzM0NloYDzIxMjYxMDIwMDEzMzQ2WjBWMSswKQYDVQQKDCJMeXJpYy5DYWNo\nZS5NZW1jYWNoZWQgdGVzdCBmaXh0dXJlMScwJQYDVQQDDB50ZXN0IGZpeHR1cmUg\nQ0EgKG5vdCBhIHNlY3JldCkwWTATBgcqhkjOPQIBBggqhkjOPQMBBwNCAAQJ+GbU\nnXnNRyz2Gkpo/q56qUJyHiLpzVQfw3czL9umg9pzIGYK43ukA1WScvsxkg3ew2kO\n6RN/UZfumtjBEy6fo2MwYTAdBgNVHQ4EFgQUYxb8fTlHTbxCUTiNQ+fvG3Gn9a4w\nHwYDVR0jBBgwFoAUYxb8fTlHTbxCUTiNQ+fvG3Gn9a4wDwYDVR0TAQH/BAUwAwEB\n/zAOBgNVHQ8BAf8EBAMCAQYwCgYIKoZIzj0EAwIDSQAwRgIhALnKZJ8vfkCNvjGa\nNShDq3T4bP42rwMxPz/OqGxN6N2uAiEArg9DCWDWCdH7cbmqfxtDX+YIRio/WHDQ\n5zHRzbIcbxE=\n-----END CERTIFICATE-----\n"

// Fixture: the server certificate, issued by the CA above.
val serverCertPem: String = "-----BEGIN CERTIFICATE-----\nMIICKjCCAdGgAwIBAgIUHrYs7K9f4hqkbnh/uIe297LcLRQwCgYIKoZIzj0EAwIw\nVjErMCkGA1UECgwiTHlyaWMuQ2FjaGUuTWVtY2FjaGVkIHRlc3QgZml4dHVyZTEn\nMCUGA1UEAwwedGVzdCBmaXh0dXJlIENBIChub3QgYSBzZWNyZXQpMCAXDTI2MTEx\nMzAxMzM0NloYDzIxMjYxMDIwMDEzMzQ2WjBBMSswKQYDVQQKDCJMeXJpYy5DYWNo\nZS5NZW1jYWNoZWQgdGVzdCBmaXh0dXJlMRIwEAYDVQQDDAlsb2NhbGhvc3QwWTAT\nBgcqhkjOPQIBBggqhkjOPQMBBwNCAAQ1a5tcMvVtGANj/d2QmFxT+W7XAoDZ1mwW\nbxqHVXmD3pXUFR4oiSS7Ddv6+VkUfrniu66Zee81XVtIof5vXicso4GPMIGMMAkG\nA1UdEwQCMAAwGgYDVR0RBBMwEYIJbG9jYWxob3N0hwR/AAABMBMGA1UdJQQMMAoG\nCCsGAQUFBwMBMA4GA1UdDwEB/wQEAwIHgDAdBgNVHQ4EFgQUFKAwWgXmyA7LI0Id\nYc1Y7E8pJvkwHwYDVR0jBBgwFoAUYxb8fTlHTbxCUTiNQ+fvG3Gn9a4wCgYIKoZI\nzj0EAwIDRwAwRAIgN864ugs1Evox2oTp8yaJSFDAkQX8uL53fvFdCth6WcICICih\nqfCzk+toAmmm8kF8MRWmwcSqMiArX/MrVYeJ6mAC\n-----END CERTIFICATE-----\n"

// Fixture: the server's private key (PKCS#8).
val serverKeyPem: String = "-----BEGIN PRIVATE KEY-----\nMIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgD1vMX3iRYCQrJJKv\nfhFLlkR+ttjmZ1lG+A/ooqBZSdGhRANCAAQ1a5tcMvVtGANj/d2QmFxT+W7XAoDZ\n1mwWbxqHVXmD3pXUFR4oiSS7Ddv6+VkUfrniu66Zee81XVtIof5vXics\n-----END PRIVATE KEY-----\n"

// ─── The fake server ─────────────────────────────────────────────────────────
record Item {
  value: slice[Byte]
  // Epoch seconds; 0 = never.
  expiresAt: Long
  cas: Long
}

/// What the fake does with the next command instead of answering it.
union Fault {
  /// Answer normally.
  case Pass
  /// Close the connection without running the command.
  case CloseNow
  /// Write `text` instead of running the command.
  case SendInstead(text: String)
  /// Write `text` instead of running the command, then close.
  case SendThenClose(text: String)
  /// Write nothing for `millis` milliseconds, then close.
  case Hang(millis: Int)
  /// Run the command, answer, then close.
  case ReplyThenClose
  /// Run the command and answer one byte at a time.
  case ReplySlowly
}

func crlf(text: in String): slice[Byte] {
  encodeUtf8(text + "\r\n")
}

func isDigits(text: in String): Bool {
  if text.length == 0 {
    return false
  }
  var i = 0
  while i < text.length {
    val unit = text.codeUnitAt(i)
    if unit < 48 or unit > 57 {
      return false
    }
    i = i + 1
  }
  true
}

// The argument of the first flag token that starts with `letter`.
func flagArg(tokens: in slice[String], from: in Int, letter: in String): Option[String] {
  var i = from
  while i < tokens.length {
    val token: String = tokens[i]
    if token.startsWith(letter) {
      return Some(value = Str.substring(token, 1))
    }
    i = i + 1
  }
  None
}

func hasFlag(tokens: in slice[String], from: in Int, letter: in String): Bool {
  match flagArg(tokens, from, letter) {
    case Some(_) -> true
    case None -> false
  }
}

// Whether a flag token starts with a letter outside `allowed`.
func hasInvalidFlag(tokens: in slice[String], from: in Int, allowed: in String): Bool {
  var i = from
  while i < tokens.length {
    val token: String = tokens[i]
    if token.length == 0 or not Str.contains(allowed, Str.substring(token, 0, 1)) {
      return true
    }
    i = i + 1
  }
  false
}

// The opaque token and the key, echoed the way memcached echoes them.
func echoed(tokens: in slice[String], from: in Int): String {
  var echo = ""
  var i = from
  while i < tokens.length {
    val token: String = tokens[i]
    if token.startsWith("O") {
      echo = echo + " " + token
    } else if token == "k" {
      echo = echo + " k" + tokens[1] + (if hasFlag(tokens, from, "b") { " b" } else { "" })
    }
    i = i + 1
  }
  echo
}

// memcached keeps the expiry in 32 bits.
func wrap32(t: in Long): Long {
  var m = t % 4294967296i64
  if m < 0i64 {
    m = m + 4294967296i64
  }
  if m >= 2147483648i64 {
    m - 4294967296i64
  } else {
    m
  }
}

// The expiry an item gets for the `T` argument `t` at the time `now`.
func expiryFor(t: in Long, now: in Long): Long {
  val exptime = wrap32(t)
  if exptime == 0i64 {
    0i64
  } else if exptime < 0i64 {
    now - 1i64
  } else if exptime <= 2592000i64 {
    now + exptime
  } else {
    exptime
  }
}

// The key of a command as the hexadecimal form of its bytes, or the error
// line.
func keyOf(tokens: in slice[String], flagsFrom: in Int): Result[String, String] {
  if tokens.length < 2 {
    return Err("CLIENT_ERROR bad command line format")
  }
  val token: String = tokens[1]
  if token.length == 0 or token.length > 250 {
    return Err("CLIENT_ERROR bad command line format")
  }
  if hasFlag(tokens, flagsFrom, "b") {
    match tryDecodeBase64(token) {
      case Some(bytes) -> Ok(encodeHex(bytes))
      case None -> Err("CLIENT_ERROR error decoding key")
    }
  } else {
    Ok(encodeHex(encodeUtf8(token)))
  }
}

func hexOf(storedKey: in String): String {
  encodeHex(encodeUtf8(storedKey))
}

func bytesOf(parts: in List[slice[Byte]]): slice[Byte] {
  Meta.concatAll(parts)
}

protected type FakeState {
  versionLine: String
  authUser: String
  authPassword: String
  itemLimit: Int
  var items: Map[String, Item] = newMap()
  var offsetSeconds: Long = 0i64
  var casCounter: Long = 0i64
  var log: List[String] = newList()
  var faults: List[Fault] = newList()
  var accepted: Int = 0
  var stopping: Bool = false
  var bumpGenerationOnRead: Bool = false
  func clockSeconds(): Long {
    nowEpochMillis() / 1000i64 + offsetSeconds
  }
  func live(hex: in String): Option[Item] {
    if not items.containsKey(hex) {
      return None
    }
    val item = items[hex]
    if item.expiresAt != 0i64 and item.expiresAt <= clockSeconds() {
      return None
    }
    Some(value = item)
  }
  func nextCas(): Long {
    casCounter = casCounter + 1i64
    casCounter
  }
  func metaGet(tokens: in slice[String]): slice[Byte] {
    if hasInvalidFlag(tokens, 2, "bvtcsfkqOT") {
      return crlf("CLIENT_ERROR invalid flag")
    }
    val hex = match keyOf(tokens, 2) { case Ok(k) -> k; case Err(line) -> return crlf(line) }
    val quiet = hasFlag(tokens, 2, "q")
    val found = match live(hex) {
      case Some(item) -> item
      case None -> {
        if quiet {
          val nothing: slice[Byte] = []
          return nothing
        }
        return crlf("EN" + echoed(tokens, 2))
      }
    }
    var item = found
    match flagArg(tokens, 2, "T") {
      case Some(text) -> match parseOptLong(text) {
        case Some(t) -> {
          item = Item(value = found.value, expiresAt = expiryFor(t, clockSeconds()), cas = found.cas)
          items[hex] = item
        }
        case None -> return crlf("CLIENT_ERROR bad token in command line format")
      }
      case None -> ()
    }
    if bumpGenerationOnRead and hex.endsWith(hexOf(" generation")) {
      match tryDecodeUtf8(item.value) {
        case Some(text) -> match parseOptLong(text) {
          case Some(n) -> {
            // Someone clears the namespace just before every read of its generation.
            item = Item(value = encodeUtf8((n + 1i64).toString()), expiresAt = item.expiresAt, cas = nextCas())
            items[hex] = item
          }
          case None -> ()
        }
        case None -> ()
      }
    }
    var ret = ""
    var i = 2
    while i < tokens.length {
      val token: String = tokens[i]
      if token == "t" {
        ret = ret + " t" + (if item.expiresAt == 0i64 { "-1" } else { (item.expiresAt - clockSeconds()).toString() })
      } else if token == "c" {
        ret = ret + " c" + item.cas.toString()
      } else if token == "s" {
        ret = ret + " s" + item.value.length.toString()
      } else if token == "f" {
        ret = ret + " f0"
      } else if token.startsWith("O") {
        ret = ret + " " + token
      } else if token == "k" {
        ret = ret + " k" + tokens[1] + (if hasFlag(tokens, 2, "b") { " b" } else { "" })
      }
      i = i + 1
    }
    if hasFlag(tokens, 2, "v") {
      val parts: List[slice[Byte]] = newList()
      parts.add(crlf("VA " + item.value.length.toString() + ret))
      parts.add(item.value)
      parts.add(crlf(""))
      bytesOf(parts)
    } else {
      crlf("HD" + ret)
    }
  }
  func metaSet(tokens: in slice[String], data: in slice[Byte]): slice[Byte] {
    if hasInvalidFlag(tokens, 3, "bTMCFqOkI") {
      return crlf("CLIENT_ERROR invalid flag")
    }
    val mode = match flagArg(tokens, 3, "M") { case Some(m) -> m; case None -> "S" }
    if mode == "A" or mode == "P" or mode == "R" {
      return crlf("SERVER_ERROR the fake memcached does not implement ms mode " + mode)
    }
    if mode != "E" and mode != "S" {
      return crlf("CLIENT_ERROR invalid mode for ms M token")
    }
    val exptime = match flagArg(tokens, 3, "T") {
      case Some(text) -> match parseOptLong(text) {
        case Some(t) -> t
        case None -> return crlf("CLIENT_ERROR bad token in command line format")
      }
      case None -> 0i64
    }
    val hex = match keyOf(tokens, 3) { case Ok(k) -> k; case Err(line) -> return crlf(line) }
    if data.length + hex.length / 2 + 59 > itemLimit {
      items.remove(hex)
      return crlf("SERVER_ERROR object too large for cache")
    }
    val existing = live(hex)
    match flagArg(tokens, 3, "C") {
      case Some(text) -> match existing {
        case None -> return crlf("NF" + echoed(tokens, 3))
        case Some(item) -> {
          if item.cas.toString() != text {
            return crlf("EX" + echoed(tokens, 3))
          }
        }
      }
      case None -> ()
    }
    if mode == "E" {
      match existing {
        case Some(_) -> return crlf("NS" + echoed(tokens, 3))
        case None -> ()
      }
    }
    items[hex] = Item(value = data, expiresAt = expiryFor(exptime, clockSeconds()), cas = nextCas())
    if hasFlag(tokens, 3, "q") {
      val nothing: slice[Byte] = []
      return nothing
    }
    crlf("HD" + echoed(tokens, 3))
  }
  func metaDelete(tokens: in slice[String]): slice[Byte] {
    if hasInvalidFlag(tokens, 2, "bCqOkTI") {
      return crlf("CLIENT_ERROR invalid flag")
    }
    val hex = match keyOf(tokens, 2) { case Ok(k) -> k; case Err(line) -> return crlf(line) }
    match live(hex) {
      case None -> {
        items.remove(hex)
        crlf("NF" + echoed(tokens, 2))
      }
      case Some(item) -> {
        match flagArg(tokens, 2, "C") {
          case Some(text) -> {
            if item.cas.toString() != text {
              return crlf("EX" + echoed(tokens, 2))
            }
          }
          case None -> ()
        }
        items.remove(hex)
        if hasFlag(tokens, 2, "q") {
          val nothing: slice[Byte] = []
          return nothing
        }
        crlf("HD" + echoed(tokens, 2))
      }
    }
  }
  func metaArithmetic(tokens: in slice[String]): slice[Byte] {
    if hasInvalidFlag(tokens, 2, "bvNJDMqOkct") {
      return crlf("CLIENT_ERROR invalid flag")
    }
    val hex = match keyOf(tokens, 2) { case Ok(k) -> k; case Err(line) -> return crlf(line) }
    var number = 0i64
    match live(hex) {
      case None -> {
        match flagArg(tokens, 2, "N") {
          case None -> return crlf("NF" + echoed(tokens, 2))
          case Some(ttlText) -> {
            val initial = match flagArg(tokens, 2, "J") { case Some(j) -> j; case None -> "0" }
            number = match parseOptLong(initial) {
              case Some(n) -> n
              case None -> return crlf("CLIENT_ERROR bad token in command line format")
            }
            val ttl = match parseOptLong(ttlText) {
              case Some(n) -> n
              case None -> return crlf("CLIENT_ERROR bad token in command line format")
            }
            items[hex] = Item(value = encodeUtf8(number.toString()), expiresAt = expiryFor(ttl, clockSeconds()), cas = nextCas())
          }
        }
      }
      case Some(item) -> {
        val current = match tryDecodeUtf8(item.value) {
          case Some(text) -> if isDigits(text) { parseOptLong(text) } else { None }
          case None -> None
        }
        val before = match current {
          case Some(n) -> n
          case None -> return crlf("CLIENT_ERROR cannot increment or decrement non-numeric value")
        }
        val delta = match flagArg(tokens, 2, "D") {
          case Some(text) -> match parseOptLong(text) {
            case Some(n) -> n
            case None -> return crlf("CLIENT_ERROR bad token in command line format")
          }
          case None -> 1i64
        }
        val mode = match flagArg(tokens, 2, "M") { case Some(m) -> m; case None -> "I" }
        number = if mode == "D" or mode == "-" { if before < delta { 0i64 } else { before - delta } } else { before + delta }
        items[hex] = Item(value = encodeUtf8(number.toString()), expiresAt = item.expiresAt, cas = nextCas())
      }
    }
    if hasFlag(tokens, 2, "q") {
      val nothing: slice[Byte] = []
      return nothing
    }
    if hasFlag(tokens, 2, "v") {
      val text = number.toString()
      crlf("VA " + text.length.toString() + echoed(tokens, 2)).concat(crlf(text))
    } else {
      crlf("HD" + echoed(tokens, 2))
    }
  }
  /// Run one command and return the bytes of its reply.
  entry execute(tokens: in slice[String], data: in slice[Byte]): slice[Byte] {
    val command: String = tokens[0]
    if command == "mn" {
      if tokens.length == 1 {
        return crlf("MN")
      }
      return crlf("ERROR")
    }
    if command == "version" {
      return crlf(versionLine)
    }
    if command == "flush_all" {
      items = newMap()
      return crlf("OK")
    }
    if command == "mg" {
      return metaGet(tokens)
    }
    if command == "ms" {
      return metaSet(tokens, data)
    }
    if command == "md" {
      return metaDelete(tokens)
    }
    if command == "ma" {
      return metaArithmetic(tokens)
    }
    crlf("ERROR")
  }
  entry record(line: in String): Unit {
    log.add(line)
  }
  /// Every command line received so far.
  func lines(): List[String] {
    val copy: List[String] = newList()
    for line in log {
      copy.add(line)
    }
    copy
  }
  entry forgetLines(): Unit {
    log = newList()
  }
  entry addFault(fault: in Fault): Unit {
    faults.add(fault)
  }
  entry nextFault(): Fault {
    if faults.count == 0 {
      return Pass
    }
    val fault = faults[0]
    faults.removeAt(0)
    fault
  }
  /// Move the server's clock forward.
  entry advance(seconds: in Int): Unit {
    offsetSeconds = offsetSeconds + seconds.toLong()
  }
  /// Remove an item behind the client's back, as an eviction does.
  entry evict(storedKey: in String): Unit {
    val _ = items.remove(hexOf(storedKey))
  }
  /// Store an item behind the client's back, as another client does.
  entry plant(storedKey: in String, value: in slice[Byte]): Unit {
    items[hexOf(storedKey)] = Item(value = value, expiresAt = 0i64, cas = nextCas())
  }
  /// The value stored under `storedKey`, expired or not.
  func stored(storedKey: in String): Option[String] {
    val hex = hexOf(storedKey)
    if not items.containsKey(hex) {
      return None
    }
    tryDecodeUtf8(items[hex].value)
  }
  func itemCount(): Int {
    items.count
  }
  entry setBumpGenerationOnRead(on: in Bool): Unit {
    bumpGenerationOnRead = on
  }
  func needsAuth(): Bool {
    authUser.length > 0
  }
  func credentialsOk(token: in String): Bool {
    token == authUser + " " + authPassword
  }
  entry noteAccepted(): Unit {
    accepted = accepted + 1
  }
  func acceptedCount(): Int {
    accepted
  }
  entry stop(): Unit {
    stopping = true
  }
  func isStopping(): Bool {
    stopping
  }
}

func findCrlf(buf: in slice[Byte]): Int {
  var i = 0
  while i + 1 < buf.length {
    if buf[i].toInt() == 13 and buf[i + 1].toInt() == 10 {
      return i
    }
    i = i + 1
  }
  0 - 1
}

// The next bytes from the client; `None` when it closed the connection or
// the fake is stopping.
func readMore(sock: in Tcp.Sock, state: in FakeState): Option[slice[Byte]] {
  while true {
    match Tcp.sockRead(sock, 65536, 200) {
      case Err(_) -> return None
      case Ok(Tcp.Data(bytes)) -> return Some(value = bytes)
      case Ok(Tcp.Eof) -> return None
      case Ok(Tcp.TimedOut) -> {
        if state.isStopping() {
          return None
        }
      }
    }
  }
  None
}

func writeSlowly(sock: in Tcp.Sock, bytes: in slice[Byte]): Unit {
  var i = 0
  while i < bytes.length {
    val _ = Tcp.sockWrite(sock, bytes.slice(i, i + 1))
    i = i + 1
  }
}

// The length of the data block that follows `tokens`, or -1 when the line
// announces none the server would read.
func announcedData(tokens: in slice[String]): Int {
  val command: String = tokens[0]
  var lengthText = ""
  if command == "ms" and tokens.length >= 3 {
    val key: String = tokens[1]
    if key.length <= 250 {
      lengthText = tokens[2]
    }
  } else if command == "set" and tokens.length >= 5 {
    lengthText = tokens[4]
  }
  if not isDigits(lengthText) or lengthText.length > 9 {
    return 0 - 1
  }
  match parseOptInt(lengthText) {
    case Some(n) -> n
    case None -> 0 - 1
  }
}

func serveConnection(accepted: in Tcp.Sock, state: in FakeState, tls: in Option[KT.ServerTls]): Unit {
  val sock = match tls {
    case None -> accepted
    case Some(settings) -> match KT.sockAcceptTls(accepted, settings, 5000) {
      case Ok(secured) -> secured
      case Err(_) -> return
    }
  }
  var buf: slice[Byte] = []
  var authed = not state.needsAuth()
  var open = true
  while open {
    val lineEnd = findCrlf(buf)
    if lineEnd < 0 {
      match readMore(sock, state) {
        case None -> {
          open = false
        }
        case Some(chunk) -> {
          buf = buf.concat(chunk)
        }
      }
    } else {
      val line = match tryDecodeUtf8(buf.slice(0, lineEnd)) { case Some(t) -> t; case None -> "<binary>" }
      var rest: slice[Byte] = buf.slice(lineEnd + 2, buf.length)
      val tokens = line.split(" ")
      val command: String = tokens[0]
      val dataLength = announcedData(tokens)
      var data: slice[Byte] = []
      var terminated = true
      if dataLength >= 0 {
        while open and rest.length < dataLength + 2 {
          match readMore(sock, state) {
            case None -> {
              open = false
            }
            case Some(chunk) -> {
              rest = rest.concat(chunk)
            }
          }
        }
        if open {
          data = rest.slice(0, dataLength)
          terminated = rest[dataLength].toInt() == 13 and rest[dataLength + 1].toInt() == 10
          rest = rest.slice(dataLength + 2, rest.length)
        }
      }
      buf = rest
      if open {
        state.record(line)
        val handshake = command == "version" or command == "set"
        val fault = if handshake { Pass } else { state.nextFault() }
        val runs = match fault { case Pass -> true; case ReplyThenClose -> true; case ReplySlowly -> true; case _ -> false }
        var reply: slice[Byte] = []
        var closeAfter = false
        if runs {
          if not authed {
            if command == "set" and dataLength >= 0 {
              val token = match tryDecodeUtf8(data) { case Some(t) -> t; case None -> "" }
              if state.credentialsOk(token) {
                authed = true
                reply = crlf("STORED")
              } else {
                reply = crlf("CLIENT_ERROR authentication failure")
                closeAfter = true
              }
            } else if command == "mn" {
              // memcached closes on an unauthenticated mn without a word.
              closeAfter = true
            } else {
              reply = crlf("CLIENT_ERROR unauthenticated")
            }
          } else if command == "set" {
            // A server without an authentication file stores the token; it
            // was sent with an expiry of -1, so there is nothing to keep.
            reply = if dataLength >= 0 { crlf("STORED") } else { crlf("ERROR") }
          } else if command == "ms" {
            if dataLength < 0 {
              reply = crlf("CLIENT_ERROR bad command line format")
            } else if not terminated {
              reply = crlf("CLIENT_ERROR bad data chunk")
            } else {
              reply = state.execute(tokens, data)
            }
          } else {
            reply = state.execute(tokens, data)
          }
        }
        match fault {
          case Pass -> {
            val _ = Tcp.sockWrite(sock, reply)
          }
          case CloseNow -> {
            open = false
          }
          case SendInstead(text) -> {
            val _ = Tcp.sockWrite(sock, encodeUtf8(text))
          }
          case SendThenClose(text) -> {
            val _ = Tcp.sockWrite(sock, encodeUtf8(text))
            open = false
          }
          case Hang(millis) -> {
            sleepMillis(millis)
            open = false
          }
          case ReplyThenClose -> {
            val _ = Tcp.sockWrite(sock, reply)
            open = false
          }
          case ReplySlowly -> writeSlowly(sock, reply)
        }
        if closeAfter {
          open = false
        }
      }
    }
  }
  Tcp.sockClose(sock)
}

func acceptLoop(server: in KT.ServerSock, state: in FakeState, tls: in Option[KT.ServerTls]): Unit {
  var running = true
  while running {
    match KT.serverAccept(server) {
      case Err(_) -> {
        running = false
      }
      case Ok(sock) -> {
        if state.isStopping() {
          Tcp.sockClose(sock)
          running = false
        } else {
          state.noteAccepted()
          val _ = KT.startBackground({ -> serveConnection(sock, state, tls) })
        }
      }
    }
  }
  KT.serverClose(server)
}

record Fake {
  port: Int
  state: FakeState
  acceptor: KT.Background
}

record FakeSetup {
  versionLine: String
  authUser: String
  authPassword: String
  itemLimit: Int
  tls: Bool
}

func plainSetup(): FakeSetup {
  FakeSetup(versionLine = "VERSION 1.6.24", authUser = "", authPassword = "", itemLimit = 1048576, tls = false)
}

func startFakeWith(setup: in FakeSetup): Fake {
  match KT.serverListen("127.0.0.1", 0) {
    case Err(m) -> panic("the fake server cannot listen: " + m)
    case Ok(server) -> {
      val state = FakeState(
        versionLine = setup.versionLine,
        authUser = setup.authUser,
        authPassword = setup.authPassword,
        itemLimit = setup.itemLimit
      )
      val tls: Option[KT.ServerTls] = if setup.tls {
        Some(value = KT.ServerTls(certPem = serverCertPem, keyPem = serverKeyPem, clientCaPem = ""))
      } else {
        None
      }
      val port = KT.serverPort(server)
      val acceptor = KT.startBackground({ -> acceptLoop(server, state, tls) })
      Fake(port = port, state = state, acceptor = acceptor)
    }
  }
}

func startFake(): Fake {
  startFakeWith(plainSetup())
}

// Stop accepting: the flag is set, and one last connection wakes the
// acceptor. A test closes its clients first.
func stopFake(fake: in Fake): Unit {
  fake.state.stop()
  match Tcp.sockConnect("127.0.0.1", fake.port, 5000) {
    case Ok(sock) -> Tcp.sockClose(sock)
    case Err(_) -> ()
  }
  KT.waitBackground(fake.acceptor)
}

func urlOf(fake: in Fake): String {
  "memcached://127.0.0.1:" + fake.port.toString()
}

func configOf(fake: in Fake): Client.MemcachedConfig {
  Client.configFor("127.0.0.1", fake.port)
}

// ─── Helpers ─────────────────────────────────────────────────────────────────
func ok[T](r: in Result[T, Client.MemcachedError], what: in String): T {
  match r {
    case Ok(v) -> v
    case Err(e) -> panic(what + " failed: " + e.message())
  }
}

func codeOf[T](r: in Result[T, Client.MemcachedError], what: in String): String {
  match r {
    case Ok(_) -> "OK (" + what + " did not fail)"
    case Err(e) -> e.code()
  }
}

func messageOf[T](r: in Result[T, Client.MemcachedError]): String {
  match r {
    case Ok(_) -> ""
    case Err(e) -> e.message()
  }
}

// The generic helpers above are not used for a `Result[List[MetaReply], _]`:
// see "Known gaps" in the README (the specialisation is compiled for Int).
func okReplies(r: in Result[List[Meta.MetaReply], Client.MemcachedError], what: in String): List[Meta.MetaReply] {
  match r {
    case Ok(v) -> v
    case Err(e) -> panic(what + " failed: " + e.message())
  }
}

func codeOfRun(r: in Result[List[Meta.MetaReply], Client.MemcachedError], what: in String): String {
  match r {
    case Ok(_) -> "OK (" + what + " did not fail)"
    case Err(e) -> e.code()
  }
}

func messageOfRun(r: in Result[List[Meta.MetaReply], Client.MemcachedError]): String {
  match r {
    case Ok(_) -> ""
    case Err(e) -> e.message()
  }
}

func expectHit(v: in Option[String], expected: in String, what: in String): Unit {
  match v {
    case Some(s) -> assertEqual(s, expected, what)
    case None -> assertTrue(false, what + ": expected a hit")
  }
}

func expectMiss(v: in Option[String], what: in String): Unit {
  match v {
    case Some(s) -> assertTrue(false, what + ": expected a miss, got " + s)
    case None -> ()
  }
}

func clientOf(fake: in Fake): Client.MemcachedClient {
  ok(Client.connect(urlOf(fake)), "connect")
}

func freshNamespace(): String {
  "ns-" + uuidToString(newUuid())
}

func storeOn(fake: in Fake, namespace: in String): MemcachedCache.MemcachedCacheStore {
  MemcachedCache.fromClient(clientOf(fake), MemcachedCache.options(namespace))
}

func wire(storedKey: in String): String {
  Keys.wireKey(storedKey)
}

// The statuses of `replies`, joined: "HD VA EN".
func statuses(replies: in List[Meta.MetaReply]): String {
  val names: List[String] = newList()
  for reply in replies {
    names.add(Meta.statusOf(reply))
  }
  Str.joinList(" ", names)
}

func valueText(reply: in Meta.MetaReply): String {
  match reply {
    case Meta.Value(data, _) -> match tryDecodeUtf8(data) { case Some(t) -> t; case None -> "<binary>" }
    case other -> "<" + Meta.statusOf(other) + ">"
  }
}

// The command lines the fake received that start with `prefix`.
func linesWith(fake: in Fake, prefix: in String): List[String] {
  val found: List[String] = newList()
  for line in fake.state.lines() {
    if line.startsWith(prefix) {
      found.add(line)
    }
  }
  found
}

// The stored key a command line names (its second token, base64-decoded).
func storedKeyOf(line: in String): String {
  val tokens = line.split(" ")
  if tokens.length < 2 {
    return "<no key>"
  }
  match tryDecodeBase64(tokens[1]) {
    case Some(bytes) -> match tryDecodeUtf8(bytes) { case Some(t) -> t; case None -> "<binary>" }
    case None -> "<not base64>"
  }
}

// The generation the fake holds for `namespace`.
func generationOn(fake: in Fake, namespace: in String): String {
  match fake.state.stored(Keys.generationKey(namespace)) {
    case Some(g) -> g
    case None -> ""
  }
}

// ─── The client ──────────────────────────────────────────────────────────────
test "connect checks the server version, and ping and version round-trip" {
  val fake = startFake()
  val client = clientOf(fake)
  assertTrue(client.isConnected(), "connect opens the connection")
  assertEqualInt(client.connections(), 1, "one connection")
  assertEqual(client.serverVersion(), "1.6.24", "the version read at the handshake")
  ok(Client.ping(client), "ping")
  assertEqual(ok(Client.serverVersion(client), "version"), "1.6.24", "version")
  assertEqualInt(fake.state.acceptedCount(), 1, "everything went over one connection")
  assertEqual(Str.joinList("|", fake.state.lines()), "version|mn|version", "what was sent")
  client.close()
  stopFake(fake)
}

test "a server older than 1.6.10 is refused" {
  val fake = startFakeWith(plainSetup().copy(versionLine = "VERSION 1.6.9"))
  val r = Client.connect(urlOf(fake))
  assertEqual(codeOf(r, "connect"), "UNSUPPORTED_SERVER", "1.6.9 has no base64 keys")
  assertTrue(
    Str.contains(messageOf(r), "1.6.9") and Str.contains(messageOf(r), "1.6.10"),
    "the message names both versions"
  )
  stopFake(fake)
  val older = startFakeWith(plainSetup().copy(versionLine = "VERSION 1.5.22"))
  assertEqual(codeOf(Client.connect(urlOf(older)), "connect"), "UNSUPPORTED_SERVER", "1.5")
  stopFake(older)
  val silent = startFakeWith(plainSetup().copy(versionLine = "ERROR"))
  assertEqual(codeOf(Client.connect(urlOf(silent)), "connect"), "UNSUPPORTED_SERVER", "a server without version")
  stopFake(silent)
}

test "newer and unjudgeable versions are accepted" {
  assertTrue(Client.versionSupported("1.6.10"), "the minimum")
  assertTrue(Client.versionSupported("1.6.24"), "a later patch")
  assertTrue(Client.versionSupported("1.7.0"), "a later minor")
  assertTrue(Client.versionSupported("2.0.0"), "a later major")
  assertTrue(Client.versionSupported("1.6.21-12-gabcdef"), "a build suffix")
  assertFalse(Client.versionSupported("1.6.9"), "one patch too old")
  assertFalse(Client.versionSupported("1.5.22"), "an older minor")
  assertFalse(Client.versionSupported("0.9.0"), "an older major")
  assertTrue(Client.versionSupported("unknown"), "a text that is not a version cannot be judged")
  assertEqual(Client.minimumServerVersion(), "1.6.10", "the documented minimum")
  val fake = startFakeWith(plainSetup().copy(versionLine = "VERSION compatible-proxy"))
  val client = clientOf(fake)
  assertEqual(client.serverVersion(), "compatible-proxy", "accepted as it is")
  client.close()
  stopFake(fake)
}

test "requests are meta commands and their replies, in order" {
  val fake = startFake()
  val client = clientOf(fake)
  val set = okReplies(client.run(Meta.encodeSet(wire("k"), encodeUtf8("value"), "T0"), 1), "ms")
  assertEqual(statuses(set), "HD", "stored")
  val parts: List[slice[Byte]] = newList()
  parts.add(Meta.encodeCommand("mg", wire("k"), "v s"))
  parts.add(Meta.encodeCommand("mg", wire("absent"), "v"))
  parts.add(Meta.encodeCommand("md", wire("k"), ""))
  parts.add(Meta.encodeCommand("md", wire("k"), ""))
  val replies = okReplies(client.run(Meta.concatAll(parts), 4), "four commands in one write")
  assertEqual(statuses(replies), "VA EN HD NF", "one reply per command")
  assertEqual(valueText(replies[0]), "value", "the value")
  match replies[0] {
    case Meta.Value(_, flags) -> match Meta.flagValue(flags, "s") {
      case Some(size) -> assertEqual(size, "5", "the size flag")
      case None -> assertTrue(false, "s was asked for")
    }
    case _ -> assertTrue(false, "a value")
  }
  client.close()
  stopFake(fake)
}

test "quiet commands are read up to the no-op" {
  val fake = startFake()
  val client = clientOf(fake)
  val _ = okReplies(client.run(Meta.encodeSet(wire("a"), encodeUtf8("1"), ""), 1), "ms")
  val _ = okReplies(client.run(Meta.encodeSet(wire("c"), encodeUtf8("3"), ""), 1), "ms")
  val parts: List[slice[Byte]] = newList()
  parts.add(Meta.encodeCommand("mg", wire("a"), "v q O0"))
  parts.add(Meta.encodeCommand("mg", wire("b"), "v q O1"))
  parts.add(Meta.encodeCommand("mg", wire("c"), "v q O2"))
  parts.add(Meta.encodeNoOp())
  val replies = okReplies(client.runUntilNoOp(Meta.concatAll(parts)), "quiet multi-get")
  assertEqual(statuses(replies), "VA VA", "the miss is not answered and the MN is not returned")
  assertEqual(valueText(replies[1]), "3", "the second hit")
  match replies[1] {
    case Meta.Value(_, flags) -> match Meta.flagValue(flags, "O") {
      case Some(o) -> assertEqual(o, "2", "the opaque token names the command")
      case None -> assertTrue(false, "O was sent")
    }
    case _ -> assertTrue(false, "a value")
  }
  val none = okReplies(client.runUntilNoOp(Meta.encodeNoOp()), "only the no-op")
  assertEqualInt(none.count, 0, "no replies before MN")
  ok(Client.ping(client), "the connection is in step afterwards")
  client.close()
  stopFake(fake)
}

test "a reply that arrives one byte at a time is put together" {
  val fake = startFake()
  val client = clientOf(fake)
  val big = Str.repeat("0123456789", 300)
  val _ = okReplies(client.run(Meta.encodeSet(wire("big"), encodeUtf8(big), ""), 1), "ms")
  fake.state.addFault(ReplySlowly)
  val replies = okReplies(client.run(Meta.encodeCommand("mg", wire("big"), "v t c"), 1), "mg")
  assertEqual(valueText(replies[0]), big, "3000 bytes, byte by byte")
  ok(Client.ping(client), "ping")
  assertEqualInt(client.connections(), 1, "still the first connection")
  client.close()
  stopFake(fake)
}

test "a value with CRLF and protocol words in it survives" {
  val fake = startFake()
  val client = clientOf(fake)
  val tricky = "line one\r\nMN\r\nVA 3\r\nEN\r\n"
  val _ = okReplies(client.run(Meta.encodeSet(wire("tricky"), encodeUtf8(tricky), ""), 1), "ms")
  val replies = okReplies(client.run(Meta.encodeCommand("mg", wire("tricky"), "v"), 1), "mg")
  assertEqual(valueText(replies[0]), tricky, "carried by its length")
  ok(Client.ping(client), "ping")
  client.close()
  stopFake(fake)
}

test "a CLIENT_ERROR is RequestRefused and the connection is replaced" {
  val fake = startFake()
  val client = clientOf(fake)
  // 252 base64 characters: one more group than memcached takes.
  val tooLong = wire(Str.repeat("k", 187))
  assertEqualInt(tooLong.length, 252, "over the 250 bytes of a key")
  val r = client.run(Meta.encodeSet(tooLong, encodeUtf8("x"), ""), 1)
  assertEqual(codeOfRun(r, "ms with a long key"), "REQUEST_REFUSED", "refused")
  assertTrue(Str.contains(messageOfRun(r), "bad command line format"), "memcached's words: " + messageOfRun(r))
  assertFalse(client.isConnected(), "the connection is closed: memcached read the data block as a command")
  ok(Client.ping(client), "the next request dials again")
  assertEqualInt(client.connections(), 2, "a second connection")
  val lines = fake.state.lines()
  assertEqual(lines[lines.count - 3], "x", "the fake, like memcached, took the data block for a command")
  client.close()
  stopFake(fake)
}

test "memcached's other refusals" {
  val fake = startFake()
  val client = clientOf(fake)
  val badFlag = client.run(encodeUtf8("mg " + wire("k") + " b Z\r\n"), 1)
  assertEqual(codeOfRun(badFlag, "an unknown flag"), "REQUEST_REFUSED", "invalid flag")
  assertTrue(Str.contains(messageOfRun(badFlag), "invalid flag"), "the reason")
  val badKey = client.run(encodeUtf8("mg !!! b v\r\n"), 1)
  assertTrue(Str.contains(messageOfRun(badKey), "error decoding key"), "a key that is not base64")
  val unknown = client.run(encodeUtf8("bogus\r\n"), 1)
  assertEqual(codeOfRun(unknown, "an unknown command"), "REQUEST_REFUSED", "ERROR")
  val badChunk = client.run(encodeUtf8("ms " + wire("k") + " 2 b\r\nxyzzy\r\n"), 1)
  assertTrue(Str.contains(messageOfRun(badChunk), "bad data chunk"), "data longer than announced")
  val badToken = client.run(encodeUtf8("ms " + wire("k") + " 1 b Tabc\r\nx\r\n"), 1)
  assertTrue(Str.contains(messageOfRun(badToken), "bad token"), "an expiry that is not a number")
  val badMode = client.run(encodeUtf8("ms " + wire("k") + " 1 b MX\r\nx\r\n"), 1)
  assertTrue(Str.contains(messageOfRun(badMode), "invalid mode"), "an unknown mode")
  val _ = okReplies(client.run(Meta.encodeSet(wire("text"), encodeUtf8("abc"), ""), 1), "ms")
  val notNumber = client.run(Meta.encodeCommand("ma", wire("text"), "v"), 1)
  assertTrue(Str.contains(messageOfRun(notNumber), "non-numeric"), "ma on text")
  ok(Client.ping(client), "the client recovers every time")
  client.close()
  stopFake(fake)
}

test "a SERVER_ERROR is a reply and the connection stays" {
  val fake = startFakeWith(plainSetup().copy(itemLimit = 2000))
  val client = clientOf(fake)
  val _ = okReplies(client.run(Meta.encodeSet(wire("k"), encodeUtf8("old"), ""), 1), "ms")
  val parts: List[slice[Byte]] = newList()
  parts.add(Meta.encodeSet(wire("k"), encodeUtf8(Str.repeat("x", 2000)), ""))
  parts.add(Meta.encodeCommand("mg", wire("k"), "v"))
  val replies = okReplies(client.run(Meta.concatAll(parts), 2), "too large, then a read")
  assertEqual(statuses(replies), "SERVER_ERROR EN", "refused, and the old value is gone as in memcached")
  match replies[0] {
    case Meta.ServerError(m) -> assertEqual(m, "object too large for cache", "memcached's words")
    case _ -> assertTrue(false, "a SERVER_ERROR")
  }
  assertEqualInt(client.connections(), 1, "the connection is kept: the server swallowed the data block")
  client.close()
  stopFake(fake)
}

test "arithmetic: ma counts, starts a counter when told to, and misses otherwise" {
  val fake = startFake()
  val client = clientOf(fake)
  assertEqual(statuses(okReplies(client.run(Meta.encodeCommand("ma", wire("n"), "v"), 1), "ma")), "NF", "no counter")
  val started = okReplies(client.run(Meta.encodeCommand("ma", wire("n"), "N0 J100 v"), 1), "ma with autovivify")
  assertEqual(valueText(started[0]), "100", "the initial value")
  assertEqual(valueText(okReplies(client.run(Meta.encodeCommand("ma", wire("n"), "v"), 1), "ma")[0]), "101", "plus one")
  assertEqual(
    valueText(okReplies(client.run(Meta.encodeCommand("ma", wire("n"), "v D9"), 1), "ma")[0]),
    "110",
    "plus nine"
  )
  assertEqual(statuses(okReplies(client.run(Meta.encodeCommand("ma", wire("n"), ""), 1), "ma")), "HD", "without v")
  assertEqual(
    valueText(okReplies(client.run(Meta.encodeCommand("mg", wire("n"), "v"), 1), "mg")[0]),
    "111",
    "stored as text"
  )
  client.close()
  stopFake(fake)
}

test "flushAll empties the whole server" {
  val fake = startFake()
  val client = clientOf(fake)
  val _ = okReplies(client.run(Meta.encodeSet(wire("a"), encodeUtf8("1"), ""), 1), "ms")
  val _ = okReplies(client.run(Meta.encodeSet(wire("b"), encodeUtf8("2"), ""), 1), "ms")
  assertEqualInt(fake.state.itemCount(), 2, "two items")
  ok(Client.flushAll(client), "flush_all")
  assertEqualInt(fake.state.itemCount(), 0, "none")
  client.close()
  stopFake(fake)
}

// ─── Connection handling ─────────────────────────────────────────────────────
test "a connection the server closed while idle is replaced and the request repeated once" {
  val fake = startFake()
  val client = clientOf(fake)
  fake.state.addFault(ReplyThenClose)
  ok(Client.ping(client), "answered, then the server closes")
  sleepMillis(100)
  ok(Client.ping(client), "the stale connection is noticed and the request repeated")
  assertEqualInt(client.connections(), 2, "on a second connection")
  client.close()
  stopFake(fake)
}

test "without the retry a stale connection is ConnectionLost, and the next request dials" {
  val fake = startFake()
  val client = ok(Client.open(Client.withStaleRetry(configOf(fake), false)), "open")
  fake.state.addFault(ReplyThenClose)
  ok(Client.ping(client), "answered, then the server closes")
  sleepMillis(100)
  assertEqual(codeOf(Client.ping(client), "ping on the stale connection"), "CONNECTION_LOST", "reported")
  ok(Client.ping(client), "the next request dials again")
  assertEqualInt(client.connections(), 2, "a second connection")
  client.close()
  stopFake(fake)
}

test "a request is repeated once, not forever" {
  val fake = startFake()
  val client = clientOf(fake)
  ok(Client.ping(client), "ping")
  fake.state.addFault(CloseNow)
  fake.state.addFault(CloseNow)
  assertEqual(codeOf(Client.ping(client), "ping"), "CONNECTION_LOST", "closed twice without a reply")
  assertEqualInt(client.connections(), 2, "one retry, on one new connection")
  ok(Client.ping(client), "the client is usable afterwards")
  client.close()
  stopFake(fake)
}

test "a connection that breaks in the middle of a reply is not retried" {
  val fake = startFake()
  val client = clientOf(fake)
  fake.state.addFault(SendThenClose(text = "VA 10\r\nabc"))
  val r = client.run(Meta.encodeCommand("mg", wire("k"), "v"), 1)
  assertEqual(codeOfRun(r, "mg"), "CONNECTION_LOST", "half a reply")
  assertEqualInt(linesWith(fake, "mg ").count, 1, "the command was sent once")
  assertFalse(client.isConnected(), "the connection is dropped")
  ok(Client.ping(client), "and replaced")
  client.close()
  stopFake(fake)
}

test "bytes that are not memcached are a protocol violation" {
  val fake = startFake()
  val client = clientOf(fake)
  fake.state.addFault(SendInstead(text = "+PONG\r\n"))
  assertEqual(codeOf(Client.ping(client), "ping"), "PROTOCOL_VIOLATION", "a Redis reply")
  assertFalse(client.isConnected(), "the stream cannot be trusted any more")
  fake.state.addFault(SendInstead(text = "HD\r\n"))
  assertEqual(codeOf(Client.ping(client), "ping"), "UNEXPECTED_REPLY", "a reply mn does not produce")
  fake.state.addFault(SendInstead(text = "SERVER_ERROR out of memory storing object\r\n"))
  val failed = Client.ping(client)
  assertEqual(codeOf(failed, "ping"), "SERVER_FAILURE", "a SERVER_ERROR where none is expected")
  assertTrue(Str.contains(messageOf(failed), "out of memory"), "with the server's words")
  client.close()
  stopFake(fake)
}

test "a silent server trips the I/O timeout" {
  val fake = startFake()
  val client = ok(Client.open(Client.withTimeouts(configOf(fake), 2000, 300)), "open")
  fake.state.addFault(Hang(millis = 1500))
  val started = nowEpochMillis()
  val r = Client.ping(client)
  val waited = nowEpochMillis() - started
  assertEqual(codeOf(r, "ping"), "CONNECTION_LOST", "no reply")
  assertTrue(Str.contains(messageOf(r), "timed out"), "the reason: " + messageOf(r))
  assertTrue(waited >= 250i64 and waited < 1400i64, "after about 300 ms, not when the server gave up")
  client.close()
  stopFake(fake)
}

// A loopback port with nothing listening: bound, read back, released.
func closedPort(): Int {
  match KT.serverListen("127.0.0.1", 0) {
    case Err(m) -> panic("cannot reserve a port: " + m)
    case Ok(server) -> {
      val port = KT.serverPort(server)
      KT.serverClose(server)
      port
    }
  }
}

test "nothing listening is ConnectFailed, at connect or at the first request of a lazy client" {
  val port = closedPort()
  val url = "memcached://127.0.0.1:" + port.toString()
  val r = Client.connect(url)
  assertEqual(codeOf(r, "connect"), "CONNECT_FAILED", "connect")
  assertTrue(Str.contains(messageOf(r), "127.0.0.1:" + port.toString()), "the message names the endpoint")
  val lazyClient = Client.lazy(Client.configFor("127.0.0.1", port))
  assertFalse(lazyClient.isConnected(), "lazy does not dial")
  assertEqual(codeOf(Client.ping(lazyClient), "ping"), "CONNECT_FAILED", "the first request does")
  lazyClient.close()
}

test "disconnect redials, close is final" {
  val fake = startFake()
  val client = clientOf(fake)
  client.disconnect()
  assertFalse(client.isConnected(), "disconnected")
  ok(Client.ping(client), "the next request dials")
  assertEqualInt(client.connections(), 2, "a second connection")
  client.close()
  assertEqual(codeOf(Client.ping(client), "ping"), "CLIENT_CLOSED", "closed for good")
  assertEqual(codeOf(client.ensureConnected(), "ensureConnected"), "CLIENT_CLOSED", "no reconnect either")
  client.close()
  stopFake(fake)
}

test "an unusable configuration is reported before dialling" {
  assertEqual(codeOf(Client.connect("redis://127.0.0.1"), "connect"), "INVALID_CONFIG", "another scheme")
  assertEqual(codeOf(Client.connect("memcached://"), "connect"), "INVALID_CONFIG", "no host")
  assertEqual(codeOf(Client.connect("memcached://a,b"), "connect"), "INVALID_CONFIG", "a server list")
  val port = closedPort()
  val plain = Client.configFor("127.0.0.1", port)
  val namedOnly = Client.withTlsOptions(plain, NetTls.plainTcp().withTlsServerName("cache.internal"))
  val r = Client.open(namedOnly)
  assertEqual(codeOf(r, "open"), "INVALID_CONFIG", "TLS options without TLS")
  assertTrue(Str.contains(messageOf(r), "memcached+tls://"), "the message says how to switch TLS on")
  val badCa = Client.withTlsOptions(plain, NetTls.plainTcp().withTls().withExclusiveCaCertificate("not a certificate"))
  assertEqual(codeOf(Client.open(badCa), "open"), "INVALID_CONFIG", "an unusable CA")
  assertEqual(
    codeOf(Client.checkConfig(badCa), "checkConfig"),
    "INVALID_CONFIG",
    "checkConfig finds it without connecting"
  )
  val spaced = Client.withCredentials(plain, "app", "two words")
  val spacedResult = Client.open(spaced)
  assertEqual(codeOf(spacedResult, "open"), "INVALID_CONFIG", "a password memcached cannot accept")
  assertFalse(Str.contains(messageOf(spacedResult), "two words"), "the message does not show the password")
  assertEqual(
    codeOf(Client.open(Client.withCredentials(plain, "a:b", "secret")), "open"),
    "INVALID_CONFIG",
    "a user name with a colon"
  )
  ok(Client.checkConfig(Client.withCredentials(plain, "app", "pa:ss")), "a colon in the password is fine")
  assertPanics("a user name without a password", { -> val _ = Client.withCredentials(plain, "app", "") })
  assertPanics("a value limit of zero", { -> val _ = Client.withMaxValueBytes(plain, 0) })
}

// ─── Authentication ──────────────────────────────────────────────────────────
func authSetup(): FakeSetup {
  plainSetup().copy(authUser = "app", authPassword = "s3cret")
}

test "credentials in the URL authenticate before anything else" {
  val fake = startFakeWith(authSetup())
  val client = ok(Client.connect("memcached://app:s3cret@127.0.0.1:" + fake.port.toString()), "connect")
  ok(Client.ping(client), "ping")
  val lines = fake.state.lines()
  assertEqual(lines[0], "set auth 0 -1 10", "the authentication set comes first, born expired")
  assertEqual(lines[1], "version", "then the version check")
  client.disconnect()
  ok(Client.ping(client), "a new connection authenticates again")
  assertEqualInt(linesWith(fake, "set auth").count, 2, "once per connection")
  client.close()
  stopFake(fake)
}

test "wrong or missing credentials are AuthFailed" {
  val fake = startFakeWith(authSetup())
  val at = "@127.0.0.1:" + fake.port.toString()
  val wrong = Client.connect("memcached://app:wrong" + at)
  assertEqual(codeOf(wrong, "connect"), "AUTH_FAILED", "a wrong password")
  assertFalse(Str.contains(messageOf(wrong), "wrong"), "the message does not show the password")
  assertEqual(codeOf(Client.connect("memcached://nobody:s3cret" + at), "connect"), "AUTH_FAILED", "a wrong user")
  val missing = Client.connect(urlOf(fake))
  assertEqual(codeOf(missing, "connect"), "AUTH_FAILED", "no credentials")
  assertTrue(Str.contains(messageOf(missing), "requires authentication"), "the message says what is missing")
  stopFake(fake)
}

test "credentials sent to a server without authentication leave nothing readable" {
  val fake = startFake()
  val client = ok(Client.connect("memcached://app:s3cret@127.0.0.1:" + fake.port.toString()), "connect")
  ok(Client.ping(client), "the server answers STORED and carries on")
  assertEqual(statuses(okReplies(client.run(encodeUtf8("mg auth v\r\n"), 1), "mg auth")), "EN", "no item named auth")
  client.close()
  stopFake(fake)
}

// ─── TLS ─────────────────────────────────────────────────────────────────────
test "a memcached+tls URL with the CA runs the protocol inside TLS" {
  val fake = startFakeWith(plainSetup().copy(tls = true))
  val fromUrl = ok(Client.configFromUrl("memcached+tls://localhost:" + fake.port.toString()), "the URL")
  assertTrue(fromUrl.tls.enabled, "the scheme switches TLS on")
  val trusted = Client.withTlsOptions(fromUrl, fromUrl.tls.withExclusiveCaCertificate(caPem))
  val store = ok(MemcachedCache.connectWith(trusted, MemcachedCache.options("tls")), "connectWith")
  store.set("k", "over TLS", 60)
  expectHit(store.get("k"), "over TLS", "a round trip inside TLS")
  assertEqualInt(store.errors.errorCount(), 0, "no failure")
  store.client.close()
  val byAddress = Client.withTlsOptions(
    Client.configFor("127.0.0.1", fake.port),
    NetTls.plainTcp().withTls().withCaCertificate(caPem)
  )
  val second = ok(Client.open(byAddress), "the certificate also names 127.0.0.1")
  ok(Client.ping(second), "ping")
  second.close()
  stopFake(fake)
}

test "without the CA the server certificate is not trusted, and the name must match" {
  val fake = startFakeWith(plainSetup().copy(tls = true))
  val url = "memcached+tls://localhost:" + fake.port.toString()
  assertEqual(codeOf(Client.connect(url), "connect"), "TLS_FAILED", "the fixture CA is not a system CA")
  assertEqual(
    codeOf(MemcachedCache.connect(url, MemcachedCache.options("tls")), "connect"),
    "TLS_FAILED",
    "nor for a store"
  )
  val fromUrl = ok(Client.configFromUrl(url), "the URL")
  val otherName = Client.withTlsOptions(
    fromUrl,
    fromUrl.tls.withExclusiveCaCertificate(caPem).withTlsServerName("cache.example")
  )
  assertEqual(codeOf(Client.open(otherName), "open"), "TLS_FAILED", "the certificate does not name cache.example")
  stopFake(fake)
}

test "a plain client on a TLS port and a TLS client on a plain port both fail cleanly" {
  val tlsFake = startFakeWith(plainSetup().copy(tls = true))
  val plainClient = Client.open(Client.withTimeouts(Client.configFor("127.0.0.1", tlsFake.port), 2000, 1500))
  assertTrue(
    codeOf(plainClient, "open") == "CONNECTION_LOST" or codeOf(plainClient, "open") == "PROTOCOL_VIOLATION",
    "a plain client gets no memcached reply: " + codeOf(plainClient, "open")
  )
  stopFake(tlsFake)
  val plainFake = startFake()
  val tlsConfig = Client.withTlsOptions(
    Client.withTimeouts(Client.configFor("127.0.0.1", plainFake.port), 2000, 1500),
    NetTls.plainTcp().withTls().withExclusiveCaCertificate(caPem)
  )
  assertEqual(codeOf(Client.open(tlsConfig), "open"), "TLS_FAILED", "the handshake gets no answer")
  stopFake(plainFake)
}

// ─── The store: conformance ──────────────────────────────────────────────────
test "MemcachedCacheStore conforms to CacheStore" {
  val fake = startFake()
  val stores: List[MemcachedCache.MemcachedCacheStore] = newList()
  val factory: () -> Cache.CacheStore = { ->
    val store = storeOn(fake, freshNamespace())
    stores.add(store)
    val asInterface: Cache.CacheStore = store
    asInterface
  }
  // The suite's clock is the fake server's: expiry is decided there.
  requireConformance(CacheConformance.run(factory, { seconds -> fake.state.advance(seconds) }))
  assertTrue(stores.count >= 12, "the suite built a store per case")
  for store in stores {
    assertEqualInt(store.errors.errorCount(), 0, "no memcached failure was swallowed during conformance")
    store.client.close()
  }
  assertEqualInt(linesWith(fake, "flush_all").count, 0, "clear never flushes the server")
  stopFake(fake)
}

test "the suite passes for stores that share one connection and one server" {
  val fake = startFake()
  val client = clientOf(fake)
  val stores: List[MemcachedCache.MemcachedCacheStore] = newList()
  val factory: () -> Cache.CacheStore = { ->
    val store = MemcachedCache.fromClient(client, MemcachedCache.options(freshNamespace()))
    stores.add(store)
    val asInterface: Cache.CacheStore = store
    asInterface
  }
  requireConformance(CacheConformance.run(factory, { seconds -> fake.state.advance(seconds) }))
  for store in stores {
    assertEqualInt(store.errors.errorCount(), 0, "no failure")
  }
  assertEqualInt(client.connections(), 1, "one connection carried every store")
  client.close()
  stopFake(fake)
}

// ─── The store: what goes over the wire ──────────────────────────────────────
test "an entry is one item under namespace, generation and key, with memcached's expiry" {
  val fake = startFake()
  val store = storeOn(fake, "app:cache")
  store.set("users:42", "v", 300)
  val generation = generationOn(fake, "app:cache")
  assertTrue(Keys.isDecimal(generation) and generation.length >= 16, "the generation is a number: " + generation)
  val sets = linesWith(fake, "ms ")
  assertEqualInt(sets.count, 2, "the start of the generation and the entry")
  assertEqual(storedKeyOf(sets[0]), "app:cache generation", "the generation item")
  assertTrue(sets[0].endsWith(" b ME T0"), "is added, never overwritten, and does not expire: " + sets[0])
  assertEqual(storedKeyOf(sets[1]), "app:cache " + generation + " users:42", "the entry's key")
  assertTrue(sets[1].endsWith(" 1 b T300"), "one byte, base64 key, 300 seconds: " + sets[1])
  expectHit(fake.state.stored("app:cache " + generation + " users:42"), "v", "the item holds the value as it is")
  fake.state.forgetLines()
  store.set("forever", "v", 0)
  assertTrue(linesWith(fake, "ms ")[0].endsWith(" b T0"), "ttlSeconds = 0 is sent as T0")
  fake.state.forgetLines()
  expectHit(store.get("users:42"), "v", "get")
  val lines = fake.state.lines()
  assertEqualInt(lines.count, 2, "a get is two commands")
  assertEqual(lines[0], "mg " + wire("app:cache generation") + " b v", "the generation is read")
  assertEqual(
    lines[1],
    "mg " + wire("app:cache " + generation + " users:42") + " b v",
    "with the entry, in the same request"
  )
  fake.state.forgetLines()
  store.delete("users:42")
  assertEqual(fake.state.lines()[1], "md " + wire("app:cache " + generation + " users:42") + " b", "delete")
  fake.state.forgetLines()
  store.clear()
  assertEqual(
    Str.joinList("|", fake.state.lines()),
    "ma " + wire("app:cache generation") + " b v",
    "clear is one increment"
  )
  assertEqualInt(store.errors.errorCount(), 0, "no failure")
  store.client.close()
  stopFake(fake)
}

test "a TTL above 30 days is sent as an absolute timestamp and still means that many seconds" {
  val fake = startFake()
  val store = storeOn(fake, "long")
  val fortyDays = 40 * 86400
  val before = nowEpochMillis() / 1000i64
  store.set("k", "v", fortyDays)
  val after = nowEpochMillis() / 1000i64
  val line = linesWith(fake, "ms ")[1]
  val token = match parseOptLong(Str.substring(line, Str.lastIndexOfRaw(line, "T") + 1)) {
    case Some(n) -> n
    case None -> panic("no expiry in: " + line)
  }
  assertTrue(
    token >= before + fortyDays.toLong() and token <= after + fortyDays.toLong(),
    "the expiry is now + 40 days as a timestamp: " + token.toString()
  )
  match ok(Extras.lifetimeOf(store, "k"), "lifetimeOf") {
    case Extras.Expiring(seconds) -> assertTrue(
      seconds > fortyDays.toLong() - 5i64 and seconds <= fortyDays.toLong(),
      "the server counts 40 days: " + seconds.toString()
    )
    case _ -> assertTrue(false, "the entry has an expiry")
  }
  fake.state.advance(39 * 86400)
  expectHit(store.get("k"), "v", "present after 39 days")
  fake.state.advance(86400 + 5)
  expectMiss(store.get("k"), "gone after 40 days")
  // What a client would get by sending the number as it is: memcached reads
  // 3456000 as a day in 1970.
  val generation = generationOn(fake, "long")
  val naive = wire("long " + generation + " naive")
  val _ = okReplies(store.client.run(Meta.encodeSet(naive, encodeUtf8("v"), "T" + fortyDays.toString()), 1), "ms")
  assertEqual(
    statuses(okReplies(store.client.run(Meta.encodeCommand("mg", naive, "v"), 1), "mg")),
    "EN",
    "the unmapped TTL expires at once"
  )
  assertEqualInt(store.errors.errorCount(), 0, "no failure")
  store.client.close()
  stopFake(fake)
}

test "30 days exactly is relative, and the longest TTL does not wrap around" {
  val fake = startFake()
  val store = storeOn(fake, "edge")
  store.set("month", "v", 2592000)
  assertTrue(linesWith(fake, "ms ")[1].endsWith(" T2592000"), "sent as seconds")
  fake.state.advance(2592000 - 10)
  expectHit(store.get("month"), "v", "present just before 30 days")
  fake.state.advance(20)
  expectMiss(store.get("month"), "gone after 30 days")
  fake.state.forgetLines()
  store.set("eternal", "v", 2147483647)
  assertTrue(linesWith(fake, "ms ")[0].endsWith(" T2147483647"), "lowered to the last timestamp memcached holds")
  expectHit(store.get("eternal"), "v", "present, not wrapped into the past")
  match ok(Extras.lifetimeOf(store, "eternal"), "lifetimeOf") {
    case Extras.Expiring(seconds) -> assertTrue(seconds > 86400i64 * 365i64, "years to live")
    case _ -> assertTrue(false, "the entry has an expiry")
  }
  store.client.close()
  stopFake(fake)
}

// ─── The store: namespaces and generations ───────────────────────────────────
test "clear empties its namespace and leaves the others and foreign items alone" {
  val fake = startFake()
  val client = clientOf(fake)
  val a = MemcachedCache.fromClient(client, MemcachedCache.options("a"))
  val b = MemcachedCache.fromClient(client, MemcachedCache.options("b"))
  // A namespace that extends another's name.
  val ab = MemcachedCache.fromClient(client, MemcachedCache.options("a:b"))
  fake.state.plant("someone-elses-key", encodeUtf8("keep"))
  a.set("k", "in a", 0)
  a.set("ttl", "in a", 600)
  b.set("k", "in b", 0)
  ab.set("k", "in a:b", 0)
  a.clear()
  expectMiss(a.get("k"), "the cleared namespace is empty")
  expectMiss(a.get("ttl"), "the entry with a TTL too")
  expectHit(b.get("k"), "in b", "another namespace is untouched")
  expectHit(ab.get("k"), "in a:b", "so is one whose name starts the same")
  expectHit(fake.state.stored("someone-elses-key"), "keep", "and an item of another application")
  a.set("k", "again", 0)
  expectHit(a.get("k"), "again", "the cleared namespace takes entries again")
  assertEqualInt(a.errors.errorCount() + b.errors.errorCount() + ab.errors.errorCount(), 0, "no failure")
  client.close()
  stopFake(fake)
}

test "a clear by one store is seen by the next operation of every other store" {
  val fake = startFake()
  val first = storeOn(fake, "shared")
  val second = storeOn(fake, "shared")
  first.set("k", "v", 0)
  expectHit(second.get("k"), "v", "stores with one namespace share their entries")
  expectHit(second.get("k"), "v", "the second store now knows the generation")
  first.clear()
  fake.state.forgetLines()
  expectMiss(second.get("k"), "the other store's very next read misses")
  assertEqualInt(
    linesWith(fake, "mg ").count,
    4,
    "one request with the old generation, found stale, and one with the new"
  )
  second.set("k", "new", 0)
  expectHit(first.get("k"), "new", "and what it writes is seen by the first")
  val before = generationOn(fake, "shared")
  second.clear()
  assertTrue(generationOn(fake, "shared") != before, "every clear moves the generation")
  first.delete("k")
  expectMiss(first.get("k"), "nothing after the second clear")
  assertEqualInt(first.errors.errorCount() + second.errors.errorCount(), 0, "no failure")
  first.client.close()
  second.client.close()
  stopFake(fake)
}

test "a write that raced a clear lands in the old generation and is never read" {
  val fake = startFake()
  val writer = storeOn(fake, "race")
  val clearer = storeOn(fake, "race")
  writer.set("k", "v1", 0)
  val oldGeneration = generationOn(fake, "race")
  clearer.clear()
  // The writer still holds the old generation: its request is answered with
  // the new one, and the set is repeated there.
  writer.set("k", "v2", 0)
  val newGeneration = generationOn(fake, "race")
  expectHit(fake.state.stored("race " + newGeneration + " k"), "v2", "the repeated set is in the new generation")
  expectHit(clearer.get("k"), "v2", "where every store reads")
  assertTrue(oldGeneration != newGeneration, "two generations")
  writer.client.close()
  clearer.client.close()
  stopFake(fake)
}

test "when the generation item is evicted the namespace is empty, and a new generation never repeats an old one" {
  val fake = startFake()
  val store = storeOn(fake, "evicted")
  store.set("k", "old", 0)
  val oldGeneration = generationOn(fake, "evicted")
  fake.state.evict("evicted generation")
  expectMiss(store.get("k"), "without a generation nothing can be read")
  store.delete("k")
  store.clear()
  assertEqual(generationOn(fake, "evicted"), "", "reads, deletes and clears do not start a generation")
  expectHit(fake.state.stored("evicted " + oldGeneration + " k"), "old", "the orphan is still in the server")
  store.set("other", "new", 0)
  val newGeneration = generationOn(fake, "evicted")
  assertTrue(newGeneration.length > 0 and newGeneration != oldGeneration, "the next write starts another generation")
  expectHit(store.get("other"), "new", "which works")
  expectMiss(store.get("k"), "and the orphan stays out of reach")
  assertEqualInt(store.errors.errorCount(), 0, "an evicted generation is not a failure")
  // A second store that still holds the old generation finds out too.
  store.client.close()
  stopFake(fake)
}

test "two stores that start a namespace at once agree on one generation" {
  val fake = startFake()
  val first = storeOn(fake, "start")
  val second = storeOn(fake, "start")
  // Both find no generation; the second's add is answered NS and it reads
  // the first's.
  expectMiss(first.get("k"), "empty")
  expectMiss(second.get("k"), "empty")
  first.set("a", "1", 0)
  second.set("b", "2", 0)
  assertEqualInt(linesWith(fake, "ms " + wire("start generation")).count, 1, "the generation was started once")
  expectHit(first.get("b"), "2", "one generation")
  expectHit(second.get("a"), "1", "for both")
  fake.state.evict("start generation")
  fake.state.plant("start generation", encodeUtf8("5"))
  first.set("c", "3", 0)
  expectHit(fake.state.stored("start 5 c"), "3", "a generation another process started is adopted")
  first.client.close()
  second.client.close()
  stopFake(fake)
}

test "a generation item that holds something else is reported, not trusted" {
  val fake = startFake()
  val store = storeOn(fake, "tampered")
  fake.state.plant("tampered generation", encodeUtf8("not a number"))
  val r = MemcachedCache.tryGet(store, "k")
  assertEqual(codeOf(r, "tryGet"), "UNEXPECTED_REPLY", "a get")
  assertTrue(Str.contains(messageOf(r), "tampered generation"), "the message names the item: " + messageOf(r))
  assertEqual(codeOf(MemcachedCache.trySet(store, "k", "v", 0), "trySet"), "UNEXPECTED_REPLY", "a set")
  assertEqual(
    codeOf(MemcachedCache.tryClear(store), "tryClear"),
    "REQUEST_REFUSED",
    "memcached refuses to increment it"
  )
  expectMiss(store.get("k"), "the interface degrades to a miss")
  assertEqualInt(store.errors.errorCount(), 1, "and counts the failure")
  store.client.close()
  stopFake(fake)
}

test "a namespace cleared without pause while one operation runs is NamespaceContended" {
  val fake = startFake()
  val store = storeOn(fake, "busy")
  store.set("k", "v", 0)
  fake.state.setBumpGenerationOnRead(true)
  val r = MemcachedCache.tryGet(store, "k")
  assertEqual(codeOf(r, "tryGet"), "NAMESPACE_CONTENDED", "every attempt found a newer generation")
  fake.state.setBumpGenerationOnRead(false)
  expectMiss(store.get("k"), "afterwards the store works, and the old entry is out of reach")
  store.set("k", "v", 0)
  expectHit(store.get("k"), "v", "a new entry")
  store.client.close()
  stopFake(fake)
}

test "an unusable namespace is refused" {
  assertPanics("empty", { -> val _ = MemcachedCache.options("") })
  assertPanics("with a space", { -> val _ = MemcachedCache.options("my app") })
  assertPanics("too long", { -> val _ = MemcachedCache.options(Str.repeat("n", 81)) })
  assertEqual(
    codeOf(MemcachedCache.optionsFor("my app"), "optionsFor"),
    "INVALID_CONFIG",
    "as a value for configuration"
  )
  assertEqual(ok(MemcachedCache.optionsFor("my-app"), "optionsFor").namespace, "my-app", "a usable one")
}

// ─── The store: keys and values ──────────────────────────────────────────────
test "any text is a key: spaces, control characters, non-ASCII, and keys far over 250 bytes" {
  val fake = startFake()
  val store = storeOn(fake, "keys")
  val keys: List[String] = newList()
  keys.add("plain")
  keys.add("with spaces and\ttabs")
  keys.add("line\r\nbreaks\r\nmn\r\n")
  keys.add("café 世界 🙂")
  keys.add(Str.repeat("k", 250))
  keys.add(Str.repeat("k", 251))
  keys.add(Str.repeat("世", 2000))
  keys.add(Str.repeat("k", 5000) + "a")
  keys.add(Str.repeat("k", 5000) + "b")
  var i = 0
  while i < keys.count {
    store.set(keys[i], "value " + i.toString(), 0)
    i = i + 1
  }
  i = 0
  while i < keys.count {
    expectHit(store.get(keys[i]), "value " + i.toString(), "key " + i.toString())
    i = i + 1
  }
  for line in linesWith(fake, "ms ") {
    assertTrue(line.split(" ")[1].length <= 250, "every key on the wire fits memcached's limit")
  }
  store.delete(keys[7])
  expectMiss(store.get(keys[7]), "a long key is deleted")
  expectHit(store.get(keys[8]), "value 8", "its neighbour, equal for 5000 characters, is not")
  assertEqualInt(store.errors.errorCount(), 0, "no failure")
  store.client.close()
  stopFake(fake)
}

test "a value over the client's limit is ValueTooLarge, is not sent, and removes the old entry" {
  val fake = startFake()
  val store = storeOn(fake, "large")
  store.set("k", "old", 0)
  fake.state.forgetLines()
  val tooBig = Str.repeat("x", 1048577)
  val r = MemcachedCache.trySet(store, "k", tooBig, 0)
  assertEqual(codeOf(r, "trySet"), "VALUE_TOO_LARGE", "one byte over 1 MiB")
  assertTrue(
    Str.contains(messageOf(r), "1048577") and Str.contains(messageOf(r), "1048576"),
    "size and limit: " + messageOf(r)
  )
  assertEqualInt(linesWith(fake, "ms ").count, 0, "the megabyte was never sent")
  assertEqualInt(linesWith(fake, "md ").count, 1, "the old entry was deleted")
  expectMiss(store.get("k"), "a failed write does not leave the old value to be read")
  store.set("k", tooBig, 0)
  assertEqualInt(store.errors.errorCount(), 1, "through the interface the failure is counted")
  match store.errors.lastError() {
    case Some(e) -> assertEqual(e.code(), "VALUE_TOO_LARGE", "and kept")
    case None -> assertTrue(false, "the failure must be recorded")
  }
  store.client.close()
  stopFake(fake)
}

test "the server's own limit is ValueTooLarge too, whatever the client was told" {
  val fake = startFake()
  val generous = Client.withMaxValueBytes(configOf(fake), 8 * 1048576)
  val store = ok(MemcachedCache.connectWith(generous, MemcachedCache.options("large")), "connectWith")
  store.set("k", "old", 0)
  // key + value + 59 bytes of item header must fit in 1 MiB.
  val storedKey = "large " + generationOn(fake, "large") + " k"
  val largest = 1048576 - 59 - storedKey.length
  ok(MemcachedCache.trySet(store, "k", Str.repeat("x", largest), 0), "the largest value the server takes")
  val r = MemcachedCache.trySet(store, "k", Str.repeat("x", largest + 1), 0)
  assertEqual(codeOf(r, "trySet"), "VALUE_TOO_LARGE", "one byte more")
  assertTrue(Str.contains(messageOf(r), "object too large for cache"), "with memcached's words: " + messageOf(r))
  expectMiss(store.get("k"), "memcached drops the old entry when it refuses the new one")
  assertEqualInt(store.client.connections(), 1, "the connection survives")
  store.client.close()
  stopFake(fake)
}

test "a value that is not UTF-8 text, stored by another client, is reported" {
  val fake = startFake()
  val store = storeOn(fake, "foreign")
  store.set("k", "text", 0)
  val bytes: List[Byte] = newList()
  bytes.add(255.toByte())
  bytes.add(254.toByte())
  fake.state.plant("foreign " + generationOn(fake, "foreign") + " k", bytes.toArray())
  assertEqual(codeOf(MemcachedCache.tryGet(store, "k"), "tryGet"), "UNEXPECTED_REPLY", "bytes that are not text")
  expectMiss(store.get("k"), "through the interface: a miss")
  assertEqualInt(store.errors.errorCount(), 1, "and a counted failure")
  store.client.close()
  stopFake(fake)
}

// ─── The store: failures ─────────────────────────────────────────────────────
func unreachableStore(): MemcachedCache.MemcachedCacheStore {
  val client = Client.lazy(Client.configFor("127.0.0.1", closedPort()))
  MemcachedCache.fromClient(client, MemcachedCache.options("unit"))
}

test "with memcached unreachable a get is a miss, writes are dropped, and each failure is recorded" {
  val store = unreachableStore()
  assertEqualInt(store.errors.errorCount(), 0, "no error before the first call")
  expectMiss(store.get("k"), "an unreachable server cannot produce a hit")
  assertEqualInt(store.errors.errorCount(), 1, "the failed get was counted")
  match store.errors.lastError() {
    case Some(e) -> assertEqual(e.code(), "CONNECT_FAILED", "the recorded error")
    case None -> assertTrue(false, "the failure must be recorded")
  }
  store.set("k", "v", 0)
  store.set("k", "v", 30)
  store.delete("k")
  store.clear()
  assertEqualInt(store.errors.errorCount(), 5, "each failed write was counted")
  val asInterface: Cache.CacheStore = store
  Cache.set(asInterface, "k", "v")
  expectMiss(Cache.get(asInterface, "k"), "nothing was stored")
  assertEqualInt(store.errors.errorCount(), 7, "the wrappers go through the same store")
}

test "the Result-returning functions report the failure and leave the error log alone" {
  val store = unreachableStore()
  assertEqual(codeOf(MemcachedCache.tryGet(store, "k"), "tryGet"), "CONNECT_FAILED", "tryGet")
  assertEqual(codeOf(MemcachedCache.trySet(store, "k", "v", 10), "trySet"), "CONNECT_FAILED", "trySet")
  assertEqual(codeOf(MemcachedCache.tryDelete(store, "k"), "tryDelete"), "CONNECT_FAILED", "tryDelete")
  assertEqual(codeOf(MemcachedCache.tryClear(store), "tryClear"), "CONNECT_FAILED", "tryClear")
  assertEqual(codeOf(MemcachedCache.ping(store), "ping"), "CONNECT_FAILED", "ping")
  assertEqual(codeOf(Extras.addIfAbsent(store, "k", "v", 0), "addIfAbsent"), "CONNECT_FAILED", "addIfAbsent")
  assertEqual(codeOf(Extras.getVersioned(store, "k"), "getVersioned"), "CONNECT_FAILED", "getVersioned")
  assertEqual(
    codeOf(Extras.compareAndSwap(store, "k", "v", 0, "1"), "compareAndSwap"),
    "CONNECT_FAILED",
    "compareAndSwap"
  )
  assertEqual(codeOf(Extras.getMany(store, ["a", "b"]), "getMany"), "CONNECT_FAILED", "getMany")
  assertEqual(codeOf(Extras.touch(store, "k", 10), "touch"), "CONNECT_FAILED", "touch")
  assertEqual(codeOf(Extras.lifetimeOf(store, "k"), "lifetimeOf"), "CONNECT_FAILED", "lifetimeOf")
  assertEqualInt(store.errors.errorCount(), 0, "the Result functions do not touch the error log")
}

test "the store keeps working across a connection the server closed" {
  val fake = startFake()
  val store = storeOn(fake, "stale")
  store.set("k", "v", 0)
  // The read of the generation passes; the read of the entry is answered and
  // the connection closed after it.
  fake.state.addFault(Pass)
  fake.state.addFault(ReplyThenClose)
  expectHit(store.get("k"), "v", "answered, then the server closes")
  sleepMillis(100)
  expectHit(store.get("k"), "v", "the next operation is repeated on a new connection")
  assertEqualInt(store.client.connections(), 2, "a second connection")
  assertEqualInt(store.errors.errorCount(), 0, "and nothing is counted as a failure")
  store.client.close()
  stopFake(fake)
}

test "connect reports an unusable URL and an unreachable server" {
  assertEqual(
    codeOf(MemcachedCache.connect("memcached://", MemcachedCache.options("p")), "connect"),
    "INVALID_CONFIG",
    "a URL without a host"
  )
  assertEqual(
    codeOf(
      MemcachedCache.connect("memcached://127.0.0.1:" + closedPort().toString(), MemcachedCache.options("p")),
      "connect"
    ),
    "CONNECT_FAILED",
    "nothing listening"
  )
}

test "the interface contract rejects an empty key and a negative TTL" {
  val store = unreachableStore()
  assertPanics("get with an empty key", { -> val _ = store.get("") })
  assertPanics("set with a negative TTL", { -> store.set("k", "v", 0 - 1) })
  assertPanics("tryGet with an empty key", { -> val _ = MemcachedCache.tryGet(store, "") })
}

test "a store shared between threads serializes its requests" {
  val fake = startFake()
  val store = storeOn(fake, "threads")
  val workers: List[KT.Background] = newList()
  var w = 0
  while w < 4 {
    val name = "w" + w.toString()
    workers.add(
      KT.startBackground(
        { -> var i = 0; while i < 50 { store.set(name + ":" + i.toString(), name + "=" + i.toString(), 0); i = i + 1 } }
      )
    )
    w = w + 1
  }
  for worker in workers {
    KT.waitBackground(worker)
  }
  w = 0
  while w < 4 {
    val name = "w" + w.toString()
    var i = 0
    while i < 50 {
      expectHit(store.get(name + ":" + i.toString()), name + "=" + i.toString(), "written by thread " + name)
      i = i + 1
    }
    w = w + 1
  }
  assertEqualInt(store.errors.errorCount(), 0, "no reply went to the wrong request")
  assertEqualInt(store.client.connections(), 1, "one connection")
  store.client.close()
  stopFake(fake)
}

// ─── Backend-specific operations ─────────────────────────────────────────────
test "addIfAbsent stores only when the key holds nothing" {
  val fake = startFake()
  val store = storeOn(fake, "extras")
  assertTrue(ok(Extras.addIfAbsent(store, "lock", "first", 30), "addIfAbsent"), "the first add stores")
  assertFalse(ok(Extras.addIfAbsent(store, "lock", "second", 30), "addIfAbsent"), "the second does not")
  expectHit(store.get("lock"), "first", "the first value stands")
  assertTrue(linesWith(fake, "ms ")[1].endsWith(" b ME T30"), "add mode with the TTL")
  fake.state.advance(31)
  assertTrue(ok(Extras.addIfAbsent(store, "lock", "third", 0), "addIfAbsent"), "after the expiry the key is free")
  store.delete("lock")
  assertTrue(ok(Extras.addIfAbsent(store, "lock", "fourth", 0), "addIfAbsent"), "and after a delete")
  store.clear()
  assertTrue(ok(Extras.addIfAbsent(store, "lock", "fifth", 0), "addIfAbsent"), "and after a clear")
  assertEqual(
    codeOf(Extras.addIfAbsent(store, "lock", Str.repeat("x", 1048577), 0), "addIfAbsent"),
    "VALUE_TOO_LARGE",
    "the value limit applies"
  )
  expectMiss(store.get("lock"), "and, as in memcached, a refused value empties the key even in add mode")
  store.client.close()
  stopFake(fake)
}

test "compareAndSwap stores only over the version that was read" {
  val fake = startFake()
  val store = storeOn(fake, "extras")
  match ok(Extras.getVersioned(store, "counter"), "getVersioned") {
    case None -> ()
    case Some(_) -> assertTrue(false, "nothing stored yet")
  }
  store.set("counter", "1", 0)
  val read = match ok(Extras.getVersioned(store, "counter"), "getVersioned") {
    case Some(v) -> v
    case None -> panic("the entry exists")
  }
  assertEqual(read.value, "1", "the value")
  assertTrue(Keys.isDecimal(read.version), "the version is memcached's CAS number: " + read.version)
  match ok(Extras.compareAndSwap(store, "counter", "2", 0, read.version), "compareAndSwap") {
    case Extras.Swapped -> ()
    case _ -> assertTrue(false, "nobody wrote in between")
  }
  expectHit(store.get("counter"), "2", "swapped")
  match ok(Extras.compareAndSwap(store, "counter", "3", 0, read.version), "compareAndSwap") {
    case Extras.Changed -> ()
    case _ -> assertTrue(false, "the version is stale now")
  }
  expectHit(store.get("counter"), "2", "nothing stored over a newer version")
  store.delete("counter")
  match ok(Extras.compareAndSwap(store, "counter", "3", 0, read.version), "compareAndSwap") {
    case Extras.Gone -> ()
    case _ -> assertTrue(false, "the entry is gone")
  }
  store.set("counter", "4", 0)
  val again = match ok(Extras.getVersioned(store, "counter"), "getVersioned") {
    case Some(v) -> v
    case None -> panic("the entry exists")
  }
  store.clear()
  match ok(Extras.compareAndSwap(store, "counter", "5", 0, again.version), "compareAndSwap") {
    case Extras.Gone -> ()
    case _ -> assertTrue(false, "a clear removes what the version named")
  }
  expectMiss(store.get("counter"), "nothing was stored into the cleared namespace")
  assertPanics("a version that is not a number", { -> val _ = Extras.compareAndSwap(store, "counter", "v", 0, "abc") })
  store.client.close()
  stopFake(fake)
}

test "compareAndSwap sets the TTL of what it stores" {
  val fake = startFake()
  val store = storeOn(fake, "extras")
  store.set("k", "1", 0)
  val read = match ok(Extras.getVersioned(store, "k"), "getVersioned") {
    case Some(v) -> v
    case None -> panic("the entry exists")
  }
  match ok(Extras.compareAndSwap(store, "k", "2", 10, read.version), "compareAndSwap") {
    case Extras.Swapped -> ()
    case _ -> assertTrue(false, "swapped")
  }
  fake.state.advance(11)
  expectMiss(store.get("k"), "expired with the TTL of the swap")
  store.client.close()
  stopFake(fake)
}

test "getMany reads a batch in one round trip and returns the hits" {
  val fake = startFake()
  val store = storeOn(fake, "many")
  val empty: slice[String] = []
  assertEqualInt(ok(Extras.getMany(store, empty), "getMany of nothing").count, 0, "no keys, no entries")
  assertEqualInt(ok(Extras.getMany(store, ["a", "b"]), "getMany").count, 0, "an empty namespace")
  store.set("a", "1", 0)
  store.set("c", "3", 0)
  store.set("key with spaces", "4", 0)
  fake.state.forgetLines()
  val found = ok(Extras.getMany(store, ["a", "b", "c", "key with spaces", "a"]), "getMany")
  assertEqualInt(found.count, 3, "three distinct hits")
  assertEqual(found["a"], "1", "a")
  assertEqual(found["c"], "3", "c")
  assertEqual(found["key with spaces"], "4", "a key with spaces")
  assertFalse(found.containsKey("b"), "the miss is absent")
  val lines = fake.state.lines()
  assertEqualInt(lines.count, 7, "the generation, five quiet reads and the no-op")
  assertTrue(lines[1].endsWith(" b v q O0") and lines[5].endsWith(" b v q O4"), "quiet, with opaque tokens")
  assertEqual(lines[6], "mn", "ended by mn")
  store.client.close()
  stopFake(fake)
}

test "getMany splits a long list into batches" {
  val fake = startFake()
  val store = storeOn(fake, "many")
  val keys: List[String] = newList()
  var i = 0
  while i < 150 {
    keys.add("k" + i.toString())
    if i % 3 == 0 {
      store.set("k" + i.toString(), "v" + i.toString(), 0)
    }
    i = i + 1
  }
  fake.state.forgetLines()
  val found = ok(Extras.getMany(store, keys.toArray()), "getMany")
  assertEqualInt(found.count, 50, "every third key")
  assertEqual(found["k0"], "v0", "from the first batch")
  assertEqual(found["k66"], "v66", "from the second")
  assertEqual(found["k147"], "v147", "from the third")
  assertFalse(found.containsKey("k1"), "a miss")
  assertEqualInt(linesWith(fake, "mn").count, 3, "150 keys in batches of 64: three round trips")
  assertEqualInt(Extras.getManyBatchSize(), 64, "the documented batch size")
  assertEqual(codeOf(Extras.getMany(store, ["a", ""]), "getMany"), "INVALID_CONFIG", "an empty key is refused")
  store.client.close()
  stopFake(fake)
}

test "touch gives an entry a new TTL without rewriting it" {
  val fake = startFake()
  val store = storeOn(fake, "extras")
  store.set("k", "v", 10)
  assertTrue(ok(Extras.touch(store, "k", 100), "touch"), "the entry exists")
  fake.state.advance(50)
  expectHit(store.get("k"), "v", "alive past the first TTL")
  assertTrue(ok(Extras.touch(store, "k", 0), "touch"), "touch with 0")
  match ok(Extras.lifetimeOf(store, "k"), "lifetimeOf") {
    case Extras.Permanent -> ()
    case _ -> assertTrue(false, "ttlSeconds = 0 removes the expiry")
  }
  assertFalse(ok(Extras.touch(store, "absent", 10), "touch"), "an absent key")
  assertEqualInt(linesWith(fake, "ms ").count, 2, "no value was rewritten")
  store.client.close()
  stopFake(fake)
}

test "lifetimeOf tells permanent, expiring and absent entries apart" {
  val fake = startFake()
  val store = storeOn(fake, "extras")
  match ok(Extras.lifetimeOf(store, "k"), "lifetimeOf") {
    case Extras.Absent -> ()
    case _ -> assertTrue(false, "an empty namespace")
  }
  store.set("forever", "v", 0)
  store.set("hour", "v", 3600)
  match ok(Extras.lifetimeOf(store, "forever"), "lifetimeOf") {
    case Extras.Permanent -> ()
    case _ -> assertTrue(false, "ttlSeconds = 0 is permanent")
  }
  match ok(Extras.lifetimeOf(store, "hour"), "lifetimeOf") {
    case Extras.Expiring(seconds) -> assertTrue(seconds > 3590i64 and seconds <= 3600i64, "about an hour")
    case _ -> assertTrue(false, "an hour-long entry is expiring")
  }
  match ok(Extras.lifetimeOf(store, "never-set"), "lifetimeOf") {
    case Extras.Absent -> ()
    case _ -> assertTrue(false, "an unset key is absent")
  }
  fake.state.advance(3601)
  match ok(Extras.lifetimeOf(store, "hour"), "lifetimeOf") {
    case Extras.Absent -> ()
    case _ -> assertTrue(false, "expired")
  }
  store.client.close()
  stopFake(fake)
}

test "ping is a liveness check of the store's server" {
  val fake = startFake()
  val store = storeOn(fake, "live")
  ok(MemcachedCache.ping(store), "ping")
  assertEqual(fake.state.lines()[1], "mn", "one mn")
  store.client.close()
  assertEqual(codeOf(MemcachedCache.ping(store), "ping"), "CLIENT_CLOSED", "a closed client is not alive")
  stopFake(fake)
}

// ─── The README's examples, as written there ─────────────────────────────────
func example(url: in String, userJson: in String): Result[Option[String], Client.MemcachedError] {
  val store = MemcachedCache.connect(url, MemcachedCache.options("myapp:cache"))?
  MemcachedCache.setWithTtl(store, "users:42", userJson, 300)
  val hit = MemcachedCache.get(store, "users:42")
  MemcachedCache.delete(store, "users:42")
  if store.errors.errorCount() > 0 {
    match store.errors.lastError() {
      case Some(e) -> println("cache degraded: " + e.message())
      case None -> ()
    }
  }
  store.client.close()
  Ok(hit)
}

func throughTheInterface(cache: in MemcachedCache.CacheStore): Option[String] {
  cache.set("k", "v", 0)
  cache.get("k")
}

func visibleFailures(store: in MemcachedCache.MemcachedCacheStore): Result[Unit, Client.MemcachedError] {
  MemcachedCache.trySet(store, "k", "v", 60)?
  val value = MemcachedCache.tryGet(store, "k")?
  val existed = MemcachedCache.tryDelete(store, "k")?
  println(existed.toString() + (match value { case Some(v) -> v; case None -> "" }))
  MemcachedCache.tryClear(store)
}

func overTls(url: in String, caPem: in String): Result[MemcachedCache.MemcachedCacheStore, Client.MemcachedError] {
  val base = Client.configFromUrl(url)?
  val cfg = Client.withTlsOptions(base, base.tls.withExclusiveCaCertificate(caPem))
  MemcachedCache.connectWith(cfg, MemcachedCache.options("myapp:cache"))
}

// Add one to a counter kept as text; false when another writer got in first
// and the caller should try again.
func increment(store: in MemcachedCache.MemcachedCacheStore, key: in String): Result[Bool, Client.MemcachedError] {
  val current = Extras.getVersioned(store, key)?
  match current {
    case None -> Extras.addIfAbsent(store, key, "1", 0)
    case Some(entry) -> {
      val next = match parseOptInt(entry.value) { case Some(n) -> n + 1; case None -> 1 }
      val outcome = Extras.compareAndSwap(store, key, next.toString(), 0, entry.version)?
      match outcome {
        case Extras.Swapped -> Ok(true)
        case _ -> Ok(false)
      }
    }
  }
}

func warm(store: in MemcachedCache.MemcachedCacheStore, ids: in slice[String]): Result[Int, Client.MemcachedError] {
  val found = Extras.getMany(store, ids)?
  Ok(found.count)
}

test "the README examples compile and do what the README says" {
  val fake = startFake()
  match ok(example(urlOf(fake), "{\"id\":42}"), "example") {
    case Some(json) -> assertEqual(json, "{\"id\":42}", "the hit")
    case None -> assertTrue(false, "example reads what it stored")
  }
  val store = storeOn(fake, "readme")
  val asInterface: Cache.CacheStore = store
  expectHit(throughTheInterface(asInterface), "v", "throughTheInterface")
  ok(visibleFailures(store), "visibleFailures")
  expectMiss(store.get("k"), "visibleFailures deleted and cleared")
  assertTrue(ok(increment(store, "hits"), "increment"), "the first increment adds the counter")
  assertTrue(ok(increment(store, "hits"), "increment"), "the second swaps it")
  expectHit(store.get("hits"), "2", "two increments")
  store.set("a", "1", 0)
  assertEqualInt(ok(warm(store, ["a", "b", "hits"]), "warm"), 2, "two of three keys are cached")
  store.client.close()
  stopFake(fake)
  val tlsFake = startFakeWith(plainSetup().copy(tls = true))
  val secured = ok(overTls("memcached+tls://localhost:" + tlsFake.port.toString(), caPem), "overTls")
  secured.set("k", "v", 0)
  expectHit(secured.get("k"), "v", "over TLS")
  secured.client.close()
  stopFake(tlsFake)
}
