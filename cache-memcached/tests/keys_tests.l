//! tests/keys_tests.l - how cache keys are stored (namespace, generation,
//! base64, the SHA-256 form of long keys) and how a `CacheStore` TTL becomes
//! a memcached expiry.
@test_module
package Cache.Memcached.KeysTests

import Std.Core
import Std.Encoding
import Std.String as Str
import Std.Testing
import Cache.Memcached.Keys

// ─── Namespaces ──────────────────────────────────────────────────────────────
test "a namespace is text without spaces or control characters" {
  assertEqual(namespaceProblem("myapp:cache"), "", "punctuation is fine")
  assertEqual(namespaceProblem("café-世界"), "", "so is non-ASCII text")
  assertEqual(namespaceProblem(""), "the namespace is empty", "empty")
  assertEqual(namespaceProblem("my app"), "the namespace contains a space or a control character", "a space")
  assertEqual(namespaceProblem("a\tb"), "the namespace contains a space or a control character", "a tab")
  assertEqual(namespaceProblem("a\nb"), "the namespace contains a space or a control character", "a line feed")
  assertEqual(namespaceProblem(Str.repeat("n", 80)), "", "80 bytes")
  assertEqual(namespaceProblem(Str.repeat("n", 81)), "the namespace is longer than 80 bytes", "81 bytes")
  assertEqual(
    namespaceProblem(Str.repeat("é", 41)),
    "the namespace is longer than 80 bytes",
    "82 bytes in 41 characters"
  )
}

// ─── Stored keys ─────────────────────────────────────────────────────────────
test "a key is stored under its namespace and generation" {
  assertEqual(dataKey("app", "17", "users:42"), "app 17 users:42", "namespace, generation, key")
  assertEqual(
    dataKey("app", "17", "key with spaces\tand\ncontrols"),
    "app 17 key with spaces\tand\ncontrols",
    "any text"
  )
  assertEqual(generationKey("app"), "app generation", "the generation counter")
}

test "keys of different namespaces and generations never collide" {
  // "a 5 b 7 k" could be key "b 7 k" of namespace "a" or key "k" of
  // namespace "a 5 b": the second namespace does not exist.
  assertPanics("a namespace with a space is refused", { -> val _ = dataKey("a 5 b", "7", "k") })
  assertTrue(dataKey("app", "1", "k") != dataKey("app", "2", "k"), "another generation is another key")
  assertTrue(dataKey("app", "1", "generation") != generationKey("app"), "a cache key named generation")
  assertTrue(dataKey("app", "1", "k") != dataKey("app2", "1", "k"), "another namespace")
}

test "a stored key of at most 186 bytes is kept as it is" {
  // "app 1 " is 6 bytes: 180 more make 186.
  val longest = Str.repeat("k", 180)
  assertFalse(isHashed("app", "1", longest), "186 bytes fit")
  assertEqual(dataKey("app", "1", longest), "app 1 " + longest, "kept verbatim")
  assertEqualInt(wireKey(dataKey("app", "1", longest)).length, 248, "248 base64 characters, under memcached's 250")
}

test "a longer key is stored under its SHA-256" {
  val long = Str.repeat("k", 181)
  assertTrue(isHashed("app", "1", long), "187 bytes do not fit")
  val stored = dataKey("app", "1", long)
  assertEqual(stored, "app 1#" + sha256Of181k, "namespace, generation, # and the hash")
  assertEqualInt(stored.length, 6 + 64, "64 hexadecimal digits")
  assertTrue(wireKey(stored).length <= 250, "the hashed key fits")
  assertTrue(dataKey("app", "1", long + "x") != stored, "another long key has another hash")
  assertEqual(dataKey("app", "1", long), stored, "the same key has the same hash")
}

// `printf 'k%.0s' $(seq 181) | sha256sum`
val sha256Of181k: String = "f6ba9a3395c9da467df2186ae5ba37deeb94f3b22e889844f8a86ff41d29d359"

test "a short key cannot imitate a hashed one" {
  val long = Str.repeat("k", 400)
  val hashed = dataKey("app", "1", long)
  val digest = Str.substring(hashed, 6)
  // The cache key that spells the hash is filed after a space, the hashed
  // key after '#'.
  assertEqual(dataKey("app", "1", digest), "app 1 " + digest, "the digest as a cache key")
  assertTrue(dataKey("app", "1", digest) != hashed, "differs from the hashed key")
  assertEqual(dataKey("app", "1", "#" + digest), "app 1 #" + digest, "even with a # of its own")
}

test "the byte length decides, not the character count" {
  // 60 characters of three bytes each: 180 bytes, 186 with "app 1 ".
  assertFalse(isHashed("app", "1", Str.repeat("世", 60)), "186 bytes in 66 characters")
  assertTrue(isHashed("app", "1", Str.repeat("世", 61)), "189 bytes")
  assertTrue(
    isHashed(Str.repeat("n", 80), "12345678901234567890", Str.repeat("k", 85)),
    "a long namespace leaves less room"
  )
  assertFalse(isHashed(Str.repeat("n", 80), "12345678901234567890", Str.repeat("k", 84)), "84 bytes still fit")
  assertTrue(
    wireKey(dataKey(Str.repeat("n", 80), "12345678901234567890", Str.repeat("k", 5000))).length <= 250,
    "the longest namespace and generation with a hashed key fit"
  )
}

test "the wire key is padded base64 of the UTF-8 bytes" {
  assertEqual(wireKey("foo"), "Zm9v", "no padding needed")
  assertEqual(wireKey("nope"), "bm9wZQ==", "padding is kept: memcached refuses a key without it")
  assertEqual(wireKey("app 1 k"), "YXBwIDEgaw==", "a space is just a byte")
  assertEqual(wireKey("é"), "w6k=", "UTF-8")
  match tryDecodeBase64(wireKey("app 17 users:42")) {
    case Some(bytes) -> match tryDecodeUtf8(bytes) {
      case Some(text) -> assertEqual(text, "app 17 users:42", "the stored key is the plain text")
      case None -> assertTrue(false, "UTF-8")
    }
    case None -> assertTrue(false, "base64")
  }
}

// ─── Generations ─────────────────────────────────────────────────────────────
test "a generation and a version are decimal numbers" {
  assertTrue(isDecimal("0"), "0")
  assertTrue(isDecimal("1794532848123456"), "a generation")
  assertTrue(isDecimal("18446744073709551615"), "the largest 64-bit counter")
  assertFalse(isDecimal(""), "empty")
  assertFalse(isDecimal("-1"), "a sign")
  assertFalse(isDecimal("12a"), "a letter")
  assertFalse(isDecimal("1 2"), "a space")
  assertFalse(isDecimal("184467440737095516150"), "21 digits")
}

test "the first generation is the time and a salt" {
  assertEqual(initialGeneration(1794532848123i64, 456), "1794532848123456", "milliseconds, then three digits")
  assertEqual(initialGeneration(1794532848123i64, 7), "1794532848123007", "the salt is padded by its place")
  assertTrue(isDecimal(initialGeneration(1794532848123i64, 999)), "a decimal number")
  assertPanics("a salt of four digits", { -> val _ = initialGeneration(1i64, 1000) })
}

// ─── TTLs ────────────────────────────────────────────────────────────────────
val now: Long = 1794532848i64

test "no TTL stays no expiry" {
  assertEqualLong(expiryToken(0, now), 0i64, "0 is never")
}

test "a TTL of at most 30 days is sent as seconds from now" {
  assertEqualLong(expiryToken(1, now), 1i64, "one second")
  assertEqualLong(expiryToken(300, now), 300i64, "five minutes")
  assertEqualLong(expiryToken(2592000, now), 2592000i64, "exactly 30 days is still relative")
}

test "a TTL above 30 days becomes an absolute timestamp" {
  assertEqualLong(expiryToken(2592001, now), now + 2592001i64, "30 days and a second")
  assertEqualLong(expiryToken(31536000, now), now + 31536000i64, "a year")
  assertTrue(expiryToken(2592001, now) > 2592000i64, "which memcached reads as a timestamp")
}

test "a timestamp memcached cannot hold is lowered to the latest it can" {
  // 2038-01-19T03:14:07Z is 352950799 seconds after `now`.
  assertEqualLong(expiryToken(352950799, now), 2147483647i64, "exactly the latest")
  assertEqualLong(expiryToken(352950800, now), 2147483647i64, "one second later is lowered")
  assertEqualLong(expiryToken(2147483647, now), 2147483647i64, "the longest TTL an Int holds")
}

test "close to the latest timestamp the relative 30 days are the longest lifetime" {
  val late = 2147483647i64 - 100i64
  assertEqualLong(expiryToken(2592001, late), 2592000i64, "100 seconds before the end")
  assertEqualLong(expiryToken(2592001, 2147483647i64 - 2592000i64), 2592000i64, "exactly 30 days before")
  assertEqualLong(expiryToken(2592001, 2147483647i64 - 2592001i64), 2147483647i64, "30 days and a second before")
  assertEqualLong(expiryToken(2592001, 2200000000i64), 2592000i64, "after it")
  assertEqualLong(expiryToken(86400, 2200000000i64), 86400i64, "a short TTL is not affected")
}

test "a negative TTL is a contract violation, never an expired entry" {
  assertPanics("ttl -1", { -> val _ = expiryToken(0 - 1, now) })
}
