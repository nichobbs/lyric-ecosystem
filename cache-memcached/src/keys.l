//! Cache.Memcached.Keys - how cache keys and TTLs are written for memcached.
//!
//! Keys. memcached takes a key of at most 250 bytes without spaces or
//! control characters; with the meta protocol's `b` flag the key is sent as
//! base64 and stored as the decoded bytes, and the 250-byte limit applies to
//! the base64 text, which leaves 186 bytes for the key itself. Every key of
//! this package is sent that way, so a cache key may hold any text. The
//! stored key is
//!
//!   <namespace> <generation> <key>            (two single spaces)
//!
//! and, when that is longer than 186 bytes,
//!
//!   <namespace> <generation>#<SHA-256 of the key, 64 lower-case hexadecimal digits>
//!
//! The generation is a decimal number, so the character after it tells the
//! two forms apart and a short key can never equal a hashed one. The
//! namespace's generation counter is stored under `<namespace> generation`.
//! A namespace has no space or control character, so one namespace's keys
//! never equal another's.
//!
//! TTLs. memcached reads an expiry of at most 2592000 (30 days) as seconds
//! from now, a larger one as an absolute Unix timestamp, 0 as "never" and a
//! negative one as "already expired"; `expiryToken` turns a
//! `CacheStore` TTL into what is sent.
@runtime_checked
package Cache.Memcached.Keys

import Std.Core
import Std.Encoding
import Std.Hash
import Std.String as Str

/// The longest key memcached stores when it is sent base64-encoded: 186
/// bytes encode to 248 characters, and 250 is the limit of the encoded text.
@pure
@experimental
pub func maxStoredKeyBytes(): Int {
  186
}

/// The longest namespace, in UTF-8 bytes. It leaves room for the generation
/// and a hashed key.
@pure
@experimental
pub func maxNamespaceBytes(): Int {
  80
}

/// The longest TTL memcached reads as seconds from now: 30 days.
@pure
@experimental
pub func maxRelativeTtlSeconds(): Int {
  2592000
}

/// The latest expiry memcached 1.6 can hold: 2038-01-19T03:14:07Z. A larger
/// timestamp wraps around in the server.
@pure
@experimental
pub func maxExpiryEpochSeconds(): Long {
  2147483647i64
}

/// What is wrong with `namespace`, or `""` when it is usable: not empty, at
/// most `maxNamespaceBytes()` bytes of UTF-8, and without a space, a control
/// character or DEL.
@pure
@experimental
pub func namespaceProblem(namespace: in String): String {
  if namespace.length == 0 {
    return "the namespace is empty"
  }
  var i = 0
  while i < namespace.length {
    val unit = namespace.codeUnitAt(i)
    if unit <= 32 or unit == 127 {
      return "the namespace contains a space or a control character"
    }
    i = i + 1
  }
  if encodeUtf8(namespace).length > maxNamespaceBytes() {
    return "the namespace is longer than " + maxNamespaceBytes().toString() + " bytes"
  }
  ""
}

/// Whether `text` is a decimal number of 1 to 20 digits: the form of a
/// generation and of a compare-and-swap version.
@pure
@experimental
pub func isDecimal(text: in String): Bool {
  if text.length == 0 or text.length > 20 {
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

/// The stored key of the generation counter of `namespace`.
@pure
@experimental
pub func generationKey(namespace: in String): String {
  namespace + " generation"
}

/// The first generation of a namespace: the time in milliseconds followed by
/// three digits of `salt`, so that a counter created again after the server
/// lost it (an eviction, a restart) does not repeat a generation that
/// earlier entries were stored under.
@pure
@experimental
pub func initialGeneration(nowEpochMillis: in Long, salt: in Int): String
  requires: nowEpochMillis >= 0i64
  requires: salt >= 0 and salt <= 999
{
  (nowEpochMillis * 1000i64 + salt.toLong()).toString()
}

/// The stored key of cache key `key` in `generation` of `namespace`.
@experimental
pub func dataKey(namespace: in String, generation: in String, key: in String): String
  requires: namespaceProblem(namespace).length == 0
  requires: isDecimal(generation)
  requires: key.length > 0
{
  val plain = namespace + " " + generation + " " + key
  if encodeUtf8(plain).length <= maxStoredKeyBytes() {
    plain
  } else {
    namespace + " " + generation + "#" + Str.toLower(sha256OfBytes(encodeUtf8(key)))
  }
}

/// Whether `dataKey` stores `key` under its SHA-256 instead of itself.
@experimental
pub func isHashed(namespace: in String, generation: in String, key: in String): Bool
  requires: namespaceProblem(namespace).length == 0
  requires: isDecimal(generation)
  requires: key.length > 0
{
  encodeUtf8(namespace + " " + generation + " " + key).length > maxStoredKeyBytes()
}

/// A stored key as it is written on the wire: base64 of its UTF-8 bytes.
@pure
@experimental
pub func wireKey(storedKey: in String): String
  requires: storedKey.length > 0
{
  encodeBase64(encodeUtf8(storedKey))
}

/// The expiry memcached is sent for a `CacheStore` TTL of `ttlSeconds`, at
/// the time `nowEpochSeconds`:
///
/// - `0` stays `0`: no expiry.
/// - up to 30 days the TTL is sent as it is, relative to the server's clock.
/// - above 30 days it becomes the absolute timestamp `now + ttl`, since
///   memcached would read the number as a timestamp anyway (and one in 1970
///   at that, which expires the entry at once).
/// - a timestamp past `maxExpiryEpochSeconds()` is lowered to it; if that
///   leaves less than 30 days, the relative 30 days are sent instead, the
///   longest lifetime the server can still express.
@pure
@experimental
pub func expiryToken(ttlSeconds: in Int, nowEpochSeconds: in Long): Long
  requires: ttlSeconds >= 0
  ensures: result >= 0i64
{
  val relativeLimit = maxRelativeTtlSeconds().toLong()
  val ttl = ttlSeconds.toLong()
  if ttl <= relativeLimit {
    return ttl
  }
  var absolute = nowEpochSeconds + ttl
  if absolute > maxExpiryEpochSeconds() {
    absolute = maxExpiryEpochSeconds()
  }
  if absolute - nowEpochSeconds <= relativeLimit {
    return relativeLimit
  }
  absolute
}
