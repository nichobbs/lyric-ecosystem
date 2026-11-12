//! Cache.Redis.Extras - operations Redis offers beyond the `CacheStore`
//! interface: a conditional write, the remaining lifetime of an entry, and a
//! count of the store's entries. Kept apart from `Cache.Redis` so that code
//! written against `CacheStore` stays portable between backends.
@runtime_checked
@io
package Cache.Redis.Extras

import Std.Core
import Std.Collections
import Cache.Redis as RedisCache
import Redis as RedisLib

/// Store `value` under `key` only when the key holds nothing (`SET NX`), in
/// one atomic step. `ttlSeconds = 0` stores without an expiry. `Ok(true)`
/// when this call stored the value: the building block of a cache-stampede
/// lock.
@experimental
pub func setIfAbsent(
  store: in RedisCache.RedisCacheStore, key: in String, value: in String, ttlSeconds: in Int
): Result[Bool, RedisLib.RedisError]
  requires: key.length > 0
  requires: ttlSeconds >= 0
{
  RedisLib.setNx(
    store.client,
    RedisCache.redisKey(store.options, key),
    value,
    ttlSeconds.toLong() * store.options.ttlUnitMillis.toLong()
  )
}

/// How long an entry still has to live.
@experimental
pub union Lifetime {
  /// The key holds nothing.
  case Absent
  /// The entry has no expiry.
  case Permanent
  /// The entry expires in `millis` milliseconds.
  case Expiring(millis: Long)
}

/// The remaining lifetime of `key` (`PTTL`).
@experimental
pub func lifetimeOf(store: in RedisCache.RedisCacheStore, key: in String): Result[Lifetime, RedisLib.RedisError]
  requires: key.length > 0
{
  match RedisLib.pttl(store.client, RedisCache.redisKey(store.options, key)) {
    case Err(e) -> Err(e)
    case Ok(n) -> if n == (0i64 - 2i64) {
      Ok(Absent)
    } else if n < 0i64 {
      Ok(Permanent)
    } else {
      Ok(Expiring(millis = n))
    }
  }
}

/// How many entries the store holds (a `SCAN` over the store's prefix, so
/// its cost grows with the size of the database).
@experimental
pub func entryCount(store: in RedisCache.RedisCacheStore): Result[Int, RedisLib.RedisError] {
  match RedisLib.scanAll(store.client, RedisLib.escapeGlob(store.options.keyPrefix) + "*") {
    case Ok(keys) -> Ok(keys.count)
    case Err(e) -> Err(e)
  }
}
