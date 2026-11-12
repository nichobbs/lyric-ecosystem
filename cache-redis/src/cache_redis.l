//! Cache.Redis - `Cache.CacheStore` on a Redis server.
//!
//! ```
//! import Cache.Redis as RedisCache
//!
//! match RedisCache.connect("redis://cache.internal:6379/1", RedisCache.options("myapp:cache:")) {
//!   case Err(e) -> println(e.message())
//!   case Ok(store) -> {
//!     RedisCache.setWithTtl(store, "users:42", userJson, 300)
//!     val hit = RedisCache.get(store, "users:42")
//!   }
//! }
//! ```
//!
//! Layout: every entry is one Redis string at `<keyPrefix><key>`, written
//! with `SET` (and `PX` for a TTL), so expiry is Redis's own. `clear()`
//! removes exactly the keys under the store's prefix with `SCAN` + `DEL`; it
//! never runs `FLUSHDB`, so one database can hold several stores and other
//! data.
//!
//! TLS: a `rediss://` URL connects over TLS with the system trust store.
//! For a private CA, a client certificate or another server name, build a
//! `Redis.RedisConfig` and pass it to `connectWith`:
//!
//! ```
//! import Redis
//!
//! val cfg = Redis.withExclusiveCaCertificate(Redis.configFromUrl("rediss://cache.internal:6380/1")?, caPem)
//! val store = RedisCache.connectWith(cfg, RedisCache.options("myapp:cache:"))?
//! ```
//!
//! Failures: `CacheStore` has no error channel (`get` returns an `Option`,
//! the writes return `Unit`), so the interface methods degrade the way a
//! cache should: a failed `get` is a miss, a failed write is dropped, and the
//! failure is counted and kept in the store's `CacheErrorLog`
//! (`errorCount()`, `lastError()`). Code that must see the failure calls the
//! `Result`-returning functions of this package (`tryGet`, `trySet`,
//! `tryDelete`, `tryClear`), which the interface methods are built on.
//!
//! Thread safety: the store holds one `Redis.RedisClient`, which serializes
//! commands on one connection, so a store may be shared between threads.
@runtime_checked
@io
package Cache.Redis

pub use Cache
import Std.Core
import Std.Collections
import Cache
import Redis as RedisLib

/// The failures the interface methods of a `RedisCacheStore` swallowed.
@experimental
pub protected type CacheErrorLog {
  var count: Int = 0
  var last: Option[RedisLib.RedisError] = None
  entry record(e: in RedisLib.RedisError): Unit {
    count = count + 1
    last = Some(value = e)
  }
  /// How many interface calls failed since the store was built.
  func errorCount(): Int {
    count
  }
  /// The most recent failure.
  func lastError(): Option[RedisLib.RedisError] {
    last
  }
}

/// How a store uses its Redis database.
@experimental
pub record RedisCacheOptions {
  /// Prepended to every cache key; `clear()` removes the keys under it.
  keyPrefix: String
  /// The length of one `ttlSeconds` unit in milliseconds. 1000 in
  /// production; a test suite lowers it so that TTLs elapse quickly while
  /// the expiry itself is still Redis's.
  ttlUnitMillis: Int
  invariant: keyPrefix.length > 0
  invariant: ttlUnitMillis >= 1 and ttlUnitMillis <= 1000
}

/// Options for `keyPrefix` with real seconds.
@experimental
pub func options(keyPrefix: in String): RedisCacheOptions
  requires: keyPrefix.length > 0
{
  RedisCacheOptions(keyPrefix = keyPrefix, ttlUnitMillis = 1000)
}

/// `options` with one `ttlSeconds` unit lasting `ttlUnitMillis` milliseconds.
@experimental
pub func optionsWithTtlUnit(keyPrefix: in String, ttlUnitMillis: in Int): RedisCacheOptions
  requires: keyPrefix.length > 0
  requires: ttlUnitMillis >= 1 and ttlUnitMillis <= 1000
{
  RedisCacheOptions(keyPrefix = keyPrefix, ttlUnitMillis = ttlUnitMillis)
}

/// A `CacheStore` kept in Redis. Build one with `connect` or `fromClient`.
@experimental
pub record RedisCacheStore {
  client: RedisLib.RedisClient
  options: RedisCacheOptions
  errors: CacheErrorLog
}

/// A store on an existing client. The client is shared, not owned: closing it
/// is the caller's business.
@experimental
pub func fromClient(client: in RedisLib.RedisClient, opts: in RedisCacheOptions): RedisCacheStore {
  RedisCacheStore(client = client, options = opts, errors = CacheErrorLog())
}

/// A store on a new connection to the server a `redis://` or `rediss://`
/// (TLS) URL names. The connection is opened and authenticated before the
/// store is returned.
@experimental
pub func connect(url: in String, opts: in RedisCacheOptions): Result[RedisCacheStore, RedisLib.RedisError] {
  match RedisLib.connect(url) {
    case Err(e) -> Err(e)
    case Ok(client) -> Ok(fromClient(client, opts))
  }
}

/// A store on a new connection described by `cfg`: everything a URL cannot
/// carry, such as the TLS options (`Redis.withExclusiveCaCertificate`,
/// `Redis.withClientIdentity`, `Redis.withTlsServerName`,
/// `Redis.withMinTlsVersion`), the timeouts and the protocol version. The
/// connection is opened and authenticated before the store is returned.
@experimental
pub func connectWith(
  cfg: in RedisLib.RedisConfig, opts: in RedisCacheOptions
): Result[RedisCacheStore, RedisLib.RedisError] {
  match RedisLib.open(cfg) {
    case Err(e) -> Err(e)
    case Ok(client) -> Ok(fromClient(client, opts))
  }
}

/// The Redis key that holds cache key `key`.
@pure
@experimental
pub func redisKey(opts: in RedisCacheOptions, key: in String): String {
  opts.keyPrefix + key
}

/// `get` with the failure visible.
@experimental
pub func tryGet(store: in RedisCacheStore, key: in String): Result[Option[String], RedisLib.RedisError]
  requires: key.length > 0
{
  RedisLib.get(store.client, redisKey(store.options, key))
}

/// `set` with the failure visible. `ttlSeconds = 0` stores without an expiry
/// and removes an earlier one.
@experimental
pub func trySet(
  store: in RedisCacheStore, key: in String, value: in String, ttlSeconds: in Int
): Result[Unit, RedisLib.RedisError]
  requires: key.length > 0
  requires: ttlSeconds >= 0
{
  val k = redisKey(store.options, key)
  if ttlSeconds == 0 {
    RedisLib.set(store.client, k, value)
  } else {
    RedisLib.setPx(store.client, k, value, ttlSeconds.toLong() * store.options.ttlUnitMillis.toLong())
  }
}

/// `delete` with the failure visible. `Ok(true)` when the key existed.
@experimental
pub func tryDelete(store: in RedisCacheStore, key: in String): Result[Bool, RedisLib.RedisError]
  requires: key.length > 0
{
  match RedisLib.del(store.client, [redisKey(store.options, key)]) {
    case Ok(n) -> Ok(n > 0i64)
    case Err(e) -> Err(e)
  }
}

val deleteBatchSize: Int = 500

/// `clear` with the failure visible: delete every key under the store's
/// prefix and return how many were deleted. Keys written while the scan runs
/// may survive it.
@experimental
pub func tryClear(store: in RedisCacheStore): Result[Long, RedisLib.RedisError] {
  val pattern = RedisLib.escapeGlob(store.options.keyPrefix) + "*"
  var deleted: Long = 0i64
  var cursor = "0"
  var done = false
  while not done {
    match RedisLib.scan(store.client, cursor, pattern, deleteBatchSize) {
      case Err(e) -> return Err(e)
      case Ok(page) -> {
        if page.keys.count > 0 {
          match RedisLib.del(store.client, page.keys.toArray()) {
            case Err(e) -> return Err(e)
            case Ok(n) -> {
              deleted = deleted + n
            }
          }
        }
        cursor = page.cursor
        done = cursor == "0"
      }
    }
  }
  Ok(deleted)
}

impl CacheStore for RedisCacheStore {
  func get(key: in String): Option[String] {
    match tryGet(self, key) {
      case Ok(v) -> v
      case Err(e) -> {
        self.errors.record(e)
        None
      }
    }
  }

  func set(key: in String, value: in String, ttlSeconds: in Int): Unit {
    match trySet(self, key, value, ttlSeconds) {
      case Ok(_) -> ()
      case Err(e) -> self.errors.record(e)
    }
  }

  func delete(key: in String): Unit {
    match tryDelete(self, key) {
      case Ok(_) -> ()
      case Err(e) -> self.errors.record(e)
    }
  }

  func clear(): Unit {
    match tryClear(self) {
      case Ok(_) -> ()
      case Err(e) -> self.errors.record(e)
    }
  }
}
