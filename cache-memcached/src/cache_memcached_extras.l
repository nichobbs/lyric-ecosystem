//! Cache.Memcached.Extras - operations memcached offers beyond the
//! `CacheStore` interface: an add-only write, compare-and-swap, a multi-get
//! in one round trip, a touch, and the remaining lifetime of an entry. Kept
//! apart from `Cache.Memcached` so that code written against `CacheStore`
//! stays portable between backends.
//!
//! All of them act on the store's namespace in its current generation, like
//! the interface methods, so they see exactly the entries `get` sees.
@runtime_checked
@io
package Cache.Memcached.Extras

import Std.Core
import Std.Collections
import Std.Parse
import Cache.Memcached as MemcachedCache
import Cache.Memcached.Client as Client
import Cache.Memcached.Keys as Keys
import Cache.Memcached.Meta as Meta

func single(command: in MemcachedCache.KeyedCommand): List[MemcachedCache.KeyedCommand] {
  val commands: List[MemcachedCache.KeyedCommand] = newList()
  commands.add(command)
  commands
}

/// Store `value` under `key` only when the key holds nothing (`ms` in add
/// mode), in one atomic step. `ttlSeconds = 0` stores without an expiry.
/// `Ok(true)` when this call stored the value: the building block of a
/// cache-stampede lock.
@experimental
pub func addIfAbsent(
  store: in MemcachedCache.MemcachedCacheStore, key: in String, value: in String, ttlSeconds: in Int
): Result[Bool, Client.MemcachedError]
  requires: key.length > 0
  requires: ttlSeconds >= 0
{
  val payload = match MemcachedCache.payloadFor(store, key, value) { case Ok(p) -> p; case Err(e) -> return Err(e) }
  val command = MemcachedCache.keyedSet(key, payload, "ME " + MemcachedCache.expiryFlag(ttlSeconds))
  match MemcachedCache.runInGeneration(store, single(command), true, false) {
    case Err(e) -> Err(e)
    case Ok(None) -> Err(Client.UnexpectedReply(message = "the namespace has no generation after one was started"))
    case Ok(Some(replies)) -> match replies[0] {
      case Meta.Head(_) -> Ok(true)
      case Meta.NotStored(_) -> Ok(false)
      case other -> Err(MemcachedCache.storeFailure("ms", payload.length, other))
    }
  }
}

/// An entry and the version memcached gave it (its CAS value): a decimal
/// number that changes with every write of the key.
@experimental
pub record Versioned {
  value: String
  version: String
}

/// The value of `key` with its version, for `compareAndSwap`.
@experimental
pub func getVersioned(
  store: in MemcachedCache.MemcachedCacheStore, key: in String
): Result[Option[Versioned], Client.MemcachedError]
  requires: key.length > 0
{
  match MemcachedCache.runInGeneration(store, single(MemcachedCache.keyed("mg", key, "v c")), false, false) {
    case Err(e) -> Err(e)
    case Ok(None) -> Ok(None)
    case Ok(Some(replies)) -> match replies[0] {
      case Meta.Miss(_) -> Ok(None)
      case Meta.Value(data, flags) -> {
        val text = match MemcachedCache.textOf(key, data) { case Ok(t) -> t; case Err(e) -> return Err(e) }
        match Meta.flagValue(flags, "c") {
          case Some(version) -> Ok(Some(value = Versioned(value = text, version = version)))
          case None -> Err(Client.UnexpectedReply(message = "mg did not return the version it was asked for"))
        }
      }
      case other -> Err(MemcachedCache.unexpectedReply("mg", other))
    }
  }
}

/// What `compareAndSwap` did.
@experimental
pub union SwapOutcome {
  /// The value was stored: the entry still had the version given.
  case Swapped
  /// The entry was written by someone else in between; nothing was stored.
  case Changed
  /// The entry no longer exists (deleted, expired, evicted or cleared);
  /// nothing was stored.
  case Gone
}

/// Store `value` under `key` only if the entry still has `version`
/// (`getVersioned`), in one atomic step. `ttlSeconds = 0` stores without an
/// expiry.
@experimental
pub func compareAndSwap(
  store: in MemcachedCache.MemcachedCacheStore,
  key: in String,
  value: in String,
  ttlSeconds: in Int,
  version: in String,
): Result[SwapOutcome, Client.MemcachedError]
  requires: key.length > 0
  requires: ttlSeconds >= 0
  requires: Keys.isDecimal(version)
{
  val payload = match MemcachedCache.payloadFor(store, key, value) { case Ok(p) -> p; case Err(e) -> return Err(e) }
  val command = MemcachedCache.keyedSet(key, payload, "C" + version + " " + MemcachedCache.expiryFlag(ttlSeconds))
  match MemcachedCache.runInGeneration(store, single(command), false, false) {
    case Err(e) -> Err(e)
    case Ok(None) -> Ok(Gone)
    case Ok(Some(replies)) -> match replies[0] {
      case Meta.Head(_) -> Ok(Swapped)
      case Meta.Exists(_) -> Ok(Changed)
      case Meta.NotFound(_) -> Ok(Gone)
      case other -> Err(MemcachedCache.storeFailure("ms", payload.length, other))
    }
  }
}

/// How many keys `getMany` asks for in one round trip.
@experimental
pub func getManyBatchSize(): Int {
  64
}

/// The values of `keys` that the store holds, by key: the keys of a batch of
/// `getManyBatchSize()` travel in one write as quiet `mg` commands, and the
/// server answers the hits only. A key that is absent, or that appears twice,
/// is simply one entry less in the result. The batches are separate round
/// trips, so the result is not a snapshot.
@experimental
pub func getMany(
  store: in MemcachedCache.MemcachedCacheStore, keys: in slice[String]
): Result[Map[String, String], Client.MemcachedError] {
  val found: Map[String, String] = newMap()
  var start = 0
  while start < keys.length {
    var stop = start + getManyBatchSize()
    if stop > keys.length {
      stop = keys.length
    }
    val commands: List[MemcachedCache.KeyedCommand] = newList()
    var i = start
    while i < stop {
      val key: String = keys[i]
      if key.length == 0 {
        return Err(Client.InvalidConfig(message = "a cache key is empty"))
      }
      commands.add(MemcachedCache.keyed("mg", key, "v q O" + i.toString()))
      i = i + 1
    }
    match MemcachedCache.runInGeneration(store, commands, false, true) {
      case Err(e) -> return Err(e)
      case Ok(None) -> return Ok(found)
      case Ok(Some(replies)) -> {
        for reply in replies {
          match reply {
            case Meta.Value(data, flags) -> {
              val index = match Meta.flagValue(flags, "O") {
                case Some(text) -> match parseOptInt(text) { case Some(n) -> n; case None -> 0 - 1 }
                case None -> 0 - 1
              }
              if index < start or index >= stop {
                return Err(Client.UnexpectedReply(message = "mg returned a value for a key that was not asked for"))
              }
              val key: String = keys[index]
              val text = match MemcachedCache.textOf(key, data) { case Ok(t) -> t; case Err(e) -> return Err(e) }
              found[key] = text
            }
            case Meta.Miss(_) -> ()
            case other -> return Err(MemcachedCache.unexpectedReply("mg", other))
          }
        }
      }
    }
    start = stop
  }
  Ok(found)
}

/// Give the entry `key` a new TTL without reading or rewriting its value
/// (`mg` with the `T` flag). `ttlSeconds = 0` removes the expiry. `Ok(true)`
/// when the entry exists.
@experimental
pub func touch(
  store: in MemcachedCache.MemcachedCacheStore, key: in String, ttlSeconds: in Int
): Result[Bool, Client.MemcachedError]
  requires: key.length > 0
  requires: ttlSeconds >= 0
{
  val command = MemcachedCache.keyed("mg", key, MemcachedCache.expiryFlag(ttlSeconds))
  match MemcachedCache.runInGeneration(store, single(command), false, false) {
    case Err(e) -> Err(e)
    case Ok(None) -> Ok(false)
    case Ok(Some(replies)) -> match replies[0] {
      case Meta.Head(_) -> Ok(true)
      case Meta.Miss(_) -> Ok(false)
      case other -> Err(MemcachedCache.unexpectedReply("mg", other))
    }
  }
}

/// How long an entry still has to live.
@experimental
pub union Lifetime {
  /// The key holds nothing.
  case Absent
  /// The entry has no expiry.
  case Permanent
  /// The entry expires in `seconds` seconds, by the server's clock.
  case Expiring(seconds: Long)
}

/// The remaining lifetime of `key` (`mg` with the `t` flag). memcached counts
/// in whole seconds.
@experimental
pub func lifetimeOf(
  store: in MemcachedCache.MemcachedCacheStore, key: in String
): Result[Lifetime, Client.MemcachedError]
  requires: key.length > 0
{
  match MemcachedCache.runInGeneration(store, single(MemcachedCache.keyed("mg", key, "t")), false, false) {
    case Err(e) -> Err(e)
    case Ok(None) -> Ok(Absent)
    case Ok(Some(replies)) -> match replies[0] {
      case Meta.Miss(_) -> Ok(Absent)
      case Meta.Head(flags) -> match Meta.flagValue(flags, "t") {
        case Some(text) -> if text == "-1" {
          Ok(Permanent)
        } else {
          match parseOptLong(text) {
            case Some(n) -> Ok(Expiring(seconds = n))
            case None -> Err(Client.UnexpectedReply(message = "mg returned the lifetime '" + text + "'"))
          }
        }
        case None -> Err(
          Client.UnexpectedReply(
            message = "mg did not return the lifetime it was asked for"
          )
        )
      }
      case other -> Err(MemcachedCache.unexpectedReply("mg", other))
    }
  }
}
