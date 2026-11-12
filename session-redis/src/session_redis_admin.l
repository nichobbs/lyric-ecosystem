//! Session.Redis.Admin - operations Redis offers beyond the `SessionStore`
//! interface: the remaining lifetime of a session, and counting or removing
//! every session of a store. Kept apart from `Session.Redis` so that code
//! written against `SessionStore` stays portable between backends.
@runtime_checked
@io
package Session.Redis.Admin

import Std.Core
import Std.Collections
import Session
import Session.Redis as RedisSessions
import Redis as RedisLib

func failure(code: in String, e: in RedisLib.RedisError): SessionError {
  SessionError(message = e.message(), code = code)
}

/// Milliseconds until the session expires: `None` when the store does not
/// hold the session, `Some(0)` when it has no expiry.
@experimental
pub func remainingMillis(
  store: in RedisSessions.RedisSessionStore, sessionId: in String
): Result[Option[Long], SessionError] {
  if not isValidSessionId(sessionId) {
    return Ok(None)
  }
  match RedisLib.pttl(store.client, RedisSessions.redisKey(store.options, sessionId)) {
    case Err(e) -> Err(failure("TTL_FAILED", e))
    case Ok(n) -> if n == (0i64 - 2i64) {
      Ok(None)
    } else if n < 0i64 {
      Ok(Some(value = 0i64))
    } else {
      Ok(Some(value = n))
    }
  }
}

func sessionKeys(store: in RedisSessions.RedisSessionStore): Result[List[String], SessionError] {
  match RedisLib.scanAll(store.client, RedisLib.escapeGlob(store.options.keyPrefix) + "*") {
    case Ok(keys) -> Ok(keys)
    case Err(e) -> Err(failure("SCAN_FAILED", e))
  }
}

/// How many sessions the store holds (a `SCAN` over the store's prefix, so
/// its cost grows with the size of the database).
@experimental
pub func sessionCount(store: in RedisSessions.RedisSessionStore): Result[Int, SessionError] {
  match sessionKeys(store) {
    case Ok(keys) -> Ok(keys.count)
    case Err(e) -> Err(e)
  }
}

/// Remove every session of the store (`SCAN` + `DEL` under its prefix; never
/// `FLUSHDB`) and return how many were removed: "log everyone out".
@experimental
pub func destroyAll(store: in RedisSessions.RedisSessionStore): Result[Long, SessionError] {
  match sessionKeys(store) {
    case Err(e) -> Err(e)
    case Ok(keys) -> {
      var removed: Long = 0i64
      var i = 0
      while i < keys.count {
        val batch: List[String] = newList()
        var j = i
        while j < keys.count and j < i + 500 {
          batch.add(keys[j])
          j = j + 1
        }
        match RedisLib.del(store.client, batch.toArray()) {
          case Err(e) -> return Err(failure("DESTROY_FAILED", e))
          case Ok(n) -> {
            removed = removed + n
          }
        }
        i = j
      }
      Ok(removed)
    }
  }
}
