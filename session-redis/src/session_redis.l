//! Session.Redis - `Session.SessionStore` on a Redis server.
//!
//! ```
//! import Session.Redis as RedisSessions
//!
//! match RedisSessions.connect("redis://sessions.internal:6379", RedisSessions.options("myapp:session:", 3600)) {
//!   case Err(e) -> println(e.message)
//!   case Ok(store) -> match store.create() {
//!     case Ok(id) -> { val _ = store.set(id, "user", "alice") }
//!     case Err(e) -> println(e.message)
//!   }
//! }
//! ```
//!
//! Layout: a session is one Redis hash at `<keyPrefix><sessionId>`. The
//! session's entries are the fields `d:<key>`; `m:created` and `m:accessed`
//! hold the creation and last-write times in epoch milliseconds. The hash's
//! Redis expiry is the session's expiry, so an expired session is removed by
//! Redis itself.
//!
//! Atomicity: every operation that reads before it writes (`create`, `set`,
//! `delete`, `clear`, `touch`, `save`, `load`) is one Lua script (`EVAL`), so
//! concurrent requests on one session never lose an update, and `set` on an
//! id the store does not hold fails with `SESSION_NOT_FOUND` without creating
//! anything (session fixation, lyric-lang #316). The server must allow `EVAL`.
//!
//! Session ids are 32 bytes from `Std.SecureRandom`, written as 64 hex
//! characters.
//!
//! Expiry: `create` and `touch` set the expiry to the store's `ttlSeconds`
//! from now. `set`, `delete` and `clear` leave it alone. `save` takes it from
//! `SessionData.expiresAt` (`0` = no expiry, as the interface defines).
//! `load` reports the remaining lifetime as `expiresAt` and does not extend
//! it.
//!
//! Errors: every failure is a `Session.SessionError`; its `code` names the
//! operation (`CREATE_FAILED`, `LOAD_FAILED`, `SAVE_FAILED`, `DESTROY_FAILED`,
//! `TOUCH_FAILED`, `GET_FAILED`, `SET_FAILED`, `DELETE_FAILED`,
//! `CLEAR_FAILED`), or is `SESSION_NOT_FOUND` / `INVALID_SESSION_ID`. The
//! message is the Redis client's and never contains the password.
//!
//! Thread safety: the store holds one `Redis.RedisClient`, which serializes
//! commands on one connection, so a store may be shared between threads.
@runtime_checked
@io
package Session.Redis

pub use Session
import Std.Core
import Std.Collections
import Std.Encoding
import Std.Parse
import Std.SecureRandom
import Std.Time
import Session
import Redis as RedisLib
import Redis.Resp as Resp

/// How a store uses its Redis database.
@experimental
pub record RedisSessionOptions {
  /// Prepended to every session id to form the Redis key.
  keyPrefix: String
  /// Lifetime of a session after `create` or `touch`, in seconds: 1 up to
  /// `Session.maxSessionTtlSeconds()` (one year).
  ttlSeconds: Int
  invariant: keyPrefix.length > 0
  invariant: ttlSeconds >= 1 and ttlSeconds <= 31536000
}

/// Options for `keyPrefix` and a session lifetime of `ttlSeconds`.
@experimental
pub func options(keyPrefix: in String, ttlSeconds: in Int): RedisSessionOptions
  requires: keyPrefix.length > 0
  requires: ttlSeconds >= 1 and ttlSeconds <= 31536000
{
  RedisSessionOptions(keyPrefix = keyPrefix, ttlSeconds = ttlSeconds)
}

/// A `SessionStore` kept in Redis. Build one with `connect` or `fromClient`.
@experimental
pub record RedisSessionStore {
  client: RedisLib.RedisClient
  options: RedisSessionOptions
}

/// A store on an existing client. The client is shared, not owned: closing it
/// is the caller's business.
@experimental
pub func fromClient(client: in RedisLib.RedisClient, opts: in RedisSessionOptions): RedisSessionStore {
  RedisSessionStore(client = client, options = opts)
}

/// A store on a new connection to the server a `redis://` URL names. The
/// connection is opened and authenticated before the store is returned.
@experimental
pub func connect(url: in String, opts: in RedisSessionOptions): Result[RedisSessionStore, SessionError] {
  match RedisLib.connect(url) {
    case Err(e) -> Err(SessionError(message = e.message(), code = "CONNECT_FAILED"))
    case Ok(client) -> Ok(fromClient(client, opts))
  }
}

/// The Redis key of session `sessionId`.
@pure
@experimental
pub func redisKey(opts: in RedisSessionOptions, sessionId: in String): String {
  opts.keyPrefix + sessionId
}

val entryFieldPrefix: String = "d:"

val createdField: String = "m:created"

val accessedField: String = "m:accessed"

/// A new session id: 32 bytes from the platform's cryptographically secure
/// random number generator, as 64 lower-case hex characters.
@experimental
pub func newSessionId(): String {
  encodeHex(secureGetBytes(32)).toLower()
}

// ─── Scripts ─────────────────────────────────────────────────────────────────
// KEYS[1] is always the session's hash. Each script returns 1 when it acted
// on an existing session (or created one) and 0 otherwise.
val createScript: String = "if redis.call('EXISTS', KEYS[1]) == 1 then return 0 end redis.call('HSET', KEYS[1], 'm:created', ARGV[1], 'm:accessed', ARGV[1]) redis.call('PEXPIRE', KEYS[1], ARGV[2]) return 1"

val setScript: String = "if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end redis.call('HSET', KEYS[1], ARGV[1], ARGV[2], 'm:accessed', ARGV[3]) return 1"

val deleteScript: String = "if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end redis.call('HDEL', KEYS[1], ARGV[1]) redis.call('HSET', KEYS[1], 'm:accessed', ARGV[2]) return 1"

val clearScript: String = "if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end local fields = redis.call('HKEYS', KEYS[1]) for _, f in ipairs(fields) do if string.sub(f, 1, 2) == 'd:' then redis.call('HDEL', KEYS[1], f) end end redis.call('HSET', KEYS[1], 'm:accessed', ARGV[1]) return 1"

val touchScript: String = "if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end redis.call('HSET', KEYS[1], 'm:accessed', ARGV[1]) redis.call('PEXPIRE', KEYS[1], ARGV[2]) return 1"

// ARGV: created, accessed, expiresAt (0 = none), then field, value pairs.
val saveScript: String = "redis.call('DEL', KEYS[1]) redis.call('HSET', KEYS[1], 'm:created', ARGV[1], 'm:accessed', ARGV[2]) for i = 4, #ARGV, 2 do redis.call('HSET', KEYS[1], ARGV[i], ARGV[i + 1]) end if tonumber(ARGV[3]) > 0 then redis.call('PEXPIREAT', KEYS[1], ARGV[3]) end return 1"

val loadScript: String = "return {redis.call('HGETALL', KEYS[1]), redis.call('PTTL', KEYS[1])}"

func failure(code: in String, e: in RedisLib.RedisError): SessionError {
  SessionError(message = e.message(), code = code)
}

func malformed(code: in String, what: in String): SessionError {
  SessionError(message = "unexpected reply from Redis: " + what, code = code)
}

func notFound(): SessionError {
  SessionError(
    message = "session not found; call create() to obtain a fresh id before set()",
    code = "SESSION_NOT_FOUND"
  )
}

func invalidId(): SessionError {
  SessionError(
    message = "invalid session id (expected 1-128 characters from [A-Za-z0-9_-])",
    code = "INVALID_SESSION_ID"
  )
}

// Run a script that answers 1 or 0.
func runFlag(
  store: in RedisSessionStore,
  code: in String,
  script: in String,
  sessionId: in String,
  args: in slice[String],
): Result[Bool, SessionError] {
  match RedisLib.eval(store.client, script, [redisKey(store.options, sessionId)], args) {
    case Err(e) -> Err(failure(code, e))
    case Ok(RInt(n)) -> Ok(n == 1i64)
    case Ok(other) -> Err(malformed(code, Resp.kindOf(other)))
  }
}

func longField(fields: in Map[String, String], name: in String): Long {
  if not fields.containsKey(name) {
    return 0i64
  }
  match parseOptLong(fields[name]) {
    case Some(n) -> n
    case None -> 0i64
  }
}

func textOf(code: in String, v: in Resp.RespValue): Result[String, SessionError] {
  match Resp.asText(v) {
    case Some(s) -> Ok(s)
    case None -> Err(malformed(code, "a session field that is not UTF-8 text"))
  }
}

func decodeLoad(sessionId: in String, reply: in Resp.RespValue): Result[Option[SessionData], SessionError] {
  match reply {
    case RArray(parts) -> {
      if parts.count != 2 {
        return Err(malformed("LOAD_FAILED", "a load reply of " + parts.count.toString() + " parts"))
      }
      val flat: List[Resp.RespValue] = match parts[0] {
        case RArray(items) -> items
        case RMap(items) -> items
        case other -> return Err(malformed("LOAD_FAILED", Resp.kindOf(other)))
      }
      if flat.count == 0 {
        return Ok(None)
      }
      if flat.count % 2 != 0 {
        return Err(malformed("LOAD_FAILED", "a hash with an odd number of items"))
      }
      val remaining: Long = match parts[1] {
        case RInt(n) -> n
        case other -> return Err(malformed("LOAD_FAILED", Resp.kindOf(other)))
      }
      val meta: Map[String, String] = newMap()
      val entries: Map[String, String] = newMap()
      var i = 0
      while i < flat.count {
        val name = match textOf("LOAD_FAILED", flat[i]) { case Ok(s) -> s; case Err(e) -> return Err(e) }
        val value = match textOf("LOAD_FAILED", flat[i + 1]) { case Ok(s) -> s; case Err(e) -> return Err(e) }
        if name.startsWith(entryFieldPrefix) {
          entries[name.substring(entryFieldPrefix.length)] = value
        } else {
          meta[name] = value
        }
        i = i + 2
      }
      val expiresAt: Long = if remaining > 0i64 { nowEpochMillis() + remaining } else { 0i64 }
      Ok(
        Some(
          value = SessionData(
            id = sessionId,
            entries = entries,
            createdAt = longField(meta, createdField),
            lastAccessedAt = longField(meta, accessedField),
            expiresAt = expiresAt
          )
        )
      )
    }
    case other -> Err(malformed("LOAD_FAILED", Resp.kindOf(other)))
  }
}

val createAttempts: Int = 3

impl SessionStore for RedisSessionStore {
  func create(): Result[String, SessionError] {
    val ttlMillis = self.options.ttlSeconds.toLong() * 1000i64
    var attempt = 0
    while attempt < createAttempts {
      val id = newSessionId()
      match runFlag(self, "CREATE_FAILED", createScript, id, [nowEpochMillis().toString(), ttlMillis.toString()]) {
        case Err(e) -> return Err(e)
        case Ok(true) -> return Ok(id)
        case Ok(false) -> ()
      }
      attempt = attempt + 1
    }
    Err(
      SessionError(
        message = "could not allocate an unused session id; check the server's random source and the key prefix",
        code = "CREATE_FAILED"
      )
    )
  }

  func load(sessionId: in String): Result[Option[SessionData], SessionError] {
    if not isValidSessionId(sessionId) {
      return Ok(None)
    }
    match RedisLib.eval(self.client, loadScript, [redisKey(self.options, sessionId)], []) {
      case Err(e) -> Err(failure("LOAD_FAILED", e))
      case Ok(reply) -> decodeLoad(sessionId, reply)
    }
  }

  func save(data: in SessionData): Result[Unit, SessionError] {
    if not isValidSessionId(data.id) {
      return Err(invalidId())
    }
    if data.expiresAt < 0i64 {
      return Err(SessionError(message = "expiresAt must not be negative", code = "SAVE_FAILED"))
    }
    val args: List[String] = newList()
    args.add(data.createdAt.toString())
    args.add(data.lastAccessedAt.toString())
    args.add(data.expiresAt.toString())
    for k in mapKeys(data.entries) {
      args.add(entryFieldPrefix + k)
      args.add(data.entries[k])
    }
    match runFlag(self, "SAVE_FAILED", saveScript, data.id, args.toArray()) {
      case Err(e) -> Err(e)
      case Ok(_) -> Ok(())
    }
  }

  func destroy(sessionId: in String): Result[Unit, SessionError] {
    if not isValidSessionId(sessionId) {
      return Ok(())
    }
    match RedisLib.del(self.client, [redisKey(self.options, sessionId)]) {
      case Err(e) -> Err(failure("DESTROY_FAILED", e))
      case Ok(_) -> Ok(())
    }
  }

  func touch(sessionId: in String): Result[Unit, SessionError] {
    if not isValidSessionId(sessionId) {
      return Ok(())
    }
    val ttlMillis = self.options.ttlSeconds.toLong() * 1000i64
    match runFlag(self, "TOUCH_FAILED", touchScript, sessionId, [nowEpochMillis().toString(), ttlMillis.toString()]) {
      case Err(e) -> Err(e)
      case Ok(_) -> Ok(())
    }
  }

  func get(sessionId: in String, key: in String): Result[Option[String], SessionError] {
    if not isValidSessionId(sessionId) {
      return Ok(None)
    }
    match RedisLib.hget(self.client, redisKey(self.options, sessionId), entryFieldPrefix + key) {
      case Err(e) -> Err(failure("GET_FAILED", e))
      case Ok(v) -> Ok(v)
    }
  }

  func set(sessionId: in String, key: in String, value: in String): Result[Unit, SessionError] {
    if not isValidSessionId(sessionId) {
      return Err(notFound())
    }
    match runFlag(self, "SET_FAILED", setScript, sessionId, [entryFieldPrefix + key, value, nowEpochMillis().toString()]) {
      case Err(e) -> Err(e)
      case Ok(true) -> Ok(())
      case Ok(false) -> Err(notFound())
    }
  }

  func delete(sessionId: in String, key: in String): Result[Unit, SessionError] {
    if not isValidSessionId(sessionId) {
      return Ok(())
    }
    match runFlag(self, "DELETE_FAILED", deleteScript, sessionId, [entryFieldPrefix + key, nowEpochMillis().toString()]) {
      case Err(e) -> Err(e)
      case Ok(_) -> Ok(())
    }
  }

  func clear(sessionId: in String): Result[Unit, SessionError] {
    if not isValidSessionId(sessionId) {
      return Ok(())
    }
    match runFlag(self, "CLEAR_FAILED", clearScript, sessionId, [nowEpochMillis().toString()]) {
      case Err(e) -> Err(e)
      case Ok(_) -> Ok(())
    }
  }
}
