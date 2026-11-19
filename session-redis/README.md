# Lyric.Session.Redis

`Session.SessionStore` (from `Lyric.Session` in lyric-lang) on a Redis server, built on the pure-Lyric
`Lyric.Redis` client of this repository. Every read-modify-write is one Lua script, so concurrent
requests on a session lose no update and `set` can never create a session. It runs on `dotnet`,
`jvm` and `native` and is `experimental` (0.1.0).

| | |
|---|---|
| Lyric packages | `Session.Redis` (the store), `Session.Redis.Admin` (Redis-only operations) |
| Targets | `dotnet`, `jvm`, `native` (Linux x86-64) |
| Stability | experimental (0.1.0) |
| Interface | `Lyric.Session` (`Session.SessionStore`), re-exported with `pub use Session`; conformance suite `Testing.Conformance.Session` from `Lyric.Conformance` |
| Third-party libraries | none; `Lyric.Redis` (`redis/` in this repository) is a `{ workspace = true }` dependency, and so is `Lyric.Net` (`net/`), which `Lyric.Redis` connects through and whose `Net.Tcp.Testing` the tests use |
| Server requirement | `EVAL` must be allowed for the connecting user; Redis 4.0 or later (the scripts use `HSET` with several fields), 6.0 or later for ACL users, RESP3 and TLS |

## Usage

Inside this workspace an application declares `"Lyric.Session.Redis" = { workspace = true }` under
`[dependencies]`, and `"Lyric.Session"` and `"Lyric.Redis"` as well when it imports them itself, as the
examples below do.

```lyric
import Std.Core
import Session
import Session.Redis as RedisSessions

func example(url: in String): Result[Option[String], SessionError] {
  val store = RedisSessions.connect(url, RedisSessions.options("myapp:session:", 3600))?
  val id = store.create()?
  store.set(id, "user", "alice")?
  val user = store.get(id, "user")?
  store.touch(id)?
  store.destroy(id)?
  store.client.close()
  Ok(user)
}
```

`example("redis://sessions.internal:6379")` connects and authenticates before the store is returned,
creates a session that lives 3600 seconds, and answers `Ok(Some("alice"))`. Every function and every
`SessionStore` method returns `Result[_, Session.SessionError]`; `SessionError` has a `code` and a
`message`.

`Session.Redis` re-exports `Session`, so the types and functions of `Session` are also reachable
through the alias (`RedisSessions.SessionStore`, `RedisSessions.isValidSessionId`). To construct a
`SessionError` or a `SessionData`, import `Session` and use the bare name, as above: on `jvm` a
constructor call written through the alias (`RedisSessions.SessionError(...)`) does not compile (see
Known gaps).

Variations:

- **Through the interface.** A `RedisSessionStore` is a `SessionStore`:

  ```lyric
  func throughTheInterface(sessions: in SessionStore, id: in String): Result[Int, SessionError] {
    match sessions.load(id)? {
      case Some(data) -> Ok(data.entries.count)
      case None -> Ok(0)
    }
  }
  ```

- **A new id at login.** `set` never creates a session, so a login handler destroys the old id and
  creates a new one:

  ```lyric
  func loginAgain(store: in RedisSessions.RedisSessionStore, oldId: in String, user: in String): Result[String, SessionError] {
    store.destroy(oldId)?
    val id = store.create()?
    store.set(id, "user", user)?
    Ok(id)
  }
  ```

- **On an existing client.** `RedisSessions.fromClient(client, RedisSessions.options("myapp:session:",
  1800))` builds a store on a `Redis.RedisClient` that other code also uses. The client is shared, not
  owned.
- **Over TLS.** `connect` takes a `rediss://` URL: TLS with the system trust store and the URL's host
  as the server name. Everything a URL cannot carry (a private CA, a client certificate, timeouts, the
  protocol version) goes through `connectWith`, which takes a `Redis.RedisConfig`:

  ```lyric
  import Redis

  func overTls(caPem: in String, certPem: in String, keyPem: in String): Result[RedisSessions.RedisSessionStore, SessionError] {
    val base = match Redis.configFromUrl("rediss://sessions.internal:6380") {
      case Ok(cfg) -> cfg
      case Err(e) -> return Err(SessionError(message = e.message(), code = "CONNECT_FAILED"))
    }
    val cfg = Redis.withClientIdentity(Redis.withExclusiveCaCertificate(base, caPem), certPem, keyPem)
    RedisSessions.connectWith(cfg, RedisSessions.options("myapp:session:", 3600))
  }
  ```

  `Redis.configFromUrl` returns a `Redis.RedisError`, not a `SessionError`, which is why it is matched
  and not propagated with `?`. The TLS options (`withExclusiveCaCertificate`, `withClientIdentity`,
  `withTlsServerName`, `withMinTlsVersion`, `withInsecureSkipVerify` and its `LYRIC_TLS_ALLOW_INSECURE`
  policy) are those of `Lyric.Redis`; see its README.

## Configuration

Explicit values only. The server is named by a URL or a `Redis.RedisConfig`; both are documented in the
`Lyric.Redis` README (URL syntax, credentials, database, timeouts, TLS). The store itself is configured
by `RedisSessionOptions`, built with `options(keyPrefix, ttlSeconds)`:

| Field | Default | Meaning |
|---|---|---|
| `keyPrefix` | (required, not empty) | Prepended to the session id to form the Redis key. Give each store its own |
| `ttlSeconds` | (required) | Lifetime of a session after `create` or `touch`: 1 to 31536000, the limit `Session.maxSessionTtlSeconds()` sets |

An empty prefix or a lifetime outside the range is a contract violation (a panic), not an `Err`.

The package reads no environment variable. In particular it does not read the
`LYRIC_CONFIG_SESSION_SESSION_*` variables that `Session.sessionConfigFromEnv()` reads (and that
the removed `Session.connectRedis()` read as well): the store's lifetime is the `ttlSeconds` passed to `options`, and an
application that also builds a cookie with `Session.cookieHeader` keeps the two lifetimes equal itself.
`LYRIC_TLS_ALLOW_INSECURE` is read by `Lyric.Redis`, and only for a configuration with
`withInsecureSkipVerify`.

## Surprises

Expiry:

- **Only `create` and `touch` set the expiry**, to `ttlSeconds` from now. `set`, `delete` and `clear`
  leave it alone, and `get` and `load` do not extend it either. A session expires `ttlSeconds` after it
  was created, however busy it is, unless the application calls `touch` on each request.
- **`get` and `load` do not count as access.** `lastAccessedAt` is advanced by `set`, `delete`, `clear`
  and `touch` only; it is the time of the last write.
- **`load` computes `expiresAt` on each call** as the local clock plus the remaining lifetime Redis
  reports, so two loads of an untouched session can differ by a few milliseconds, and the value follows
  the application's clock, not the server's. `load` followed by `save` keeps the remaining lifetime.
- **`save` takes the expiry from `SessionData.expiresAt`, not from `ttlSeconds`.** `0` saves a session
  that never expires (as the interface defines), and a time already past removes the session. Because
  the time is absolute and Redis compares it with its own clock, a clock difference between the
  application and Redis shifts the lifetime of a saved session; `create` and `touch` use relative
  lifetimes and are not affected.
- **`touch` gives a session without an expiry one.** A session saved with `expiresAt = 0` expires
  `ttlSeconds` after its first `touch`.
- **Expired sessions are removed by Redis**: the hash's Redis expiry is the session's expiry, on the
  server's clock.

Ids and existence:

- **`set` never creates a session.** On an id the store does not hold (never created, destroyed,
  expired or malformed) it is `Err(SESSION_NOT_FOUND)` and writes nothing (session fixation, lyric-lang
  #316); a `set` racing a `destroy` cannot bring the session back.
- **`save` does create a session, under any valid id the caller chooses**, and replaces the whole
  session when one exists. Never pass it an id that came from a request.
- **The other methods are silent on an unknown id.** `touch`, `delete`, `clear` and `destroy` answer
  `Ok` and do nothing; `get` and `load` answer `Ok(None)`. Only `set` tells a missing session apart.
- **Malformed ids** (`Session.isValidSessionId` is false: not 1 to 128 characters of `[A-Za-z0-9_-]`)
  are answered the same way without contacting Redis: `load` and `get` are `Ok(None)`,
  `delete`/`clear`/`touch`/`destroy` are `Ok`, `set` is `SESSION_NOT_FOUND`, `save` is
  `INVALID_SESSION_ID`.
- **Session ids are 64 lower-case hex characters** (32 bytes from `Std.SecureRandom`), not the UUIDs of
  `Session.InProcessSessionStore`.

Layout:

- **A session is one Redis hash at `<keyPrefix><sessionId>`.** Entries are the fields `d:<key>`;
  `m:created` and `m:accessed` hold the creation and last-write times (epoch milliseconds, from the
  application's clock). Entry keys and values are stored verbatim: an empty key, an empty value and a
  key named `m:created` are all ordinary entries.
- **This layout is not that of the removed `Session.connectRedis()`** of lyric-lang, which stored a
  session as one JSON string (see "Relation" below). Sessions it wrote under a prefix cannot be read
  here and a key of another type under the prefix is an error, so use a new prefix or delete the old keys.
- **A key of another Redis type under the prefix is an error, not a missing session**: `load` is
  `LOAD_FAILED` and `set` is `SET_FAILED`. `destroy` removes it.
- **Prefixes nest.** `Admin.sessionCount` and `Admin.destroyAll` work by `SCAN MATCH <prefix>*`, so
  they count and delete every key under the prefix, the sessions of a store whose prefix extends this
  one included. Choose prefixes of which none is the beginning of another.

Errors and connection:

- **Every connection failure is `CONNECT_FAILED`**: an unusable URL, an unreachable server, a refused
  password, an unusable TLS configuration and a failed handshake all become a `SessionError` with that
  code and the Redis client's message. Other failures carry the code of the operation
  (`CREATE_FAILED`, `LOAD_FAILED`, `SAVE_FAILED`, `DESTROY_FAILED`, `TOUCH_FAILED`, `GET_FAILED`,
  `SET_FAILED`, `DELETE_FAILED`, `CLEAR_FAILED`; `TTL_FAILED` and `SCAN_FAILED` from `Admin`), or are
  `SESSION_NOT_FOUND` / `INVALID_SESSION_ID`. Messages never contain the password. No Redis failure
  panics.
- **A user without `EVAL` permission can connect but not use the store**: `create`, `load`, `save`,
  `touch`, `set`, `delete` and `clear` are each one `EVAL` and fail with their own code and the
  server's `NOPERM` message. Only `get` (`HGET`) and `destroy` (`DEL`) are plain commands.
- **Atomicity is per call.** Each method is atomic; a `load`, change, `save` sequence is not, and the
  `save` overwrites whatever was written in between. Use `set`/`delete` for single entries.
- **A store is one connection and has no `close`.** It holds one `Redis.RedisClient`, which serializes
  commands, so a store can be shared between threads but its calls queue behind each other. Close the
  connection with `store.client.close()`; after that every operation is an `Err`.
- **With Redis down every call waits for the connect timeout** (5 s by default) before it fails; lower
  it with `Redis.withTimeouts` and `connectWith`.

## Backend-specific module

`Session.Redis.Admin` holds what Redis offers beyond `SessionStore`, kept apart so that code written
against the interface stays portable. All three return `Result[_, SessionError]`.

| Function | |
|---|---|
| `remainingMillis(store, sessionId)` | Milliseconds until the session expires: `None` when the store does not hold it (or the id is malformed), `Some(0)` when it has no expiry |
| `sessionCount(store)` | How many keys are under the prefix, by `SCAN`; its cost grows with the size of the database |
| `destroyAll(store)` | Remove every key under the prefix (`SCAN` + `DEL`, never `FLUSHDB`) and return how many: "log everyone out" |

`Session.Redis` itself adds `redisKey(options, sessionId)` and `newSessionId()`.

### Relation to the removed `Session.connectRedis()` in lyric-lang

`Lyric.Session` had an experimental Redis store (`NativeSessionStore` and `Session.connectRedis()`, on
StackExchange.Redis and Lettuce) that stored a session as one JSON string and updated it with separate
load and save round trips. It was removed in lyric-lang (D614) and this package is its replacement:
`Lyric.Session` is the `SessionStore` interface and the in-process store only. To migrate, build a store
with `connect(url, options(prefix, ttlSeconds))`, passing the URL, key prefix and lifetime the old
`LYRIC_CONFIG_SESSION_REDISSESSION_*` and `LYRIC_CONFIG_SESSION_SESSION_TTLSECONDS` variables carried;
existing sessions are not carried over.

## Tests

```sh
# Hermetic (no Redis needed): session ids, key naming, malformed ids, behaviour with Redis unreachable
lyric test --manifest session-redis/lyric.toml --target dotnet
lyric test --manifest session-redis/lyric.toml --target jvm
lyric test --manifest session-redis/lyric.toml --target native

# Integration: the SessionStore conformance suite (RESP2, RESP3, and over TLS with a client
# certificate) and backend tests against a real server
redis-server --port 16379 --save "" --appendonly no --daemonize yes
export LYRIC_REDIS_URL=redis://127.0.0.1:16379
eval "$(scripts/ci/start-redis-tls.sh)"
lyric test --manifest session-redis/lyric.toml --target dotnet --features integration
lyric test --manifest session-redis/lyric.toml --target jvm --features integration
lyric test --manifest session-redis/lyric.toml --target native --features integration
```

`bash scripts/ci/test-native.sh [--integration]` runs every package that claims `native`, as the
`native` CI job does.

Run from the repository root. The integration module is compiled only with `--features integration`
and fails with a message naming the variable when one is unset:

| Variable | Meaning |
|---|---|
| `LYRIC_REDIS_URL` | `redis://` URL of a plain server |
| `LYRIC_REDIS_MTLS_URL` | `rediss://` URL of a TLS server that requires a client certificate |
| `LYRIC_REDIS_TLS_CA`, `LYRIC_REDIS_TLS_CERT`, `LYRIC_REDIS_TLS_KEY` | PEM files: the CA, a client certificate and its PKCS#8 key |

`scripts/ci/start-redis-tls.sh` starts the TLS servers and prints the TLS variables. Every session the
suite writes is under a random prefix and is removed. The conformance suite cannot run hermetically:
there is no in-process Redis.

## Pinned versions

No third-party library. `Lyric.Redis` is the version in this repository; `Lyric.Session` and
`Lyric.Conformance` are those of the `lyric-lang` commit in `.lyric-lang-rev`, resolved through the root
`[workspace.overrides]`. Redis servers the suites run against: the `redis:7.4` service container (plain)
and the `ubuntu-latest` runner's `redis-server` package (TLS) in CI, 7.0.15 in development.

## Known gaps

- Everything listed under "Known gaps" of `Lyric.Redis` (one connection, no Cluster or Sentinel, the
  limits of its TLS options).
- No sliding expiry: the application calls `touch`.
- Scripts are sent with `EVAL` on every call (no `EVALSHA`). Each script touches one key, so it is valid
  on Redis Cluster, but the client itself has no Cluster support.
- The option contracts spell the one-year TTL limit as the literal `31536000`: calling
  `Session.maxSessionTtlSeconds()` in a `requires:` clause of this package is rejected with `T0133`
  ("not @pure") although the function is `@pure` (reported to lyric-lang; a test pins the two values
  equal).
- On `jvm`, constructing a re-exported record through the import alias
  (`RedisSessions.SessionError(message = ..., code = ...)`) fails with `J008` ("unresolved call to
  'SessionError'"); the same line compiles on `dotnet`. Import `Session` and use the bare name. No
  lyric-lang issue number yet.
- The lyric-lang packages are `{ workspace = true }` dependencies resolved through the root
  `[workspace.overrides]`; they become registry ranges once the interface is consumed from the registry.
