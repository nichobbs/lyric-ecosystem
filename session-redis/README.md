# Lyric.Session.Redis

`Session.SessionStore` (from `Lyric.Session` in lyric-lang) on a Redis server, built on the pure-Lyric
`Lyric.Redis` client of this repository. Every read-modify-write is one Lua script, so concurrent
requests on a session lose no update and `set` can never create a session.

| | |
|---|---|
| Lyric packages | `Session.Redis` (the store), `Session.Redis.Admin` (Redis-only operations) |
| Targets | `dotnet`, `jvm` |
| Stability | experimental (0.1.0) |
| Interface | `Lyric.Session` 0.1.0 (`Session.SessionStore`); conformance suite `Testing.Conformance.Session` from `Lyric.Testing` 0.1.0 |
| Third-party libraries | none (`Lyric.Redis`, path dependency `../redis`) |
| Tested against | Redis 7.0.15, plain and TLS |
| Server requirement | `EVAL` must be allowed for the connecting user |

## Use

```lyric
import Session.Redis as RedisSessions

match RedisSessions.connect("redis://sessions.internal:6379", RedisSessions.options("myapp:session:", 3600)) {
  case Err(e) -> println(e.message)
  case Ok(store) -> match store.create() {
    case Ok(id) -> { val _ = store.set(id, "user", "alice") }
    case Err(e) -> println(e.message)
  }
}
```

`Session.Redis` re-exports `Session`, so `SessionStore`, `SessionData`, `SessionError` and the
`Session` functions are reachable through it (`RedisSessions.SessionStore`, `RedisSessions.isValidSessionId`). `RedisSessions.fromClient(client, options)` builds a store
on an existing `Redis.RedisClient`.

## TLS

`RedisSessions.connect` takes a `rediss://` URL: TLS with the system trust store and the URL's host as
the server name. Everything a URL cannot carry goes through `RedisSessions.connectWith(cfg, options)`,
which takes a `Redis.RedisConfig`:

```lyric
import Redis
import Session.Redis as RedisSessions

match Redis.configFromUrl("rediss://sessions.internal:6380") {
  case Err(e) -> println(e.message())
  case Ok(base) -> {
    val cfg = Redis.withClientIdentity(Redis.withExclusiveCaCertificate(base, caPem), certPem, keyPem)
    val store = RedisSessions.connectWith(cfg, RedisSessions.options("myapp:session:", 3600))
  }
}
```

The TLS options (`withExclusiveCaCertificate`, `withClientIdentity`, `withTlsServerName`,
`withMinTlsVersion`, `withInsecureSkipVerify` and its `LYRIC_TLS_ALLOW_INSECURE` policy) are those of
`Lyric.Redis`; see its README. An unusable TLS configuration and a failed handshake are both
`SessionError` values with the code `CONNECT_FAILED` and the Redis client's message.

## Configuration

Explicit values only: the URL or `Redis.RedisConfig` (see `Lyric.Redis`) and `RedisSessionOptions`:

- `keyPrefix` (required, not empty): prepended to the session id to form the Redis key.
- `ttlSeconds` (1 to 31536000, the limit `Session.maxSessionTtlSeconds()` sets): lifetime of a session
  after `create` or `touch`.

## Behaviour

- **Layout.** A session is one Redis hash at `<keyPrefix><sessionId>`: entries are the fields `d:<key>`,
  and `m:created` / `m:accessed` hold the creation and last-write times (epoch milliseconds). The hash's
  Redis expiry is the session's expiry, so Redis removes expired sessions.
- **Session ids** are 32 bytes from `Std.SecureRandom`, as 64 hex characters.
- **Atomicity.** `create`, `set`, `delete`, `clear`, `touch`, `save` and `load` are each one `EVAL`.
  `set` on an id the store does not hold (never created, destroyed, expired or malformed) is
  `Err(SESSION_NOT_FOUND)` and creates nothing (session fixation, lyric-lang #316); a `set` racing a
  `destroy` cannot bring the session back.
- **Expiry.** `create` and `touch` set the expiry to `ttlSeconds` from now. `set`, `delete` and `clear`
  leave it alone. `save` takes it from `SessionData.expiresAt` (`0` = no expiry, as the interface
  defines; a time already past removes the session). `load` reports the remaining lifetime as `expiresAt`
  and does not extend it, so `load` followed by `save` keeps the lifetime.
- **Malformed ids** (`Session.isValidSessionId` is false) are answered without contacting Redis: `load`
  and `get` are `Ok(None)`, `delete`/`clear`/`touch`/`destroy` are `Ok`, `set` is `SESSION_NOT_FOUND`,
  `save` is `INVALID_SESSION_ID`.
- **Errors** are `Session.SessionError` values with the codes `CONNECT_FAILED`, `CREATE_FAILED`,
  `LOAD_FAILED`, `SAVE_FAILED`, `DESTROY_FAILED`, `TOUCH_FAILED`, `GET_FAILED`, `SET_FAILED`,
  `DELETE_FAILED`, `CLEAR_FAILED`, `SESSION_NOT_FOUND`, `INVALID_SESSION_ID`. Messages never contain the
  password. No Redis failure panics.
- **Thread safety.** A store holds one `Redis.RedisClient`, which serializes commands on one connection;
  a store can be shared between threads.
- `Session.Redis.Admin`: `remainingMillis`, `sessionCount`, `destroyAll` (`SCAN` + `DEL` under the
  prefix, never `FLUSHDB`).

## Relation to `Session.connectRedis()` in lyric-lang

`Lyric.Session` still carries an experimental Redis store (`NativeSessionStore`, behind its `dotnet` /
`jvm` features, on StackExchange.Redis / Lettuce) that stores a session as one JSON string and updates it
with separate load and save round trips. This package does not use or change it and defines no type of
the same name. The two layouts are different, so a deployment uses one or the other for a given prefix.

## Tests

```sh
# Hermetic (no Redis needed): session ids, key naming, malformed ids, behaviour with Redis unreachable
lyric test --manifest session-redis/lyric.toml --target dotnet
lyric test --manifest session-redis/lyric.toml --target jvm

# Integration: the SessionStore conformance suite (RESP2, RESP3, and over TLS with a client
# certificate) and backend tests against a real server
redis-server --port 16379 --save "" --appendonly no --daemonize yes
export LYRIC_REDIS_URL=redis://127.0.0.1:16379
eval "$(scripts/ci/start-redis-tls.sh)"
lyric test --manifest session-redis/lyric.toml --target dotnet --features integration
lyric test --manifest session-redis/lyric.toml --target jvm --features integration
```

The integration module is compiled only with `--features integration` and fails with a message naming
the variable when one is unset: `LYRIC_REDIS_URL` (a plain server), and for the TLS run
`LYRIC_REDIS_MTLS_URL` (a `rediss://` server that requires a client certificate), `LYRIC_REDIS_TLS_CA`,
`LYRIC_REDIS_TLS_CERT` and `LYRIC_REDIS_TLS_KEY` (PEM files), which `scripts/ci/start-redis-tls.sh`
prints. Every session it writes is under a random prefix and is removed. The conformance suite cannot run hermetically: there is no in-process Redis.

## Known gaps

- Everything listed under "Known gaps" of `Lyric.Redis` (one connection, no Cluster or Sentinel, the
  limits of its TLS options).
- Scripts are sent with `EVAL` on every call (no `EVALSHA`). Each script touches one key, so it is valid
  on Redis Cluster, but the client itself has no Cluster support.
- The option contracts spell the one-year TTL limit as the literal `31536000`: calling
  `Session.maxSessionTtlSeconds()` in a `requires:` clause of this package is rejected with `T0133`
  ("not @pure") although the function is `@pure` (reported to lyric-lang; a test pins the two values
  equal).
- The lyric-lang packages are `{ workspace = true }` dependencies resolved through the root
  `[workspace.overrides]`; they become registry ranges once the interface is consumed from the registry.
