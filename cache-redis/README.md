# Lyric.Cache.Redis

`Cache.CacheStore` (from `Lyric.Cache` in lyric-lang) on a Redis server, built on the pure-Lyric
`Lyric.Redis` client of this repository. It runs on `dotnet`, `jvm` and `native` and is `experimental` (0.1.0).

| | |
|---|---|
| Lyric packages | `Cache.Redis` (the store), `Cache.Redis.Extras` (Redis-only operations) |
| Targets | `dotnet`, `jvm`, `native` (Linux x86-64) |
| Stability | experimental (0.1.0) |
| Interface | `Lyric.Cache` (`Cache.CacheStore`), re-exported with `pub use Cache`; conformance suite `Testing.Conformance.Cache` from `Lyric.Conformance` |
| Third-party libraries | none; `Lyric.Redis` (`redis/` in this repository) is a `{ workspace = true }` dependency, and so is `Lyric.Net` (`net/`), which `Lyric.Redis` connects through and whose `Net.Tcp.Testing` the tests use |

## Usage

Inside this workspace an application declares `"Lyric.Cache.Redis" = { workspace = true }` under
`[dependencies]`, and `"Lyric.Redis"` as well when it names `Redis.RedisError` or builds a
`Redis.RedisConfig` itself.

```lyric
import Std.Core
import Redis
import Cache.Redis as RedisCache

func example(url: in String, userJson: in String): Result[Option[String], Redis.RedisError] {
  val store = RedisCache.connect(url, RedisCache.options("myapp:cache:"))?
  RedisCache.setWithTtl(store, "users:42", userJson, 300)
  val hit = RedisCache.get(store, "users:42")
  RedisCache.delete(store, "users:42")
  if store.errors.errorCount() > 0 {
    match store.errors.lastError() {
      case Some(e) -> println("cache degraded: " + e.message())
      case None -> ()
    }
  }
  store.client.close()
  Ok(hit)
}
```

`example("redis://cache.internal:6379/1", json)` connects and authenticates before the store is
returned; `connect` is the only step with an error (`Redis.RedisError`). `Cache.Redis` re-exports
`Cache`, so `CacheStore` and the `Cache` wrappers (`get`, `set`, `setWithTtl`, `delete`, `clear`) are
reachable through it, as above. The wrappers and the interface methods return `Option[String]` or
`Unit`: they have no error to propagate.

Variations:

- **Through the interface.** A `RedisCacheStore` is a `CacheStore`:

  ```lyric
  func throughTheInterface(cache: in RedisCache.CacheStore): Option[String] {
    cache.set("k", "v", 0)
    cache.get("k")
  }
  ```

- **With the failure visible.** `tryGet`, `trySet`, `tryDelete` and `tryClear` return
  `Result[_, Redis.RedisError]`; the interface methods are built on them:

  ```lyric
  func visibleFailures(store: in RedisCache.RedisCacheStore): Result[Long, Redis.RedisError] {
    RedisCache.trySet(store, "k", "v", 60)?
    val value = RedisCache.tryGet(store, "k")?
    val existed = RedisCache.tryDelete(store, "k")?
    println(existed.toString() + (match value { case Some(v) -> v; case None -> "" }))
    RedisCache.tryClear(store)
  }
  ```

- **On an existing client.** `RedisCache.fromClient(client, RedisCache.options("myapp:cache:"))` builds
  a store on a `Redis.RedisClient` that other code also uses. The client is shared, not owned.
- **Over TLS.** `connect` takes a `rediss://` URL: TLS with the system trust store and the URL's host
  as the server name. Everything a URL cannot carry (a private CA, a client certificate, timeouts, the
  protocol version) goes through `connectWith`, which takes a `Redis.RedisConfig`:

  ```lyric
  func overTls(caPem: in String, certPem: in String, keyPem: in String): Result[RedisCache.RedisCacheStore, Redis.RedisError] {
    val base = Redis.configFromUrl("rediss://cache.internal:6380/1")?
    val cfg = Redis.withClientIdentity(Redis.withExclusiveCaCertificate(base, caPem), certPem, keyPem)
    RedisCache.connectWith(cfg, RedisCache.options("myapp:cache:"))
  }
  ```

  The TLS options (`withExclusiveCaCertificate`, `withClientIdentity`, `withTlsServerName`,
  `withMinTlsVersion`, `withInsecureSkipVerify` and its `LYRIC_TLS_ALLOW_INSECURE` policy) are those of
  `Lyric.Redis`; see its README. An unusable TLS configuration is `InvalidConfig` and a failed
  handshake `TlsFailed`, both returned by `connect` / `connectWith`.

## Configuration

Explicit values only. The server is named by a URL or a `Redis.RedisConfig`; both are documented in the
`Lyric.Redis` README (URL syntax, credentials, database, timeouts, TLS). The store itself is configured
by `RedisCacheOptions`:

| Field | Built with | Default | Meaning |
|---|---|---|---|
| `keyPrefix` | `options(keyPrefix)` | (required, not empty) | Prepended to every cache key; `clear()` removes the keys under it. Give each store its own |
| `ttlUnitMillis` | `optionsWithTtlUnit(keyPrefix, ttlUnitMillis)` | 1000 | The length of one `ttlSeconds` unit, 1 to 1000. Leave it at 1000 in production; it exists so a test suite can make TTLs elapse quickly while Redis still does the expiring |

The connection defaults that matter to a cache are `Lyric.Redis`'s: database 0, RESP2, and 5000 ms each
for the connect and I/O timeouts (`Redis.withTimeouts`).

The package reads no environment variable. `LYRIC_TLS_ALLOW_INSECURE` is read by `Lyric.Redis`, and
only for a configuration with `withInsecureSkipVerify`.

## Surprises

Failures:

- **The interface methods never report a failure.** `CacheStore` has no error channel, so a failed
  `get` is a miss and a failed `set`, `delete` or `clear` is dropped. Nothing panics. The failure is
  counted and kept in `store.errors` (`errorCount()`, `lastError()`): the count only grows and only the
  most recent error is kept. The `try...` functions do not touch that log.
- **An unreachable Redis makes every call slow, not just the first.** Each call dials again and waits
  up to the connect timeout (5 s by default) before it answers a miss; there is no circuit breaker. A
  server that accepts the connection but answers late costs up to the I/O timeout per call. Lower both
  with `Redis.withTimeouts` and `connectWith` for a cache that must fail fast.
- **`connect` and `connectWith` do report errors**, and connect eagerly: an unreachable server at
  start-up is an `Err`, not a store that misses. A store built with `fromClient` on a `Redis.lazy`
  client starts in the degraded mode instead.
- **A store has no `close`.** Close the connection with `store.client.close()`. After that every `get`
  is a recorded miss (`CLIENT_CLOSED`).

Keys and values:

- **An entry is one Redis string at `<keyPrefix><key>`**, written with `SET` (`SET ... PX` for a TTL).
  Keys that another client writes under the prefix are entries of the store: `get` returns them and
  `clear` deletes them.
- **`clear()` deletes by prefix, so a nested prefix is part of the outer store.** It runs `SCAN MATCH
  <prefix>*` and `DEL` (the prefix is glob-escaped) and never `FLUSHDB`, so a database can hold several
  stores and other data. But clearing a store with the prefix `app:` also empties a store with the
  prefix `app:sessions:`. Choose prefixes of which none is the beginning of another.
- **`clear()` is neither atomic nor cheap.** Keys written while it runs may survive it, and `SCAN`
  walks the whole database, so its cost grows with everything stored there, not with the store.
- **A value that is not text is a miss.** `get` on a key under the prefix that holds bytes that are not
  UTF-8, or a Redis type other than a string, answers `None` and records `UNEXPECTED_REPLY` or
  `SERVER_ERROR`. `set` overwrites such a key whatever its type.
- **An empty string is a value.** `set(k, "", 0)` followed by `get(k)` is `Some("")`.
- **An empty key or a negative TTL panics** with a contract violation, as the interface's `requires:`
  clauses say; it is not swallowed like a Redis failure.
- **`native` differs from the managed targets in three ways a user meets.** A panic (a violated
  `requires:` clause such as an empty key, or a panicking application callback) cannot be caught
  there: it prints its message and ends the process, where on `dotnet` and `jvm` it unwinds. The text
  of a TLS or connect failure is OpenSSL's (`certificate verify failed`, ...), not `SslStream`'s or
  JSSE's; the error kinds are the same. And `native` is built and run on Linux x86-64 only, with
  OpenSSL 3 (`libssl.so.3`) needed at run time for TLS.

Expiry and eviction:

- **Expiry is Redis's, on the server's clock.** `Cache.ManualClock` and `Cache.inProcessWithClock` have
  no counterpart here: a test cannot advance time, only shorten the unit with `optionsWithTtlUnit`.
- **`ttlSeconds = 0` stores without an expiry and removes an earlier one**, because `SET` does. A later
  `set` with a TTL replaces the expiry.
- **Nothing bounds the number of entries.** `Cache.InProcessCacheStore` evicts first-in-first-out at
  `maxEntries`; this store relies on the server's `maxmemory` and `maxmemory-policy`. With a policy
  that evicts, an entry can disappear before its TTL.

Connection:

- **A store is one connection.** It holds one `Redis.RedisClient`, which serializes commands, so a
  store can be shared between threads but its calls queue behind each other, and behind every other
  user of a client passed to `fromClient`.
- **The database is the client's**, not the store's: two stores on one client differ only by prefix.

## Backend-specific module

`Cache.Redis.Extras` holds what Redis offers beyond `CacheStore`, kept apart so that code written
against the interface stays portable. All three return `Result[_, Redis.RedisError]`.

| Function | |
|---|---|
| `setIfAbsent(store, key, value, ttlSeconds)` | `SET NX`, atomic: `Ok(true)` when this call stored the value. The building block of a cache-stampede lock. `ttlSeconds = 0` stores without an expiry, so such a lock is never released by Redis |
| `lifetimeOf(store, key)` | `PTTL` as a `Lifetime`: `Absent`, `Permanent` or `Expiring(millis)` |
| `entryCount(store)` | How many keys are under the prefix, by `SCAN`; its cost grows with the size of the database |

`Cache.Redis` itself adds `redisKey(options, key)` (the Redis key of a cache key) and the `try...`
functions described under Usage.

## Tests

```sh
# Hermetic (no Redis needed): key naming, option validation, behaviour with Redis unreachable
lyric test --manifest cache-redis/lyric.toml --target dotnet
lyric test --manifest cache-redis/lyric.toml --target jvm
lyric test --manifest cache-redis/lyric.toml --target native

# Integration: the CacheStore conformance suite and backend tests against a real server, and the
# conformance suite a second time over TLS with a client certificate
redis-server --port 16379 --save "" --appendonly no --daemonize yes
export LYRIC_REDIS_URL=redis://127.0.0.1:16379
eval "$(scripts/ci/start-redis-tls.sh)"
lyric test --manifest cache-redis/lyric.toml --target dotnet --features integration
lyric test --manifest cache-redis/lyric.toml --target jvm --features integration
lyric test --manifest cache-redis/lyric.toml --target native --features integration
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

`scripts/ci/start-redis-tls.sh` starts the TLS servers and prints the TLS variables. Every key the
suite writes carries a random prefix and is removed.

The conformance suite advances its clock by 26 seconds in total. The stores it runs against use
`ttlUnitMillis = 50` and the clock hook sleeps 50 ms per second, so each expiry the suite observes is
performed by Redis on a shorter scale; a separate test checks an expiry with the production unit of one
real second. The conformance suite cannot run hermetically: there is no in-process Redis.

## Pinned versions

No third-party library. `Lyric.Redis` is the version in this repository; `Lyric.Cache` and
`Lyric.Conformance` are those of the `lyric-lang` commit in `.lyric-lang-rev`, resolved through the root
`[workspace.overrides]`. Redis servers the suites run against: the `redis:7.4` service container (plain)
and the `ubuntu-latest` runner's `redis-server` package (TLS) in CI, 7.0.15 in development.

## Known gaps

- Everything listed under "Known gaps" of `Lyric.Redis` (one connection, no Cluster or Sentinel, the
  limits of its TLS options).
- No entry-count limit or eviction policy of its own: configure `maxmemory` and `maxmemory-policy` on the
  server.
- No circuit breaker: with Redis down every call waits for the connect timeout.
- The error log keeps a count and the last error only; there is no hook to observe each failure.
- Values are text (`CacheStore` is `String` to `String`); binary values need `Lyric.Redis` directly.
- The lyric-lang packages are `{ workspace = true }` dependencies resolved through the root
  `[workspace.overrides]`; they become registry ranges once the interface is consumed from the registry.
