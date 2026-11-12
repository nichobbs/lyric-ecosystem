# Lyric.Cache.Redis

`Cache.CacheStore` (from `Lyric.Cache` in lyric-lang) on a Redis server, built on the pure-Lyric
`Lyric.Redis` client of this repository.

| | |
|---|---|
| Lyric packages | `Cache.Redis` (the store), `Cache.Redis.Extras` (Redis-only operations) |
| Targets | `dotnet`, `jvm` |
| Stability | experimental (0.1.0) |
| Interface | `Lyric.Cache` 0.1.0 (`Cache.CacheStore`); conformance suite `Testing.Conformance.Cache` from `Lyric.Testing` 0.1.0 |
| Third-party libraries | none (`Lyric.Redis`, path dependency `../redis`) |
| Tested against | Redis 7.0.15 |

## Use

```lyric
import Cache.Redis as RedisCache

match RedisCache.connect("redis://cache.internal:6379/1", RedisCache.options("myapp:cache:")) {
  case Err(e) -> println(e.message())
  case Ok(store) -> {
    RedisCache.setWithTtl(store, "users:42", userJson, 300)
    val hit = RedisCache.get(store, "users:42")
  }
}
```

`Cache.Redis` re-exports `Cache`, so `CacheStore` and the `Cache` wrappers (`get`, `set`, `setWithTtl`, `delete`, `clear`) are reachable through it, as above.
`RedisCache.fromClient(client, options)` builds a store on an existing `Redis.RedisClient`.

## Configuration

Explicit values only: the URL (see `Lyric.Redis` for its form) and `RedisCacheOptions`:

- `keyPrefix` (required, not empty): prepended to every cache key. Give each store its own.
- `ttlUnitMillis` (default 1000): the length of one `ttlSeconds` unit. Leave it at 1000 in production;
  `optionsWithTtlUnit` exists so a test suite can make TTLs elapse quickly while Redis still does the
  expiring.

## Behaviour

- An entry is one Redis string at `<keyPrefix><key>`, written with `SET` (`SET ... PX` for a TTL), so
  expiry is done by Redis. `ttlSeconds = 0` stores without an expiry and removes an earlier one.
- `clear()` deletes exactly the keys under the store's prefix with `SCAN MATCH` + `DEL` (the prefix is
  glob-escaped). It never runs `FLUSHDB`, so a database can hold several stores and other data. Keys
  written while a clear runs may survive it.
- **Failures.** `CacheStore` has no error channel, so the interface methods degrade as a cache should:
  a failed `get` is a miss, a failed write is dropped, and the failure is counted and kept in
  `store.errors` (`errorCount()`, `lastError()`). Code that must see the failure uses `tryGet`, `trySet`,
  `tryDelete` and `tryClear`, which return `Result[_, Redis.RedisError]` and which the interface methods
  are built on. No Redis failure panics.
- **Thread safety.** A store holds one `Redis.RedisClient`, which serializes commands on one connection;
  a store can be shared between threads.
- `Cache.Redis.Extras`: `setIfAbsent` (`SET NX`, atomic), `lifetimeOf` (`PTTL`), `entryCount`.

## Tests

```sh
# Hermetic (no Redis needed): key naming, option validation, behaviour with Redis unreachable
lyric test --manifest cache-redis/lyric.toml --target dotnet
lyric test --manifest cache-redis/lyric.toml --target jvm

# Integration: the CacheStore conformance suite and backend tests against a real server
redis-server --port 16379 --save "" --appendonly no --daemonize yes
LYRIC_REDIS_URL=redis://127.0.0.1:16379 lyric test --manifest cache-redis/lyric.toml --target dotnet --features integration
LYRIC_REDIS_URL=redis://127.0.0.1:16379 lyric test --manifest cache-redis/lyric.toml --target jvm --features integration
```

The integration module is compiled only with `--features integration` and fails with a message naming
`LYRIC_REDIS_URL` when the variable is unset. Every key it writes carries a random prefix and is removed.

The conformance suite advances its clock by 26 seconds in total. The stores it runs against use
`ttlUnitMillis = 50` and the clock hook sleeps 50 ms per second, so each expiry the suite observes is
performed by Redis on a shorter scale; a separate test checks an expiry with the production unit of one
real second. The conformance suite cannot run hermetically: there is no in-process Redis.

## Known gaps

- Everything listed under "Known gaps" of `Lyric.Redis` (no TLS, one connection, connect timeout on
  dotnet).
- No entry-count limit or eviction policy of its own: configure `maxmemory` and `maxmemory-policy` on the
  server.
- Values are text (`CacheStore` is `String` to `String`); binary values need `Lyric.Redis` directly.
- The lyric-lang packages are `{ workspace = true }` dependencies resolved through the root
  `[workspace.overrides]`; they become registry ranges once the interface is consumed from the registry.
