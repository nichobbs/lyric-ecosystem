# Lyric.Cache.Memcached

`Cache.CacheStore` (from `Lyric.Cache` in lyric-lang) on a memcached server, through a client for
memcached's meta text protocol written in Lyric. It runs on `dotnet` and `jvm` and is `experimental`
(0.1.0).

| | |
|---|---|
| Lyric packages | `Cache.Memcached` (the store), `Cache.Memcached.Extras` (memcached-only operations), `Cache.Memcached.Client` (the connection), `Cache.Memcached.Meta` (the protocol codec), `Cache.Memcached.Keys` (key and TTL mapping), `Cache.Memcached.Url` |
| Targets | `dotnet`, `jvm` |
| Stability | experimental (0.1.0) |
| Interface | `Lyric.Cache` (`Cache.CacheStore`), re-exported with `pub use Cache`; conformance suite `Testing.Conformance.Cache` from `Lyric.Conformance` |
| Server | memcached 1.6.10 or later (the first release with the stable meta protocol and base64 keys); tested against 1.6.24 |
| Third-party libraries | none (`[nuget]` and `[maven]` are empty); `Lyric.Net` (`net/` in this repository) is a `{ workspace = true }` dependency for the TCP and TLS connection. The package has no extern declarations |

## Usage

Inside this workspace an application declares `"Lyric.Cache.Memcached" = { workspace = true }` under
`[dependencies]`.

```lyric
import Std.Core
import Cache.Memcached as MemcachedCache
import Cache.Memcached.Client as Client

func example(url: in String, userJson: in String): Result[Option[String], Client.MemcachedError] {
  val store = MemcachedCache.connect(url, MemcachedCache.options("myapp:cache"))?
  MemcachedCache.setWithTtl(store, "users:42", userJson, 300)
  val hit = MemcachedCache.get(store, "users:42")
  MemcachedCache.delete(store, "users:42")
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

`example("memcached://cache.internal:11211", json)` connects, authenticates if the URL carries
credentials, and checks the server version before the store is returned; `connect` is the only step
with an error (`Client.MemcachedError`). `Cache.Memcached` re-exports `Cache`, so `CacheStore` and the
`Cache` wrappers (`get`, `set`, `setWithTtl`, `delete`, `clear`) are reachable through it, as above.
The wrappers and the interface methods return `Option[String]` or `Unit`: they have no error to
propagate.

Variations:

- **Through the interface.** A `MemcachedCacheStore` is a `CacheStore`:

  ```lyric
  func throughTheInterface(cache: in MemcachedCache.CacheStore): Option[String] {
    cache.set("k", "v", 0)
    cache.get("k")
  }
  ```

- **With the failure visible.** `tryGet`, `trySet`, `tryDelete` and `tryClear` return
  `Result[_, Client.MemcachedError]`; the interface methods are built on them:

  ```lyric
  func visibleFailures(store: in MemcachedCache.MemcachedCacheStore): Result[Unit, Client.MemcachedError] {
    MemcachedCache.trySet(store, "k", "v", 60)?
    val value = MemcachedCache.tryGet(store, "k")?
    val existed = MemcachedCache.tryDelete(store, "k")?
    println(existed.toString() + (match value { case Some(v) -> v; case None -> "" }))
    MemcachedCache.tryClear(store)
  }
  ```

- **On an existing client.** `MemcachedCache.fromClient(client, MemcachedCache.options("myapp:cache"))`
  builds a store on a `Client.MemcachedClient` that other stores also use. The client is shared, not
  owned.
- **Over TLS.** `connect` takes a `memcached+tls://` URL: TLS with the system trust store and the
  URL's host as the server name. Everything a URL cannot carry goes through `connectWith`, which takes
  a `Client.MemcachedConfig`; its TLS options are a `Net.Tls.TlsOptions`:

  ```lyric
  func overTls(url: in String, caPem: in String): Result[MemcachedCache.MemcachedCacheStore, Client.MemcachedError] {
    val base = Client.configFromUrl(url)?
    val cfg = Client.withTlsOptions(base, base.tls.withExclusiveCaCertificate(caPem))
    MemcachedCache.connectWith(cfg, MemcachedCache.options("myapp:cache"))
  }
  ```

  `base.tls` already has TLS switched on by the URL scheme. The options are `Lyric.Net`'s
  (`withCaCertificate`, `withExclusiveCaCertificate`, `withClientIdentity`, `withTlsServerName`,
  `withMinTlsVersion`, `withInsecureSkipVerify` and its `LYRIC_TLS_ALLOW_INSECURE` policy; see
  `net/README.md`), and `Net.Tls.tlsOptionsFromEnv` reads them from environment variables. An unusable
  TLS configuration is `InvalidConfig` and a failed handshake `TlsFailed`, both returned by `connect` /
  `connectWith`.
- **Compare-and-swap and a multi-get** (`Cache.Memcached.Extras`, see below):

  ```lyric
  import Std.Parse
  import Cache.Memcached.Extras as Extras

  // Add one to a counter kept as text; false when another writer got in first
  // and the caller should try again.
  func increment(store: in MemcachedCache.MemcachedCacheStore, key: in String): Result[Bool, Client.MemcachedError] {
    val current = Extras.getVersioned(store, key)?
    match current {
      case None -> Extras.addIfAbsent(store, key, "1", 0)
      case Some(entry) -> {
        val next = match parseOptInt(entry.value) { case Some(n) -> n + 1; case None -> 1 }
        val outcome = Extras.compareAndSwap(store, key, next.toString(), 0, entry.version)?
        match outcome {
          case Extras.Swapped -> Ok(true)
          case _ -> Ok(false)
        }
      }
    }
  }

  func warm(store: in MemcachedCache.MemcachedCacheStore, ids: in slice[String]): Result[Int, Client.MemcachedError] {
    val found = Extras.getMany(store, ids)?
    Ok(found.count)
  }
  ```

Every example above is compiled and run, as written here, by `tests/fake_server_tests.l` ("the README
examples compile and do what the README says").

## Configuration

Explicit values only: the package reads no environment variable of its own. `LYRIC_TLS_ALLOW_INSECURE`
is read by `Lyric.Net`, and only for a configuration with `withInsecureSkipVerify`.

**The URL** names one server:

```
memcached://[user:password@]host[:port]
memcached+tls://[user:password@]host[:port]
```

The port defaults to 11211. `user:password` are the credentials of memcached's ASCII authentication
(a server started with `-Y <file>`), percent-decoded. An IPv6 host is written in brackets. The scheme
is matched without regard to case. A path, a query, a fragment or a comma-separated server list is
refused. memcached has no registered URL scheme: `memcached` is what most clients accept, and `+tls`
follows the `<protocol>+<transport>` form this repository already uses (`stomp+ssl`) for "the same
protocol inside TLS", which is what a memcached started with `-Z` speaks.

**`Client.MemcachedConfig`** (`configFor(host, port)`, `configFromUrl(url)`):

| Field | Changed with | Default | Meaning |
|---|---|---|---|
| `host`, `port` | `configFor`, the URL | (required), 11211 in a URL | The server |
| `username`, `password` | `withCredentials`, the URL | none | ASCII authentication. No space or control character in either, no `:` in the user name (memcached's file format); checked by `checkConfig` |
| `connectTimeoutMs` | `withTimeouts` | 5000 | Longest wait for the TCP connection; 0 = no limit |
| `ioTimeoutMs` | `withTimeouts` | 5000 | Longest wait for one read or write, and for the TLS handshake; 0 = no limit |
| `retryStaleConnection` | `withStaleRetry` | true | Repeat a request once when the server had closed the idle connection |
| `maxValueBytes` | `withMaxValueBytes` | 1048576 | The largest value sent at all. Raise it for a server started with a larger `-I` |
| `tls` | `withTlsOptions`, the `+tls` scheme | plain TCP | A `Net.Tls.TlsOptions` |

**`MemcachedCacheOptions`** (`options(namespace)`, or `optionsFor(namespace)` which returns
`Err(InvalidConfig)` instead of panicking, for a namespace that comes from configuration):

| Field | Default | Meaning |
|---|---|---|
| `namespace` | (required) | The name every key of the store is filed under; `clear()` empties it and nothing else. Not empty, at most 80 bytes of UTF-8, no space, control character or DEL |

## Surprises

Failures:

- **The interface methods never report a failure.** `CacheStore` has no error channel, so a failed
  `get` is a miss and a failed `set`, `delete` or `clear` is dropped. Nothing panics. The failure is
  counted and kept in `store.errors` (`errorCount()`, `lastError()`): the count only grows and only the
  most recent error is kept. The `try...` functions do not touch that log.
- **An unreachable memcached makes every call slow, not just the first.** Each call dials again and
  waits up to the connect timeout (5 s by default) before it answers a miss; there is no circuit
  breaker. Lower both timeouts with `Client.withTimeouts` and `connectWith` for a cache that must fail
  fast.
- **`connect` and `connectWith` do report errors**, and connect eagerly: an unreachable server, a
  wrong password or a server older than 1.6.10 (`UnsupportedServer`) is an `Err`, not a store that
  misses. A store built with `fromClient` on a `Client.lazy` client starts in the degraded mode
  instead.
- **A store has no `close`.** Close the connection with `store.client.close()`. After that every `get`
  is a recorded miss (`CLIENT_CLOSED`).
- **A request memcached refuses as malformed costs the connection.** After a `CLIENT_ERROR` or `ERROR`
  memcached may read the rest of the request as commands of its own, so the client closes the
  connection (`RequestRefused`) and the next request dials again. The store validates what it sends,
  so in practice this is seen only when another program put text where a generation number belongs.

Keys:

- **A namespace is not a key prefix.** Every entry is stored under
  `<namespace> <generation> <key>` (single spaces), and a namespace has no space, so `app` and
  `app:sessions` are two unrelated stores: clearing one never touches the other. Stores with the same
  namespace, in any process, share their entries.
- **Any text is a key.** memcached itself takes at most 250 bytes without spaces or control
  characters. Every key is sent base64-encoded with the meta protocol's `b` flag and stored as the
  decoded bytes, so spaces, line breaks and non-ASCII text are safe; the 250-byte limit then applies
  to the base64 text, which leaves 186 bytes for the stored key. A stored key that would be longer is
  replaced by `<namespace> <generation>#<SHA-256 of the key, 64 hexadecimal digits>`. Both were
  needed: base64 alone still has a length limit, and hashing every key would make the server's
  contents unreadable for no gain. What a caller sees of the hashing is nothing, with two footnotes:
  two long keys would share an entry only if their SHA-256 collided, and a long key cannot be read
  back from the server (`Keys.isHashed` tells which form a key takes). The character after the
  generation (` ` or `#`) keeps a short key from ever equalling a hashed one.
- **Other tools see keys with spaces in them.** `<namespace> <generation> <key>` cannot be typed into
  the classic `get`; use `mg <base64> b v`. `Keys.dataKey` and `Keys.wireKey` compute both forms.

Values:

- **Values are UTF-8 text.** An item under one of the store's keys that holds other bytes (written by
  another client) is a miss through the interface and `UNEXPECTED_REPLY` through `tryGet`. Items are
  written with client flags 0.
- **The item limit is reported as `ValueTooLarge`, by whichever side notices.** memcached's limit
  (1 MiB unless started with `-I`) covers the stored key and an item header as well as the value: on
  1.6.24 the largest value is 1048576 - 59 - (bytes of the stored key). A value over `maxValueBytes`
  is refused by the client without being sent; anything between is refused by the server
  (`SERVER_ERROR object too large for cache`), and both are `ValueTooLarge`.
- **A value that is refused as too large empties the key.** memcached removes the entry the key held
  when it refuses a value, in every mode, and the client does the same when it refuses one itself, so
  a failed write never leaves the previous value to be read as current. This applies to `addIfAbsent`
  and `compareAndSwap` too, where it is less expected: an oversized add removes an existing entry.
- **An empty string is a value.** `set(k, "", 0)` followed by `get(k)` is `Some("")`.
- **An empty key or a negative TTL panics** with a contract violation, as the interface's `requires:`
  clauses say; it is not swallowed like a memcached failure.

Expiry and eviction:

- **`ttlSeconds = 0` stores without an expiry and removes an earlier one** (`T0`); a later `set` with a
  TTL replaces the expiry. memcached's "negative means already expired" cannot be reached: the
  interface requires `ttlSeconds >= 0`.
- **Up to 30 days a TTL is counted on the server's clock, in whole seconds.** memcached's clock ticks
  once a second, so an entry with a TTL of `n` lives between `n - 1` and `n` seconds.
- **Above 30 days the TTL becomes an absolute timestamp computed on the client's clock.** memcached
  reads an expiry above 2592000 as a Unix timestamp, so `ttlSeconds = 3456000` sent as it is would be
  a day in 1970 and the entry would expire at once. The store sends `now + ttlSeconds` instead
  (`Keys.expiryToken`). The entry then lives as long as asked, give or take the difference between
  the client's and the server's clocks; TTLs of 30 days or less are not affected by clock skew.
- **No entry outlives 2038-01-19T03:14:07Z.** memcached 1.6 keeps the expiry in 32 bits and a later
  timestamp wraps around (into the past, or to "never"). A longer TTL is lowered to that instant; once
  that is less than 30 days away, the relative 30 days are sent instead.
- **Expiry cannot be driven by a test.** `Cache.ManualClock` and `Cache.inProcessWithClock` have no
  counterpart here, and memcached has no sub-second TTL, so a suite that exercises expiry against a
  real server waits in real seconds: the conformance suite takes 26.
- **memcached evicts.** Nothing bounds the number of entries; under memory pressure (`-m`) the least
  recently used items go, before their TTL. A server started with `-M` returns
  `SERVER_ERROR out of memory storing object` instead, which is `ServerFailure`.

`clear()` and generations:

- **`clear()` never runs `flush_all`**, which would empty the server for every application. memcached
  cannot list its keys either, so a namespace keeps a generation counter in the item
  `<namespace> generation`, the generation is part of every stored key, and `clear()` adds one to it
  (`ma`, atomic on the server). The entries of the old generation are never asked for again.
- **Cleared entries are orphaned, not freed.** They stay in the server until their TTL ends or the LRU
  needs the memory. A namespace of entries without TTL that is cleared often relies on eviction to
  get that memory back.
- **Every operation checks the generation, in the same round trip.** A get, set or delete sends the
  read of the generation and the command for the key in one write, with the key composed from the
  generation the store saw last; the answer counts only if the generation read back is that one,
  otherwise the command is repeated with the new generation. The cost is one more command line and
  reply per request (a few dozen bytes plus the namespace), no extra round trip, and it means a `clear()`
  in any process is seen by the very next operation of every other store on the namespace. The
  alternative, caching the generation for a while, would save those bytes and let stores serve
  entries that were already cleared; that is the wrong trade for a cache that is cleared on purpose.
  The first operation of a store pays one extra round trip to learn the generation, and the first
  write to an empty namespace two (it starts the generation).
- **If the generation item is lost, the namespace is empty.** After an eviction of that item or a
  server restart, reads miss and deletes and clears do nothing; the next write starts a new
  generation. It is the time in milliseconds followed by three random digits, so it does not repeat a
  generation that old entries were stored under: an evicted counter costs a cleared cache, never
  stale entries coming back. The item has no TTL and is read by every operation, which keeps it at
  the hot end of the LRU; it is only at risk in a namespace that is not used.
- **A write that races a `clear()` may be lost, and that is all.** A set whose request was built
  before another process cleared lands either in the old generation (and is repeated in the new one,
  since the generation read back differs) or just before the clear (and is cleared). If the namespace
  is cleared six times in a row while one operation runs, the operation gives up with
  `NamespaceContended`.
- **The generation item is trusted only as a number.** If another program stores something else under
  `<namespace> generation`, every operation fails (`UNEXPECTED_REPLY`; a miss and a counted error
  through the interface) until it is deleted.

Connection and server:

- **A store is one connection to one server.** It holds one `Client.MemcachedClient`, which
  serializes requests, so a store can be shared between threads but its calls queue behind each
  other, and behind every other store on a client passed to `fromClient`. There is no server list and
  no consistent hashing (see Known gaps).
- **A request is repeated once, and only when it did not run**: the write failed, or the server had
  closed the idle connection before sending any reply byte. A timeout or a reset in the middle of the
  replies is `ConnectionLost` and not retried.
- **The server version is checked on every new connection** (`version`), which costs one round trip
  per connection. A version text that does not start with `major.minor.patch` numbers (a proxy)
  cannot be judged and is accepted.
- **Authentication is memcached's ASCII authentication** (`-Y <file>`), not SASL. The credentials
  travel in clear unless the URL is `memcached+tls://`. Sent to a server that has no authentication
  file, the token would be stored as an item named `auth`; it is sent with an expiry of -1, so that
  item is expired the moment it is stored and cannot be read by anyone.
- **`Client.flushAll` empties the whole server**: every namespace and every other application's
  entries. It exists for test suites on a disposable server and is not used by the store.

Between targets: none known. The stored keys are identical (the SHA-256 form is lower-case on both),
so `dotnet` and `jvm` processes share a namespace. The text of a `TlsFailed` or `ConnectFailed` error
comes from the platform (`SslStream` or JSSE) and differs.

## Backend-specific module

`Cache.Memcached.Extras` holds what memcached offers beyond `CacheStore`, kept apart so that code
written against the interface stays portable. All of them act on the store's namespace in its current
generation and return `Result[_, Client.MemcachedError]`.

| Function | |
|---|---|
| `addIfAbsent(store, key, value, ttlSeconds)` | `ms` in add mode, atomic: `Ok(true)` when this call stored the value. The building block of a cache-stampede lock. `ttlSeconds = 0` stores without an expiry, so such a lock is never released by memcached |
| `getVersioned(store, key)` | The value with memcached's CAS number as `version` |
| `compareAndSwap(store, key, value, ttlSeconds, version)` | Stores only over the version that was read: `Swapped`, `Changed` (someone wrote in between) or `Gone` (deleted, expired, evicted or cleared) |
| `getMany(store, keys)` | The hits among `keys` as a `Map`, 64 keys per round trip, sent as quiet `mg` commands so that only hits are answered. Not a snapshot across batches |
| `touch(store, key, ttlSeconds)` | A new TTL without rewriting the value; `0` removes the expiry. `Ok(false)` when the key is absent |
| `lifetimeOf(store, key)` | `Absent`, `Permanent` or `Expiring(seconds)`, by the server's clock |

`Cache.Memcached` itself adds `ping(store)` (one `mn` round trip, a liveness check) and the `try...`
functions; `Cache.Memcached.Client` adds `ping`, `serverVersion` and `flushAll` on a client, and
`client.run` / `client.runUntilNoOp` for meta commands built with `Cache.Memcached.Meta`.

## Tests

```sh
# Hermetic (no memcached needed): the protocol codec, key and TTL mapping, URL parsing, and the
# client, the store, the extras and the CacheStore conformance suite against an in-process fake
# memcached with a clock the tests move
lyric test --manifest cache-memcached/lyric.toml --target dotnet
lyric test --manifest cache-memcached/lyric.toml --target jvm

# Integration: the conformance suite (plain and over TLS) and backend tests against real servers
eval "$(scripts/ci/start-memcached.sh)"
lyric test --manifest cache-memcached/lyric.toml --target dotnet --features integration
lyric test --manifest cache-memcached/lyric.toml --target jvm --features integration
```

Run from the repository root. The integration module is compiled only with `--features integration`
and fails with a message naming the variable when one is unset:

| Variable | Meaning |
|---|---|
| `LYRIC_MEMCACHED_URL` | `memcached://` URL of a plain server |
| `LYRIC_MEMCACHED_TLS_URL` | `memcached+tls://` URL of a server started with `-Z` |
| `LYRIC_MEMCACHED_TLS_CA` | PEM file: the CA of that server's certificate |
| `LYRIC_MEMCACHED_AUTH_URL` | `memcached://user:password@...` URL of a server started with `-Y`. **The suite runs `flush_all` on this server**: it must be disposable |

`scripts/ci/start-memcached.sh` installs memcached if needed (Ubuntu's package is built with TLS),
starts the three servers on 127.0.0.1:19960-19962 with a fresh CA, certificate and password, and
prints the variables. Every namespace the suite uses has a random name; its entries are orphaned by a
final `clear()`.

The integration run takes about 80 seconds per target: memcached's clock cannot be moved, so the
conformance suite's clock is a real sleep of one second per second it advances (26, and the suite
runs twice, plain and over TLS). The hermetic conformance run uses the fake server's clock instead:
there, too, expiry is decided by the server, never by the client.

## Known gaps

- **One server.** No server list, no consistent hashing (ketama or otherwise), no failover. A URL
  with a comma is refused rather than half-supported. An application that shards does it above the
  store, with one store per server.
- **No SASL.** memcached's SASL (`-S`) belongs to the binary protocol, which this client does not
  speak. The ASCII authentication file (`-Y`), which memcached marks experimental, is supported.
- **No connection pool.** One connection per client; requests are serialized.
- **Text only.** `CacheStore` stores `String`; there is no binary value API and client flags are not
  used.
- **Long TTLs use the client's clock** (see Surprises); the server's time is not read.
- **Compiler (dotnet): a generic function whose type argument is inferred from the result of an
  imported protected type's entry is specialised for `Int`.** `ok[T](r: Result[T, E])` called as
  `ok(client.run(...))`, where `run` returns `Result[List[Meta.MetaReply], MemcachedError]`, is
  compiled as `ok__Int` and fails at run time with "match not exhaustive"; the JVM build is correct.
  It affects no library code: the two test modules that hit it use non-generic helpers (`okReplies`)
  for that one result type (nichobbs/lyric-lang#9077).
