# Lyric.Redis

A Redis client written in Lyric. The protocol (RESP2 and RESP3 encoder, incremental decoder), the URL
parser and the client are plain Lyric; only the TCP socket is reached through a per-target kernel. No
third-party SDK.

| | |
|---|---|
| Lyric packages | `Redis` (client), `Redis.Resp` (protocol), `Redis.Url` (URL parser), `Redis.Kernel` (socket boundary) |
| Targets | `dotnet`, `jvm` |
| Stability | experimental (0.1.0) |
| Third-party libraries | none |
| Tested against | Redis 7.0.15 |
| Server requirement | Redis 6.0 or later for ACL users (`AUTH user password`) and RESP3; older servers work with RESP2 and a `requirepass` password |

It is the base of `Lyric.Cache.Redis` and `Lyric.Session.Redis` in this repository.

## Use

```lyric
import Redis

match Redis.connect("redis://app:secret@cache.internal:6379/2") {
  case Err(e) -> println(e.message())
  case Ok(client) -> {
    val _ = Redis.setEx(client, "greeting", "hello", 60i64)
    match Redis.get(client, "greeting") {
      case Ok(Some(v)) -> println(v)
      case Ok(None) -> println("expired")
      case Err(e) -> println(e.message())
    }
    client.close()
  }
}
```

Every operation returns `Result[_, RedisError]`; nothing throws. `RedisError` is a union
(`InvalidConfig`, `ConnectFailed`, `AuthFailed`, `ConnectionLost`, `ProtocolViolation`, `ServerError`,
`UnexpectedReply`, `ClientClosed`) with `message()` and a stable `code()`. No message contains the
password.

Commands: `ping`, `get`, `getBytes`, `set`, `setBytes`, `setEx`, `setPx`, `setNx`, `del`, `exists`,
`expire`, `pexpire`, `persist`, `ttl`, `pttl`, `incrBy`, `hget`, `hset`, `hdel`, `hgetAll`, `scan`,
`scanAll` (with `escapeGlob` for literal `MATCH` patterns), `flushDb`, `eval`. Anything else goes through
`Redis.command(client, ["XADD", ...])`, which returns the decoded `Redis.Resp.RespValue`, or
`client.execute(args)` for binary arguments. `client.pipeline(commands)` sends several commands in one
write.

## Configuration

Connection settings are explicit values; the library reads no environment variable. An application
keeps the URL in its own `config { }` block and passes it in.

- `Redis.connect(url)` with `redis://[username[:password]@]host[:port][/database][?db=N&protocol=2|3]`.
  `redis://:secret@host` is a password without a user name; user name and password are percent-decoded;
  an IPv6 host is written `[::1]`.
- `Redis.configFor(host, port)` or `Redis.configFromUrl(url)`, adjusted with `withCredentials`,
  `withDatabase`, `withProtocol` (2 is the default; 3 negotiates with `HELLO 3`), `withTimeouts` (connect
  and I/O, milliseconds, both 5000 by default) and `withStaleRetry`, then `Redis.open(cfg)` (connects and
  authenticates at once) or `Redis.lazy(cfg)` (connects on the first command).

## Behaviour

- **Thread safety.** `RedisClient` is a `protected type` holding one connection. A command, or a
  pipeline, runs under the client's lock from its first byte written to the last byte of its reply, so a
  client can be shared between threads; commands are serialized, not multiplexed, and there is no
  connection pool in 0.1. A sequence of separate calls is not atomic: use `eval`.
- **Reconnect.** A connection that breaks is closed, and the next command dials again and repeats
  `AUTH`/`HELLO` and `SELECT`. A command is repeated once on a fresh connection only when it provably did
  not run: the write failed, or the server had already closed the idle connection (end of stream before
  any reply byte). A timeout or a reset inside a reply is `ConnectionLost` and is never retried, since the
  command may have run. `withStaleRetry(cfg, false)` turns the repeat off. A pipeline is never retried.
- **Timeouts.** A reply that does not arrive within the I/O timeout is `ConnectionLost`, and the
  connection is dropped so that a late reply cannot be mistaken for the next one.
- **RESP3.** Every RESP3 type is decoded; push messages that arrive between replies are skipped;
  attributes are dropped. Map replies (`HGETALL`) are handled by the typed commands under both versions.

## Kernels

`src/_kernel_dotnet/redis_kernel.l` uses the standard library's `Std.TcpHost`
(`System.Net.Sockets.TcpClient`); `src/_kernel_jvm/redis_kernel.l` binds `java.net.Socket` through
`extern type` and auto-FFI (`Std.TcpHost` has no JVM kernel). Both declare the package `Redis.Kernel`
with the same surface (`sockConnect`, `sockRead`, `sockWrite`, `sockClose`, plus `serverListen` /
`serverAccept` and `startBackground` / `waitBackground`, which the test suites use for the in-process
fake server and for threads), and a file-level `@cfg(target = ...)` leaves one of them in a build.

## Tests

```sh
# Hermetic (no Redis needed): RESP, URL parsing, and the client against an in-process fake RESP server
lyric test --manifest redis/lyric.toml --target dotnet
lyric test --manifest redis/lyric.toml --target jvm

# Integration, against a real server
redis-server --port 16379 --save "" --appendonly no --daemonize yes
LYRIC_REDIS_URL=redis://127.0.0.1:16379 lyric test --manifest redis/lyric.toml --target dotnet --features integration
LYRIC_REDIS_URL=redis://127.0.0.1:16379 lyric test --manifest redis/lyric.toml --target jvm --features integration
```

The integration module (`tests/integration/`) is compiled only with `--features integration` and fails
with a message naming `LYRIC_REDIS_URL` when the variable is unset. Use a disposable server: the suite
creates and deletes ACL users and runs `FLUSHDB` on database 15. All other keys carry a random prefix
and are deleted again.

## Known gaps

- **No TLS.** `rediss://` is refused with `InvalidConfig`.
- **Connect timeout on dotnet.** `connectTimeoutMs` is applied on `jvm`. On `dotnet` the connection is
  dialled by `Std.TcpHost.hostConnect`, which has no timeout parameter, so the operating system's connect
  timeout applies. The I/O timeout is applied on both targets (on `jvm` to reads only: `SO_TIMEOUT` does
  not bound a blocked write).
- **One connection per client**: no pool, no multiplexing, no pub/sub API, no `MULTI`/`EXEC` helper (use
  `eval` or a pipeline), no Cluster or Sentinel support.
- `EVAL` sends the script text each time; there is no `EVALSHA` cache.
