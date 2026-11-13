# lyric-mq-rabbitmq

RabbitMQ backend for the `Mq.MessageQueue` and `Mq.QueueConsumer` interfaces of `lyric-mq`. The
client is an AMQP 0-9-1 implementation written in Lyric; only the TCP socket and the TLS layer
over it come from the host platform, so there is no RabbitMQ client SDK dependency.

| | |
|---|---|
| Package | `Lyric.Mq.RabbitMq` (Lyric packages `Mq.RabbitMq`, `Mq.RabbitMq.Extras`, `Mq.RabbitMq.Amqp`, `Mq.RabbitMq.Url`) |
| Targets | `dotnet`, `jvm` |
| Stability | `experimental` |
| Broker | RabbitMQ over AMQP 0-9-1. Tested against RabbitMQ 3.12.1 (Erlang/OTP 25, the Ubuntu 24.04 package; every suite, both targets, on a development machine) and 4.0.5 (the Ubuntu 26.04 package, in CI). See "RabbitMQ 3.x and 4.x". |
| Transport | plain TCP, or TLS 1.2 / 1.3 (`SslStream` on dotnet, `SSLSocket` on jvm) |
| SDK dependencies | none (`[nuget]` and `[maven]` are empty) |
| Interface | `Lyric.Mq` 0.1.0 |

## Use

```lyric
import Mq
import Mq.RabbitMq

val queue = Mq.RabbitMq.connectUrl("amqp://app:secret@broker.internal:5672/prod", "orders")?

Mq.publish(queue, Mq.Message(
  id = "order-42",
  body = "{\"total\":12}",
  headers = [Mq.MessageHeader(key = "content-type", value = "application/json")],
  deliveryCount = 0))?

Mq.consume(queue, 5000, { m -> process(m) })?
Mq.ack(queue, "order-42")?
queue.close()
```

`Mq.RabbitMq` re-exports `Mq` (`pub use Mq`), so with `import Mq.RabbitMq` alone the interface
names are visible unqualified (`Message`, `MessageQueue`, `publish`). To write them qualified
(`Mq.Message`, `Mq.publish`), as above, import `Mq` too and declare `Lyric.Mq` next to
`Lyric.Mq.RabbitMq` in the application's `[dependencies]`. This example and the `Extras` one below
are compiled by `tests/reexport_tests.l`.

One `RabbitMqQueue` is one connection, one channel and one queue; it implements both interfaces.

## Configuration

Three ways to connect; none of them reads a credential from source.

- `connectWith(options)`: a `ConnectOptions` record. Start from
  `defaultOptions(host, port, queue)` and adjust with `.copy(...)`. `validateOptions(options)`
  runs the same checks without connecting.
- `connectUrl(url, queueName)`: a URL in the RabbitMQ URI scheme,
  `amqp://[user[:password]@]host[:port][/vhost][?parameters]` (port 5672) or `amqps://...` (TLS,
  port 5671). User, password and virtual host are percent-decoded. Without user information the
  user is `guest` / `guest`. Parameters: `heartbeat` (seconds), `connection_timeout`
  (milliseconds), `frame_max` (bytes); any other parameter is an error.
  `optionsFromUrl(url, queueName)` returns the `ConnectOptions` instead of connecting, so TLS
  settings and queue arguments can be added before `connectWith`.
- `connectFromEnv()` (`optionsFromEnv()` for the options only): reads the variables below. An
  empty variable counts as unset.

  | Variable | Meaning |
  |---|---|
  | `LYRIC_CONFIG_MQ_RABBITMQ_URL` | broker URL (required) |
  | `LYRIC_CONFIG_MQ_RABBITMQ_QUEUENAME` | queue name (required) |
  | `LYRIC_CONFIG_MQ_RABBITMQ_ACKMODE` | `Manual` (default) or `Auto` |
  | `LYRIC_CONFIG_MQ_RABBITMQ_PREFETCHCOUNT` | prefetch window (default 10) |
  | `LYRIC_CONFIG_MQ_RABBITMQ_TLSCAFILE` | PEM file: a CA certificate trusted in addition to the platform trust store |
  | `LYRIC_CONFIG_MQ_RABBITMQ_TLSCAEXCLUSIVE` | `true`: trust only that CA (default `false`) |
  | `LYRIC_CONFIG_MQ_RABBITMQ_TLSCERTFILE` | PEM file: client certificate (mutual TLS) |
  | `LYRIC_CONFIG_MQ_RABBITMQ_TLSKEYFILE` | PEM file: its PKCS8 private key |
  | `LYRIC_CONFIG_MQ_RABBITMQ_TLSSERVERNAME` | name to check the certificate against, when it is not the URL's host |
  | `LYRIC_CONFIG_MQ_RABBITMQ_TLSMINVERSION` | `1.2` (default) or `1.3` |

  The `TLS...` variables apply to an `amqps://` URL; with an `amqp://` URL they are an error.
  `LYRIC_TLS_ALLOW_INSECURE` is the standard library's second key for skipping verification, see
  "TLS".

`ConnectOptions` fields and their defaults:

| Field | Default | Meaning |
|---|---|---|
| `host`, `port` | (required) | where the broker listens |
| `user`, `password` | `guest`, `guest` | PLAIN credentials |
| `virtualHost` | `/` | the virtual host |
| `queue` | (required) | the queue published to and consumed from |
| `declare` | `true` | declare the queue on connect; `false` only checks that it exists (`404` otherwise) and ignores the next three fields |
| `durable` | `true` | the queue survives a broker restart |
| `autoDelete` | `false` | the broker deletes the queue when its last consumer leaves |
| `queueArguments` | none | `x-queue-type`, `x-dead-letter-exchange`, ...; built by `Mq.RabbitMq.Extras` |
| `ackMode` | `Manual` | `Auto` acknowledges when the handler returns `Ok` and requeues when it returns `Err` |
| `persistent` | `true` | delivery mode 2 for published messages |
| `prefetchCount` | 10 | `basic.qos`: the most deliveries left unsettled on this connection; 0 = no limit |
| `heartbeatSeconds` | 0 (off) | heartbeat interval to ask for, see "Heartbeats" |
| `frameMax` | 131072 | largest frame, at least 4096; the smaller of this and the broker's limit is used |
| `connectTimeoutMs` | 10000 | TCP connect limit, and again the TLS handshake limit; 0 = the system's |
| `requestTimeoutMs` | 10000 | how long to wait for a reply or a publisher confirm |
| `maxMessageBytes` | 16 MiB | largest message body accepted from the broker |
| `connectionName` | empty | shown in the broker's management UI |
| `tls` | `plainTcp()` | TLS settings, see below |

## TLS

An `amqps://` URL, or `withTls()` on the options, runs a TLS handshake before the AMQP protocol
header. By default the broker's certificate must chain to the platform trust store and must carry
the host name (or IP address) the client connects to. The option names and the rule for skipping
verification are those of `Std.Http.HttpClientBuilder` (docs/61 in lyric-lang) and of
`Lyric.Redis`.

```lyric
val options = Mq.RabbitMq.optionsFromUrl("amqps://app:secret@broker.internal:5671/prod", "orders")?
  .withCaCertificate(caPem)
  .withClientIdentity(certificatePem, keyPem)
val queue = Mq.RabbitMq.connectWith(options)?
```

| Function on `ConnectOptions` | Effect |
|---|---|
| `withTls()` | TLS with the defaults. Implied by an `amqps://` URL. |
| `withCaCertificate(pem)` | trust this CA certificate in addition to the platform trust store |
| `withExclusiveCaCertificate(pem)` | trust only this CA certificate; wins over `withCaCertificate` |
| `withClientIdentity(certificatePem, keyPem)` | present a client certificate, for a listener with `fail_if_no_peer_cert = true` |
| `withTlsServerName(name)` | send `name` as SNI and check the certificate against it instead of `host` |
| `withMinTlsVersion(Tls13)` | TLS 1.3 only; the default `Tls12` accepts 1.2 and 1.3 |
| `withInsecureSkipVerify()` | ask for certificate and name checks to be skipped, see below |

- The arguments are PEM text. The PEM is checked with `Std.Tls` before any connection is made, so
  the rules are `Std.Tls`'s: one certificate per PEM, and an unencrypted PKCS8 key
  (`-----BEGIN PRIVATE KEY-----`; convert with `openssl pkcs8 -topk8 -nocrypt`). Read a file with
  `Std.File.readText`.
- Setting a TLS option on a connection that is not TLS is an error, not a silent downgrade.
- `withInsecureSkipVerify()` does nothing on its own. Verification is skipped only when the
  environment also has `LYRIC_TLS_ALLOW_INSECURE=1` at the time the connection is opened;
  otherwise verification stays on and a warning is written to standard error. The variable
  without the option does nothing. `connectFromEnv` has no variable for it, and the URL has no
  parameter for it.
- A handshake failure is `Err("rabbitmq: TLS handshake with HOST:PORT failed: ...")`. A broker
  that requires a client certificate and gets none may, with TLS 1.3, only refuse after the
  client's handshake has finished; the connect then fails with
  `rabbitmq: cannot open a connection to HOST:PORT: ...`.
- Authentication is still PLAIN inside TLS. The client certificate authenticates the transport
  only; SASL EXTERNAL (`rabbitmq_auth_mechanism_ssl`) is not implemented.
- Revocation (CRL, OCSP) is not checked, as in `Std.Http`.

## How the interface maps onto RabbitMQ

| Interface | On the wire | Broker behaviour relied on |
|---|---|---|
| connect | protocol header, `connection.start-ok` (PLAIN), `tune-ok`, `connection.open`, `channel.open`, `confirm.select`, `queue.declare` | |
| `publish` | `basic.publish` to the default exchange, routing key = queue name, `mandatory`; returns on the broker's `basic.ack` | publisher confirms: the ack is sent once the message is enqueued (and written to disk when persistent in a durable queue) |
| `publishBatch` | all the publishes, then every confirm is awaited | confirms are per message; see "Surprises" |
| `consume` | first call sends `basic.qos` and `basic.consume` (manual ack); then waits for a `basic.deliver` | deliveries arrive in queue order, at most `prefetchCount` unsettled |
| `ack(id)` | `basic.ack` for that delivery | |
| `nack(id, true)` | `basic.nack` with `requeue` | the message returns to its original position when possible |
| `nack(id, false)` | `basic.nack` without `requeue` | dead-lettered when the queue has a dead-letter exchange, discarded otherwise |
| `close` | `channel.close`, `connection.close`, each awaited, then the socket | unsettled deliveries return to the queue flagged `redelivered` |

Message mapping:

- `Message.id` is the `message-id` property (at most 255 bytes). Ids are not required to be
  unique; `ack`/`nack` settle the oldest unsettled delivery with that id. Settling an id with no
  unsettled delivery is an `Err`. A message from another producer without a `message-id` is
  delivered with the id `delivery-N`, N being its delivery tag on this channel.
- `Message.headers` is an ordered list that may repeat keys; an AMQP `headers` table is not
  reliably either (RabbitMQ 4 keeps one entry per key and does not keep their order). So the
  headers travel twice in the `headers` table:
  - exactly, as one entry named `lyric_headers` (`HEADER_HEADERS`) whose value is a JSON array of
    `[key, value]` string pairs. This is what a consumer using this package reads, so order,
    repeated keys and every key and value (empty, longer than 255 bytes, any character) survive
    on every broker version by construction. A message without headers has no such entry.
  - for interoperability, as one plain long-string entry per distinct key, holding the first
    value of that key. Other AMQP clients read these, and a `headers` exchange matches on them.
    A key that cannot be a table name (empty, over 255 bytes, or `lyric_headers` itself) has no
    plain entry.
- On consume (`headersOf`): when `lyric_headers` is present and holds a header list, that list is
  `Message.headers`. Plain entries whose key the list does not name (added by the broker, such as
  `x-death`, or by something that forwarded the message) follow it in the broker's order. A plain
  entry whose key the list does name is ignored, even if its value was changed on the way: the
  list wins. When `lyric_headers` is absent (a message from another producer) or is not a header
  list, every table entry is a header, as text, in the order the broker sent them; repeated keys
  are then not possible on RabbitMQ 4 and the order is the broker's.
- `Message.body` is UTF-8 text. No `content-type` is set; `Extras.publishWith` can set one.
- `Message.deliveryCount` is the `x-delivery-count` header when the broker sends one (quorum
  queues), else 1 when the delivery is flagged `redelivered`, else 0. `x-delivery-count` is not
  repeated in `headers`.
- Header values that are not strings (from another producer, or added by the broker, such as
  `x-death`) are delivered as text: integers and timestamps as digits, booleans as `true` /
  `false`, void as the empty string, decimals in decimal notation, byte arrays as their text when
  they are UTF-8 and as base64 otherwise, arrays and tables as JSON. `fieldText` is the function.

Error mapping: every failure is an `Err(String)` starting with `rabbitmq: `; nothing throws.

| Situation | `Err` text contains | Connection afterwards |
|---|---|---|
| no listener, DNS failure, connect timeout | `cannot connect to HOST:PORT` | n/a |
| bad credentials | `cannot open a connection to HOST:PORT: the broker closed the connection: 403 ACCESS_REFUSED` | n/a |
| unknown virtual host, no permission on it | `530 NOT_ALLOWED` | n/a |
| channel error (`404 NOT_FOUND`, `406 PRECONDITION_FAILED`, `403 ACCESS_REFUSED`, ...) | `the broker closed the channel: CODE TEXT` | usable: a new channel is opened |
| unroutable `mandatory` publish | `returned message 'ID' as unroutable: 312 NO_ROUTE` | usable |
| publish refused by the broker | `refused the message (basic.nack)` | usable |
| no confirm, or no reply, within `requestTimeoutMs` | `did not confirm the publish within N ms`, `did not answer METHOD within N ms` | lost |
| broker closed the connection (`320 CONNECTION_FORCED`, ...) or the socket | `connection lost: ...` | lost |
| no message within the consume timeout | exactly `timeout` | usable |

`lastReplyCode(queue)` returns the AMQP reply code of the most recent channel or connection error
or returned message, for code that needs to tell 404 from 406 without reading text.
`replyCodeName(code)` gives the specification's name for a code.

## Heartbeats

Heartbeats are off by default (`heartbeatSeconds = 0`, sent as 0 in `connection.tune-ok`, which
RabbitMQ accepts whatever it proposed). The reason is that the client is synchronous: it has no
thread of its own, so it can only write to the socket while the application is inside one of its
functions. A heartbeat that is negotiated but not sent makes the broker close a healthy idle
connection, which is worse than no heartbeat.

With `heartbeatSeconds = N` (or `?heartbeat=N` in the URL) the interval is the smaller of N and
the broker's proposal, and precisely this happens:

- While the application is inside an operation that waits (a `consume` with a timeout, a publish
  waiting for its confirm, any request waiting for its reply), the client sends a heartbeat frame
  every N/2 seconds and gives the connection up
  (`connection lost: no frame from the broker for ...`) when nothing has arrived for 2N seconds.
  A consumer loop that calls `consume(timeoutMs, ...)` back to back is therefore fully covered.
- Between operations nothing is sent. An application that does nothing with the connection for
  longer than about 2N seconds is disconnected by the broker; the next operation returns
  `connection lost: the broker closed the connection; this client had sent nothing for ...`.
- `keepAlive(queue)` sends one heartbeat frame and handles whatever the broker has sent. An
  application that negotiated a heartbeat and has idle periods calls it from its own timer at
  least every N/2 seconds. It takes the connection's lock like any operation.

Without a heartbeat an idle connection stays open as long as the network path keeps it (mind
load balancers and NAT with idle timeouts), and a broker that vanished without closing the socket
is noticed by the next operation: at once if the operating system already knows, otherwise when
`requestTimeoutMs` runs out. `keepAlive` works as a liveness probe here too.

## Surprises

Behaviour a user would not expect from the `Mq` interfaces alone:

- **Deliveries are pushed, and prefetched.** After the first `consume` the broker sends up to
  `prefetchCount` (default 10) messages ahead. They are held by this connection, invisible to
  other consumers, until handed to a handler and settled. Consequences:
  - `consume(0, ...)` returns only a message that has already arrived on the socket. A message
    published an instant earlier, even on the same connection, may not have been pushed yet.
  - In `Manual` mode a consumer that does not `ack`/`nack` fills the window; `consume` then
    returns `Err("timeout")` although the queue is not empty.
  - A requeued message returns to the head of the queue, but messages already in this
    connection's window are handed out first. With `prefetchCount = 1` a requeued message is the
    next one delivered.
  - Closing (or losing) a connection returns the whole window to the queue, and the broker flags
    all of it `redelivered`, including messages no handler ever saw.
  - A connection that only publishes starts no consumer and takes no messages.
- **`deliveryCount` stops at 1 on a classic queue.** RabbitMQ records only a `redelivered` flag
  there. Quorum queues count (`Extras.withQuorumQueue()`), and `Extras.withDeliveryLimit(n)` makes
  the broker dead-letter a poison message; do not build poison detection on `deliveryCount` with a
  classic queue. RabbitMQ 4 applies a delivery limit of 20 to quorum queues by default: without a
  dead-letter exchange, a message requeued more often than that is dropped by the broker.
- **`nack(id, false)` destroys the message unless the queue has a dead-letter exchange**
  (`Extras.withDeadLetter`). The same happens to a delivery this client cannot represent: a body
  that is not UTF-8, or one larger than `maxMessageBytes`, is rejected without requeueing so that
  it does not block the queue, and `consume` returns an `Err` naming it. Give a queue that other
  producers write to a dead-letter exchange.
- **`publishBatch` is not atomic.** Confirms are per message. The batch is written in one go and
  every confirm awaited; on an `Err`, messages the broker had already confirmed stay in the queue.
  (The `Mq` documentation mentions RabbitMQ confirms as all-or-nothing; they are not.)
- **`publish` returning `Err` does not always mean "not enqueued".** After
  `did not confirm the publish within N ms` the message may or may not be in the queue, and the
  connection is closed because its confirm stream can no longer be trusted. Consumers should be
  idempotent.
- **A broker under a memory or disk alarm blocks publishers.** `publish` then waits up to
  `requestTimeoutMs` and fails with `... it has blocked publishers (REASON)`, losing the
  connection. `blockedReason(queue)` shows the state.
- **`ack` and `nack` return `Ok` once written.** AMQP has no reply to them. A failure shows up as
  an `Err` from a later operation.
- **`Auto` mode requeues at once when the handler fails**, so a message whose handler always
  fails is redelivered in a tight loop on a classic queue. Use `Manual` with `nack(id, false)`
  and a dead-letter exchange, or a quorum queue with a delivery limit.
- **A channel error closes the channel, and the client opens a new one.** The failed operation
  returns the broker's reason; the connection stays usable. Deliveries that were unsettled on the
  old channel can no longer be settled (`no unsettled delivery has message id ...`); the broker
  requeues them and they arrive again, flagged `redelivered`.
- **Connecting declares the queue**, durable by default. If a queue of that name exists with
  other properties or arguments, connecting fails with `406 PRECONDITION_FAILED`; pass the same
  ones or `declare = false`. Queue arguments cannot be changed after creation.
- **The default `mandatory` flag makes a publish to a deleted queue an `Err`** (`312 NO_ROUTE`)
  instead of a silent drop.
- **URLs follow the RabbitMQ URI specification**: `amqp://host` is the virtual host `/`, but
  `amqp://host/` is the virtual host with the empty name, which a stock broker does not have
  (`530 NOT_ALLOWED`). Write `/%2f` or leave the path out. A `/` inside a name is `%2F`.
- **The `guest` user only works from the loopback address** on a stock broker.
- **No automatic reconnect.** After `connection lost`, every operation returns `Err`; open a new
  connection.
- **An empty message id on `publish` / `publishBatch` violates the interface's precondition.**
  `publish` panics with a contract violation on both targets. `publishBatch` panics on jvm and
  returns `Err("rabbitmq: empty message id")` on dotnet, because the inherited `requires:` clause
  is only enforced on jvm (see "Known gaps"). `ack("")` and `nack("")` are an `Err` on both.
- **Messages carry a `lyric_headers` header** next to the plain ones whenever `Message.headers`
  is not empty; other consumers of the queue see it. A consumer using this package that receives
  a message whose plain header was rewritten in transit (a shovel or a plugin, say) gets the
  original value from `lyric_headers`, not the rewritten one.
- **Floating-point header values** are rendered with the platform's number formatting, which can
  differ between dotnet and jvm for values that are not short decimals. Everything else is
  identical on both targets.
- **Threads.** A `RabbitMqQueue` can be shared between threads. Operations are serialized by a
  lock; a waiting `consume` releases it every 100 ms, and the lock is fair on the JVM so that a
  publisher waiting for it gets it then. The handler runs outside the lock and may call
  `ack`/`nack`.
- **`DeadLetterStore` is not implemented.** The interface is an append/list/count store with a
  capacity; a RabbitMQ dead-letter queue can only be consumed, so it does not map. Read dead
  letters by connecting to the dead-letter queue, or use `Mq.inProcessDeadLetterStore`.

## RabbitMQ 3.x and 4.x

The package does not depend on the broker version; where the brokers differ, this is what it
relies on and what was seen.

| Behaviour | 3.12.1 | 4.0.5 | What the package does |
|---|---|---|---|
| order and repeated keys of the `headers` table | kept as sent | one entry per key, order not kept | carries the list in `lyric_headers`; not affected |
| classic queue redelivery | `redelivered` flag only | the same | `deliveryCount` 0 or 1 |
| quorum queue `x-delivery-count` after `nack(id, true)` | 1, 2, 3, ... | the same in the integration suite | `deliveryCount` is that header |
| quorum queue delivery limit | none unless `x-delivery-limit` is set | a default limit of 20 (RabbitMQ documentation; not exercised by the suite) | nothing: a message requeued more often than the limit is dead-lettered or dropped by the broker. Set `withDeliveryLimit` and `withDeadLetter` deliberately. |
| position of a requeued message | classic: head of the queue | classic: head of the queue | tested on classic queues only; on a quorum queue the position depends on the version and on whether a delivery limit applies, and nothing here relies on it |
| queue type when no `x-queue-type` is sent | the virtual host's default, classic on a stock broker | the same, and the node can set `default_queue_type` | sends `x-queue-type` only for `withQuorumQueue()`. A broker whose default is quorum refuses `durable = false` and `autoDelete = true`. |
| `durable = false` on a shared queue | accepted | accepted, deprecated (`transient_nonexcl_queues`) and may be refused by a later version | keeps `durable = true` as the default |
| largest message | 128 MiB by default | 16 MiB by default (`max_message_size`) | a larger publish is a `406 PRECONDITION_FAILED` channel error from the broker |
| dead-letter `x-death` header | array of tables | the same in the integration suite | delivered as JSON text |

The 4.0.5 column comes from the CI run (dotnet integration suite) and the RabbitMQ documentation;
no 4.x broker was available on the development machine. The hermetic suite has a broker that
reshapes header tables the way RabbitMQ 4 does (one entry per key, order changed), and the whole
conformance suite runs against it, so the header mapping cannot regress on a 3.x-only machine.

## RabbitMQ-specific features (`Mq.RabbitMq.Extras`)

Import the package whole (`import Mq.RabbitMq.Extras`) for the `with...` methods on
`ConnectOptions`.

- Queue arguments, applied when the connection declares its queue: `withQuorumQueue()`,
  `withDeadLetter(exchange, routingKey)`, `withMessageTtl(ms)`, `withQueueExpiry(ms)`,
  `withMaxLength(n)`, `withMaxPriority(levels)`, `withDeliveryLimit(n)`,
  `withQueueArgument(name, value)` for any other.
- Exchanges: `declareExchange(queue, name, kind, options)` (`direct`, `fanout`, `topic`,
  `headers`), `deleteExchange`, `bindQueue(queue, exchange, routingKey)`, `bindQueueWith` (binding
  arguments for a `headers` exchange).
- Other queues on the same connection (a dead-letter queue, say): `declareQueue`, `inspectQueue`
  (message and consumer counts), `purgeQueue`, `deleteQueue`.
- `publishWith(queue, message, options)` with `PublishOptions`: `exchange`, `routingKey`,
  `mandatory`, `persistent`, `priority` (0 to 255), `expirationMs`, `contentType`,
  `contentEncoding`, `correlationId`, `replyTo`, `kind` (the `type` property), `appId`,
  `timestamp`. `defaultPublishOptions(queue)` publishes like `Mq.publish`;
  `toExchange(queue, exchange, routingKey)` targets an exchange.

```lyric
import Mq.RabbitMq
import Mq.RabbitMq.Extras

val options = Mq.RabbitMq.optionsFromUrl(url, "orders")?
  .withQuorumQueue()
  .withDeliveryLimit(5)
  .withDeadLetter("", "orders.dead")
val orders = Mq.RabbitMq.connectWith(options)?
Mq.RabbitMq.Extras.declareQueue(orders, "orders.dead", Mq.RabbitMq.Extras.defaultQueueOptions())?
Mq.RabbitMq.Extras.declareExchange(orders, "events", "topic", Mq.RabbitMq.Extras.defaultExchangeOptions())?
Mq.RabbitMq.Extras.bindQueue(orders, "events", "order.*")?
```

Lower level: `Mq.RabbitMq.publishTo` takes raw `Amqp.Properties`, `Mq.RabbitMq.request` sends one
of the exchange and queue management methods, and `Mq.RabbitMq.Amqp` is the codec (frames, field
tables, methods, content headers) with no I/O in it.

## Tests

Run from the repository root.

```sh
# hermetic: codec, URL, options, and the client against an in-process AMQP broker (plain, TLS, mutual TLS)
lyric test --manifest mq-rabbitmq/lyric.toml --target dotnet
lyric test --manifest mq-rabbitmq/lyric.toml --target jvm

# integration: a real RabbitMQ
eval "$(bash scripts/ci/start-rabbitmq.sh)"
lyric test --manifest mq-rabbitmq/lyric.toml --target dotnet --features integration
lyric test --manifest mq-rabbitmq/lyric.toml --target jvm --features integration
kill $(cat "$LYRIC_RABBITMQ_WORK"/node-*/pid)
```

`scripts/ci/start-rabbitmq.sh` installs Ubuntu's `rabbitmq-server` package if it is missing and
starts two disposable nodes as plain processes (no systemd, data in a temporary directory, a random
password): one with a plain listener and a TLS listener, one whose TLS listener requires a client
certificate. A RabbitMQ node has a single set of TLS options, hence the second node. It prints,
or under GitHub Actions exports:

| Variable | Meaning |
|---|---|
| `LYRIC_RABBITMQ_URL` | `amqp://user:password@host:port/vhost`; the user needs configure, write and read permission on the virtual host |
| `LYRIC_RABBITMQ_TLS_URL` | `amqps://...` for a listener that does not require a client certificate |
| `LYRIC_RABBITMQ_MTLS_URL` | `amqps://...` for a listener that requires one |
| `LYRIC_RABBITMQ_TLS_CA` | PEM file: the CA of the broker's certificate, which must name the URLs' host |
| `LYRIC_RABBITMQ_TLS_CERT` | PEM file: a client certificate the broker trusts |
| `LYRIC_RABBITMQ_TLS_KEY` | PEM file: its PKCS8 private key |

With `--features integration` every test fails with a message naming the variable it needs when
that variable is unset. The integration suite runs `Testing.Conformance.Mq.runQueue` on a classic
queue, on a quorum queue and over TLS, and the backend tests: reply codes (403, 404, 406, 530,
312), channel recovery, requeue order, prefetch, dead-lettering, delivery limits, TTL, priorities,
exchanges, frame splitting, heartbeats (kept alive, and disconnected when idle), threads, and the
TLS cases (verified, untrusted, wrong name, TLS 1.3, mutual TLS accepted and refused).

The hermetic `tests/client_tests.l` contains the fake broker and the TEST-ONLY certificates it
uses; they are fixtures, generated for that file, and trusted by nothing else.

## Known gaps

Compiler issues worked around (nichobbs/lyric-lang):

- [#9043](https://github.com/nichobbs/lyric-lang/issues/9043): on jvm an interface's inherited
  `requires:` clause is resolved in the implementing package. `Mq.MessageQueue.publishBatch`
  requires `allHaveIds(messages)`, so `src/rabbitmq.l` declares a private `allHaveIds` (the build
  warns `W0037` about the shadowing). The package uses it itself to refuse a batch with an empty
  id, which is what dotnet callers get, since the inherited clause is not enforced there.
- [#9049](https://github.com/nichobbs/lyric-lang/issues/9049): on jvm a `slice[Byte]` bound from
  `val x = match r { case Ok(b) -> b; case Err(e) -> return ... }` without a type annotation is
  cast to `Object[]` and fails at run time. The affected locals in `src/amqp.l` and
  `src/rabbitmq.l` carry an explicit `slice[Byte]` type.
- Extension methods declared in a package that is imported under an alias cannot be called with
  method syntax (`T0113`), so `Mq.RabbitMq.Extras` must be imported whole for
  `options.withQuorumQueue()`. No issue filed yet.

Not implemented (no issue filed yet):

- A background heartbeat; see "Heartbeats" for what the synchronous client does instead.
- Automatic reconnect and topology recovery.
- More than one channel per connection, and more than one queue per `RabbitMqQueue`.
- SASL mechanisms other than PLAIN (EXTERNAL, AMQPLAIN), and `connection.update-secret`.
- Exclusive and server-named queues, `basic.get`, `basic.recover`, transactions (`tx.*`),
  `exchange.bind` / `queue.unbind`, consumer arguments and consumer priorities, direct reply-to.
- Streams (`x-queue-type = stream` needs consumer offsets, which `basic.consume` here does not
  send) and AMQP 1.0.
- Binary bodies: the `Mq.Message` body is a `String`.
- `DeadLetterStore` (see "Surprises").
