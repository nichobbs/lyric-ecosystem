# lyric-mq-rabbitmq

RabbitMQ backend for the `Mq.MessageQueue` and `Mq.QueueConsumer` interfaces of `lyric-mq`. The
client is an AMQP 0-9-1 implementation written in Lyric; only the TCP socket and the TLS layer
over it come from the host platform, through `Net.Tcp` of the `Lyric.Net` package in this
repository (`net/`), so there is no RabbitMQ client SDK dependency.

| | |
|---|---|
| Package | `Lyric.Mq.RabbitMq` (Lyric packages `Mq.RabbitMq`, `Mq.RabbitMq.Extras`, `Mq.RabbitMq.Amqp`, `Mq.RabbitMq.Url`) |
| Targets | `dotnet`, `jvm` |
| Stability | `experimental` |
| Broker | RabbitMQ over AMQP 0-9-1. Tested against RabbitMQ 3.12.1 (Erlang/OTP 25, the Ubuntu 24.04 package; every suite, both targets, on a development machine) and 4.0.5 (the Ubuntu 26.04 package, in CI). See "RabbitMQ 3.x and 4.x". |
| Transport | plain TCP, or TLS 1.2 / 1.3 (`SslStream` on dotnet, `SSLSocket` on jvm) |
| SDK dependencies | none (`[nuget]` and `[maven]` are empty) |
| Depends on | `Lyric.Net` (`net/`): TCP and TLS connection, TLS options, URL authority parsing |
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
By default the connection sends heartbeats and is re-established when it is lost (see "Heartbeats"
and "Recovery"); it owns a background thread for that until `close()`.

## Configuration

Three ways to connect; none of them reads a credential from source.

- `connectWith(options)`: a `ConnectOptions` record. Start from
  `defaultOptions(host, port, queue)` and adjust with `.copy(...)`. `validateOptions(options)`
  runs the same checks without connecting.
- `connectUrl(url, queueName)`: a URL in the RabbitMQ URI scheme,
  `amqp://[user[:password]@]host[:port][/vhost][?parameters]` (port 5672) or `amqps://...` (TLS,
  port 5671). User, password and virtual host are percent-decoded; an IPv6 address is written in
  brackets (`amqp://[::1]:5672`) and refused without them. Without user information the
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
  | `LYRIC_CONFIG_MQ_RABBITMQ_RECOVERY` | `true` (default) or `false`: re-establish a lost connection, with the default delays |
  | `LYRIC_CONFIG_MQ_RABBITMQ_TLSCAFILE` | PEM file: a CA certificate trusted in addition to the platform trust store |
  | `LYRIC_CONFIG_MQ_RABBITMQ_TLSCAEXCLUSIVE` | `true`: trust only that CA (default `false`) |
  | `LYRIC_CONFIG_MQ_RABBITMQ_TLSCERTFILE` | PEM file: client certificate (mutual TLS) |
  | `LYRIC_CONFIG_MQ_RABBITMQ_TLSKEYFILE` | PEM file: its PKCS8 private key |
  | `LYRIC_CONFIG_MQ_RABBITMQ_TLSSERVERNAME` | name to check the certificate against, when it is not the URL's host |
  | `LYRIC_CONFIG_MQ_RABBITMQ_TLSMINVERSION` | `1.2` (default) or `1.3` |

  The heartbeat comes from the URL (`?heartbeat=N`). The `TLS...` variables apply to an `amqps://`
  URL; with an `amqp://` URL they are an error.
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
| `heartbeatSeconds` | 60 | heartbeat interval to ask for, 0 to 65535; 0 = off. See "Heartbeats" |
| `frameMax` | 131072 | largest frame, at least 4096; the smaller of this and the broker's limit is used |
| `connectTimeoutMs` | 10000 | TCP connect limit, and again the TLS handshake limit; 0 = the system's |
| `requestTimeoutMs` | 10000 | how long to wait for a reply or a publisher confirm, and how long a write may make no progress because the broker is not reading |
| `maxMessageBytes` | 16 MiB | largest message body accepted from the broker |
| `connectionName` | empty | shown in the broker's management UI |
| `tls` | `plainTcp()` | TLS settings (`TlsOptions` of `Net.Tls`, re-exported by `Mq.RabbitMq`), see below |
| `recovery` | `defaultRecovery()` | whether and how a lost connection is re-established, see "Recovery" |

`RecoveryOptions` fields (`withRecovery(r)` sets them, `withoutRecovery()` turns recovery off):

| Field | Default | Meaning |
|---|---|---|
| `enabled` | `true` | `false`: after a loss every operation returns `Err` until the application connects again |
| `initialDelayMs` | 1000 | wait before the first attempt, at least 1; doubled after every failed attempt |
| `maxDelayMs` | 30000 | cap of that wait, not below `initialDelayMs` |
| `maxAttempts` | 0 | failed attempts in a row after which the connection is given up for good; 0 = no limit |

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
| the broker accepts no data for `requestTimeoutMs` during a write | `connection lost: the write timed out: ...` | lost |
| nothing from the broker for two heartbeat intervals | `connection lost: no frame from the broker for N s (heartbeat M s)` | lost |
| any operation while the connection is being re-established | `connection lost: REASON; the client is reconnecting; the operation was not attempted` (`isNotAttempted`) | lost, coming back |
| a publish written, or partly written, when the connection was lost | `connection lost: REASON; ...; the outcome of the publish is unknown: ...` (`isOutcomeUnknown`) | lost, coming back |
| `ack` / `nack` of a delivery from a lost connection or closed channel | `no unsettled delivery has message id 'ID': it was delivered before the connection or channel was lost ...` | usable |
| recovery given up (`maxAttempts`, or a refusal) | `connection lost: REASON; reconnecting was given up ...`, `...; not reconnecting, the broker's refusal is permanent` | lost for good |
| no message within the consume timeout | exactly `timeout` | usable |

"Lost" means: with recovery on (the default) the connection is being re-established and becomes
usable again; with recovery off every later operation returns `connection lost: REASON`.

`lastReplyCode(queue)` returns the AMQP reply code of the most recent channel or connection error
or returned message, for code that needs to tell 404 from 406 without reading text.
`replyCodeName(code)` gives the specification's name for a code.

## Heartbeats

The client asks for an interval of `heartbeatSeconds` (60 by default, or `?heartbeat=N` in the
URL) in `connection.tune-ok`:

| Client asks | Broker proposes | Interval |
|---|---|---|
| 0 | anything | none: heartbeats are off, the broker sends none and expects none |
| N | 0 | N |
| N | M | the smaller of N and M |

(This is the rule of the AMQP specification, where the client's `tune-ok` decides. Some clients
treat their own 0 as "no preference" and take the broker's value; here 0 always means off.)

With an interval of N seconds:

- The client sends a heartbeat frame (frame type 8, channel 0) whenever it has written nothing
  for N/2 seconds. Any frame it writes counts, so a busy connection sends none.
- It gives the connection up with `connection lost: no frame from the broker for ...` when nothing
  at all has arrived for 2N seconds. Any frame from the broker counts, not only heartbeats.
- An operation that waits (a `consume` with a timeout, a publish waiting for its confirm) does
  both itself. Between operations the connection's background thread does, under the connection's
  lock: it wakes a few times per second (every N/8 seconds, between 50 and 250 ms), reads what the
  broker has sent and sends a heartbeat when one is due. An idle application needs to do nothing.
- Deliveries the background thread reads are kept for the next `consume`; it stops reading ahead
  when 1024 wait, so a consumer with `prefetchCount = 0` that stops consuming is held back by TCP
  as before. A channel error it comes across (the broker's answer to an invalid `ack`, say) is
  returned by the application's next operation.

With `heartbeatSeconds = 0` nothing is sent. An idle connection then stays open as long as the
network path keeps it (mind load balancers and NAT with idle timeouts), and a broker that vanished
without closing the socket is only noticed by an operation, when `requestTimeoutMs` runs out.
`keepAlive(queue)` sends one heartbeat frame and reads what has arrived: a liveness probe for that
case. `heartbeatSeconds(queue)` is the negotiated interval.

The background thread exists when a heartbeat was negotiated or recovery is on, which is the
default. With `heartbeatSeconds = 0` and `withoutRecovery()` the connection has no thread of its
own and only touches the socket inside the application's calls.

## Recovery

A connection is lost when its socket fails, when the broker is silent for two heartbeat intervals,
when a reply or a confirm does not come within `requestTimeoutMs`, or when the broker closes it
(`320 CONNECTION_FORCED` at a shutdown or from `rabbitmqctl close_connection`, for instance). With
`recovery.enabled` (the default) the background thread then re-establishes it:

1. It waits. The wait before attempt k is `initialDelayMs * 2^(k-1)`, capped at `maxDelayMs`, and
   then a random point in the upper half of that (attempt 1 with the defaults: 0.5 to 1 s; from
   attempt 6 on: 15 to 30 s), so that clients which lost a broker together do not return together.
2. It connects, authenticates and opens the channel in publisher-confirm mode, exactly as
   `connectWith` does, with the same options (the same host: there is no list of hosts).
3. It restores what this client had set up, in the order it was made:
   - the connection's queue, declared (or, with `declare = false`, checked) from the options;
   - every exchange and queue declared and every binding made through `Mq.RabbitMq.Extras`
     (`declareExchange`, `declareQueue`, `bindQueue`, `bindQueueWith`, or `request`), unless it was
     deleted again through `deleteExchange` / `deleteQueue`. Durable entities are declared again
     too; that is a no-op on a broker that still has them;
   - the prefetch window (`basic.qos`) and the consumer (`basic.consume`), when one had been
     started by `consume` or `subscribe` and not cancelled by the broker.
4. The connection is usable again. `recoveryCount(queue)` counts these.

What is not restored, because the client did not create it or cannot: messages, the contents of
non-durable queues, entities made by other connections, anything published.

An attempt that fails is followed by the next wait, except when trying again cannot help:

| Failure of an attempt | Recovery |
|---|---|
| no listener, timeout, TLS handshake failure, the socket closed during the handshake, `320`, `541`, any other reply code | next attempt after the wait |
| `403 ACCESS_REFUSED`, `530 NOT_ALLOWED` (credentials, virtual host), `402` | given up |
| `404`, `405`, `406 PRECONDITION_FAILED` while restoring (the queue of a `declare = false` connection is gone, an entity now exists with other properties) | given up |
| the peer does not speak AMQP 0-9-1 or does not offer PLAIN | given up |
| `maxAttempts` attempts in a row failed (when it is not 0) | given up |

The same reply codes decide at the loss itself: a broker that closes an established connection with
`403`, `530` or another code of the second and third row is not reconnected to. Once recovery is
given up the connection is lost for good: every operation returns
`connection lost: REASON; reconnecting was given up ...` and the background thread ends.

Finding out where a connection stands, without reading error text:

| Function | |
|---|---|
| `connectionState(queue)` | `Connected`, `Reconnecting(failedAttempts)`, `Lost(reason)` (for good) or `Closed` |
| `isOpen(queue)` | true while connected |
| `awaitConnected(queue, timeoutMs)` | wait for a connection that is being re-established; `Err` when the time runs out, or when it is closed or lost for good |
| `recoveryCount(queue)` | how often the connection has been re-established |
| `isReconnecting(e)`, `isNotAttempted(e)`, `isOutcomeUnknown(e)` | classify the `Err` text of an operation (below) |
| `backgroundRunning(queue)` | whether the background thread is alive |
| `abortTransport(queue)` | close the socket under the client, as a network failure would; for testing an application across a reconnect |

What the operations do across a loss is in "Surprises", under "Across a lost connection".

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
  connection is given up (and re-established) because its confirm stream can no longer be
  trusted. The error says so (`isOutcomeUnknown`), as it does for every publish the connection
  was lost under. Consumers should be idempotent.
- **A broker under a memory or disk alarm blocks publishers.** `publish` then waits up to
  `requestTimeoutMs` and fails with `... it has blocked publishers (REASON)`, losing the
  connection, which is then re-established. `blockedReason(queue)` shows the state.
- **`ack` and `nack` return `Ok` once written.** AMQP has no reply to them. A failure shows up as
  an `Err` from a later operation.
- **`Auto` mode requeues at once when the handler fails**, so a message whose handler always
  fails is redelivered in a tight loop on a classic queue. Use `Manual` with `nack(id, false)`
  and a dead-letter exchange, or a quorum queue with a delivery limit.
- **A channel error closes the channel, and the client opens a new one.** The failed operation
  returns the broker's reason; the connection stays usable. Deliveries that were unsettled on the
  old channel can no longer be settled (`no unsettled delivery has message id ...: it was delivered
  before the connection or channel was lost ...`); the broker requeues them and they arrive again,
  flagged `redelivered`. The consumer is not restarted here; the next `consume` starts it.
- **Connecting declares the queue**, durable by default. If a queue of that name exists with
  other properties or arguments, connecting fails with `406 PRECONDITION_FAILED`; pass the same
  ones or `declare = false`. Queue arguments cannot be changed after creation.
- **The default `mandatory` flag makes a publish to a deleted queue an `Err`** (`312 NO_ROUTE`)
  instead of a silent drop.
- **URLs follow the RabbitMQ URI specification**: `amqp://host` is the virtual host `/`, but
  `amqp://host/` is the virtual host with the empty name, which a stock broker does not have
  (`530 NOT_ALLOWED`). Write `/%2f` or leave the path out. A `/` inside a name is `%2F`.
- **The `guest` user only works from the loopback address** on a stock broker.
- **Across a lost connection** (with recovery on, the default; see "Recovery"):
  - **Operations issued while disconnected fail at once**; they do not wait and nothing is queued
    in the client. `publish`, `publishBatch`, `ack`, `nack`, `subscribe`, `keepAlive` and the
    `Extras` requests return `connection lost: REASON; the client is reconnecting; the operation
    was not attempted`. `isNotAttempted(e)` is true for it: nothing reached the broker and
    repeating the operation later is safe. Why not wait: how long a broker stays away is unknown
    (the default recovery never gives up), a publish that blocks for that long hides the outage
    from the one caller who can decide what to do with the message, and a client-side queue of
    publishes would be lost with the process while `publish` had already said `Ok`. A caller that
    prefers to wait calls `awaitConnected(queue, timeoutMs)` and publishes again.
  - **`consume` is the exception: it waits.** A `consume(timeoutMs, ...)` that is waiting when
    the connection drops, or that is called while it is down, keeps waiting within its timeout and
    returns a message once the connection is back and one arrives. When the time runs out first
    it returns the `connection lost: ...; the client is reconnecting; ...` error, not `timeout`.
    `consume(0, ...)` returns that error at once. So a loop around `consume` neither spins nor
    needs to know about reconnects.
  - **A publish that was on the wire when the connection dropped has an unknown outcome.** The
    broker may have enqueued it and the confirm was lost, or it never arrived. The publish returns
    `connection lost: REASON; ...; the outcome of the publish is unknown: the broker may or may not
    have enqueued the message, and it was not sent again` (`isOutcomeUnknown(e)`). The client
    never repeats it on the new connection, since that could enqueue it twice, and never reports
    it as `Ok`. Publishing again is the caller's decision; a consumer that deduplicates by
    `Message.id` makes it safe. For `publishBatch` this covers every message of the batch that had
    no confirm yet: none, some or all of them may be in the queue.
  - **Unsettled deliveries come back.** The broker requeues every delivery that was not
    acknowledged when the connection went, including those in the prefetch window that no handler
    had seen, and delivers them again on the new connection (or to another consumer of the queue)
    flagged `redelivered`: `deliveryCount` is 1 on a classic queue, however often this happens,
    and the broker's `x-delivery-count` on a quorum queue (1 after one loss on RabbitMQ 3.12.1;
    whether a loss counts towards a delivery limit depends on the broker version). A handler can
    therefore see a message twice although it never called `nack`: handlers must be idempotent.
  - **An `ack` or `nack` for a delivery from before the reconnect is an `Err`** (`no unsettled
    delivery has message id 'ID': it was delivered before the connection or channel was lost and
    cannot be settled any more; ...`) and writes nothing. Delivery tags are numbered per channel
    and start again at 1 on the new one, so the old tag would name a different message there.
    The client keeps tags per channel incarnation: settling by id only ever uses a tag of the
    present channel, and a delivery that was read from the old connection but had not reached a
    handler is dropped instead of being handed out. If the message has been delivered again in
    the meantime and is unsettled, `ack(id)` settles that new delivery, which is the same message.
    In `Auto` mode a handler that returned `Ok` while the connection dropped under it makes
    `consume` return an `Err` (this one, or the `connection lost` one while the client is still
    reconnecting); the message is delivered again.
  - **Recovery does not go on for ever against a refusal.** Wrong credentials, a missing virtual
    host, a queue or exchange that now exists with other properties (`406`), or the queue of a
    `declare = false` connection that is gone (`404`) end it after one attempt; see the table in
    "Recovery". Against an unreachable broker it goes on with waits of up to `maxDelayMs` unless
    `maxAttempts` is set. A TLS failure (an expired certificate, say) cannot be told from a
    transient one and is retried like an unreachable broker.
  - **Recovery declares again, it does not compare.** If the queue was deleted while the client
    was away, recovery creates it again, empty. An auto-delete queue whose only consumer was this
    connection is deleted by the broker at the loss, with its messages and bindings, and declared
    and bound again by recovery.
  - **`close()` during recovery returns at once.** It does not wait for the wait between attempts
    or for an attempt in progress; an attempt's socket is closed under it. The background thread
    ends within a moment (`backgroundRunning(queue)`); only an attempt that is still inside the TCP
    connect or the TLS handshake runs into `connectTimeoutMs` first, and its connection is then
    closed unused.
  - **The first connection is not retried.** `connectWith` returns `Err` when the broker cannot be
    reached; recovery starts once a connection has been handed out.
  - **`lastReplyCode`, `serverInfo`, `heartbeatSeconds` and `negotiatedFrameMax` describe the
    present connection** and can change with a recovery.
- **With `withoutRecovery()`** a lost connection stays lost: every operation returns
  `connection lost: REASON`; open a new connection.
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
  `ack`/`nack`. Every connection with a heartbeat or with recovery (the default has both) owns a
  background thread until `close()`, or until it is lost for good; it works under the same lock
  and only when no operation holds it. On `dotnet` the socket adds a reader thread of its own
  (see the `Lyric.Net` README), and the client does not use the runtime's shared thread pool. The
  threads do not keep a process alive, but a connection that is dropped without `close()` keeps
  them, and keeps reconnecting: always `close`.
- **A write to a broker that has stopped reading fails after `requestTimeoutMs`** and loses the
  connection: how much of the frame was sent is unknown.
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
| heartbeat proposed in `connection.tune` | 60 s | 60 s | asks for 60 s by default, so the interval is 60 s |
| quorum queue `x-delivery-count` after the consumer's connection was lost | 1 | not asserted: 4.x separates deliveries from acquisitions, and a loss need not count towards the delivery limit | `deliveryCount` is at least 1 (the `redelivered` flag); the integration suite asserts `>= 1` |

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
exchanges, frame splitting, heartbeats (inside a blocked consume, and on an idle connection), recovery
after the client's socket is failed with `abortTransport` (redelivery on a classic and on a quorum
queue, the refused stale `ack`, a blocked consume that continues, an auto-delete queue with its
binding and consumer restored, a `404` that ends recovery, `close` during the wait), threads, and
the TLS cases (verified, untrusted, wrong name, TLS 1.3, mutual TLS accepted and refused).

The hermetic `tests/client_tests.l` contains the fake broker, built on `Net.Tcp.Testing`, and the
TEST-ONLY certificates it uses; they are fixtures, generated for that file, and trusted by nothing
else. The fake broker can drop its connections, close them with a reply code, refuse or ignore new
ones and stay silent, and it counts what it receives; the heartbeat and recovery tests use that to
assert what went over the wire (heartbeat frames from an idle client, what is declared again, that
a stale `ack` writes nothing, that a publish is not repeated). Most other tests connect with
`heartbeatSeconds = 0` and `withoutRecovery()`, so that nothing happens in the background of a
test about one operation; the conformance suite also runs with the defaults. Waits in these tests
end when the awaited state is reached; their limits (30 s and more) are only reached on failure. The socket-level tests (timed reads, write timeout, the lock, independence from the thread
pool) are in `net/tests/tcp_tests.l`.

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

The socket and TLS boundary, the TLS options and their checks, and the authority part of the URL
are `Lyric.Net`'s (`net/`), shared with `mq-activemq` and `redis`; its README lists the gaps of that
layer (lyric-lang#9042, lyric-lang#9064) and what each target does.

Not implemented (no issue filed yet):

- Recovery connects to the one configured host; there is no list of hosts to fail over to.
- No notification of a loss or a recovery (a listener); `connectionState` and `recoveryCount`
  are polled.
- A failed TLS handshake during recovery is retried like an unreachable broker, because the
  platform's error text does not tell an expired or untrusted certificate from a transient
  failure. Set `maxAttempts` to bound it.
- An attempt that is inside the TCP connect or the TLS handshake cannot be interrupted by
  `close()`: `Net.Tcp` has no way to abandon a `sockConnect` or `sockStartTls` in progress. It
  ends with `connectTimeoutMs`.
- More than one channel per connection, and more than one queue per `RabbitMqQueue`.
- SASL mechanisms other than PLAIN (EXTERNAL, AMQPLAIN), and `connection.update-secret`.
- Exclusive and server-named queues, `basic.get`, `basic.recover`, transactions (`tx.*`),
  `exchange.bind` / `queue.unbind`, consumer arguments and consumer priorities, direct reply-to.
- Streams (`x-queue-type = stream` needs consumer offsets, which `basic.consume` here does not
  send) and AMQP 1.0.
- Binary bodies: the `Mq.Message` body is a `String`.
- `DeadLetterStore` (see "Surprises").
