# lyric-mq-activemq

Apache ActiveMQ backend for the `Mq.MessageQueue` and `Mq.QueueConsumer` interfaces of `lyric-mq`,
for `dotnet` and `jvm`, `experimental` (0.1.0). The client is a STOMP 1.2 implementation written in
Lyric; only the TCP socket and the TLS layer over it come from the host platform, so there is no
JMS, NMS or other SDK dependency. One `ActiveMqQueue` is one connection to the broker and one
destination, and implements both interfaces.

| | |
|---|---|
| Package | `Lyric.Mq.ActiveMq` (Lyric packages `Mq.ActiveMq`, `Mq.ActiveMq.Extras`) |
| Interfaces | `Mq.MessageQueue`, `Mq.QueueConsumer` of `Lyric.Mq` 0.1.0. `Mq.DeadLetterStore` is not implemented. |
| Targets | `dotnet`, `jvm` |
| Stability | `experimental` (0.1.0) |
| Broker | Apache ActiveMQ Classic with a `stomp://` or `stomp+ssl://` transport connector. Tested against 5.17.6. STOMP 1.2 needs ActiveMQ 5.9 or later. |
| Transport | plain TCP, or TLS 1.2 / 1.3 (`SslStream` on dotnet, `SSLSocket` on jvm) |
| SDK dependencies | none (`[nuget]` and `[maven]` are empty) |

ActiveMQ Artemis is a different broker with different redelivery rules and is not covered.

## Usage

Inside this workspace a package declares `"Lyric.Mq.ActiveMq" = { workspace = true }` under
`[dependencies]`, and `"Lyric.Mq" = { workspace = true }` as well when it imports `Mq` itself, as
the examples here do.

```lyric
import Std.Core
import Mq
import Mq.ActiveMq as ActiveMq

func handle(m: in Mq.Message): Result[Unit, String] {
  println(m.id + ": " + m.body)
  Ok(())
}

func example(url: in String): Result[Unit, String] {
  val queue = ActiveMq.connectUrl(url, "orders")?
  defer {
    queue.close()
  }
  Mq.publish(
    queue,
    Mq.Message(
      id = "order-42",
      body = "{\"total\":12}",
      headers = [Mq.MessageHeader(key = "content-type", value = "application/json")],
      deliveryCount = 0
    )
  )?
  Mq.consume(queue, 5000, { m -> handle(m) })?
  Mq.ack(queue, "order-42")?
  Ok(())
}
```

`example("stomp://app:passcode@broker.internal:61613")` connects, authenticates as `app`, publishes
to `/queue/orders`, waits up to five seconds for a message, acknowledges it and disconnects.

Every operation returns `Result[_, String]`; no socket or TLS failure throws. The only panics are
the interface's contracts (`PreconditionViolated`): an empty id passed to `publish`, an empty slice
passed to `publishBatch`, a negative `timeoutMs`. The error texts:

| Text | Meaning |
|---|---|
| `timeout` | `consume`: no message arrived within `timeoutMs` (the interface's text, no prefix) |
| the handler's own text | `consume`: the handler returned `Err`; it is passed through unchanged |
| `activemq: empty message id` | an empty id given to `ack`, `nack`, `publishWithHeaders`, or (on dotnet) inside a `publishBatch` |
| `activemq: no unsettled delivery has message id '...'` | `ack`/`nack` of an id that was never delivered on this connection, or is already settled |
| `activemq: broker error: ...` | the broker answered with an `ERROR` frame; the text is its `message` header. The connection stays usable. |
| `activemq: connection lost: ...` | the transport failed or a request was not confirmed in time; the connection is unusable from here on |
| `activemq: the connection is closed` | any operation after `close` |
| `activemq: cannot connect to HOST:PORT: ...` | the TCP connection could not be opened |
| `activemq: TLS handshake with HOST:PORT failed: ...` | the TLS handshake failed or timed out |
| `activemq: message ID cannot be delivered: ...` | `consume` received a message this client cannot represent and rejected it |
| `activemq: invalid broker URL: ...`, `activemq: TLS ...`, other `activemq: ...` | unusable URL, options, certificate or key; nothing was dialled |

No text contains the passcode or a private key. The part after the last colon of a connect,
handshake or lost-connection error is the host platform's wording and differs between targets.

Variations:

- **One import.** `Mq.ActiveMq` re-exports `Mq` (`pub use Mq`), so with `import Mq.ActiveMq` alone
  the interface names are visible unqualified (`Message`, `MessageHeader`, `MessageQueue`,
  `publish`, `consume`, `ack`) or as `Mq.ActiveMq.Message`. `Mq.publish` and `Mq.Message` need
  `import Mq`.
- **Explicit options** instead of a URL, here with `Auto` acknowledgement:

  ```lyric
  func configured(passcode: in String): Result[ActiveMq.ActiveMqQueue, String] {
    val base = ActiveMq.defaultOptions("broker.internal", 61613, ActiveMq.resolveDestination("orders"))
    ActiveMq.connectWith(
      base.copy(
        login = "app",
        passcode = passcode,
        ackMode = Mq.AckMode.Auto,
        prefetchSize = 1,
        connectTimeoutMs = 3000,
        requestTimeoutMs = 5000
      )
    )
  }
  ```

- **The environment** (variables under "Configuration"):

  ```lyric
  func fromEnvironment(): Result[ActiveMq.ActiveMqQueue, String] {
    ActiveMq.connectFromEnv()
  }
  ```

- **Settling by hand** (`Manual`, the default). The handler's result does not settle anything; the
  application acknowledges, requeues or rejects, here giving up after five deliveries:

  ```lyric
  func process(m: in Mq.Message): Result[Unit, String] {
    if m.body.length == 0 {
      Err("empty body")
    } else {
      Ok(())
    }
  }

  ref record Seen {
    var id: String
    var deliveryCount: Int
  }

  func settleOne(queue: in ActiveMq.ActiveMqQueue): Result[Unit, String] {
    val seen = Seen(id = "", deliveryCount = 0)
    val outcome = queue.consume(
      1000,
      { m ->
        seen.id = m.id
        seen.deliveryCount = m.deliveryCount
        process(m)
      }
    )
    if seen.id.length == 0 {
      return outcome
    }
    match outcome {
      case Ok(_) -> queue.ack(seen.id)
      case Err(_) -> queue.nack(seen.id, seen.deliveryCount < 5)
    }
  }
  ```

- **Through the interfaces**, for code that must not know the backend:

  ```lyric
  func send(sink: in Mq.MessageQueue, id: in String, body: in String): Result[Unit, String] {
    sink.publish(Mq.Message(id = id, body = body, headers = [], deliveryCount = 0))
  }

  func wired(queue: in ActiveMq.ActiveMqQueue): Result[Unit, String] {
    val publisher: Mq.MessageQueue = queue
    val consumer: Mq.QueueConsumer = queue
    send(publisher, "a-1", "first")?
    Mq.publishBatch(publisher, [
      Mq.Message(id = "a-2", body = "second", headers = [], deliveryCount = 0),
      Mq.Message(id = "a-3", body = "third", headers = [], deliveryCount = 0)
    ])?
    consumer.consume(0, { m -> Ok(()) })
  }
  ```

- **TLS.** A `stomp+ssl://` URL verifies the broker against the platform trust store and the URL's
  host. A private CA, a client certificate (mutual TLS) and the other settings are added to the
  options before connecting:

  ```lyric
  import Std.File as File
  import Std.Tls as Tls

  func tlsPublic(): Result[ActiveMq.ActiveMqQueue, String] {
    ActiveMq.connectUrl("stomp+ssl://app:passcode@broker.example.com:61614", "orders")
  }

  func pem(path: in String): Result[String, String] {
    match File.readText(path) {
      case Ok(text) -> Ok(text)
      case Err(_) -> Err("cannot read " + path)
    }
  }

  func tlsPrivate(): Result[ActiveMq.ActiveMqQueue, String] {
    val ca = pem("/etc/app/broker-ca.pem")?
    val certificate = pem("/etc/app/client.pem")?
    val key = pem("/etc/app/client.key")?
    val base = ActiveMq.optionsFromUrl("stomp+ssl://app:passcode@10.0.0.5:61614", "orders")?
    ActiveMq.connectWith(
      base.withExclusiveCaCertificate(ca).withClientIdentity(certificate, key).withTlsServerName("broker.internal").withMinTlsVersion(
        Tls.TlsVersion.Tls13
      )
    )
  }

  func tlsByHand(caPem: in String): Result[ActiveMq.ActiveMqQueue, String] {
    val base = ActiveMq.defaultOptions("broker.internal", 61614, "/queue/orders")
    ActiveMq.connectWith(base.copy(login = "app", passcode = "passcode").withTls().withCaCertificate(caPem))
  }
  ```

  Certificates and keys are PEM text, one certificate per PEM; a private key must be an unencrypted
  PKCS8 block (`-----BEGIN PRIVATE KEY-----`; convert with `openssl pkcs8 -topk8 -nocrypt`).

Other functions of `Mq.ActiveMq`: `subscribe(queue)` (open the subscription before the first
`consume`), `publishWithHeaders(queue, message, extra)`, `serverInfo(queue)` (the broker's `server`
header, such as `ActiveMQ/5.17.6`), `destination(queue)`, `isOpen(queue)`, `unsettledCount(queue)`,
`resolveDestination(name)`, `skipsVerification(tls)` and `insecureAllowedByEnvironment()`.

## Configuration

Three ways to connect; none of them reads a credential from source. An application keeps the URL in
its own `config { }` block and passes it in, or uses `connectFromEnv`.

- `connectWith(options)`: a `ConnectOptions` record. Start from
  `defaultOptions(host, port, destination)` and adjust with `.copy(...)` and the `with...`
  functions.
- `connectUrl(url, queueName)`; `optionsFromUrl(url, queueName)` returns the `ConnectOptions`
  instead of connecting, so settings can be changed before `connectWith`.
- `connectFromEnv()`: reads the variables below.

The URL:

```
stomp://[login[:passcode]@]host[:port]
stomp+ssl://...                   the same, over TLS
```

| Part | Default | Notes |
|---|---|---|
| scheme | (required) | `stomp` or `stomp+ssl`, in any case. `ssl://`, `stomps://`, `tcp://` and every other scheme are refused. |
| `login`, `passcode` | none | Percent-decoded as UTF-8, so `@`, `:`, `/` and `%` in a passcode are written `%40`, `%3A`, `%2F`, `%25`. A login without a passcode is accepted; an empty login before `@` is refused. |
| `host` | (required) | An IPv6 address is written `[::1]` |
| `port` | 61613 for `stomp`, 61614 for `stomp+ssl` | the ports of a stock ActiveMQ |
| path, query, fragment | | Refused (one trailing `/` is tolerated). The destination is the separate `queueName` argument, and no option can be set in the URL. |

`queueName` is a queue name (`orders` becomes `/queue/orders`), or, when it starts with `/`, a
full STOMP destination taken as written (`/queue/NAME`, `/topic/NAME`). An empty name is an `Err`.

`ConnectOptions`:

| Field | Default | Meaning |
|---|---|---|
| `host`, `port` | (required) | the broker's STOMP connector |
| `destination` | (required) | a STOMP destination starting with `/`; a bare name is refused here (use `resolveDestination`) |
| `login`, `passcode` | empty | credentials; an empty login sends none |
| `virtualHost` | empty: `host` | the STOMP `host` header |
| `ackMode` | `Manual` | `Auto`: `consume` acknowledges when the handler returns `Ok` and requeues when it returns `Err`; `ack` and `nack` are then no-ops |
| `persistent` | `true` | delivery mode of messages sent with `publish` and `publishBatch` |
| `selector` | empty | JMS selector of the subscription |
| `prefetchSize` | 0 (broker default) | `activemq.prefetchSize` of the subscription; set 1 for fair sharing between slow consumers |
| `connectTimeoutMs` | 10000 | limit for the TCP connect, and again for the TLS handshake; 0 is no limit |
| `requestTimeoutMs` | 10000 | how long to wait for the broker to confirm a frame; at least 1 |
| `maxFrameBytes` | 16777216 (16 MiB) | largest message body accepted from the broker; at least 1 |
| `tls` | `plainTcp()` | `TlsOptions`, set with the functions below |

TLS options. The function names and the rule for skipping verification are those of
`Std.Http.HttpClientBuilder` (docs/61 in lyric-lang):

| Function on `ConnectOptions` | Default without it | Effect |
|---|---|---|
| `withTls()` | plain TCP | TLS with the defaults. Implied by a `stomp+ssl://` URL. |
| `withCaCertificate(pem)` | the platform trust store | trust this CA certificate in addition to the platform trust store |
| `withExclusiveCaCertificate(pem)` | the platform trust store | trust only this CA certificate; wins over `withCaCertificate` when both are set |
| `withClientIdentity(certificatePem, keyPem)` | no client certificate | present a client certificate, for a connector with `needClientAuth=true` |
| `withTlsServerName(name)` | `host` | send `name` as SNI and check the certificate against it instead of `host` |
| `withMinTlsVersion(Std.Tls.TlsVersion.Tls13)` | `Tls12`: TLS 1.2 or 1.3 | TLS 1.3 only. Nothing older than 1.2 can be configured. |
| `withInsecureSkipVerify()` | verification on | one of the two keys that switch verification off (see "Surprises") |

Environment variables. An empty variable counts as unset.

| Variable | Read by | Meaning |
|---|---|---|
| `LYRIC_CONFIG_MQ_ACTIVEMQ_URL` | `connectFromEnv` | broker URL (required) |
| `LYRIC_CONFIG_MQ_ACTIVEMQ_QUEUENAME` | `connectFromEnv` | queue name or destination (required) |
| `LYRIC_CONFIG_MQ_ACTIVEMQ_ACKMODE` | `connectFromEnv` | `Manual` (default) or `Auto`, in any case |
| `LYRIC_CONFIG_MQ_ACTIVEMQ_TLSCAFILE` | `connectFromEnv` | PEM file: a CA certificate trusted in addition to the platform trust store |
| `LYRIC_CONFIG_MQ_ACTIVEMQ_TLSCAEXCLUSIVE` | `connectFromEnv` | `true`: trust only that CA (default `false`; any other value is an `Err`, and so is `true` without a CA file) |
| `LYRIC_CONFIG_MQ_ACTIVEMQ_TLSCERTFILE` | `connectFromEnv` | PEM file: client certificate (mutual TLS) |
| `LYRIC_CONFIG_MQ_ACTIVEMQ_TLSKEYFILE` | `connectFromEnv` | PEM file: its PKCS8 private key |
| `LYRIC_CONFIG_MQ_ACTIVEMQ_TLSSERVERNAME` | `connectFromEnv` | name to check the certificate against, when it is not the URL's host |
| `LYRIC_CONFIG_MQ_ACTIVEMQ_TLSMINVERSION` | `connectFromEnv` | `1.2` (default) or `1.3` |
| `LYRIC_TLS_ALLOW_INSECURE` | every TLS connect | with the exact value `1`, and only for options that have `withInsecureSkipVerify()`, the certificate chain and the name are not checked |

The `TLS...` variables apply to a `stomp+ssl://` URL; with a `stomp://` URL a CA file, a
certificate or key file, a server name or `TLSMINVERSION=1.3` is an `Err`. The PEM files are read
on each `connectFromEnv` call. `persistent`, `selector`, `prefetchSize`, `virtualHost`, the
timeouts and `maxFrameBytes` have no variable: use `optionsFromUrl` and `.copy(...)`.

## Surprises

How the interface maps onto ActiveMQ:

| Interface | On the wire | Broker behaviour relied on |
|---|---|---|
| `publish` | `SEND` with `receipt`; returns on the `RECEIPT` | the receipt is sent after the broker has taken the message |
| `publishBatch` | `BEGIN`, one `SEND` per message, `COMMIT` with `receipt` | STOMP transactions: all of the batch or none |
| `consume` | first call sends `SUBSCRIBE` with `ack:client-individual`; then waits for a `MESSAGE` | dispatch in queue order |
| `ack(id)` | `ACK` for that delivery | individual acknowledgement |
| `nack(id, false)` | `NACK` | ActiveMQ treats a STOMP `NACK` as a poison acknowledgement |
| `nack(id, true)` | `BEGIN`; `SEND` of a copy with `lyric_delivery_count` raised by one; `ACK` of the original; `COMMIT` | STOMP transactions, so the copy and the acknowledgement are applied together or not at all |
| `close` | `UNSUBSCRIBE`, `DISCONNECT` with `receipt`, socket close | unsettled deliveries return to the queue flagged `redelivered` |

Requeue, reject and `deliveryCount`:

- **`nack(id, true)` is a transactional republish, not a broker redelivery.** ActiveMQ has no STOMP
  frame that puts one delivery back on its queue: `NACK` dead-letters, aborting a transaction that
  holds an `ACK` redelivers nothing, and the broker only redelivers when the subscription or
  connection ends, which returns every unsettled delivery at once. So the client sends a copy and
  acknowledges the original in one transaction.
- **A requeued message goes to the tail of the queue.** Messages already waiting are delivered
  first, and with several consumers the copy may go to another one. Order is not kept for a
  message that is requeued. Messages already dispatched to this connection's prefetch window also
  come first.
- **The copy is a new broker message**: a new `message-id` and timestamp. Its `Message.id`, body,
  headers, persistence, priority, expiry (the same absolute time), correlation id, reply-to, type
  and JMS properties are copied.
- **A requeue is a publish.** The connected user needs write permission on the destination it
  consumes from. On a topic, the copy goes to every subscriber.
- **`deliveryCount` is exact for requeues and a lower bound for broker redeliveries.** It is the
  `lyric_delivery_count` header (0 on the first delivery, raised by one per `nack(id, true)`), plus
  one when the broker flags the delivery `redelivered`. ActiveMQ sets that flag for deliveries a
  consumer left unsettled when it unsubscribed or disconnected, but does not expose its redelivery
  counter over STOMP, so several such rounds still add one. A requeue after a flagged delivery
  writes the raised count into the copy, where it stays.
- **A `deliveryCount` above 0 on a published message is sent and comes back.** The reference
  broker resets it to 0 on publish. A negative one is sent as 0.
- **`nack(id, false)` is final and where the message goes depends on its delivery mode.** A
  persistent message moves to the dead-letter queue (`ActiveMQ.DLQ`, shared by all queues, with the
  default strategy) and keeps its id, body and headers there. A non-persistent one is discarded
  (the broker's default `processNonPersistent=false`). The interface's "routes to the broker
  dead-letter queue (if configured)" is therefore true only for persistent messages.
- **Nothing limits the number of deliveries.** No option compares `deliveryCount` with a maximum
  (the `LYRIC_CONFIG_MQ_CONNECTION_MAXDELIVERYCOUNT` mentioned in `lyric-mq` is not read). In
  `Manual` mode the application decides, as in `settleOne` above. In `Auto` mode a handler that
  always returns `Err` receives the same message for ever.
- **`ack` and `nack` settle by `Message.id`, oldest unsettled delivery first.** Ids are not
  required to be unique; two unsettled deliveries with the same id are settled in delivery order.
- **Settling an unknown or already settled id is an `Err`**, not `Ok` as with the reference broker.
- **A failed `ack` or `nack` cannot be repeated.** The delivery leaves the client's in-flight set
  before the frame is sent. If the broker refuses the frame with an `ERROR` (the connection stays
  open), a second call with the same id is "no unsettled delivery"; the message stays unsettled on
  the broker and is redelivered when this connection closes.
- **In `Manual` mode a handler's `Err` settles nothing.** `consume` returns the handler's error
  and the delivery stays unsettled until `ack`, `nack` or `close`.
- **In `Auto` mode `ack` and `nack` are no-ops that answer `Ok`** for any non-empty id, delivered
  or not. `consume` returns the handler's error after the requeue; if the requeue itself fails the
  text is `HANDLER ERROR (and requeueing the message failed: ...)`.
- **The default acknowledgement mode is `Manual`**, where `lyric-mq` documents `Auto` as the
  default of its own configuration.

Publishing:

- **`publish` returns when the broker has confirmed the message**, so each publish is a round
  trip. `Ok` means the broker has it.
- **`publishBatch` is one transaction with one confirmation.** The messages are delivered in
  order, all or none; the reference broker publishes them one by one and can stop half way. The
  broker holds the batch until the `COMMIT`.
- **An unconfirmed publish may or may not have been published.** When no receipt arrives within
  `requestTimeoutMs`, or the connection breaks after the frame was written, the result is
  `connection lost` and the outcome is unknown. Publishing again on a new connection can produce a
  duplicate; nothing deduplicates by id.
- **An empty message id** (lyric-lang#9043):
  - `ack("")` and `nack("", ...)` are `Err("activemq: empty message id")`, in `Auto` mode too. The
    text is this package's own, not `Mq.EMPTY_MESSAGE_ID`.
  - `publish` with an empty id is a `PreconditionViolated` panic on both targets;
    `publishWithHeaders` returns the `Err` instead.
  - `publishBatch` with an empty id in the batch differs by target. The interface declares
    `requires: allHaveIds(messages)`. On `jvm` the inherited clause is resolved in this package,
    where it binds to the private helper `allHaveIds` in `src/activemq.l` (which exists for that
    reason), and the call panics with `PreconditionViolated`. On `dotnet` the inherited clause is
    not enforced, the method's own check runs, and the call returns
    `Err("activemq: empty message id")`. The wrapper `Mq.publishBatch(queue, ...)` panics on both
    targets, because its own contract is checked first. Nothing is sent in any of these cases.
  - The helper shadows `Mq.allHaveIds`, so every build of the package prints a `W0037` warning.
- **An id with a NUL character is an `Err`** (`a STOMP header contains a NUL character`), nothing
  is written and the connection stays usable. Every other character is allowed.
- **`publishWithHeaders` lets an extra header replace one of the client's own**, `destination`
  included, so it can publish to another destination than the connection's.

How a message travels, and what does not round-trip:

- **`Message.id` is the `lyric_id` header.** It is a JMS property, so a selector can match it
  (`Extras.selectorForId`). The broker's own `message-id` is not exposed for these messages.
- **`Message.headers` is one JSON array in the `lyric_headers` header**, so order, repeated keys
  and every character survive. They are not individual JMS properties and a selector cannot see
  them; use `Extras.SendOptions.properties` for that. An empty list sends no header.
- **All headers of a frame share a size limit.** This client refuses a frame from the broker whose
  header section exceeds 65536 bytes and then treats the connection as lost, and ActiveMQ has its
  own limits on header size. Large data belongs in the body.
- **`Message.body` is UTF-8 text** sent with `content-length` (so it may contain NUL) and
  `amq-msg-type:text`, which makes it a JMS `TextMessage` for other consumers. There is no binary
  body.
- **Delivery options do not come back.** Priority, expiry, correlation id, reply-to, JMS type and
  the properties set with `Extras.publishWith` reach the broker and selectors, but a consumer using
  this package does not see them: for a message with `lyric_id`, `Message.headers` is exactly what
  the publisher put there.
- **A message from another producer** (no `lyric_id`) is delivered with the broker's `message-id`
  as its id (`ID:...`) and its STOMP headers as `headers`, without the transport ones
  (`destination`, `message-id`, `ack`, `subscription`, `content-length`, `expires`, `priority`,
  `timestamp`, `redelivered`, `persistent`, `amq-msg-type`, `receipt`, `transaction`).
  `correlation-id`, `reply-to`, `type` and JMS properties are therefore among them. Requeueing such
  a message changes its id, because the copy gets a new `message-id`.
- **A message this client cannot represent is rejected, not skipped.** A body that is not UTF-8
  (a JMS `BytesMessage` from another producer), or malformed `lyric_headers` /
  `lyric_delivery_count`, is answered with `NACK`, so a persistent one moves to the dead-letter
  queue and a non-persistent one is destroyed, and `consume` returns an `Err` naming it at once
  without waiting for the next message. Do not point this client at a queue that carries binary
  messages for someone else.
- **A body larger than `maxFrameBytes` breaks the connection.** The frame is refused before it is
  read, the connection is `lost`, and the message, never settled, is redelivered to the next
  connection. Raise `maxFrameBytes` on consumers of a queue with large messages. Publishing has no
  size limit of its own.

Consuming and timeouts:

- **The subscription opens on the first `consume` (or `subscribe`) and stays open until `close`.**
  From then on the broker dispatches up to its prefetch window (normally 1000 for a queue) to this
  connection, and those messages are invisible to other consumers until this connection consumes
  or closes. Set `prefetchSize = 1` when several consumers share a queue.
- **A broken selector is reported by the first `consume`**, as a `broker error`, not by the
  connect.
- **`consume` returns `Err("timeout")` when nothing arrived**, the interface's text. The timeout
  bounds the wait for a message only: the handler, and in `Auto` mode the settlement, run after
  it.
- **`consume(0, ...)` returns only a message the broker has already dispatched to this
  connection.** It still waits about 1 ms for the socket on both targets. The first call also
  sends `SUBSCRIBE` and waits for its receipt (up to `requestTimeoutMs`), and can report `timeout`
  on a non-empty queue, because the broker dispatches after the receipt.
- **A `consume` can overrun its timeout** by the time it waits for the connection's lock (another
  thread's publish waiting for a receipt) plus one 100 ms slice.
- **A broker `ERROR` frame that arrives during a `consume` ends it** with `broker error`. The
  connection stays usable if the broker keeps it open (ActiveMQ does for most errors).
- **A topic delivers only what is published after its subscription exists.** Call
  `Mq.ActiveMq.subscribe(queue)` before the publisher starts. Unsettled topic deliveries are not
  redelivered.

Connection, heart-beats and reconnecting:

- **No heart-beats are negotiated** (`heart-beat:0,0`), over TLS as over plain TCP. The client is
  synchronous and has no thread to send them from. `TCP_NODELAY` is set.
- **A connection that dies while idle is noticed only by the next operation, and a silent death
  looks like an empty queue.** If a firewall or NAT drops the idle connection without a reset, a
  `consume` keeps returning `Err("timeout")`: nothing distinguishes it from a queue with no
  messages. A `publish` on such a connection fails after `requestTimeoutMs` with
  `connection lost: the broker did not answer within N ms`. When the broker closes the connection
  properly, the next operation reports `connection lost: the broker closed the connection` or the
  platform's write error. A consumer that must detect a dead connection has to publish, or
  reconnect, periodically.
- **No automatic reconnect.** After a transport failure, or a request the broker did not confirm
  within `requestTimeoutMs`, every operation returns `Err("activemq: connection lost: ...")` with
  the first reason; open a new connection. Deliveries that were unsettled are redelivered by the
  broker, flagged `redelivered`, to whichever consumer is next; `unsettledCount` on the lost
  connection keeps counting them.
- **A slow broker loses the connection.** A confirmation that takes longer than
  `requestTimeoutMs` (10 s by default) marks the connection lost even if the broker is healthy,
  because a late receipt could otherwise be taken for the next one.
- **`isOpen` does not probe.** It is true until `close` or until an operation has failed.
- **Connecting can take `connectTimeoutMs` twice plus `requestTimeoutMs`**: the TCP connect, the
  TLS handshake and the wait for `CONNECTED` each have their own limit. Writes are not bounded by
  any timeout on either target.
- **`close` is idempotent and waits.** It sends `DISCONNECT` and waits up to 2 s (or
  `requestTimeoutMs` if shorter) for the receipt. Deliveries not yet settled go back to the queue
  in their original order, flagged `redelivered`. `close` on a lost connection does nothing.
- **A connection is one destination.** Publishing to one queue and consuming from another takes
  two connections.
- **A queue is shared by everyone who names it.** With the reference broker each `Mq.connect` is
  a private, empty queue; here two connections to `orders` compete for the same messages, and
  messages survive the connection.

Threads:

- **An `ActiveMqQueue` can be shared between threads; operations are serialized by one lock.**
  There is one socket and no pipelining, so a `publish` waiting for its receipt (up to
  `requestTimeoutMs`) blocks every other operation on the connection, `close` included.
- **A waiting `consume` gives the lock up every 100 ms**, so a publisher on another thread is held
  for at most that long. On `jvm` the lock is fair, so the publisher gets it at that point; on
  `dotnet` it is a monitor, which makes no such promise.
- **The handler runs outside the lock** and may call `ack`, `nack`, `publish` or `consume` on the
  same connection.
- **Two threads may `consume` on one connection**; each delivery goes to one of them.

TLS:

- **Verification is on by default**: the broker's certificate must chain to a trusted root and
  carry the name (or IP address) the client connects to. A broker reached by IP address needs an
  IP entry in its certificate, or `withTlsServerName`.
- **`withCaCertificate` adds; `withExclusiveCaCertificate` replaces.** With the first, a
  certificate issued by any public CA of the platform trust store is still accepted; with the
  second only the given CA is. When both are set, the exclusive one wins and the other is ignored.
- **"Platform trust store" differs by target.** On `dotnet` it is the operating system's root
  stores (machine and user). On `jvm` it is the JDK's (`cacerts`, or what
  `javax.net.ssl.trustStore` names), not the operating system's.
- **The arguments are PEM text, not `Std.Tls.Certificate` / `Identity` values.** Those types do
  not expose their host handle outside the standard library (see "Known gaps"). The PEM is checked
  with `Std.Tls` before any connection is made, so a bad certificate or key is an `Err` without a
  dial, and the rules are `Std.Tls`'s: one certificate per PEM (no bundle), an unencrypted PKCS8
  key, and a key that belongs to the certificate.
- **Setting a TLS option on a connection that is not TLS is an `Err`**, not a silent downgrade.
  `withTlsServerName` changes neither the address dialled nor the STOMP `host` header.
- **Skipping verification takes two keys.** `withInsecureSkipVerify()` does nothing on its own:
  verification is skipped only when the environment also has `LYRIC_TLS_ALLOW_INSECURE=1` (exactly
  `1`; `true` does not count) at the time the connection is opened. With the option alone,
  verification stays on and a warning is written to standard error on every connect. The variable
  without the option does nothing. `connectFromEnv` has no variable for the option. Skipping
  covers the certificate chain and the host name together on both targets.
  `skipsVerification(options.tls)` tells what a connect would do now.
- **A missing client certificate can look like a lost connection.** A broker that requires a
  client certificate and gets none (or one from another CA) may, with TLS 1.3, only refuse after
  the client's side of the handshake has finished. The connect then fails with
  `activemq: TLS handshake with ...`, `activemq: connection lost: ...` or
  `activemq: HOST:PORT did not answer CONNECT within N ms`, depending on timing.
- **The wrong transport fails slowly or oddly.** TLS towards a plain connector fails as a
  handshake error after up to `connectTimeoutMs`. Plain STOMP towards a TLS connector fails as
  `connection lost` or `did not answer CONNECT` within `requestTimeoutMs`.
- **On `dotnet` a client key passes through a private temporary directory** (mode 700, deleted at
  once) inside `Std.TlsHost`, once when the options are checked and once when the handshake is
  prepared, because .NET's in-memory PEM pairing takes spans, which auto-FFI cannot pass.
- **The handshake limit is `connectTimeoutMs`**, for the whole handshake on `dotnet` and for each
  read of it on `jvm`.
- Revocation (CRL, OCSP) is not checked, as in `Std.Http`. A client certificate is harmless on a
  connector that asks for none.
- Everything above the handshake (frames, receipts, transactions, consume timeouts, error mapping,
  the lock) is the same code for both transports.

Differences between `dotnet` and `jvm`, collected:

| | `dotnet` | `jvm` |
|---|---|---|
| `publishBatch` with an empty id | `Err("activemq: empty message id")` | `PreconditionViolated` panic (lyric-lang#9043) |
| Platform trust store | operating system root stores | JDK trust store |
| Connection lock | monitor, not fair | fair `ReentrantLock` |
| TLS handshake limit | whole handshake | each read |
| Skip-verify | chain policy with `X509VerificationFlags.AllFlags` | a trust manager that accepts everything, and no endpoint identification |
| Reads | one pending `Stream.ReadAsync`, waited on with a limit | `SO_TIMEOUT` on the socket |
| Platform part of error texts | .NET exception messages | JDK exception messages |

Differences from the reference in-process broker (`Mq.connect`), collected:

| | Reference broker | This backend |
|---|---|---|
| Queue identity | each connect is a private queue | the named queue on the broker, shared and durable |
| `ack`/`nack` of an unknown id | `Ok` | `Err` |
| Two unsettled deliveries with one id | the second replaces the first | both tracked, settled oldest first |
| `nack(id, false)` | dropped | dead-lettered if persistent, discarded if not |
| `publishBatch` | one by one, can stop half way | one transaction |
| Published `deliveryCount` | reset to 0 | kept |
| Acknowledgement mode | none: `ack`/`nack` always act | `Manual` or `Auto` |
| Empty id on `ack`/`nack` | `Err(Mq.EMPTY_MESSAGE_ID)` | `Err("activemq: empty message id")` |
| Failure modes | none | connection loss, request timeouts, broker errors |

Things the tests assert that look odd, and why:

- `tests/client_tests.l` checks the empty-id `Err` only through `publishWithHeaders`, because
  `publish` panics before it can return one.
- The mutual-TLS tests accept any `activemq: ...` error for a missing client certificate, for the
  TLS 1.3 reason above.
- `"an ERROR frame alone does not close the connection"` holds against the fake broker and against
  ActiveMQ for a refused `SEND` or a second `ACK`; a refused `CONNECT` does close it.
- A consumed message's `headers` are empty after `Extras.publishWith` with properties ("delivery
  options are not message headers").
- The integration suite expects `JMSDeliveryMode = 'NON_PERSISTENT'`, a string: that is how
  ActiveMQ's selectors see the delivery mode.

`DeadLetterStore` is not implemented. The interface is an append/list/count store with a capacity;
ActiveMQ's DLQ is a queue that can only be consumed, so it does not map. Read dead letters by
connecting to `ActiveMQ.DLQ` (below), or use `Mq.inProcessDeadLetterStore`.

## Backend-specific module

`Mq.ActiveMq.Extras` holds what ActiveMQ offers beyond the interface, kept apart so that code
written against `Mq.MessageQueue` stays portable. The examples continue the file of "Usage"
(`import Mq`, `import Mq.ActiveMq as ActiveMq`).

```lyric
import Mq.ActiveMq.Extras as Extras
import Mq.ActiveMq.Stomp as Stomp

func extras(queue: in ActiveMq.ActiveMqQueue): Result[Unit, String] {
  val options = Extras.defaultSendOptions()
  Extras.publishWith(
    queue,
    Mq.Message(id = "inv-7", body = "{}", headers = [], deliveryCount = 0),
    options.copy(
      persistent = false,
      priority = 7,
      timeToLiveMs = 60000i64,
      correlationId = "req-19",
      replyTo = Extras.queueDestination("replies"),
      jmsType = "invoice",
      properties = [Extras.Property(name = "region", value = "eu")]
    )
  )
}

func selected(url: in String): Result[ActiveMq.ActiveMqQueue, String] {
  val base = ActiveMq.optionsFromUrl(url, "orders")?
  ActiveMq.connectWith(base.copy(selector = Extras.selectorEquals("region", "eu")))
}

func topicSubscriber(url: in String): Result[ActiveMq.ActiveMqQueue, String] {
  val prices = ActiveMq.connectUrl(url, Extras.topicDestination("prices"))?
  ActiveMq.subscribe(prices)?
  Ok(prices)
}

func deadLetters(url: in String, id: in String): Result[ActiveMq.ActiveMqQueue, String] {
  val base = ActiveMq.optionsFromUrl(url, "ActiveMQ.DLQ")?
  ActiveMq.connectWith(base.copy(selector = Extras.selectorForId(id)))
}

func rawHeaders(queue: in ActiveMq.ActiveMqQueue): Result[Unit, String] {
  ActiveMq.publishWithHeaders(
    queue,
    Mq.Message(id = "g-1", body = "x", headers = [], deliveryCount = 0),
    [Stomp.Header(name = "JMSXGroupID", value = "customer-9")]
  )
}
```

| Function | |
|---|---|
| `queueDestination(name)`, `topicDestination(name)` | `/queue/NAME`, `/topic/NAME`. An empty name is a contract violation. |
| `defaultSendOptions()` | persistent, broker default priority, no expiry, nothing else set |
| `publishWith(queue, message, options)` | `publish` with `SendOptions`; returns once the broker has confirmed |
| `isPropertyName(name)` | whether `name` may be a property in `SendOptions.properties` |
| `selectorEquals(name, value)`, `selectorForId(id)` | a selector `name = 'value'` with the value quoted; by message id |

`SendOptions`:

| Field | Default | Meaning |
|---|---|---|
| `persistent` | `true` | delivery mode of this message |
| `priority` | -1 | 0 to 9; -1 leaves the broker's default of 4. Anything else is an `Err`. |
| `timeToLiveMs` | 0 | milliseconds after which the broker discards an undelivered message; 0 is never; negative is an `Err` |
| `correlationId`, `replyTo`, `jmsType` | empty | the JMS correlation id, a STOMP destination for replies, the JMS type; empty is not set |
| `properties` | none | JMS properties, usable in selectors |

Things to know:

- **`SendOptions.persistent` replaces `ConnectOptions.persistent`.** `publishWith` always sends its
  own value, and `defaultSendOptions()` says `true`, so on a connection opened with
  `persistent = false` a `publishWith` with default options sends a persistent message.
- **`timeToLiveMs` becomes an absolute expiry on the client's clock** (`expires` is epoch
  milliseconds), so a clock difference between the application and the broker shifts it.
- **Property names are restricted**: an ASCII identifier (letters, digits, `_`, `$`, not starting
  with a digit) that does not start with `JMS` or `lyric_` and is not a selector keyword (`NULL`,
  `TRUE`, `FALSE`, `NOT`, `AND`, `OR`, `BETWEEN`, `LIKE`, `IN`, `IS`, `ESCAPE`, in any case). A
  name outside that, or one set twice, is an `Err` and nothing is sent. `publishWithHeaders` does
  not check.
- **Property values are strings.** Compare them with quoted literals in a selector.
- **Selectors** are SQL-92 conditions over properties and JMS headers (`JMSPriority`, `JMSType`,
  `JMSCorrelationID`, `JMSDeliveryMode`). `selectorEquals` quotes the value but does not check the
  name. A message the selector does not match stays on the queue for other consumers.
- **Message priority only changes delivery order** when the broker's destination policy enables
  `prioritizedMessages`.
- **Durable topic subscriptions are not available**; a topic subscriber that is not connected
  misses the message.

The other packages:

| File | Package | Content |
|---|---|---|
| `src/activemq.l` | `Mq.ActiveMq` | connection, interface implementation |
| `src/extras.l` | `Mq.ActiveMq.Extras` | topics, delivery options, selectors |
| `src/stomp.l` | `Mq.ActiveMq.Stomp` | STOMP 1.2 frame encoder and incremental decoder, no I/O |
| `src/url.l` | `Mq.ActiveMq.Url` | broker URL parsing (`parseUrl`, `percentDecode`) |
| `src/_kernel_dotnet/socket_kernel.l` | `Mq.ActiveMq.Kernel` | `System.Net.Sockets`, `System.Net.Security.SslStream`, monitor lock, clock, task |
| `src/_kernel_jvm/socket_kernel.l` | `Mq.ActiveMq.Kernel` | `java.net.Socket`, `javax.net.ssl.SSLSocket`, `ReentrantLock`, clock, thread |

The two kernel files declare the same package and surface; a file-level `@cfg(target = ...)` keeps
one per build. They are the only files with extern declarations. The kernel also has a listener,
the server side of a TLS handshake and a background-thread primitive, used by the in-process
broker of the hermetic tests. `Mq.ActiveMq.Kernel` and the fields of `ActiveMqQueue` are public
for those tests; they are not an application API.

How the kernels do TLS:

- dotnet: `SslStream.AuthenticateAsClient` with `SslClientAuthenticationOptions`. Trust is a
  `CertificateChainPolicy` (custom root store; the platform's root stores are added to it for
  `withCaCertificate`), and skip-verify is that policy with `X509VerificationFlags.AllFlags`. No
  validation callback is involved. A read is a `Stream.ReadAsync` task the connection keeps and
  waits on with a limit, so a consume timeout is exact whether or not TLS has buffered data; the
  plain transport uses the same path.
- jvm: an `SSLContext` from a `KeyStore`-backed `TrustManagerFactory` and `KeyManagerFactory`,
  an `SSLSocket` layered over the connected socket, and
  `SSLParameters.setEndpointIdentificationAlgorithm("HTTPS")`. Reads and timeouts are the socket's
  own (`setSoTimeout`), as for plain TCP.
- Both load certificates and keys through `Std.TlsHost`, the standard library's PEM loader.

## Tests

Run from the repository root.

Hermetic (no broker needed), on each target:

```sh
lyric test --manifest mq-activemq/lyric.toml --target dotnet
lyric test --manifest mq-activemq/lyric.toml --target jvm
```

This runs the frame codec, URL, kernel, re-export and `Extras` unit tests, and
`tests/client_tests.l`: the `Testing.Conformance.Mq` queue suite and the error paths (refused
connection, bad credentials, `ERROR` frame, connection dropped during publish and during consume,
unconfirmed request) against an in-process STOMP broker on a loopback socket. The same broker is
also started behind TLS and behind TLS with a required client certificate: the conformance suite
runs once more over TLS, with tests of CA trust (exclusive and additional), an untrusted CA, the
name check and the server name override, the two keys of skip-verify, mutual TLS with and without
a client certificate, TLS 1.3 only, a transport that does not match the connector, option
validation and the TLS variables of `connectFromEnv`. The certificates and keys for this are
constants at the end of `tests/client_tests.l`, named `TEST_ONLY_...`: generated for the tests,
valid until 2126, and worthless as secrets. The run reports the integration module as gated
(`0 test(s), module gated by inactive @cfg`). It sets and clears `LYRIC_TLS_ALLOW_INSECURE` and
the `LYRIC_CONFIG_MQ_ACTIVEMQ_*` variables itself, writes three PEM files named
`lyric-mq-activemq-test-only-*` into `$TMPDIR` (or `/tmp`), and prints the skip-verify warning
twice; that is expected.

Integration (real broker). The suite is gated by the `integration` feature; every test fails with
a message naming its variable when that is unset. The broker must enforce authentication and use
the default dead-letter strategy. Each test uses its own destination, named with a fresh UUID.

| Variable | Meaning |
|---|---|
| `LYRIC_ACTIVEMQ_URL` | `stomp://USER:PASSCODE@HOST:PORT`, a plain connector |
| `LYRIC_ACTIVEMQ_TLS_URL` | `stomp+ssl://USER:PASSCODE@HOST:PORT`, a TLS connector that asks for no client certificate |
| `LYRIC_ACTIVEMQ_MTLS_URL` | the same for a connector with `needClientAuth=true` |
| `LYRIC_ACTIVEMQ_TLS_CA` | PEM file: the CA of the broker's certificate. The certificate must carry the URLs' host and `localhost`. |
| `LYRIC_ACTIVEMQ_TLS_CERT` | PEM file: a client certificate the broker trusts |
| `LYRIC_ACTIVEMQ_TLS_KEY` | PEM file: its PKCS8 private key |

```sh
lyric test --manifest mq-activemq/lyric.toml --target dotnet --features integration
lyric test --manifest mq-activemq/lyric.toml --target jvm --features integration
```

Over the plain connector it runs the conformance suite and tests of redelivery, the dead-letter
queue, batches, `Auto` mode, large bodies, foreign and binary messages, selectors, delivery
options, expiry, topics and competing consumers. Over TLS it runs the conformance suite again and
tests of a verified connection with the custom CA, refusal without it, refusal of a name the
certificate lacks, skip-verify with one key and with both, mutual TLS with a client certificate
and without, and consume timeouts, receipts, transactions, large bodies, redelivery and `ERROR`
frames.

Starting a broker without a container: `scripts/ci/start-activemq.sh` (at the repository root)
installs the Ubuntu package (`activemq` 5.17.6) with `sudo apt-get` when `/usr/share/activemq` is
missing, generates a CA, a server certificate (SAN `localhost`, `127.0.0.1`), a client certificate
and the broker's PKCS12 stores into a fresh temporary directory with `openssl` and `keytool` (both
must be installed, with a JDK to run the broker), and starts one in-memory broker from
`tests/integration/broker/activemq.xml` with a random passcode for the user `lyric` and three
connectors on 127.0.0.1: 61613 (plain), 61614 (TLS) and 61615 (TLS, client certificate required).
The ports are fixed. It waits up to 60 s for them and prints the broker's log on failure. The
broker keeps running after the script returns and there is no stop script: end the `java`
process.

Under GitHub Actions the script appends the six variables to `$GITHUB_ENV` and masks the passcode.
Elsewhere it prints the plain URL alone on the first line of standard output, without a variable
name, and then an `export NAME=value` line for each of the other five:

```sh
bash scripts/ci/start-activemq.sh > broker.env
export LYRIC_ACTIVEMQ_URL="$(head -n 1 broker.env)"
eval "$(tail -n +2 broker.env)"
```

`eval` of the whole output would fail on the first line.

## Pinned versions

There is no third-party library (`[nuget]` and `[maven]` are empty). What the suites run against:

| | |
|---|---|
| Interface | `Lyric.Mq` 0.1.0 and `Lyric.Testing` from the lyric-lang commit in `.lyric-lang-rev` |
| ActiveMQ (CI and development) | ActiveMQ Classic 5.17.6, the `activemq` package of the `ubuntu-latest` runner, started by `scripts/ci/start-activemq.sh` |
| STOMP | 1.2 only; a broker that answers with another version is refused |
| Runtimes | .NET 10; Temurin 22 in CI, OpenJDK 21 in development |

## Known gaps

- No heart-beats and no automatic reconnect, on either transport (see "Surprises").
- TLS: one CA certificate per option, not a bundle; no PKCS12 or encrypted keys; no revocation
  checks; no client certificate chain (intermediates between the client certificate and the
  broker's CA are not sent).
- On dotnet, skip-verify relies on `SslStream` honouring `X509VerificationFlags.IgnoreInvalidName`
  from the chain policy for the name check. That is what .NET 10 does (the tests cover it); a
  runtime that does not would refuse the connection rather than accept it.
- The kernels import `Std.TlsHost`, the standard library's kernel package for PEM loading. It is
  public but not marked `@stable`.
- `DeadLetterStore` is not implemented (see "Surprises").
- No binary message bodies, in either direction, and no access to the delivery options of a
  received message.
- A failed `ack` or `nack` cannot be retried on the same connection (see "Surprises").
- Durable topic subscriptions (`activemq.subscriptionName` with a `client-id`) are not exposed.
- Only ActiveMQ Classic 5.17.6 has been tested. The official `apache/activemq-classic` container
  image has not been run against this suite.
- Compiler and standard library issues found while writing this package, tracked in
  `nichobbs/lyric-lang`:
  - lyric-lang#9043: on `--target jvm` the interface's inherited `requires: allHaveIds(messages)`
    clause is resolved in the implementing package (`J008` when the name is not declared there), and
    on `--target dotnet` the inherited clause is not enforced at all. `src/activemq.l` has a private
    `allHaveIds` helper that it uses to reject empty ids; its name is what lets the JVM build resolve
    the inherited clause, and it causes a `W0037` shadowing warning on every build. Rename it once
    the compiler is fixed. The same issue covers `Mq.EMPTY_MESSAGE_ID` and `Mq.allHaveIds` not being
    usable from this package (`T0020`, `T0133`), which is why the backend returns its own text for an
    empty message id.
  - lyric-lang#9044: `for x in [a.name, a.value]` over `String` fields loses the element type, and
    `Bug.typeName` is the simple class name on the JVM.
  - lyric-lang#9042: `Std.TcpHost` has no JVM kernel and cannot tell a read timeout from a failure,
    so both socket kernels bind the host socket API directly.
  - lyric-lang#9064: the standard library has no TLS client for a raw socket, so each kernel binds
    the platform's TLS (`SslStream`, JSSE) itself and the TLS options here repeat those of
    `Std.Http`.
- Standard library and compiler limits met while adding TLS (not yet filed):
  - `Std.Tls.Certificate.rawHandle` and `Identity.rawHandle` are `internal`, so a package outside
    the standard library cannot hand a `Certificate` or `Identity` to a host TLS API. This is why
    the options take PEM text and the kernels load it again through `Std.TlsHost`.
  - A Lyric lambda cannot become a named .NET delegate (`RemoteCertificateValidationCallback`;
    docs/66 gap 2), so the dotnet kernel configures trust through `CertificateChainPolicy` only.
  - The JVM auto-FFI cannot pass `null` (lyric-lang#4775) or a typed reference array
    (`TrustManager[]`, `Certificate[]`, `String[]`) as an argument, and a record cannot implement
    `X509TrustManager`. The JVM kernel makes those calls through `java.lang.reflect`, as the
    standard library's `_kernel_jvm/http_host.l` does.
  - `@externTarget("System.Threading.Tasks.Task`1[System.Int32].get_Result")` on an `extern type`
    for the closed generic emits a member reference the runtime rejects ("The signature is
    incorrect"); the built-in `Task[Int]` with `Task`1.get_Result` works and is what the kernel
    uses.
  - A `[project.tests]` module cannot import a non-test package from `tests/` (`T0215`), so the
    test certificates sit in `tests/client_tests.l` itself.
