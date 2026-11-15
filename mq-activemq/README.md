# lyric-mq-activemq

Apache ActiveMQ backend for the `Mq.MessageQueue` and `Mq.QueueConsumer` interfaces of `lyric-mq`,
for `dotnet` and `jvm`, `experimental` (0.1.0). The client is a STOMP 1.2 implementation written in
Lyric; only the TCP socket and the TLS layer over it come from the host platform, so there is no
JMS, NMS or other SDK dependency. One `ActiveMqQueue` is one connection to the broker and one
destination, and implements both interfaces. The connection sends and expects STOMP heart-beats
and is re-established when it is lost; both can be tuned or switched off.

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
| `activemq: broker error: ...` | the broker refused this request: an `ERROR` frame carrying the request's `receipt-id` (or the answer to `CONNECT`); the text is its `message` header. The connection stays usable, except after `CONNECT`. |
| `activemq: connection lost: REASON` | returned by the operation that met the loss: the transport failed, a request was not confirmed or a write not taken within `requestTimeoutMs`, the broker sent nothing for twice the agreed heart-beat interval, it sent an `ERROR` frame that answers no request in flight, or it ended the connection during login. With reconnecting on, a new connection is opened in the background; with it off, every later operation returns the same text. |
| `...; the outcome is unknown: ...` | appended to `connection lost` for a `publish`, `publishBatch` (`COMMIT`), `ack` or `nack` that had been written when the connection was lost: the broker may or may not have applied it, and the client does not send it again. `isOutcomeUnknownError(e)` |
| `...; the transaction was not committed, so nothing of it was applied` | appended to `connection lost` for a `publishBatch` or `nack(id, true)` that lost the connection before its `COMMIT` was written |
| `activemq: not connected, reconnecting (nothing was sent); connection lost: REASON` | an operation called while the connection is being re-established. Nothing was written; the same call can succeed later. `isNotConnectedError(e)` |
| `activemq: connection lost: REASON; reconnecting was given up ...` | reconnecting ended, after `maxAttempts` failed attempts or because the broker refused the new `CONNECT` or `SUBSCRIBE`; the text carries the last failure. Final: open a new connection. |
| `activemq: stale delivery: message 'ID' was delivered on a connection that has since been lost ...` | `ack`/`nack` of a delivery received before a reconnect: its ack id died with the connection and the broker redelivers the message. `isStaleDeliveryError(e)` |
| `activemq: message 'ID' has the negative deliveryCount N` | `publish`, `publishBatch`, `publishWithHeaders`: nothing was sent |
| `activemq: the connection is closed` | any operation after `close` |
| `activemq: cannot connect to HOST:PORT: ...` | the TCP connection could not be opened |
| `activemq: TLS handshake with HOST:PORT failed: ...` | the TLS handshake failed or timed out |
| `activemq: message ID cannot be delivered: ...` | `consume` received a message it cannot hand to a handler; the text gives the reason, what was done with the message (`onUndeliverable`) and the broker id to settle it by |
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

- **Outages.** Reconnecting is on by default, so a consumer loop needs nothing: `consume` waits for
  the new connection within its timeout. A publisher decides what an error means with the three
  predicates; only "not connected" is safe to repeat blindly. Heart-beats and the backoff are
  options:

  ```lyric
  func publishOrKeep(queue: in ActiveMq.ActiveMqQueue, m: in Mq.Message): Result[Bool, String] {
    match queue.publish(m) {
      case Ok(_) -> Ok(true)
      case Err(e) -> if ActiveMq.isNotConnectedError(e) {
        // Nothing was sent: keep the message and publish it later.
        Ok(false)
      } else {
        // `isOutcomeUnknownError(e)`: the broker may have it. Publishing it
        // again can make a duplicate; that is the application's decision.
        Err(e)
      }
    }
  }

  func tuned(url: in String): Result[ActiveMq.ActiveMqQueue, String] {
    val base = ActiveMq.optionsFromUrl(url, "orders")?
    val reconnect = ActiveMq.defaultReconnect()
    ActiveMq.connectWith(
      base.copy(
        heartBeatSendMs = 5000,
        heartBeatReceiveMs = 5000,
        reconnect = reconnect.copy(initialDelayMs = 500, maxDelayMs = 10000, maxAttempts = 20)
      )
    )
  }

  func withoutReconnect(url: in String): Result[ActiveMq.ActiveMqQueue, String] {
    val base = ActiveMq.optionsFromUrl(url, "orders")?
    ActiveMq.connectWith(base.copy(heartBeatSendMs = 0, heartBeatReceiveMs = 0, reconnect = ActiveMq.noReconnect()))
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
    consumer.consume(0, { m -> handle(m) })
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
`status(queue)` (a `ConnectionStatus`: `connected`, `reconnecting`, `lostBecause`, `failedAttempts`,
`lastAttemptError`, `reconnects`, and the agreed `heartBeatSendMs` / `heartBeatReceiveMs`),
`hasBackgroundThread(queue)`, `isNotConnectedError(e)`, `isOutcomeUnknownError(e)`,
`isStaleDeliveryError(e)`, `defaultReconnect()`, `noReconnect()`, `resolveDestination(name)`,
`skipsVerification(tls)` and `insecureAllowedByEnvironment()`.

All of the examples above are compiled by `tests/readme_tests.l`, which also runs the ones that
connect as far as a refused connection.

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
| `host` | (required) | An IPv6 address is written in brackets, `[::1]` or `[::1]:61613`. Without brackets it is refused (`stomp://::1` is not host `::`, port 1). |
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
| `ackMode` | `Manual` | `Auto`: `consume` acknowledges when the handler returns `Ok` and requeues when it returns `Err`; `ack` and `nack` are then no-ops, except for a delivery `consume` could not settle |
| `persistent` | `true` | delivery mode of messages sent with `publish` and `publishBatch` |
| `selector` | empty | JMS selector of the subscription |
| `prefetchSize` | 0 (broker default) | `activemq.prefetchSize` of the subscription; set 1 for fair sharing between slow consumers |
| `connectTimeoutMs` | 10000 | limit for the TCP connect, and again for the TLS handshake; 0 is no limit |
| `requestTimeoutMs` | 10000 | how long to wait for the broker to confirm a frame, and how long a write may make no progress because the broker is not reading; at least 1 |
| `maxFrameBytes` | 16777216 (16 MiB) | largest message body accepted from the broker, in bytes, inclusive; at least 1. A larger message is undeliverable (next row); the connection stays usable. |
| `onUndeliverable` | `UndeliverablePolicy.LeaveUnacknowledged` | what `consume` does with a message it cannot hand to a handler: `LeaveUnacknowledged` leaves it on the broker, `RejectToBroker` sends `NACK` (dead-letter queue if persistent, discarded if not). Either way `consume` returns an `Err`. See "Surprises". |
| `tls` | `plainTcp()` | `TlsOptions` (`Net.Tls`, re-exported by `Mq.ActiveMq`), set with the functions below |
| `heartBeatSendMs` | 10000 | the heart-beat the client offers to send, in milliseconds (the first number of the STOMP `heart-beat` header); 0 = none. The agreed interval is the larger of this and what the broker asks for. |
| `heartBeatReceiveMs` | 10000 | the heart-beat the client asks the broker for (the second number); 0 = none. The agreed interval is the larger of this and what the broker says it can do. The connection counts as dead after twice the agreed interval without a byte. |
| `reconnect` | `defaultReconnect()` | `ReconnectOptions`, below; `noReconnect()` switches reconnecting off |

`ReconnectOptions` (start from `defaultReconnect()` and `.copy(...)`):

| Field | Default | Meaning |
|---|---|---|
| `enabled` | `true` | re-establish a lost connection in the background. `false`: a lost connection stays lost. |
| `initialDelayMs` | 1000 | wait before the first attempt; doubled after every failed attempt. At least 1. |
| `maxDelayMs` | 30000 | the longest wait between two attempts; not less than `initialDelayMs` |
| `maxAttempts` | 0 (no limit) | failed attempts in a row after which reconnecting is given up |
| `jitterPercent` | 20 | each wait is shortened by a random part of up to this many percent of it (0 to 100) |
| `publishWaitMs` | 0 (fail at once) | how long `publish`, `publishBatch` and `publishWithHeaders` wait for a connection that is being re-established before they return the "not connected" error |

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
| `LYRIC_CONFIG_MQ_ACTIVEMQ_ONUNDELIVERABLE` | `connectFromEnv` | `Leave` (default) or `Reject`, in any case: the `onUndeliverable` option |
| `LYRIC_CONFIG_MQ_ACTIVEMQ_HEARTBEATSENDMS` | `connectFromEnv` | `heartBeatSendMs` (default 10000; 0 = none) |
| `LYRIC_CONFIG_MQ_ACTIVEMQ_HEARTBEATRECEIVEMS` | `connectFromEnv` | `heartBeatReceiveMs` (default 10000; 0 = none) |
| `LYRIC_CONFIG_MQ_ACTIVEMQ_RECONNECT` | `connectFromEnv` | `true` (default) or `false`, in any case: `reconnect.enabled` |
| `LYRIC_CONFIG_MQ_ACTIVEMQ_RECONNECTINITIALDELAYMS` | `connectFromEnv` | `reconnect.initialDelayMs` (default 1000) |
| `LYRIC_CONFIG_MQ_ACTIVEMQ_RECONNECTMAXDELAYMS` | `connectFromEnv` | `reconnect.maxDelayMs` (default 30000, or the initial delay when that is larger) |
| `LYRIC_CONFIG_MQ_ACTIVEMQ_RECONNECTMAXATTEMPTS` | `connectFromEnv` | `reconnect.maxAttempts` (default 0 = no limit) |
| `LYRIC_CONFIG_MQ_ACTIVEMQ_TLSCAFILE` | `connectFromEnv` | PEM file: a CA certificate trusted in addition to the platform trust store |
| `LYRIC_CONFIG_MQ_ACTIVEMQ_TLSCAEXCLUSIVE` | `connectFromEnv` | `true`: trust only that CA (default `false`; any other value is an `Err`, and so is `true` without a CA file) |
| `LYRIC_CONFIG_MQ_ACTIVEMQ_TLSCERTFILE` | `connectFromEnv` | PEM file: client certificate (mutual TLS) |
| `LYRIC_CONFIG_MQ_ACTIVEMQ_TLSKEYFILE` | `connectFromEnv` | PEM file: its PKCS8 private key |
| `LYRIC_CONFIG_MQ_ACTIVEMQ_TLSSERVERNAME` | `connectFromEnv` | name to check the certificate against, when it is not the URL's host |
| `LYRIC_CONFIG_MQ_ACTIVEMQ_TLSMINVERSION` | `connectFromEnv` | `1.2` (default) or `1.3` |
| `LYRIC_TLS_ALLOW_INSECURE` | every TLS connect | with the exact value `1`, and only for options that have `withInsecureSkipVerify()`, the certificate chain and the name are not checked |

The `TLS...` variables apply to a `stomp+ssl://` URL; with a `stomp://` URL a CA file, a
certificate or key file, a server name or `TLSMINVERSION=1.3` is an `Err`. The PEM files are read
on each `connectFromEnv` call. The numeric variables take whole numbers that are not negative;
anything else is an `Err` naming the variable. `persistent`, `selector`, `prefetchSize`,
`virtualHost`, the timeouts, `maxFrameBytes`, `reconnect.jitterPercent` and
`reconnect.publishWaitMs` have no variable: use `optionsFromUrl` and `.copy(...)`.

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
  broker resets it to 0 on publish. A negative one is an `Err` and nothing is sent (for a batch,
  none of it), because a consumer refuses a negative `lyric_delivery_count`.
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
- **A refused `ack` or `nack` can be repeated.** A delivery leaves the client's in-flight set only
  when the broker has confirmed the settlement (the `RECEIPT` of the `ACK`, `NACK` or, for a
  requeue, the `COMMIT`). If the broker refuses it with an `ERROR` and the connection stays open,
  the call returns `broker error`, the delivery is still unsettled here and on the broker, and
  `ack` / `nack` with the same id tries again. A refused requeue leaves no copy: its transaction
  is aborted. If the connection is lost instead, the deliveries it carried cannot be settled any
  more and the broker redelivers them (see "Connection, heart-beats and reconnecting").
- **In `Manual` mode a handler's `Err` settles nothing.** `consume` returns the handler's error
  and the delivery stays unsettled until `ack`, `nack` or `close`.
- **In `Auto` mode `ack` and `nack` are no-ops that answer `Ok`** for any non-empty id, delivered
  or not, with one exception: a delivery `consume` did not settle. That is a delivery whose
  automatic acknowledgement or requeue the broker refused (`consume` then returns the broker's
  error, or `HANDLER ERROR (and requeueing the message failed: ...)`), and an undeliverable
  message left on the broker. For those, `ack` and `nack` act as in `Manual` mode, so the
  delivery can still be settled; `unsettledCount` counts them.
- **The default acknowledgement mode is `Manual`**, where `lyric-mq` documents `Auto` as the
  default of its own configuration.

Publishing:

- **`publish` returns when the broker has confirmed the message**, so each publish is a round
  trip. `Ok` means the broker has it.
- **`publishBatch` is one transaction with one confirmation.** The messages are delivered in
  order, all or none; the reference broker publishes them one by one and can stop half way. The
  broker holds the batch until the `COMMIT`.
- **An unconfirmed publish may or may not have been published, and is never sent again.** When no
  receipt arrives within `requestTimeoutMs`, or the connection breaks after the frame was written,
  the result is `connection lost: ...; the outcome is unknown: ...` (`isOutcomeUnknownError`). The
  client does not repeat it on the next connection, because that could publish it twice, and does
  not report success, because the broker may never have seen it. Publishing it again is the
  application's decision; nothing deduplicates by id.
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
- **All headers of a frame share a size limit.** A message whose header section (the command
  line, every header line and their line ends) exceeds 65536 bytes is undeliverable (below), and
  ActiveMQ has its own limits on header size. Large data belongs in the body.
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
- **Four kinds of message cannot be handed to a handler** ("undeliverable"): a body that is not
  UTF-8 (a JMS `BytesMessage` from another producer); malformed `lyric_headers` or
  `lyric_delivery_count`; a body of more than `maxFrameBytes` bytes; a header section of more
  than 65536 bytes. `consume` returns
  `Err("activemq: message BROKER-ID cannot be delivered: REASON; OUTCOME")` at once, without
  waiting for the next message, and the connection stays usable: the next `consume` delivers the
  next message. What happens to the message is `ConnectOptions.onUndeliverable`:
  - `LeaveUnacknowledged` (the default) leaves it unacknowledged on the broker. Nothing is
    destroyed: the broker redelivers it, flagged `redelivered`, when this connection closes, to
    whichever consumer is next (one with a larger `maxFrameBytes`, or the JMS consumer the binary
    message was meant for). Until then the delivery is tracked under the broker's `message-id`,
    which the error names: `ack(brokerId)` removes the message, `nack(brokerId, false)` rejects it
    to the dead-letter handling, `nack(brokerId, true)` requeues it as it is (not possible for a
    message that was too large to read: there is no body to copy, and the call is an `Err`).
    `unsettledCount` counts it.
  - `RejectToBroker` sends `NACK`, so a persistent message moves to the dead-letter queue and a
    non-persistent one is destroyed. Use it on a queue this application owns. If the `NACK` is
    refused, the message is left as with the other policy and the error says so.
- **A message left unacknowledged takes a place in the prefetch window, on every connection that
  meets it.** With `prefetchSize = 1` a connection that has left one undeliverable message
  receives nothing more (its `consume` reports `timeout`) until the message is settled or the
  connection closes, and the next connection meets the same message first. With the default
  window the effect is one `Err` per connection. An application that sees the error repeat should
  settle the message by its id, raise `maxFrameBytes`, or choose `RejectToBroker`.
- **A message over a limit is read past, not kept.** The client reads the oversize frame to its
  end and drops the bytes as they arrive, so memory stays bounded (at most 64 KiB of headers and
  one read of 16 KiB are held), but the bytes still cross the network: the wait for the next
  message includes the time to receive the oversize one. Of an oversize header section the client
  keeps the headers within the first 64 KiB and, from the rest, `ack`, `message-id`,
  `subscription`, `destination` and `content-length` when their line is at most 1024 bytes; a
  message whose `ack` header cannot be found that way loses the connection. Raise `maxFrameBytes`
  on consumers of a queue with large messages. `maxFrameBytes` counts body bytes and is
  inclusive; headers do not count towards it. Publishing has no size limit of its own.
- **A frame other than `MESSAGE` over a limit, or a command line over 64 KiB, loses the
  connection.** The broker has no reason to send one.

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
- **`timeoutMs` is an upper bound on the wait for a message, counted from the call.** Everything
  that precedes the hand-over to the handler counts against it: waiting for the connection's lock
  (another thread's `publish` waiting for its receipt holds it), sending `SUBSCRIBE` and waiting
  for its receipt on the first call, and waiting for a `MESSAGE`. When the time is up, `consume`
  returns `Err("timeout")`, whichever of these it was waiting for. What is not bounded: the
  handler, the settlement in `Auto` mode, the `NACK` of an undeliverable message under
  `RejectToBroker` (each up to `requestTimeoutMs`), and one last poll of the socket of about 1 ms.
- **`consume(0, ...)` returns only a message the broker has already dispatched to this
  connection**, after a poll of about 1 ms on both targets. The first `consume(0)` sends
  `SUBSCRIBE` and does not wait for the receipt, so it reports `timeout` even on a non-empty
  queue; later calls pick the receipt up. Call `subscribe(queue)` first, or use a timeout, when
  the first poll matters.
- **A subscription that an expired `consume` left unconfirmed is completed by later calls.** Its
  receipt has `requestTimeoutMs` from the moment `SUBSCRIBE` was sent, whichever operation reads
  it; when that passes without one, the next `consume` reports the connection lost. A refusal
  of the `SUBSCRIBE` (a broken selector) that arrives while a `publish` is waiting is reported by
  the next `consume`. `subscribe(queue)` waits the whole `requestTimeoutMs`.
- **An `ERROR` frame is a request's failure only when it carries that request's `receipt-id`.**
  ActiveMQ puts the `receipt-id` on the `ERROR` for a frame sent with a `receipt` (checked against
  5.17.6 for a refused `SUBSCRIBE`), which is how a refused `SEND`, `SUBSCRIBE`, `ACK`, `NACK` or
  `COMMIT` becomes `broker error` with the connection still usable. An `ERROR` without a `receipt-id`, or with another one, answers a frame
  that was sent without a receipt (`BEGIN`, a `SEND` or `ACK` inside a transaction, `ABORT`,
  `UNSUBSCRIBE`) or nothing this client sent. It cannot be attributed, STOMP 1.2 has the broker
  close the connection after it, and the client treats the connection as lost:
  `connection lost: the broker sent an ERROR frame that answers no request in flight: TEXT`,
  from whichever operation read it, `consume` included. The outcome of a request that was in
  flight then is unknown, as for any lost connection.
- **A topic delivers only what is published after its subscription exists.** Call
  `Mq.ActiveMq.subscribe(queue)` before the publisher starts. Unsettled topic deliveries are not
  redelivered.

Connection, heart-beats and reconnecting:

- **Heart-beats are on by default, 10 s in each direction.** `CONNECT` carries
  `heart-beat:heartBeatSendMs,heartBeatReceiveMs`, and the interval agreed for each direction is
  the larger of what the sender guarantees and what the receiver asks for (STOMP 1.2); a 0 on
  either side switches that direction off. ActiveMQ answers with the client's numbers swapped, so
  the agreed intervals are the configured ones. `status(queue)` shows them. `TCP_NODELAY` is set.
- **The client writes its heart-beat early.** A newline is written when nothing was written for
  half of the agreed interval, looked at every quarter of it (at most every 250 ms), so the
  longest silence is about three quarters of the interval. ActiveMQ's inactivity monitor allows
  no grace on top of the interval, so a client that wrote exactly on time would be dropped now
  and then. Any frame counts as a heart-beat.
- **The broker is taken for dead after twice the agreed interval without a single byte**, 20 s by
  default. STOMP leaves the margin to the receiver; twice is what leaves room for one late
  heart-beat without waiting much longer than needed. The factor is fixed. The error is
  `connection lost: the broker sent nothing for N ms, having agreed to a heart-beat every M ms`.
  A blocked `consume` ends with it (or, with reconnecting on, waits for the new connection);
  it is not reported as `timeout`.
- **With `heartBeatReceiveMs = 0`, or against a broker that sends no heart-beats, a silent death
  looks like an empty queue**: `consume` returns `Err("timeout")` for ever and a `publish` fails
  after `requestTimeoutMs`. A close by the broker is still noticed at once.
- **A lost connection is re-established in the background.** Whatever loses the connection
  (a socket error, a missed heart-beat, a request that was not confirmed in time, an `ERROR` frame
  that ends the connection, a close by the broker), the keeper thread waits `initialDelayMs`,
  opens a new transport (with the TLS handshake of the options), sends `CONNECT` again and, when
  the connection had a subscription or had asked for one, `SUBSCRIBE` again with the same
  selector and prefetch size. Only then do operations see the new connection. After a failed
  attempt the wait doubles, up to `maxDelayMs`; every wait is shortened by up to `jitterPercent`
  of itself. After a successful reconnect the next outage starts again at `initialDelayMs`.
  `status(queue).reconnects` counts them.
- **Only an established connection is re-established.** `connectWith`, `connectUrl` and
  `connectFromEnv` try once and return the error; an application that starts before its broker
  repeats the connect itself.
- **A `publish` issued while there is no connection fails at once**, with
  `activemq: not connected, reconnecting (nothing was sent); connection lost: REASON`
  (`isNotConnectedError`). It does not wait and it is not queued: a publisher that blocks for the
  length of an outage, or a client-side queue that is lost with the process, would both hide the
  outage from the one party that can decide what to do with the message. The error is safe to
  retry, because nothing was written. `reconnect.publishWaitMs` makes `publish`, `publishBatch`
  and `publishWithHeaders` wait up to that long for the connection first; `ack`, `nack` and
  `subscribe` never wait.
- **A request in flight when the connection drops has an unknown outcome and is not repeated.**
  A `publish` whose `SEND` was written, a `publishBatch` whose `COMMIT` was written and an `ack` or
  `nack` whose frame was written return `connection lost: ...; the outcome is unknown: ...`
  (`isOutcomeUnknownError`). They are not sent again on the new connection (a second copy of a
  message the broker already has would be a duplicate) and not reported as `Ok`. A batch or a
  requeue that lost the connection before its `COMMIT` says instead that the transaction was not
  committed: the broker rolls it back, nothing of it was applied.
- **`consume` waits through an outage.** A `consume` that is waiting when the connection is lost,
  or is called while it is away, goes on waiting for the new connection and then for a message,
  all within its `timeoutMs`. If the time is up first it returns the loss
  (`connection lost: ...` or `not connected, reconnecting ...`), not `timeout`, so an outage is
  never mistaken for an empty queue. `consume(0)` returns that error at once.
- **Deliveries of a lost connection cannot be settled; the broker redelivers them.** STOMP ack ids
  belong to the connection. When a connection is lost with reconnecting on, the deliveries it
  carried leave `unsettledCount`, and messages it had received but not yet handed to a handler
  are dropped. `ack` or `nack` of such a delivery returns `activemq: stale delivery: ...`
  (`isStaleDeliveryError`), once, whether the new connection is there yet or not; a
  `nack(id, true)` makes no copy. ActiveMQ puts those messages back on the queue when it sees the
  old connection end and dispatches them again, to the new subscription or to another consumer.
  A handler therefore can see a message twice: at-least-once delivery, as after any disconnect.
- **The redelivery replaces the dead delivery.** Once `consume` has handed out the redelivery of
  an id, `ack(id)` settles that one and no stale error is reported for the id any more. Before
  that, `ack(id)` is the stale error. What is remembered of unsettled deliveries of lost
  connections is their id only, and it is forgotten after eight further reconnects.
- **`deliveryCount` after a reconnect is one higher**, for every message the lost connection had
  received and not settled, including those still waiting in the client that no handler had
  seen: ActiveMQ flags them `redelivered`, and the flag counts as one delivery (and only one,
  however many outages; see "Requeue, reject and `deliveryCount`").
- **In `Auto` mode a handler's success can be followed by a stale error.** If the connection is
  lost while the handler runs, the acknowledgement that follows cannot be sent; `consume` returns
  the stale error and the message comes again.
- **Reconnecting ends for good in three cases.** The broker answers the new `CONNECT` with an
  `ERROR` frame (a login that is no longer valid) or with a STOMP version other than 1.2; it
  refuses the new `SUBSCRIBE` (a permission that was withdrawn); or `maxAttempts` attempts failed
  in a row. Every operation then returns
  `activemq: connection lost: REASON; reconnecting was given up ...` with the last failure, and
  the background thread ends. Everything else is tried again, for ever by default: a refused TCP
  connection, a timeout, a TLS handshake that fails (the client cannot tell a broker that is
  restarting from a certificate that will never verify), and a login that ended without a
  readable answer (see the wrong-passcode entry below). Set `maxAttempts`, or watch
  `status(queue)`, where that matters.
- **`close` does not wait for a reconnect.** It returns at once while the keeper waits between two
  attempts, and the keeper ends within 50 ms. An attempt that is already under way (a TCP connect
  or TLS handshake, up to `connectTimeoutMs` each) cannot be interrupted: it runs to its end in
  the background and its connection is closed right away, without the application waiting for it.
- **Messages already in the client are bounded.** While no operation runs, the keeper reads what
  the broker sends, so that heart-beats and a close are noticed; it stops reading when 64
  messages wait for `consume`, and the broker is not judged by its silence while that is so.
- **A dead connection without reconnecting stays dead.** With `reconnect.enabled = false`, every
  operation after the loss returns `Err("activemq: connection lost: ...")` with the first reason;
  open a new connection. Deliveries that were unsettled are redelivered by the broker, flagged
  `redelivered`, to whichever consumer is next; `unsettledCount` on the lost connection keeps
  counting them, and `ack` / `nack` return the `connection lost` error.
- **A slow broker loses the connection.** A confirmation that takes longer than
  `requestTimeoutMs` (10 s by default) marks the connection lost even if the broker is healthy,
  because a late receipt could otherwise be taken for the next one. The request's outcome is
  unknown, and a new connection is opened as after any other loss.
- **`isOpen` does not probe.** It is false after `close`, and from the moment a loss is noticed
  until the new connection is in place.
- **Connecting can take `connectTimeoutMs` twice plus `requestTimeoutMs`**: the TCP connect, the
  TLS handshake and the wait for `CONNECTED` each have their own limit.
- **A write is given up when the broker takes no data for `requestTimeoutMs`.** A broker that
  stops reading (or a stalled network) would otherwise block `publish` for ever while it holds the
  connection's lock. The limit is on progress, not on the whole write, so a large message on a
  slow link is not cut off while bytes keep moving (on `jvm` progress is measured per 64 KiB). The
  connection is lost afterwards (`connection lost: the write timed out: ...`), because part of a
  frame may have been sent.
- **A wrong login or passcode is usually `broker error`, and sometimes `connection lost`.**
  ActiveMQ answers the `CONNECT` with an `ERROR` frame and closes the connection at once, and now
  and then the close overtakes the frame: on plain TCP the client sees a bare close (between one
  login in a hundred and one in a thousand against a local 5.17.6, with any client), over TLS it can see a failed read
  instead (.NET: "The decryption operation failed"). The client delivers the `ERROR` frame
  whenever it arrived whole. When it did not, the error is
  `activemq: connection lost: HOST:PORT ended the connection during login without a readable answer; a broker does that when it refuses the login or passcode (PLATFORM REASON)`.
  Neither form contains the passcode. Treat both as a refused login; a network fault during login
  gives the second form too.
- **`close` is idempotent and waits.** It sends `DISCONNECT` and waits up to 2 s (or
  `requestTimeoutMs` if shorter) for the receipt. Deliveries not yet settled go back to the queue
  in their original order, flagged `redelivered`. `close` on a lost connection sends nothing; it
  stops the reconnecting.
- **A connection is one destination.** Publishing to one queue and consuming from another takes
  two connections.
- **A queue is shared by everyone who names it.** With the reference broker each `Mq.connect` is
  a private, empty queue; here two connections to `orders` compete for the same messages, and
  messages survive the connection.

Threads:

- **An `ActiveMqQueue` can be shared between threads; operations are serialized by one lock.**
  There is one socket and no pipelining, so a `publish` waiting for its receipt (up to
  `requestTimeoutMs`) blocks every other operation on the connection, `close` included. A
  `consume` does not wait for the lock beyond its own timeout.
- **Every connection has a background thread, the keeper**, on both targets, unless heart-beats
  were not agreed in either direction and reconnecting is off. It is a daemon thread (it does not
  keep the process alive), sleeps between ticks of at most 250 ms, and ends within one tick of
  `close`, when reconnecting is given up, or, with reconnecting off, when the connection is lost.
  `hasBackgroundThread(queue)` tells. A connection that is dropped without `close` keeps its
  keeper, which keeps the connection alive with heart-beats and reconnects it: always `close`.
- **On `dotnet` a connection also owns a reader thread** until `close` (or until it is lost; a
  new connection gets a new one). Neither thread comes from the runtime's shared thread pool, so
  the client keeps its timing, heart-beats included, when the application's pool threads are all
  busy.
- **The keeper takes the connection's lock for a moment every tick** to read what has arrived.
  `consume` waits that moment out rather than report `timeout` because of it, so `consume(0)`
  can take a few milliseconds longer than its poll (longer on a machine that is short of CPU).
  The keeper's first round comes one tick after the connect.
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
| Reads | a reader thread per connection doing blocking reads, handing each chunk over; no shared thread pool | `SO_TIMEOUT` on the socket |
| Threads per connection | the keeper and, once read from, the reader | the keeper |
| Write limit | `SendTimeout` on the socket | the write runs on a virtual thread; the socket is closed when no 64 KiB chunk gets through in time |
| A refused login whose `ERROR` frame was cut off, over TLS | a failed read ("The decryption operation failed") | a failed read or a bare close |
| Platform part of error texts | .NET exception messages | JDK exception messages |

Differences from the reference in-process broker (`Mq.connect`), collected:

| | Reference broker | This backend |
|---|---|---|
| Queue identity | each connect is a private queue | the named queue on the broker, shared and durable |
| `ack`/`nack` of an unknown id | `Ok` | `Err` |
| Two unsettled deliveries with one id | the second replaces the first | both tracked, settled oldest first |
| `nack(id, false)` | dropped | dead-lettered if persistent, discarded if not |
| `publishBatch` | one by one, can stop half way | one transaction |
| Published `deliveryCount` | reset to 0 | kept; a negative one is an `Err` |
| Acknowledgement mode | none: `ack`/`nack` always act | `Manual` or `Auto` |
| Empty id on `ack`/`nack` | `Err(Mq.EMPTY_MESSAGE_ID)` | `Err("activemq: empty message id")` |
| Failure modes | none | connection loss (followed by a reconnect), request timeouts, broker errors, unknown outcomes, stale deliveries |

Things the tests assert that look odd, and why:

- `tests/client_tests.l` checks the empty-id `Err` only through `publishWithHeaders`, because
  `publish` panics before it can return one.
- The mutual-TLS tests accept any `activemq: ...` error for a missing client certificate, for the
  TLS 1.3 reason above.
- `"an ERROR frame alone does not close the connection"` holds against the fake broker and against
  ActiveMQ for a refused `SEND` or `SUBSCRIBE`, because the frame carries the request's
  `receipt-id`; a refused `CONNECT` does close it.
- The tests of a connection that stays lost switch reconnecting off (`lasting(...)` in
  `tests/client_tests.l`); with it on, the same connection would come back.
- `"a batch interrupted by a lost connection ..."` accepts either wording, "not committed" or
  "the outcome is unknown": which frame of the batch meets the closed socket depends on timing.
- The heart-beat tests wait for events with limits of 10 to 60 s and use one fixed pause: three
  seconds during which a connection with a 1 s heart-beat must stay alive.
- The integration suite accepts two errors for a wrong passcode (`expectLoginRefused`), for the
  reason under "Connection" above, and checks over 300 logins that it is one of the two and
  mostly the broker's.
- Tests whose handler must return `Ok` use a named function, not `{ m -> Ok(()) }` (see "Known
  gaps").
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
    options.withPersistent(false).copy(
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
| `defaultSendOptions()` | the connection's persistence, broker default priority, no expiry, nothing else set |
| `options.withPersistent(true / false)` | this message is persistent, or not, whatever the connection's setting |
| `publishWith(queue, message, options)` | `publish` with `SendOptions`; returns once the broker has confirmed |
| `isPropertyName(name)` | whether `name` may be a property in `SendOptions.properties` |
| `selectorEquals(name, value)`, `selectorForId(id)` | a selector `name = 'value'` with the value quoted; by message id |

`SendOptions`:

| Field | Default | Meaning |
|---|---|---|
| `persistent` | `None` | `Option[Bool]`: `None` sends the message with the connection's `ConnectOptions.persistent`; `Some(value = true)` / `Some(value = false)` (or `withPersistent`) decide for this message |
| `priority` | -1 | 0 to 9; -1 leaves the broker's default of 4. Anything else is an `Err`. |
| `timeToLiveMs` | 0 | milliseconds after which the broker discards an undelivered message; 0 is never; negative is an `Err` |
| `correlationId`, `replyTo`, `jmsType` | empty | the JMS correlation id, a STOMP destination for replies, the JMS type; empty is not set |
| `properties` | none | JMS properties, usable in selectors |

Things to know:

- **`SendOptions.persistent` is unset by default and then the connection decides.** A
  `publishWith` with `defaultSendOptions()` on a connection opened with `persistent = false`
  sends a non-persistent message, like `publish`. Only `withPersistent(...)` (or
  `persistent = Some(...)`) overrides `ConnectOptions.persistent`, in either direction.
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
| `src/stomp.l` | `Mq.ActiveMq.Stomp` | STOMP 1.2 frame encoder and incremental decoder (which reads past frames over a limit and reports them), no I/O |
| `src/url.l` | `Mq.ActiveMq.Url` | broker URL parsing (`parseUrl`, `percentDecode`) |

This package has no extern declarations. The TCP connection, the TLS handshake, the lock and the
clock are `Net.Tcp`'s; the keeper thread is started with `Net.Threads.start` (a
dedicated thread on both targets); `TlsOptions`, `plainTcp()`, their validation, `skipsVerification(tls)`,
`insecureAllowedByEnvironment()` and the reader of the `...TLS*` variables are `Net.Tls`'s; the
authority part of the broker URL is parsed by `Net.Url`. All three are in the `Lyric.Net` package
of this repository (`net/`), shared with `mq-rabbitmq` and `redis`; its README describes how each
target does sockets and TLS and what that costs (one thread per connection that has been read from
on dotnet, for one). `Mq.ActiveMq` re-exports `Net.Tls` (`pub use`), so `TlsOptions` and those
functions are reachable through `import Mq.ActiveMq`. The in-process broker of the hermetic tests
is built on `Net.Tcp.Testing`. The fields of `ActiveMqQueue` are public for those tests; they are
not an application API.

## Tests

Run from the repository root.

Hermetic (no broker needed), on each target:

```sh
lyric test --manifest mq-activemq/lyric.toml --target dotnet
lyric test --manifest mq-activemq/lyric.toml --target jvm
```

This runs the frame codec, URL, re-export and `Extras` unit tests, and
`tests/readme_tests.l` (the examples of this file) and
`tests/client_tests.l`: the `Testing.Conformance.Mq` queue suite and the error paths (refused
connection, bad credentials, `ERROR` frame with and without a `receipt-id`, connection dropped
during publish, during consume and during login, unconfirmed request, refused settlements,
undeliverable and oversize messages under both policies, a broker that stops reading, the bounds
of `consume`, and a busy thread pool) against an in-process STOMP broker on a loopback socket. For
heart-beats and reconnecting each test starts a broker of its own that it can steer: the
`heart-beat` header it answers with, whether it sends heart-beats, whether it refuses logins or
subscriptions, and a switch that closes every connection without a word. Those tests cover the
negotiation in both directions, heart-beats arriving from an idle client, a broker that falls
silent (noticed by an idle connection and by a blocked `consume`), a dropped connection that
comes back, subscribes again and receives the redelivery and later messages, a publisher that
reconnects without subscribing, what `publish`, `publishBatch`, `ack`, `nack` and `consume` return
while the connection is away, the unknown outcome of a publish in flight (and that the message
is on the broker exactly once), stale deliveries during and after the outage, `publishWaitMs`,
giving up after `maxAttempts`, a refused login and a refused subscription on reconnect, `close`
during a 60 s reconnect wait, and the variables of `connectFromEnv`. The
socket-level tests (timed reads, write timeout, bytes arriving in pieces across read timeouts,
independence from the thread pool) are in `net/tests/tcp_tests.l`. The same broker is
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
queue, batches, `Auto` mode, heart-beats (the agreed intervals; that ActiveMQ drops a raw
connection that promised heart-beats and sends none; that a connection with 500 ms heart-beats
idles for seconds without being dropped by either side), a connection cut under the client (the
test closes the client's socket without a `DISCONNECT`: the client reconnects, subscribes again,
reports the old delivery as stale, receives the redelivery with `deliveryCount` 1 and goes on
publishing and consuming, also while a `consume` is waiting), large bodies, foreign and binary messages under both
`onUndeliverable` policies, bodies and header sections over the limits, repeated wrong-passcode
logins and broken selectors, selectors, delivery options and their inheritance, expiry, topics
and competing consumers. Over TLS it runs the conformance suite again and
tests of a verified connection with the custom CA, refusal without it, refusal of a name the
certificate lacks, the cut connection again (the reconnect repeats the TLS handshake),
skip-verify with one key and with both, mutual TLS with a client certificate
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

- Reconnecting: the first connect is not retried; a failing TLS handshake is retried like a
  network failure, because the two cannot be told apart, so with the default `maxAttempts = 0` a
  broker whose certificate no longer verifies is tried for ever (at most every `maxDelayMs`); an
  attempt under way cannot be interrupted by `close` (it is abandoned, not awaited); the factor
  of two for a missing heart-beat is not configurable; nothing is buffered or replayed, by
  design (see "Surprises").
- `Lyric.Net` has no way to interrupt a blocked connect, handshake, read or `sleepMs`, and
  `Net.Threads` has no cancellation, which is why `close` abandons a reconnect attempt under way
  and why the keeper sleeps in pieces of at most 250 ms instead of waiting on a signal.
- Every connection holds its keeper thread until it is closed, and on `dotnet` a reader thread as
  well once it has been read from.
- Not reproduced here: the two failures seen on a hosted CI runner (`dotnet`, TLS) that led to the
  reader-thread design, a `consume(1000)` that missed a delivered message and a wrong-passcode
  login reported as "The decryption operation failed". The first matches what a starved thread
  pool does to the earlier `ReadAsync` design (reproduced deterministically by the hermetic test
  "a busy thread pool does not delay connecting, publishing or consuming"); the second matches the
  broker's close overtaking its `ERROR` frame (reproduced on plain TCP with this client and with a
  plain Python socket, 23 of 3000 logins).
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
  - lyric-lang#9042 and lyric-lang#9064: why the socket and TLS boundary is `Lyric.Net`'s own
    kernel and not the standard library's; see the `Lyric.Net` README.
- Compiler defect met while fixing the settlement paths (not yet filed): on `--target dotnet` the
  lambda `{ m -> Ok(()) }` passed as a `(Mq.Message) -> Result[Unit, String]` handler builds a
  `Result` of the wrong generic instantiation, and `consume` fails with an invalid cast
  (`Result_Ok<Object, Object>` to `Result<Object, String>`) at the moment it calls the handler.
  A handler that calls a function with a declared result type (`{ m -> handle(m) }`), as every
  example here does, is not affected; `--target jvm` is not affected.
- Compiler limit met while adding TLS (not yet filed; the kernel-side ones are in the
  `Lyric.Net` README):
  - A `[project.tests]` module cannot import a non-test package from `tests/` (`T0215`), so the
    test certificates sit in `tests/client_tests.l` itself.
