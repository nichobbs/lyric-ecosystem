# lyric-mq-activemq

Apache ActiveMQ backend for the `Mq.MessageQueue` and `Mq.QueueConsumer` interfaces of
`lyric-mq`. The client is a STOMP 1.2 implementation written in Lyric; nothing but a TCP socket
comes from the host platform, so there is no JMS, NMS or other SDK dependency.

| | |
|---|---|
| Package | `Lyric.Mq.ActiveMq` (Lyric packages `Mq.ActiveMq`, `Mq.ActiveMq.Extras`) |
| Targets | `dotnet`, `jvm` |
| Stability | `experimental` |
| Broker | Apache ActiveMQ Classic with a `stomp://` transport connector. Tested against 5.17.6. STOMP 1.2 needs ActiveMQ 5.9 or later. |
| SDK dependencies | none (`[nuget]` and `[maven]` are empty) |
| Interface | `Lyric.Mq` 0.1.0 |

ActiveMQ Artemis is a different broker with different redelivery rules and is not covered.

## Use

```lyric
import Mq.ActiveMq

val queue = Mq.ActiveMq.connectUrl("stomp://app:passcode@broker.internal:61613", "orders")?

Mq.publish(queue, Mq.Message(
  id = "order-42",
  body = "{\"total\":12}",
  headers = [Mq.MessageHeader(key = "content-type", value = "application/json")],
  deliveryCount = 0))?

Mq.consume(queue, 5000, { m -> process(m) })?
Mq.ack(queue, "order-42")?
queue.close()
```

`Mq.ActiveMq` re-exports `Mq` (`pub use Mq`), so `Message`, `MessageQueue` and the other interface
names are visible through it.

## Configuration

Three ways to connect; none of them reads a credential from source.

- `connectWith(options)`: a `ConnectOptions` record. Start from
  `defaultOptions(host, port, destination)` and adjust with `.copy(...)`.
- `connectUrl(url, queueName)`: `stomp://[login[:passcode]@]host[:port]`, port 61613 when omitted.
  The login and passcode are percent-decoded. `queueName` is a queue name, or a full destination
  (`/queue/NAME`, `/topic/NAME`).
- `connectFromEnv()`: reads

  | Variable | Meaning |
  |---|---|
  | `LYRIC_CONFIG_MQ_ACTIVEMQ_URL` | broker URL (required) |
  | `LYRIC_CONFIG_MQ_ACTIVEMQ_QUEUENAME` | queue name or destination (required) |
  | `LYRIC_CONFIG_MQ_ACTIVEMQ_ACKMODE` | `Manual` (default) or `Auto` |

`ConnectOptions` fields and their defaults:

| Field | Default | Meaning |
|---|---|---|
| `login`, `passcode` | empty | credentials; empty login sends none |
| `virtualHost` | `host` | the STOMP `host` header |
| `ackMode` | `Manual` | `Auto` acknowledges when the handler returns `Ok` and requeues when it returns `Err` |
| `persistent` | `true` | delivery mode of published messages |
| `selector` | empty | JMS selector of the subscription |
| `prefetchSize` | 0 (broker default) | `activemq.prefetchSize` of the subscription; set 1 for fair sharing between slow consumers |
| `connectTimeoutMs` | 10000 | TCP connect limit |
| `requestTimeoutMs` | 10000 | how long to wait for the broker to confirm a frame |
| `maxFrameBytes` | 16 MiB | largest message body accepted from the broker |

## How the interface maps onto ActiveMQ

| Interface | On the wire | Broker behaviour relied on |
|---|---|---|
| `publish` | `SEND` with `receipt`; returns on the `RECEIPT` | the receipt is sent after the broker has taken the message |
| `publishBatch` | `BEGIN`, one `SEND` per message, `COMMIT` with `receipt` | STOMP transactions: all of the batch or none |
| `consume` | first call sends `SUBSCRIBE` with `ack:client-individual`; then waits for a `MESSAGE` | dispatch in queue order |
| `ack(id)` | `ACK` for that delivery | individual acknowledgement |
| `nack(id, false)` | `NACK` | ActiveMQ treats a STOMP `NACK` as a poison acknowledgement: the message is never redelivered. A persistent message moves to the dead-letter queue (`ActiveMQ.DLQ` with the default strategy); a non-persistent one is discarded (default `processNonPersistent=false`). |
| `nack(id, true)` | `BEGIN`; `SEND` of a copy with `lyric_delivery_count` raised by one; `ACK` of the original; `COMMIT` | STOMP transactions, so the copy and the acknowledgement are applied together or not at all |
| `close` | `UNSUBSCRIBE`, `DISCONNECT` with `receipt`, socket close | unsettled deliveries return to the queue flagged `redelivered` |

Why requeue is a transactional republish: ActiveMQ has no STOMP frame that puts one delivery back
on its queue. `NACK` dead-letters, aborting a transaction that holds an `ACK` redelivers nothing,
and the broker only redelivers when the subscription or connection ends, which returns every
unsettled delivery at once and does not report how many times a message was delivered. The
republish gives exact per-message requeue and an exact `deliveryCount`. Its consequences:

- A requeued message goes to the tail of the queue (as in the reference in-process broker), and gets
  a new broker `message-id` and timestamp. Its body, headers, persistence, priority, expiry and
  other properties are copied.
- On a topic, requeue republishes to the topic, so every subscriber gets the copy.

Message mapping:

- `Message.id` is the `lyric_id` header. It is a JMS property, so a selector can match it
  (`Extras.selectorForId`). Ids are not required to be unique; `ack`/`nack` settle the oldest
  unsettled delivery with that id. Settling an id with no unsettled delivery is an `Err`.
- `Message.headers` is one JSON array in the `lyric_headers` header, so order, repeated keys and
  every character survive. They are not individual JMS properties; use
  `Extras.SendOptions.properties` for values a selector must see.
- `Message.body` is UTF-8 text sent with `content-length` (so it may contain NUL) and
  `amq-msg-type:text`, which makes it a JMS `TextMessage` for other consumers.
- `Message.deliveryCount` is `lyric_delivery_count` plus one when the broker flags the delivery
  `redelivered`. After several broker-side redeliveries (a consumer that keeps disconnecting without
  settling) it is a lower bound, because ActiveMQ does not expose its redelivery counter over STOMP.
- A message from another producer (no `lyric_id`) is delivered with the broker's `message-id` as
  its id and its application headers as `headers`.
- A message this client cannot represent (a body that is not UTF-8, malformed `lyric_` headers) is
  rejected with `NACK` so it does not block the destination, and `consume` returns an `Err` naming
  it.

Other decisions:

- Heart-beats are not negotiated (`heart-beat:0,0`). The client is synchronous and has no thread
  to send them from. `TCP_NODELAY` is set. A connection that dies while idle is reported by the
  next operation.
- No automatic reconnect. After a transport failure, or a request the broker did not confirm
  within `requestTimeoutMs`, every operation returns `Err("activemq: connection lost: ...")`; open
  a new connection. Deliveries that were unsettled are redelivered by the broker.
- A broker `ERROR` frame fails the operation that caused it with the broker's message. The
  connection stays usable if the broker keeps it open (ActiveMQ does for most errors).
- `consume(0, ...)` returns only a message the broker has already dispatched to this connection.
- An `ActiveMqQueue` can be shared between threads. Operations are serialized by a lock; a waiting
  `consume` releases it every 100 ms. The handler runs outside the lock and may call `ack`/`nack`.
- `DeadLetterStore` is not implemented. The interface is an append/list/count store with a
  capacity; ActiveMQ's DLQ is a queue that can only be consumed, so it does not map. Read dead
  letters by connecting to `/queue/ActiveMQ.DLQ`, or use `Mq.inProcessDeadLetterStore`.

## ActiveMQ-specific features (`Mq.ActiveMq.Extras`)

- `queueDestination(name)`, `topicDestination(name)`. A topic delivers a copy to every
  subscription open at publish time; call `Mq.ActiveMq.subscribe(queue)` before publishing starts.
- `publishWith(queue, message, options)` with `SendOptions`: `persistent`, `priority` (0 to 9),
  `timeToLiveMs`, `correlationId`, `replyTo`, `jmsType`, and `properties` (JMS properties).
- Selectors: `ConnectOptions.selector` takes an SQL-92 JMS selector over properties and JMS headers
  (`JMSPriority`, `JMSType`, `JMSCorrelationID`, `JMSDeliveryMode`). `selectorEquals(name, value)`
  and `selectorForId(id)` build quoted ones.
- `Mq.ActiveMq.publishWithHeaders` sends arbitrary extra STOMP headers.

Message priority only changes delivery order when the broker's destination policy enables
`prioritizedMessages`.

## Layout

| File | Package | Content |
|---|---|---|
| `src/activemq.l` | `Mq.ActiveMq` | connection, interface implementation |
| `src/extras.l` | `Mq.ActiveMq.Extras` | topics, delivery options, selectors |
| `src/stomp.l` | `Mq.ActiveMq.Stomp` | STOMP 1.2 frame encoder and incremental decoder |
| `src/url.l` | `Mq.ActiveMq.Url` | broker URL parsing |
| `src/_kernel_dotnet/socket_kernel.l` | `Mq.ActiveMq.Kernel` | `System.Net.Sockets`, monitor lock, clock, task |
| `src/_kernel_jvm/socket_kernel.l` | `Mq.ActiveMq.Kernel` | `java.net.Socket`, `ReentrantLock`, clock, thread |

The two kernel files declare the same package and surface; a file-level `@cfg(target = ...)` keeps
one per build. They are the only files with extern declarations. The kernel also has a listener
and a background-thread primitive, used by the in-process broker of the hermetic tests.

## Tests

Hermetic (no broker needed), on each target:

```sh
lyric test --manifest mq-activemq/lyric.toml --target dotnet
lyric test --manifest mq-activemq/lyric.toml --target jvm
```

This runs the frame codec, URL, kernel and `Extras` unit tests, and `tests/client_tests.l`: the
`Testing.Conformance.Mq` queue suite and the error paths (refused connection, bad credentials,
`ERROR` frame, connection dropped during publish and during consume, unconfirmed request) against
an in-process STOMP broker on a loopback socket.

Integration (real broker). The suite is gated by the `integration` feature and reads
`LYRIC_ACTIVEMQ_URL`; every test fails with a message naming the variable when it is unset. The
broker must enforce authentication and use the default dead-letter strategy.

```sh
export LYRIC_ACTIVEMQ_URL=stomp://USER:PASSCODE@127.0.0.1:61613
lyric test --manifest mq-activemq/lyric.toml --target dotnet --features integration
lyric test --manifest mq-activemq/lyric.toml --target jvm --features integration
```

It runs the conformance suite and tests of redelivery, the dead-letter queue, batches, `Auto`
mode, large bodies, foreign and binary messages, selectors, delivery options, expiry, topics and
competing consumers.

Starting a broker without a container, from the Ubuntu package (`activemq` 5.17.6) and the
configuration in `tests/integration/broker/activemq.xml` (in memory, one user, STOMP on
127.0.0.1; user, passcode and port are system properties):

```sh
sudo apt-get install -y activemq
WORK=$(mktemp -d)
PASSCODE=$(head -c 12 /dev/urandom | od -An -tx1 | tr -d ' \n')
java -Dactivemq.home=/usr/share/activemq -Dactivemq.base="$WORK" -Dactivemq.data="$WORK/data" \
  -Dlyric.activemq.user=lyric -Dlyric.activemq.passcode="$PASSCODE" -Dlyric.activemq.stompPort=61613 \
  -cp "/usr/share/activemq/bin/activemq.jar:/usr/share/activemq/lib/*:/usr/share/activemq/lib/optional/*" \
  org.apache.activemq.console.Main start "xbean:file:$PWD/mq-activemq/tests/integration/broker/activemq.xml" \
  > "$WORK/broker.log" 2>&1 &
until (echo > /dev/tcp/127.0.0.1/61613) 2>/dev/null; do sleep 1; done
export LYRIC_ACTIVEMQ_URL="stomp://lyric:$PASSCODE@127.0.0.1:61613"
```

## Known gaps

- TLS (`stomp+ssl://`) is not supported; the URL parser refuses it. Terminate TLS in front of the
  client or use a network the plain connector is safe on.
- No heart-beats and no automatic reconnect (see above).
- `DeadLetterStore` is not implemented (see above).
- Durable topic subscriptions (`activemq.subscriptionName` with a `client-id`) are not exposed.
- Only ActiveMQ Classic 5.17.6 has been tested. The official `apache/activemq-classic` container
  image has not been run against this suite.
- Compiler issues found while writing this package, tracked in `nichobbs/lyric-lang`:
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
