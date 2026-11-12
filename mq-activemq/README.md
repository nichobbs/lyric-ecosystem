# lyric-mq-activemq

Apache ActiveMQ backend for the `Mq.MessageQueue` and `Mq.QueueConsumer` interfaces of
`lyric-mq`. The client is a STOMP 1.2 implementation written in Lyric; only the TCP socket and the
TLS layer over it come from the host platform, so there is no JMS, NMS or other SDK dependency.

| | |
|---|---|
| Package | `Lyric.Mq.ActiveMq` (Lyric packages `Mq.ActiveMq`, `Mq.ActiveMq.Extras`) |
| Targets | `dotnet`, `jvm` |
| Stability | `experimental` |
| Broker | Apache ActiveMQ Classic with a `stomp://` or `stomp+ssl://` transport connector. Tested against 5.17.6. STOMP 1.2 needs ActiveMQ 5.9 or later. |
| Transport | plain TCP, or TLS 1.2 / 1.3 (`SslStream` on dotnet, `SSLSocket` on jvm) |
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
- `connectUrl(url, queueName)`: `stomp://[login[:passcode]@]host[:port]` (port 61613 when
  omitted) or `stomp+ssl://[login[:passcode]@]host[:port]` (TLS, port 61614 when omitted). The
  login and passcode are percent-decoded. `queueName` is a queue name, or a full destination
  (`/queue/NAME`, `/topic/NAME`). `optionsFromUrl(url, queueName)` returns the `ConnectOptions`
  instead of connecting, so TLS settings can be added before `connectWith`.
- `connectFromEnv()`: reads the variables below. An empty variable counts as unset.

  | Variable | Meaning |
  |---|---|
  | `LYRIC_CONFIG_MQ_ACTIVEMQ_URL` | broker URL (required) |
  | `LYRIC_CONFIG_MQ_ACTIVEMQ_QUEUENAME` | queue name or destination (required) |
  | `LYRIC_CONFIG_MQ_ACTIVEMQ_ACKMODE` | `Manual` (default) or `Auto` |
  | `LYRIC_CONFIG_MQ_ACTIVEMQ_TLSCAFILE` | PEM file: a CA certificate trusted in addition to the platform trust store |
  | `LYRIC_CONFIG_MQ_ACTIVEMQ_TLSCAEXCLUSIVE` | `true`: trust only that CA (default `false`) |
  | `LYRIC_CONFIG_MQ_ACTIVEMQ_TLSCERTFILE` | PEM file: client certificate (mutual TLS) |
  | `LYRIC_CONFIG_MQ_ACTIVEMQ_TLSKEYFILE` | PEM file: its PKCS8 private key |
  | `LYRIC_CONFIG_MQ_ACTIVEMQ_TLSSERVERNAME` | name to check the certificate against, when it is not the URL's host |
  | `LYRIC_CONFIG_MQ_ACTIVEMQ_TLSMINVERSION` | `1.2` (default) or `1.3` |

  The `TLS...` variables apply to a `stomp+ssl://` URL; with a `stomp://` URL they are an error.

`ConnectOptions` fields and their defaults:

| Field | Default | Meaning |
|---|---|---|
| `login`, `passcode` | empty | credentials; empty login sends none |
| `virtualHost` | `host` | the STOMP `host` header |
| `ackMode` | `Manual` | `Auto` acknowledges when the handler returns `Ok` and requeues when it returns `Err` |
| `persistent` | `true` | delivery mode of published messages |
| `selector` | empty | JMS selector of the subscription |
| `prefetchSize` | 0 (broker default) | `activemq.prefetchSize` of the subscription; set 1 for fair sharing between slow consumers |
| `connectTimeoutMs` | 10000 | TCP connect limit, and again the TLS handshake limit |
| `requestTimeoutMs` | 10000 | how long to wait for the broker to confirm a frame |
| `maxFrameBytes` | 16 MiB | largest message body accepted from the broker |
| `tls` | `plainTcp()` | TLS settings, see below |

## TLS

A `stomp+ssl://` URL, or `withTls()` on the options, runs a TLS handshake before the STOMP
`CONNECT`. By default the broker's certificate must chain to the platform trust store and must
carry the host name (or IP address) the client connects to. The option names and the rule for
skipping verification are those of `Std.Http.HttpClientBuilder` (docs/61 in lyric-lang).

```lyric
val options = Mq.ActiveMq.optionsFromUrl("stomp+ssl://app:passcode@broker.internal:61614", "orders")?
  .withCaCertificate(caPem)
  .withClientIdentity(certificatePem, keyPem)
val queue = Mq.ActiveMq.connectWith(options)?
```

| Function on `ConnectOptions` | Effect |
|---|---|
| `withTls()` | TLS with the defaults. Implied by a `stomp+ssl://` URL. |
| `withCaCertificate(pem)` | trust this CA certificate in addition to the platform trust store |
| `withExclusiveCaCertificate(pem)` | trust only this CA certificate; wins over `withCaCertificate` |
| `withClientIdentity(certificatePem, keyPem)` | present a client certificate, for a connector with `needClientAuth=true` |
| `withTlsServerName(name)` | send `name` as SNI and check the certificate against it instead of `host` |
| `withMinTlsVersion(Tls13)` | TLS 1.3 only; the default `Tls12` accepts 1.2 and 1.3 |
| `withInsecureSkipVerify()` | ask for certificate and name checks to be skipped, see below |

- The arguments are PEM text, not `Std.Tls.Certificate` / `Identity` values: those types do not
  expose their host handle outside the standard library (see "Known gaps"). The PEM is checked
  with `Std.Tls` before any connection is made, so the rules are `Std.Tls`'s: one certificate per
  PEM, and an unencrypted PKCS8 key (`-----BEGIN PRIVATE KEY-----`; convert with
  `openssl pkcs8 -topk8 -nocrypt`). Read a file with `Std.File.readText`.
- Setting a TLS option on a connection that is not TLS is an error, not a silent downgrade.
- `withInsecureSkipVerify()` does nothing on its own. Verification is skipped only when the
  environment also has `LYRIC_TLS_ALLOW_INSECURE=1` at the time the connection is opened;
  otherwise verification stays on and a warning is written to standard error. The variable
  without the option does nothing. `connectFromEnv` has no variable for it. On both targets
  skipping covers the certificate chain and the host name together.
- Everything above the handshake (frames, receipts, transactions, consume timeouts, error
  mapping, the lock) is the same code for both transports. A handshake failure is
  `Err("activemq: TLS handshake with HOST:PORT failed: ...")`. A broker that requires a client
  certificate and gets none may, with TLS 1.3, only refuse after the client's handshake has
  finished; the connect then fails with `activemq: connection lost: ...`.
- Revocation (CRL, OCSP) is not checked, as in `Std.Http`.

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

- Heart-beats are not negotiated (`heart-beat:0,0`), over TLS as over plain TCP. The client is
  synchronous and has no thread to send them from. `TCP_NODELAY` is set. A connection that dies
  while idle is reported by the next operation.
- No automatic reconnect. After a transport failure, or a request the broker did not confirm
  within `requestTimeoutMs`, every operation returns `Err("activemq: connection lost: ...")`; open
  a new connection. Deliveries that were unsettled are redelivered by the broker.
- A broker `ERROR` frame fails the operation that caused it with the broker's message. The
  connection stays usable if the broker keeps it open (ActiveMQ does for most errors).
- `consume(0, ...)` returns only a message the broker has already dispatched to this connection.
- An `ActiveMqQueue` can be shared between threads. Operations are serialized by a lock; a waiting
  `consume` releases it every 100 ms, and the lock is fair on the JVM so that a publisher waiting
  for it gets it then. The handler runs outside the lock and may call `ack`/`nack`.
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
| `src/_kernel_dotnet/socket_kernel.l` | `Mq.ActiveMq.Kernel` | `System.Net.Sockets`, `System.Net.Security.SslStream`, monitor lock, clock, task |
| `src/_kernel_jvm/socket_kernel.l` | `Mq.ActiveMq.Kernel` | `java.net.Socket`, `javax.net.ssl.SSLSocket`, `ReentrantLock`, clock, thread |

The two kernel files declare the same package and surface; a file-level `@cfg(target = ...)` keeps
one per build. They are the only files with extern declarations. The kernel also has a listener,
the server side of a TLS handshake and a background-thread primitive, used by the in-process
broker of the hermetic tests.

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

Hermetic (no broker needed), on each target:

```sh
lyric test --manifest mq-activemq/lyric.toml --target dotnet
lyric test --manifest mq-activemq/lyric.toml --target jvm
```

This runs the frame codec, URL, kernel and `Extras` unit tests, and `tests/client_tests.l`: the
`Testing.Conformance.Mq` queue suite and the error paths (refused connection, bad credentials,
`ERROR` frame, connection dropped during publish and during consume, unconfirmed request) against
an in-process STOMP broker on a loopback socket. The same broker is also started behind TLS and
behind TLS with a required client certificate: the conformance suite runs once more over TLS,
with tests of CA trust (exclusive and additional), an untrusted CA, the name check and the server
name override, the two keys of skip-verify, mutual TLS with and without a client certificate,
TLS 1.3 only, a transport that does not match the connector, option validation and the TLS
variables of `connectFromEnv`. The certificates and keys for this are constants at the end of
`tests/client_tests.l`, named `TEST_ONLY_...`: generated for the tests, valid until 2126, and
worthless as secrets.

Integration (real broker). The suite is gated by the `integration` feature; every test fails with
a message naming its variable when that is unset. The broker must enforce authentication and use
the default dead-letter strategy.

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
installs the Ubuntu package (`activemq` 5.17.6) when it is missing, generates a CA, a server and a
client certificate and the broker's PKCS12 stores into a temporary directory with `openssl` and
`keytool`, and starts one in-memory broker from `tests/integration/broker/activemq.xml` with three
connectors on 127.0.0.1: 61613 (plain), 61614 (TLS) and 61615 (TLS, client certificate required).
Under GitHub Actions it exports the six variables through `$GITHUB_ENV`. Elsewhere it prints the
plain URL on the first line and an `export NAME=value` line for each of the other five:

```sh
bash scripts/ci/start-activemq.sh > broker.env
export LYRIC_ACTIVEMQ_URL="$(head -n 1 broker.env)"
eval "$(tail -n +2 broker.env)"
```

## Known gaps

- No heart-beats and no automatic reconnect (see above), on either transport.
- TLS: one CA certificate per option, not a bundle; no PKCS12 or encrypted keys; no revocation
  checks; no client certificate chain (intermediates between the client certificate and the
  broker's CA are not sent).
- On dotnet, skip-verify relies on `SslStream` honouring `X509VerificationFlags.IgnoreInvalidName`
  from the chain policy for the name check. That is what .NET 10 does (the tests cover it); a
  runtime that does not would refuse the connection rather than accept it.
- The kernels import `Std.TlsHost`, the standard library's kernel package for PEM loading. It is
  public but not marked `@stable`.
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
