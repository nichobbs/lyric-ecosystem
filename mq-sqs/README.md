# lyric-mq-sqs

Amazon SQS backend for the `Mq.MessageQueue` and `Mq.QueueConsumer` interfaces of `lyric-mq`, for
`dotnet` and `jvm`, `experimental` (0.1.0). The client is written in Lyric: the AWS JSON 1.0
protocol over `Std.Http`, signed with AWS Signature Version 4 by `Lyric.Aws.SigV4`. There is no AWS
SDK underneath and no extern declaration in the package. One `SqsQueue` is one queue and implements
both interfaces.

| | |
|---|---|
| Package | `Lyric.Mq.Sqs` (Lyric packages `Mq.Sqs`, `Mq.Sqs.Client`, `Mq.Sqs.Extras`, `Mq.Sqs.Address`, `Mq.Sqs.Wire`) |
| Interfaces | `Mq.MessageQueue`, `Mq.QueueConsumer` of `Lyric.Mq` 0.1.0. `Mq.DeadLetterStore` is not implemented. |
| Targets | `dotnet`, `jvm` |
| Stability | `experimental` (0.1.0) |
| Service | Amazon SQS, standard and FIFO queues, and SQS-compatible servers that speak the JSON protocol (ElasticMQ 1.6.11 is what CI runs) |
| Protocol | AWS JSON 1.0 (`X-Amz-Target: AmazonSQS.<Action>`, `Content-Type: application/x-amz-json-1.0`), AWS Signature Version 4, service `sqs` |
| Dependencies | `Lyric.Aws.SigV4` (`aws-sigv4/` in this repository), a `{ workspace = true }` dependency. No SDK, `[nuget]` and `[maven]` are empty |

## Usage

Inside this workspace a package declares `"Lyric.Mq.Sqs" = { workspace = true }` under
`[dependencies]`, and `"Lyric.Mq" = { workspace = true }` as well when it imports `Mq` itself, as
the examples here do.

```lyric
import Std.Core
import Mq
import Mq.Sqs as Sqs
import Mq.Sqs.Extras as SqsExtras

func handle(m: in Mq.Message): Result[Unit, String] {
  println(m.id + ": " + m.body)
  Ok(())
}

func example(queueUrl: in String): Result[Unit, String] {
  val cfg = match Sqs.configFromEnv(queueUrl) {
    case Ok(c) -> c
    case Err(e) -> return Err(e.text())
  }
  val queue = Sqs.connectWith(cfg)?
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

`example("https://sqs.eu-west-1.amazonaws.com/123456789012/orders")` takes the credentials from
`AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`, the endpoint and the region from the queue URL,
publishes, waits up to five seconds for a message, deletes it and closes.

Creating a FIFO queue with a dead-letter queue, and publishing to a message group:

```lyric
func provision(cfg: in Sqs.SqsConfig): Result[Sqs.SqsQueue, String] {
  val client = match Sqs.openClient(cfg) {
    case Ok(c) -> c
    case Err(e) -> return Err(e.text())
  }
  val deadUrl = match SqsExtras.createQueue(client, "orders-dead.fifo", SqsExtras.fifoQueue()) {
    case Ok(url) -> url
    case Err(e) -> return Err(e.text())
  }
  val deadArn = match SqsExtras.queueArn(client, deadUrl) {
    case Ok(arn) -> arn
    case Err(e) -> return Err(e.text())
  }
  val settings = SqsExtras.fifoQueue().withRedrive(deadArn, 5)
  val url = match SqsExtras.createQueue(client, "orders.fifo", settings) {
    case Ok(u) -> u
    case Err(e) -> return Err(e.text())
  }
  val queue = Sqs.connectWith(cfg.copy(queueUrl = url).withMessageGroup("tenant-7").withReceiveBatch(10))?
  val order = Mq.Message(id = "order-43", body = "{}", headers = [], deliveryCount = 0)
  match SqsExtras.publishToGroup(queue, order, "tenant-8") {
    case Ok(sqsMessageId) -> println("stored as " + sqsMessageId)
    case Err(e) -> return Err(e.text())
  }
  Ok(queue)
}
```

Both examples are compiled and run by `tests/readme_tests.l`.

The interface operations return `Result[_, String]`; the text begins with `sqs: `, except a
handler's own error, which `consume` returns as it is, and the interface's `"timeout"`. The
operations of `Mq.Sqs.Client` and `Mq.Sqs.Extras` return `Result[_, SqsError]`, whose `code` is the
SQS error code. No HTTP failure throws.

The open function is `connectWith`, not `connect`: `Mq.connect` is the in-process broker's, and a
file that imports both `Mq` and `Mq.Sqs` would find a bare `connect` ambiguous.

## How the interface maps onto SQS

| Interface | SQS action | Notes |
|---|---|---|
| `publish` | `SendMessage` | `Ok` means SQS stored the message |
| `publishBatch` | `SendMessageBatch`, one request per ten messages or `maxBatchBytes` bytes | not atomic, see below |
| `consume(timeoutMs, handler)` | `ReceiveMessage` with `WaitTimeSeconds` | long polls for the whole seconds, short polls for the rest |
| `ack(id)` | `DeleteMessage` with the receipt handle of that delivery | |
| `nack(id, true)` | `ChangeMessageVisibility` to 0, then `ChangeMessageVisibilityBatch` to 0 for what was received with it and not yet handed out | receivable again at once |
| `nack(id, false)` | `SendMessage` to `deadLetterQueueUrl` when one is configured, then `DeleteMessage` | see "Why `nack(id, false)` deletes" |
| `close` | `ChangeMessageVisibilityBatch` to 0 for every delivery the consumer still holds | then every operation is `Err("sqs: the queue is closed")` |

How a message is carried:

| `Mq.Message` | SQS message |
|---|---|
| `body` | `MessageBody`, as it is. When it holds a character SQS refuses (see "Surprises"): the base64 of its UTF-8 bytes, with attribute `lyric.body-encoding = base64`. When it is empty: `-`, with `lyric.body-encoding = empty` |
| `id` | message attribute `lyric.id` (`String`). SQS's own `MessageId` is a different thing |
| `headers` | message attribute `lyric.headers` (`String`): a JSON array of `{"key":...,"value":...}` in order; absent when there are no headers |
| `deliveryCount` | not sent. On receipt: the system attribute `ApproximateReceiveCount` minus one |

### `publishBatch` and atomicity

`SendMessageBatch` takes at most ten messages and, by default here, 262144 bytes of bodies and
attributes per request (`maxBatchBytes`; see "Surprises" for the 1 MiB limit). A larger
`publishBatch` is split into consecutive requests, sent in order.

SQS applies a batch entry by entry, so even one request is not all-or-nothing, and several
requests are not either. What is lost compared with a transactional broker, exactly:

- Every message is encoded before anything is sent. A message that cannot be encoded (an id SQS
  cannot carry) fails the call with nothing published.
- Requests are sent in order. At the first request that SQS refuses, or that reports an entry it did
  not accept, `publishBatch` stops and returns `Err`. The text names the ids that were not accepted
  with SQS's code for each, and says how many messages of earlier requests stay published, how many
  of the failing request were accepted, and how many later messages were not sent.
- An entry SQS lists neither as successful nor as failed is reported as `not acknowledged`.
- When a request gets no response (a timeout, a dropped connection), the text says that it is not
  known whether SQS accepted its messages.
- Nothing is rolled back and nothing is retried. Messages accepted before the failure are in the
  queue and will be delivered. A caller that publishes the batch again must expect those twice on a
  standard queue; on a FIFO queue with `Deduplication.ById` (the default) SQS drops the repeats for
  five minutes.

### Why `nack(id, false)` deletes

The interface says a nack without requeue "routes to the broker dead-letter queue (if configured)"
and is not redelivered. SQS has no call that moves a message to a dead-letter queue. Its redrive
policy acts only when a message has been received `maxReceiveCount` times without being deleted, so
leaving a nacked message to it means the message is redelivered `maxReceiveCount - 1` more times
first (or for ever, on a queue without a policy), which is what `requeue = false` rules out.

So `nack(id, false)` deletes the message. When the configuration names a `deadLetterQueueUrl`, the
SQS message is first copied to that queue (same body, same `lyric.*` attributes, so a consumer of
the dead-letter queue sees the same `Mq.Message`), and deleted only if the copy was accepted; if it
was not, the `Err` says why and the delivery stays in flight. The copy and the delete are two
requests: a process that dies between them leaves the message in both queues. A redrive policy on
the queue still does its own job for messages that keep failing without being nacked
(`Mq.Sqs.Extras.setRedrivePolicy`).

## The conformance suite

`Testing.Conformance.Mq.runQueue` has eleven cases. Nine hold on any SQS queue. Two assert publish
order: "messages are delivered in publish order" (`a`, `b`, `c`) and "publishBatch delivers every
message in order" (`x`, `y`). A standard queue promises no order, so those two cannot be guaranteed
there. No case assumes exactly-once delivery: every case uses a fresh queue and consumes within the
visibility timeout, so at-least-once delivery does not show.

The suite is therefore run against **FIFO queues**, on both targets, hermetically against the fake
server and in the integration suite against ElasticMQ, with all eleven cases and no case changed or
skipped. `tests/fake_sqs_tests.l` also runs it against standard queues on a fake that returns the
newest message first (which SQS is free to do) and asserts that exactly the delivery-order
expectations of those two cases fail and nothing else does.

One configuration detail matters, and it is SQS's, not the suite's: the suite takes `a`, `b` and
`c` one after the other without acknowledging any of them. A FIFO queue does not release the next
message of a message group while an earlier one is in flight, so a consumer that receives one
message per request would get `a` and then nothing until `a` is settled or times out. The suite's
queues are opened with `withReceiveBatch(10)`: one `ReceiveMessage` returns the messages of the
group together, in order, and the consumer hands them out one per `consume`. With
`receiveBatchSize = 1` (the default) on a FIFO queue, `consume` returns `Err("timeout")` while an
earlier message of the same group is unsettled; that is strict FIFO processing, and the right
setting when each message must be finished before the next starts.

(`mq-activemq` satisfies the same two cases because an ActiveMQ queue dispatches in order and its
prefetch window delivers the next message before the previous one is acknowledged.)

## Configuration

`SqsConfig` (`awsConfig(queueUrl, accessKeyId, secretAccessKey)`,
`endpointConfig(endpoint, queueUrl, region, accessKeyId, secretAccessKey)`, `configFromEnv(queueUrl)`,
then `.copy(...)` or the `with...` functions):

| Field | Default | Meaning |
|---|---|---|
| `queueUrl` | required by `connectWith` | the full queue URL. May be empty for `openClient` (queue management only) |
| `region` | from the queue URL or the endpoint | required when neither is an AWS host name (`sqs.<region>.amazonaws.com`) |
| `accessKeyId`, `secretAccessKey` | required | |
| `sessionToken` | `""` | temporary credentials (`withSessionToken`) |
| `endpoint` | the queue URL's scheme and host | the base URL requests are sent to, for an SQS-compatible server, a VPC endpoint, or a queue URL whose host is not reachable as written |
| `requestTimeoutMs` | 30000 | deadline of one request, not counting the seconds a long poll waits (`withRequestTimeout`) |
| `ackMode` | `Mq.AckMode.Manual` | `Auto`: `consume` deletes on `Ok` and requeues on `Err`; `ack` and `nack` do nothing (`withAckMode`) |
| `visibilityTimeoutSeconds` | 30 | 1 to 43200; sent with every receive (`withVisibilityTimeout`) |
| `receiveBatchSize` | 1 | 1 to 10 messages per `ReceiveMessage` (`withReceiveBatch`) |
| `messageGroupId` | `default` | FIFO: the group of published messages (`withMessageGroup`) |
| `deduplication` | `Deduplication.ById` | FIFO: `ById`, `Unique` or `ContentBased` (`withDeduplication`) |
| `deadLetterQueueUrl` | `""` | where `nack(id, false)` copies a message before deleting it (`withDeadLetterQueue`) |
| `maxBatchBytes` | 262144 | the largest `SendMessageBatch` request |
| `additionalCaCertificatePem` | `""` | a certificate authority (PEM text, one certificate) trusted in addition to the system trust store (`withCaCertificate(pem)`, `withCaCertificateFile(path)`) |
| `caCertificatePem` | `""` | the only certificate authority the server certificate may chain to; the system trust store is not consulted, and it wins over `additionalCaCertificatePem` (`withExclusiveCaCertificate(pem)`, `withExclusiveCaCertificateFile(path)`) |

`connectWith` and `openClient` make no request; they return `Err` (`INVALID_CONFIG` for
`openClient`) for empty credentials, a malformed queue URL or endpoint, a missing or malformed
region, a non-positive timeout, a visibility timeout outside 1 to 43200, a receive batch outside 1
to 10, a non-positive `maxBatchBytes`, a malformed dead-letter queue URL, a CA certificate that is
not a PEM certificate, and, for a FIFO queue, a message group id SQS would refuse.

Both the queue URL and the endpoint must be `https://` unless the host is `localhost`, `127.0.0.1`
or `[::1]`: a signature does not hide the access key id, the queue or the messages, and a request
sent in the clear can be replayed for the 15 minutes its signature is valid. They must be
`scheme://host[:port]` (plus the queue path for a queue URL), with no credentials, query or
fragment.

TLS: the server certificate of an `https://` URL is verified against the system trust store and must
carry the host name. For an SQS-compatible server whose certificate comes from a private authority,
or is self-signed, give the authority's certificate (or the self-signed certificate itself) to
`withCaCertificate`, which adds it to the system trust store, or to `withExclusiveCaCertificate`,
which trusts nothing else. These are `Std.Http`'s options of the same names, and the names and
meaning are those of `Lyric.Redis`. The `...File` forms read a PEM file and return a
`Result[SqsConfig, SqsError]`: a file that is missing or empty is `INVALID_CONFIG`. A CA certificate
does not relax the rule above (plain HTTP stays loopback-only), and there is no option to skip
certificate verification.

```lyric
val base = Sqs.endpointConfig("https://sqs.internal.example:9324", queueUrl, "us-east-1", keyId, secret)
val cfg = match base.withCaCertificateFile("/etc/ssl/private-ca.pem") { case Ok(c) -> c; case Err(e) -> return Err(e.text()) }
val queue = Sqs.connectWith(cfg)?
```

Environment variables:

| Variable | Read by | Meaning |
|---|---|---|
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` | `configFromEnv`, `connectFromEnv` | required |
| `AWS_SESSION_TOKEN` | same | optional |
| `AWS_REGION`, else `AWS_DEFAULT_REGION` | same | optional when the queue URL carries the region |
| `AWS_ENDPOINT_URL_SQS`, else `AWS_ENDPOINT_URL` | same | optional endpoint override |
| `LYRIC_CONFIG_MQ_SQS_QUEUEURL` | `connectFromEnv` | the queue URL (required) |
| `LYRIC_CONFIG_MQ_SQS_ACKMODE` | `connectFromEnv` | `Manual` (default) or `Auto` |
| `LYRIC_CONFIG_MQ_SQS_VISIBILITYTIMEOUT` | `connectFromEnv` | seconds, default 30 |
| `LYRIC_CONFIG_MQ_SQS_RECEIVEBATCH` | `connectFromEnv` | 1 to 10, default 1 |
| `LYRIC_CONFIG_MQ_SQS_MESSAGEGROUP` | `connectFromEnv` | FIFO message group, default `default` |
| `LYRIC_CONFIG_MQ_SQS_DEADLETTERQUEUEURL` | `connectFromEnv` | dead-letter queue URL, default none |
| `LYRIC_CONFIG_MQ_SQS_CACERTIFICATEFILE` | `connectFromEnv` | a PEM file with a certificate authority to trust in addition to the system trust store (`withCaCertificateFile`), default none |
| `LYRIC_SQS_ENDPOINT`, `LYRIC_SQS_REGION` | the integration suite only | where the test server is |
| `LYRIC_SQS_CA_FILE` | the integration suite only | optional: a PEM file with a certificate authority, passed to `withCaCertificateFile` |

`Mq`'s own `LYRIC_CONFIG_MQ_CONNECTION_*` variables are for `Mq.connect()` and are not read. A
variable set to the empty string counts as unset. Credentials are never read from a file and never
appear in an error message.

Error codes (`SqsError.code`):

| Code | When |
|---|---|
| the SQS code, verbatim (`QueueDoesNotExist`, `InvalidSignatureException`, `UnrecognizedClientException`, `RequestThrottled`, `ReceiptHandleIsInvalid`, `QueueNameExists`, `BatchRequestTooLong`, `InvalidMessageContents`, ...) | `__type` of the error document, without its namespace |
| `HTTP_<status>` | a failed response without an error document (a proxy's HTML page, a redirect) |
| `TIMEOUT` | no response within the request deadline (`requestTimeoutMs`, plus the wait of a long poll). The request may have reached SQS and been applied |
| `CONNECTION_FAILED` | no response for any other reason: refused, reset, TLS failure |
| `MALFORMED_RESPONSE` | a 200 this client cannot read |
| `INVALID_CONFIG`, `INVALID_ARGUMENT` | refused before anything was sent |
| `NOT_IN_FLIGHT`, `CLOSED` | `extendVisibility` and `publishWith` on a delivery or queue that is gone |

## Surprises

Ordering and delivery:

- **A standard queue delivers at least once and in no guaranteed order.** `publishBatch([a, b])`
  may arrive as `b`, `a`, and a message may arrive twice. Use a FIFO queue when order matters.
- **A FIFO queue with `receiveBatchSize = 1` delivers the next message of a group only after the
  previous one is settled** (or its visibility timeout passes). `consume` returns `Err("timeout")`
  until then. See "The conformance suite".
- **With `receiveBatchSize > 1`, messages wait inside the consumer, already in flight.** Their
  visibility timeout is running. One that is not handed to a handler before it runs out is dropped
  by the consumer and redelivered by SQS, with a `deliveryCount` of 1 although no handler saw it.
  Another consumer of the same queue cannot receive them meanwhile.
- **`nack(id, true)` also returns the messages that were received with it and not yet handed out**,
  so that they cannot overtake the requeued one on a FIFO queue. Their receive count goes up by one
  too. A redrive policy counts those receives.
- **A requeued message is receivable immediately**, with no backoff. A handler that always fails
  spins; set a redrive policy, or use `Mq.Sqs.Extras.extendVisibility` to delay the retry.
- **`deliveryCount` is `ApproximateReceiveCount - 1`**: SQS calls it approximate, and it counts
  receives by every consumer, including receives after a visibility timeout.

Acknowledgement:

- **`ack` and `nack` work only on the `SqsQueue` that delivered the message, and only until the
  visibility timeout passes.** SQS settles a delivery by its receipt handle, which the consumer
  keeps in memory under the message's id. After `visibilityTimeoutSeconds` (30 by default) the
  handle is forgotten, `ack` returns `Err` saying the timeout passed, and SQS redelivers the
  message. For longer work raise the timeout or call `extendVisibility`.
- **`visibilityTimeoutSeconds` is sent with every receive and overrides the queue's own
  `VisibilityTimeout` attribute** for this consumer; 0 is refused.
- **Two unsettled deliveries with the same id are settled oldest first.**
- **In `Manual` mode a handler that returns `Err` leaves the delivery in flight.** `consume` returns
  the handler's error; the caller nacks, or the message comes back after the visibility timeout.
- **A failed `ack` or `nack` can be retried**: the delivery stays known to the consumer when the
  request fails.

Messages:

- **SQS refuses some characters.** A body may hold only tab, line feed, carriage return, U+0020 to
  U+D7FF, U+E000 to U+FFFD and the supplementary planes, and may not be empty. A body outside that
  is sent base64-encoded and marked, so every `String` round-trips through this library; a consumer
  that is not this library sees the base64 text and the `lyric.body-encoding` attribute. A body
  that is not well-formed UTF-16 (an unpaired surrogate) is not preserved: the surrogate is replaced when
  the body is encoded as UTF-8.
- **An id with a control character, U+FFFE, U+FFFF or an unpaired surrogate is refused** with
  `Err`, before anything is sent: the id is the handle `ack` takes, so it is not altered.
- **Headers are one attribute**, `lyric.headers`, not one attribute per header: SQS allows ten
  attributes, restricts their names to letters, digits, `-`, `_` and `.`, refuses empty values and
  returns them unordered. SNS filter policies and other consumers therefore cannot match on a
  single header.
- **A message from another producer** (no `lyric.id`) is delivered with the SQS `MessageId` as its
  id, its body as it is, and its `String` and `Number` attributes as headers in name order. Binary
  attributes are dropped.
- **A message whose `lyric.*` attributes are not what this library writes** makes `consume` return
  `Err` naming the message; it stays in flight under its id and can be nacked.
- **Sizes.** SQS counts body and attributes. This client does not check a single message's size;
  SQS answers `InvalidParameterValue`. `maxBatchBytes` defaults to 262144, the limit SQS had until
  2025 and that ElasticMQ and LocalStack still enforce; Amazon SQS now accepts 1 MiB, and
  `cfg.copy(maxBatchBytes = 1048576)` uses it.
- **`MD5OfMessageBody` is not verified** (the standard library has no MD5); integrity is TLS's.

FIFO:

- **`Deduplication.ById` is the default**: the `MessageDeduplicationId` is the message's id (its
  SHA-256 when the id is longer than 128 characters or holds anything but letters, digits and ASCII
  punctuation). Publishing the same id again within five minutes returns `Ok` and stores nothing.
  `Deduplication.Unique` sends a random id; `Deduplication.ContentBased` sends none and needs the
  queue attribute `ContentBasedDeduplication`.
- **All messages go to one message group**, `default`, unless `withMessageGroup` or
  `Mq.Sqs.Extras.publishToGroup` says otherwise. One group is one strictly ordered stream.
- **SQS refuses a per-message delay on a FIFO queue** (`publishDelayed`).
- **Order across message groups is not defined.**

Waiting and failures:

- **`consume` timeouts.** `WaitTimeSeconds` is whole seconds, at most 20. `consume(0, ...)` is one
  short poll. `consume(300, ...)` is short polls 100 ms apart for 300 ms. `consume(2300, ...)` is a
  2 s long poll, then short polls. `consume(45000, ...)` is long polls of 20 and 20 seconds, then of the whole seconds that are left, then short
  polls. Every
  poll is a billed request. On Amazon SQS a short poll samples a subset of servers and can miss a
  message that a long poll would find.
- **A `consume` cannot be interrupted**, and a long poll holds its HTTP connection for up to 20 s
  plus `requestTimeoutMs`.
- **A timeout is its own outcome, with one message on both targets**: code `TIMEOUT`, text
  `<Action> timed out after <n> ms at <endpoint>`. It means the request may have been applied: a
  `publish` that timed out may be in the queue, a `publishBatch` reports that it is not known whether
  SQS accepted the messages of that request, and a `consume` whose `ReceiveMessage` timed out may
  have put messages in flight that this consumer never saw (they return after the visibility
  timeout). `CONNECTION_FAILED` (text `<Action> got no response from <endpoint>: ...`) is every other
  transport failure; `publishBatch` treats it as unknown too, because a connection can also break
  after the request was sent. A slow `ReceiveMessage` is never reported as the interface's
  `"timeout"`, which only means the queue had nothing.
- **Only the first certificate of the PEM text is read.** `withCaCertificate` and
  `withExclusiveCaCertificate` take one certificate authority, not a bundle: in a file with several
  certificates the others are ignored, on both targets. Pass the authority that signed the server
  certificate (for a self-signed server, its own certificate).
- **An untrusted server certificate is `CONNECTION_FAILED`, and the message differs by target.** On
  the JVM it names the cause (`PKIX path building failed: ... unable to find valid certification path
  to requested target`); on dotnet it is only `The SSL connection could not be established, see inner
  exception`. The code is the same, and so are the CA options: both targets accept the same PEM and
  behave the same in the hermetic and integration suites.
- **A CA certificate is for the endpoint and for every queue URL the client is used with**: there is
  one HTTP client per `SqsClient`. On an `http://` (loopback) endpoint the option is validated and
  then has nothing to verify.
- **Nothing is retried.** Throttling (`RequestThrottled`, `ThrottlingException`), 5xx responses and
  transport errors are returned as they are.
- **Redirects are not followed**: a redirect would send a signed request to a host the signature
  does not cover; it is reported as `HTTP_3xx`.
- **`close` makes requests.** It returns unsettled deliveries to the queue (best effort) and does
  not delete them.
- **The clock matters.** SQS refuses a request whose `x-amz-date` is more than 15 minutes off.

Differences between the targets: none in behaviour that the suites can see, except one inherited
from the compiler. On `jvm`, `publishBatch` with an empty message id panics with
`PreconditionViolated` (the interface's `requires: allHaveIds(messages)` is enforced); on `dotnet`
the inherited clause is not enforced and the call returns `Err("sqs: empty message id")`. Do not
pass an empty id.

## Backend-specific module

`Mq.Sqs.Extras`, separate from the interface implementation:

- Queue management, on an `SqsClient` (`openClient(cfg)`, or `queue.client`): `createQueue(client,
  name, settings)` with `standardQueue()` / `fifoQueue()` settings (`fifo`,
  `contentBasedDeduplication`, `visibilityTimeoutSeconds`, `delaySeconds`,
  `messageRetentionSeconds`, `withRedrive(deadLetterTargetArn, maxReceiveCount)`), `queueUrl(client,
  name)` (`GetQueueUrl`), `purgeQueue`, `deleteQueue`, `queueAttributes`, `queueArn`,
  `setRedrivePolicy`, `setVisibilityTimeout`, and `redrivePolicyJson`.
- Sending: `publishWith(queue, message, SendOptions)` (delay seconds, message group, deduplication
  id), `publishDelayed`, `publishToGroup`. They return the `MessageId` SQS assigned.
- Visibility: `extendVisibility(queue, messageId, seconds)`.

Also public, for code that needs the layer below: `Mq.Sqs.Client` (`SqsClient.call(action, body,
extraWaitMs)` for any SQS action, and typed `sendMessage`, `sendMessageBatch`, `receiveMessages`,
`deleteMessage`, `changeVisibility`, `changeVisibilityBatch`), `Mq.Sqs.Wire` (the JSON codec) and
`Mq.Sqs.Address` (endpoint and queue URL parsing). In `Mq.Sqs` itself: `isOpen`, `unsettledCount`,
`waitingCount`, `deduplicationIdFor`, `waitSecondsFor`.

## Tests

Run from the repository root.

Hermetic (no server needed), on each target:

```sh
lyric test --manifest mq-sqs/lyric.toml --target dotnet
lyric test --manifest mq-sqs/lyric.toml --target jvm
```

- `tests/address_tests.l` (6 tests): endpoints and queue URLs, the HTTPS-off-loopback rule, regions
  in AWS host names.
- `tests/wire_tests.l` (13 tests): every request body against literal JSON, the response parsers
  on the documents Amazon SQS and ElasticMQ send, the message encoding, the batch split.
- `tests/config_tests.l` (9 tests): what `openClient`, `connectWith`, `configFromEnv` and
  `connectFromEnv` accept, derive and refuse. They set and restore the `AWS_*` and
  `LYRIC_CONFIG_MQ_SQS_*` variables.
- `tests/reexport_tests.l` (1 test), `tests/readme_tests.l` (1 test).
- `tests/fake_sqs_tests.l` (17 tests): the client over real HTTP against an SQS server written in
  the test on `Std.HttpServer`. The server verifies the SigV4 signature of every request (and the
  session token of temporary credentials), implements visibility timeouts, receive counts, receipt
  handles, long polling, FIFO group blocking and deduplication, redrive policies and SQS's limits.
  It runs `Testing.Conformance.Mq` on FIFO queues, the standard-queue ordering check described
  above, and the backend's own behaviour: characters, settlement, batches (the ten-message and byte
  splits, partial failures), timeouts, visibility expiry, dead letters, FIFO, `Auto` mode, `close`,
  signature and error mapping, queue management, messages from other producers, and timeouts (a
  listener that accepts and never answers, 40 short deadlines in a row, every call path) against
  refused connections (port 1). The same server is also started over HTTPS
  (`Std.HttpServer.startListenerTls`, both targets) with a fixture certificate from a private CA: a
  client given that CA with `withCaCertificate` or `withExclusiveCaCertificate` creates a queue,
  publishes, consumes and acknowledges, and a client without it, or with another CA, gets
  `CONNECTION_FAILED` and never reaches the server. The certificates and the key in the file are
  labelled test fixtures. It listens on
  127.0.0.1 ports 19801 to 19814. The whole hermetic run takes about half a minute on dotnet and a
  minute and a half on jvm.

The run reports the integration module as gated (`0 test(s), module gated by inactive @cfg`).

Integration, against a real SQS-compatible server:

```sh
bash scripts/ci/start-elasticmq.sh          # prints the variables below
LYRIC_SQS_ENDPOINT=http://127.0.0.1:9324 LYRIC_SQS_REGION=elasticmq \
AWS_ACCESS_KEY_ID=elasticmq AWS_SECRET_ACCESS_KEY=elasticmq \
  lyric test --manifest mq-sqs/lyric.toml --target dotnet --features integration
```

`tests/integration/sqs_integration_tests.l` (14 tests) needs `LYRIC_SQS_ENDPOINT`,
`LYRIC_SQS_REGION`, `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` and fails naming the first one
that is unset. It creates its own queues (`lyric-it-<run>-<n>`) and deletes them: the conformance
suite on FIFO queues, standard queues, a 25-message batch, timeouts (including one over 20 s),
visibility expiry and extension, both kinds of dead-lettering, FIFO order, group blocking and
deduplication, `Auto` mode, queue management and `close`. It takes one to three minutes.
`start-elasticmq.sh` resolves ElasticMQ with Maven and needs a JDK; it accepts `ELASTICMQ_PORT`.

The same suite runs against [AWS Emulator](https://github.com/aws/aws-emulator), AWS's own local
emulator, in the `aws-emulator` CI job and by hand:

```sh
set -a; eval "$(bash scripts/ci/start-aws-emulator.sh)"; set +a   # downloads, verifies and installs the pinned package
bash scripts/ci/test-aws-emulator.sh dotnet                        # or jvm; runs mq-sqs and storage-s3
```

The emulator checks every SigV4 signature against a key it generates at start (a wrong secret is
`SignatureDoesNotMatch`, HTTP 403), which ElasticMQ does not. It serves HTTPS with a certificate
made at start, from its own CA. `test-aws-emulator.sh` passes that CA as `LYRIC_SQS_CA_FILE`, so the
suite trusts it through `withCaCertificateFile` and nothing process-wide is changed (no
`SSL_CERT_FILE`, no JVM trust store): this run is the check of the CA option against a real HTTPS
server with a private certificate, on both targets.

The integration suite also runs against Amazon SQS itself (set `LYRIC_SQS_ENDPOINT` to
`https://sqs.<region>.amazonaws.com` and real credentials), but that has not been done; see "Known
gaps".

## Pinned versions

There is no third-party library (`[nuget]` and `[maven]` are empty). What the suites run against:

| | |
|---|---|
| Interface | `Lyric.Mq` 0.1.0 and `Lyric.Testing` from the lyric-lang commit in `.lyric-lang-rev` |
| Signing | `Lyric.Aws.SigV4` 0.1.0, the version in this repository (`aws-sigv4/`) |
| SQS API | the JSON 1.0 protocol of the 2012-11-05 API |
| ElasticMQ (CI and development) | `org.elasticmq:elasticmq-server_2.13` 1.6.11 from Maven Central, `sqs-limits = strict`, started by `scripts/ci/start-elasticmq.sh` |
| AWS Emulator (CI) | 1.0.341, the signed Debian package, started by `scripts/ci/start-aws-emulator.sh`; all 14 integration tests pass on both targets |
| Runtimes | .NET 10; OpenJDK 21 in development |

## Known gaps

- **Not run against Amazon SQS.** The integration suite runs against ElasticMQ and against AWS
  Emulator. The emulator is AWS's own and verifies signatures, so signing is now checked by an AWS
  implementation as well as by the hermetic fake server and the published examples in `aws-sigv4`,
  but it is an emulator, in memory and in one region: behaviour that only the service has
  (short-poll sampling across servers, the 120000 in-flight limit, KMS, IAM policy errors,
  throttling) is untested. No issue filed.
- No retries, backoff or throttling handling; no issue filed.
- Credentials from the environment or explicit values only (no profiles, IMDS, ECS, SSO, STS).
- `MD5OfMessageBody` and `MD5OfMessageAttributes` are not verified: `Std.Hash` has no MD5. No issue
  filed.
- Binary message attributes are dropped on receipt and cannot be sent; message system attributes
  other than the receive count (`SentTimestamp`, `AWSTraceHeader`, ...) are not exposed.
- No `DeleteMessageBatch` (acknowledging several deliveries in one request), no `ListQueues`, queue
  tags, permissions, server-side encryption attributes or message move tasks.
- `Mq.DeadLetterStore` is not implemented: dead letters are a queue (`deadLetterQueueUrl`, or SQS's
  redrive policy), not a store this package can list.
- A blocked `consume` cannot be cancelled.
- **Timeouts are recognised partly by message text.** `Std.Http` reports every transport failure as
  `HttpError.ConnectionFailed` and never produces `HttpError.Timeout`. The client first asks the
  deadline's cancellation token; on `--target jvm` that is not enough, because the JDK's request
  timer ends the request up to a millisecond before the token reads as cancelled (about a third of
  the timeouts in a measurement), so `Mq.Sqs.Client.isTimeoutCause` also matches the JDK's two
  timeout messages exactly (`request timed out`, `HTTP connect timed out`) and, on dotnet, the
  message of `HttpClient.Timeout` (its 100 s cap, reachable only with `requestTimeoutMs` above
  80000). A host runtime that words these differently would turn a `TIMEOUT` into
  `CONNECTION_FAILED`. Not yet filed in lyric-lang; the fix belongs in `Std.Http` (produce
  `HttpError.Timeout`).
- On `--target jvm` the request deadline covers the time until the response headers arrive, not the
  reading of the body (`Std.Http`'s JVM kernel reads the body after the JDK's timer has stopped). A
  server that sends headers and then stalls would hold the call. SQS responses are small and sent
  whole, so this has not been observed. Not filed.
- Compiler issues met while writing this package, in `nichobbs/lyric-lang`:
  - lyric-lang#9043: on `--target jvm` the interface's inherited `requires: allHaveIds(messages)`
    is resolved in the implementing package. This package declares a private `allHaveIds` helper,
    which it uses to refuse a batch with an empty id; its name is what lets the JVM build resolve
    the clause. On `dotnet` the inherited clause is not enforced at all (the difference between
    targets noted under "Surprises"). The same issue covers a `pub val` of a linked package not
    resolving in a dependent package (`T0020`), which is why this package and `aws-sigv4` export
    constants as functions.
  - On `--target jvm`, `Std.HttpServer` exports a `JObject` that makes `Std.JsonValue.JObject`
    ambiguous in a file that imports both (the fake server qualifies it). No issue filed.
