//! Mq.RabbitMq.Extras - what RabbitMQ offers beyond `Mq.MessageQueue`:
//! exchanges and bindings, queue arguments (quorum queues, dead-lettering,
//! TTL, length limits, priorities), per-message properties (persistence,
//! priority, expiration, correlation id, reply-to, type) and publishing to
//! an exchange with a routing key.
//!
//! Queue arguments are part of the queue's identity: the `with...` functions
//! below add them to `ConnectOptions.queueArguments`, and the broker refuses
//! (`406 PRECONDITION_FAILED`) a declaration whose arguments differ from the
//! existing queue's.
//!
//!   val options = Mq.RabbitMq.optionsFromUrl(url, "orders")?
//!     .withQuorumQueue()
//!     .withDeliveryLimit(5)
//!     .withDeadLetter("", "orders.dead")
//!   val queue = Mq.RabbitMq.connectWith(options)?
//!   Extras.declareQueue(queue, "orders.dead", Extras.defaultQueueOptions())?
//!
//! With a dead-letter exchange, `nack(id, false)` (and a message that expires
//! or exceeds the delivery limit) is republished by the broker to that
//! exchange instead of being discarded. The dead-lettered copy carries an
//! `x-death` header, which a consumer of the dead-letter queue receives as
//! JSON text.
@runtime_checked
@io
package Mq.RabbitMq.Extras

import Std.Core
import Std.Collections
import Std.String as Str
import Mq
import Mq.RabbitMq as RabbitMq
import Mq.RabbitMq.Amqp as Amqp

// ─── Queue arguments ─────────────────────────────────────────────────────────
// `table` with the entry called `name` replaced, or added at the end.
func withEntry(table: in slice[Amqp.TableEntry], name: in String, value: in Amqp.FieldValue): slice[Amqp.TableEntry] {
  val entries: List[Amqp.TableEntry] = newList()
  var replaced = false
  for e in table {
    if e.name == name {
      entries.add(Amqp.entry(name, value))
      replaced = true
    } else {
      entries.add(e)
    }
  }
  if not replaced {
    entries.add(Amqp.entry(name, value))
  }
  entries.toArray()
}

/// Set one queue argument by name; an argument of the same name is replaced.
pub func ConnectOptions.withQueueArgument(
  o: in RabbitMq.ConnectOptions, name: in String, value: in Amqp.FieldValue
): RabbitMq.ConnectOptions
  requires: name.length > 0
{
  o.copy(queueArguments = withEntry(o.queueArguments, name, value))
}

/// Declare the queue as a quorum queue (`x-queue-type = quorum`): replicated,
/// always durable, and the only type that counts deliveries
/// (`Message.deliveryCount` beyond 1, `withDeliveryLimit`).
pub func ConnectOptions.withQuorumQueue(o: in RabbitMq.ConnectOptions): RabbitMq.ConnectOptions {
  o.copy(durable = true, queueArguments = withEntry(o.queueArguments, "x-queue-type", Amqp.fText("quorum")))
}

/// Dead-letter rejected, expired and over-limit messages to `exchange` with
/// `routingKey` (`x-dead-letter-exchange`, `x-dead-letter-routing-key`). The
/// default exchange `""` with a queue name as the routing key sends them
/// straight to that queue. An empty `routingKey` keeps each message's own.
pub func ConnectOptions.withDeadLetter(
  o: in RabbitMq.ConnectOptions, exchange: in String, routingKey: in String
): RabbitMq.ConnectOptions {
  val withExchange = withEntry(o.queueArguments, "x-dead-letter-exchange", Amqp.fText(exchange))
  if routingKey.length == 0 {
    return o.copy(queueArguments = withExchange)
  }
  o.copy(queueArguments = withEntry(withExchange, "x-dead-letter-routing-key", Amqp.fText(routingKey)))
}

/// Expire messages that wait in the queue longer than `milliseconds`
/// (`x-message-ttl`).
pub func ConnectOptions.withMessageTtl(o: in RabbitMq.ConnectOptions, milliseconds: in Long): RabbitMq.ConnectOptions
  requires: milliseconds >= 0i64
{
  o.copy(queueArguments = withEntry(o.queueArguments, "x-message-ttl", Amqp.fLong(milliseconds)))
}

/// Delete the queue after it has been unused for `milliseconds`
/// (`x-expires`).
pub func ConnectOptions.withQueueExpiry(o: in RabbitMq.ConnectOptions, milliseconds: in Long): RabbitMq.ConnectOptions
  requires: milliseconds > 0i64
{
  o.copy(queueArguments = withEntry(o.queueArguments, "x-expires", Amqp.fLong(milliseconds)))
}

/// Keep at most `messages` in the queue (`x-max-length`); the broker drops
/// or dead-letters the oldest beyond that.
pub func ConnectOptions.withMaxLength(o: in RabbitMq.ConnectOptions, messages: in Long): RabbitMq.ConnectOptions
  requires: messages >= 0i64
{
  o.copy(queueArguments = withEntry(o.queueArguments, "x-max-length", Amqp.fLong(messages)))
}

/// Make the queue a priority queue with priorities 0 to `levels`
/// (`x-max-priority`, classic queues only). Without it `PublishOptions.priority`
/// is carried but does not reorder deliveries.
pub func ConnectOptions.withMaxPriority(o: in RabbitMq.ConnectOptions, levels: in Int): RabbitMq.ConnectOptions
  requires: levels >= 1 and levels <= 255
{
  o.copy(queueArguments = withEntry(o.queueArguments, "x-max-priority", Amqp.fInt(levels)))
}

/// Dead-letter (or drop) a message once it has been redelivered `limit`
/// times (`x-delivery-limit`, quorum queues only).
pub func ConnectOptions.withDeliveryLimit(o: in RabbitMq.ConnectOptions, limit: in Int): RabbitMq.ConnectOptions
  requires: limit >= 0
{
  o.copy(queueArguments = withEntry(o.queueArguments, "x-delivery-limit", Amqp.fInt(limit)))
}

// ─── Exchanges and queues ────────────────────────────────────────────────────
/// How an exchange is declared.
pub record ExchangeOptions {
  /// Whether the exchange survives a broker restart.
  durable: Bool
  /// Whether the broker deletes it when its last binding goes.
  autoDelete: Bool
  /// An internal exchange accepts messages from other exchanges only.
  isInternal: Bool
  arguments: slice[Amqp.TableEntry]
}

/// A durable exchange that is neither auto-deleted nor internal.
pub func defaultExchangeOptions(): ExchangeOptions {
  ExchangeOptions(durable = true, autoDelete = false, isInternal = false, arguments = Amqp.emptyTable())
}

func unitReply(reply: in Result[Amqp.Method, String]): Result[Unit, String] {
  match reply {
    case Ok(_) -> Ok(())
    case Err(e) -> Err(e)
  }
}

/// Declare the exchange `name` of type `kind` (`direct`, `fanout`, `topic`,
/// `headers`). Declaring an exchange that exists with the same properties is
/// a no-op; with different ones the broker refuses (`406`).
pub func declareExchange(
  q: in RabbitMq.RabbitMqQueue, name: in String, kind: in String, options: in ExchangeOptions
): Result[Unit, String]
  requires: name.length > 0
{
  unitReply(
    RabbitMq.request(
      q,
      Amqp.ExchangeDeclare(
        exchange = name,
        kind = kind,
        passive = false,
        durable = options.durable,
        autoDelete = options.autoDelete,
        isInternal = options.isInternal,
        arguments = options.arguments
      )
    )
  )
}

/// Delete the exchange `name`; with `ifUnused`, only when it has no bindings
/// (`406` otherwise). Deleting an exchange that does not exist succeeds.
pub func deleteExchange(q: in RabbitMq.RabbitMqQueue, name: in String, ifUnused: in Bool): Result[Unit, String]
  requires: name.length > 0
{
  unitReply(RabbitMq.request(q, Amqp.ExchangeDelete(exchange = name, ifUnused = ifUnused)))
}

/// Bind the connection's queue to `exchange` for `routingKey`.
pub func bindQueue(q: in RabbitMq.RabbitMqQueue, exchange: in String, routingKey: in String): Result[Unit, String] {
  bindQueueWith(q, exchange, routingKey, Amqp.emptyTable())
}

/// `bindQueue` with binding arguments (the match table of a `headers`
/// exchange).
pub func bindQueueWith(
  q: in RabbitMq.RabbitMqQueue,
  exchange: in String,
  routingKey: in String,
  arguments: in slice[Amqp.TableEntry],
): Result[Unit, String] {
  unitReply(
    RabbitMq.request(
      q,
      Amqp.QueueBind(queue = RabbitMq.queueName(q), exchange = exchange, routingKey = routingKey, arguments = arguments)
    )
  )
}

/// How another queue is declared with `declareQueue`.
pub record QueueOptions {
  durable: Bool
  autoDelete: Bool
  arguments: slice[Amqp.TableEntry]
}

/// A durable queue without arguments.
pub func defaultQueueOptions(): QueueOptions {
  QueueOptions(durable = true, autoDelete = false, arguments = Amqp.emptyTable())
}

/// What the broker reports about a queue.
pub record QueueInfo {
  name: String
  /// Messages ready for delivery (unsettled deliveries are not counted).
  messageCount: Long
  consumerCount: Long
}

func queueInfo(reply: in Result[Amqp.Method, String]): Result[QueueInfo, String] {
  match reply {
    case Err(e) -> Err(e)
    case Ok(QueueDeclareOk(name, messages, consumers)) -> {
      Ok(QueueInfo(name = name, messageCount = messages, consumerCount = consumers))
    }
    case Ok(_) -> Err("rabbitmq: unexpected reply to queue.declare")
  }
}

/// Declare another queue on the same connection, a dead-letter queue for
/// one.
pub func declareQueue(
  q: in RabbitMq.RabbitMqQueue, name: in String, options: in QueueOptions
): Result[QueueInfo, String]
  requires: name.length > 0
{
  queueInfo(
    RabbitMq.request(
      q,
      Amqp.QueueDeclare(
        queue = name,
        passive = false,
        durable = options.durable,
        exclusive = false,
        autoDelete = options.autoDelete,
        arguments = options.arguments
      )
    )
  )
}

/// The broker's counts for the queue `name`, which must exist (`404`
/// otherwise).
pub func inspectQueue(q: in RabbitMq.RabbitMqQueue, name: in String): Result[QueueInfo, String]
  requires: name.length > 0
{
  queueInfo(
    RabbitMq.request(
      q,
      Amqp.QueueDeclare(
        queue = name,
        passive = true,
        durable = false,
        exclusive = false,
        autoDelete = false,
        arguments = Amqp.emptyTable()
      )
    )
  )
}

func countReply(reply: in Result[Amqp.Method, String]): Result[Long, String] {
  match reply {
    case Err(e) -> Err(e)
    case Ok(QueuePurgeOk(n)) -> Ok(n)
    case Ok(QueueDeleteOk(n)) -> Ok(n)
    case Ok(_) -> Err("rabbitmq: unexpected reply")
  }
}

/// Remove every message waiting in the queue `name` (unsettled deliveries
/// stay). Returns how many were removed.
pub func purgeQueue(q: in RabbitMq.RabbitMqQueue, name: in String): Result[Long, String]
  requires: name.length > 0
{
  countReply(RabbitMq.request(q, Amqp.QueuePurge(queue = name)))
}

/// Delete the queue `name` with its messages. Returns how many messages it
/// held. Deleting a queue that does not exist succeeds.
pub func deleteQueue(q: in RabbitMq.RabbitMqQueue, name: in String): Result[Long, String]
  requires: name.length > 0
{
  countReply(RabbitMq.request(q, Amqp.QueueDelete(queue = name, ifUnused = false, ifEmpty = false)))
}

// ─── Publishing with properties ──────────────────────────────────────────────
/// Where and how one message is published.
pub record PublishOptions {
  /// The exchange; `""` is the default exchange, which routes by queue name.
  exchange: String
  routingKey: String
  /// Fail with an `Err` when no queue receives the message. Without it an
  /// unroutable message is dropped by the broker and `publish` returns `Ok`.
  mandatory: Bool
  /// Persistent messages are written to disk in a durable queue.
  persistent: Bool
  /// 0 to 255; 0 is the lowest and the default. Only a queue declared with
  /// `withMaxPriority` orders by it.
  priority: Int
  /// Milliseconds after which the broker discards (or dead-letters) the
  /// message if it is still waiting; 0 = never.
  expirationMs: Long
  contentType: String
  contentEncoding: String
  correlationId: String
  replyTo: String
  /// The `type` property: an application-defined message type name.
  kind: String
  appId: String
  /// Seconds since the Unix epoch; 0 sends no timestamp.
  timestamp: Long
}

/// Options that publish the way `Mq.publish` does: to the connection's queue
/// through the default exchange, mandatory, with the connection's
/// persistence and no other property.
pub func defaultPublishOptions(q: in RabbitMq.RabbitMqQueue): PublishOptions {
  PublishOptions(
    exchange = "",
    routingKey = RabbitMq.queueName(q),
    mandatory = true,
    persistent = q.options.persistent,
    priority = 0,
    expirationMs = 0i64,
    contentType = "",
    contentEncoding = "",
    correlationId = "",
    replyTo = "",
    kind = "",
    appId = "",
    timestamp = 0i64
  )
}

/// Options that publish to `exchange` with `routingKey`, otherwise as
/// `defaultPublishOptions`.
pub func toExchange(q: in RabbitMq.RabbitMqQueue, exchange: in String, routingKey: in String): PublishOptions {
  defaultPublishOptions(q).copy(exchange = exchange, routingKey = routingKey)
}

/// The AMQP properties `options` stand for.
pub func propertiesOf(options: in PublishOptions): Result[Amqp.Properties, String] {
  if options.priority < 0 or options.priority > 255 {
    return Err("rabbitmq: priority " + options.priority.toString() + " is out of range (0 to 255)")
  }
  if options.expirationMs < 0i64 {
    return Err("rabbitmq: expirationMs must not be negative")
  }
  if options.timestamp < 0i64 {
    return Err("rabbitmq: timestamp must not be negative")
  }
  Ok(
    Amqp.noProperties().copy(
      contentType = options.contentType,
      contentEncoding = options.contentEncoding,
      deliveryMode = if options.persistent { 2 } else { 1 },
      priority = options.priority,
      correlationId = options.correlationId,
      replyTo = options.replyTo,
      expiration = if options.expirationMs > 0i64 { options.expirationMs.toString() } else { "" },
      timestamp = options.timestamp,
      kind = options.kind,
      appId = options.appId
    )
  )
}

/// Publish `message` as `options` say. Like `Mq.publish`, it returns once the
/// broker has confirmed the message.
pub func publishWith(
  q: in RabbitMq.RabbitMqQueue, message: in Mq.Message, options: in PublishOptions
): Result[Unit, String] {
  val properties = match propertiesOf(options) { case Ok(p) -> p; case Err(e) -> return Err(e) }
  RabbitMq.publishTo(q, options.exchange, options.routingKey, options.mandatory, message, properties)
}
