//! Mq.ActiveMq.Extras - what ActiveMQ offers beyond `Mq.MessageQueue`:
//! topics, per-message delivery options (persistence, priority, expiry,
//! correlation id, reply-to, JMS type), JMS properties and selectors.
//!
//! Destinations. `queueDestination("orders")` is `/queue/orders`: each message
//! goes to one consumer. `topicDestination("prices")` is `/topic/prices`:
//! every subscription open at publish time gets its own copy, and nothing is
//! kept for later subscribers, so a topic consumer calls
//! `Mq.ActiveMq.subscribe` before the publisher starts. On a topic,
//! `nack(id, true)` republishes to the topic, which reaches every subscriber.
//!
//! Properties and selectors. `SendOptions.properties` become JMS properties of
//! the message. A consumer whose `ConnectOptions.selector` is set receives
//! only the messages the selector matches; a selector is an SQL-92 condition
//! over properties and JMS headers (`JMSPriority`, `JMSType`,
//! `JMSCorrelationID`, `JMSDeliveryMode`). The message id is selectable as the
//! `lyric_id` property. `Message.headers` are not properties: they travel as
//! one JSON value and cannot be selected on.
@runtime_checked
@io
package Mq.ActiveMq.Extras

import Std.Core
import Std.Collections
import Std.Time
import Std.String as Str
import Mq
import Mq.ActiveMq as ActiveMq
import Mq.ActiveMq.Stomp as Stomp

/// The STOMP destination of the queue called `name`.
pub func queueDestination(name: in String): String
  requires: name.length > 0
{
  "/queue/" + name
}

/// The STOMP destination of the topic called `name`.
pub func topicDestination(name: in String): String
  requires: name.length > 0
{
  "/topic/" + name
}

/// One JMS property of a published message.
pub record Property {
  name: String
  value: String
}

/// Per-message delivery options for `publishWith`.
pub record SendOptions {
  /// Persistent messages survive a broker restart and are dead-lettered, not
  /// discarded, when rejected. `None` sends the message as the connection's
  /// `ConnectOptions.persistent` says; `Some` decides for this message.
  persistent: Option[Bool]
  /// 0 (lowest) to 9 (highest); -1 leaves the broker's default of 4.
  priority: Int
  /// Milliseconds after which the broker discards an undelivered message;
  /// 0 = never.
  timeToLiveMs: Long
  /// Empty = not set.
  correlationId: String
  /// A STOMP destination for replies; empty = not set.
  replyTo: String
  /// The JMS type; empty = not set.
  jmsType: String
  /// JMS properties, usable in selectors.
  properties: slice[Property]
}

/// The connection's persistence, default priority, no expiry, nothing else
/// set.
pub func defaultSendOptions(): SendOptions {
  val none: slice[Property] = []
  SendOptions(
    persistent = None,
    priority = -1,
    timeToLiveMs = 0i64,
    correlationId = "",
    replyTo = "",
    jmsType = "",
    properties = none
  )
}

/// Send the message persistent (`true`) or not (`false`), whatever the
/// connection's `ConnectOptions.persistent` is.
pub func SendOptions.withPersistent(o: in SendOptions, persistent: in Bool): SendOptions {
  o.copy(persistent = Some(value = persistent))
}

func isLetter(unit: in Int): Bool {
  (unit >= 65 and unit <= 90) or (unit >= 97 and unit <= 122) or unit == 95 or unit == 36
}

func isDigit(unit: in Int): Bool {
  unit >= 48 and unit <= 57
}

/// True when `name` can be a JMS property this client may set: an ASCII
/// identifier (letters, digits, `_`, `$`, not starting with a digit) that is
/// not a JMS header (`JMS...`), a selector keyword or one of this client's
/// own `lyric_` headers.
pub func isPropertyName(name: in String): Bool {
  if name.length == 0 or not isLetter(name.codeUnitAt(0)) {
    return false
  }
  var i = 1
  while i < name.length {
    val unit = name.codeUnitAt(i)
    if not isLetter(unit) and not isDigit(unit) {
      return false
    }
    i += 1
  }
  if Str.startsWith(name, "JMS") or Str.startsWith(name, "lyric_") {
    return false
  }
  not isSelectorKeyword(Str.toUpper(name))
}

func isSelectorKeyword(upper: in String): Bool {
  upper == "NULL" or upper == "TRUE" or upper == "FALSE" or upper == "NOT" or upper == "AND" or upper == "OR" or
    upper == "BETWEEN" or
    upper == "LIKE" or
    upper == "IN" or
    upper == "IS" or
    upper == "ESCAPE"
}

/// A selector that matches messages whose property `name` equals `value`.
pub func selectorEquals(name: in String, value: in String): String
  requires: name.length > 0
{
  name + " = '" + Str.replace(value, "'", "''") + "'"
}

/// A selector that matches the message published with id `messageId`.
pub func selectorForId(messageId: in String): String {
  selectorEquals(ActiveMq.HEADER_ID, messageId)
}

func stompHeaders(options: in SendOptions): Result[slice[Stomp.Header], String] {
  if options.priority < -1 or options.priority > 9 {
    return Err("activemq: priority " + options.priority.toString() + " is outside 0..9")
  }
  if options.timeToLiveMs < 0i64 {
    return Err("activemq: timeToLiveMs must not be negative")
  }
  val headers: List[Stomp.Header] = newList()
  // Without a header of its own the SEND keeps the connection's.
  match options.persistent {
    case Some(persistent) -> headers.add(
      Stomp.Header(name = "persistent", value = if persistent { "true" } else { "false" })
    )
    case None -> ()
  }
  if options.priority >= 0 {
    headers.add(Stomp.Header(name = "priority", value = options.priority.toString()))
  }
  if options.timeToLiveMs > 0i64 {
    // ActiveMQ reads `expires` as an absolute time in epoch milliseconds.
    headers.add(Stomp.Header(name = "expires", value = (nowEpochMillis() + options.timeToLiveMs).toString()))
  }
  if options.correlationId.length > 0 {
    headers.add(Stomp.Header(name = "correlation-id", value = options.correlationId))
  }
  if options.replyTo.length > 0 {
    headers.add(Stomp.Header(name = "reply-to", value = options.replyTo))
  }
  if options.jmsType.length > 0 {
    headers.add(Stomp.Header(name = "type", value = options.jmsType))
  }
  val seen: Map[String, Bool] = newMap()
  for p in options.properties {
    if not isPropertyName(p.name) {
      return Err("activemq: '" + p.name + "' cannot be used as a JMS property name")
    }
    if seen.containsKey(p.name) {
      return Err("activemq: property '" + p.name + "' is set twice")
    }
    seen[p.name] = true
    headers.add(Stomp.Header(name = p.name, value = p.value))
  }
  Ok(headers.toArray())
}

/// Publish `message` with `options`. Like `publish`, it returns once the
/// broker has confirmed the message.
pub func publishWith(
  queue: in ActiveMq.ActiveMqQueue, message: in Mq.Message, options: in SendOptions
): Result[Unit, String] {
  match stompHeaders(options) {
    case Ok(headers) -> ActiveMq.publishWithHeaders(queue, message, headers)
    case Err(e) -> Err(e)
  }
}
