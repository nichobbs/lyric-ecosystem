//! Mq.RabbitMq - RabbitMQ backend for `Mq.MessageQueue` and
//! `Mq.QueueConsumer`, over a pure-Lyric AMQP 0-9-1 client.
//!
//! One `RabbitMqQueue` is one connection to the broker (plain TCP for
//! `amqp://`, TLS for `amqps://`), one channel on it and one queue. It
//! implements both interfaces, so the same value publishes and consumes.
//!
//!   import Mq.RabbitMq
//!
//!   val queue = Mq.RabbitMq.connectUrl("amqp://app:secret@broker:5672/prod", "orders")?
//!   Mq.publish(queue, Mq.Message(id = "o-1", body = "{}", headers = [], deliveryCount = 0))?
//!   Mq.consume(queue, 5000, { m -> handle(m) })?
//!   Mq.ack(queue, "o-1")?
//!   queue.close()
//!
//! How the interface maps onto the broker:
//!
//! - Connecting declares the queue (durable unless the options say
//!   otherwise) and puts the channel in publisher-confirm mode.
//! - `publish` is a `basic.publish` to the default exchange with the queue
//!   name as the routing key and the `mandatory` flag. It returns `Ok` only
//!   after the broker's `basic.ack` for that message; a `basic.nack` or a
//!   `basic.return` (the message could not be routed) is an `Err`.
//!   `publishBatch` sends the whole batch and then waits for every confirm.
//!   Confirms are per message: a batch is not atomic.
//! - A message's `id` is the `message-id` property and its body is UTF-8
//!   text. Its headers travel twice in the `headers` table: exactly (order
//!   and repeated keys included) as a JSON array in the `lyric_headers`
//!   entry, which is what a consumer of this package reads, and as one plain
//!   long-string entry per distinct key for other AMQP consumers and for
//!   `headers` exchanges. See `headersOf`.
//! - The first `consume` sets the prefetch window (`basic.qos`) and starts a
//!   consumer (`basic.consume`, manual acknowledgement). The broker pushes
//!   deliveries; `consume` hands the handler the oldest one not yet handed
//!   out. `consume(0, ...)` looks at what has already arrived and never
//!   waits.
//! - `ack(id)` is `basic.ack` for the oldest unsettled delivery with that
//!   message id; `nack(id, true)` is `basic.nack` with `requeue`;
//!   `nack(id, false)` is `basic.nack` without it: the broker dead-letters
//!   the message when the queue has a dead-letter exchange and discards it
//!   otherwise.
//! - `deliveryCount` is the `x-delivery-count` header a quorum queue adds, or
//!   1 when the broker flags the delivery `redelivered` and there is no such
//!   header (a classic queue records only that flag, so the count stops at 1).
//! - Header values of other types (from another producer) are delivered as
//!   text; see `fieldText`.
//! - `close` closes the channel and the connection with the AMQP close
//!   handshake. Deliveries not yet settled go back to the queue.
//!
//! Heartbeats are off by default (`heartbeatSeconds = 0`): the client is
//! synchronous and has no thread to send them from. With a heartbeat
//! negotiated the client sends heartbeat frames whenever it is inside an
//! operation (a blocked `consume`, a publish waiting for its confirm) and
//! notices a silent broker there; between operations nothing is sent, so an
//! application that idles longer than twice the interval is disconnected by
//! the broker unless it calls `keepAlive` in the meantime.
//!
//! A channel error (`404 NOT_FOUND`, `406 PRECONDITION_FAILED`, ...) fails
//! the operation that caused it; the client then opens a fresh channel, so
//! the connection stays usable. Deliveries that were unsettled on the old
//! channel are requeued by the broker and can no longer be acknowledged.
//!
//! A `RabbitMqQueue` may be shared between threads: operations take the
//! connection's lock one at a time, and a blocked `consume` releases it every
//! 100 ms so a publisher on another thread is not held for the whole wait.
//! The handler runs outside the lock and may call `ack` / `nack` itself.
//!
//! There is no automatic reconnect. After a transport failure every operation
//! returns `Err`; open a new connection.
//!
//! TLS: an `amqps://` URL, or `withTls()` on the options, runs a TLS
//! handshake before the AMQP protocol header. The server certificate is
//! verified against the platform trust store and its name against the host,
//! unless the options say otherwise:
//!
//!   val options = Mq.RabbitMq.optionsFromUrl("amqps://app:secret@broker:5671", "orders")?
//!     .withCaCertificate(caPem)
//!     .withClientIdentity(certPem, keyPem)
//!   val queue = Mq.RabbitMq.connectWith(options)?
//!
//! The option names and the rule for skipping verification
//! (`withInsecureSkipVerify()` and `LYRIC_TLS_ALLOW_INSECURE=1` together) are
//! those of `Std.Http.HttpClientBuilder` and of `Lyric.Redis`.
@runtime_checked
@io
package Mq.RabbitMq

import Std.Core
import Std.Collections
import Std.Parse
import Std.Math as Math
import Std.String as Str
import Std.Environment as Env
import Std.Console as Console
import Std.Tls as Tls
import Net.Tcp as K
import Net.Tls
import Mq.RabbitMq.Amqp as Amqp
import Mq.RabbitMq.Url as Url
import Mq
import Std.Encoding
pub use Mq
pub use Net.Tls

/// The environment variable that must be `1` for `withInsecureSkipVerify()`
/// to take effect (the standard library's, docs/61 §4 in lyric-lang).
pub val INSECURE_ALLOW_ENV: String = "LYRIC_TLS_ALLOW_INSECURE"

/// The header a quorum queue adds to a redelivered message. It is reported
/// as `Message.deliveryCount` and not as a header.
pub val HEADER_DELIVERY_COUNT: String = "x-delivery-count"

/// The header-table entry that carries `Message.headers` exactly: a JSON
/// array of `[key, value]` pairs. A broker may reorder a table's entries and
/// merge entries with one key (RabbitMQ 4 does both); it cannot touch the
/// inside of one value.
pub val HEADER_HEADERS: String = "lyric_headers"

/// The version this client reports to the broker.
pub val CLIENT_VERSION: String = "0.1.0"

/// How to reach a broker and which queue to use.
pub record ConnectOptions {
  host: String
  port: Int
  user: String
  password: String
  /// The virtual host; `/` is the broker's default.
  virtualHost: String
  /// The queue this connection publishes to and consumes from.
  queue: String
  /// `true` (the default) declares the queue when connecting, with the
  /// properties below; the broker refuses (`406`) a declaration that differs
  /// from an existing queue. `false` only checks that the queue exists
  /// (`404` otherwise) and ignores `durable`, `autoDelete` and
  /// `queueArguments`.
  declare: Bool
  /// Whether the queue survives a broker restart.
  durable: Bool
  /// Whether the broker deletes the queue when its last consumer leaves.
  autoDelete: Bool
  /// The queue's optional arguments (`x-queue-type`, `x-dead-letter-exchange`,
  /// `x-message-ttl`, ...). `Mq.RabbitMq.Extras` builds the common ones.
  queueArguments: slice[Amqp.TableEntry]
  /// `Manual` (the default): the application calls `ack` / `nack`. `Auto`:
  /// `consume` acknowledges when the handler returns `Ok` and requeues when it
  /// returns `Err`; `ack` and `nack` are then no-ops.
  ackMode: Mq.AckMode
  /// Whether published messages are persistent (delivery mode 2: written to
  /// disk in a durable queue).
  persistent: Bool
  /// The most deliveries the broker may leave unsettled on this connection
  /// (`basic.qos`), which is also how many messages the client may hold
  /// before a handler has seen them. 0 = no limit.
  prefetchCount: Int
  /// The heartbeat interval to ask for, in seconds; 0 (the default) turns
  /// heartbeats off. See the package comment for what a non-zero value
  /// requires of the application.
  heartbeatSeconds: Int
  /// The largest frame to accept and send, in bytes (at least 4096). The
  /// smaller of this and the broker's limit is used; bodies are split to fit.
  frameMax: Int
  /// Milliseconds to wait for the TCP connection, and again for the TLS
  /// handshake of a TLS connection; 0 = the system's limit.
  connectTimeoutMs: Int
  /// Milliseconds to wait for the broker to answer a request or confirm a
  /// publish.
  requestTimeoutMs: Int
  /// The largest message body accepted from the broker, in bytes. A larger
  /// delivery is rejected without requeueing and reported as an `Err`.
  maxMessageBytes: Int
  /// A name for the connection, shown in the broker's management UI; empty
  /// sends none.
  connectionName: String
  /// TLS settings; `plainTcp()` for an `amqp://` listener.
  tls: TlsOptions
}

/// Options for `queue` on `host:port` with the defaults: plain TCP, the
/// `guest` user, the `/` virtual host, a durable queue declared on connect,
/// manual acknowledgement, persistent messages, a prefetch of 10, no
/// heartbeat, a 128 KiB frame limit, a 10 s connect and request timeout and a
/// 16 MiB message limit. Adjust with `.copy(...)` and the `with...` functions.
pub func defaultOptions(host: in String, port: in Int, queue: in String): ConnectOptions {
  ConnectOptions(
    host = host,
    port = port,
    user = Url.DEFAULT_USER,
    password = Url.DEFAULT_USER,
    virtualHost = Url.DEFAULT_VHOST,
    queue = queue,
    declare = true,
    durable = true,
    autoDelete = false,
    queueArguments = Amqp.emptyTable(),
    ackMode = Mq.AckMode.Manual,
    persistent = true,
    prefetchCount = 10,
    heartbeatSeconds = 0,
    frameMax = 131072,
    connectTimeoutMs = 10000,
    requestTimeoutMs = 10000,
    maxMessageBytes = 16777216,
    connectionName = "",
    tls = plainTcp()
  )
}

/// Connect over TLS, verifying the server certificate against the platform
/// trust store and its name against `host`.
pub func ConnectOptions.withTls(o: in ConnectOptions): ConnectOptions {
  o.copy(tls = o.tls.withTls())
}

/// Check the server certificate against `name` instead of `host`, and send
/// `name` as SNI. For a broker reached by address or through a tunnel.
pub func ConnectOptions.withTlsServerName(o: in ConnectOptions, name: in String): ConnectOptions {
  o.copy(tls = o.tls.withTlsServerName(name))
}

/// Trust the CA certificate in `pem` in addition to the platform trust
/// store. `pem` holds one certificate.
pub func ConnectOptions.withCaCertificate(o: in ConnectOptions, pem: in String): ConnectOptions {
  o.copy(tls = o.tls.withCaCertificate(pem))
}

/// Trust only the CA certificate in `pem`; the platform trust store is not
/// consulted. Wins over `withCaCertificate`.
pub func ConnectOptions.withExclusiveCaCertificate(o: in ConnectOptions, pem: in String): ConnectOptions {
  o.copy(tls = o.tls.withExclusiveCaCertificate(pem))
}

/// Present a client certificate (mutual TLS). `keyPem` is the certificate's
/// unencrypted PKCS8 private key.
pub func ConnectOptions.withClientIdentity(
  o: in ConnectOptions, certificatePem: in String, keyPem: in String
): ConnectOptions {
  o.copy(tls = o.tls.withClientIdentity(certificatePem, keyPem))
}

/// Refuse a TLS version older than `version`.
pub func ConnectOptions.withMinTlsVersion(o: in ConnectOptions, version: in Tls.TlsVersion): ConnectOptions {
  o.copy(tls = o.tls.withMinTlsVersion(version))
}

/// Ask for certificate and name verification to be skipped. This alone does
/// nothing: `LYRIC_TLS_ALLOW_INSECURE=1` must also be set when the
/// connection is opened, otherwise verification stays on and a warning goes
/// to standard error. Never for production.
pub func ConnectOptions.withInsecureSkipVerify(o: in ConnectOptions): ConnectOptions {
  o.copy(tls = o.tls.withInsecureSkipVerify())
}

/// One delivery from the broker, before it becomes an `Mq.Message`.
pub record Delivery {
  tag: Long
  redelivered: Bool
  properties: Amqp.Properties
  body: slice[Byte]
  /// The body was larger than `maxMessageBytes` and was not kept.
  oversize: Bool
  declaredSize: Long
}

/// A message the broker handed back because it could not be routed.
pub record Returned {
  replyCode: Int
  replyText: String
  messageId: String
}

/// An open connection to one RabbitMQ queue. Obtain one from `connectWith`,
/// `connectUrl` or `connectFromEnv`.
pub ref record RabbitMqQueue {
  options: ConnectOptions
  sock: K.Sock
  decoder: Amqp.Decoder
  lock: K.Lock
  var server: String
  var lost: String
  var closed: Bool
  var frameMax: Int
  var heartbeatMs: Long
  var lastSent: Long
  var lastReceived: Long
  var lastCode: Int
  var blocked: String
  var consuming: Bool
  var publishSeq: Long
  var unconfirmed: List[Long]
  var nacked: List[Long]
  var returned: List[Returned]
  var backlog: List[Delivery]
  var inFlight: Map[String, List[Long]]
  // The content-carrying method whose header and body frames are arriving:
  // 0 = none, 1 = basic.deliver, 2 = basic.return.
  var pendingKind: Int
  var pendingTag: Long
  var pendingRedelivered: Bool
  var pendingCode: Int
  var pendingText: String
  var pendingHasHeader: Bool
  var pendingProperties: Amqp.Properties
  var pendingSize: Long
  var pendingGot: Long
  var pendingDiscard: Bool
  var pendingChunks: List[slice[Byte]]
}

val EMPTY_ID: String = "rabbitmq: empty message id"

val CHANNEL: Int = 1

val WAIT_SLICE_MS: Int = 100

val READ_CHUNK_BYTES: Int = 65536

// One frame, method or message from the broker.
union Event {
  case EvMethod(channel: Int, method: Amqp.Method)
  case EvDelivery(delivery: Delivery)
  case EvReturn(returned: Returned)
}

// What one step of reading produced for the operation in progress.
union Step {
  // A synchronous reply (or any method the client does not handle itself).
  case Reply(method: Amqp.Method)
  // Something asynchronous was handled: a confirm, a delivery, a return.
  case Progress
  // Nothing arrived within the wait.
  case Idle
}

/// The specification's name for a reply code (`NOT_FOUND` for 404), or an
/// empty string for a code it does not define.
pub func replyCodeName(code: in Int): String {
  if code == 200 {
    "REPLY_SUCCESS"
  } else if code == 311 {
    "CONTENT_TOO_LARGE"
  } else if code == 312 {
    "NO_ROUTE"
  } else if code == 313 {
    "NO_CONSUMERS"
  } else if code == 320 {
    "CONNECTION_FORCED"
  } else if code == 402 {
    "INVALID_PATH"
  } else if code == 403 {
    "ACCESS_REFUSED"
  } else if code == 404 {
    "NOT_FOUND"
  } else if code == 405 {
    "RESOURCE_LOCKED"
  } else if code == 406 {
    "PRECONDITION_FAILED"
  } else if code == 501 {
    "FRAME_ERROR"
  } else if code == 502 {
    "SYNTAX_ERROR"
  } else if code == 503 {
    "COMMAND_INVALID"
  } else if code == 504 {
    "CHANNEL_ERROR"
  } else if code == 505 {
    "UNEXPECTED_FRAME"
  } else if code == 506 {
    "RESOURCE_ERROR"
  } else if code == 530 {
    "NOT_ALLOWED"
  } else if code == 540 {
    "NOT_IMPLEMENTED"
  } else if code == 541 {
    "INTERNAL_ERROR"
  } else {
    ""
  }
}

// `404 NOT_FOUND - no queue 'x'`: the code, then the broker's text, or the
// specification's name when the broker sent none.
func replyText(code: in Int, text: in String): String {
  val name = replyCodeName(code)
  if text.length > 0 {
    code.toString() + " " + text
  } else if name.length > 0 {
    code.toString() + " " + name
  } else {
    code.toString()
  }
}

// ─── Transport (callers hold the lock) ───────────────────────────────────────
func markLost(q: in RabbitMqQueue, reason: in String): String {
  if q.lost.length == 0 {
    q.lost = reason
    K.sockClose(q.sock)
  }
  "rabbitmq: connection lost: " + q.lost
}

func checkUsable(q: in RabbitMqQueue): Result[Unit, String] {
  if q.closed {
    return Err("rabbitmq: the connection is closed")
  }
  if q.lost.length > 0 {
    return Err("rabbitmq: connection lost: " + q.lost)
  }
  Ok(())
}

func writeBytes(q: in RabbitMqQueue, bytes: in slice[Byte]): Result[Unit, String] {
  match K.sockWriteWithin(q.sock, bytes, q.options.requestTimeoutMs) {
    case Ok(_) -> {
      q.lastSent = K.monotonicMillis()
      Ok(())
    }
    case Err(e) -> Err(markLost(q, e))
  }
}

func writeMethod(q: in RabbitMqQueue, channel: in Int, m: in Amqp.Method): Result[Unit, String] {
  val bytes: slice[Byte] = match Amqp.methodFrame(channel, m) {
    case Ok(b) -> b
    case Err(e) -> return Err("rabbitmq: " + Amqp.methodName(m) + ": " + e)
  }
  writeBytes(q, bytes)
}

// Send a heartbeat frame when one is due: half the negotiated interval after
// the last write, as the specification recommends.
func beat(q: in RabbitMqQueue): Result[Unit, String] {
  if q.heartbeatMs > 0i64 and K.monotonicMillis() - q.lastSent >= q.heartbeatMs / 2i64 {
    return writeBytes(q, Amqp.heartbeatFrame())
  }
  Ok(())
}

func closedByPeer(q: in RabbitMqQueue): String {
  val idle = K.monotonicMillis() - q.lastSent
  if q.heartbeatMs > 0i64 and idle > 2i64 * q.heartbeatMs {
    return "the broker closed the connection; this client had sent nothing for " + (idle / 1000i64).toString() +
      " s with a heartbeat of " +
      (q.heartbeatMs / 1000i64).toString() +
      " s (call keepAlive between operations)"
  }
  "the broker closed the connection"
}

// The next frame, waiting at most `timeoutMs` for it (0 = only what has
// already arrived). `Ok(None)` is a timeout. Heartbeats are sent while
// waiting, and a broker that stays silent for two intervals is given up on.
func readFrame(q: in RabbitMqQueue, timeoutMs: in Int): Result[Option[Amqp.Frame], String] {
  val deadline = K.monotonicMillis() + timeoutMs.toLong()
  while true {
    match Amqp.nextFrame(q.decoder) {
      case Err(e) -> return Err(markLost(q, e))
      case Ok(Some(frame)) -> return Ok(Some(value = frame))
      case Ok(None) -> ()
    }
    match beat(q) {
      case Err(e) -> return Err(e)
      case Ok(_) -> ()
    }
    val now = K.monotonicMillis()
    var remaining = deadline - now
    if remaining < 0i64 {
      remaining = 0i64
    }
    if q.heartbeatMs > 0i64 {
      var untilBeat = q.heartbeatMs / 2i64 - (now - q.lastSent)
      if untilBeat < 1i64 {
        untilBeat = 1i64
      }
      if remaining > untilBeat {
        remaining = untilBeat
      }
    }
    match K.sockRead(q.sock, READ_CHUNK_BYTES, remaining.toInt()) {
      case Err(e) -> return Err(markLost(q, e))
      case Ok(Eof) -> return Err(markLost(q, closedByPeer(q)))
      case Ok(Data(bytes)) -> {
        q.lastReceived = K.monotonicMillis()
        Amqp.feed(q.decoder, bytes)
      }
      case Ok(TimedOut) -> {
        val after = K.monotonicMillis()
        if q.heartbeatMs > 0i64 and after - q.lastReceived > 2i64 * q.heartbeatMs {
          return Err(
            markLost(
              q,
              "no frame from the broker for " + ((after - q.lastReceived) / 1000i64).toString() + " s (heartbeat " +
                (q.heartbeatMs / 1000i64).toString() +
                " s)"
            )
          )
        }
        if after >= deadline {
          return Ok(None)
        }
      }
    }
  }
  Ok(None)
}

func clearPending(q: in RabbitMqQueue): Unit {
  q.pendingKind = 0
  q.pendingHasHeader = false
  q.pendingSize = 0i64
  q.pendingGot = 0i64
  q.pendingDiscard = false
  q.pendingChunks = newList()
}

// The event the fully received content amounts to.
func finishContent(q: in RabbitMqQueue): Event {
  val event = if q.pendingKind == 1 {
    val body: slice[Byte] = Amqp.joinChunks(q.pendingChunks)
    EvDelivery(
      delivery = Delivery(
        tag = q.pendingTag,
        redelivered = q.pendingRedelivered,
        properties = q.pendingProperties,
        body = body,
        oversize = q.pendingDiscard,
        declaredSize = q.pendingSize
      )
    )
  } else {
    EvReturn(
      returned = Returned(replyCode = q.pendingCode, replyText = q.pendingText, messageId = q.pendingProperties.messageId)
    )
  }
  clearPending(q)
  event
}

// The next event, waiting at most `timeoutMs`. Content frames are assembled
// here: a delivery or a return is one event however many frames it took.
func nextEvent(q: in RabbitMqQueue, timeoutMs: in Int): Result[Option[Event], String] {
  val deadline = K.monotonicMillis() + timeoutMs.toLong()
  while true {
    val remaining = deadline - K.monotonicMillis()
    val wait = if remaining <= 0i64 { 0 } else { remaining.toInt() }
    val frame = match readFrame(q, wait) {
      case Err(e) -> return Err(e)
      case Ok(None) -> return Ok(None)
      case Ok(Some(f)) -> f
    }
    if frame.kind == Amqp.FRAME_HEARTBEAT {
      if frame.channel != 0 {
        return Err(markLost(q, "the broker sent a heartbeat on channel " + frame.channel.toString()))
      }
    } else if frame.channel != 0 and frame.channel != CHANNEL {
      return Err(markLost(q, "the broker sent a frame on channel " + frame.channel.toString() + ", which is not open"))
    } else if frame.kind == Amqp.FRAME_METHOD {
      if q.pendingKind != 0 and frame.channel == CHANNEL {
        return Err(markLost(q, "the broker sent a method frame in the middle of a message"))
      }
      val method = match Amqp.decodeMethod(frame.payload) { case Ok(m) -> m; case Err(e) -> return Err(markLost(q, e)) }
      match method {
        case BasicDeliver(_, tag, redelivered, _, _) -> {
          q.pendingKind = 1
          q.pendingTag = tag
          q.pendingRedelivered = redelivered
        }
        case BasicReturn(code, text, _, _) -> {
          q.pendingKind = 2
          q.pendingCode = code
          q.pendingText = text
        }
        case _ -> return Ok(Some(value = EvMethod(channel = frame.channel, method = method)))
      }
    } else if frame.kind == Amqp.FRAME_HEADER {
      if q.pendingKind == 0 or q.pendingHasHeader {
        return Err(markLost(q, "the broker sent a content header that belongs to no message"))
      }
      val header = match Amqp.decodeContentHeader(frame.payload) {
        case Ok(h) -> h
        case Err(e) -> return Err(markLost(q, e))
      }
      q.pendingHasHeader = true
      q.pendingProperties = header.properties
      q.pendingSize = header.bodySize
      // A returned message's body is of no use: the id is in its properties.
      q.pendingDiscard = q.pendingKind == 2 or header.bodySize > q.options.maxMessageBytes.toLong()
      if header.bodySize == 0i64 {
        return Ok(Some(value = finishContent(q)))
      }
    } else {
      if q.pendingKind == 0 or not q.pendingHasHeader {
        return Err(markLost(q, "the broker sent a body frame that belongs to no message"))
      }
      q.pendingGot = q.pendingGot + frame.payload.length.toLong()
      if q.pendingGot > q.pendingSize {
        return Err(markLost(q, "the broker sent more body bytes than the content header announced"))
      }
      if not q.pendingDiscard {
        q.pendingChunks.add(frame.payload)
      }
      if q.pendingGot == q.pendingSize {
        return Ok(Some(value = finishContent(q)))
      }
    }
  }
  Ok(None)
}

// A publisher confirm: `tag` (and every earlier one when `multiple`) is no
// longer awaited.
func confirm(q: in RabbitMqQueue, tag: in Long, multiple: in Bool, refused: in Bool): Unit {
  val still: List[Long] = newList()
  for seq in q.unconfirmed.toArray() {
    if seq == tag or (multiple and seq <= tag) {
      if refused {
        q.nacked.add(seq)
      }
    } else {
      still.add(seq)
    }
  }
  q.unconfirmed = still
}

// Forget everything that belonged to the channel that just closed.
func resetChannelState(q: in RabbitMqQueue): Unit {
  q.consuming = false
  q.publishSeq = 0i64
  q.unconfirmed = newList()
  q.nacked = newList()
  q.returned = newList()
  q.backlog = newList()
  q.inFlight = newMap()
  clearPending(q)
}

// One step: read the next event and handle everything that is not a reply to
// a request. With `recover`, a channel the broker closed is reopened before
// the error is returned.
func step(q: in RabbitMqQueue, timeoutMs: in Int, recover: in Bool): Result[Step, String] {
  val event = match nextEvent(q, timeoutMs) {
    case Err(e) -> return Err(e)
    case Ok(None) -> return Ok(Idle)
    case Ok(Some(ev)) -> ev
  }
  match event {
    case EvDelivery(delivery) -> {
      q.backlog.add(delivery)
      Ok(Progress)
    }
    case EvReturn(returned) -> {
      q.returned.add(returned)
      Ok(Progress)
    }
    case EvMethod(channel, method) -> {
      match method {
        case ConnectionClose(code, text, _, _) -> {
          q.lastCode = code
          val _ = writeMethod(q, 0, Amqp.ConnectionCloseOk)
          Err(markLost(q, "the broker closed the connection: " + replyText(code, text)))
        }
        case ConnectionBlocked(reason) -> {
          q.blocked = if reason.length > 0 { reason } else { "resource alarm" }
          Ok(Progress)
        }
        case ConnectionUnblocked -> {
          q.blocked = ""
          Ok(Progress)
        }
        case ChannelClose(code, text, _, _) -> {
          q.lastCode = code
          resetChannelState(q)
          match writeMethod(q, CHANNEL, Amqp.ChannelCloseOk) {
            case Err(e) -> return Err(e)
            case Ok(_) -> ()
          }
          val failure = "rabbitmq: the broker closed the channel: " + replyText(code, text)
          if recover {
            match openChannel(q) {
              case Ok(_) -> ()
              case Err(e) -> return Err(failure + " (and reopening the channel failed: " + e + ")")
            }
          }
          Err(failure)
        }
        case BasicAck(tag, multiple) -> {
          confirm(q, tag, multiple, false)
          Ok(Progress)
        }
        case BasicNack(tag, multiple, _) -> {
          confirm(q, tag, multiple, true)
          Ok(Progress)
        }
        case BasicCancel(_, _) -> {
          // The broker cancelled the consumer (the queue was deleted, say).
          // The next consume starts a new one and reports what the broker
          // says about it.
          q.consuming = false
          Ok(Progress)
        }
        case ChannelFlow(active) -> {
          match writeMethod(q, CHANNEL, Amqp.ChannelFlowOk(active = active)) {
            case Err(e) -> Err(e)
            case Ok(_) -> Ok(Progress)
          }
        }
        case _ -> Ok(Reply(method = method))
      }
    }
  }
}

// Send `request` and wait for the method called `expected`.
func rpc(
  q: in RabbitMqQueue, channel: in Int, request: in Amqp.Method, expected: in String, recover: in Bool
): Result[Amqp.Method, String] {
  match writeMethod(q, channel, request) {
    case Err(e) -> return Err(e)
    case Ok(_) -> ()
  }
  awaitReply(q, Amqp.methodName(request), expected, recover)
}

func awaitReply(
  q: in RabbitMqQueue, what: in String, expected: in String, recover: in Bool
): Result[Amqp.Method, String] {
  val timeoutMs = q.options.requestTimeoutMs
  val deadline = K.monotonicMillis() + timeoutMs.toLong()
  while true {
    val remaining = deadline - K.monotonicMillis()
    val wait = if remaining <= 0i64 { 0 } else { remaining.toInt() }
    match step(q, wait, recover) {
      case Err(e) -> return Err(e)
      case Ok(Reply(m)) -> {
        if Amqp.methodName(m) == expected {
          return Ok(m)
        }
        return Err(markLost(q, "the broker answered " + what + " with " + Amqp.methodName(m)))
      }
      case Ok(Progress) -> ()
      case Ok(Idle) -> {
        if remaining <= 0i64 {
          return Err(markLost(q, "the broker did not answer " + what + " within " + timeoutMs.toString() + " ms"))
        }
      }
    }
  }
  Err(markLost(q, "the broker did not answer " + what))
}

// Open the channel and put it in confirm mode.
func openChannel(q: in RabbitMqQueue): Result[Unit, String] {
  match rpc(q, CHANNEL, Amqp.ChannelOpen, "channel.open-ok", false) {
    case Err(e) -> return Err(e)
    case Ok(_) -> ()
  }
  match rpc(q, CHANNEL, Amqp.ConfirmSelect, "confirm.select-ok", false) {
    case Err(e) -> Err(e)
    case Ok(_) -> Ok(())
  }
}

// Handle whatever has already arrived, without waiting.
func drain(q: in RabbitMqQueue): Result[Unit, String] {
  while true {
    match step(q, 0, true) {
      case Err(e) -> return Err(e)
      case Ok(Idle) -> return Ok(())
      case Ok(Progress) -> ()
      case Ok(Reply(m)) -> return Err(markLost(q, "the broker sent an unexpected " + Amqp.methodName(m)))
    }
  }
  Ok(())
}

// ─── Connecting ──────────────────────────────────────────────────────────────
func validate(options: in ConnectOptions): Result[Unit, String] {
  if options.host.length == 0 {
    return Err("rabbitmq: no broker host configured")
  }
  if options.port < 1 or options.port > 65535 {
    return Err("rabbitmq: broker port " + options.port.toString() + " is out of range")
  }
  if options.user.length == 0 {
    return Err("rabbitmq: no user configured")
  }
  if Str.contains(options.user, "\u{0000}") or Str.contains(options.password, "\u{0000}") {
    return Err("rabbitmq: the user and password cannot contain a NUL character")
  }
  if encodeUtf8(options.virtualHost).length > 255 {
    return Err("rabbitmq: the virtual host name is longer than 255 bytes")
  }
  if options.queue.length == 0 {
    return Err("rabbitmq: no queue name configured")
  }
  if encodeUtf8(options.queue).length > 255 {
    return Err("rabbitmq: the queue name is longer than 255 bytes")
  }
  if options.prefetchCount < 0 or options.prefetchCount > 65535 {
    return Err("rabbitmq: prefetchCount must be between 0 and 65535")
  }
  if options.heartbeatSeconds < 0 or options.heartbeatSeconds > 65535 {
    return Err("rabbitmq: heartbeatSeconds must be between 0 and 65535")
  }
  if options.frameMax < Amqp.FRAME_MIN_SIZE {
    return Err("rabbitmq: frameMax must be at least " + Amqp.FRAME_MIN_SIZE.toString())
  }
  if options.connectTimeoutMs < 0 {
    return Err("rabbitmq: connectTimeoutMs must not be negative")
  }
  if options.requestTimeoutMs < 1 {
    return Err("rabbitmq: requestTimeoutMs must be at least 1")
  }
  if options.maxMessageBytes < 1 {
    return Err("rabbitmq: maxMessageBytes must be at least 1")
  }
  match Amqp.encodeTable(options.queueArguments) {
    case Err(e) -> return Err("rabbitmq: queueArguments: " + e)
    case Ok(_) -> ()
  }
  match validateTlsOptions(options.tls) {
    case Ok(_) -> Ok(())
    case Err(problem) -> Err("rabbitmq: " + tlsProblemText(problem, "an amqps:// URL"))
  }
}

/// Check `options` without connecting: the same checks `connectWith` makes
/// before it opens a socket.
pub func validateOptions(options: in ConnectOptions): Result[Unit, String] {
  validate(options)
}

// The transport: a TCP connection, secured when the options say so.
func openTransport(options: in ConnectOptions, endpoint: in String): Result[K.Sock, String] {
  val plain = match K.sockConnect(options.host, options.port, options.connectTimeoutMs) {
    case Ok(s) -> s
    case Err(e) -> return Err("rabbitmq: cannot connect to " + endpoint + ": " + e)
  }
  if not options.tls.enabled {
    return Ok(plain)
  }
  val warning = insecureIgnoredWarning(options.tls, "Mq.RabbitMq")
  if warning.length > 0 {
    Console.error(warning)
  }
  match K.sockStartTls(plain, options.tls, options.host, skipsVerification(options.tls), options.connectTimeoutMs) {
    case Ok(s) -> Ok(s)
    case Err(e) -> Err("rabbitmq: TLS handshake with " + endpoint + " failed: " + e)
  }
}

func hasWord(text: in String, word: in String): Bool {
  for part in Str.split(text, " ") {
    if part == word {
      return true
    }
  }
  false
}

func clientProperties(options: in ConnectOptions): slice[Amqp.TableEntry] {
  val capabilities: slice[Amqp.TableEntry] = [
    Amqp.entry("publisher_confirms", Amqp.FBool(value = true)),
    Amqp.entry("consumer_cancel_notify", Amqp.FBool(value = true)),
    Amqp.entry("connection.blocked", Amqp.FBool(value = true)),
    Amqp.entry("authentication_failure_close", Amqp.FBool(value = true)),
    Amqp.entry("basic.nack", Amqp.FBool(value = true))
  ]
  val properties: List[Amqp.TableEntry] = newList()
  properties.add(Amqp.entry("product", Amqp.fText("lyric-mq-rabbitmq")))
  properties.add(Amqp.entry("version", Amqp.fText(CLIENT_VERSION)))
  properties.add(Amqp.entry("platform", Amqp.fText("Lyric")))
  properties.add(Amqp.entry("capabilities", Amqp.FTable(entries = capabilities)))
  if options.connectionName.length > 0 {
    properties.add(Amqp.entry("connection_name", Amqp.fText(options.connectionName)))
  }
  properties.toArray()
}

func plainResponse(user: in String, password: in String): slice[Byte] {
  val nul: slice[Byte] = [0u8]
  nul.concat(encodeUtf8(user)).concat(nul).concat(encodeUtf8(password))
}

func serverDescription(properties: in slice[Amqp.TableEntry]): String {
  val product = match Amqp.tableGet(properties, "product") { case Some(v) -> fieldText(v); case None -> "" }
  val version = match Amqp.tableGet(properties, "version") { case Some(v) -> fieldText(v); case None -> "" }
  if product.length > 0 and version.length > 0 {
    product + " " + version
  } else {
    product + version
  }
}

// The handshake after the socket is open: protocol header, start, tune,
// open, then the channel and the queue.
func handshake(q: in RabbitMqQueue, endpoint: in String): Result[Unit, String] {
  val options = q.options
  match writeBytes(q, Amqp.protocolHeader()) {
    case Err(e) -> return Err(e)
    case Ok(_) -> ()
  }
  val start = match awaitReply(q, "the protocol header", "connection.start", false) {
    case Ok(m) -> m
    case Err(e) -> return Err(e)
  }
  match start {
    case ConnectionStart(major, minor, properties, mechanisms, _) -> {
      if major != 0 or minor != 9 {
        return Err(
          "rabbitmq: " + endpoint + " speaks AMQP " + major.toString() + "-" + minor.toString() +
            "; this client needs 0-9-1"
        )
      }
      if not hasWord(mechanisms, "PLAIN") {
        return Err("rabbitmq: " + endpoint + " does not offer PLAIN authentication (it offers: " + mechanisms + ")")
      }
      q.server = serverDescription(properties)
    }
    case _ -> ()
  }
  val startOk = Amqp.ConnectionStartOk(
    clientProperties = clientProperties(options),
    mechanism = "PLAIN",
    response = plainResponse(options.user, options.password),
    locale = "en_US"
  )
  match writeMethod(q, 0, startOk) {
    case Err(e) -> return Err(e)
    case Ok(_) -> ()
  }
  val tune = match step(q, options.requestTimeoutMs, false) {
    case Ok(Reply(m)) -> m
    case Ok(_) -> return Err(
      markLost(
        q,
        "the broker did not answer connection.start-ok within " + options.requestTimeoutMs.toString() + " ms"
      )
    )
    case Err(e) -> {
      // A broker without `authentication_failure_close` just drops the
      // socket on bad credentials.
      if q.lastCode == 0 and Str.contains(e, "the broker closed the connection") {
        return Err("rabbitmq: " + endpoint + " closed the connection during authentication; check the user and password")
      }
      return Err(e)
    }
  }
  match tune {
    case ConnectionTune(_, serverFrameMax, serverHeartbeat) -> {
      val wanted = options.frameMax.toLong()
      val frameMax = if serverFrameMax == 0i64 or serverFrameMax > wanted { wanted } else { serverFrameMax }
      if frameMax < Amqp.FRAME_MIN_SIZE.toLong() {
        return Err(
          markLost(
            q,
            "the broker proposed a frame-max of " + serverFrameMax.toString() + ", below the protocol minimum"
          )
        )
      }
      val heartbeat = if options.heartbeatSeconds == 0 {
        0
      } else if serverHeartbeat == 0 or serverHeartbeat > options.heartbeatSeconds {
        options.heartbeatSeconds
      } else {
        serverHeartbeat
      }
      match writeMethod(q, 0, Amqp.ConnectionTuneOk(channelMax = 1, frameMax = frameMax, heartbeat = heartbeat)) {
        case Err(e) -> return Err(e)
        case Ok(_) -> ()
      }
      q.frameMax = frameMax.toInt()
      q.heartbeatMs = heartbeat.toLong() * 1000i64
      Amqp.setFrameMax(q.decoder, q.frameMax)
    }
    case ConnectionSecure(_) -> {
      return Err(markLost(q, "the broker asked for a SASL challenge response, which PLAIN does not have"))
    }
    case _ -> return Err(markLost(q, "the broker answered connection.start-ok with " + Amqp.methodName(tune)))
  }
  match rpc(q, 0, Amqp.ConnectionOpen(virtualHost = options.virtualHost), "connection.open-ok", false) {
    case Err(e) -> return Err(e)
    case Ok(_) -> ()
  }
  match openChannel(q) {
    case Err(e) -> return Err(e)
    case Ok(_) -> ()
  }
  val declare = Amqp.QueueDeclare(
    queue = options.queue,
    passive = not options.declare,
    durable = options.declare and options.durable,
    exclusive = false,
    autoDelete = options.declare and options.autoDelete,
    arguments = if options.declare { options.queueArguments } else { Amqp.emptyTable() }
  )
  match rpc(q, CHANNEL, declare, "queue.declare-ok", false) {
    case Err(e) -> Err(e)
    case Ok(_) -> Ok(())
  }
}

/// Connect to the broker, authenticate, open the channel and declare (or
/// check) the queue. The consumer is started by the first `consume` (or by
/// `subscribe`), so a connection that only publishes takes no messages.
pub func connectWith(options: in ConnectOptions): Result[RabbitMqQueue, String] {
  match validate(options) {
    case Err(e) -> return Err(e)
    case Ok(_) -> ()
  }
  val endpoint = options.host + ":" + options.port.toString()
  val sock = match openTransport(options, endpoint) { case Ok(s) -> s; case Err(e) -> return Err(e) }
  val now = K.monotonicMillis()
  val q = RabbitMqQueue(
    options = options,
    sock = sock,
    decoder = Amqp.newDecoder(Amqp.FRAME_MIN_SIZE),
    lock = K.newLock(),
    server = "",
    lost = "",
    closed = false,
    frameMax = Amqp.FRAME_MIN_SIZE,
    heartbeatMs = 0i64,
    lastSent = now,
    lastReceived = now,
    lastCode = 0,
    blocked = "",
    consuming = false,
    publishSeq = 0i64,
    unconfirmed = newList(),
    nacked = newList(),
    returned = newList(),
    backlog = newList(),
    inFlight = newMap(),
    pendingKind = 0,
    pendingTag = 0i64,
    pendingRedelivered = false,
    pendingCode = 0,
    pendingText = "",
    pendingHasHeader = false,
    pendingProperties = Amqp.noProperties(),
    pendingSize = 0i64,
    pendingGot = 0i64,
    pendingDiscard = false,
    pendingChunks = newList()
  )
  match handshake(q, endpoint) {
    case Ok(_) -> Ok(q)
    case Err(e) -> {
      K.sockClose(sock)
      q.closed = true
      // The connection was never handed out, so it was refused, not lost.
      val lostPrefix = "rabbitmq: connection lost: "
      if Str.startsWith(e, lostPrefix) {
        Err("rabbitmq: cannot open a connection to " + endpoint + ": " + Str.substring(e, lostPrefix.length))
      } else {
        Err(e)
      }
    }
  }
}

/// The options for the broker named by an `amqp://` or `amqps://` URL (see
/// `Mq.RabbitMq.Url`) and for `queueName`. An `amqps` URL turns TLS on with
/// the defaults (platform trust store, name checked against the host); add a
/// CA or a client identity with the `with...` functions before `connectWith`.
pub func optionsFromUrl(url: in String, queueName: in String): Result[ConnectOptions, String] {
  if queueName.length == 0 {
    return Err("rabbitmq: no queue name configured")
  }
  val address = match Url.parseUrl(url) { case Ok(a) -> a; case Err(e) -> return Err(e) }
  var options = defaultOptions(address.host, address.port, queueName).copy(
    user = address.user,
    password = address.password,
    virtualHost = address.virtualHost
  )
  if address.heartbeatSeconds >= 0 {
    options = options.copy(heartbeatSeconds = address.heartbeatSeconds)
  }
  if address.connectTimeoutMs >= 0 {
    options = options.copy(connectTimeoutMs = address.connectTimeoutMs)
  }
  if address.frameMax >= 0 {
    options = options.copy(frameMax = address.frameMax)
  }
  if address.tls {
    Ok(options.withTls())
  } else {
    Ok(options)
  }
}

/// Connect to the broker named by an `amqp://` or `amqps://` URL (see
/// `optionsFromUrl`) and use `queueName`.
pub func connectUrl(url: in String, queueName: in String): Result[RabbitMqQueue, String] {
  match optionsFromUrl(url, queueName) {
    case Ok(options) -> connectWith(options)
    case Err(e) -> Err(e)
  }
}

func envName(field: in String): String {
  "LYRIC_CONFIG_MQ_RABBITMQ_" + field
}

// The variable `field`, or `fallback` when it is unset or empty.
func envOr(field: in String, fallback: in String): String {
  val value = Env.getVarOrDefault(envName(field), "")
  if value.length > 0 {
    value
  } else {
    fallback
  }
}

// The TLS settings of `connectFromEnv`, applied to `base`.
func tlsFromEnv(base: in ConnectOptions): Result[ConnectOptions, String] {
  match tlsOptionsFromEnv(envName(""), base.tls) {
    case Ok(tls) -> Ok(base.copy(tls = tls))
    case Err(e) -> Err("rabbitmq: " + e)
  }
}

/// The options `connectFromEnv` connects with. A variable that is empty
/// counts as unset:
///
/// - `LYRIC_CONFIG_MQ_RABBITMQ_URL`: the broker URL (required)
/// - `LYRIC_CONFIG_MQ_RABBITMQ_QUEUENAME`: the queue (required)
/// - `LYRIC_CONFIG_MQ_RABBITMQ_ACKMODE`: `Manual` (default) or `Auto`
/// - `LYRIC_CONFIG_MQ_RABBITMQ_PREFETCHCOUNT`: the prefetch window
///   (default 10)
///
/// and, for an `amqps://` URL:
///
/// - `LYRIC_CONFIG_MQ_RABBITMQ_TLSCAFILE`: a PEM file with a CA certificate
///   to trust in addition to the platform trust store
/// - `LYRIC_CONFIG_MQ_RABBITMQ_TLSCAEXCLUSIVE`: `true` to trust only that CA
/// - `LYRIC_CONFIG_MQ_RABBITMQ_TLSCERTFILE`, `..._TLSKEYFILE`: PEM files with
///   the client certificate and its PKCS8 private key (mutual TLS)
/// - `LYRIC_CONFIG_MQ_RABBITMQ_TLSSERVERNAME`: the name to check the server
///   certificate against, when it is not the URL's host
/// - `LYRIC_CONFIG_MQ_RABBITMQ_TLSMINVERSION`: `1.2` (default) or `1.3`
///
/// Verification cannot be switched off from the environment; that takes
/// `withInsecureSkipVerify()` in code as well as `LYRIC_TLS_ALLOW_INSECURE=1`.
pub func optionsFromEnv(): Result[ConnectOptions, String] {
  val url = envOr("URL", "")
  if url.length == 0 {
    return Err("rabbitmq: no broker URL configured (set LYRIC_CONFIG_MQ_RABBITMQ_URL)")
  }
  val queueName = envOr("QUEUENAME", "")
  if queueName.length == 0 {
    return Err("rabbitmq: no queue name configured (set LYRIC_CONFIG_MQ_RABBITMQ_QUEUENAME)")
  }
  val modeText = Str.toLower(envOr("ACKMODE", "manual"))
  val mode = if modeText == "manual" {
    Mq.AckMode.Manual
  } else if modeText == "auto" {
    Mq.AckMode.Auto
  } else {
    return Err("rabbitmq: LYRIC_CONFIG_MQ_RABBITMQ_ACKMODE must be Manual or Auto")
  }
  val prefetchText = envOr("PREFETCHCOUNT", "10")
  val prefetch = match parseOptInt(prefetchText) {
    case Some(n) -> n
    case None -> return Err("rabbitmq: LYRIC_CONFIG_MQ_RABBITMQ_PREFETCHCOUNT is not a number")
  }
  val base = match optionsFromUrl(url, queueName) { case Ok(o) -> o; case Err(e) -> return Err(e) }
  tlsFromEnv(base.copy(ackMode = mode, prefetchCount = prefetch))
}

/// Connect using the environment; see `optionsFromEnv` for the variables.
pub func connectFromEnv(): Result[RabbitMqQueue, String] {
  match optionsFromEnv() {
    case Ok(options) -> connectWith(options)
    case Err(e) -> Err(e)
  }
}

// ─── Field values as text ────────────────────────────────────────────────────
func decimalText(scale: in Int, unscaled: in Int): String {
  val negative = unscaled < 0
  var digits = if negative { (0i64 - unscaled.toLong()).toString() } else { unscaled.toString() }
  if scale == 0 {
    return if negative { "-" + digits } else { digits }
  }
  while digits.length <= scale {
    digits = "0" + digits
  }
  val whole = Str.substring(digits, 0, digits.length - scale)
  val fraction = Str.substring(digits, digits.length - scale);
  (if negative { "-" } else { "" }) + whole + "." + fraction
}

func bytesText(bytes: in slice[Byte]): String {
  match tryDecodeUtf8(bytes) {
    case Some(s) -> s
    case None -> encodeBase64(bytes)
  }
}

// `text` as a JSON string literal. Written out here, not taken from the
// platform's JSON library, so both targets produce the same text.
func jsonString(text: in String): String {
  val sb = Str.StringBuilder.new()
  sb.append("\"")
  var start = 0
  var i = 0
  while i < text.length {
    val unit = text.codeUnitAt(i)
    val replacement = if unit == 34 {
      "\\\""
    } else if unit == 92 {
      "\\\\"
    } else if unit == 10 {
      "\\n"
    } else if unit == 13 {
      "\\r"
    } else if unit == 9 {
      "\\t"
    } else if unit < 32 {
      "\\u00" + (if unit < 16 { "0" } else { "1" }) + Str.substring("0123456789abcdef", unit % 16, 1)
    } else {
      ""
    }
    if replacement.length > 0 {
      sb.append(Str.substring(text, start, i - start))
      sb.append(replacement)
      start = i + 1
    }
    i += 1
  }
  sb.append(Str.substring(text, start))
  sb.append("\"")
  sb.toString()
}

// A value as JSON, for the elements of arrays and tables.
func fieldJson(value: in Amqp.FieldValue): String {
  match value {
    case FString(bytes) -> jsonString(bytesText(bytes))
    case FBytes(bytes) -> jsonString(bytesText(bytes))
    case FVoid -> "null"
    case FFloat(_) -> jsonString(fieldText(value))
    case FDouble(_) -> jsonString(fieldText(value))
    case _ -> fieldText(value)
  }
}

/// A field-table value as the text `Message.headers` carries it:
///
/// - a long string or byte array: its text when it is UTF-8, else its base64
/// - an integer or a timestamp (seconds since the epoch): decimal digits
/// - a boolean: `true` or `false`; void: the empty string
/// - a decimal: decimal notation; a float or double: the platform's shortest
///   notation for it
/// - an array or a table (`x-death`, for one): JSON, with tables as objects,
///   integers, decimals and booleans bare, void as `null` and everything
///   else (floats included) as a string
pub func fieldText(value: in Amqp.FieldValue): String {
  match value {
    case FBool(b) -> if b { "true" } else { "false" }
    case FInteger(_, v) -> v.toString()
    case FFloat(bits) -> Math.floatFromBits(bits).toDouble().toString()
    case FDouble(bits) -> Math.doubleFromBits(bits).toString()
    case FDecimal(scale, unscaled) -> decimalText(scale, unscaled)
    case FString(bytes) -> bytesText(bytes)
    case FBytes(bytes) -> bytesText(bytes)
    case FTimestamp(seconds) -> seconds.toString()
    case FVoid -> ""
    case FArray(items) -> {
      val parts: List[String] = newList()
      for item in items {
        parts.add(fieldJson(item))
      }
      "[" + Str.joinList(",", parts) + "]"
    }
    case FTable(entries) -> {
      val parts: List[String] = newList()
      for e in entries {
        parts.add(jsonString(e.name) + ":" + fieldJson(e.value))
      }
      "{" + Str.joinList(",", parts) + "}"
    }
  }
}

// ─── Publishing ──────────────────────────────────────────────────────────────
// The JSON array of [key, value] pairs that carries `headers` exactly.
func encodeHeaderList(headers: in slice[Mq.MessageHeader]): String {
  val parts: List[String] = newList()
  for h in headers {
    parts.add("[" + jsonString(h.key) + "," + jsonString(h.value) + "]")
  }
  "[" + Str.joinList(",", parts) + "]"
}

// A cursor over the carrier's text.
ref record JsonCursor {
  text: String
  var pos: Int
}

func skipSpace(c: in JsonCursor): Unit {
  while c.pos < c.text.length {
    val unit = c.text.codeUnitAt(c.pos)
    if unit != 32 and unit != 9 and unit != 10 and unit != 13 {
      return
    }
    c.pos += 1
  }
}

// Consume `unit` (after white space); false when something else is there.
func accept(c: in JsonCursor, unit: in Int): Bool {
  skipSpace(c)
  if c.pos < c.text.length and c.text.codeUnitAt(c.pos) == unit {
    c.pos += 1
    return true
  }
  false
}

func hexDigit(unit: in Int): Int {
  if unit >= 48 and unit <= 57 {
    unit - 48
  } else if unit >= 65 and unit <= 70 {
    unit - 55
  } else if unit >= 97 and unit <= 102 {
    unit - 87
  } else {
    -1
  }
}

// The four hexadecimal digits at the cursor, -1 when they are not there.
func hex4(c: in JsonCursor): Int {
  if c.pos + 4 > c.text.length {
    return -1
  }
  var value = 0
  var i = 0
  while i < 4 {
    val digit = hexDigit(c.text.codeUnitAt(c.pos + i))
    if digit < 0 {
      return -1
    }
    value = value * 16 + digit
    i += 1
  }
  c.pos += 4
  value
}

// A JSON string at the cursor; `None` when there is none or it is malformed.
func jsonStringAt(c: in JsonCursor): Option[String] {
  if not accept(c, 34) {
    return None
  }
  val sb = Str.StringBuilder.new()
  var start = c.pos
  while c.pos < c.text.length {
    val unit = c.text.codeUnitAt(c.pos)
    if unit == 34 {
      sb.append(Str.substring(c.text, start, c.pos - start))
      c.pos += 1
      return Some(value = sb.toString())
    }
    if unit < 32 {
      return None
    }
    if unit == 92 {
      sb.append(Str.substring(c.text, start, c.pos - start))
      if c.pos + 1 >= c.text.length {
        return None
      }
      val escape = c.text.codeUnitAt(c.pos + 1)
      c.pos += 2
      if escape == 117 {
        var code = hex4(c)
        if code < 0 {
          return None
        }
        if code >= 55296 and code <= 56319 {
          // A high surrogate must be followed by an escaped low one.
          if not (c.pos + 1 < c.text.length and c.text.codeUnitAt(c.pos) == 92 and c.text.codeUnitAt(c.pos + 1) == 117) {
            return None
          }
          c.pos += 2
          val low = hex4(c)
          if low < 56320 or low > 57343 {
            return None
          }
          code = 65536 + (code - 55296) * 1024 + (low - 56320)
        } else if code >= 56320 and code <= 57343 {
          return None
        }
        sb.append(codepointToString(code))
      } else if escape == 34 {
        sb.append("\"")
      } else if escape == 92 {
        sb.append("\\")
      } else if escape == 47 {
        sb.append("/")
      } else if escape == 110 {
        sb.append("\n")
      } else if escape == 114 {
        sb.append("\r")
      } else if escape == 116 {
        sb.append("\t")
      } else if escape == 98 {
        sb.append(codepointToString(8))
      } else if escape == 102 {
        sb.append(codepointToString(12))
      } else {
        return None
      }
      start = c.pos
    } else {
      c.pos += 1
    }
  }
  None
}

/// The header list in a `lyric_headers` value: a JSON array of
/// `[key, value]` string pairs. `None` when the text is anything else.
pub func decodeHeaderList(text: in String): Option[slice[Mq.MessageHeader]] {
  val c = JsonCursor(text = text, pos = 0)
  val headers: List[Mq.MessageHeader] = newList()
  if not accept(c, 91) {
    return None
  }
  if not accept(c, 93) {
    var more = true
    while more {
      if not accept(c, 91) {
        return None
      }
      val key = match jsonStringAt(c) { case Some(k) -> k; case None -> return None }
      if not accept(c, 44) {
        return None
      }
      val value = match jsonStringAt(c) { case Some(v) -> v; case None -> return None }
      if not accept(c, 93) {
        return None
      }
      headers.add(Mq.MessageHeader(key = key, value = value))
      if accept(c, 93) {
        more = false
      } else if not accept(c, 44) {
        return None
      }
    }
  }
  skipSpace(c)
  if c.pos != c.text.length {
    return None
  }
  Some(value = headers.toArray())
}

func hasEntry(table: in List[Amqp.TableEntry], name: in String): Bool {
  for e in table.toArray() {
    if e.name == name {
      return true
    }
  }
  false
}

// The `headers` table of a message: the entries of `base`, one plain entry
// per distinct key of `headers` (its first value; for other AMQP consumers
// and for `headers` exchanges) and the carrier entry that holds the list
// exactly. A key that cannot be a table name (empty, over 255 bytes, the
// carrier's own name) travels in the carrier only.
func headerTable(base: in slice[Amqp.TableEntry], headers: in slice[Mq.MessageHeader]): slice[Amqp.TableEntry] {
  if headers.length == 0 {
    return base
  }
  val entries: List[Amqp.TableEntry] = newList()
  for e in base {
    if e.name != HEADER_HEADERS {
      entries.add(e)
    }
  }
  for h in headers {
    val fits = h.key.length > 0 and h.key != HEADER_HEADERS and encodeUtf8(h.key).length <= 255
    if fits and not hasEntry(entries, h.key) {
      entries.add(Amqp.entry(h.key, Amqp.fText(h.value)))
    }
  }
  entries.add(Amqp.entry(HEADER_HEADERS, Amqp.fText(encodeHeaderList(headers))))
  entries.toArray()
}

// The properties of `message`: `base` with the id, the headers and (unless
// `base` chose one) the delivery mode of the connection.
func propertiesFor(q: in RabbitMqQueue, message: in Mq.Message, base: in Amqp.Properties): Amqp.Properties {
  val mode = if base.deliveryMode != 0 { base.deliveryMode } else if q.options.persistent { 2 } else { 1 }
  base.copy(messageId = message.id, headers = headerTable(base.headers, message.headers), deliveryMode = mode)
}

func sendMessage(
  q: in RabbitMqQueue,
  exchange: in String,
  routingKey: in String,
  mandatory: in Bool,
  message: in Mq.Message,
  base: in Amqp.Properties,
): Result[Long, String] {
  val publish = Amqp.BasicPublish(
    exchange = exchange,
    routingKey = routingKey,
    mandatory = mandatory,
    immediate = false
  )
  val frames: slice[Byte] = match Amqp.contentFrames(CHANNEL, publish, propertiesFor(q, message, base), encodeUtf8(message.body), q.frameMax) {
    case Ok(b) -> b
    case Err(e) -> return Err("rabbitmq: message '" + message.id + "' cannot be sent: " + e)
  }
  match writeBytes(q, frames) {
    case Err(e) -> return Err(e)
    case Ok(_) -> ()
  }
  q.publishSeq = q.publishSeq + 1i64
  q.unconfirmed.add(q.publishSeq)
  Ok(q.publishSeq)
}

// Wait until the broker has confirmed every publish so far.
func awaitConfirms(q: in RabbitMqQueue): Result[Unit, String] {
  val timeoutMs = q.options.requestTimeoutMs
  val deadline = K.monotonicMillis() + timeoutMs.toLong()
  while q.unconfirmed.count > 0 {
    val remaining = deadline - K.monotonicMillis()
    val wait = if remaining <= 0i64 { 0 } else { remaining.toInt() }
    match step(q, wait, true) {
      case Err(e) -> return Err(e)
      case Ok(Progress) -> ()
      case Ok(Reply(m)) -> return Err(markLost(q, "the broker sent an unexpected " + Amqp.methodName(m)))
      case Ok(Idle) -> {
        if remaining <= 0i64 {
          val why = if q.blocked.length > 0 {
            "the broker did not confirm the publish within " + timeoutMs.toString() + " ms; it has blocked publishers (" +
              q.blocked +
              ")"
          } else {
            "the broker did not confirm the publish within " + timeoutMs.toString() + " ms"
          }
          return Err(markLost(q, why))
        }
      }
    }
  }
  Ok(())
}

// The outcome of the publishes since `beginPublish`: the first message the
// broker returned or refused, if any.
func publishOutcome(q: in RabbitMqQueue): Result[Unit, String] {
  if q.returned.count > 0 {
    val r = q.returned[0]
    val which = if r.messageId.length > 0 { "message '" + r.messageId + "'" } else { "a message" }
    q.lastCode = r.replyCode
    return Err("rabbitmq: the broker returned " + which + " as unroutable: " + replyText(r.replyCode, r.replyText))
  }
  if q.nacked.count > 0 {
    return Err("rabbitmq: the broker refused the message (basic.nack); it was not enqueued")
  }
  Ok(())
}

func beginPublish(q: in RabbitMqQueue): Result[Unit, String] {
  match checkUsable(q) {
    case Err(e) -> return Err(e)
    case Ok(_) -> ()
  }
  q.returned = newList()
  q.nacked = newList()
  // Notice a channel or connection the broker closed since the last
  // operation before writing to it.
  drain(q)
}

/// Publish `message` to `exchange` with `routingKey` and the properties in
/// `base` (the message id, headers and, when `base` leaves it 0, the delivery
/// mode are filled in from `message` and the connection). With `mandatory`,
/// a message no queue receives is an `Err`; without it the broker drops such
/// a message silently. Returns once the broker has confirmed the message.
/// `Mq.RabbitMq.Extras.publishWith` is the convenient form.
pub func publishTo(
  q: in RabbitMqQueue,
  exchange: in String,
  routingKey: in String,
  mandatory: in Bool,
  message: in Mq.Message,
  base: in Amqp.Properties,
): Result[Unit, String] {
  if message.id.length == 0 {
    return Err(EMPTY_ID)
  }
  K.lockEnter(q.lock)
  defer {
    K.lockExit(q.lock)
  }
  match beginPublish(q) {
    case Err(e) -> return Err(e)
    case Ok(_) -> ()
  }
  match sendMessage(q, exchange, routingKey, mandatory, message, base) {
    case Err(e) -> return Err(e)
    case Ok(_) -> ()
  }
  match awaitConfirms(q) {
    case Err(e) -> return Err(e)
    case Ok(_) -> ()
  }
  publishOutcome(q)
}

func publishBatchLocked(q: in RabbitMqQueue, messages: in slice[Mq.Message]): Result[Unit, String] {
  match beginPublish(q) {
    case Err(e) -> return Err(e)
    case Ok(_) -> ()
  }
  for m in messages {
    match sendMessage(q, "", q.options.queue, true, m, Amqp.noProperties()) {
      case Err(e) -> {
        // What was already sent is still confirmed or refused by the broker.
        if q.lost.length == 0 {
          val _ = awaitConfirms(q)
        }
        return Err(e)
      }
      case Ok(_) -> ()
    }
    // Take confirms as they come, so neither side's buffer fills up.
    match drain(q) {
      case Err(e) -> return Err(e)
      case Ok(_) -> ()
    }
  }
  match awaitConfirms(q) {
    case Err(e) -> return Err(e)
    case Ok(_) -> ()
  }
  publishOutcome(q)
}

// `Mq.MessageQueue.publishBatch` requires `allHaveIds(messages)`. The JVM
// backend resolves that inherited clause in the implementing package and
// fails with J008 when the name is not declared there (lyric-lang#9043), so
// this package declares the same predicate and uses it to refuse a batch with
// an empty id before anything is sent.
@pure
func allHaveIds(messages: in slice[Mq.Message]): Bool {
  for m in messages {
    if m.id.length == 0 {
      return false
    }
  }
  true
}

impl Mq.MessageQueue for RabbitMqQueue {
  func publish(message: in Mq.Message): Result[Unit, String]
    requires: message.id.length > 0
  {
    return publishTo(self, "", self.options.queue, true, message, Amqp.noProperties())
  }

  func publishBatch(messages: in slice[Mq.Message]): Result[Unit, String]
    requires: messages.length > 0
  {
    if not allHaveIds(messages) {
      return Err(EMPTY_ID)
    }
    K.lockEnter(self.lock)
    defer {
      K.lockExit(self.lock)
    }
    return publishBatchLocked(self, messages)
  }
}

// ─── Consuming ───────────────────────────────────────────────────────────────
func subscribeLocked(q: in RabbitMqQueue): Result[Unit, String] {
  match checkUsable(q) {
    case Err(e) -> return Err(e)
    case Ok(_) -> ()
  }
  if q.consuming {
    return Ok(())
  }
  if q.options.prefetchCount > 0 {
    val qos = Amqp.BasicQos(prefetchSize = 0i64, prefetchCount = q.options.prefetchCount, global = false)
    match rpc(q, CHANNEL, qos, "basic.qos-ok", true) {
      case Err(e) -> return Err(e)
      case Ok(_) -> ()
    }
  }
  val consume = Amqp.BasicConsume(
    queue = q.options.queue,
    consumerTag = "",
    noLocal = false,
    noAck = false,
    exclusive = false,
    arguments = Amqp.emptyTable()
  )
  match rpc(q, CHANNEL, consume, "basic.consume-ok", true) {
    case Err(e) -> Err(e)
    case Ok(_) -> {
      q.consuming = true
      Ok(())
    }
  }
}

/// Start the consumer now instead of on the first `consume`, so the broker
/// begins filling the prefetch window.
pub func subscribe(q: in RabbitMqQueue): Result[Unit, String] {
  K.lockEnter(q.lock)
  defer {
    K.lockExit(q.lock)
  }
  subscribeLocked(q)
}

// The oldest delivery not yet handed to a handler, waiting up to `waitMs`
// for one to arrive.
func nextDeliveryLocked(q: in RabbitMqQueue, waitMs: in Int): Result[Option[Delivery], String] {
  match subscribeLocked(q) {
    case Err(e) -> return Err(e)
    case Ok(_) -> ()
  }
  val deadline = K.monotonicMillis() + waitMs.toLong()
  while q.backlog.count == 0 {
    val remaining = deadline - K.monotonicMillis()
    val wait = if remaining <= 0i64 { 0 } else { remaining.toInt() }
    match step(q, wait, true) {
      case Err(e) -> return Err(e)
      case Ok(Progress) -> ()
      case Ok(Reply(m)) -> return Err(markLost(q, "the broker sent an unexpected " + Amqp.methodName(m)))
      case Ok(Idle) -> {
        if remaining <= 0i64 {
          return Ok(None)
        }
      }
    }
  }
  val delivery = q.backlog[0]
  q.backlog.removeAt(0)
  Ok(Some(value = delivery))
}

func nextDelivery(q: in RabbitMqQueue, waitMs: in Int): Result[Option[Delivery], String] {
  K.lockEnter(q.lock)
  defer {
    K.lockExit(q.lock)
  }
  nextDeliveryLocked(q, waitMs)
}

func deliveryCountOf(delivery: in Delivery): Int {
  val flagged = if delivery.redelivered { 1 } else { 0 }
  match Amqp.tableGet(delivery.properties.headers, HEADER_DELIVERY_COUNT) {
    case Some(FInteger(_, n)) -> {
      if n > 2147483647i64 {
        2147483647
      } else if n > flagged.toLong() {
        n.toInt()
      } else {
        flagged
      }
    }
    case _ -> flagged
  }
}

func namesKey(headers: in slice[Mq.MessageHeader], key: in String): Bool {
  for h in headers {
    if h.key == key {
      return true
    }
  }
  false
}

/// The `Message.headers` of a message with this `headers` table.
///
/// With a `lyric_headers` entry (a message this client published) the list
/// in it is the answer, in its order and with its repeated keys, whatever
/// the broker did to the plain entries. Plain entries whose key the list
/// does not name (added by the broker, such as `x-death`, or by whatever
/// forwarded the message) follow it, as text. A plain entry whose key the
/// list does name is ignored, even when its value differs.
///
/// Without the entry, or when its value is not a header list, every entry is
/// a header, as text and in the order the broker sent them.
///
/// `x-delivery-count` is never a header; it is `Message.deliveryCount`.
pub func headersOf(table: in slice[Amqp.TableEntry]): slice[Mq.MessageHeader] {
  val carried: Option[slice[Mq.MessageHeader]] = match Amqp.tableGet(table, HEADER_HEADERS) {
    case Some(FString(bytes)) -> decodeHeaderList(bytesText(bytes))
    case _ -> None
  }
  val headers: List[Mq.MessageHeader] = newList()
  match carried {
    case Some(list) -> {
      for h in list {
        headers.add(h)
      }
      for e in table {
        if e.name != HEADER_DELIVERY_COUNT and e.name != HEADER_HEADERS and not namesKey(list, e.name) {
          headers.add(Mq.MessageHeader(key = e.name, value = fieldText(e.value)))
        }
      }
    }
    case None -> {
      for e in table {
        if e.name != HEADER_DELIVERY_COUNT {
          headers.add(Mq.MessageHeader(key = e.name, value = fieldText(e.value)))
        }
      }
    }
  }
  headers.toArray()
}

/// The `Mq.Message` a delivery is handed to a handler as. `Err` when the
/// body is not UTF-8 text.
pub func messageOf(delivery: in Delivery): Result[Mq.Message, String] {
  val body = match tryDecodeUtf8(delivery.body) {
    case Some(s) -> s
    case None -> return Err("its body is not UTF-8 text")
  }
  val headers = headersOf(delivery.properties.headers)
  val id = if delivery.properties.messageId.length > 0 {
    delivery.properties.messageId
  } else {
    "delivery-" + delivery.tag.toString()
  }
  Ok(Mq.Message(id = id, body = body, headers = headers, deliveryCount = deliveryCountOf(delivery)))
}

// Turn a delivery into a tracked message. One this client cannot represent
// is rejected without requeueing (dead-lettered when the queue has a
// dead-letter exchange) so it does not block the queue, and reported as an
// `Err`.
func admit(q: in RabbitMqQueue, delivery: in Delivery): Result[Mq.Message, String] {
  K.lockEnter(q.lock)
  defer {
    K.lockExit(q.lock)
  }
  val decoded: Result[Mq.Message, String] = if delivery.oversize {
    Err(
      "its body of " + delivery.declaredSize.toString() + " bytes exceeds maxMessageBytes (" +
        q.options.maxMessageBytes.toString() +
        ")"
    )
  } else {
    messageOf(delivery)
  }
  val message = match decoded {
    case Ok(m) -> m
    case Err(why) -> {
      val label = if delivery.properties.messageId.length > 0 {
        "message '" + delivery.properties.messageId + "'"
      } else {
        "delivery " + delivery.tag.toString()
      }
      val reject = Amqp.BasicNack(deliveryTag = delivery.tag, multiple = false, requeue = false)
      val outcome = match writeMethod(q, CHANNEL, reject) {
        case Ok(_) -> "; it was rejected without requeueing"
        case Err(e) -> "; rejecting it failed: " + e
      }
      return Err("rabbitmq: " + label + " cannot be delivered: " + why + outcome)
    }
  }
  if q.inFlight.containsKey(message.id) {
    q.inFlight[message.id].add(delivery.tag)
  } else {
    val waiting: List[Long] = newList()
    waiting.add(delivery.tag)
    q.inFlight[message.id] = waiting
  }
  Ok(message)
}

// The delivery tag of the oldest unsettled delivery of `messageId`, removed
// from the in-flight set.
func takeDelivery(q: in RabbitMqQueue, messageId: in String): Result[Long, String] {
  if not q.inFlight.containsKey(messageId) {
    return Err("rabbitmq: no unsettled delivery has message id '" + messageId + "'")
  }
  val waiting = q.inFlight[messageId]
  val tag = waiting[0]
  waiting.removeAt(0)
  if waiting.count == 0 {
    q.inFlight.remove(messageId)
  }
  Ok(tag)
}

// Settle the oldest delivery of `messageId`: 0 = ack, 1 = reject, 2 = requeue.
func settle(q: in RabbitMqQueue, messageId: in String, how: in Int): Result[Unit, String] {
  if messageId.length == 0 {
    return Err(EMPTY_ID)
  }
  K.lockEnter(q.lock)
  defer {
    K.lockExit(q.lock)
  }
  match checkUsable(q) {
    case Err(e) -> return Err(e)
    case Ok(_) -> ()
  }
  // A channel the broker closed since the delivery took every unsettled
  // delivery with it; find out before answering for one of them.
  match drain(q) {
    case Err(e) -> return Err(e)
    case Ok(_) -> ()
  }
  val tag = match takeDelivery(q, messageId) { case Ok(t) -> t; case Err(e) -> return Err(e) }
  val method = if how == 0 {
    Amqp.BasicAck(deliveryTag = tag, multiple = false)
  } else {
    Amqp.BasicNack(deliveryTag = tag, multiple = false, requeue = how == 2)
  }
  writeMethod(q, CHANNEL, method)
}

func isAuto(q: in RabbitMqQueue): Bool {
  match q.options.ackMode {
    case Auto -> true
    case Manual -> false
  }
}

// Wait for the method called `expected`, ignoring everything else.
func awaitQuietly(q: in RabbitMqQueue, expected: in String, limitMs: in Int): Unit {
  val deadline = K.monotonicMillis() + limitMs.toLong()
  while true {
    val remaining = deadline - K.monotonicMillis()
    if remaining <= 0i64 {
      return
    }
    match nextEvent(q, remaining.toInt()) {
      case Err(_) -> {
        return
      }
      case Ok(None) -> {
        return
      }
      case Ok(Some(EvMethod(_, m))) -> {
        if Amqp.methodName(m) == expected {
          return
        }
      }
      case Ok(Some(_)) -> ()
    }
  }
}

func closeLocked(q: in RabbitMqQueue): Unit {
  if q.closed {
    return
  }
  q.closed = true
  if q.lost.length > 0 {
    return
  }
  val limit = if q.options.requestTimeoutMs < 2000 { q.options.requestTimeoutMs } else { 2000 }
  val closing = Amqp.ChannelClose(replyCode = 200, replyText = "closed by the application", classId = 0, methodId = 0)
  match writeMethod(q, CHANNEL, closing) {
    case Ok(_) -> awaitQuietly(q, "channel.close-ok", limit)
    case Err(_) -> ()
  }
  if q.lost.length == 0 {
    val goodbye = Amqp.ConnectionClose(
      replyCode = 200,
      replyText = "closed by the application",
      classId = 0,
      methodId = 0
    )
    match writeMethod(q, 0, goodbye) {
      case Ok(_) -> awaitQuietly(q, "connection.close-ok", limit)
      case Err(_) -> ()
    }
  }
  K.sockClose(q.sock)
}

impl Mq.QueueConsumer for RabbitMqQueue {
  func consume(timeoutMs: in Int, handler: (Mq.Message) -> Result[Unit, String]): Result[Unit, String]
    requires: timeoutMs >= 0
  {
    val deadline = K.monotonicMillis() + timeoutMs.toLong()
    var first = true
    while true {
      val remaining = deadline - K.monotonicMillis()
      if not first and remaining <= 0i64 {
        return Err("timeout")
      }
      first = false
      val wait = if remaining <= 0i64 {
        0
      } else if remaining > WAIT_SLICE_MS.toLong() {
        WAIT_SLICE_MS
      } else {
        remaining.toInt()
      }
      match nextDelivery(self, wait) {
        case Err(e) -> return Err(e)
        case Ok(None) -> ()
        case Ok(Some(delivery)) -> {
          val message = match admit(self, delivery) { case Ok(m) -> m; case Err(e) -> return Err(e) }
          val outcome = handler(message)
          if not isAuto(self) {
            return outcome
          }
          match outcome {
            case Ok(_) -> return settle(self, message.id, 0)
            case Err(e) -> {
              return match settle(self, message.id, 2) {
                case Ok(_) -> Err(e)
                case Err(requeueError) -> Err(e + " (and requeueing the message failed: " + requeueError + ")")
              }
            }
          }
        }
      }
    }
    return Err("timeout")
  }

  func ack(messageId: in String): Result[Unit, String] {
    if messageId.length > 0 and isAuto(self) {
      return Ok(())
    }
    return settle(self, messageId, 0)
  }

  func nack(messageId: in String, requeue: in Bool): Result[Unit, String] {
    if messageId.length > 0 and isAuto(self) {
      return Ok(())
    }
    return settle(self, messageId, if requeue { 2 } else { 1 })
  }

  func close(): Unit {
    K.lockEnter(self.lock)
    defer {
      K.lockExit(self.lock)
    }
    closeLocked(self)
  }
}

// ─── Requests for Mq.RabbitMq.Extras ─────────────────────────────────────────
func replyNameFor(request: in Amqp.Method): String {
  match request {
    case ExchangeDeclare(_, _, _, _, _, _, _) -> "exchange.declare-ok"
    case ExchangeDelete(_, _) -> "exchange.delete-ok"
    case QueueDeclare(_, _, _, _, _, _) -> "queue.declare-ok"
    case QueueBind(_, _, _, _) -> "queue.bind-ok"
    case QueuePurge(_) -> "queue.purge-ok"
    case QueueDelete(_, _, _) -> "queue.delete-ok"
    case _ -> ""
  }
}

/// Send one of the exchange and queue management methods
/// (`exchange.declare`, `exchange.delete`, `queue.declare`, `queue.bind`,
/// `queue.purge`, `queue.delete`) on the connection's channel and return the
/// broker's reply. Any other method is refused: the client's own state
/// depends on the rest. `Mq.RabbitMq.Extras` wraps each of these.
pub func request(q: in RabbitMqQueue, method: in Amqp.Method): Result[Amqp.Method, String] {
  val expected = replyNameFor(method)
  if expected.length == 0 {
    return Err("rabbitmq: " + Amqp.methodName(method) + " cannot be sent through request")
  }
  K.lockEnter(q.lock)
  defer {
    K.lockExit(q.lock)
  }
  match checkUsable(q) {
    case Err(e) -> return Err(e)
    case Ok(_) -> ()
  }
  match drain(q) {
    case Err(e) -> return Err(e)
    case Ok(_) -> ()
  }
  rpc(q, CHANNEL, method, expected, true)
}

/// Send a heartbeat frame and handle whatever the broker has sent since the
/// last operation (its heartbeats, deliveries, a close). An application that
/// negotiated a heartbeat and sits idle between operations calls this at
/// least once per interval; without a heartbeat it is a cheap liveness probe
/// for a connection that may have been closed.
pub func keepAlive(q: in RabbitMqQueue): Result[Unit, String] {
  K.lockEnter(q.lock)
  defer {
    K.lockExit(q.lock)
  }
  match checkUsable(q) {
    case Err(e) -> return Err(e)
    case Ok(_) -> ()
  }
  match writeBytes(q, Amqp.heartbeatFrame()) {
    case Err(e) -> return Err(e)
    case Ok(_) -> ()
  }
  drain(q)
}

// ─── Introspection ───────────────────────────────────────────────────────────
/// The broker's product and version from `connection.start`, e.g.
/// `RabbitMQ 3.12.1`.
pub func serverInfo(q: in RabbitMqQueue): String {
  q.server
}

/// The queue this connection publishes to and consumes from.
pub func queueName(q: in RabbitMqQueue): String {
  q.options.queue
}

/// The negotiated heartbeat interval in seconds; 0 = heartbeats are off.
pub func heartbeatSeconds(q: in RabbitMqQueue): Int {
  (q.heartbeatMs / 1000i64).toInt()
}

/// The negotiated frame size limit in bytes.
pub func negotiatedFrameMax(q: in RabbitMqQueue): Int {
  q.frameMax
}

/// The AMQP reply code of the most recent channel or connection error, or of
/// the most recent returned message (404, 406, 312, ...); 0 when there has
/// been none.
pub func lastReplyCode(q: in RabbitMqQueue): Int {
  K.lockEnter(q.lock)
  defer {
    K.lockExit(q.lock)
  }
  q.lastCode
}

/// The reason the broker gave for blocking publishers (a memory or disk
/// alarm), or an empty string while it is not blocking them.
pub func blockedReason(q: in RabbitMqQueue): String {
  K.lockEnter(q.lock)
  defer {
    K.lockExit(q.lock)
  }
  q.blocked
}

/// True until `close` is called or the connection is lost.
pub func isOpen(q: in RabbitMqQueue): Bool {
  K.lockEnter(q.lock)
  defer {
    K.lockExit(q.lock)
  }
  not q.closed and q.lost.length == 0
}

/// The number of deliveries handed to a handler and not yet settled.
pub func unsettledCount(q: in RabbitMqQueue): Int {
  K.lockEnter(q.lock)
  defer {
    K.lockExit(q.lock)
  }
  var total = 0
  for waiting in mapValues(q.inFlight).toArray() {
    total = total + waiting.count
  }
  total
}
