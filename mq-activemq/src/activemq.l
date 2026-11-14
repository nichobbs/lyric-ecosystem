//! Mq.ActiveMq - Apache ActiveMQ backend for `Mq.MessageQueue` and
//! `Mq.QueueConsumer`, over a pure-Lyric STOMP 1.2 client.
//!
//! One `ActiveMqQueue` is one connection to the broker's STOMP connector
//! (plain TCP for `stomp://`, TLS for `stomp+ssl://`) and one destination. It
//! implements both interfaces, so the same value publishes and consumes.
//!
//!   import Mq.ActiveMq
//!
//!   val queue = Mq.ActiveMq.connectUrl("stomp://app:secret@broker:61613", "orders")?
//!   Mq.publish(queue, Mq.Message(id = "o-1", body = "{}", headers = [], deliveryCount = 0))?
//!   Mq.consume(queue, 5000, { m -> handle(m) })?
//!   Mq.ack(queue, "o-1")?
//!   queue.close()
//!
//! How the interface maps onto the broker:
//!
//! - `publish` is a `SEND` with a `receipt`; it returns once the broker has
//!   answered with the `RECEIPT`, so `Ok` means the broker accepted the
//!   message. `publishBatch` sends the batch inside one STOMP transaction and
//!   returns on the `COMMIT` receipt: all of the batch or none of it.
//! - A message's `id` travels in the `lyric_id` header, its headers as a JSON
//!   array in `lyric_headers` (order, duplicates and every character survive)
//!   and its body as UTF-8 text with a `content-length`, marked
//!   `amq-msg-type:text` so JMS consumers see a `TextMessage`. A message from
//!   another producer (no `lyric_id`) is delivered with its broker
//!   `message-id` as the id and its application headers as the headers.
//! - The first `consume` subscribes with `ack:client-individual`. `ack` sends
//!   `ACK` for that one delivery. A delivery is forgotten once the broker has
//!   confirmed its settlement; after a refused one it can be settled again.
//! - `consume(timeoutMs, ...)` returns `Err("timeout")` when no message could
//!   be handed to the handler within `timeoutMs`, counted from the call: the
//!   wait for the connection's lock and for the subscription's receipt are
//!   part of it.
//! - A message this client cannot hand to a handler (a body that is not
//!   UTF-8, malformed `lyric_` headers, a body over `maxFrameBytes` or headers
//!   over 64 KiB) is reported by `consume` as an `Err` and, by default, left
//!   unacknowledged on the broker; `onUndeliverable` chooses.
//! - `nack(id, false)` sends `NACK`. ActiveMQ treats a STOMP `NACK` as a
//!   poison acknowledgement: the message is never redelivered; a persistent
//!   message moves to the dead-letter queue (`ActiveMQ.DLQ` unless the broker
//!   configures another strategy) and a non-persistent one is discarded
//!   (the broker's default `processNonPersistent = false`).
//! - `nack(id, true)` requeues. ActiveMQ has no STOMP frame that returns one
//!   delivery to its queue, so the requeue is one STOMP transaction that sends
//!   a copy of the message (same body and headers, `lyric_delivery_count`
//!   raised by one) to the destination it came from and acknowledges the
//!   original. The broker applies both or neither. The copy joins the tail of
//!   the queue, as in the reference in-process broker.
//! - `deliveryCount` is `lyric_delivery_count`, plus one when the broker marks
//!   the delivery `redelivered` (it does so for deliveries a consumer left
//!   unacknowledged when it unsubscribed or disconnected).
//! - `close` unsubscribes, sends `DISCONNECT` with a receipt and closes the
//!   socket. Deliveries not yet settled go back to the queue.
//!
//! Heart-beats (STOMP 1.2) are negotiated by `CONNECT`, 10 s in each
//! direction by default (`heartBeatSendMs`, `heartBeatReceiveMs`; 0 switches
//! a direction off). A background thread per connection, the keeper, writes
//! a newline when nothing was written for half of the agreed interval, reads
//! what the broker sends while no operation is doing so, and declares the
//! connection lost when nothing at all arrived for twice the agreed interval.
//!
//! A lost connection is re-established by the keeper (`reconnect`, on by
//! default): capped exponential backoff with jitter, a new `CONNECT`, and a
//! new `SUBSCRIBE` when the connection had one. While that goes on, `publish`,
//! `ack` and `nack` fail at once with a "not connected, reconnecting" error
//! (`isNotConnectedError`: nothing was sent, try again later) and `consume`
//! waits within its own timeout. Nothing is ever sent twice: a request that
//! was in flight when the connection dropped reports that its outcome is
//! unknown (`isOutcomeUnknownError`). Deliveries received on a lost
//! connection cannot be settled any more (`isStaleDeliveryError`); the broker
//! redelivers them. A broker that refuses the new `CONNECT` or `SUBSCRIBE`
//! ends the attempts, as does `maxAttempts`.
//!
//! An `ActiveMqQueue` may be shared between threads: operations take the
//! connection's lock one at a time, and a blocked `consume` releases it every
//! 100 ms so a publisher on another thread is not held for the whole wait.
//! The handler runs outside the lock and may call `ack` / `nack` itself.
//!
//! An `ERROR` frame that carries the `receipt-id` of the request in flight
//! fails that request and leaves the connection usable. Any other `ERROR`
//! frame cannot be attributed, so the connection is treated as lost, as
//! STOMP 1.2 has the broker close it after an `ERROR`.
//!
//! With `reconnect.enabled = false` a lost connection stays lost: every
//! operation returns `Err`; open a new connection.
//!
//! TLS: a `stomp+ssl://` URL, or `withTls()` on the options, runs a TLS
//! handshake before the STOMP `CONNECT`. The server certificate is verified
//! against the platform trust store and its name against the host, unless
//! the options say otherwise:
//!
//!   val options = Mq.ActiveMq.optionsFromUrl("stomp+ssl://app:secret@broker:61614", "orders")?
//!     .withCaCertificate(caPem)
//!     .withClientIdentity(certPem, keyPem)
//!   val queue = Mq.ActiveMq.connectWith(options)?
//!
//! The option names and the rule for skipping verification
//! (`withInsecureSkipVerify()` and `LYRIC_TLS_ALLOW_INSECURE=1` together) are
//! those of `Std.Http.HttpClientBuilder`. Everything above the handshake is
//! the same code for both transports.
@runtime_checked
@io
package Mq.ActiveMq

import Std.Core
import Std.Collections
import Std.Json
import Std.Parse
import Std.String as Str
import Std.Environment as Env
import Std.Console as Console
import Std.Tls as Tls
import Std.SecureRandom
import Net.Tcp as K
import Net.Threads as Threads
import Net.Tls
import Mq.ActiveMq.Stomp as Stomp
import Mq.ActiveMq.Url as Url
import Mq
import Std.Encoding
pub use Mq
pub use Net.Tls

/// The header that carries `Message.id`.
pub val HEADER_ID: String = "lyric_id"

/// The header that carries `Message.headers` as a JSON array.
pub val HEADER_HEADERS: String = "lyric_headers"

/// The header that carries the number of earlier deliveries.
pub val HEADER_DELIVERY_COUNT: String = "lyric_delivery_count"

/// The environment variable that must be `1` for `withInsecureSkipVerify()`
/// to take effect (the standard library's, docs/61 §4 in lyric-lang).
pub val INSECURE_ALLOW_ENV: String = "LYRIC_TLS_ALLOW_INSECURE"

/// What `consume` does with a message it cannot hand to a handler: a body that
/// is not UTF-8 text, malformed `lyric_headers` or `lyric_delivery_count`, a
/// body over `maxFrameBytes`, or a header section over 64 KiB.
pub enum UndeliverablePolicy {
  /// Leave it unacknowledged and report it. Nothing is lost: the broker
  /// redelivers it when this connection closes, and the application may
  /// settle it by the broker id the error names. Until then it occupies one
  /// place of the subscription's prefetch window.
  case LeaveUnacknowledged
  /// Send `NACK` and report it. ActiveMQ moves a persistent message to its
  /// dead-letter queue and discards a non-persistent one, so this destroys
  /// or diverts messages meant for other consumers of a shared queue.
  case RejectToBroker
}

/// How a lost connection is re-established.
pub record ReconnectOptions {
  /// False: a lost connection stays lost and every operation returns `Err`.
  enabled: Bool
  /// Milliseconds before the first attempt; doubled after every failed
  /// attempt, up to `maxDelayMs`. At least 1.
  initialDelayMs: Int
  /// The longest wait between two attempts, in milliseconds.
  maxDelayMs: Int
  /// Failed attempts in a row after which reconnecting is given up; 0 = no
  /// limit.
  maxAttempts: Int
  /// Each wait is shortened by a random part of up to this many percent of
  /// it (0 to 100), so that clients that lost the same broker do not all
  /// return at the same moment.
  jitterPercent: Int
  /// How long `publish`, `publishBatch` and `publishWithHeaders` wait for a
  /// connection that is being re-established before they fail, in
  /// milliseconds; 0 = fail at once.
  publishWaitMs: Int
}

/// Reconnecting switched on: first attempt after 1 s, doubling up to 30 s,
/// 20 % jitter, no limit on the attempts, and a publish that fails at once
/// while there is no connection.
pub func defaultReconnect(): ReconnectOptions {
  ReconnectOptions(
    enabled = true,
    initialDelayMs = 1000,
    maxDelayMs = 30000,
    maxAttempts = 0,
    jitterPercent = 20,
    publishWaitMs = 0
  )
}

/// Reconnecting switched off.
pub func noReconnect(): ReconnectOptions {
  val base = defaultReconnect()
  base.copy(enabled = false)
}

/// How to reach a broker and which destination to use.
pub record ConnectOptions {
  host: String
  port: Int
  /// Empty for a broker without authentication.
  login: String
  passcode: String
  /// The STOMP `host` header; empty sends the `host` field.
  virtualHost: String
  /// A STOMP destination: `/queue/NAME` or `/topic/NAME`.
  destination: String
  /// `Manual` (the default): the application calls `ack` / `nack`. `Auto`:
  /// `consume` acknowledges when the handler returns `Ok` and requeues when it
  /// returns `Err`; `ack` and `nack` are then no-ops, except for a delivery
  /// `consume` could not settle (the broker refused the settlement, or the
  /// message was undeliverable).
  ackMode: Mq.AckMode
  /// Whether published messages are persistent (survive a broker restart and
  /// are dead-lettered rather than discarded on `nack(id, false)`).
  persistent: Bool
  /// A JMS selector for the subscription; empty selects everything.
  selector: String
  /// The broker's dispatch window for the subscription
  /// (`activemq.prefetchSize`); 0 keeps the broker's default.
  prefetchSize: Int
  /// Milliseconds to wait for the TCP connection, and again for the TLS
  /// handshake of a TLS connection; 0 = the system's limit.
  connectTimeoutMs: Int
  /// Milliseconds to wait for the broker to answer a frame, and the longest
  /// a write may make no progress because the broker is not reading.
  requestTimeoutMs: Int
  /// The largest message body accepted from the broker, in bytes. A larger
  /// message is read past without being kept, and handled as
  /// `onUndeliverable` says; the connection stays usable.
  maxFrameBytes: Int
  /// What to do with a message that cannot be handed to a handler.
  onUndeliverable: UndeliverablePolicy
  /// TLS settings; `plainTcp()` for a `stomp://` connector.
  tls: TlsOptions
  /// The heart-beat this client offers to send: the smallest number of
  /// milliseconds it guarantees between two writes (the first number of the
  /// STOMP `heart-beat` header); 0 = it sends none. The broker may ask for a
  /// longer interval, not a shorter one.
  heartBeatSendMs: Int
  /// The heart-beat this client asks the broker for: the number of
  /// milliseconds between two writes of the broker (the second number of
  /// the header); 0 = none, and a dead peer is then only noticed by an
  /// operation. The broker may answer with a longer interval.
  heartBeatReceiveMs: Int
  /// Whether and how a lost connection is re-established.
  reconnect: ReconnectOptions
}

/// Options for `destination` on `host:port` with the defaults: plain TCP, no
/// credentials, manual acknowledgement, persistent messages, a 10 s connect
/// and request timeout, a 16 MiB frame limit, undeliverable messages left on
/// the broker, heart-beats every 10 s in both directions and reconnecting
/// switched on (`defaultReconnect()`). Adjust with `.copy(...)` and the
/// `with...` functions below.
pub func defaultOptions(host: in String, port: in Int, destination: in String): ConnectOptions {
  ConnectOptions(
    host = host,
    port = port,
    login = "",
    passcode = "",
    virtualHost = "",
    destination = destination,
    ackMode = Mq.AckMode.Manual,
    persistent = true,
    selector = "",
    prefetchSize = 0,
    connectTimeoutMs = 10000,
    requestTimeoutMs = 10000,
    maxFrameBytes = 16777216,
    onUndeliverable = UndeliverablePolicy.LeaveUnacknowledged,
    tls = plainTcp(),
    heartBeatSendMs = 10000,
    heartBeatReceiveMs = 10000,
    reconnect = defaultReconnect()
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

/// The STOMP destination for `name`: a name that starts with `/` is already
/// one (`/queue/a`, `/topic/b`); anything else names a queue.
pub func resolveDestination(name: in String): String {
  if Str.startsWith(name, "/") {
    name
  } else {
    "/queue/" + name
  }
}

pub record Delivery {
  ackId: String
  frame: Stomp.Frame
  deliveryCount: Int
  // False for a message that was too large to read: there is no body to copy.
  requeueable: Bool
  // In `Auto` mode: `ack` / `nack` act on this delivery, because `consume`
  // will not settle it (it was never handed to a handler, or its automatic
  // settlement was refused).
  byHand: Bool
  // The connection it arrived on (`ActiveMqQueue.epoch`).
  epoch: Int
  // True once that connection is lost: the broker no longer knows `ackId`
  // and redelivers the message. The frame is dropped then.
  stale: Bool
}

/// An open connection to one ActiveMQ destination. Obtain one from
/// `connectWith`, `connectUrl` or `connectFromEnv`.
///
/// Three locks: `lock` serializes operations and guards every field not
/// named below; `writeLock` serializes writes to the socket and guards
/// `lastWriteAt`, `writable` and `sendIntervalMs`; `flagsLock` guards
/// `closing`, `keeperRunning` and `keeperTurn`. They are taken in that order.
pub ref record ActiveMqQueue {
  options: ConnectOptions
  var sock: K.Sock
  var decoder: Stomp.Decoder
  lock: K.Lock
  writeLock: K.Lock
  flagsLock: K.Lock
  var server: String
  var lost: String
  var closed: Bool
  var subscribed: Bool
  var counter: Int
  var backlog: List[Stomp.Decoded]
  var inFlight: Map[String, List[Delivery]]
  // The receipt of a SUBSCRIBE the broker has not answered yet ("" = none),
  // and the time by which it must.
  var subscribeReceipt: String
  var subscribeDeadline: Long
  // The broker's refusal of the SUBSCRIBE, kept for the next `consume` when
  // it arrived while another request was waiting.
  var subscribeFailure: String
  // Whether a subscription was asked for, so that a new connection opens it
  // again.
  var wantSubscription: Bool
  // Why reconnecting was given up ("" = it was not): final.
  var gaveUp: String
  // The number of the current connection: 0 for the first, one more for
  // each one that replaced a lost one.
  var epoch: Int
  // Failed attempts since the connection was lost, and the last failure.
  var attempts: Int
  var lastAttemptError: String
  // The agreed heart-beat intervals in milliseconds, 0 = none.
  var sendIntervalMs: Int
  var receiveIntervalMs: Int
  // When bytes last arrived from, and were last written to, the broker.
  var lastReadAt: Long
  var lastWriteAt: Long
  // False once the socket must not be written to by the keeper.
  var writable: Bool
  // Set by `close` before it waits for `lock`, so that the keeper stops
  // waiting.
  var closing: Bool
  var keeperRunning: Bool
  // True while the keeper takes or holds `lock`, which it does for a moment
  // every tick. `consume` waits that moment out instead of counting it
  // against a short timeout.
  var keeperTurn: Bool
}

val EMPTY_ID: String = "activemq: empty message id"

val SUBSCRIPTION_ID: String = "lyric-sub-0"

val WAIT_SLICE_MS: Int = 100

val READ_CHUNK_BYTES: Int = 16384

// The connection is taken for dead when nothing arrived for this many agreed
// heart-beat intervals.
val HEART_BEAT_TOLERANCE: Int = 2

// The keeper stops reading ahead when this many messages wait for `consume`.
val KEEPER_BACKLOG_LIMIT: Int = 64

// The keeper's longest sleep, which bounds how late it notices `close`.
val KEEPER_TICK_MS: Int = 250

// Deliveries of a lost connection are remembered for this many reconnects.
val STALE_EPOCHS_KEPT: Int = 8

// `consume` waits for the lock in pieces of this length while the keeper
// has it.
val KEEPER_GRACE_MS: Int = 50

val NOT_CONNECTED_PREFIX: String = "activemq: not connected, reconnecting"

val OUTCOME_UNKNOWN: String = "the outcome is unknown"

val STALE_DELIVERY_PREFIX: String = "activemq: stale delivery"

/// True for the error of an operation that found the connection lost and
/// being re-established: nothing was sent, and the same call can succeed
/// later.
pub func isNotConnectedError(error: in String): Bool {
  Str.startsWith(error, NOT_CONNECTED_PREFIX)
}

/// True for the error of a request that was in flight when the connection
/// was lost: the broker may or may not have applied it, and this client did
/// not and will not send it again.
pub func isOutcomeUnknownError(error: in String): Bool {
  Str.contains(error, OUTCOME_UNKNOWN)
}

/// True for the error of an `ack` or `nack` of a delivery that arrived on a
/// connection that has since been lost. The broker redelivers the message;
/// settle that delivery instead.
pub func isStaleDeliveryError(error: in String): Bool {
  Str.startsWith(error, STALE_DELIVERY_PREFIX)
}

func header(name: in String, value: in String): Stomp.Header {
  Stomp.Header(name = name, value = value)
}

// The broker's explanation in an ERROR frame: the `message` header, or the
// first line of the body when the header is absent; "" when it gives none.
func errorDetail(frame: in Stomp.Frame): String {
  val summary = Stomp.headerOr(frame, "message", "")
  if summary.length > 0 {
    return summary
  }
  val body = match Stomp.bodyText(frame) { case Some(s) -> s; case None -> "" }
  val lineEnd = Str.indexOfRaw(body, "\n")
  val first = if lineEnd < 0 { body } else { Str.substring(body, 0, lineEnd) }
  Str.trim(first)
}

func errorText(frame: in Stomp.Frame): String {
  val detail = errorDetail(frame)
  if detail.length > 0 {
    "activemq: broker error: " + detail
  } else {
    "activemq: broker error"
  }
}

// ─── Transport (callers hold the lock) ───────────────────────────────────────
func emptyFrame(): Stomp.Frame {
  val none: slice[Stomp.Header] = []
  Stomp.Frame(command = "", headers = none, body = noBody())
}

// The deliveries of the connection that was just lost can no longer be
// settled. They stay known, without their frame, so that `ack` and `nack`
// can say what happened to them.
func retireDeliveries(q: in ActiveMqQueue): Unit {
  for id in mapKeys(q.inFlight).toArray() {
    val waiting = q.inFlight[id]
    var i = 0
    while i < waiting.count {
      if not waiting[i].stale {
        waiting[i] = waiting[i].copy(stale = true, frame = emptyFrame())
      }
      i += 1
    }
  }
}

func markLost(q: in ActiveMqQueue, reason: in String): String {
  if q.lost.length == 0 {
    q.lost = reason
    // Closing first ends a heart-beat write that is stuck, which holds
    // `writeLock`.
    K.sockClose(q.sock)
    K.lockEnter(q.writeLock)
    q.writable = false
    K.lockExit(q.writeLock)
    if q.options.reconnect.enabled {
      retireDeliveries(q)
      q.backlog = newList()
    }
  }
  "activemq: connection lost: " + q.lost
}

func checkUsable(q: in ActiveMqQueue): Result[Unit, String] {
  if q.closed {
    return Err("activemq: the connection is closed")
  }
  if q.gaveUp.length > 0 {
    return Err("activemq: connection lost: " + q.gaveUp)
  }
  if q.lost.length > 0 {
    if q.options.reconnect.enabled {
      return Err(NOT_CONNECTED_PREFIX + " (nothing was sent); connection lost: " + q.lost)
    }
    return Err("activemq: connection lost: " + q.lost)
  }
  Ok(())
}

func writeFrame(q: in ActiveMqQueue, frame: in Stomp.Frame): Result[Unit, String] {
  val bytes = match Stomp.encodeFrame(frame) { case Ok(b) -> b; case Err(e) -> return Err("activemq: " + e) }
  K.lockEnter(q.writeLock)
  val outcome = K.sockWriteWithin(q.sock, bytes, q.options.requestTimeoutMs)
  q.lastWriteAt = K.monotonicMillis()
  K.lockExit(q.writeLock)
  match outcome {
    case Ok(_) -> Ok(())
    case Err(e) -> Err(markLost(q, e))
  }
}

// Why the broker must be taken for dead, "" when it need not: it agreed to
// send something every `receiveIntervalMs` and nothing arrived for
// `HEART_BEAT_TOLERANCE` times that. Only meaningful right after a read that
// found the socket empty.
func peerSilence(q: in ActiveMqQueue): String {
  if q.receiveIntervalMs == 0 {
    return ""
  }
  val silentFor = K.monotonicMillis() - q.lastReadAt
  if silentFor <= q.receiveIntervalMs.toLong() * HEART_BEAT_TOLERANCE.toLong() {
    return ""
  }
  "the broker sent nothing for " + silentFor.toString() + " ms, having agreed to a heart-beat every " +
    q.receiveIntervalMs.toString() +
    " ms"
}

// The next frame, waiting at most `timeoutMs` for it (0 = only what has
// already arrived). `Ok(None)` is a timeout.
func readFrame(q: in ActiveMqQueue, timeoutMs: in Int): Result[Option[Stomp.Decoded], String] {
  val deadline = K.monotonicMillis() + timeoutMs.toLong()
  while true {
    match Stomp.nextDecoded(q.decoder) {
      case Err(e) -> return Err(markLost(q, e))
      case Ok(Some(item)) -> return Ok(Some(value = item))
      case Ok(None) -> ()
    }
    val remaining = deadline - K.monotonicMillis()
    val wait = if remaining <= 0i64 { 0 } else { remaining.toInt() }
    match K.sockRead(q.sock, READ_CHUNK_BYTES, wait) {
      case Err(e) -> return Err(markLost(q, e))
      case Ok(Eof) -> return Err(markLost(q, "the broker closed the connection"))
      case Ok(TimedOut) -> {
        val silence = peerSilence(q)
        if silence.length > 0 {
          return Err(markLost(q, silence))
        }
        return Ok(None)
      }
      case Ok(Data(bytes)) -> {
        q.lastReadAt = K.monotonicMillis()
        Stomp.feed(q.decoder, bytes)
      }
    }
  }
  Ok(None)
}

func nextToken(q: in ActiveMqQueue, prefix: in String): String {
  q.counter = q.counter + 1
  prefix + "-" + q.counter.toString()
}

// Act on a frame from the broker while `awaited` is the receipt the caller
// waits for ("" = none). `Ok(true)`: that receipt arrived. `Err`: the broker
// refused the awaited request (the connection stays usable), or the
// connection is lost.
//
// A MESSAGE is kept for `consume`. The receipt or refusal of a SUBSCRIBE that
// an earlier `consume` did not wait for is recorded. An ERROR frame is the
// awaited request's only when it carries that request's `receipt-id`; one
// that does not answers some frame sent without a receipt, or nothing this
// client sent, and STOMP 1.2 has the broker close the connection after it.
func route(q: in ActiveMqQueue, item: in Stomp.Decoded, awaited: in String): Result[Bool, String] {
  val frame = match item {
    case Complete(f) -> f
    case TooLarge(skipped) -> {
      if skipped.command == "MESSAGE" {
        q.backlog.add(item)
        return Ok(false)
      }
      return Err(markLost(q, "the broker sent a " + skipped.command + " frame this client cannot read: " + skipped.reason))
    }
  }
  if frame.command == "MESSAGE" {
    q.backlog.add(item)
    return Ok(false)
  }
  val receiptId = Stomp.headerOr(frame, "receipt-id", "")
  val answersSubscribe = receiptId.length > 0 and receiptId == q.subscribeReceipt
  val answersAwaited = receiptId.length > 0 and receiptId == awaited
  if frame.command == "RECEIPT" {
    if answersSubscribe {
      q.subscribeReceipt = ""
      q.subscribed = true
    }
    return Ok(answersAwaited)
  }
  if frame.command != "ERROR" {
    return Ok(false)
  }
  if answersSubscribe {
    q.subscribeReceipt = ""
    q.wantSubscription = false
    if not answersAwaited {
      q.subscribeFailure = errorText(frame)
      return Ok(false)
    }
  }
  if answersAwaited {
    return Err(errorText(frame))
  }
  val detail = errorDetail(frame)
  Err(
    markLost(
      q,
      "the broker sent an ERROR frame that answers no request in flight" +
        (if detail.length > 0 { ": " + detail } else { "" })
    )
  )
}

// Wait for the RECEIPT named `receiptId`.
func awaitReceipt(q: in ActiveMqQueue, receiptId: in String, timeoutMs: in Int): Result[Unit, String] {
  val deadline = K.monotonicMillis() + timeoutMs.toLong()
  while true {
    val remaining = deadline - K.monotonicMillis()
    val wait = if remaining <= 0i64 { 0 } else { remaining.toInt() }
    val item = match readFrame(q, wait) {
      case Err(e) -> return Err(e)
      case Ok(None) -> return Err(markLost(q, "the broker did not answer within " + timeoutMs.toString() + " ms"))
      case Ok(Some(i)) -> i
    }
    match route(q, item, receiptId) {
      case Err(e) -> return Err(e)
      case Ok(true) -> return Ok(())
      case Ok(false) -> ()
    }
  }
  Ok(())
}

// Send `command` with a receipt and wait for the broker to confirm it.
func request(
  q: in ActiveMqQueue, command: in String, headers: in List[Stomp.Header], body: in slice[Byte]
): Result[Unit, String] {
  val receiptId = nextToken(q, "lyric-r")
  headers.add(header("receipt", receiptId))
  match writeFrame(q, Stomp.Frame(command = command, headers = headers.toArray(), body = body)) {
    case Err(e) -> return Err(e)
    case Ok(_) -> ()
  }
  awaitReceipt(q, receiptId, q.options.requestTimeoutMs)
}

func send(
  q: in ActiveMqQueue, command: in String, headers: in List[Stomp.Header], body: in slice[Byte]
): Result[Unit, String] {
  writeFrame(q, Stomp.Frame(command = command, headers = headers.toArray(), body = body))
}

func noBody(): slice[Byte] {
  val empty: slice[Byte] = []
  empty
}

// ─── Connecting ──────────────────────────────────────────────────────────────
func validate(options: in ConnectOptions): Result[Unit, String] {
  if options.host.length == 0 {
    return Err("activemq: no broker host configured")
  }
  if options.port < 1 or options.port > 65535 {
    return Err("activemq: broker port " + options.port.toString() + " is out of range")
  }
  if not Str.startsWith(options.destination, "/") or options.destination.length < 2 {
    return Err("activemq: destination '" + options.destination + "' is not a STOMP destination such as /queue/NAME")
  }
  if options.prefetchSize < 0 {
    return Err("activemq: prefetchSize must not be negative")
  }
  if options.connectTimeoutMs < 0 {
    return Err("activemq: connectTimeoutMs must not be negative")
  }
  if options.requestTimeoutMs < 1 {
    return Err("activemq: requestTimeoutMs must be at least 1")
  }
  if options.maxFrameBytes < 1 {
    return Err("activemq: maxFrameBytes must be at least 1")
  }
  if options.heartBeatSendMs < 0 or options.heartBeatReceiveMs < 0 {
    return Err("activemq: heartBeatSendMs and heartBeatReceiveMs must not be negative")
  }
  val r = options.reconnect
  if r.initialDelayMs < 1 {
    return Err("activemq: reconnect.initialDelayMs must be at least 1")
  }
  if r.maxDelayMs < r.initialDelayMs {
    return Err("activemq: reconnect.maxDelayMs must not be less than reconnect.initialDelayMs")
  }
  if r.maxAttempts < 0 {
    return Err("activemq: reconnect.maxAttempts must not be negative (0 is no limit)")
  }
  if r.jitterPercent < 0 or r.jitterPercent > 100 {
    return Err("activemq: reconnect.jitterPercent must be between 0 and 100")
  }
  if r.publishWaitMs < 0 {
    return Err("activemq: reconnect.publishWaitMs must not be negative")
  }
  match validateTlsOptions(options.tls) {
    case Ok(_) -> Ok(())
    case Err(problem) -> Err("activemq: " + tlsProblemText(problem, "a stomp+ssl:// URL"))
  }
}

// The transport: a TCP connection, secured when the options say so.
func openTransport(options: in ConnectOptions, endpoint: in String): Result[K.Sock, String] {
  val plain = match K.sockConnect(options.host, options.port, options.connectTimeoutMs) {
    case Ok(s) -> s
    case Err(e) -> return Err("activemq: cannot connect to " + endpoint + ": " + e)
  }
  if not options.tls.enabled {
    return Ok(plain)
  }
  val warning = insecureIgnoredWarning(options.tls, "Mq.ActiveMq")
  if warning.length > 0 {
    Console.error(warning)
  }
  match K.sockStartTls(plain, options.tls, options.host, skipsVerification(options.tls), options.connectTimeoutMs) {
    case Ok(s) -> Ok(s)
    case Err(e) -> Err("activemq: TLS handshake with " + endpoint + " failed: " + e)
  }
}

// Why a connection could not be opened. `permanent`: the broker answered
// and refused, so trying again with the same options cannot succeed.
record OpenFailure {
  text: String
  permanent: Bool
}

func transient(text: in String): Result[ActiveMqQueue, OpenFailure] {
  Err(OpenFailure(text = text, permanent = false))
}

func permanent(text: in String): Result[ActiveMqQueue, OpenFailure] {
  Err(OpenFailure(text = text, permanent = true))
}

// One number of a `heart-beat` header; anything that is not one counts as 0.
func heartBeatNumber(text: in String): Int {
  match parseOptInt(Str.trim(text)) {
    case Some(n) -> if n > 0 { n } else { 0 }
    case None -> 0
  }
}

// The interval agreed for one direction (STOMP 1.2, "Heart-beating"): none
// when either side says 0, otherwise the larger of what the sender
// guarantees and what the receiver asks for.
func agreedInterval(guaranteed: in Int, wanted: in Int): Int {
  if guaranteed == 0 or wanted == 0 {
    0
  } else if guaranteed > wanted {
    guaranteed
  } else {
    wanted
  }
}

// Open a transport, send CONNECT and read the answer. The result is a
// connection without a keeper.
func openSession(options: in ConnectOptions): Result[ActiveMqQueue, OpenFailure] {
  val endpoint = options.host + ":" + options.port.toString()
  val sock = match openTransport(options, endpoint) { case Ok(s) -> s; case Err(e) -> return transient(e) }
  val now = K.monotonicMillis()
  val q = ActiveMqQueue(
    options = options,
    sock = sock,
    decoder = Stomp.newDecoder(options.maxFrameBytes),
    lock = K.newLock(),
    writeLock = K.newLock(),
    flagsLock = K.newLock(),
    server = "",
    lost = "",
    closed = false,
    subscribed = false,
    counter = 0,
    backlog = newList(),
    inFlight = newMap(),
    subscribeReceipt = "",
    subscribeDeadline = 0i64,
    subscribeFailure = "",
    wantSubscription = false,
    gaveUp = "",
    epoch = 0,
    attempts = 0,
    lastAttemptError = "",
    sendIntervalMs = 0,
    receiveIntervalMs = 0,
    lastReadAt = now,
    lastWriteAt = now,
    writable = false,
    closing = false,
    keeperRunning = false,
    keeperTurn = false
  )
  val headers: List[Stomp.Header] = newList()
  headers.add(header("accept-version", "1.2"))
  headers.add(header("host", if options.virtualHost.length > 0 { options.virtualHost } else { options.host }))
  if options.login.length > 0 {
    headers.add(header("login", options.login))
    headers.add(header("passcode", options.passcode))
  }
  headers.add(header("heart-beat", options.heartBeatSendMs.toString() + "," + options.heartBeatReceiveMs.toString()))
  match send(q, "CONNECT", headers, noBody()) {
    case Err(e) -> return transient(e)
    case Ok(_) -> ()
  }
  val answer = match readFrame(q, options.requestTimeoutMs) {
    // ActiveMQ refuses a login with an ERROR frame and closes the connection
    // at once; now and then the close overtakes the frame, or cuts a TLS
    // record short, and the refusal arrives as a bare close or a read error.
    case Err(_) -> return transient(
      "activemq: connection lost: " + endpoint +
        " ended the connection during login without a readable answer; a broker does that when it refuses the login or passcode (" +
        q.lost +
        ")"
    )
    case Ok(None) -> {
      K.sockClose(sock)
      return transient(
        "activemq: " + endpoint + " did not answer CONNECT within " + options.requestTimeoutMs.toString() + " ms"
      )
    }
    case Ok(Some(item)) -> match item {
      case Complete(f) -> f
      case TooLarge(skipped) -> {
        K.sockClose(sock)
        return permanent(
          "activemq: " + endpoint + " answered CONNECT with a " + skipped.command + " frame this client cannot read: " +
            skipped.reason
        )
      }
    }
  }
  if answer.command == "ERROR" {
    K.sockClose(sock)
    return permanent(errorText(answer))
  }
  if answer.command != "CONNECTED" {
    K.sockClose(sock)
    return permanent("activemq: " + endpoint + " answered CONNECT with " + answer.command)
  }
  val version = Stomp.headerOr(answer, "version", "1.0")
  if version != "1.2" {
    K.sockClose(sock)
    return permanent("activemq: the broker speaks STOMP " + version + "; this client needs 1.2")
  }
  q.server = Stomp.headerOr(answer, "server", "")
  val theirs = Stomp.headerOr(answer, "heart-beat", "0,0")
  val comma = Str.indexOfRaw(theirs, ",")
  val brokerSends = if comma < 0 { 0 } else { heartBeatNumber(Str.substring(theirs, 0, comma)) }
  val brokerWants = if comma < 0 { 0 } else { heartBeatNumber(Str.substring(theirs, comma + 1)) }
  q.sendIntervalMs = agreedInterval(options.heartBeatSendMs, brokerWants)
  q.receiveIntervalMs = agreedInterval(brokerSends, options.heartBeatReceiveMs)
  q.lastReadAt = K.monotonicMillis()
  q.writable = true
  Ok(q)
}

/// Connect to the broker and authenticate. The subscription is opened by the
/// first `consume` (or by `subscribe`). Connecting is tried once; it is a
/// connection that was established and then lost that `reconnect` restores.
pub func connectWith(options: in ConnectOptions): Result[ActiveMqQueue, String] {
  match validate(options) {
    case Err(e) -> return Err(e)
    case Ok(_) -> ()
  }
  val q = match openSession(options) { case Ok(opened) -> opened; case Err(failure) -> return Err(failure.text) }
  if options.reconnect.enabled or q.sendIntervalMs > 0 or q.receiveIntervalMs > 0 {
    q.keeperRunning = true
    val _ = Threads.start({ -> keeperMain(q) })
  }
  Ok(q)
}

/// The options for the broker named by a
/// `stomp://[login[:passcode]@]host[:port]` or
/// `stomp+ssl://[login[:passcode]@]host[:port]` URL and for `queueName`: a
/// bare name is a queue, `/queue/NAME` and `/topic/NAME` are taken as
/// written. A `stomp+ssl` URL turns TLS on with the defaults (platform trust
/// store, name checked against the host); add a CA or a client identity with
/// the `with...` functions before `connectWith`.
pub func optionsFromUrl(url: in String, queueName: in String): Result[ConnectOptions, String] {
  if queueName.length == 0 {
    return Err("activemq: no queue name configured")
  }
  val address = match Url.parseUrl(url) { case Ok(a) -> a; case Err(e) -> return Err(e) }
  val base = defaultOptions(address.host, address.port, resolveDestination(queueName))
  val withLogin = base.copy(login = address.login, passcode = address.passcode)
  if address.tls {
    Ok(withLogin.withTls())
  } else {
    Ok(withLogin)
  }
}

/// Connect to the broker named by a `stomp://` or `stomp+ssl://` URL (see
/// `optionsFromUrl`) and use `queueName`.
pub func connectUrl(url: in String, queueName: in String): Result[ActiveMqQueue, String] {
  match optionsFromUrl(url, queueName) {
    case Ok(options) -> connectWith(options)
    case Err(e) -> Err(e)
  }
}

func envName(field: in String): String {
  "LYRIC_CONFIG_MQ_ACTIVEMQ_" + field
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

// The variable `field` as a number that is not negative, `fallback` when it
// is unset or empty.
func envInt(field: in String, fallback: in Int): Result[Int, String] {
  val text = envOr(field, "")
  if text.length == 0 {
    return Ok(fallback)
  }
  match parseOptInt(Str.trim(text)) {
    case Some(n) -> if n >= 0 { Ok(n) } else { Err("activemq: " + envName(field) + " must not be negative") }
    case None -> Err("activemq: " + envName(field) + " must be a whole number")
  }
}

// The TLS settings of `connectFromEnv`, applied to `base`.
func tlsFromEnv(base: in ConnectOptions): Result[ConnectOptions, String] {
  match tlsOptionsFromEnv(envName(""), base.tls) {
    case Ok(tls) -> Ok(base.copy(tls = tls))
    case Err(e) -> Err("activemq: " + e)
  }
}

/// Connect using the environment (a variable that is empty counts as
/// unset):
///
/// - `LYRIC_CONFIG_MQ_ACTIVEMQ_URL`: the broker URL (required)
/// - `LYRIC_CONFIG_MQ_ACTIVEMQ_QUEUENAME`: the queue or destination (required)
/// - `LYRIC_CONFIG_MQ_ACTIVEMQ_ACKMODE`: `Manual` (default) or `Auto`
/// - `LYRIC_CONFIG_MQ_ACTIVEMQ_ONUNDELIVERABLE`: `Leave` (default) or
///   `Reject`, the `onUndeliverable` option
/// - `LYRIC_CONFIG_MQ_ACTIVEMQ_HEARTBEATSENDMS`,
///   `LYRIC_CONFIG_MQ_ACTIVEMQ_HEARTBEATRECEIVEMS`: the heart-beat options in
///   milliseconds (default 10000 each; 0 = none)
/// - `LYRIC_CONFIG_MQ_ACTIVEMQ_RECONNECT`: `true` (default) or `false`
/// - `LYRIC_CONFIG_MQ_ACTIVEMQ_RECONNECTINITIALDELAYMS`,
///   `LYRIC_CONFIG_MQ_ACTIVEMQ_RECONNECTMAXDELAYMS`,
///   `LYRIC_CONFIG_MQ_ACTIVEMQ_RECONNECTMAXATTEMPTS`: the `reconnect` options
///   of the same names (defaults 1000, 30000 and 0 = no limit)
///
/// and, for a `stomp+ssl://` URL:
///
/// - `LYRIC_CONFIG_MQ_ACTIVEMQ_TLSCAFILE`: a PEM file with a CA certificate
///   to trust in addition to the platform trust store
/// - `LYRIC_CONFIG_MQ_ACTIVEMQ_TLSCAEXCLUSIVE`: `true` to trust only that CA
/// - `LYRIC_CONFIG_MQ_ACTIVEMQ_TLSCERTFILE`, `..._TLSKEYFILE`: PEM files with
///   the client certificate and its PKCS8 private key (mutual TLS)
/// - `LYRIC_CONFIG_MQ_ACTIVEMQ_TLSSERVERNAME`: the name to check the server
///   certificate against, when it is not the URL's host
/// - `LYRIC_CONFIG_MQ_ACTIVEMQ_TLSMINVERSION`: `1.2` (default) or `1.3`
///
/// Verification cannot be switched off from the environment; that takes
/// `withInsecureSkipVerify()` in code as well as `LYRIC_TLS_ALLOW_INSECURE=1`.
pub func connectFromEnv(): Result[ActiveMqQueue, String] {
  val url = envOr("URL", "")
  if url.length == 0 {
    return Err("activemq: no broker URL configured (set LYRIC_CONFIG_MQ_ACTIVEMQ_URL)")
  }
  val queueName = envOr("QUEUENAME", "")
  if queueName.length == 0 {
    return Err("activemq: no queue name configured (set LYRIC_CONFIG_MQ_ACTIVEMQ_QUEUENAME)")
  }
  val modeText = Str.toLower(envOr("ACKMODE", "manual"))
  val mode = if modeText == "manual" {
    Mq.AckMode.Manual
  } else if modeText == "auto" {
    Mq.AckMode.Auto
  } else {
    return Err("activemq: LYRIC_CONFIG_MQ_ACTIVEMQ_ACKMODE must be Manual or Auto")
  }
  val policyText = Str.toLower(envOr("ONUNDELIVERABLE", "leave"))
  val policy = if policyText == "leave" {
    UndeliverablePolicy.LeaveUnacknowledged
  } else if policyText == "reject" {
    UndeliverablePolicy.RejectToBroker
  } else {
    return Err("activemq: LYRIC_CONFIG_MQ_ACTIVEMQ_ONUNDELIVERABLE must be Leave or Reject")
  }
  val base = match optionsFromUrl(url, queueName) { case Ok(o) -> o; case Err(e) -> return Err(e) }
  val sendMs = match envInt("HEARTBEATSENDMS", base.heartBeatSendMs) { case Ok(n) -> n; case Err(e) -> return Err(e) }
  val receiveMs = match envInt("HEARTBEATRECEIVEMS", base.heartBeatReceiveMs) {
    case Ok(n) -> n
    case Err(e) -> return Err(e)
  }
  val reconnectText = Str.toLower(envOr("RECONNECT", "true"))
  if reconnectText != "true" and reconnectText != "false" {
    return Err("activemq: LYRIC_CONFIG_MQ_ACTIVEMQ_RECONNECT must be true or false")
  }
  val r = base.reconnect
  val initialDelay = match envInt("RECONNECTINITIALDELAYMS", r.initialDelayMs) {
    case Ok(n) -> n
    case Err(e) -> return Err(e)
  }
  val maxDelay = match envInt("RECONNECTMAXDELAYMS", if initialDelay > r.maxDelayMs { initialDelay } else { r.maxDelayMs }) {
    case Ok(n) -> n
    case Err(e) -> return Err(e)
  }
  val maxAttempts = match envInt("RECONNECTMAXATTEMPTS", r.maxAttempts) {
    case Ok(n) -> n
    case Err(e) -> return Err(e)
  }
  val tuned = base.copy(
    ackMode = mode,
    onUndeliverable = policy,
    heartBeatSendMs = sendMs,
    heartBeatReceiveMs = receiveMs,
    reconnect = r.copy(
      enabled = reconnectText == "true",
      initialDelayMs = initialDelay,
      maxDelayMs = maxDelay,
      maxAttempts = maxAttempts
    )
  )
  match tlsFromEnv(tuned) {
    case Ok(options) -> connectWith(options)
    case Err(e) -> Err(e)
  }
}

// ─── Publishing ──────────────────────────────────────────────────────────────
func headersJson(headers: in slice[Mq.MessageHeader]): String {
  val parts: List[String] = newList()
  for h in headers {
    parts.add("{\"key\":" + encodeString(h.key) + ",\"value\":" + encodeString(h.value) + "}")
  }
  "[" + Str.joinList(",", parts) + "]"
}

func hasHeader(headers: in slice[Stomp.Header], name: in String): Bool {
  for h in headers {
    if h.name == name {
      return true
    }
  }
  false
}

// The SEND headers of `message`. A header in `extra` replaces the default of
// the same name.
func sendHeaders(q: in ActiveMqQueue, message: in Mq.Message, extra: in slice[Stomp.Header]): List[Stomp.Header] {
  val defaults: List[Stomp.Header] = newList()
  defaults.add(header("destination", q.options.destination))
  defaults.add(header(HEADER_ID, message.id))
  if message.headers.length > 0 {
    defaults.add(header(HEADER_HEADERS, headersJson(message.headers)))
  }
  if message.deliveryCount > 0 {
    defaults.add(header(HEADER_DELIVERY_COUNT, message.deliveryCount.toString()))
  }
  if q.options.persistent {
    defaults.add(header("persistent", "true"))
  }
  defaults.add(header("amq-msg-type", "text"))
  val headers: List[Stomp.Header] = newList()
  for d in defaults.toArray() {
    if not hasHeader(extra, d.name) {
      headers.add(d)
    }
  }
  for e in extra {
    headers.add(e)
  }
  headers
}

// A consumer refuses a negative `lyric_delivery_count`, so none is sent.
func checkDeliveryCount(message: in Mq.Message): Result[Unit, String] {
  if message.deliveryCount < 0 {
    return Err(
      "activemq: message '" + message.id + "' has the negative deliveryCount " + message.deliveryCount.toString()
    )
  }
  Ok(())
}

/// Publish `message` with extra STOMP headers on the `SEND` frame (ActiveMQ
/// stores unknown headers as JMS properties). A header in `extra` replaces the
/// one this client would send under the same name. `Mq.ActiveMq.Extras` builds
/// the common ones.
pub func publishWithHeaders(
  q: in ActiveMqQueue, message: in Mq.Message, extra: in slice[Stomp.Header]
): Result[Unit, String] {
  if message.id.length == 0 {
    return Err(EMPTY_ID)
  }
  match checkDeliveryCount(message) {
    case Err(e) -> return Err(e)
    case Ok(_) -> ()
  }
  awaitConnection(q)
  K.lockEnter(q.lock)
  defer {
    K.lockExit(q.lock)
  }
  match checkUsable(q) {
    case Err(e) -> return Err(e)
    case Ok(_) -> ()
  }
  unknownIfLost(
    q,
    request(q, "SEND", sendHeaders(q, message, extra), encodeUtf8(message.body)),
    "the broker may or may not have accepted the message, and it was not sent again"
  )
}

// Whether the connection is lost and the keeper is opening a new one.
func isReconnecting(q: in ActiveMqQueue): Bool {
  K.lockEnter(q.lock)
  defer {
    K.lockExit(q.lock)
  }
  not q.closed and q.gaveUp.length == 0 and q.lost.length > 0 and q.options.reconnect.enabled
}

// Give a connection that is being re-established `reconnect.publishWaitMs`
// to come back. The caller checks the state again under the lock.
func awaitConnection(q: in ActiveMqQueue): Unit {
  val limit = q.options.reconnect.publishWaitMs
  if limit == 0 {
    return
  }
  val deadline = K.monotonicMillis() + limit.toLong()
  while true {
    val left = millisUntil(deadline)
    if left == 0 or not isReconnecting(q) {
      return
    }
    K.sleepMs(if left > 20 { 20 } else { left })
  }
}

// The result of a request that was written, or partly written, to a
// connection that is lost now: the broker's answer will never arrive, and
// the request is not repeated on the next connection.
func unknownIfLost(q: in ActiveMqQueue, outcome: in Result[Unit, String], what: in String): Result[Unit, String] {
  match outcome {
    case Ok(_) -> Ok(())
    case Err(e) -> if q.lost.length > 0 { Err(e + "; " + OUTCOME_UNKNOWN + ": " + what) } else { Err(e) }
  }
}

// The same for the frames of a transaction that was never committed.
func notCommittedIfLost(q: in ActiveMqQueue, e: in String): String {
  if q.lost.length > 0 {
    e + "; the transaction was not committed, so nothing of it was applied"
  } else {
    e
  }
}

func abortQuietly(q: in ActiveMqQueue, tx: in String): Unit {
  if q.lost.length == 0 {
    val headers: List[Stomp.Header] = newList()
    headers.add(header("transaction", tx))
    val _ = send(q, "ABORT", headers, noBody())
  }
}

func beginTransaction(q: in ActiveMqQueue): Result[String, String] {
  val tx = nextToken(q, "lyric-tx")
  val headers: List[Stomp.Header] = newList()
  headers.add(header("transaction", tx))
  match send(q, "BEGIN", headers, noBody()) {
    case Err(e) -> Err(e)
    case Ok(_) -> Ok(tx)
  }
}

func commitTransaction(q: in ActiveMqQueue, tx: in String): Result[Unit, String] {
  val headers: List[Stomp.Header] = newList()
  headers.add(header("transaction", tx))
  match request(q, "COMMIT", headers, noBody()) {
    case Ok(_) -> Ok(())
    case Err(e) -> {
      abortQuietly(q, tx)
      unknownIfLost(
        q,
        Err(e),
        "the broker may or may not have committed the transaction (all of it or none), and it was not sent again"
      )
    }
  }
}

func publishBatchLocked(q: in ActiveMqQueue, messages: in slice[Mq.Message]): Result[Unit, String] {
  match checkUsable(q) {
    case Err(e) -> return Err(e)
    case Ok(_) -> ()
  }
  val tx = match beginTransaction(q) { case Ok(t) -> t; case Err(e) -> return Err(notCommittedIfLost(q, e)) }
  val none: slice[Stomp.Header] = []
  for m in messages {
    val headers = sendHeaders(q, m, none)
    headers.add(header("transaction", tx))
    match send(q, "SEND", headers, encodeUtf8(m.body)) {
      case Ok(_) -> ()
      case Err(e) -> {
        abortQuietly(q, tx)
        return Err(notCommittedIfLost(q, e))
      }
    }
  }
  commitTransaction(q, tx)
}

// `Mq.MessageQueue.publishBatch` requires `allHaveIds(messages)`. The JVM
// backend resolves that inherited clause in the implementing package and
// fails with J008 when the name is not declared there (see the README's known
// gaps), so this package declares the same predicate. Delete it once the
// compiler resolves the clause in `Mq`.
@pure
func allHaveIds(messages: in slice[Mq.Message]): Bool {
  for m in messages {
    if m.id.length == 0 {
      return false
    }
  }
  true
}

impl Mq.MessageQueue for ActiveMqQueue {
  func publish(message: in Mq.Message): Result[Unit, String]
    requires: message.id.length > 0
  {
    val none: slice[Stomp.Header] = []
    return publishWithHeaders(self, message, none)
  }

  func publishBatch(messages: in slice[Mq.Message]): Result[Unit, String]
    requires: messages.length > 0
  {
    if not allHaveIds(messages) {
      return Err(EMPTY_ID)
    }
    for m in messages {
      match checkDeliveryCount(m) {
        case Err(e) -> return Err(e)
        case Ok(_) -> ()
      }
    }
    awaitConnection(self)
    K.lockEnter(self.lock)
    defer {
      K.lockExit(self.lock)
    }
    return publishBatchLocked(self, messages)
  }
}

// ─── Consuming ───────────────────────────────────────────────────────────────
func millisUntil(deadline: in Long): Int {
  val remaining = deadline - K.monotonicMillis()
  if remaining <= 0i64 {
    0
  } else {
    remaining.toInt()
  }
}

// Open the subscription, waiting at most `waitMs` for the broker to confirm
// it. `Ok(false)`: the SUBSCRIBE is on its way but not confirmed yet; a later
// call goes on waiting for the same receipt, which has `requestTimeoutMs`
// from the moment the frame was sent.
func subscribeLocked(q: in ActiveMqQueue, waitMs: in Int): Result[Bool, String] {
  match checkUsable(q) {
    case Err(e) -> return Err(e)
    case Ok(_) -> ()
  }
  if q.subscribed {
    return Ok(true)
  }
  if q.subscribeFailure.length > 0 {
    val refusal = q.subscribeFailure
    q.subscribeFailure = ""
    return Err(refusal)
  }
  val waitUntil = K.monotonicMillis() + waitMs.toLong()
  if q.subscribeReceipt.length == 0 {
    val receiptId = nextToken(q, "lyric-r")
    val headers: List[Stomp.Header] = newList()
    headers.add(header("id", SUBSCRIPTION_ID))
    headers.add(header("destination", q.options.destination))
    headers.add(header("ack", "client-individual"))
    if q.options.selector.length > 0 {
      headers.add(header("selector", q.options.selector))
    }
    if q.options.prefetchSize > 0 {
      headers.add(header("activemq.prefetchSize", q.options.prefetchSize.toString()))
    }
    headers.add(header("receipt", receiptId))
    q.wantSubscription = true
    match send(q, "SUBSCRIBE", headers, noBody()) {
      case Err(e) -> return Err(e)
      case Ok(_) -> ()
    }
    q.subscribeReceipt = receiptId
    q.subscribeDeadline = K.monotonicMillis() + q.options.requestTimeoutMs.toLong()
  }
  val receiptId = q.subscribeReceipt
  while not q.subscribed {
    val limit = if waitUntil < q.subscribeDeadline { waitUntil } else { q.subscribeDeadline }
    val item = match readFrame(q, millisUntil(limit)) {
      case Err(e) -> return Err(e)
      case Ok(None) -> {
        if K.monotonicMillis() >= q.subscribeDeadline {
          return Err(markLost(q, "the broker did not answer SUBSCRIBE within " + q.options.requestTimeoutMs.toString() + " ms"))
        }
        return Ok(false)
      }
      case Ok(Some(i)) -> i
    }
    match route(q, item, receiptId) {
      case Err(e) -> return Err(e)
      case Ok(_) -> ()
    }
  }
  Ok(true)
}

/// Open the subscription now instead of on the first `consume`. A topic only
/// delivers what is published after its subscription exists, so a topic
/// consumer calls this before the publisher starts.
pub func subscribe(q: in ActiveMqQueue): Result[Unit, String] {
  K.lockEnter(q.lock)
  defer {
    K.lockExit(q.lock)
  }
  match subscribeLocked(q, q.options.requestTimeoutMs) {
    case Err(e) -> Err(e)
    case Ok(true) -> Ok(())
    case Ok(false) -> Err(
      markLost(
        q,
        "the broker did not answer SUBSCRIBE within " + q.options.requestTimeoutMs.toString() + " ms"
      )
    )
  }
}

func takeBacklog(q: in ActiveMqQueue): Stomp.Decoded {
  val item = q.backlog[0]
  q.backlog.removeAt(0)
  item
}

// The next MESSAGE, from the backlog or the wire, within `waitMs`; the wait
// for the subscription is part of `waitMs`.
func nextMessageLocked(q: in ActiveMqQueue, waitMs: in Int): Result[Option[Stomp.Decoded], String] {
  val deadline = K.monotonicMillis() + waitMs.toLong()
  match subscribeLocked(q, waitMs) {
    case Err(e) -> return Err(e)
    case Ok(false) -> return Ok(None)
    case Ok(true) -> ()
  }
  while q.backlog.count == 0 {
    val item = match readFrame(q, millisUntil(deadline)) {
      case Err(e) -> return Err(e)
      case Ok(None) -> return Ok(None)
      case Ok(Some(i)) -> i
    }
    match route(q, item, "") {
      case Err(e) -> return Err(e)
      case Ok(_) -> ()
    }
  }
  Ok(Some(value = takeBacklog(q)))
}

// Headers the broker or this client put on a MESSAGE frame; everything else
// on a message from another producer is application data.
func isTransportHeader(name: in String): Bool {
  name == "destination" or name == "message-id" or name == "ack" or name == "subscription" or name == "content-length" or
    name == "expires" or
    name == "priority" or
    name == "timestamp" or
    name == "redelivered" or
    name == "persistent" or
    name == "amq-msg-type" or
    name == "receipt" or
    name == "transaction" or
    name == HEADER_ID or
    name == HEADER_HEADERS or
    name == HEADER_DELIVERY_COUNT
}

func decodeHeaders(frame: in Stomp.Frame): Result[slice[Mq.MessageHeader], String] {
  if not isSome(Stomp.headerValue(frame, HEADER_ID)) {
    val foreign: List[Mq.MessageHeader] = newList()
    for h in frame.headers {
      if not isTransportHeader(h.name) {
        foreign.add(Mq.MessageHeader(key = h.name, value = h.value))
      }
    }
    return Ok(foreign.toArray())
  }
  match Stomp.headerValue(frame, HEADER_HEADERS) {
    case None -> {
      val none: slice[Mq.MessageHeader] = []
      Ok(none)
    }
    case Some(json) -> match Mq.messageFromJson("{\"headers\":" + json + "}") {
      case Ok(m) -> Ok(m.headers)
      case Err(e) -> Err("its " + HEADER_HEADERS + " header is malformed (" + e + ")")
    }
  }
}

func decodeMessage(frame: in Stomp.Frame): Result[Mq.Message, String] {
  val body = match Stomp.bodyText(frame) { case Some(s) -> s; case None -> return Err("its body is not UTF-8 text") }
  val headers = match decodeHeaders(frame) { case Ok(hs) -> hs; case Err(e) -> return Err(e) }
  val stored = match Stomp.headerValue(frame, HEADER_DELIVERY_COUNT) {
    case None -> 0
    case Some(text) -> match parseOptInt(text) {
      case Some(n) -> if n >= 0 {
        n
      } else {
        return Err("its " + HEADER_DELIVERY_COUNT + " header is negative")
      }
      case None -> return Err("its " + HEADER_DELIVERY_COUNT + " header is not a number")
    }
  }
  val redelivered = if Stomp.headerOr(frame, "redelivered", "false") == "true" { 1 } else { 0 }
  val brokerId = Stomp.headerOr(frame, "message-id", "")
  Ok(
    Mq.Message(
      id = Stomp.headerOr(frame, HEADER_ID, brokerId),
      body = body,
      headers = headers,
      deliveryCount = stored + redelivered
    )
  )
}

func ackHeaders(ackId: in String): List[Stomp.Header] {
  val headers: List[Stomp.Header] = newList()
  headers.add(header("id", ackId))
  headers
}

// A delivery of `id` replaces what is remembered of deliveries of `id` on
// lost connections: it is the broker's redelivery of them.
func track(q: in ActiveMqQueue, id: in String, delivery: in Delivery): Unit {
  if q.inFlight.containsKey(id) {
    val waiting = q.inFlight[id]
    while waiting.count > 0 and waiting[0].stale {
      waiting.removeAt(0)
    }
    q.inFlight[id].add(delivery)
  } else {
    val waiting: List[Delivery] = newList()
    waiting.add(delivery)
    q.inFlight[id] = waiting
  }
}

// A MESSAGE that cannot be handed to a handler: apply `onUndeliverable` and
// report it. Unless the broker took the rejection, the delivery is tracked
// under the broker's `message-id`, so the application can settle it.
func refuseDelivery(
  q: in ActiveMqQueue, frame: in Stomp.Frame, why: in String, requeueable: in Bool
): Result[Mq.Message, String] {
  val ackId = Stomp.headerOr(frame, "ack", "")
  if ackId.length == 0 {
    return Err(markLost(q, "the broker sent a MESSAGE without an ack header"))
  }
  val brokerId = Stomp.headerOr(frame, "message-id", "")
  if brokerId.length == 0 {
    return Err(markLost(q, "the broker sent a MESSAGE without a message-id header"))
  }
  val delivery = Delivery(
    ackId = ackId,
    frame = frame,
    deliveryCount = 0,
    requeueable = requeueable,
    byHand = true,
    epoch = q.epoch,
    stale = false
  )
  val problem = "activemq: message " + brokerId + " cannot be delivered: " + why
  val settleHint = "ack or nack the id " + brokerId + " to settle it"
  match q.options.onUndeliverable {
    case RejectToBroker -> match request(q, "NACK", ackHeaders(ackId), noBody()) {
      case Ok(_) -> Err(problem + "; it was rejected to the broker's dead-letter handling")
      case Err(e) -> {
        track(q, brokerId, delivery)
        Err(problem + "; rejecting it failed (" + e + "), so it is left unacknowledged on the broker; " + settleHint)
      }
    }
    case LeaveUnacknowledged -> {
      track(q, brokerId, delivery)
      Err(
        problem + "; it was left unacknowledged on the broker, which redelivers it when this connection closes; " +
          settleHint
      )
    }
  }
}

// Turn what the broker sent into a tracked delivery for the handler.
func admitLocked(q: in ActiveMqQueue, item: in Stomp.Decoded): Result[Mq.Message, String] {
  val frame = match item {
    case Complete(f) -> f
    case TooLarge(skipped) -> return refuseDelivery(
      q,
      Stomp.Frame(command = skipped.command, headers = skipped.headers, body = noBody()),
      skipped.reason + " accepted by this connection",
      false
    )
  }
  val ackId = Stomp.headerOr(frame, "ack", "")
  if ackId.length == 0 {
    return Err(markLost(q, "the broker sent a MESSAGE without an ack header"))
  }
  val message = match decodeMessage(frame) {
    case Ok(m) -> m
    case Err(why) -> return refuseDelivery(q, frame, why, true)
  }
  if message.id.length == 0 {
    return Err(markLost(q, "the broker sent a MESSAGE without a message-id header"))
  }
  track(
    q,
    message.id,
    Delivery(
      ackId = ackId,
      frame = frame,
      deliveryCount = message.deliveryCount,
      requeueable = true,
      byHand = false,
      epoch = q.epoch,
      stale = false
    )
  )
  Ok(message)
}

// The next message for a handler, `Ok(None)` when none could be had before
// `deadline`: the lock was not free, the subscription is not confirmed yet,
// or nothing arrived. One call waits at most one slice for the wire, so the
// lock is given up between slices.
func nextDelivery(q: in ActiveMqQueue, deadline: in Long): Result[Option[Mq.Message], String] {
  if not K.lockTryEnter(q.lock, millisUntil(deadline)) {
    // The keeper holds the lock for a moment at a time, and that moment is
    // waited out. An operation may hold it for as long as a request takes,
    // which is not waited for.
    var entered = false
    while not entered and keeperHasTurn(q) {
      entered = K.lockTryEnter(q.lock, KEEPER_GRACE_MS)
    }
    if not entered {
      return Ok(None)
    }
  }
  defer {
    K.lockExit(q.lock)
  }
  val remaining = millisUntil(deadline)
  val item = match nextMessageLocked(q, if remaining > WAIT_SLICE_MS { WAIT_SLICE_MS } else { remaining }) {
    case Err(e) -> return Err(e)
    case Ok(None) -> return Ok(None)
    case Ok(Some(i)) -> i
  }
  match admitLocked(q, item) {
    case Ok(message) -> Ok(Some(value = message))
    case Err(e) -> Err(e)
  }
}

// The oldest unsettled delivery of `messageId`. It stays in the in-flight
// set until `forgetDelivery`.
func oldestDelivery(q: in ActiveMqQueue, messageId: in String): Result[Delivery, String] {
  if not q.inFlight.containsKey(messageId) {
    return Err("activemq: no unsettled delivery has message id '" + messageId + "'")
  }
  Ok(q.inFlight[messageId][0])
}

func forgetDelivery(q: in ActiveMqQueue, messageId: in String): Unit {
  val waiting = q.inFlight[messageId]
  waiting.removeAt(0)
  if waiting.count == 0 {
    q.inFlight.remove(messageId)
  }
}

// Headers of a delivered message that must not be copied onto its requeued
// copy: the broker sets them afresh, or the copy sets its own.
func isRequeueDropped(name: in String): Bool {
  name == "destination" or name == "message-id" or name == "ack" or name == "subscription" or name == "content-length" or
    name == "expires" or
    name == "timestamp" or
    name == "redelivered" or
    name == "receipt" or
    name == "transaction" or
    name == HEADER_DELIVERY_COUNT
}

func requeueLocked(q: in ActiveMqQueue, delivery: in Delivery): Result[Unit, String] {
  val tx = match beginTransaction(q) { case Ok(t) -> t; case Err(e) -> return Err(notCommittedIfLost(q, e)) }
  val copy: List[Stomp.Header] = newList()
  copy.add(header("destination", Stomp.headerOr(delivery.frame, "destination", q.options.destination)))
  for h in delivery.frame.headers {
    if not isRequeueDropped(h.name) {
      copy.add(h)
    }
  }
  // `expires` is an absolute time; 0 means the message never expires.
  val expires = Stomp.headerOr(delivery.frame, "expires", "0")
  if expires != "0" {
    copy.add(header("expires", expires))
  }
  copy.add(header(HEADER_DELIVERY_COUNT, (delivery.deliveryCount + 1).toString()))
  copy.add(header("transaction", tx))
  match send(q, "SEND", copy, delivery.frame.body) {
    case Ok(_) -> ()
    case Err(e) -> {
      abortQuietly(q, tx)
      return Err(notCommittedIfLost(q, e))
    }
  }
  val ack = ackHeaders(delivery.ackId)
  ack.add(header("transaction", tx))
  match send(q, "ACK", ack, noBody()) {
    case Ok(_) -> ()
    case Err(e) -> {
      abortQuietly(q, tx)
      return Err(notCommittedIfLost(q, e))
    }
  }
  commitTransaction(q, tx)
}

// Settle the oldest delivery of `messageId`: 0 = ack, 1 = reject, 2 = requeue.
// The delivery is forgotten only when the broker has confirmed the
// settlement. After a refusal it stays unsettled, here as on the broker, and
// can be settled again; `automatic` (the settlement `consume` does in `Auto`
// mode) then hands it over to `ack` / `nack`.
func settle(q: in ActiveMqQueue, messageId: in String, how: in Int, automatic: in Bool): Result[Unit, String] {
  if messageId.length == 0 {
    return Err(EMPTY_ID)
  }
  K.lockEnter(q.lock)
  defer {
    K.lockExit(q.lock)
  }
  if q.closed {
    return Err("activemq: the connection is closed")
  }
  if q.inFlight.containsKey(messageId) and q.inFlight[messageId][0].stale {
    forgetDelivery(q, messageId)
    return Err(
      STALE_DELIVERY_PREFIX + ": message '" + messageId +
        "' was delivered on a connection that has since been lost, so that delivery cannot be settled; the broker redelivers the message"
    )
  }
  match checkUsable(q) {
    case Err(e) -> return Err(e)
    case Ok(_) -> ()
  }
  val delivery = match oldestDelivery(q, messageId) { case Ok(d) -> d; case Err(e) -> return Err(e) }
  val unsettled = "the broker may or may not have applied this settlement; if it did not, it redelivers the message"
  val outcome = if how == 0 {
    unknownIfLost(q, request(q, "ACK", ackHeaders(delivery.ackId), noBody()), unsettled)
  } else if how == 1 {
    unknownIfLost(q, request(q, "NACK", ackHeaders(delivery.ackId), noBody()), unsettled)
  } else if not delivery.requeueable {
    Err(
      "activemq: message " + messageId +
        " was too large to be read, so there is nothing to requeue; ack it or reject it with nack(id, false)"
    )
  } else {
    requeueLocked(q, delivery)
  }
  match outcome {
    case Ok(_) -> {
      forgetDelivery(q, messageId)
      Ok(())
    }
    case Err(e) -> {
      // A delivery of a connection that this request lost is stale now and
      // stays as `markLost` left it.
      if automatic and not delivery.byHand and q.lost.length == 0 {
        q.inFlight[messageId][0] = delivery.copy(byHand = true)
      }
      Err(e)
    }
  }
}

// In `Auto` mode: whether `ack` / `nack` must act on `messageId`, because its
// oldest unsettled delivery is one `consume` will not settle.
func settledByHand(q: in ActiveMqQueue, messageId: in String): Bool {
  K.lockEnter(q.lock)
  defer {
    K.lockExit(q.lock)
  }
  match oldestDelivery(q, messageId) {
    case Ok(delivery) -> delivery.byHand
    case Err(_) -> false
  }
}

// `ack` / `nack`: in `Auto` mode they do nothing, except for a delivery
// `consume` will not settle.
func settleByCaller(q: in ActiveMqQueue, messageId: in String, how: in Int): Result[Unit, String] {
  if messageId.length > 0 and isAuto(q) and not settledByHand(q, messageId) {
    return Ok(())
  }
  settle(q, messageId, how, false)
}

func isAuto(q: in ActiveMqQueue): Bool {
  match q.options.ackMode {
    case Auto -> true
    case Manual -> false
  }
}

func closeLocked(q: in ActiveMqQueue): Unit {
  if q.closed {
    return
  }
  q.closed = true
  K.lockEnter(q.writeLock)
  q.writable = false
  K.lockExit(q.writeLock)
  if q.lost.length > 0 {
    return
  }
  if q.subscribed or q.subscribeReceipt.length > 0 {
    val headers: List[Stomp.Header] = newList()
    headers.add(header("id", SUBSCRIPTION_ID))
    val _ = send(q, "UNSUBSCRIBE", headers, noBody())
    q.subscribed = false
    q.subscribeReceipt = ""
  }
  if q.lost.length == 0 {
    val receiptId = nextToken(q, "lyric-r")
    val headers: List[Stomp.Header] = newList()
    headers.add(header("receipt", receiptId))
    match send(q, "DISCONNECT", headers, noBody()) {
      case Ok(_) -> {
        val limit = if q.options.requestTimeoutMs < 2000 { q.options.requestTimeoutMs } else { 2000 }
        val _ = awaitReceipt(q, receiptId, limit)
      }
      case Err(_) -> ()
    }
  }
  K.sockClose(q.sock)
}

impl Mq.QueueConsumer for ActiveMqQueue {
  func consume(timeoutMs: in Int, handler: (Mq.Message) -> Result[Unit, String]): Result[Unit, String]
    requires: timeoutMs >= 0
  {
    val deadline = K.monotonicMillis() + timeoutMs.toLong()
    var first = true
    // The error of a connection that is being re-established, for as long
    // as it is: `consume` waits for the new connection, within `timeoutMs`,
    // whether the loss was there before the call or happened during it.
    var notConnected = ""
    while true {
      if not first and K.monotonicMillis() >= deadline {
        return Err(if notConnected.length > 0 { notConnected } else { "timeout" })
      }
      first = false
      match nextDelivery(self, deadline) {
        case Err(e) -> {
          val left = millisUntil(deadline)
          if left == 0 or not isReconnecting(self) {
            return Err(e)
          }
          notConnected = e
          K.sleepMs(if left > 20 { 20 } else { left })
        }
        case Ok(None) -> {
          notConnected = ""
        }
        case Ok(Some(message)) -> {
          val outcome = handler(message)
          if not isAuto(self) {
            return outcome
          }
          match outcome {
            case Ok(_) -> return settle(self, message.id, 0, true)
            case Err(e) -> {
              return match settle(self, message.id, 2, true) {
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
    return settleByCaller(self, messageId, 0)
  }

  func nack(messageId: in String, requeue: in Bool): Result[Unit, String] {
    return settleByCaller(self, messageId, if requeue { 2 } else { 1 })
  }

  func close(): Unit {
    K.lockEnter(self.flagsLock)
    self.closing = true
    K.lockExit(self.flagsLock)
    K.lockEnter(self.lock)
    defer {
      K.lockExit(self.lock)
    }
    closeLocked(self)
  }
}

// ─── The keeper: heart-beats and reconnecting ────────────────────────────────
// One background thread per connection. Every tick it writes a heart-beat
// when one is due, and, when no operation holds the lock, reads what the
// broker has sent and checks that the broker is still there. When the
// connection is lost it opens a new one. It ends when the connection is
// closed, when reconnecting is switched off and the connection is lost, or
// when reconnecting is given up.
func isClosing(q: in ActiveMqQueue): Bool {
  K.lockEnter(q.flagsLock)
  val closing = q.closing
  K.lockExit(q.flagsLock)
  closing
}

func setKeeperTurn(q: in ActiveMqQueue, on: in Bool): Unit {
  K.lockEnter(q.flagsLock)
  q.keeperTurn = on
  K.lockExit(q.flagsLock)
}

func keeperHasTurn(q: in ActiveMqQueue): Bool {
  K.lockEnter(q.flagsLock)
  val turn = q.keeperTurn
  K.lockExit(q.flagsLock)
  turn
}

// The keeper's ways to the lock. Its round every tick is announced before
// it tries, so that a `consume` that finds the lock taken knows by whom and
// waits. Where the keeper itself waits for the lock (behind an operation,
// when the connection is lost), the announcement follows, so that nobody
// waits behind both.
func keeperEnter(q: in ActiveMqQueue): Unit {
  K.lockEnter(q.lock)
  setKeeperTurn(q, true)
}

func keeperTryEnter(q: in ActiveMqQueue): Bool {
  setKeeperTurn(q, true)
  if K.lockTryEnter(q.lock, 0) {
    return true
  }
  setKeeperTurn(q, false)
  false
}

func keeperExit(q: in ActiveMqQueue): Unit {
  K.lockExit(q.lock)
  setKeeperTurn(q, false)
}

func newline(): slice[Byte] {
  encodeUtf8("\n")
}

// Write a heart-beat when nothing was written for half of the agreed
// interval: ActiveMQ's inactivity monitor allows no grace on top of the
// interval, so writing exactly on time would be too late now and then.
// Returns the failure of the write, "" when there was none.
func beat(q: in ActiveMqQueue): String {
  if not K.lockTryEnter(q.writeLock, 0) {
    // Somebody is writing a frame, which is as good as a heart-beat.
    return ""
  }
  defer {
    K.lockExit(q.writeLock)
  }
  if not q.writable or q.sendIntervalMs == 0 {
    return ""
  }
  if K.monotonicMillis() - q.lastWriteAt < (q.sendIntervalMs / 2).toLong() {
    return ""
  }
  match K.sockWriteWithin(q.sock, newline(), q.options.requestTimeoutMs) {
    case Ok(_) -> {
      q.lastWriteAt = K.monotonicMillis()
      ""
    }
    case Err(e) -> {
      q.writable = false
      e
    }
  }
}

// Read what has arrived (lock held, connection usable): messages go to the
// backlog, a pending SUBSCRIBE receipt is recorded, heart-beats count as a
// sign of life. With a full backlog nothing is read, so that a consumer that
// has stopped consuming does not have its whole prefetch window buffered
// here; the broker is then not judged by its silence either.
func readAhead(q: in ActiveMqQueue): Unit {
  var more = true
  while more and q.lost.length == 0 {
    if q.backlog.count >= KEEPER_BACKLOG_LIMIT {
      q.lastReadAt = K.monotonicMillis()
      more = false
    } else {
      match readFrame(q, 0) {
        case Ok(Some(item)) -> {
          more = isOk(route(q, item, ""))
        }
        case _ -> {
          more = false
        }
      }
    }
  }
}

// How long the keeper sleeps between two rounds: a quarter of the shorter
// heart-beat interval, between 10 ms and `KEEPER_TICK_MS`.
func tickOf(sendMs: in Int, receiveMs: in Int): Int {
  var shortest = KEEPER_TICK_MS * 4
  if sendMs > 0 and sendMs < shortest {
    shortest = sendMs
  }
  if receiveMs > 0 and receiveMs < shortest {
    shortest = receiveMs
  }
  val tick = shortest / 4
  if tick < 10 {
    10
  } else {
    tick
  }
}

// Sleep `ms`, in pieces, unless the connection is being closed. False when
// it is.
func sleepUnlessClosing(q: in ActiveMqQueue, ms: in Int): Bool {
  val deadline = K.monotonicMillis() + ms.toLong()
  while true {
    if isClosing(q) {
      return false
    }
    val left = millisUntil(deadline)
    if left == 0 {
      return true
    }
    K.sleepMs(if left > 50 { 50 } else { left })
  }
  true
}

// `delayMs` shortened by a random part of up to `percent` percent of it.
func jittered(delayMs: in Int, percent: in Int): Int {
  val span = (delayMs.toLong() * percent.toLong() / 100i64).toInt()
  if span < 1 {
    delayMs
  } else {
    delayMs - secureNextInt(span + 1)
  }
}

func giveUp(q: in ActiveMqQueue, why: in String): Unit {
  keeperEnter(q)
  q.gaveUp = q.lost + "; " + why
  keeperExit(q)
}

// What the new connection must have before it replaces the lost one: the
// subscription, when the lost one had or wanted one. `Ok("")`: it has.
// `Ok(text)`: this attempt failed with `text`, try again. `Err`: the broker
// refused the subscription.
func resubscribe(fresh: in ActiveMqQueue, wanted: in Bool): Result[String, String] {
  if not wanted {
    return Ok("")
  }
  K.lockEnter(fresh.lock)
  val outcome = subscribeLocked(fresh, fresh.options.requestTimeoutMs)
  val lost = fresh.lost.length > 0
  K.lockExit(fresh.lock)
  match outcome {
    case Ok(true) -> Ok("")
    case Ok(false) -> {
      K.sockClose(fresh.sock)
      Ok("the broker did not confirm the subscription in time")
    }
    case Err(e) -> {
      K.sockClose(fresh.sock)
      if lost {
        Ok(e)
      } else {
        Err(e)
      }
    }
  }
}

// Make `fresh` the connection of `q`. False when `q` was closed meanwhile.
func install(q: in ActiveMqQueue, fresh: in ActiveMqQueue): Bool {
  keeperEnter(q)
  defer {
    keeperExit(q)
  }
  if q.closed or isClosing(q) {
    K.sockClose(fresh.sock)
    return false
  }
  val now = K.monotonicMillis()
  K.lockEnter(q.writeLock)
  q.sock = fresh.sock
  q.sendIntervalMs = fresh.sendIntervalMs
  q.lastWriteAt = now
  q.writable = true
  K.lockExit(q.writeLock)
  q.decoder = fresh.decoder
  q.backlog = fresh.backlog
  q.server = fresh.server
  q.subscribed = fresh.subscribed
  q.subscribeReceipt = ""
  q.subscribeFailure = ""
  q.receiveIntervalMs = fresh.receiveIntervalMs
  q.lastReadAt = now
  q.epoch = q.epoch + 1
  q.attempts = 0
  q.lastAttemptError = ""
  q.lost = ""
  // What is remembered of deliveries of connections long gone is dropped.
  for id in mapKeys(q.inFlight).toArray() {
    val waiting = q.inFlight[id]
    while waiting.count > 0 and waiting[0].stale and q.epoch - waiting[0].epoch > STALE_EPOCHS_KEPT {
      waiting.removeAt(0)
    }
    if waiting.count == 0 {
      q.inFlight.remove(id)
    }
  }
  true
}

// Open a new connection in place of the lost one, as often as the options
// allow. False when the keeper has nothing left to do: the connection was
// closed, or reconnecting was given up.
func reconnect(q: in ActiveMqQueue): Bool {
  val r = q.options.reconnect
  var attempt = 0
  var delay = r.initialDelayMs
  var lastError = ""
  while true {
    if r.maxAttempts > 0 and attempt >= r.maxAttempts {
      giveUp(
        q,
        "reconnecting was given up after " + attempt.toString() + " failed attempt(s), the last one with: " + lastError
      )
      return false
    }
    if not sleepUnlessClosing(q, jittered(delay, r.jitterPercent)) {
      return false
    }
    attempt += 1
    keeperEnter(q)
    val wanted = q.wantSubscription
    keeperExit(q)
    match openSession(q.options) {
      case Err(failure) -> {
        if failure.permanent {
          giveUp(q, "reconnecting was given up because the broker refused the new connection: " + failure.text)
          return false
        }
        lastError = failure.text
      }
      case Ok(fresh) -> match resubscribe(fresh, wanted) {
        case Err(refusal) -> {
          giveUp(q, "reconnecting was given up because the broker refused the subscription: " + refusal)
          return false
        }
        case Ok(problem) -> {
          if problem.length == 0 {
            return install(q, fresh)
          }
          lastError = problem
        }
      }
    }
    keeperEnter(q)
    q.attempts = attempt
    q.lastAttemptError = lastError
    keeperExit(q)
    delay = if delay > r.maxDelayMs / 2 { r.maxDelayMs } else { delay * 2 }
  }
  false
}

func keeperLoop(q: in ActiveMqQueue): Unit {
  var tick = KEEPER_TICK_MS
  // The first round comes one tick after the connect, not on top of the
  // application's first operation.
  var first = false
  while true {
    if not first {
      K.sleepMs(tick)
    }
    first = false
    if isClosing(q) {
      return
    }
    val writeFailure = beat(q)
    if writeFailure.length > 0 {
      keeperEnter(q)
    } else if not keeperTryEnter(q) {
      // An operation is using the connection; it reads and checks itself.
      continue
    }
    if q.closed {
      keeperExit(q)
      return
    }
    if q.lost.length == 0 and writeFailure.length > 0 {
      val _ = markLost(q, writeFailure)
    }
    if q.lost.length == 0 {
      readAhead(q)
    }
    if q.lost.length == 0 {
      val silence = peerSilence(q)
      if silence.length > 0 {
        val _ = markLost(q, silence)
      }
    }
    val down = q.lost.length > 0
    tick = tickOf(q.sendIntervalMs, q.receiveIntervalMs)
    keeperExit(q)
    if down {
      if not q.options.reconnect.enabled {
        return
      }
      if not reconnect(q) {
        return
      }
      first = true
    }
  }
}

func keeperMain(q: in ActiveMqQueue): Unit {
  try {
    keeperLoop(q)
  } catch Bug as b {
    // Nothing above is expected to raise. If something does, the
    // connection must not look alive without its keeper.
    keeperEnter(q)
    val _ = markLost(q, "the connection's background thread failed: " + b.message)
    q.gaveUp = q.lost
    keeperExit(q)
  }
  K.lockEnter(q.flagsLock)
  q.keeperRunning = false
  K.lockExit(q.flagsLock)
}

// ─── Introspection ───────────────────────────────────────────────────────────
/// What a connection is doing about its link to the broker.
pub record ConnectionStatus {
  /// There is a connection now.
  connected: Bool
  /// The connection is lost and the keeper is opening a new one.
  reconnecting: Bool
  /// Why the connection is lost; empty while connected. After reconnecting
  /// was given up it also says so, and why.
  lostBecause: String
  /// Failed attempts since the connection was lost, and what the last one
  /// failed with.
  failedAttempts: Int
  lastAttemptError: String
  /// How many times a lost connection was replaced by a new one.
  reconnects: Int
  /// The agreed heart-beat intervals in milliseconds: how often this client
  /// writes at least, and how often the broker does; 0 = no heart-beat in
  /// that direction.
  heartBeatSendMs: Int
  heartBeatReceiveMs: Int
}

/// The state of the connection, without probing it.
pub func status(q: in ActiveMqQueue): ConnectionStatus {
  K.lockEnter(q.lock)
  defer {
    K.lockExit(q.lock)
  }
  val down = q.lost.length > 0
  ConnectionStatus(
    connected = not q.closed and not down,
    reconnecting = not q.closed and down and q.gaveUp.length == 0 and q.options.reconnect.enabled,
    lostBecause = if q.gaveUp.length > 0 { q.gaveUp } else { q.lost },
    failedAttempts = q.attempts,
    lastAttemptError = q.lastAttemptError,
    reconnects = q.epoch,
    heartBeatSendMs = if down { 0 } else { q.sendIntervalMs },
    heartBeatReceiveMs = if down { 0 } else { q.receiveIntervalMs }
  )
}

/// True while the connection's background thread (heart-beats, reconnecting)
/// runs. It ends shortly after `close`.
pub func hasBackgroundThread(q: in ActiveMqQueue): Bool {
  K.lockEnter(q.flagsLock)
  defer {
    K.lockExit(q.flagsLock)
  }
  q.keeperRunning
}

/// The broker's product and version from its `CONNECTED` frame, e.g.
/// `ActiveMQ/5.17.6`.
pub func serverInfo(q: in ActiveMqQueue): String {
  q.server
}

/// The STOMP destination this connection publishes to and consumes from.
pub func destination(q: in ActiveMqQueue): String {
  q.options.destination
}

/// True while there is a connection: false after `close`, and while a lost
/// connection is being re-established or was given up.
pub func isOpen(q: in ActiveMqQueue): Bool {
  K.lockEnter(q.lock)
  defer {
    K.lockExit(q.lock)
  }
  not q.closed and q.lost.length == 0
}

/// The number of deliveries handed to a handler and not yet settled.
/// Deliveries of a connection that was lost do not count: they cannot be
/// settled any more.
pub func unsettledCount(q: in ActiveMqQueue): Int {
  K.lockEnter(q.lock)
  defer {
    K.lockExit(q.lock)
  }
  var total = 0
  for waiting in mapValues(q.inFlight).toArray() {
    for delivery in waiting.toArray() {
      if not delivery.stale {
        total = total + 1
      }
    }
  }
  total
}
