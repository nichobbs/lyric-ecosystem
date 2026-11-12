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
//! Heart-beats are not negotiated (`heart-beat:0,0`): the client is
//! synchronous and has no thread to send them from. A connection lost while
//! idle is reported by the next operation.
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
//! There is no automatic reconnect. After a transport failure every operation
//! returns `Err`; open a new connection.
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
import Std.File as File
import Std.Tls as Tls
import Mq.ActiveMq.Kernel as K
import Mq.ActiveMq.Stomp as Stomp
import Mq.ActiveMq.Url as Url
import Mq
import Std.Encoding
pub use Mq

/// The header that carries `Message.id`.
pub val HEADER_ID: String = "lyric_id"

/// The header that carries `Message.headers` as a JSON array.
pub val HEADER_HEADERS: String = "lyric_headers"

/// The header that carries the number of earlier deliveries.
pub val HEADER_DELIVERY_COUNT: String = "lyric_delivery_count"

/// The environment variable that must be `1` for `withInsecureSkipVerify()`
/// to take effect (the standard library's, docs/61 §4 in lyric-lang).
pub val INSECURE_ALLOW_ENV: String = "LYRIC_TLS_ALLOW_INSECURE"

/// TLS settings of a connection. PEM text; an empty string means "not
/// configured". Build one with the `with...` functions on `ConnectOptions`.
pub record TlsOptions {
  /// Whether the connection is TLS at all.
  enabled: Bool
  /// The name sent as SNI and checked against the server certificate; empty
  /// uses the `host` field.
  serverName: String
  /// A trust anchor accepted in addition to the platform trust store.
  caCertificatePem: String
  /// A trust anchor that replaces the platform trust store. Wins over
  /// `caCertificatePem` when both are set.
  exclusiveCaCertificatePem: String
  /// The client certificate and its private key, for a connector that
  /// requires client authentication (`needClientAuth`). The key is an
  /// unencrypted PKCS8 `PRIVATE KEY` block, as for `Std.Tls.Identity`.
  clientCertificatePem: String
  clientKeyPem: String
  /// `Tls12` (the default) accepts TLS 1.2 and later; `Tls13` only TLS 1.3.
  minVersion: Tls.TlsVersion
  /// A request to skip certificate and name verification. Takes effect only
  /// when `LYRIC_TLS_ALLOW_INSECURE=1` is also set in the environment.
  insecureSkipVerify: Bool
}

/// No TLS: the connection is plain TCP.
pub func plainTcp(): TlsOptions {
  TlsOptions(
    enabled = false,
    serverName = "",
    caCertificatePem = "",
    exclusiveCaCertificatePem = "",
    clientCertificatePem = "",
    clientKeyPem = "",
    minVersion = Tls.TlsVersion.Tls12,
    insecureSkipVerify = false
  )
}

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
}

/// Options for `destination` on `host:port` with the defaults: plain TCP, no
/// credentials, manual acknowledgement, persistent messages, a 10 s connect
/// and request timeout, a 16 MiB frame limit and undeliverable messages left
/// on the broker. Adjust with `.copy(...)` and the `with...` functions below.
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
    tls = plainTcp()
  )
}

/// Connect over TLS, verifying the server certificate against the platform
/// trust store and its name against `host`.
pub func ConnectOptions.withTls(o: in ConnectOptions): ConnectOptions {
  o.copy(tls = o.tls.copy(enabled = true))
}

/// Check the server certificate against `name` instead of `host`, and send
/// `name` as SNI. For a broker reached by address or through a tunnel.
pub func ConnectOptions.withTlsServerName(o: in ConnectOptions, name: in String): ConnectOptions {
  o.copy(tls = o.tls.copy(serverName = name))
}

/// Trust the CA certificate in `pem` in addition to the platform trust
/// store. `pem` holds one certificate.
pub func ConnectOptions.withCaCertificate(o: in ConnectOptions, pem: in String): ConnectOptions {
  o.copy(tls = o.tls.copy(caCertificatePem = pem))
}

/// Trust only the CA certificate in `pem`; the platform trust store is not
/// consulted. Wins over `withCaCertificate`.
pub func ConnectOptions.withExclusiveCaCertificate(o: in ConnectOptions, pem: in String): ConnectOptions {
  o.copy(tls = o.tls.copy(exclusiveCaCertificatePem = pem))
}

/// Present a client certificate (mutual TLS). `keyPem` is the certificate's
/// unencrypted PKCS8 private key.
pub func ConnectOptions.withClientIdentity(
  o: in ConnectOptions, certificatePem: in String, keyPem: in String
): ConnectOptions {
  o.copy(tls = o.tls.copy(clientCertificatePem = certificatePem, clientKeyPem = keyPem))
}

/// Refuse a TLS version older than `version`.
pub func ConnectOptions.withMinTlsVersion(o: in ConnectOptions, version: in Tls.TlsVersion): ConnectOptions {
  o.copy(tls = o.tls.copy(minVersion = version))
}

/// Ask for certificate and name verification to be skipped. This alone does
/// nothing: `LYRIC_TLS_ALLOW_INSECURE=1` must also be set when the
/// connection is opened, otherwise verification stays on and a warning goes
/// to standard error. Never for production.
pub func ConnectOptions.withInsecureSkipVerify(o: in ConnectOptions): ConnectOptions {
  o.copy(tls = o.tls.copy(insecureSkipVerify = true))
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
}

/// An open connection to one ActiveMQ destination. Obtain one from
/// `connectWith`, `connectUrl` or `connectFromEnv`.
pub ref record ActiveMqQueue {
  options: ConnectOptions
  sock: K.Sock
  decoder: Stomp.Decoder
  lock: K.Lock
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
}

val EMPTY_ID: String = "activemq: empty message id"

val SUBSCRIPTION_ID: String = "lyric-sub-0"

val WAIT_SLICE_MS: Int = 100

val READ_CHUNK_BYTES: Int = 16384

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
func markLost(q: in ActiveMqQueue, reason: in String): String {
  if q.lost.length == 0 {
    q.lost = reason
    K.sockClose(q.sock)
  }
  "activemq: connection lost: " + q.lost
}

func checkUsable(q: in ActiveMqQueue): Result[Unit, String] {
  if q.closed {
    return Err("activemq: the connection is closed")
  }
  if q.lost.length > 0 {
    return Err("activemq: connection lost: " + q.lost)
  }
  Ok(())
}

func writeFrame(q: in ActiveMqQueue, frame: in Stomp.Frame): Result[Unit, String] {
  val bytes = match Stomp.encodeFrame(frame) { case Ok(b) -> b; case Err(e) -> return Err("activemq: " + e) }
  match K.sockWriteWithin(q.sock, bytes, q.options.requestTimeoutMs) {
    case Ok(_) -> Ok(())
    case Err(e) -> Err(markLost(q, e))
  }
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
      case Ok(TimedOut) -> return Ok(None)
      case Ok(Data(bytes)) -> Stomp.feed(q.decoder, bytes)
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
  validateTls(options.tls)
}

func hasTlsSettings(tls: in TlsOptions): Bool {
  tls.serverName.length > 0 or tls.caCertificatePem.length > 0 or tls.exclusiveCaCertificatePem.length > 0 or
    tls.clientCertificatePem.length > 0 or
    tls.clientKeyPem.length > 0 or
    tls.insecureSkipVerify or
    (match tls.minVersion { case Tls13 -> true; case Tls12 -> false })
}

func checkCertificate(label: in String, pem: in String): Result[Unit, String] {
  if pem.length == 0 {
    return Ok(())
  }
  match Tls.Certificate.fromPem(pem) {
    case Ok(_) -> Ok(())
    case Err(e) -> Err("activemq: TLS " + label + ": " + e.message())
  }
}

// The PEM is parsed here with the standard library's loaders, so a bad
// certificate or key is reported before any connection is made and with the
// same rules on both targets.
func validateTls(tls: in TlsOptions): Result[Unit, String] {
  if not tls.enabled {
    if hasTlsSettings(tls) {
      return Err("activemq: TLS options are set but the connection is not TLS; use a stomp+ssl:// URL or withTls()")
    }
    return Ok(())
  }
  match checkCertificate("CA certificate", tls.caCertificatePem) {
    case Err(e) -> return Err(e)
    case Ok(_) -> ()
  }
  match checkCertificate("exclusive CA certificate", tls.exclusiveCaCertificatePem) {
    case Err(e) -> return Err(e)
    case Ok(_) -> ()
  }
  if tls.clientCertificatePem.length == 0 and tls.clientKeyPem.length == 0 {
    return Ok(())
  }
  if tls.clientCertificatePem.length == 0 {
    return Err("activemq: TLS client identity: a private key is set without its certificate")
  }
  if tls.clientKeyPem.length == 0 {
    return Err("activemq: TLS client identity: a certificate is set without its private key")
  }
  match Tls.Identity.fromPem(tls.clientCertificatePem, tls.clientKeyPem) {
    case Ok(_) -> Ok(())
    case Err(e) -> Err("activemq: TLS client identity: " + e.message())
  }
}

/// Whether the environment holds the second key of the insecure policy
/// (`LYRIC_TLS_ALLOW_INSECURE=1`).
pub func insecureAllowedByEnvironment(): Bool {
  Env.getVarOrDefault(INSECURE_ALLOW_ENV, "") == "1"
}

/// Whether a connection opened with `tls` now would skip verification: only
/// when the options ask for it and the environment allows it.
pub func skipsVerification(tls: in TlsOptions): Bool {
  tls.enabled and tls.insecureSkipVerify and insecureAllowedByEnvironment()
}

func kernelTls(options: in ConnectOptions): K.ClientTls {
  val tls = options.tls
  val exclusive = tls.exclusiveCaCertificatePem.length > 0
  K.ClientTls(
    serverName = if tls.serverName.length > 0 { tls.serverName } else { options.host },
    caPem = if exclusive { tls.exclusiveCaCertificatePem } else { tls.caCertificatePem },
    exclusiveCa = exclusive,
    certPem = tls.clientCertificatePem,
    keyPem = tls.clientKeyPem,
    tls13Only = match tls.minVersion { case Tls13 -> true; case Tls12 -> false },
    skipVerify = skipsVerification(tls)
  )
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
  if options.tls.insecureSkipVerify and not insecureAllowedByEnvironment() {
    Console.error(
      "[Mq.ActiveMq] warning: withInsecureSkipVerify() requested but " + INSECURE_ALLOW_ENV +
        " is not set; TLS certificate verification remains ENABLED."
    )
  }
  match K.sockStartTls(plain, kernelTls(options), options.connectTimeoutMs) {
    case Ok(s) -> Ok(s)
    case Err(e) -> Err("activemq: TLS handshake with " + endpoint + " failed: " + e)
  }
}

/// Connect to the broker and authenticate. The subscription is opened by the
/// first `consume` (or by `subscribe`).
pub func connectWith(options: in ConnectOptions): Result[ActiveMqQueue, String] {
  match validate(options) {
    case Err(e) -> return Err(e)
    case Ok(_) -> ()
  }
  val endpoint = options.host + ":" + options.port.toString()
  val sock = match openTransport(options, endpoint) { case Ok(s) -> s; case Err(e) -> return Err(e) }
  val q = ActiveMqQueue(
    options = options,
    sock = sock,
    decoder = Stomp.newDecoder(options.maxFrameBytes),
    lock = K.newLock(),
    server = "",
    lost = "",
    closed = false,
    subscribed = false,
    counter = 0,
    backlog = newList(),
    inFlight = newMap(),
    subscribeReceipt = "",
    subscribeDeadline = 0i64,
    subscribeFailure = ""
  )
  val headers: List[Stomp.Header] = newList()
  headers.add(header("accept-version", "1.2"))
  headers.add(header("host", if options.virtualHost.length > 0 { options.virtualHost } else { options.host }))
  if options.login.length > 0 {
    headers.add(header("login", options.login))
    headers.add(header("passcode", options.passcode))
  }
  headers.add(header("heart-beat", "0,0"))
  match send(q, "CONNECT", headers, noBody()) {
    case Err(e) -> return Err(e)
    case Ok(_) -> ()
  }
  val answer = match readFrame(q, options.requestTimeoutMs) {
    // ActiveMQ refuses a login with an ERROR frame and closes the connection
    // at once; now and then the close overtakes the frame, or cuts a TLS
    // record short, and the refusal arrives as a bare close or a read error.
    case Err(_) -> return Err(
      "activemq: connection lost: " + endpoint +
        " ended the connection during login without a readable answer; a broker does that when it refuses the login or passcode (" +
        q.lost +
        ")"
    )
    case Ok(None) -> {
      K.sockClose(sock)
      return Err("activemq: " + endpoint + " did not answer CONNECT within " + options.requestTimeoutMs.toString() + " ms")
    }
    case Ok(Some(item)) -> match item {
      case Complete(f) -> f
      case TooLarge(skipped) -> {
        K.sockClose(sock)
        return Err(
          "activemq: " + endpoint + " answered CONNECT with a " + skipped.command + " frame this client cannot read: " +
            skipped.reason
        )
      }
    }
  }
  if answer.command == "ERROR" {
    K.sockClose(sock)
    return Err(errorText(answer))
  }
  if answer.command != "CONNECTED" {
    K.sockClose(sock)
    return Err("activemq: " + endpoint + " answered CONNECT with " + answer.command)
  }
  val version = Stomp.headerOr(answer, "version", "1.0")
  if version != "1.2" {
    K.sockClose(sock)
    return Err("activemq: the broker speaks STOMP " + version + "; this client needs 1.2")
  }
  q.server = Stomp.headerOr(answer, "server", "")
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

// The text of the PEM file named by the variable `field`; `Ok("")` when the
// variable is unset.
func pemFromEnv(field: in String): Result[String, String] {
  val path = envOr(field, "")
  if path.length == 0 {
    return Ok("")
  }
  match File.readText(path) {
    case Ok(text) -> Ok(text)
    case Err(_) -> Err("activemq: cannot read the file named by " + envName(field) + " (" + path + ")")
  }
}

// The TLS settings of `connectFromEnv`, applied to `base`.
func tlsFromEnv(base: in ConnectOptions): Result[ConnectOptions, String] {
  var options = base
  val ca = match pemFromEnv("TLSCAFILE") { case Ok(t) -> t; case Err(e) -> return Err(e) }
  val exclusiveText = Str.toLower(envOr("TLSCAEXCLUSIVE", "false"))
  if exclusiveText != "true" and exclusiveText != "false" {
    return Err("activemq: " + envName("TLSCAEXCLUSIVE") + " must be true or false")
  }
  if ca.length > 0 {
    options = if exclusiveText == "true" { options.withExclusiveCaCertificate(ca) } else { options.withCaCertificate(ca) }
  } else if exclusiveText == "true" {
    return Err("activemq: " + envName("TLSCAEXCLUSIVE") + " is set without " + envName("TLSCAFILE"))
  }
  val certificate = match pemFromEnv("TLSCERTFILE") { case Ok(t) -> t; case Err(e) -> return Err(e) }
  val key = match pemFromEnv("TLSKEYFILE") { case Ok(t) -> t; case Err(e) -> return Err(e) }
  if certificate.length > 0 or key.length > 0 {
    options = options.withClientIdentity(certificate, key)
  }
  val serverName = envOr("TLSSERVERNAME", "")
  if serverName.length > 0 {
    options = options.withTlsServerName(serverName)
  }
  val versionText = envOr("TLSMINVERSION", "1.2")
  if versionText == "1.3" {
    options = options.withMinTlsVersion(Tls.TlsVersion.Tls13)
  } else if versionText != "1.2" {
    return Err("activemq: " + envName("TLSMINVERSION") + " must be 1.2 or 1.3")
  }
  Ok(options)
}

/// Connect using the environment (a variable that is empty counts as
/// unset):
///
/// - `LYRIC_CONFIG_MQ_ACTIVEMQ_URL`: the broker URL (required)
/// - `LYRIC_CONFIG_MQ_ACTIVEMQ_QUEUENAME`: the queue or destination (required)
/// - `LYRIC_CONFIG_MQ_ACTIVEMQ_ACKMODE`: `Manual` (default) or `Auto`
/// - `LYRIC_CONFIG_MQ_ACTIVEMQ_ONUNDELIVERABLE`: `Leave` (default) or
///   `Reject`, the `onUndeliverable` option
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
  match tlsFromEnv(base.copy(ackMode = mode, onUndeliverable = policy)) {
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
  K.lockEnter(q.lock)
  defer {
    K.lockExit(q.lock)
  }
  match checkUsable(q) {
    case Err(e) -> return Err(e)
    case Ok(_) -> ()
  }
  request(q, "SEND", sendHeaders(q, message, extra), encodeUtf8(message.body))
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
      Err(e)
    }
  }
}

func publishBatchLocked(q: in ActiveMqQueue, messages: in slice[Mq.Message]): Result[Unit, String] {
  match checkUsable(q) {
    case Err(e) -> return Err(e)
    case Ok(_) -> ()
  }
  val tx = match beginTransaction(q) { case Ok(t) -> t; case Err(e) -> return Err(e) }
  val none: slice[Stomp.Header] = []
  for m in messages {
    val headers = sendHeaders(q, m, none)
    headers.add(header("transaction", tx))
    match send(q, "SEND", headers, encodeUtf8(m.body)) {
      case Ok(_) -> ()
      case Err(e) -> {
        abortQuietly(q, tx)
        return Err(e)
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

func track(q: in ActiveMqQueue, id: in String, delivery: in Delivery): Unit {
  if q.inFlight.containsKey(id) {
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
  val delivery = Delivery(ackId = ackId, frame = frame, deliveryCount = 0, requeueable = requeueable, byHand = true)
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
    Delivery(ackId = ackId, frame = frame, deliveryCount = message.deliveryCount, requeueable = true, byHand = false)
  )
  Ok(message)
}

// The next message for a handler, `Ok(None)` when none could be had before
// `deadline`: the lock was not free, the subscription is not confirmed yet,
// or nothing arrived. One call waits at most one slice for the wire, so the
// lock is given up between slices.
func nextDelivery(q: in ActiveMqQueue, deadline: in Long): Result[Option[Mq.Message], String] {
  if not K.lockTryEnter(q.lock, millisUntil(deadline)) {
    return Ok(None)
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
  val tx = match beginTransaction(q) { case Ok(t) -> t; case Err(e) -> return Err(e) }
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
      return Err(e)
    }
  }
  val ack = ackHeaders(delivery.ackId)
  ack.add(header("transaction", tx))
  match send(q, "ACK", ack, noBody()) {
    case Ok(_) -> ()
    case Err(e) -> {
      abortQuietly(q, tx)
      return Err(e)
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
  match checkUsable(q) {
    case Err(e) -> return Err(e)
    case Ok(_) -> ()
  }
  val delivery = match oldestDelivery(q, messageId) { case Ok(d) -> d; case Err(e) -> return Err(e) }
  val outcome = if how == 0 {
    request(q, "ACK", ackHeaders(delivery.ackId), noBody())
  } else if how == 1 {
    request(q, "NACK", ackHeaders(delivery.ackId), noBody())
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
      if automatic and not delivery.byHand {
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
    while true {
      if not first and K.monotonicMillis() >= deadline {
        return Err("timeout")
      }
      first = false
      match nextDelivery(self, deadline) {
        case Err(e) -> return Err(e)
        case Ok(None) -> ()
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
    K.lockEnter(self.lock)
    defer {
      K.lockExit(self.lock)
    }
    closeLocked(self)
  }
}

// ─── Introspection ───────────────────────────────────────────────────────────
/// The broker's product and version from its `CONNECTED` frame, e.g.
/// `ActiveMQ/5.17.6`.
pub func serverInfo(q: in ActiveMqQueue): String {
  q.server
}

/// The STOMP destination this connection publishes to and consumes from.
pub func destination(q: in ActiveMqQueue): String {
  q.options.destination
}

/// True until `close` is called or the connection is lost.
pub func isOpen(q: in ActiveMqQueue): Bool {
  K.lockEnter(q.lock)
  defer {
    K.lockExit(q.lock)
  }
  not q.closed and q.lost.length == 0
}

/// The number of deliveries handed to a handler and not yet settled.
pub func unsettledCount(q: in ActiveMqQueue): Int {
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
