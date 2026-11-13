//! Mq.Sqs.Client - a signed Amazon SQS client over `Std.Http`.
//!
//! One `SqsClient` talks to one SQS endpoint with the AWS JSON 1.0 protocol:
//! every action is a `POST /` with `X-Amz-Target: AmazonSQS.<Action>` and a
//! JSON body (`Mq.Sqs.Wire`), signed with AWS Signature Version 4
//! (`Aws.SigV4`, service `sqs`). There is no AWS SDK underneath, so the same
//! code runs on .NET and the JVM.
//!
//! Failures are `SqsError` values. `code` is the SQS error code of the
//! response (`QueueDoesNotExist`, `InvalidSignatureException`,
//! `ReceiptHandleIsInvalid`); a failed response without an error document is
//! `HTTP_<status>`; a request that was not answered within its deadline is
//! `TIMEOUT`, one that failed for any other reason before a response arrived
//! (refused, reset, TLS) `CONNECTION_FAILED`, one that could not be built
//! `INVALID_ARGUMENT`, a configuration that cannot be
//! used `INVALID_CONFIG`, and a successful response this client cannot read
//! `MALFORMED_RESPONSE`.
//!
//! Requests are not retried: a throttled or failed request is returned as the
//! error it is.
@runtime_checked
@io
package Mq.Sqs.Client

import Std.Core
import Std.Collections
import Std.Encoding as Enc
import Std.Errors
import Std.Http
import Std.JsonValue
import Std.String as Str
import Std.Task
import Aws.SigV4
import Aws.SigV4.Env as AwsEnv
import Mq
import Mq.Sqs.Address
import Mq.Sqs.Wire

// The default per-request deadline, 30 seconds. A long poll adds its wait.
val DEFAULT_REQUEST_TIMEOUT_MS: Int = 30000

// SQS's default visibility timeout.
val DEFAULT_VISIBILITY_TIMEOUT_SECONDS: Int = 30

// The request size SQS accepted until 2025; it now takes 1 MiB, but
// SQS-compatible servers and older regions' quotas still answer
// BatchRequestTooLong above this.
val DEFAULT_MAX_BATCH_BYTES: Int = 262144

/// A failed SQS operation.
pub record SqsError {
  /// The SQS error code, or one of this client's own (module documentation).
  code: String
  message: String
  /// The HTTP status of the response, 0 when there was none.
  status: Int
}

/// The error as the `Err` text of an `Mq` operation: `sqs: <message>`.
@pure
pub func SqsError.text(e: in SqsError): String {
  "sqs: " + e.message
}

func invalidConfig(message: in String): SqsError {
  SqsError(code = "INVALID_CONFIG", message = message, status = 0)
}

/// How a FIFO queue's `MessageDeduplicationId` is chosen for a published
/// message. A standard queue has no deduplication and ignores this.
pub enum Deduplication {
  /// The message's id (its SHA-256 when the id is longer than 128 characters
  /// or holds a character a deduplication id may not): publishing the same
  /// id again within SQS's five-minute window is accepted and not delivered
  /// twice.
  case ById
  /// A fresh random id per publish: nothing is ever deduplicated.
  case Unique
  /// No id is sent; the queue must have `ContentBasedDeduplication` enabled,
  /// and SQS then deduplicates by the SHA-256 of the body.
  case ContentBased
}

/// Where a queue is, how to sign for it and how to consume from it.
///
/// `queueUrl` is the full queue URL
/// (`https://sqs.eu-west-1.amazonaws.com/123456789012/orders`); it may be
/// empty for a client that only manages queues (`openClient`). `region` may
/// be empty when the queue URL or the endpoint is an AWS host name that
/// carries it. `endpoint` is empty to use the queue URL's host (or the
/// region's AWS endpoint when there is no queue URL), or the base URL of an
/// SQS-compatible server. Both URLs must be `https://` unless the host is
/// loopback. `sessionToken` is empty for long-term credentials.
pub record SqsConfig {
  queueUrl: String
  region: String
  accessKeyId: String
  secretAccessKey: String
  sessionToken: String
  endpoint: String
  /// The deadline of one request in milliseconds, not counting the seconds a
  /// long poll waits.
  requestTimeoutMs: Int
  /// `Manual` (the default): a delivery stays in flight until `ack` or
  /// `nack`. `Auto`: `consume` deletes the message when the handler returns
  /// `Ok` and makes it visible again when it returns `Err`.
  ackMode: Mq.AckMode
  /// How long a received message stays hidden from other receives, 0 to
  /// 43200 seconds. Sent with every receive, so it overrides the queue's own
  /// setting for this consumer.
  visibilityTimeoutSeconds: Int
  /// How many messages one receive asks for, 1 to 10. Messages beyond the
  /// first wait in the consumer, already in flight, for the next `consume`.
  receiveBatchSize: Int
  /// The `MessageGroupId` of messages published to a FIFO queue.
  messageGroupId: String
  /// The `MessageDeduplicationId` of messages published to a FIFO queue.
  deduplication: Deduplication
  /// A queue `nack(id, false)` copies the message to before deleting it, or
  /// empty to delete it only.
  deadLetterQueueUrl: String
  /// The largest `SendMessageBatch` request, in bytes of message bodies and
  /// attributes; a larger `publishBatch` is split.
  maxBatchBytes: Int
}

/// Configuration for a queue on an SQS-compatible server at `endpoint`
/// (ElasticMQ, LocalStack), or on AWS through a custom endpoint.
pub func endpointConfig(
  endpoint: in String,
  queueUrl: in String,
  region: in String,
  accessKeyId: in String,
  secretAccessKey: in String,
): SqsConfig {
  SqsConfig(
    queueUrl = queueUrl,
    region = region,
    accessKeyId = accessKeyId,
    secretAccessKey = secretAccessKey,
    sessionToken = "",
    endpoint = endpoint,
    requestTimeoutMs = DEFAULT_REQUEST_TIMEOUT_MS,
    ackMode = Mq.AckMode.Manual,
    visibilityTimeoutSeconds = DEFAULT_VISIBILITY_TIMEOUT_SECONDS,
    receiveBatchSize = 1,
    messageGroupId = "default",
    deduplication = Deduplication.ById,
    deadLetterQueueUrl = "",
    maxBatchBytes = DEFAULT_MAX_BATCH_BYTES
  )
}

/// Configuration for a queue on AWS: the endpoint and the region are the
/// queue URL's.
pub func awsConfig(queueUrl: in String, accessKeyId: in String, secretAccessKey: in String): SqsConfig {
  endpointConfig("", queueUrl, "", accessKeyId, secretAccessKey)
}

/// Configuration for `queueUrl` from the standard AWS environment variables:
/// `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, optional `AWS_SESSION_TOKEN`,
/// the region from `AWS_REGION` or `AWS_DEFAULT_REGION` (optional when the
/// queue URL carries it), and an optional endpoint from
/// `AWS_ENDPOINT_URL_SQS` or `AWS_ENDPOINT_URL`. `Err(INVALID_CONFIG)` names
/// the first required variable that is missing.
pub func configFromEnv(queueUrl: in String): Result[SqsConfig, SqsError] {
  val creds = match AwsEnv.credentialsFromEnv() {
    case Ok(c) -> c
    case Err(message) -> return Err(invalidConfig(message))
  }
  val region = match AwsEnv.regionFromEnv() { case Ok(r) -> r; case Err(_) -> "" }
  Ok(
    endpointConfig(AwsEnv.endpointFromEnv("SQS"), queueUrl, region, creds.accessKeyId, creds.secretAccessKey).copy(
      sessionToken = creds.sessionToken
    )
  )
}

/// `cfg` with temporary credentials' session token.
pub func SqsConfig.withSessionToken(cfg: in SqsConfig, sessionToken: in String): SqsConfig {
  cfg.copy(sessionToken = sessionToken)
}

/// `cfg` with a per-request deadline in milliseconds.
pub func SqsConfig.withRequestTimeout(cfg: in SqsConfig, requestTimeoutMs: in Int): SqsConfig {
  cfg.copy(requestTimeoutMs = requestTimeoutMs)
}

/// `cfg` with an acknowledgement mode.
pub func SqsConfig.withAckMode(cfg: in SqsConfig, ackMode: in Mq.AckMode): SqsConfig {
  cfg.copy(ackMode = ackMode)
}

/// `cfg` with a visibility timeout in seconds.
pub func SqsConfig.withVisibilityTimeout(cfg: in SqsConfig, seconds: in Int): SqsConfig {
  cfg.copy(visibilityTimeoutSeconds = seconds)
}

/// `cfg` receiving up to `count` messages per request.
pub func SqsConfig.withReceiveBatch(cfg: in SqsConfig, count: in Int): SqsConfig {
  cfg.copy(receiveBatchSize = count)
}

/// `cfg` publishing to a FIFO queue's message group `groupId`.
pub func SqsConfig.withMessageGroup(cfg: in SqsConfig, groupId: in String): SqsConfig {
  cfg.copy(messageGroupId = groupId)
}

/// `cfg` with a FIFO deduplication rule.
pub func SqsConfig.withDeduplication(cfg: in SqsConfig, deduplication: in Deduplication): SqsConfig {
  cfg.copy(deduplication = deduplication)
}

/// `cfg` copying a message to the queue at `queueUrl` when it is nacked
/// without requeue.
pub func SqsConfig.withDeadLetterQueue(cfg: in SqsConfig, queueUrl: in String): SqsConfig {
  cfg.copy(deadLetterQueueUrl = queueUrl)
}

/// A connection to one SQS endpoint. Build one with `openClient`; the fields
/// are derived from a validated `SqsConfig` and are not meant to be set by
/// hand.
pub record SqsClient {
  region: String
  creds: Credentials
  // `https://host[:port]`, no trailing slash.
  origin: String
  // The authority sent as `Host` and signed: `host[:port]`.
  host: String
  requestTimeoutMs: Int
  http: HttpClient
}

/// Validate the connection part of `cfg` and open a client for its endpoint.
/// No request is made: a wrong credential shows up on the first operation.
///
/// `Err(INVALID_CONFIG)` for empty credentials, a malformed endpoint or
/// queue URL, a plain `http://` URL that is not loopback, a region that is
/// neither given nor part of an AWS host name, or a non-positive timeout.
pub func openClient(cfg: in SqsConfig): Result[SqsClient, SqsError] {
  if cfg.accessKeyId.length == 0 or cfg.secretAccessKey.length == 0 {
    return Err(invalidConfig("SQS access key id and secret access key must be non-empty"))
  }
  if cfg.requestTimeoutMs <= 0 {
    return Err(invalidConfig("requestTimeoutMs must be > 0, got " + cfg.requestTimeoutMs.toString()))
  }
  var queueRegion = ""
  var endpoint = Endpoint(scheme = "", authority = "")
  if cfg.queueUrl.length > 0 {
    match parseQueueUrl(cfg.queueUrl) {
      case Err(message) -> return Err(invalidConfig(message))
      case Ok(q) -> {
        queueRegion = q.region
        endpoint = q.endpoint
      }
    }
  }
  if cfg.endpoint.length > 0 {
    match parseEndpoint(cfg.endpoint) {
      case Err(message) -> return Err(invalidConfig(message))
      case Ok(e) -> {
        endpoint = e
      }
    }
  }
  var region = cfg.region
  if region.length == 0 {
    region = if queueRegion.length > 0 { queueRegion } else { regionOfHost(endpoint.authority) }
  }
  if region.length == 0 {
    return Err(invalidConfig("no AWS region: set one, or use a queue URL or endpoint on sqs.<region>.amazonaws.com"))
  }
  if not isValidRegion(region) {
    return Err(invalidConfig("invalid AWS region: '" + region + "'"))
  }
  if endpoint.authority.length == 0 {
    endpoint = Endpoint(scheme = "https", authority = "sqs." + region + ".amazonaws.com")
  }
  Ok(
    SqsClient(
      region = region,
      creds = Credentials(
        accessKeyId = cfg.accessKeyId,
        secretAccessKey = cfg.secretAccessKey,
        sessionToken = cfg.sessionToken
      ),
      origin = endpoint.origin(),
      host = endpoint.authority,
      requestTimeoutMs = cfg.requestTimeoutMs,
      // A redirect would be followed to a host the signature does not cover;
      // it is returned as the error it is instead.
      http = clientNoRedirects()
    )
  )
}

func transportError(message: in String): SqsError {
  SqsError(code = "CONNECTION_FAILED", message = message, status = 0)
}

func invalidArgument(message: in String): SqsError {
  SqsError(code = "INVALID_ARGUMENT", message = message, status = 0)
}

/// True when `cause`, the message of an `HttpError.ConnectionFailed`, is one
/// of the texts the host HTTP client uses for a request that ran out of
/// time.
///
/// `Std.Http` reports every transport failure as `ConnectionFailed` and has
/// no timeout variant in use. The request deadline is a cancellation token,
/// and a cancelled token is the first evidence of a timeout; but on the JVM
/// the deadline is enforced by the JDK's own request timer, which fires up
/// to a millisecond before the token's deadline is reached, so the token
/// still reads "not cancelled" for about a third of the timeouts there. The
/// JDK's two messages for that timer are matched exactly: `request timed
/// out` (`HttpTimeoutException`) and `HTTP connect timed out`
/// (`HttpConnectTimeoutException`, when the time ran out while connecting).
/// On .NET the token is always cancelled when its timer ends a request; the
/// one other timer there is `HttpClient.Timeout` (100 seconds), whose message
/// names it.
@pure
pub func isTimeoutCause(cause: in String): Bool {
  cause == "request timed out" or cause == "HTTP connect timed out" or Str.contains(cause, "HttpClient.Timeout")
}

func timeoutError(action: in String, timeoutMs: in Int, origin: in String): SqsError {
  SqsError(
    code = "TIMEOUT",
    message = action + " timed out after " + timeoutMs.toString() + " ms at " + origin,
    status = 0
  )
}

// Plain (non-`async`) so it may `await` in place and still wrap the send in
// `try` (V0012 forbids `await` inside `try` in an `async func`).
func sendCatching(
  client: in SqsClient, req: in HttpRequest, action: in String, timeoutMs: in Int
): Result[HttpResponse, SqsError] {
  val src = makeCancelSourceTimeout(timeoutMs)
  val tok = sourceToken(src)
  val sent = try {
    await client.http.sendWithCancel(req, tok)
  } catch Bug as bug {
    Err(error = ConnectionFailed(url = client.origin, message = bug.message))
  }
  val deadlinePassed = isCancelled(tok)
  disposeSource(src)
  match sent {
    case Ok(resp) -> Ok(resp)
    case Err(e) -> {
      val timedOut = deadlinePassed or
        match e {
          case ConnectionFailed(_, cause) -> isTimeoutCause(cause)
          case Timeout(_, _) -> true
          case _ -> false
        }
      if timedOut {
        Err(timeoutError(action, timeoutMs, client.origin))
      } else {
        Err(transportError(action + " got no response from " + client.origin + ": " + HttpError.message(e)))
      }
    }
  }
}

func readBody(resp: in HttpResponse, action: in String): Result[slice[Byte], SqsError] {
  val read = try {
    await HttpResponse.bodyBytes(resp)
  } catch Bug as bug {
    return Err(transportError(action + ": cannot read the response body: " + bug.message))
  }
  match read {
    case Ok(bytes) -> Ok(bytes)
    case Err(e) -> Err(transportError(action + ": cannot read the response body: " + IOError.message(e)))
  }
}

/// The `SqsError` for a response that is not a 200: the SQS error code and
/// message when the body is an error document, `HTTP_<status>` otherwise.
@pure
pub func faultOf(action: in String, status: in Int, body: in String): SqsError {
  match parseFault(body) {
    case Some(fault) -> SqsError(
      code = fault.code,
      message = action + " failed: " + fault.code + " (HTTP " + status.toString() + ")" +
        (if fault.message.length > 0 { ": " + fault.message } else { "" }),
      status = status
    )
    case None -> SqsError(
      code = "HTTP_" + status.toString(),
      message = action + " failed: SQS answered HTTP " + status.toString(),
      status = status
    )
  }
}

/// Sign and send one action and return the response document.
///
/// `action` is the SQS action name (`SendMessage`), `body` the JSON request
/// (`Mq.Sqs.Wire`), and `extraWaitMs` what the server may legitimately spend
/// before it answers (the wait of a long poll), added to the request
/// deadline.
pub func SqsClient.call(
  client: in SqsClient, action: in String, body: in String, extraWaitMs: in Int
): Result[JsonValue, SqsError]
  requires: extraWaitMs >= 0
{
  val bytes = Enc.encodeUtf8(body)
  val extra: List[Pair] = newList()
  extra.add(Pair(name = "content-type", value = "application/x-amz-json-1.0"))
  extra.add(Pair(name = "x-amz-target", value = "AmazonSQS." + action))
  val noQuery: List[Pair] = newList()
  val signed = signedHeaders(
    "POST",
    canonicalUri("/", false),
    noQuery,
    client.host,
    extra,
    sha256Hex(bytes),
    false,
    client.creds,
    client.region,
    "sqs",
    AwsEnv.amzDateNow()
  )
  val url = client.origin + "/"
  val parsed = match Url.tryFrom(url) {
    case Ok(u) -> u
    case Err(_) -> return Err(invalidArgument("cannot build an SQS request URL: " + url))
  }
  var req = match withBytesBody(request(POST, parsed), "application/x-amz-json-1.0", bytes) {
    case Ok(r) -> r
    case Err(e) -> return Err(invalidArgument(HttpError.message(e)))
  }
  // `Host` is set by the HTTP client from the URL and `Content-Type` by the
  // body; every other signed header is sent as signed, then `Authorization`.
  var k = 0
  while k < signed.headers.count {
    if signed.headers[k].name != "content-type" {
      match withHeader(req, signed.headers[k].name, signed.headers[k].value) {
        case Ok(r) -> {
          req = r
        }
        case Err(e) -> return Err(invalidArgument(HttpError.message(e)))
      }
    }
    k = k + 1
  }
  val resp = match sendCatching(client, req, action, client.requestTimeoutMs + extraWaitMs) {
    case Ok(r) -> r
    case Err(e) -> return Err(e)
  }
  val responseBytes = match readBody(resp, action) { case Ok(b) -> b; case Err(e) -> return Err(e) }
  val status = HttpResponse.statusCode(resp)
  val responseText = match Enc.tryDecodeUtf8(responseBytes) { case Some(t) -> t; case None -> "" }
  if status != 200 {
    return Err(faultOf(action, status, responseText))
  }
  match parseDocument(responseText) {
    case Ok(doc) -> Ok(doc)
    case Err(message) -> Err(SqsError(code = "MALFORMED_RESPONSE", message = action + ": " + message, status = status))
  }
}

func malformed(action: in String, message: in String): SqsError {
  SqsError(code = "MALFORMED_RESPONSE", message = action + ": " + message, status = 200)
}

/// SendMessage: returns the `MessageId` SQS assigned.
pub func SqsClient.sendMessage(
  client: in SqsClient, queueUrl: in String, outgoing: in Outgoing, send: in SendFields
): Result[String, SqsError] {
  match client.call("SendMessage", sendMessageRequest(queueUrl, outgoing, send), 0) {
    case Err(e) -> Err(e)
    case Ok(doc) -> match getString(doc, "MessageId") {
      case Some(id) -> Ok(id)
      case None -> Err(malformed("SendMessage", "the response has no MessageId"))
    }
  }
}

/// SendMessageBatch for at most ten entries. `Ok` means SQS answered: the
/// outcome says which entries it accepted.
pub func SqsClient.sendMessageBatch(
  client: in SqsClient, queueUrl: in String, entries: in List[BatchEntry]
): Result[BatchOutcome, SqsError]
  requires: entries.count >= 1 and entries.count <= 10
{
  match client.call("SendMessageBatch", sendMessageBatchRequest(queueUrl, entries), 0) {
    case Err(e) -> Err(e)
    case Ok(doc) -> match parseBatchResponse(doc) {
      case Ok(outcome) -> Ok(outcome)
      case Err(message) -> Err(malformed("SendMessageBatch", message))
    }
  }
}

/// ReceiveMessage: up to `maxMessages` messages, waiting up to `waitSeconds`
/// (0 to 20) for the first; each is hidden for `visibilityTimeoutSeconds`.
pub func SqsClient.receiveMessages(
  client: in SqsClient,
  queueUrl: in String,
  maxMessages: in Int,
  waitSeconds: in Int,
  visibilityTimeoutSeconds: in Int,
): Result[List[Incoming], SqsError]
  requires: maxMessages >= 1 and maxMessages <= 10
  requires: waitSeconds >= 0 and waitSeconds <= 20
  requires: visibilityTimeoutSeconds >= 0 and visibilityTimeoutSeconds <= 43200
{
  val body = receiveMessageRequest(queueUrl, maxMessages, waitSeconds, visibilityTimeoutSeconds)
  match client.call("ReceiveMessage", body, waitSeconds * 1000) {
    case Err(e) -> Err(e)
    case Ok(doc) -> match parseReceiveResponse(doc) {
      case Ok(messages) -> Ok(messages)
      case Err(message) -> Err(malformed("ReceiveMessage", message))
    }
  }
}

/// DeleteMessage: settle the delivery `receiptHandle` names.
pub func SqsClient.deleteMessage(
  client: in SqsClient, queueUrl: in String, receiptHandle: in String
): Result[Unit, SqsError] {
  match client.call("DeleteMessage", deleteMessageRequest(queueUrl, receiptHandle), 0) {
    case Err(e) -> Err(e)
    case Ok(_) -> Ok(())
  }
}

/// ChangeMessageVisibility: hide the delivery for `seconds` from now; 0
/// returns it to the queue.
pub func SqsClient.changeVisibility(
  client: in SqsClient, queueUrl: in String, receiptHandle: in String, seconds: in Int
): Result[Unit, SqsError]
  requires: seconds >= 0 and seconds <= 43200
{
  match client.call("ChangeMessageVisibility", changeVisibilityRequest(queueUrl, receiptHandle, seconds), 0) {
    case Err(e) -> Err(e)
    case Ok(_) -> Ok(())
  }
}

/// ChangeMessageVisibilityBatch for at most ten handles. Entry ids in the
/// outcome are the handles' positions.
pub func SqsClient.changeVisibilityBatch(
  client: in SqsClient,
  queueUrl: in String,
  receiptHandles: in List[String],
  seconds: in Int,
): Result[BatchOutcome, SqsError]
  requires: receiptHandles.count >= 1 and receiptHandles.count <= 10
  requires: seconds >= 0 and seconds <= 43200
{
  val body = changeVisibilityBatchRequest(queueUrl, receiptHandles, seconds)
  match client.call("ChangeMessageVisibilityBatch", body, 0) {
    case Err(e) -> Err(e)
    case Ok(doc) -> match parseBatchResponse(doc) {
      case Ok(outcome) -> Ok(outcome)
      case Err(message) -> Err(malformed("ChangeMessageVisibilityBatch", message))
    }
  }
}
