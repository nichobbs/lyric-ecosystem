//! tests/config_tests.l - configuration: what `openClient`, `connectWith`,
//! `configFromEnv` and `connectFromEnv` accept, derive and refuse. Nothing
//! here makes a request. The tests set the environment variables they read
//! (to documentation values, never a real credential) and restore them.
@test_module
package Mq.Sqs.ConfigTests

import Std.Core
import Std.Collections
import Std.Environment as Env
import Std.String as Str
import Std.Testing
import Mq
import Mq.Sqs

val AWS_QUEUE: String = "https://sqs.eu-west-1.amazonaws.com/123456789012/orders"

func clientText(cfg: in SqsConfig): String {
  match openClient(cfg) {
    case Ok(c) -> c.origin + " | " + c.host + " | " + c.region
    case Err(e) -> e.code + ": " + e.message
  }
}

func connectText(cfg: in SqsConfig): String {
  match connectWith(cfg) {
    case Ok(q) -> q.queue.url + " | " + (if q.queue.fifo { "fifo" } else { "standard" }) + " | " + q.client.origin +
      " | " +
      q.client.region
    case Err(e) -> e
  }
}

test "an AWS queue URL supplies the endpoint and the region" {
  assertEqual(
    connectText(awsConfig(AWS_QUEUE, "AKIDEXAMPLE", "secret")),
    AWS_QUEUE + " | standard | https://sqs.eu-west-1.amazonaws.com | eu-west-1",
    "a standard queue"
  )
  assertEqual(
    connectText(awsConfig(AWS_QUEUE + ".fifo", "AKIDEXAMPLE", "secret")),
    AWS_QUEUE + ".fifo | fifo | https://sqs.eu-west-1.amazonaws.com | eu-west-1",
    "a FIFO queue"
  )
  assertEqual(
    connectText(awsConfig(AWS_QUEUE, "AKIDEXAMPLE", "secret").copy(region = "eu-central-1")),
    AWS_QUEUE + " | standard | https://sqs.eu-west-1.amazonaws.com | eu-central-1",
    "an explicit region wins"
  )
  assertEqual(
    clientText(awsConfig("", "AKIDEXAMPLE", "secret").copy(region = "us-east-2")),
    "https://sqs.us-east-2.amazonaws.com | sqs.us-east-2.amazonaws.com | us-east-2",
    "a client without a queue: the region's endpoint"
  )
}

test "a custom endpoint is used instead of the queue URL's host" {
  assertEqual(
    connectText(endpointConfig("http://127.0.0.1:9324", "http://localhost:9324/000000000000/q", "elasticmq", "x", "x")),
    "http://localhost:9324/000000000000/q | standard | http://127.0.0.1:9324 | elasticmq",
    "ElasticMQ"
  )
  assertEqual(
    clientText(endpointConfig("https://vpce-1.sqs.eu-west-1.vpce.amazonaws.com", AWS_QUEUE, "", "x", "x")),
    "https://vpce-1.sqs.eu-west-1.vpce.amazonaws.com | vpce-1.sqs.eu-west-1.vpce.amazonaws.com | eu-west-1",
    "a VPC endpoint: the region still comes from the queue URL"
  )
}

test "openClient refuses what it cannot sign for or must not send to" {
  assertEqual(
    clientText(awsConfig(AWS_QUEUE, "", "secret")),
    "INVALID_CONFIG: SQS access key id and secret access key must be non-empty",
    "no access key"
  )
  assertEqual(
    clientText(awsConfig(AWS_QUEUE, "AKIDEXAMPLE", "")),
    "INVALID_CONFIG: SQS access key id and secret access key must be non-empty",
    "no secret"
  )
  assertEqual(
    clientText(awsConfig(AWS_QUEUE, "a", "s").withRequestTimeout(0)),
    "INVALID_CONFIG: requestTimeoutMs must be > 0, got 0",
    "no timeout"
  )
  assertEqual(
    clientText(awsConfig("http://sqs.internal/1/q", "a", "s")),
    "INVALID_CONFIG: SQS queue URL must use https:// (http:// is allowed only for localhost, 127.0.0.1 and [::1]): http://sqs.internal/1/q",
    "a plain-http queue URL off loopback"
  )
  assertEqual(
    clientText(endpointConfig("http://sqs.internal:9324", AWS_QUEUE, "eu-west-1", "a", "s")),
    "INVALID_CONFIG: SQS endpoint must use https:// (http:// is allowed only for localhost, 127.0.0.1 and [::1]): http://sqs.internal:9324",
    "a plain-http endpoint off loopback"
  )
  assertEqual(
    clientText(endpointConfig("http://127.0.0.1:9324", "http://127.0.0.1:9324/1/q", "", "a", "s")),
    "INVALID_CONFIG: no AWS region: set one, or use a queue URL or endpoint on sqs.<region>.amazonaws.com",
    "no region to sign for"
  )
  assertEqual(
    clientText(endpointConfig("http://127.0.0.1:9324", "", "EU West", "a", "s")),
    "INVALID_CONFIG: invalid AWS region: 'EU West'",
    "a region that is not a region"
  )
  assertEqual(
    clientText(awsConfig("", "a", "s")),
    "INVALID_CONFIG: no AWS region: set one, or use a queue URL or endpoint on sqs.<region>.amazonaws.com",
    "neither a queue nor a region"
  )
}

test "connectWith refuses a queue configuration SQS would refuse later" {
  val good = awsConfig(AWS_QUEUE, "a", "s")
  assertEqual(connectText(good.copy(queueUrl = "")), "sqs: no queue URL configured", "no queue")
  assertEqual(
    connectText(good.copy(queueUrl = "orders")),
    "sqs: SQS queue URL must be an absolute http(s) URL: orders",
    "a queue name is not a queue URL"
  )
  assertEqual(
    connectText(good.withVisibilityTimeout(0)),
    "sqs: visibilityTimeoutSeconds must be 1 to 43200, got 0",
    "a delivery must be hidden for some time to be settled"
  )
  assertEqual(
    connectText(good.withVisibilityTimeout(43201)),
    "sqs: visibilityTimeoutSeconds must be 1 to 43200, got 43201",
    "over twelve hours"
  )
  assertEqual(connectText(good.withReceiveBatch(0)), "sqs: receiveBatchSize must be 1 to 10, got 0", "no batch")
  assertEqual(connectText(good.withReceiveBatch(11)), "sqs: receiveBatchSize must be 1 to 10, got 11", "over ten")
  assertEqual(connectText(good.copy(maxBatchBytes = 0)), "sqs: maxBatchBytes must be > 0, got 0", "no bytes")
  assertEqual(
    connectText(good.withDeadLetterQueue("dead")),
    "sqs: dead-letter queue: SQS queue URL must be an absolute http(s) URL: dead",
    "a dead-letter queue name"
  )
  assertEqual(
    connectText(awsConfig(AWS_QUEUE + ".fifo", "a", "s").withMessageGroup("group one")),
    "sqs: messageGroupId must be 1 to 128 letters, digits and ASCII punctuation for a FIFO queue",
    "a group id with a space"
  )
  assertEqual(
    connectText(good.withMessageGroup("group one")),
    AWS_QUEUE + " | standard | https://sqs.eu-west-1.amazonaws.com | eu-west-1",
    "a standard queue has no groups to validate"
  )
}

test "the defaults are SQS's" {
  val cfg = awsConfig(AWS_QUEUE, "a", "s")
  assertEqualInt(cfg.requestTimeoutMs, 30000, "request timeout")
  assertEqualInt(cfg.visibilityTimeoutSeconds, 30, "visibility timeout")
  assertEqualInt(cfg.receiveBatchSize, 1, "one message per receive")
  assertEqualInt(cfg.maxBatchBytes, 262144, "256 KiB per batch")
  assertEqual(cfg.messageGroupId, "default", "message group")
  assertEqual(cfg.deadLetterQueueUrl, "", "no dead-letter queue")
  assertEqual(cfg.sessionToken, "", "long-term credentials")
  assertTrue(match cfg.ackMode { case Manual -> true; case Auto -> false }, "manual acknowledgement")
  assertTrue(
    match cfg.deduplication { case ById -> true; case Unique -> false; case ContentBased -> false },
    "deduplication by message id"
  )
  assertEqual(cfg.withSessionToken("t").sessionToken, "t", "withSessionToken")
}

val NAMES: slice[String] = ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_REGION", "AWS_DEFAULT_REGION", "AWS_ENDPOINT_URL", "AWS_ENDPOINT_URL_SQS", "LYRIC_CONFIG_MQ_SQS_QUEUEURL", "LYRIC_CONFIG_MQ_SQS_ACKMODE", "LYRIC_CONFIG_MQ_SQS_VISIBILITYTIMEOUT", "LYRIC_CONFIG_MQ_SQS_RECEIVEBATCH", "LYRIC_CONFIG_MQ_SQS_MESSAGEGROUP", "LYRIC_CONFIG_MQ_SQS_DEADLETTERQUEUEURL"]

func saved(): slice[String] {
  val acc: List[String] = newList()
  for name in NAMES {
    acc.add(Env.getVarOrDefault(name, ""))
  }
  acc.toArray()
}

func restore(values: in slice[String]): Unit {
  var i = 0
  while i < NAMES.length {
    Env.setVar(NAMES[i], values[i])
    i = i + 1
  }
}

func clearAll(): Unit {
  for name in NAMES {
    Env.setVar(name, "")
  }
}

func envConfigText(queueUrl: in String): String {
  match configFromEnv(queueUrl) {
    case Err(e) -> e.code + ": " + e.message
    case Ok(cfg) -> cfg.accessKeyId + " | " + cfg.sessionToken + " | " + cfg.region + " | " + cfg.endpoint
  }
}

func envConnectText(): String {
  match connectFromEnv() {
    case Err(e) -> e
    case Ok(q) -> q.queue.url + " | " + q.client.origin + " | " + q.client.region + " | " +
      (match q.cfg.ackMode { case Manual -> "Manual"; case Auto -> "Auto" }) +
      " | " +
      q.cfg.visibilityTimeoutSeconds.toString() +
      " | " +
      q.cfg.receiveBatchSize.toString() +
      " | " +
      q.cfg.messageGroupId +
      " | " +
      q.cfg.deadLetterQueueUrl
  }
}

test "configFromEnv reads the standard AWS variables" {
  val before = saved()
  clearAll()
  assertEqual(envConfigText(AWS_QUEUE), "INVALID_CONFIG: AWS_ACCESS_KEY_ID is not set", "nothing set")
  Env.setVar("AWS_ACCESS_KEY_ID", "AKIDEXAMPLE")
  assertEqual(envConfigText(AWS_QUEUE), "INVALID_CONFIG: AWS_SECRET_ACCESS_KEY is not set", "no secret")
  Env.setVar("AWS_SECRET_ACCESS_KEY", "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY")
  assertEqual(envConfigText(AWS_QUEUE), "AKIDEXAMPLE |  |  | ", "the region is optional: the queue URL carries it")
  Env.setVar("AWS_SESSION_TOKEN", "token")
  Env.setVar("AWS_DEFAULT_REGION", "eu-west-2")
  Env.setVar("AWS_ENDPOINT_URL", "http://127.0.0.1:4566")
  assertEqual(
    envConfigText(AWS_QUEUE),
    "AKIDEXAMPLE | token | eu-west-2 | http://127.0.0.1:4566",
    "token, region, endpoint"
  )
  Env.setVar("AWS_REGION", "eu-west-3")
  Env.setVar("AWS_ENDPOINT_URL_SQS", "http://127.0.0.1:9324")
  assertEqual(
    envConfigText(AWS_QUEUE),
    "AKIDEXAMPLE | token | eu-west-3 | http://127.0.0.1:9324",
    "the specific variables win"
  )
  restore(before)
}

test "connectFromEnv opens the queue the environment describes" {
  val before = saved()
  clearAll()
  assertEqual(envConnectText(), "sqs: no queue URL configured (set LYRIC_CONFIG_MQ_SQS_QUEUEURL)", "no queue URL")
  Env.setVar("LYRIC_CONFIG_MQ_SQS_QUEUEURL", AWS_QUEUE + ".fifo")
  assertEqual(envConnectText(), "sqs: AWS_ACCESS_KEY_ID is not set", "no credentials")
  Env.setVar("AWS_ACCESS_KEY_ID", "AKIDEXAMPLE")
  Env.setVar("AWS_SECRET_ACCESS_KEY", "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY")
  assertEqual(
    envConnectText(),
    AWS_QUEUE + ".fifo | https://sqs.eu-west-1.amazonaws.com | eu-west-1 | Manual | 30 | 1 | default | ",
    "the defaults"
  )
  Env.setVar("LYRIC_CONFIG_MQ_SQS_ACKMODE", "Auto")
  Env.setVar("LYRIC_CONFIG_MQ_SQS_VISIBILITYTIMEOUT", "120")
  Env.setVar("LYRIC_CONFIG_MQ_SQS_RECEIVEBATCH", "10")
  Env.setVar("LYRIC_CONFIG_MQ_SQS_MESSAGEGROUP", "tenant-7")
  Env.setVar("LYRIC_CONFIG_MQ_SQS_DEADLETTERQUEUEURL", AWS_QUEUE + "-dead.fifo")
  assertEqual(
    envConnectText(),
    AWS_QUEUE + ".fifo | https://sqs.eu-west-1.amazonaws.com | eu-west-1 | Auto | 120 | 10 | tenant-7 | " + AWS_QUEUE +
      "-dead.fifo",
    "every variable"
  )
  Env.setVar("LYRIC_CONFIG_MQ_SQS_ACKMODE", "sometimes")
  assertEqual(envConnectText(), "sqs: LYRIC_CONFIG_MQ_SQS_ACKMODE must be Manual or Auto", "a bad ack mode")
  Env.setVar("LYRIC_CONFIG_MQ_SQS_ACKMODE", "manual")
  Env.setVar("LYRIC_CONFIG_MQ_SQS_VISIBILITYTIMEOUT", "30s")
  assertEqual(
    envConnectText(),
    "sqs: LYRIC_CONFIG_MQ_SQS_VISIBILITYTIMEOUT must be a number of seconds",
    "a bad visibility timeout"
  )
  Env.setVar("LYRIC_CONFIG_MQ_SQS_VISIBILITYTIMEOUT", "30")
  Env.setVar("LYRIC_CONFIG_MQ_SQS_RECEIVEBATCH", "many")
  assertEqual(envConnectText(), "sqs: LYRIC_CONFIG_MQ_SQS_RECEIVEBATCH must be a number from 1 to 10", "a bad batch")
  Env.setVar("LYRIC_CONFIG_MQ_SQS_RECEIVEBATCH", "50")
  assertEqual(envConnectText(), "sqs: receiveBatchSize must be 1 to 10, got 50", "a batch out of range")
  restore(before)
}

test "errors render as the Err text of an Mq operation" {
  val e = SqsError(
    code = "QueueDoesNotExist",
    message = "SendMessage failed: QueueDoesNotExist (HTTP 400)",
    status = 400
  )
  assertEqual(e.text(), "sqs: SendMessage failed: QueueDoesNotExist (HTTP 400)", "text")
  val f = faultOf(
    "ReceiveMessage",
    400,
    "{\"__type\":\"com.amazonaws.sqs#OverLimit\",\"message\":\"too many in flight\"}"
  )
  assertEqual(f.code, "OverLimit", "code")
  assertEqual(f.message, "ReceiveMessage failed: OverLimit (HTTP 400): too many in flight", "message")
  assertEqualInt(f.status, 400, "status")
  val bare = faultOf("DeleteMessage", 503, "")
  assertEqual(bare.code, "HTTP_503", "no document")
  assertEqual(bare.message, "DeleteMessage failed: SQS answered HTTP 503", "no document message")
}

test "the host HTTP clients' timeout texts are recognised, and nothing else" {
  // --target jvm: the JDK request timer, which can fire before the deadline
  // token reads as cancelled.
  assertTrue(isTimeoutCause("request timed out"), "java.net.http.HttpTimeoutException")
  assertTrue(isTimeoutCause("HTTP connect timed out"), "java.net.http.HttpConnectTimeoutException")
  // --target dotnet: the 100 s cap of HttpClient itself.
  assertTrue(
    isTimeoutCause(
      "One or more errors occurred. (The request was canceled due to the configured HttpClient.Timeout of 100 seconds elapsing.)"
    ),
    "HttpClient.Timeout"
  )
  // What a refused or broken connection looks like on each target.
  assertTrue(not isTimeoutCause("java.net.ConnectException"), "jvm: connection refused")
  assertTrue(
    not isTimeoutCause("One or more errors occurred. (Connection refused (127.0.0.1:1))"),
    "dotnet: connection refused"
  )
  assertTrue(not isTimeoutCause("Connection reset"), "a reset")
  assertTrue(not isTimeoutCause("the request timed out of patience"), "only the exact JDK text")
  assertTrue(not isTimeoutCause(""), "no cause")
}
