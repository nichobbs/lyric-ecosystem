//! tests/readme_tests.l - the examples of the README, as written there,
//! compiled, and the ones that connect run as far as a refused connection
//! (nothing listens on the port they are given). What the examples do with a
//! broker is covered by `tests/client_tests.l` and the integration suite.
@test_module
package Mq.ActiveMq.ReadmeTests

import Std.Testing
import Std.String as Str
import Std.Core
import Mq
import Mq.ActiveMq as ActiveMq
import Std.File as File
import Std.Tls as Tls
import Mq.ActiveMq.Extras as Extras
import Mq.ActiveMq.Stomp as Stomp

func handle(m: in Mq.Message): Result[Unit, String] {
  println(m.id + ": " + m.body)
  Ok(())
}

func example(url: in String): Result[Unit, String] {
  val queue = ActiveMq.connectUrl(url, "orders")?
  defer {
    queue.close()
  }
  Mq.publish(queue, Mq.Message(id = "order-42", body = "{\"total\":12}", headers = [Mq.MessageHeader(key = "content-type", value = "application/json")], deliveryCount = 0))?
  Mq.consume(queue, 5000, { m -> handle(m) })?
  Mq.ack(queue, "order-42")?
  Ok(())
}

func configured(passcode: in String): Result[ActiveMq.ActiveMqQueue, String] {
  val base = ActiveMq.defaultOptions("broker.internal", 61613, ActiveMq.resolveDestination("orders"))
  ActiveMq.connectWith(
    base.copy(
      login = "app",
      passcode = passcode,
      ackMode = Mq.AckMode.Auto,
      prefetchSize = 1,
      connectTimeoutMs = 3000,
      requestTimeoutMs = 5000
    )
  )
}

func fromEnvironment(): Result[ActiveMq.ActiveMqQueue, String] {
  ActiveMq.connectFromEnv()
}

func process(m: in Mq.Message): Result[Unit, String] {
  if m.body.length == 0 {
    Err("empty body")
  } else {
    Ok(())
  }
}

ref record Seen {
  var id: String
  var deliveryCount: Int
}

func settleOne(queue: in ActiveMq.ActiveMqQueue): Result[Unit, String] {
  val seen = Seen(id = "", deliveryCount = 0)
  val outcome = queue.consume(1000, { m -> seen.id = m.id; seen.deliveryCount = m.deliveryCount; process(m) })
  if seen.id.length == 0 {
    return outcome
  }
  match outcome {
    case Ok(_) -> queue.ack(seen.id)
    case Err(_) -> queue.nack(seen.id, seen.deliveryCount < 5)
  }
}

func publishOrKeep(queue: in ActiveMq.ActiveMqQueue, m: in Mq.Message): Result[Bool, String] {
  match queue.publish(m) {
    case Ok(_) -> Ok(true)
    case Err(e) -> if ActiveMq.isNotConnectedError(e) {
      // Nothing was sent: keep the message and publish it later.
      Ok(false)
    } else {
      // `isOutcomeUnknownError(e)`: the broker may have it. Publishing it
      // again can make a duplicate; that is the application's decision.
      Err(e)
    }
  }
}

func tuned(url: in String): Result[ActiveMq.ActiveMqQueue, String] {
  val base = ActiveMq.optionsFromUrl(url, "orders")?
  val reconnect = ActiveMq.defaultReconnect()
  ActiveMq.connectWith(
    base.copy(
      heartBeatSendMs = 5000,
      heartBeatReceiveMs = 5000,
      reconnect = reconnect.copy(initialDelayMs = 500, maxDelayMs = 10000, maxAttempts = 20)
    )
  )
}

func withoutReconnect(url: in String): Result[ActiveMq.ActiveMqQueue, String] {
  val base = ActiveMq.optionsFromUrl(url, "orders")?
  ActiveMq.connectWith(base.copy(heartBeatSendMs = 0, heartBeatReceiveMs = 0, reconnect = ActiveMq.noReconnect()))
}

func send(sink: in Mq.MessageQueue, id: in String, body: in String): Result[Unit, String] {
  sink.publish(Mq.Message(id = id, body = body, headers = [], deliveryCount = 0))
}

func wired(queue: in ActiveMq.ActiveMqQueue): Result[Unit, String] {
  val publisher: Mq.MessageQueue = queue
  val consumer: Mq.QueueConsumer = queue
  send(publisher, "a-1", "first")?
  Mq.publishBatch(publisher, [Mq.Message(id = "a-2", body = "second", headers = [], deliveryCount = 0), Mq.Message(id = "a-3", body = "third", headers = [], deliveryCount = 0)])?
  consumer.consume(0, { m -> handle(m) })
}

func tlsPublic(): Result[ActiveMq.ActiveMqQueue, String] {
  ActiveMq.connectUrl("stomp+ssl://app:passcode@broker.example.com:61614", "orders")
}

func pem(path: in String): Result[String, String] {
  match File.readText(path) {
    case Ok(text) -> Ok(text)
    case Err(_) -> Err("cannot read " + path)
  }
}

func tlsPrivate(): Result[ActiveMq.ActiveMqQueue, String] {
  val ca = pem("/etc/app/broker-ca.pem")?
  val certificate = pem("/etc/app/client.pem")?
  val key = pem("/etc/app/client.key")?
  val base = ActiveMq.optionsFromUrl("stomp+ssl://app:passcode@10.0.0.5:61614", "orders")?
  ActiveMq.connectWith(
    base.withExclusiveCaCertificate(ca).withClientIdentity(certificate, key).withTlsServerName("broker.internal").withMinTlsVersion(
      Tls.TlsVersion.Tls13
    )
  )
}

func tlsByHand(caPem: in String): Result[ActiveMq.ActiveMqQueue, String] {
  val base = ActiveMq.defaultOptions("broker.internal", 61614, "/queue/orders")
  ActiveMq.connectWith(base.copy(login = "app", passcode = "passcode").withTls().withCaCertificate(caPem))
}

func extras(queue: in ActiveMq.ActiveMqQueue): Result[Unit, String] {
  val options = Extras.defaultSendOptions()
  Extras.publishWith(
    queue,
    Mq.Message(id = "inv-7", body = "{}", headers = [], deliveryCount = 0),
    options.withPersistent(false).copy(
      priority = 7,
      timeToLiveMs = 60000i64,
      correlationId = "req-19",
      replyTo = Extras.queueDestination("replies"),
      jmsType = "invoice",
      properties = [Extras.Property(name = "region", value = "eu")]
    )
  )
}

func selected(url: in String): Result[ActiveMq.ActiveMqQueue, String] {
  val base = ActiveMq.optionsFromUrl(url, "orders")?
  ActiveMq.connectWith(base.copy(selector = Extras.selectorEquals("region", "eu")))
}

func topicSubscriber(url: in String): Result[ActiveMq.ActiveMqQueue, String] {
  val prices = ActiveMq.connectUrl(url, Extras.topicDestination("prices"))?
  ActiveMq.subscribe(prices)?
  Ok(prices)
}

func deadLetters(url: in String, id: in String): Result[ActiveMq.ActiveMqQueue, String] {
  val base = ActiveMq.optionsFromUrl(url, "ActiveMQ.DLQ")?
  ActiveMq.connectWith(base.copy(selector = Extras.selectorForId(id)))
}

func rawHeaders(queue: in ActiveMq.ActiveMqQueue): Result[Unit, String] {
  ActiveMq.publishWithHeaders(
    queue,
    Mq.Message(id = "g-1", body = "x", headers = [], deliveryCount = 0),
    [Stomp.Header(name = "JMSXGroupID", value = "customer-9")]
  )
}

// A port of the loopback interface that nothing listens on.
val NOWHERE: String = "stomp://app:passcode@127.0.0.1:1"

func refused[T](r: in Result[T, String], label: in String): Unit {
  match r {
    case Ok(_) -> assertTrue(false, label + ": nothing listens there")
    case Err(e) -> assertTrue(Str.contains(e, "activemq: cannot connect to 127.0.0.1:1"), label + ": got \"" + e + "\"")
  }
}

test "the usage examples compile and report a refused connection" {
  refused(example(NOWHERE), "example")
  refused(tuned(NOWHERE), "tuned")
  refused(withoutReconnect(NOWHERE), "withoutReconnect")
  refused(selected(NOWHERE), "selected")
  refused(topicSubscriber(NOWHERE), "topicSubscriber")
  refused(deadLetters(NOWHERE, "order-42"), "deadLetters")
}
