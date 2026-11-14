//! tests/reexport_tests.l - an application that imports only `Mq.RabbitMq`
//! can name the interface types, because the backend re-exports `Mq`, and
//! the TLS options, because it re-exports `Net.Tls`.
@test_module
package Mq.RabbitMq.ReexportTests

import Std.Core
import Std.Testing
import Mq
import Mq.RabbitMq
import Mq.RabbitMq.Extras

func describe(queue: in MessageQueue, m: in Message): String {
  m.id + "/" + m.headers.length.toString()
}

test "Message, MessageHeader and the interfaces are visible through the backend" {
  val m = Message(id = "m-1", body = "b", headers = [MessageHeader(key = "k", value = "v")], deliveryCount = 0)
  assertEqual(m.id, "m-1", "Message")
  assertEqual(m.headers[0].key, "k", "MessageHeader")
  val options = defaultOptions("127.0.0.1", 5672, "orders")
  assertEqual(options.queue, "orders", "the backend's own names")
  assertTrue(options.persistent, "persistent by default")
  assertTrue(options.durable and options.declare, "a durable queue, declared on connect")
  assertEqualInt(options.prefetchCount, 10, "the default prefetch")
  assertEqualInt(options.heartbeatSeconds, 60, "a 60 s heartbeat by default")
  assertTrue(options.recovery.enabled, "recovery on by default")
  assertEqual(replyCodeName(404), "NOT_FOUND", "reply code names")
  assertEqual(replyCodeName(406), "PRECONDITION_FAILED", "reply code names")
  assertEqual(replyCodeName(999), "", "an undefined reply code")
}

// The usage example of the README, compiled as written there.
func process(m: in Message): Result[Unit, String] {
  Ok(())
}

func readmeExample(url: in String): Result[Unit, String] {
  val queue = Mq.RabbitMq.connectUrl(url, "orders")?
  Mq.publish(queue, Mq.Message(id = "order-42", body = "{\"total\":12}", headers = [Mq.MessageHeader(key = "content-type", value = "application/json")], deliveryCount = 0))?
  Mq.consume(queue, 5000, { m -> process(m) })?
  Mq.ack(queue, "order-42")?
  queue.close()
  Ok(())
}

test "the README example compiles and reports an unreachable broker as an Err" {
  match readmeExample("amqp://app:secret@127.0.0.1:1/prod") {
    case Ok(_) -> assertTrue(false, "nothing listens on port 1")
    case Err(e) -> assertTrue(e.contains("rabbitmq: cannot connect to 127.0.0.1:1"), e)
  }
}

// The `Mq.RabbitMq.Extras` example of the README, compiled as written there.
func readmeExtrasExample(url: in String): Result[Unit, String] {
  val options = Mq.RabbitMq.optionsFromUrl(url, "orders")?.withQuorumQueue().withDeliveryLimit(5).withDeadLetter(
    "",
    "orders.dead"
  )
  val orders = Mq.RabbitMq.connectWith(options)?
  Mq.RabbitMq.Extras.declareQueue(orders, "orders.dead", Mq.RabbitMq.Extras.defaultQueueOptions())?
  Mq.RabbitMq.Extras.declareExchange(orders, "events", "topic", Mq.RabbitMq.Extras.defaultExchangeOptions())?
  Mq.RabbitMq.Extras.bindQueue(orders, "events", "order.*")?
  orders.close()
  Ok(())
}

test "the README Extras example compiles and reports an unreachable broker as an Err" {
  match readmeExtrasExample("amqp://app:secret@127.0.0.1:1/prod") {
    case Ok(_) -> assertTrue(false, "nothing listens on port 1")
    case Err(e) -> assertTrue(e.contains("rabbitmq: cannot connect to 127.0.0.1:1"), e)
  }
}

test "TlsOptions and its functions are visible through the backend" {
  val plain: TlsOptions = plainTcp()
  assertTrue(not plain.enabled, "plainTcp")
  val options = defaultOptions("127.0.0.1", 1, "orders").withTls().withTlsServerName("broker.internal")
  assertTrue(options.tls.enabled, "ConnectOptions.withTls")
  assertEqual(options.tls.serverName, "broker.internal", "ConnectOptions.withTlsServerName")
  assertEqual(options.tls.withCaCertificate("pem").caCertificatePem, "pem", "TlsOptions.withCaCertificate")
  assertTrue(not skipsVerification(options.tls), "skipsVerification")
  assertEqual(INSECURE_ALLOW_ENV, "LYRIC_TLS_ALLOW_INSECURE", "the variable's name")
}
