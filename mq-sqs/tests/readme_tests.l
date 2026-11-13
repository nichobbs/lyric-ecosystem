//! tests/readme_tests.l - the usage examples of README.md, verbatim, so the
//! README cannot drift from what compiles.
@test_module
package Mq.Sqs.ReadmeTests

import Std.Core
import Std.Testing
import Mq
import Mq.Sqs as Sqs
import Mq.Sqs.Extras as SqsExtras

func handle(m: in Mq.Message): Result[Unit, String] {
  println(m.id + ": " + m.body)
  Ok(())
}

func example(queueUrl: in String): Result[Unit, String] {
  val cfg = match Sqs.configFromEnv(queueUrl) { case Ok(c) -> c; case Err(e) -> return Err(e.text()) }
  val queue = Sqs.connectWith(cfg)?
  defer {
    queue.close()
  }
  Mq.publish(queue, Mq.Message(id = "order-42", body = "{\"total\":12}", headers = [Mq.MessageHeader(key = "content-type", value = "application/json")], deliveryCount = 0))?
  Mq.consume(queue, 5000, { m -> handle(m) })?
  Mq.ack(queue, "order-42")?
  Ok(())
}

func provision(cfg: in Sqs.SqsConfig): Result[Sqs.SqsQueue, String] {
  val client = match Sqs.openClient(cfg) { case Ok(c) -> c; case Err(e) -> return Err(e.text()) }
  val deadUrl = match SqsExtras.createQueue(client, "orders-dead.fifo", SqsExtras.fifoQueue()) {
    case Ok(url) -> url
    case Err(e) -> return Err(e.text())
  }
  val deadArn = match SqsExtras.queueArn(client, deadUrl) { case Ok(arn) -> arn; case Err(e) -> return Err(e.text()) }
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

test "the README examples compile and fail cleanly without a server" {
  val cfg = Sqs.endpointConfig("http://127.0.0.1:1", "http://127.0.0.1:1/000000000000/orders", "elasticmq", "x", "x").withRequestTimeout(
    2000
  )
  assertTrue(isErr(provision(cfg)), "nothing listens there")
}
