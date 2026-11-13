//! tests/reexport_tests.l - an application that imports only `Mq.Sqs` can
//! name the interface types and the client's configuration, because the
//! backend re-exports `Mq` and `Mq.Sqs.Client`.
@test_module
package Mq.Sqs.ReexportTests

import Std.Core
import Std.Testing
import Mq.Sqs

func describe(queue: in MessageQueue, consumer: in QueueConsumer, m: in Message): String {
  m.id + "/" + m.headers.length.toString()
}

test "Message, MessageHeader, AckMode and the interfaces are visible through the backend" {
  val m = Message(id = "m-1", body = "b", headers = [MessageHeader(key = "k", value = "v")], deliveryCount = 0)
  assertEqual(m.id, "m-1", "Message")
  assertEqual(m.headers[0].key, "k", "MessageHeader")
  val cfg = awsConfig("https://sqs.eu-west-1.amazonaws.com/123456789012/orders", "AKIDEXAMPLE", "secret").withAckMode(
    AckMode.Auto
  )
  assertEqualInt(cfg.receiveBatchSize, 1, "the backend's own names")
  match connectWith(cfg) {
    case Err(e) -> assertTrue(false, "connectWith: " + e)
    case Ok(queue) -> {
      assertEqual(describe(queue, queue, m), "m-1/1", "an SqsQueue is a MessageQueue and a QueueConsumer")
      assertEqual(queue.queue.name, "orders", "the queue")
      assertTrue(isOpen(queue), "open")
    }
  }
}
