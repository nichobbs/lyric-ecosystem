//! tests/reexport_tests.l - an application that imports only `Mq.ActiveMq`
//! can name the interface types, because the backend re-exports `Mq`.
@test_module
package Mq.ActiveMq.ReexportTests

import Std.Core
import Std.Testing
import Mq.ActiveMq

func describe(queue: in MessageQueue, m: in Message): String {
  m.id + "/" + m.headers.length.toString()
}

test "Message, MessageHeader and the interfaces are visible through the backend" {
  val m = Message(id = "m-1", body = "b", headers = [MessageHeader(key = "k", value = "v")], deliveryCount = 0)
  assertEqual(m.id, "m-1", "Message")
  assertEqual(m.headers[0].key, "k", "MessageHeader")
  val options = defaultOptions("127.0.0.1", 61613, resolveDestination("orders"))
  assertEqual(options.destination, "/queue/orders", "the backend's own names")
  assertTrue(options.persistent, "persistent by default")
}
