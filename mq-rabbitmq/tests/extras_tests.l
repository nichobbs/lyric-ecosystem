//! tests/extras_tests.l - the queue-argument builders and publish options of
//! `Mq.RabbitMq.Extras`, without a broker.
@test_module
package Mq.RabbitMq.ExtrasTests

import Std.Core
import Std.Testing
import Mq
import Mq.RabbitMq as RabbitMq
import Mq.RabbitMq.Extras
import Mq.RabbitMq.Extras as Extras
import Mq.RabbitMq.Amqp as Amqp

func base(): RabbitMq.ConnectOptions {
  RabbitMq.defaultOptions("127.0.0.1", 5672, "orders")
}

func argument(o: in RabbitMq.ConnectOptions, name: in String): String {
  match Amqp.tableGet(o.queueArguments, name) {
    case Some(v) -> RabbitMq.fieldText(v)
    case None -> "<absent>"
  }
}

func tagOf(o: in RabbitMq.ConnectOptions, name: in String): Int {
  match Amqp.tableGet(o.queueArguments, name) {
    case Some(FInteger(tag, _)) -> tag
    case Some(FString(_)) -> 83
    case _ -> 0
  }
}

test "the defaults declare a queue without arguments" {
  assertEqualInt(base().queueArguments.length, 0, "no arguments")
}

test "withQuorumQueue sets x-queue-type and makes the queue durable" {
  val o = base().copy(durable = false).withQuorumQueue()
  assertEqual(argument(o, "x-queue-type"), "quorum", "x-queue-type")
  assertEqualInt(tagOf(o, "x-queue-type"), 83, "as a long string")
  assertTrue(o.durable, "a quorum queue is always durable")
}

test "withDeadLetter sets the exchange and, when given, the routing key" {
  val o = base().withDeadLetter("dlx", "orders.dead")
  assertEqual(argument(o, "x-dead-letter-exchange"), "dlx", "exchange")
  assertEqual(argument(o, "x-dead-letter-routing-key"), "orders.dead", "routing key")
  val direct = base().withDeadLetter("", "orders.dead")
  assertEqual(argument(direct, "x-dead-letter-exchange"), "", "the default exchange is an empty name")
  assertEqualInt(direct.queueArguments.length, 2, "both arguments are sent")
  val keep = base().withDeadLetter("dlx", "")
  assertEqual(argument(keep, "x-dead-letter-routing-key"), "<absent>", "no routing key keeps the message's own")
}

test "the numeric arguments are sent as integers" {
  val o = base().withMessageTtl(60000i64).withQueueExpiry(3600000i64).withMaxLength(1000i64).withMaxPriority(9).withDeliveryLimit(
    5
  )
  assertEqual(argument(o, "x-message-ttl"), "60000", "x-message-ttl")
  assertEqualInt(tagOf(o, "x-message-ttl"), 108, "a 64-bit integer")
  assertEqual(argument(o, "x-expires"), "3600000", "x-expires")
  assertEqual(argument(o, "x-max-length"), "1000", "x-max-length")
  assertEqual(argument(o, "x-max-priority"), "9", "x-max-priority")
  assertEqualInt(tagOf(o, "x-max-priority"), 73, "a 32-bit integer")
  assertEqual(argument(o, "x-delivery-limit"), "5", "x-delivery-limit")
  assertEqualInt(o.queueArguments.length, 5, "five arguments")
  assertTrue(isOk(RabbitMq.validateOptions(o)), "the arguments encode")
}

test "setting an argument again replaces it in place" {
  val o = base().withMessageTtl(1i64).withMaxLength(5i64).withMessageTtl(2i64)
  assertEqualInt(o.queueArguments.length, 2, "no duplicate")
  assertEqual(o.queueArguments[0].name, "x-message-ttl", "it keeps its position")
  assertEqual(argument(o, "x-message-ttl"), "2", "with the new value")
  val custom = base().withQueueArgument("x-single-active-consumer", Amqp.FBool(value = true))
  assertEqual(argument(custom, "x-single-active-consumer"), "true", "any other argument by name")
}

test "the builders refuse values the broker would refuse" {
  assertPanics("a negative TTL", { -> ignore(base().withMessageTtl(-1i64)) })
  assertPanics("a zero expiry", { -> ignore(base().withQueueExpiry(0i64)) })
  assertPanics("a priority range of 0", { -> ignore(base().withMaxPriority(0)) })
  assertPanics("a priority range over 255", { -> ignore(base().withMaxPriority(256)) })
  assertPanics("a negative delivery limit", { -> ignore(base().withDeliveryLimit(-1)) })
  assertPanics("an argument without a name", { -> ignore(base().withQueueArgument("", Amqp.FVoid)) })
}

func ignore(o: in RabbitMq.ConnectOptions): Unit {
  ()
}

test "the default exchange and queue options are durable" {
  val e = Extras.defaultExchangeOptions()
  assertTrue(e.durable and not e.autoDelete and not e.isInternal, "a durable exchange")
  assertEqualInt(e.arguments.length, 0, "without arguments")
  val q = Extras.defaultQueueOptions()
  assertTrue(q.durable and not q.autoDelete, "a durable queue")
}
