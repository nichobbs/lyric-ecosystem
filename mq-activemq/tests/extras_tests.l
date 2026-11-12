//! tests/extras_tests.l - the option validation and selector helpers of
//! `Mq.ActiveMq.Extras`. What the broker does with the options is covered by
//! the integration suite.
@test_module
package Mq.ActiveMq.ExtrasTests

import Std.Core
import Std.Testing
import Mq.ActiveMq.Extras as Extras

test "destinations" {
  assertEqual(Extras.queueDestination("orders"), "/queue/orders", "queue")
  assertEqual(Extras.topicDestination("prices.eur"), "/topic/prices.eur", "topic")
}

test "default send options" {
  val o = Extras.defaultSendOptions()
  assertTrue(o.persistent, "persistent")
  assertEqualInt(o.priority, -1, "broker default priority")
  assertTrue(o.timeToLiveMs == 0i64, "no expiry")
  assertEqualInt(o.properties.length, 0, "no properties")
}

test "property names" {
  assertTrue(Extras.isPropertyName("color"), "a plain identifier")
  assertTrue(Extras.isPropertyName("_tenant$1"), "underscore, dollar and digits")
  assertFalse(Extras.isPropertyName(""), "empty")
  assertFalse(Extras.isPropertyName("1st"), "starts with a digit")
  assertFalse(Extras.isPropertyName("content-type"), "a hyphen")
  assertFalse(Extras.isPropertyName("a b"), "a space")
  assertFalse(Extras.isPropertyName("naïve"), "non-ASCII")
  assertFalse(Extras.isPropertyName("JMSPriority"), "a JMS header")
  assertFalse(Extras.isPropertyName("lyric_id"), "one of the client's own headers")
  assertFalse(Extras.isPropertyName("like"), "a selector keyword")
  assertFalse(Extras.isPropertyName("Between"), "a selector keyword in any case")
}

test "selectors quote their value" {
  assertEqual(Extras.selectorEquals("color", "red"), "color = 'red'", "plain")
  assertEqual(Extras.selectorEquals("owner", "o'brien"), "owner = 'o''brien'", "an apostrophe is doubled")
  assertEqual(Extras.selectorForId("m-1"), "lyric_id = 'm-1'", "by message id")
}
