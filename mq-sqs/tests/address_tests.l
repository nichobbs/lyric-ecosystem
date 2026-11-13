//! tests/address_tests.l - `Mq.Sqs.Address`: endpoints and queue URLs.
@test_module
package Mq.Sqs.AddressTests

import Std.Core
import Std.String as Str
import Std.Testing
import Mq.Sqs.Address

func endpointText(raw: in String): String {
  match parseEndpoint(raw) {
    case Ok(e) -> e.origin()
    case Err(message) -> "Err: " + message
  }
}

func queueText(raw: in String): String {
  match parseQueueUrl(raw) {
    case Ok(q) -> q.url + " | " + q.endpoint.authority + " | " + q.path + " | " + q.name + " | " +
      (if q.fifo { "fifo" } else { "standard" }) +
      " | " +
      q.region
    case Err(message) -> "Err: " + message
  }
}

test "an endpoint is scheme://host[:port], lowercased, without its default port" {
  assertEqual(endpointText("https://sqs.eu-west-1.amazonaws.com"), "https://sqs.eu-west-1.amazonaws.com", "AWS")
  assertEqual(
    endpointText("https://SQS.EU-West-1.amazonaws.com:443/"),
    "https://sqs.eu-west-1.amazonaws.com",
    "normalised"
  )
  assertEqual(endpointText("http://127.0.0.1:9324"), "http://127.0.0.1:9324", "loopback over http")
  assertEqual(endpointText("http://localhost:80"), "http://localhost", "the http default port is dropped")
  assertEqual(endpointText("http://[::1]:9324/"), "http://[::1]:9324", "IPv6 loopback")
}

test "an endpoint that could leak a signed request is refused" {
  assertEqual(
    endpointText("http://sqs.internal:9324"),
    "Err: SQS endpoint must use https:// (http:// is allowed only for localhost, 127.0.0.1 and [::1]): http://sqs.internal:9324",
    "http off loopback"
  )
  assertEqual(
    endpointText("http://127.0.0.1.evil.example"),
    "Err: SQS endpoint must use https:// (http:// is allowed only for localhost, 127.0.0.1 and [::1]): http://127.0.0.1.evil.example",
    "a host that only starts like loopback"
  )
  assertEqual(
    endpointText("sqs.eu-west-1.amazonaws.com"),
    "Err: SQS endpoint must be an absolute http(s) URL: sqs.eu-west-1.amazonaws.com",
    "no scheme"
  )
  assertEqual(
    endpointText("ftp://host"),
    "Err: SQS endpoint must be an absolute http(s) URL: ftp://host",
    "another scheme"
  )
  assertEqual(endpointText("https://"), "Err: SQS endpoint has no host: https://", "no host")
  assertEqual(
    endpointText("https://host/path"),
    "Err: SQS endpoint must be scheme://host[:port] with no path: https://host/path",
    "a path"
  )
  assertEqual(
    endpointText("https://user:secret@host"),
    "Err: SQS endpoint must not carry credentials, a query or a fragment: https://user:secret@host",
    "credentials"
  )
  assertEqual(
    endpointText("https://host?x=1"),
    "Err: SQS endpoint must not carry credentials, a query or a fragment: https://host?x=1",
    "a query"
  )
}

test "a queue URL splits into endpoint, path, name, kind and region" {
  assertEqual(
    queueText("https://sqs.eu-west-1.amazonaws.com/123456789012/orders"),
    "https://sqs.eu-west-1.amazonaws.com/123456789012/orders | sqs.eu-west-1.amazonaws.com | /123456789012/orders | orders | standard | eu-west-1",
    "a standard queue on AWS"
  )
  assertEqual(
    queueText("https://sqs.us-east-2.amazonaws.com:443/123456789012/orders.fifo/"),
    "https://sqs.us-east-2.amazonaws.com/123456789012/orders.fifo | sqs.us-east-2.amazonaws.com | /123456789012/orders.fifo | orders.fifo | fifo | us-east-2",
    "a FIFO queue, default port and trailing slash dropped"
  )
  assertEqual(
    queueText("http://127.0.0.1:9324/000000000000/q_1-a"),
    "http://127.0.0.1:9324/000000000000/q_1-a | 127.0.0.1:9324 | /000000000000/q_1-a | q_1-a | standard | ",
    "a local server names no region"
  )
  assertEqual(
    queueText("http://localhost:9324/queue/jobs"),
    "http://localhost:9324/queue/jobs | localhost:9324 | /queue/jobs | jobs | standard | ",
    "the ElasticMQ path form"
  )
}

test "a malformed queue URL is refused with the reason" {
  assertEqual(queueText("orders"), "Err: SQS queue URL must be an absolute http(s) URL: orders", "a bare name")
  assertEqual(
    queueText("https://sqs.eu-west-1.amazonaws.com"),
    "Err: SQS queue URL has no queue path: https://sqs.eu-west-1.amazonaws.com",
    "no path"
  )
  assertEqual(
    queueText("https://sqs.eu-west-1.amazonaws.com/"),
    "Err: SQS queue URL does not end in a queue name: https://sqs.eu-west-1.amazonaws.com/",
    "an empty path"
  )
  assertEqual(
    queueText("http://sqs.internal/1/q"),
    "Err: SQS queue URL must use https:// (http:// is allowed only for localhost, 127.0.0.1 and [::1]): http://sqs.internal/1/q",
    "http off loopback"
  )
  assertEqual(
    queueText("https://h/1/q?Action=SendMessage"),
    "Err: SQS queue URL must not carry a query, a fragment or encoded characters: https://h/1/q?Action=SendMessage",
    "a query"
  )
  assertEqual(
    queueText("https://h/1/a%20b"),
    "Err: SQS queue URL must not carry a query, a fragment or encoded characters: https://h/1/a%20b",
    "an encoded character"
  )
  assertEqual(
    queueText("https://u:p@h/1/q"),
    "Err: SQS queue URL must not carry credentials, a query or a fragment: https://u:p@h/1/q",
    "credentials"
  )
  assertEqual(
    queueText("https://h/1/.fifo"),
    "Err: SQS queue URL does not end in a queue name: https://h/1/.fifo",
    "only a suffix"
  )
}

test "the region is read from AWS host names only" {
  assertEqual(regionOfHost("sqs.eu-west-1.amazonaws.com"), "eu-west-1", "the regional endpoint")
  assertEqual(regionOfHost("sqs-fips.us-gov-west-1.amazonaws.com"), "us-gov-west-1", "FIPS")
  assertEqual(regionOfHost("sqs.cn-north-1.amazonaws.com.cn"), "cn-north-1", "China")
  assertEqual(regionOfHost("sqs.eu-west-1.api.aws"), "eu-west-1", "dual stack")
  assertEqual(regionOfHost("eu-west-1.queue.amazonaws.com"), "eu-west-1", "legacy")
  assertEqual(regionOfHost("sqs.eu-west-1.amazonaws.com:443"), "eu-west-1", "with a port")
  assertEqual(regionOfHost("127.0.0.1:9324"), "", "an address")
  assertEqual(regionOfHost("sqs.eu-west-1.example.com"), "", "another domain")
  assertEqual(regionOfHost("s3.eu-west-1.amazonaws.com"), "", "another service")
}

test "queue names and regions are validated before they are built into anything" {
  assertTrue(isValidQueueName("orders"), "letters")
  assertTrue(isValidQueueName("Orders_2-b.fifo"), "FIFO suffix")
  assertTrue(not isValidQueueName(""), "empty")
  assertTrue(not isValidQueueName(".fifo"), "suffix only")
  assertTrue(not isValidQueueName("a b"), "a space")
  assertTrue(not isValidQueueName("a/b"), "a slash")
  assertTrue(not isValidQueueName("a.b"), "a dot that is not the suffix")
  assertTrue(isValidQueueName(Str.repeat("a", 80)), "80 characters")
  assertTrue(not isValidQueueName(Str.repeat("a", 81)), "81 characters")
  assertTrue(isValidRegion("us-east-1"), "a region")
  assertTrue(isValidRegion("elasticmq"), "a local region name")
  assertTrue(not isValidRegion(""), "empty")
  assertTrue(not isValidRegion("us-east-1/sqs"), "a slash would alter the credential scope")
  assertTrue(not isValidRegion("US-EAST-1"), "uppercase")
  assertTrue(isLoopbackAuthority("localhost:9324"), "localhost")
  assertTrue(isLoopbackAuthority("[::1]"), "IPv6")
  assertTrue(not isLoopbackAuthority("localhost.example.com"), "a longer name")
}
