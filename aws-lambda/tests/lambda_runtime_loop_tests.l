//! tests/lambda_runtime_loop_tests.l — mock Runtime API server exercising
//! `Lambda.Dispatch`'s real HTTP transport (`pollAndDispatchOnce`) end to
//! end over a live loopback connection: a synthetic event is served from
//! the mock's `GET .../invocation/next` endpoint, and the dispatch loop's
//! `POST .../invocation/{id}/response` (or `/error`) is captured and
//! asserted — the concrete "local mock-server test" that `lambda_tests.l`
//! and `lambda_dispatch_tests.l` (deliberately pure / no I/O) leave open,
//! and the gap PR #5135 investigated and found no server-lifecycle test
//! infra existed anywhere in the repo to build this on.
//!
//! ## Concurrency
//!
//! The mock server and the client dispatch loop run in one process and need
//! two genuinely concurrent pieces of work. `spawn` is only concurrent on
//! `--target jvm` (a virtual-thread `ExecutorService`); on `--target dotnet`
//! it runs the spawned expression inline, which would deadlock here, so the
//! dotnet variant of `runOneEventThroughMockRuntimeApi` starts the mock on a
//! `System.Threading.Tasks.Task` through an `extern type` declared in this
//! test file, as `aws-credentials`, `mq-sqs` and `storage-s3`'s fake servers
//! do. The assertions run on the test thread, after the mock has stopped.
//! Each test uses a port of its own: on dotnet a stopped listener is not
//! released at once, and a second test on the same port would wait for
//! requests that never reach its server.
@test_module
package Lambda.RuntimeLoopTests

import Std.Core
import Std.Testing
import Std.Collections
import Std.HttpServer
import Std.Encoding
import Std.Time
import Lambda
import Lambda.Direct
import Lambda.Dispatch
import Lambda.Events

// A fixed, distinct loopback port (see the other lyric-* test suites'
// fixed-port precedent, e.g. lyric-stdlib/tests/http_server_dotnet_tests.l's
// 19401-19417 range) — `Std.HttpServer.startListener` requires a nonzero
// port (no OS-assigned-ephemeral-port support), so a fixed value it is.
val mockRuntimeApiPort: Int = 19430

// ─── Fixture handlers ─────────────────────────────────────────────────────
record CaptureSqsHandler {
}

impl Lambda.SqsHandler for CaptureSqsHandler {
  func handle(event: in SqsEvent, ctx: in LambdaContext): Result[Unit, LambdaError] {
    assertEqualInt(event.records.length, 1, "mock server's SQS event has one record")
    assertEqual(
      event.records[0].body,
      "hello from the mock runtime API",
      "SQS record body decoded from the live HTTP round trip"
    )
    assertEqual(
      ctx.requestId,
      "req-mock-1",
      "LambdaContext.requestId built from the mock's Lambda-Runtime-Aws-Request-Id header"
    )
    return Ok(value = ())
  }
}

func sqsMockEventBody(): String {
  return "{\"Records\":[{\"messageId\":\"m1\",\"receiptHandle\":\"rh1\",\"body\":\"hello from the mock runtime API\"," +
    "\"attributes\":{},\"messageAttributes\":{},\"md5OfBody\":\"x\",\"eventSource\":\"aws:sqs\"," +
    "\"eventSourceARN\":\"arn:aws:sqs:us-east-1:123456789012:mock-queue\",\"awsRegion\":\"us-east-1\"}]}"
}

// ─── Mock Runtime API: exactly one GET-next then one POST-response(-or-error) ─
record MockCapturedRequest {
  path: String
  body: String
}

// Services exactly the two HTTP requests one `pollAndDispatchOnce` cycle
// makes against `runtimeApi` -- the GET to `.../invocation/next` (answered
// with `eventBody` plus the headers `getNextInvocation` reads) and the
// following POST to `.../invocation/{id}/response` or `/error` (captured
// verbatim, not distinguished here -- the test asserts on the captured path
// instead).
func mockRuntimeApiCycle(listener: in HttpListener, eventBody: in String): MockCapturedRequest {
  val nextCtx = nextContext(listener)
  val nextHeaders: Map[String, String] = newMap()
  nextHeaders.add("Lambda-Runtime-Aws-Request-Id", "req-mock-1")
  nextHeaders.add("Lambda-Runtime-Deadline-Ms", (nowEpochMillis() + 30000i64).toString())
  nextHeaders.add("Lambda-Runtime-Invoked-Function-Arn", "arn:aws:lambda:us-east-1:123456789012:function:mock-fn")
  respondBytesWithHeaders(nextCtx, 200, "application/json", encodeUtf8(eventBody), nextHeaders)

  val postCtx = nextContext(listener)
  val captured = MockCapturedRequest(path = requestPath(postCtx), body = requestBody(postCtx))
  respondText(postCtx, 202, "")
  return captured
}

record MockRunOutcome {
  dispatchOk: Bool
  dispatchErr: String
  captured: MockCapturedRequest
}

ref record CaptureSlot {
  var captured: MockCapturedRequest
}

func outcomeOf(dispatchResult: in Result[Unit, String], captured: in MockCapturedRequest): MockRunOutcome {
  match dispatchResult {
    case Ok(_) -> MockRunOutcome(dispatchOk = true, dispatchErr = "", captured = captured)
    case Err(e) -> MockRunOutcome(dispatchOk = false, dispatchErr = e, captured = captured)
  }
}

@cfg(target = "dotnet")
extern type WorkerTask = "System.Threading.Tasks.Task"

@cfg(target = "dotnet")
@externStatic
@externTarget("System.Threading.Tasks.Task.Run")
func taskRun(work: in () -> Unit): WorkerTask = ()

@cfg(target = "dotnet")
@externInstance
@externTarget("System.Threading.Tasks.Task.Wait")
func taskWait(t: in WorkerTask): Unit = ()

func serveInto(slot: in CaptureSlot, listener: in HttpListener, eventBody: in String): Unit {
  slot.captured = mockRuntimeApiCycle(listener, eventBody)
}

@cfg(target = "dotnet")
func runOneEventThroughMockRuntimeApi(port: in Int, eventBody: in String, app: in LambdaApp): MockRunOutcome {
  val listener = startListener("http://127.0.0.1:" + port.toString() + "/")
  val slot = CaptureSlot(captured = MockCapturedRequest(path = "", body = ""))
  val server = taskRun({ -> serveInto(slot, listener, eventBody) })
  val dispatchResult = pollAndDispatchOnce("127.0.0.1:" + port.toString(), app)
  taskWait(server)
  stopListener(listener)
  outcomeOf(dispatchResult, slot.captured)
}

@cfg(target = "jvm")
func runOneEventThroughMockRuntimeApi(port: in Int, eventBody: in String, app: in LambdaApp): MockRunOutcome {
  val listener = startListener("http://127.0.0.1:" + port.toString() + "/")
  var outcome = outcomeOf(Ok(()), MockCapturedRequest(path = "", body = ""))
  scope {
    val serverTask = spawn mockRuntimeApiCycle(listener, eventBody)
    val dispatchResult = pollAndDispatchOnce("127.0.0.1:" + port.toString(), app)
    val captured = await serverTask
    outcome = outcomeOf(dispatchResult, captured)
  }
  stopListener(listener)
  outcome
}

// ─── Tests ────────────────────────────────────────────────────────────────
test "pollAndDispatchOnce: aws:sqs event round-trips through a live mock Runtime API server" {
  val app = Lambda.withDirect(Lambda.newApp(), Lambda.Direct.sqsHandler(CaptureSqsHandler()))
  val outcome = runOneEventThroughMockRuntimeApi(mockRuntimeApiPort, sqsMockEventBody(), app)
  assertTrue(outcome.dispatchOk, "pollAndDispatchOnce reported the transport itself as healthy: " + outcome.dispatchErr)
  assertEqual(
    outcome.captured.path,
    "/2018-06-01/runtime/invocation/req-mock-1/response",
    "posted the batch-success response to the /response endpoint, keyed by the request id captured from /next"
  )
  assertEqual(outcome.captured.body, "null", "whole-batch success response body")
}

test "pollAndDispatchOnce: a handler failure posts to the mock's /error endpoint but the transport still reports healthy" {
  // No RawDirect handler registered: dispatchInvocation returns
  // Err(InternalError(...)) for the raw catch-all, which
  // pollAndDispatchOnce posts to /error (not /response) and still returns
  // Ok(Unit) itself -- a handler-level failure is not a transport failure
  // (see pollAndDispatchOnce's own doc comment in dispatch.l).
  val app = Lambda.newApp()
  val outcome = runOneEventThroughMockRuntimeApi(mockRuntimeApiPort + 1, "{\"unrecognised\":true}", app)
  assertTrue(outcome.dispatchOk, "a handler-level failure must not surface as a transport Err: " + outcome.dispatchErr)
  assertEqual(
    outcome.captured.path,
    "/2018-06-01/runtime/invocation/req-mock-1/error",
    "posted to the /error endpoint (not /response) when no handler is registered"
  )
  assertTrue(outcome.captured.body.contains("\"errorType\":\"InternalError\""), "AWS error JSON shape posted to /error")
  assertTrue(
    outcome.captured.body.contains("no handler registered for raw"),
    "error message names the missing raw handler"
  )
}
