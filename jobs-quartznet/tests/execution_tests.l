//! tests/execution_tests.l — real `Jobs.JobHandler`s run by Quartz.NET.
//!
//! Every test submits jobs through the `Jobs.JobScheduler` interface to a
//! started Quartz.NET scheduler and observes the handler running on one of
//! its worker threads: outputs, retries, timeouts, cancellation, handler
//! lookup and the scheduler lifecycle.
@test_module
package Jobs.QuartzNet.ExecutionTests

import Std.Core
import Std.Collections
import Std.Testing
import Std.String as Str
import Jobs
import Jobs.QuartzNet as Quartz
import Jobs.QuartzNet.Kernel.Dotnet as Kernel

// ─── Fixtures ─────────────────────────────────────────────────────────────────
// What handlers saw, shared between worker threads and the test thread.
protected type Log {
  var calls: Int = 0
  var lines: List[String] = newList()
  var open: Bool = false
  entry record(line: in String): Int {
    calls = calls + 1
    lines.add(line)
    return calls
  }
  entry count(): Int {
    return calls
  }
  entry line(i: in Int): String {
    return lines[i]
  }
  entry release() {
    open = true
  }
  entry released(): Bool {
    return open
  }
}

func testConfig(): Quartz.QuartzConfig {
  Quartz.defaultConfig()
}

func started(config: in Quartz.QuartzConfig): Quartz.QuartzJobScheduler {
  match Quartz.create(config) {
    case Err(e) -> panic(e)
    case Ok(s) -> {
      match Quartz.start(s) {
        case Err(e) -> panic(e)
        case Ok(_) -> s
      }
    }
  }
}

func stop(s: in Quartz.QuartzJobScheduler): Unit {
  match Quartz.shutdown(s, false) {
    case Ok(_) -> ()
    case Err(e) -> panic(e)
  }
}

func statusName(r: in Result[Jobs.JobStatus, String]): String {
  match r {
    case Ok(s) -> {
      match s {
        case Jobs.JobStatus.Pending -> return "Pending"
        case Jobs.JobStatus.Running -> return "Running"
        case Jobs.JobStatus.Succeeded -> return "Succeeded"
        case Jobs.JobStatus.Failed -> return "Failed"
        case Jobs.JobStatus.Cancelled -> return "Cancelled"
      }
    }
    case Err(e) -> return "Err(" + e + ")"
  }
}

func mustId(r: in Result[String, String]): String {
  match r {
    case Ok(id) -> id
    case Err(e) -> panic(e)
  }
}

func finished(s: in Quartz.QuartzJobScheduler, id: in String): String {
  statusName(Quartz.awaitFinished(s, id, 10000))
}

func resultsOf(s: in Quartz.QuartzJobScheduler, id: in String): slice[Jobs.JobResult] {
  match s.results(id) {
    case Ok(list) -> list.items
    case Err(e) -> panic(e)
  }
}

func waitUntilDropped(s: in Quartz.QuartzJobScheduler, id: in String, timeoutMs: in Int): Bool {
  var waited = 0
  while waited < timeoutMs {
    match Quartz.heldByQuartz(s, id) {
      case Ok(held) -> {
        if not held {
          return true
        }
      }
      case Err(e) -> panic(e)
    }
    Jobs.sleepMs(5)
    waited = waited + 5
  }
  false
}

func waitForCalls(log: in Log, n: in Int, timeoutMs: in Int): Bool {
  var waited = 0
  while log.count() < n and waited < timeoutMs {
    Jobs.sleepMs(5)
    waited = waited + 5
  }
  log.count() >= n
}

// Echoes what it was given, and says whether it ran on a thread of the
// scheduler's own pool: not the thread that built the handler, and not a
// thread of the shared .NET thread pool.
record Echo {
  log: Log
  builtOn: Int
}

func echo(log: in Log): Echo {
  Echo(log = log, builtOn = Kernel.currentThreadId())
}

func threadKind(builtOn: in Int): String {
  if Kernel.currentThreadId() == builtOn {
    return "caller"
  }
  if Kernel.onSharedThreadPool() {
    return "shared-pool"
  }
  "worker"
}

impl Jobs.JobHandler for Echo {
  func handle(spec: in Jobs.JobSpec): Result[String, Jobs.JobError] {
    val _ = self.log.record(
      spec.id + "|" + spec.name + "|" + spec.payload + "|" + spec.maxAttempts.toString() + "|" +
        spec.timeoutMs.toString() +
        "|" +
        threadKind(self.builtOn)
    )
    Ok("echo:" + spec.payload)
  }
}

// Fails its first `failures` calls, then succeeds.
record Flaky {
  log: Log
  failures: Int
}

impl Jobs.JobHandler for Flaky {
  func handle(spec: in Jobs.JobSpec): Result[String, Jobs.JobError] {
    val call = self.log.record(spec.id)
    if call <= self.failures {
      return Err(Jobs.JobError(message = "boom " + call.toString(), code = "E_FLAKY"))
    }
    Ok("ok after " + call.toString())
  }
}

record Panics {
  log: Log
}

impl Jobs.JobHandler for Panics {
  func handle(spec: in Jobs.JobSpec): Result[String, Jobs.JobError] {
    val _ = self.log.record(spec.id)
    panic("handler blew up")
  }
}

record Sleeps {
  log: Log
  ms: Int
}

impl Jobs.JobHandler for Sleeps {
  func handle(spec: in Jobs.JobSpec): Result[String, Jobs.JobError] {
    val _ = self.log.record(spec.id)
    Jobs.sleepMs(self.ms)
    Ok("slept")
  }
}

// Runs until the scheduler asks it to stop (timeout or cancel), or `ms` pass.
record Cooperative {
  log: Log
  sched: Quartz.QuartzJobScheduler
  ms: Int
}

impl Jobs.JobHandler for Cooperative {
  func handle(spec: in Jobs.JobSpec): Result[String, Jobs.JobError] {
    val _ = self.log.record(spec.id)
    val until = Jobs.nowMs() + self.ms.toLong()
    while Jobs.nowMs() < until {
      if Quartz.stopRequested(self.sched, spec.id) {
        val _ = self.log.record("stopped")
        return Ok("stopped early")
      }
      Jobs.sleepMs(5)
    }
    Ok("ran to the end")
  }
}

// Blocks until the log is released.
record Blocks {
  log: Log
}

impl Jobs.JobHandler for Blocks {
  func handle(spec: in Jobs.JobSpec): Result[String, Jobs.JobError] {
    val _ = self.log.record(spec.id)
    while not self.log.released() {
      Jobs.sleepMs(5)
    }
    Ok("unblocked")
  }
}

record Tagged {
  log: Log
  tag: String
}

impl Jobs.JobHandler for Tagged {
  func handle(spec: in Jobs.JobSpec): Result[String, Jobs.JobError] {
    val _ = self.log.record(self.tag)
    Ok(self.tag)
  }
}

// ─── Success ──────────────────────────────────────────────────────────────────
test "an enqueued job runs its handler on a Quartz.NET worker thread and succeeds" {
  val s = started(testConfig())
  val log = Log()
  Quartz.registerHandler(s, "echo", echo(log))
  val id = mustId(s.enqueue("echo", "{\"n\":7}", 3, 30000))
  assertEqual(finished(s, id), "Succeeded", "final status")
  assertEqualInt(log.count(), 1, "the handler ran exactly once")
  assertEqual(
    log.line(0),
    id + "|echo|{\"n\":7}|3|30000|worker",
    "the handler saw the spec, on a thread of the scheduler's own pool"
  )
  val results = resultsOf(s, id)
  assertEqualInt(results.length, 1, "one attempt recorded")
  val r: Jobs.JobResult = results[0]
  assertEqual(r.jobId, id, "result job id")
  assertEqual(statusName(Ok(r.status)), "Succeeded", "attempt status")
  assertEqual(r.output, "echo:{\"n\":7}", "the handler's output is recorded")
  assertEqual(r.error, "", "no error")
  assertTrue(r.startedAt > 0i64 and r.finishedAt >= r.startedAt, "attempt timestamps")
  assertEqualInt(Quartz.attemptsRun(s, id), 1, "attemptsRun")
  assertEqualInt(Quartz.timesFired(s, id), 1, "timesFired")
  // The job is removed right after it reaches its final status, on the worker thread.
  assertTrue(waitUntilDropped(s, id, 10000), "Quartz.NET drops a finished one-shot job")
  stop(s)
}

test "the convenience wrappers drive a real execution" {
  val s = started(testConfig())
  val log = Log()
  Quartz.registerHandler(s, "echo", echo(log))
  val id = mustId(Jobs.enqueue(s, "echo", "wrapped"))
  assertEqual(finished(s, id), "Succeeded", "final status")
  assertEqual(statusName(Jobs.status(s, id)), "Succeeded", "Jobs.status")
  assertEqual(resultsOf(s, id)[0].output, "echo:wrapped", "output")
  stop(s)
}

test "many jobs run concurrently on the worker pool and all succeed" {
  val s = started(testConfig().copy(workerThreads = 4))
  val gate = Log()
  Quartz.registerHandler(s, "block", Blocks(log = gate))
  val blocked: List[String] = newList()
  var i = 0
  while i < 4 {
    blocked.add(mustId(s.enqueue("block", "", 1, 30000)))
    i = i + 1
  }
  assertTrue(waitForCalls(gate, 4, 10000), "four handlers are running at the same time on four workers")
  for id in blocked {
    assertEqual(statusName(s.status(id)), "Running", "status while the handler runs")
  }
  gate.release()
  for id in blocked {
    assertEqual(finished(s, id), "Succeeded", "blocked job")
  }

  val log = Log()
  Quartz.registerHandler(s, "echo", echo(log))
  val ids: List[String] = newList()
  var n = 0
  while n < 60 {
    ids.add(mustId(s.enqueue("echo", n.toString(), 1, 30000)))
    n = n + 1
  }
  var k = 0
  while k < 60 {
    assertEqual(finished(s, ids[k]), "Succeeded", "job " + k.toString())
    assertEqual(resultsOf(s, ids[k])[0].output, "echo:" + k.toString(), "each job keeps its own output")
    k = k + 1
  }
  assertEqualInt(log.count(), 60, "every job ran exactly once")
  stop(s)
}

// ─── Failure and retries ──────────────────────────────────────────────────────
test "a failing handler is retried up to maxAttempts and the job fails" {
  val s = started(testConfig())
  val log = Log()
  Quartz.registerHandler(s, "flaky", Flaky(log = log, failures = 100))
  val id = mustId(s.enqueue("flaky", "", 3, 30000))
  assertEqual(finished(s, id), "Failed", "final status")
  assertEqualInt(log.count(), 3, "the handler ran maxAttempts times")
  assertEqualInt(Quartz.attemptsRun(s, id), 3, "attemptsRun")
  assertEqualInt(Quartz.timesFired(s, id), 1, "retries happen within one firing")
  val results = resultsOf(s, id)
  assertEqualInt(results.length, 3, "one result per attempt")
  var i = 0
  while i < 3 {
    val r: Jobs.JobResult = results[i]
    assertEqual(statusName(Ok(r.status)), "Failed", "attempt status")
    assertEqual(r.error, "boom " + (i + 1).toString(), "attempt error")
    assertEqual(r.output, "", "a failed attempt has no output")
    i = i + 1
  }
  stop(s)
}

test "a handler that fails twice then succeeds makes the job succeed on the third attempt" {
  val s = started(testConfig())
  val log = Log()
  Quartz.registerHandler(s, "flaky", Flaky(log = log, failures = 2))
  val id = mustId(s.enqueue("flaky", "", 5, 30000))
  assertEqual(finished(s, id), "Succeeded", "final status")
  assertEqualInt(log.count(), 3, "no attempt after the successful one")
  val results = resultsOf(s, id)
  assertEqualInt(results.length, 3, "one result per attempt")
  assertEqual(statusName(Ok(results[0].status)), "Failed", "first attempt")
  assertEqual(statusName(Ok(results[1].status)), "Failed", "second attempt")
  assertEqual(statusName(Ok(results[2].status)), "Succeeded", "third attempt")
  assertEqual(results[2].output, "ok after 3", "the successful attempt's output")
  stop(s)
}

test "maxAttempts 1 means no retry" {
  val s = started(testConfig())
  val log = Log()
  Quartz.registerHandler(s, "flaky", Flaky(log = log, failures = 1))
  val id = mustId(s.enqueue("flaky", "", 1, 30000))
  assertEqual(finished(s, id), "Failed", "final status")
  assertEqualInt(log.count(), 1, "one attempt only")
  stop(s)
}

test "retryDelayMs separates attempts" {
  val s = started(testConfig().copy(retryDelayMs = 200))
  val log = Log()
  Quartz.registerHandler(s, "flaky", Flaky(log = log, failures = 1))
  val id = mustId(s.enqueue("flaky", "", 2, 30000))
  assertEqual(finished(s, id), "Succeeded", "final status")
  val results = resultsOf(s, id)
  assertEqualInt(results.length, 2, "two attempts")
  val gap = results[1].startedAt - results[0].finishedAt
  assertTrue(gap >= 190i64, "the second attempt starts retryDelayMs after the first ended, gap " + gap.toString())
  stop(s)
}

test "a panicking handler is a failed attempt, retried, and does not harm the scheduler" {
  val s = started(testConfig())
  val log = Log()
  Quartz.registerHandler(s, "panics", Panics(log = log))
  Quartz.registerHandler(s, "echo", echo(Log()))
  val id = mustId(s.enqueue("panics", "", 2, 30000))
  assertEqual(finished(s, id), "Failed", "final status")
  assertEqualInt(log.count(), 2, "the panicking handler was retried")
  val r: Jobs.JobResult = resultsOf(s, id)[1]
  assertTrue(Str.contains(r.error, "handler panicked"), "error: " + r.error)
  assertTrue(Str.contains(r.error, "handler blew up"), "the panic message is kept: " + r.error)
  val next = mustId(s.enqueue("echo", "after", 1, 30000))
  assertEqual(finished(s, next), "Succeeded", "the scheduler still runs jobs")
  stop(s)
}

test "results keeps only the most recent maxResultsPerJob attempts" {
  val s = started(testConfig().copy(maxResultsPerJob = 2))
  val log = Log()
  Quartz.registerHandler(s, "flaky", Flaky(log = log, failures = 100))
  val id = mustId(s.enqueue("flaky", "", 4, 30000))
  assertEqual(finished(s, id), "Failed", "final status")
  assertEqualInt(Quartz.attemptsRun(s, id), 4, "attemptsRun counts every attempt")
  val results = resultsOf(s, id)
  assertEqualInt(results.length, 2, "two results kept")
  assertEqual(results[0].error, "boom 3", "oldest kept attempt")
  assertEqual(results[1].error, "boom 4", "newest attempt")
  stop(s)
}

// ─── Timeout ──────────────────────────────────────────────────────────────────
test "an attempt that overruns timeoutMs runs to its end and is recorded as timed out" {
  val s = started(testConfig())
  val log = Log()
  Quartz.registerHandler(s, "slow", Sleeps(log = log, ms = 400))
  val id = mustId(s.enqueue("slow", "", 2, 150))
  assertEqual(finished(s, id), "Failed", "final status")
  assertEqualInt(log.count(), 2, "each attempt started the handler")
  val results = resultsOf(s, id)
  assertEqualInt(results.length, 2, "two attempts recorded")
  for r in results {
    assertEqual(statusName(Ok(r.status)), "Failed", "attempt status")
    assertTrue(Str.contains(r.error, "exceeded timeoutMs (150ms)"), "error: " + r.error)
    assertEqual(r.output, "", "the late output is discarded")
    assertTrue(
      r.finishedAt - r.startedAt >= 390i64,
      "the handler was not stopped: the attempt lasted " + (r.finishedAt - r.startedAt).toString() + "ms"
    )
  }
  assertTrue(
    results[1].startedAt >= results[0].finishedAt,
    "the retry starts after the overrunning attempt has returned"
  )
  stop(s)
}

test "an overrunning handler holds its worker thread until it returns" {
  val s = started(testConfig().copy(workerThreads = 1))
  val log = Log()
  Quartz.registerHandler(s, "slow", Sleeps(log = log, ms = 600))
  Quartz.registerHandler(s, "echo", echo(Log()))
  val slow = mustId(s.enqueue("slow", "", 1, 100))
  assertTrue(waitForCalls(log, 1, 10000), "the slow handler is running")
  val next = mustId(s.enqueue("echo", "after", 1, 30000))
  Jobs.sleepMs(300)
  assertEqual(statusName(s.status(slow)), "Running", "past its timeout the job is still Running")
  assertEqual(statusName(s.status(next)), "Pending", "the only worker is still held")
  assertEqual(finished(s, slow), "Failed", "final status of the slow job")
  assertEqual(finished(s, next), "Succeeded", "the worker runs the next job afterwards")
  val late: Jobs.JobResult = resultsOf(s, slow)[0]
  assertTrue(Str.contains(late.error, "exceeded timeoutMs (100ms)"), "error: " + late.error)
  assertTrue(
    resultsOf(s, next)[0].startedAt >= late.finishedAt,
    "the next job started once the slow handler had returned"
  )
  stop(s)
}

test "a handler that asks stopRequested ends at its timeout and frees the worker" {
  val s = started(testConfig().copy(workerThreads = 1))
  val log = Log()
  Quartz.registerHandler(s, "coop", Cooperative(log = log, sched = s, ms = 60000))
  Quartz.registerHandler(s, "slow", Sleeps(log = Log(), ms = 50))
  val before = Jobs.nowMs()
  val id = mustId(s.enqueue("coop", "", 1, 150))
  assertEqual(finished(s, id), "Failed", "final status")
  val elapsed = Jobs.nowMs() - before
  assertTrue(elapsed >= 140i64, "not before the timeout, took " + elapsed.toString() + "ms")
  assertTrue(elapsed < 20000i64, "long before the handler's own 60 s, took " + elapsed.toString() + "ms")
  assertEqualInt(log.count(), 2, "the handler started and saw the stop request")
  val r: Jobs.JobResult = resultsOf(s, id)[0]
  assertTrue(Str.contains(r.error, "exceeded timeoutMs (150ms)"), "error: " + r.error)
  assertEqual(r.output, "", "what the handler returned is discarded")
  val next = mustId(s.enqueue("slow", "", 1, 30000))
  assertEqual(finished(s, next), "Succeeded", "the only worker runs the next job")
  stop(s)
}

test "stopRequested is false while an attempt is within its time and true for jobs that are not running" {
  val s = started(testConfig())
  val gate = Log()
  Quartz.registerHandler(s, "block", Blocks(log = gate))
  val id = mustId(s.enqueue("block", "", 1, 600000))
  assertTrue(waitForCalls(gate, 1, 10000), "the handler is running")
  assertFalse(Quartz.stopRequested(s, id), "running, in time, not cancelled")
  gate.release()
  assertEqual(finished(s, id), "Succeeded", "final status")
  assertTrue(Quartz.stopRequested(s, id), "a finished job")
  assertTrue(Quartz.stopRequested(s, "no-such-job"), "an unknown job")
  stop(s)
}

test "a handler that finishes within timeoutMs is not affected by it" {
  val s = started(testConfig())
  val log = Log()
  Quartz.registerHandler(s, "slow", Sleeps(log = log, ms = 100))
  val id = mustId(s.enqueue("slow", "", 1, 5000))
  assertEqual(finished(s, id), "Succeeded", "final status")
  assertEqual(resultsOf(s, id)[0].output, "slept", "output")
  stop(s)
}

// ─── Cancellation ─────────────────────────────────────────────────────────────
test "a job cancelled in standby never runs" {
  val log = Log()
  match Quartz.create(testConfig()) {
    case Err(e) -> assertTrue(false, e)
    case Ok(s) -> {
      Quartz.registerHandler(s, "echo", echo(log))
      val doomed = mustId(s.enqueue("echo", "doomed", 3, 30000))
      val kept = mustId(s.enqueue("echo", "kept", 3, 30000))
      match s.cancel(doomed) {
        case Ok(_) -> ()
        case Err(e) -> assertTrue(false, e)
      }
      match Quartz.heldByQuartz(s, doomed) {
        case Ok(stored) -> assertFalse(stored, "cancel removes the job from Quartz.NET")
        case Err(e) -> assertTrue(false, e)
      }
      match Quartz.start(s) {
        case Ok(_) -> ()
        case Err(e) -> assertTrue(false, e)
      }
      assertEqual(finished(s, kept), "Succeeded", "the other job runs")
      Jobs.sleepMs(300)
      assertEqualInt(log.count(), 1, "only the job that was not cancelled ran")
      assertTrue(Str.contains(log.line(0), "|kept|"), "the job that ran: " + log.line(0))
      assertEqual(statusName(s.status(doomed)), "Cancelled", "the cancelled job")
      assertEqualInt(resultsOf(s, doomed).length, 0, "a job that never ran has no attempts")
      assertEqualInt(Quartz.timesFired(s, doomed), 0, "timesFired")
      stop(s)
    }
  }
}

test "a job queued behind a busy worker can be cancelled before it runs" {
  val s = started(testConfig().copy(workerThreads = 1))
  val gate = Log()
  val log = Log()
  Quartz.registerHandler(s, "block", Blocks(log = gate))
  Quartz.registerHandler(s, "echo", echo(log))
  val blocker = mustId(s.enqueue("block", "", 1, 30000))
  assertTrue(waitForCalls(gate, 1, 10000), "the blocker holds the only worker")
  val queued = mustId(s.enqueue("echo", "queued", 1, 30000))
  assertEqual(statusName(s.status(queued)), "Pending", "queued behind the blocker")
  match s.cancel(queued) {
    case Ok(_) -> ()
    case Err(e) -> assertTrue(false, e)
  }
  gate.release()
  assertEqual(finished(s, blocker), "Succeeded", "the blocker")
  Jobs.sleepMs(300)
  assertEqualInt(log.count(), 0, "the cancelled job never ran")
  assertEqual(statusName(s.status(queued)), "Cancelled", "the cancelled job")
  stop(s)
}

test "cancelling a running job marks it Cancelled at once, lets the handler finish and stops retries" {
  val s = started(testConfig().copy(workerThreads = 1))
  val gate = Log()
  Quartz.registerHandler(s, "block", Blocks(log = gate))
  Quartz.registerHandler(s, "slow", Sleeps(log = Log(), ms = 50))
  val id = mustId(s.enqueue("block", "", 3, 600000))
  assertTrue(waitForCalls(gate, 1, 10000), "the handler is running")
  assertEqual(statusName(s.status(id)), "Running", "status while running")
  match s.cancel(id) {
    case Ok(_) -> ()
    case Err(e) -> assertTrue(false, e)
  }
  assertEqual(statusName(s.status(id)), "Cancelled", "status right after cancel")
  match Quartz.heldByQuartz(s, id) {
    case Ok(stored) -> assertFalse(stored, "cancel removes the job from Quartz.NET")
    case Err(e) -> assertTrue(false, e)
  }
  // The handler cannot be interrupted: it keeps the only worker.
  val next = mustId(s.enqueue("slow", "", 1, 30000))
  Jobs.sleepMs(300)
  assertEqualInt(Quartz.attemptsRun(s, id), 0, "the cancelled handler is still running")
  assertEqual(statusName(s.status(next)), "Pending", "the only worker is still held by the cancelled handler")
  gate.release()
  var waited = 0
  while Quartz.attemptsRun(s, id) < 1 and waited < 10000 {
    Jobs.sleepMs(5)
    waited = waited + 5
  }
  val results = resultsOf(s, id)
  assertEqualInt(results.length, 1, "the cancelled attempt is recorded")
  assertEqual(statusName(Ok(results[0].status)), "Failed", "attempt status")
  assertEqual(results[0].error, "cancelled while running", "attempt error")
  assertEqual(results[0].output, "", "what the handler returned is discarded")
  assertEqual(finished(s, next), "Succeeded", "the worker is free once the handler has returned")
  assertEqual(statusName(s.status(id)), "Cancelled", "the job stays Cancelled")
  assertEqualInt(gate.count(), 1, "no retry after cancel")
  stop(s)
}

test "a handler that asks stopRequested ends when its job is cancelled" {
  val s = started(testConfig().copy(workerThreads = 1))
  val log = Log()
  Quartz.registerHandler(s, "coop", Cooperative(log = log, sched = s, ms = 60000))
  Quartz.registerHandler(s, "slow", Sleeps(log = Log(), ms = 50))
  val id = mustId(s.enqueue("coop", "", 3, 600000))
  assertTrue(waitForCalls(log, 1, 10000), "the handler is running")
  val cancelledAt = Jobs.nowMs()
  match s.cancel(id) {
    case Ok(_) -> ()
    case Err(e) -> assertTrue(false, e)
  }
  val next = mustId(s.enqueue("slow", "", 1, 30000))
  assertEqual(statusName(Quartz.awaitFinished(s, next, 30000)), "Succeeded", "the worker runs the next job")
  val freed = Jobs.nowMs() - cancelledAt
  assertTrue(freed < 20000i64, "long before the handler's own 60 s, worker free after " + freed.toString() + "ms")
  val results = resultsOf(s, id)
  assertEqualInt(results.length, 1, "one attempt, no retry")
  assertEqual(results[0].error, "cancelled while running", "attempt error")
  assertEqual(statusName(s.status(id)), "Cancelled", "the job stays Cancelled")
  stop(s)
}

test "a cancel that arrives after the attempt's timeout leaves the attempt timed out" {
  val s = started(testConfig())
  val gate = Log()
  Quartz.registerHandler(s, "block", Blocks(log = gate))
  val id = mustId(s.enqueue("block", "", 3, 100))
  assertTrue(waitForCalls(gate, 1, 10000), "the handler is running")
  Jobs.sleepMs(400)
  match s.cancel(id) {
    case Ok(_) -> ()
    case Err(e) -> assertTrue(false, e)
  }
  gate.release()
  var waited = 0
  while Quartz.attemptsRun(s, id) < 1 and waited < 10000 {
    Jobs.sleepMs(5)
    waited = waited + 5
  }
  Jobs.sleepMs(200)
  val results = resultsOf(s, id)
  assertEqualInt(results.length, 1, "one attempt, no retry after cancel")
  assertTrue(Str.contains(results[0].error, "exceeded timeoutMs (100ms)"), "error: " + results[0].error)
  assertEqual(statusName(s.status(id)), "Cancelled", "the job is Cancelled")
  assertEqualInt(gate.count(), 1, "the handler ran once")
  stop(s)
}

test "a job cancelled between two attempts never runs its handler again" {
  val s = started(testConfig().copy(workerThreads = 1, retryDelayMs = 8000))
  val log = Log()
  Quartz.registerHandler(s, "flaky", Flaky(log = log, failures = 100))
  Quartz.registerHandler(s, "echo", echo(Log()))
  val id = mustId(s.enqueue("flaky", "", 3, 30000))
  var waited = 0
  while Quartz.attemptsRun(s, id) < 1 and waited < 10000 {
    Jobs.sleepMs(5)
    waited = waited + 5
  }
  assertEqualInt(Quartz.attemptsRun(s, id), 1, "the first attempt has failed; the firing is in its retry delay")
  val cancelledAt = Jobs.nowMs()
  match s.cancel(id) {
    case Ok(_) -> ()
    case Err(e) -> assertTrue(false, e)
  }
  assertEqual(statusName(s.status(id)), "Cancelled", "status right after cancel")
  // The only worker is free again once the cancelled firing has ended.
  val next = mustId(s.enqueue("echo", "after", 1, 30000))
  assertEqual(statusName(Quartz.awaitFinished(s, next, 30000)), "Succeeded", "the worker runs the next job")
  val freed = Jobs.nowMs() - cancelledAt
  assertTrue(freed < 6000i64, "cancel cuts the 8000ms retry delay short, worker free after " + freed.toString() + "ms")
  assertEqualInt(log.count(), 1, "the handler is not called again after cancel")
  assertEqualInt(Quartz.attemptsRun(s, id), 1, "no attempt is recorded after cancel")
  assertEqualInt(resultsOf(s, id).length, 1, "only the attempt before the cancel is recorded")
  assertEqual(statusName(s.status(id)), "Cancelled", "the job stays Cancelled")
  stop(s)
}

test "cancel is a no-op for finished and unknown jobs" {
  val s = started(testConfig())
  Quartz.registerHandler(s, "echo", echo(Log()))
  val id = mustId(s.enqueue("echo", "x", 1, 30000))
  assertEqual(finished(s, id), "Succeeded", "final status")
  match s.cancel(id) {
    case Ok(_) -> ()
    case Err(e) -> assertTrue(false, e)
  }
  assertEqual(statusName(s.status(id)), "Succeeded", "a finished job stays finished")
  match s.cancel("no-such-job") {
    case Ok(_) -> ()
    case Err(e) -> assertTrue(false, e)
  }
  stop(s)
}

// ─── Handler lookup ───────────────────────────────────────────────────────────
test "handlers are chosen by job name, with the default handler as fallback" {
  val s = started(testConfig())
  val log = Log()
  Quartz.registerHandler(s, "a", Tagged(log = log, tag = "handler-a"))
  Quartz.registerHandler(s, "b", Tagged(log = log, tag = "handler-b"))
  Quartz.registerDefaultHandler(s, Tagged(log = log, tag = "default"))
  val a = mustId(s.enqueue("a", "", 1, 30000))
  val b = mustId(s.enqueue("b", "", 1, 30000))
  val c = mustId(s.enqueue("c", "", 1, 30000))
  assertEqual(finished(s, a), "Succeeded", "a")
  assertEqual(finished(s, b), "Succeeded", "b")
  assertEqual(finished(s, c), "Succeeded", "c")
  assertEqual(resultsOf(s, a)[0].output, "handler-a", "job a ran handler a")
  assertEqual(resultsOf(s, b)[0].output, "handler-b", "job b ran handler b")
  assertEqual(resultsOf(s, c)[0].output, "default", "job c ran the default handler")

  Quartz.registerHandler(s, "a", Tagged(log = log, tag = "handler-a2"))
  val again = mustId(s.enqueue("a", "", 1, 30000))
  assertEqual(finished(s, again), "Succeeded", "a again")
  assertEqual(resultsOf(s, again)[0].output, "handler-a2", "registering again replaces the handler")

  Quartz.unregisterHandler(s, "b")
  val afterDrop = mustId(s.enqueue("b", "", 1, 30000))
  assertEqual(finished(s, afterDrop), "Succeeded", "b after unregister")
  assertEqual(resultsOf(s, afterDrop)[0].output, "default", "an unregistered name falls back to the default")
  stop(s)
}

test "a job with no handler fails with a clear error and is not retried" {
  val s = started(testConfig())
  val id = mustId(s.enqueue("orphan", "", 3, 30000))
  assertEqual(finished(s, id), "Failed", "final status")
  val results = resultsOf(s, id)
  assertEqualInt(results.length, 1, "one recorded failure")
  assertEqual(results[0].error, "no handler registered for job 'orphan'", "error")
  stop(s)
}

// ─── Lifecycle ────────────────────────────────────────────────────────────────
test "nothing fires before start, and standby pauses firing" {
  val log = Log()
  match Quartz.create(testConfig()) {
    case Err(e) -> assertTrue(false, e)
    case Ok(s) -> {
      Quartz.registerHandler(s, "echo", echo(log))
      assertFalse(Quartz.isRunning(s), "a new scheduler is in standby")
      val first = mustId(s.enqueue("echo", "1", 1, 30000))
      Jobs.sleepMs(300)
      assertEqual(statusName(s.status(first)), "Pending", "not started: the job waits")
      assertEqualInt(log.count(), 0, "not started: no handler ran")
      match Quartz.start(s) {
        case Ok(_) -> ()
        case Err(e) -> assertTrue(false, e)
      }
      assertTrue(Quartz.isRunning(s), "running after start")
      assertEqual(finished(s, first), "Succeeded", "runs once started")
      match Quartz.standby(s) {
        case Ok(_) -> ()
        case Err(e) -> assertTrue(false, e)
      }
      assertFalse(Quartz.isRunning(s), "not running in standby")
      val second = mustId(s.enqueue("echo", "2", 1, 30000))
      Jobs.sleepMs(300)
      assertEqual(statusName(s.status(second)), "Pending", "standby: the job waits")
      match Quartz.start(s) {
        case Ok(_) -> ()
        case Err(e) -> assertTrue(false, e)
      }
      assertEqual(finished(s, second), "Succeeded", "runs once resumed")
      stop(s)
    }
  }
}

test "shutdown with waitForJobs waits for the running handler" {
  val s = started(testConfig())
  val log = Log()
  Quartz.registerHandler(s, "slow", Sleeps(log = log, ms = 400))
  val id = mustId(s.enqueue("slow", "", 1, 30000))
  assertTrue(waitForCalls(log, 1, 10000), "the handler is running")
  match Quartz.shutdown(s, true) {
    case Ok(_) -> ()
    case Err(e) -> assertTrue(false, e)
  }
  assertEqual(statusName(s.status(id)), "Succeeded", "the job finished before shutdown returned")
  assertTrue(Quartz.isShutDown(s), "isShutDown")
  assertFalse(Quartz.isRunning(s), "isRunning after shutdown")
}

test "after shutdown submissions are errors, state stays readable, and shutdown is repeatable" {
  val s = started(testConfig())
  Quartz.registerHandler(s, "echo", echo(Log()))
  val done = mustId(s.enqueue("echo", "x", 1, 30000))
  assertEqual(finished(s, done), "Succeeded", "final status")
  match Quartz.standby(s) {
    case Ok(_) -> ()
    case Err(e) -> assertTrue(false, e)
  }
  val waiting = mustId(s.enqueue("echo", "y", 1, 30000))
  stop(s)
  stop(s)
  match s.enqueue("echo", "z", 1, 30000) {
    case Ok(_) -> assertTrue(false, "enqueue after shutdown must fail")
    case Err(e) -> assertTrue(Str.startsWith(e, "quartz: cannot schedule job 'echo'"), e)
  }
  match s.schedule("echo", "z", "*/5 * * * *", 1, 30000) {
    case Ok(_) -> assertTrue(false, "schedule after shutdown must fail")
    case Err(e) -> assertTrue(Str.startsWith(e, "quartz: cannot schedule job 'echo'"), e)
  }
  match Quartz.start(s) {
    case Ok(_) -> assertTrue(false, "start after shutdown must fail")
    case Err(e) -> assertTrue(Str.startsWith(e, "quartz: cannot start scheduler"), e)
  }
  assertEqual(statusName(s.status(done)), "Succeeded", "status is readable after shutdown")
  assertEqual(resultsOf(s, done)[0].output, "echo:x", "results are readable after shutdown")
  match s.cancel(waiting) {
    case Ok(_) -> ()
    case Err(e) -> assertTrue(false, "cancel after shutdown: " + e)
  }
  assertEqual(statusName(s.status(waiting)), "Cancelled", "a job that never ran can still be cancelled")
}

test "a scheduler name can be used once per process until it is shut down" {
  val config = testConfig().copy(instanceName = "lyric-quartznet-name-test")
  match Quartz.create(config) {
    case Err(e) -> assertTrue(false, e)
    case Ok(first) -> {
      match Quartz.create(config) {
        case Ok(_) -> assertTrue(false, "a second scheduler with the same name must be rejected")
        case Err(e) -> assertTrue(Str.contains(e, "already exists"), e)
      }
      stop(first)
      match Quartz.create(config) {
        case Err(e) -> assertTrue(false, "the name is free again after shutdown: " + e)
        case Ok(second) -> stop(second)
      }
    }
  }
}

test "purgeFinished forgets finished jobs only" {
  val s = started(testConfig())
  val gate = Log()
  Quartz.registerHandler(s, "echo", echo(Log()))
  Quartz.registerHandler(s, "block", Blocks(log = gate))
  val done = mustId(s.enqueue("echo", "x", 1, 30000))
  val cancelled = mustId(s.enqueue("orphan", "", 1, 30000))
  assertEqual(finished(s, done), "Succeeded", "done")
  assertEqual(finished(s, cancelled), "Failed", "failed")
  val running = mustId(s.enqueue("block", "", 1, 30000))
  assertTrue(waitForCalls(gate, 1, 10000), "the blocker is running")
  assertEqualInt(Quartz.purgeFinished(s), 2, "two finished jobs dropped")
  assertEqual(statusName(s.status(done)), "Err(job not found: " + done + ")", "a purged job is unknown")
  assertEqual(statusName(s.status(running)), "Running", "a running job is kept")
  gate.release()
  assertEqual(finished(s, running), "Succeeded", "the running job finishes")
  stop(s)
}
