//! tests/execution_tests.l — real `Jobs.JobHandler`s run by Hangfire.
//!
//! Every test submits jobs through the `Jobs.JobScheduler` interface to a
//! scheduler on an in-memory Hangfire storage and observes the handler
//! running on a worker thread of its `BackgroundJobServer`: outputs, retries,
//! timeouts, cancellation, handler lookup, queues, shared storages and the
//! scheduler lifecycle.
@test_module
package Jobs.Hangfire.ExecutionTests

import Std.Core
import Std.Collections
import Std.Testing
import Std.String as Str
import Jobs
import Jobs.Hangfire as Hangfire
import Jobs.Hangfire.Native as Native
import Jobs.Hangfire.Kernel.Dotnet as Kernel

// ─── Fixtures ─────────────────────────────────────────────────────────────────
// What handlers saw, shared between worker threads and the test thread.
protected type Log {
  var calls: Int = 0
  var lines: List[String] = newList()
  var open: Bool = false
  var busy: Int = 0
  var peak: Int = 0
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
  entry enter() {
    busy = busy + 1
    if busy > peak {
      peak = busy
    }
  }
  entry leave() {
    busy = busy - 1
  }
  entry most(): Int {
    return peak
  }
}

func memory(): Hangfire.HangfireStorage {
  match Hangfire.inMemoryStorage() {
    case Ok(st) -> st
    case Err(e) -> panic(e)
  }
}

func testConfig(): Hangfire.HangfireConfig {
  Hangfire.defaultConfig().copy(schedulePollingIntervalMs = 100)
}

func client(config: in Hangfire.HangfireConfig, storage: in Hangfire.HangfireStorage): Hangfire.HangfireJobScheduler {
  match Hangfire.create(config, storage) {
    case Ok(s) -> s
    case Err(e) -> panic(e)
  }
}

func begin(s: in Hangfire.HangfireJobScheduler): Unit {
  match Hangfire.start(s) {
    case Ok(_) -> ()
    case Err(e) -> panic(e)
  }
}

func started(config: in Hangfire.HangfireConfig): Hangfire.HangfireJobScheduler {
  val s = client(config, memory())
  begin(s)
  s
}

func stop(s: in Hangfire.HangfireJobScheduler): Unit {
  match Hangfire.shutdown(s, true) {
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

func submit(
  s: in Hangfire.HangfireJobScheduler,
  name: in String,
  payload: in String,
  attempts: in Int,
  timeoutMs: in Int,
): String {
  match s.enqueue(name, payload, attempts, timeoutMs) {
    case Ok(id) -> id
    case Err(e) -> panic(e)
  }
}

func finished(s: in Hangfire.HangfireJobScheduler, id: in String): String {
  statusName(Hangfire.awaitFinished(s, id, 30000))
}

func attemptsOf(s: in Hangfire.HangfireJobScheduler, id: in String): slice[Jobs.JobResult] {
  match s.results(id) {
    case Ok(list) -> list.items
    case Err(e) -> panic(e)
  }
}

func hangfireState(s: in Hangfire.HangfireJobScheduler, id: in String): String {
  match Native.hangfireState(s, id) {
    case Ok(name) -> name
    case Err(e) -> "Err(" + e + ")"
  }
}

// Wait until `cond` holds, at most `ms`.
func eventually(ms: in Int, cond: in () -> Bool): Bool {
  var waited = 0
  while waited < ms {
    if cond() {
      return true
    }
    Jobs.sleepMs(10)
    waited = waited + 10
  }
  cond()
}

// ─── Handlers ─────────────────────────────────────────────────────────────────
record Echo {
  log: Log
}

impl Jobs.JobHandler for Echo {
  func handle(spec: in Jobs.JobSpec): Result[String, Jobs.JobError] {
    val _ = self.log.record(
      spec.id + "~" + spec.name + "~" + spec.payload + "~" + spec.maxAttempts.toString() + "~" +
        spec.timeoutMs.toString() +
        "~" +
        spec.scheduledAt.toString() +
        "~" +
        Kernel.currentThreadId().toString()
    )
    Ok("echo:" + spec.payload)
  }
}

// Fails its first `failures` calls.
record Flaky {
  log: Log
  failures: Int
}

impl Jobs.JobHandler for Flaky {
  func handle(spec: in Jobs.JobSpec): Result[String, Jobs.JobError] {
    val n = self.log.record(spec.id)
    if n <= self.failures {
      return Err(Jobs.JobError(message = "boom " + n.toString(), code = "E_BOOM"))
    }
    Ok("recovered on call " + n.toString())
  }
}

record Panics {
  log: Log
}

impl Jobs.JobHandler for Panics {
  func handle(spec: in Jobs.JobSpec): Result[String, Jobs.JobError] {
    val _ = self.log.record(spec.id)
    panic("kaboom")
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

// Runs until the log is released.
record Blocks {
  log: Log
}

impl Jobs.JobHandler for Blocks {
  func handle(spec: in Jobs.JobSpec): Result[String, Jobs.JobError] {
    val _ = self.log.record(spec.id)
    self.log.enter()
    while not self.log.released() {
      Jobs.sleepMs(5)
    }
    self.log.leave()
    Ok("released")
  }
}

// Runs until `stopRequested`, at most 20 s.
record UntilStopped {
  sched: Hangfire.HangfireJobScheduler
  log: Log
}

impl Jobs.JobHandler for UntilStopped {
  func handle(spec: in Jobs.JobSpec): Result[String, Jobs.JobError] {
    val _ = self.log.record("started")
    var waited = 0
    while waited < 20000 {
      if Hangfire.stopRequested(self.sched, spec.id) {
        val _ = self.log.record("stopped after " + waited.toString())
        return Err(Jobs.JobError(message = "stopped", code = "stopped"))
      }
      Jobs.sleepMs(5)
      waited = waited + 5
    }
    Ok("never asked to stop")
  }
}

// ─── Running a job ────────────────────────────────────────────────────────────
test "an enqueued job is run by a Hangfire worker with the submitted spec" {
  val s = started(testConfig())
  val log = Log()
  Hangfire.registerHandler(s, "echo", Echo(log = log))
  val id = submit(s, "echo", "{\"n\":1}", 3, 30000)
  assertEqual(finished(s, id), "Succeeded", "the job succeeds")
  assertEqualInt(log.count(), 1, "the handler ran once")
  val parts = Str.split(log.line(0), "~")
  assertEqual(parts[0], id, "JobSpec.id is the job id")
  assertEqual(parts[1], "echo", "JobSpec.name")
  assertEqual(parts[2], "{\"n\":1}", "JobSpec.payload")
  assertEqual(parts[3], "3", "JobSpec.maxAttempts")
  assertEqual(parts[4], "30000", "JobSpec.timeoutMs")
  assertEqual(parts[5], "0", "JobSpec.scheduledAt is 0 for enqueue")
  assertTrue(parts[6] != Kernel.currentThreadId().toString(), "the handler ran on another thread")
  val rs = attemptsOf(s, id)
  assertEqualInt(rs.length, 1, "one attempt recorded")
  assertEqual(rs[0].jobId, id, "JobResult.jobId")
  assertEqual(statusName(Ok(rs[0].status)), "Succeeded", "the attempt succeeded")
  assertEqual(rs[0].output, "echo:{\"n\":1}", "the handler's output")
  assertEqual(rs[0].error, "", "no error")
  assertTrue(rs[0].startedAt > 0i64 and rs[0].finishedAt >= rs[0].startedAt, "attempt times")
  assertEqual(hangfireState(s, id), "Succeeded", "Hangfire's own state")
  stop(s)
}

test "payload and output survive Hangfire's storage unchanged" {
  val s = started(testConfig())
  val log = Log()
  Hangfire.registerHandler(s, "echo", Echo(log = log))
  val payload = "line one\nline two\r\n\t\"quoted\" \\ back | pipe % percent %0A %7C é 日本 {\"k\":[1,2]}"
  val id = submit(s, "echo", payload, 1, 30000)
  assertEqual(finished(s, id), "Succeeded", "the job succeeds")
  val got = log.line(0)
  assertTrue(Str.contains(got, "~" + payload + "~1~30000~"), "the handler received the payload unchanged")
  val rs = attemptsOf(s, id)
  assertEqual(rs[0].output, "echo:" + payload, "the output is read back unchanged")
  val empty = submit(s, "echo", "", 1, 30000)
  assertEqual(finished(s, empty), "Succeeded", "an empty payload")
  assertEqual(attemptsOf(s, empty)[0].output, "echo:", "an empty payload stays empty")
  stop(s)
}

// ─── Retries ──────────────────────────────────────────────────────────────────
test "a failing job is retried maxAttempts times and ends Failed" {
  val s = started(testConfig())
  val log = Log()
  Hangfire.registerHandler(s, "flaky", Flaky(log = log, failures = 100))
  val id = submit(s, "flaky", "", 3, 30000)
  assertEqual(finished(s, id), "Failed", "the job fails")
  assertEqualInt(log.count(), 3, "three attempts ran")
  val rs = attemptsOf(s, id)
  assertEqualInt(rs.length, 3, "three attempts recorded")
  assertEqual(rs[0].error, "boom 1", "first error")
  assertEqual(rs[2].error, "boom 3", "last error")
  assertEqual(statusName(Ok(rs[2].status)), "Failed", "a failed attempt")
  assertEqual(rs[2].output, "", "no output for a failed attempt")
  assertEqual(hangfireState(s, id), "Failed", "Hangfire's own state")
  Jobs.sleepMs(300)
  assertEqualInt(log.count(), 3, "no further attempt after the last")
  stop(s)
}

test "a job that recovers within maxAttempts ends Succeeded" {
  val s = started(testConfig())
  val log = Log()
  Hangfire.registerHandler(s, "flaky", Flaky(log = log, failures = 2))
  val id = submit(s, "flaky", "", 5, 30000)
  assertEqual(finished(s, id), "Succeeded", "the job succeeds")
  val rs = attemptsOf(s, id)
  assertEqualInt(rs.length, 3, "two failures and a success")
  assertEqual(rs[0].error, "boom 1", "first attempt")
  assertEqual(rs[1].error, "boom 2", "second attempt")
  assertEqual(rs[2].output, "recovered on call 3", "third attempt")
  stop(s)
}

test "a job that is being retried is never reported Failed" {
  val s = client(testConfig(), memory())
  val log = Log()
  Hangfire.registerHandler(s, "flaky", Flaky(log = log, failures = 15))
  begin(s)
  val job = submit(s, "flaky", "", 16, 30000)
  // Read the status as fast as possible while Hangfire moves the job from
  // Processing back to its queue fifteen times.  The job succeeds in the
  // end, so it is never Failed, however briefly.
  var reads = 0
  var last = ""
  val deadline = Jobs.nowMs() + 60000i64
  while last != "Succeeded" and Jobs.nowMs() < deadline {
    last = statusName(s.status(job))
    reads = reads + 1
    if last != "Pending" and last != "Running" and last != "Succeeded" {
      panic("read " + reads.toString() + " saw " + last + " after " + log.count().toString() + " attempt(s)")
    }
  }
  assertEqual(last, "Succeeded", "the job succeeds on its sixteenth attempt")
  assertEqualInt(log.count(), 16, "sixteen attempts ran")
  assertTrue(reads > 16, "the status was read throughout")
  stop(s)
}

test "maxAttempts 1 is a single attempt" {
  val s = started(testConfig())
  val log = Log()
  Hangfire.registerHandler(s, "flaky", Flaky(log = log, failures = 100))
  val id = submit(s, "flaky", "", 1, 30000)
  assertEqual(finished(s, id), "Failed", "the job fails")
  Jobs.sleepMs(200)
  assertEqualInt(log.count(), 1, "one attempt")
  stop(s)
}

test "a panicking handler is a failed attempt and is retried" {
  val s = started(testConfig())
  val log = Log()
  Hangfire.registerHandler(s, "panics", Panics(log = log))
  val id = submit(s, "panics", "", 2, 30000)
  assertEqual(finished(s, id), "Failed", "the job fails")
  val rs = attemptsOf(s, id)
  assertEqualInt(rs.length, 2, "two attempts")
  assertTrue(Str.contains(rs[0].error, "handler panicked"), "the panic is recorded: " + rs[0].error)
  assertTrue(Str.contains(rs[0].error, "kaboom"), "with its message: " + rs[0].error)
  stop(s)
}

test "retryDelayMs spaces the attempts" {
  val s = started(testConfig().copy(retryDelayMs = 1000))
  val log = Log()
  Hangfire.registerHandler(s, "flaky", Flaky(log = log, failures = 1))
  val id = submit(s, "flaky", "", 2, 30000)
  assertTrue(eventually(10000, { -> log.count() == 1 }), "the first attempt runs")
  assertTrue(
    eventually(5000, { -> hangfireState(s, id) == "Scheduled" }),
    "the job waits in Hangfire's Scheduled state"
  )
  assertEqual(statusName(s.status(id)), "Pending", "which is Pending")
  assertEqual(finished(s, id), "Succeeded", "the job succeeds")
  val rs = attemptsOf(s, id)
  assertEqualInt(rs.length, 2, "two attempts")
  assertTrue(rs[1].startedAt - rs[0].startedAt >= 1000i64, "the second attempt came at least retryDelayMs later")
  stop(s)
}

test "results keeps the most recent maxResultsPerJob attempts" {
  val s = started(testConfig().copy(maxResultsPerJob = 2))
  val log = Log()
  Hangfire.registerHandler(s, "flaky", Flaky(log = log, failures = 100))
  val id = submit(s, "flaky", "", 4, 30000)
  assertEqual(finished(s, id), "Failed", "the job fails")
  assertEqualInt(log.count(), 4, "four attempts ran although only two results are kept")
  val rs = attemptsOf(s, id)
  assertEqualInt(rs.length, 2, "two results kept")
  assertEqual(rs[0].error, "boom 3", "the third")
  assertEqual(rs[1].error, "boom 4", "and the fourth")
  stop(s)
}

// ─── Handler lookup ───────────────────────────────────────────────────────────
test "a job without a handler fails once and is not retried" {
  val s = started(testConfig())
  val id = submit(s, "nobody-home", "", 5, 30000)
  assertEqual(finished(s, id), "Failed", "the job fails")
  val rs = attemptsOf(s, id)
  assertEqualInt(rs.length, 1, "one attempt, not five")
  assertEqual(rs[0].error, "no handler registered for job 'nobody-home'", "the error names the job")
  stop(s)
}

test "the default handler serves names without their own, and handlers can be replaced and removed" {
  val s = started(testConfig())
  val named = Log()
  val fallback = Log()
  Hangfire.registerHandler(s, "named", Echo(log = named))
  Hangfire.registerDefaultHandler(s, Echo(log = fallback))
  assertEqual(finished(s, submit(s, "named", "a", 1, 30000)), "Succeeded", "named job")
  assertEqual(finished(s, submit(s, "other", "b", 1, 30000)), "Succeeded", "other job")
  assertEqualInt(named.count(), 1, "the named handler ran its job")
  assertEqualInt(fallback.count(), 1, "the default handler ran the other")
  val replacement = Log()
  Hangfire.registerHandler(s, "named", Echo(log = replacement))
  assertEqual(finished(s, submit(s, "named", "c", 1, 30000)), "Succeeded", "named job again")
  assertEqualInt(named.count(), 1, "the replaced handler is not called")
  assertEqualInt(replacement.count(), 1, "the replacement is")
  Hangfire.unregisterHandler(s, "named")
  assertEqual(finished(s, submit(s, "named", "d", 1, 30000)), "Succeeded", "named job after unregister")
  assertEqualInt(fallback.count(), 2, "falls back to the default handler")
  stop(s)
}

test "a handler registered after the job was submitted runs it" {
  val s = client(testConfig(), memory())
  val id = submit(s, "late-handler", "x", 1, 30000)
  val log = Log()
  Hangfire.registerHandler(s, "late-handler", Echo(log = log))
  begin(s)
  assertEqual(finished(s, id), "Succeeded", "the job succeeds")
  assertEqualInt(log.count(), 1, "the handler ran")
  stop(s)
}

// ─── Timeout ──────────────────────────────────────────────────────────────────
test "a handler that overruns timeoutMs is a failed attempt, found out when it returns" {
  val s = started(testConfig())
  val log = Log()
  Hangfire.registerHandler(s, "slow", Sleeps(log = log, ms = 400))
  val id = submit(s, "slow", "", 2, 100)
  assertTrue(eventually(10000, { -> log.count() == 1 }), "the first attempt starts")
  assertEqual(statusName(s.status(id)), "Running", "the job is Running past its timeout")
  assertEqual(finished(s, id), "Failed", "the job fails")
  assertEqualInt(log.count(), 2, "both attempts ran to their end")
  val rs = attemptsOf(s, id)
  assertEqualInt(rs.length, 2, "two attempts")
  assertEqual(rs[0].error, "handler exceeded timeoutMs (100ms)", "the timeout error")
  assertEqual(rs[0].output, "", "the handler's output is discarded")
  assertTrue(rs[0].finishedAt - rs[0].startedAt >= 350i64, "the attempt lasted as long as the handler")
  stop(s)
}

test "stopRequested turns true when the attempt has used up its timeoutMs" {
  val s = started(testConfig())
  val log = Log()
  Hangfire.registerHandler(s, "patient", UntilStopped(sched = s, log = log))
  val id = submit(s, "patient", "", 1, 300)
  assertEqual(finished(s, id), "Failed", "the job fails")
  assertEqualInt(log.count(), 2, "the handler started and stopped")
  assertTrue(Str.startsWith(log.line(1), "stopped after"), "it stopped by itself: " + log.line(1))
  assertEqual(attemptsOf(s, id)[0].error, "handler exceeded timeoutMs (300ms)", "recorded as a timeout")
  assertTrue(Hangfire.stopRequested(s, id), "stopRequested is true for a job that is not running")
  assertTrue(Hangfire.stopRequested(s, "no-such-job"), "and for an unknown job")
  stop(s)
}

// ─── Cancel ───────────────────────────────────────────────────────────────────
test "a job cancelled before a worker takes it never runs" {
  val s = client(testConfig(), memory())
  val log = Log()
  Hangfire.registerHandler(s, "echo", Echo(log = log))
  val doomed = submit(s, "echo", "doomed", 1, 30000)
  val kept = submit(s, "echo", "kept", 1, 30000)
  match s.cancel(doomed) {
    case Ok(_) -> ()
    case Err(e) -> panic(e)
  }
  assertEqual(hangfireState(s, doomed), "Deleted", "Hangfire's Deleted state")
  begin(s)
  assertEqual(finished(s, kept), "Succeeded", "the other job runs")
  Jobs.sleepMs(300)
  assertEqualInt(log.count(), 1, "only the other job ran")
  assertEqual(statusName(s.status(doomed)), "Cancelled", "the cancelled job stays Cancelled")
  assertEqualInt(attemptsOf(s, doomed).length, 0, "and has no attempts")
  stop(s)
}

test "cancelling a running job marks it Cancelled at once; the handler ends on its own" {
  val s = started(testConfig())
  val log = Log()
  Hangfire.registerHandler(s, "blocks", Blocks(log = log))
  val id = submit(s, "blocks", "", 3, 30000)
  assertTrue(eventually(10000, { -> log.count() == 1 }), "the handler is running")
  assertEqual(statusName(s.status(id)), "Running", "Running")
  match s.cancel(id) {
    case Ok(_) -> ()
    case Err(e) -> panic(e)
  }
  assertEqual(statusName(s.status(id)), "Cancelled", "Cancelled while the handler still runs")
  assertEqualInt(attemptsOf(s, id).length, 0, "no attempt recorded yet")
  log.release()
  assertTrue(
    eventually(10000, { -> attemptsOf(s, id).length == 1 }),
    "the attempt is recorded when the handler returns"
  )
  val rs = attemptsOf(s, id)
  assertEqual(rs[0].error, "cancelled while running", "as cancelled")
  assertEqual(rs[0].output, "", "its output is discarded")
  Jobs.sleepMs(300)
  assertEqualInt(log.count(), 1, "and it is not retried")
  assertEqual(statusName(s.status(id)), "Cancelled", "still Cancelled")
  stop(s)
}

test "stopRequested turns true when the running job is cancelled" {
  val s = started(testConfig())
  val log = Log()
  Hangfire.registerHandler(s, "patient", UntilStopped(sched = s, log = log))
  val id = submit(s, "patient", "", 3, 600000)
  assertTrue(eventually(10000, { -> log.count() == 1 }), "the handler is running")
  assertFalse(Hangfire.stopRequested(s, id), "not asked to stop yet")
  match s.cancel(id) {
    case Ok(_) -> ()
    case Err(e) -> panic(e)
  }
  assertTrue(eventually(10000, { -> log.count() == 2 }), "the handler sees the request and returns")
  assertTrue(eventually(10000, { -> attemptsOf(s, id).length == 1 }), "the attempt is recorded")
  assertEqual(attemptsOf(s, id)[0].error, "cancelled while running", "as cancelled")
  stop(s)
}

test "cancelling a finished or unknown job changes nothing" {
  val s = started(testConfig())
  val log = Log()
  Hangfire.registerHandler(s, "echo", Echo(log = log))
  Hangfire.registerHandler(s, "flaky", Flaky(log = Log(), failures = 100))
  val good = submit(s, "echo", "", 1, 30000)
  val bad = submit(s, "flaky", "", 1, 30000)
  assertEqual(finished(s, good), "Succeeded", "one succeeds")
  assertEqual(finished(s, bad), "Failed", "one fails")
  for id in [good, bad, "no-such-job", "12345678"] {
    match s.cancel(id) {
      case Ok(_) -> ()
      case Err(e) -> panic("cancel(" + id + ") failed: " + e)
    }
  }
  assertEqual(statusName(s.status(good)), "Succeeded", "a succeeded job stays Succeeded")
  assertEqual(statusName(s.status(bad)), "Failed", "a failed job stays Failed")
  assertEqualInt(attemptsOf(s, good).length, 1, "and keeps its results")
  stop(s)
}

// ─── Retention ────────────────────────────────────────────────────────────────
test "Hangfire forgets a finished job after the storage's retention time" {
  val storage = memory()
  match Hangfire.keepFinishedJobsFor(storage, 1000i64) {
    case Ok(_) -> ()
    case Err(e) -> panic(e)
  }
  val s = client(testConfig(), storage)
  val log = Log()
  Hangfire.registerHandler(s, "echo", Echo(log = log))
  Hangfire.registerHandler(s, "flaky", Flaky(log = Log(), failures = 100))
  begin(s)
  val good = submit(s, "echo", "", 1, 30000)
  val bad = submit(s, "flaky", "", 1, 30000)
  val doomed = submit(s, "no-such-handler-but-cancelled", "", 1, 600000)
  assertEqual(finished(s, good), "Succeeded", "one job succeeds")
  assertEqual(finished(s, bad), "Failed", "one fails")
  assertTrue(
    eventually(20000, { -> statusName(s.status(good)) == "Err(job not found: " + good + ")" }),
    "the succeeded job becomes unknown: " + statusName(s.status(good))
  )
  match s.results(good) {
    case Ok(_) -> panic("the results of a forgotten job must be an Err")
    case Err(e) -> assertEqual(e, "job not found: " + good, "results of a forgotten job")
  }
  assertEqual(statusName(s.status(bad)), "Failed", "a failed job is kept")
  assertEqualInt(attemptsOf(s, bad).length, 1, "with its results")
  assertTrue(doomed.length > 0, "the third job had an id")
  stop(s)
}

// ─── Workers, queues, shared storage ──────────────────────────────────────────
test "workerCount jobs run at the same time, the rest wait Pending" {
  val s = started(testConfig().copy(workerCount = 3))
  val log = Log()
  Hangfire.registerHandler(s, "blocks", Blocks(log = log))
  val ids: List[String] = newList()
  var i = 0
  while i < 5 {
    ids.add(submit(s, "blocks", "", 1, 30000))
    i = i + 1
  }
  assertTrue(eventually(10000, { -> log.count() == 3 }), "three handlers start")
  Jobs.sleepMs(300)
  assertEqualInt(log.count(), 3, "and no more than three")
  var running = 0
  var pending = 0
  for id in ids {
    val st = statusName(s.status(id))
    if st == "Running" {
      running = running + 1
    }
    if st == "Pending" {
      pending = pending + 1
    }
  }
  assertEqualInt(running, 3, "three Running")
  assertEqualInt(pending, 2, "two Pending")
  match Native.counts(s) {
    case Err(e) -> panic(e)
    case Ok(c) -> {
      assertEqualLong(c.processing, 3i64, "Hangfire counts three processing")
      assertEqualLong(c.enqueued, 2i64, "and two enqueued")
    }
  }
  log.release()
  for id in ids {
    assertEqual(finished(s, id), "Succeeded", "every job finishes")
  }
  assertEqualInt(log.most(), 3, "never more than three at once")
  stop(s)
}

test "two schedulers on one storage with different queues each run their own jobs" {
  val storage = memory()
  val a = client(testConfig().copy(queue = "alpha"), storage)
  val b = client(testConfig().copy(queue = "beta_2-x"), storage)
  val seenA = Log()
  val seenB = Log()
  Hangfire.registerDefaultHandler(a, Echo(log = seenA))
  Hangfire.registerDefaultHandler(b, Echo(log = seenB))
  begin(a)
  begin(b)
  val ja = submit(a, "job", "for-a", 1, 30000)
  val jb = submit(b, "job", "for-b", 1, 30000)
  assertEqual(finished(a, ja), "Succeeded", "a's job")
  assertEqual(finished(b, jb), "Succeeded", "b's job")
  assertEqualInt(seenA.count(), 1, "a ran one job")
  assertEqualInt(seenB.count(), 1, "b ran one job")
  assertTrue(Str.contains(seenA.line(0), "~for-a~"), "a ran its own job")
  assertTrue(Str.contains(seenB.line(0), "~for-b~"), "b ran its own job")
  // The storage is shared, so either scheduler can report either job.
  assertEqual(statusName(b.status(ja)), "Succeeded", "b sees a's job")
  stop(a)
  stop(b)
}

test "a retried job stays on its scheduler's queue" {
  val storage = memory()
  val a = client(testConfig().copy(queue = "alpha", retryDelayMs = 1000), storage)
  val other = client(testConfig(), storage)
  val seenA = Log()
  val seenOther = Log()
  Hangfire.registerDefaultHandler(a, Flaky(log = seenA, failures = 1))
  Hangfire.registerDefaultHandler(other, Echo(log = seenOther))
  begin(a)
  begin(other)
  val id = submit(a, "job", "", 2, 30000)
  assertEqual(finished(a, id), "Succeeded", "the job succeeds on its second attempt")
  assertEqualInt(seenA.count(), 2, "both attempts ran on the scheduler that owns the queue")
  assertEqualInt(seenOther.count(), 0, "the scheduler on the default queue never saw it")
  stop(a)
  stop(other)
}

test "a scheduler without a server submits jobs that another scheduler runs" {
  val storage = memory()
  val web = client(testConfig(), storage)
  val worker = client(testConfig(), storage)
  val log = Log()
  Hangfire.registerHandler(worker, "echo", Echo(log = log))
  val id = submit(web, "echo", "from-web", 1, 30000)
  assertEqual(statusName(web.status(id)), "Pending", "Pending while no server runs")
  assertFalse(Hangfire.isRunning(web), "the submitting scheduler has no server")
  begin(worker)
  assertEqual(finished(web, id), "Succeeded", "the submitting scheduler sees the outcome")
  assertEqual(attemptsOf(web, id)[0].output, "echo:from-web", "and the result")
  assertEqualInt(log.count(), 1, "the worker's handler ran")
  stop(worker)
}

// ─── Lifecycle ────────────────────────────────────────────────────────────────
test "create rejects a queue name Hangfire would not accept" {
  for queue in ["", "Upper", "has space", "dot.ted", "ünï"] {
    match Hangfire.create(testConfig().copy(queue = queue), memory()) {
      case Ok(_) -> panic("queue '" + queue + "' must be rejected")
      case Err(e) -> assertTrue(Str.contains(e, "invalid queue name"), "the error says why: " + e)
    }
  }
}

test "start twice is an Err, shutdown twice is not, and a scheduler can be started again" {
  val s = client(testConfig(), memory())
  val log = Log()
  Hangfire.registerHandler(s, "echo", Echo(log = log))
  assertFalse(Hangfire.isRunning(s), "not running before start")
  begin(s)
  assertTrue(Hangfire.isRunning(s), "running after start")
  match Hangfire.start(s) {
    case Ok(_) -> panic("a second start must fail")
    case Err(e) -> assertTrue(Str.contains(e, "already started"), "the error says why: " + e)
  }
  assertEqual(finished(s, submit(s, "echo", "1", 1, 30000)), "Succeeded", "a job runs")
  stop(s)
  assertFalse(Hangfire.isRunning(s), "not running after shutdown")
  stop(s)
  val queued = submit(s, "echo", "2", 1, 30000)
  Jobs.sleepMs(300)
  assertEqual(statusName(s.status(queued)), "Pending", "a job submitted after shutdown waits")
  begin(s)
  assertEqual(finished(s, queued), "Succeeded", "and runs once the scheduler is started again")
  assertEqualInt(log.count(), 2, "both jobs ran")
  stop(s)
}

test "shutdown with waitForJobs waits for the running handler and its outcome is stored" {
  val s = started(testConfig())
  val log = Log()
  Hangfire.registerHandler(s, "slow", Sleeps(log = log, ms = 700))
  val id = submit(s, "slow", "", 1, 30000)
  assertTrue(eventually(10000, { -> log.count() == 1 }), "the handler is running")
  val before = Jobs.nowMs()
  stop(s)
  assertTrue(Jobs.nowMs() - before >= 400i64, "shutdown waited for the handler")
  assertEqual(statusName(s.status(id)), "Succeeded", "the job's outcome was stored")
}

test "shutdown without waitForJobs returns at once, and with it reports jobs still running" {
  val s = started(testConfig().copy(shutdownTimeoutMs = 300))
  val log = Log()
  Hangfire.registerHandler(s, "blocks", Blocks(log = log))
  val id = submit(s, "blocks", "", 1, 30000)
  assertTrue(eventually(10000, { -> log.count() == 1 }), "the handler is running")
  val before = Jobs.nowMs()
  match Hangfire.shutdown(s, false) {
    case Ok(_) -> ()
    case Err(e) -> panic(e)
  }
  assertTrue(Jobs.nowMs() - before < 5000i64, "shutdown did not wait for the handler")
  assertFalse(Hangfire.isRunning(s), "the server is gone")
  log.release()

  val t = started(testConfig().copy(shutdownTimeoutMs = 300))
  val held = Log()
  Hangfire.registerHandler(t, "blocks", Blocks(log = held))
  val _ = submit(t, "blocks", "", 1, 30000)
  assertTrue(eventually(10000, { -> held.count() == 1 }), "the second handler is running")
  match Hangfire.shutdown(t, true) {
    case Ok(_) -> panic("shutdown must report the job that is still running")
    case Err(e) -> assertTrue(Str.contains(e, "1 job(s) were still running after 300ms"), "the error says so: " + e)
  }
  held.release()
  assertTrue(id.length > 0, "the first job had an id")
}
