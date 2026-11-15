//! tests/native_tests.l — recurring jobs and `Jobs.Hangfire.Native`.
//!
//! Recurring jobs fired by Hangfire's own recurring-job scheduler (through
//! `JobScheduler.schedule` and `Native.scheduleCron`), delayed jobs,
//! continuations, manual triggers, queues across schedulers that share a
//! storage, and Hangfire's counts; all with real handlers on real workers,
//! on an in-memory storage.
@test_module
package Jobs.Hangfire.NativeTests

import Std.Core
import Std.Collections
import Std.Testing
import Std.String as Str
import Jobs
import Jobs.Hangfire as Hangfire
import Jobs.Hangfire.Native as Native

// ─── Fixtures ─────────────────────────────────────────────────────────────────
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

func id(r: in Result[String, String]): String {
  match r {
    case Ok(v) -> v
    case Err(e) -> panic(e)
  }
}

func cancelJob(s: in Hangfire.HangfireJobScheduler, jobId: in String): Unit {
  match s.cancel(jobId) {
    case Ok(_) -> ()
    case Err(e) -> panic(e)
  }
}

func finished(s: in Hangfire.HangfireJobScheduler, jobId: in String): String {
  statusName(Hangfire.awaitFinished(s, jobId, 30000))
}

func attemptsOf(s: in Hangfire.HangfireJobScheduler, jobId: in String): slice[Jobs.JobResult] {
  match s.results(jobId) {
    case Ok(list) -> list.items
    case Err(e) -> panic(e)
  }
}

func firingsOf(s: in Hangfire.HangfireJobScheduler, jobId: in String): slice[String] {
  match Native.firings(s, jobId) {
    case Ok(ids) -> ids
    case Err(e) -> panic(e)
  }
}

func hangfireState(s: in Hangfire.HangfireJobScheduler, jobId: in String): String {
  match Native.hangfireState(s, jobId) {
    case Ok(name) -> name
    case Err(e) -> "Err(" + e + ")"
  }
}

func countsOf(s: in Hangfire.HangfireJobScheduler): Native.JobCounts {
  match Native.counts(s) {
    case Ok(c) -> c
    case Err(e) -> panic(e)
  }
}

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

// A cron expression that is not due during a test run: 1 January, 00:00.
val farAway: String = "0 0 1 1 *"

val everySecond: String = "* * * * * *"

// ─── Handlers ─────────────────────────────────────────────────────────────────
record Echo {
  log: Log
}

impl Jobs.JobHandler for Echo {
  func handle(spec: in Jobs.JobSpec): Result[String, Jobs.JobError] {
    val _ = self.log.record(spec.id + "~" + spec.name + "~" + spec.payload + "~" + spec.scheduledAt.toString())
    Ok("echo:" + spec.payload)
  }
}

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

record Blocks {
  log: Log
}

impl Jobs.JobHandler for Blocks {
  func handle(spec: in Jobs.JobSpec): Result[String, Jobs.JobError] {
    val _ = self.log.record(spec.id)
    while not self.log.released() {
      Jobs.sleepMs(5)
    }
    Ok("released")
  }
}

// ─── Recurring jobs ───────────────────────────────────────────────────────────
test "a recurring job fires repeatedly on Hangfire's scheduler until it is cancelled" {
  val s = started(testConfig())
  val log = Log()
  Hangfire.registerHandler(s, "tick", Echo(log = log))
  val before = Jobs.nowMs()
  val job = id(Native.scheduleCron(s, "tick", "p", everySecond, 1, 30000))
  assertTrue(Str.startsWith(job, "lyric-recurring-"), "a recurring job id: " + job)
  assertTrue(eventually(15000, { -> log.count() >= 3 }), "the job fires at least three times")
  val parts = Str.split(log.line(1), "~")
  assertEqual(parts[0], job, "JobSpec.id is the recurring job id, the same for every firing")
  assertEqual(parts[1], "tick", "JobSpec.name")
  assertEqual(parts[2], "p", "JobSpec.payload")
  assertTrue(parts[3] != "0", "JobSpec.scheduledAt is the time of the firing")
  assertTrue(eventually(5000, { -> attemptsOf(s, job).length >= 3 }), "one result per firing")
  val rs = attemptsOf(s, job)
  assertEqual(rs[0].jobId, job, "JobResult.jobId is the recurring job id")
  assertEqual(rs[0].output, "echo:p", "the output of a firing")
  assertTrue(rs[0].startedAt >= before and rs[0].finishedAt >= rs[0].startedAt, "attempt times")
  val fired = firingsOf(s, job)
  assertTrue(fired.length >= 3, "Hangfire created a background job per firing")
  assertTrue(fired[0] != fired[1], "each with its own id")
  assertEqual(hangfireState(s, fired[0]), "Succeeded", "which Hangfire marks Succeeded")
  assertEqual(statusName(s.status(fired[0])), "Succeeded", "and which has a status of its own")
  val st = statusName(s.status(job))
  assertTrue(st == "Pending" or st == "Running", "the recurring job itself never finishes: " + st)
  cancelJob(s, job)
  assertEqual(statusName(s.status(job)), "Cancelled", "Cancelled after cancel")
  Jobs.sleepMs(1500)
  val settled = log.count()
  Jobs.sleepMs(2500)
  assertEqualInt(log.count(), settled, "no firing after cancel")
  assertTrue(attemptsOf(s, job).length >= 3, "the results stay readable")
  cancelJob(s, job)
  assertEqual(finished(s, job), "Cancelled", "awaitFinished ends on Cancelled")
  stop(s)
}

test "schedule takes a 5-field expression and fires at the next minute" {
  val s = started(testConfig())
  val log = Log()
  Hangfire.registerHandler(s, "minutely", Echo(log = log))
  val job = id(s.schedule("minutely", "m", "* * * * *", 1, 30000))
  assertEqual(statusName(s.status(job)), "Pending", "Pending until it fires")
  assertTrue(eventually(75000, { -> log.count() >= 1 }), "the job fires within the next minute")
  assertTrue(eventually(5000, { -> attemptsOf(s, job).length >= 1 }), "and records its result")
  assertEqual(attemptsOf(s, job)[0].output, "echo:m", "the output")
  cancelJob(s, job)
  stop(s)
}

test "an invalid Hangfire cron expression is an Err, and schedule keeps to the 5-field dialect" {
  val s = client(testConfig(), memory())
  for expr in ["not a cron", "61 * * * * *", "* * * * * * * *"] {
    match Native.scheduleCron(s, "job", "", expr, 1, 30000) {
      case Ok(_) -> panic("scheduleCron(\"" + expr + "\") must be rejected")
      case Err(e) -> assertTrue(Str.contains(e, "cannot schedule job"), "the error says what failed: " + e)
    }
  }
  // Valid for Hangfire, but not the interface's dialect.
  for expr in ["* * * * * *", "0 3 * * MON", "@hourly"] {
    match s.schedule("job", "", expr, 1, 30000) {
      case Ok(_) -> panic("schedule(\"" + expr + "\") must be rejected")
      case Err(e) -> assertTrue(Str.startsWith(e, "cron: "), "rejected by the interface's parser: " + e)
    }
    assertTrue(
      Str.startsWith(id(Native.scheduleCron(s, "job", "", expr, 1, 30000)), "lyric-recurring-"),
      "scheduleCron accepts " + expr
    )
  }
}

test "schedule accepts what the interface's dialect allows, Sunday as 7 and a date that never comes included" {
  val s = client(testConfig(), memory())
  for expr in ["0 3 * * 7", "0 3 * * 0", "0 0 31 2 *", "50/5 * * * *", "1-5/2 */3 1,15 */2 1-5"] {
    val job = id(s.schedule("job", "", expr, 1, 30000))
    assertEqual(statusName(s.status(job)), "Pending", "schedule(\"" + expr + "\") is Pending")
    cancelJob(s, job)
  }
}

test "firings of one recurring job overlap when a firing outlasts the interval" {
  val s = started(testConfig())
  val log = Log()
  Hangfire.registerHandler(s, "blocks", Blocks(log = log))
  val job = id(Native.scheduleCron(s, "blocks", "", everySecond, 1, 30000))
  assertTrue(eventually(15000, { -> log.count() >= 3 }), "three firings run at the same time")
  assertEqual(statusName(s.status(job)), "Running", "the job is Running")
  cancelJob(s, job)
  log.release()
  stop(s)
}

// Day of the month (1-31) and day of the week (0 = Sunday) of an epoch
// millisecond, in UTC.
func dayOfMonth(ms: in Long): Int {
  val z = ms / 86400000i64 + 719468i64
  val era = z / 146097i64
  val doe = z - era * 146097i64
  val yoe = (doe - doe / 1460i64 + doe / 36524i64 - doe / 146096i64) / 365i64
  val doy = doe - (365i64 * yoe + yoe / 4i64 - yoe / 100i64)
  val mp = (5i64 * doy + 2i64) / 153i64;
  (doy - (153i64 * mp + 2i64) / 5i64 + 1i64).toInt()
}

func dayOfWeek(ms: in Long): Int {
  ((ms / 86400000i64 + 4i64) % 7i64).toInt()
}

test "when both day fields are restricted, a day must match both" {
  // Keep clear of midnight UTC, when "today" changes.
  while Jobs.nowMs() % 86400000i64 > 86400000i64 - 20000i64 {
    Jobs.sleepMs(1000)
  }
  val now = Jobs.nowMs()
  val today = dayOfMonth(now).toString()
  val weekday = dayOfWeek(now)
  val otherWeekday = (weekday + 3) % 7
  val s = started(testConfig())
  val both = Log()
  val onlyDate = Log()
  Hangfire.registerHandler(s, "both", Echo(log = both))
  Hangfire.registerHandler(s, "only-date", Echo(log = onlyDate))
  val matching = id(Native.scheduleCron(s, "both", "", "* * * " + today + " * " + weekday.toString(), 1, 30000))
  val halfMatching = id(
    Native.scheduleCron(
      s,
      "only-date",
      "",
      "* * * " + today + " * " + otherWeekday.toString(),
      1,
      30000
    )
  )
  assertTrue(eventually(15000, { -> both.count() >= 2 }), "today's date and today's weekday: fires")
  assertEqualInt(onlyDate.count(), 0, "today's date but another weekday: does not fire")
  cancelJob(s, matching)
  cancelJob(s, halfMatching)
  stop(s)
}

test "triggerNow fires a recurring job outside its schedule" {
  val s = started(testConfig())
  val log = Log()
  Hangfire.registerHandler(s, "yearly", Echo(log = log))
  val job = id(s.schedule("yearly", "y", farAway, 1, 30000))
  Jobs.sleepMs(500)
  assertEqualInt(log.count(), 0, "not due by itself")
  assertEqualInt(attemptsOf(s, job).length, 0, "no results yet")
  val fired = id(Native.triggerNow(s, job))
  assertTrue(eventually(10000, { -> attemptsOf(s, job).length == 1 }), "the triggered firing runs")
  assertEqual(firingsOf(s, job)[0], fired, "triggerNow returned the firing's Hangfire id")
  assertEqual(statusName(s.status(job)), "Pending", "the recurring job is Pending again")
  cancelJob(s, job)
  match Native.triggerNow(s, job) {
    case Ok(_) -> panic("a cancelled job cannot be triggered")
    case Err(e) -> assertEqual(e, "job not found: " + job, "the error for a cancelled job")
  }
  match Native.triggerNow(s, fired) {
    case Ok(_) -> panic("a one-off job cannot be triggered")
    case Err(e) -> assertTrue(Str.contains(e, "not a recurring job"), "the error for a one-off job: " + e)
  }
  match Native.triggerNow(s, "lyric-recurring-unknown") {
    case Ok(_) -> panic("an unknown job cannot be triggered")
    case Err(e) -> assertEqual(e, "job not found: lyric-recurring-unknown", "the error for an unknown job")
  }
  assertEqualInt(log.count(), 1, "one firing in all")
  stop(s)
}

test "a firing of a recurring job is retried like any job" {
  val s = started(testConfig())
  val log = Log()
  Hangfire.registerHandler(s, "flaky", Flaky(log = log, failures = 1))
  val job = id(s.schedule("flaky", "", farAway, 3, 30000))
  val fired = id(Native.triggerNow(s, job))
  assertTrue(eventually(10000, { -> attemptsOf(s, job).length == 2 }), "two attempts of the one firing")
  val rs = attemptsOf(s, job)
  assertEqual(rs[0].error, "boom 1", "the failed attempt")
  assertEqual(rs[1].output, "recovered on call 2", "the retry")
  assertEqualInt(firingsOf(s, job).length, 1, "still one firing")
  assertEqual(finished(s, fired), "Succeeded", "the firing's background job succeeded")
  cancelJob(s, job)
  stop(s)
}

test "a recurring job is Running while a firing runs, and cancelling it then discards the firing" {
  val s = started(testConfig())
  val log = Log()
  Hangfire.registerHandler(s, "blocks", Blocks(log = log))
  val job = id(s.schedule("blocks", "", farAway, 3, 30000))
  assertEqual(statusName(s.status(job)), "Pending", "Pending before it fires")
  val _ = id(Native.triggerNow(s, job))
  assertTrue(eventually(10000, { -> log.count() == 1 }), "the firing runs")
  assertEqual(statusName(s.status(job)), "Running", "Running while it does")
  assertFalse(Hangfire.stopRequested(s, job), "the handler is not asked to stop")
  cancelJob(s, job)
  assertEqual(statusName(s.status(job)), "Cancelled", "Cancelled at once")
  assertTrue(eventually(5000, { -> Hangfire.stopRequested(s, job) }), "the handler is asked to stop")
  log.release()
  assertTrue(
    eventually(10000, { -> attemptsOf(s, job).length == 1 }),
    "the attempt is recorded when the handler returns"
  )
  assertEqual(attemptsOf(s, job)[0].error, "cancelled while running", "as cancelled")
  Jobs.sleepMs(300)
  assertEqualInt(log.count(), 1, "and it is not retried")
  stop(s)
}

test "a firing that was queued when its recurring job was cancelled does not run the handler" {
  val s = client(testConfig(), memory())
  val log = Log()
  Hangfire.registerHandler(s, "echo", Echo(log = log))
  val job = id(s.schedule("echo", "", farAway, 1, 30000))
  val fired = id(Native.triggerNow(s, job))
  assertEqual(hangfireState(s, fired), "Enqueued", "the firing waits for a worker")
  cancelJob(s, job)
  begin(s)
  assertTrue(eventually(10000, { -> hangfireState(s, fired) == "Succeeded" }), "Hangfire processes the firing")
  assertEqualInt(log.count(), 0, "but the handler is not called")
  assertEqualInt(attemptsOf(s, job).length, 0, "and no attempt is recorded")
  stop(s)
}

test "a recurring job keeps the results of its most recent firings" {
  val s = started(testConfig().copy(maxResultsPerJob = 3))
  val log = Log()
  Hangfire.registerHandler(s, "echo", Echo(log = log))
  val job = id(s.schedule("echo", "", farAway, 1, 30000))
  val fired: List[String] = newList()
  var i = 0
  while i < 5 {
    fired.add(id(Native.triggerNow(s, job)))
    val want = i + 1
    assertTrue(eventually(10000, { -> log.count() == want }), "firing " + want.toString() + " runs")
    assertEqual(finished(s, fired[i]), "Succeeded", "and finishes")
    i = i + 1
  }
  assertEqualInt(attemptsOf(s, job).length, 3, "three results kept")
  val kept = firingsOf(s, job)
  assertEqualInt(kept.length, 3, "three firings kept")
  assertEqual(kept[0], fired[2], "the third firing is the oldest kept")
  assertEqual(kept[2], fired[4], "the fifth is the newest")
  cancelJob(s, job)
  stop(s)
}

// ─── Delayed jobs and continuations ───────────────────────────────────────────
test "enqueueIn holds a job back in Hangfire's Scheduled state" {
  val s = started(testConfig())
  val log = Log()
  Hangfire.registerHandler(s, "echo", Echo(log = log))
  val before = Jobs.nowMs()
  val job = id(Native.enqueueIn(s, "echo", "later", 2000i64, 1, 30000))
  assertEqual(hangfireState(s, job), "Scheduled", "Scheduled in Hangfire")
  assertEqual(statusName(s.status(job)), "Pending", "which is Pending")
  assertEqualLong(countsOf(s).scheduled, 1i64, "Hangfire counts one scheduled job")
  Jobs.sleepMs(600)
  assertEqualInt(log.count(), 0, "not run before its time")
  assertEqual(finished(s, job), "Succeeded", "run when due")
  assertTrue(attemptsOf(s, job)[0].startedAt - before >= 2000i64, "at least two seconds later")
  val gone = id(Native.enqueueIn(s, "echo", "never", 60000i64, 1, 30000))
  cancelJob(s, gone)
  assertEqual(statusName(s.status(gone)), "Cancelled", "a delayed job can be cancelled")
  stop(s)
}

test "a delayed job that another scheduler's server finds due still goes to its own queue" {
  val storage = memory()
  val owner = client(testConfig().copy(queue = "alpha"), storage)
  val other = client(testConfig(), storage)
  val seenOwner = Log()
  val seenOther = Log()
  Hangfire.registerDefaultHandler(owner, Echo(log = seenOwner))
  Hangfire.registerDefaultHandler(other, Echo(log = seenOther))
  // Only the other scheduler has a server when the job comes due.
  begin(other)
  val job = id(Native.enqueueIn(owner, "job", "", 1000i64, 1, 30000))
  assertTrue(eventually(15000, { -> hangfireState(owner, job) == "Enqueued" }), "the other server enqueues it")
  assertEqualLong(countsOf(owner).enqueued, 1i64, "on the owner's queue")
  assertEqualLong(countsOf(other).enqueued, 0i64, "not on the default queue")
  Jobs.sleepMs(300)
  assertEqualInt(seenOther.count(), 0, "the other scheduler does not run it")
  begin(owner)
  assertEqual(finished(owner, job), "Succeeded", "the owner runs it")
  assertEqualInt(seenOwner.count(), 1, "with its own handler")
  stop(owner)
  stop(other)
}

test "a recurring job fires on its scheduler's queue whichever server fires it" {
  val storage = memory()
  val owner = client(testConfig().copy(queue = "alpha"), storage)
  val other = client(testConfig(), storage)
  val seenOwner = Log()
  val seenOther = Log()
  Hangfire.registerDefaultHandler(owner, Echo(log = seenOwner))
  Hangfire.registerDefaultHandler(other, Echo(log = seenOther))
  begin(other)
  val job = id(Native.scheduleCron(owner, "tick", "", everySecond, 1, 30000))
  assertTrue(
    eventually(15000, { -> countsOf(owner).enqueued >= 2i64 }),
    "the other server creates the firings on the owner's queue"
  )
  assertEqualInt(seenOther.count(), 0, "and does not run them")
  begin(owner)
  assertTrue(eventually(15000, { -> seenOwner.count() >= 2 }), "the owner runs them")
  assertEqualInt(seenOther.count(), 0, "the other scheduler never ran one")
  cancelJob(owner, job)
  stop(owner)
  stop(other)
}

test "enqueueAfter runs a job once its parent has succeeded" {
  val s = started(testConfig().copy(queue = "chain"))
  val parentLog = Log()
  val childLog = Log()
  Hangfire.registerHandler(s, "parent", Blocks(log = parentLog))
  Hangfire.registerHandler(s, "child", Echo(log = childLog))
  val parent = id(s.enqueue("parent", "", 1, 30000))
  val child = id(Native.enqueueAfter(s, parent, "child", "after", 1, 30000))
  assertTrue(eventually(10000, { -> parentLog.count() == 1 }), "the parent runs")
  assertEqual(hangfireState(s, child), "Awaiting", "the child awaits it")
  assertEqual(statusName(s.status(child)), "Pending", "which is Pending")
  Jobs.sleepMs(300)
  assertEqualInt(childLog.count(), 0, "the child does not run while the parent does")
  parentLog.release()
  assertEqual(finished(s, parent), "Succeeded", "the parent succeeds")
  assertEqual(finished(s, child), "Succeeded", "then the child runs")
  assertEqual(attemptsOf(s, child)[0].output, "echo:after", "with its own payload")
  assertTrue(attemptsOf(s, child)[0].startedAt >= attemptsOf(s, parent)[0].finishedAt, "after the parent ended")
  val late = id(Native.enqueueAfter(s, parent, "child", "late", 1, 30000))
  assertEqual(finished(s, late), "Succeeded", "a continuation of a parent that already succeeded runs at once")
  stop(s)
}

test "a continuation of a failed parent stays Pending, and the parent must be a known one-off job" {
  val s = started(testConfig())
  val childLog = Log()
  Hangfire.registerHandler(s, "parent", Flaky(log = Log(), failures = 100))
  Hangfire.registerHandler(s, "child", Echo(log = childLog))
  val parent = id(s.enqueue("parent", "", 1, 30000))
  val child = id(Native.enqueueAfter(s, parent, "child", "", 1, 30000))
  assertEqual(finished(s, parent), "Failed", "the parent fails")
  Jobs.sleepMs(500)
  assertEqualInt(childLog.count(), 0, "the child does not run")
  assertEqual(hangfireState(s, child), "Awaiting", "it is still Awaiting")
  assertEqual(statusName(s.status(child)), "Pending", "which is Pending")
  cancelJob(s, child)
  assertEqual(statusName(s.status(child)), "Cancelled", "and can be cancelled")
  match Native.enqueueAfter(s, "no-such-job", "child", "", 1, 30000) {
    case Ok(_) -> panic("an unknown parent must be rejected")
    case Err(e) -> assertEqual(e, "job not found: no-such-job", "the error for an unknown parent")
  }
  val recurring = id(s.schedule("child", "", farAway, 1, 30000))
  match Native.enqueueAfter(s, recurring, "child", "", 1, 30000) {
    case Ok(_) -> panic("a recurring parent must be rejected")
    case Err(e) -> assertTrue(Str.contains(e, "recurring job has no end"), "the error for a recurring parent: " + e)
  }
  cancelJob(s, recurring)
  stop(s)
}

// ─── Counts ───────────────────────────────────────────────────────────────────
test "counts reports Hangfire's own numbers" {
  val s = client(testConfig(), memory())
  Hangfire.registerHandler(s, "ok", Echo(log = Log()))
  Hangfire.registerHandler(s, "bad", Flaky(log = Log(), failures = 100))
  val ok1 = id(s.enqueue("ok", "", 1, 30000))
  val ok2 = id(s.enqueue("ok", "", 1, 30000))
  val bad = id(s.enqueue("bad", "", 1, 30000))
  // A cancelled delayed job: one that was never on the queue.
  val gone = id(Native.enqueueIn(s, "ok", "", 60000i64, 1, 30000))
  cancelJob(s, gone)
  val waiting = countsOf(s)
  assertEqualLong(waiting.enqueued, 3i64, "three enqueued")
  assertEqualLong(waiting.deleted, 1i64, "one deleted")
  assertEqualLong(waiting.succeeded, 0i64, "none succeeded yet")
  begin(s)
  assertEqual(finished(s, ok1), "Succeeded", "first")
  assertEqual(finished(s, ok2), "Succeeded", "second")
  assertEqual(finished(s, bad), "Failed", "third")
  val done = countsOf(s)
  assertEqualLong(done.enqueued, 0i64, "the queue is empty")
  assertEqualLong(done.succeeded, 2i64, "two succeeded")
  assertEqualLong(done.failed, 1i64, "one failed")
  assertEqualLong(done.processing, 0i64, "none processing")
  stop(s)
}
