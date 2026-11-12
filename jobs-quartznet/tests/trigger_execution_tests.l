//! tests/trigger_execution_tests.l — recurring and Quartz.NET-specific triggers
//! firing real handlers: native Quartz cron at seconds resolution, interval
//! triggers, delayed one-shots, misfire policies, and `JobScheduler.schedule`
//! with a 5-field cron expression (which waits for the next minute boundary).
@test_module
package Jobs.QuartzNet.TriggerExecutionTests

import Std.Core
import Std.Collections
import Std.Testing
import Std.String as Str
import Jobs
import Jobs.QuartzNet as Quartz
import Jobs.QuartzNet.Triggers

protected type Counter {
  var calls: Int = 0
  var stamps: List[Long] = newList()
  entry hit(at: in Long): Int {
    calls = calls + 1
    stamps.add(at)
    return calls
  }
  entry count(): Int {
    return calls
  }
  entry stamp(i: in Int): Long {
    return stamps[i]
  }
}

// Succeeds, except on the calls listed in `failOn`.
record Tick {
  counter: Counter
  failOn: slice[Int]
}

impl Jobs.JobHandler for Tick {
  func handle(spec: in Jobs.JobSpec): Result[String, Jobs.JobError] {
    val call = self.counter.hit(spec.scheduledAt)
    for n in self.failOn {
      if n == call {
        return Err(Jobs.JobError(message = "tick " + call.toString() + " failed", code = ""))
      }
    }
    Ok("tick " + call.toString())
  }
}

func ticker(counter: in Counter): Tick {
  val none: slice[Int] = []
  Tick(counter = counter, failOn = none)
}

func testConfig(): Quartz.QuartzConfig {
  Quartz.defaultConfig()
}

func created(config: in Quartz.QuartzConfig): Quartz.QuartzJobScheduler {
  match Quartz.create(config) {
    case Err(e) -> panic(e)
    case Ok(s) -> s
  }
}

func begin(s: in Quartz.QuartzJobScheduler): Unit {
  match Quartz.start(s) {
    case Err(e) -> panic(e)
    case Ok(_) -> ()
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

func mustSpec(r: in Result[Triggers.TriggerSpec, String]): Triggers.TriggerSpec {
  match r {
    case Ok(spec) -> spec
    case Err(e) -> panic(e)
  }
}

func resultsOf(s: in Quartz.QuartzJobScheduler, id: in String): slice[Jobs.JobResult] {
  match s.results(id) {
    case Ok(list) -> list.items
    case Err(e) -> panic(e)
  }
}

func waitForCount(counter: in Counter, n: in Int, timeoutMs: in Int): Bool {
  var waited = 0
  while counter.count() < n and waited < timeoutMs {
    Jobs.sleepMs(10)
    waited = waited + 10
  }
  counter.count() >= n
}

func waitForPending(s: in Quartz.QuartzJobScheduler, id: in String): Unit {
  var waited = 0
  while statusName(s.status(id)) == "Running" and waited < 5000 {
    Jobs.sleepMs(5)
    waited = waited + 5
  }
}

// ─── Native Quartz cron ───────────────────────────────────────────────────────
test "a Quartz.NET cron job fires its handler every second until cancelled" {
  val s = created(testConfig())
  val counter = Counter()
  Quartz.registerHandler(s, "tick", ticker(counter))
  begin(s)
  val spec = mustSpec(Triggers.quartzCron("* * * * * ?", Triggers.MisfirePolicy.Skip))
  val id = mustId(Quartz.scheduleTrigger(s, "tick", "{}", spec, 1, 30000))
  assertTrue(waitForCount(counter, 3, 8000), "the handler ran three times, got " + counter.count().toString())
  val gap = counter.stamp(2) - counter.stamp(1)
  assertEqualLong(gap, 1000i64, "consecutive firings are scheduled one second apart")
  assertEqualLong(counter.stamp(1) % 1000i64, 0i64, "firings are scheduled on whole seconds")
  waitForPending(s, id)
  assertEqual(statusName(s.status(id)), "Pending", "a recurring job is Pending between firings")
  assertTrue(Quartz.timesFired(s, id) >= 3, "timesFired")
  val results = resultsOf(s, id)
  assertTrue(results.length >= 3, "one result per firing")
  assertEqual(results[0].output, "tick 1", "first firing's output")
  assertEqual(results[2].output, "tick 3", "third firing's output")

  match s.cancel(id) {
    case Ok(_) -> ()
    case Err(e) -> assertTrue(false, e)
  }
  assertEqual(statusName(s.status(id)), "Cancelled", "status after cancel")
  match Quartz.heldByQuartz(s, id) {
    case Ok(held) -> assertFalse(held, "cancel removes the cron job from Quartz.NET")
    case Err(e) -> assertTrue(false, e)
  }
  Jobs.sleepMs(200)
  val after = counter.count()
  Jobs.sleepMs(2200)
  assertEqualInt(counter.count(), after, "no firing after cancel")
  stop(s)
}

test "a failed firing of a recurring job is retried and does not stop later firings" {
  val s = created(testConfig())
  val counter = Counter()
  Quartz.registerHandler(s, "tick", Tick(counter = counter, failOn = [1, 2, 3]))
  begin(s)
  val spec = mustSpec(Triggers.interval(0i64, 300i64, -1, Triggers.MisfirePolicy.Smart))
  val id = mustId(Quartz.scheduleTrigger(s, "tick", "", spec, 2, 30000))
  // Firing 1: calls 1 and 2 fail (both attempts).  Firing 2: call 3 fails,
  // call 4 succeeds.  Firing 3: call 5 succeeds.
  assertTrue(waitForCount(counter, 5, 8000), "five handler calls, got " + counter.count().toString())
  waitForPending(s, id)
  match s.cancel(id) {
    case Ok(_) -> ()
    case Err(e) -> assertTrue(false, e)
  }
  val results = resultsOf(s, id)
  assertEqual(results[0].error, "tick 1 failed", "firing 1, attempt 1")
  assertEqual(results[1].error, "tick 2 failed", "firing 1, attempt 2")
  assertEqual(results[2].error, "tick 3 failed", "firing 2, attempt 1")
  assertEqual(results[3].output, "tick 4", "firing 2, attempt 2")
  assertEqual(results[4].output, "tick 5", "firing 3")
  assertEqualLong(counter.stamp(0), counter.stamp(1), "a retry belongs to the same firing")
  assertEqualLong(counter.stamp(2), counter.stamp(3), "a retry belongs to the same firing")
  assertEqualLong(counter.stamp(2) - counter.stamp(0), 300i64, "firings are one interval apart")
  stop(s)
}

// ─── Interval and one-shot triggers ───────────────────────────────────────────
test "an interval trigger with a repeat count fires that many times and then finishes" {
  val s = created(testConfig())
  val counter = Counter()
  Quartz.registerHandler(s, "tick", ticker(counter))
  begin(s)
  val spec = mustSpec(Triggers.interval(0i64, 150i64, 2, Triggers.MisfirePolicy.Smart))
  val id = mustId(Quartz.scheduleTrigger(s, "tick", "", spec, 1, 30000))
  assertEqual(statusName(Quartz.awaitFinished(s, id, 10000)), "Succeeded", "final status after the last firing")
  assertEqualInt(counter.count(), 3, "repeatCount 2 means three firings")
  assertEqualInt(Quartz.timesFired(s, id), 3, "timesFired")
  assertEqualInt(resultsOf(s, id).length, 3, "one result per firing")
  Jobs.sleepMs(400)
  assertEqualInt(counter.count(), 3, "no firing after the last one")
  match Quartz.heldByQuartz(s, id) {
    case Ok(held) -> assertFalse(held, "Quartz.NET drops the job after its last firing")
    case Err(e) -> assertTrue(false, e)
  }
  stop(s)
}

test "the last firing decides the final status of a finite recurring job" {
  val s = created(testConfig())
  val counter = Counter()
  Quartz.registerHandler(s, "tick", Tick(counter = counter, failOn = [2]))
  begin(s)
  val spec = mustSpec(Triggers.interval(0i64, 150i64, 1, Triggers.MisfirePolicy.Smart))
  val id = mustId(Quartz.scheduleTrigger(s, "tick", "", spec, 1, 30000))
  assertEqual(statusName(Quartz.awaitFinished(s, id, 10000)), "Failed", "the second, last firing failed")
  assertEqualInt(counter.count(), 2, "two firings")
  stop(s)
}

test "a delayed one-shot job runs after its delay, and not at all when cancelled first" {
  val s = created(testConfig())
  val counter = Counter()
  Quartz.registerHandler(s, "tick", ticker(counter))
  begin(s)
  val before = Jobs.nowMs()
  val later = mustId(Quartz.scheduleTrigger(s, "tick", "", mustSpec(Triggers.once(400i64)), 1, 30000))
  val doomed = mustId(Quartz.scheduleTrigger(s, "tick", "", mustSpec(Triggers.once(400i64)), 1, 30000))
  Jobs.sleepMs(100)
  assertEqual(statusName(s.status(later)), "Pending", "waiting for its start time")
  assertEqualInt(counter.count(), 0, "nothing ran before the delay")
  match s.cancel(doomed) {
    case Ok(_) -> ()
    case Err(e) -> assertTrue(false, e)
  }
  assertEqual(statusName(Quartz.awaitFinished(s, later, 10000)), "Succeeded", "the delayed job ran")
  assertTrue(Jobs.nowMs() - before >= 390i64, "not before its delay")
  Jobs.sleepMs(300)
  assertEqualInt(counter.count(), 1, "the cancelled delayed job never ran")
  assertEqual(statusName(s.status(doomed)), "Cancelled", "the cancelled job")
  stop(s)
}

test "a firing is skipped while the previous firing of the same job is still running" {
  val s = created(testConfig())
  val counter = Counter()
  Quartz.registerHandler(s, "slow", SlowTick(counter = counter, ms = 700))
  begin(s)
  val spec = mustSpec(Triggers.interval(0i64, 200i64, 2, Triggers.MisfirePolicy.Smart))
  val id = mustId(Quartz.scheduleTrigger(s, "slow", "", spec, 1, 30000))
  Jobs.sleepMs(1500)
  assertEqualInt(counter.count(), 1, "the firings at 200ms and 400ms overlapped the first and were skipped")
  assertEqualInt(Quartz.timesFired(s, id), 1, "timesFired counts only firings that ran")
  assertEqual(statusName(Quartz.awaitFinished(s, id, 10000)), "Succeeded", "the firing that ran finishes the job")
  stop(s)
}

record SlowTick {
  counter: Counter
  ms: Int
}

impl Jobs.JobHandler for SlowTick {
  func handle(spec: in Jobs.JobSpec): Result[String, Jobs.JobError] {
    val _ = self.counter.hit(spec.scheduledAt)
    Jobs.sleepMs(self.ms)
    Ok("slow")
  }
}

// Counts its calls and holds each one until the gate is opened.
protected type Gate {
  var calls: Int = 0
  var open: Bool = false
  entry enter(): Int {
    calls = calls + 1
    return calls
  }
  entry count(): Int {
    return calls
  }
  entry release() {
    open = true
  }
  entry released(): Bool {
    return open
  }
}

record Held {
  gate: Gate
  fail: Bool
}

impl Jobs.JobHandler for Held {
  func handle(spec: in Jobs.JobSpec): Result[String, Jobs.JobError] {
    val _ = self.gate.enter()
    while not self.gate.released() {
      Jobs.sleepMs(5)
    }
    if self.fail {
      return Err(Jobs.JobError(message = "held failed", code = ""))
    }
    Ok("held")
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
    Jobs.sleepMs(10)
    waited = waited + 10
  }
  false
}

// The first firing is held until every later firing of the trigger, the last
// one included, has come and been skipped; the job must still finish, with
// the outcome of the firing that ran.
func statusAfterSkippedLastFiring(fail: in Bool): String {
  val s = created(testConfig())
  val gate = Gate()
  Quartz.registerHandler(s, "held", Held(gate = gate, fail = fail))
  begin(s)
  val spec = mustSpec(Triggers.interval(0i64, 100i64, 2, Triggers.MisfirePolicy.Smart))
  val id = mustId(Quartz.scheduleTrigger(s, "held", "", spec, 1, 600000))
  assertTrue(waitUntilDropped(s, id, 30000), "Quartz.NET has finished the trigger while the first firing still runs")
  assertEqual(statusName(s.status(id)), "Running", "the first firing is still running")
  gate.release()
  val final = statusName(Quartz.awaitFinished(s, id, 30000))
  assertEqualInt(gate.count(), 1, "the later firings were skipped")
  assertEqualInt(Quartz.timesFired(s, id), 1, "timesFired")
  assertEqualInt(resultsOf(s, id).length, 1, "one attempt recorded")
  stop(s)
  final
}

test "a finite job whose last firing is skipped succeeds when its running firing succeeds" {
  assertEqual(statusAfterSkippedLastFiring(false), "Succeeded", "final status")
}

test "a finite job whose last firing is skipped fails when its running firing fails" {
  assertEqual(statusAfterSkippedLastFiring(true), "Failed", "final status")
}

// One worker thread, and the first firing holds it until the two later
// firings are later than the misfire threshold: Quartz.NET then drops them and
// ends the trigger without calling the job again.  The job must still finish,
// with the outcome of the firing that ran, and leave Quartz.NET.
func statusAfterDroppedLastFirings(fail: in Bool): String {
  val s = created(testConfig().copy(workerThreads = 1, misfireThresholdMs = 500))
  val gate = Gate()
  Quartz.registerHandler(s, "held", Held(gate = gate, fail = fail))
  begin(s)
  val spec = mustSpec(Triggers.interval(300i64, 200i64, 2, Triggers.MisfirePolicy.Skip))
  val id = mustId(Quartz.scheduleTrigger(s, "held", "", spec, 1, 600000))
  var waited = 0
  while gate.count() < 1 and waited < 30000 {
    Jobs.sleepMs(10)
    waited = waited + 10
  }
  assertEqualInt(gate.count(), 1, "the first firing started")
  // The firings 200 ms and 400 ms after the first become misfires.
  Jobs.sleepMs(1500)
  assertEqual(statusName(s.status(id)), "Running", "the first firing is still running")
  gate.release()
  val final = statusName(Quartz.awaitFinished(s, id, 30000))
  assertEqualInt(gate.count(), 1, "the later firings were dropped")
  assertEqualInt(Quartz.timesFired(s, id), 1, "timesFired")
  assertEqualInt(resultsOf(s, id).length, 1, "one attempt recorded")
  assertTrue(waitUntilDropped(s, id, 30000), "Quartz.NET no longer stores the finished job")
  stop(s)
  final
}

test "a finite job whose remaining firings are dropped as misfires succeeds when the firing that ran succeeded" {
  assertEqual(statusAfterDroppedLastFirings(false), "Succeeded", "final status")
}

test "a finite job whose remaining firings are dropped as misfires fails when the firing that ran failed" {
  assertEqual(statusAfterDroppedLastFirings(true), "Failed", "final status")
}

test "a finite job whose every firing is dropped as a misfire ends Cancelled without running" {
  val s = created(testConfig().copy(workerThreads = 1, misfireThresholdMs = 100))
  val counter = Counter()
  Quartz.registerHandler(s, "tick", ticker(counter))
  val spec = mustSpec(Triggers.interval(0i64, 100i64, 2, Triggers.MisfirePolicy.Skip))
  val id = mustId(Quartz.scheduleTrigger(s, "tick", "", spec, 1, 30000))
  // All three firings pass the misfire threshold while the scheduler is in standby.
  Jobs.sleepMs(1000)
  assertEqual(statusName(s.status(id)), "Pending", "nothing fires in standby")
  begin(s)
  assertEqual(statusName(Quartz.awaitFinished(s, id, 30000)), "Cancelled", "final status of a job that never ran")
  assertEqualInt(counter.count(), 0, "the handler was never called")
  assertEqualInt(Quartz.timesFired(s, id), 0, "timesFired")
  assertEqualInt(Quartz.attemptsRun(s, id), 0, "attemptsRun")
  assertEqualInt(resultsOf(s, id).length, 0, "no attempt recorded")
  assertTrue(waitUntilDropped(s, id, 30000), "Quartz.NET no longer stores the finished job")
  stop(s)
}

// ─── Misfire policy ───────────────────────────────────────────────────────────
// Ten firings of a 200 ms interval are missed while the scheduler is in
// standby; the count shortly after `start` shows what the policy did with them.
func firedAfterStandby(policy: in Triggers.MisfirePolicy): Int {
  val s = created(testConfig().copy(workerThreads = 1, misfireThresholdMs = 100))
  val counter = Counter()
  Quartz.registerHandler(s, "tick", ticker(counter))
  val spec = mustSpec(Triggers.interval(0i64, 200i64, -1, policy))
  val id = mustId(Quartz.scheduleTrigger(s, "tick", "", spec, 1, 30000))
  Jobs.sleepMs(2000)
  assertEqualInt(counter.count(), 0, "nothing fires in standby")
  begin(s)
  Jobs.sleepMs(300)
  val fired = counter.count()
  match s.cancel(id) {
    case Ok(_) -> ()
    case Err(e) -> panic(e)
  }
  stop(s)
  fired
}

test "MisfirePolicy.IgnoreMisfires runs every missed firing" {
  val fired = firedAfterStandby(Triggers.MisfirePolicy.IgnoreMisfires)
  assertTrue(fired >= 10, "the ten missed firings ran back to back, got " + fired.toString())
}

test "MisfirePolicy.Skip drops the missed firings" {
  val fired = firedAfterStandby(Triggers.MisfirePolicy.Skip)
  assertTrue(fired <= 5, "only firings due after start ran, got " + fired.toString())
}

// ─── 5-field cron through the interface ───────────────────────────────────────
test "JobScheduler.schedule fires the handler at the next minute, once per minute" {
  val s = created(testConfig())
  val plain = Counter()
  val either = Counter()
  Quartz.registerHandler(s, "plain", ticker(plain))
  Quartz.registerHandler(s, "either", ticker(either))
  begin(s)
  val plainId = mustId(s.schedule("plain", "{}", "* * * * *", 1, 30000))
  // Day-of-month and day-of-week both given: two Quartz.NET triggers, each of
  // which fires every minute; the job must still run once per minute.
  val eitherId = mustId(s.schedule("either", "{}", "* * 1-31 * 0-6", 1, 30000))
  assertEqual(statusName(s.status(plainId)), "Pending", "a scheduled job waits for its time")
  assertTrue(waitForCount(plain, 1, 65000), "the every-minute job fired within a minute")
  assertTrue(waitForCount(either, 1, 5000), "the two-trigger job fired at the same minute")
  Jobs.sleepMs(1500)
  assertEqualInt(plain.count(), 1, "one run for the minute")
  assertEqualInt(either.count(), 1, "two triggers selecting the same minute run the job once")
  assertEqualLong(plain.stamp(0) % 60000i64, 0i64, "the firing was scheduled on a whole minute")
  assertEqualLong(either.stamp(0), plain.stamp(0), "both jobs fired for the same minute")
  assertEqual(resultsOf(s, plainId)[0].output, "tick 1", "the handler's output is recorded")
  waitForPending(s, plainId)
  assertEqual(statusName(s.status(plainId)), "Pending", "Pending again until the next minute")
  match s.cancel(plainId) {
    case Ok(_) -> ()
    case Err(e) -> assertTrue(false, e)
  }
  match s.cancel(eitherId) {
    case Ok(_) -> ()
    case Err(e) -> assertTrue(false, e)
  }
  stop(s)
}

test "schedule reports a cron expression that can never fire" {
  val s = created(testConfig())
  match s.schedule("never", "{}", "0 0 31 2 *", 1, 30000) {
    case Ok(_) -> assertTrue(false, "February 31st never comes")
    case Err(e) -> assertTrue(Str.startsWith(e, "quartz: cannot schedule job 'never'"), e)
  }
  stop(s)
}
