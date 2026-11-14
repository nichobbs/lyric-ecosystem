//! tests/integration/sqlite_integration_tests.l — the backend on a
//! persistent Hangfire storage (`Hangfire.Storage.SQLite`).
//!
//! Needs `LYRIC_HANGFIRE_SQLITE_DIR`, a writable directory for the database
//! files, and the native SQLite library `e_sqlite3` on the library search
//! path (`scripts/ci/start-hangfire-sqlite.sh` provides both).  Compiled only
//! with `--features integration`:
//!
//!   eval "$(scripts/ci/start-hangfire-sqlite.sh)"
//!   lyric test --manifest jobs-hangfire/lyric.toml --target dotnet --features integration
//!
//! Every test uses a database file of its own, named after a fresh UUID.
@cfg(feature = "integration")
@test_module
package Jobs.Hangfire.SqliteIntegrationTests

import Std.Core
import Std.Collections
import Std.Environment as Env
import Std.String as Str
import Std.Testing
import Std.Uuid
import Testing.Conformance
import Testing.Conformance.Jobs as JobsConformance
import Jobs
import Jobs.Hangfire as Hangfire
import Jobs.Hangfire.Native as Native

// ─── Fixtures ─────────────────────────────────────────────────────────────────
func databaseDir(): String {
  val dir = Env.getVarOrDefault("LYRIC_HANGFIRE_SQLITE_DIR", "")
  if dir.length == 0 {
    panic(
      "LYRIC_HANGFIRE_SQLITE_DIR is not set: the integration tests need a writable directory for SQLite database files, for example LYRIC_HANGFIRE_SQLITE_DIR=/tmp/hangfire-it (see scripts/ci/start-hangfire-sqlite.sh)"
    )
  }
  dir
}

func newDatabasePath(): String {
  databaseDir() + "/lyric-hangfire-it-" + uuidToString(newUuid()) + ".db"
}

func open(path: in String): Hangfire.HangfireStorage {
  match Hangfire.sqliteStorage(path, 100) {
    case Ok(st) -> st
    case Err(e) -> panic(e + " (is the native library e_sqlite3 on the library search path?)")
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
  statusName(Hangfire.awaitFinished(s, jobId, 60000))
}

func attemptsOf(s: in Hangfire.HangfireJobScheduler, jobId: in String): slice[Jobs.JobResult] {
  match s.results(jobId) {
    case Ok(list) -> list.items
    case Err(e) -> panic(e)
  }
}

func eventually(ms: in Int, cond: in () -> Bool): Bool {
  var waited = 0
  while waited < ms {
    if cond() {
      return true
    }
    Jobs.sleepMs(20)
    waited = waited + 20
  }
  cond()
}

protected type Log {
  var calls: Int = 0
  var lines: List[String] = newList()
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
}

record Echo {
  log: Log
}

impl Jobs.JobHandler for Echo {
  func handle(spec: in Jobs.JobSpec): Result[String, Jobs.JobError] {
    val _ = self.log.record(spec.id + "~" + spec.payload)
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

// What is recorded about `jobId`, for a failure message.
func describe(s: in Hangfire.HangfireJobScheduler, jobId: in String, log: in Log): String {
  var text = "handler calls=" + log.count().toString()
  match Native.hangfireState(s, jobId) {
    case Ok(name) -> {
      text = text + " state=" + name
    }
    case Err(e) -> {
      text = text + " state=Err(" + e + ")"
    }
  }
  match s.results(jobId) {
    case Err(e) -> {
      text = text + " results=Err(" + e + ")"
    }
    case Ok(list) -> {
      for r in list.items {
        text = text + " [" + r.output + r.error + "]"
      }
    }
  }
  text
}

// ─── Tests ────────────────────────────────────────────────────────────────────
func sqliteClient(): Jobs.JobScheduler {
  val s = client(testConfig(), open(newDatabasePath()))
  val iface: Jobs.JobScheduler = s
  return iface
}

test "a scheduler on a SQLite storage conforms to JobScheduler" {
  requireConformance(JobsConformance.run({ -> sqliteClient() }))
}

test "jobs on a SQLite storage run, retry, fail and are cancelled" {
  val s = client(testConfig(), open(newDatabasePath()))
  val echo = Log()
  val flaky = Log()
  Hangfire.registerHandler(s, "echo", Echo(log = echo))
  Hangfire.registerHandler(s, "flaky", Flaky(log = flaky, failures = 2))
  val doomed = id(s.enqueue("echo", "doomed", 1, 30000))
  cancelJob(s, doomed)
  begin(s)
  val payload = "line one\nline two \"quoted\" | % é 日本"
  val good = id(s.enqueue("echo", payload, 1, 30000))
  val recovering = id(s.enqueue("flaky", "", 3, 30000))
  val nobody = id(s.enqueue("nobody-home", "", 3, 30000))
  assertEqual(finished(s, good), "Succeeded", "a job succeeds")
  assertEqual(attemptsOf(s, good)[0].output, "echo:" + payload, "its output is read back unchanged")
  val recovered = finished(s, recovering)
  assertEqual(recovered, "Succeeded", "a flaky job recovers: " + describe(s, recovering, flaky))
  val rs = attemptsOf(s, recovering)
  assertEqualInt(rs.length, 3, "two failures and a success")
  assertEqual(rs[0].error, "boom 1", "the first failure")
  assertEqual(rs[2].output, "recovered on call 3", "the success")
  assertEqual(finished(s, nobody), "Failed", "a job without a handler fails")
  assertEqualInt(attemptsOf(s, nobody).length, 1, "once")
  assertEqual(statusName(s.status(doomed)), "Cancelled", "the cancelled job stays Cancelled")
  assertEqualInt(echo.count(), 1, "and never ran")
  val delayed = id(Native.enqueueIn(s, "echo", "later", 1000i64, 1, 30000))
  val after = id(Native.enqueueAfter(s, delayed, "echo", "after", 1, 30000))
  assertEqual(finished(s, delayed), "Succeeded", "a delayed job runs")
  assertEqual(finished(s, after), "Succeeded", "and its continuation after it")
  match Native.counts(s) {
    case Err(e) -> panic(e)
    case Ok(c) -> {
      assertEqualLong(c.succeeded, 4i64, "Hangfire counts four succeeded jobs")
      assertEqualLong(c.failed, 1i64, "one failed")
      assertEqualLong(c.deleted, 1i64, "and one deleted")
    }
  }
  stop(s)
}

test "a job that is being retried is never reported Failed" {
  val s = client(testConfig(), open(newDatabasePath()))
  val log = Log()
  Hangfire.registerHandler(s, "flaky", Flaky(log = log, failures = 15))
  begin(s)
  val job = id(s.enqueue("flaky", "", 16, 30000))
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

test "a recurring job on a SQLite storage fires, keeps its results and is cancelled" {
  val s = client(testConfig(), open(newDatabasePath()))
  val log = Log()
  Hangfire.registerHandler(s, "tick", Echo(log = log))
  begin(s)
  val job = id(Native.scheduleCron(s, "tick", "p", "* * * * * *", 1, 30000))
  assertTrue(eventually(30000, { -> attemptsOf(s, job).length >= 2 }), "the job fires at least twice")
  assertEqual(attemptsOf(s, job)[0].output, "echo:p", "the output of a firing")
  cancelJob(s, job)
  assertEqual(statusName(s.status(job)), "Cancelled", "Cancelled after cancel")
  Jobs.sleepMs(1500)
  val settled = log.count()
  Jobs.sleepMs(2500)
  assertEqualInt(log.count(), settled, "no firing after cancel")
  stop(s)
}

test "jobs, schedules and results survive closing and reopening the storage" {
  val path = newDatabasePath()

  // First life: submit, without ever running a server.
  val first = client(testConfig(), open(path))
  val queued = id(first.enqueue("echo", "queued before the restart", 1, 30000))
  val cancelled = id(first.enqueue("echo", "cancelled before the restart", 1, 30000))
  val yearly = id(first.schedule("echo", "recurring", "0 0 1 1 *", 1, 30000))
  cancelJob(first, cancelled)

  // Second life: a new storage object on the same file, and a new scheduler.
  val second = client(testConfig(), open(path))
  assertEqual(statusName(second.status(queued)), "Pending", "the queued job is still there")
  assertEqual(statusName(second.status(cancelled)), "Cancelled", "the cancelled job is still Cancelled")
  assertEqual(statusName(second.status(yearly)), "Pending", "the recurring job is still scheduled")
  val log = Log()
  Hangfire.registerHandler(second, "echo", Echo(log = log))
  begin(second)
  assertEqual(finished(second, queued), "Succeeded", "the job queued in the first life runs in the second")
  assertEqual(log.line(0), queued + "~queued before the restart", "with its payload")
  val fired = id(Native.triggerNow(second, yearly))
  assertEqual(finished(second, fired), "Succeeded", "the recurring job of the first life can be fired")
  stop(second)

  // Third life: only reading.
  val third = client(testConfig(), open(path))
  assertEqual(statusName(third.status(queued)), "Succeeded", "the outcome is still there")
  assertEqual(attemptsOf(third, queued)[0].output, "echo:queued before the restart", "and so is the result")
  assertEqualInt(attemptsOf(third, yearly).length, 1, "the recurring job's firing too")
  assertEqual(attemptsOf(third, yearly)[0].output, "echo:recurring", "with its output")
  cancelJob(third, yearly)
  val fourth = client(testConfig(), open(path))
  assertEqual(statusName(fourth.status(yearly)), "Cancelled", "a cancel is stored as well")
}
