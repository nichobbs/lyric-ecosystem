//! tests/integration/postgres_handover_read_tests.l — second half of the
//! persistence check across processes: this process finds the jobs that
//! `postgres_handover_write_tests.l` submitted in an earlier process, and
//! runs them.
@cfg(feature = "integration")
@test_module
package Jobs.Hangfire.PostgresHandoverReadTests

import Std.Core
import Std.Collections
import Std.Environment as Env
import Std.String as Str
import Std.Testing
import Jobs
import Jobs.Hangfire as Hangfire
import Jobs.Hangfire.Native as Native
import Jobs.Hangfire.Kernel.Dotnet as Kernel

// The schema both handover modules use, and the sorted set in it where the
// writer leaves the ids for the reader: one entry per run of the writer,
// "<queued>,<cancelled>,<recurring>" with score -<epoch ms>, so that the
// lowest score is the latest run.
val handoverSchema: String = "lyric_it_handover"

val handoverKey: String = "lyric-it:handover"

func connectionString(): String {
  val value = Env.getVarOrDefault("LYRIC_HANGFIRE_POSTGRES", "")
  if value.length == 0 {
    panic(
      "LYRIC_HANGFIRE_POSTGRES is not set: the integration tests need a PostgreSQL database (see scripts/ci/start-hangfire-postgres.sh)"
    )
  }
  value
}

func scheduler(): Hangfire.HangfireJobScheduler {
  val storage = match Hangfire.postgresStorage(connectionString(), Hangfire.defaultPostgresOptions().copy(schemaName = handoverSchema, queuePollIntervalMs = 100)) {
    case Ok(st) -> st
    case Err(e) -> panic(e)
  }
  match Hangfire.create(Hangfire.defaultConfig().copy(schedulePollingIntervalMs = 100), storage) {
    case Ok(s) -> s
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

protected type Log {
  var lines: List[String] = newList()
  entry record(line: in String) {
    lines.add(line)
  }
  entry count(): Int {
    return lines.count
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
    self.log.record(spec.id + "~" + spec.payload)
    Ok("echo:" + spec.payload)
  }
}

func finished(s: in Hangfire.HangfireJobScheduler, jobId: in String): String {
  statusName(Hangfire.awaitFinished(s, jobId, 60000))
}

test "another process finds the jobs, runs them and sees their results" {
  val s = scheduler()
  val entry = match Kernel.setLowest(s.storage.native, handoverKey, -1.0e18, 0.0) {
    case Ok(Some(v)) -> v
    case Ok(None) -> panic(
      "nothing was handed over: postgres_handover_write_tests.l must run before this module, against the same database"
    )
    case Err(e) -> panic(e)
  }
  val ids = Str.split(entry, ",")
  assertEqualInt(ids.length, 3, "three ids were handed over: " + entry)
  val queued = ids[0]
  val cancelled = ids[1]
  val recurring = ids[2]
  assertEqual(statusName(s.status(queued)), "Pending", "the job the other process queued is still waiting")
  assertEqual(statusName(s.status(cancelled)), "Cancelled", "the job it cancelled is still Cancelled")
  assertEqual(statusName(s.status(recurring)), "Pending", "the job it scheduled is still scheduled")
  val log = Log()
  Hangfire.registerHandler(s, "handover", Echo(log = log))
  match Hangfire.start(s) {
    case Ok(_) -> ()
    case Err(e) -> panic(e)
  }
  assertEqual(finished(s, queued), "Succeeded", "this process runs the queued job")
  assertEqual(log.line(0), queued + "~queued by the first process", "with the payload the other process gave it")
  val fired = id(Native.triggerNow(s, recurring))
  assertEqual(finished(s, fired), "Succeeded", "and can fire the recurring job")
  match s.results(recurring) {
    case Ok(list) -> assertEqual(
      list.items[0].output,
      "echo:scheduled by the first process",
      "the recurring job's payload"
    )
    case Err(e) -> panic(e)
  }
  match s.cancel(recurring) {
    case Ok(_) -> ()
    case Err(e) -> panic(e)
  }
  assertEqualInt(log.count(), 2, "the cancelled job never ran")
  match Hangfire.shutdown(s, true) {
    case Ok(_) -> ()
    case Err(e) -> panic(e)
  }
}
