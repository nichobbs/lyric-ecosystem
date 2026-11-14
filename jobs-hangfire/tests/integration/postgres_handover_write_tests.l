//! tests/integration/postgres_handover_write_tests.l — first half of the
//! persistence check across processes.
//!
//! `lyric test` runs every test module as a program of its own.  This module
//! submits jobs to a PostgreSQL storage without ever starting a server and
//! exits; `postgres_handover_read_tests.l`, the next module in the manifest
//! and so the next process, finds them and runs them.
@cfg(feature = "integration")
@test_module
package Jobs.Hangfire.PostgresHandoverWriteTests

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

test "a process submits jobs and exits without running them" {
  val s = scheduler()
  val queued = id(s.enqueue("handover", "queued by the first process", 2, 30000))
  val cancelled = id(s.enqueue("handover", "cancelled by the first process", 1, 30000))
  val recurring = id(s.schedule("handover", "scheduled by the first process", "0 0 1 1 *", 1, 30000))
  match s.cancel(cancelled) {
    case Ok(_) -> ()
    case Err(e) -> panic(e)
  }
  assertEqual(statusName(s.status(queued)), "Pending", "the queued job waits")
  match Kernel.setAdd(s.storage.native, handoverKey, queued + "," + cancelled + "," + recurring, 0.0 - Jobs.nowMs().toDouble()) {
    case Ok(_) -> ()
    case Err(e) -> panic(e)
  }
  assertFalse(Hangfire.isRunning(s), "this process never had a server")
}
