//! tests/conformance_tests.l — `Testing.Conformance.Jobs` against the
//! Hangfire backend.
//!
//! The suite checks submission, status, cancellation and cron validation, and
//! expects a newly enqueued job to be `Pending`.  It runs twice: against a
//! scheduler without a server (a Hangfire client), where that is exact, and
//! against a started scheduler whose only worker is held by a blocking job,
//! so the suite's jobs are really queued behind it in a live Hangfire server.
//! Handler execution is covered by `execution_tests.l`.
@test_module
package Jobs.Hangfire.ConformanceTests

import Std.Core
import Std.Collections
import Std.Testing
import Testing.Conformance
import Testing.Conformance.Jobs as JobsConformance
import Jobs
import Jobs.Hangfire as Hangfire

func newScheduler(config: in Hangfire.HangfireConfig): Hangfire.HangfireJobScheduler {
  val storage = match Hangfire.inMemoryStorage() { case Ok(st) -> st; case Err(e) -> panic(e) }
  match Hangfire.create(config, storage) {
    case Ok(s) -> s
    case Err(e) -> panic(e)
  }
}

func shutdownAll(made: in List[Hangfire.HangfireJobScheduler]): Unit {
  for s in made {
    match Hangfire.shutdown(s, true) {
      case Ok(_) -> ()
      case Err(e) -> panic(e)
    }
  }
}

func clientScheduler(made: in List[Hangfire.HangfireJobScheduler]): Jobs.JobScheduler {
  val s = newScheduler(Hangfire.defaultConfig())
  made.add(s)
  val iface: Jobs.JobScheduler = s
  return iface
}

test "a Hangfire scheduler without a server conforms to JobScheduler" {
  val made: List[Hangfire.HangfireJobScheduler] = newList()
  val checks = JobsConformance.run({ -> clientScheduler(made) })
  shutdownAll(made)
  requireConformance(checks)
}

ref record Gate {
  var open: Bool
}

record BlockUntilOpen {
  gate: Gate
}

impl Jobs.JobHandler for BlockUntilOpen {
  func handle(spec: in Jobs.JobSpec): Result[String, Jobs.JobError] {
    while not self.gate.open {
      Jobs.sleepMs(5)
    }
    Ok("released")
  }
}

func isRunning(r: in Result[Jobs.JobStatus, String]): Bool {
  match r {
    case Ok(st) -> {
      match st {
        case Jobs.JobStatus.Running -> true
        case _ -> false
      }
    }
    case Err(_) -> false
  }
}

func busyScheduler(made: in List[Hangfire.HangfireJobScheduler], gate: in Gate): Jobs.JobScheduler {
  val s = newScheduler(Hangfire.defaultConfig().copy(workerCount = 1))
  made.add(s)
  Hangfire.registerHandler(s, "hold-the-worker", BlockUntilOpen(gate = gate))
  match Hangfire.start(s) {
    case Ok(_) -> ()
    case Err(e) -> panic(e)
  }
  match s.enqueue("hold-the-worker", "", 1, 600000) {
    case Err(e) -> panic(e)
    case Ok(id) -> {
      var waited = 0
      while waited < 10000 and not isRunning(s.status(id)) {
        Jobs.sleepMs(5)
        waited = waited + 5
      }
      assertTrue(isRunning(s.status(id)), "the blocking job holds the only worker")
    }
  }
  val iface: Jobs.JobScheduler = s
  return iface
}

test "a started Hangfire scheduler with a busy worker conforms to JobScheduler" {
  val made: List[Hangfire.HangfireJobScheduler] = newList()
  val gate = Gate(open = false)
  val checks = JobsConformance.run({ -> busyScheduler(made, gate) })
  gate.open = true
  shutdownAll(made)
  requireConformance(checks)
}
