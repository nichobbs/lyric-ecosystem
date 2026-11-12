//! tests/conformance_tests.l — `Testing.Conformance.Jobs` against the
//! Quartz.NET backend.
//!
//! The suite checks submission, status, cancellation and cron validation, and
//! expects a newly enqueued job to be `Pending`.  It runs twice: against a
//! scheduler in standby, where that is exact, and against a started scheduler
//! whose only worker thread is held by a blocking job, so the suite's jobs
//! are really queued behind it in a live Quartz.NET instance.  Handler execution
//! is covered by `execution_tests.l`.
@test_module
package Jobs.QuartzNet.ConformanceTests

import Std.Core
import Std.Collections
import Std.Testing
import Testing.Conformance
import Testing.Conformance.Jobs as JobsConformance
import Jobs
import Jobs.QuartzNet as Quartz

func testConfig(): Quartz.QuartzConfig {
  Quartz.defaultConfig()
}

func newScheduler(config: in Quartz.QuartzConfig): Quartz.QuartzJobScheduler {
  match Quartz.create(config) {
    case Ok(s) -> s
    case Err(e) -> panic(e)
  }
}

func shutdownAll(made: in List[Quartz.QuartzJobScheduler]): Unit {
  for s in made {
    match Quartz.shutdown(s, false) {
      case Ok(_) -> ()
      case Err(e) -> panic(e)
    }
  }
}

func standbyScheduler(made: in List[Quartz.QuartzJobScheduler]): Jobs.JobScheduler {
  val s = newScheduler(testConfig())
  made.add(s)
  val iface: Jobs.JobScheduler = s
  return iface
}

test "a Quartz.NET scheduler in standby conforms to JobScheduler" {
  val made: List[Quartz.QuartzJobScheduler] = newList()
  val checks = JobsConformance.run({ -> standbyScheduler(made) })
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

func busyScheduler(made: in List[Quartz.QuartzJobScheduler], gate: in Gate): Jobs.JobScheduler {
  val s = newScheduler(testConfig().copy(workerThreads = 1))
  made.add(s)
  Quartz.registerHandler(s, "hold-the-worker", BlockUntilOpen(gate = gate))
  match Quartz.start(s) {
    case Ok(_) -> ()
    case Err(e) -> panic(e)
  }
  match s.enqueue("hold-the-worker", "", 1, 600000) {
    case Err(e) -> panic(e)
    case Ok(id) -> {
      var waited = 0
      while waited < 5000 and Quartz.timesFired(s, id) == 0 {
        Jobs.sleepMs(5)
        waited = waited + 5
      }
      assertTrue(Quartz.timesFired(s, id) == 1, "the blocking job holds the only worker thread")
    }
  }
  val iface: Jobs.JobScheduler = s
  return iface
}

test "a started Quartz.NET scheduler with a busy worker conforms to JobScheduler" {
  val made: List[Quartz.QuartzJobScheduler] = newList()
  val gate = Gate(open = false)
  val checks = JobsConformance.run({ -> busyScheduler(made, gate) })
  gate.open = true
  shutdownAll(made)
  requireConformance(checks)
}
