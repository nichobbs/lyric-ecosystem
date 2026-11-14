//! tests/readme_examples_tests.l — the code shown in the README, compiled
//! and run, so that it cannot drift from the package.
@test_module
package Jobs.Hangfire.ReadmeExamplesTests

import Std.Core
import Std.Testing
import Jobs
import Jobs.Hangfire as Hangfire
import Jobs.Hangfire.Native as Native

record SendEmail {
  tag: Int
}

impl Jobs.JobHandler for SendEmail {
  func handle(spec: in Jobs.JobSpec): Result[String, Jobs.JobError] {
    if spec.payload.length == 0 {
      return Err(Jobs.JobError(message = "empty payload", code = "bad-payload"))
    }
    Ok("sent " + spec.payload)
  }
}

func run(): Result[Jobs.JobStatus, String] {
  val storage = Hangfire.inMemoryStorage()?
  val sched = Hangfire.create(Hangfire.defaultConfig().copy(workerCount = 8), storage)?
  Hangfire.registerHandler(sched, "send-email", SendEmail(tag = 0))
  Hangfire.start(sched)?

  // JobScheduler: run once now, with up to 3 attempts of at most 30 s each.
  val once = sched.enqueue("send-email", "{\"to\":\"a@example.com\"}", 3, 30000)?
  // JobScheduler: 5-field Unix cron, 03:00 UTC Monday to Friday.
  val nightly = sched.schedule("send-email", "{}", "0 3 * * 1-5", 1, 30000)?

  val outcome = Hangfire.awaitFinished(sched, once, 60000)?
  val attempts = sched.results(once)?
  for r in attempts.items {
    println(r.output + r.error)
  }
  sched.cancel(nightly)?
  Hangfire.shutdown(sched, true)?
  Ok(outcome)
}

func viaInterface(jobs: in Jobs.JobScheduler): Result[String, String] {
  Jobs.enqueue(jobs, "send-email", "{}")
}

record Export {
  sched: Hangfire.HangfireJobScheduler
  rows: Int
}

impl Jobs.JobHandler for Export {
  func handle(spec: in Jobs.JobSpec): Result[String, Jobs.JobError] {
    var done = 0
    while done < self.rows {
      if Hangfire.stopRequested(self.sched, spec.id) {
        return Err(Jobs.JobError(message = "stopped after " + done.toString() + " rows", code = "stopped"))
      }
      Jobs.sleepMs(1)
      done = done + 1
    }
    Ok(done.toString() + " rows")
  }
}

func hangfireOnly(sched: in Hangfire.HangfireJobScheduler): Result[Unit, String] {
  val inOneMinute = Native.enqueueIn(sched, "send-email", "{}", 60000i64, 3, 30000)?
  val afterwards = Native.enqueueAfter(sched, inOneMinute, "send-email", "{\"audit\":true}", 1, 30000)?
  val everyTenSeconds = Native.scheduleCron(sched, "send-email", "{}", "*/10 * * * * *", 1, 30000)?
  val firedNow = Native.triggerNow(sched, everyTenSeconds)?
  val counts = Native.counts(sched)?
  println(afterwards + " " + firedNow + " " + counts.scheduled.toString())
  sched.cancel(everyTenSeconds)?
  Ok(())
}

func statusName(st: in Jobs.JobStatus): String {
  match st {
    case Jobs.JobStatus.Pending -> "Pending"
    case Jobs.JobStatus.Running -> "Running"
    case Jobs.JobStatus.Succeeded -> "Succeeded"
    case Jobs.JobStatus.Failed -> "Failed"
    case Jobs.JobStatus.Cancelled -> "Cancelled"
  }
}

test "the usage example runs" {
  match run() {
    case Ok(st) -> assertEqual(statusName(st), "Succeeded", "the job succeeded")
    case Err(e) -> panic(e)
  }
}

func scheduler(): Hangfire.HangfireJobScheduler {
  val storage = match Hangfire.inMemoryStorage() { case Ok(st) -> st; case Err(e) -> panic(e) }
  match Hangfire.create(Hangfire.defaultConfig(), storage) {
    case Ok(s) -> s
    case Err(e) -> panic(e)
  }
}

test "the interface, cooperative-stop and Hangfire-only examples run" {
  val sched = scheduler()
  Hangfire.registerHandler(sched, "send-email", SendEmail(tag = 0))
  Hangfire.registerHandler(sched, "export", Export(sched = sched, rows = 50))
  match Hangfire.start(sched) {
    case Ok(_) -> ()
    case Err(e) -> panic(e)
  }
  val iface: Jobs.JobScheduler = sched
  val viaIface = match viaInterface(iface) { case Ok(id) -> id; case Err(e) -> panic(e) }
  match Hangfire.awaitFinished(sched, viaIface, 30000) {
    case Ok(st) -> assertEqual(statusName(st), "Succeeded", "the job submitted through the interface")
    case Err(e) -> panic(e)
  }
  val export = match sched.enqueue("export", "", 1, 30000) { case Ok(id) -> id; case Err(e) -> panic(e) }
  match Hangfire.awaitFinished(sched, export, 30000) {
    case Ok(st) -> assertEqual(statusName(st), "Succeeded", "the export ran to its end")
    case Err(e) -> panic(e)
  }
  match sched.results(export) {
    case Ok(list) -> assertEqual(list.items[0].output, "50 rows", "the export's output")
    case Err(e) -> panic(e)
  }
  match hangfireOnly(sched) {
    case Ok(_) -> ()
    case Err(e) -> panic(e)
  }
  match Hangfire.shutdown(sched, true) {
    case Ok(_) -> ()
    case Err(e) -> panic(e)
  }
}
