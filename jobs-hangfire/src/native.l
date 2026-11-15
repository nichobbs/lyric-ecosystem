//! Jobs.Hangfire.Native — what Hangfire can do beyond `Jobs.JobScheduler`.
//!
//! Every function takes a `Jobs.Hangfire` scheduler and works on its storage
//! and queue.  The job ids returned work with `status`, `results` and
//! `cancel` like any other.
//!
//!   import Jobs.Hangfire as Hangfire
//!   import Jobs.Hangfire.Native as Native
//!
//!   val later = Native.enqueueIn(sched, "send-email", "{}", 60000i64, 3, 30000)?
//!   val after = Native.enqueueAfter(sched, later, "audit", "{}", 1, 30000)?
//!   val tick = Native.scheduleCron(sched, "poll", "{}", "*/10 * * * * *", 1, 5000)?
@runtime_checked
@io
package Jobs.Hangfire.Native

import Std.Core
import Std.Collections
import Jobs.Hangfire
import Jobs.Hangfire.Store
import Jobs.Hangfire.Kernel.Dotnet as Kernel

/// Run job `name` once, `delayMs` from now.  The job is `Scheduled` in
/// Hangfire (`Pending`) until a server finds it due; Hangfire keeps the due
/// time in whole seconds, and servers look every
/// `HangfireConfig.schedulePollingIntervalMs`.
pub func enqueueIn(
  s: in Hangfire.HangfireJobScheduler,
  name: in String,
  payload: in String,
  delayMs: in Long,
  maxAttempts: in Int,
  timeoutMs: in Int,
): Result[String, String]
  requires: name.length > 0
  requires: delayMs >= 0i64
  requires: maxAttempts >= 1
  requires: timeoutMs >= 1
{
  Store.submit(s.storage.native, s.config.queue, name, payload, maxAttempts, timeoutMs, delayMs, "")
}

/// Run job `name` once job `parentJobId` has succeeded (a Hangfire
/// continuation).  The job is `Awaiting` (`Pending`) until then; when the
/// parent fails or is cancelled it stays so.  `Err` when `parentJobId` is a
/// recurring job or is not known.
pub func enqueueAfter(
  s: in Hangfire.HangfireJobScheduler,
  parentJobId: in String,
  name: in String,
  payload: in String,
  maxAttempts: in Int,
  timeoutMs: in Int,
): Result[String, String]
  requires: parentJobId.length > 0
  requires: name.length > 0
  requires: maxAttempts >= 1
  requires: timeoutMs >= 1
{
  if Store.isRecurringId(parentJobId) {
    return Err("hangfire: a recurring job has no end to continue from: " + parentJobId)
  }
  val _ = Store.rawState(s.storage.native, parentJobId)?
  Store.submit(s.storage.native, s.config.queue, name, payload, maxAttempts, timeoutMs, 0i64, parentJobId)
}

/// Schedule job `name` on a Hangfire cron expression, which Hangfire itself
/// validates: five fields, or six with seconds first
/// (`*/10 * * * * *`), with names (`MON`, `JAN`), `L`, `W`, `#` and `?`, and
/// the macros (`@hourly`).  Evaluated in UTC.
pub func scheduleCron(
  s: in Hangfire.HangfireJobScheduler,
  name: in String,
  payload: in String,
  cron: in String,
  maxAttempts: in Int,
  timeoutMs: in Int,
): Result[String, String]
  requires: name.length > 0
  requires: cron.length > 0
  requires: maxAttempts >= 1
  requires: timeoutMs >= 1
{
  Store.submitRecurring(s.storage.native, s.config.queue, name, payload, cron, maxAttempts, timeoutMs)
}

/// Fire recurring job `recurringJobId` now, besides its schedule.  Returns
/// Hangfire's id of the background job created.  `Err` when the id is not a
/// recurring job, or the job has been cancelled.
pub func triggerNow(s: in Hangfire.HangfireJobScheduler, recurringJobId: in String): Result[String, String]
  requires: recurringJobId.length > 0
{
  if not Store.isRecurringId(recurringJobId) {
    return Err("hangfire: not a recurring job: " + recurringJobId)
  }
  match Kernel.triggerRecurring(s.storage.native, recurringJobId) {
    case Err(e) -> Err("hangfire: cannot trigger job " + recurringJobId + ": " + e)
    case Ok(None) -> Err("job not found: " + recurringJobId)
    case Ok(Some(jobId)) -> Ok(jobId)
  }
}

/// Hangfire's ids of the background jobs that recurring job
/// `recurringJobId` has been run as, oldest first (at most the
/// `HangfireConfig.maxResultsPerJob` most recent).  A firing is listed once
/// a worker has started it.
pub func firings(s: in Hangfire.HangfireJobScheduler, recurringJobId: in String): Result[slice[String], String]
  requires: recurringJobId.length > 0
{
  val ids = Store.firingIds(s.storage.native, recurringJobId, s.config.maxResultsPerJob)?
  Ok(ids.toArray())
}

/// Hangfire's own state name for background job `jobId` (a one-off job id,
/// or an id from `firings` or `triggerNow`): `Enqueued`, `Scheduled`,
/// `Awaiting`, `Processing`, `Succeeded`, `Failed` or `Deleted`.
pub func hangfireState(s: in Hangfire.HangfireJobScheduler, jobId: in String): Result[String, String]
  requires: jobId.length > 0
{
  Store.rawState(s.storage.native, jobId)
}

/// Hangfire's job counts for the scheduler's storage.  `enqueued` counts the
/// scheduler's own queue; the others count the whole storage, whichever
/// queue or scheduler the jobs belong to.
pub record JobCounts {
  enqueued: Long
  scheduled: Long
  processing: Long
  succeeded: Long
  failed: Long
  deleted: Long
}

pub func counts(s: in Hangfire.HangfireJobScheduler): Result[JobCounts, String] {
  match Kernel.storageCounts(s.storage.native, s.config.queue) {
    case Err(e) -> Err("hangfire: " + e)
    case Ok(c) -> Ok(
      JobCounts(
        enqueued = c.enqueued,
        scheduled = c.scheduled,
        processing = c.processing,
        succeeded = c.succeeded,
        failed = c.failed,
        deleted = c.deleted
      )
    )
  }
}
