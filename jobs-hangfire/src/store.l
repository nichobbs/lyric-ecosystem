//! Jobs.Hangfire.Store — what this backend keeps in a Hangfire storage, and
//! how the interface's statuses and results are read back from it.
//!
//! Nothing here is held in process memory: a job, its state, its attempts
//! and its results live in the Hangfire storage, so they are seen by every
//! process that uses the same storage and outlive the process when the
//! storage does.  `Jobs.Hangfire` and `Jobs.Hangfire.Native` are the public
//! API; this package is their shared lower layer.
//!
//! One-off job.  The job id is Hangfire's own background-job id.  Its status
//! is Hangfire's state:
//!
//!   Enqueued, Scheduled, Awaiting   Pending
//!   Processing                      Running
//!   Succeeded                       Succeeded
//!   Failed                          Failed
//!   Deleted                         Cancelled
//!
//! Its attempts are two job parameters: `lyric.attempts` (how many attempts
//! have run) and `lyric.results` (one line per attempt, the most recent
//! `keep`).  The kernel adds two more: `lyric.queue` (the queue the job was
//! submitted to) and `lyric.retry` (whether the failure being reported is
//! to be retried).
//!
//! Recurring job.  The job id is `lyric-recurring-<uuid>`, which is also the
//! id of the Hangfire recurring job.  Hangfire creates a new background job
//! for every firing, so two sorted sets tie them to the id:
//!
//!   lyric:job:<id>       "scheduled" (score 1), and "cancelled" (score 0)
//!                        once it has been cancelled
//!   lyric:firings:<id>   "<ms>|<background job id>" with score -<ms>, the
//!                        most recent `keep` firings
//!
//! A firing's background job carries the parameter `lyric.firing` once it is
//! in that set, and its attempts as any one-off job does.
//!
//! Only string-valued storage calls are used (`GetJobParameter`,
//! `GetFirstByLowestScoreFromSet`, `AddToSet`), which every Hangfire storage
//! implements.
@runtime_checked
@io
package Jobs.Hangfire.Store

import Std.Core
import Std.Collections
import Std.Parse
import Std.Uuid
import Std.String as Str
import Jobs
import Jobs.Hangfire.Kernel.Dotnet as Kernel

val recurringPrefix: String = "lyric-recurring-"

val attemptsParameter: String = "lyric.attempts"

val resultsParameter: String = "lyric.results"

val firingParameter: String = "lyric.firing"

val markerScheduled: String = "scheduled"

val markerCancelled: String = "cancelled"

// How many of a recurring job's most recent firings `status` looks at to
// decide between `Running` and `Pending`.
val firingsWatched: Int = 8

/// Whether `id` names a recurring job (an id `submitRecurring` returned).
pub func isRecurringId(id: in String): Bool {
  Str.startsWith(id, recurringPrefix)
}

func markerKey(recurringId: in String): String {
  "lyric:job:" + recurringId
}

func firingsKey(recurringId: in String): String {
  "lyric:firings:" + recurringId
}

func notFound(id: in String): String {
  "job not found: " + id
}

// ─── Submission ───────────────────────────────────────────────────────────────
/// Create a one-off job; see `Kernel.createJob` for `delayMs` and `parentId`.
pub func submit(
  storage: in Kernel.NStorage,
  queue: in String,
  name: in String,
  payload: in String,
  maxAttempts: in Int,
  timeoutMs: in Int,
  delayMs: in Long,
  parentId: in String,
): Result[String, String]
  requires: name.length > 0
  requires: maxAttempts >= 1
  requires: timeoutMs >= 1
  requires: delayMs >= 0i64
{
  match Kernel.createJob(storage, queue, name, payload, maxAttempts, timeoutMs, delayMs, parentId) {
    case Ok(id) -> Ok(id)
    case Err(e) -> Err("hangfire: cannot create job '" + name + "': " + e)
  }
}

/// Create a recurring job on the Hangfire cron expression `cron`, whose
/// firings go to `queue`.
pub func submitRecurring(
  storage: in Kernel.NStorage,
  queue: in String,
  name: in String,
  payload: in String,
  cron: in String,
  maxAttempts: in Int,
  timeoutMs: in Int,
): Result[String, String]
  requires: name.length > 0
  requires: maxAttempts >= 1
  requires: timeoutMs >= 1
{
  val id = recurringPrefix + uuidToString(newUuid())
  // The marker goes in first: a firing may be created as soon as Hangfire
  // has the recurring job.
  match Kernel.setAdd(storage, markerKey(id), markerScheduled, 1.0) {
    case Err(e) -> return Err("hangfire: cannot schedule job '" + name + "': " + e)
    case Ok(_) -> ()
  }
  match Kernel.addRecurring(storage, queue, id, cron, name, payload, maxAttempts, timeoutMs) {
    case Ok(_) -> Ok(id)
    case Err(e) -> {
      val _ = Kernel.setRemove(storage, markerKey(id), markerScheduled)
      Err("hangfire: cannot schedule job '" + name + "' on '" + cron + "': " + e)
    }
  }
}

// ─── Recurring jobs: marker and firings ───────────────────────────────────────
// "cancelled" (score 0) sorts before "scheduled" (score 1).
func marker(storage: in Kernel.NStorage, recurringId: in String): Result[Option[String], String] {
  match Kernel.setLowest(storage, markerKey(recurringId), 0.0, 1.0) {
    case Ok(found) -> Ok(found)
    case Err(e) -> Err("hangfire: " + e)
  }
}

/// Whether recurring job `recurringId` has been cancelled.  False for an id
/// the storage does not know.
pub func recurringCancelled(storage: in Kernel.NStorage, recurringId: in String): Result[Bool, String] {
  match marker(storage, recurringId)? {
    case Some(m) -> Ok(m == markerCancelled)
    case None -> Ok(false)
  }
}

func entryMs(entry: in String): Long {
  match Str.indexOf(entry, "|") {
    case None -> 0i64
    case Some(at) -> {
      match parseOptLong(Str.substring(entry, 0, at)) {
        case Some(ms) -> ms
        case None -> 0i64
      }
    }
  }
}

func entryJobId(entry: in String): String {
  match Str.indexOf(entry, "|") {
    case None -> entry
    case Some(at) -> Str.substring(entry, at + 1)
  }
}

// The entries of `lyric:firings:<id>`, most recent first, at most `limit`.
// Every entry has its own whole-number score, so the next one is found by
// asking for the lowest score above the last.
func firingEntries(storage: in Kernel.NStorage, recurringId: in String, limit: in Int): Result[List[String], String] {
  val entries: List[String] = newList()
  var fromScore = -1.0e18
  var more = true
  while more and entries.count < limit {
    match Kernel.setLowest(storage, firingsKey(recurringId), fromScore, 0.0) {
      case Err(e) -> return Err("hangfire: " + e)
      case Ok(None) -> {
        more = false
      }
      case Ok(Some(entry)) -> {
        entries.add(entry)
        fromScore = 1.0 - entryMs(entry).toDouble()
      }
    }
  }
  Ok(entries)
}

/// Hangfire's ids of the background jobs created for recurring job
/// `recurringId`, oldest first; at most the `keep` most recent that
/// `noteFiring` kept.
pub func firingIds(storage: in Kernel.NStorage, recurringId: in String, keep: in Int): Result[List[String], String]
  requires: keep >= 1
{
  val entries = firingEntries(storage, recurringId, keep)?
  val ids: List[String] = newList()
  var i = entries.count - 1
  while i >= 0 {
    ids.add(entryJobId(entries[i]))
    i = i - 1
  }
  Ok(ids)
}

func noteFiringLocked(
  storage: in Kernel.NStorage,
  recurringId: in String,
  jobId: in String,
  createdAtMs: in Long,
  keep: in Int,
): Result[Unit, String] {
  val key = firingsKey(recurringId)
  var ms = if createdAtMs > 0i64 { createdAtMs } else { 1i64 }
  var taken = true
  while taken {
    match Kernel.setLowest(storage, key, 0.0 - ms.toDouble(), 0.0 - ms.toDouble())? {
      case Some(_) -> {
        ms = ms + 1i64
      }
      case None -> {
        taken = false
      }
    }
  }
  Kernel.setAdd(storage, key, ms.toString() + "|" + jobId, 0.0 - ms.toDouble())?
  Kernel.setJobParameter(storage, jobId, firingParameter, ms.toString())?
  val entries = firingEntries(storage, recurringId, keep + 64)?
  var i = keep
  while i < entries.count {
    Kernel.setRemove(storage, key, entries[i])?
    i = i + 1
  }
  Ok(())
}

/// Record that background job `jobId` is a firing of recurring job
/// `recurringId`.  Does nothing when it is recorded already (a later attempt
/// of the same firing).  Keeps the `keep` most recent firings.
pub func noteFiring(
  storage: in Kernel.NStorage,
  recurringId: in String,
  jobId: in String,
  createdAtMs: in Long,
  keep: in Int,
): Result[Unit, String]
  requires: keep >= 1
{
  match Kernel.jobParameter(storage, jobId, firingParameter) {
    case Err(e) -> return Err("hangfire: " + e)
    case Ok(Some(_)) -> return Ok(())
    case Ok(None) -> ()
  }
  // Two firings created in the same millisecond must not take the same score.
  match Kernel.locked(storage, firingsKey(recurringId), 15000, { -> noteFiringLocked(storage, recurringId, jobId, createdAtMs, keep) }) {
    case Ok(_) -> Ok(())
    case Err(e) -> Err("hangfire: cannot record the firing of " + recurringId + ": " + e)
  }
}

// ─── Attempts ─────────────────────────────────────────────────────────────────
func escapeText(s: in String): String {
  Str.replace(Str.replace(Str.replace(Str.replace(s, "%", "%25"), "\n", "%0A"), "\r", "%0D"), "|", "%7C")
}

func unescapeText(s: in String): String {
  Str.replace(Str.replace(Str.replace(Str.replace(s, "%7C", "|"), "%0D", "\r"), "%0A", "\n"), "%25", "%")
}

func parseLine(id: in String, line: in String): Option[Jobs.JobResult] {
  val parts = Str.split(line, "|")
  if parts.length != 4 {
    return None
  }
  val startedAt = match parseOptLong(parts[1]) { case Some(v) -> v; case None -> return None }
  val finishedAt = match parseOptLong(parts[2]) { case Some(v) -> v; case None -> return None }
  if finishedAt < startedAt {
    return None
  }
  val text = unescapeText(parts[3])
  if parts[0] == "S" {
    Some(
      value = Jobs.JobResult(
        jobId = id,
        status = Jobs.JobStatus.Succeeded,
        output = text,
        error = "",
        startedAt = startedAt,
        finishedAt = finishedAt
      )
    )
  } else {
    Some(
      value = Jobs.JobResult(
        jobId = id,
        status = Jobs.JobStatus.Failed,
        output = "",
        error = text,
        startedAt = startedAt,
        finishedAt = finishedAt
      )
    )
  }
}

func storedLines(storage: in Kernel.NStorage, jobId: in String): Result[List[String], String] {
  val lines: List[String] = newList()
  match Kernel.jobParameter(storage, jobId, resultsParameter) {
    case Err(e) -> return Err("hangfire: " + e)
    case Ok(None) -> ()
    case Ok(Some(text)) -> {
      if text.length > 0 {
        for line in Str.split(text, "\n") {
          lines.add(line)
        }
      }
    }
  }
  Ok(lines)
}

/// How many attempts of background job `jobId` have been recorded.
pub func attemptsOf(storage: in Kernel.NStorage, jobId: in String): Result[Int, String] {
  match Kernel.jobParameter(storage, jobId, attemptsParameter) {
    case Err(e) -> Err("hangfire: " + e)
    case Ok(None) -> Ok(0)
    case Ok(Some(text)) -> {
      match parseOptInt(text) {
        case Some(n) -> Ok(n)
        case None -> Ok(0)
      }
    }
  }
}

/// Record one attempt of background job `jobId`: `text` is the handler's
/// output when `ok`, the error otherwise.  The `keep` most recent attempts
/// are kept; the attempt count is not capped.
pub func recordAttempt(
  storage: in Kernel.NStorage,
  jobId: in String,
  ok: in Bool,
  text: in String,
  startedAt: in Long,
  finishedAt: in Long,
  keep: in Int,
): Result[Unit, String]
  requires: keep >= 1
{
  val lines = storedLines(storage, jobId)?
  val ended = if finishedAt >= startedAt { finishedAt } else { startedAt }
  lines.add((if ok { "S" } else { "F" }) + "|" + startedAt.toString() + "|" + ended.toString() + "|" + escapeText(text))
  while lines.count > keep {
    lines.removeAt(0)
  }
  val attempts = attemptsOf(storage, jobId)?
  match Kernel.setJobParameter(storage, jobId, resultsParameter, Str.joinList("\n", lines)) {
    case Err(e) -> return Err("hangfire: " + e)
    case Ok(_) -> ()
  }
  match Kernel.setJobParameter(storage, jobId, attemptsParameter, (attempts + 1).toString()) {
    case Err(e) -> Err("hangfire: " + e)
    case Ok(_) -> Ok(())
  }
}

// ─── Status, results, cancel ──────────────────────────────────────────────────
/// Hangfire's state name for background job `jobId`.
pub func rawState(storage: in Kernel.NStorage, jobId: in String): Result[String, String] {
  match Kernel.jobState(storage, jobId) {
    case Err(e) -> Err("hangfire: " + e)
    case Ok(None) -> Err(notFound(jobId))
    case Ok(Some(name)) -> Ok(name)
  }
}

func statusOfState(jobId: in String, name: in String): Result[Jobs.JobStatus, String] {
  // "" is a job Hangfire has created and not yet given its first state.
  if name == Kernel.stateEnqueued or name == Kernel.stateScheduled or name == Kernel.stateAwaiting or name.length == 0 {
    Ok(Jobs.JobStatus.Pending)
  } else if name == Kernel.stateProcessing {
    Ok(Jobs.JobStatus.Running)
  } else if name == Kernel.stateSucceeded {
    Ok(Jobs.JobStatus.Succeeded)
  } else if name == Kernel.stateFailed {
    Ok(Jobs.JobStatus.Failed)
  } else if name == Kernel.stateDeleted {
    Ok(Jobs.JobStatus.Cancelled)
  } else {
    Err("hangfire: job " + jobId + " is in the state '" + name + "', which has no JobStatus")
  }
}

func recurringStatus(storage: in Kernel.NStorage, id: in String): Result[Jobs.JobStatus, String] {
  match marker(storage, id)? {
    case None -> return Err(notFound(id))
    case Some(m) -> {
      if m == markerCancelled {
        return Ok(Jobs.JobStatus.Cancelled)
      }
    }
  }
  val recent = firingEntries(storage, id, firingsWatched)?
  for entry in recent {
    match Kernel.jobState(storage, entryJobId(entry)) {
      case Err(e) -> return Err("hangfire: " + e)
      case Ok(None) -> ()
      case Ok(Some(name)) -> {
        if name == Kernel.stateProcessing {
          return Ok(Jobs.JobStatus.Running)
        }
      }
    }
  }
  Ok(Jobs.JobStatus.Pending)
}

/// The interface status of job `id` (see the package comment).
pub func statusOf(storage: in Kernel.NStorage, id: in String): Result[Jobs.JobStatus, String] {
  if isRecurringId(id) {
    return recurringStatus(storage, id)
  }
  statusOfState(id, rawState(storage, id)?)
}

func appendResults(
  storage: in Kernel.NStorage, id: in String, jobId: in String, into: in List[Jobs.JobResult]
): Result[Unit, String] {
  for line in storedLines(storage, jobId)? {
    match parseLine(id, line) {
      case Some(r) -> into.add(r)
      case None -> ()
    }
  }
  Ok(())
}

/// One `JobResult` per recorded attempt of job `id`, oldest first, at most
/// the `keep` most recent.  For a recurring job, the attempts of its `keep`
/// most recent firings.
pub func resultsOf(storage: in Kernel.NStorage, id: in String, keep: in Int): Result[Jobs.JobResultList, String]
  requires: keep >= 1
{
  val found: List[Jobs.JobResult] = newList()
  if isRecurringId(id) {
    match marker(storage, id)? {
      case None -> return Err(notFound(id))
      case Some(_) -> ()
    }
    for jobId in firingIds(storage, id, keep)? {
      appendResults(storage, id, jobId, found)?
    }
  } else {
    val _ = rawState(storage, id)?
    appendResults(storage, id, id, found)?
  }
  while found.count > keep {
    found.removeAt(0)
  }
  Ok(Jobs.JobResultList(items = found.toArray()))
}

func isLive(name: in String): Bool {
  name == Kernel.stateEnqueued or name == Kernel.stateScheduled or name == Kernel.stateAwaiting or
    name == Kernel.stateProcessing
}

func cancelOnce(storage: in Kernel.NStorage, id: in String): Result[Unit, String] {
  // The delete is conditional on the state just read, so that a job that
  // finishes meanwhile keeps its outcome; a job that moved on between the
  // two calls (picked up by a worker, say) is read again.
  var round = 0
  while round < 20 {
    match Kernel.jobState(storage, id) {
      case Err(e) -> return Err("hangfire: cannot cancel job " + id + ": " + e)
      case Ok(None) -> return Ok(())
      case Ok(Some(name)) -> {
        // A job without a state yet is about to get one: read again.
        if name.length > 0 {
          if not isLive(name) {
            return Ok(())
          }
          match Kernel.deleteJob(storage, id, name) {
            case Err(e) -> return Err("hangfire: cannot cancel job " + id + ": " + e)
            case Ok(true) -> return Ok(())
            case Ok(false) -> ()
          }
        }
      }
    }
    round = round + 1
  }
  Err("hangfire: cannot cancel job " + id + ": its state kept changing")
}

func cancelRecurring(storage: in Kernel.NStorage, id: in String): Result[Unit, String] {
  match marker(storage, id)? {
    case None -> return Ok(())
    case Some(m) -> {
      if m == markerCancelled {
        return Ok(())
      }
    }
  }
  match Kernel.setAdd(storage, markerKey(id), markerCancelled, 0.0) {
    case Err(e) -> return Err("hangfire: cannot cancel job " + id + ": " + e)
    case Ok(_) -> ()
  }
  match Kernel.removeRecurring(storage, id) {
    case Ok(_) -> Ok(())
    case Err(e) -> Err("hangfire: job " + id + " is cancelled but Hangfire still holds its schedule: " + e)
  }
}

/// Cancel job `id`.  A job that is unknown or already finished is left as
/// it is.
pub func cancelJob(storage: in Kernel.NStorage, id: in String): Result[Unit, String] {
  if isRecurringId(id) {
    cancelRecurring(storage, id)
  } else {
    cancelOnce(storage, id)
  }
}
