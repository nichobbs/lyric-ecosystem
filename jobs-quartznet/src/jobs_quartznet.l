//! Jobs.QuartzNet — a `Jobs.JobScheduler` backed by Quartz.NET (.NET).
//!
//! Jobs are stored in a Quartz.NET `RAMJobStore` and fired by Quartz.NET
//! triggers on the scheduler's own worker threads; each firing runs the
//! `Jobs.JobHandler` registered for the job's name (or the default handler),
//! with retries and a timeout.
//!
//!   import Jobs.QuartzNet as Quartz
//!
//!   val sched = Quartz.create(Quartz.defaultConfig())?
//!   Quartz.registerHandler(sched, "send-email", SendEmail())
//!   Quartz.start(sched)?
//!   val id = sched.enqueue("send-email", "{\"to\":\"a@example.com\"}", 3, 30000)?
//!   ...
//!   Quartz.shutdown(sched, true)?
//!
//! The API, the statuses and the trigger handling are those of `Jobs.Quartz`
//! (Quartz on the JVM), so that an application can change target by changing
//! the import.  What differs is listed in the README; the main point is below.
//!
//! Lifecycle: `create` returns a scheduler in standby.  Jobs can be submitted
//! and cancelled in standby, and stay `Pending`; triggers fire only after
//! `start`.  `shutdown` is final.
//!
//! Status model (lined up with `Jobs.InProcessJobScheduler`):
//!
//!   Pending    waiting for a trigger to fire (a recurring job is `Pending`
//!              again between firings)
//!   Running    a worker thread is running the handler
//!   Succeeded  no trigger of the job can fire again, and the last firing
//!              that ran ended with an `Ok` attempt
//!   Failed     no trigger of the job can fire again, and the last firing
//!              that ran used up `maxAttempts`
//!   Cancelled  `cancel` was called before the job reached a final status,
//!              or Quartz.NET dropped every firing as a misfire and the job
//!              never ran
//!
//! `results` holds one `JobResult` per attempt, oldest first (`Succeeded`
//! with the handler's output, or `Failed` with the error), capped at
//! `QuartzConfig.maxResultsPerJob` most recent attempts.
//!
//! A handler runs on a Quartz.NET worker thread, and .NET cannot interrupt a
//! thread.  A handler therefore always runs to its own end.  When it returns
//! after more than `timeoutMs`, or after the job was cancelled, what it
//! returned is discarded and the attempt is recorded as failed (timed out,
//! or cancelled); the worker thread is held until then.  A handler that wants
//! to stop early asks `stopRequested`.  A failed attempt is retried at once
//! (or after `QuartzConfig.retryDelayMs`) within the same firing, up to
//! `maxAttempts`.  Once `cancel` has returned no further attempt starts and a
//! retry delay in progress ends.
//!
//! A recurring job whose previous firing is still running skips the firing.
//! A job finishes when its last trigger has ended, whether the trigger's last
//! firing ran, was skipped, or was dropped by Quartz.NET as a misfire; the
//! last firing that ran decides the final status.
//!
//! Quartz.NET-only scheduling (native cron, intervals, delays, misfire
//! policy) is described by `Jobs.QuartzNet.Triggers` and submitted with
//! `scheduleTrigger`.
@runtime_checked
@io
package Jobs.QuartzNet

import Std.Core
import Std.Collections
import Std.Uuid
pub use Jobs
import Jobs.QuartzNet.Triggers
import Jobs.QuartzNet.Kernel.Dotnet as Kernel

// ─── Configuration ────────────────────────────────────────────────────────────
/// Settings for one Quartz.NET scheduler.
pub record QuartzConfig {
  /// Quartz.NET scheduler name; must be unique within the process.
  instanceName: String
  /// Number of worker threads, which bounds how many jobs run at once.
  workerThreads: Int
  /// Pause between a failed attempt and the next one, in milliseconds.
  retryDelayMs: Int
  /// How many attempt results `results` keeps per job (the most recent).
  maxResultsPerJob: Int
  /// How late a firing may be before the trigger's misfire policy applies.
  misfireThresholdMs: Int
  invariant: instanceName.length > 0
  invariant: workerThreads >= 1 and retryDelayMs >= 0 and maxResultsPerJob >= 1 and misfireThresholdMs >= 1
}

/// Four worker threads, immediate retries, 100 results per job, Quartz.NET's
/// 60 s misfire threshold, and a fresh unique instance name.
pub func defaultConfig(): QuartzConfig {
  QuartzConfig(
    instanceName = "lyric-quartz-" + uuidToString(newUuid()),
    workerThreads = 4,
    retryDelayMs = 0,
    maxResultsPerJob = 100,
    misfireThresholdMs = 60000
  )
}

// ─── Shared state ─────────────────────────────────────────────────────────────
// One job as the table tracks it.  Mutated only inside `JobTable` entries.
ref record JobEntry {
  id: String
  name: String
  payload: String
  maxAttempts: Int
  timeoutMs: Int
  var status: Jobs.JobStatus
  var results: List[Jobs.JobResult]
  var attempts: Int
  var firings: Int
  var lastFireMs: Long
  var lastFiringOk: Bool
  // Names of the job's triggers that can still fire.  The job is over once
  // this is empty and no firing is running.
  var liveTriggers: List[String]
  // Set by `cancel`; ends a retry delay.
  cancelled: Kernel.Signal
  // The attempt in progress: `attemptDeadline` is the `Kernel.monotonicMs`
  // time at which it has run for `timeoutMs` (0 when no attempt is running),
  // and `attemptCancelled` records a `cancel` that arrived before then.
  var attemptDeadline: Long
  var attemptCancelled: Bool
}

// The trigger named `trigger` will never fire again.
func retireTrigger(e: in JobEntry, trigger: in String): Unit {
  var i = e.liveTriggers.count - 1
  while i >= 0 {
    if e.liveTriggers[i] == trigger {
      e.liveTriggers.removeAt(i)
    }
    i = i - 1
  }
}

func isFinal(s: in Jobs.JobStatus): Bool {
  match s {
    case Jobs.JobStatus.Succeeded -> true
    case Jobs.JobStatus.Failed -> true
    case Jobs.JobStatus.Cancelled -> true
    case _ -> false
  }
}

func inFlight(s: in Jobs.JobStatus): Bool {
  match s {
    case Jobs.JobStatus.Running -> true
    case _ -> false
  }
}

val attemptCompleted: Int = 0

val attemptTimedOut: Int = 1

val attemptWasCancelled: Int = 2

val cancelUnknown: Int = 0

val cancelDone: Int = 1

val cancelAlreadyFinal: Int = 2

// Job state shared between callers and Quartz worker threads; every entry
// holds the instance lock.
protected type JobTable {
  var jobs: Map[String, JobEntry] = newMap()
  entry add(e: in JobEntry) {
    jobs[e.id] = e
  }
  entry remove(id: in String) {
    val _ = jobs.remove(id)
  }
  entry statusOf(id: in String): Option[Jobs.JobStatus] {
    if not jobs.containsKey(id) {
      return None
    }
    val e: JobEntry = jobs[id]
    return Some(value = e.status)
  }
  entry resultsOf(id: in String): Option[Jobs.JobResultList] {
    if not jobs.containsKey(id) {
      return None
    }
    val e: JobEntry = jobs[id]
    return Some(value = Jobs.JobResultList(items = e.results.toArray()))
  }
  entry attemptsOf(id: in String): Int {
    if not jobs.containsKey(id) {
      return 0
    }
    val e: JobEntry = jobs[id]
    return e.attempts
  }
  entry firingsOf(id: in String): Int {
    if not jobs.containsKey(id) {
      return 0
    }
    val e: JobEntry = jobs[id]
    return e.firings
  }
  entry cancel(id: in String): Int {
    if not jobs.containsKey(id) {
      return cancelUnknown
    }
    val e: JobEntry = jobs[id]
    if isFinal(e.status) {
      return cancelAlreadyFinal
    }
    e.status = Jobs.JobStatus.Cancelled
    if e.attemptDeadline > 0i64 and Kernel.monotonicMs() <= e.attemptDeadline {
      e.attemptCancelled = true
    }
    Kernel.raise(e.cancelled)
    return cancelDone
  }
  // Claim one firing.  `None` when the job is unknown, already final, still
  // running an earlier firing, or has already run for this scheduled time
  // (two triggers of one job selecting the same instant).
  entry beginFiring(id: in String, scheduledMs: in Long): Option[Jobs.JobSpec] {
    if not jobs.containsKey(id) {
      return None
    }
    val e: JobEntry = jobs[id]
    if inFlight(e.status) or isFinal(e.status) or e.lastFireMs == scheduledMs {
      return None
    }
    e.status = Jobs.JobStatus.Running
    e.firings = e.firings + 1
    e.lastFireMs = scheduledMs
    return Some(
      value = Jobs.JobSpec(
        id = e.id,
        name = e.name,
        payload = e.payload,
        maxAttempts = e.maxAttempts,
        timeoutMs = e.timeoutMs,
        scheduledAt = if scheduledMs > 0i64 { scheduledMs } else { 0i64 }
      )
    )
  }
  // False once the job is cancelled (or forgotten): the firing in progress
  // must not start, or wait for, another attempt.
  entry stillRunning(id: in String): Bool {
    if not jobs.containsKey(id) {
      return false
    }
    val e: JobEntry = jobs[id]
    return inFlight(e.status)
  }
  // Claim one attempt of the firing in progress.  False when the job has been
  // cancelled (or forgotten) meanwhile: the handler must not be called.  From
  // here until `endAttempt` a `cancel` counts as arriving while the handler runs.
  entry beginAttempt(id: in String, timeoutMs: in Int): Bool {
    if not jobs.containsKey(id) {
      return false
    }
    val e: JobEntry = jobs[id]
    if not inFlight(e.status) {
      return false
    }
    e.attemptDeadline = Kernel.monotonicMs() + timeoutMs.toLong()
    e.attemptCancelled = false
    return true
  }
  // Close the attempt and say how it ended: cancelled while it ran (and
  // before its time was up), out of time, or completed.
  entry endAttempt(id: in String): Int {
    if not jobs.containsKey(id) {
      return attemptWasCancelled
    }
    val e: JobEntry = jobs[id]
    val late = Kernel.monotonicMs() > e.attemptDeadline
    val wasCancelled = e.attemptCancelled
    e.attemptDeadline = 0i64
    e.attemptCancelled = false
    if wasCancelled {
      return attemptWasCancelled
    }
    if late {
      return attemptTimedOut
    }
    return attemptCompleted
  }
  // Whether the handler now running for `id` is asked to stop: the job is
  // cancelled or unknown, or the attempt has used up its `timeoutMs`.
  entry stopWanted(id: in String): Bool {
    if not jobs.containsKey(id) {
      return true
    }
    val e: JobEntry = jobs[id]
    if not inFlight(e.status) {
      return true
    }
    return e.attemptDeadline > 0i64 and Kernel.monotonicMs() > e.attemptDeadline
  }
  entry cancelSignal(id: in String): Option[Kernel.Signal] {
    if not jobs.containsKey(id) {
      return None
    }
    val e: JobEntry = jobs[id]
    return Some(value = e.cancelled)
  }
  entry recordAttempt(id: in String, r: in Jobs.JobResult, keep: in Int) {
    if jobs.containsKey(id) {
      val e: JobEntry = jobs[id]
      e.attempts = e.attempts + 1
      e.results.add(r)
      while e.results.count > keep {
        e.results.removeAt(0)
      }
    }
  }
  // Close a firing made by `trigger`.  A cancelled job stays cancelled.  The
  // job goes back to `Pending` while any of its triggers can still fire;
  // otherwise this firing was its last.  True when the job became final here.
  entry endFiring(id: in String, trigger: in String, succeeded: in Bool, mayFireAgain: in Bool): Bool {
    if not jobs.containsKey(id) {
      return false
    }
    val e: JobEntry = jobs[id]
    if not mayFireAgain {
      retireTrigger(e, trigger)
    }
    if not inFlight(e.status) {
      return false
    }
    e.lastFiringOk = succeeded
    if e.liveTriggers.count > 0 {
      e.status = Jobs.JobStatus.Pending
      return false
    }
    e.status = if succeeded { Jobs.JobStatus.Succeeded } else { Jobs.JobStatus.Failed }
    return true
  }
  // `trigger` will never fire again and is not running a firing of its own:
  // its last firing was skipped, or Quartz ended it.  When that was the job's
  // last live trigger and no firing is running (a running one closes the job
  // in `endFiring`), the job is over: the last firing that ran decides, and
  // a job that never ran is `Cancelled`.  True when the job became final here.
  entry triggerEnded(id: in String, trigger: in String): Bool {
    if not jobs.containsKey(id) {
      return false
    }
    val e: JobEntry = jobs[id]
    retireTrigger(e, trigger)
    if inFlight(e.status) or isFinal(e.status) or e.liveTriggers.count > 0 {
      return false
    }
    if e.firings == 0 {
      e.status = Jobs.JobStatus.Cancelled
    } else if e.lastFiringOk {
      e.status = Jobs.JobStatus.Succeeded
    } else {
      e.status = Jobs.JobStatus.Failed
    }
    return true
  }
  entry purgeFinal(): Int {
    val doomed: List[String] = newList()
    for id in mapKeys(jobs) {
      val e: JobEntry = jobs[id]
      if isFinal(e.status) {
        doomed.add(id)
      }
    }
    for id in doomed {
      val _ = jobs.remove(id)
    }
    return doomed.count
  }
}

// Handlers by job name, plus an optional default (`fallback` holds 0 or 1).
protected type HandlerRegistry {
  var byName: Map[String, Jobs.JobHandler] = newMap()
  var fallback: List[Jobs.JobHandler] = newList()
  entry put(name: in String, h: in Jobs.JobHandler) {
    byName[name] = h
  }
  entry putDefault(h: in Jobs.JobHandler) {
    fallback.clear()
    fallback.add(h)
  }
  entry drop(name: in String) {
    val _ = byName.remove(name)
  }
  entry find(name: in String): Option[Jobs.JobHandler] {
    if byName.containsKey(name) {
      return Some(value = byName[name])
    }
    if fallback.count > 0 {
      return Some(value = fallback[0])
    }
    return None
  }
}

// ─── Running a firing (Quartz worker thread) ──────────────────────────────────
record AttemptOutcome {
  ok: Bool
  text: String
}

func callHandler(handler: in Jobs.JobHandler, spec: in Jobs.JobSpec): AttemptOutcome {
  match handler.handle(spec) {
    case Ok(output) -> AttemptOutcome(ok = true, text = output)
    case Err(jobErr) -> AttemptOutcome(ok = false, text = jobErr.message)
  }
}

func safeCall(handler: in Jobs.JobHandler, spec: in Jobs.JobSpec): AttemptOutcome {
  try {
    callHandler(handler, spec)
  } catch Bug as b {
    AttemptOutcome(ok = false, text = "handler panicked: " + b.message)
  }
}

// One attempt on the calling (Quartz.NET worker) thread.  .NET cannot
// interrupt a thread, so the handler runs to its own end; `spec.timeoutMs`
// and `cancel` decide afterwards what the attempt counts as.  An attempt that
// took longer than `timeoutMs`, or during which the job was cancelled, is
// failed and what the handler returned is discarded.
//
// `None` when the job was cancelled before the attempt was claimed: the
// handler was not called.  `cancel` and the claim take the same lock, so a
// cancel either prevents the call or finds the attempt running.
func runAttempt(table: in JobTable, handler: in Jobs.JobHandler, spec: in Jobs.JobSpec): Option[AttemptOutcome] {
  if not table.beginAttempt(spec.id, spec.timeoutMs) {
    return None
  }
  val outcome = safeCall(handler, spec)
  val ended = table.endAttempt(spec.id)
  if ended == attemptTimedOut {
    return Some(
      value = AttemptOutcome(ok = false, text = "handler exceeded timeoutMs (" + spec.timeoutMs.toString() + "ms)")
    )
  }
  if ended == attemptWasCancelled {
    return Some(value = AttemptOutcome(ok = false, text = "cancelled while running"))
  }
  Some(value = outcome)
}

// The pause before a retry, ended early by `cancel`; false when the job was
// cancelled before or during it.
func retryPause(table: in JobTable, jobId: in String, ms: in Int): Bool {
  match table.cancelSignal(jobId) {
    case None -> false
    case Some(signal) -> {
      val _ = Kernel.awaitSignal(signal, ms)
      table.stillRunning(jobId)
    }
  }
}

func attemptResult(spec: in Jobs.JobSpec, outcome: in AttemptOutcome, startedAt: in Long): Jobs.JobResult {
  val finishedAt = Jobs.nowMs()
  if outcome.ok {
    Jobs.JobResult(
      jobId = spec.id,
      status = Jobs.JobStatus.Succeeded,
      output = outcome.text,
      error = "",
      startedAt = startedAt,
      finishedAt = if finishedAt >= startedAt { finishedAt } else { startedAt }
    )
  } else {
    Jobs.JobResult(
      jobId = spec.id,
      status = Jobs.JobStatus.Failed,
      output = "",
      error = outcome.text,
      startedAt = startedAt,
      finishedAt = if finishedAt >= startedAt { finishedAt } else { startedAt }
    )
  }
}

// One firing of `jobId` by `trigger`.  True when the job reached its final
// status here and Quartz may forget it.
func runFiring(
  table: in JobTable,
  handlers: in HandlerRegistry,
  config: in QuartzConfig,
  jobId: in String,
  trigger: in String,
  scheduledMs: in Long,
  mayFireAgain: in Bool,
): Bool {
  match table.beginFiring(jobId, scheduledMs) {
    case None -> {
      // Skipped.  A trigger skipped on its last firing has ended.
      if mayFireAgain {
        false
      } else {
        table.triggerEnded(jobId, trigger)
      }
    }
    case Some(claimed) -> {
      val spec: Jobs.JobSpec = claimed
      var succeeded = false
      match handlers.find(spec.name) {
        case None -> {
          val missing = AttemptOutcome(ok = false, text = "no handler registered for job '" + spec.name + "'")
          table.recordAttempt(jobId, attemptResult(spec, missing, Jobs.nowMs()), config.maxResultsPerJob)
        }
        case Some(found) -> {
          val handler: Jobs.JobHandler = found
          var attempt = 1
          var live = true
          while live and not succeeded and attempt <= spec.maxAttempts {
            if attempt > 1 and config.retryDelayMs > 0 {
              live = retryPause(table, jobId, config.retryDelayMs)
            }
            if live {
              val startedAt = Jobs.nowMs()
              match runAttempt(table, handler, spec) {
                case None -> {
                  live = false
                }
                case Some(outcome) -> {
                  table.recordAttempt(jobId, attemptResult(spec, outcome, startedAt), config.maxResultsPerJob)
                  succeeded = outcome.ok
                  live = table.stillRunning(jobId)
                }
              }
            }
            attempt = attempt + 1
          }
        }
      }
      table.endFiring(jobId, trigger, succeeded, mayFireAgain)
    }
  }
}

// ─── The scheduler ────────────────────────────────────────────────────────────
// Everything behind a scheduler: the job table, the handler registry and the
// Quartz.NET instance.
pub opaque type QuartzState {
  table: JobTable
  handlers: HandlerRegistry
  quartz: Kernel.NScheduler
}

/// A Quartz.NET-backed `Jobs.JobScheduler`.  Build one with `create`.
pub record QuartzJobScheduler {
  config: QuartzConfig
  state: QuartzState
}

/// Build a scheduler in standby: jobs can be submitted, nothing fires until
/// `start`.  `Err` when Quartz.NET cannot be initialised or
/// `config.instanceName` is already in use in this process.
pub func create(config: in QuartzConfig): Result[QuartzJobScheduler, String] {
  val table = JobTable()
  val handlers = HandlerRegistry()
  val run: (String, String, Long, Bool) -> Bool = { id: String, trigger: String, at: Long, again: Bool ->
    runFiring(table, handlers, config, id, trigger, at, again)
  }
  val ended: (String, String) -> Bool = { id: String, trigger: String -> table.triggerEnded(id, trigger) }
  match Kernel.newScheduler(config.instanceName, config.workerThreads, config.misfireThresholdMs, run, ended) {
    case Err(e) -> Err("quartz: cannot create scheduler: " + e)
    case Ok(q) -> Ok(
      QuartzJobScheduler(
        config = config,
        state = QuartzState(table = table, handlers = handlers, quartz = q)
      )
    )
  }
}

/// Start (or resume from `standby`) firing triggers.
pub func start(s: in QuartzJobScheduler): Result[Unit, String] {
  match Kernel.startScheduler(s.state.quartz) {
    case Err(e) -> Err("quartz: cannot start scheduler: " + e)
    case Ok(_) -> Ok(())
  }
}

/// Pause firing; jobs already running finish.  `start` resumes.
pub func standby(s: in QuartzJobScheduler): Result[Unit, String] {
  match Kernel.standbyScheduler(s.state.quartz) {
    case Err(e) -> Err("quartz: cannot pause scheduler: " + e)
    case Ok(_) -> Ok(())
  }
}

/// Stop the scheduler for good.  With `waitForJobs` the call returns once
/// every running job has finished; without, running handlers are left to
/// finish on their own.  Statuses and results stay
/// readable; later submissions are an `Err`.  Shutting down twice is a no-op.
pub func shutdown(s: in QuartzJobScheduler, waitForJobs: in Bool): Result[Unit, String] {
  match Kernel.shutdownScheduler(s.state.quartz, waitForJobs) {
    case Err(e) -> Err("quartz: cannot shut down scheduler: " + e)
    case Ok(_) -> Ok(())
  }
}

/// True while triggers fire (started, not in standby, not shut down).
pub func isRunning(s: in QuartzJobScheduler): Bool {
  Kernel.schedulerRunning(s.state.quartz)
}

/// True once `shutdown` has been called.
pub func isShutDown(s: in QuartzJobScheduler): Bool {
  Kernel.schedulerShutDown(s.state.quartz)
}

/// Run `handler` for every job named `name`, replacing any earlier handler
/// for that name.  May be called at any time, from any thread.
pub func registerHandler(s: in QuartzJobScheduler, name: in String, handler: in Jobs.JobHandler): Unit
  requires: name.length > 0
{
  s.state.handlers.put(name, handler)
}

/// Run `handler` for jobs whose name has no handler of its own.
pub func registerDefaultHandler(s: in QuartzJobScheduler, handler: in Jobs.JobHandler): Unit {
  s.state.handlers.putDefault(handler)
}

/// Remove the handler for `name`; later firings fall back to the default
/// handler, or fail when there is none.
pub func unregisterHandler(s: in QuartzJobScheduler, name: in String): Unit
  requires: name.length > 0
{
  s.state.handlers.drop(name)
}

// The Quartz.NET name of each of a job's triggers, by position in `specs`.
func triggerNames(jobId: in String, count: in Int): List[String] {
  val names: List[String] = newList()
  var i = 0
  while i < count {
    names.add(jobId + "#" + i.toString())
    i = i + 1
  }
  names
}

func triggerPlans(names: in List[String], specs: in slice[TriggerSpec]): slice[Kernel.TriggerPlan] {
  val plans: List[Kernel.TriggerPlan] = newList()
  var i = 0
  while i < specs.length {
    val spec = specs[i]
    val kind = match spec.kind {
      case TriggerKind.Once -> Kernel.triggerOnce
      case TriggerKind.Cron -> Kernel.triggerCron
      case TriggerKind.Interval -> Kernel.triggerInterval
    }
    plans.add(
      Kernel.TriggerPlan(
        name = names[i],
        kind = kind,
        cron = spec.cron,
        delayMs = spec.delayMs,
        intervalMs = spec.intervalMs,
        repeatCount = spec.repeatCount,
        misfire = misfireCode(spec.misfire)
      )
    )
    i = i + 1
  }
  plans.toArray()
}

func submit(
  s: in QuartzJobScheduler,
  name: in String,
  payload: in String,
  specs: in slice[TriggerSpec],
  maxAttempts: in Int,
  timeoutMs: in Int,
): Result[String, String] {
  val id = "quartz-" + uuidToString(newUuid())
  val names = triggerNames(id, specs.length)
  // Registered before Quartz.NET sees the job: a trigger may fire on a worker
  // thread before `submitJob` returns.
  s.state.table.add(
    JobEntry(
      id = id,
      name = name,
      payload = payload,
      maxAttempts = maxAttempts,
      timeoutMs = timeoutMs,
      status = Jobs.JobStatus.Pending,
      results = newList(),
      attempts = 0,
      firings = 0,
      lastFireMs = -1i64,
      lastFiringOk = false,
      liveTriggers = names,
      cancelled = Kernel.newSignal(),
      attemptDeadline = 0i64,
      attemptCancelled = false
    )
  )
  match Kernel.submitJob(s.state.quartz, id, triggerPlans(names, specs)) {
    case Err(e) -> {
      s.state.table.remove(id)
      Err("quartz: cannot schedule job '" + name + "': " + e)
    }
    case Ok(_) -> Ok(id)
  }
}

/// Schedule job `name` on a Quartz.NET-specific trigger (see
/// `Jobs.QuartzNet.Triggers`).  Returns the job id; `status`, `results` and
/// `cancel` work on it as for any other job.
pub func scheduleTrigger(
  s: in QuartzJobScheduler,
  name: in String,
  payload: in String,
  trigger: in TriggerSpec,
  maxAttempts: in Int,
  timeoutMs: in Int,
): Result[String, String]
  requires: name.length > 0
  requires: maxAttempts >= 1
  requires: timeoutMs >= 1
{
  submit(s, name, payload, [trigger], maxAttempts, timeoutMs)
}

func cronSpecs(exprs: in slice[String]): slice[TriggerSpec] {
  val specs: List[TriggerSpec] = newList()
  for expr in exprs {
    specs.add(
      TriggerSpec(
        kind = TriggerKind.Cron,
        cron = expr,
        delayMs = 0i64,
        intervalMs = 0i64,
        repeatCount = 0,
        misfire = MisfirePolicy.Smart
      )
    )
  }
  specs.toArray()
}

func cancelJob(s: in QuartzJobScheduler, jobId: in String): Result[Unit, String] {
  if s.state.table.cancel(jobId) != cancelDone {
    return Ok(())
  }
  // The table already refuses further firings and attempts, and has ended a
  // retry delay; removing the job from Quartz.NET stops its triggers.  After
  // `shutdown` Quartz.NET refuses the call, and no trigger fires any more.
  match Kernel.deleteJob(s.state.quartz, jobId) {
    case Ok(_) -> Ok(())
    case Err(e) -> {
      if Kernel.schedulerShutDown(s.state.quartz) {
        Ok(())
      } else {
        Err("quartz: job '" + jobId + "' is cancelled but its triggers could not be removed: " + e)
      }
    }
  }
}

func jobStatus(s: in QuartzJobScheduler, jobId: in String): Result[Jobs.JobStatus, String] {
  match s.state.table.statusOf(jobId) {
    case None -> Err("job not found: " + jobId)
    case Some(st) -> Ok(st)
  }
}

func jobResults(s: in QuartzJobScheduler, jobId: in String): Result[Jobs.JobResultList, String] {
  match s.state.table.resultsOf(jobId) {
    case None -> Err("job not found: " + jobId)
    case Some(list) -> Ok(list)
  }
}

impl Jobs.JobScheduler for QuartzJobScheduler {
  func enqueue(name: in String, payload: in String, maxAttempts: in Int, timeoutMs: in Int): Result[String, String]
    requires: name.length > 0
    requires: maxAttempts >= 1
    requires: timeoutMs >= 1
  {
    match once(0i64) {
      case Err(e) -> Err(e)
      case Ok(spec) -> submit(self, name, payload, [spec], maxAttempts, timeoutMs)
    }
  }

  func schedule(
    name: in String, payload: in String, cronExpr: in String, maxAttempts: in Int, timeoutMs: in Int
  ): Result[String, String]
    requires: name.length > 0
    requires: cronExpr.length > 0
    requires: maxAttempts >= 1
    requires: timeoutMs >= 1
  {
    match unixCronToQuartz(cronExpr) {
      case Err(e) -> Err(e)
      case Ok(exprs) -> submit(self, name, payload, cronSpecs(exprs), maxAttempts, timeoutMs)
    }
  }

  func cancel(jobId: in String): Result[Unit, String]
    requires: jobId.length > 0
  {
    cancelJob(self, jobId)
  }

  func status(jobId: in String): Result[Jobs.JobStatus, String]
    requires: jobId.length > 0
  {
    jobStatus(self, jobId)
  }

  func results(jobId: in String): Result[Jobs.JobResultList, String]
    requires: jobId.length > 0
  {
    jobResults(self, jobId)
  }
}

// ─── Cooperative stop ─────────────────────────────────────────────────────────
/// For a handler to call while it runs job `jobId` (`JobSpec.id`): true once
/// the job has been cancelled, or the attempt has run for longer than its
/// `timeoutMs`.  The attempt is already lost then, so the handler can return
/// at once; what it returns is discarded.  Also true for a job that is not
/// running or not known.
pub func stopRequested(s: in QuartzJobScheduler, jobId: in String): Bool
  requires: jobId.length > 0
{
  s.state.table.stopWanted(jobId)
}

// ─── Introspection ────────────────────────────────────────────────────────────
/// Handler attempts run for `jobId` so far, over all its firings (0 for an
/// unknown job).
pub func attemptsRun(s: in QuartzJobScheduler, jobId: in String): Int {
  s.state.table.attemptsOf(jobId)
}

/// Times a trigger has fired `jobId` and the job was run (0 for an unknown job).
pub func timesFired(s: in QuartzJobScheduler, jobId: in String): Int {
  s.state.table.firingsOf(jobId)
}

/// Whether Quartz.NET itself still holds `jobId` (the job is dropped when it
/// reaches a final status).
pub func heldByQuartz(s: in QuartzJobScheduler, jobId: in String): Result[Bool, String]
  requires: jobId.length > 0
{
  match Kernel.jobStored(s.state.quartz, jobId) {
    case Err(e) -> Err("quartz: " + e)
    case Ok(stored) -> Ok(stored)
  }
}

/// Block until `jobId` is `Succeeded`, `Failed` or `Cancelled`, polling every
/// few milliseconds, and return that status.  `Err` for an unknown job or
/// when `timeoutMs` passes first.
pub func awaitFinished(s: in QuartzJobScheduler, jobId: in String, timeoutMs: in Int): Result[Jobs.JobStatus, String]
  requires: jobId.length > 0
  requires: timeoutMs >= 0
{
  val deadline = Jobs.nowMs() + timeoutMs.toLong()
  while true {
    match s.state.table.statusOf(jobId) {
      case None -> return Err("job not found: " + jobId)
      case Some(st) -> {
        if isFinal(st) {
          return Ok(st)
        }
      }
    }
    if Jobs.nowMs() >= deadline {
      return Err("job " + jobId + " did not finish within " + timeoutMs.toString() + "ms")
    }
    Jobs.sleepMs(5)
  }
  Err("job " + jobId + " did not finish")
}

/// Forget every job that is `Succeeded`, `Failed` or `Cancelled`, freeing its
/// recorded results; returns how many were dropped.  Their ids become unknown.
pub func purgeFinished(s: in QuartzJobScheduler): Int {
  s.state.table.purgeFinal()
}
