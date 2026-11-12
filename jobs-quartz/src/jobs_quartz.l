//! Jobs.Quartz — a `Jobs.JobScheduler` backed by the Quartz Scheduler (JVM).
//!
//! Jobs are stored in a Quartz `RAMJobStore` and fired by Quartz triggers on
//! Quartz worker threads; each firing runs the `Jobs.JobHandler` registered
//! for the job's name (or the default handler), with retries and a timeout.
//!
//!   import Jobs.Quartz
//!
//!   val sched = Quartz.create(Quartz.defaultConfig())?
//!   Quartz.registerHandler(sched, "send-email", SendEmail())
//!   Quartz.start(sched)?
//!   val id = sched.enqueue("send-email", "{\"to\":\"a@example.com\"}", 3, 30000)?
//!   ...
//!   Quartz.shutdown(sched, true)?
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
//!   Succeeded  the last possible firing ended with an `Ok` attempt
//!   Failed     the last possible firing used up `maxAttempts`
//!   Cancelled  `cancel` was called before the job reached a final status
//!
//! `results` holds one `JobResult` per attempt, oldest first (`Succeeded`
//! with the handler's output, or `Failed` with the error), capped at
//! `QuartzConfig.maxResultsPerJob` most recent attempts.
//!
//! A handler runs on the Quartz worker thread.  When an attempt passes
//! `timeoutMs`, or the job is cancelled while it runs, that thread is
//! interrupted: a handler blocked in a sleep, a wait or interruptible I/O
//! ends there.  The JVM cannot stop a handler that ignores interruption; it
//! keeps its worker thread until it returns, and the attempt still counts as
//! timed out.  A failed attempt is retried at once (or after
//! `QuartzConfig.retryDelayMs`) within the same firing, up to `maxAttempts`.
//!
//! A recurring job whose previous firing is still running skips the firing.
//!
//! Quartz-only scheduling (native cron, intervals, delays, misfire policy) is
//! described by `Jobs.Quartz.Triggers` and submitted with `scheduleTrigger`.
@runtime_checked
@io
package Jobs.Quartz

import Std.Core
import Std.Collections
import Std.Uuid
pub use Jobs
import Jobs.Quartz.Triggers
import Jobs.Quartz.Kernel.Jvm as Kernel

// ─── Configuration ────────────────────────────────────────────────────────────
/// Settings for one Quartz scheduler.
pub record QuartzConfig {
  /// Quartz scheduler name; must be unique within the JVM.
  instanceName: String
  /// Number of Quartz worker threads, which bounds how many jobs run at once.
  workerThreads: Int
  /// Run Quartz's threads as daemon threads.  When false (Quartz's default)
  /// a started scheduler keeps the JVM alive until `shutdown`.
  daemonThreads: Bool
  /// Pause between a failed attempt and the next one, in milliseconds.
  retryDelayMs: Int
  /// How many attempt results `results` keeps per job (the most recent).
  maxResultsPerJob: Int
  /// How late a firing may be before the trigger's misfire policy applies.
  misfireThresholdMs: Int
  invariant: instanceName.length > 0
  invariant: workerThreads >= 1 and retryDelayMs >= 0 and maxResultsPerJob >= 1 and misfireThresholdMs >= 1
}

/// Four worker threads, non-daemon threads, immediate retries, 100 results
/// per job, Quartz's 60 s misfire threshold, and a fresh unique instance name.
pub func defaultConfig(): QuartzConfig {
  QuartzConfig(
    instanceName = "lyric-quartz-" + uuidToString(newUuid()),
    workerThreads = 4,
    daemonThreads = false,
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
    if isFinal(e.status) or inFlight(e.status) or e.lastFireMs == scheduledMs {
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
  // Record one attempt; false when the job was cancelled meanwhile, so the
  // caller stops retrying.
  entry recordAttempt(id: in String, r: in Jobs.JobResult, keep: in Int): Bool {
    if not jobs.containsKey(id) {
      return false
    }
    val e: JobEntry = jobs[id]
    e.attempts = e.attempts + 1
    e.results.add(r)
    while e.results.count > keep {
      e.results.removeAt(0)
    }
    return inFlight(e.status)
  }
  // Close a firing.  A cancelled job stays cancelled.
  entry endFiring(id: in String, succeeded: in Bool, mayFireAgain: in Bool) {
    if jobs.containsKey(id) {
      val e: JobEntry = jobs[id]
      if inFlight(e.status) {
        if mayFireAgain {
          e.status = Jobs.JobStatus.Pending
        } else if succeeded {
          e.status = Jobs.JobStatus.Succeeded
        } else {
          e.status = Jobs.JobStatus.Failed
        }
      }
    }
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

// One attempt on the calling (Quartz worker) thread, bounded by
// `spec.timeoutMs`: when the time is up the thread is interrupted, which ends
// a handler blocked in a sleep, a wait or interruptible I/O.  A handler that
// ignores the interrupt overruns; either way the attempt counts as failed.
func runAttempt(handler: in Jobs.JobHandler, spec: in Jobs.JobSpec): AttemptOutcome {
  val slot: List[AttemptOutcome] = newList()
  val ended = Kernel.runBounded(spec.id, spec.timeoutMs, { -> slot.add(safeCall(handler, spec)) })
  if ended == Kernel.runTimedOut {
    return AttemptOutcome(ok = false, text = "handler exceeded timeoutMs (" + spec.timeoutMs.toString() + "ms)")
  }
  if ended == Kernel.runInterrupted {
    return AttemptOutcome(ok = false, text = "cancelled while running")
  }
  slot[0]
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

func runFiring(
  table: in JobTable,
  handlers: in HandlerRegistry,
  config: in QuartzConfig,
  jobId: in String,
  scheduledMs: in Long,
  mayFireAgain: in Bool,
): Unit {
  match table.beginFiring(jobId, scheduledMs) {
    case None -> ()
    case Some(claimed) -> {
      val spec: Jobs.JobSpec = claimed
      var succeeded = false
      match handlers.find(spec.name) {
        case None -> {
          val missing = AttemptOutcome(ok = false, text = "no handler registered for job '" + spec.name + "'")
          val _ = table.recordAttempt(jobId, attemptResult(spec, missing, Jobs.nowMs()), config.maxResultsPerJob)
        }
        case Some(found) -> {
          val handler: Jobs.JobHandler = found
          var attempt = 1
          var live = true
          while live and not succeeded and attempt <= spec.maxAttempts {
            if attempt > 1 and config.retryDelayMs > 0 {
              Jobs.sleepMs(config.retryDelayMs)
            }
            val startedAt = Jobs.nowMs()
            val outcome = runAttempt(handler, spec)
            live = table.recordAttempt(jobId, attemptResult(spec, outcome, startedAt), config.maxResultsPerJob)
            succeeded = outcome.ok
            attempt = attempt + 1
          }
        }
      }
      table.endFiring(jobId, succeeded, mayFireAgain)
    }
  }
}

// ─── The scheduler ────────────────────────────────────────────────────────────
// Everything behind a scheduler: the job table, the handler registry and the
// Quartz instance.
pub opaque type QuartzState {
  table: JobTable
  handlers: HandlerRegistry
  quartz: Kernel.JScheduler
}

/// A Quartz-backed `Jobs.JobScheduler`.  Build one with `create`.
pub record QuartzJobScheduler {
  config: QuartzConfig
  state: QuartzState
}

/// Build a scheduler in standby: jobs can be submitted, nothing fires until
/// `start`.  `Err` when Quartz cannot be initialised or `config.instanceName`
/// is already in use in this JVM.
pub func create(config: in QuartzConfig): Result[QuartzJobScheduler, String] {
  val table = JobTable()
  val handlers = HandlerRegistry()
  val run: (String, Long, Bool) -> Unit = { id: String, at: Long, again: Bool ->
    runFiring(table, handlers, config, id, at, again)
  }
  match Kernel.newScheduler(config.instanceName, config.workerThreads, config.daemonThreads, config.misfireThresholdMs, run) {
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

/// Stop the scheduler for good and release its threads.  With `waitForJobs`
/// the call returns once every running job has finished; without, running
/// handlers are left to finish on their own.  Statuses and results stay
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

func buildTriggers(jobId: in String, specs: in slice[TriggerSpec]): Result[slice[Kernel.JTrigger], String] {
  val built: List[Kernel.JTrigger] = newList()
  var i = 0
  while i < specs.length {
    val spec = specs[i]
    val name = jobId + "#" + i.toString()
    val made = match spec.kind {
      case TriggerKind.Once -> Kernel.onceTrigger(name, spec.delayMs)
      case TriggerKind.Cron -> Kernel.cronTrigger(name, spec.cron, misfireCode(spec.misfire))
      case TriggerKind.Interval -> Kernel.intervalTrigger(
        name,
        spec.delayMs,
        spec.intervalMs,
        spec.repeatCount,
        misfireCode(spec.misfire)
      )
    }
    match made {
      case Err(e) -> return Err(e)
      case Ok(t) -> built.add(t)
    }
    i = i + 1
  }
  Ok(built.toArray())
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
  match buildTriggers(id, specs) {
    case Err(e) -> Err("quartz: " + e)
    case Ok(triggers) -> {
      // Registered before Quartz sees the job: a trigger may fire on a worker
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
          lastFireMs = -1i64
        )
      )
      match Kernel.submitJob(s.state.quartz, id, triggers) {
        case Err(e) -> {
          s.state.table.remove(id)
          Err("quartz: cannot schedule job '" + name + "': " + e)
        }
        case Ok(_) -> Ok(id)
      }
    }
  }
}

/// Schedule job `name` on a Quartz-specific trigger (see
/// `Jobs.Quartz.Triggers`).  Returns the job id; `status`, `results` and
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
  val _ = Kernel.interruptRun(jobId)
  // The table already refuses further firings; removing the job from
  // Quartz stops its triggers.  After `shutdown` Quartz has dropped them.
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

/// Whether Quartz itself still holds `jobId` (it drops a job when its last
/// trigger has finished, and when the job is cancelled).
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
