//! Jobs.QuartzNet.Kernel.Dotnet — .NET extern boundary for the Quartz.NET
//! backend.
//!
//! Every binding is an `extern type` plus auto-FFI against the Quartz.NET
//! assembly restored from NuGet; there is no host shim.  The kernel owns:
//!
//!   - building a Quartz.NET `IScheduler` (StdSchedulerFactory,
//!     DedicatedThreadPool, RAMJobStore) and its lifecycle,
//!   - `DispatchJob` / `DispatchFactory`, Lyric records that implement
//!     `Quartz.IJob` and `Quartz.Spi.IJobFactory`, so every firing on a
//!     Quartz.NET worker thread calls back into a Lyric closure,
//!   - `TriggerWatch`, a Lyric record that implements
//!     `Quartz.ISchedulerListener`, so the Lyric side hears of every trigger
//!     Quartz.NET ends, including one it ends without firing the job,
//!   - trigger construction (one-shot, cron, fixed interval) from plain
//!     `TriggerPlan` data, and job removal,
//!   - `Signal`, a settable event a waiting thread can be woken through.
//!
//! Quartz.NET's API returns `Task`s.  With a `RAMJobStore` nothing in it waits
//! for I/O, so every call here blocks on the task it gets back, and
//! `IJob.Execute` runs the firing to its end on the worker thread before it
//! returns a completed task.
//!
//! No Quartz.NET or BCL exception leaves this file: every entry point returns
//! `Result[_, String]`.
@io
package Jobs.QuartzNet.Kernel.Dotnet

import Std.Core
import Std.String as Str

extern type NTask = "System.Threading.Tasks.Task"

extern type NTaskAwaiter = "System.Runtime.CompilerServices.TaskAwaiter"

extern type NCancel = "System.Threading.CancellationToken"

extern type NType = "System.Type"

extern type NString = "System.String"

extern type NFormatProvider = "System.IFormatProvider"

extern type NCulture = "System.Globalization.CultureInfo"

extern type NDateTimeOffset = "System.DateTimeOffset"

extern type NTimeSpan = "System.TimeSpan"

extern type NEnvironment = "System.Environment"

extern type NThread = "System.Threading.Thread"

extern type NEvent = "System.Threading.ManualResetEventSlim"

extern type NProps = "System.Collections.Specialized.NameValueCollection"

pub extern type NScheduler = "Quartz.IScheduler"

extern type NFactory = "Quartz.Impl.StdSchedulerFactory"

extern type NRepository = "Quartz.Impl.SchedulerRepository"

extern type NJob = "Quartz.IJob"

extern type NJobFactory = "Quartz.Spi.IJobFactory"

extern type NBundle = "Quartz.Spi.TriggerFiredBundle"

extern type NContext = "Quartz.IJobExecutionContext"

extern type NJobBuilder = "Quartz.JobBuilder"

extern type NJobDetail = "Quartz.IJobDetail"

extern type NJobKey = "Quartz.JobKey"

extern type NTriggerBuilder = "Quartz.TriggerBuilder"

extern type NTrigger = "Quartz.ITrigger"

extern type NTriggerKey = "Quartz.TriggerKey"

extern type NListener = "Quartz.ISchedulerListener"

extern type NSchedulerException = "Quartz.SchedulerException"

extern type NScheduleBuilder = "Quartz.IScheduleBuilder"

extern type NSimpleSchedule = "Quartz.SimpleScheduleBuilder"

extern type NCronSchedule = "Quartz.CronScheduleBuilder"

extern type NCronExpression = "Quartz.CronExpression"

/// `TriggerPlan.misfire` values.
pub val misfireSmart: Int = 0

pub val misfireFireNow: Int = 1

pub val misfireSkip: Int = 2

pub val misfireIgnore: Int = 3

// Every job and trigger this backend creates lives in one Quartz.NET group.
// The job name is the Lyric job id and a trigger's name is `<job id>#<n>`;
// both are also stored as the item's description, which is how a firing and
// a listener call read them back: `JobKey.Name` is declared on the generic
// base `Key<T>`, which auto-FFI does not reach.
val jobGroup: String = "lyric"

func describe(typeName: in String, message: in String): String {
  if message.length == 0 {
    return typeName
  }
  typeName + ": " + message
}

// ─── Tasks ────────────────────────────────────────────────────────────────────
// `TaskAwaiter.GetResult` rethrows the task's own exception; `Task.Wait` and
// `Task<T>.Result` would wrap it in an `AggregateException`.
@externInstance
@externTarget("System.Threading.Tasks.Task.GetAwaiter")
func awaiterOf[T](t: in Task[T]): NTaskAwaiter = ()

@externInstance
@externTarget("System.Threading.Tasks.Task`1.get_Result")
func resultOf[T](t: in Task[T]): T = ()

func finish(t: in NTask): Unit {
  t.GetAwaiter().GetResult()
}

func finishWith[T](t: in Task[T]): T {
  awaiterOf(t).GetResult()
  resultOf(t)
}

// ─── Job dispatch ─────────────────────────────────────────────────────────────
// `run(jobId, triggerName, scheduledFireMs, mayFireAgain)` is the Lyric side
// of a firing.  Quartz.NET asks the `IJobFactory` for an `IJob` each time a
// trigger fires and then calls `Execute` on a worker thread.  `run` answers
// true when the job has reached its final status and is to be removed from
// Quartz.NET.
record DispatchJob {
  run: (String, String, Long, Bool) -> Bool
}

// `ScheduledFireTimeUtc` is a `DateTimeOffset?`.  Boxed into the `object`
// argument of `String.Format` it is either null (formatted as "") or the
// `DateTimeOffset`, written in the round-trip format and parsed back.
func scheduledFireMs(ctx: in NContext): Long {
  val text = NString.Format("{0:O}", ctx.ScheduledFireTimeUtc)
  if text.length == 0 {
    return 0i64
  }
  val culture: NFormatProvider = NCulture.InvariantCulture
  NDateTimeOffset.Parse(text, culture).ToUnixTimeMilliseconds()
}

func jobIdOfTrigger(name: in String): String {
  match Str.lastIndexOf(name, "#") {
    case Some(at) -> Str.substring(name, 0, at)
    case None -> name
  }
}

// Quartz.NET drops a job by itself only when its last trigger ends by firing;
// a trigger ended as a misfire stays stored, and the job with it.  A failure
// has nobody to report to, and leaves nothing behind after `shutdown`.
func dropFinished(s: in NScheduler, jobId: in String): Unit {
  val _ = deleteJob(s, jobId)
}

impl NJob for DispatchJob {
  func Execute(ctx: in NContext): NTask {
    // An exception escaping `Execute` would be logged by Quartz.NET and, for
    // a `JobExecutionException`, could alter the trigger; the Lyric side owns
    // retries and outcomes, so nothing is allowed out.
    try {
      val trigger = ctx.Trigger
      val triggerName = trigger.Description
      val jobId = jobIdOfTrigger(triggerName)
      if self.run(jobId, triggerName, scheduledFireMs(ctx), trigger.GetMayFireAgain()) {
        dropFinished(ctx.Scheduler, jobId)
      }
    } catch Bug as _ {
      ()
    }
    NTask.CompletedTask
  }
}

record DispatchFactory {
  run: (String, String, Long, Bool) -> Bool
}

impl NJobFactory for DispatchFactory {
  func NewJob(bundle: in NBundle, scheduler: in NScheduler): NJob {
    val job: NJob = DispatchJob(run = self.run)
    job
  }

  func ReturnJob(job: in NJob): Unit {
    ()
  }
}

// ─── Trigger end ──────────────────────────────────────────────────────────────
// `ended(jobId, triggerName)` is the Lyric side of `TriggerFinalized`:
// Quartz.NET calls it for a trigger that will never fire again, after the
// trigger's last firing has run (worker thread) or when a misfire policy
// drops its remaining firings (scheduler thread, with no call to the job at
// all).  `ended` answers true when that gave the job its final status and it
// is to be removed from Quartz.NET.  Every other notification is ignored.
record TriggerWatch {
  scheduler: NScheduler
  ended: (String, String) -> Bool
}

impl NListener for TriggerWatch {
  func TriggerFinalized(trigger: in NTrigger, ct: in NCancel): NTask {
    try {
      val triggerName = trigger.Description
      val jobId = jobIdOfTrigger(triggerName)
      if self.ended(jobId, triggerName) {
        dropFinished(self.scheduler, jobId)
      }
    } catch Bug as _ {
      ()
    }
    NTask.CompletedTask
  }

  func JobScheduled(trigger: in NTrigger, ct: in NCancel): NTask {
    NTask.CompletedTask
  }

  func JobUnscheduled(key: in NTriggerKey, ct: in NCancel): NTask {
    NTask.CompletedTask
  }

  func TriggerPaused(key: in NTriggerKey, ct: in NCancel): NTask {
    NTask.CompletedTask
  }

  func TriggersPaused(group: in String, ct: in NCancel): NTask {
    NTask.CompletedTask
  }

  func TriggerResumed(key: in NTriggerKey, ct: in NCancel): NTask {
    NTask.CompletedTask
  }

  func TriggersResumed(group: in String, ct: in NCancel): NTask {
    NTask.CompletedTask
  }

  func JobAdded(detail: in NJobDetail, ct: in NCancel): NTask {
    NTask.CompletedTask
  }

  func JobDeleted(key: in NJobKey, ct: in NCancel): NTask {
    NTask.CompletedTask
  }

  func JobInterrupted(key: in NJobKey, ct: in NCancel): NTask {
    NTask.CompletedTask
  }

  func JobPaused(key: in NJobKey, ct: in NCancel): NTask {
    NTask.CompletedTask
  }

  func JobsPaused(group: in String, ct: in NCancel): NTask {
    NTask.CompletedTask
  }

  func JobResumed(key: in NJobKey, ct: in NCancel): NTask {
    NTask.CompletedTask
  }

  func JobsResumed(group: in String, ct: in NCancel): NTask {
    NTask.CompletedTask
  }

  func SchedulerError(message: in String, cause: in NSchedulerException, ct: in NCancel): NTask {
    NTask.CompletedTask
  }

  func SchedulerInStandbyMode(ct: in NCancel): NTask {
    NTask.CompletedTask
  }

  func SchedulerStarted(ct: in NCancel): NTask {
    NTask.CompletedTask
  }

  func SchedulerStarting(ct: in NCancel): NTask {
    NTask.CompletedTask
  }

  func SchedulerShutdown(ct: in NCancel): NTask {
    NTask.CompletedTask
  }

  func SchedulerShuttingdown(ct: in NCancel): NTask {
    NTask.CompletedTask
  }

  func SchedulingDataCleared(ct: in NCancel): NTask {
    NTask.CompletedTask
  }
}

@externInstance
@externTarget("System.Object.GetType")
func typeOfJob(job: in NJob): NType = ()

func discardFiring(id: in String, trigger: in String, at: in Long, again: in Bool): Bool {
  false
}

// `JobBuilder.Create` wants the `Type` of the job; the factory above ignores
// it and builds the instance itself, but Quartz.NET still checks it
// implements `IJob`, so it is the type of the record that does.
func dispatchJobType(): NType {
  val probe: NJob = DispatchJob(
    run = { id: String, trigger: String, at: Long, again: Bool ->
      discardFiring(id, trigger, at, again)
    }
  )
  typeOfJob(probe)
}

// ─── Scheduler lifecycle ──────────────────────────────────────────────────────
@externInstance
@externTarget("Quartz.Impl.SchedulerRepository.Lookup")
func repositoryLookup(r: in NRepository, name: in String): Option[NScheduler] = ()

func schedulerNameTaken(name: in String): Bool {
  match repositoryLookup(NRepository.Instance, name) {
    case Some(_) -> true
    case None -> false
  }
}

/// Build a scheduler in standby (no trigger fires until `startScheduler`).
/// `run` is called on a worker thread for every firing, `ended` for every
/// trigger Quartz.NET ends (see `DispatchJob` and `TriggerWatch`).  Quartz.NET
/// keeps a process-wide registry of schedulers by name and would silently
/// hand back the existing one, so a name already in use is an `Err`.
pub func newScheduler(
  instanceName: in String,
  workerThreads: in Int,
  misfireThresholdMs: in Int,
  run: in (String, String, Long, Bool) -> Bool,
  ended: in (String, String) -> Bool,
): Result[NScheduler, String]
  requires: instanceName.length > 0
  requires: workerThreads >= 1
  requires: misfireThresholdMs >= 1
{
  try {
    if schedulerNameTaken(instanceName) {
      return Err("a Quartz.NET scheduler named '" + instanceName + "' already exists in this process")
    }
    val props = NProps.new()
    props.Set("quartz.scheduler.instanceName", instanceName)
    props.Set("quartz.threadPool.type", "Quartz.Simpl.DedicatedThreadPool, Quartz")
    props.Set("quartz.threadPool.maxConcurrency", workerThreads.toString())
    props.Set("quartz.jobStore.type", "Quartz.Simpl.RAMJobStore, Quartz")
    props.Set("quartz.jobStore.misfireThreshold", misfireThresholdMs.toString())
    val pending: Task[NScheduler] = NFactory.new(props).GetScheduler()
    val sched = finishWith(pending)
    val factory: NJobFactory = DispatchFactory(run = run)
    sched.JobFactory = factory
    val watch: NListener = TriggerWatch(scheduler = sched, ended = ended)
    sched.ListenerManager.AddSchedulerListener(watch)
    Ok(sched)
  } catch Bug as b {
    Err(describe(b.typeName, b.message))
  }
}

pub func startScheduler(s: in NScheduler): Result[Unit, String] {
  try {
    finish(s.Start())
    Ok(())
  } catch Bug as b {
    Err(describe(b.typeName, b.message))
  }
}

pub func standbyScheduler(s: in NScheduler): Result[Unit, String] {
  try {
    finish(s.Standby())
    Ok(())
  } catch Bug as b {
    Err(describe(b.typeName, b.message))
  }
}

/// Stop the scheduler for good.  With `waitForJobs` the call returns after
/// every executing job has finished.
pub func shutdownScheduler(s: in NScheduler, waitForJobs: in Bool): Result[Unit, String] {
  try {
    finish(s.Shutdown(waitForJobs))
    Ok(())
  } catch Bug as b {
    Err(describe(b.typeName, b.message))
  }
}

/// True while triggers fire: started, not in standby, not shut down.
pub func schedulerRunning(s: in NScheduler): Bool {
  try {
    s.IsStarted and not s.InStandbyMode and not s.IsShutdown
  } catch Bug as _ {
    false
  }
}

pub func schedulerShutDown(s: in NScheduler): Bool {
  try {
    s.IsShutdown
  } catch Bug as _ {
    true
  }
}

// ─── Triggers ─────────────────────────────────────────────────────────────────
func triggerBuilder(triggerName: in String, delayMs: in Long): NTriggerBuilder {
  val b = NTriggerBuilder.Create().WithIdentity(triggerName, jobGroup).WithDescription(triggerName).ForJob(
    jobIdOfTrigger(triggerName),
    jobGroup
  )
  if delayMs > 0i64 {
    b.StartAt(NDateTimeOffset.FromUnixTimeMilliseconds(NDateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + delayMs))
  } else {
    b.StartNow()
  }
}

func cronSchedule(expr: in String, misfire: in Int): NScheduleBuilder {
  val b = NCronSchedule.CronSchedule(expr)
  if misfire == misfireFireNow {
    b.WithMisfireHandlingInstructionFireAndProceed()
  } else if misfire == misfireSkip {
    b.WithMisfireHandlingInstructionDoNothing()
  } else if misfire == misfireIgnore {
    b.WithMisfireHandlingInstructionIgnoreMisfires()
  } else {
    b
  }
}

func intervalSchedule(intervalMs: in Long, repeatCount: in Int, misfire: in Int): NScheduleBuilder {
  val every = NSimpleSchedule.Create().WithInterval(NTimeSpan.FromMilliseconds(intervalMs.toDouble()))
  val b = if repeatCount < 0 { every.RepeatForever() } else { every.WithRepeatCount(repeatCount) }
  if misfire == misfireFireNow {
    b.WithMisfireHandlingInstructionFireNow()
  } else if misfire == misfireSkip {
    b.WithMisfireHandlingInstructionNextWithRemainingCount()
  } else if misfire == misfireIgnore {
    b.WithMisfireHandlingInstructionIgnoreMisfires()
  } else {
    b
  }
}

/// `TriggerPlan.kind` values.
pub val triggerOnce: Int = 0

pub val triggerCron: Int = 1

pub val triggerInterval: Int = 2

/// One trigger to build for a job.  `name` is `<job id>#<n>`.
pub record TriggerPlan {
  name: String
  kind: Int
  /// Quartz cron expression, 6 or 7 fields (`triggerCron`).
  cron: String
  /// Delay before the first firing (`triggerOnce`, `triggerInterval`); 0 =
  /// as soon as a worker is free.
  delayMs: Long
  /// Time between firings (`triggerInterval`).
  intervalMs: Long
  /// Firings after the first, negative = forever (`triggerInterval`).
  repeatCount: Int
  /// One of the `misfire*` codes (`triggerCron`, `triggerInterval`).
  misfire: Int
  invariant: name.length > 0 and delayMs >= 0i64
}

func buildTrigger(plan: in TriggerPlan): NTrigger {
  if plan.kind == triggerCron {
    triggerBuilder(plan.name, 0i64).WithSchedule(cronSchedule(plan.cron, plan.misfire)).Build()
  } else if plan.kind == triggerInterval {
    triggerBuilder(plan.name, plan.delayMs).WithSchedule(intervalSchedule(plan.intervalMs, plan.repeatCount, plan.misfire)).Build()
  } else {
    triggerBuilder(plan.name, plan.delayMs).Build()
  }
}

/// Whether Quartz.NET accepts `expr` as a cron expression.
pub func validQuartzCron(expr: in String): Bool {
  try {
    NCronExpression.IsValidExpression(expr)
  } catch Bug as _ {
    false
  }
}

// ─── Jobs ─────────────────────────────────────────────────────────────────────
// `IScheduler.ScheduleJob(IJobDetail, IReadOnlyCollection<ITrigger>, bool)`
// would store the job and all its triggers in one step, but a Lyric slice
// cannot be passed as an `IReadOnlyCollection<ITrigger>` through auto-FFI.
// The job goes in with its first trigger and the others are added to it.
func scheduleEach(s: in NScheduler, detail: in NJobDetail, plans: in slice[TriggerPlan]): Unit {
  val first: Task[NDateTimeOffset] = s.ScheduleJob(detail, buildTrigger(plans[0]))
  awaiterOf(first).GetResult()
  var i = 1
  while i < plans.length {
    val more: Task[NDateTimeOffset] = s.ScheduleJob(buildTrigger(plans[i]))
    awaiterOf(more).GetResult()
    i = i + 1
  }
}

/// Store job `jobId` with one trigger per plan; each plan's name must be
/// `<jobId>#<n>`.  When a trigger is refused the job is removed again, so it
/// is stored with all of them or not at all; the first trigger may fire
/// before the last one is stored.  The job is not durable: Quartz.NET drops
/// it once its last trigger has fired for the last time.
pub func submitJob(s: in NScheduler, jobId: in String, plans: in slice[TriggerPlan]): Result[Unit, String]
  requires: jobId.length > 0
  requires: plans.length > 0
{
  for plan in plans {
    if plan.kind == triggerCron and not validQuartzCron(plan.cron) {
      return Err("invalid Quartz cron expression '" + plan.cron + "'")
    }
    if plan.kind == triggerInterval and plan.intervalMs < 1i64 {
      return Err("interval must be at least 1ms, got " + plan.intervalMs.toString())
    }
  }
  try {
    val detail = NJobBuilder.Create(dispatchJobType()).WithIdentity(jobId, jobGroup).WithDescription(jobId).Build()
    scheduleEach(s, detail, plans)
    Ok(())
  } catch Bug as b {
    val _ = deleteJob(s, jobId)
    Err(describe(b.typeName, b.message))
  }
}

/// Remove job `jobId` and its triggers.  `Ok(false)` when Quartz.NET no
/// longer has it (never stored, or already finished).
pub func deleteJob(s: in NScheduler, jobId: in String): Result[Bool, String] {
  try {
    val pending: Task[Bool] = s.DeleteJob(NJobKey.new(jobId, jobGroup))
    Ok(finishWith(pending))
  } catch Bug as b {
    Err(describe(b.typeName, b.message))
  }
}

/// Whether Quartz.NET still holds job `jobId`.
pub func jobStored(s: in NScheduler, jobId: in String): Result[Bool, String] {
  try {
    val pending: Task[Bool] = s.CheckExists(NJobKey.new(jobId, jobGroup))
    Ok(finishWith(pending))
  } catch Bug as b {
    Err(describe(b.typeName, b.message))
  }
}

// ─── Signals and time ─────────────────────────────────────────────────────────
/// An event that starts unset and stays set once `raise` has been called.
pub record Signal {
  event: NEvent
}

pub func newSignal(): Signal {
  Signal(event = NEvent.new(false))
}

/// Set `s`, waking every thread blocked in `awaitSignal`.
pub func raise(s: in Signal): Unit {
  s.event.Set()
}

/// Block the calling thread until `s` is set or `ms` have passed; true when
/// `s` is set.
pub func awaitSignal(s: in Signal, ms: in Int): Bool
  requires: ms >= 0
{
  s.event.Wait(ms)
}

/// Milliseconds on a clock that never goes backwards (not the time of day).
pub func monotonicMs(): Long {
  NEnvironment.TickCount64
}

// ─── Diagnostics ──────────────────────────────────────────────────────────────
/// The managed id of the calling thread.
pub func currentThreadId(): Int {
  NEnvironment.CurrentManagedThreadId
}

/// Whether the calling thread belongs to the shared .NET thread pool.  The
/// scheduler's worker threads do not: they are threads of its own
/// `DedicatedThreadPool`.
pub func onSharedThreadPool(): Bool {
  NThread.CurrentThread.IsThreadPoolThread
}
