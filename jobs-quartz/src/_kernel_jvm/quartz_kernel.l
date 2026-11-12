//! Jobs.Quartz.Kernel.Jvm — JVM extern boundary for the Quartz backend.
//!
//! Every binding is an `extern type` plus JVM auto-FFI against the Quartz
//! JAR; there is no host shim.  The kernel owns three things:
//!
//!   - building a Quartz `Scheduler` (StdSchedulerFactory, SimpleThreadPool,
//!     RAMJobStore) and its lifecycle,
//!   - `DispatchJob` / `DispatchFactory`, Lyric records that implement
//!     `org.quartz.Job` and `org.quartz.spi.JobFactory`, so every firing on a
//!     Quartz worker thread calls back into a Lyric closure,
//!   - trigger construction (one-shot, cron, fixed interval) and job removal.
//!
//! No Quartz or JDK exception leaves this file: every entry point returns
//! `Result[_, String]`.
@io
package Jobs.Quartz.Kernel.Jvm

import Std.Core
import Std.Collections

extern type JObject = "java.lang.Object"

extern type JObjects = "java.util.Objects"

extern type JClass = "java.lang.Class"

extern type JDate = "java.util.Date"

extern type JSystem = "java.lang.System"

extern type JHashSet = "java.util.HashSet"

extern type JProperties = "java.util.Properties"

extern type JScheduler = "org.quartz.Scheduler"

extern type JStdSchedulerFactory = "org.quartz.impl.StdSchedulerFactory"

extern type JSchedulerRepository = "org.quartz.impl.SchedulerRepository"

extern type JJob = "org.quartz.Job"

extern type JJobFactory = "org.quartz.spi.JobFactory"

extern type JTriggerFiredBundle = "org.quartz.spi.TriggerFiredBundle"

extern type JJobExecutionContext = "org.quartz.JobExecutionContext"

extern type JJobBuilder = "org.quartz.JobBuilder"

extern type JJobDetail = "org.quartz.JobDetail"

extern type JJobKey = "org.quartz.JobKey"

extern type JTriggerBuilder = "org.quartz.TriggerBuilder"

extern type JTrigger = "org.quartz.Trigger"

extern type JScheduleBuilder = "org.quartz.ScheduleBuilder"

extern type JSimpleScheduleBuilder = "org.quartz.SimpleScheduleBuilder"

extern type JCronScheduleBuilder = "org.quartz.CronScheduleBuilder"

extern type JCronExpression = "org.quartz.CronExpression"

/// Misfire codes shared by `cronTrigger` and `intervalTrigger`.
pub val misfireSmart: Int = 0

pub val misfireFireNow: Int = 1

pub val misfireSkip: Int = 2

pub val misfireIgnore: Int = 3

// Every job and trigger this backend creates lives in one Quartz group; the
// job name is the Lyric job id.
val jobGroup: String = "lyric"

func describe(typeName: in String, message: in String): String {
  if message.length == 0 {
    return typeName
  }
  typeName + ": " + message
}

// ─── Job dispatch ─────────────────────────────────────────────────────────────
// `run(jobId, scheduledFireMs, mayFireAgain)` is the Lyric side of a firing.
// Quartz asks the `JobFactory` for a `Job` each time a trigger fires and then
// calls `execute` on a worker thread.
record DispatchJob {
  run: (String, Long, Bool) -> Unit
}

func scheduledFireMs(ctx: in JJobExecutionContext): Long {
  ctx.getScheduledFireTime().getTime()
}

func mayFireAgain(ctx: in JJobExecutionContext): Bool {
  not JObjects.isNull(ctx.getNextFireTime())
}

func firedJobId(ctx: in JJobExecutionContext): String {
  ctx.getJobDetail().getKey().getName()
}

impl JJob for DispatchJob {
  func execute(ctx: in JJobExecutionContext): Unit {
    // A throwable escaping `execute` would be logged by Quartz and, for a
    // `JobExecutionException`, could alter the trigger; the Lyric side owns
    // retries and outcomes, so nothing is allowed out.
    try {
      self.run(firedJobId(ctx), scheduledFireMs(ctx), mayFireAgain(ctx))
    } catch Bug as _ {
      ()
    }
  }
}

record DispatchFactory {
  run: (String, Long, Bool) -> Unit
}

impl JJobFactory for DispatchFactory {
  func newJob(bundle: in JTriggerFiredBundle, scheduler: in JScheduler): JJob {
    val job: JJob = DispatchJob(run = self.run)
    job
  }
}

func classOf(o: in JObject): JClass {
  o.getClass()
}

func discardFiring(id: in String, at: in Long, again: in Bool): Unit {
  ()
}

// `JobBuilder.newJob` wants the `Class` of the job; the factory above ignores
// it and builds the instance itself, but Quartz still checks it implements
// `Job`, so it is the class of the record that does.
func dispatchJobClass(): JClass {
  val probe: JJob = DispatchJob(run = { id: String, at: Long, again: Bool -> discardFiring(id, at, again) })
  classOf(probe)
}

// ─── Scheduler lifecycle ──────────────────────────────────────────────────────
func schedulerNameTaken(name: in String): Bool {
  not JObjects.isNull(JSchedulerRepository.getInstance().lookup(name))
}

func boolText(b: in Bool): String {
  if b {
    "true"
  } else {
    "false"
  }
}

func setProp(props: in JProperties, key: in String, value: in String): Unit {
  val _ = props.setProperty(key, value)
}

/// Build a scheduler in standby (no trigger fires until `startScheduler`).
/// `run` is called on a worker thread for every firing.  Quartz keeps a
/// process-wide registry of schedulers by name and would silently hand back
/// the existing one, so a name already in use is an `Err`.
pub func newScheduler(
  instanceName: in String,
  workerThreads: in Int,
  daemonThreads: in Bool,
  misfireThresholdMs: in Int,
  run: in (String, Long, Bool) -> Unit,
): Result[JScheduler, String]
  requires: instanceName.length > 0
  requires: workerThreads >= 1
  requires: misfireThresholdMs >= 1
{
  try {
    if schedulerNameTaken(instanceName) {
      return Err("a Quartz scheduler named '" + instanceName + "' already exists in this JVM")
    }
    val props = JProperties.new()
    setProp(props, "org.quartz.scheduler.instanceName", instanceName)
    setProp(props, "org.quartz.scheduler.makeSchedulerThreadDaemon", boolText(daemonThreads))
    setProp(props, "org.quartz.threadPool.class", "org.quartz.simpl.SimpleThreadPool")
    setProp(props, "org.quartz.threadPool.threadCount", workerThreads.toString())
    setProp(props, "org.quartz.threadPool.makeThreadsDaemons", boolText(daemonThreads))
    setProp(props, "org.quartz.jobStore.class", "org.quartz.simpl.RAMJobStore")
    setProp(props, "org.quartz.jobStore.misfireThreshold", misfireThresholdMs.toString())
    val sched = JStdSchedulerFactory.new(props).getScheduler()
    val factory: JJobFactory = DispatchFactory(run = run)
    sched.setJobFactory(factory)
    Ok(sched)
  } catch Bug as b {
    Err(describe(b.typeName, b.message))
  }
}

pub func startScheduler(s: in JScheduler): Result[Unit, String] {
  try {
    s.start()
    Ok(())
  } catch Bug as b {
    Err(describe(b.typeName, b.message))
  }
}

pub func standbyScheduler(s: in JScheduler): Result[Unit, String] {
  try {
    s.standby()
    Ok(())
  } catch Bug as b {
    Err(describe(b.typeName, b.message))
  }
}

/// Stop the scheduler for good.  With `waitForJobs` the call returns after
/// every executing job has finished.
pub func shutdownScheduler(s: in JScheduler, waitForJobs: in Bool): Result[Unit, String] {
  try {
    s.shutdown(waitForJobs)
    Ok(())
  } catch Bug as b {
    Err(describe(b.typeName, b.message))
  }
}

/// True while triggers fire: started, not in standby, not shut down.
pub func schedulerRunning(s: in JScheduler): Bool {
  try {
    s.isStarted() and not s.isInStandbyMode() and not s.isShutdown()
  } catch Bug as _ {
    false
  }
}

pub func schedulerShutDown(s: in JScheduler): Bool {
  try {
    s.isShutdown()
  } catch Bug as _ {
    true
  }
}

// ─── Triggers ─────────────────────────────────────────────────────────────────
func triggerBuilder(triggerName: in String, delayMs: in Long): JTriggerBuilder {
  val b = JTriggerBuilder.newTrigger().withIdentity(triggerName, jobGroup)
  if delayMs > 0i64 {
    b.startAt(JDate.new(JSystem.currentTimeMillis() + delayMs))
  } else {
    b.startNow()
  }
}

/// A trigger that fires once, `delayMs` from now (0 = as soon as a worker is free).
pub func onceTrigger(triggerName: in String, delayMs: in Long): Result[JTrigger, String]
  requires: delayMs >= 0i64
{
  try {
    Ok(triggerBuilder(triggerName, delayMs).build())
  } catch Bug as b {
    Err(describe(b.typeName, b.message))
  }
}

func cronSchedule(expr: in String, misfire: in Int): JScheduleBuilder {
  val b = JCronScheduleBuilder.cronSchedule(expr)
  if misfire == misfireFireNow {
    b.withMisfireHandlingInstructionFireAndProceed()
  } else if misfire == misfireSkip {
    b.withMisfireHandlingInstructionDoNothing()
  } else if misfire == misfireIgnore {
    b.withMisfireHandlingInstructionIgnoreMisfires()
  } else {
    b
  }
}

/// A trigger on a Quartz cron expression (6 or 7 fields).
pub func cronTrigger(triggerName: in String, expr: in String, misfire: in Int): Result[JTrigger, String] {
  try {
    Ok(triggerBuilder(triggerName, 0i64).withSchedule(cronSchedule(expr, misfire)).build())
  } catch Bug as b {
    Err("invalid Quartz cron expression '" + expr + "': " + describe(b.typeName, b.message))
  }
}

func intervalSchedule(intervalMs: in Long, repeatCount: in Int, misfire: in Int): JScheduleBuilder {
  val every = JSimpleScheduleBuilder.simpleSchedule().withIntervalInMilliseconds(intervalMs)
  val b = if repeatCount < 0 { every.repeatForever() } else { every.withRepeatCount(repeatCount) }
  if misfire == misfireFireNow {
    b.withMisfireHandlingInstructionFireNow()
  } else if misfire == misfireSkip {
    b.withMisfireHandlingInstructionNextWithRemainingCount()
  } else if misfire == misfireIgnore {
    b.withMisfireHandlingInstructionIgnoreMisfires()
  } else {
    b
  }
}

/// A trigger that fires after `delayMs` and then every `intervalMs`,
/// `repeatCount` more times (negative = forever).
pub func intervalTrigger(
  triggerName: in String,
  delayMs: in Long,
  intervalMs: in Long,
  repeatCount: in Int,
  misfire: in Int,
): Result[JTrigger, String]
  requires: delayMs >= 0i64
  requires: intervalMs >= 1i64
{
  try {
    Ok(triggerBuilder(triggerName, delayMs).withSchedule(intervalSchedule(intervalMs, repeatCount, misfire)).build())
  } catch Bug as b {
    Err(describe(b.typeName, b.message))
  }
}

/// Whether Quartz accepts `expr` as a cron expression.
pub func validQuartzCron(expr: in String): Bool {
  try {
    JCronExpression.isValidExpression(expr)
  } catch Bug as _ {
    false
  }
}

// ─── Jobs ─────────────────────────────────────────────────────────────────────
/// Store job `jobId` with all of `triggers`, atomically.  The job is not
/// durable: Quartz drops it once its last trigger has finished.
pub func submitJob(s: in JScheduler, jobId: in String, triggers: in slice[JTrigger]): Result[Unit, String]
  requires: jobId.length > 0
  requires: triggers.length > 0
{
  try {
    val detail = JJobBuilder.newJob(dispatchJobClass()).withIdentity(jobId, jobGroup).build()
    val set = JHashSet.new()
    for t in triggers {
      val _ = set.add(t)
    }
    s.scheduleJob(detail, set, false)
    Ok(())
  } catch Bug as b {
    Err(describe(b.typeName, b.message))
  }
}

/// Remove job `jobId` and its triggers.  `Ok(false)` when Quartz no longer
/// has it (never stored, or already finished).
pub func deleteJob(s: in JScheduler, jobId: in String): Result[Bool, String] {
  try {
    Ok(s.deleteJob(JJobKey.jobKey(jobId, jobGroup)))
  } catch Bug as b {
    Err(describe(b.typeName, b.message))
  }
}

/// Whether Quartz still holds job `jobId`.
pub func jobStored(s: in JScheduler, jobId: in String): Result[Bool, String] {
  try {
    Ok(s.checkExists(JJobKey.jobKey(jobId, jobGroup)))
  } catch Bug as b {
    Err(describe(b.typeName, b.message))
  }
}

// ─── Bounded execution on the calling thread ──────────────────────────────────
// A handler runs on the Quartz worker thread itself.  To bound it, a watchdog
// interrupts that thread when the time is up; `interruptRun` does the same on
// request.  The JVM cannot stop a thread that ignores interruption, so such a
// handler overruns and the overrun is reported when it returns.
extern type JThread = "java.lang.Thread"

extern type JRunnable = "java.lang.Runnable"

extern type JThreadFactory = "java.util.concurrent.ThreadFactory"

extern type JScheduledPool = "java.util.concurrent.ScheduledThreadPoolExecutor"

extern type JFuture = "java.util.concurrent.ScheduledFuture"

extern type JTimeUnit = "java.util.concurrent.TimeUnit"

/// `runBounded` outcomes.
pub val runCompleted: Int = 0

pub val runTimedOut: Int = 1

pub val runInterrupted: Int = 2

ref record Watch {
  thread: JThread
  token: Long
  var armed: Bool
  var reason: Int
}

func interruptThread(t: in JThread): Unit {
  t.interrupt()
}

// The instance lock makes "is the run still going" and "interrupt its thread"
// one step, so an interrupt can never land after `disarm` has returned and
// leak into whatever the worker thread runs next.
protected type WatchTable {
  var byKey: Map[String, Watch] = newMap()
  var tokens: Long = 0i64
  entry arm(key: in String, thread: in JThread): Long {
    tokens = tokens + 1i64
    byKey[key] = Watch(thread = thread, token = tokens, armed = true, reason = runCompleted)
    return tokens
  }
  // `token` 0 matches whichever run currently holds `key`.
  entry fire(key: in String, token: in Long, reason: in Int): Bool {
    if not byKey.containsKey(key) {
      return false
    }
    val w: Watch = byKey[key]
    if not w.armed or (token != 0i64 and w.token != token) {
      return false
    }
    w.armed = false
    w.reason = reason
    interruptThread(w.thread)
    return true
  }
  entry disarm(key: in String): Int {
    if not byKey.containsKey(key) {
      return runCompleted
    }
    val w: Watch = byKey[key]
    val _ = byKey.remove(key)
    w.armed = false
    return w.reason
  }
}

val watches: WatchTable = WatchTable()

record Watchdog {
  key: String
  token: Long
}

impl JRunnable for Watchdog {
  func run(): Unit {
    val _ = watches.fire(self.key, self.token, runTimedOut)
  }
}

record DaemonThreads {
  name: String
}

func daemonThread(r: in JRunnable, name: in String): JThread {
  val t = JThread.new(r, name)
  t.setDaemon(true)
  t
}

impl JThreadFactory for DaemonThreads {
  func newThread(r: in JRunnable): JThread {
    daemonThread(r, self.name)
  }
}

func newWatchdogPool(): JScheduledPool {
  val factory: JThreadFactory = DaemonThreads(name = "lyric-quartz-watchdog")
  val pool = JScheduledPool.new(1, factory)
  pool.setRemoveOnCancelPolicy(true)
  pool
}

val watchdogPool: JScheduledPool = newWatchdogPool()

func scheduleWatchdog(key: in String, token: in Long, timeoutMs: in Int): JFuture {
  val dog: JRunnable = Watchdog(key = key, token = token)
  watchdogPool.schedule(dog, timeoutMs.toLong(), JTimeUnit.MILLISECONDS)
}

func cancelWatchdog(f: in JFuture): Unit {
  val _ = f.cancel(false)
}

func clearInterrupt(): Unit {
  val _ = JThread.interrupted()
}

/// Run `action` on the calling thread, interrupting that thread after
/// `timeoutMs`, or earlier when `interruptRun(key)` is called.  Returns
/// `runCompleted`, `runTimedOut` or `runInterrupted`; in the last two cases
/// `action` was interrupted (it may still have run to its end if it ignored
/// that).  `action` must not let a panic escape.  `key` identifies the run
/// while it lasts; one key runs at most once at a time.
pub func runBounded(key: in String, timeoutMs: in Int, action: in () -> Unit): Int
  requires: key.length > 0
  requires: timeoutMs >= 1
{
  val token = watches.arm(key, JThread.currentThread())
  val dog = scheduleWatchdog(key, token, timeoutMs)
  action()
  val reason = watches.disarm(key)
  cancelWatchdog(dog)
  if reason != runCompleted {
    clearInterrupt()
  }
  reason
}

/// Interrupt the run currently registered under `key`, if any.
pub func interruptRun(key: in String): Bool {
  watches.fire(key, 0i64, runInterrupted)
}

// ─── Diagnostics ──────────────────────────────────────────────────────────────
/// The name of the calling thread.  Quartz names its worker threads
/// `<instanceName>_Worker-<n>`.
pub func currentThreadName(): String {
  JThread.currentThread().getName()
}
