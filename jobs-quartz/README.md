# lyric-jobs-quartz

`Lyric.Jobs.Quartz` implements the `Jobs.JobScheduler` interface (defined in `lyric-jobs`,
`nichobbs/lyric-lang`) on the [Quartz Scheduler](https://www.quartz-scheduler.org/). Jobs are stored in
a Quartz `RAMJobStore`, fired by Quartz triggers, and run by application `Jobs.JobHandler`s on Quartz
worker threads. It runs on `jvm` only and is `experimental` (0.1.0).

| | |
|---|---|
| Targets | `jvm` only (`[package] targets = ["jvm"]`; `--target dotnet` fails with `F0054`) |
| Stability | `experimental`, 0.1.0 |
| Interface | `Lyric.Jobs` (`Jobs.JobScheduler`, `Jobs.JobHandler`), re-exported with `pub use Jobs` |
| Lyric packages | `Jobs.Quartz` (`src/jobs_quartz.l`): the scheduler, handler registration, lifecycle. `Jobs.Quartz.Triggers` (`src/triggers.l`): Quartz-only triggers and the cron translation. `Jobs.Quartz.Kernel.Jvm` (`src/_kernel_jvm/quartz_kernel.l`): every Java extern |
| Job store | `org.quartz.simpl.RAMJobStore` (in memory, not persistent) |

## Usage

The package is not in the registry yet. Inside this workspace an application declares
`"Lyric.Jobs.Quartz" = { workspace = true }` under `[dependencies]` and runs `lyric restore` once so
that the Quartz JAR is fetched from Maven.

```lyric
import Std.Core
import Jobs
import Jobs.Quartz

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
  val sched = Quartz.create(Quartz.defaultConfig().copy(workerThreads = 8))?
  Quartz.registerHandler(sched, "send-email", SendEmail(tag = 0))
  Quartz.start(sched)?

  // JobScheduler: run once now, with up to 3 attempts of at most 30 s each.
  val once = sched.enqueue("send-email", "{\"to\":\"a@example.com\"}", 3, 30000)?
  // JobScheduler: 5-field Unix cron, 03:00 Monday to Friday.
  val nightly = sched.schedule("send-email", "{}", "0 3 * * 1-5", 1, 30000)?

  val outcome = Quartz.awaitFinished(sched, once, 60000)?
  val attempts = sched.results(once)?
  for r in attempts.items {
    println(r.output + r.error)
  }
  sched.cancel(nightly)?
  Quartz.shutdown(sched, true)?
  Ok(outcome)
}
```

Every function of the package, and every `JobScheduler` method, returns `Result[_, String]`; the
error is a message that starts with `quartz: ` when it comes from Quartz, `cron: ` when the 5-field
expression is invalid, and `job not found: <id>` for an unknown id. The handler's own error type is
`Jobs.JobError`.

Variations:

- **Through the interface.** `QuartzJobScheduler` is a `Jobs.JobScheduler`, so it can be passed to code
  that knows only the interface, and to the wrappers of `Jobs` with their default limits (3 attempts,
  30 s):

  ```lyric
  func viaInterface(jobs: in Jobs.JobScheduler): Result[String, String] {
    Jobs.enqueue(jobs, "send-email", "{}")
  }
  ```

- **Handlers.** `Quartz.registerHandler(sched, name, handler)` binds a handler to a job name and
  replaces an earlier one; `Quartz.registerDefaultHandler(sched, handler)` serves every name without a
  handler of its own; `Quartz.unregisterHandler(sched, name)` removes one. All three may be called at
  any time, from any thread, before or after the jobs are submitted.
- **Lifecycle.** `Quartz.create` returns a scheduler in standby. `Quartz.start` begins (or resumes)
  firing, `Quartz.standby` pauses it, `Quartz.shutdown(sched, waitForJobs)` is final. `Quartz.isRunning`
  and `Quartz.isShutDown` report the state.
- **Introspection.** `Quartz.awaitFinished(sched, id, timeoutMs)` blocks (polling every 5 ms) until the
  job is `Succeeded`, `Failed` or `Cancelled`. `Quartz.attemptsRun` and `Quartz.timesFired` count
  handler attempts and firings, `Quartz.heldByQuartz` tells whether Quartz still stores the job (it
  does until the job is finished), and
  `Quartz.purgeFinished(sched)` forgets every finished job and returns how many.
- **Quartz-only triggers** (`import Jobs.Quartz.Triggers`), submitted with `Quartz.scheduleTrigger`:

  ```lyric
  func quartzOnly(sched: in Quartz.QuartzJobScheduler): Result[Unit, String] {
    val everyTenSeconds = Triggers.quartzCron("0/10 * * * * ?", Triggers.MisfirePolicy.Skip)?
    val fiveTimes = Triggers.interval(0i64, 500i64, 4, Triggers.MisfirePolicy.Smart)?
    val inOneMinute = Triggers.once(60000i64)?
    val a = Quartz.scheduleTrigger(sched, "send-email", "{}", everyTenSeconds, 1, 30000)?
    val b = Quartz.scheduleTrigger(sched, "send-email", "{}", fiveTimes, 1, 30000)?
    val c = Quartz.scheduleTrigger(sched, "send-email", "{}", inOneMinute, 3, 30000)?
    println(a + b + c)
    Ok(())
  }
  ```

## Configuration

`QuartzConfig`; start from `Quartz.defaultConfig()` and `.copy(...)`. There are no environment
variables, no connection settings and no secrets: the job store is in memory.

| Field | Default | Meaning |
|---|---|---|
| `instanceName` | `lyric-quartz-<uuid>` | Quartz scheduler name. Must not be empty and must be unique in the JVM; a name in use is an `Err` from `create`. It is free again after `shutdown`. Worker threads are named `<instanceName>_Worker-<n>` |
| `workerThreads` | 4 | Quartz worker threads (at least 1); bounds how many handlers run at once |
| `daemonThreads` | false | Run the scheduler and worker threads as daemon threads. When false a started scheduler keeps the JVM alive until `shutdown` |
| `retryDelayMs` | 0 | Pause between a failed attempt and the next one of the same firing (0 or more) |
| `maxResultsPerJob` | 100 | Attempt results `results` keeps per job, the most recent (at least 1) |
| `misfireThresholdMs` | 60000 | How late a firing may be before the trigger's `MisfirePolicy` applies (at least 1) |

Per job, the arguments of `enqueue`, `schedule` and `scheduleTrigger`: `maxAttempts` (at least 1) and
`timeoutMs` (at least 1, per attempt). The limits above are record invariants and `requires:`
clauses, so a value outside them panics; it is not an `Err`.

Quartz logs through SLF4J. The package does not choose a logging binding; without one on the classpath
SLF4J prints its "No SLF4J providers were found" notice once and discards Quartz's log output.

## Surprises

Status and results:

- **A recurring job is `Pending` between firings**, and reaches a final status only when none of its
  triggers can fire again; the outcome of the last firing that ran alone decides between `Succeeded`
  and `Failed`. It does not matter how the trigger ended. When its last firing is skipped because an
  earlier firing is still running (see below), that earlier firing is the last one that ran, and the
  job finishes when it ends. When Quartz drops the remaining firings as misfires (`MisfirePolicy.Skip`
  and every remaining firing later than `misfireThresholdMs`, because no worker was free or the
  scheduler was in standby), the job finishes then, or when its running firing ends. A cron job
  without a year field has no last firing, so it never finishes: `awaitFinished` on it times out
  unless the job is cancelled.
- **A job whose every firing was dropped as a misfire is `Cancelled`**, although nobody called
  `cancel`: the handler was never called, `results` is empty, and `attemptsRun` and `timesFired` are
  0. `Failed` always comes with a failed attempt in `results`, and there is none here; `Cancelled` is
  the interface's status for a job that ended without running (in `Jobs.InProcessJobScheduler` too,
  a job that leaves the queue without running is `Cancelled`).
- **A finished job leaves Quartz.** `heldByQuartz` is `Ok(false)` once a job is `Succeeded`, `Failed`
  or `Cancelled`, however its triggers ended.
- **A newly enqueued job is not reliably `Pending`** on a started scheduler: the trigger fires as soon
  as a worker is free, possibly before `enqueue` returns. It is `Pending` for certain only in standby
  or while every worker is busy.
- **`results` holds one entry per attempt, not per job**, over all firings, oldest first, cut to the
  most recent `maxResultsPerJob`. `attemptsRun` keeps counting past that cap. An attempt's status is
  only ever `Succeeded` or `Failed`; a cancelled attempt is a `Failed` entry with the error
  `cancelled while running` while the job is `Cancelled`.
- **Finished jobs are kept in memory until `purgeFinished`.** Statuses and results stay readable after
  the job has ended and after `shutdown`; nothing removes them on its own, so a long-lived scheduler
  that enqueues many jobs must purge. A purged id is unknown: `status` and `results` answer
  `Err("job not found: <id>")`.
- **`cancel` of an unknown or finished job is `Ok`**, as in `Jobs.InProcessJobScheduler`; `status` of an
  unknown job is an `Err`.
- **`JobSpec.scheduledAt` is never 0.** The handler receives the time Quartz scheduled the firing for
  (epoch milliseconds), also for `enqueue`, where the interface documents 0. For a 5-field cron job it
  is the whole minute.

Handlers, retries, timeout, cancel:

- **Handlers belong to the scheduler and are looked up by job name when a trigger fires**, not when the
  job is submitted. A job may be submitted before its handler is registered, and replacing a handler
  changes what pending and recurring jobs run. `Jobs.InProcessJobScheduler` instead takes the handler
  as an argument of `runNext` and runs it on the caller's thread.
- **A firing with no handler is one failed attempt** with the error
  `no handler registered for job '<name>'`; it is not retried, whatever `maxAttempts` says.
- **Retries happen inside the firing, on the same worker thread.** A failed attempt (an `Err`, a panic,
  a timeout) is repeated at once, or after `retryDelayMs`, up to `maxAttempts`. The worker is held for
  the whole chain, the delays included, and each firing of a recurring job gets `maxAttempts` again.
  Only `JobError.message` is recorded; `JobError.code` is dropped. A panic is recorded as
  `handler panicked: <message>`.
- **A timeout interrupts; it cannot kill.** When an attempt passes `timeoutMs` the worker thread is
  interrupted, which ends a handler blocked in a sleep, a wait or interruptible I/O. A handler that
  ignores interruption keeps its worker until it returns; the attempt is still recorded as
  `handler exceeded timeoutMs (<n>ms)` and whatever the handler returned late is discarded.
- **Cancelling a running job** marks it `Cancelled` at once and interrupts the handler in the same way,
  so the handler may still be running when `cancel` returns. The handler is not called again for
  the job once `cancel` has returned: a cancel that arrives between two attempts ends the
  `retryDelayMs` pause at once, no further attempt is started or recorded, and the worker is free.
- **A firing is skipped, not queued, while the previous firing of the same job is still running.**
  `timesFired` counts only the firings that ran. Different jobs with the same name do run concurrently,
  so a handler must be safe to call from several worker threads.
- **A handler that never returns holds a worker thread.** With every worker held, no job fires.

Lifecycle:

- **Nothing fires until `start`.** A scheduler from `create` accepts, cancels and reports jobs, and
  leaves them `Pending`.
- **`shutdown` is final and does not finish the queue.** Later `enqueue`, `schedule`, `scheduleTrigger`
  and `start` are `Err`; jobs that were still `Pending` stay `Pending` for ever (they can still be
  cancelled). With `waitForJobs = false` handlers that are running continue on their threads and
  their results are still recorded. Shutting down twice is `Ok`.
- **With the default `daemonThreads = false` a started scheduler keeps the JVM from exiting** until
  `shutdown` is called.
- **Jobs are lost when the process exits**: the store is a `RAMJobStore`.

5-field cron (`JobScheduler.schedule`):

- **The expression is translated, not passed through.** `Jobs.parseCron` validates it (numbers, `*`,
  ranges, steps and lists only; no names such as `MON`), then `Triggers.unixCronToQuartz` writes every
  list, range and step out as explicit values, sets seconds to `0`, and renumbers day-of-week from
  Unix `0-7` (0 and 7 are Sunday) to Quartz `1-7` (1 is Sunday): `0 3 * * 1-5` becomes
  `0 0 3 ? * 2,3,4,5,6`. `a/n` is read as `a-max/n` (`50/5` in the minute field is `50,55`). The
  function is public, so an application can see what an expression becomes.
- **Day-of-month and day-of-week both given means either, as POSIX defines**, which one Quartz
  expression cannot say. The job gets two Quartz triggers, one per field, and runs once for a minute
  both select. A field counts as given unless it is exactly `*`: `*/2` in day-of-week counts as given
  (the Vixie rule that treats `*/n` as `*` is not followed).
- **An expression that can never fire is an `Err`** from `schedule` (`0 0 31 2 *`,
  `quartz: cannot schedule job ...`), because Quartz refuses a trigger without a fire time.
- **Times are in the JVM default time zone**, with Quartz's handling of daylight-saving changes.
- **Missed firings are not all made up.** These jobs use `MisfirePolicy.Smart`. A firing that is late by
  less than `misfireThresholdMs` runs late. Beyond the threshold (the scheduler was in standby, or no
  worker was free) Quartz fires the job once as soon as it can and drops the other missed times.

## Backend-specific module

`Jobs.Quartz.Triggers` holds what Quartz can do beyond the interface. A `TriggerSpec` is plain data,
built and validated by one of three constructors and submitted with `Quartz.scheduleTrigger`; the job
id it returns works with `status`, `results` and `cancel` like any other.

| Function | |
|---|---|
| `once(delayMs)` | Fire once, `delayMs` from now |
| `quartzCron(expr, misfire)` | A native Quartz cron expression: 6 or 7 fields (`seconds minutes hours day-of-month month day-of-week [year]`), seconds resolution, `L`, `W`, `#`, names; exactly one of the day fields is `?`; day-of-week counts from 1 = Sunday. Validated by Quartz's own parser |
| `interval(delayMs, intervalMs, repeatCount, misfire)` | Fire after `delayMs`, then every `intervalMs`. `repeatCount` is the number of firings after the first, so `repeatCount + 1` in total; negative repeats for ever |
| `isRecurring(spec)` | Whether more than one firing can come from the trigger |
| `unixCronToQuartz(expr)` | The translation `schedule` uses, as one or two Quartz expressions |

`MisfirePolicy` says what happens to a firing that is later than `misfireThresholdMs`: `Smart`
(Quartz's choice for the trigger type), `FireNow` (once, as soon as possible, then the schedule),
`Skip` (drop the missed firings) and `IgnoreMisfires` (run every missed firing back to back).

`Jobs.Quartz.Kernel.Jvm` is the extern boundary: `extern type` declarations bound by auto-FFI against
the Quartz JAR. `DispatchJob`, `DispatchFactory` and `TriggerWatch` are Lyric records implementing
`org.quartz.Job`, `org.quartz.spi.JobFactory` and `org.quartz.SchedulerListener`; there is no Java
shim. `TriggerWatch` is how the scheduler learns that Quartz ended a trigger (`triggerFinalized`),
including one it ended without firing the job; a job is finished when its last trigger has ended and
no firing is running. Every job and trigger lives in the Quartz group
`lyric`, and a job's Quartz name is its Lyric job id (`quartz-<uuid>`). One daemon thread per JVM,
`lyric-quartz-watchdog`, delivers the timeout interrupts for all schedulers.

## Tests

Everything is hermetic (no external service) and runs on `--target jvm`. Run from the repository root,
with the Maven resolver available to `lyric restore`:

```sh
lyric restore --manifest jobs-quartz/lyric.toml
lyric test --manifest jobs-quartz/lyric.toml --target jvm
```

No environment variable is read and there is no integration suite or start script. The run takes about
two minutes.

- `tests/conformance_tests.l`: `Testing.Conformance.Jobs.run` against a Quartz scheduler in standby
  and against a started scheduler whose single worker is busy.
- `tests/execution_tests.l`: real handlers run by Quartz worker threads: output recording, retries,
  panics, timeouts, cancellation before a run, during a run and between two attempts, handler lookup,
  lifecycle.
- `tests/trigger_execution_tests.l`: Quartz cron at seconds resolution, interval and delayed triggers,
  overlap skipping (including a skipped last firing), misfire policies, finite jobs whose remaining or
  whole firings are dropped as misfires, and `schedule` with a 5-field expression firing at the next
  minute (this one test waits up to 65 s).
- `tests/triggers_tests.l`: the cron translation and trigger validation, checked against Quartz's parser.

`lyric build --manifest jobs-quartz/lyric.toml --target dotnet` fails with `F0054`, as intended.

## Pinned versions

| | |
|---|---|
| Quartz | `org.quartz-scheduler:quartz` 2.5.2 (`[maven]` in `lyric.toml`), with its transitive dependencies as Maven resolves them |
| Interface and conformance suite | `Lyric.Jobs` and `Lyric.Conformance` at the `lyric-lang` commit in `.lyric-lang-rev`, through the root `[workspace.overrides]` |

## Known gaps

- Jobs are not persistent: `RAMJobStore` only, so pending and recurring jobs are lost when the process
  exits. JDBC job stores and clustering are not exposed.
- A handler that ignores thread interruption cannot be stopped on timeout or cancel (a JVM limit).
- Cron expressions use the JVM default time zone; there is no per-job time zone.
- `schedule` always uses `MisfirePolicy.Smart`; choose another policy with `Triggers.quartzCron` and
  `scheduleTrigger`.
- Compiler issues met while writing this package (tracked in `nichobbs/lyric-lang`):
  - `impl Interface for T` where `T` is a `pub opaque type` does not produce a working implementation
    on either target, so `QuartzJobScheduler` is a `pub record` whose internals sit in the opaque
    `QuartzState` (lyric-lang#9038).
  - On the JVM a slice literal written directly inside `Ok(...)` is built as a list
    (`ClassCastException` at the use site); `Triggers.unixCronToQuartz` binds it to a typed local first (lyric-lang#9039).
  - `{ workspace = true }` dependencies resolve for `lyric test` only when it is run from the workspace
    root (lyric-lang#9040).
