# lyric-jobs-quartznet

`Lyric.Jobs.QuartzNet` implements the `Jobs.JobScheduler` interface (defined in `lyric-jobs`,
`nichobbs/lyric-lang`) on [Quartz.NET](https://www.quartz-scheduler.net/). Jobs are stored in a
Quartz.NET `RAMJobStore`, fired by Quartz.NET triggers, and run by application `Jobs.JobHandler`s on
the scheduler's own worker threads. It runs on `dotnet` only and is `experimental` (0.1.0).

It is the .NET sibling of `lyric-jobs-quartz` (Quartz on the JVM, `jobs-quartz/`): same function and
type names, same statuses and results, same cron translation, same `Triggers` module. An application
changes target by depending on the other package and changing one import. What cannot be the same is
listed under [Differences from lyric-jobs-quartz](#differences-from-lyric-jobs-quartz); the one that
matters is that .NET cannot interrupt a running handler.

| | |
|---|---|
| Targets | `dotnet` only (`[package] targets = ["dotnet"]`; `--target jvm` fails with `F0054`) |
| Stability | `experimental`, 0.1.0 |
| Interface | `Lyric.Jobs` (`Jobs.JobScheduler`, `Jobs.JobHandler`), re-exported with `pub use Jobs` |
| Lyric packages | `Jobs.QuartzNet` (`src/jobs_quartznet.l`): the scheduler, handler registration, lifecycle. `Jobs.QuartzNet.Triggers` (`src/triggers.l`): Quartz.NET-only triggers and the cron translation. `Jobs.QuartzNet.Kernel.Dotnet` (`src/_kernel_dotnet/quartznet_kernel.l`): every .NET extern |
| Job store | `Quartz.Simpl.RAMJobStore` (in memory, not persistent) |
| Thread pool | `Quartz.Simpl.DedicatedThreadPool` (the scheduler's own threads, not the shared .NET thread pool) |

## Usage

The package is not in the registry yet. Inside this workspace an application declares
`"Lyric.Jobs.QuartzNet" = { workspace = true }` under `[dependencies]` and runs `lyric restore` once so
that the Quartz.NET assembly is fetched from NuGet.

```lyric
import Std.Core
import Jobs
import Jobs.QuartzNet as Quartz

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

The import alias `Quartz` is what makes this text identical to the `lyric-jobs-quartz` example; without
it the functions are `QuartzNet.create`, `QuartzNet.start` and so on.

Every function of the package, and every `JobScheduler` method, returns `Result[_, String]`; the
error is a message that starts with `quartz: ` when it comes from Quartz.NET, `cron: ` when the 5-field
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
- **A handler that can be stopped.** A running handler is never interrupted (see Surprises). A handler
  that may run long asks `Quartz.stopRequested(sched, spec.id)` now and then; it is true once the job
  has been cancelled or the attempt has run for longer than its `timeoutMs`, and whatever the handler
  returns after that is discarded:

  ```lyric
  record Export {
    sched: Quartz.QuartzJobScheduler
    rows: Int
  }

  impl Jobs.JobHandler for Export {
    func handle(spec: in Jobs.JobSpec): Result[String, Jobs.JobError] {
      var done = 0
      while done < self.rows {
        if Quartz.stopRequested(self.sched, spec.id) {
          return Err(Jobs.JobError(message = "stopped after " + done.toString() + " rows", code = "stopped"))
        }
        Jobs.sleepMs(1)
        done = done + 1
      }
      Ok(done.toString() + " rows")
    }
  }
  ```

- **Lifecycle.** `Quartz.create` returns a scheduler in standby. `Quartz.start` begins (or resumes)
  firing, `Quartz.standby` pauses it, `Quartz.shutdown(sched, waitForJobs)` is final. `Quartz.isRunning`
  and `Quartz.isShutDown` report the state.
- **Introspection.** `Quartz.awaitFinished(sched, id, timeoutMs)` blocks (polling every 5 ms) until the
  job is `Succeeded`, `Failed` or `Cancelled`. `Quartz.attemptsRun` and `Quartz.timesFired` count
  handler attempts and firings, `Quartz.heldByQuartz` tells whether Quartz.NET still stores the job (it
  does until the job is finished), and `Quartz.purgeFinished(sched)` forgets every finished job and
  returns how many.
- **Quartz.NET-only triggers** (`import Jobs.QuartzNet.Triggers`), submitted with
  `Quartz.scheduleTrigger`:

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

The four examples above were compiled and run against this package on `--target dotnet`.

## Configuration

`QuartzConfig`; start from `Quartz.defaultConfig()` and `.copy(...)`. There are no environment
variables, no connection settings and no secrets: the job store is in memory.

| Field | Default | Meaning |
|---|---|---|
| `instanceName` | `lyric-quartz-<uuid>` | Quartz.NET scheduler name. Must not be empty and must be unique in the process; a name in use is an `Err` from `create`. It is free again after `shutdown` |
| `workerThreads` | 4 | Worker threads of the scheduler's `DedicatedThreadPool` (at least 1); bounds how many handlers run at once |
| `retryDelayMs` | 0 | Pause between a failed attempt and the next one of the same firing (0 or more) |
| `maxResultsPerJob` | 100 | Attempt results `results` keeps per job, the most recent (at least 1) |
| `misfireThresholdMs` | 60000 | How late a firing may be before the trigger's `MisfirePolicy` applies (at least 1) |

Per job, the arguments of `enqueue`, `schedule` and `scheduleTrigger`: `maxAttempts` (at least 1) and
`timeoutMs` (at least 1, per attempt). The limits above are record invariants and `requires:`
clauses, so a value outside them panics; it is not an `Err`.

Quartz.NET logs through `Microsoft.Extensions.Logging.Abstractions`. The package configures no logger,
so Quartz.NET's log output is discarded.

## Surprises

Handlers, timeout and cancel (where .NET differs from the JVM package):

- **A running handler is never stopped.** .NET has no way to interrupt a thread, and
  `Jobs.JobHandler.handle(spec)` has no cancellation token, so a handler always runs until it returns.
  `timeoutMs` and `cancel` decide what the attempt counts as; they do not shorten it.
- **A timeout is found out when the handler returns.** An attempt that took longer than `timeoutMs` is
  recorded as failed with `handler exceeded timeoutMs (<n>ms)` and whatever the handler returned, `Ok`
  or `Err`, is discarded. Until the handler returns the job is `Running`, the attempt is not in
  `results`, and the worker thread is held. A retry, if attempts remain, starts only after that. A
  handler that sleeps 5 s under `timeoutMs = 150` with `maxAttempts = 2` therefore takes 10 s to fail,
  not 0.3 s.
- **Cancelling a running job** marks it `Cancelled` at once and removes it from Quartz.NET, but the
  handler keeps running and keeps its worker thread. When it returns, the attempt is recorded as failed
  with `cancelled while running` (or with the timeout error, if the attempt had already passed
  `timeoutMs` when `cancel` was called), its output is discarded, and no further attempt starts.
- **No attempt starts after `cancel` has returned.** A cancel that arrives between two attempts ends
  the `retryDelayMs` pause at once, no further attempt is started or recorded, and the worker is free.
  An attempt counts as started from the moment the worker claims it, just before it calls the handler;
  a cancel at that very moment finds the attempt running, as above.
- **`stopRequested` is the only way to end early**, and it is cooperative: the handler has to hold the
  scheduler and ask. It is also true for a job that is not running or not known.
- **A handler that never returns holds a worker thread for ever**, whatever `timeoutMs` says, and its
  job stays `Running` (or `Cancelled`, once cancelled). With every worker held, no job fires.
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
- **A firing is skipped, not queued, while the previous firing of the same job is still running.**
  `timesFired` counts only the firings that ran. Different jobs with the same name do run concurrently,
  so a handler must be safe to call from several worker threads.

Status and results:

- **A recurring job is `Pending` between firings**, and reaches a final status only when none of its
  triggers can fire again; the outcome of the last firing that ran alone decides between `Succeeded`
  and `Failed`. It does not matter how the trigger ended. When its last firing is skipped because an
  earlier firing is still running, that earlier firing is the last one that ran, and the job finishes
  when it ends. When Quartz.NET drops the remaining firings as misfires (`MisfirePolicy.Skip` and every
  remaining firing later than `misfireThresholdMs`, because no worker was free or the scheduler was in
  standby), the job finishes then, or when its running firing ends. A cron job without a year field
  has no last firing, so it never finishes: `awaitFinished` on it times out unless the job is
  cancelled.
- **A job whose every firing was dropped as a misfire is `Cancelled`**, although nobody called
  `cancel`: the handler was never called, `results` is empty, and `attemptsRun` and `timesFired` are
  0. `Failed` always comes with a failed attempt in `results`, and there is none here; `Cancelled` is
  the interface's status for a job that ended without running.
- **A finished job leaves Quartz.NET**, however its triggers ended. A cancelled job is gone when
  `cancel` returns. A job that ends by itself is removed by the worker thread right after it becomes
  `Succeeded` or `Failed`, so `heldByQuartz` can still be `Ok(true)` for a moment after `awaitFinished`
  has returned.
- **A newly enqueued job is not reliably `Pending`** on a started scheduler: the trigger fires as soon
  as a worker is free, possibly before `enqueue` returns. It is `Pending` for certain only in standby
  or while every worker is busy.
- **`results` holds one entry per attempt, not per job**, over all firings, oldest first, cut to the
  most recent `maxResultsPerJob`. `attemptsRun` keeps counting past that cap. An attempt's status is
  only ever `Succeeded` or `Failed`; a cancelled attempt is a `Failed` entry while the job is
  `Cancelled`.
- **Finished jobs are kept in memory until `purgeFinished`.** Statuses and results stay readable after
  the job has ended and after `shutdown`; nothing removes them on its own, so a long-lived scheduler
  that enqueues many jobs must purge. A purged id is unknown: `status` and `results` answer
  `Err("job not found: <id>")`.
- **`cancel` of an unknown or finished job is `Ok`**, as in `Jobs.InProcessJobScheduler`; `status` of an
  unknown job is an `Err`.
- **`JobSpec.scheduledAt` is never 0.** The handler receives the time Quartz.NET scheduled the firing
  for (epoch milliseconds), also for `enqueue`, where the interface documents 0. For a 5-field cron job
  it is the whole minute.

Lifecycle:

- **Nothing fires until `start`.** A scheduler from `create` accepts, cancels and reports jobs, and
  leaves them `Pending`.
- **`shutdown` is final and does not finish the queue.** Later `enqueue`, `schedule`, `scheduleTrigger`
  and `start` are `Err`; jobs that were still `Pending` stay `Pending` for ever (they can still be
  cancelled). With `waitForJobs = false` handlers that are running continue on their threads and
  their results are still recorded. Shutting down twice is `Ok`.
- **A started scheduler does not keep the process alive.** The worker threads are background threads
  (`Thread.IsBackground`), and Quartz.NET has no setting for that. A program that returns from `main` while jobs are pending
  or running loses them; wait with `awaitFinished` or `shutdown(sched, true)`.
- **Jobs are lost when the process exits**: the store is a `RAMJobStore`.

5-field cron (`JobScheduler.schedule`):

- **The expression is translated, not passed through.** `Jobs.parseCron` validates it (numbers, `*`,
  ranges, steps and lists only; no names such as `MON`), then `Triggers.unixCronToQuartz` writes every
  list, range and step out as explicit values, sets seconds to `0`, and renumbers day-of-week from
  Unix `0-7` (0 and 7 are Sunday) to Quartz `1-7` (1 is Sunday): `0 3 * * 1-5` becomes
  `0 0 3 ? * 2,3,4,5,6`. `a/n` is read as `a-max/n` (`50/5` in the minute field is `50,55`). The
  function is public, so an application can see what an expression becomes.
- **Day-of-month and day-of-week both given means either, as POSIX defines**, which one Quartz
  expression cannot say. The job gets two Quartz.NET triggers, one per field, and runs once for a
  minute both select. A field counts as given unless it is exactly `*`: `*/2` in day-of-week counts as
  given (the Vixie rule that treats `*/n` as `*` is not followed).
- **An expression that can never fire is an `Err`** from `schedule` (`0 0 31 2 *`,
  `quartz: cannot schedule job ...`), because Quartz.NET refuses a trigger without a fire time.
- **Times are in the local time zone of the process** (`TimeZoneInfo.Local`), with Quartz.NET's
  handling of daylight-saving changes.
- **Missed firings are not all made up.** These jobs use `MisfirePolicy.Smart`. A firing that is late by
  less than `misfireThresholdMs` runs late. Beyond the threshold (the scheduler was in standby, or no
  worker was free) Quartz.NET fires the job once as soon as it can and drops the other missed times.

### Differences from lyric-jobs-quartz

Everything not listed here behaves the same on both packages. The test suites are the same tests
except for the timeout and running-cancel cases, which assert the behaviour in the right-hand column.

| | `lyric-jobs-quartz` (JVM) | `lyric-jobs-quartznet` (.NET) |
|---|---|---|
| Import | `import Jobs.Quartz` | `import Jobs.QuartzNet as Quartz` |
| Timeout of a running attempt | The worker thread is interrupted; a handler blocked in a sleep, a wait or interruptible I/O ends at `timeoutMs` | The handler runs to its end; the attempt is recorded as timed out when it returns |
| Cancel of a running job | The worker thread is interrupted in the same way | The handler runs to its end; the attempt is recorded as cancelled when it returns |
| A handler that ignores the request | Possible (it must swallow the interrupt); then as on .NET | Every handler, unless it asks `stopRequested` |
| `Quartz.stopRequested(sched, jobId)` | Does not exist | The cooperative stop check |
| `QuartzConfig.daemonThreads` | Exists; false keeps the JVM alive until `shutdown` | Does not exist: worker threads are always background threads |
| Worker thread names | `<instanceName>_Worker-<n>` | Not derived from `instanceName`; do not rely on them |
| Job with two triggers (`schedule` with both day fields) | Stored in one Quartz call | The job is stored with its first trigger, then the second is added; if that fails the job is removed again and `schedule` is an `Err` |
| Cron time zone | JVM default time zone | `TimeZoneInfo.Local` |
| Logging | SLF4J (prints a notice when no binding is present) | `Microsoft.Extensions.Logging.Abstractions`, silent |
| Duplicate `instanceName` message | `... already exists in this JVM` | `... already exists in this process` |

## Backend-specific module

`Jobs.QuartzNet.Triggers` holds what Quartz.NET can do beyond the interface. A `TriggerSpec` is plain
data, built and validated by one of three constructors and submitted with `Quartz.scheduleTrigger`; the
job id it returns works with `status`, `results` and `cancel` like any other.

| Function | |
|---|---|
| `once(delayMs)` | Fire once, `delayMs` from now |
| `quartzCron(expr, misfire)` | A native Quartz cron expression: 6 or 7 fields (`seconds minutes hours day-of-month month day-of-week [year]`), seconds resolution, `L`, `W`, `#`, names; exactly one of the day fields is `?`; day-of-week counts from 1 = Sunday. Validated by Quartz.NET's own parser |
| `interval(delayMs, intervalMs, repeatCount, misfire)` | Fire after `delayMs`, then every `intervalMs`. `repeatCount` is the number of firings after the first, so `repeatCount + 1` in total; negative repeats for ever |
| `isRecurring(spec)` | Whether more than one firing can come from the trigger |
| `unixCronToQuartz(expr)` | The translation `schedule` uses, as one or two Quartz expressions |

`MisfirePolicy` says what happens to a firing that is later than `misfireThresholdMs`: `Smart`
(Quartz.NET's choice for the trigger type), `FireNow` (once, as soon as possible, then the schedule),
`Skip` (drop the missed firings) and `IgnoreMisfires` (run every missed firing back to back).

`Jobs.QuartzNet.Kernel.Dotnet` is the extern boundary: `extern type` declarations bound by auto-FFI
against the Quartz.NET assembly, and four `@externTarget` bindings where auto-FFI does not reach
(`Task<T>.Result`, `Task.GetAwaiter` on a `Task<T>`, `Object.GetType` on an interface value, and
`SchedulerRepository.Lookup` with its nullable result as an `Option`). `DispatchJob`,
`DispatchFactory` and `TriggerWatch` are Lyric records implementing `Quartz.IJob`,
`Quartz.Spi.IJobFactory` and `Quartz.ISchedulerListener`; there is no C# shim. Quartz.NET's API returns
`Task`s; with a `RAMJobStore` nothing in it waits for I/O, so the kernel blocks on each task, and
`IJob.Execute` runs the whole firing on the worker thread and returns a completed task. `TriggerWatch`
is how the scheduler learns that Quartz.NET ended a trigger (`TriggerFinalized`), including one it
ended without firing the job; a job is finished when its last trigger has ended and no firing is
running. Every job and trigger lives in the Quartz.NET group `lyric`; a job's Quartz.NET name is its
Lyric job id (`quartz-<uuid>`) and a trigger's is `<job id>#<n>`.

## Tests

Everything is hermetic (no external service) and runs on `--target dotnet`. Run from the repository
root:

```sh
lyric restore --manifest jobs-quartznet/lyric.toml
lyric test --manifest jobs-quartznet/lyric.toml --target dotnet
```

No environment variable is read and there is no integration suite or start script. The run takes about
two minutes. `lyric restore` writes `Lyric.Jobs.QuartzNet-restore.csproj` and `obj/` into the package
directory; the package's `.gitignore` excludes them.

- `tests/conformance_tests.l`: `Testing.Conformance.Jobs.run` against a Quartz.NET scheduler in standby
  and against a started scheduler whose single worker is busy.
- `tests/execution_tests.l`: real handlers run by Quartz.NET worker threads: output recording, retries,
  panics, timeouts (overrun, held worker, `stopRequested`), cancellation before a run, during a run,
  after the timeout and between two attempts, handler lookup, lifecycle, duplicate instance name.
- `tests/trigger_execution_tests.l`: Quartz cron at seconds resolution, interval and delayed triggers,
  overlap skipping (including a skipped last firing), misfire policies, finite jobs whose remaining or
  whole firings are dropped as misfires, and `schedule` with a 5-field expression firing at the next
  minute (this one test waits up to 65 s).
- `tests/triggers_tests.l`: the cron translation and trigger validation, checked against Quartz.NET's
  parser.

`lyric build --manifest jobs-quartznet/lyric.toml --target jvm` fails with `F0054`, as intended.

## Pinned versions

| | |
|---|---|
| Quartz.NET | NuGet `Quartz` 3.22.10 (`[nuget]` in `lyric.toml`), with its one dependency `Microsoft.Extensions.Logging.Abstractions` 2.1.1 as NuGet resolves it |
| .NET | `net10.0` |
| Interface and conformance suite | `Lyric.Jobs` and `Lyric.Testing` at the `lyric-lang` commit in `.lyric-lang-rev`, through the root `[workspace.overrides]` |

Quartz.NET 4.x is not supported: its `IJob.Execute` returns a `ValueTask`.

## Known gaps

- Jobs are not persistent: `RAMJobStore` only, so pending and recurring jobs are lost when the process
  exits. ADO.NET job stores and clustering are not exposed.
- A running handler cannot be stopped on timeout or cancel (see Surprises); `Jobs.JobHandler` would
  need a cancellation token for that.
- Cron expressions use the process's local time zone; there is no per-job time zone.
- `schedule` always uses `MisfirePolicy.Smart`; choose another policy with `Triggers.quartzCron` and
  `scheduleTrigger`.
- Compiler issues met while writing this package (to be filed in `nichobbs/lyric-lang`; none is worked
  around with a shim):
  - `impl Interface for T` where `T` is a `pub opaque type` does not produce a working implementation,
    so `QuartzJobScheduler` is a `pub record` whose internals sit in the opaque `QuartzState`
    (lyric-lang#9038).
  - `{ workspace = true }` dependencies resolve for `lyric test` only when it is run from the workspace
    root (lyric-lang#9040).
  - MSIL: a `match` on a `Result[slice[K.X], String]`, where `X` is an extern type of another package
    of the project named through an import alias, builds and then panics at run time with
    `match not exhaustive`. Triggers are therefore described to the kernel as plain `TriggerPlan`
    records and built inside it.
  - MSIL auto-FFI: a member of a `Nullable<T>` result, and a member that a `Task<T>` result inherits
    from `Task`, build and then panic at run time with `unsupported method`. The kernel reads
    `ScheduledFireTimeUtc` (a `DateTimeOffset?`) through `String.Format` and binds `Task<T>` members
    with `@externTarget`.
  - MSIL auto-FFI: a member declared on a generic base class is not found (`JobKey.Name` is on
    `Key<T>`), so job and trigger ids travel in the Quartz.NET description field.
  - MSIL auto-FFI: a slice is not accepted where the host parameter is `IReadOnlyCollection<T>`, and an
    `@externTarget` binding declared with the slice builds and fails with `MissingMethodException`. A
    job's triggers are therefore stored one by one.
