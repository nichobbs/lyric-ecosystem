# lyric-jobs-quartz

`Lyric.Jobs.Quartz` implements the `Jobs.JobScheduler` interface (defined in `lyric-jobs`,
`nichobbs/lyric-lang`) on the [Quartz Scheduler](https://www.quartz-scheduler.org/). Jobs are stored in
a Quartz `RAMJobStore`, fired by Quartz triggers, and run by application `Jobs.JobHandler`s on Quartz
worker threads.

| | |
|---|---|
| Targets | `jvm` only (`[package] targets = ["jvm"]`; `--target dotnet` fails with `F0054`) |
| Stability | `experimental`, 0.1.0 |
| Quartz | `org.quartz-scheduler:quartz` 2.5.2 (pinned in `[maven]`) |
| Job store | `org.quartz.simpl.RAMJobStore` (in memory, not persistent) |
| Interface | `Lyric.Jobs` (`Jobs.JobScheduler`, `Jobs.JobHandler`), re-exported with `pub use Jobs` |

## Packages

- `Jobs.Quartz` (`src/jobs_quartz.l`): the `JobScheduler` implementation, handler registration and
  the scheduler lifecycle.
- `Jobs.Quartz.Triggers` (`src/triggers.l`): Quartz-only scheduling (native cron, intervals, delayed
  one-shots, misfire policy) and the 5-field cron translation.
- `Jobs.Quartz.Kernel.Jvm` (`src/_kernel_jvm/quartz_kernel.l`): every Java extern. `DispatchJob` and
  `DispatchFactory` are Lyric records implementing `org.quartz.Job` and `org.quartz.spi.JobFactory`;
  there is no Java shim.

## Usage

```lyric
import Std.Core
import Jobs
import Jobs.Quartz
import Jobs.Quartz.Triggers

record SendEmail {
  tag: Int
}

impl Jobs.JobHandler for SendEmail {
  func handle(spec: in Jobs.JobSpec): Result[String, Jobs.JobError] {
    Ok("sent " + spec.payload)
  }
}

func run(): Result[Unit, String] {
  val sched = Quartz.create(Quartz.defaultConfig().copy(workerThreads = 8))?
  Quartz.registerHandler(sched, "send-email", SendEmail(tag = 0))
  Quartz.start(sched)?

  val once = sched.enqueue("send-email", "{\"to\":\"a@example.com\"}", 3, 30000)?
  val nightly = sched.schedule("send-email", "{}", "0 3 * * 1-5", 1, 30000)?
  val everyTenSeconds = Quartz.scheduleTrigger(
    sched, "send-email", "{}", Triggers.quartzCron("0/10 * * * * ?", Triggers.MisfirePolicy.Skip)?, 1, 30000
  )?

  val outcome = Quartz.awaitFinished(sched, once, 60000)?
  Quartz.shutdown(sched, true)
}
```

## Behaviour

Lifecycle. `create` returns a scheduler in standby: jobs can be submitted and cancelled and stay
`Pending`; nothing fires until `start`. `standby` pauses firing, `start` resumes, `shutdown(s,
waitForJobs)` is final (statuses and results stay readable, submissions become `Err`).

Handlers. `registerHandler(s, name, handler)` binds a `Jobs.JobHandler` to a job name;
`registerDefaultHandler` covers names without one. A firing with no handler fails with
`no handler registered for job '<name>'` and is not retried.

Status. `Pending` (waiting for a trigger; a recurring job is `Pending` again between firings),
`Running`, then `Succeeded` or `Failed` after the last possible firing, or `Cancelled`. `results`
returns one `JobResult` per attempt, oldest first, capped at `maxResultsPerJob`. `attemptsRun` and
`timesFired` count attempts and firings; `purgeFinished` forgets finished jobs.

Retries and timeout. A failed attempt (an `Err`, a panic or a timeout) is retried within the same
firing, at once or after `retryDelayMs`, up to `maxAttempts`. The handler runs on the Quartz worker
thread. When an attempt passes `timeoutMs` that thread is interrupted, which ends a handler blocked in a
sleep, a wait or interruptible I/O. The JVM cannot stop a thread that ignores interruption: such a
handler keeps its worker until it returns, and the attempt still counts as timed out.

Cancel. A pending job is removed from Quartz and never runs. A running job is marked `Cancelled`, its
handler is interrupted as for a timeout, and it is not retried. Cancelling a finished or unknown job is
a no-op, as in `Jobs.InProcessJobScheduler`.

Recurring jobs. A firing that comes while the previous firing of the same job is still running is
skipped.

5-field cron (`JobScheduler.schedule`). The expression is validated by `Jobs.parseCron` and translated
by `Triggers.unixCronToQuartz`: seconds are `0`, lists, ranges and steps are written out as explicit
values (`a/n` is read as `a-max/n`), day-of-week is renumbered from Unix `0-7` (0 and 7 are Sunday) to
Quartz `1-7` (1 is Sunday), and the field that is `*` among day-of-month and day-of-week becomes `?`.
When both are given, POSIX fires on either; that becomes two Quartz triggers on one job, which runs once
for a minute both select. A field counts as given unless it is exactly `*` (the Vixie rule that `*/n`
also counts as `*` is not followed). Expressions are evaluated in the JVM default time zone. An
expression that can never fire (`0 0 31 2 *`) is an `Err`.

## Configuration

`QuartzConfig` (start from `defaultConfig()` and `.copy(...)`):

| Field | Default | Meaning |
|---|---|---|
| `instanceName` | `lyric-quartz-<uuid>` | Quartz scheduler name, unique per JVM; a name in use is an `Err` from `create` |
| `workerThreads` | 4 | Quartz worker threads; bounds concurrent handlers |
| `daemonThreads` | false | daemon scheduler and worker threads; when false a started scheduler keeps the JVM alive until `shutdown` |
| `retryDelayMs` | 0 | pause between attempts |
| `maxResultsPerJob` | 100 | attempt results kept per job |
| `misfireThresholdMs` | 60000 | lateness after which a trigger's `MisfirePolicy` applies |

There are no secrets or connection settings: the job store is in memory.

Quartz logs through SLF4J. The package does not choose a logging binding; without one on the classpath
SLF4J prints its "No SLF4J providers were found" notice once and discards Quartz's log output.

## Tests

Everything is hermetic (no external service) and runs on `--target jvm`. Run from the repository root,
with the Maven resolver available to `lyric restore`:

```sh
lyric restore --manifest jobs-quartz/lyric.toml
lyric test --manifest jobs-quartz/lyric.toml --target jvm
```

- `tests/conformance_tests.l`: `Testing.Conformance.Jobs.run` against a Quartz scheduler in standby
  and against a started scheduler whose single worker is busy.
- `tests/execution_tests.l`: real handlers run by Quartz worker threads: output recording, retries,
  panics, timeouts, cancellation before and during a run, handler lookup, lifecycle.
- `tests/trigger_execution_tests.l`: Quartz cron at seconds resolution, interval and delayed triggers,
  overlap skipping, misfire policies, and `schedule` with a 5-field expression firing at the next minute
  (this one test waits up to 65 s).
- `tests/triggers_tests.l`: the cron translation and trigger validation, checked against Quartz's parser.

`lyric build --manifest jobs-quartz/lyric.toml --target dotnet` fails with `F0054`, as intended.

## Known gaps

- Jobs are not persistent: `RAMJobStore` only, so pending and recurring jobs are lost when the process
  exits. JDBC job stores and clustering are not exposed.
- A handler that ignores thread interruption cannot be stopped on timeout or cancel (a JVM limit).
- Cron expressions use the JVM default time zone; there is no per-job time zone.
- Compiler issues met while writing this package (tracked in `nichobbs/lyric-lang`):
  - `impl Interface for T` where `T` is a `pub opaque type` does not produce a working implementation
    on either target, so `QuartzJobScheduler` is a `pub record` whose internals sit in the opaque
    `QuartzState` (lyric-lang#9038).
  - On the JVM a slice literal written directly inside `Ok(...)` is built as a list
    (`ClassCastException` at the use site); `Triggers.unixCronToQuartz` binds it to a typed local first (lyric-lang#9039).
  - `{ workspace = true }` dependencies resolve for `lyric test` only when it is run from the workspace
    root (lyric-lang#9040).
