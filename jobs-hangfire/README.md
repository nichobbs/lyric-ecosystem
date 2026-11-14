# lyric-jobs-hangfire

`Lyric.Jobs.Hangfire` implements the `Jobs.JobScheduler` interface (defined in `lyric-jobs`,
`nichobbs/lyric-lang`) on [Hangfire](https://www.hangfire.io/). Jobs are Hangfire background jobs in a
Hangfire storage, fetched by the workers of a Hangfire `BackgroundJobServer`, and run by application
`Jobs.JobHandler`s. It runs on `dotnet` only and is `experimental` (0.1.0).

Unlike the Quartz packages, nothing about a job is held in the scheduler: a job, its state, its
attempts and its results are in the storage. Several schedulers, in one process or in several, can
share a storage, and with a persistent storage jobs outlive the process.

| | |
|---|---|
| Targets | `dotnet` only (`[package] targets = ["dotnet"]`; `--target jvm` fails with `F0054`) |
| Stability | `experimental`, 0.1.0 |
| Interface | `Lyric.Jobs` (`Jobs.JobScheduler`, `Jobs.JobHandler`), re-exported with `pub use Jobs` |
| Storages | In memory (`Hangfire.InMemory`); SQLite file (`Hangfire.Storage.SQLite`), which needs a native library on the search path, see [SQLite storage](#sqlite-storage) |
| Lyric packages | `Jobs.Hangfire` (`src/jobs_hangfire.l`): storages, the scheduler, handlers, lifecycle. `Jobs.Hangfire.Native` (`src/native.l`): Hangfire-only features. `Jobs.Hangfire.Store` (`src/store.l`): what is kept in the storage and how statuses are read from it. `Jobs.Hangfire.Kernel.Dotnet` (`src/_kernel_dotnet/hangfire_kernel.l`): every .NET extern |

## Usage

The package is not in the registry yet. Inside this workspace an application declares
`"Lyric.Jobs.Hangfire" = { workspace = true }` under `[dependencies]` and runs `lyric restore` once so
that the Hangfire assemblies are fetched from NuGet.

```lyric
import Std.Core
import Jobs
import Jobs.Hangfire as Hangfire

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
```

Every function of the package, and every `JobScheduler` method, returns `Result[_, String]`; the error
is a message that starts with `hangfire: ` when it comes from Hangfire or the storage, `cron: ` when
the 5-field expression is invalid, and `job not found: <id>` for an unknown id. The handler's own
error type is `Jobs.JobError`.

Variations:

- **Client and server.** A scheduler from `create` is a Hangfire client: it submits, cancels and
  reports jobs, which stay `Pending`. `Hangfire.start` adds a `BackgroundJobServer` whose workers run
  the jobs of the scheduler's queue; `Hangfire.shutdown(sched, waitForJobs)` stops it and leaves the
  client, and `start` may be called again. `Hangfire.isRunning` tells whether the scheduler has a
  server. A process that only submits (a web front end, say) never calls `start`; another process,
  with the same storage and the handlers, does.
- **Through the interface.** `HangfireJobScheduler` is a `Jobs.JobScheduler`, so it can be passed to
  code that knows only the interface, and to the wrappers of `Jobs` with their default limits (3
  attempts, 30 s):

  ```lyric
  func viaInterface(jobs: in Jobs.JobScheduler): Result[String, String] {
    Jobs.enqueue(jobs, "send-email", "{}")
  }
  ```

- **Handlers.** `Hangfire.registerHandler(sched, name, handler)` binds a handler to a job name and
  replaces an earlier one; `Hangfire.registerDefaultHandler(sched, handler)` serves every name without
  a handler of its own; `Hangfire.unregisterHandler(sched, name)` removes one. All three may be called
  at any time, from any thread, before or after the jobs are submitted.
- **A handler that can be stopped.** A running handler is never interrupted (see Surprises). A handler
  that may run long asks `Hangfire.stopRequested(sched, spec.id)` now and then; it is true once the job
  has been cancelled or the attempt has run for longer than its `timeoutMs`, and whatever the handler
  returns after that is discarded:

  ```lyric
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
  ```

- **Waiting.** `Hangfire.awaitFinished(sched, id, timeoutMs)` blocks (reading the status every 10 ms)
  until the job is `Succeeded`, `Failed` or `Cancelled`.
- **Hangfire-only features** (`import Jobs.Hangfire.Native as Native`):

  ```lyric
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
  ```

The examples above are `tests/readme_examples_tests.l`, which compiles and runs them.

## Configuration

There are no environment variables and no secrets in the package itself. Everything is an argument.

Storage, passed to `create`:

| Function | |
|---|---|
| `Hangfire.inMemoryStorage()` | A new, empty storage in process memory. Jobs are lost when the process exits |
| `Hangfire.sqliteStorage(path, queuePollIntervalMs)` | The SQLite database file `path`, created when missing. Workers look for new jobs every `queuePollIntervalMs` (at least 1; Hangfire.Storage.SQLite's own default is 15000). See [SQLite storage](#sqlite-storage) |
| `Hangfire.keepFinishedJobsFor(storage, ms)` | How long the storage keeps a job after it succeeded or was cancelled (Hangfire's default: one day). Applies to jobs that finish afterwards, in this process |

`HangfireConfig`; start from `Hangfire.defaultConfig()` and `.copy(...)`:

| Field | Default | Meaning |
|---|---|---|
| `queue` | `default` | The Hangfire queue the scheduler submits to and its server fetches from. Lowercase letters, digits, `_` and `-`; anything else is an `Err` from `create` |
| `workerCount` | 4 | Worker threads of the server (at least 1); bounds how many handlers run at once |
| `schedulePollingIntervalMs` | 1000 | How often the server looks for delayed jobs that are due (at least 1; Hangfire's own default is 15000). Affects `Native.enqueueIn` and retries with a delay, not recurring jobs |
| `retryDelayMs` | 0 | Pause between a failed attempt and the next one (0 or more). 0 puts the job straight back on the queue |
| `maxResultsPerJob` | 100 | Attempt results `results` keeps per job, the most recent (at least 1); for a recurring job also how many firings are remembered |
| `shutdownTimeoutMs` | 15000 | How long `shutdown(sched, true)` waits for running jobs, and how long a job that is running at shutdown may still take and have its outcome stored (0 or more) |

Per job, the arguments of `enqueue`, `schedule` and the `Native` functions: `maxAttempts` (at least 1)
and `timeoutMs` (at least 1, per attempt). The numeric limits above are record invariants and
`requires:` clauses, so a value outside them panics; it is not an `Err`.

Hangfire logs through its own `LogProvider`. The package configures no logger, so Hangfire's log
output is discarded. Hangfire's process-wide settings (`GlobalConfiguration`, `JobStorage.Current`,
`GlobalJobFilters`) are neither read nor changed.

### SQLite storage

`Hangfire.Storage.SQLite` calls the native library `e_sqlite3` (NuGet package
`SQLitePCLRaw.lib.e_sqlite3`). `lyric restore` downloads it, but `lyric build`, `lyric run` and
`lyric test` do not copy native NuGet assets next to the program, so the library has to be put on the
operating system's library search path by hand. On Linux x64:

```sh
export LD_LIBRARY_PATH="$HOME/.nuget/packages/sqlitepclraw.lib.e_sqlite3/2.1.14/runtimes/linux-x64/native:$LD_LIBRARY_PATH"
```

Without it `sqliteStorage` is an `Err` (`TypeInitializationException: The type initializer for
'SQLite.SQLiteConnection' threw an exception`). The in-memory storage needs nothing of the kind. This
has been run on Linux x64 only.

## Surprises

Where the jobs are:

- **Everything is in the storage.** `status`, `results` and `cancel` are storage reads and writes. Any
  scheduler on the same storage can report or cancel any job, whichever scheduler submitted it.
- **A job id is Hangfire's own id, and only unique within a storage.** With the in-memory and SQLite
  storages ids are `1`, `2`, `3`, ...; two storages hand out the same ids.
- **Hangfire forgets finished jobs.** A job that succeeded or was cancelled is removed by Hangfire one
  day later (`keepFinishedJobsFor` changes that); from then on `status` and `results` answer
  `Err("job not found: <id>")`. A `Failed` job is kept. `Hangfire.InMemory` has an upper limit of its
  own for the retention (three hours by default, its `MaxExpirationTime`; from its source, not
  exercised by the tests).
- **The status is Hangfire's state.** `Enqueued`, `Scheduled` and `Awaiting` are `Pending`,
  `Processing` is `Running`, `Deleted` is `Cancelled`. A job that waits for its next attempt is
  `Pending` again; it is never reported `Failed` between attempts.
- **Hangfire's own tools can show a retried job as `Failed` for a moment on SQLite.** The status is
  read from the job's record. Hangfire also keeps a state history, and when an attempt fails and is
  retried, the `Failed` state Hangfire was about to apply is written to that history before the job
  goes back to its queue. `Hangfire.Storage.SQLite` answers Hangfire's `GetStateData` from the history
  and writes the two in separate statements, so code that asks Hangfire directly (not through this
  package) can see `Failed` for an instant. The history of a job that was retried shows a `Failed`
  entry per failed attempt on every storage.
- **A newly enqueued job is not reliably `Pending`** on a started scheduler: a worker may take it
  before `enqueue` returns. It is `Pending` for certain only while no server runs on its queue or
  every worker is busy.

Handlers, timeout and cancel:

- **A running handler is never stopped.** .NET has no way to interrupt a thread, and
  `Jobs.JobHandler.handle(spec)` has no cancellation token, so a handler always runs until it returns.
  `timeoutMs` and `cancel` decide what the attempt counts as; they do not shorten it.
- **A timeout is found out when the handler returns.** An attempt that took longer than `timeoutMs` is
  recorded as failed with `handler exceeded timeoutMs (<n>ms)` and whatever the handler returned is
  discarded. Until the handler returns the job is `Running` and the worker is held.
- **Cancelling a running job** makes it `Cancelled` at once (Hangfire's `Deleted` state), but the
  handler keeps running and keeps its worker. When it returns, the attempt is recorded as failed with
  `cancelled while running`, its output is discarded, and the job is not retried.
- **`stopRequested` is the only way to end early**, and it is cooperative: the handler has to hold the
  scheduler and ask. It reads the job's state from the storage, at most every 50 ms. It is also true
  for a job the scheduler is not running.
- **Handlers belong to the scheduler that runs the job and are looked up when an attempt starts**, not
  when the job is submitted. A job may be submitted before its handler is registered, by a scheduler
  that has no handlers at all.
- **An attempt with no handler fails the job at once** with `no handler registered for job '<name>'`;
  it is not retried, whatever `maxAttempts` says.
- **Every scheduler with a server on a queue takes jobs from it.** Two started schedulers on the same
  storage and the same queue share the jobs, and each runs what it fetches with its own handlers; give
  them the same handlers, or different queues.
- **A cancelled job stays `Cancelled`, and a finished job stays as it is.** `cancel` of a job that is
  unknown, `Succeeded`, `Failed` or `Cancelled` is `Ok` and changes nothing.

Retries:

- **Hangfire's automatic retry (10 attempts with a growing delay) is not used.** `maxAttempts` is the
  number of attempts; a job with `maxAttempts = 1` runs once.
- **Every attempt is a separate pass through Hangfire.** After a failed attempt (an `Err`, a panic, a
  timeout) the job goes back to its queue, behind the jobs already waiting, and any worker of any
  server on that queue may run the next attempt. With `retryDelayMs > 0` it goes to Hangfire's
  `Scheduled` state instead and no worker is held during the pause.
- **A delay is at least what was asked, and up to one second and one polling interval more.** Hangfire
  stores the due time in whole seconds, so `retryDelayMs` and the delay of `Native.enqueueIn` are
  rounded up to the next second, and a server finds the job due at its next look
  (`schedulePollingIntervalMs`).
- **Only `JobError.message` is recorded;** `JobError.code` is dropped. A panic is recorded as
  `handler panicked: <message>`.
- **In Hangfire's own view** (its dashboard, its counts) a successful job carries the handler's output
  as its result, and a job that used up its attempts is `Failed` with a Lyric panic as the exception.
- **`results` holds one entry per attempt, not per job**, oldest first, cut to the most recent
  `maxResultsPerJob`. An attempt's status is only ever `Succeeded` or `Failed`; a cancelled attempt is
  a `Failed` entry while the job is `Cancelled`.
- **Hangfire runs a job at least once, not exactly once.** A job whose handler is still running when
  the process dies, or when `shutdownTimeoutMs` has passed after a shutdown, stays in the storage and
  is run again by a later server; the interrupted attempt is not in `results` (Hangfire's behaviour,
  not exercised by the tests).

Recurring jobs (`schedule`, `Native.scheduleCron`):

- **The expression goes to Hangfire unchanged, and is evaluated in UTC**, not in the local time zone.
  `schedule` first validates it with `Jobs.parseCron`, so it accepts exactly the interface's dialect
  (five fields; numbers, `*`, ranges, steps and lists). `Native.scheduleCron` skips that check.
- **When both day-of-month and day-of-week are restricted, a day must match both.** POSIX cron, and the
  Quartz packages, fire when either matches. `0 3 13 * 5` fires at 03:00 on a Friday the 13th only.
- **A recurring job never finishes.** Its id (`lyric-recurring-<uuid>`) stands for the schedule;
  Hangfire creates a new background job for every firing. The status is `Running` while one of its 8
  most recent firings is running, `Pending` otherwise, and `Cancelled` after `cancel`; it is never
  `Succeeded` or `Failed`, and `awaitFinished` on it ends only when it is cancelled.
- **`JobSpec.id` is the recurring job's id for every firing**, and `JobSpec.scheduledAt` is the time
  Hangfire created the firing's background job (0 for a one-off job, delayed ones included).
- **`results` of a recurring job are the attempts of its most recent `maxResultsPerJob` firings**,
  ordered by firing, then by attempt. Each firing is retried on its own, up to `maxAttempts`. A firing
  is known from the moment a worker starts it, and its results disappear when Hangfire forgets its
  background job (one day after it succeeded).
- **Firings overlap.** A firing that outlasts the interval does not hold back the next one; two firings
  of the same job then run at the same time, on different workers. `stopRequested(sched, id)` answers
  for the one that started last.
- **Cancelling a recurring job** removes the schedule. A firing that is running goes on and is recorded
  as `cancelled while running`; a firing that was already queued is processed by Hangfire without the
  handler being called (Hangfire shows it as succeeded, with the result `not run: the recurring job was
  cancelled`).
- **Nothing fires without a server.** Hangfire's recurring-job scheduler is part of the server; with
  several servers on a storage, any of them creates the firing, on the queue of the scheduler that
  scheduled the job. After a time without any server Hangfire fires a job once for the firings it
  missed (Hangfire's default misfire handling, not exercised by the tests).
- **An expression that never comes due is accepted** (`0 0 31 2 *`); the job is `Pending` for ever.

Queues and other Hangfire servers:

- **A scheduler has one queue**, and each job remembers the queue it was submitted to (the job
  parameter `lyric.queue`). A delayed job that comes due is put on that queue even when the server of
  another scheduler finds it.
- **A Hangfire server that this package did not start cannot run these jobs.** On a C# server that
  shares the storage and the queue, an attempt ends with the exception `job <id> was picked up by a
  Hangfire server that Lyric.Jobs.Hangfire did not start; it has no handlers`, and that server's own
  retry rules apply. Keep such servers on other queues (not exercised by the tests: there is no C#
  server in them).
- **`Native.counts`**: `enqueued` is the scheduler's own queue, the other numbers are the whole
  storage. A job that was cancelled while it waited stays counted as enqueued until a worker discards
  it.

Lifecycle:

- **`shutdown(sched, false)` returns at once** and leaves running handlers to finish; their outcome is
  still stored when they return within `shutdownTimeoutMs`. `shutdown(sched, true)` waits that long and
  is an `Err` naming the number of jobs still running when a handler has not returned by then; the
  server is stopped either way. Shutting down twice is `Ok`.
- **A started scheduler does not keep the process alive.** Hangfire's worker threads are background
  threads (`IsBackground`, in Hangfire's source). A program that returns from `main` while jobs are running abandons them; wait with
  `awaitFinished` or `shutdown(sched, true)`.
- **`start` on a started scheduler is an `Err`.**

### Differences from the Quartz packages

`lyric-jobs-quartznet` and `lyric-jobs-quartz` implement the same interface with jobs held in memory.

| | Quartz packages | `lyric-jobs-hangfire` |
|---|---|---|
| Where jobs, statuses and results live | In the scheduler object | In the Hangfire storage, shared by every scheduler on it |
| Persistence | None | With `sqliteStorage` |
| Before `start` | Standby: jobs accepted, nothing fires | Client: the same, and another scheduler's server may run the jobs |
| After `shutdown` | Final; submissions are an `Err` | Client again; `start` may be called again |
| Retries | Inside one firing, on the same worker | The job goes back to the queue (or to `Scheduled`) between attempts |
| Cron time zone | Local | UTC |
| Both day fields restricted | Either may match | Both must match |
| Recurring job whose firing outlasts the interval | The next firing is skipped | The firings overlap |
| Recurring job status | `Pending` between firings, final when its trigger ends | `Pending` or `Running`; only ever ends as `Cancelled` |
| Finished jobs | Kept until `purgeFinished` | Removed by Hangfire after its retention time |
| Backend-only module | `Triggers` (native cron, intervals, misfire policies) | `Native` (delays, continuations, native cron, manual trigger, counts) |

## Backend-specific module

`Jobs.Hangfire.Native` holds what Hangfire can do beyond the interface. Every function takes the
scheduler; the job ids work with `status`, `results` and `cancel` like any other.

| Function | |
|---|---|
| `enqueueIn(sched, name, payload, delayMs, maxAttempts, timeoutMs)` | Run once, `delayMs` from now (Hangfire's `Scheduled` state) |
| `enqueueAfter(sched, parentJobId, name, payload, maxAttempts, timeoutMs)` | A Hangfire continuation: run once job `parentJobId` has succeeded (`Awaiting` until then). When the parent fails or is cancelled it stays `Pending`; cancel it then. `Err` for an unknown or recurring parent |
| `scheduleCron(sched, name, payload, cron, maxAttempts, timeoutMs)` | A recurring job on a Hangfire cron expression: five fields, or six with seconds first, with names (`MON`), `L`, `W`, `#`, `?` and macros (`@hourly`). Validated by Hangfire |
| `triggerNow(sched, recurringJobId)` | Fire a recurring job now, besides its schedule; returns Hangfire's id of the firing |
| `firings(sched, recurringJobId)` | Hangfire's ids of the background jobs a recurring job has been run as, oldest first |
| `hangfireState(sched, jobId)` | Hangfire's own state name of a background job |
| `counts(sched)` | Hangfire's job counts: `enqueued`, `scheduled`, `processing`, `succeeded`, `failed`, `deleted` |

How it works. Hangfire normally takes a job as a C# expression tree, which Lyric cannot write. The
kernel builds the job from `Hangfire.Storage.InvocationData` instead, the four strings Hangfire itself
stores for every job (type, method, parameter types, arguments). Every job names the same method, the
Lyric function `Jobs.Hangfire.Kernel.Dotnet.dispatch(name, payload, maxAttempts, timeoutMs,
recurringId, PerformContext)`, which the compiler emits as a public static method; a Hangfire worker
invokes it by reflection like any C# job method. Two Lyric records implement Hangfire filter
interfaces and are installed in the server's own filter list: `Binder` (`IServerFilter`) tells
`dispatch` which scheduler the calling worker belongs to, and `Elector` (`IElectStateFilter`) turns a
failed attempt into a retry and keeps jobs on their queue. A third, `Stamper` (`IClientFilter`),
stamps each new job with its queue. There is no C# shim.

`Jobs.Hangfire.Store` documents the storage layout: the job parameters `lyric.attempts`,
`lyric.results`, `lyric.retry`, `lyric.queue` and `lyric.firing`, and for a recurring job the sorted
sets `lyric:job:<id>` and `lyric:firings:<id>`. Only string-valued storage calls are used, which every
Hangfire storage implements.

## Tests

Run from the repository root:

```sh
lyric restore --manifest jobs-hangfire/lyric.toml
lyric test --manifest jobs-hangfire/lyric.toml --target dotnet
```

The hermetic suites use the in-memory storage, read no environment variable and take about a minute
and a half.

- `tests/conformance_tests.l`: `Testing.Conformance.Jobs.run` against a scheduler without a server and
  against a started scheduler whose single worker is busy.
- `tests/execution_tests.l`: real handlers run by Hangfire workers: outputs, payload fidelity, retries
  (with and without a delay, and the status read continuously through sixteen attempts), panics,
  timeouts, `stopRequested`, cancellation before and during a run,
  handler lookup, job retention, worker count, queues and shared storages, lifecycle.
- `tests/native_tests.l`: recurring jobs fired by Hangfire (seconds resolution, the 5-field dialect at
  the next minute, both day fields, overlap, retries, cancel, manual trigger, kept firings), delayed
  jobs, continuations, queues across schedulers that share a storage, counts.
- `tests/readme_examples_tests.l`: the examples of this file.

The integration suite runs the backend on a SQLite storage, including jobs, schedules and results
that survive closing and reopening the database file, and the same continuous status read through
sixteen attempts. It is compiled only with `--features
integration` and needs:

| Variable | |
|---|---|
| `LYRIC_HANGFIRE_SQLITE_DIR` | A writable directory for database files; each test creates one named after a fresh UUID. Unset, the suite fails with a message naming the variable |
| `LD_LIBRARY_PATH` | Must include the directory of `libe_sqlite3.so` (see [SQLite storage](#sqlite-storage)) |

```sh
eval "$(scripts/ci/start-hangfire-sqlite.sh)"
lyric test --manifest jobs-hangfire/lyric.toml --target dotnet --features integration
```

`scripts/ci/start-hangfire-sqlite.sh` starts no server: it copies the native library for the runner's
architecture out of the NuGet cache, creates a temporary database directory, and exports the two
variables (through `$GITHUB_ENV` under GitHub Actions).

`lyric build --manifest jobs-hangfire/lyric.toml --target jvm` fails with `F0054`, as intended.

## Pinned versions

| | |
|---|---|
| Hangfire | NuGet `Hangfire.Core` 1.8.25 |
| In-memory storage | NuGet `Hangfire.InMemory` 1.0.0 |
| SQLite storage | NuGet `Hangfire.Storage.SQLite` 0.4.3 |
| Native SQLite | NuGet `SQLitePCLRaw.lib.e_sqlite3` 2.1.14, pinned above the 2.1.11 that `Hangfire.Storage.SQLite` asks for, which NuGet reports as vulnerable (GHSA-2m69-gcr7-jv3q) |
| Resolved transitively | `Newtonsoft.Json` 13.0.4, `SQLitePCLRaw.bundle_green`, `.core` and `.provider.e_sqlite3` 2.1.11, `sqlite-net-pcl` 1.9.172 |
| .NET | `net10.0` |
| Interface and conformance suite | `Lyric.Jobs` and `Lyric.Testing` at the `lyric-lang` commit in `.lyric-lang-rev`, through the root `[workspace.overrides]` |

## Known gaps

- Storages: in memory and SQLite only. Hangfire's SQL Server, PostgreSQL and Redis storages are not
  exposed.
- The SQLite storage needs its native library put on the search path by hand, because the toolchain
  does not deploy native NuGet assets (below); it has been run on Linux x64 only.
- A running handler cannot be stopped on timeout or cancel (see Surprises); `Jobs.JobHandler` would
  need a cancellation token for that.
- Recurring jobs are evaluated in UTC; there is no time zone setting. Hangfire's misfire handling is
  left at its default.
- One queue per scheduler; a server cannot listen on several queues.
- The two sorted sets of a recurring job (a marker and at most `maxResultsPerJob` firing ids) stay in
  the storage after the job is cancelled; nothing removes them.
- The recurring job's queue is set through `RecurringJobOptions.QueueName`, which Hangfire 1.8 marks
  obsolete and plans to remove in 2.0. It is the one way that works on every storage; the replacement
  needs the storage feature `Job.Queue`.
- Hangfire's dashboard is not part of this package (it is an ASP.NET Core component).
- Compiler and toolchain issues met while writing this package (none is worked around with a shim; the
  `Option` case is nichobbs/lyric-lang#9068, the `List` case nichobbs/lyric-lang#9094, the rest
  nichobbs/lyric-lang#9095):
  - `impl Interface for T` where `T` is a `pub opaque type` does not produce a working implementation,
    so `HangfireJobScheduler` is a `pub record` whose internals sit in the opaque `HangfireState`
    (lyric-lang#9038).
  - MSIL: a `match` on an `Option` of an extern type of another package of the project, named through
    an import alias, builds and then panics at run time with `match not exhaustive`.
  - MSIL: a `List` of such an extern type, as a field of a protected type, builds and then fails at run
    time with an `InvalidCastException` (`List<BackgroundJobServer>` to `List<Object>`). The kernel
    therefore hands the server out wrapped in a Lyric record (`ServerHandle`).
  - MSIL auto-FFI: members of a generic BCL collection that a Hangfire call returns
    (`Dictionary<string,string>`, `HashSet<string>`, `List<string>`) cannot be called, and such a
    value cannot be passed on (`String.Join`). The backend therefore uses only Hangfire storage calls
    that take and return strings, and keeps what it needs in job parameters and sorted sets.
  - MSIL auto-FFI: a member an interface inherits from another interface is not found
    (`IStorageConnection.Dispose`, from `IDisposable`); the kernel binds it with `@externTarget`.
  - A value of an extern class is not accepted where the host parameter is an interface the class
    implements (`BackgroundJobClient` as `IBackgroundJobClient`), unless it first goes through a `val`
    annotated with the interface.
  - `lyric build`/`run`/`test` do not copy the native runtime assets of a NuGet package
    (`runtimes/<rid>/native/`), and `lyric restore` accepts such a package without the `B0031` that
    `docs/21-nuget-linking.md` describes. This is why the SQLite storage needs `LD_LIBRARY_PATH`.
  - `{ workspace = true }` dependencies resolve for `lyric test` only when it is run from the workspace
    root (lyric-lang#9040).
