//! Jobs.Hangfire.Kernel.Dotnet — .NET extern boundary for the Hangfire
//! backend.
//!
//! Every binding is an `extern type` plus auto-FFI against the Hangfire
//! assemblies restored from NuGet; there is no host shim.  The kernel owns:
//!
//!   - the job storages (`Hangfire.InMemory`, `Hangfire.PostgreSql`),
//!   - job creation.  Hangfire normally takes a C# expression tree; here a
//!     job is built from `Hangfire.Storage.InvocationData`, the same four
//!     strings Hangfire stores for every job (type, method, parameter types,
//!     arguments), naming the static method `dispatch` below,
//!   - `dispatch`, the method a Hangfire worker invokes for every job, and
//!     `Binder`, a Lyric record implementing `Hangfire.Server.IServerFilter`
//!     that tells `dispatch` which Lyric closure belongs to the server whose
//!     worker thread is calling it,
//!   - `Elector`, a Lyric record implementing
//!     `Hangfire.States.IElectStateFilter`: it turns a failed attempt into a
//!     retry when `dispatch` asked for one, and puts a job that Hangfire
//!     enqueues by itself on the queue the job was submitted to, which
//!     `Stamper` (`Hangfire.Client.IClientFilter`) recorded in the job,
//!   - the `BackgroundJobServer` lifecycle,
//!   - the storage reads and writes the Lyric side needs (job state, job
//!     parameters, sorted sets, a distributed lock, monitoring counts).
//!
//! No Hangfire or BCL exception leaves a `pub func` of this file, except
//! from `dispatch`, whose exception is how Hangfire learns that an attempt
//! failed.
@io
package Jobs.Hangfire.Kernel.Dotnet

import Std.Core
import Std.Collections

extern type NEnvironment = "System.Environment"

extern type NType = "System.Type"

extern type NTimeSpan = "System.TimeSpan"

extern type NDateTimeOffset = "System.DateTimeOffset"

extern type NDisposable = "System.IDisposable"

extern type NJson = "Newtonsoft.Json.JsonConvert"

pub extern type NStorage = "Hangfire.JobStorage"

extern type NInMemory = "Hangfire.InMemory.InMemoryStorage"

extern type NConnection = "Hangfire.Storage.IStorageConnection"

extern type NTransaction = "Hangfire.Storage.IWriteOnlyTransaction"

extern type NJobData = "Hangfire.Storage.JobData"

extern type NMonitor = "Hangfire.Storage.IMonitoringApi"

extern type NInvocation = "Hangfire.Storage.InvocationData"

extern type NJob = "Hangfire.Common.Job"

extern type NClient = "Hangfire.BackgroundJobClient"

extern type NClientApi = "Hangfire.IBackgroundJobClient"

extern type NRecurring = "Hangfire.RecurringJobManager"

extern type NRecurringOptions = "Hangfire.RecurringJobOptions"

extern type NServer = "Hangfire.BackgroundJobServer"

extern type NServerOptions = "Hangfire.BackgroundJobServerOptions"

extern type NState = "Hangfire.States.IState"

extern type NEnqueued = "Hangfire.States.EnqueuedState"

extern type NScheduled = "Hangfire.States.ScheduledState"

extern type NAwaiting = "Hangfire.States.AwaitingState"

extern type NDeleted = "Hangfire.States.DeletedState"

extern type NElectFilter = "Hangfire.States.IElectStateFilter"

extern type NElectContext = "Hangfire.States.ElectStateContext"

extern type NClientFilter = "Hangfire.Client.IClientFilter"

extern type NCreatingContext = "Hangfire.Client.CreatingContext"

extern type NCreatedContext = "Hangfire.Client.CreatedContext"

extern type NFilterProvider = "Hangfire.Common.IJobFilterProvider"

extern type NServerFilter = "Hangfire.Server.IServerFilter"

extern type NPerformContext = "Hangfire.Server.PerformContext"

extern type NPerformingContext = "Hangfire.Server.PerformingContext"

extern type NPerformedContext = "Hangfire.Server.PerformedContext"

extern type NFilters = "Hangfire.Common.JobFilterCollection"

extern type NCaptureCulture = "Hangfire.CaptureCultureAttribute"

extern type NStatistics = "Hangfire.StatisticsHistoryAttribute"

extern type NContinuations = "Hangfire.ContinuationsSupportAttribute"

/// Hangfire's state names, as `jobState` returns them.
pub val stateEnqueued: String = "Enqueued"

pub val stateScheduled: String = "Scheduled"

pub val stateAwaiting: String = "Awaiting"

pub val stateProcessing: String = "Processing"

pub val stateSucceeded: String = "Succeeded"

pub val stateFailed: String = "Failed"

pub val stateDeleted: String = "Deleted"

// Job parameter through which `dispatch`'s caller tells `Elector` whether a
// failed attempt is to be repeated ("1") or is final.
val retryParameter: String = "lyric.retry"

// Job parameter holding the queue a one-off job belongs to, as JSON (a
// quoted string): Hangfire serialises a parameter set while a job is being
// created.
val queueParameter: String = "lyric.queue"

// Hangfire keeps the due time of a `Scheduled` job in whole seconds, rounded
// down, so a delay of `delayMs` would be due up to a second early.  One
// second more makes it due between `delayMs` and `delayMs` + 1 s from now.
func dueIn(delayMs: in Long): NTimeSpan {
  NTimeSpan.FromMilliseconds((delayMs + 1000i64).toDouble())
}

func quoted(queue: in String): String {
  "\"" + queue + "\""
}

func describe(typeName: in String, message: in String): String {
  if message.length == 0 {
    return typeName
  }
  typeName + ": " + message
}

// ─── Storage connections ──────────────────────────────────────────────────────
// `IStorageConnection` and `IWriteOnlyTransaction` inherit `Dispose` from
// `IDisposable`; auto-FFI does not look at inherited interface members.
@externInstance
@externTarget("System.IDisposable.Dispose")
func closeConnection(c: in NConnection): Unit = ()

@externInstance
@externTarget("System.IDisposable.Dispose")
func closeTransaction(t: in NTransaction): Unit = ()

// The three reads below answer null for "not there".
@externInstance
@externTarget("Hangfire.Storage.IStorageConnection.GetJobData")
func jobDataOf(c: in NConnection, jobId: in String): Option[NJobData] = ()

// Null for a job that has been created and has no state yet.
@externInstance
@externTarget("Hangfire.Storage.JobData.get_State")
func currentStateOf(d: in NJobData): Option[String] = ()

@externInstance
@externTarget("Hangfire.Storage.IStorageConnection.GetJobParameter")
func parameterOf(c: in NConnection, jobId: in String, name: in String): Option[String] = ()

@externInstance
@externTarget("Hangfire.Storage.IStorageConnection.GetFirstByLowestScoreFromSet")
func lowestInSet(c: in NConnection, key: in String, fromScore: in Double, toScore: in Double): Option[String] = ()

// ─── Storages ─────────────────────────────────────────────────────────────────
/// A new, empty in-memory storage.  Its jobs live as long as the process.
pub func inMemoryStorage(): Result[NStorage, String] {
  try {
    val s: NStorage = NInMemory.new()
    Ok(s)
  } catch Bug as b {
    Err(describe(b.typeName, b.message))
  }
}

extern type NPostgres = "Hangfire.PostgreSql.PostgreSqlStorage"

extern type NPostgresOptions = "Hangfire.PostgreSql.PostgreSqlStorageOptions"

extern type NPostgresFactory = "Hangfire.PostgreSql.Factories.NpgsqlConnectionFactory"

extern type NConnectionFactory = "Hangfire.PostgreSql.IConnectionFactory"

/// A storage in the PostgreSQL database `connectionString` (an Npgsql
/// connection string) names, in the schema `schemaName`.  With
/// `prepareSchema` the schema and its tables are created or upgraded now;
/// without, they must exist.  The database is read once before returning.
/// Workers look for new jobs every `queuePollIntervalMs`.  The `Err` text
/// is the driver's message and is not screened for secrets here; the caller
/// does that.
pub func postgresStorage(
  connectionString: in String,
  schemaName: in String,
  queuePollIntervalMs: in Int,
  prepareSchema: in Bool,
): Result[NStorage, String]
  requires: connectionString.length > 0
  requires: schemaName.length > 0
  requires: queuePollIntervalMs >= 50
{
  try {
    val options = NPostgresOptions.new()
    options.SchemaName = schemaName
    options.QueuePollInterval = NTimeSpan.FromMilliseconds(queuePollIntervalMs.toDouble())
    options.PrepareSchemaIfNecessary = prepareSchema
    // The storage's default is to retry a failed start five times with a
    // growing delay and then to carry on without a database; a caller here
    // is told at once instead.
    options.StartupConnectionMaxRetries = 0
    val factory: NConnectionFactory = NPostgresFactory.new(connectionString, options)
    val s: NStorage = NPostgres.new(factory, options)
    // One read, so that a database that cannot be reached, or a schema that
    // is not there, is an `Err` now and not at the first job.
    val c = s.GetConnection()
    val _ = jobDataOf(c, "0")
    closeConnection(c)
    Ok(s)
  } catch Bug as b {
    Err(describe(b.typeName, b.message))
  }
}

/// How long `storage` keeps a job after it has finished (`Succeeded` or
/// `Deleted`); Hangfire removes it then.
pub func setJobExpiration(storage: in NStorage, ms: in Long): Result[Unit, String]
  requires: ms >= 0i64
{
  try {
    storage.JobExpirationTimeout = NTimeSpan.FromMilliseconds(ms.toDouble())
    Ok(())
  } catch Bug as b {
    Err(describe(b.typeName, b.message))
  }
}

// ─── Dispatch ─────────────────────────────────────────────────────────────────
/// One invocation of a job by a Hangfire worker.
pub record DispatchCall {
  /// Hangfire's id of the background job being run.
  jobId: String
  name: String
  payload: String
  maxAttempts: Int
  timeoutMs: Int
  /// Id of the recurring job this is a firing of; "" for a one-off job.
  recurringId: String
  /// When Hangfire created the background job, epoch milliseconds.
  createdAtMs: Long
}

/// What the attempt came to.  `ok` makes the Hangfire job `Succeeded` with
/// `text` as its result; otherwise `text` is the failure message.
pub record DispatchOutcome {
  ok: Bool
  text: String
}

// Which Lyric closure runs the job a worker thread is about to invoke.
// Hangfire gives a static job method no way to reach the server that calls
// it, so `Binder.OnPerforming`, called on the worker thread just before the
// method, leaves the closure here under the thread's id.
protected type Bindings {
  var byThread: Map[Int, (DispatchCall) -> DispatchOutcome] = newMap()
  entry attach(thread: in Int, run: in (DispatchCall) -> DispatchOutcome) {
    byThread[thread] = run
  }
  entry detach(thread: in Int) {
    val _ = byThread.remove(thread)
  }
  entry find(thread: in Int): Option[(DispatchCall) -> DispatchOutcome] {
    if byThread.containsKey(thread) {
      return Some(value = byThread[thread])
    }
    return None
  }
}

val bindings: Bindings = Bindings()

record Binder {
  run: (DispatchCall) -> DispatchOutcome
}

impl NServerFilter for Binder {
  func OnPerforming(ctx: in NPerformingContext): Unit {
    bindings.attach(NEnvironment.CurrentManagedThreadId, self.run)
  }

  func OnPerformed(ctx: in NPerformedContext): Unit {
    bindings.detach(NEnvironment.CurrentManagedThreadId)
  }
}

/// The method every job of this backend names.  A Hangfire worker calls it
/// with the arguments stored in the job and its own `PerformContext`.  A
/// normal return makes the job `Succeeded` with the returned text as its
/// result; a panic is the exception Hangfire records as the failure.
pub func dispatch(
  name: in String,
  payload: in String,
  maxAttempts: in Int,
  timeoutMs: in Int,
  recurringId: in String,
  ctx: in NPerformContext,
): String {
  val job = ctx.BackgroundJob
  match bindings.find(NEnvironment.CurrentManagedThreadId) {
    case None -> panic(
      "job " + job.Id +
        " was picked up by a Hangfire server that Lyric.Jobs.Hangfire did not start; it has no handlers"
    )
    case Some(run) -> {
      val outcome = run(
        DispatchCall(
          jobId = job.Id,
          name = name,
          payload = payload,
          maxAttempts = maxAttempts,
          timeoutMs = timeoutMs,
          recurringId = recurringId,
          createdAtMs = NDateTimeOffset.new(job.CreatedAt).ToUnixTimeMilliseconds()
        )
      )
      if not outcome.ok {
        panic(outcome.text)
      }
      outcome.text
    }
  }
}

record Anchor {
  tag: Int
}

@externInstance
@externTarget("System.Object.GetType")
func typeOfAnchor(a: in Anchor): NType = ()

// Top-level functions of this package are static methods of the class
// `Jobs.Hangfire.Kernel.Dotnet.Program`, in whatever assembly the package
// was linked into; `Anchor` is a type of that same assembly.
func dispatchTypeName(): String {
  typeOfAnchor(Anchor(tag = 0)).Assembly.GetType("Jobs.Hangfire.Kernel.Dotnet.Program").AssemblyQualifiedName
}

// The job as Hangfire stores it.  Each argument is JSON, and the argument
// list is a JSON array of those texts; the last argument is null because
// Hangfire fills a `PerformContext` parameter in itself.
func dispatchJob(
  name: in String, payload: in String, maxAttempts: in Int, timeoutMs: in Int, recurringId: in String
): NJob {
  val arguments = "[" + NJson.ToString(NJson.ToString(name)) + "," + NJson.ToString(NJson.ToString(payload)) + ",\"" +
    maxAttempts.toString() +
    "\",\"" +
    timeoutMs.toString() +
    "\"," +
    NJson.ToString(NJson.ToString(recurringId)) +
    ",null]"
  NInvocation.new(dispatchTypeName(), "dispatch", "[\"System.String\",\"System.String\",\"System.Int32\",\"System.Int32\",\"System.String\",\"Hangfire.Server.PerformContext, Hangfire.Core\"]", arguments).DeserializeJob()
}

// ─── Retries and queues ───────────────────────────────────────────────────────
// A one-off job is stamped with its queue while it is created, so that it
// can be put back there whichever server handles it later.
record Stamper {
  queue: String
}

impl NClientFilter for Stamper {
  func OnCreating(ctx: in NCreatingContext): Unit {
    ctx.SetJobParameter(queueParameter, self.queue)
  }

  func OnCreated(ctx: in NCreatedContext): Unit {
    ()
  }
}

// The filters of a client: Hangfire's defaults that act on job creation,
// and the stamp.
func clientFilters(queue: in String): NFilterProvider {
  val filters = NFilters.new()
  filters.Add(NCaptureCulture.new())
  filters.Add(NContinuations.new())
  val stamper: NClientFilter = Stamper(queue = queue)
  filters.Add(stamper)
  filters
}

// Hangfire asks every `IElectStateFilter` of the server that handles a job
// before the job changes state.
//
// Failed: Hangfire's own `AutomaticRetry` filter is not installed.  The
// Lyric side counts attempts and leaves "1" in the job's `lyric.retry`
// parameter when another attempt is due; the job then goes back to the queue
// it was fetched from (or to `Scheduled`, when a delay is configured)
// instead of to `Failed`.  The parameter is cleared here, so a failure that
// never reached `dispatch` (a job Hangfire cannot load) is not retried on a
// stale request.
//
// Enqueued: a delayed job that has come due is enqueued by whichever server
// finds it, on the queue `default`.  A job that carries a queue stamp is put
// on that queue instead.  A firing of a recurring job has no stamp and
// arrives with the queue of its recurring job.
record Elector {
  queue: String
  retryDelayMs: Int
}

impl NElectFilter for Elector {
  func OnStateElection(ctx: in NElectContext): Unit {
    val candidate = ctx.CandidateState.Name
    val jobId = ctx.BackgroundJob.Id
    if candidate == stateEnqueued {
      match parameterOf(ctx.Connection, jobId, queueParameter) {
        case None -> ()
        case Some(stamp) -> {
          if stamp.length > 2 {
            val pinned: NState = NEnqueued.new(stamp.substring(1, stamp.length - 2))
            ctx.CandidateState = pinned
          }
        }
      }
    } else if candidate == stateFailed {
      val wanted = match parameterOf(ctx.Connection, jobId, retryParameter) { case Some(v) -> v == "1"; case None -> false }
      if wanted {
        ctx.Connection.SetJobParameter(jobId, retryParameter, "0")
        ctx.Connection.SetJobParameter(jobId, queueParameter, quoted(self.queue))
        if self.retryDelayMs > 0 {
          val later: NState = NScheduled.new(dueIn(self.retryDelayMs.toLong()))
          ctx.CandidateState = later
        } else {
          val again: NState = NEnqueued.new(self.queue)
          ctx.CandidateState = again
        }
      }
    }
  }
}

// ─── Server ───────────────────────────────────────────────────────────────────
/// A running Hangfire server.  A Lyric record, not the extern type itself:
/// other packages keep it in a `List`, and a generic over an extern type of
/// another package is miscompiled (see the README, Known gaps).
pub record ServerHandle {
  server: NServer
}

/// Start a Hangfire server on `storage`: `workerCount` worker threads
/// fetching from `queue`, a scheduler that looks for due delayed jobs every
/// `schedulePollingIntervalMs`, and the `Elector` and `Binder` filters.  `run` is called
/// on a worker thread for every job.
pub func startServer(
  storage: in NStorage,
  queue: in String,
  workerCount: in Int,
  schedulePollingIntervalMs: in Int,
  retryDelayMs: in Int,
  shutdownTimeoutMs: in Int,
  run: in (DispatchCall) -> DispatchOutcome,
): Result[ServerHandle, String]
  requires: workerCount >= 1
  requires: schedulePollingIntervalMs >= 1
  requires: retryDelayMs >= 0
  requires: shutdownTimeoutMs >= 0
{
  try {
    val options = NServerOptions.new()
    options.WorkerCount = workerCount
    options.Queues = [queue]
    options.SchedulePollingInterval = NTimeSpan.FromMilliseconds(schedulePollingIntervalMs.toDouble())
    options.ShutdownTimeout = NTimeSpan.FromMilliseconds(shutdownTimeoutMs.toDouble())
    // Hangfire's default filters, without `AutomaticRetry`.
    val filters = NFilters.new()
    filters.Add(NCaptureCulture.new())
    filters.Add(NStatistics.new())
    filters.Add(NContinuations.new())
    val elector: NElectFilter = Elector(queue = queue, retryDelayMs = retryDelayMs)
    filters.Add(elector)
    val binder: NServerFilter = Binder(run = run)
    filters.Add(binder)
    options.FilterProvider = filters
    Ok(ServerHandle(server = NServer.new(options, storage)))
  } catch Bug as b {
    Err(describe(b.typeName, b.message))
  }
}

/// Stop `server` and release it.  Workers stop fetching at once; the call
/// then waits up to `waitMs` for the jobs that are running.  True when every
/// worker had ended by then.
pub func stopServer(handle: in ServerHandle, waitMs: in Int): Result[Bool, String]
  requires: waitMs >= 0
{
  try {
    val server = handle.server
    server.SendStop()
    // `Dispose` waits without limit unless `WaitForShutdown` was called.
    val ended = server.WaitForShutdown(NTimeSpan.FromMilliseconds(waitMs.toDouble()))
    server.Dispose()
    Ok(ended)
  } catch Bug as b {
    Err(describe(b.typeName, b.message))
  }
}

// ─── Jobs ─────────────────────────────────────────────────────────────────────
/// Create a background job on `queue` and return Hangfire's id for it.
/// `delayMs > 0` creates it `Scheduled`, to be enqueued after the delay; a
/// non-empty `parentId` creates it `Awaiting`, to be enqueued when job
/// `parentId` has succeeded; otherwise it is `Enqueued` at once.
pub func createJob(
  storage: in NStorage,
  queue: in String,
  name: in String,
  payload: in String,
  maxAttempts: in Int,
  timeoutMs: in Int,
  delayMs: in Long,
  parentId: in String,
): Result[String, String]
  requires: delayMs >= 0i64
{
  try {
    val job = dispatchJob(name, payload, maxAttempts, timeoutMs, "")
    val enqueued: NState = NEnqueued.new(queue)
    val client = NClient.new(storage, clientFilters(queue))
    if parentId.length > 0 {
      val awaiting: NState = NAwaiting.new(parentId, enqueued)
      Ok(client.Create(job, awaiting))
    } else if delayMs > 0i64 {
      val scheduled: NState = NScheduled.new(dueIn(delayMs))
      Ok(client.Create(job, scheduled))
    } else {
      Ok(client.Create(job, enqueued))
    }
  } catch Bug as b {
    Err(describe(b.typeName, b.message))
  }
}

/// Move job `jobId` to `Deleted`, provided it is still in `expectedState`.
/// False when it was not (or does not exist).
pub func deleteJob(storage: in NStorage, jobId: in String, expectedState: in String): Result[Bool, String] {
  try {
    val client: NClientApi = NClient.new(storage)
    val deleted: NState = NDeleted.new()
    Ok(client.ChangeState(jobId, deleted, expectedState))
  } catch Bug as b {
    Err(describe(b.typeName, b.message))
  }
}

/// The name of the state job `jobId` is in; `None` for a job the storage
/// does not have, "" for a job that is being created and has no state yet.
///
/// This is the state in the job's own record (`GetJobData`), not what
/// `GetStateData` answers.  When a filter replaces the state Hangfire was
/// about to apply, as `Elector` does to retry a failed attempt, Hangfire
/// still writes the replaced state to the job's state history, and a storage
/// may answer `GetStateData` from the history (Hangfire.Storage.SQLite does),
/// so that a job being retried reads as `Failed` for a moment there.
pub func jobState(storage: in NStorage, jobId: in String): Result[Option[String], String] {
  try {
    val c = storage.GetConnection()
    val found = jobDataOf(c, jobId)
    closeConnection(c)
    match found {
      case None -> Ok(None)
      case Some(data) -> {
        match currentStateOf(data) {
          case Some(name) -> Ok(Some(value = name))
          case None -> Ok(Some(value = ""))
        }
      }
    }
  } catch Bug as b {
    Err(describe(b.typeName, b.message))
  }
}

pub func jobParameter(storage: in NStorage, jobId: in String, name: in String): Result[Option[String], String] {
  try {
    val c = storage.GetConnection()
    val found = parameterOf(c, jobId, name)
    closeConnection(c)
    Ok(found)
  } catch Bug as b {
    Err(describe(b.typeName, b.message))
  }
}

pub func setJobParameter(
  storage: in NStorage, jobId: in String, name: in String, value: in String
): Result[Unit, String] {
  try {
    val c = storage.GetConnection()
    c.SetJobParameter(jobId, name, value)
    closeConnection(c)
    Ok(())
  } catch Bug as b {
    Err(describe(b.typeName, b.message))
  }
}

/// Ask for (or decline) another attempt of job `jobId` after the failure
/// `dispatch` is about to report.
pub func requestRetry(storage: in NStorage, jobId: in String, wanted: in Bool): Result[Unit, String] {
  setJobParameter(storage, jobId, retryParameter, if wanted { "1" } else { "0" })
}

// ─── Recurring jobs ───────────────────────────────────────────────────────────
/// Create or replace recurring job `recurringId`: every time `cron` (a
/// Hangfire cron expression, evaluated in UTC) is due, a server creates a
/// background job for it on `queue`.
pub func addRecurring(
  storage: in NStorage,
  queue: in String,
  recurringId: in String,
  cron: in String,
  name: in String,
  payload: in String,
  maxAttempts: in Int,
  timeoutMs: in Int,
): Result[Unit, String]
  requires: recurringId.length > 0
{
  try {
    val job = dispatchJob(name, payload, maxAttempts, timeoutMs, recurringId)
    val options = NRecurringOptions.new()
    // The one way to give a recurring job a queue that every storage
    // supports.  Hangfire marks it obsolete in favour of a queue on the job
    // itself, which needs the storage feature `Job.Queue`.
    options.QueueName = queue
    NRecurring.new(storage).AddOrUpdate(recurringId, job, cron, options)
    Ok(())
  } catch Bug as b {
    Err(describe(b.typeName, b.message))
  }
}

pub func removeRecurring(storage: in NStorage, recurringId: in String): Result[Unit, String] {
  try {
    NRecurring.new(storage).RemoveIfExists(recurringId)
    Ok(())
  } catch Bug as b {
    Err(describe(b.typeName, b.message))
  }
}

@externInstance
@externTarget("Hangfire.RecurringJobManager.TriggerJob")
func triggerJob(m: in NRecurring, recurringId: in String): Option[String] = ()

/// Fire recurring job `recurringId` now.  The id of the background job
/// created, or `None` when Hangfire has no such recurring job.
pub func triggerRecurring(storage: in NStorage, recurringId: in String): Result[Option[String], String] {
  try {
    Ok(triggerJob(NRecurring.new(storage), recurringId))
  } catch Bug as b {
    Err(describe(b.typeName, b.message))
  }
}

// ─── Sorted sets ──────────────────────────────────────────────────────────────
/// Add `value` to the sorted set `key` with `score` (or change its score).
pub func setAdd(storage: in NStorage, key: in String, value: in String, score: in Double): Result[Unit, String] {
  try {
    val c = storage.GetConnection()
    val t = c.CreateWriteTransaction()
    t.AddToSet(key, value, score)
    t.Commit()
    closeTransaction(t)
    closeConnection(c)
    Ok(())
  } catch Bug as b {
    Err(describe(b.typeName, b.message))
  }
}

pub func setRemove(storage: in NStorage, key: in String, value: in String): Result[Unit, String] {
  try {
    val c = storage.GetConnection()
    val t = c.CreateWriteTransaction()
    t.RemoveFromSet(key, value)
    t.Commit()
    closeTransaction(t)
    closeConnection(c)
    Ok(())
  } catch Bug as b {
    Err(describe(b.typeName, b.message))
  }
}

/// The value with the lowest score between `fromScore` and `toScore`
/// (both inclusive) in the sorted set `key`.
pub func setLowest(
  storage: in NStorage, key: in String, fromScore: in Double, toScore: in Double
): Result[Option[String], String] {
  try {
    val c = storage.GetConnection()
    val found = lowestInSet(c, key, fromScore, toScore)
    closeConnection(c)
    Ok(found)
  } catch Bug as b {
    Err(describe(b.typeName, b.message))
  }
}

/// Run `body` while holding Hangfire's distributed lock `resource`, waiting
/// up to `timeoutMs` for it.  The `Err` of `body`, or of the lock, is
/// returned.
pub func locked(
  storage: in NStorage, resource: in String, timeoutMs: in Int, body: in () -> Result[Unit, String]
): Result[Unit, String]
  requires: timeoutMs >= 0
{
  try {
    val c = storage.GetConnection()
    val held: NDisposable = c.AcquireDistributedLock(resource, NTimeSpan.FromMilliseconds(timeoutMs.toDouble()))
    val outcome = try {
      body()
    } catch Bug as inner {
      Err(describe(inner.typeName, inner.message))
    }
    held.Dispose()
    closeConnection(c)
    outcome
  } catch Bug as b {
    Err(describe(b.typeName, b.message))
  }
}

// ─── Monitoring ───────────────────────────────────────────────────────────────
/// Hangfire's own job counts for a storage.
pub record StorageCounts {
  /// Jobs waiting on the queue asked about.
  enqueued: Long
  scheduled: Long
  processing: Long
  succeeded: Long
  failed: Long
  deleted: Long
}

pub func storageCounts(storage: in NStorage, queue: in String): Result[StorageCounts, String] {
  try {
    val m = storage.GetMonitoringApi()
    Ok(
      StorageCounts(
        enqueued = m.EnqueuedCount(queue),
        scheduled = m.ScheduledCount(),
        processing = m.ProcessingCount(),
        succeeded = m.SucceededListCount(),
        failed = m.FailedCount(),
        deleted = m.DeletedListCount()
      )
    )
  } catch Bug as b {
    Err(describe(b.typeName, b.message))
  }
}

// ─── Time ─────────────────────────────────────────────────────────────────────
/// Milliseconds on a clock that never goes backwards (not the time of day).
pub func monotonicMs(): Long {
  NEnvironment.TickCount64
}

/// The managed id of the calling thread.
pub func currentThreadId(): Int {
  NEnvironment.CurrentManagedThreadId
}
