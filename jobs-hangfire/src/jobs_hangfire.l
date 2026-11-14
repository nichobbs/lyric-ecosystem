//! Jobs.Hangfire — a `Jobs.JobScheduler` backed by Hangfire (.NET).
//!
//! Jobs are Hangfire background jobs in a Hangfire storage, picked up by the
//! workers of a Hangfire `BackgroundJobServer`; each attempt runs the
//! `Jobs.JobHandler` registered for the job's name (or the default handler).
//!
//!   import Jobs.Hangfire as Hangfire
//!
//!   val storage = Hangfire.inMemoryStorage()?
//!   val sched = Hangfire.create(Hangfire.defaultConfig(), storage)?
//!   Hangfire.registerHandler(sched, "send-email", SendEmail())
//!   Hangfire.start(sched)?
//!   val id = sched.enqueue("send-email", "{\"to\":\"a@example.com\"}", 3, 30000)?
//!   ...
//!   Hangfire.shutdown(sched, true)?
//!
//! A scheduler is a Hangfire client from `create` on: it submits, cancels
//! and reports jobs, which stay `Pending`.  `start` adds a server, whose
//! workers run them; `shutdown` stops the server and leaves the client.  A
//! process that only submits never calls `start`.
//!
//! Statuses and results are read from the storage every time (see
//! `Jobs.Hangfire.Store`), not kept in the scheduler.
//!
//! An attempt runs on a Hangfire worker thread, and .NET cannot interrupt a
//! thread, so a handler always runs to its own end.  When it returns after
//! more than `timeoutMs`, or after the job was cancelled, what it returned
//! is discarded and the attempt is recorded as failed.  A handler that wants
//! to stop early asks `stopRequested`.  After a failed attempt the job goes
//! back to Hangfire (to its queue, or to `Scheduled` for
//! `HangfireConfig.retryDelayMs`) until `maxAttempts` attempts have run.
//!
//! Hangfire-only features (delays, continuations, native cron, counts) are
//! in `Jobs.Hangfire.Native`.
@runtime_checked
@io
package Jobs.Hangfire

import Std.Core
import Std.Collections
import Std.String as Str
pub use Jobs
import Jobs.Hangfire.Store
import Jobs.Hangfire.Kernel.Dotnet as Kernel

// ─── Storage ──────────────────────────────────────────────────────────────────
/// A Hangfire job storage.  Build one with `inMemoryStorage` or
/// `postgresStorage`; several schedulers, in one process or in several, may
/// share one.
pub record HangfireStorage {
  native: Kernel.NStorage
  /// "in-memory" or "postgresql".
  kind: String
}

/// A new, empty storage in process memory (`Hangfire.InMemory`).  Jobs are
/// lost when the process exits.
pub func inMemoryStorage(): Result[HangfireStorage, String] {
  match Kernel.inMemoryStorage() {
    case Ok(native) -> Ok(HangfireStorage(native = native, kind = "in-memory"))
    case Err(e) -> Err("hangfire: cannot create the in-memory storage: " + e)
  }
}

/// Settings of a PostgreSQL storage.
pub record PostgresOptions {
  /// The schema that holds Hangfire's tables: a lowercase letter or `_`,
  /// then lowercase letters, digits and `_`.  Applications that use
  /// different schemas can share a database without seeing each other's
  /// jobs.
  schemaName: String
  /// How often an idle worker looks for a new job.
  queuePollIntervalMs: Int
  /// Create the schema and its tables when they are missing, and upgrade
  /// them when they are older than the storage expects.  Without it the
  /// schema must exist already.
  prepareSchema: Bool
  invariant: queuePollIntervalMs >= 50
}

/// Schema `hangfire`, a look at the queue every second, and the schema
/// created when missing.
pub func defaultPostgresOptions(): PostgresOptions {
  PostgresOptions(schemaName = "hangfire", queuePollIntervalMs = 1000, prepareSchema = true)
}

func validSchemaName(name: in String): Bool {
  if name.length == 0 or name.length > 63 {
    return false
  }
  var i = 0
  while i < name.length {
    val c = Str.codeUnitAt(name, i)
    val lower = c >= 97 and c <= 122
    val digit = c >= 48 and c <= 57
    if not lower and c != 95 and not (digit and i > 0) {
      return false
    }
    i = i + 1
  }
  true
}

// `message` without anything that came from `connectionString`: the string
// itself, and the value of every `key=value` part of it.  A driver message
// may quote either, and the string holds the password.
func withoutSecrets(message: in String, connectionString: in String): String {
  var clean = Str.replace(message, connectionString, "<connection string>")
  for part in Str.split(connectionString, ";") {
    match Str.indexOf(part, "=") {
      case None -> ()
      case Some(at) -> {
        val key = Str.toLower(Str.trim(Str.substring(part, 0, at)))
        val value = Str.trim(Str.substring(part, at + 1))
        if (key == "password" or key == "pwd") and value.length > 0 {
          clean = Str.replace(clean, value, "<password>")
        }
      }
    }
  }
  clean
}

/// A storage in a PostgreSQL database (`Hangfire.PostgreSql`).  Jobs survive
/// the process and are shared by every process that opens the same database
/// and schema.  `connectionString` is an Npgsql connection string
/// (`Host=...;Port=...;Database=...;Username=...;Password=...`); it is never
/// part of an `Err`.  The database is contacted at once: `Err` when it
/// cannot be reached, the login is refused, or the schema is missing and
/// `options.prepareSchema` is false.
pub func postgresStorage(connectionString: in String, options: in PostgresOptions): Result[HangfireStorage, String]
  requires: connectionString.length > 0
{
  if not validSchemaName(options.schemaName) {
    return Err(
      "hangfire: invalid schema name '" + options.schemaName +
        "': use a lowercase letter or '_', then lowercase letters, digits and '_'"
    )
  }
  match Kernel.postgresStorage(connectionString, options.schemaName, options.queuePollIntervalMs, options.prepareSchema) {
    case Ok(native) -> Ok(HangfireStorage(native = native, kind = "postgresql"))
    case Err(e) -> Err("hangfire: cannot open the PostgreSQL storage: " + withoutSecrets(e, connectionString))
  }
}

/// Set how long `storage` keeps a job after it has succeeded or been
/// cancelled; Hangfire's default is one day.  Applies to jobs that finish
/// from now on, through any scheduler of this process on `storage`.
pub func keepFinishedJobsFor(storage: in HangfireStorage, ms: in Long): Result[Unit, String]
  requires: ms >= 0i64
{
  match Kernel.setJobExpiration(storage.native, ms) {
    case Ok(_) -> Ok(())
    case Err(e) -> Err("hangfire: cannot set the job retention: " + e)
  }
}

// ─── Configuration ────────────────────────────────────────────────────────────
/// Settings for one scheduler.
pub record HangfireConfig {
  /// The Hangfire queue this scheduler submits to and its server fetches
  /// from: lowercase letters, digits, `_` and `-`.
  queue: String
  /// Number of worker threads, which bounds how many jobs run at once.
  workerCount: Int
  /// How often the server looks for delayed jobs that are due.
  schedulePollingIntervalMs: Int
  /// Pause between a failed attempt and the next one; 0 retries at once.
  retryDelayMs: Int
  /// How many attempt results `results` keeps per job (the most recent).
  maxResultsPerJob: Int
  /// How long `shutdown` waits for running jobs, and how long a job that is
  /// running at shutdown may still take and have its outcome stored.
  shutdownTimeoutMs: Int
  invariant: workerCount >= 1 and schedulePollingIntervalMs >= 1 and retryDelayMs >= 0
  invariant: maxResultsPerJob >= 1 and shutdownTimeoutMs >= 0
}

/// Queue `default`, four workers, a 1 s look-out for delayed jobs, immediate
/// retries, 100 results per job and a 15 s shutdown wait.
pub func defaultConfig(): HangfireConfig {
  HangfireConfig(
    queue = "default",
    workerCount = 4,
    schedulePollingIntervalMs = 1000,
    retryDelayMs = 0,
    maxResultsPerJob = 100,
    shutdownTimeoutMs = 15000
  )
}

func validQueueName(queue: in String): Bool {
  if queue.length == 0 {
    return false
  }
  var i = 0
  while i < queue.length {
    val c = Str.codeUnitAt(queue, i)
    val lower = c >= 97 and c <= 122
    val digit = c >= 48 and c <= 57
    if not lower and not digit and c != 95 and c != 45 {
      return false
    }
    i = i + 1
  }
  true
}

// ─── Shared state ─────────────────────────────────────────────────────────────
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

// One attempt in progress in this process.
ref record Attempt {
  // Hangfire's id of the background job (the firing, for a recurring job).
  jobId: String
  // `Kernel.monotonicMs` time at which the attempt has run for `timeoutMs`.
  deadline: Long
  // The last answer `stopRequested` read from the storage, and when.
  var checkedAt: Long
  var stopSeen: Bool
}

record AttemptView {
  jobId: String
  deadline: Long
  checkedAt: Long
  stopSeen: Bool
}

// Attempts in progress, by the job id the handler was given (`JobSpec.id`).
protected type AttemptTable {
  var running: Map[String, Attempt] = newMap()
  entry opened(id: in String, jobId: in String, deadline: in Long) {
    running[id] = Attempt(jobId = jobId, deadline = deadline, checkedAt = 0i64, stopSeen = false)
  }
  // Two firings of one recurring job may overlap; the entry belongs to the
  // one that began last.
  entry closed(id: in String, jobId: in String) {
    if running.containsKey(id) {
      val a: Attempt = running[id]
      if a.jobId == jobId {
        val _ = running.remove(id)
      }
    }
  }
  entry view(id: in String): Option[AttemptView] {
    if not running.containsKey(id) {
      return None
    }
    val a: Attempt = running[id]
    return Some(
      value = AttemptView(jobId = a.jobId, deadline = a.deadline, checkedAt = a.checkedAt, stopSeen = a.stopSeen)
    )
  }
  entry inFlight(): Int {
    return running.count
  }
  entry noteCheck(id: in String, jobId: in String, at: in Long, stopSeen: in Bool) {
    if running.containsKey(id) {
      val a: Attempt = running[id]
      if a.jobId == jobId {
        a.checkedAt = at
        a.stopSeen = stopSeen
      }
    }
  }
}

// The scheduler's Hangfire server, when it has one.
protected type ServerSlot {
  var claimed: Bool = false
  var held: List[Kernel.ServerHandle] = newList()
  // True for the one caller that may now start a server.
  entry claim(): Bool {
    if claimed {
      return false
    }
    claimed = true
    return true
  }
  entry hold(server: in Kernel.ServerHandle) {
    held.add(server)
  }
  entry abandon() {
    claimed = false
  }
  // The server, taken out of the slot: a list of none or one.
  entry release(): List[Kernel.ServerHandle] {
    val taken: List[Kernel.ServerHandle] = newList()
    for server in held {
      taken.add(server)
    }
    held.clear()
    claimed = false
    return taken
  }
  entry running(): Bool {
    return held.count > 0
  }
}

// ─── Running an attempt (Hangfire worker thread) ──────────────────────────────
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

// Whether job `call.jobId` was cancelled: a one-off job has left
// `Processing` (Hangfire moved it to `Deleted`), a recurring job carries the
// cancelled marker.
func cancelledNow(storage: in Kernel.NStorage, call: in Kernel.DispatchCall): Result[Bool, String] {
  if call.recurringId.length > 0 {
    if Store.recurringCancelled(storage, call.recurringId)? {
      return Ok(true)
    }
  }
  match Kernel.jobState(storage, call.jobId) {
    case Err(e) -> Err("hangfire: " + e)
    case Ok(None) -> Ok(true)
    case Ok(Some(name)) -> Ok(name != Kernel.stateProcessing)
  }
}

// One attempt of the background job `call.jobId`, on a Hangfire worker
// thread.  The outcome is recorded in the storage, and a retry is requested
// from Hangfire when the attempt failed and attempts remain.
func runAttempt(
  storage: in Kernel.NStorage,
  handlers: in HandlerRegistry,
  attempts: in AttemptTable,
  config: in HangfireConfig,
  call: in Kernel.DispatchCall,
): Result[Kernel.DispatchOutcome, String] {
  val recurring = call.recurringId.length > 0
  val id = if recurring { call.recurringId } else { call.jobId }
  if recurring {
    // A firing that was already queued when the recurring job was cancelled.
    if Store.recurringCancelled(storage, id)? {
      match Kernel.requestRetry(storage, call.jobId, false) {
        case Err(e) -> return Err("hangfire: " + e)
        case Ok(_) -> ()
      }
      return Ok(Kernel.DispatchOutcome(ok = true, text = "not run: the recurring job was cancelled"))
    }
    Store.noteFiring(storage, id, call.jobId, call.createdAtMs, config.maxResultsPerJob)?
  }
  val before = Store.attemptsOf(storage, call.jobId)?
  val spec = Jobs.JobSpec(
    id = id,
    name = call.name,
    payload = call.payload,
    maxAttempts = call.maxAttempts,
    timeoutMs = call.timeoutMs,
    scheduledAt = if recurring and call.createdAtMs > 0i64 { call.createdAtMs } else { 0i64 }
  )
  val startedAt = Jobs.nowMs()
  var outcome = AttemptOutcome(ok = false, text = "no handler registered for job '" + call.name + "'")
  var mayRetry = false
  match handlers.find(call.name) {
    case None -> ()
    case Some(found) -> {
      val handler: Jobs.JobHandler = found
      val deadline = Kernel.monotonicMs() + call.timeoutMs.toLong()
      attempts.opened(id, call.jobId, deadline)
      outcome = safeCall(handler, spec)
      val late = Kernel.monotonicMs() > deadline
      attempts.closed(id, call.jobId)
      mayRetry = true
      if cancelledNow(storage, call)? {
        outcome = AttemptOutcome(ok = false, text = "cancelled while running")
        mayRetry = false
      } else if late {
        outcome = AttemptOutcome(ok = false, text = "handler exceeded timeoutMs (" + call.timeoutMs.toString() + "ms)")
      }
    }
  }
  Store.recordAttempt(storage, call.jobId, outcome.ok, outcome.text, startedAt, Jobs.nowMs(), config.maxResultsPerJob)?
  val retry = not outcome.ok and mayRetry and before + 1 < call.maxAttempts
  match Kernel.requestRetry(storage, call.jobId, retry) {
    case Err(e) -> return Err("hangfire: " + e)
    case Ok(_) -> ()
  }
  Ok(Kernel.DispatchOutcome(ok = outcome.ok, text = outcome.text))
}

// ─── The scheduler ────────────────────────────────────────────────────────────
// What a scheduler holds besides its storage and configuration.
pub opaque type HangfireState {
  handlers: HandlerRegistry
  attempts: AttemptTable
  server: ServerSlot
}

/// A Hangfire-backed `Jobs.JobScheduler`.  Build one with `create`.
pub record HangfireJobScheduler {
  config: HangfireConfig
  storage: HangfireStorage
  state: HangfireState
}

/// Build a scheduler on `storage`.  It can submit, cancel and report jobs at
/// once; none runs until `start`.  `Err` when `config.queue` is not a valid
/// Hangfire queue name.
pub func create(config: in HangfireConfig, storage: in HangfireStorage): Result[HangfireJobScheduler, String] {
  if not validQueueName(config.queue) {
    return Err("hangfire: invalid queue name '" + config.queue + "': use lowercase letters, digits, '_' and '-'")
  }
  Ok(
    HangfireJobScheduler(
      config = config,
      storage = storage,
      state = HangfireState(handlers = HandlerRegistry(), attempts = AttemptTable(), server = ServerSlot())
    )
  )
}

/// Start a Hangfire server for this scheduler: its workers run the jobs on
/// `config.queue`, whoever submitted them.  `Err` when the scheduler already
/// has a server.
pub func start(s: in HangfireJobScheduler): Result[Unit, String] {
  if not s.state.server.claim() {
    return Err("hangfire: the scheduler is already started")
  }
  val storage = s.storage.native
  val handlers = s.state.handlers
  val attempts = s.state.attempts
  val config = s.config
  val run: (Kernel.DispatchCall) -> Kernel.DispatchOutcome = { call: Kernel.DispatchCall ->
    match runAttempt(storage, handlers, attempts, config, call) {
      case Ok(outcome) -> outcome
      // The storage failed while the attempt was being recorded.  The panic
      // reaches Hangfire as the job's failure.
      case Err(e) -> panic(e)
    }
  }
  match Kernel.startServer(storage, config.queue, config.workerCount, config.schedulePollingIntervalMs, config.retryDelayMs, config.shutdownTimeoutMs, run) {
    case Ok(server) -> {
      s.state.server.hold(server)
      Ok(())
    }
    case Err(e) -> {
      s.state.server.abandon()
      Err("hangfire: cannot start the server: " + e)
    }
  }
}

/// Stop the scheduler's server; a no-op when it has none.  Workers stop
/// fetching at once.  With `waitForJobs` the call waits up to
/// `config.shutdownTimeoutMs` for running jobs and is an `Err` when a
/// handler is still running then; without, it returns at once.  Either way
/// running handlers are left to finish.  The scheduler stays usable as a
/// client and can be started again.
pub func shutdown(s: in HangfireJobScheduler, waitForJobs: in Bool): Result[Unit, String] {
  val waitMs = if waitForJobs { s.config.shutdownTimeoutMs } else { 0 }
  for server in s.state.server.release() {
    match Kernel.stopServer(server, waitMs) {
      case Err(e) -> return Err("hangfire: cannot stop the server: " + e)
      case Ok(_) -> ()
    }
    // Hangfire gives up on its workers after the same time, so what it
    // reports cannot tell "all ended" from "abandoned"; the attempts this
    // scheduler is still inside can.
    val left = s.state.attempts.inFlight()
    if waitForJobs and left > 0 {
      return Err(
        "hangfire: the server is stopped, but " + left.toString() + " job(s) were still running after " +
          waitMs.toString() +
          "ms"
      )
    }
  }
  Ok(())
}

/// True while the scheduler has a server (between `start` and `shutdown`).
pub func isRunning(s: in HangfireJobScheduler): Bool {
  s.state.server.running()
}

/// Run `handler` for every job named `name`, replacing any earlier handler
/// for that name.  May be called at any time, from any thread.
pub func registerHandler(s: in HangfireJobScheduler, name: in String, handler: in Jobs.JobHandler): Unit
  requires: name.length > 0
{
  s.state.handlers.put(name, handler)
}

/// Run `handler` for jobs whose name has no handler of its own.
pub func registerDefaultHandler(s: in HangfireJobScheduler, handler: in Jobs.JobHandler): Unit {
  s.state.handlers.putDefault(handler)
}

/// Remove the handler for `name`; later attempts fall back to the default
/// handler, or fail when there is none.
pub func unregisterHandler(s: in HangfireJobScheduler, name: in String): Unit
  requires: name.length > 0
{
  s.state.handlers.drop(name)
}

impl Jobs.JobScheduler for HangfireJobScheduler {
  func enqueue(name: in String, payload: in String, maxAttempts: in Int, timeoutMs: in Int): Result[String, String]
    requires: name.length > 0
    requires: maxAttempts >= 1
    requires: timeoutMs >= 1
  {
    Store.submit(self.storage.native, self.config.queue, name, payload, maxAttempts, timeoutMs, 0i64, "")
  }

  func schedule(
    name: in String, payload: in String, cronExpr: in String, maxAttempts: in Int, timeoutMs: in Int
  ): Result[String, String]
    requires: name.length > 0
    requires: cronExpr.length > 0
    requires: maxAttempts >= 1
    requires: timeoutMs >= 1
  {
    match Jobs.parseCron(cronExpr) {
      case Err(e) -> Err(e)
      case Ok(_) -> Store.submitRecurring(
        self.storage.native,
        self.config.queue,
        name,
        payload,
        cronExpr,
        maxAttempts,
        timeoutMs
      )
    }
  }

  func cancel(jobId: in String): Result[Unit, String]
    requires: jobId.length > 0
  {
    Store.cancelJob(self.storage.native, jobId)
  }

  func status(jobId: in String): Result[Jobs.JobStatus, String]
    requires: jobId.length > 0
  {
    Store.statusOf(self.storage.native, jobId)
  }

  func results(jobId: in String): Result[Jobs.JobResultList, String]
    requires: jobId.length > 0
  {
    Store.resultsOf(self.storage.native, jobId, self.config.maxResultsPerJob)
  }
}

// ─── Cooperative stop ─────────────────────────────────────────────────────────
// How long an answer read from the storage is reused.
val stopCheckIntervalMs: Long = 50i64

/// For a handler to call while it runs job `jobId` (`JobSpec.id`): true once
/// the job has been cancelled, or the attempt has run for longer than its
/// `timeoutMs`.  The attempt is already lost then, so the handler can return
/// at once; what it returns is discarded.  Also true for a job this
/// scheduler is not running.  The job's state is read from the storage at
/// most every 50 ms.
pub func stopRequested(s: in HangfireJobScheduler, jobId: in String): Bool
  requires: jobId.length > 0
{
  match s.state.attempts.view(jobId) {
    case None -> true
    case Some(a) -> {
      val now = Kernel.monotonicMs()
      if now > a.deadline or a.stopSeen {
        return true
      }
      if a.checkedAt > 0i64 and now - a.checkedAt < stopCheckIntervalMs {
        return false
      }
      val call = Kernel.DispatchCall(
        jobId = a.jobId,
        name = "",
        payload = "",
        maxAttempts = 1,
        timeoutMs = 1,
        recurringId = if Store.isRecurringId(jobId) { jobId } else { "" },
        createdAtMs = 0i64
      )
      // A storage that cannot be read is not a request to stop.
      val stop = match cancelledNow(s.storage.native, call) { case Ok(cancelled) -> cancelled; case Err(_) -> false }
      s.state.attempts.noteCheck(jobId, a.jobId, now, stop)
      stop
    }
  }
}

// ─── Waiting ──────────────────────────────────────────────────────────────────
func isFinal(st: in Jobs.JobStatus): Bool {
  match st {
    case Jobs.JobStatus.Succeeded -> true
    case Jobs.JobStatus.Failed -> true
    case Jobs.JobStatus.Cancelled -> true
    case _ -> false
  }
}

/// Block until `jobId` is `Succeeded`, `Failed` or `Cancelled`, reading its
/// status every 10 ms, and return that status.  `Err` for an unknown job,
/// when the storage cannot be read, or when `timeoutMs` passes first.  A
/// recurring job only ever ends as `Cancelled`.
pub func awaitFinished(s: in HangfireJobScheduler, jobId: in String, timeoutMs: in Int): Result[Jobs.JobStatus, String]
  requires: jobId.length > 0
  requires: timeoutMs >= 0
{
  val deadline = Kernel.monotonicMs() + timeoutMs.toLong()
  while true {
    val st = Store.statusOf(s.storage.native, jobId)?
    if isFinal(st) {
      return Ok(st)
    }
    if Kernel.monotonicMs() >= deadline {
      return Err("job " + jobId + " did not finish within " + timeoutMs.toString() + "ms")
    }
    Jobs.sleepMs(10)
  }
  Err("job " + jobId + " did not finish")
}
