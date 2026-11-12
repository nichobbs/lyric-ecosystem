//! Flags.Flipt - `Flags.FlagStore` on a Flipt server (https://flipt.io).
//!
//! Pure Lyric over `Std.Http`: Flipt's v1 REST API, no SDK, so the backend is
//! the same code on .NET and the JVM.
//!
//!   import Flags.Flipt as Flipt
//!
//!   val store = Flipt.connect(Flipt.config("https://flipt.internal", "production").withToken(token))?
//!   if Flags.isEnabled(store, "new-checkout") { ... }
//!   val limit = Flags.getInt(store, "page-size", 25)
//!
//! Reads. In `Snapshot` mode (the default) the store keeps a snapshot of the
//! namespace's flags and answers every read from it, so a read is a map
//! lookup. The snapshot is refreshed lazily: a read that finds the last
//! refresh attempt older than `refreshIntervalMs` lists the flags again
//! before it answers. There is no background thread: nothing has to be
//! started or stopped, a store nobody reads makes no requests, and the
//! behaviour is the same on both targets. The cost is that the read that
//! triggers a refresh waits for it (at most `requestTimeoutMs` per page);
//! other threads keep reading the current snapshot meanwhile.
//!
//! In `Evaluate` mode (`withEvaluation(entityId, context)`) every `getValue`
//! and `isEnabled` is one evaluation request for the configured entity, so
//! Flipt's rules, segments and rollouts apply.
//!
//! Failures. `FlagStore` reads have no error channel, and a flag must not
//! flip because the network did: when Flipt cannot be reached a read is
//! answered with the last value this store read from the server, for as long
//! as `maxStaleMs` allows (0, the default: without limit). Only when there is
//! no such value, or it is older than `maxStaleMs`, the flag has no value and
//! the caller's default applies (`isEnabled` is then false). Every failed
//! request is counted (`errorCount`, `lastError`). Code that must see the
//! failure calls `tryGetValue`, `tryIsEnabled`, `tryListFlags` or
//! `tryRefresh`, which return a `Result`.
//!
//! How Flipt's flag types map to `FlagValue` is described in
//! `Flags.Flipt.Wire`; evaluation for other entities is in
//! `Flags.Flipt.Extras`, provisioning in `Flags.Flipt.Admin`.
//!
//! Thread safety: the store's state is a protected type and the HTTP client
//! is shareable, so a store may be shared between threads.
@runtime_checked
@io
package Flags.Flipt

pub use Flags
pub use Flags.Flipt.Client
import Std.Core
import Std.Collections
import Std.Time as Time
import Flags
import Flags.Flipt.Client
import Flags.Flipt.Wire

/// What a store has read from the server, and what went wrong.
@experimental
pub protected type StoreState {
  var values: Map[String, FlagValue] = newMap()
  var entries: List[FlagEntry] = newList()
  var loaded: Bool = false
  var loadedAtMs: Long = 0i64
  var attempted: Bool = false
  var attemptAtMs: Long = 0i64
  var attemptOk: Bool = false
  var refreshing: Bool = false
  var failures: Int = 0
  var refreshes: Int = 0
  var last: Option[FliptError] = None
  // `Evaluate` mode: the last answer per flag and when it was read. A flag
  // Flipt answered "no value" for is in `evaluatedAtMs` only.
  var evaluated: Map[String, FlagValue] = newMap()
  var evaluatedAtMs: Map[String, Long] = newMap()
  // True when the caller must refresh now: a refresh is due (or forced) and
  // no other thread is refreshing.
  entry claim(now: in Long, intervalMs: in Int, force: in Bool): Bool {
    if refreshing {
      return false
    }
    val due = force or not attempted or now - attemptAtMs >= intervalMs.toLong()
    if due {
      refreshing = true
      attempted = true
      attemptAtMs = now
    }
    due
  }
  entry install(now: in Long, fresh: in List[FlagEntry]) {
    val next: Map[String, FlagValue] = newMap()
    for e in fresh {
      next[e.name] = e.value
    }
    values = next
    entries = fresh
    loaded = true
    loadedAtMs = now
    attemptOk = true
    refreshing = false
    refreshes = refreshes + 1
  }
  entry refreshFailed(e: in FliptError) {
    attemptOk = false
    refreshing = false
    failures = failures + 1
    last = Some(value = e)
  }
  entry requestFailed(e: in FliptError) {
    failures = failures + 1
    last = Some(value = e)
  }
  entry remember(name: in String, value: in Option[FlagValue], now: in Long) {
    match value {
      case Some(v) -> {
        evaluated[name] = v
      }
      case None -> {
        val _ = evaluated.remove(name)
      }
    }
    evaluatedAtMs[name] = now
  }
  // The error of the last refresh attempt, `None` when it succeeded.
  func refreshProblem(): Option[FliptError] {
    if attemptOk {
      return None
    }
    match last {
      case Some(e) -> Some(value = e)
      case None -> Some(
        value = FliptError(
          code = "NOT_LOADED",
          message = "the flags have not been read from Flipt yet",
          status = 0
        )
      )
    }
  }
  // The snapshot's value of `name`, while the snapshot may be served.
  func lookup(name: in String, now: in Long, maxStaleMs: in Int): Option[FlagValue] {
    if loaded and (maxStaleMs == 0 or now - loadedAtMs <= maxStaleMs.toLong()) {
      mapGet(values, name)
    } else {
      None
    }
  }
  // The last evaluation of `name`, while it may be served; `found` is false
  // when there is none.
  func lastEvaluation(name: in String, now: in Long, maxStaleMs: in Int): EvaluatedLookup {
    match mapGet(evaluatedAtMs, name) {
      case None -> EvaluatedLookup(found = false, value = None)
      case Some(at) -> if maxStaleMs == 0 or now - at <= maxStaleMs.toLong() {
        EvaluatedLookup(found = true, value = mapGet(evaluated, name))
      } else {
        EvaluatedLookup(found = false, value = None)
      }
    }
  }
  func listing(now: in Long, maxStaleMs: in Int): slice[FlagEntry] {
    if loaded and (maxStaleMs == 0 or now - loadedAtMs <= maxStaleMs.toLong()) {
      entries.toArray()
    } else {
      []
    }
  }
  func current(name: in String): Option[FlagValue] {
    mapGet(values, name)
  }
  func currentListing(): slice[FlagEntry] {
    entries.toArray()
  }
  func errorCount(): Int {
    failures
  }
  func lastError(): Option[FliptError] {
    last
  }
  func refreshCount(): Int {
    refreshes
  }
  func ageMs(now: in Long): Option[Long] {
    if loaded {
      Some(value = now - loadedAtMs)
    } else {
      None
    }
  }
}

/// What `StoreState` remembers of one flag's last evaluation.
@experimental
pub record EvaluatedLookup {
  found: Bool
  value: Option[FlagValue]
}

/// A `FlagStore` read from a Flipt namespace. Build one with `open` or
/// `connect`; the fields are derived from a validated `FliptConfig` and are
/// not meant to be set by hand.
@experimental
pub record FliptFlagStore {
  client: FliptClient
  namespace: String
  /// True in `Evaluate` mode.
  evaluating: Bool
  entityId: String
  context: Map[String, String]
  refreshIntervalMs: Int
  maxStaleMs: Int
  // Milliseconds on a clock that never goes back.
  clock: () -> Long
  state: StoreState
}

func monotonicMillis(): Long {
  Time.monotonicNanos() / 1000000i64
}

func nowOf(store: in FliptFlagStore): Long {
  val clock = store.clock
  clock()
}

func invalidConfig(message: in String): FliptError {
  FliptError(code = "INVALID_CONFIG", message = message, status = 0)
}

/// A store for `cfg` that reads time from `clock` (milliseconds, never going
/// back): for tests of the refresh interval and the staleness limit.
/// `open` uses the process's monotonic clock.
@experimental
pub func openWithClock(cfg: in FliptConfig, clock: in () -> Long): Result[FliptFlagStore, FliptError] {
  val client = match openClient(cfg) { case Ok(c) -> c; case Err(e) -> return Err(e) }
  if not isValidKey(cfg.namespace) {
    return Err(invalidConfig("namespace '" + cfg.namespace + "' is not a Flipt key (letters, digits, '-', '_' and ',')"))
  }
  if cfg.refreshIntervalMs < 0 {
    return Err(invalidConfig("refreshIntervalMs must be >= 0, got " + cfg.refreshIntervalMs.toString()))
  }
  if cfg.maxStaleMs < 0 {
    return Err(invalidConfig("maxStaleMs must be >= 0, got " + cfg.maxStaleMs.toString()))
  }
  Ok(
    FliptFlagStore(
      client = client,
      namespace = cfg.namespace,
      evaluating = cfg.evaluates(),
      entityId = cfg.entityId,
      context = cfg.context,
      refreshIntervalMs = cfg.refreshIntervalMs,
      maxStaleMs = cfg.maxStaleMs,
      clock = clock,
      state = StoreState()
    )
  )
}

/// Validate `cfg` and build a store. No request is made: the flags are read
/// on the first read, and a server that is down then shows as reads without
/// a value and a counted error.
@experimental
pub func open(cfg: in FliptConfig): Result[FliptFlagStore, FliptError] {
  openWithClock(cfg, { -> monotonicMillis() })
}

/// `open`, then check that the namespace exists and read the flags once, so
/// a wrong URL, token or namespace is an `Err` here instead of a store whose
/// reads all fall back. `NOT_FOUND` for a namespace Flipt does not have
/// (listing its flags would succeed with an empty list).
@experimental
pub func connect(cfg: in FliptConfig): Result[FliptFlagStore, FliptError] {
  val store = match open(cfg) { case Ok(s) -> s; case Err(e) -> return Err(e) }
  match store.client.namespaceExists(store.namespace) {
    case Err(e) -> return Err(e)
    case Ok(false) -> return Err(
      FliptError(
        code = "NOT_FOUND",
        message = "Flipt has no namespace '" + store.namespace + "' at " + store.client.origin,
        status = 404
      )
    )
    case Ok(true) -> ()
  }
  match tryRefresh(store) {
    case Err(e) -> Err(e)
    case Ok(_) -> Ok(store)
  }
}

/// `connect(configFromEnv())`: `FLIPT_URL`, `FLIPT_NAMESPACE`,
/// `FLIPT_CLIENT_TOKEN`.
@experimental
pub func connectFromEnv(): Result[FliptFlagStore, FliptError] {
  match configFromEnv() {
    case Err(e) -> Err(e)
    case Ok(cfg) -> connect(cfg)
  }
}

// ─── Loading ─────────────────────────────────────────────────────────────────
func inputFor(store: in FliptFlagStore, flagKey: in String): EvaluationInput {
  EvaluationInput(flagKey = flagKey, entityId = store.entityId, context = store.context)
}

// The namespace's flags that have a value, in Flipt's order: their default
// values in `Snapshot` mode, their evaluation for the store's entity in
// `Evaluate` mode (one listing and one batch evaluation).
func load(store: in FliptFlagStore): Result[List[FlagEntry], FliptError] {
  val flags = match store.client.listFlags(store.namespace) { case Ok(f) -> f; case Err(e) -> return Err(e) }
  val found: List[FlagEntry] = newList()
  if not store.evaluating {
    for f in flags {
      match snapshotValue(f) {
        case Some(v) -> found.add(FlagEntry(name = f.key, value = v, description = f.description))
        case None -> ()
      }
    }
    return Ok(found)
  }
  val inputs: List[EvaluationInput] = newList()
  for f in flags {
    inputs.add(inputFor(store, f.key))
  }
  val answers = match store.client.evaluateBatch(store.namespace, inputs) {
    case Ok(a) -> a
    case Err(e) -> return Err(e)
  }
  var i = 0
  while i < flags.count {
    match evaluationValue(answers[i]) {
      case Some(v) -> found.add(FlagEntry(name = flags[i].key, value = v, description = flags[i].description))
      case None -> ()
    }
    i = i + 1
  }
  Ok(found)
}

func refreshWhen(store: in FliptFlagStore, force: in Bool): Result[Unit, FliptError] {
  val now = nowOf(store)
  if not store.state.claim(now, store.refreshIntervalMs, force) {
    // Not due, or another thread is refreshing: the outcome of the last
    // attempt stands.
    return match store.state.refreshProblem() { case Some(e) -> Err(e); case None -> Ok(()) }
  }
  // The claim must be released whatever happens, or no read would ever
  // refresh again.
  val loaded = try {
    load(store)
  } catch Bug as bug {
    Err(error = FliptError(code = "MALFORMED_RESPONSE", message = "ListFlags: " + bug.message, status = 0))
  }
  match loaded {
    case Ok(fresh) -> {
      store.state.install(now, fresh)
      Ok(())
    }
    case Err(e) -> {
      store.state.refreshFailed(e)
      Err(e)
    }
  }
}

func evaluateOne(store: in FliptFlagStore, name: in String): Result[Option[FlagValue], FliptError] {
  val inputs: List[EvaluationInput] = newList()
  inputs.add(inputFor(store, name))
  match store.client.evaluateBatch(store.namespace, inputs) {
    case Err(e) -> {
      store.state.requestFailed(e)
      Err(e)
    }
    case Ok(answers) -> {
      val value = evaluationValue(answers[0])
      store.state.remember(name, value, nowOf(store))
      Ok(value)
    }
  }
}

func invalidName(name: in String): FliptError {
  FliptError(
    code = "INVALID_ARGUMENT",
    message = "'" + name + "' cannot be a Flipt flag key (letters, digits, '-', '_' and ',')",
    status = 0
  )
}

// ─── Reads with the failure visible ──────────────────────────────────────────
/// Read the flags from Flipt now, whatever the refresh interval says. `Ok`
/// means the store now serves what Flipt holds.
@experimental
pub func tryRefresh(store: in FliptFlagStore): Result[Unit, FliptError] {
  refreshWhen(store, true)
}

/// `getValue` with the failure visible: `Ok` only for an answer Flipt gave
/// within the refresh interval (`Snapshot`) or just now (`Evaluate`), never
/// for a value served from an earlier read. `Ok(None)` for a flag Flipt does
/// not have or that has no value; `Err(INVALID_ARGUMENT)` for a name that
/// cannot be a Flipt key.
@experimental
pub func tryGetValue(store: in FliptFlagStore, name: in String): Result[Option[FlagValue], FliptError]
  requires: name.length > 0
{
  if not isValidKey(name) {
    return Err(invalidName(name))
  }
  if store.evaluating {
    return evaluateOne(store, name)
  }
  match refreshWhen(store, false) {
    case Err(e) -> Err(e)
    case Ok(_) -> Ok(store.state.current(name))
  }
}

/// `isEnabled` with the failure visible.
@experimental
pub func tryIsEnabled(store: in FliptFlagStore, name: in String): Result[Bool, FliptError]
  requires: name.length > 0
{
  match tryGetValue(store, name) {
    case Err(e) -> Err(e)
    case Ok(value) -> Ok(isTrue(value))
  }
}

/// `listFlags` with the failure visible: the flags that have a value, read
/// from Flipt within the refresh interval.
@experimental
pub func tryListFlags(store: in FliptFlagStore): Result[slice[FlagEntry], FliptError] {
  match refreshWhen(store, false) {
    case Err(e) -> Err(e)
    case Ok(_) -> Ok(store.state.currentListing())
  }
}

// ─── What the store went through ─────────────────────────────────────────────
/// How many requests to Flipt failed since the store was built.
@experimental
pub func errorCount(store: in FliptFlagStore): Int {
  store.state.errorCount()
}

/// The most recent failure.
@experimental
pub func lastError(store: in FliptFlagStore): Option[FliptError] {
  store.state.lastError()
}

/// How many times the flags were read from Flipt successfully.
@experimental
pub func refreshCount(store: in FliptFlagStore): Int {
  store.state.refreshCount()
}

/// The age of the flags the store holds in milliseconds, `None` before the
/// first successful read.
@experimental
pub func snapshotAgeMs(store: in FliptFlagStore): Option[Long] {
  store.state.ageMs(nowOf(store))
}

// ─── The interface ───────────────────────────────────────────────────────────
func isTrue(value: in Option[FlagValue]): Bool {
  match value {
    case Some(fv) -> match fv { case FlagBool(b) -> b; case _ -> false }
    case None -> false
  }
}

func readValue(store: in FliptFlagStore, name: in String): Option[FlagValue] {
  if not isValidKey(name) {
    return None
  }
  if not store.evaluating {
    val _ = refreshWhen(store, false)
    return store.state.lookup(name, nowOf(store), store.maxStaleMs)
  }
  match evaluateOne(store, name) {
    case Ok(value) -> value
    case Err(_) -> {
      val now = nowOf(store)
      val remembered = store.state.lastEvaluation(name, now, store.maxStaleMs)
      if remembered.found {
        remembered.value
      } else {
        // Never evaluated on its own: what the last listing evaluated.
        store.state.lookup(name, now, store.maxStaleMs)
      }
    }
  }
}

// Qualified on purpose: on the JVM an unqualified interface of another
// package is taken for a JDK interface, and `listFlags` then gets a typed
// array descriptor that does not override the interface's.
impl Flags.FlagStore for FliptFlagStore {
  func isEnabled(name: in String): Bool {
    isTrue(readValue(self, name))
  }

  func getValue(name: in String): Option[FlagValue] {
    readValue(self, name)
  }

  func listFlags(): slice[FlagEntry] {
    val _ = refreshWhen(self, false)
    self.state.listing(nowOf(self), self.maxStaleMs)
  }

  func refresh(): Result[Unit, FlagError] {
    match tryRefresh(self) {
      case Ok(_) -> Ok(())
      case Err(e) -> Err(FlagError(message = e.text(), code = e.code))
    }
  }
}
