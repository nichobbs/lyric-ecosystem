//! Cache.Memcached - `Cache.CacheStore` on a memcached server.
//!
//! ```
//! import Cache.Memcached as MemcachedCache
//!
//! match MemcachedCache.connect("memcached://cache.internal:11211", MemcachedCache.options("myapp:cache")) {
//!   case Err(e) -> println(e.message())
//!   case Ok(store) -> {
//!     MemcachedCache.setWithTtl(store, "users:42", userJson, 300)
//!     val hit = MemcachedCache.get(store, "users:42")
//!   }
//! }
//! ```
//!
//! Layout: every entry is one memcached item, stored under
//! `<namespace> <generation> <key>` (`Cache.Memcached.Keys`) with memcached's
//! own expiry. The generation is a counter kept in the item
//! `<namespace> generation`.
//!
//! `clear()`: memcached cannot list its keys, and `flush_all` would empty the
//! server for every application, so `clear()` adds one to the generation
//! (`ma`). The entries of the old generation are never asked for again and
//! leave the server when their TTL ends or when memory is needed. A get, set
//! or delete sends the read of the generation and the command for the key in
//! ONE write, the key composed with the generation this store saw last; the
//! answer counts only when the generation read back is that one, otherwise
//! the command is repeated with the new generation. So a `clear()` by any
//! process is seen by the next operation of every other store on the
//! namespace, at the cost of a few dozen bytes per request and no extra round
//! trip. When the generation item itself is gone (evicted, or the server
//! restarted) the namespace is empty: reads miss, and the next write starts
//! a new generation that no earlier one equals.
//!
//! Failures: `CacheStore` has no error channel (`get` returns an `Option`,
//! the writes return `Unit`), so the interface methods degrade the way a
//! cache should: a failed `get` is a miss, a failed write is dropped, and the
//! failure is counted and kept in the store's `CacheErrorLog`
//! (`errorCount()`, `lastError()`). Code that must see the failure calls the
//! `Result`-returning functions of this package (`tryGet`, `trySet`,
//! `tryDelete`, `tryClear`), which the interface methods are built on.
//!
//! Thread safety: the store holds one `MemcachedClient`, which serializes
//! requests on one connection, so a store may be shared between threads.
@runtime_checked
@io
package Cache.Memcached

pub use Cache
import Std.Core
import Std.Collections
import Std.Encoding
import Std.Random
import Std.String as Str
import Std.Time
import Cache
import Cache.Memcached.Client as Client
import Cache.Memcached.Keys as Keys
import Cache.Memcached.Meta as Meta

/// The failures the interface methods of a `MemcachedCacheStore` swallowed.
@experimental
pub protected type CacheErrorLog {
  var count: Int = 0
  var last: Option[Client.MemcachedError] = None
  entry record(e: in Client.MemcachedError): Unit {
    count = count + 1
    last = Some(value = e)
  }
  /// How many interface calls failed since the store was built.
  func errorCount(): Int {
    count
  }
  /// The most recent failure.
  func lastError(): Option[Client.MemcachedError] {
    last
  }
}

/// The generation of the namespace as this store saw it last.
@experimental
pub protected type GenerationCell {
  var known: Bool = false
  var value: String = ""
  entry put(generation: in String): Unit {
    known = true
    value = generation
  }
  entry forget(): Unit {
    known = false
    value = ""
  }
  /// The generation, or `None` before the first operation and after the
  /// server was found to hold none.
  func current(): Option[String] {
    if known {
      Some(value = value)
    } else {
      None
    }
  }
}

/// How a store uses its memcached server.
@experimental
pub record MemcachedCacheOptions {
  /// The name every key of the store is filed under; `clear()` empties it and
  /// nothing else. Stores with the same namespace share their entries. No
  /// space or control character, at most 80 bytes.
  namespace: String
  invariant: Keys.namespaceProblem(namespace).length == 0
}

/// Options for `namespace`.
@experimental
pub func options(namespace: in String): MemcachedCacheOptions
  requires: Keys.namespaceProblem(namespace).length == 0
{
  MemcachedCacheOptions(namespace = namespace)
}

/// A `CacheStore` kept in memcached. Build one with `connect`, `connectWith`
/// or `fromClient`.
@experimental
pub record MemcachedCacheStore {
  client: Client.MemcachedClient
  options: MemcachedCacheOptions
  errors: CacheErrorLog
  generation: GenerationCell
}

/// A store on an existing client. The client is shared, not owned: closing it
/// is the caller's business.
@experimental
pub func fromClient(client: in Client.MemcachedClient, opts: in MemcachedCacheOptions): MemcachedCacheStore {
  MemcachedCacheStore(client = client, options = opts, errors = CacheErrorLog(), generation = GenerationCell())
}

func checkedOptions(namespace: in String): Result[MemcachedCacheOptions, Client.MemcachedError] {
  val problem = Keys.namespaceProblem(namespace)
  if problem.length > 0 {
    Err(Client.InvalidConfig(message = problem))
  } else {
    Ok(MemcachedCacheOptions(namespace = namespace))
  }
}

/// `options` for a namespace that comes from configuration: an unusable one
/// is `Err(InvalidConfig)` instead of a contract violation.
@experimental
pub func optionsFor(namespace: in String): Result[MemcachedCacheOptions, Client.MemcachedError] {
  checkedOptions(namespace)
}

/// A store on a new connection to the server a `memcached://` or
/// `memcached+tls://` (TLS) URL names. The connection is opened,
/// authenticated and checked for the server version before the store is
/// returned.
@experimental
pub func connect(url: in String, opts: in MemcachedCacheOptions): Result[MemcachedCacheStore, Client.MemcachedError] {
  match Client.connect(url) {
    case Err(e) -> Err(e)
    case Ok(client) -> Ok(fromClient(client, opts))
  }
}

/// A store on a new connection described by `cfg`: everything a URL cannot
/// carry, such as the TLS options (`Client.withTlsOptions`), the timeouts
/// and the server's item size limit.
@experimental
pub func connectWith(
  cfg: in Client.MemcachedConfig, opts: in MemcachedCacheOptions
): Result[MemcachedCacheStore, Client.MemcachedError] {
  match Client.open(cfg) {
    case Err(e) -> Err(e)
    case Ok(client) -> Ok(fromClient(client, opts))
  }
}

// ─── Requests in the namespace's generation ──────────────────────────────────
/// One meta command on a cache key of the store: the building block of
/// `runInGeneration`.
@experimental
pub record KeyedCommand {
  /// `mg`, `ms` or `md`.
  verb: String
  /// The cache key, as the application names it.
  key: String
  /// Request flags separated by one space, without `b`.
  flags: String
  /// The data block of an `ms`.
  payload: slice[Byte]
  invariant: verb == "mg" or verb == "ms" or verb == "md"
  invariant: key.length > 0
}

/// A command without a data block.
@experimental
pub func keyed(verb: in String, key: in String, flags: in String): KeyedCommand
  requires: verb == "mg" or verb == "md"
  requires: key.length > 0
{
  val none: slice[Byte] = []
  KeyedCommand(verb = verb, key = key, flags = flags, payload = none)
}

/// An `ms` storing `payload`.
@experimental
pub func keyedSet(key: in String, payload: in slice[Byte], flags: in String): KeyedCommand
  requires: key.length > 0
{
  KeyedCommand(verb = "ms", key = key, flags = flags, payload = payload)
}

/// The error for a reply a command does not produce; a `SERVER_ERROR`
/// becomes `ServerFailure`.
@experimental
pub func unexpectedReply(what: in String, reply: in Meta.MetaReply): Client.MemcachedError {
  match reply {
    case Meta.ServerError(m) -> Client.ServerFailure(message = m)
    case other -> Client.UnexpectedReply(message = what + " was answered with " + Meta.statusOf(other))
  }
}

func generationOf(namespace: in String, reply: in Meta.MetaReply): Result[Option[String], Client.MemcachedError] {
  match reply {
    case Meta.Miss(_) -> Ok(None)
    case Meta.NotFound(_) -> Ok(None)
    case Meta.Value(data, _) -> match tryDecodeUtf8(data) {
      case Some(text) -> if Keys.isDecimal(text) {
        Ok(Some(value = text))
      } else {
        Err(
          Client.UnexpectedReply(
            message = "the item '" + Keys.generationKey(namespace) + "' does not hold a generation number"
          )
        )
      }
      case None -> Err(
        Client.UnexpectedReply(
          message = "the item '" + Keys.generationKey(namespace) +
            "' does not hold a generation number"
        )
      )
    }
    case other -> Err(unexpectedReply("the read of the generation", other))
  }
}

func encodeKeyed(namespace: in String, generation: in String, command: in KeyedCommand): slice[Byte] {
  val wire = Keys.wireKey(Keys.dataKey(namespace, generation, command.key))
  if command.verb == "ms" {
    Meta.encodeSet(wire, command.payload, command.flags)
  } else {
    Meta.encodeCommand(command.verb, wire, command.flags)
  }
}

// A clear between the read of the generation and the command makes the
// command count for nothing, and it is repeated; this many clears in a row
// would have to interleave with one operation to exhaust the attempts.
val generationAttempts: Int = 6

/// Run `commands` on keys of the store's namespace, in its current
/// generation, in one round trip: the request is the read of the generation
/// followed by the commands, composed with the generation this store saw
/// last, and the replies count only when the generation read back is that
/// one (see the package header). Returns the replies of `commands` in order.
///
/// `Ok(None)` means the namespace holds nothing: it has no generation and
/// `create` is false. With `create` a missing generation is started first.
/// With `quiet` the commands carry the `q` flag and the replies are those the
/// server chose to send. Only the last command may change anything.
@experimental
pub func runInGeneration(
  store: in MemcachedCacheStore, commands: in List[KeyedCommand], create: in Bool, quiet: in Bool
): Result[Option[List[Meta.MetaReply]], Client.MemcachedError]
  requires: commands.count > 0
{
  val namespace = store.options.namespace
  val generationWire = Keys.wireKey(Keys.generationKey(namespace))
  val readGeneration = Meta.encodeCommand("mg", generationWire, "v")
  var attempt = 0
  while attempt < generationAttempts {
    attempt = attempt + 1
    match store.generation.current() {
      case None -> {
        val replies = match store.client.run(readGeneration, 1) { case Ok(r) -> r; case Err(e) -> return Err(e) }
        match generationOf(namespace, replies[0]) {
          case Err(e) -> return Err(e)
          case Ok(Some(found)) -> store.generation.put(found)
          case Ok(None) -> {
            if not create {
              return Ok(None)
            }
            val fresh = Keys.initialGeneration(nowEpochMillis(), nextIntBelow(sharedRandom(), 1000))
            val started = match store.client.run(Meta.encodeSet(generationWire, encodeUtf8(fresh), "ME T0"), 1) {
              case Ok(r) -> r
              case Err(e) -> return Err(e)
            }
            match started[0] {
              case Meta.Head(_) -> store.generation.put(fresh)
              // Another process started the generation first: read it.
              case Meta.NotStored(_) -> ()
              case other -> return Err(unexpectedReply("the start of the generation", other))
            }
          }
        }
      }
      case Some(generation) -> {
        val parts: List[slice[Byte]] = newList()
        parts.add(readGeneration)
        for command in commands {
          parts.add(encodeKeyed(namespace, generation, command))
        }
        if quiet {
          parts.add(Meta.encodeNoOp())
        }
        val request = Meta.concatAll(parts)
        val outcome = if quiet { store.client.runUntilNoOp(request) } else { store.client.run(request, commands.count + 1) }
        val replies = match outcome { case Ok(r) -> r; case Err(e) -> return Err(e) }
        if replies.count == 0 {
          return Err(Client.UnexpectedReply(message = "the read of the generation was not answered"))
        }
        match generationOf(namespace, replies[0]) {
          case Err(e) -> return Err(e)
          case Ok(None) -> store.generation.forget()
          case Ok(Some(found)) -> {
            if found == generation {
              val rest: List[Meta.MetaReply] = newList()
              var i = 1
              while i < replies.count {
                rest.add(replies[i])
                i = i + 1
              }
              return Ok(Some(value = rest))
            }
            store.generation.put(found)
          }
        }
      }
    }
  }
  Err(
    Client.NamespaceContended(
      message = "namespace '" + namespace + "' was cleared " + generationAttempts.toString() +
        " times while one operation ran"
    )
  )
}

/// The request flag that gives an entry the `CacheStore` TTL `ttlSeconds`
/// (`Keys.expiryToken` at the current time): `"T0"` for no expiry.
@experimental
pub func expiryFlag(ttlSeconds: in Int): String
  requires: ttlSeconds >= 0
{
  "T" + Keys.expiryToken(ttlSeconds, nowEpochMillis() / 1000i64).toString()
}

func tooLarge(bytes: in Int, limit: in Int): Client.MemcachedError {
  Client.ValueTooLarge(
    message = "the value is " + bytes.toString() + " bytes and the limit is " + limit.toString() + " bytes"
  )
}

/// `value` as the bytes to store. A value over the client's limit
/// (`MemcachedConfig.maxValueBytes`) is `Err(ValueTooLarge)`, and the entry
/// `key` held before is removed. That is what memcached itself does when it
/// refuses a value as too large, in every mode (set, add, compare-and-swap),
/// so a refused value empties the key whichever side refuses it, and a
/// failed write never leaves the old value to be read as current.
@experimental
pub func payloadFor(
  store: in MemcachedCacheStore, key: in String, value: in String
): Result[slice[Byte], Client.MemcachedError]
  requires: key.length > 0
{
  val bytes = encodeUtf8(value)
  val limit = store.client.maxValueBytes()
  if bytes.length <= limit {
    return Ok(bytes)
  }
  match tryDelete(store, key) {
    case Err(e) -> Err(e)
    case Ok(_) -> Err(tooLarge(bytes.length, limit))
  }
}

/// The error for the reply to a storing command that is not a success:
/// memcached's "object too large for cache" is `ValueTooLarge`.
@experimental
pub func storeFailure(what: in String, bytes: in Int, reply: in Meta.MetaReply): Client.MemcachedError {
  match reply {
    case Meta.ServerError(m) -> if Str.contains(m, "too large") {
      Client.ValueTooLarge(message = "the server refused the value of " + bytes.toString() + " bytes: " + m)
    } else {
      Client.ServerFailure(message = m)
    }
    case other -> unexpectedReply(what, other)
  }
}

/// The text of a stored value. Values are UTF-8; bytes another client stored
/// that are not are `UnexpectedReply`.
@experimental
pub func textOf(key: in String, data: in slice[Byte]): Result[String, Client.MemcachedError] {
  match tryDecodeUtf8(data) {
    case Some(text) -> Ok(text)
    case None -> Err(Client.UnexpectedReply(message = "the value of '" + key + "' is not UTF-8 text"))
  }
}

func single(command: in KeyedCommand): List[KeyedCommand] {
  val commands: List[KeyedCommand] = newList()
  commands.add(command)
  commands
}

// ─── The store's operations ──────────────────────────────────────────────────
/// `get` with the failure visible.
@experimental
pub func tryGet(store: in MemcachedCacheStore, key: in String): Result[Option[String], Client.MemcachedError]
  requires: key.length > 0
{
  match runInGeneration(store, single(keyed("mg", key, "v")), false, false) {
    case Err(e) -> Err(e)
    case Ok(None) -> Ok(None)
    case Ok(Some(replies)) -> match replies[0] {
      case Meta.Miss(_) -> Ok(None)
      case Meta.Value(data, _) -> match textOf(key, data) {
        case Ok(text) -> Ok(Some(value = text))
        case Err(e) -> Err(e)
      }
      case other -> Err(unexpectedReply("mg", other))
    }
  }
}

/// `set` with the failure visible. `ttlSeconds = 0` stores without an expiry
/// and removes an earlier one. A value memcached cannot hold is
/// `Err(ValueTooLarge)` and leaves the key empty.
@experimental
pub func trySet(
  store: in MemcachedCacheStore, key: in String, value: in String, ttlSeconds: in Int
): Result[Unit, Client.MemcachedError]
  requires: key.length > 0
  requires: ttlSeconds >= 0
{
  val payload = match payloadFor(store, key, value) { case Ok(p) -> p; case Err(e) -> return Err(e) }
  match runInGeneration(store, single(keyedSet(key, payload, expiryFlag(ttlSeconds))), true, false) {
    case Err(e) -> Err(e)
    case Ok(None) -> Err(Client.UnexpectedReply(message = "the namespace has no generation after one was started"))
    case Ok(Some(replies)) -> match replies[0] {
      case Meta.Head(_) -> Ok(())
      case other -> Err(storeFailure("ms", payload.length, other))
    }
  }
}

/// `delete` with the failure visible. `Ok(true)` when the key existed.
@experimental
pub func tryDelete(store: in MemcachedCacheStore, key: in String): Result[Bool, Client.MemcachedError]
  requires: key.length > 0
{
  match runInGeneration(store, single(keyed("md", key, "")), false, false) {
    case Err(e) -> Err(e)
    case Ok(None) -> Ok(false)
    case Ok(Some(replies)) -> match replies[0] {
      case Meta.Head(_) -> Ok(true)
      case Meta.NotFound(_) -> Ok(false)
      case other -> Err(unexpectedReply("md", other))
    }
  }
}

/// `clear` with the failure visible: move the namespace to its next
/// generation, in one atomic step on the server. Entries of other namespaces
/// and of other applications are not touched.
@experimental
pub func tryClear(store: in MemcachedCacheStore): Result[Unit, Client.MemcachedError] {
  val generationWire = Keys.wireKey(Keys.generationKey(store.options.namespace))
  match store.client.run(Meta.encodeCommand("ma", generationWire, "v"), 1) {
    case Err(e) -> Err(e)
    case Ok(replies) -> match generationOf(store.options.namespace, replies[0]) {
      case Err(e) -> Err(e)
      // No generation: the namespace holds nothing that could be read.
      case Ok(None) -> {
        store.generation.forget()
        Ok(())
      }
      case Ok(Some(next)) -> {
        store.generation.put(next)
        Ok(())
      }
    }
  }
}

/// A liveness check of the store's server: one `mn` round trip.
@experimental
pub func ping(store: in MemcachedCacheStore): Result[Unit, Client.MemcachedError] {
  Client.ping(store.client)
}

impl CacheStore for MemcachedCacheStore {
  func get(key: in String): Option[String] {
    match tryGet(self, key) {
      case Ok(v) -> v
      case Err(e) -> {
        self.errors.record(e)
        None
      }
    }
  }

  func set(key: in String, value: in String, ttlSeconds: in Int): Unit {
    match trySet(self, key, value, ttlSeconds) {
      case Ok(_) -> ()
      case Err(e) -> self.errors.record(e)
    }
  }

  func delete(key: in String): Unit {
    match tryDelete(self, key) {
      case Ok(_) -> ()
      case Err(e) -> self.errors.record(e)
    }
  }

  func clear(): Unit {
    match tryClear(self) {
      case Ok(_) -> ()
      case Err(e) -> self.errors.record(e)
    }
  }
}
