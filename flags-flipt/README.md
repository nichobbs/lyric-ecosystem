# lyric-flags-flipt

[Flipt](https://flipt.io) backend for the `Flags.FlagStore` interface of `lyric-feature-flags`, for
`dotnet` and `jvm`, `experimental` (0.1.0). Flipt is an open-source, self-hosted feature flag
server; this package reads its flags over the v1 REST API. The client is written in Lyric on
`Std.Http` and `Std.JsonValue`: there is no Flipt SDK underneath and no extern declaration in the
package, so the same code runs on both targets.

`lyric-feature-flags` ships only an in-process store; its README explains that an earlier remote
store was removed because it never had a real HTTP client behind it. This package is a remote store
with one.

| | |
|---|---|
| Package | `Lyric.Flags.Flipt` (Lyric packages `Flags.Flipt`, `Flags.Flipt.Client`, `Flags.Flipt.Extras`, `Flags.Flipt.Admin`, `Flags.Flipt.Wire`) |
| Interface | `Flags.FlagStore` of `Lyric.Flags` 0.1.0 |
| Targets | `dotnet`, `jvm` |
| Stability | `experimental` (0.1.0) |
| Service | Flipt v1 (v1.62.0 is what CI runs). Flipt v2 has a different API and is not supported. |
| Protocol | Flipt's v1 REST API, JSON over HTTP, `Authorization: Bearer <client token>` |
| Dependencies | none besides the interface. No SDK, `[nuget]` and `[maven]` are empty |

## Usage

Inside this workspace a package declares `"Lyric.Flags.Flipt" = { workspace = true }` under
`[dependencies]`, and `"Lyric.Flags" = { workspace = true }` as well when it imports `Flags` itself,
as the examples here do. The examples are compiled by `tests/readme_tests.l`.

```lyric
import Std.Core
import Std.Collections
import Flags
import Flags.Flipt as Flipt
import Flags.Flipt.Admin as FliptAdmin
import Flags.Flipt.Extras as FliptExtras

func pageSize(token: in String): Result[Int, String] {
  val cfg = Flipt.config("https://flipt.internal.example", "production").withToken(token)
  val store = match Flipt.connect(cfg) { case Ok(s) -> s; case Err(e) -> return Err(e.text()) }
  if Flags.isEnabled(store, "new-checkout") {
    println("new checkout is on")
  }
  Ok(Flags.getInt(store, "page-size", 25))
}
```

`connect` checks the namespace and reads the flags once, so a wrong URL, token or namespace is an
`Err` at start-up. After that every read is answered from the store's snapshot, which is refreshed
when it is older than the refresh interval (10 seconds by default). `Flipt.connectFromEnv()` takes
the URL, the namespace and the token from `FLIPT_URL`, `FLIPT_NAMESPACE` and `FLIPT_CLIENT_TOKEN`.
Take the token from the application's `config { }` block or the environment, never from source.

`FlagStore` reads cannot fail, so when Flipt is unreachable they answer with the last value the
store read (see "Reads, the cache and failures"). Code that must know calls the `try*` functions:

```lyric
func killSwitch(store: in Flipt.FliptFlagStore): Bool {
  match Flipt.tryIsEnabled(store, "kill-switch") {
    case Ok(on) -> on
    case Err(e) -> {
      println("Flipt is unreachable (" + e.code + "), " + Flipt.errorCount(store).toString() + " failures so far")
      // The last value read from Flipt, or false when there is none.
      store.isEnabled("kill-switch")
    }
  }
}
```

A snapshot has no targeting: every flag has its default value. To let Flipt's rules, segments and
rollouts decide, evaluate for an entity, either one flag at a time through `Flags.Flipt.Extras`:

```lyric
func themeFor(store: in Flipt.FliptFlagStore, userId: in String, plan: in String): String {
  val context: Map[String, String] = newMap()
  context["plan"] = plan
  match FliptExtras.evaluateVariant(store, "theme", userId, context) {
    case Ok(result) -> if result.variantKey.length > 0 { result.variantKey } else { "light" }
    case Err(_) -> "light"
  }
}
```

or with a store in `Evaluate` mode, which is a `FlagStore` whose every read is evaluated for one
entity (one request per read):

```lyric
func storeForUser(cfg: in Flipt.FliptConfig, userId: in String, plan: in String): Result[Flags.FlagStore, String] {
  val context: Map[String, String] = newMap()
  context["plan"] = plan
  match Flipt.connect(cfg.withEvaluation(userId, context).withMaxStale(300000)) {
    case Ok(store) -> Ok(store)
    case Err(e) -> Err(e.text())
  }
}
```

`Flags.Flipt.Admin` writes `FlagEntry` values into a namespace, for seeding a development or test
server (the token needs write access):

```lyric
func provision(cfg: in Flipt.FliptConfig): Result[Unit, String] {
  val client = match Flipt.openClient(cfg) { case Ok(c) -> c; case Err(e) -> return Err(e.text()) }
  val entries = [
    Flags.makeFlagWithDesc("new-checkout", false, "the 2027 checkout flow"),
    Flags.makeIntFlag("page-size", 25),
    Flags.makeStringFlag("banner", "spring")
  ]
  match FliptAdmin.seed(client, cfg.namespace, entries) {
    case Ok(_) -> Ok(())
    case Err(e) -> Err(e.text())
  }
}
```

`Flags.Flipt` re-exports `Flags` and `Flags.Flipt.Client`, so an application that imports only
`Flags.Flipt` can name `FlagStore`, `FlagEntry`, `FlagValue`, the accessors, `FliptConfig` and
`FliptError` (`tests/reexport_tests.l`).

## How the interface maps onto Flipt

Flipt has two flag types; `FlagStore` has four value types.

| In Flipt | `FlagValue` |
|---|---|
| Boolean flag | `FlagBool` of the flag's `enabled` field |
| Variant flag, the variant's attachment is a JSON string (`"spring"`) | `FlagString` of the string |
| Variant flag, attachment is an integer that fits `Int` (`25`) | `FlagInt` |
| Variant flag, attachment is any other number (`0.5`, `2.0`, `1e3`, `3000000000`) | `FlagFloat` |
| Variant flag, attachment is `true` or `false` | `FlagBool` |
| Variant flag, no attachment, or an object, array or `null` attachment | `FlagString` of the variant key |
| Variant flag without a variant to answer | no value: `getValue` is `None`, the flag is not listed |

Which variant answers:

- In a snapshot, and whenever no rule matches: the flag's default variant. A variant flag without a
  default variant has no value.
- In `Evaluate` mode and in `Flags.Flipt.Extras`: the variant Flipt's evaluation returns for the
  entity, which is the variant of the first matching rule, else the default variant.

`isEnabled(name)` is true only for a `FlagBool(true)`, as the interface requires: a boolean flag
that is on, or a variant whose attachment is `true`. `getString`, `getInt`, `getFloat` and
`getBool` of `Flags` return the caller's default for a missing flag, a flag without a value and a
value of another type; nothing is converted (a `FlagInt` is not a `FlagFloat`, `"25"` is not `25`).

`listFlags()` returns the flags that have a value, in Flipt's order (creation order), with the
flag's description. `refresh()` reads the flags from Flipt now and returns the failure as a
`FlagError` whose `code` is the `FliptError` code.

`Flags.Flipt.Admin.setFlag` stores an entry so that it reads back as the same value: a `FlagBool`
as a boolean flag, anything else as an enabled variant flag with one variant, key `value`, whose
attachment is the value as JSON, set as the default variant.

## Reads, the cache and failures

**Snapshot mode (the default).** The store keeps the namespace's flags in memory and answers every
read from them: `isEnabled`, `getValue` and `listFlags` make no request. The snapshot is refreshed
lazily. A read that finds the last refresh attempt at least `refreshIntervalMs` old lists the flags
again (`GET /api/v1/namespaces/{namespace}/flags`, following the pages) before it answers. There is
no background thread, for three reasons: nothing has to be started, stopped or owned by the
application; a store nobody reads makes no requests; and the behaviour is the same on both targets.
The cost is that the read that triggers a refresh waits for it, at most `requestTimeoutMs` per
page. Other threads reading at that moment are answered from the current snapshot. `refresh()` and
`tryRefresh` read the flags at once, whatever the interval says.

**Evaluate mode.** `cfg.withEvaluation(entityId, context)`: `getValue` and `isEnabled` are one
`POST /evaluate/v1/batch` each, for that entity and context (the batch endpoint is used because it
answers for a flag of either type). `listFlags` lists the namespace and evaluates every flag in one
more batch request, at most once per `refreshIntervalMs`.

**When Flipt cannot be reached.** `FlagStore` reads have no error channel, and a flag must not flip
because the network did. So:

1. A read is answered with the last value this store read from Flipt for that flag: the snapshot in
   snapshot mode, the flag's last evaluation in evaluate mode (for a flag never evaluated on its
   own, what the last listing evaluated).
2. That lasts for `maxStaleMs` after the value was read. The default, 0, is no limit: the last known
   values are served for as long as the outage lasts.
3. When there is no such value (the store never reached Flipt), or it is older than `maxStaleMs`,
   the flag has no value: `getValue` is `None`, `isEnabled` is false, `listFlags` is empty, and the
   typed accessors return the caller's default.
4. A failed refresh is retried when the next read comes at least `refreshIntervalMs` later, not on
   every read, so an outage costs one waiting read per interval.

Every failed request is counted: `errorCount(store)`, `lastError(store)`. `refreshCount(store)` and
`snapshotAgeMs(store)` say how fresh the snapshot is. Nothing panics.

`tryGetValue`, `tryIsEnabled` and `tryListFlags` are the same reads with the failure visible. They
return `Ok` only for an answer Flipt gave within the refresh interval (snapshot mode) or just now
(evaluate mode), never for a value served from an earlier read.

`FliptError.code`:

| Code | Meaning |
|---|---|
| `UNAUTHENTICATED` | HTTP 401: the token is missing or wrong |
| `PERMISSION_DENIED` | HTTP 403 |
| `NOT_FOUND` | HTTP 404: no such flag (evaluation, `getFlag`) or namespace (`connect`) |
| `INVALID_ARGUMENT` | HTTP 400, or a request this client refused to send (a name that cannot be a Flipt key) |
| `HTTP_<status>` | any other response that is not a 200, redirects included |
| `TIMEOUT` | no response within `requestTimeoutMs` |
| `CONNECTION_FAILED` | no response for another reason |
| `MALFORMED_RESPONSE` | a 200 this client cannot read |
| `INVALID_CONFIG` | the configuration cannot be used |
| `NOT_LOADED` | `try*` on a store whose first refresh another thread is still making |

Requests are not retried and redirects are not followed.

## The conformance suite

`Testing.Conformance.Flags.run(factory)` passes, 8 of 8 cases, in snapshot mode and in evaluate
mode, on both targets, against the in-process fake and against a real Flipt 1.62.0. The suite's
factory receives the entries a store must hold, so the test factory creates a new namespace per
case, stores the entries through `Flags.Flipt.Admin.seed`, and returns a store connected to that
namespace.

## Configuration

`Flipt.config(url, namespace)` returns the defaults; the `with*` functions change one setting.

| Field | Builder | Default | Meaning |
|---|---|---|---|
| `url` | `config(url, _)` | required | `http(s)://host[:port]` of the Flipt server, no path. `https://` unless the host is `localhost`, `127.0.0.1` or `[::1]` |
| `namespace` | `config(_, namespace)`, `withNamespace` | required | the Flipt namespace the flags are read from |
| `token` | `withToken` | empty | a Flipt client token, sent as `Authorization: Bearer`; empty for a server without authentication |
| `requestTimeoutMs` | `withRequestTimeout` | 5000 | deadline of one request |
| `refreshIntervalMs` | `withRefreshInterval` | 10000 | how old the last refresh attempt may be before a read refreshes; 0 refreshes on every read |
| `maxStaleMs` | `withMaxStale` | 0 | how long the last known values are served while Flipt is unreachable; 0 is no limit |
| `mode`, `entityId`, `context` | `withEvaluation(entityId, context)` | `Snapshot` | `Evaluate` mode for that entity and context |
| `pageSize` | `withPageSize` | 100 | flags per `ListFlags` request, 1 to 1000 |

Environment variables read by `configFromEnv()` and `connectFromEnv()`:

| Variable | Meaning |
|---|---|
| `FLIPT_URL` | the server URL; required |
| `FLIPT_NAMESPACE` | the namespace; `default` when unset or empty |
| `FLIPT_CLIENT_TOKEN` | the client token; optional |

Functions: `open(cfg)` validates and makes no request; `connect(cfg)` also checks the namespace and
reads the flags; `openWithClock(cfg, clock)` takes the clock (milliseconds) from the caller, for
tests of the interval and the staleness limit; `openClient(cfg)` returns the bare `FliptClient` for
`Flags.Flipt.Admin`.

Authentication: Flipt's static client tokens (its `token` authentication method; a server you run
can be given one with the `authentication.methods.token.bootstrap.token` setting, as
`scripts/ci/start-flipt.sh` does). Pass the token with `withToken` or `FLIPT_CLIENT_TOKEN`. The
token must be allowed to read the namespace's flags through the management API and, for evaluate
mode and `Extras`, to evaluate.

## Surprises

Behaviour a user would not expect from `FlagStore` alone.

- **A read can block.** In snapshot mode one read per refresh interval lists the flags over HTTP
  and waits for the answer, up to `requestTimeoutMs` per page. In evaluate mode every read is a
  request. Use `withRefreshInterval` and `withRequestTimeout` to bound it.
- **A read never fails and never panics; it may be stale.** See "When Flipt cannot be reached". With
  the default `maxStaleMs = 0` a store serves the values of its last successful read for the whole
  of an outage. Check `errorCount` or use the `try*` functions to see it.
- **A snapshot ignores rules, segments and rollouts.** A boolean flag with a rollout to 10 % of
  entities is its plain `enabled` value in a snapshot; a variant flag is its default variant. Use
  evaluate mode or `Flags.Flipt.Extras` for targeting.
- **A disabled variant flag still has a value.** Flipt answers the default variant of a disabled
  variant flag (disabling switches the rules off, not the default), so the store does too, in both
  modes. Only a variant flag without a default variant has no value.
- **A variant flag without a default variant does not exist for a snapshot.** It is not in
  `listFlags()`, and `getValue` is `None`, exactly like a flag that is not there.
- **`open` does not notice a missing namespace.** Flipt lists a namespace that does not exist as an
  empty list of flags, so a store opened on a misspelled namespace reads as empty, with no error.
  `connect` checks (`GetNamespace`) and returns `NOT_FOUND`.
- **Flag names are Flipt keys.** Letters, digits, `-`, `_` and `,` only. A read of any other name
  (`"checkout.v2"`, `"new checkout"`) has no value and makes no request; `tryGetValue` returns
  `INVALID_ARGUMENT`; `Admin.setFlag` refuses it.
- **Types come from the attachment, not from the caller.** `getInt` of a variant whose attachment
  is `"25"` (a JSON string) is the default, because the value is a `FlagString`. An attachment
  `2.0` is a `FlagFloat`, `2` a `FlagInt`. An integer outside `Int` (`3000000000`) is a `FlagFloat`,
  rounded to 32 bits. A JSON object attachment, the usual thing in Flipt's UI, is not a value: the
  flag reads as a `FlagString` of the variant key, and the attachment itself is available as text
  from `Extras.evaluateVariant`.
- **`FlagFloat` is 32-bit.** `Float` is binary32 on every Lyric target, so `0.1` read from Flipt is
  the `Float` nearest 0.1.
- **A variant attachment `true` makes the flag enabled.** `isEnabled` is true for it, although it is
  not a boolean flag in Flipt.
- **Descriptions are the flag's, not the variant's.** `listFlags()` carries the flag description.
- **`Admin.setFlag` replaces by deleting.** The old flag, with its variants, rules and rollouts, is
  deleted, then the new one is created in up to three requests. It is not atomic, and a reader in
  between sees the flag missing. It is meant for seeding, not for flipping a production flag: flip
  flags in Flipt.
- **`http://` only on loopback.** A URL with `http://` and any host other than `localhost`,
  `127.0.0.1` or `[::1]` is `INVALID_CONFIG`: the token and the flags would cross the network in
  clear. This includes private addresses and service names inside a cluster.
- **No base path.** The URL is `scheme://host[:port]`; a Flipt served under a path prefix is not
  supported.
- **Differences between targets:** none known. The same tests pass on both.

## Backend-specific modules

`Flags.Flipt.Extras` (evaluation for an entity; one request each, no cache, no fallback):

| Function | Flipt endpoint |
|---|---|
| `evaluateBoolean(store, flagKey, entityId, context)` | `POST /evaluate/v1/boolean`: `enabled` and the reason |
| `evaluateVariant(store, flagKey, entityId, context)` | `POST /evaluate/v1/variant`: variant key, attachment, matched segments, reason |
| `evaluateBatch(store, inputs)` | `POST /evaluate/v1/batch`: flags of either type, answers in request order, a missing flag is a `FlagNotFound` |
| `valueFor(store, flagKey, entityId, context)` | one batch request, the answer as an `Option[FlagValue]` |

`Flags.Flipt.Admin` (management API): `createNamespace`, `deleteNamespace` (deletes the flags
first, Flipt refuses otherwise), `setFlag`, `seed`, `deleteFlag`.

`Flags.Flipt.Client` is the HTTP client both are built on (`FliptClient.call` sends any request and
returns the response document) and `Flags.Flipt.Wire` the JSON codec and the type mapping.

## Tests

Run from the repository root.

Hermetic (no service needed), 26 tests on each target:

```sh
lyric test --manifest flags-flipt/lyric.toml --target dotnet
lyric test --manifest flags-flipt/lyric.toml --target jvm
```

- `tests/wire_tests.l` (11): keys, the attachment-to-value mapping and its inverse, every response
  document (taken from a Flipt 1.62.0 server) and every request body.
- `tests/config_tests.l` (5): the URL rule, defaults, builders, validation, the environment
  variables, the error codes.
- `tests/reexport_tests.l` (1), `tests/readme_tests.l` (1): what an importer can name; the examples
  above.
- `tests/fake_flipt_tests.l` (8): the backend over real HTTP against a Flipt written in Lyric on
  `Std.HttpServer`, which checks the bearer token on every request and implements the management
  and evaluation endpoints the backend uses with Flipt's observed behaviour. The conformance suite
  in both modes; the cache with a clock the test moves (no request within the interval, one at the
  interval, `refresh()`, paging); an outage (last known values, the staleness limit, error counts,
  recovery, a store opened during the outage); evaluate mode (entity and context reach the server,
  per-flag fallback); `Extras`; authentication and every bad response (401, 403, 404, 500, an HTML
  502, a redirect, a body that is not JSON, wrong shapes, a listing that never ends, a slow server
  against the request deadline, nothing listening); `Admin`.

Integration (a real Flipt server), 6 more tests on each target:

```sh
bash scripts/ci/start-flipt.sh        # prints LYRIC_FLIPT_URL and LYRIC_FLIPT_TOKEN
LYRIC_FLIPT_URL=http://127.0.0.1:19930 LYRIC_FLIPT_TOKEN=... \
  lyric test --manifest flags-flipt/lyric.toml --target dotnet --features integration
```

| Variable | Meaning |
|---|---|
| `LYRIC_FLIPT_URL` | base URL of the server; required under `--features integration`, the suite fails naming it when it is unset |
| `LYRIC_FLIPT_TOKEN` | a token allowed to manage namespaces and flags; unset for a server without authentication |

`tests/integration/flipt_integration_tests.l` runs the conformance suite in both modes, `connect`
failures, the full type mapping on flags made as an operator makes them, snapshot refresh and
paging, and real segments, rules and rollouts through `Extras` and evaluate mode. It creates
namespaces named `lyric-it-<time>-<n>` and deletes them; nothing else on the server is touched.

`scripts/ci/start-flipt.sh` downloads the Flipt 1.62.0 release archive (SHA-256 pinned), caches the
binary under `~/.cache/lyric-ci/flipt`, and starts it on `127.0.0.1:19930` with SQLite storage in a
temporary directory and a random bootstrap token.

## Pinned versions

| What | Version |
|---|---|
| Flipt server tested against | v1.62.0 (release `flipt_linux_x86_64.tar.gz`, SHA-256 `fbea0403e9bc7d55ddb8f18e8c76c22c37d0a485bdf2385e677501f24cfd8f41`) |
| Flipt API | v1 REST: `/api/v1/namespaces/...`, `/evaluate/v1/boolean`, `/evaluate/v1/variant`, `/evaluate/v1/batch`, as in the `openapi.yaml` of that release |
| `Lyric.Flags` | 0.1.0 |
| SDKs | none |

## Known gaps

- The snapshot is read through the management API's `ListFlags`, so the token must be allowed to
  list flags. Flipt's client-side SDKs use an internal snapshot endpoint
  (`/internal/v1/evaluation/snapshot/namespace/{key}`) with `ETag` support instead; it is not part
  of the documented API and is not used here, so an unchanged namespace is downloaded again at
  every refresh.
- No streaming or push updates: changes arrive at the next refresh.
- No TLS options (private CA, client certificate): the client uses `Std.Http`'s defaults and the
  system trust store.
- Client-token authentication only; Flipt's JWT and Kubernetes methods are not supported.
- `Admin` does not manage segments, rules, distributions or rollouts.
- Flipt v2 (Git-native storage, `/api/v2`) is not supported.
- Compiler problems found while writing this package, none worked around in a way that changes
  behaviour; issues are to be filed in `nichobbs/lyric-lang`:
  - dotnet: an enum is miscompiled in a package that is listed in `[project.packages]` before the
    package that declares it (reading a record field of that enum type is an
    `InvalidProgramException`, a `match` on it a `TypeLoadException`). `lyric.toml` lists the
    packages in dependency order, and no package compares or matches an enum of another package.
  - jvm: `impl FlagStore for X`, with the interface of another package named without its package,
    is compiled as the implementation of a JDK interface; a method returning `slice[FlagEntry]`
    then gets the descriptor `()[LFlags/FlagEntry;` and does not override the interface's
    `()[Ljava/lang/Object;` (an `AbstractMethodError` at the first `listFlags()` call). The
    implementation is written `impl Flags.FlagStore for FliptFlagStore`.
  - `Std.JsonValue.writeValue(JFloat(2.0))` writes `2`, which `parseValue` reads back as an
    integer. `Wire.attachmentOf` appends `.0` to a whole `Float` so that it stays a `FlagFloat`.
