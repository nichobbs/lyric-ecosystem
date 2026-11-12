//! tests/integration/flipt_integration_tests.l - the backend against a real
//! Flipt server (v1.62.0 in CI, started by `scripts/ci/start-flipt.sh`).
//!
//! Compiled only with `--features integration`, and configured from the
//! environment:
//!
//!   LYRIC_FLIPT_URL     base URL of the server, e.g. http://127.0.0.1:19930
//!   LYRIC_FLIPT_TOKEN   a client token allowed to manage namespaces and
//!                       flags; leave it unset for a server without
//!                       authentication
//!
//!   LYRIC_FLIPT_URL=http://127.0.0.1:19930 LYRIC_FLIPT_TOKEN=... \
//!     lyric test --manifest flags-flipt/lyric.toml --target dotnet --features integration
//!
//! Every test creates namespaces named `lyric-it-<time>-<n>` and deletes them
//! when it is done; nothing else on the server is touched.
@cfg(feature = "integration")
@test_module
package Flags.Flipt.IntegrationTests

import Std.Core
import Std.Collections
import Std.Environment as Env
import Std.Http
import Std.JsonValue as Json
import Std.Testing
import Std.Time as Time
import Testing.Conformance
import Testing.Conformance.Flags as FlagsConformance
import Flags
import Flags.Flipt
import Flags.Flipt.Admin
import Flags.Flipt.Extras
import Flags.Flipt.Wire

func liveConfig(): FliptConfig {
  val url = Env.getVarOrDefault("LYRIC_FLIPT_URL", "")
  if url.length == 0 {
    panic(
      "LYRIC_FLIPT_URL is not set: the Flipt integration tests need the base URL of a Flipt server, and " +
        "LYRIC_FLIPT_TOKEN when it requires authentication (see tests/integration/flipt_integration_tests.l)"
    )
  }
  config(url, "default").withToken(Env.getVarOrDefault("LYRIC_FLIPT_TOKEN", "")).withRequestTimeout(15000)
}

func liveClient(): FliptClient {
  match openClient(liveConfig()) {
    case Ok(c) -> c
    case Err(e) -> panic("cannot open the Flipt client: " + e.code + " " + e.message)
  }
}

ref record Namespaces {
  prefix: String
  var next: Int
  var created: List[String]
}

func newNamespaces(): Namespaces {
  Namespaces(prefix = "lyric-it-" + Time.nowEpochMillis().toString() + "-", next = 0, created = newList())
}

// A new, empty namespace.
func fresh(spaces: in Namespaces): String {
  spaces.next = spaces.next + 1
  val key = spaces.prefix + spaces.next.toString()
  match createNamespace(liveClient(), key, "lyric-ecosystem integration test") {
    case Ok(_) -> ()
    case Err(e) -> panic("cannot create namespace " + key + ": " + e.code + " " + e.message)
  }
  spaces.created.add(key)
  key
}

func cleanUp(spaces: in Namespaces): Unit {
  for key in spaces.created {
    match deleteNamespace(liveClient(), key) {
      case Ok(_) -> ()
      case Err(e) -> assertTrue(false, "cannot delete namespace " + key + ": " + e.code + " " + e.message)
    }
  }
}

func must(what: in String, r: in Result[Unit, FliptError]): Unit {
  match r {
    case Ok(_) -> ()
    case Err(e) -> panic(what + ": " + e.code + " " + e.message)
  }
}

func connected(cfg: in FliptConfig): FliptFlagStore {
  match connect(cfg) {
    case Ok(s) -> s
    case Err(e) -> panic("connect: " + e.code + " " + e.message)
  }
}

// A store on a new namespace that holds `entries`.
func seeded(spaces: in Namespaces, entries: in slice[FlagEntry]): FlagStore {
  val key = fresh(spaces)
  must("seed", seed(liveClient(), key, entries))
  connected(liveConfig().withNamespace(key))
}

func seededEvaluating(spaces: in Namespaces, entries: in slice[FlagEntry]): FlagStore {
  val key = fresh(spaces)
  must("seed", seed(liveClient(), key, entries))
  val none: Map[String, String] = newMap()
  connected(liveConfig().withNamespace(key).withEvaluation("conformance", none))
}

// One management request the library has no function for (segments, rules,
// rollouts): the response document.
func manage(method: in HttpMethod, path: in String, body: in String): Json.JsonValue {
  match liveClient().call("test setup " + path, method, path, body) {
    case Ok(doc) -> doc
    case Err(e) -> panic("test setup " + path + ": " + e.code + " " + e.message)
  }
}

func idOf(doc: in Json.JsonValue): String {
  match Json.getString(doc, "id") {
    case Some(id) -> id
    case None -> panic("the created object has no id")
  }
}

func describe(v: in Option[FlagValue]): String {
  match v {
    case None -> "none"
    case Some(fv) -> match fv {
      case FlagBool(b) -> "bool:" + b.toString()
      case FlagString(s) -> "string:" + s
      case FlagInt(n) -> "int:" + n.toString()
      case FlagFloat(f) -> "float:" + f.toDouble().toString()
    }
  }
}

func codeOf[T](r: in Result[T, FliptError]): String {
  match r {
    case Ok(_) -> "ok"
    case Err(e) -> e.code
  }
}

test "FliptFlagStore conforms to FlagStore against a real Flipt server" {
  val spaces = newNamespaces()
  val checks = FlagsConformance.run({ entries -> seeded(spaces, entries) })
  cleanUp(spaces)
  requireConformance(checks)
  assertEqualInt(caseCount(checks), 8, "every conformance case ran")
  assertEqualInt(spaces.created.count, 8, "one namespace per case")
}

test "FliptFlagStore in Evaluate mode conforms to FlagStore against a real Flipt server" {
  val spaces = newNamespaces()
  val checks = FlagsConformance.run({ entries -> seededEvaluating(spaces, entries) })
  cleanUp(spaces)
  requireConformance(checks)
  assertEqualInt(caseCount(checks), 8, "every conformance case ran")
}

test "connect reports a missing namespace, a wrong token and a server that is not there" {
  assertEqual(codeOf(connect(liveConfig().withNamespace("lyric-it-no-such-namespace"))), "NOT_FOUND", "namespace")
  if Env.getVarOrDefault("LYRIC_FLIPT_TOKEN", "").length > 0 {
    assertEqual(codeOf(connect(liveConfig().withToken("not-the-token"))), "UNAUTHENTICATED", "wrong token")
    assertEqual(codeOf(connect(liveConfig().withToken(""))), "UNAUTHENTICATED", "no token")
  }
  assertEqual(
    codeOf(connect(config("http://127.0.0.1:1", "default").withRequestTimeout(3000))),
    "CONNECTION_FAILED",
    "nothing listens there"
  )
}

test "Flipt's flag types map to FlagValue as documented" {
  val spaces = newNamespaces()
  val key = fresh(spaces)
  val client = liveClient()
  must(
    "seed",
    seed(
      client,
      key,
      [
        makeFlagWithDesc("on", true, "a boolean"),
        makeStringFlag("text", "héllo \"quoted\" \\ wörld"),
        makeIntFlag("negative", -2147483648),
        makeFloatFlag("whole", 2.0),
        makeFloatFlag("tenth", 0.1)
      ]
    )
  )
  // Flags as an operator makes them in Flipt's UI.
  must("plain", client.createVariantFlag(key, "plain", "", true))
  val plain = match client.createVariant(key, "plain", "blue", "") { case Ok(v) -> v; case Err(e) -> panic(e.message) }
  must("plain default", client.updateFlag(key, "plain", "", true, plain.id))
  must("shaped", client.createVariantFlag(key, "shaped", "", true))
  val shaped = match client.createVariant(key, "shaped", "big", "{\"size\": 3}") {
    case Ok(v) -> v
    case Err(e) -> panic(e.message)
  }
  must("shaped default", client.updateFlag(key, "shaped", "", true, shaped.id))
  must("truthy", client.createVariantFlag(key, "truthy", "", true))
  val truthy = match client.createVariant(key, "truthy", "yes", "true") {
    case Ok(v) -> v
    case Err(e) -> panic(e.message)
  }
  must("truthy default", client.updateFlag(key, "truthy", "", true, truthy.id))
  must("wide", client.createVariantFlag(key, "wide", "", true))
  val wide = match client.createVariant(key, "wide", "n", "3000000000") {
    case Ok(v) -> v
    case Err(e) -> panic(e.message)
  }
  must("wide default", client.updateFlag(key, "wide", "", true, wide.id))
  must("no default", client.createVariantFlag(key, "undecided", "", true))
  match client.createVariant(key, "undecided", "a", "") {
    case Ok(_) -> ()
    case Err(e) -> panic(e.message)
  }
  must("disabled", client.createVariantFlag(key, "parked", "", true))
  val parked = match client.createVariant(key, "parked", "a", "\"x\"") {
    case Ok(v) -> v
    case Err(e) -> panic(e.message)
  }
  must("disabled default", client.updateFlag(key, "parked", "", false, parked.id))

  val store = connected(liveConfig().withNamespace(key))
  assertEqual(describe(store.getValue("on")), "bool:true", "boolean flag")
  assertEqual(describe(store.getValue("text")), "string:héllo \"quoted\" \\ wörld", "string attachment")
  assertEqual(describe(store.getValue("negative")), "int:-2147483648", "integer attachment")
  assertEqual(describe(store.getValue("whole")), "float:2", "a Float stays a Float")
  assertTrue(getFloat(store, "tenth", 0.0) == 0.1, "a Float survives exactly")
  assertEqual(describe(store.getValue("plain")), "string:blue", "no attachment: the variant key")
  assertEqual(describe(store.getValue("shaped")), "string:big", "object attachment: the variant key")
  assertEqual(describe(store.getValue("truthy")), "bool:true", "boolean attachment")
  assertTrue(store.isEnabled("truthy"), "and it is enabled")
  assertEqual(describe(store.getValue("wide")), "float:3000000000", "an integer beyond Int is a Float")
  assertEqual(describe(store.getValue("undecided")), "none", "no default variant: no value")
  assertEqual(describe(store.getValue("parked")), "string:x", "a disabled variant flag keeps its default variant")
  assertEqual(describe(store.getValue("not.a.key")), "none", "a name Flipt cannot hold")
  assertEqualInt(store.listFlags().length, 10, "a flag without a value is not listed")
  assertEqualInt(getInt(store, "text", 7), 7, "a type mismatch is the default")
  assertEqualInt(errorCount(store), 0, "nothing failed")

  // The same through evaluation.
  val none: Map[String, String] = newMap()
  val evaluating = connected(liveConfig().withNamespace(key).withEvaluation("someone", none))
  assertEqual(describe(evaluating.getValue("text")), "string:héllo \"quoted\" \\ wörld", "evaluated string")
  assertEqual(describe(evaluating.getValue("negative")), "int:-2147483648", "evaluated integer")
  assertEqual(describe(evaluating.getValue("undecided")), "none", "evaluated: no variant")
  assertEqual(describe(evaluating.getValue("parked")), "string:x", "evaluated: disabled, the default variant")
  assertEqual(describe(evaluating.getValue("absent")), "none", "evaluated: no such flag")
  assertEqualInt(evaluating.listFlags().length, 10, "evaluated listing")
  assertEqualInt(errorCount(evaluating), 0, "nothing failed")
  cleanUp(spaces)
}

test "a snapshot follows the server at the refresh interval, on refresh(), and pages through a long listing" {
  val spaces = newNamespaces()
  val key = fresh(spaces)
  val client = liveClient()
  must(
    "seed",
    seed(
      client,
      key,
      [makeFlag("a", true), makeFlag("b", false), makeIntFlag("c", 1), makeStringFlag("d", "x"), makeFlag("e", true)]
    )
  )
  val slow = connected(liveConfig().withNamespace(key).withRefreshInterval(3600000).withPageSize(2))
  val eager = connected(liveConfig().withNamespace(key).withRefreshInterval(0))
  assertEqualInt(slow.listFlags().length, 5, "three pages of two")
  assertEqualInt(refreshCount(slow), 1, "one refresh so far")
  must("flip", setFlag(client, key, makeFlag("b", true)))
  must("add", setFlag(client, key, makeIntFlag("f", 6)))
  must("remove", deleteFlag(client, key, "a"))
  assertTrue(not slow.isEnabled("b"), "the snapshot is not refreshed before its interval")
  assertTrue(slow.isEnabled("a"), "a deleted flag is still in the old snapshot")
  assertEqualInt(refreshCount(slow), 1, "reads within the interval make no request")
  assertTrue(eager.isEnabled("b"), "interval 0 reads the server on every read")
  assertTrue(not eager.isEnabled("a"), "the deleted flag is gone")
  match slow.refresh() {
    case Ok(_) -> ()
    case Err(e) -> assertTrue(false, "refresh: " + e.message)
  }
  assertTrue(slow.isEnabled("b"), "refresh() reads the server")
  assertTrue(not slow.isEnabled("a"), "and drops what was deleted")
  assertEqualInt(getInt(slow, "f", 0), 6, "and sees what was added")
  assertEqualInt(refreshCount(slow), 2, "one more refresh")
  cleanUp(spaces)
}

test "rules, segments and rollouts decide in Extras and in Evaluate mode, not in a snapshot" {
  val spaces = newNamespaces()
  val key = fresh(spaces)
  val client = liveClient()
  val base = "/api/v1/namespaces/" + key
  val _ = manage(
    POST,
    base + "/segments",
    "{\"key\":\"enterprise\",\"name\":\"enterprise\",\"matchType\":\"ALL_MATCH_TYPE\"}"
  )
  val _ = manage(
    POST,
    base + "/segments/enterprise/constraints",
    "{\"type\":\"STRING_COMPARISON_TYPE\",\"property\":\"plan\",\"operator\":\"eq\",\"value\":\"enterprise\"}"
  )
  must("beta", client.createBooleanFlag(key, "beta", "", false))
  val _ = manage(
    POST,
    base + "/flags/beta/rollouts",
    "{\"rank\":1,\"segment\":{\"segmentKeys\":[\"enterprise\"],\"value\":true}}"
  )
  must("theme", client.createVariantFlag(key, "theme", "", true))
  val dark = match client.createVariant(key, "theme", "dark", "\"dark\"") {
    case Ok(v) -> v
    case Err(e) -> panic(e.message)
  }
  val light = match client.createVariant(key, "theme", "light", "\"light\"") {
    case Ok(v) -> v
    case Err(e) -> panic(e.message)
  }
  must("theme default", client.updateFlag(key, "theme", "", true, light.id))
  val rule = idOf(manage(POST, base + "/flags/theme/rules", "{\"rank\":1,\"segmentKeys\":[\"enterprise\"]}"))
  val _ = manage(
    POST,
    base + "/flags/theme/rules/" + rule + "/distributions",
    "{\"variantId\":\"" + dark.id + "\",\"rollout\":100}"
  )

  val enterprise: Map[String, String] = newMap()
  enterprise["plan"] = "enterprise"
  val free: Map[String, String] = newMap()
  free["plan"] = "free"

  val store = connected(liveConfig().withNamespace(key))
  assertTrue(not store.isEnabled("beta"), "a snapshot has the flag's own value")
  assertEqual(describe(store.getValue("theme")), "string:light", "a snapshot has the default variant")

  match evaluateBoolean(store, "beta", "user-1", enterprise) {
    case Ok(r) -> {
      assertTrue(r.enabled, "the rollout matches the segment")
      assertEqual(r.reason, "MATCH_EVALUATION_REASON", "reason")
    }
    case Err(e) -> assertTrue(false, "evaluateBoolean: " + e.message)
  }
  match evaluateBoolean(store, "beta", "user-2", free) {
    case Ok(r) -> {
      assertTrue(not r.enabled, "no rollout matches")
      assertEqual(r.reason, "DEFAULT_EVALUATION_REASON", "reason")
    }
    case Err(e) -> assertTrue(false, "evaluateBoolean: " + e.message)
  }
  match evaluateVariant(store, "theme", "user-1", enterprise) {
    case Ok(r) -> {
      assertTrue(r.matched, "the rule matches")
      assertEqual(r.variantKey, "dark", "variant")
      assertEqual(r.attachment, "\"dark\"", "attachment")
      assertEqualInt(r.segmentKeys.count, 1, "segments")
    }
    case Err(e) -> assertTrue(false, "evaluateVariant: " + e.message)
  }
  match evaluateVariant(store, "theme", "user-2", free) {
    case Ok(r) -> {
      assertTrue(not r.matched, "no rule matches")
      assertEqual(r.variantKey, "light", "the default variant")
    }
    case Err(e) -> assertTrue(false, "evaluateVariant: " + e.message)
  }
  assertEqual(codeOf(evaluateBoolean(store, "theme", "u", free)), "INVALID_ARGUMENT", "a variant flag is not boolean")
  assertEqual(codeOf(evaluateVariant(store, "beta", "u", free)), "INVALID_ARGUMENT", "a boolean flag has no variant")
  assertEqual(codeOf(evaluateBoolean(store, "absent", "u", free)), "NOT_FOUND", "no such flag")
  match valueFor(store, "theme", "user-1", enterprise) {
    case Ok(v) -> assertEqual(describe(v), "string:dark", "valueFor")
    case Err(e) -> assertTrue(false, "valueFor: " + e.message)
  }
  val inputs: List[EvaluationInput] = newList()
  inputs.add(input("beta", "user-1", enterprise))
  inputs.add(input("absent", "user-1", enterprise))
  inputs.add(input("theme", "user-2", free))
  match evaluateBatch(store, inputs) {
    case Err(e) -> assertTrue(false, "evaluateBatch: " + e.message)
    case Ok(answers) -> {
      assertEqualInt(answers.count, 3, "one answer per request")
      assertEqual(describe(evaluationValue(answers[0])), "bool:true", "batch 0")
      assertEqual(describe(evaluationValue(answers[1])), "none", "batch 1")
      assertEqual(describe(evaluationValue(answers[2])), "string:light", "batch 2")
    }
  }

  val targeted = connected(liveConfig().withNamespace(key).withEvaluation("user-1", enterprise))
  assertTrue(targeted.isEnabled("beta"), "Evaluate mode: the rollout decides")
  assertEqual(getString(targeted, "theme", "?"), "dark", "Evaluate mode: the rule decides")
  var sawDark = false
  for f in targeted.listFlags() {
    if f.name == "theme" {
      sawDark = describe(Some(value = f.value)) == "string:dark"
    }
  }
  assertTrue(sawDark, "Evaluate mode: the listing is evaluated for the entity")
  cleanUp(spaces)
}
