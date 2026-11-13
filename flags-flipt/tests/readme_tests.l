//! tests/readme_tests.l - the usage examples of README.md, verbatim, so the
//! README cannot drift from what compiles.
@test_module
package Flags.Flipt.ReadmeTests

import Std.Core
import Std.Collections
import Std.Testing
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

func themeFor(store: in Flipt.FliptFlagStore, userId: in String, plan: in String): String {
  val context: Map[String, String] = newMap()
  context["plan"] = plan
  match FliptExtras.evaluateVariant(store, "theme", userId, context) {
    case Ok(result) -> if result.variantKey.length > 0 { result.variantKey } else { "light" }
    case Err(_) -> "light"
  }
}

func storeForUser(cfg: in Flipt.FliptConfig, userId: in String, plan: in String): Result[Flags.FlagStore, String] {
  val context: Map[String, String] = newMap()
  context["plan"] = plan
  match Flipt.connect(cfg.withEvaluation(userId, context).withMaxStale(300000)) {
    case Ok(store) -> Ok(store)
    case Err(e) -> Err(e.text())
  }
}

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

test "the README examples compile and fail cleanly without a server" {
  val cfg = Flipt.config("http://127.0.0.1:1", "default").withRequestTimeout(2000)
  assertTrue(isErr(pageSize("fixture-token")), "the example host does not resolve to a Flipt server")
  assertTrue(isErr(provision(cfg)), "nothing listens there")
  assertTrue(isErr(storeForUser(cfg, "user-1", "pro")), "nothing listens there")
  match Flipt.open(cfg) {
    case Err(e) -> assertTrue(false, e.message)
    case Ok(store) -> {
      assertFalse(killSwitch(store), "no value was ever read: false")
      assertEqual(themeFor(store, "user-1", "pro"), "light", "the fallback theme")
      assertTrue(Flipt.errorCount(store) >= 1, "the failed refresh was counted")
    }
  }
}
