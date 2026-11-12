//! tests/reexport_tests.l - an application that imports only `Flags.Flipt`
//! can name the interface types and the client's configuration, because the
//! backend re-exports `Flags` and `Flags.Flipt.Client`.
@test_module
package Flags.Flipt.ReexportTests

import Std.Core
import Std.Testing
import Flags.Flipt

func describe(store: in FlagStore, entry: in FlagEntry): String {
  entry.name + "/" + store.listFlags().length.toString()
}

test "FlagStore, FlagEntry, FlagValue and the accessors are visible through the backend" {
  val entry = makeFlagWithDesc("checkout", true, "d")
  match entry.value {
    case FlagBool(b) -> assertTrue(b, "FlagValue")
    case _ -> assertTrue(false, "a Bool entry")
  }
  val cfg = config("http://127.0.0.1:1", "default").withRequestTimeout(1000).withRefreshInterval(3600000)
  assertEqualInt(cfg.pageSize, 100, "the backend's own names")
  match open(cfg) {
    case Err(e) -> assertTrue(false, "open: " + e.message)
    case Ok(store) -> {
      assertEqual(describe(store, entry), "checkout/0", "a FliptFlagStore is a FlagStore")
      assertEqualInt(getInt(store, "limit", 7), 7, "the interface's accessors")
      val failed: Result[Unit, FlagError] = refresh(store)
      match failed {
        case Ok(_) -> assertTrue(false, "nothing listens there")
        case Err(e) -> assertEqual(e.code, "CONNECTION_FAILED", "FlagError carries the backend's code")
      }
    }
  }
}
