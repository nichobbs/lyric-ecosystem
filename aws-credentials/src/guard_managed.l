//! Aws.Credentials.Guard, managed targets (`dotnet`, `jvm`) - one call of a
//! credential provider with a panic in it caught. The native twin
//! (`guard_native.l`, same package) has no unwinding to catch.
//!
//! Internal: only `Aws.Credentials` should import this.
@cfg(not(target = "native"))
package Aws.Credentials.Guard

import Std.Core

/// `body()`: `None` when it returned, the panic's message when it panicked.
pub func guardedCall(body: in () -> Unit): Option[String] {
  try {
    body()
    None
  } catch Bug as b {
    Some(value = b.message)
  }
}
