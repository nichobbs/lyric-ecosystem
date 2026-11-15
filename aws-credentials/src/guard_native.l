//! Aws.Credentials.Guard on `--target native`, the twin of `guard_managed.l`
//! in the same package.
//!
//! Native has no unwinding (D-N-003): a panic in a provider ends the process,
//! so the provider is called directly.
//!
//! Internal: only `Aws.Credentials` should import this.
@cfg(target = "native")
package Aws.Credentials.Guard

import Std.Core

/// `body()`. A panic in it ends the process on this target.
pub func guardedCall(body: in () -> Unit): Option[String] {
  body()
  None
}
