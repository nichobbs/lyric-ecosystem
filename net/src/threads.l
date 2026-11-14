//! Net.Threads - a thread of its own for a protocol client's background work:
//! sending heart-beats on an idle connection, noticing a silent peer,
//! reconnecting after a loss.
//!
//! The thread is the kernel's (`Net.Tcp.Kernel.startBackground`): a dedicated
//! operating-system thread on every target, never the runtime's shared pool,
//! which a blocked socket read can starve on dotnet. There is no cancellation:
//! the work decides when to return, usually by checking a flag of its client
//! that `close()` sets.
@runtime_checked
@io
package Net.Threads

import Std.Core
import Net.Tcp.Kernel as Kernel

/// A started thread.
pub record Thread {
  inner: Kernel.Background
}

/// Run `work` on a thread of its own. The thread ends when `work` returns.
pub func start(work: in () -> Unit): Thread {
  Thread(inner = Kernel.startBackground(work))
}

/// Block until the thread has finished.
pub func join(t: in Thread): Unit {
  Kernel.waitBackground(t.inner)
}
