//! Flags.Flipt.Admin - provisioning a Flipt namespace from `Flags.FlagEntry`
//! values, through Flipt's management API.
//!
//! For seeding a test or a development server, and for migrating flags that
//! lived in an `InProcessFlagStore`. The token needs write access to the
//! namespace. Rules, segments and rollouts are not managed here: use Flipt's
//! UI, its declarative files or its own API for those.
//!
//! How an entry is stored (the inverse of the mapping in `Flags.Flipt.Wire`):
//!
//!   * a `FlagBool` is a boolean flag whose `enabled` is the value;
//!   * a `FlagString`, `FlagInt` or `FlagFloat` is an enabled variant flag
//!     with one variant, key `value`, whose attachment is the value as JSON,
//!     set as the flag's default variant.
@runtime_checked
@io
package Flags.Flipt.Admin

import Std.Core
import Std.Collections
import Flags
import Flags.Flipt
import Flags.Flipt.Wire

// The key of the one variant `setFlag` creates.
val VALUE_VARIANT: String = "value"

/// Create the namespace `namespaceKey`. `INVALID_ARGUMENT` when it exists.
@experimental
pub func createNamespace(
  client: in FliptClient, namespaceKey: in String, description: in String
): Result[Unit, FliptError] {
  client.createNamespace(namespaceKey, description)
}

/// Delete every flag of `namespaceKey`, then the namespace. Flipt does not
/// delete a namespace that still has flags.
@experimental
pub func deleteNamespace(client: in FliptClient, namespaceKey: in String): Result[Unit, FliptError] {
  val flags = match client.listFlags(namespaceKey) { case Ok(f) -> f; case Err(e) -> return Err(e) }
  for f in flags {
    match client.deleteFlag(namespaceKey, f.key) {
      case Err(e) -> return Err(e)
      case Ok(_) -> ()
    }
  }
  client.deleteNamespace(namespaceKey)
}

/// Delete the flag `flagKey`; `Ok` when there was none.
@experimental
pub func deleteFlag(client: in FliptClient, namespaceKey: in String, flagKey: in String): Result[Unit, FliptError] {
  client.deleteFlag(namespaceKey, flagKey)
}

/// Store `entry` as a flag of `namespaceKey` (module documentation),
/// replacing a flag of the same name. Not atomic: the old flag, with its
/// rules and rollouts, is deleted first, then the new one is created in up
/// to three requests; a failure in between leaves the flag missing or
/// without its value. `INVALID_ARGUMENT` for a name that cannot be a Flipt
/// key and for a `FlagFloat` that is not finite.
@experimental
pub func setFlag(client: in FliptClient, namespaceKey: in String, entry: in FlagEntry): Result[Unit, FliptError] {
  match client.deleteFlag(namespaceKey, entry.name) {
    case Err(e) -> return Err(e)
    case Ok(_) -> ()
  }
  match entry.value {
    case FlagBool(enabled) -> client.createBooleanFlag(namespaceKey, entry.name, entry.description, enabled)
    case _ -> {
      val attachment = match attachmentOf(entry.value) {
        case Ok(a) -> a
        case Err(message) -> return Err(FliptError(code = "INVALID_ARGUMENT", message = "flag '" + entry.name + "': " + message, status = 0))
      }
      match client.createVariantFlag(namespaceKey, entry.name, entry.description, true) {
        case Err(e) -> return Err(e)
        case Ok(_) -> ()
      }
      val variant = match client.createVariant(namespaceKey, entry.name, VALUE_VARIANT, attachment) {
        case Ok(v) -> v
        case Err(e) -> return Err(e)
      }
      client.updateFlag(namespaceKey, entry.name, entry.description, true, variant.id)
    }
  }
}

/// `setFlag` for every entry, in order, stopping at the first failure.
@experimental
pub func seed(client: in FliptClient, namespaceKey: in String, entries: in slice[FlagEntry]): Result[Unit, FliptError] {
  for entry in entries {
    match setFlag(client, namespaceKey, entry) {
      case Err(e) -> return Err(e)
      case Ok(_) -> ()
    }
  }
  Ok(())
}
