//! Flags.Flipt.Extras - what Flipt can do that `Flags.FlagStore` cannot ask
//! for: evaluating a flag for a given entity and context, so that the rules,
//! segments and rollouts configured in Flipt decide.
//!
//!   import Flags.Flipt as Flipt
//!   import Flags.Flipt.Extras as FliptExtras
//!
//!   val ctx: Map[String, String] = newMap()
//!   ctx["plan"] = "enterprise"
//!   match FliptExtras.evaluateBoolean(store, "new-checkout", user.id, ctx) {
//!     case Ok(r) -> r.enabled
//!     case Err(e) -> false
//!   }
//!
//! Every function here is one request to Flipt and returns its failure; there
//! is no cache and no fallback. They work on a store in either read mode and
//! do not touch its snapshot.
@runtime_checked
@io
package Flags.Flipt.Extras

import Std.Core
import Std.Collections
import Flags
import Flags.Flipt
import Flags.Flipt.Wire

/// An evaluation request for `flagKey`, `entityId` and `context`.
@experimental
pub func input(flagKey: in String, entityId: in String, context: in Map[String, String]): EvaluationInput {
  EvaluationInput(flagKey = flagKey, entityId = entityId, context = context)
}

/// Evaluate the boolean flag `flagKey` for `entityId` with `context`
/// (`POST /evaluate/v1/boolean`). `NOT_FOUND` for a flag that does not exist,
/// `INVALID_ARGUMENT` for a variant flag.
@experimental
pub func evaluateBoolean(
  store: in FliptFlagStore,
  flagKey: in String,
  entityId: in String,
  context: in Map[String, String],
): Result[BooleanResult, FliptError] {
  store.client.evaluateBoolean(store.namespace, input(flagKey, entityId, context))
}

/// Evaluate the variant flag `flagKey` for `entityId` with `context`
/// (`POST /evaluate/v1/variant`). The result's `variantKey` is empty when no
/// variant applies. `NOT_FOUND` for a flag that does not exist,
/// `INVALID_ARGUMENT` for a boolean flag.
@experimental
pub func evaluateVariant(
  store: in FliptFlagStore,
  flagKey: in String,
  entityId: in String,
  context: in Map[String, String],
): Result[VariantResult, FliptError] {
  store.client.evaluateVariant(store.namespace, input(flagKey, entityId, context))
}

/// Evaluate several flags of either type in one request
/// (`POST /evaluate/v1/batch`). The answers are in request order; a flag that
/// does not exist is a `FlagNotFound`.
@experimental
pub func evaluateBatch(store: in FliptFlagStore, inputs: in List[EvaluationInput]): Result[List[Evaluation], FliptError] {
  store.client.evaluateBatch(store.namespace, inputs)
}

/// The `FlagValue` of `flagKey` for `entityId` with `context`, whatever the
/// flag's type: one batch request. `Ok(None)` for a flag that does not exist
/// or evaluates to no variant.
@experimental
pub func valueFor(
  store: in FliptFlagStore, flagKey: in String, entityId: in String, context: in Map[String, String]
): Result[Option[FlagValue], FliptError] {
  val inputs: List[EvaluationInput] = newList()
  inputs.add(input(flagKey, entityId, context))
  match store.client.evaluateBatch(store.namespace, inputs) {
    case Err(e) -> Err(e)
    case Ok(answers) -> Ok(evaluationValue(answers[0]))
  }
}
