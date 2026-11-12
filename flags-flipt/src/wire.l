//! Flags.Flipt.Wire - the JSON of Flipt's v1 REST API and the mapping between
//! Flipt flags and `Flags.FlagValue`.
//!
//! Pure functions: request bodies are built and responses are read here, so
//! the codec is tested without a server. The shapes are those of Flipt
//! v1.62.0 (`openapi.yaml` of the release, checked against a running server).
//!
//! Type mapping. Flipt has two flag types:
//!
//!   * a boolean flag is a `FlagBool` of its `enabled` field (the value Flipt
//!     evaluates to when no rollout matches);
//!   * a variant flag has the value of one variant. Without targeting that is
//!     its default variant, whether the flag is enabled or not (disabling a
//!     variant flag switches its rules off, not its default); a variant flag
//!     without a default variant has no value.
//!
//! The value of a variant is its attachment (a JSON document) when that is a
//! scalar: a JSON string is a `FlagString`, an integer that fits `Int` a
//! `FlagInt`, any other number a `FlagFloat`, `true`/`false` a `FlagBool`. A
//! variant without an attachment, or with an object, array or `null`
//! attachment, is a `FlagString` of the variant key.
@runtime_checked
package Flags.Flipt.Wire

import Std.Core
import Std.Collections
import Std.JsonValue
import Std.Math as Math
import Std.String as Str
import Flags

/// Flipt's two flag types.
pub enum FlagKind {
  case Boolean
  case Variant
}

/// One variant of a variant flag.
pub record FliptVariant {
  /// The id Flipt assigned; `UpdateFlag` names the default variant by it.
  id: String
  key: String
  /// The attachment as Flipt returns it: JSON text, or empty.
  attachment: String
}

/// A flag as the management API lists it.
pub record FliptFlag {
  key: String
  name: String
  description: String
  enabled: Bool
  kind: FlagKind
  variants: List[FliptVariant]
  /// The default variant of a variant flag, when it has one.
  defaultVariant: Option[FliptVariant]
}

/// One page of `ListFlags`.
pub record FlagPage {
  flags: List[FliptFlag]
  /// Empty on the last page.
  nextPageToken: String
}

/// The answer of a boolean evaluation.
pub record BooleanResult {
  flagKey: String
  enabled: Bool
  /// `MATCH_EVALUATION_REASON` when a rollout decided,
  /// `DEFAULT_EVALUATION_REASON` when the flag's own value was used.
  reason: String
}

/// The answer of a variant evaluation.
pub record VariantResult {
  flagKey: String
  /// True when a rule matched the entity. The default variant is returned
  /// with `matched = false` and `DEFAULT_EVALUATION_REASON`.
  matched: Bool
  /// Empty when the evaluation produced no variant.
  variantKey: String
  /// The variant's attachment (JSON text), or empty.
  attachment: String
  reason: String
  segmentKeys: List[String]
}

/// One answer of a batch evaluation.
pub union Evaluation {
  case BooleanEvaluation(answer: BooleanResult)
  case VariantEvaluation(answer: VariantResult)
  /// The flag does not exist in the namespace.
  case FlagNotFound(flagKey: String)
}

/// One request of an evaluation: a flag, the entity it is evaluated for and
/// the context its segments' constraints are matched against.
pub record EvaluationInput {
  flagKey: String
  entityId: String
  context: Map[String, String]
}

/// The error document of a failed request: `{"code": <gRPC code>, "message": ...}`.
pub record Fault {
  code: Int
  message: String
}

// ─── Keys ────────────────────────────────────────────────────────────────────
/// True for a key Flipt accepts for a namespace or a flag: one or more of
/// letters, digits, `-`, `_` and `,`. Such a key needs no escaping in a URL
/// path.
@pure
pub func isValidKey(key: in String): Bool {
  if key.length == 0 {
    return false
  }
  var i = 0
  while i < key.length {
    val u = Str.codeUnitAt(key, i)
    val ok = (u >= 97 and u <= 122) or (u >= 65 and u <= 90) or (u >= 48 and u <= 57) or u == 45 or u == 95 or u == 44
    if not ok {
      return false
    }
    i = i + 1
  }
  true
}

// ─── Values ──────────────────────────────────────────────────────────────────
/// The `FlagValue` of a variant with key `variantKey` and attachment
/// `attachment` (module documentation).
@pure
pub func variantValue(variantKey: in String, attachment: in String): FlagValue {
  val text = Str.trim(attachment)
  if text.length == 0 {
    return FlagString(value = variantKey)
  }
  match parseValue(text) {
    case Err(_) -> FlagString(value = variantKey)
    case Ok(doc) -> match doc {
      case JString(s) -> FlagString(value = s)
      case JBool(b) -> FlagBool(value = b)
      case JInt(n) -> if n >= -2147483648i64 and n <= 2147483647i64 {
        FlagInt(value = n.toInt())
      } else {
        FlagFloat(value = n.toDouble().toFloat())
      }
      case JFloat(f) -> FlagFloat(value = f.toFloat())
      case _ -> FlagString(value = variantKey)
    }
  }
}

/// The value a store serves for `flag` without targeting, or `None` when the
/// flag has none (a variant flag without a default variant). This is what
/// Flipt evaluates to when no rule or rollout matches: a disabled variant
/// flag still answers its default variant.
@pure
pub func snapshotValue(flag: in FliptFlag): Option[FlagValue] {
  if flag.kind == FlagKind.Boolean {
    return Some(value = FlagBool(value = flag.enabled))
  }
  match flag.defaultVariant {
    case Some(v) -> Some(value = variantValue(v.key, v.attachment))
    case None -> None
  }
}

/// The value of an evaluation, or `None` when it produced none: the flag does
/// not exist, or a variant evaluation returned no variant (no rule matched,
/// or the flag is disabled, and there is no default variant).
@pure
pub func evaluationValue(e: in Evaluation): Option[FlagValue] {
  match e {
    case BooleanEvaluation(r) -> Some(value = FlagBool(value = r.enabled))
    case VariantEvaluation(r) -> if r.variantKey.length == 0 {
      None
    } else {
      Some(value = variantValue(r.variantKey, r.attachment))
    }
    case FlagNotFound(_) -> None
  }
}

/// The attachment that makes a variant carry `value`: the inverse of
/// `variantValue` for `FlagString`, `FlagInt` and `FlagFloat`. `Err` for a
/// `FlagBool` (a boolean is a boolean flag, not a variant) and for a
/// `FlagFloat` that is not finite (JSON has no such number).
@pure
pub func attachmentOf(value: in FlagValue): Result[String, String] {
  match value {
    case FlagString(s) -> Ok(writeValue(JString(value = s)))
    case FlagInt(n) -> Ok(writeValue(JInt(value = n.toLong())))
    case FlagFloat(f) -> {
      val d = f.toDouble()
      if not Math.isFinite(d) {
        return Err("a Flipt attachment cannot hold a Float that is not finite")
      }
      // The writer drops the fraction of a whole number, and `2` would be
      // read back as an integer.
      val written = writeValue(JFloat(value = d))
      if Str.contains(written, ".") or Str.contains(written, "E") or Str.contains(written, "e") {
        Ok(written)
      } else {
        Ok(written + ".0")
      }
    }
    case FlagBool(_) -> Err("a Bool value is a boolean flag, not a variant attachment")
  }
}

// ─── Requests ────────────────────────────────────────────────────────────────
func field(name: in String, value: in JsonValue): JsonField {
  JsonField(name = name, value = value)
}

func text(value: in String): JsonValue {
  JString(value = value)
}

func object(fields: in List[JsonField]): JsonValue {
  JObject(fields = fields)
}

func evaluationObject(namespaceKey: in String, input: in EvaluationInput): JsonValue {
  val context: List[JsonField] = newList()
  for e in mapEntries(input.context) {
    context.add(field(e.key, text(e.value)))
  }
  val fields: List[JsonField] = newList()
  fields.add(field("namespaceKey", text(namespaceKey)))
  fields.add(field("flagKey", text(input.flagKey)))
  fields.add(field("entityId", text(input.entityId)))
  fields.add(field("context", object(context)))
  object(fields)
}

/// The body of `POST /evaluate/v1/boolean` and `POST /evaluate/v1/variant`.
@pure
pub func evaluationRequest(namespaceKey: in String, input: in EvaluationInput): String {
  writeValue(evaluationObject(namespaceKey, input))
}

/// The body of `POST /evaluate/v1/batch`.
@pure
pub func batchRequest(namespaceKey: in String, inputs: in List[EvaluationInput]): String {
  val requests: List[JsonValue] = newList()
  for input in inputs {
    requests.add(evaluationObject(namespaceKey, input))
  }
  val fields: List[JsonField] = newList()
  fields.add(field("requests", JArray(items = requests)))
  writeValue(object(fields))
}

/// The body of `POST /api/v1/namespaces`.
@pure
pub func createNamespaceRequest(key: in String, name: in String, description: in String): String {
  val fields: List[JsonField] = newList()
  fields.add(field("key", text(key)))
  fields.add(field("name", text(name)))
  fields.add(field("description", text(description)))
  writeValue(object(fields))
}

func kindName(kind: in FlagKind): String {
  match kind {
    case Boolean -> "BOOLEAN_FLAG_TYPE"
    case Variant -> "VARIANT_FLAG_TYPE"
  }
}

/// The body of `POST /api/v1/namespaces/{namespace}/flags`.
@pure
pub func createFlagRequest(key: in String, description: in String, enabled: in Bool, kind: in FlagKind): String {
  val fields: List[JsonField] = newList()
  fields.add(field("key", text(key)))
  // Flipt refuses an empty display name; the key is the name.
  fields.add(field("name", text(key)))
  fields.add(field("description", text(description)))
  fields.add(field("enabled", JBool(value = enabled)))
  fields.add(field("type", text(kindName(kind))))
  writeValue(object(fields))
}

/// The body of `PUT /api/v1/namespaces/{namespace}/flags/{key}`. Flipt
/// replaces the whole flag: a field left out is reset, so the name, the
/// description, `enabled` and the default variant are always sent
/// (`defaultVariantId` empty for none).
@pure
pub func updateFlagRequest(
  name: in String, description: in String, enabled: in Bool, defaultVariantId: in String
): String {
  val fields: List[JsonField] = newList()
  fields.add(field("name", text(name)))
  fields.add(field("description", text(description)))
  fields.add(field("enabled", JBool(value = enabled)))
  if defaultVariantId.length > 0 {
    fields.add(field("defaultVariantId", text(defaultVariantId)))
  }
  writeValue(object(fields))
}

/// The body of `POST /api/v1/namespaces/{namespace}/flags/{key}/variants`.
@pure
pub func createVariantRequest(key: in String, attachment: in String): String {
  val fields: List[JsonField] = newList()
  fields.add(field("key", text(key)))
  if attachment.length > 0 {
    fields.add(field("attachment", text(attachment)))
  }
  writeValue(object(fields))
}

// ─── Responses ───────────────────────────────────────────────────────────────
func stringOr(doc: in JsonValue, name: in String, fallback: in String): String {
  match Std.JsonValue.getString(doc, name) {
    case Some(v) -> v
    case None -> fallback
  }
}

func boolOr(doc: in JsonValue, name: in String, fallback: in Bool): Bool {
  match Std.JsonValue.getBool(doc, name) {
    case Some(v) -> v
    case None -> fallback
  }
}

func isObject(doc: in JsonValue): Bool {
  match doc {
    case JObject(_) -> true
    case _ -> false
  }
}

/// Parse a response body that must be a JSON object.
@pure
pub func parseDocument(body: in String): Result[JsonValue, String] {
  match parseValue(body) {
    case Err(e) -> Err("the response is not JSON: " + JsonParseError.message(e))
    case Ok(doc) -> if isObject(doc) { Ok(doc) } else { Err("the response is not a JSON object") }
  }
}

/// The error document of a failed response, when the body is one.
@pure
pub func parseFault(body: in String): Option[Fault] {
  match parseValue(body) {
    case Err(_) -> None
    case Ok(doc) -> match Std.JsonValue.getInt(doc, "code") {
      case None -> None
      case Some(code) -> Some(
        value = Fault(code = code.toInt(), message = stringOr(doc, "message", ""))
      )
    }
  }
}

/// Read a `Variant` object. `Err` when it has no key.
@pure
pub func parseVariant(doc: in JsonValue): Result[FliptVariant, String] {
  match Std.JsonValue.getString(doc, "key") {
    case None -> Err("a variant has no key")
    case Some(key) -> Ok(
      FliptVariant(
        id = stringOr(doc, "id", ""),
        key = key,
        attachment = stringOr(doc, "attachment", "")
      )
    )
  }
}

/// Read a `Flag` object. `Err` when it has no key or a type this client does
/// not know.
@pure
pub func parseFlag(doc: in JsonValue): Result[FliptFlag, String] {
  val key = match Std.JsonValue.getString(doc, "key") {
    case Some(k) -> k
    case None -> return Err("a flag has no key")
  }
  val kindText = stringOr(doc, "type", "VARIANT_FLAG_TYPE")
  val kind = if kindText == "BOOLEAN_FLAG_TYPE" {
    FlagKind.Boolean
  } else if kindText == "VARIANT_FLAG_TYPE" {
    FlagKind.Variant
  } else {
    return Err("flag '" + key + "' has the unknown type " + kindText)
  }
  val variants: List[FliptVariant] = newList()
  match getArray(doc, "variants") {
    case None -> ()
    case Some(items) -> {
      for item in items {
        match parseVariant(item) {
          case Ok(v) -> variants.add(v)
          case Err(message) -> return Err("flag '" + key + "': " + message)
        }
      }
    }
  }
  var defaultVariant: Option[FliptVariant] = None
  match getField(doc, "defaultVariant") {
    case None -> ()
    case Some(d) -> {
      if isObject(d) {
        match parseVariant(d) {
          case Ok(v) -> {
            defaultVariant = Some(value = v)
          }
          case Err(message) -> return Err("flag '" + key + "': default " + message)
        }
      }
    }
  }
  Ok(
    FliptFlag(
      key = key,
      name = stringOr(doc, "name", key),
      description = stringOr(doc, "description", ""),
      enabled = boolOr(doc, "enabled", false),
      kind = kind,
      variants = variants,
      defaultVariant = defaultVariant
    )
  )
}

/// Read a `FlagList` document.
@pure
pub func parseFlagPage(doc: in JsonValue): Result[FlagPage, String] {
  val items = match getArray(doc, "flags") {
    case Some(a) -> a
    case None -> return Err("the flag list has no flags array")
  }
  val flags: List[FliptFlag] = newList()
  for item in items {
    match parseFlag(item) {
      case Ok(f) -> flags.add(f)
      case Err(message) -> return Err(message)
    }
  }
  Ok(FlagPage(flags = flags, nextPageToken = stringOr(doc, "nextPageToken", "")))
}

/// Read a `BooleanEvaluationResponse`.
@pure
pub func parseBooleanResult(doc: in JsonValue): Result[BooleanResult, String] {
  match Std.JsonValue.getBool(doc, "enabled") {
    case None -> Err("the boolean evaluation has no enabled field")
    case Some(enabled) -> Ok(
      BooleanResult(
        flagKey = stringOr(doc, "flagKey", ""),
        enabled = enabled,
        reason = stringOr(doc, "reason", "")
      )
    )
  }
}

/// Read a `VariantEvaluationResponse`.
@pure
pub func parseVariantResult(doc: in JsonValue): Result[VariantResult, String] {
  if not isObject(doc) {
    return Err("the variant evaluation is not an object")
  }
  val segments: List[String] = newList()
  match getArray(doc, "segmentKeys") {
    case None -> ()
    case Some(items) -> {
      for item in items {
        match asString(item) {
          case Some(s) -> segments.add(s)
          case None -> ()
        }
      }
    }
  }
  Ok(
    VariantResult(
      flagKey = stringOr(doc, "flagKey", ""),
      matched = boolOr(doc, "match", false),
      variantKey = stringOr(doc, "variantKey", ""),
      attachment = stringOr(doc, "variantAttachment", ""),
      reason = stringOr(doc, "reason", ""),
      segmentKeys = segments
    )
  )
}

func parseEvaluation(doc: in JsonValue): Result[Evaluation, String] {
  val kind = stringOr(doc, "type", "")
  if kind == "BOOLEAN_EVALUATION_RESPONSE_TYPE" {
    match getField(doc, "booleanResponse") {
      case None -> return Err("a boolean batch answer has no booleanResponse")
      case Some(inner) -> match parseBooleanResult(inner) {
        case Ok(r) -> return Ok(BooleanEvaluation(answer = r))
        case Err(message) -> return Err(message)
      }
    }
  }
  if kind == "VARIANT_EVALUATION_RESPONSE_TYPE" {
    match getField(doc, "variantResponse") {
      case None -> return Err("a variant batch answer has no variantResponse")
      case Some(inner) -> match parseVariantResult(inner) {
        case Ok(r) -> return Ok(VariantEvaluation(answer = r))
        case Err(message) -> return Err(message)
      }
    }
  }
  if kind == "ERROR_EVALUATION_RESPONSE_TYPE" {
    match getField(doc, "errorResponse") {
      case None -> return Err("an error batch answer has no errorResponse")
      case Some(inner) -> {
        val reason = stringOr(inner, "reason", "")
        if reason == "NOT_FOUND_ERROR_EVALUATION_REASON" {
          return Ok(FlagNotFound(flagKey = stringOr(inner, "flagKey", "")))
        }
        return Err("flag '" + stringOr(inner, "flagKey", "") + "' could not be evaluated: " + reason)
      }
    }
  }
  Err("a batch answer has the unknown type '" + kind + "'")
}

/// Read a `BatchEvaluationResponse`: one `Evaluation` per request, in request
/// order. `Err` when the answer count is not `expected`.
@pure
pub func parseBatchResponse(doc: in JsonValue, expected: in Int): Result[List[Evaluation], String] {
  val items = match getArray(doc, "responses") {
    case Some(a) -> a
    case None -> return Err("the batch evaluation has no responses array")
  }
  if items.count != expected {
    return Err("the batch evaluation answered " + items.count.toString() + " of " + expected.toString() + " requests")
  }
  val answers: List[Evaluation] = newList()
  for item in items {
    match parseEvaluation(item) {
      case Ok(e) -> answers.add(e)
      case Err(message) -> return Err(message)
    }
  }
  Ok(answers)
}
