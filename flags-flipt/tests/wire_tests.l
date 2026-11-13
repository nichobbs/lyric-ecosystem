//! tests/wire_tests.l - the JSON codec and the Flipt-to-FlagValue mapping,
//! without a server. The response documents are those of Flipt v1.62.0.
@test_module
package Flags.Flipt.WireTests

import Std.Core
import Std.Collections
import Std.JsonValue as Json
import Std.Testing
import Flags
import Flags.Flipt.Wire

func show(v: in FlagValue): String {
  match v {
    case FlagBool(b) -> "bool:" + b.toString()
    case FlagString(s) -> "string:" + s
    case FlagInt(n) -> "int:" + n.toString()
    case FlagFloat(f) -> "float:" + f.toDouble().toString()
  }
}

func showOption(v: in Option[FlagValue]): String {
  match v {
    case Some(fv) -> show(fv)
    case None -> "none"
  }
}

func document(text: in String): Json.JsonValue {
  match parseDocument(text) {
    case Ok(doc) -> doc
    case Err(message) -> panic("not a document: " + message)
  }
}

func flagOf(text: in String): FliptFlag {
  match parseFlag(document(text)) {
    case Ok(flag) -> flag
    case Err(message) -> panic("not a flag: " + message)
  }
}

func errorOf[T](r: in Result[T, String]): String {
  match r {
    case Ok(_) -> "ok"
    case Err(message) -> message
  }
}

test "a key is letters, digits, '-', '_' and ','" {
  assertTrue(isValidKey("new-checkout_2,x"), "every allowed character")
  assertTrue(isValidKey("A"), "one character")
  assertFalse(isValidKey(""), "empty")
  assertFalse(isValidKey("a.b"), "dot")
  assertFalse(isValidKey("a b"), "space")
  assertFalse(isValidKey("a/b"), "slash")
  assertFalse(isValidKey("../x"), "path traversal")
  assertFalse(isValidKey("a?b=c"), "query")
  assertFalse(isValidKey("café"), "outside ASCII")
}

test "a variant's value is its scalar attachment, else its key" {
  assertEqual(show(variantValue("k", "\"spring\"")), "string:spring", "JSON string")
  assertEqual(show(variantValue("k", "\"\"")), "string:", "empty JSON string")
  assertEqual(show(variantValue("k", "\"a \\\"b\\\" \\u00e9\"")), "string:a \"b\" é", "escapes")
  assertEqual(show(variantValue("k", "25")), "int:25", "integer")
  assertEqual(show(variantValue("k", "-2147483648")), "int:-2147483648", "smallest Int")
  assertEqual(show(variantValue("k", "2147483647")), "int:2147483647", "largest Int")
  assertEqual(show(variantValue("k", "2147483648")), "float:2147483648", "beyond Int")
  assertEqual(show(variantValue("k", "0.5")), "float:0.5", "fraction")
  assertEqual(show(variantValue("k", "2.0")), "float:2", "a whole number written as a fraction")
  assertEqual(show(variantValue("k", "1e3")), "float:1000", "exponent")
  assertEqual(show(variantValue("k", "true")), "bool:true", "true")
  assertEqual(show(variantValue("k", "false")), "bool:false", "false")
  assertEqual(show(variantValue("k", " 25 ")), "int:25", "Flipt lists the attachment as it was stored")
  assertEqual(show(variantValue("k", "")), "string:k", "no attachment")
  assertEqual(show(variantValue("k", "  ")), "string:k", "blank attachment")
  assertEqual(show(variantValue("k", "{\"a\": 1}")), "string:k", "object")
  assertEqual(show(variantValue("k", "[1, 2]")), "string:k", "array")
  assertEqual(show(variantValue("k", "null")), "string:k", "null")
  assertEqual(show(variantValue("k", "{nope")), "string:k", "not JSON")
}

test "attachmentOf is the inverse of variantValue for String, Int and Float" {
  val values: List[FlagValue] = newList()
  values.add(FlagString(value = "spring"))
  values.add(FlagString(value = ""))
  values.add(FlagString(value = "25"))
  values.add(FlagString(value = "true"))
  values.add(FlagString(value = "line\nbreak \"quoted\" \\ é世"))
  values.add(FlagInt(value = 0))
  values.add(FlagInt(value = -2147483648))
  values.add(FlagInt(value = 2147483647))
  values.add(FlagFloat(value = 0.5))
  values.add(FlagFloat(value = 2.0))
  values.add(FlagFloat(value = -0.1))
  values.add(FlagFloat(value = 1.0e30))
  values.add(FlagFloat(value = 16777216.0))
  for v in values {
    match attachmentOf(v) {
      case Err(message) -> assertTrue(false, show(v) + ": " + message)
      case Ok(attachment) -> assertEqual(show(variantValue("value", attachment)), show(v), "through " + attachment)
    }
  }
  assertEqual(errorOf(attachmentOf(FlagString(value = "x"))), "ok", "a String has an attachment")
  match attachmentOf(FlagFloat(value = 2.0)) {
    case Ok(a) -> assertEqual(a, "2.0", "a whole Float keeps its fraction")
    case Err(message) -> assertTrue(false, message)
  }
  assertEqual(
    errorOf(attachmentOf(FlagBool(value = true))),
    "a Bool value is a boolean flag, not a variant attachment",
    "Bool"
  )
  val zero: Float = 0.0
  assertEqual(
    errorOf(attachmentOf(FlagFloat(value = 1.0 / zero))),
    "a Flipt attachment cannot hold a Float that is not finite",
    "infinity"
  )
  assertEqual(
    errorOf(attachmentOf(FlagFloat(value = zero / zero))),
    "a Flipt attachment cannot hold a Float that is not finite",
    "NaN"
  )
}

val BOOLEAN_FLAG: String = "{\"key\":\"checkout\",\"name\":\"checkout\",\"description\":\"new checkout flow\",\"enabled\":true,\"createdAt\":\"2026-11-12T22:40:21.894406Z\",\"updatedAt\":\"2026-11-12T22:40:21.894406Z\",\"variants\":[],\"namespaceKey\":\"exp-1\",\"type\":\"BOOLEAN_FLAG_TYPE\",\"metadata\":{}}"

val VARIANT_FLAG: String = "{\"key\":\"banner\",\"name\":\"banner\",\"description\":\"\",\"enabled\":true,\"variants\":[{\"id\":\"a2e14df9\",\"flagKey\":\"banner\",\"key\":\"value\",\"name\":\"\",\"description\":\"\",\"attachment\":\"\\\"spring\\\"\",\"namespaceKey\":\"exp-1\"},{\"id\":\"dd5a018e\",\"flagKey\":\"banner\",\"key\":\"n\",\"attachment\":\"25\",\"namespaceKey\":\"exp-1\"},{\"id\":\"bcc0f9ce\",\"flagKey\":\"banner\",\"key\":\"plain\",\"attachment\":\"\"}],\"namespaceKey\":\"exp-1\",\"type\":\"VARIANT_FLAG_TYPE\",\"defaultVariant\":{\"id\":\"dd5a018e\",\"flagKey\":\"banner\",\"key\":\"n\",\"attachment\":\"25\",\"namespaceKey\":\"exp-1\"}}"

test "a Flag document is read with its variants and default variant" {
  val checkout = flagOf(BOOLEAN_FLAG)
  assertEqual(checkout.key, "checkout", "key")
  assertEqual(checkout.description, "new checkout flow", "description")
  assertTrue(checkout.enabled, "enabled")
  assertTrue(checkout.kind == FlagKind.Boolean, "boolean")
  assertEqualInt(checkout.variants.count, 0, "no variants")
  assertTrue(isNone(checkout.defaultVariant), "no default variant")

  val banner = flagOf(VARIANT_FLAG)
  assertTrue(banner.kind == FlagKind.Variant, "variant")
  assertEqualInt(banner.variants.count, 3, "variants")
  assertEqual(banner.variants[0].id, "a2e14df9", "variant id")
  assertEqual(banner.variants[0].attachment, "\"spring\"", "variant attachment")
  assertEqual(banner.variants[2].key, "plain", "variant key")
  match banner.defaultVariant {
    case Some(v) -> assertEqual(v.key + "/" + v.id + "/" + v.attachment, "n/dd5a018e/25", "default variant")
    case None -> assertTrue(false, "the default variant is read")
  }

  assertEqual(errorOf(parseFlag(document("{\"name\":\"x\"}"))), "a flag has no key", "no key")
  assertEqual(
    errorOf(parseFlag(document("{\"key\":\"x\",\"type\":\"JSON_FLAG_TYPE\"}"))),
    "flag 'x' has the unknown type JSON_FLAG_TYPE",
    "unknown type"
  )
  assertEqual(
    errorOf(parseFlag(document("{\"key\":\"x\",\"type\":\"VARIANT_FLAG_TYPE\",\"variants\":[{\"id\":\"1\"}]}"))),
    "flag 'x': a variant has no key",
    "variant without a key"
  )
  // Flipt before flag types listed every flag without a type: a variant flag.
  assertTrue(flagOf("{\"key\":\"old\",\"enabled\":true}").kind == FlagKind.Variant, "no type")
  // `null` where Flipt would leave the field out.
  assertTrue(isNone(flagOf("{\"key\":\"x\",\"defaultVariant\":null}").defaultVariant), "null default variant")
}

test "the value a snapshot serves" {
  assertEqual(showOption(snapshotValue(flagOf(BOOLEAN_FLAG))), "bool:true", "an enabled boolean flag")
  assertEqual(
    showOption(snapshotValue(flagOf("{\"key\":\"x\",\"type\":\"BOOLEAN_FLAG_TYPE\",\"enabled\":false}"))),
    "bool:false",
    "a disabled boolean flag is false, not absent"
  )
  assertEqual(showOption(snapshotValue(flagOf(VARIANT_FLAG))), "int:25", "the default variant")
  assertEqual(
    showOption(
      snapshotValue(
        flagOf(
          "{\"key\":\"x\",\"type\":\"VARIANT_FLAG_TYPE\",\"enabled\":false,\"defaultVariant\":{\"key\":\"a\",\"attachment\":\"1\"}}"
        )
      )
    ),
    "int:1",
    "a disabled variant flag still has its default variant, as Flipt evaluates it"
  )
  assertEqual(
    showOption(
      snapshotValue(
        flagOf("{\"key\":\"x\",\"type\":\"VARIANT_FLAG_TYPE\",\"enabled\":true,\"variants\":[{\"key\":\"a\"}]}")
      )
    ),
    "none",
    "no default variant"
  )
}

test "a FlagList page" {
  val page = match parseFlagPage(document("{\"flags\":[" + BOOLEAN_FLAG + "," + VARIANT_FLAG + "],\"nextPageToken\":\"eyJrZXkiOiJvZmYifQ==\",\"totalCount\":3}")) {
    case Ok(p) -> p
    case Err(message) -> panic(message)
  }
  assertEqualInt(page.flags.count, 2, "flags")
  assertEqual(page.flags[1].key, "banner", "order")
  assertEqual(page.nextPageToken, "eyJrZXkiOiJvZmYifQ==", "next page")
  match parseFlagPage(document("{\"flags\":[],\"nextPageToken\":\"\",\"totalCount\":0}")) {
    case Ok(p) -> assertEqual(p.flags.count.toString() + "/" + p.nextPageToken, "0/", "an empty last page")
    case Err(message) -> assertTrue(false, message)
  }
  assertEqual(errorOf(parseFlagPage(document("{}"))), "the flag list has no flags array", "no flags")
  assertEqual(errorOf(parseFlagPage(document("{\"flags\":[{}]}"))), "a flag has no key", "a broken flag")
}

test "response documents and error documents" {
  assertEqual(errorOf(parseDocument("{}")), "ok", "an object")
  assertEqual(errorOf(parseDocument("[1]")), "the response is not a JSON object", "an array")
  assertTrue(errorOf(parseDocument("<html>")).startsWith("the response is not JSON"), "not JSON")
  assertTrue(errorOf(parseDocument("")).startsWith("the response is not JSON"), "empty")
  match parseFault("{\"code\":5,\"message\":\"flag \\\"exp-1/nope\\\" not found\",\"details\":[]}") {
    case Some(fault) -> assertEqual(
      fault.code.toString() + " " + fault.message,
      "5 flag \"exp-1/nope\" not found",
      "fault"
    )
    case None -> assertTrue(false, "an error document is read")
  }
  assertTrue(isNone(parseFault("<html>502</html>")), "an HTML error page")
  assertTrue(isNone(parseFault("{\"message\":\"x\"}")), "no code")
  assertTrue(isNone(parseFault("")), "empty")
}

test "evaluation requests" {
  val ctx: Map[String, String] = newMap()
  ctx["plan"] = "enter\"prise"
  val one = input("new-checkout", "user é", ctx)
  assertEqual(
    evaluationRequest("prod", one),
    "{\"namespaceKey\":\"prod\",\"flagKey\":\"new-checkout\",\"entityId\":\"user é\",\"context\":{\"plan\":\"enter\\\"prise\"}}",
    "one evaluation"
  )
  val none: Map[String, String] = newMap()
  val inputs: List[EvaluationInput] = newList()
  inputs.add(input("a", "", none))
  inputs.add(input("b", "e", none))
  assertEqual(
    batchRequest("prod", inputs),
    "{\"requests\":[{\"namespaceKey\":\"prod\",\"flagKey\":\"a\",\"entityId\":\"\",\"context\":{}},{\"namespaceKey\":\"prod\",\"flagKey\":\"b\",\"entityId\":\"e\",\"context\":{}}]}",
    "a batch: the context is always an object, as Flipt requires"
  )
}

func input(flagKey: in String, entityId: in String, context: in Map[String, String]): EvaluationInput {
  EvaluationInput(flagKey = flagKey, entityId = entityId, context = context)
}

test "management requests" {
  assertEqual(
    createNamespaceRequest("prod", "prod", "live \"traffic\""),
    "{\"key\":\"prod\",\"name\":\"prod\",\"description\":\"live \\\"traffic\\\"\"}",
    "namespace"
  )
  assertEqual(
    createFlagRequest("beta", "d", true, FlagKind.Boolean),
    "{\"key\":\"beta\",\"name\":\"beta\",\"description\":\"d\",\"enabled\":true,\"type\":\"BOOLEAN_FLAG_TYPE\"}",
    "boolean flag: the name is the key, Flipt refuses an empty one"
  )
  assertEqual(
    createFlagRequest("theme", "", false, FlagKind.Variant),
    "{\"key\":\"theme\",\"name\":\"theme\",\"description\":\"\",\"enabled\":false,\"type\":\"VARIANT_FLAG_TYPE\"}",
    "variant flag"
  )
  assertEqual(
    updateFlagRequest("theme", "d", true, "dd5a018e"),
    "{\"name\":\"theme\",\"description\":\"d\",\"enabled\":true,\"defaultVariantId\":\"dd5a018e\"}",
    "an update sends every field: Flipt resets what is left out"
  )
  assertEqual(
    updateFlagRequest("theme", "", false, ""),
    "{\"name\":\"theme\",\"description\":\"\",\"enabled\":false}",
    "no default variant"
  )
  assertEqual(
    createVariantRequest("value", "\"spring\""),
    "{\"key\":\"value\",\"attachment\":\"\\\"spring\\\"\"}",
    "the attachment is JSON inside a JSON string"
  )
  assertEqual(createVariantRequest("plain", ""), "{\"key\":\"plain\"}", "no attachment")
}

test "evaluation responses" {
  match parseBooleanResult(document("{\"enabled\":true,\"reason\":\"MATCH_EVALUATION_REASON\",\"requestId\":\"eca0\",\"requestDurationMillis\":0.52,\"timestamp\":\"2026-11-12T22:40:35.223135990Z\",\"flagKey\":\"checkout\",\"segmentKeys\":[\"enterprise\"]}")) {
    case Ok(r) -> assertEqual(
      r.flagKey + " " + r.enabled.toString() + " " + r.reason,
      "checkout true MATCH_EVALUATION_REASON",
      "boolean"
    )
    case Err(message) -> assertTrue(false, message)
  }
  assertEqual(
    errorOf(parseBooleanResult(document("{\"reason\":\"x\"}"))),
    "the boolean evaluation has no enabled field",
    "no enabled"
  )
  match parseVariantResult(document("{\"match\":true,\"segmentKeys\":[\"enterprise\",\"beta\"],\"reason\":\"MATCH_EVALUATION_REASON\",\"variantKey\":\"dark\",\"variantAttachment\":\"\\\"dark\\\"\",\"requestId\":\"\",\"flagKey\":\"theme\"}")) {
    case Ok(r) -> {
      assertEqual(
        r.flagKey + " " + r.variantKey + " " + r.attachment + " " + r.reason,
        "theme dark \"dark\" MATCH_EVALUATION_REASON",
        "variant"
      )
      assertTrue(r.matched, "matched")
      assertEqualInt(r.segmentKeys.count, 2, "segments")
    }
    case Err(message) -> assertTrue(false, message)
  }
  match parseVariantResult(document("{\"match\":false,\"segmentKeys\":[],\"reason\":\"FLAG_DISABLED_EVALUATION_REASON\",\"variantKey\":\"\",\"variantAttachment\":\"\",\"flagKey\":\"banner\"}")) {
    case Ok(r) -> assertEqual(
      showOption(evaluationValue(VariantEvaluation(answer = r))),
      "none",
      "no variant, no value"
    )
    case Err(message) -> assertTrue(false, message)
  }
  // The default variant comes back with match false.
  match parseVariantResult(document("{\"match\":false,\"segmentKeys\":[],\"reason\":\"DEFAULT_EVALUATION_REASON\",\"variantKey\":\"n\",\"variantAttachment\":\"25\",\"flagKey\":\"banner\"}")) {
    case Ok(r) -> assertEqual(
      showOption(evaluationValue(VariantEvaluation(answer = r))),
      "int:25",
      "the default variant is a value"
    )
    case Err(message) -> assertTrue(false, message)
  }
}

val BATCH: String = "{\"requestId\":\"8703\",\"responses\":[{\"type\":\"BOOLEAN_EVALUATION_RESPONSE_TYPE\",\"booleanResponse\":{\"enabled\":true,\"reason\":\"DEFAULT_EVALUATION_REASON\",\"flagKey\":\"checkout\",\"segmentKeys\":[]}},{\"type\":\"VARIANT_EVALUATION_RESPONSE_TYPE\",\"variantResponse\":{\"match\":false,\"segmentKeys\":[],\"reason\":\"DEFAULT_EVALUATION_REASON\",\"variantKey\":\"value\",\"variantAttachment\":\"\\\"spring\\\"\",\"flagKey\":\"banner\"}},{\"type\":\"ERROR_EVALUATION_RESPONSE_TYPE\",\"errorResponse\":{\"flagKey\":\"nope\",\"namespaceKey\":\"exp-1\",\"reason\":\"NOT_FOUND_ERROR_EVALUATION_REASON\"}}],\"requestDurationMillis\":0.48}"

test "a batch response has one answer per request, of either type" {
  match parseBatchResponse(document(BATCH), 3) {
    case Err(message) -> assertTrue(false, message)
    case Ok(answers) -> {
      assertEqualInt(answers.count, 3, "answers")
      assertEqual(showOption(evaluationValue(answers[0])), "bool:true", "boolean")
      assertEqual(showOption(evaluationValue(answers[1])), "string:spring", "variant")
      assertEqual(showOption(evaluationValue(answers[2])), "none", "not found")
      match answers[2] {
        case FlagNotFound(key) -> assertEqual(key, "nope", "the missing flag")
        case _ -> assertTrue(false, "a missing flag is a FlagNotFound")
      }
    }
  }
  assertEqual(
    errorOf(parseBatchResponse(document(BATCH), 2)),
    "the batch evaluation answered 3 of 2 requests",
    "wrong count"
  )
  assertEqual(
    errorOf(parseBatchResponse(document("{}"), 0)),
    "the batch evaluation has no responses array",
    "no responses"
  )
  assertEqual(
    errorOf(parseBatchResponse(document("{\"responses\":[{\"type\":\"WHAT\"}]}"), 1)),
    "a batch answer has the unknown type 'WHAT'",
    "unknown type"
  )
  assertEqual(
    errorOf(parseBatchResponse(document("{\"responses\":[{\"type\":\"BOOLEAN_EVALUATION_RESPONSE_TYPE\"}]}"), 1)),
    "a boolean batch answer has no booleanResponse",
    "no payload"
  )
  assertEqual(
    errorOf(
      parseBatchResponse(
        document(
          "{\"responses\":[{\"type\":\"ERROR_EVALUATION_RESPONSE_TYPE\",\"errorResponse\":{\"flagKey\":\"x\",\"reason\":\"UNKNOWN_ERROR_EVALUATION_REASON\"}}]}"
        ),
        1
      )
    ),
    "flag 'x' could not be evaluated: UNKNOWN_ERROR_EVALUATION_REASON",
    "an error that is not 'not found' is an error"
  )
}
