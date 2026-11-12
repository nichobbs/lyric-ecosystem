//! tests/fake_flipt_tests.l - the Flipt backend over real HTTP, against a
//! Flipt server written in Lyric on `Std.HttpServer`.
//!
//! The fake does what Flipt v1.62.0 does with the requests this backend
//! makes (every behaviour below was first observed on a real server), so
//! nothing here is answered from a canned response:
//!
//!   * every API request must carry `Authorization: Bearer <token>`; a
//!     missing or wrong token is a 401 with Flipt's error document;
//!   * namespaces and flags are stored: keys are validated, a duplicate is a
//!     400, listing pages with `limit` and `pageToken`, listing a namespace
//!     that does not exist is an empty list, `PUT` of a flag replaces the
//!     whole flag (a field left out is reset), a variant's attachment must be
//!     JSON, deleting a flag that is not there is a 200, a namespace with
//!     flags cannot be deleted;
//!   * the evaluation endpoints answer as Flipt does without rules: a boolean
//!     flag is its `enabled`, a variant flag its default variant (`match`
//!     false, also when the flag is disabled) or no variant. The wrong
//!     endpoint for a flag's type is a 400, a missing flag a 404, and in a
//!     batch an error answer.
//!
//! Flipt's rules and rollouts are replaced by one fake-only control
//! (`/__target`): "when the request's entity id or a context property equals
//! this value, answer that". It is enough to prove that the entity and the
//! context reach the server; real rules run in the integration suite.
//!
//! Other controls: `/__down?on=1` makes every API request a 503 until
//! `/__down?on=0`, `/__log` returns the API requests served since the last
//! call, `/__stop` ends the server. A namespace whose key begins with `fail-`
//! answers with a failure on purpose (an HTML error page, a body that is not
//! JSON, a redirect, a slow response, a listing that never ends).
//!
//! The server runs on its own thread: a `Task.Run` worker on `--target dotnet`
//! (`spawn` is synchronous there), a `spawn` inside a `scope` on `--target
//! jvm`. No assertion runs while it is waiting for a request: a scenario
//! returns its findings as lines and ends by asking the server to stop.
@test_module
package Flags.Flipt.FakeFliptTests

import Std.Core
import Std.Collections
import Std.Encoding as Enc
import Std.Errors
import Std.Http
import Std.HttpServer
import Std.JsonValue as Json
import Std.String as Str
import Std.Testing
import Std.Time as Time
import Testing.Conformance
import Testing.Conformance.Flags as FlagsConformance
import Flags
import Flags.Flipt
import Flags.Flipt.Admin
import Flags.Flipt.Extras
import Flags.Flipt.Wire

// ─── The fake server's state ─────────────────────────────────────────────────
// A fixture, not a credential of anything.
val TOKEN: String = "fake-client-token-for-tests-only"

val STOP_PATH: String = "/__stop"

val LOG_PATH: String = "/__log"

val DOWN_PATH: String = "/__down"

val TARGET_PATH: String = "/__target"

ref record FakeVariant {
  id: String
  key: String
  attachment: String
}

// "When `property` (`entityId`, or a context key) equals `value`, answer
// `outcome`": `true`/`false` for a boolean flag, a variant key otherwise.
ref record FakeTarget {
  subject: String
  value: String
  outcome: String
}

ref record FakeFlag {
  key: String
  boolean: Bool
  var name: String
  var description: String
  var enabled: Bool
  var variants: List[FakeVariant]
  var defaultVariantId: String
  var targets: List[FakeTarget]
}

ref record FakeNamespace {
  key: String
  var flags: List[FakeFlag]
}

ref record FakeFlipt {
  var namespaces: Map[String, FakeNamespace]
  var log: List[String]
  var down: Bool
  var counter: Int
  var requestCount: Int
}

func newFakeFlipt(): FakeFlipt {
  val namespaces: Map[String, FakeNamespace] = newMap()
  namespaces["default"] = FakeNamespace(key = "default", flags = newList())
  FakeFlipt(namespaces = namespaces, log = newList(), down = false, counter = 0, requestCount = 0)
}

func nextId(flipt: in FakeFlipt): String {
  flipt.counter = flipt.counter + 1
  "00000000-0000-4000-8000-" + (100000000 + flipt.counter).toString()
}

func findFlag(ns: in FakeNamespace, key: in String): Option[FakeFlag] {
  for f in ns.flags {
    if f.key == key {
      return Some(value = f)
    }
  }
  None
}

func findVariantById(flag: in FakeFlag, id: in String): Option[FakeVariant] {
  for v in flag.variants {
    if v.id == id {
      return Some(value = v)
    }
  }
  None
}

func findVariantByKey(flag: in FakeFlag, key: in String): Option[FakeVariant] {
  for v in flag.variants {
    if v.key == key {
      return Some(value = v)
    }
  }
  None
}

// ─── Request inspection ──────────────────────────────────────────────────────
func headerValue(ctx: in HttpContext, name: in String): String {
  val wanted = Str.toLower(name)
  var found = ""
  for e in mapEntries(requestHeaders(ctx)) {
    if Str.toLower(e.key) == wanted {
      found = e.value
    }
  }
  found
}

func queryValue(ctx: in HttpContext, name: in String): String {
  for part in Str.split(requestQuery(ctx), "&") {
    if Str.startsWith(part, name + "=") {
      return urlDecode(part.substring(name.length + 1))
    }
  }
  ""
}

func textField(doc: in Json.JsonValue, name: in String): String {
  match Json.getString(doc, name) {
    case Some(v) -> v
    case None -> ""
  }
}

func digits(text: in String, fallback: in Int): Int {
  if text.length == 0 or text.length > 9 {
    return fallback
  }
  var n = 0
  var i = 0
  while i < text.length {
    val u = Str.codeUnitAt(text, i)
    if u < 48 or u > 57 {
      return fallback
    }
    n = n * 10 + (u - 48)
    i = i + 1
  }
  n
}

// ─── Responses ───────────────────────────────────────────────────────────────
func jf(name: in String, value: in Json.JsonValue): Json.JsonField {
  Json.JsonField(name = name, value = value)
}

func jt(value: in String): Json.JsonValue {
  Json.JString(value = value)
}

func jo(fields: in List[Json.JsonField]): Json.JsonValue {
  Json.JObject(fields = fields)
}

func respondRaw(ctx: in HttpContext, status: in Int, contentType: in String, body: in String): Unit {
  val none: Map[String, String] = newMap()
  respondBytesWithHeaders(ctx, status, contentType, Enc.encodeUtf8(body), none)
}

func respondDocument(ctx: in HttpContext, doc: in Json.JsonValue): Unit {
  respondRaw(ctx, 200, "application/json", Json.writeValue(doc))
}

func respondEmpty(ctx: in HttpContext): Unit {
  respondRaw(ctx, 200, "application/json", "{}")
}

// Flipt's error document: the gRPC code and a message.
func respondFault(ctx: in HttpContext, status: in Int, code: in Int, message: in String): Unit {
  val fields: List[Json.JsonField] = newList()
  fields.add(jf("code", Json.JInt(value = code.toLong())))
  fields.add(jf("message", jt(message)))
  val details: List[Json.JsonValue] = newList()
  fields.add(jf("details", Json.JArray(items = details)))
  respondRaw(ctx, status, "application/json", Json.writeValue(jo(fields)))
}

func variantObject(ns: in String, flag: in FakeFlag, v: in FakeVariant): Json.JsonValue {
  val fields: List[Json.JsonField] = newList()
  fields.add(jf("id", jt(v.id)))
  fields.add(jf("flagKey", jt(flag.key)))
  fields.add(jf("key", jt(v.key)))
  fields.add(jf("name", jt("")))
  fields.add(jf("description", jt("")))
  fields.add(jf("attachment", jt(v.attachment)))
  fields.add(jf("namespaceKey", jt(ns)))
  jo(fields)
}

func flagObject(ns: in String, flag: in FakeFlag): Json.JsonValue {
  val variants: List[Json.JsonValue] = newList()
  for v in flag.variants {
    variants.add(variantObject(ns, flag, v))
  }
  val fields: List[Json.JsonField] = newList()
  fields.add(jf("key", jt(flag.key)))
  fields.add(jf("name", jt(flag.name)))
  fields.add(jf("description", jt(flag.description)))
  fields.add(jf("enabled", Json.JBool(value = flag.enabled)))
  fields.add(jf("createdAt", jt("2026-01-01T00:00:00Z")))
  fields.add(jf("variants", Json.JArray(items = variants)))
  fields.add(jf("namespaceKey", jt(ns)))
  fields.add(jf("type", jt(if flag.boolean { "BOOLEAN_FLAG_TYPE" } else { "VARIANT_FLAG_TYPE" })))
  match findVariantById(flag, flag.defaultVariantId) {
    case Some(v) -> fields.add(jf("defaultVariant", variantObject(ns, flag, v)))
    case None -> ()
  }
  jo(fields)
}

// ─── Evaluation ──────────────────────────────────────────────────────────────
func contextValue(request: in Json.JsonValue, subject: in String): String {
  if subject == "entityId" {
    return textField(request, "entityId")
  }
  match Json.getField(request, "context") {
    case Some(context) -> textField(context, subject)
    case None -> ""
  }
}

func matchingTarget(flag: in FakeFlag, request: in Json.JsonValue): Option[FakeTarget] {
  for t in flag.targets {
    if contextValue(request, t.subject) == t.value {
      return Some(value = t)
    }
  }
  None
}

func booleanAnswer(flag: in FakeFlag, request: in Json.JsonValue): Json.JsonValue {
  var enabled = flag.enabled
  var reason = "DEFAULT_EVALUATION_REASON"
  match matchingTarget(flag, request) {
    case Some(t) -> {
      enabled = t.outcome == "true"
      reason = "MATCH_EVALUATION_REASON"
    }
    case None -> ()
  }
  val fields: List[Json.JsonField] = newList()
  fields.add(jf("enabled", Json.JBool(value = enabled)))
  fields.add(jf("reason", jt(reason)))
  fields.add(jf("requestId", jt("")))
  fields.add(jf("requestDurationMillis", Json.JFloat(value = 0.25)))
  fields.add(jf("flagKey", jt(flag.key)))
  jo(fields)
}

func variantAnswer(flag: in FakeFlag, request: in Json.JsonValue): Json.JsonValue {
  var matched = false
  var reason = "UNKNOWN_EVALUATION_REASON"
  var chosen: Option[FakeVariant] = None
  val segments: List[Json.JsonValue] = newList()
  if not flag.enabled {
    // A disabled flag skips its rules and still answers its default variant.
    reason = "FLAG_DISABLED_EVALUATION_REASON"
    chosen = findVariantById(flag, flag.defaultVariantId)
  } else {
    match matchingTarget(flag, request) {
      case Some(t) -> {
        matched = true
        reason = "MATCH_EVALUATION_REASON"
        chosen = findVariantByKey(flag, t.outcome)
        segments.add(jt(t.subject + "-" + t.value))
      }
      case None -> {
        chosen = findVariantById(flag, flag.defaultVariantId)
        if isSome(chosen) {
          reason = "DEFAULT_EVALUATION_REASON"
        }
      }
    }
  }
  val fields: List[Json.JsonField] = newList()
  fields.add(jf("match", Json.JBool(value = matched)))
  fields.add(jf("segmentKeys", Json.JArray(items = segments)))
  fields.add(jf("reason", jt(reason)))
  fields.add(jf("variantKey", jt(match chosen { case Some(v) -> v.key; case None -> "" })))
  fields.add(jf("variantAttachment", jt(match chosen { case Some(v) -> v.attachment; case None -> "" })))
  fields.add(jf("requestId", jt("")))
  fields.add(jf("flagKey", jt(flag.key)))
  jo(fields)
}

func flagOfRequest(flipt: in FakeFlipt, request: in Json.JsonValue): Option[FakeFlag] {
  match mapGet(flipt.namespaces, textField(request, "namespaceKey")) {
    case None -> None
    case Some(ns) -> findFlag(ns, textField(request, "flagKey"))
  }
}

func describeRequest(request: in Json.JsonValue): String {
  var ctx = ""
  match Json.getObject(request, "context") {
    case Some(fields) -> {
      for f in fields {
        ctx = ctx + " " + f.name + "=" + (match Json.asString(f.value) { case Some(s) -> s; case None -> "?" })
      }
    }
    case None -> {
      ctx = " <no context>"
    }
  }
  textField(request, "namespaceKey") + "/" + textField(request, "flagKey") + " entity=" + textField(request, "entityId") +
    ctx
}

func evaluateSingle(flipt: in FakeFlipt, ctx: in HttpContext, request: in Json.JsonValue, boolean: in Bool): Unit {
  flipt.log.add((if boolean { "boolean " } else { "variant " }) + describeRequest(request))
  val name = "\"" + textField(request, "namespaceKey") + "/" + textField(request, "flagKey") + "\""
  match flagOfRequest(flipt, request) {
    case None -> respondFault(ctx, 404, 5, "flag " + name + " not found")
    case Some(flag) -> {
      if flag.boolean != boolean {
        val kind = if flag.boolean { "BOOLEAN_FLAG_TYPE" } else { "VARIANT_FLAG_TYPE" }
        respondFault(ctx, 400, 3, "flag type " + kind + " invalid")
      } else if boolean {
        respondDocument(ctx, booleanAnswer(flag, request))
      } else {
        respondDocument(ctx, variantAnswer(flag, request))
      }
    }
  }
}

func evaluateBatchRequest(flipt: in FakeFlipt, ctx: in HttpContext, body: in Json.JsonValue): Unit {
  val requests = match Json.getArray(body, "requests") {
    case Some(items) -> items
    case None -> {
      respondFault(ctx, 400, 3, "requests is required")
      return
    }
  }
  val answers: List[Json.JsonValue] = newList()
  for request in requests {
    flipt.log.add("batch " + describeRequest(request))
    val fields: List[Json.JsonField] = newList()
    match flagOfRequest(flipt, request) {
      case None -> {
        val error: List[Json.JsonField] = newList()
        error.add(jf("flagKey", jt(textField(request, "flagKey"))))
        error.add(jf("namespaceKey", jt(textField(request, "namespaceKey"))))
        error.add(jf("reason", jt("NOT_FOUND_ERROR_EVALUATION_REASON")))
        fields.add(jf("type", jt("ERROR_EVALUATION_RESPONSE_TYPE")))
        fields.add(jf("errorResponse", jo(error)))
      }
      case Some(flag) -> {
        if flag.boolean {
          fields.add(jf("type", jt("BOOLEAN_EVALUATION_RESPONSE_TYPE")))
          fields.add(jf("booleanResponse", booleanAnswer(flag, request)))
        } else {
          fields.add(jf("type", jt("VARIANT_EVALUATION_RESPONSE_TYPE")))
          fields.add(jf("variantResponse", variantAnswer(flag, request)))
        }
      }
    }
    answers.add(jo(fields))
  }
  val document: List[Json.JsonField] = newList()
  document.add(jf("requestId", jt("")))
  document.add(jf("responses", Json.JArray(items = answers)))
  respondDocument(ctx, jo(document))
}

// ─── Management ──────────────────────────────────────────────────────────────
func listFlagsRequest(flipt: in FakeFlipt, ctx: in HttpContext, nsKey: in String): Unit {
  val limit = digits(queryValue(ctx, "limit"), 0)
  val token = queryValue(ctx, "pageToken")
  flipt.log.add("list " + nsKey + " limit=" + limit.toString() + (if token.length > 0 { " page=" + token } else { "" }))
  val flags: List[Json.JsonValue] = newList()
  var next = ""
  var total = 0
  match mapGet(flipt.namespaces, nsKey) {
    // Flipt answers an empty list for a namespace it does not have.
    case None -> ()
    case Some(ns) -> {
      total = ns.flags.count
      // A page token is `o<offset>`.
      val offset = if Str.startsWith(token, "o") { digits(token.substring(1), 0) } else { 0 }
      var i = offset
      while i < ns.flags.count and (limit == 0 or i < offset + limit) {
        flags.add(flagObject(nsKey, ns.flags[i]))
        i = i + 1
      }
      if i < ns.flags.count {
        next = "o" + i.toString()
      }
    }
  }
  val fields: List[Json.JsonField] = newList()
  fields.add(jf("flags", Json.JArray(items = flags)))
  fields.add(jf("nextPageToken", jt(next)))
  fields.add(jf("totalCount", Json.JInt(value = total.toLong())))
  respondDocument(ctx, jo(fields))
}

func storeFlag(flipt: in FakeFlipt, ctx: in HttpContext, ns: in FakeNamespace, body: in Json.JsonValue): Unit {
  val key = textField(body, "key")
  flipt.log.add("create-flag " + ns.key + "/" + key)
  if not isValidKey(key) {
    respondFault(ctx, 400, 3, "invalid field key: contains invalid characters")
    return
  }
  if textField(body, "name").length == 0 {
    respondFault(ctx, 400, 3, "invalid field name: must not be empty")
    return
  }
  if isSome(findFlag(ns, key)) {
    respondFault(ctx, 400, 3, "flag \"" + ns.key + "/" + key + "\" is not unique")
    return
  }
  val kind = textField(body, "type")
  val flag = FakeFlag(
    key = key,
    boolean = kind == "BOOLEAN_FLAG_TYPE",
    name = textField(body, "name"),
    description = textField(body, "description"),
    enabled = match Json.getBool(body, "enabled") { case Some(b) -> b; case None -> false },
    variants = newList(),
    defaultVariantId = "",
    targets = newList()
  )
  ns.flags.add(flag)
  respondDocument(ctx, flagObject(ns.key, flag))
}

// Flipt replaces the flag with what the request holds.
func replaceFlag(
  flipt: in FakeFlipt, ctx: in HttpContext, ns: in FakeNamespace, key: in String, body: in Json.JsonValue
): Unit {
  flipt.log.add("update-flag " + ns.key + "/" + key)
  match findFlag(ns, key) {
    case None -> respondFault(ctx, 404, 5, "flag \"" + ns.key + "/" + key + "\" not found")
    case Some(flag) -> {
      if textField(body, "name").length == 0 {
        respondFault(ctx, 400, 3, "invalid field name: must not be empty")
        return
      }
      val defaultId = textField(body, "defaultVariantId")
      if defaultId.length > 0 and not isSome(findVariantById(flag, defaultId)) {
        respondFault(ctx, 400, 3, "variant \"" + defaultId + "\" not found for flag")
        return
      }
      flag.name = textField(body, "name")
      flag.description = textField(body, "description")
      flag.enabled = match Json.getBool(body, "enabled") { case Some(b) -> b; case None -> false }
      flag.defaultVariantId = defaultId
      respondDocument(ctx, flagObject(ns.key, flag))
    }
  }
}

func storeVariant(
  flipt: in FakeFlipt,
  ctx: in HttpContext,
  ns: in FakeNamespace,
  key: in String,
  body: in Json.JsonValue,
): Unit {
  flipt.log.add("create-variant " + ns.key + "/" + key + "/" + textField(body, "key"))
  match findFlag(ns, key) {
    case None -> respondFault(ctx, 404, 5, "flag \"" + ns.key + "/" + key + "\" not found")
    case Some(flag) -> {
      val attachment = textField(body, "attachment")
      if attachment.length > 0 and isErr(Json.parseValue(attachment)) {
        respondFault(ctx, 400, 3, "invalid field attachment: must be a json string")
        return
      }
      if textField(body, "key").length == 0 {
        respondFault(ctx, 400, 3, "invalid field key: must not be empty")
        return
      }
      val variant = FakeVariant(id = nextId(flipt), key = textField(body, "key"), attachment = attachment)
      flag.variants.add(variant)
      respondDocument(ctx, variantObject(ns.key, flag, variant))
    }
  }
}

func deleteFlagRequest(flipt: in FakeFlipt, ctx: in HttpContext, ns: in FakeNamespace, key: in String): Unit {
  flipt.log.add("delete-flag " + ns.key + "/" + key)
  val kept: List[FakeFlag] = newList()
  for f in ns.flags {
    if f.key != key {
      kept.add(f)
    }
  }
  ns.flags = kept
  respondEmpty(ctx)
}

// `/api/v1/namespaces[/{ns}[/flags[/{flag}[/variants]]]]`.
func management(flipt: in FakeFlipt, ctx: in HttpContext, parts: in slice[String]): Unit {
  val method = requestMethod(ctx)
  val body = match Json.parseValue(requestBody(ctx)) { case Ok(doc) -> doc; case Err(_) -> Json.JNull }
  if parts.length == 4 {
    if method != "POST" {
      respondFault(ctx, 501, 12, "Method Not Allowed")
      return
    }
    val key = textField(body, "key")
    flipt.log.add("create-namespace " + key)
    if not isValidKey(key) {
      respondFault(ctx, 400, 3, "invalid field key: contains invalid characters")
    } else if flipt.namespaces.containsKey(key) {
      respondFault(ctx, 400, 3, "namespace \"" + key + "\" is not unique")
    } else {
      flipt.namespaces[key] = FakeNamespace(key = key, flags = newList())
      respondEmpty(ctx)
    }
    return
  }
  val nsKey = parts[4]
  if parts.length == 6 and parts[5] == "flags" and method == "GET" {
    listFlagsRequest(flipt, ctx, nsKey)
    return
  }
  val ns = match mapGet(flipt.namespaces, nsKey) {
    case Some(n) -> n
    case None -> {
      flipt.log.add(method + " missing namespace " + nsKey)
      respondFault(ctx, 404, 5, "namespace \"" + nsKey + "\" not found")
      return
    }
  }
  if parts.length == 5 and method == "GET" {
    flipt.log.add("get-namespace " + nsKey)
    val fields: List[Json.JsonField] = newList()
    fields.add(jf("key", jt(nsKey)))
    fields.add(jf("name", jt(nsKey)))
    respondDocument(ctx, jo(fields))
  } else if parts.length == 5 and method == "DELETE" {
    flipt.log.add("delete-namespace " + nsKey)
    if ns.flags.count > 0 {
      respondFault(ctx, 400, 3, "namespace \"" + nsKey + "\" cannot be deleted; flags must be deleted first")
    } else {
      val _ = flipt.namespaces.remove(nsKey)
      respondEmpty(ctx)
    }
  } else if parts.length == 6 and parts[5] == "flags" and method == "POST" {
    storeFlag(flipt, ctx, ns, body)
  } else if parts.length == 7 and parts[5] == "flags" and method == "GET" {
    flipt.log.add("get-flag " + nsKey + "/" + parts[6])
    match findFlag(ns, parts[6]) {
      case Some(flag) -> respondDocument(ctx, flagObject(nsKey, flag))
      case None -> respondFault(ctx, 404, 5, "flag \"" + nsKey + "/" + parts[6] + "\" not found")
    }
  } else if parts.length == 7 and parts[5] == "flags" and method == "PUT" {
    replaceFlag(flipt, ctx, ns, parts[6], body)
  } else if parts.length == 7 and parts[5] == "flags" and method == "DELETE" {
    deleteFlagRequest(flipt, ctx, ns, parts[6])
  } else if parts.length == 8 and parts[5] == "flags" and parts[7] == "variants" and method == "POST" {
    storeVariant(flipt, ctx, ns, parts[6], body)
  } else {
    respondFault(ctx, 501, 12, "Method Not Allowed")
  }
}

// The failures a `fail-` namespace asks for. True when one was served.
func servedFailure(flipt: in FakeFlipt, ctx: in HttpContext, nsKey: in String): Bool {
  if nsKey == "fail-html" {
    respondRaw(ctx, 502, "text/html", "<html><body>502 Bad Gateway</body></html>")
  } else if nsKey == "fail-garbage" {
    respondRaw(ctx, 200, "application/json", "this is not JSON")
  } else if nsKey == "fail-array" {
    respondRaw(ctx, 200, "application/json", "[1, 2]")
  } else if nsKey == "fail-shape" {
    respondRaw(ctx, 200, "application/json", "{\"flags\": 3, \"responses\": [{\"type\": \"WHAT\"}]}")
  } else if nsKey == "fail-untyped" {
    respondRaw(ctx, 200, "application/json", "{\"flags\": [{\"key\": \"x\", \"type\": \"JSON_FLAG_TYPE\"}]}")
  } else if nsKey == "fail-internal" {
    respondFault(ctx, 500, 13, "database is locked")
  } else if nsKey == "fail-forbidden" {
    respondFault(ctx, 403, 7, "permission denied")
  } else if nsKey == "fail-redirect" {
    val headers: Map[String, String] = newMap()
    headers["Location"] = "http://127.0.0.1:1/elsewhere"
    val empty: slice[Byte] = []
    respondBytesWithHeaders(ctx, 307, "text/plain", empty, headers)
  } else if nsKey == "fail-slow" {
    Time.sleepMillis(1500)
    respondRaw(ctx, 200, "application/json", "{\"flags\": [], \"nextPageToken\": \"\"}")
  } else if nsKey == "fail-endless" {
    respondRaw(ctx, 200, "application/json", "{\"flags\": [], \"nextPageToken\": \"again\"}")
  } else {
    return false
  }
  flipt.log.add("failure " + nsKey)
  true
}

func handle(flipt: in FakeFlipt, ctx: in HttpContext): Unit {
  flipt.requestCount = flipt.requestCount + 1
  if flipt.down {
    flipt.log.add("down")
    respondRaw(ctx, 503, "text/plain", "upstream connect error")
    return
  }
  if headerValue(ctx, "Authorization") != "Bearer " + TOKEN {
    flipt.log.add("refused")
    respondFault(ctx, 401, 16, "request was not authenticated")
    return
  }
  val path = requestPath(ctx)
  val parts = Str.split(path, "/")
  if Str.startsWith(path, "/api/v1/namespaces") and parts.length >= 4 {
    if parts.length >= 5 and servedFailure(flipt, ctx, parts[4]) {
      return
    }
    management(flipt, ctx, parts)
    return
  }
  if requestMethod(ctx) == "POST" and Str.startsWith(path, "/evaluate/v1/") {
    val body = match Json.parseValue(requestBody(ctx)) {
      case Ok(doc) -> doc
      case Err(e) -> {
        respondFault(ctx, 400, 3, Json.JsonParseError.message(e))
        return
      }
    }
    if path == "/evaluate/v1/batch" {
      val first = match Json.getArray(body, "requests") {
        case Some(items) -> if items.count > 0 { textField(items[0], "namespaceKey") } else { "" }
        case None -> ""
      }
      if not servedFailure(flipt, ctx, first) {
        evaluateBatchRequest(flipt, ctx, body)
      }
    } else if servedFailure(flipt, ctx, textField(body, "namespaceKey")) {
    } else if path == "/evaluate/v1/boolean" {
      evaluateSingle(flipt, ctx, body, true)
    } else if path == "/evaluate/v1/variant" {
      evaluateSingle(flipt, ctx, body, false)
    } else {
      respondFault(ctx, 404, 5, "Not Found")
    }
    return
  }
  respondFault(ctx, 404, 5, "Not Found")
}

func control(flipt: in FakeFlipt, ctx: in HttpContext): Unit {
  val path = requestPath(ctx)
  if path == LOG_PATH {
    respondText(ctx, 200, Str.joinList("\n", flipt.log))
    flipt.log = newList()
  } else if path == DOWN_PATH {
    flipt.down = queryValue(ctx, "on") == "1"
    respondText(ctx, 200, "ok")
  } else {
    // TARGET_PATH
    var answer = "no such flag"
    match mapGet(flipt.namespaces, queryValue(ctx, "ns")) {
      case None -> ()
      case Some(ns) -> match findFlag(ns, queryValue(ctx, "flag")) {
        case None -> ()
        case Some(flag) -> {
          flag.targets.add(
            FakeTarget(
              subject = queryValue(ctx, "property"),
              value = queryValue(ctx, "value"),
              outcome = queryValue(ctx, "outcome")
            )
          )
          answer = "ok"
        }
      }
    }
    respondText(ctx, 200, answer)
  }
}

func logLine(flipt: in FakeFlipt, line: in String): Unit {
  flipt.log.add(line)
}

func serveUntilStopped(l: in HttpListener): Unit {
  val flipt = newFakeFlipt()
  var running = true
  while running {
    val ctx = nextContext(l)
    val path = requestPath(ctx)
    if path == STOP_PATH {
      respondText(ctx, 200, "served " + flipt.requestCount.toString())
      running = false
    } else if path == LOG_PATH or path == DOWN_PATH or path == TARGET_PATH {
      control(flipt, ctx)
    } else {
      // A bug in the fake must surface as a failed request, not as a server
      // thread that died and a test run that waits for it.
      try {
        handle(flipt, ctx)
      } catch Bug as b {
        // The client may be gone (it gave up on a slow response), and then
        // this answer cannot be written either.
        try {
          respondFault(ctx, 500, 13, "fake server bug: " + b.message)
        } catch Bug as unanswered {
          logLine(flipt, "unanswered: " + unanswered.message)
        }
      }
    }
  }
}

// ─── Running a scenario against the server ───────────────────────────────────
@cfg(target = "dotnet")
extern type WorkerTask = "System.Threading.Tasks.Task"

@cfg(target = "dotnet")
@externStatic
@externTarget("System.Threading.Tasks.Task.Run")
func taskRun(work: in () -> Unit): WorkerTask = ()

@cfg(target = "dotnet")
@externInstance
@externTarget("System.Threading.Tasks.Task.Wait")
func taskWait(t: in WorkerTask): Unit = ()

func endpointOf(port: in Int): String {
  "http://127.0.0.1:" + port.toString()
}

@cfg(target = "dotnet")
func withFakeFlipt(port: in Int, scenario: in (String) -> String): String {
  val l = startListener(endpointOf(port) + "/")
  val server = taskRun({ -> serveUntilStopped(l) })
  val transcript = scenario(endpointOf(port))
  taskWait(server)
  stopListener(l)
  transcript
}

@cfg(target = "jvm")
func withFakeFlipt(port: in Int, scenario: in (String) -> String): String {
  val l = startListener(endpointOf(port) + "/")
  var transcript = ""
  scope {
    val server = spawn serveUntilStopped(l)
    transcript = scenario(endpointOf(port))
    await server
  }
  stopListener(l)
  transcript
}

func plainGet(url: in String): String {
  match await defaultClient().get(url) {
    case Err(e) -> "request failed: " + HttpError.message(e)
    case Ok(resp) -> match await HttpResponse.bodyText(resp) { case Ok(text) -> text; case Err(_) -> "unreadable" }
  }
}

// Asks the server to stop; the last thing every scenario does. Returns what
// the server said: how many API requests it served.
func stopFake(endpoint: in String): String {
  plainGet(endpoint + STOP_PATH)
}

// What the server did since the last call, joined with `; `.
func serverLog(endpoint: in String): String {
  Str.replace(plainGet(endpoint + LOG_PATH), "\n", "; ")
}

func discardLog(endpoint: in String): Unit {
  val _ = serverLog(endpoint)
}

func setDown(endpoint: in String, down: in Bool): Unit {
  val _ = plainGet(endpoint + DOWN_PATH + "?on=" + (if down { "1" } else { "0" }))
}

func addTarget(
  endpoint: in String,
  ns: in String,
  flag: in String,
  subject: in String,
  value: in String,
  outcome: in String,
): String {
  plainGet(
    endpoint + TARGET_PATH + "?ns=" + ns + "&flag=" + flag + "&property=" + subject + "&value=" + value + "&outcome=" +
      outcome
  )
}

// A scenario's findings: `label=value` lines.
ref record Notes {
  var lines: List[String]
}

func note(n: in Notes, label: in String, value: in String): Unit {
  n.lines.add(label + "=" + Str.replace(value, "\n", "\\n"))
}

// The findings of `scenario`, run against a fresh fake on `port`.
func run(port: in Int, scenario: in (String, Notes) -> Unit): slice[String] {
  Str.split(
    withFakeFlipt(
      port,
      { endpoint ->
        val n = Notes(lines = newList())
        try {
          scenario(endpoint, n)
        } catch Bug as b {
          note(n, "panicked", b.message)
        }
        note(n, "stopped", stopFake(endpoint))
        Str.joinList("\n", n.lines)
      }
    ),
    "\n"
  )
}

func found(lines: in slice[String], label: in String): String {
  for line in lines {
    if Str.startsWith(line, label + "=") {
      return line.substring(label.length + 1)
    }
  }
  "<no '" + label + "' line; the scenario said: " + Str.join(" | ", lines) + ">"
}

func check(lines: in slice[String], label: in String, expected: in String): Unit {
  assertEqual(found(lines, label), expected, label)
}

func checkContains(lines: in slice[String], label: in String, part: in String): Unit {
  val value = found(lines, label)
  assertTrue(Str.contains(value, part), label + ": expected to contain \"" + part + "\", got \"" + value + "\"")
}

// Every scenario must have run to its end and stopped the server.
func expectClean(lines: in slice[String]): Unit {
  for line in lines {
    assertTrue(not Str.startsWith(line, "panicked="), "the scenario panicked: " + line)
  }
  assertTrue(
    Str.startsWith(found(lines, "stopped"), "served "),
    "the fake server stopped cleanly: " + found(lines, "stopped")
  )
}

// ─── Clients and stores ──────────────────────────────────────────────────────
func configFor(endpoint: in String, namespace: in String): FliptConfig {
  config(endpoint, namespace).withToken(TOKEN).withRequestTimeout(10000)
}

func adminFor(endpoint: in String): FliptClient {
  match openClient(configFor(endpoint, "default")) {
    case Ok(c) -> c
    case Err(e) -> panic("openClient: " + e.message)
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

// A namespace `key` holding `entries`.
func provision(endpoint: in String, key: in String, entries: in slice[FlagEntry]): Unit {
  val client = adminFor(endpoint)
  must("createNamespace", createNamespace(client, key, "test"))
  must("seed", seed(client, key, entries))
}

// A clock the test moves.
ref record ManualClock {
  var now: Long
}

func clockedStore(cfg: in FliptConfig, clock: in ManualClock): FliptFlagStore {
  match openWithClock(cfg, { -> clock.now }) {
    case Ok(s) -> s
    case Err(e) -> panic("openWithClock: " + e.message)
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

func outcome[T](r: in Result[T, FliptError]): String {
  match r {
    case Ok(_) -> "ok"
    case Err(e) -> e.code + "/" + e.status.toString()
  }
}

func valueOutcome(r: in Result[Option[FlagValue], FliptError]): String {
  match r {
    case Ok(v) -> describe(v)
    case Err(e) -> e.code + "/" + e.status.toString()
  }
}

func lastCode(store: in FliptFlagStore): String {
  match lastError(store) {
    case Some(e) -> e.code
    case None -> "none"
  }
}

func names(entries: in slice[FlagEntry]): String {
  var text = ""
  for e in entries {
    text = text + (if text.length > 0 { "," } else { "" }) + e.name
  }
  text
}

// What a store answers for the three flags the cache scenarios use, and how
// many requests have failed.
func reading(store: in FliptFlagStore): String {
  "on=" + store.isEnabled("on").toString() + " size=" + getInt(store, "size", -1).toString() + " label=" +
    getString(store, "label", "<default>") +
    " listed=" +
    store.listFlags().length.toString() +
    " errors=" +
    errorCount(store).toString()
}

func cacheEntries(): slice[FlagEntry] {
  [makeFlag("on", true), makeIntFlag("size", 10), makeStringFlag("label", "green")]
}

// ─── Conformance ─────────────────────────────────────────────────────────────
ref record Counter {
  var next: Int
}

func freshStore(endpoint: in String, counter: in Counter, entries: in slice[FlagEntry], evaluating: in Bool): FlagStore {
  counter.next = counter.next + 1
  val key = "conf-" + counter.next.toString()
  provision(endpoint, key, entries)
  val none: Map[String, String] = newMap()
  val cfg = if evaluating {
    configFor(endpoint, key).withEvaluation("conformance", none)
  } else {
    configFor(endpoint, key)
  }
  connected(cfg)
}

func conformanceScenario(endpoint: in String, evaluating: in Bool, into: in List[Checks]): String {
  val counter = Counter(next = 0)
  into.add(FlagsConformance.run({ entries -> freshStore(endpoint, counter, entries, evaluating) }))
  stopFake(endpoint)
}

test "FliptFlagStore conforms to FlagStore in Snapshot mode, against the token-checking fake" {
  val ran: List[Checks] = newList()
  val stopped = withFakeFlipt(19932, { endpoint -> conformanceScenario(endpoint, false, ran) })
  assertEqualInt(ran.count, 1, "the suite ran: " + stopped)
  requireConformance(ran[0])
  assertEqualInt(caseCount(ran[0]), 8, "every conformance case ran")
  // 8 cases, each seeding a namespace over HTTP: far more requests than
  // cases, so the suite did not pass by never reaching the server.
  assertTrue(
    stopped.startsWith("served ") and stopped.length >= 9,
    "the suite's requests reached the server: " + stopped
  )
}

test "FliptFlagStore conforms to FlagStore in Evaluate mode, against the token-checking fake" {
  val ran: List[Checks] = newList()
  val stopped = withFakeFlipt(19933, { endpoint -> conformanceScenario(endpoint, true, ran) })
  assertEqualInt(ran.count, 1, "the suite ran: " + stopped)
  requireConformance(ran[0])
  assertEqualInt(caseCount(ran[0]), 8, "every conformance case ran")
  assertTrue(
    stopped.startsWith("served ") and stopped.length >= 9,
    "the suite's requests reached the server: " + stopped
  )
}

// ─── The snapshot cache ──────────────────────────────────────────────────────
func cacheScenario(endpoint: in String, n: in Notes): Unit {
  provision(endpoint, "app", cacheEntries())
  val client = adminFor(endpoint)
  val clock = ManualClock(now = 1000i64)
  val store = clockedStore(configFor(endpoint, "app").withRefreshInterval(10000), clock)
  discardLog(endpoint)
  note(n, "openedAge", match snapshotAgeMs(store) { case Some(_) -> "loaded"; case None -> "not loaded" })
  note(n, "openLog", serverLog(endpoint))

  note(n, "first", reading(store))
  note(n, "firstLog", serverLog(endpoint))
  var i = 0
  while i < 50 {
    val _ = reading(store)
    i = i + 1
  }
  note(n, "manyReadsLog", serverLog(endpoint))
  note(n, "refreshes1", refreshCount(store).toString())

  // The server changes; the snapshot does not until the interval has passed.
  must("flip", setFlag(client, "app", makeFlag("on", false)))
  must("resize", setFlag(client, "app", makeIntFlag("size", 20)))
  must("add", setFlag(client, "app", makeStringFlag("extra", "x")))
  discardLog(endpoint)
  clock.now = 10999i64
  note(n, "beforeInterval", reading(store))
  note(n, "beforeIntervalLog", serverLog(endpoint))
  clock.now = 11000i64
  note(n, "atInterval", reading(store))
  note(n, "atIntervalLog", serverLog(endpoint))
  note(n, "age", match snapshotAgeMs(store) { case Some(a) -> a.toString(); case None -> "not loaded" })
  note(n, "names", names(store.listFlags()))

  // On demand.
  must("flip back", setFlag(client, "app", makeFlag("on", true)))
  discardLog(endpoint)
  note(n, "stale", store.isEnabled("on").toString())
  note(n, "refresh", match store.refresh() { case Ok(_) -> "ok"; case Err(e) -> e.code })
  note(n, "refreshLog", serverLog(endpoint))
  note(n, "afterRefresh", store.isEnabled("on").toString())
  note(n, "refreshes2", refreshCount(store).toString())

  // tryGetValue reads the same snapshot.
  note(n, "tryPresent", valueOutcome(tryGetValue(store, "size")))
  note(n, "tryAbsent", valueOutcome(tryGetValue(store, "absent")))
  note(n, "tryInvalid", valueOutcome(tryGetValue(store, "not a key")))
  note(n, "invalidName", describe(store.getValue("not a key")))
  note(n, "tryLog", serverLog(endpoint))

  // Interval 0: every read asks.
  val eager = clockedStore(configFor(endpoint, "app").withRefreshInterval(0), clock)
  val _ = eager.isEnabled("on")
  val _ = eager.isEnabled("on")
  note(n, "eagerLog", serverLog(endpoint))

  // A page size smaller than the namespace.
  val paged = connected(configFor(endpoint, "app").withPageSize(2))
  discardLog(endpoint)
  must("page refresh", tryRefresh(paged))
  note(n, "pagedLog", serverLog(endpoint))
  note(n, "pagedNames", names(paged.listFlags()))
}

test "a snapshot answers reads without a request and refreshes lazily at the interval" {
  val lines = run(19934, { endpoint, n -> cacheScenario(endpoint, n) })
  expectClean(lines)
  check(lines, "openedAge", "not loaded")
  check(lines, "openLog", "")
  check(lines, "first", "on=true size=10 label=green listed=3 errors=0")
  check(lines, "firstLog", "list app limit=100")
  check(lines, "manyReadsLog", "")
  check(lines, "refreshes1", "1")
  check(lines, "beforeInterval", "on=true size=10 label=green listed=3 errors=0")
  check(lines, "beforeIntervalLog", "")
  check(lines, "atInterval", "on=false size=20 label=green listed=4 errors=0")
  check(lines, "atIntervalLog", "list app limit=100")
  check(lines, "age", "0")
  check(lines, "names", "label,on,size,extra")
  check(lines, "stale", "false")
  check(lines, "refresh", "ok")
  check(lines, "refreshLog", "list app limit=100")
  check(lines, "afterRefresh", "true")
  check(lines, "refreshes2", "3")
  check(lines, "tryPresent", "int:20")
  check(lines, "tryAbsent", "none")
  check(lines, "tryInvalid", "INVALID_ARGUMENT/0")
  check(lines, "invalidName", "none")
  check(lines, "tryLog", "")
  check(lines, "eagerLog", "list app limit=100; list app limit=100")
  check(lines, "pagedLog", "list app limit=2; list app limit=2 page=o2")
  check(lines, "pagedNames", "label,size,extra,on")
}

// ─── Outages ─────────────────────────────────────────────────────────────────
func outageScenario(endpoint: in String, n: in Notes): Unit {
  provision(endpoint, "app", cacheEntries())
  val client = adminFor(endpoint)
  val clock = ManualClock(now = 0i64)
  val cfg = configFor(endpoint, "app").withRefreshInterval(10000)
  val limited = clockedStore(cfg.withMaxStale(60000), clock)
  val unlimited = clockedStore(cfg, clock)
  note(n, "loaded", reading(limited) + " | " + reading(unlimited))

  setDown(endpoint, true)
  discardLog(endpoint)
  clock.now = 10000i64
  note(n, "down", reading(limited))
  // One failed attempt for the four reads: the next is due an interval later.
  note(n, "downLog", serverLog(endpoint))
  note(n, "downCode", lastCode(limited))
  note(n, "downTry", valueOutcome(tryGetValue(limited, "size")))
  note(n, "downTryList", outcome(tryListFlags(limited)))
  note(n, "downTryEnabled", outcome(tryIsEnabled(limited, "on")))
  note(n, "downTryLog", serverLog(endpoint))
  note(n, "downRefresh", match limited.refresh() { case Ok(_) -> "ok"; case Err(e) -> e.code + ": " + e.message })
  note(n, "downErrors", errorCount(limited).toString())

  // Still down an interval later: one more attempt, the same values.
  clock.now = 20000i64
  note(n, "stillDown", reading(limited))
  // Exactly at the staleness limit the values are still served.
  clock.now = 60000i64
  note(n, "atLimit", reading(limited))
  // Past it the flags have no value: the caller's defaults apply.
  clock.now = 60001i64
  note(n, "pastLimit", reading(limited))
  note(n, "pastLimitValue", describe(limited.getValue("on")))
  // Without a limit the last known values are served however long it takes.
  clock.now = 86400000i64
  note(n, "noLimit", reading(unlimited))

  // The server comes back changed.
  setDown(endpoint, false)
  must("resize", setFlag(client, "app", makeIntFlag("size", 30)))
  clock.now = 86400000i64 + 10000i64
  note(n, "recovered", reading(limited))
  note(n, "recoveredTry", valueOutcome(tryGetValue(limited, "size")))

  // A store opened while the server is down.
  setDown(endpoint, true)
  val late = clockedStore(cfg, clock)
  note(n, "lateDown", reading(late))
  note(n, "lateTry", valueOutcome(tryGetValue(late, "on")))
  note(n, "lateConnect", outcome(connect(cfg)))
  setDown(endpoint, false)
  note(n, "lateWithinInterval", reading(late))
  clock.now = clock.now + 10000i64
  note(n, "lateRecovered", reading(late))
}

test "during an outage reads keep the last known values, up to the staleness limit, and failures are counted" {
  val lines = run(19935, { endpoint, n -> outageScenario(endpoint, n) })
  expectClean(lines)
  check(
    lines,
    "loaded",
    "on=true size=10 label=green listed=3 errors=0 | on=true size=10 label=green listed=3 errors=0"
  )
  check(lines, "down", "on=true size=10 label=green listed=3 errors=1")
  check(lines, "downLog", "down")
  check(lines, "downCode", "HTTP_503")
  check(lines, "downTry", "HTTP_503/503")
  check(lines, "downTryList", "HTTP_503/503")
  check(lines, "downTryEnabled", "HTTP_503/503")
  check(lines, "downTryLog", "")
  check(lines, "downRefresh", "HTTP_503: flipt: ListFlags failed: Flipt answered HTTP 503")
  check(lines, "downErrors", "2")
  check(lines, "stillDown", "on=true size=10 label=green listed=3 errors=3")
  check(lines, "atLimit", "on=true size=10 label=green listed=3 errors=4")
  check(lines, "pastLimit", "on=false size=-1 label=<default> listed=0 errors=4")
  check(lines, "pastLimitValue", "none")
  check(lines, "noLimit", "on=true size=10 label=green listed=3 errors=1")
  check(lines, "recovered", "on=true size=30 label=green listed=3 errors=4")
  check(lines, "recoveredTry", "int:30")
  check(lines, "lateDown", "on=false size=-1 label=<default> listed=0 errors=1")
  check(lines, "lateTry", "HTTP_503/503")
  check(lines, "lateConnect", "HTTP_503/503")
  check(lines, "lateWithinInterval", "on=false size=-1 label=<default> listed=0 errors=1")
  check(lines, "lateRecovered", "on=true size=30 label=green listed=3 errors=1")
}

// ─── Evaluate mode ───────────────────────────────────────────────────────────
func evaluateScenario(endpoint: in String, n: in Notes): Unit {
  provision(endpoint, "app", [makeFlag("on", false), makeStringFlag("label", "green"), makeIntFlag("size", 10)])
  val client = adminFor(endpoint)
  val blue = match client.createVariant("app", "label", "blue", "\"blue\"") {
    case Ok(v) -> v
    case Err(e) -> panic(e.message)
  }
  note(n, "targetOn", addTarget(endpoint, "app", "on", "plan", "enterprise", "true"))
  note(n, "targetLabel", addTarget(endpoint, "app", "label", "entityId", "user-7", "blue"))
  val ctx: Map[String, String] = newMap()
  ctx["plan"] = "enterprise"
  val clock = ManualClock(now = 0i64)
  val base = configFor(endpoint, "app").withMaxStale(60000)
  val store = clockedStore(base.withEvaluation("user-7", ctx), clock)
  val plain = clockedStore(base, clock)
  val nobody: Map[String, String] = newMap()
  val other = clockedStore(base.withEvaluation("user-8", nobody), clock)
  discardLog(endpoint)

  note(n, "targeted", "on=" + store.isEnabled("on").toString() + " label=" + getString(store, "label", "?"))
  note(n, "targetedLog", serverLog(endpoint))
  note(n, "untargeted", "on=" + other.isEnabled("on").toString() + " label=" + getString(other, "label", "?"))
  note(n, "snapshot", "on=" + plain.isEnabled("on").toString() + " label=" + getString(plain, "label", "?"))
  note(n, "absent", describe(store.getValue("absent")))
  note(n, "tryAbsent", valueOutcome(tryGetValue(store, "absent")))
  discardLog(endpoint)
  note(n, "listing", names(store.listFlags()))
  note(n, "listingLog", serverLog(endpoint))
  var listedLabel = ""
  for f in store.listFlags() {
    if f.name == "label" {
      listedLabel = describe(Some(value = f.value))
    }
  }
  note(n, "listedLabel", listedLabel)
  note(n, "listedAgainLog", serverLog(endpoint))

  // An outage: each flag keeps its last evaluation; `size` was never
  // evaluated on its own and comes from the last listing.
  setDown(endpoint, true)
  clock.now = 30000i64
  note(
    n,
    "down",
    "on=" + store.isEnabled("on").toString() + " label=" + getString(store, "label", "?") + " size=" +
      getInt(store, "size", -1).toString() +
      " absent=" +
      describe(store.getValue("absent")) +
      " errors=" +
      errorCount(store).toString()
  )
  note(n, "downTry", valueOutcome(tryGetValue(store, "on")))
  clock.now = 60001i64
  note(
    n,
    "pastLimit",
    "on=" + store.isEnabled("on").toString() + " label=" + getString(store, "label", "?") + " size=" +
      getInt(store, "size", -1).toString()
  )
  setDown(endpoint, false)
  note(n, "recovered", "on=" + store.isEnabled("on").toString() + " label=" + getString(store, "label", "?"))

  // A flag deleted on the server stops having a value at once.
  must("delete", deleteFlag(client, "app", "on"))
  note(n, "deleted", describe(store.getValue("on")))
  setDown(endpoint, true)
  note(n, "deletedThenDown", describe(store.getValue("on")))
  setDown(endpoint, false)
}

test "Evaluate mode asks Flipt on every read with the configured entity and context" {
  val lines = run(19936, { endpoint, n -> evaluateScenario(endpoint, n) })
  expectClean(lines)
  check(lines, "targetOn", "ok")
  check(lines, "targetLabel", "ok")
  check(lines, "targeted", "on=true label=blue")
  check(
    lines,
    "targetedLog",
    "batch app/on entity=user-7 plan=enterprise; batch app/label entity=user-7 plan=enterprise"
  )
  check(lines, "untargeted", "on=false label=green")
  check(lines, "snapshot", "on=false label=green")
  check(lines, "absent", "none")
  check(lines, "tryAbsent", "none")
  check(lines, "listing", "on,label,size")
  check(
    lines,
    "listingLog",
    "list app limit=100; batch app/on entity=user-7 plan=enterprise; batch app/label entity=user-7 plan=enterprise; batch app/size entity=user-7 plan=enterprise"
  )
  check(lines, "listedLabel", "string:blue")
  check(lines, "listedAgainLog", "")
  check(lines, "down", "on=true label=blue size=10 absent=none errors=4")
  check(lines, "downTry", "HTTP_503/503")
  check(lines, "pastLimit", "on=false label=? size=-1")
  check(lines, "recovered", "on=true label=blue")
  check(lines, "deleted", "none")
  check(lines, "deletedThenDown", "none")
}

// ─── Extras ──────────────────────────────────────────────────────────────────
func boolText(r: in Result[BooleanResult, FliptError]): String {
  match r {
    case Ok(b) -> b.flagKey + " " + b.enabled.toString() + " " + b.reason
    case Err(e) -> e.code + "/" + e.status.toString() + ": " + e.message
  }
}

func variantText(r: in Result[VariantResult, FliptError]): String {
  match r {
    case Ok(v) -> v.flagKey + " " + v.matched.toString() + " key=" + v.variantKey + " attachment=" + v.attachment + " " +
      v.reason +
      " segments=" +
      v.segmentKeys.count.toString()
    case Err(e) -> e.code + "/" + e.status.toString() + ": " + e.message
  }
}

func extrasScenario(endpoint: in String, n: in Notes): Unit {
  provision(endpoint, "app", [makeFlag("beta", false), makeStringFlag("theme", "light")])
  val client = adminFor(endpoint)
  match client.createVariant("app", "theme", "dark", "{\"background\": \"#000\"}") {
    case Ok(_) -> ()
    case Err(e) -> panic(e.message)
  }
  must("parked", client.createVariantFlag("app", "parked", "", false))
  val parkedDefault = match client.createVariant("app", "parked", "a", "\"x\"") {
    case Ok(v) -> v
    case Err(e) -> panic(e.message)
  }
  must("parked default", client.updateFlag("app", "parked", "", false, parkedDefault.id))
  must("off", client.createVariantFlag("app", "off", "", false))
  must("undecided", client.createVariantFlag("app", "undecided", "", true))
  val _ = addTarget(endpoint, "app", "beta", "plan", "enterprise", "true")
  val _ = addTarget(endpoint, "app", "theme", "plan", "enterprise", "dark")
  val store = connected(configFor(endpoint, "app"))
  val enterprise: Map[String, String] = newMap()
  enterprise["plan"] = "enterprise"
  val free: Map[String, String] = newMap()
  free["plan"] = "free"
  discardLog(endpoint)

  note(n, "boolMatch", boolText(evaluateBoolean(store, "beta", "user-1", enterprise)))
  note(n, "boolDefault", boolText(evaluateBoolean(store, "beta", "user-2", free)))
  note(n, "variantMatch", variantText(evaluateVariant(store, "theme", "user-1", enterprise)))
  note(n, "variantDefault", variantText(evaluateVariant(store, "theme", "user-2", free)))
  note(n, "variantDisabled", variantText(evaluateVariant(store, "parked", "user-1", free)))
  note(n, "variantOff", variantText(evaluateVariant(store, "off", "user-1", free)))
  note(n, "variantNone", variantText(evaluateVariant(store, "undecided", "user-1", free)))
  note(n, "log", serverLog(endpoint))
  note(n, "boolOnVariant", boolText(evaluateBoolean(store, "theme", "u", free)))
  note(n, "variantOnBool", variantText(evaluateVariant(store, "beta", "u", free)))
  note(n, "boolMissing", boolText(evaluateBoolean(store, "absent", "u", free)))
  note(n, "variantMissing", variantText(evaluateVariant(store, "absent", "u", free)))
  discardLog(endpoint)
  note(n, "invalidKey", boolText(evaluateBoolean(store, "not a key", "u", free)))
  note(n, "invalidKeyLog", serverLog(endpoint))

  note(n, "valueForMatch", valueOutcome(valueFor(store, "theme", "user-1", enterprise)))
  note(n, "valueForDefault", valueOutcome(valueFor(store, "theme", "user-2", free)))
  note(n, "valueForBool", valueOutcome(valueFor(store, "beta", "user-1", enterprise)))
  note(n, "valueForMissing", valueOutcome(valueFor(store, "absent", "user-1", enterprise)))
  note(n, "valueForDisabled", valueOutcome(valueFor(store, "parked", "user-1", enterprise)))
  note(n, "valueForOff", valueOutcome(valueFor(store, "off", "user-1", enterprise)))

  val inputs: List[EvaluationInput] = newList()
  inputs.add(input("beta", "user-1", enterprise))
  inputs.add(input("absent", "user-1", enterprise))
  inputs.add(input("theme", "user-2", free))
  discardLog(endpoint)
  match evaluateBatch(store, inputs) {
    case Err(e) -> note(n, "batch", e.code + ": " + e.message)
    case Ok(answers) -> {
      var text = ""
      for a in answers {
        text = text + describe(evaluationValue(a)) + ";"
      }
      note(n, "batch", text)
    }
  }
  note(n, "batchLog", serverLog(endpoint))
  val nothing: List[EvaluationInput] = newList()
  note(n, "emptyBatch", match evaluateBatch(store, nothing) { case Ok(a) -> a.count.toString(); case Err(e) -> e.code })
  note(n, "emptyBatchLog", serverLog(endpoint))
  // Evaluations do not touch the snapshot or its error count.
  note(
    n,
    "snapshot",
    "beta=" + store.isEnabled("beta").toString() + " theme=" + getString(store, "theme", "?") + " parked=" +
      getString(store, "parked", "?") +
      " off=" +
      getString(store, "off", "?") +
      " listed=" +
      names(store.listFlags())
  )
  note(n, "errors", errorCount(store).toString())
}

test "Extras evaluates boolean and variant flags for an entity and a context, singly and in a batch" {
  val lines = run(19937, { endpoint, n -> extrasScenario(endpoint, n) })
  expectClean(lines)
  check(lines, "boolMatch", "beta true MATCH_EVALUATION_REASON")
  check(lines, "boolDefault", "beta false DEFAULT_EVALUATION_REASON")
  check(
    lines,
    "variantMatch",
    "theme true key=dark attachment={\"background\": \"#000\"} MATCH_EVALUATION_REASON segments=1"
  )
  check(lines, "variantDefault", "theme false key=value attachment=\"light\" DEFAULT_EVALUATION_REASON segments=0")
  check(lines, "variantDisabled", "parked false key=a attachment=\"x\" FLAG_DISABLED_EVALUATION_REASON segments=0")
  check(lines, "variantOff", "off false key= attachment= FLAG_DISABLED_EVALUATION_REASON segments=0")
  check(lines, "variantNone", "undecided false key= attachment= UNKNOWN_EVALUATION_REASON segments=0")
  check(
    lines,
    "log",
    "boolean app/beta entity=user-1 plan=enterprise; boolean app/beta entity=user-2 plan=free; variant app/theme entity=user-1 plan=enterprise; variant app/theme entity=user-2 plan=free; variant app/parked entity=user-1 plan=free; variant app/off entity=user-1 plan=free; variant app/undecided entity=user-1 plan=free"
  )
  check(
    lines,
    "boolOnVariant",
    "INVALID_ARGUMENT/400: EvaluateBoolean failed: Flipt answered HTTP 400: flag type VARIANT_FLAG_TYPE invalid"
  )
  check(
    lines,
    "variantOnBool",
    "INVALID_ARGUMENT/400: EvaluateVariant failed: Flipt answered HTTP 400: flag type BOOLEAN_FLAG_TYPE invalid"
  )
  check(
    lines,
    "boolMissing",
    "NOT_FOUND/404: EvaluateBoolean failed: Flipt answered HTTP 404: flag \"app/absent\" not found"
  )
  check(
    lines,
    "variantMissing",
    "NOT_FOUND/404: EvaluateVariant failed: Flipt answered HTTP 404: flag \"app/absent\" not found"
  )
  checkContains(lines, "invalidKey", "INVALID_ARGUMENT/0: flag 'not a key' is not a Flipt key")
  check(lines, "invalidKeyLog", "")
  check(lines, "valueForMatch", "string:dark")
  check(lines, "valueForDefault", "string:light")
  check(lines, "valueForBool", "bool:true")
  check(lines, "valueForMissing", "none")
  check(lines, "valueForDisabled", "string:x")
  check(lines, "valueForOff", "none")
  check(lines, "batch", "bool:true;none;string:light;")
  check(
    lines,
    "batchLog",
    "batch app/beta entity=user-1 plan=enterprise; batch app/absent entity=user-1 plan=enterprise; batch app/theme entity=user-2 plan=free"
  )
  check(lines, "emptyBatch", "0")
  check(lines, "emptyBatchLog", "")
  check(lines, "snapshot", "beta=false theme=light parked=x off=? listed=beta,theme,parked")
  check(lines, "errors", "0")
}

// ─── Authentication and failures ─────────────────────────────────────────────
func errorText(r: in Result[Unit, FliptError]): String {
  match r {
    case Ok(_) -> "ok"
    case Err(e) -> e.code + "/" + e.status.toString() + ": " + e.text()
  }
}

func connectText(cfg: in FliptConfig): String {
  match connect(cfg) {
    case Ok(_) -> "ok"
    case Err(e) -> e.code + "/" + e.status.toString() + ": " + e.text()
  }
}

func openedStore(cfg: in FliptConfig): FliptFlagStore {
  match open(cfg) {
    case Ok(s) -> s
    case Err(e) -> panic("open: " + e.message)
  }
}

func failureScenario(endpoint: in String, n: in Notes): Unit {
  provision(endpoint, "app", cacheEntries())
  discardLog(endpoint)

  // Authentication.
  note(n, "wrongToken", connectText(configFor(endpoint, "app").withToken("not-the-token")))
  note(n, "noToken", connectText(config(endpoint, "app")))
  note(n, "authLog", serverLog(endpoint))
  val locked = openedStore(config(endpoint, "app"))
  note(n, "lockedRead", reading(locked))
  note(n, "lockedCode", lastCode(locked))
  note(n, "lockedRefresh", match locked.refresh() { case Ok(_) -> "ok"; case Err(e) -> e.code + ": " + e.message })
  note(n, "rightToken", connectText(configFor(endpoint, "app")))

  // The namespace.
  note(n, "missingNamespace", connectText(configFor(endpoint, "nowhere")))
  // `open` does not ask: Flipt lists a missing namespace as empty.
  val nowhere = openedStore(configFor(endpoint, "nowhere"))
  note(n, "missingNamespaceRead", reading(nowhere))

  // Responses that are not what Flipt answers.
  note(n, "html", errorText(tryRefresh(openedStore(configFor(endpoint, "fail-html")))))
  note(n, "garbage", errorText(tryRefresh(openedStore(configFor(endpoint, "fail-garbage")))))
  note(n, "array", errorText(tryRefresh(openedStore(configFor(endpoint, "fail-array")))))
  note(n, "shape", errorText(tryRefresh(openedStore(configFor(endpoint, "fail-shape")))))
  note(n, "untyped", errorText(tryRefresh(openedStore(configFor(endpoint, "fail-untyped")))))
  note(n, "internal", errorText(tryRefresh(openedStore(configFor(endpoint, "fail-internal")))))
  note(n, "forbidden", errorText(tryRefresh(openedStore(configFor(endpoint, "fail-forbidden")))))
  note(n, "redirect", errorText(tryRefresh(openedStore(configFor(endpoint, "fail-redirect")))))
  note(n, "endless", errorText(tryRefresh(openedStore(configFor(endpoint, "fail-endless")))))
  val none: Map[String, String] = newMap()
  val garbled = openedStore(configFor(endpoint, "fail-garbage").withEvaluation("u", none))
  note(n, "evaluateGarbage", valueOutcome(tryGetValue(garbled, "x")))
  note(n, "evaluateGarbageRead", describe(garbled.getValue("x")) + " errors=" + errorCount(garbled).toString())
  val misshapen = openedStore(configFor(endpoint, "fail-shape").withEvaluation("u", none))
  note(n, "evaluateShape", valueOutcome(tryGetValue(misshapen, "x")))

  // A slow server against the request deadline.
  val slow = openedStore(configFor(endpoint, "fail-slow").withRequestTimeout(300))
  val started = Time.monotonicNanos()
  note(n, "slow", errorText(tryRefresh(slow)))
  val waitedMs = (Time.monotonicNanos() - started) / 1000000i64
  note(n, "slowGaveUpEarly", (waitedMs < 1400i64).toString())
  note(n, "slowRead", reading(slow))
  val patient = openedStore(configFor(endpoint, "fail-slow").withRequestTimeout(10000))
  note(n, "patient", errorText(tryRefresh(patient)))

  // Nothing listening.
  val dead = openedStore(config("http://127.0.0.1:1", "app").withToken(TOKEN).withRequestTimeout(3000))
  note(n, "nothingListening", match tryRefresh(dead) { case Ok(_) -> "ok"; case Err(e) -> e.code })
  note(n, "nothingListeningRead", reading(dead))
  note(n, "nothingListeningConnect", outcome(connect(config("http://127.0.0.1:1", "app").withRequestTimeout(3000))))
}

test "authentication, a missing namespace and every kind of bad response are reported, never panics or flipped flags" {
  val lines = run(19938, { endpoint, n -> failureScenario(endpoint, n) })
  expectClean(lines)
  check(
    lines,
    "wrongToken",
    "UNAUTHENTICATED/401: flipt: GetNamespace failed: Flipt answered HTTP 401: request was not authenticated"
  )
  check(
    lines,
    "noToken",
    "UNAUTHENTICATED/401: flipt: GetNamespace failed: Flipt answered HTTP 401: request was not authenticated"
  )
  check(lines, "authLog", "refused; refused")
  check(lines, "lockedRead", "on=false size=-1 label=<default> listed=0 errors=1")
  check(lines, "lockedCode", "UNAUTHENTICATED")
  check(
    lines,
    "lockedRefresh",
    "UNAUTHENTICATED: flipt: ListFlags failed: Flipt answered HTTP 401: request was not authenticated"
  )
  check(lines, "rightToken", "ok")
  checkContains(
    lines,
    "missingNamespace",
    "NOT_FOUND/404: flipt: Flipt has no namespace 'nowhere' at http://127.0.0.1:19938"
  )
  check(lines, "missingNamespaceRead", "on=false size=-1 label=<default> listed=0 errors=0")
  check(lines, "html", "HTTP_502/502: flipt: ListFlags failed: Flipt answered HTTP 502")
  checkContains(lines, "garbage", "MALFORMED_RESPONSE/200: flipt: ListFlags: the response is not JSON")
  check(lines, "array", "MALFORMED_RESPONSE/200: flipt: ListFlags: the response is not a JSON object")
  check(lines, "shape", "MALFORMED_RESPONSE/200: flipt: ListFlags: the flag list has no flags array")
  check(lines, "untyped", "MALFORMED_RESPONSE/200: flipt: ListFlags: flag 'x' has the unknown type JSON_FLAG_TYPE")
  check(lines, "internal", "HTTP_500/500: flipt: ListFlags failed: Flipt answered HTTP 500: database is locked")
  check(
    lines,
    "forbidden",
    "PERMISSION_DENIED/403: flipt: ListFlags failed: Flipt answered HTTP 403: permission denied"
  )
  check(lines, "redirect", "HTTP_307/307: flipt: ListFlags failed: Flipt answered HTTP 307")
  check(lines, "endless", "MALFORMED_RESPONSE/200: flipt: ListFlags: the listing does not end")
  check(lines, "evaluateGarbage", "MALFORMED_RESPONSE/200")
  check(lines, "evaluateGarbageRead", "none errors=2")
  check(lines, "evaluateShape", "MALFORMED_RESPONSE/200")
  checkContains(lines, "slow", "TIMEOUT/0: flipt: ListFlags timed out after 300 ms at http://127.0.0.1:19938")
  check(lines, "slowGaveUpEarly", "true")
  check(lines, "slowRead", "on=false size=-1 label=<default> listed=0 errors=1")
  check(lines, "patient", "ok")
  check(lines, "nothingListening", "CONNECTION_FAILED")
  check(lines, "nothingListeningRead", "on=false size=-1 label=<default> listed=0 errors=1")
  check(lines, "nothingListeningConnect", "CONNECTION_FAILED/0")
}

// ─── Provisioning ────────────────────────────────────────────────────────────
func adminScenario(endpoint: in String, n: in Notes): Unit {
  val client = adminFor(endpoint)
  note(n, "create", errorText(createNamespace(client, "tools", "a namespace")))
  note(n, "createAgain", errorText(createNamespace(client, "tools", "a namespace")))
  note(n, "createInvalid", errorText(createNamespace(client, "not a key", "")))
  discardLog(endpoint)
  note(n, "bool", errorText(setFlag(client, "tools", makeFlagWithDesc("kill-switch", true, "stops everything"))))
  note(n, "boolLog", serverLog(endpoint))
  note(n, "int", errorText(setFlag(client, "tools", makeIntFlag("workers", 4))))
  note(n, "intLog", serverLog(endpoint))
  note(n, "invalidName", errorText(setFlag(client, "tools", makeFlag("has.dot", true))))
  note(n, "invalidNameLog", serverLog(endpoint))
  note(n, "notFinite", errorText(setFlag(client, "tools", makeFloatFlag("ratio", (1.0 / 0.0).toFloat()))))
  note(n, "missingNamespace", errorText(setFlag(client, "nowhere", makeFlag("x", true))))

  val store = connected(configFor(endpoint, "tools").withRefreshInterval(0))
  note(
    n,
    "read",
    "kill-switch=" + store.isEnabled("kill-switch").toString() + " workers=" + getInt(store, "workers", -1).toString()
  )
  // Replacing changes the type too.
  note(n, "replace", errorText(setFlag(client, "tools", makeStringFlag("kill-switch", "armed"))))
  note(n, "replaced", describe(store.getValue("kill-switch")) + " enabled=" + store.isEnabled("kill-switch").toString())
  var description = "<not listed>"
  must("describe", setFlag(client, "tools", makeFlagWithDesc("kill-switch", true, "stops everything")))
  for f in store.listFlags() {
    if f.name == "kill-switch" {
      description = f.description
    }
  }
  note(n, "description", description)
  match client.getFlag("tools", "workers") {
    case Ok(flag) -> note(
      n,
      "stored",
      flag.key + " enabled=" + flag.enabled.toString() + " variants=" + flag.variants.count.toString() +
        " default=" +
        (match flag.defaultVariant { case Some(v) -> v.key + ":" + v.attachment; case None -> "none" })
    )
    case Err(e) -> note(n, "stored", e.code)
  }
  note(n, "getMissing", match client.getFlag("tools", "absent") { case Ok(_) -> "ok"; case Err(e) -> e.code })
  note(n, "exists", match client.namespaceExists("tools") { case Ok(b) -> b.toString(); case Err(e) -> e.code })
  note(n, "deleteFlag", errorText(deleteFlag(client, "tools", "workers")))
  note(n, "deleteFlagAgain", errorText(deleteFlag(client, "tools", "workers")))
  note(n, "plainDeleteRefused", errorText(client.deleteNamespace("tools")))
  note(n, "deleteNamespace", errorText(deleteNamespace(client, "tools")))
  note(n, "gone", match client.namespaceExists("tools") { case Ok(b) -> b.toString(); case Err(e) -> e.code })
}

test "Admin stores FlagEntry values as Flipt flags and removes them" {
  val lines = run(19939, { endpoint, n -> adminScenario(endpoint, n) })
  expectClean(lines)
  check(lines, "create", "ok")
  check(
    lines,
    "createAgain",
    "INVALID_ARGUMENT/400: flipt: CreateNamespace failed: Flipt answered HTTP 400: namespace \"tools\" is not unique"
  )
  checkContains(lines, "createInvalid", "INVALID_ARGUMENT/0: flipt: namespace 'not a key' is not a Flipt key")
  check(lines, "bool", "ok")
  check(lines, "boolLog", "delete-flag tools/kill-switch; create-flag tools/kill-switch")
  check(lines, "int", "ok")
  check(
    lines,
    "intLog",
    "delete-flag tools/workers; create-flag tools/workers; create-variant tools/workers/value; update-flag tools/workers"
  )
  checkContains(lines, "invalidName", "INVALID_ARGUMENT/0: flipt: flag 'has.dot' is not a Flipt key")
  check(lines, "invalidNameLog", "")
  checkContains(lines, "notFinite", "INVALID_ARGUMENT/0: flipt: flag 'ratio': a Flipt attachment cannot hold a Float")
  checkContains(lines, "missingNamespace", "NOT_FOUND/404")
  check(lines, "read", "kill-switch=true workers=4")
  check(lines, "replace", "ok")
  check(lines, "replaced", "string:armed enabled=false")
  check(lines, "description", "stops everything")
  check(lines, "stored", "workers enabled=true variants=1 default=value:4")
  check(lines, "getMissing", "NOT_FOUND")
  check(lines, "exists", "true")
  check(lines, "deleteFlag", "ok")
  check(lines, "deleteFlagAgain", "ok")
  checkContains(lines, "plainDeleteRefused", "INVALID_ARGUMENT/400")
  check(lines, "deleteNamespace", "ok")
  check(lines, "gone", "false")
}
