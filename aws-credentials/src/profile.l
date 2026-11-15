//! Aws.Credentials.Profile - the AWS shared files: `~/.aws/config` and
//! `~/.aws/credentials`.
//!
//! Which files and which profile are read comes from the environment, as the
//! AWS SDKs define: `AWS_CONFIG_FILE`, `AWS_SHARED_CREDENTIALS_FILE` and
//! `AWS_PROFILE`, with `~/.aws/config`, `~/.aws/credentials` and `default`
//! when they are unset. A profile is `[default]` or `[profile <name>]` in the
//! config file and `[<name>]` in the credentials file; the two are merged and
//! the credentials file wins.
//!
//! A profile gives credentials in one of two ways: static keys
//! (`aws_access_key_id`, `aws_secret_access_key`, optional
//! `aws_session_token`) or `credential_process`, a command whose JSON output
//! holds them. A profile that needs anything else (`role_arn`,
//! `source_profile`, `credential_source`, `web_identity_token_file`, the
//! `sso_*` settings, `login_session`) is `UNSUPPORTED_PROFILE`: it is never
//! read as if those settings were absent.
//!
//! `settingsFromEnvironment` reads what is not a credential: the region, the
//! endpoint override and the CA bundle, from the environment first and the
//! profile second.
@runtime_checked
@io
package Aws.Credentials.Profile

import Std.Core
import Std.Collections
import Std.File as File
import Std.JsonValue
import Std.Process as Process
import Std.String as Str
import Aws.SigV4
import Aws.Credentials
import Aws.Credentials.Ini

val PROVIDER: String = "profile"

// How long a `credential_process` command may run.
val PROCESS_TIMEOUT_MS: Int = 60000

func failure(code: in String, message: in String): CredentialError {
  CredentialError(code = code, provider = PROVIDER, message = message)
}

/// Which shared files to read, and which profile in them.
pub record SharedFiles {
  /// The config file (`[default]`, `[profile <name>]`), or `""` for none.
  configPath: String
  /// The credentials file (`[<name>]`), or `""` for none.
  credentialsPath: String
  profile: String
  /// True when the profile was named (`AWS_PROFILE`): it must then exist.
  profileIsExplicit: Bool
}

func expandHome(path: in String, home: in String): String {
  if path == "~" {
    return home
  }
  if (Str.startsWith(path, "~/") or Str.startsWith(path, "~\\")) and home.length > 0 {
    return home + path.substring(1)
  }
  path
}

/// The files and profile the environment names: `AWS_CONFIG_FILE` (else
/// `~/.aws/config`), `AWS_SHARED_CREDENTIALS_FILE` (else
/// `~/.aws/credentials`) and `AWS_PROFILE` (else `default`). The home
/// directory is `HOME`, else `USERPROFILE`; a leading `~/` in either variable
/// is expanded. With no home directory and no variable a path is `""`.
pub func sharedFilesFromEnvironment(lookup: in (String) -> String): SharedFiles {
  var home = lookup("HOME")
  if home.length == 0 {
    home = lookup("USERPROFILE")
  }
  var configPath = expandHome(lookup("AWS_CONFIG_FILE"), home)
  if configPath.length == 0 and home.length > 0 {
    configPath = home + "/.aws/config"
  }
  var credentialsPath = expandHome(lookup("AWS_SHARED_CREDENTIALS_FILE"), home)
  if credentialsPath.length == 0 and home.length > 0 {
    credentialsPath = home + "/.aws/credentials"
  }
  val named = lookup("AWS_PROFILE")
  SharedFiles(
    configPath = configPath,
    credentialsPath = credentialsPath,
    profile = if named.length > 0 { named } else { "default" },
    profileIsExplicit = named.length > 0
  )
}

/// One profile: the properties of its sections in both files, merged.
pub record Profile {
  name: String
  /// False when neither file has a section for the profile.
  found: Bool
  properties: Map[String, String]
}

/// The value of `key` (lowercase), or `""`.
@pure
pub func Profile.get(p: in Profile, key: in String): String {
  match mapGet(p.properties, key) {
    case Some(v) -> v
    case None -> ""
  }
}

// The words of a section name: `profile   dev` is `profile`, `dev`.
func words(text: in String): List[String] {
  val acc: List[String] = newList()
  val parts = Str.split(Str.replace(text, "\t", " "), " ")
  var i = 0
  while i < parts.length {
    if parts[i].length > 0 {
      acc.add(parts[i])
    }
    i = i + 1
  }
  acc
}

func copyInto(target: in Map[String, String], section: in IniSection): Unit {
  val entries = mapEntries(section.properties)
  for e in entries {
    target[e.key] = e.value
  }
}

/// The profile `name` of a config file and a credentials file given as text
/// (`""` for a file that does not exist). In the config file the profile is
/// `[profile <name>]`, and for `default` also `[default]`, with
/// `[profile default]` winning where both set a property; in the credentials
/// file it is `[<name>]`. Properties of the credentials file replace those of
/// the config file. `Err` is the parse error of a file.
pub func profileOf(configText: in String, credentialsText: in String, name: in String): Result[Profile, String] {
  val properties: Map[String, String] = newMap()
  var found = false
  val config = match parseIni(configText, "the config file") { case Ok(f) -> f; case Err(m) -> return Err(m) }
  if name == "default" {
    match config.section("default") {
      case Some(s) -> {
        copyInto(properties, s)
        found = true
      }
      case None -> ()
    }
  }
  var i = 0
  while i < config.sections.count {
    val parts = words(config.sections[i].name)
    if parts.count == 2 and parts[0] == "profile" and parts[1] == name {
      copyInto(properties, config.sections[i])
      found = true
    }
    i = i + 1
  }
  val credentials = match parseIni(credentialsText, "the credentials file") {
    case Ok(f) -> f
    case Err(m) -> return Err(m)
  }
  match credentials.section(name) {
    case Some(s) -> {
      copyInto(properties, s)
      found = true
    }
    case None -> ()
  }
  Ok(Profile(name = name, found = found, properties = properties))
}

// The text of a shared file; `""` when there is no such file.
func readShared(path: in String): Result[String, CredentialError] {
  if path.length == 0 or not File.fileExists(path) {
    return Ok("")
  }
  match File.readText(path) {
    case Ok(text) -> Ok(text)
    case Err(_) -> Err(failure("INVALID_CONFIG", "cannot read " + path))
  }
}

/// Read the profile `files` names. A file that does not exist is an empty
/// file. `Err(INVALID_CONFIG)` for a file that cannot be read or parsed, and
/// for a profile named with `AWS_PROFILE` that neither file has; a `default`
/// profile that neither file has is returned with `found = false`.
pub func loadProfile(files: in SharedFiles): Result[Profile, CredentialError] {
  val configText = match readShared(files.configPath) { case Ok(t) -> t; case Err(e) -> return Err(e) }
  val credentialsText = match readShared(files.credentialsPath) { case Ok(t) -> t; case Err(e) -> return Err(e) }
  val profile = match profileOf(configText, credentialsText, files.profile) {
    case Ok(p) -> p
    case Err(message) -> return Err(
      failure(
        "INVALID_CONFIG",
        Str.replace(
          Str.replace(message, "the config file", files.configPath),
          "the credentials file",
          files.credentialsPath
        )
      )
    )
  }
  if not profile.found and files.profileIsExplicit {
    return Err(
      failure(
        "INVALID_CONFIG",
        "the profile '" + files.profile + "' is in neither " + describePath(files.configPath) + " nor " +
          describePath(files.credentialsPath)
      )
    )
  }
  Ok(profile)
}

func describePath(path: in String): String {
  if path.length == 0 {
    "(no file: there is no home directory)"
  } else {
    path
  }
}

// ─── credential_process ──────────────────────────────────────────────────────
/// The words of a `credential_process` command line: separated by whitespace,
/// with `"..."` (where `\"` and `\\` are escapes) and `'...'` holding
/// whitespace. The command is run directly, without a shell: there is no
/// variable expansion, globbing or redirection. `Err` for an unclosed quote
/// or an empty command.
@pure
pub func splitCommandLine(commandLine: in String): Result[List[String], String] {
  val parts: List[String] = newList()
  var current = Str.StringBuilder.new()
  var inWord = false
  var quote = ' '
  var i = 0
  while i < commandLine.length {
    val c = Str.charAtOrReplacement(commandLine, i)
    if quote == '"' {
      if c == '\\' and i + 1 < commandLine.length and (Str.charAtOrReplacement(commandLine, i + 1) == '"' or Str.charAtOrReplacement(commandLine, i + 1) == '\\') {
        current.appendChar(Str.charAtOrReplacement(commandLine, i + 1))
        i = i + 1
      } else if c == '"' {
        quote = ' '
      } else {
        current.appendChar(c)
      }
    } else if quote == '\'' {
      if c == '\'' {
        quote = ' '
      } else {
        current.appendChar(c)
      }
    } else if c == '"' or c == '\'' {
      quote = c
      inWord = true
    } else if c == ' ' or c == '\t' or c == '\n' or c == '\r' {
      if inWord {
        parts.add(current.toString())
        current = Str.StringBuilder.new()
        inWord = false
      }
    } else {
      current.appendChar(c)
      inWord = true
    }
    i = i + 1
  }
  if quote != ' ' {
    return Err("an unclosed quote")
  }
  if inWord {
    parts.add(current.toString())
  }
  if parts.count == 0 {
    return Err("an empty command")
  }
  Ok(parts)
}

/// The credentials in the JSON a `credential_process` command (or the
/// container and instance metadata services, with `tokenField = "Token"`)
/// prints: `AccessKeyId`, `SecretAccessKey`, the optional session token in
/// `tokenField` and the optional `Expiration`. `Err` says what is missing;
/// it never quotes the document.
pub func credentialsOfJson(
  text: in String, tokenField: in String, source: in String
): Result[ResolvedCredentials, String] {
  val doc = match parseValue(text) { case Ok(v) -> v; case Err(_) -> return Err("the output is not JSON") }
  if not isSome(asObject(doc)) {
    return Err("the output is not a JSON object")
  }
  val accessKeyId = match getString(doc, "AccessKeyId") { case Some(v) -> v; case None -> "" }
  val secretAccessKey = match getString(doc, "SecretAccessKey") { case Some(v) -> v; case None -> "" }
  if accessKeyId.length == 0 {
    return Err("the output has no AccessKeyId")
  }
  if secretAccessKey.length == 0 {
    return Err("the output has no SecretAccessKey")
  }
  val expiresAt: Option[Long] = match getString(doc, "Expiration") {
    case None -> None
    case Some(stamp) -> match parseTimestampMs(stamp) {
      case Some(ms) -> Some(value = ms)
      case None -> return Err("the Expiration in the output is not an ISO 8601 timestamp")
    }
  }
  Ok(
    ResolvedCredentials(
      credentials = Credentials(
        accessKeyId = accessKeyId,
        secretAccessKey = secretAccessKey,
        sessionToken = match getString(doc, tokenField) { case Some(v) -> v; case None -> "" }
      ),
      expiresAtEpochMs = expiresAt,
      source = source
    )
  )
}

func runCredentialProcess(commandLine: in String, profile: in String): Result[ResolvedCredentials, CredentialError] {
  val what = "the credential_process of profile '" + profile + "'"
  val parts = match splitCommandLine(commandLine) {
    case Ok(p) -> p
    case Err(message) -> return Err(failure("INVALID_CONFIG", what + " is not a command line: " + message))
  }
  val args: List[String] = newList()
  var i = 1
  while i < parts.count {
    args.add(parts[i])
    i = i + 1
  }
  val ran = Process.runCapture(parts[0], args, PROCESS_TIMEOUT_MS)
  val result = match ran {
    case Ok(r) -> r
    case Err(_) -> return Err(failure("PROCESS_FAILED", what + " could not be started: " + parts[0]))
  }
  if result.timedOut {
    return Err(
      failure(
        "PROCESS_FAILED",
        what + " did not finish within " + (PROCESS_TIMEOUT_MS / 1000).toString() + " seconds"
      )
    )
  }
  // Neither stream is quoted: either may hold the secret the command exists
  // to produce.
  if result.exitCode != 0 {
    return Err(failure("PROCESS_FAILED", what + " exited with code " + result.exitCode.toString()))
  }
  val doc = match parseValue(result.stdout) {
    case Ok(v) -> v
    case Err(_) -> return Err(failure("MALFORMED_RESPONSE", what + ": the output is not JSON"))
  }
  match getInt(doc, "Version") {
    case Some(v) -> if v != 1i64 {
      return Err(failure("MALFORMED_RESPONSE", what + ": Version is " + v.toString() + ", only 1 is understood"))
    }
    case None -> return Err(failure("MALFORMED_RESPONSE", what + ": the output has no Version"))
  }
  match credentialsOfJson(result.stdout, "SessionToken", "credential_process") {
    case Ok(c) -> Ok(c)
    case Err(message) -> Err(failure("MALFORMED_RESPONSE", what + ": " + message))
  }
}

// ─── The provider ────────────────────────────────────────────────────────────
func firstPresent(profile: in Profile, keys: in slice[String]): String {
  for key in keys {
    if profile.properties.containsKey(key) {
      return key
    }
  }
  ""
}

/// The credentials of a loaded profile: static keys, or the output of its
/// `credential_process` (which is run). See the package documentation for
/// the profiles that are `UNSUPPORTED_PROFILE`. A profile with none of these
/// is `NOT_CONFIGURED`.
pub func credentialsOfProfile(profile: in Profile): Result[ResolvedCredentials, CredentialError] {
  val named = "the profile '" + profile.name + "'"
  val role = firstPresent(profile, ["role_arn", "source_profile", "credential_source", "web_identity_token_file"])
  if role.length > 0 {
    return Err(
      failure("UNSUPPORTED_PROFILE", named + " sets " + role + ": assuming a role from a profile is not supported")
    )
  }
  val sso = firstPresent(
    profile,
    ["sso_session", "sso_start_url", "sso_account_id", "sso_role_name", "sso_region", "login_session"]
  )
  if sso.length > 0 {
    return Err(
      failure(
        "UNSUPPORTED_PROFILE",
        named + " sets " + sso + ": IAM Identity Center (SSO) and console login sessions are not supported"
      )
    )
  }
  val accessKeyId = profile.get("aws_access_key_id")
  val secretAccessKey = profile.get("aws_secret_access_key")
  if accessKeyId.length > 0 or secretAccessKey.length > 0 {
    if accessKeyId.length == 0 {
      return Err(failure("INVALID_CONFIG", named + " has aws_secret_access_key but no aws_access_key_id"))
    }
    if secretAccessKey.length == 0 {
      return Err(failure("INVALID_CONFIG", named + " has aws_access_key_id but no aws_secret_access_key"))
    }
    return Ok(
      ResolvedCredentials(
        credentials = Credentials(
          accessKeyId = accessKeyId,
          secretAccessKey = secretAccessKey,
          sessionToken = profile.get("aws_session_token")
        ),
        expiresAtEpochMs = None,
        source = PROVIDER
      )
    )
  }
  val process = profile.get("credential_process")
  if process.length > 0 {
    return runCredentialProcess(process, profile.name)
  }
  Err(failure("NOT_CONFIGURED", named + " has no credentials"))
}

record ProfileProvider {
  lookup: (String) -> String
}

impl CredentialProvider for ProfileProvider {
  func resolve(): Result[ResolvedCredentials, CredentialError] {
    resolveShared(sharedFilesFromEnvironment(self.lookup))
  }
}

record FilesProvider {
  files: SharedFiles
}

impl CredentialProvider for FilesProvider {
  func resolve(): Result[ResolvedCredentials, CredentialError] {
    resolveShared(self.files)
  }
}

func resolveShared(files: in SharedFiles): Result[ResolvedCredentials, CredentialError] {
  val profile = match loadProfile(files) { case Ok(p) -> p; case Err(e) -> return Err(e) }
  if not profile.found {
    return Err(
      failure(
        "NOT_CONFIGURED",
        "there is no profile '" + files.profile + "' in " + describePath(files.configPath) + " or " +
          describePath(files.credentialsPath)
      )
    )
  }
  credentialsOfProfile(profile)
}

/// The profile the environment names (`sharedFilesFromEnvironment`), read on
/// every `resolve`.
pub func profileProvider(lookup: in (String) -> String): CredentialProvider {
  val p: CredentialProvider = ProfileProvider(lookup = lookup)
  p
}

/// The profile `files` names, read on every `resolve`.
pub func profileProviderOf(files: in SharedFiles): CredentialProvider {
  val p: CredentialProvider = FilesProvider(files = files)
  p
}

// ─── Settings that are not credentials ───────────────────────────────────────
/// The region, endpoint and CA bundle an AWS client should use; each is `""`
/// when nothing sets it.
pub record AwsSettings {
  region: String
  /// The endpoint override for the service asked about.
  endpointUrl: String
  /// The path of a PEM file of certificate authorities to trust.
  caBundlePath: String
}

func firstNonEmpty(values: in slice[String]): String {
  for v in values {
    if v.length > 0 {
      return v
    }
  }
  ""
}

/// The settings for `service` (`S3`, `SQS`, `STS`), from the environment and
/// then the profile:
///
///   * region: `AWS_REGION`, `AWS_DEFAULT_REGION`, the profile's `region`;
///   * endpoint: `AWS_ENDPOINT_URL_<SERVICE>`, `AWS_ENDPOINT_URL`, the
///     profile's `endpoint_url` (not when
///     `AWS_IGNORE_CONFIGURED_ENDPOINT_URLS` is `true`);
///   * CA bundle: `AWS_CA_BUNDLE`, the profile's `ca_bundle`.
///
/// The profile is read only when the environment leaves something unset.
/// `Err` as `loadProfile`.
pub func settingsFromEnvironment(
  lookup: in (String) -> String, service: in String
): Result[AwsSettings, CredentialError] {
  var region = firstNonEmpty([lookup("AWS_REGION"), lookup("AWS_DEFAULT_REGION")])
  val ignoreEndpoints = Str.toLower(lookup("AWS_IGNORE_CONFIGURED_ENDPOINT_URLS")) == "true"
  var endpointUrl = if ignoreEndpoints {
    ""
  } else {
    firstNonEmpty([lookup("AWS_ENDPOINT_URL_" + service), lookup("AWS_ENDPOINT_URL")])
  }
  var caBundlePath = lookup("AWS_CA_BUNDLE")
  if region.length == 0 or (endpointUrl.length == 0 and not ignoreEndpoints) or caBundlePath.length == 0 {
    val profile = match loadProfile(sharedFilesFromEnvironment(lookup)) { case Ok(p) -> p; case Err(e) -> return Err(e) }
    if region.length == 0 {
      region = profile.get("region")
    }
    if endpointUrl.length == 0 and not ignoreEndpoints {
      endpointUrl = profile.get("endpoint_url")
    }
    if caBundlePath.length == 0 {
      caBundlePath = profile.get("ca_bundle")
    }
  }
  Ok(AwsSettings(region = region, endpointUrl = endpointUrl, caBundlePath = caBundlePath))
}
