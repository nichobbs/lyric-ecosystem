//! tests/profile_tests.l - the shared config and credentials files: which
//! files and profile the environment names, how the two files merge, which
//! profiles are refused, `credential_process` with a real command, and the
//! settings that are not credentials.
//!
//! The `credential_process` cases run `/bin/sh` on a script the test writes.
@test_module
package Aws.Credentials.ProfileTests

import Std.Core
import Std.Collections
import Std.Environment as Env
import Std.File as File
import Std.String as Str
import Std.Testing
import Std.Time as Time
import Aws.Credentials
import Aws.Credentials.Profile

// Fixture values, not real credentials.
val SECRET: String = "fixture/secret+key/for-tests-only"

val TOKEN: String = "fixture-session-token=="

func describe(r: in Result[ResolvedCredentials, CredentialError]): String {
  match r {
    case Ok(c) -> "ok " + c.credentials.accessKeyId + " from " + c.source +
      (if c.credentials.secretAccessKey == SECRET { "" } else { " WRONG-SECRET" }) +
      (if c.credentials.sessionToken.length > 0 { " token=" + c.credentials.sessionToken } else { "" }) +
      (match c.expiresAtEpochMs { case Some(at) -> " until " + at.toString(); case None -> "" })
    case Err(e) -> e.code + " " + e.text()
  }
}

func profileText(config: in String, credentials: in String, name: in String): String {
  match profileOf(config, credentials, name) {
    case Err(message) -> "error: " + message
    case Ok(p) -> describe(
      if p.found {
        credentialsOfProfile(p)
      } else {
        Err(CredentialError(code = "ABSENT", provider = "test", message = name))
      }
    )
  }
}

func envOf(pairs: in slice[String]): (String) -> String {
  val values: Map[String, String] = newMap()
  var i = 0
  while i + 1 < pairs.length {
    values[pairs[i]] = pairs[i + 1]
    i = i + 2
  }
  environmentOf(values)
}

test "the environment names the files and the profile, with the SDK defaults" {
  val plain = sharedFilesFromEnvironment(envOf(["HOME", "/home/someone"]))
  assertEqual(plain.configPath, "/home/someone/.aws/config", "default config path")
  assertEqual(plain.credentialsPath, "/home/someone/.aws/credentials", "default credentials path")
  assertEqual(plain.profile, "default", "default profile")
  assertFalse(plain.profileIsExplicit, "not explicit")
  val named = sharedFilesFromEnvironment(
    envOf(
      [
        "HOME",
        "/home/someone",
        "AWS_CONFIG_FILE",
        "/etc/aws/config",
        "AWS_SHARED_CREDENTIALS_FILE",
        "~/secrets/creds",
        "AWS_PROFILE",
        "dev"
      ]
    )
  )
  assertEqual(named.configPath, "/etc/aws/config", "AWS_CONFIG_FILE")
  assertEqual(named.credentialsPath, "/home/someone/secrets/creds", "AWS_SHARED_CREDENTIALS_FILE, ~ expanded")
  assertEqual(named.profile, "dev", "AWS_PROFILE")
  assertTrue(named.profileIsExplicit, "explicit")
  val windows = sharedFilesFromEnvironment(envOf(["USERPROFILE", "C:\\Users\\someone"]))
  assertEqual(windows.configPath, "C:\\Users\\someone/.aws/config", "USERPROFILE when HOME is unset")
  val homeless = sharedFilesFromEnvironment(envOf(["AWS_PROFILE", "x"]))
  assertEqual(homeless.configPath, "", "no home, no path")
}

test "static keys come from the credentials file, the config file, or both merged" {
  val credentials = "[default]\naws_access_key_id = AKIADEFAULT\naws_secret_access_key = " + SECRET +
    "\n\n[dev]\naws_access_key_id = AKIADEV\naws_secret_access_key = " +
    SECRET +
    "\naws_session_token = " +
    TOKEN +
    "\n"
  assertEqual(profileText("", credentials, "default"), "ok AKIADEFAULT from profile", "default, credentials file")
  assertEqual(
    profileText("", credentials, "dev"),
    "ok AKIADEV from profile token=" + TOKEN,
    "a named profile with a token"
  )
  val config = "[default]\nregion = us-east-1\n[profile dev]\naws_access_key_id = AKIACONFIG\naws_secret_access_key = " +
    SECRET +
    "\n[profile only-config]\naws_access_key_id = AKIAONLY\naws_secret_access_key = " +
    SECRET +
    "\n"
  assertEqual(profileText(config, "", "dev"), "ok AKIACONFIG from profile", "keys in the config file")
  assertEqual(
    profileText(config, credentials, "dev"),
    "ok AKIADEV from profile token=" + TOKEN,
    "the credentials file wins"
  )
  assertEqual(profileText(config, credentials, "only-config"), "ok AKIAONLY from profile", "a profile in one file only")
  assertEqual(profileText(config, credentials, "nope"), "ABSENT test: nope", "a profile in neither")
}

test "section naming: [profile x] in config, [x] in credentials, and never the other way round" {
  val keys = "\naws_access_key_id = AKIAX\naws_secret_access_key = " + SECRET + "\n"
  assertEqual(profileText("[dev]" + keys, "", "dev"), "ABSENT test: dev", "[dev] in the config file is not a profile")
  assertEqual(
    profileText("", "[profile dev]" + keys, "dev"),
    "ABSENT test: dev",
    "[profile dev] in the credentials file is not dev"
  )
  assertEqual(
    profileText("[profile   dev ]" + keys, "", "dev"),
    "ok AKIAX from profile",
    "extra whitespace in the header"
  )
  assertEqual(profileText("[default]" + keys, "", "default"), "ok AKIAX from profile", "[default] in config")
  val both = "[default]\naws_access_key_id = AKIAPLAIN\naws_secret_access_key = " + SECRET +
    "\n[profile default]\naws_access_key_id = AKIAPREFIXED\n"
  assertEqual(profileText(both, "", "default"), "ok AKIAPREFIXED from profile", "[profile default] wins over [default]")
  assertEqual(
    profileText("[sso-session dev]" + keys + "[services dev]" + keys, "", "dev"),
    "ABSENT test: dev",
    "other section kinds are not profiles"
  )
}

test "a sub-property block is not read as the profile's credentials" {
  val config = "[default]\nregion = us-east-1\ns3 =\n  aws_access_key_id = AKIANESTED\n  aws_secret_access_key = nested\n"
  assertEqual(
    profileText(config, "", "default"),
    "NOT_CONFIGURED profile: the profile 'default' has no credentials",
    "nested keys are ignored"
  )
}

test "half a key pair is INVALID_CONFIG, and the message does not hold the secret" {
  val onlySecret = profileText("", "[default]\naws_secret_access_key = " + SECRET + "\n", "default")
  assertEqual(
    onlySecret,
    "INVALID_CONFIG profile: the profile 'default' has aws_secret_access_key but no aws_access_key_id",
    "no id"
  )
  assertFalse(Str.contains(onlySecret, SECRET), "no secret in the message")
  assertEqual(
    profileText("", "[default]\naws_access_key_id = AKIAHALF\n", "default"),
    "INVALID_CONFIG profile: the profile 'default' has aws_access_key_id but no aws_secret_access_key",
    "no secret"
  )
}

test "profiles that need SSO or an assumed role are refused, not read as something else" {
  val keys = "aws_access_key_id = AKIAX\naws_secret_access_key = " + SECRET + "\n"
  assertEqual(
    profileText("[profile a]\nrole_arn = arn:aws:iam::123456789012:role/x\nsource_profile = default\n" + keys, "", "a"),
    "UNSUPPORTED_PROFILE profile: the profile 'a' sets role_arn: assuming a role from a profile is not supported",
    "role_arn, even next to static keys"
  )
  assertEqual(
    profileText("[profile a]\ncredential_source = Ec2InstanceMetadata\n", "", "a"),
    "UNSUPPORTED_PROFILE profile: the profile 'a' sets credential_source: assuming a role from a profile is not supported",
    "credential_source"
  )
  assertEqual(
    profileText("[profile a]\nweb_identity_token_file = /var/run/token\n", "", "a"),
    "UNSUPPORTED_PROFILE profile: the profile 'a' sets web_identity_token_file: assuming a role from a profile is not supported",
    "web identity in a profile"
  )
  assertEqual(
    profileText("[profile a]\nsso_session = corp\nsso_account_id = 123456789012\nsso_role_name = Dev\n", "", "a"),
    "UNSUPPORTED_PROFILE profile: the profile 'a' sets sso_session: IAM Identity Center (SSO) and console login sessions are not supported",
    "sso_session"
  )
  assertEqual(
    profileText("[profile a]\nsso_start_url = https://corp.awsapps.com/start\n" + keys, "", "a"),
    "UNSUPPORTED_PROFILE profile: the profile 'a' sets sso_start_url: IAM Identity Center (SSO) and console login sessions are not supported",
    "legacy SSO, even next to static keys"
  )
}

test "a file that does not parse is INVALID_CONFIG with its line, and no content" {
  assertEqual(
    profileText("[default]\n" + SECRET + "\n", "", "default"),
    "error: the config file line 2: expected '[section]' or 'name = value'",
    "the config file"
  )
  assertEqual(
    profileText("", "[default\n", "default"),
    "error: the credentials file line 1: a section header without ']'",
    "the credentials file"
  )
}

// ─── Real files ──────────────────────────────────────────────────────────────
func scratchDir(label: in String): String {
  val dir = Env.getVarOrDefault("TMPDIR", "/tmp") + "/lyric-aws-credentials-" + label + "-" +
    Time.nowEpochMillis().toString()
  match File.createDir(dir) {
    case Ok(_) -> dir
    case Err(_) -> panic("cannot create " + dir)
  }
}

func write(path: in String, text: in String): Unit {
  match File.writeText(path, text) {
    case Ok(_) -> ()
    case Err(_) -> panic("cannot write " + path)
  }
}

test "the provider reads the files the environment names, on every resolve" {
  val dir = scratchDir("files")
  val configPath = dir + "/config"
  val credentialsPath = dir + "/credentials"
  val values: Map[String, String] = newMap()
  values["AWS_CONFIG_FILE"] = configPath
  values["AWS_SHARED_CREDENTIALS_FILE"] = credentialsPath
  val provider = profileProvider(environmentOf(values))
  assertEqual(
    describe(provider.resolve()),
    "NOT_CONFIGURED profile: there is no profile 'default' in " + configPath + " or " + credentialsPath,
    "neither file exists"
  )
  write(credentialsPath, "[default]\naws_access_key_id = AKIAFILE1\naws_secret_access_key = " + SECRET + "\n")
  assertEqual(describe(provider.resolve()), "ok AKIAFILE1 from profile", "the credentials file alone")
  write(credentialsPath, "[default]\naws_access_key_id = AKIAFILE2\naws_secret_access_key = " + SECRET + "\n")
  assertEqual(describe(provider.resolve()), "ok AKIAFILE2 from profile", "rotated keys are seen")
  write(configPath, "[profile dev]\naws_access_key_id = AKIADEVFILE\naws_secret_access_key = " + SECRET + "\n")
  values["AWS_PROFILE"] = "dev"
  assertEqual(describe(provider.resolve()), "ok AKIADEVFILE from profile", "AWS_PROFILE selects the profile")
  values["AWS_PROFILE"] = "missing"
  assertEqual(
    describe(provider.resolve()),
    "INVALID_CONFIG profile: the profile 'missing' is in neither " + configPath + " nor " + credentialsPath,
    "a profile that was asked for by name must exist"
  )
  write(configPath, "[profile dev\n")
  values["AWS_PROFILE"] = "dev"
  assertEqual(
    describe(provider.resolve()),
    "INVALID_CONFIG profile: " + configPath + " line 1: a section header without ']'",
    "a parse error names the file"
  )
  val _ = File.deleteDirRecursive(dir)
}

// ─── credential_process ──────────────────────────────────────────────────────
func words(commandLine: in String): String {
  match splitCommandLine(commandLine) {
    case Ok(parts) -> Str.joinList("|", parts)
    case Err(message) -> "error: " + message
  }
}

test "a credential_process command line is split like a shell would, without a shell" {
  assertEqual(words("/usr/bin/tool --profile dev"), "/usr/bin/tool|--profile|dev", "plain words")
  assertEqual(words("  tool \t a\n  b  "), "tool|a|b", "any whitespace, and the continuation newline")
  assertEqual(words("\"/opt/my tools/get\" 'two words' x"), "/opt/my tools/get|two words|x", "both quote styles")
  assertEqual(words("tool \"a \\\"quoted\\\" \\\\ word\""), "tool|a \"quoted\" \\ word", "escapes inside double quotes")
  assertEqual(words("tool '' \"\""), "tool||", "empty quoted words are words")
  assertEqual(words("tool $HOME *.txt"), "tool|$HOME|*.txt", "no expansion")
  assertEqual(words("tool \"unclosed"), "error: an unclosed quote", "an unclosed quote")
  assertEqual(words("   "), "error: an empty command", "nothing")
}

func processProfile(dir: in String, script: in String, args: in String): Result[ResolvedCredentials, CredentialError] {
  val scriptPath = dir + "/get creds.sh"
  write(scriptPath, script)
  val config = "[profile proc]\ncredential_process = /bin/sh \"" + scriptPath + "\" " + args + "\n"
  match profileOf(config, "", "proc") {
    case Err(message) -> Err(CredentialError(code = "TEST", provider = "test", message = message))
    case Ok(p) -> credentialsOfProfile(p)
  }
}

test "credential_process runs the command and reads its JSON, with and without an expiry" {
  val dir = scratchDir("process")
  // The script echoes its first argument as the key id: the arguments of the
  // command line reach the command, quoted ones intact.
  val temporary = "printf '{\"Version\": 1, \"AccessKeyId\": \"%s\", \"SecretAccessKey\": \"" + SECRET +
    "\", \"SessionToken\": \"" +
    TOKEN +
    "\", \"Expiration\": \"2026-11-13T10:15:00Z\"}' \"$1\"\n"
  assertEqual(
    describe(processProfile(dir, temporary, "'ASIA PROC'")),
    "ok ASIA PROC from credential_process token=" + TOKEN + " until 1794564900000",
    "temporary credentials"
  )
  val longTerm = "echo '{\"Version\": 1, \"AccessKeyId\": \"AKIAPROC\", \"SecretAccessKey\": \"" + SECRET + "\"}'\n"
  assertEqual(
    describe(processProfile(dir, longTerm, "")),
    "ok AKIAPROC from credential_process",
    "long-term credentials"
  )
  val _ = File.deleteDirRecursive(dir)
}

test "a credential_process that fails is PROCESS_FAILED, and its output is never quoted" {
  val dir = scratchDir("process-fail")
  val failing = "echo 'partial " + SECRET + "'\necho 'oops " + SECRET + "' >&2\nexit 3\n"
  val failed = describe(processProfile(dir, failing, ""))
  assertEqual(
    failed,
    "PROCESS_FAILED profile: the credential_process of profile 'proc' exited with code 3",
    "the exit code"
  )
  assertFalse(Str.contains(failed, SECRET), "neither stream is quoted")
  val notJson = describe(processProfile(dir, "echo 'AccessKeyId=" + SECRET + "'\n", ""))
  assertEqual(
    notJson,
    "MALFORMED_RESPONSE profile: the credential_process of profile 'proc': the output is not JSON",
    "not JSON"
  )
  assertFalse(Str.contains(notJson, SECRET), "the output is not quoted")
  assertEqual(
    describe(processProfile(dir, "echo '{\"Version\": 2, \"AccessKeyId\": \"A\", \"SecretAccessKey\": \"B\"}'\n", "")),
    "MALFORMED_RESPONSE profile: the credential_process of profile 'proc': Version is 2, only 1 is understood",
    "an unknown version"
  )
  assertEqual(
    describe(processProfile(dir, "echo '{\"Version\": 1, \"AccessKeyId\": \"A\"}'\n", "")),
    "MALFORMED_RESPONSE profile: the credential_process of profile 'proc': the output has no SecretAccessKey",
    "no secret"
  )
  assertEqual(
    describe(
      processProfile(
        dir,
        "echo '{\"Version\": 1, \"AccessKeyId\": \"A\", \"SecretAccessKey\": \"" + SECRET +
          "\", \"Expiration\": \"soon\"}'\n",
        ""
      )
    ),
    "MALFORMED_RESPONSE profile: the credential_process of profile 'proc': the Expiration in the output is not an ISO 8601 timestamp",
    "a bad expiry"
  )
  val _ = File.deleteDirRecursive(dir)
  match profileOf("[profile proc]\ncredential_process = /nonexistent/lyric-no-such-command --x\n", "", "proc") {
    case Err(message) -> assertTrue(false, message)
    case Ok(p) -> assertEqual(
      describe(credentialsOfProfile(p)),
      "PROCESS_FAILED profile: the credential_process of profile 'proc' could not be started: /nonexistent/lyric-no-such-command",
      "a command that does not exist"
    )
  }
}

// ─── Settings ────────────────────────────────────────────────────────────────
func settingsText(r: in Result[AwsSettings, CredentialError]): String {
  match r {
    case Ok(s) -> "region=" + s.region + " endpoint=" + s.endpointUrl + " ca=" + s.caBundlePath
    case Err(e) -> e.code + " " + e.text()
  }
}

test "region, endpoint and CA bundle come from the environment first and the profile second" {
  val dir = scratchDir("settings")
  val configPath = dir + "/config"
  write(
    configPath,
    "[default]\nregion = us-east-1\nendpoint_url = https://127.0.0.1:9297\nca_bundle = /etc/ca.pem\ns3 =\n  signature_version = s3v4\n  endpoint_url = https://nested.invalid\n[profile dev]\nregion = eu-west-1\n"
  )
  val values: Map[String, String] = newMap()
  values["AWS_CONFIG_FILE"] = configPath
  values["AWS_SHARED_CREDENTIALS_FILE"] = dir + "/none"
  val lookup = environmentOf(values)
  assertEqual(
    settingsText(settingsFromEnvironment(lookup, "S3")),
    "region=us-east-1 endpoint=https://127.0.0.1:9297 ca=/etc/ca.pem",
    "all three from the default profile; the s3 block's endpoint_url is not the profile's"
  )
  values["AWS_PROFILE"] = "dev"
  assertEqual(settingsText(settingsFromEnvironment(lookup, "S3")), "region=eu-west-1 endpoint= ca=", "another profile")
  values["AWS_PROFILE"] = ""
  values["AWS_DEFAULT_REGION"] = "ap-south-1"
  assertEqual(
    settingsText(settingsFromEnvironment(lookup, "S3")),
    "region=ap-south-1 endpoint=https://127.0.0.1:9297 ca=/etc/ca.pem",
    "AWS_DEFAULT_REGION over the profile"
  )
  values["AWS_REGION"] = "eu-north-1"
  values["AWS_ENDPOINT_URL"] = "https://all.example"
  values["AWS_CA_BUNDLE"] = "/env/ca.pem"
  assertEqual(
    settingsText(settingsFromEnvironment(lookup, "S3")),
    "region=eu-north-1 endpoint=https://all.example ca=/env/ca.pem",
    "AWS_REGION, AWS_ENDPOINT_URL and AWS_CA_BUNDLE over everything below"
  )
  values["AWS_ENDPOINT_URL_S3"] = "https://s3.example"
  assertEqual(
    settingsText(settingsFromEnvironment(lookup, "S3")),
    "region=eu-north-1 endpoint=https://s3.example ca=/env/ca.pem",
    "the service's own variable first"
  )
  assertEqual(
    settingsText(settingsFromEnvironment(lookup, "SQS")),
    "region=eu-north-1 endpoint=https://all.example ca=/env/ca.pem",
    "another service does not see it"
  )
  values["AWS_IGNORE_CONFIGURED_ENDPOINT_URLS"] = "true"
  assertEqual(
    settingsText(settingsFromEnvironment(lookup, "S3")),
    "region=eu-north-1 endpoint= ca=/env/ca.pem",
    "AWS_IGNORE_CONFIGURED_ENDPOINT_URLS"
  )
  val _ = File.deleteDirRecursive(dir)
  val empty: Map[String, String] = newMap()
  assertEqual(
    settingsText(settingsFromEnvironment(environmentOf(empty), "S3")),
    "region= endpoint= ca=",
    "nothing anywhere"
  )
}
