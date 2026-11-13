//! Storage.AzureBlob.Config - where a container is and which account key
//! signs for it.
//!
//! An `AzureBlobConfig` comes from explicit values (`azureConfig`,
//! `endpointConfig`), from an Azure Storage connection string
//! (`parseConnectionString`), or from the environment (`configFromEnv`).
//! Take the account key from the application's `config { }` block or the
//! environment, never from source.
@runtime_checked
@io
package Storage.AzureBlob.Config

import Std.Core
import Std.Collections
import Std.Environment as Env
import Std.String as Str
import Storage

// The default per-request deadline, 30 seconds.
val DEFAULT_REQUEST_TIMEOUT_MS: Int = 30000

val DEV_ACCOUNT_NAME: String = "devstoreaccount1"

val DEV_ACCOUNT_KEY: String = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="

val DEV_BLOB_ENDPOINT: String = "http://127.0.0.1:10000/devstoreaccount1"

/// The account name of the Azurite emulator and the legacy Storage Emulator:
/// `devstoreaccount1`. A public constant of the emulators, the same on every
/// installation.
@pure
pub func devAccountName(): String {
  DEV_ACCOUNT_NAME
}

/// The account key of the Azurite emulator and the legacy Storage Emulator.
/// Not a secret: Microsoft documents it, it is the same on every installation
/// and it opens nothing but an emulator.
@pure
pub func devAccountKey(): String {
  DEV_ACCOUNT_KEY
}

/// The Blob endpoint an emulator listens on by default:
/// `http://127.0.0.1:10000/devstoreaccount1`.
@pure
pub func devBlobEndpoint(): String {
  DEV_BLOB_ENDPOINT
}

/// Where a container is and how to sign for it.
///
/// `accountKey` is the base64 text Azure issues. `endpoint` is empty for
/// Azure itself (`https://<account>.blob.core.windows.net`), or the Blob
/// endpoint to use instead: `https://<host>[:port]` for a sovereign cloud or
/// a custom domain, or `http(s)://<host>[:port]/<account>` for an emulator,
/// which puts the account name in the path. It must be `https://` unless the
/// host is loopback.
pub record AzureBlobConfig {
  accountName: String
  accountKey: String
  container: String
  endpoint: String
  requestTimeoutMs: Int
}

/// Configuration for a container of a storage account on Azure.
pub func azureConfig(accountName: in String, accountKey: in String, container: in String): AzureBlobConfig {
  AzureBlobConfig(
    accountName = accountName,
    accountKey = accountKey,
    container = container,
    endpoint = "",
    requestTimeoutMs = DEFAULT_REQUEST_TIMEOUT_MS
  )
}

/// Configuration for a container behind the Blob endpoint `endpoint`
/// (an emulator, a sovereign cloud, a custom domain).
pub func endpointConfig(
  endpoint: in String, accountName: in String, accountKey: in String, container: in String
): AzureBlobConfig {
  AzureBlobConfig(
    accountName = accountName,
    accountKey = accountKey,
    container = container,
    endpoint = endpoint,
    requestTimeoutMs = DEFAULT_REQUEST_TIMEOUT_MS
  )
}

/// Configuration for a container of an emulator (Azurite) on its default
/// port, with the emulators' well-known development account.
pub func emulatorConfig(container: in String): AzureBlobConfig {
  endpointConfig(DEV_BLOB_ENDPOINT, DEV_ACCOUNT_NAME, DEV_ACCOUNT_KEY, container)
}

/// `cfg` with a per-request deadline in milliseconds.
pub func AzureBlobConfig.withRequestTimeout(cfg: in AzureBlobConfig, requestTimeoutMs: in Int): AzureBlobConfig {
  AzureBlobConfig(
    accountName = cfg.accountName,
    accountKey = cfg.accountKey,
    container = cfg.container,
    endpoint = cfg.endpoint,
    requestTimeoutMs = requestTimeoutMs
  )
}

func isLowerOrDigit(c: in Char): Bool {
  (c >= 'a' and c <= 'z') or (c >= '0' and c <= '9')
}

/// True for a storage account name: 3 to 24 lowercase letters and digits.
/// Checked before the name is built into a host name.
@pure
pub func isValidAccountName(name: in String): Bool {
  if name.length < 3 or name.length > 24 {
    return false
  }
  var i = 0
  while i < name.length {
    if not isLowerOrDigit(Str.charAtOrReplacement(name, i)) {
      return false
    }
    i = i + 1
  }
  true
}

/// True for a container name Azure accepts: 3 to 63 lowercase letters,
/// digits and `-`, starting and ending with a letter or digit, with no two
/// `-` in a row. The system containers (`$root`, `$web`, `$logs`) are not
/// accepted.
@pure
pub func isValidContainerName(name: in String): Bool {
  if name.length < 3 or name.length > 63 {
    return false
  }
  var i = 0
  while i < name.length {
    val c = Str.charAtOrReplacement(name, i)
    if not (isLowerOrDigit(c) or c == '-') {
      return false
    }
    i = i + 1
  }
  isLowerOrDigit(Str.charAtOrReplacement(name, 0)) and isLowerOrDigit(Str.charAtOrReplacement(name, name.length - 1)) and
    not Str.contains(name, "--")
}

func invalidConfig(message: in String): StorageError {
  StorageError(message = message, code = "INVALID_CONFIG")
}

// The settings of a connection string this backend reads or knowingly
// ignores, lowercase.
func isKnownSetting(lower: in String): Bool {
  lower == "defaultendpointsprotocol" or lower == "accountname" or lower == "accountkey" or lower == "blobendpoint" or
    lower == "endpointsuffix" or
    lower == "usedevelopmentstorage" or
    lower == "developmentstorageproxyuri" or
    lower == "sharedaccesssignature" or
    lower == "queueendpoint" or
    lower == "tableendpoint" or
    lower == "fileendpoint"
}

func trimTrailingSlash(text: in String): String {
  if Str.endsWith(text, "/") {
    text.substring(0, text.length - 1)
  } else {
    text
  }
}

/// Configuration for `container` from an Azure Storage connection string:
/// `Name=Value` settings separated by `;`, names in any case.
///
///   DefaultEndpointsProtocol=https;AccountName=...;AccountKey=...;EndpointSuffix=core.windows.net
///   AccountName=...;AccountKey=...;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1
///   UseDevelopmentStorage=true
///
/// `BlobEndpoint` wins over the endpoint built from the protocol, account
/// name and `EndpointSuffix`. `UseDevelopmentStorage=true` is the emulators'
/// development account on `127.0.0.1:10000` (or on the host of
/// `DevelopmentStorageProxyUri`). The queue, table and file endpoints are
/// ignored.
///
/// `Err(INVALID_CONFIG)` for a setting without `=`, a repeated or unknown
/// setting, a missing account name or key, or a `SharedAccessSignature`
/// (this backend signs with the account key). The message names the
/// setting and never repeats a value.
pub func parseConnectionString(connectionString: in String, container: in String): Result[AzureBlobConfig, StorageError] {
  val settings: Map[String, String] = newMap()
  val parts = Str.split(connectionString, ";")
  var i = 0
  while i < parts.length {
    val part = Str.trim(parts[i])
    if part.length > 0 {
      val eq = Str.indexOfRaw(part, "=")
      if eq <= 0 {
        return Err(invalidConfig("connection string holds a setting that is not Name=Value"))
      }
      val name = Str.trim(part.substring(0, eq))
      val lower = Str.toLower(name)
      if not isKnownSetting(lower) {
        return Err(invalidConfig("connection string holds an unknown setting '" + name + "'"))
      }
      if settings.containsKey(lower) {
        return Err(invalidConfig("connection string sets '" + name + "' more than once"))
      }
      // An account key ends in `=` padding: everything after the first `=`
      // is the value.
      settings.add(lower, Str.trim(part.substring(eq + 1)))
    }
    i = i + 1
  }
  if settings.containsKey("sharedaccesssignature") {
    return Err(
      invalidConfig(
        "connection string carries a SharedAccessSignature: this backend authorizes with the account key (AccountKey)"
      )
    )
  }
  val blobEndpoint = match mapGet(settings, "blobendpoint") { case Some(v) -> v; case None -> "" }
  val development = match mapGet(settings, "usedevelopmentstorage") {
    case Some(v) -> Str.toLower(v) == "true"
    case None -> false
  }
  if development {
    val proxy = match mapGet(settings, "developmentstorageproxyuri") {
      case Some(v) -> trimTrailingSlash(v)
      case None -> "http://127.0.0.1"
    }
    val endpoint = if blobEndpoint.length > 0 { blobEndpoint } else { proxy + ":10000/" + DEV_ACCOUNT_NAME }
    return Ok(endpointConfig(endpoint, DEV_ACCOUNT_NAME, DEV_ACCOUNT_KEY, container))
  }
  val accountName = match mapGet(settings, "accountname") { case Some(v) -> v; case None -> "" }
  if accountName.length == 0 {
    return Err(invalidConfig("connection string has no AccountName"))
  }
  val accountKey = match mapGet(settings, "accountkey") { case Some(v) -> v; case None -> "" }
  if accountKey.length == 0 {
    return Err(invalidConfig("connection string has no AccountKey"))
  }
  if blobEndpoint.length > 0 {
    return Ok(endpointConfig(blobEndpoint, accountName, accountKey, container))
  }
  val protocol = match mapGet(settings, "defaultendpointsprotocol") {
    case Some(v) -> Str.toLower(v)
    case None -> "https"
  }
  if protocol != "https" and protocol != "http" {
    return Err(invalidConfig("connection string DefaultEndpointsProtocol must be https or http"))
  }
  val suffix = match mapGet(settings, "endpointsuffix") { case Some(v) -> v; case None -> "core.windows.net" }
  if protocol == "https" and suffix == "core.windows.net" {
    return Ok(azureConfig(accountName, accountKey, container))
  }
  Ok(endpointConfig(protocol + "://" + accountName + ".blob." + suffix, accountName, accountKey, container))
}

/// Configuration for `container` from the environment, as the Azure CLI
/// reads it: `AZURE_STORAGE_CONNECTION_STRING` when it is set, otherwise
/// `AZURE_STORAGE_ACCOUNT` and `AZURE_STORAGE_KEY`, with an optional Blob
/// endpoint in `AZURE_STORAGE_SERVICE_ENDPOINT`. `Err(INVALID_CONFIG)` names
/// the variable that is missing.
pub func configFromEnv(container: in String): Result[AzureBlobConfig, StorageError] {
  val connectionString = Env.getVarOrDefault("AZURE_STORAGE_CONNECTION_STRING", "")
  if connectionString.length > 0 {
    return parseConnectionString(connectionString, container)
  }
  val accountName = Env.getVarOrDefault("AZURE_STORAGE_ACCOUNT", "")
  if accountName.length == 0 {
    return Err(invalidConfig("neither AZURE_STORAGE_CONNECTION_STRING nor AZURE_STORAGE_ACCOUNT is set"))
  }
  val accountKey = Env.getVarOrDefault("AZURE_STORAGE_KEY", "")
  if accountKey.length == 0 {
    return Err(invalidConfig("AZURE_STORAGE_KEY is not set"))
  }
  Ok(endpointConfig(Env.getVarOrDefault("AZURE_STORAGE_SERVICE_ENDPOINT", ""), accountName, accountKey, container))
}
