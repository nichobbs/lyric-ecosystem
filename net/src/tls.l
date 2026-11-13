//! Net.Tls - how a client secures a connection: the options record, the
//! functions that build it, its validation, the two-key policy for switching
//! verification off, and a reader of the options from environment variables.
//!
//! The option names are those of `Std.Http`'s `HttpClientBuilder`
//! (lyric-lang docs/61): `withTls`, `withCaCertificate`,
//! `withExclusiveCaCertificate`, `withClientIdentity`, `withTlsServerName`,
//! `withMinTlsVersion`, `withInsecureSkipVerify`.
//!
//! ```
//! import Net.Tls
//!
//! val tls = plainTcp().withTls().withCaCertificate(caPem).withClientIdentity(certPem, keyPem)
//! match validateTlsOptions(tls) {
//!   case Ok(_) -> ()
//!   case Err(problem) -> println(tlsProblemText(problem, "a tcps:// URL"))
//! }
//! ```
//!
//! Certificates and keys are PEM text, not `Std.Tls.Certificate` or
//! `Std.Tls.Identity` values: those are opaque to anything outside the
//! standard library, so a connection made here could not use them
//! (nichobbs/lyric-lang#9064). The PEM is checked with the `Std.Tls` loaders
//! (`validateTlsOptions`), so what is accepted is exactly what `Std.Tls`
//! accepts: one certificate per PEM text, a private key as an unencrypted
//! PKCS8 `PRIVATE KEY` block.
//!
//! Switching verification off takes two keys (docs/61 section 4): the option
//! (`withInsecureSkipVerify`) and the environment variable
//! `LYRIC_TLS_ALLOW_INSECURE=1`. Either alone leaves verification on.
@runtime_checked
@io
package Net.Tls

import Std.Core
import Std.Environment as Env
import Std.File as File
import Std.String as Str
import Std.Tls as Tls

/// TLS settings of a connection. PEM text; an empty string means "not
/// configured". Build one from `plainTcp()` with the `with...` functions.
pub record TlsOptions {
  /// Whether the connection is TLS at all.
  enabled: Bool
  /// The name sent as SNI and checked against the server certificate; empty
  /// uses the host the connection is made to.
  serverName: String
  /// A trust anchor accepted in addition to the platform trust store.
  caCertificatePem: String
  /// A trust anchor that replaces the platform trust store. Wins over
  /// `caCertificatePem` when both are set.
  exclusiveCaCertificatePem: String
  /// The client certificate and its private key, for a server that requires
  /// client authentication. The key is an unencrypted PKCS8 `PRIVATE KEY`
  /// block, as for `Std.Tls.Identity`.
  clientCertificatePem: String
  clientKeyPem: String
  /// `Tls12` (the default) accepts TLS 1.2 and later; `Tls13` only TLS 1.3.
  minVersion: Tls.TlsVersion
  /// A request to skip certificate and name verification. Takes effect only
  /// when `LYRIC_TLS_ALLOW_INSECURE=1` is also set in the environment.
  insecureSkipVerify: Bool
}

/// No TLS: the connection is plain TCP.
pub func plainTcp(): TlsOptions {
  TlsOptions(
    enabled = false,
    serverName = "",
    caCertificatePem = "",
    exclusiveCaCertificatePem = "",
    clientCertificatePem = "",
    clientKeyPem = "",
    minVersion = Tls.TlsVersion.Tls12,
    insecureSkipVerify = false
  )
}

/// Connect over TLS, verifying the server certificate against the platform
/// trust store and its name against the host.
pub func TlsOptions.withTls(t: in TlsOptions): TlsOptions {
  t.copy(enabled = true)
}

/// Check the server certificate against `name` instead of the host, and send
/// `name` as SNI. For a server reached by address or through a tunnel.
pub func TlsOptions.withTlsServerName(t: in TlsOptions, name: in String): TlsOptions {
  t.copy(serverName = name)
}

/// Trust the CA certificate in `pem` in addition to the platform trust
/// store. `pem` holds one certificate.
pub func TlsOptions.withCaCertificate(t: in TlsOptions, pem: in String): TlsOptions {
  t.copy(caCertificatePem = pem)
}

/// Trust only the CA certificate in `pem`; the platform trust store is not
/// consulted. Wins over `withCaCertificate`.
pub func TlsOptions.withExclusiveCaCertificate(t: in TlsOptions, pem: in String): TlsOptions {
  t.copy(exclusiveCaCertificatePem = pem)
}

/// Present a client certificate (mutual TLS). `keyPem` is the certificate's
/// unencrypted PKCS8 private key.
pub func TlsOptions.withClientIdentity(t: in TlsOptions, certificatePem: in String, keyPem: in String): TlsOptions {
  t.copy(clientCertificatePem = certificatePem, clientKeyPem = keyPem)
}

/// Refuse a TLS version older than `version`.
pub func TlsOptions.withMinTlsVersion(t: in TlsOptions, version: in Tls.TlsVersion): TlsOptions {
  t.copy(minVersion = version)
}

/// Ask for certificate and name verification to be skipped. This alone does
/// nothing: `LYRIC_TLS_ALLOW_INSECURE=1` must also be set when the connection
/// is opened (`skipsVerification`). Never for production.
pub func TlsOptions.withInsecureSkipVerify(t: in TlsOptions): TlsOptions {
  t.copy(insecureSkipVerify = true)
}

/// Whether `t` accepts TLS 1.3 only.
pub func requiresTls13(t: in TlsOptions): Bool {
  match t.minVersion {
    case Tls13 -> true
    case Tls12 -> false
  }
}

/// Whether any option other than `enabled` differs from `plainTcp()`.
pub func hasTlsSettings(t: in TlsOptions): Bool {
  t.serverName.length > 0 or t.caCertificatePem.length > 0 or t.exclusiveCaCertificatePem.length > 0 or
    t.clientCertificatePem.length > 0 or
    t.clientKeyPem.length > 0 or
    t.insecureSkipVerify or
    requiresTls13(t)
}

/// What is wrong with a `TlsOptions`.
pub union TlsProblem {
  /// Options are set but `enabled` is not.
  case TlsNotEnabled
  /// `caCertificatePem` is not a certificate `Std.Tls` loads.
  case CaCertificateUnusable(reason: String)
  /// `exclusiveCaCertificatePem` is not a certificate `Std.Tls` loads.
  case ExclusiveCaCertificateUnusable(reason: String)
  case ClientKeyWithoutCertificate
  case ClientCertificateWithoutKey
  /// The client certificate or key does not load, or the key is not the
  /// certificate's.
  case ClientIdentityUnusable(reason: String)
}

/// `problem` as text. `howToEnable` completes "use ... or withTls()", for
/// example `"a stomp+ssl:// URL"`.
pub func tlsProblemText(problem: in TlsProblem, howToEnable: in String): String {
  match problem {
    case TlsNotEnabled -> "TLS options are set but the connection is not TLS; use " + howToEnable + " or withTls()"
    case CaCertificateUnusable(reason) -> "TLS CA certificate: " + reason
    case ExclusiveCaCertificateUnusable(reason) -> "TLS exclusive CA certificate: " + reason
    case ClientKeyWithoutCertificate -> "TLS client identity: a private key is set without its certificate"
    case ClientCertificateWithoutKey -> "TLS client identity: a certificate is set without its private key"
    case ClientIdentityUnusable(reason) -> "TLS client identity: " + reason
  }
}

func certificateFailure(pem: in String): String {
  if pem.length == 0 {
    return ""
  }
  match Tls.Certificate.fromPem(pem) {
    case Ok(_) -> ""
    case Err(e) -> e.message()
  }
}

/// Check `t` without connecting. The PEM is parsed with the standard
/// library's loaders, so a bad certificate or key is reported before any
/// connection is made and with the same rules on both targets.
pub func validateTlsOptions(t: in TlsOptions): Result[Unit, TlsProblem] {
  if not t.enabled {
    if hasTlsSettings(t) {
      return Err(TlsNotEnabled)
    }
    return Ok(())
  }
  val ca = certificateFailure(t.caCertificatePem)
  if ca.length > 0 {
    return Err(CaCertificateUnusable(reason = ca))
  }
  val exclusiveCa = certificateFailure(t.exclusiveCaCertificatePem)
  if exclusiveCa.length > 0 {
    return Err(ExclusiveCaCertificateUnusable(reason = exclusiveCa))
  }
  if t.clientCertificatePem.length == 0 and t.clientKeyPem.length == 0 {
    return Ok(())
  }
  if t.clientCertificatePem.length == 0 {
    return Err(ClientKeyWithoutCertificate)
  }
  if t.clientKeyPem.length == 0 {
    return Err(ClientCertificateWithoutKey)
  }
  match Tls.Identity.fromPem(t.clientCertificatePem, t.clientKeyPem) {
    case Ok(_) -> Ok(())
    case Err(e) -> Err(ClientIdentityUnusable(reason = e.message()))
  }
}

/// The name of the environment variable that must be `1` for
/// `withInsecureSkipVerify()` to take effect: `LYRIC_TLS_ALLOW_INSECURE`, the
/// standard library's (docs/61 section 4 in lyric-lang).
pub func insecureAllowEnvName(): String {
  "LYRIC_TLS_ALLOW_INSECURE"
}

/// Whether the environment holds the second key of the insecure policy
/// (`LYRIC_TLS_ALLOW_INSECURE=1`).
pub func insecureAllowedByEnvironment(): Bool {
  Env.getVarOrDefault(insecureAllowEnvName(), "") == "1"
}

/// Whether a connection opened with `t` now would skip verification: only
/// when the options ask for it and the environment allows it.
pub func skipsVerification(t: in TlsOptions): Bool {
  t.enabled and t.insecureSkipVerify and insecureAllowedByEnvironment()
}

/// The warning to print when `t` asks for verification to be skipped and the
/// environment does not allow it, `""` otherwise. `component` names who
/// warns, for example `"Mq.ActiveMq"`.
pub func insecureIgnoredWarning(t: in TlsOptions, component: in String): String {
  if t.enabled and t.insecureSkipVerify and not insecureAllowedByEnvironment() {
    "[" + component + "] warning: withInsecureSkipVerify() requested but " + insecureAllowEnvName() +
      " is not set; TLS certificate verification remains ENABLED."
  } else {
    ""
  }
}

func envOr(name: in String, fallback: in String): String {
  val value = Env.getVarOrDefault(name, "")
  if value.length > 0 {
    value
  } else {
    fallback
  }
}

// The text of the PEM file named by the variable `name`; `Ok("")` when the
// variable is unset.
func pemFromEnv(name: in String): Result[String, String] {
  val path = envOr(name, "")
  if path.length == 0 {
    return Ok("")
  }
  match File.readText(path) {
    case Ok(text) -> Ok(text)
    case Err(_) -> Err("cannot read the file named by " + name + " (" + path + ")")
  }
}

/// `base` with the TLS settings named by the environment variables that start
/// with `prefix` (a variable that is empty counts as unset):
///
/// - `<prefix>TLSCAFILE`: a PEM file with a CA certificate to trust in
///   addition to the platform trust store
/// - `<prefix>TLSCAEXCLUSIVE`: `true` to trust only that CA
/// - `<prefix>TLSCERTFILE`, `<prefix>TLSKEYFILE`: PEM files with the client
///   certificate and its PKCS8 private key (mutual TLS)
/// - `<prefix>TLSSERVERNAME`: the name to check the server certificate
///   against
/// - `<prefix>TLSMINVERSION`: `1.2` (default) or `1.3`
///
/// It neither switches TLS on nor verification off: the first is the
/// caller's (a URL scheme, usually), the second takes
/// `withInsecureSkipVerify()` in code. The `Err` text names the variable.
pub func tlsOptionsFromEnv(prefix: in String, base: in TlsOptions): Result[TlsOptions, String] {
  var options = base
  val ca = match pemFromEnv(prefix + "TLSCAFILE") { case Ok(t) -> t; case Err(e) -> return Err(e) }
  val exclusiveText = Str.toLower(envOr(prefix + "TLSCAEXCLUSIVE", "false"))
  if exclusiveText != "true" and exclusiveText != "false" {
    return Err(prefix + "TLSCAEXCLUSIVE must be true or false")
  }
  if ca.length > 0 {
    options = if exclusiveText == "true" { options.withExclusiveCaCertificate(ca) } else { options.withCaCertificate(ca) }
  } else if exclusiveText == "true" {
    return Err(prefix + "TLSCAEXCLUSIVE is set without " + prefix + "TLSCAFILE")
  }
  val certificate = match pemFromEnv(prefix + "TLSCERTFILE") { case Ok(t) -> t; case Err(e) -> return Err(e) }
  val key = match pemFromEnv(prefix + "TLSKEYFILE") { case Ok(t) -> t; case Err(e) -> return Err(e) }
  if certificate.length > 0 or key.length > 0 {
    options = options.withClientIdentity(certificate, key)
  }
  val serverName = envOr(prefix + "TLSSERVERNAME", "")
  if serverName.length > 0 {
    options = options.withTlsServerName(serverName)
  }
  val versionText = envOr(prefix + "TLSMINVERSION", "1.2")
  if versionText == "1.3" {
    options = options.withMinTlsVersion(Tls.TlsVersion.Tls13)
  } else if versionText != "1.2" {
    return Err(prefix + "TLSMINVERSION must be 1.2 or 1.3")
  }
  Ok(options)
}
