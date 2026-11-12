//! Aws.SigV4.Env - what a signer takes from its surroundings: credentials and
//! the region from the standard AWS environment variables, and the current
//! instant in the `x-amz-date` form.
//!
//! Only the environment is consulted. Shared credential files
//! (`~/.aws/credentials`), profiles, SSO, the container and instance metadata
//! endpoints and `AssumeRole` are not; an application that uses those resolves
//! its credentials itself and passes them explicitly.
@runtime_checked
@io
package Aws.SigV4.Env

import Std.Core
import Std.Environment as Env
import Std.Time as Time
import Aws.SigV4

val ACCESS_KEY_ID_VAR: String = "AWS_ACCESS_KEY_ID"

val SECRET_ACCESS_KEY_VAR: String = "AWS_SECRET_ACCESS_KEY"

val SESSION_TOKEN_VAR: String = "AWS_SESSION_TOKEN"

/// Credentials from explicit values. `Err` names what is empty; the secret is
/// never part of a message.
pub func credentialsOf(
  accessKeyId: in String, secretAccessKey: in String, sessionToken: in String
): Result[Credentials, String] {
  if accessKeyId.length == 0 {
    return Err("the access key id is empty")
  }
  if secretAccessKey.length == 0 {
    return Err("the secret access key is empty")
  }
  Ok(Credentials(accessKeyId = accessKeyId, secretAccessKey = secretAccessKey, sessionToken = sessionToken))
}

/// Credentials from `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and the
/// optional `AWS_SESSION_TOKEN`. `Err` is `<variable> is not set` for the
/// first required variable that is unset or empty.
pub func credentialsFromEnv(): Result[Credentials, String] {
  val accessKeyId = Env.getVarOrDefault(ACCESS_KEY_ID_VAR, "")
  if accessKeyId.length == 0 {
    return Err(ACCESS_KEY_ID_VAR + " is not set")
  }
  val secretAccessKey = Env.getVarOrDefault(SECRET_ACCESS_KEY_VAR, "")
  if secretAccessKey.length == 0 {
    return Err(SECRET_ACCESS_KEY_VAR + " is not set")
  }
  Ok(
    Credentials(
      accessKeyId = accessKeyId,
      secretAccessKey = secretAccessKey,
      sessionToken = Env.getVarOrDefault(SESSION_TOKEN_VAR, "")
    )
  )
}

/// The first of `names` that is set to a non-empty value in the environment,
/// or `""`.
pub func firstSet(names: in slice[String]): String {
  for name in names {
    val v = Env.getVarOrDefault(name, "")
    if v.length > 0 {
      return v
    }
  }
  ""
}

/// The region from `AWS_REGION`, else `AWS_DEFAULT_REGION`. `Err` when
/// neither is set.
pub func regionFromEnv(): Result[String, String] {
  val region = firstSet(["AWS_REGION", "AWS_DEFAULT_REGION"])
  if region.length == 0 {
    return Err("neither AWS_REGION nor AWS_DEFAULT_REGION is set")
  }
  Ok(region)
}

/// The endpoint override for `service` (`S3`, `SQS`): `AWS_ENDPOINT_URL_<SERVICE>`,
/// else `AWS_ENDPOINT_URL`, else `""`.
pub func endpointFromEnv(service: in String): String {
  firstSet(["AWS_ENDPOINT_URL_" + service, "AWS_ENDPOINT_URL"])
}

/// `yyyymmddThhmmssZ` for the current instant: the `x-amz-date` of a request
/// signed now.
pub func amzDateNow(): String {
  amzDateOfIso(Time.toIsoString(Time.now()))
}
