//! Aws.Credentials.Chain - the AWS default credential provider chain.
@runtime_checked
@io
package Aws.Credentials.Chain

import Std.Core
import Std.Collections
import Aws.Credentials
import Aws.Credentials.Profile
import Aws.Credentials.Remote

/// The default chain over `lookup`, in the AWS SDKs' order:
///
///   1. the environment (`AWS_ACCESS_KEY_ID`, ...);
///   2. web identity (`AWS_WEB_IDENTITY_TOKEN_FILE`, `AWS_ROLE_ARN`);
///   3. the shared config and credentials files (`AWS_PROFILE`);
///   4. the container credential endpoint (`AWS_CONTAINER_CREDENTIALS_*`);
///   5. the EC2 instance metadata service.
///
/// It is not cached: every `resolve` walks the chain. See `chainOf` for which
/// errors end it.
pub func defaultChain(lookup: in (String) -> String): CredentialProvider {
  val providers: List[CredentialProvider] = newList()
  providers.add(environmentProvider(lookup))
  providers.add(webIdentityProvider(lookup))
  providers.add(profileProvider(lookup))
  providers.add(containerProvider(lookup))
  providers.add(imdsProvider(lookup))
  chainOf(providers)
}

/// The default chain over the process environment, behind a cache
/// (`defaultCacheOptions()`): what an AWS client uses when it is given no
/// credentials. Safe to share between clients and threads.
pub func defaultProvider(): CredentialProvider {
  cached(defaultChain(processEnvironment()))
}
