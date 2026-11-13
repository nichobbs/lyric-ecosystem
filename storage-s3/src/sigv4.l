//! Storage.S3.SigV4 - what Amazon S3 adds to AWS Signature Version 4.
//!
//! The signing itself is `Aws.SigV4` (package `Lyric.Aws.SigV4`), shared with
//! the other AWS backends and re-exported here, so `import Storage.S3.SigV4`
//! still brings `Pair`, `Credentials`, `signRequest`, `presignQuery` and the
//! rest into scope. What is S3's own is how an object key becomes a canonical
//! URI: percent-encoded once, with `/` left alone and no path normalisation.
@runtime_checked
package Storage.S3.SigV4

import Std.Core
import Aws.SigV4
pub use Aws.SigV4

/// The canonical URI of an object: `/` followed by the key, encoded once with
/// `/` kept.
@pure
pub func canonicalKeyPath(key: in String): String {
  canonicalUri("/" + key, true)
}
