#!/usr/bin/env bash
# Run the feature variants of aws-lambda and aws-xray that a plain `lyric test`
# (scripts/ci/test-packages.sh) does not select.
#
# Usage: scripts/ci/test-feature-variants.sh <dotnet|jvm>
#
# aws-lambda selects its `serve()` transport with one of the features `aws`,
# `local` and `jvm`; the plain run compiles none of them, so the shared suites
# are run once per variant that is valid on the target (`aws` and `local` on
# dotnet, `jvm` and `local` on the JVM).
#
# aws-xray records through the AWS X-Ray recorder SDK of the target when built
# with `aws` (NuGet AWSXRayRecorder.Core, dotnet) or `jvm` (Maven
# aws-xray-recorder-sdk-core); its default feature `local` drops subsegments and
# is what the plain run covers. The SDK variants need `lyric restore`.
#
# Environment: LYRIC names the compiler binary (default: `lyric` on PATH).
set -euo pipefail

target="${1:?usage: test-feature-variants.sh <dotnet|jvm>}"
case "$target" in
  dotnet) lambda_variants=(aws local); xray_feature=aws ;;
  jvm) lambda_variants=(jvm local); xray_feature=jvm ;;
  *) echo "error: unknown target $target" >&2; exit 1 ;;
esac
lyric="${LYRIC:-lyric}"
cd "$(dirname "$0")/../.."

failed=()
for feature in "${lambda_variants[@]}"; do
  echo "== aws-lambda --features $feature (--target $target)"
  "$lyric" test --manifest aws-lambda/lyric.toml --target "$target" --features "$feature" || failed+=("aws-lambda:$feature")
done

echo "== aws-xray --features $xray_feature (--target $target)"
if "$lyric" restore --manifest aws-xray/lyric.toml; then
  "$lyric" test --manifest aws-xray/lyric.toml --target "$target" --no-default-features --features "$xray_feature" ||
    failed+=("aws-xray:$xray_feature")
else
  failed+=("aws-xray:$xray_feature (restore)")
fi

if [ "${#failed[@]}" -gt 0 ]; then
  echo "FAILED: ${failed[*]}" >&2
  exit 1
fi
echo "all feature variants passed on $target"
