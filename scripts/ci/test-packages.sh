#!/usr/bin/env bash
# Run `lyric test` for every workspace package that claims TARGET in its
# `[package] targets`, on that target only (CLAUDE.md: CI runs each package on
# exactly the targets it claims).
#
# Usage: scripts/ci/test-packages.sh <dotnet|jvm> [--integration]
#
# Default: the hermetic suites. With --integration: only the packages that
# declare an `integration` feature, run with `--features integration`, which
# enables their `@cfg(feature = "integration")` suites under tests/integration/
# (they need a real service; the package README names the environment
# variables that locate it).
#
# Environment: LYRIC names the compiler binary (default: `lyric` on PATH).
set -euo pipefail

target="${1:?usage: test-packages.sh <dotnet|jvm> [--integration]}"
mode="${2:-}"
lyric="${LYRIC:-lyric}"
root="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$root"

claims_target() {
  # A manifest without `targets` supports every target.
  local line
  line="$(grep -E '^targets[[:space:]]*=' "$1" || true)"
  [ -z "$line" ] || grep -q "\"$target\"" <<<"$line"
}

failed=()
ran=0
for manifest in */lyric.toml; do
  pkg="$(dirname "$manifest")"
  claims_target "$manifest" || { echo "== $pkg: does not claim $target, skipped"; continue; }
  extra=()
  if [ "$mode" = "--integration" ]; then
    grep -Eq '^integration[[:space:]]*=' "$manifest" || continue
    extra=(--features integration)
  fi
  echo "== $pkg ($manifest, --target $target)"
  ran=$((ran + 1))
  if ! "$lyric" restore --manifest "$manifest"; then
    failed+=("$pkg (restore)")
    continue
  fi
  if ! "$lyric" test --manifest "$manifest" --target "$target" "${extra[@]}"; then
    failed+=("$pkg")
  fi
done

if [ "$ran" -eq 0 ]; then
  echo "error: no package ran for --target $target $mode" >&2
  exit 1
fi
if [ "${#failed[@]}" -gt 0 ]; then
  echo "FAILED: ${failed[*]}" >&2
  exit 1
fi
echo "all $ran package(s) passed on $target"
