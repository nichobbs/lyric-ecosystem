#!/usr/bin/env bash
# Run `lyric test --target native` for every workspace package that claims
# `native` in its `[package] targets`.
#
# Usage: scripts/ci/test-native.sh [--integration]
#
# Default: the hermetic suites. With --integration: only the packages that
# declare an `integration` feature, run with `--features integration` (they
# need a real service; the package README names the environment variables
# that locate it).
#
# A native test run compiles the package and its workspace dependencies from
# source into one executable per `[project.tests]` entry and runs it; there is
# nothing to restore.
#
# Needs clang and the native runtime (`make -C ../lyric-lang/lyric-rt`).
# Environment: LYRIC names the compiler binary (default: `lyric` on PATH).
set -euo pipefail

mode="${1:-}"
lyric="${LYRIC:-lyric}"
root="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$root"

failed=()
ran=0
for manifest in */lyric.toml; do
  pkg="$(dirname "$manifest")"
  grep -E '^targets[[:space:]]*=' "$manifest" | grep -q '"native"' || continue
  extra=()
  if [ "$mode" = "--integration" ]; then
    grep -Eq '^integration[[:space:]]*=' "$manifest" || continue
    extra=(--features integration)
  fi
  echo "== $pkg ($manifest, --target native)"
  ran=$((ran + 1))
  if ! "$lyric" test --manifest "$manifest" --target native "${extra[@]}"; then
    failed+=("$pkg")
  fi
done

if [ "$ran" -eq 0 ]; then
  echo "error: no package ran for --target native $mode" >&2
  exit 1
fi
if [ "${#failed[@]}" -gt 0 ]; then
  echo "FAILED: ${failed[*]}" >&2
  exit 1
fi
echo "all $ran package(s) passed on native"
