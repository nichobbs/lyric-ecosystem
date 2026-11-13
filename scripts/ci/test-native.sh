#!/usr/bin/env bash
# Run the suites of every workspace package that claims `native` in its
# `[package] targets`, on --target native.
#
# Usage: scripts/ci/test-native.sh [--integration]
#
# `lyric test --manifest ... --target native` is refused today
# (nichobbs/lyric-lang#6815, item 3(b)), so scripts/ci/native-tests.py builds
# each `[project.tests]` module into a native executable the way that command
# will, and runs it. When the command exists this script becomes
# `test-packages.sh native`.
#
# Needs clang, python3 and the native runtime (`make -C ../lyric-lang/lyric-rt`).
# Environment: LYRIC names the compiler binary (default: `lyric` on PATH).
set -euo pipefail

mode="${1:-}"
lyric="${LYRIC:-lyric}"
root="$(cd "$(dirname "$0")/../.." && pwd)"
work="$(mktemp -d)"
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
  echo "== $pkg (--target native)"
  ran=$((ran + 1))
  python3 -I scripts/ci/native-tests.py "$lyric" "$root" "$pkg" "$work/$pkg" "${extra[@]}" || failed+=("$pkg")
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
