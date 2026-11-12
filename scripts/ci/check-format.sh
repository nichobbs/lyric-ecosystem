#!/usr/bin/env bash
# Fail when a tracked .l file is not in `lyric fmt` canonical form.
# Environment: LYRIC names the compiler binary (default: `lyric` on PATH).
set -euo pipefail

lyric="${LYRIC:-lyric}"
cd "$(dirname "$0")/../.."

"$lyric" --version >/dev/null || { echo "error: cannot run the compiler ($lyric); set LYRIC" >&2; exit 2; }

status=0
while IFS= read -r file; do
  if ! "$lyric" fmt --check "$file" >/dev/null 2>&1; then
    echo "not formatted (run: lyric fmt --write $file): $file" >&2
    status=1
  fi
done < <(git ls-files '*.l')
exit "$status"
