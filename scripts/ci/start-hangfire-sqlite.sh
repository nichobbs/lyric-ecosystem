#!/usr/bin/env bash
# Prepare the jobs-hangfire integration suite, which runs the backend on a
# persistent Hangfire storage (Hangfire.Storage.SQLite). No server is
# started: SQLite is a library. Two things are needed:
#
#   - a writable directory for the database files (a fresh temporary one);
#   - the native SQLite library `e_sqlite3` on the library search path. It
#     ships in the NuGet package SQLitePCLRaw.lib.e_sqlite3, which
#     `lyric restore` fetches, but `lyric test` does not copy native NuGet
#     assets next to the test program, so this script copies the library for
#     the runner's architecture into a directory of its own and puts that
#     directory on LD_LIBRARY_PATH.
#
# Linux only (x64 and arm64).
#
# Under GitHub Actions the values are exported through $GITHUB_ENV; otherwise
# they are printed as `export NAME=value` lines, so a shell can
# `eval "$(scripts/ci/start-hangfire-sqlite.sh)"`:
#
#   LYRIC_HANGFIRE_SQLITE_DIR  the directory for database files
#   LD_LIBRARY_PATH            with the directory of libe_sqlite3.so in front
#
# Environment: LYRIC names the compiler binary (default: `lyric` on PATH);
# NUGET_PACKAGES names the NuGet package cache (default: ~/.nuget/packages).
set -euo pipefail

lyric="${LYRIC:-lyric}"
root="$(cd "$(dirname "$0")/../.." && pwd)"
manifest="$root/jobs-hangfire/lyric.toml"

# The version pinned in the manifest.
version="$(sed -n 's/^"SQLitePCLRaw.lib.e_sqlite3"[[:space:]]*=[[:space:]]*"\(.*\)"$/\1/p' "$manifest")"
if [ -z "$version" ]; then
  echo "error: $manifest does not pin SQLitePCLRaw.lib.e_sqlite3" >&2
  exit 1
fi

case "$(uname -s)-$(uname -m)" in
  Linux-x86_64) rid=linux-x64 ;;
  Linux-aarch64) rid=linux-arm64 ;;
  *)
    echo "error: no e_sqlite3 set up for $(uname -s) $(uname -m); this script covers Linux x64 and arm64" >&2
    exit 1
    ;;
esac

packages="${NUGET_PACKAGES:-$HOME/.nuget/packages}"
library="$packages/sqlitepclraw.lib.e_sqlite3/$version/runtimes/$rid/native/libe_sqlite3.so"
if [ ! -f "$library" ]; then
  (cd "$root" && "$lyric" restore --manifest jobs-hangfire/lyric.toml) >&2
fi
if [ ! -f "$library" ]; then
  echo "error: $library is missing after lyric restore" >&2
  exit 1
fi

work="$(mktemp -d)"
mkdir -p "$work/native" "$work/db"
cp "$library" "$work/native/"

emit() {
  if [ -n "${GITHUB_ENV:-}" ]; then
    echo "$1=$2" >> "$GITHUB_ENV"
  else
    echo "export $1=$2"
  fi
}
emit LYRIC_HANGFIRE_SQLITE_DIR "$work/db"
emit LD_LIBRARY_PATH "$work/native${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
if [ -n "${GITHUB_ENV:-}" ]; then
  echo "e_sqlite3 $version ($rid) is on LD_LIBRARY_PATH; database files go to $work/db"
fi
