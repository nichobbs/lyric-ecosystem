#!/usr/bin/env bash
# Start a disposable PostgreSQL server on 127.0.0.1 for the jobs-hangfire
# integration suite, which runs the backend on a persistent Hangfire storage
# (Hangfire.PostgreSql).
#
#   19485  PostgreSQL            (override: LYRIC_HANGFIRE_POSTGRES_PORT)
#
# The data directory is a fresh temporary one and the password of the one
# user is generated on every run; nothing is reused, and no existing cluster
# is touched. The server keeps running after the script returns. The suite
# creates a schema of its own per test (`lyric_it_<uuid>`) in the database
# `hangfire_it` and leaves it there.
#
# PostgreSQL refuses to run as root: when the script is root, the server runs
# as the `postgres` user that the package creates.
#
# Under GitHub Actions the connection string is exported through $GITHUB_ENV
# (the password masked); otherwise it is printed as an `export NAME='value'`
# line, so a shell can `eval "$(scripts/ci/start-hangfire-postgres.sh)"`:
#
#   LYRIC_HANGFIRE_POSTGRES  Host=127.0.0.1;Port=19485;Database=hangfire_it;Username=lyric;Password=...
set -euo pipefail

port="${LYRIC_HANGFIRE_POSTGRES_PORT:-19485}"

as_root() {
  if [ "$(id -u)" = 0 ]; then "$@"; else sudo "$@"; fi
}

# The directory of the newest installed server binaries, or nothing.
find_bin() {
  local dir
  if command -v pg_ctl > /dev/null && command -v initdb > /dev/null; then
    dirname "$(command -v pg_ctl)"
    return 0
  fi
  for dir in /usr/lib/postgresql/*/bin; do
    if [ -x "$dir/pg_ctl" ] && [ -x "$dir/initdb" ]; then
      echo "$dir"
    fi
  done | sort -V | tail -1
}

bin="$(find_bin)"
if [ -z "$bin" ]; then
  # The package starts a server on 5432 when it is installed; policy-rc.d
  # stops that, since only the server started below is wanted.
  printf '#!/bin/sh\nexit 101\n' | as_root tee /usr/sbin/policy-rc.d > /dev/null
  as_root chmod +x /usr/sbin/policy-rc.d
  as_root apt-get update -qq >&2
  as_root apt-get install -y --no-install-recommends postgresql >&2
  as_root rm -f /usr/sbin/policy-rc.d
  bin="$(find_bin)"
fi
if [ -z "$bin" ]; then
  echo "error: no PostgreSQL server binaries (pg_ctl, initdb) found" >&2
  exit 1
fi
command -v openssl > /dev/null || { echo "error: openssl is not installed" >&2; exit 1; }

if (echo > "/dev/tcp/127.0.0.1/$port") 2> /dev/null; then
  echo "error: something already listens on 127.0.0.1:$port" >&2
  exit 1
fi

work="$(mktemp -d)"
chmod 755 "$work"
password="$(openssl rand -hex 16)"
(umask 077 && printf '%s\n' "$password" > "$work/pwfile")

# pg <program> <arguments>...: run a PostgreSQL program as a user that may.
if [ "$(id -u)" = 0 ]; then
  id postgres > /dev/null 2>&1 || { echo "error: running as root and there is no 'postgres' user" >&2; exit 1; }
  chown -R postgres "$work"
  pg() {
    local program="$1"
    shift
    runuser -u postgres -- "$bin/$program" "$@"
  }
else
  pg() {
    local program="$1"
    shift
    "$bin/$program" "$@"
  }
fi

if ! pg initdb -D "$work/data" -U lyric --auth=scram-sha-256 --pwfile="$work/pwfile" \
  -E UTF8 --locale=C > "$work/initdb.log" 2>&1; then
  echo "error: initdb failed; log follows" >&2
  cat "$work/initdb.log" >&2
  exit 1
fi
rm -f "$work/pwfile"

# Only the loopback address; the socket directory is the work directory, so
# that nothing is written to /var/run/postgresql.
if ! pg pg_ctl -D "$work/data" -l "$work/postgres.log" -w -t 60 \
  -o "-p $port -c listen_addresses=127.0.0.1 -c unix_socket_directories=$work -c max_connections=200" \
  start > "$work/pg_ctl.log" 2>&1; then
  echo "error: PostgreSQL did not start on port $port; logs follow" >&2
  cat "$work/pg_ctl.log" "$work/postgres.log" >&2 || true
  exit 1
fi

PGPASSWORD="$password" "$bin/psql" -q -h 127.0.0.1 -p "$port" -U lyric -d postgres \
  -v ON_ERROR_STOP=1 -c 'CREATE DATABASE hangfire_it' >&2

value="Host=127.0.0.1;Port=$port;Database=hangfire_it;Username=lyric;Password=$password"
if [ -n "${GITHUB_ENV:-}" ]; then
  echo "::add-mask::$password"
  echo "LYRIC_HANGFIRE_POSTGRES=$value" >> "$GITHUB_ENV"
  echo "PostgreSQL $("$bin/postgres" --version | awk '{print $3}') is up on 127.0.0.1:$port (data in $work)"
else
  echo "export LYRIC_HANGFIRE_POSTGRES='$value'"
fi
