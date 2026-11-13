#!/usr/bin/env bash
# Start a disposable Azurite, Microsoft's Azure Storage emulator, for the
# storage-azure-blob integration suite: the Blob service only, in memory, on
# 127.0.0.1:$AZURITE_BLOB_PORT (default 10000).
#
# Azurite is the pinned `azurite` npm package, installed with npm into
# $AZURITE_DIR (default ~/.cache/lyric-ci/azurite), which CI caches. It needs
# node and npm, which GitHub's runners have.
#
# Under GitHub Actions the variables below are exported through $GITHUB_ENV;
# otherwise they are printed on stdout as `export NAME=value` lines.
#
#   LYRIC_AZURE_BLOB_ENDPOINT   http://127.0.0.1:<port>/devstoreaccount1
#   LYRIC_AZURE_BLOB_ACCOUNT    devstoreaccount1
#   LYRIC_AZURE_BLOB_KEY        the emulator's account key
#   LYRIC_AZURE_BLOB_CONTAINER  lyric-it (the suite creates it)
#
# The account name and key are Azurite's built-in development account. They
# are public constants Microsoft documents, the same on every installation,
# and they open nothing but an emulator, so the key is not masked.
set -euo pipefail

version="3.35.0"
dir="${AZURITE_DIR:-$HOME/.cache/lyric-ci/azurite}"
port="${AZURITE_BLOB_PORT:-10000}"
account="devstoreaccount1"
key="Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="

command -v node > /dev/null || { echo "error: node is not installed" >&2; exit 1; }
command -v npm > /dev/null || { echo "error: npm is not installed" >&2; exit 1; }

bin="$dir/node_modules/.bin/azurite-blob"
installed="$(node -p "require('$dir/node_modules/azurite/package.json').version" 2> /dev/null || true)"
if [ ! -x "$bin" ] || [ "$installed" != "$version" ]; then
  mkdir -p "$dir"
  npm install --prefix "$dir" --no-audit --no-fund --ignore-scripts --loglevel=error "azurite@$version" >&2
fi

work="$(mktemp -d)"
(cd "$work" && nohup "$bin" --blobHost 127.0.0.1 --blobPort "$port" --inMemoryPersistence --silent \
  > "$work/azurite.log" 2>&1 &)

endpoint="http://127.0.0.1:$port/$account"
for _ in $(seq 60); do
  # Any HTTP answer means the service is listening; an unsigned request is
  # refused with 403.
  status="$(curl -s --max-time 2 -o /dev/null -w '%{http_code}' "$endpoint?comp=list" || true)"
  if [ "$status" = "403" ]; then
    if [ -n "${GITHUB_ENV:-}" ]; then
      {
        echo "LYRIC_AZURE_BLOB_ENDPOINT=$endpoint"
        echo "LYRIC_AZURE_BLOB_ACCOUNT=$account"
        echo "LYRIC_AZURE_BLOB_KEY=$key"
        echo "LYRIC_AZURE_BLOB_CONTAINER=lyric-it"
      } >> "$GITHUB_ENV"
      echo "Azurite $version (Blob) is up on 127.0.0.1:$port"
    else
      echo "export LYRIC_AZURE_BLOB_ENDPOINT=$endpoint"
      echo "export LYRIC_AZURE_BLOB_ACCOUNT=$account"
      echo "export LYRIC_AZURE_BLOB_KEY=$key"
      echo "export LYRIC_AZURE_BLOB_CONTAINER=lyric-it"
    fi
    exit 0
  fi
  sleep 1
done
echo "error: Azurite did not answer on port $port; log follows" >&2
cat "$work/azurite.log" >&2
exit 1
