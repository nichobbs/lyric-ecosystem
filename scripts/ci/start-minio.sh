#!/usr/bin/env bash
# Start a disposable single-node MinIO for the storage-s3 integration suite on
# 127.0.0.1:9000. The `minio/minio` image is no longer on Docker Hub, so the
# server is built from the pinned module version with the Go toolchain; the
# binary lands in $MINIO_BIN_DIR (default ~/.cache/lyric-ci/minio), which CI
# caches. Under GitHub Actions it exports AWS_ACCESS_KEY_ID and
# AWS_SECRET_ACCESS_KEY through $GITHUB_ENV (secret masked); otherwise it
# prints them on stdout.
set -euo pipefail

version="v0.0.0-20260212201848-7aac2a2c5b7c"
bin_dir="${MINIO_BIN_DIR:-$HOME/.cache/lyric-ci/minio}"
port=9000

if [ ! -x "$bin_dir/minio" ]; then
  mkdir -p "$bin_dir"
  GOBIN="$bin_dir" go install "github.com/minio/minio@$version"
fi

work="$(mktemp -d)"
user=lyricit
secret="$(head -c 16 /dev/urandom | od -An -tx1 | tr -d ' \n')"
MINIO_ROOT_USER="$user" MINIO_ROOT_PASSWORD="$secret" nohup "$bin_dir/minio" server "$work/data" \
  --address "127.0.0.1:$port" > "$work/minio.log" 2>&1 &

for _ in $(seq 60); do
  if curl -sf "http://127.0.0.1:$port/minio/health/live"; then
    if [ -n "${GITHUB_ENV:-}" ]; then
      echo "::add-mask::$secret"
      echo "AWS_ACCESS_KEY_ID=$user" >> "$GITHUB_ENV"
      echo "AWS_SECRET_ACCESS_KEY=$secret" >> "$GITHUB_ENV"
      echo "MinIO is up on 127.0.0.1:$port"
    else
      echo "AWS_ACCESS_KEY_ID=$user"
      echo "AWS_SECRET_ACCESS_KEY=$secret"
    fi
    exit 0
  fi
  sleep 1
done
echo "error: MinIO did not become healthy on port $port; log follows" >&2
cat "$work/minio.log" >&2
exit 1
