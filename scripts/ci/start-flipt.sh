#!/usr/bin/env bash
# Start a disposable Flipt server (https://flipt.io) for the flags-flipt
# integration suite on 127.0.0.1:19930 (gRPC, unused by the suite, on 19931):
# SQLite storage in a fresh temporary directory, so the management API is
# writable, and token authentication required, with one bootstrap token
# generated for this run.
#
# Flipt is one Go binary. `go install go.flipt.io/flipt/cmd/flipt@v1.62.0` does
# not work (the module's go.mod has replace directives, and its `core` module
# is not published to the Go proxy), so the release archive is downloaded from
# GitHub and checked against the SHA-256 pinned below. The binary lands in
# $FLIPT_BIN_DIR (default ~/.cache/lyric-ci/flipt), which CI caches.
#
# Under GitHub Actions LYRIC_FLIPT_URL and LYRIC_FLIPT_TOKEN are exported
# through $GITHUB_ENV (token masked); otherwise they are printed on stdout.
# FLIPT_PORT and FLIPT_GRPC_PORT override the ports.
set -euo pipefail

version="v1.62.0"
bin_dir="${FLIPT_BIN_DIR:-$HOME/.cache/lyric-ci/flipt}"
port="${FLIPT_PORT:-19930}"
grpc_port="${FLIPT_GRPC_PORT:-19931}"

case "$(uname -s)-$(uname -m)" in
  Linux-x86_64)
    asset="flipt_linux_x86_64.tar.gz"
    sha256="fbea0403e9bc7d55ddb8f18e8c76c22c37d0a485bdf2385e677501f24cfd8f41"
    ;;
  Linux-aarch64 | Linux-arm64)
    asset="flipt_linux_arm64.tar.gz"
    sha256="92b3fbd909098b30cd17cb0f987459ddb5ed7acf4ed2acfc1a73878af9fabe43"
    ;;
  *)
    echo "error: no pinned Flipt $version archive for $(uname -s)-$(uname -m)" >&2
    exit 1
    ;;
esac

if [ ! -x "$bin_dir/flipt" ] || ! "$bin_dir/flipt" --version 2> /dev/null | grep -q "Version: $version"; then
  mkdir -p "$bin_dir"
  download="$(mktemp -d)"
  curl -sSfL -o "$download/$asset" \
    "https://github.com/flipt-io/flipt/releases/download/$version/$asset"
  echo "$sha256  $download/$asset" | sha256sum -c - > /dev/null
  tar -xzf "$download/$asset" -C "$bin_dir" flipt
  rm -rf "$download"
fi

work="$(mktemp -d)"
token="$(head -c 24 /dev/urandom | od -An -tx1 | tr -d ' \n')"
cat > "$work/flipt.yml" <<CONFIG
log:
  level: WARN
ui:
  enabled: false
server:
  host: 127.0.0.1
  http_port: $port
  grpc_port: $grpc_port
db:
  url: file:$work/flipt.db
meta:
  check_for_updates: false
  telemetry_enabled: false
authentication:
  required: true
  methods:
    token:
      enabled: true
      bootstrap:
        token: "$token"
CONFIG
chmod 600 "$work/flipt.yml"

nohup "$bin_dir/flipt" --config "$work/flipt.yml" > "$work/flipt.log" 2>&1 &

url="http://127.0.0.1:$port"
for _ in $(seq 60); do
  # /health answers before the bootstrap token is usable; wait for the API.
  if curl -sf -o /dev/null -H "Authorization: Bearer $token" "$url/api/v1/namespaces/default"; then
    if [ -n "${GITHUB_ENV:-}" ]; then
      echo "::add-mask::$token"
      echo "LYRIC_FLIPT_URL=$url" >> "$GITHUB_ENV"
      echo "LYRIC_FLIPT_TOKEN=$token" >> "$GITHUB_ENV"
      echo "Flipt $version is up on 127.0.0.1:$port"
    else
      echo "LYRIC_FLIPT_URL=$url"
      echo "LYRIC_FLIPT_TOKEN=$token"
    fi
    exit 0
  fi
  sleep 1
done
echo "error: Flipt did not become ready on port $port; log follows" >&2
cat "$work/flipt.log" >&2
exit 1
