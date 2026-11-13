#!/usr/bin/env bash
# Start two disposable Redis servers that speak TLS only (no plain port), for
# the TLS halves of the redis, cache-redis and session-redis integration suites:
# one on 127.0.0.1:16380 (override with LYRIC_REDIS_TLS_PORT) that accepts
# clients with or without a certificate (`tls-auth-clients optional`), and one
# on 127.0.0.1:16381 (LYRIC_REDIS_MTLS_PORT) that refuses clients without a
# certificate signed by the test CA (`tls-auth-clients yes`).
#
# The CA, the server certificate (SAN: localhost, 127.0.0.1) and the client
# certificate are generated with openssl into a fresh temporary directory on
# every run; nothing is reused and nothing is a secret. Keys are PKCS#8
# ("BEGIN PRIVATE KEY"), the only key format Std.Tls loads.
#
# Under GitHub Actions the locations are exported through $GITHUB_ENV;
# otherwise they are printed as `export NAME=value` lines, so a shell can
# `eval "$(scripts/ci/start-redis-tls.sh)"`:
#
#   LYRIC_REDIS_TLS_URL   rediss://localhost:16380      (client certificate optional)
#   LYRIC_REDIS_MTLS_URL  rediss://localhost:16381      (client certificate required)
#   LYRIC_REDIS_TLS_CA    the CA certificate (PEM file)
#   LYRIC_REDIS_TLS_CERT  the client certificate (PEM file)
#   LYRIC_REDIS_TLS_KEY   the client private key (PEM file, PKCS#8)
set -euo pipefail

tls_port="${LYRIC_REDIS_TLS_PORT:-16380}"
mtls_port="${LYRIC_REDIS_MTLS_PORT:-16381}"

if ! command -v redis-server > /dev/null; then
  # The package starts a server on 6379 when it is installed; policy-rc.d
  # stops that, since the port may belong to another Redis and only the
  # servers started below are wanted.
  printf '#!/bin/sh\nexit 101\n' | sudo tee /usr/sbin/policy-rc.d > /dev/null
  sudo chmod +x /usr/sbin/policy-rc.d
  sudo apt-get update -qq
  sudo apt-get install -y --no-install-recommends redis-server
  sudo rm -f /usr/sbin/policy-rc.d
fi
command -v openssl > /dev/null || { echo "error: openssl is not installed" >&2; exit 1; }

work="$(mktemp -d)"
certs="$work/certs"
mkdir -p "$certs"

gen_key() {
  openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out "$1" 2> /dev/null
}

# The CA.
gen_key "$certs/ca.key"
openssl req -x509 -new -key "$certs/ca.key" -sha256 -days 30 \
  -subj "/O=Lyric ecosystem CI/CN=Lyric Redis test CA" \
  -addext "basicConstraints=critical,CA:TRUE" -addext "keyUsage=critical,keyCertSign,cRLSign" \
  -out "$certs/ca.crt"

# issue <name> <subject CN> <extension lines>
issue() {
  gen_key "$certs/$1.key"
  openssl req -new -key "$certs/$1.key" -subj "/O=Lyric ecosystem CI/CN=$2" -out "$certs/$1.csr"
  printf '%s\n' "basicConstraints=CA:FALSE" "$3" > "$certs/$1.ext"
  openssl x509 -req -in "$certs/$1.csr" -CA "$certs/ca.crt" -CAkey "$certs/ca.key" -CAcreateserial \
    -sha256 -days 30 -extfile "$certs/$1.ext" -out "$certs/$1.crt" 2> /dev/null
}
issue server localhost "$(printf 'subjectAltName=DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth\nkeyUsage=critical,digitalSignature,keyEncipherment')"
issue client lyric-redis-client "$(printf 'extendedKeyUsage=clientAuth\nkeyUsage=critical,digitalSignature,keyEncipherment')"

# start <tls port> <tls-auth-clients>
start() {
  redis-server --port 0 --tls-port "$1" --bind 127.0.0.1 -::1 \
    --tls-cert-file "$certs/server.crt" --tls-key-file "$certs/server.key" \
    --tls-ca-cert-file "$certs/ca.crt" --tls-auth-clients "$2" \
    --save "" --appendonly no --dir "$work" \
    --daemonize yes --pidfile "$work/redis-$1.pid" --logfile "$work/redis-$1.log" > /dev/null
}
start "$tls_port" optional
start "$mtls_port" yes

wait_for() {
  for _ in $(seq 60); do
    if (echo > "/dev/tcp/127.0.0.1/$1") 2> /dev/null; then
      return 0
    fi
    sleep 0.5
  done
  echo "error: redis-server did not open TLS port $1; log follows" >&2
  cat "$work/redis-$1.log" >&2
  exit 1
}
wait_for "$tls_port"
wait_for "$mtls_port"

emit() {
  if [ -n "${GITHUB_ENV:-}" ]; then
    echo "$1=$2" >> "$GITHUB_ENV"
  else
    echo "export $1=$2"
  fi
}
emit LYRIC_REDIS_TLS_URL "rediss://localhost:$tls_port"
emit LYRIC_REDIS_MTLS_URL "rediss://localhost:$mtls_port"
emit LYRIC_REDIS_TLS_CA "$certs/ca.crt"
emit LYRIC_REDIS_TLS_CERT "$certs/client.crt"
emit LYRIC_REDIS_TLS_KEY "$certs/client.key"
if [ -n "${GITHUB_ENV:-}" ]; then
  echo "Redis with TLS is up on 127.0.0.1:$tls_port (client certificate optional) and 127.0.0.1:$mtls_port (required)"
fi
