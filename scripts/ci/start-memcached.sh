#!/usr/bin/env bash
# Start three disposable memcached servers on 127.0.0.1 for the
# cache-memcached integration suite:
#
#   19960  plain TCP                         (override: LYRIC_MEMCACHED_PORT)
#   19961  TLS only (-Z)                     (override: LYRIC_MEMCACHED_TLS_PORT)
#   19962  plain TCP with ASCII authentication (-Y), one user
#                                            (override: LYRIC_MEMCACHED_AUTH_PORT)
#
# The suite runs `flush_all` on the third one, so never point
# LYRIC_MEMCACHED_AUTH_URL at a server that holds anything.
#
# The CA and the server certificate (SAN: localhost, 127.0.0.1) are generated
# with openssl into a fresh temporary directory on every run, and so is the
# password of the authentication file; nothing is reused. memcached needs a
# build with TLS (Ubuntu's package has it; `memcached -h` lists -Z).
#
# Under GitHub Actions the locations are exported through $GITHUB_ENV (the
# password masked); otherwise they are printed as `export NAME=value` lines,
# so a shell can `eval "$(scripts/ci/start-memcached.sh)"`:
#
#   LYRIC_MEMCACHED_URL       memcached://127.0.0.1:19960
#   LYRIC_MEMCACHED_TLS_URL   memcached+tls://localhost:19961
#   LYRIC_MEMCACHED_TLS_CA    the CA certificate (PEM file)
#   LYRIC_MEMCACHED_AUTH_URL  memcached://lyric:PASSWORD@127.0.0.1:19962
set -euo pipefail

port="${LYRIC_MEMCACHED_PORT:-19960}"
tls_port="${LYRIC_MEMCACHED_TLS_PORT:-19961}"
auth_port="${LYRIC_MEMCACHED_AUTH_PORT:-19962}"

as_root() {
  if [ "$(id -u)" = 0 ]; then "$@"; else sudo "$@"; fi
}

if ! command -v memcached > /dev/null; then
  # The package starts a server on 11211 when it is installed; policy-rc.d
  # stops that, since only the servers started below are wanted.
  printf '#!/bin/sh\nexit 101\n' | as_root tee /usr/sbin/policy-rc.d > /dev/null
  as_root chmod +x /usr/sbin/policy-rc.d
  as_root apt-get update -qq
  as_root apt-get install -y --no-install-recommends memcached
  as_root rm -f /usr/sbin/policy-rc.d
fi
command -v openssl > /dev/null || { echo "error: openssl is not installed" >&2; exit 1; }
if ! memcached -h | grep -q -- '--enable-ssl'; then
  echo "error: this memcached was built without TLS (no -Z in memcached -h)" >&2
  exit 1
fi

listening() {
  (echo > "/dev/tcp/127.0.0.1/$1") 2> /dev/null
}
for taken in "$port" "$tls_port" "$auth_port"; do
  if listening "$taken"; then
    echo "error: something already listens on 127.0.0.1:$taken" >&2
    exit 1
  fi
done

work="$(mktemp -d)"
certs="$work/certs"
mkdir -p "$certs"

# memcached refuses to run as root unless it is told which user to be.
user=()
if [ "$(id -u)" = 0 ]; then
  user=(-u root)
fi

gen_key() {
  openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out "$1" 2> /dev/null
}

# The CA.
gen_key "$certs/ca.key"
openssl req -x509 -new -key "$certs/ca.key" -sha256 -days 30 \
  -subj "/O=Lyric ecosystem CI/CN=Lyric memcached test CA" \
  -addext "basicConstraints=critical,CA:TRUE" -addext "keyUsage=critical,keyCertSign,cRLSign" \
  -out "$certs/ca.crt"

# The server certificate.
gen_key "$certs/server.key"
openssl req -new -key "$certs/server.key" -subj "/O=Lyric ecosystem CI/CN=localhost" -out "$certs/server.csr"
printf '%s\n' "basicConstraints=CA:FALSE" "subjectAltName=DNS:localhost,IP:127.0.0.1" \
  "extendedKeyUsage=serverAuth" "keyUsage=critical,digitalSignature,keyEncipherment" > "$certs/server.ext"
openssl x509 -req -in "$certs/server.csr" -CA "$certs/ca.crt" -CAkey "$certs/ca.key" -CAcreateserial \
  -sha256 -days 30 -extfile "$certs/server.ext" -out "$certs/server.crt" 2> /dev/null

# The authentication file: `user:password` lines. memcached takes neither a
# space nor a colon in them, so the password is hexadecimal.
password="$(openssl rand -hex 16)"
printf 'lyric:%s\n' "$password" > "$work/auth"
chmod 600 "$work/auth"

# start <name> <port> <extra arguments>...
start() {
  local name="$1" listen="$2"
  shift 2
  nohup memcached "${user[@]}" -l 127.0.0.1 -p "$listen" -U 0 -m 64 "$@" \
    > "$work/memcached-$name.log" 2>&1 &
}
start plain "$port"
start tls "$tls_port" -Z -o "ssl_chain_cert=$certs/server.crt,ssl_key=$certs/server.key"
start auth "$auth_port" -Y "$work/auth"

# wait_for <name> <port>
wait_for() {
  for _ in $(seq 60); do
    if listening "$2"; then
      return 0
    fi
    sleep 0.5
  done
  echo "error: memcached ($1) did not open port $2; log follows" >&2
  cat "$work/memcached-$1.log" >&2
  exit 1
}
wait_for plain "$port"
wait_for tls "$tls_port"
wait_for auth "$auth_port"

emit() {
  if [ -n "${GITHUB_ENV:-}" ]; then
    echo "$1=$2" >> "$GITHUB_ENV"
  else
    echo "export $1=$2"
  fi
}
if [ -n "${GITHUB_ENV:-}" ]; then
  echo "::add-mask::$password"
fi
emit LYRIC_MEMCACHED_URL "memcached://127.0.0.1:$port"
emit LYRIC_MEMCACHED_TLS_URL "memcached+tls://localhost:$tls_port"
emit LYRIC_MEMCACHED_TLS_CA "$certs/ca.crt"
emit LYRIC_MEMCACHED_AUTH_URL "memcached://lyric:$password@127.0.0.1:$auth_port"
if [ -n "${GITHUB_ENV:-}" ]; then
  echo "memcached $(memcached -V | awk '{print $2}') is up on 127.0.0.1: plain $port, TLS $tls_port, authentication $auth_port"
fi
