#!/usr/bin/env bash
# Start two disposable RabbitMQ nodes (Ubuntu's `rabbitmq-server` package) for
# the mq-rabbitmq integration suite, as plain processes with their data in a
# fresh temporary directory, one user and one virtual host:
#
#   node A   127.0.0.1:35672  AMQP over plain TCP
#            127.0.0.1:35671  AMQP over TLS; a client certificate is verified
#                             when one is presented but not required
#   node B   127.0.0.1:35673  AMQP over TLS; a client certificate signed by
#                             the test CA is required (fail_if_no_peer_cert)
#
# A RabbitMQ node has one set of TLS options for all its listeners, hence the
# second node. Override the ports with LYRIC_RABBITMQ_PORT,
# LYRIC_RABBITMQ_TLS_PORT and LYRIC_RABBITMQ_MTLS_PORT. The nodes also use
# three loopback ports for Erlang distribution (LYRIC_RABBITMQ_EPMD_PORT,
# default 35690, and the two after it).
#
# The CA, the server certificate (SAN: localhost, 127.0.0.1) and the client
# certificate are generated with openssl on every run; nothing is reused and
# nothing is a secret. Keys are PKCS#8 ("BEGIN PRIVATE KEY"), the only key
# format Std.Tls loads. The user's password is random.
#
# Under GitHub Actions the variables below are exported through $GITHUB_ENV
# (password masked); otherwise they are printed as `export NAME=value` lines,
# so a shell can `eval "$(scripts/ci/start-rabbitmq.sh)"`:
#
#   LYRIC_RABBITMQ_URL        amqp://lyric:PASSWORD@127.0.0.1:35672/lyric-test
#   LYRIC_RABBITMQ_TLS_URL    amqps://lyric:PASSWORD@localhost:35671/lyric-test
#   LYRIC_RABBITMQ_MTLS_URL   amqps://lyric:PASSWORD@localhost:35673/lyric-test
#   LYRIC_RABBITMQ_TLS_CA     the CA certificate (PEM file)
#   LYRIC_RABBITMQ_TLS_CERT   the client certificate (PEM file)
#   LYRIC_RABBITMQ_TLS_KEY    the client private key (PEM file, PKCS#8)
#
# Stop the nodes with `kill $(cat "$LYRIC_RABBITMQ_WORK"/node-*/pid)`; the
# work directory is printed as LYRIC_RABBITMQ_WORK.
set -euo pipefail

port="${LYRIC_RABBITMQ_PORT:-35672}"
tls_port="${LYRIC_RABBITMQ_TLS_PORT:-35671}"
mtls_port="${LYRIC_RABBITMQ_MTLS_PORT:-35673}"
epmd_port="${LYRIC_RABBITMQ_EPMD_PORT:-35690}"
vhost=lyric-test
user=lyric

server=/usr/lib/rabbitmq/bin/rabbitmq-server
if [ ! -x "$server" ]; then
  # The package starts a node on 5672 when it is installed; policy-rc.d stops
  # that, since only the nodes started below are wanted.
  printf '#!/bin/sh\nexit 101\n' | sudo tee /usr/sbin/policy-rc.d > /dev/null
  sudo chmod +x /usr/sbin/policy-rc.d
  sudo apt-get update -qq
  sudo apt-get install -y --no-install-recommends rabbitmq-server
  sudo rm -f /usr/sbin/policy-rc.d
fi
[ -x "$server" ] || { echo "error: $server is not installed" >&2; exit 1; }
command -v openssl > /dev/null || { echo "error: openssl is not installed" >&2; exit 1; }

work="$(mktemp -d)"
password="$(head -c 12 /dev/urandom | od -An -tx1 | tr -d ' \n')"

# ─── Certificates ─────────────────────────────────────────────────────────────
certs="$work/tls"
mkdir -p "$certs"

gen_key() {
  openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out "$1" 2> /dev/null
}

gen_key "$certs/ca.key"
openssl req -x509 -new -key "$certs/ca.key" -sha256 -days 30 \
  -subj "/O=Lyric ecosystem CI/CN=Lyric RabbitMQ test CA" \
  -addext "basicConstraints=critical,CA:TRUE" -addext "keyUsage=critical,keyCertSign,cRLSign" \
  -out "$certs/ca.pem"

# issue <name> <subject CN> <extension lines>
issue() {
  gen_key "$certs/$1.key"
  openssl req -new -key "$certs/$1.key" -subj "/O=Lyric ecosystem CI/CN=$2" -out "$certs/$1.csr"
  printf '%s\n' "basicConstraints=CA:FALSE" "$3" > "$certs/$1.ext"
  openssl x509 -req -in "$certs/$1.csr" -CA "$certs/ca.pem" -CAkey "$certs/ca.key" -CAcreateserial \
    -sha256 -days 30 -extfile "$certs/$1.ext" -out "$certs/$1.pem" 2> /dev/null
}
issue server localhost "$(printf 'subjectAltName=DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth\nkeyUsage=critical,digitalSignature,keyEncipherment')"
issue client lyric-rabbitmq-client "$(printf 'extendedKeyUsage=clientAuth\nkeyUsage=critical,digitalSignature,keyEncipherment')"
chmod 644 "$certs"/*.pem "$certs/server.key" "$certs/client.key"

# ─── The nodes ────────────────────────────────────────────────────────────────
# start_node <name> <distribution port> <rabbitmq.conf listener lines>
start_node() {
  local dir="$work/node-$1"
  mkdir -p "$dir/mnesia" "$dir/log"
  : > "$dir/enabled_plugins"
  {
    printf '%s\n' "$3"
    echo "ssl_options.cacertfile = $certs/ca.pem"
    echo "ssl_options.certfile = $certs/server.pem"
    echo "ssl_options.keyfile = $certs/server.key"
    echo "ssl_options.verify = verify_peer"
    echo "default_user = $user"
    echo "default_pass = $password"
    echo "default_vhost = $vhost"
    echo "log.console = false"
  } > "$dir/rabbitmq.conf"
  # HOME holds the Erlang cookie; a private one keeps the nodes apart from
  # any other Erlang node of this user.
  HOME="$dir" \
    ERL_EPMD_PORT="$epmd_port" ERL_EPMD_ADDRESS=127.0.0.1 \
    RABBITMQ_NODENAME="lyric-rmq-$1@localhost" \
    RABBITMQ_DIST_PORT="$2" \
    RABBITMQ_MNESIA_BASE="$dir/mnesia" \
    RABBITMQ_LOG_BASE="$dir/log" \
    RABBITMQ_CONFIG_FILE="$dir/rabbitmq.conf" \
    RABBITMQ_ENABLED_PLUGINS_FILE="$dir/enabled_plugins" \
    RABBITMQ_PID_FILE="$dir/pid" \
    nohup "$server" > "$dir/server.log" 2>&1 &
}

start_node a "$((epmd_port + 1))" "$(printf 'listeners.tcp.1 = 127.0.0.1:%s\nlisteners.ssl.1 = 127.0.0.1:%s\nssl_options.fail_if_no_peer_cert = false' "$port" "$tls_port")"
start_node b "$((epmd_port + 2))" "$(printf 'listeners.tcp = none\nlisteners.ssl.1 = 127.0.0.1:%s\nssl_options.fail_if_no_peer_cert = true' "$mtls_port")"

listening() {
  (echo > "/dev/tcp/127.0.0.1/$1") 2> /dev/null
}

# A listener that accepts TCP is not yet a node that has created its user; the
# pid file is written once boot has finished.
for _ in $(seq 120); do
  if listening "$port" && listening "$tls_port" && listening "$mtls_port" \
    && [ -s "$work/node-a/pid" ] && [ -s "$work/node-b/pid" ]; then
    url="amqp://$user:$password@127.0.0.1:$port/$vhost"
    tls_url="amqps://$user:$password@localhost:$tls_port/$vhost"
    mtls_url="amqps://$user:$password@localhost:$mtls_port/$vhost"
    if [ -n "${GITHUB_ENV:-}" ]; then
      echo "::add-mask::$password"
      {
        echo "LYRIC_RABBITMQ_URL=$url"
        echo "LYRIC_RABBITMQ_TLS_URL=$tls_url"
        echo "LYRIC_RABBITMQ_MTLS_URL=$mtls_url"
        echo "LYRIC_RABBITMQ_TLS_CA=$certs/ca.pem"
        echo "LYRIC_RABBITMQ_TLS_CERT=$certs/client.pem"
        echo "LYRIC_RABBITMQ_TLS_KEY=$certs/client.key"
        echo "LYRIC_RABBITMQ_WORK=$work"
      } >> "$GITHUB_ENV"
      echo "RabbitMQ is up on 127.0.0.1: AMQP $port, TLS $tls_port, TLS with client certificates $mtls_port"
    else
      echo "export LYRIC_RABBITMQ_URL=$url"
      echo "export LYRIC_RABBITMQ_TLS_URL=$tls_url"
      echo "export LYRIC_RABBITMQ_MTLS_URL=$mtls_url"
      echo "export LYRIC_RABBITMQ_TLS_CA=$certs/ca.pem"
      echo "export LYRIC_RABBITMQ_TLS_CERT=$certs/client.pem"
      echo "export LYRIC_RABBITMQ_TLS_KEY=$certs/client.key"
      echo "export LYRIC_RABBITMQ_WORK=$work"
    fi
    exit 0
  fi
  sleep 1
done
echo "error: the RabbitMQ nodes did not open ports $port, $tls_port and $mtls_port; logs follow" >&2
cat "$work"/node-*/server.log "$work"/node-*/log/*.log >&2 || true
exit 1
