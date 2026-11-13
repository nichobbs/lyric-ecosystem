#!/usr/bin/env bash
# Start a disposable ActiveMQ Classic broker (Ubuntu's `activemq` package) for
# the mq-activemq integration suite: in memory, one user, configured by
# mq-activemq/tests/integration/broker/activemq.xml, with three STOMP
# connectors on 127.0.0.1:
#
#   61613  plain TCP
#   61614  TLS
#   61615  TLS, client certificate required (needClientAuth)
#
# The CA, the server certificate (SAN: localhost, 127.0.0.1), the client
# certificate and the broker's key and trust stores are generated with openssl
# and keytool into a fresh temporary directory on every run; nothing is reused
# and nothing is a secret. Keys are PKCS#8 ("BEGIN PRIVATE KEY"), the only key
# format Std.Tls loads.
#
# Under GitHub Actions the variables below are exported through $GITHUB_ENV
# (passcode masked). Otherwise the script prints the plain URL on the first
# line of stdout, as it always has, followed by one `export NAME=value` line
# for each TLS variable:
#
#   LYRIC_ACTIVEMQ_URL        stomp://lyric:PASSCODE@127.0.0.1:61613
#   LYRIC_ACTIVEMQ_TLS_URL    stomp+ssl://lyric:PASSCODE@127.0.0.1:61614
#   LYRIC_ACTIVEMQ_MTLS_URL   stomp+ssl://lyric:PASSCODE@127.0.0.1:61615
#   LYRIC_ACTIVEMQ_TLS_CA     the CA certificate (PEM file)
#   LYRIC_ACTIVEMQ_TLS_CERT   the client certificate (PEM file)
#   LYRIC_ACTIVEMQ_TLS_KEY    the client private key (PEM file, PKCS#8)
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"
home=/usr/share/activemq
port=61613
tls_port=61614
mtls_port=61615

if [ ! -d "$home" ]; then
  sudo apt-get update -qq
  sudo apt-get install -y --no-install-recommends activemq
fi
command -v openssl > /dev/null || { echo "error: openssl is not installed" >&2; exit 1; }
command -v keytool > /dev/null || { echo "error: keytool (a JDK) is not installed" >&2; exit 1; }

work="$(mktemp -d)"
passcode="$(head -c 12 /dev/urandom | od -An -tx1 | tr -d ' \n')"
store_password="$(head -c 12 /dev/urandom | od -An -tx1 | tr -d ' \n')"

# ─── Certificates and stores ──────────────────────────────────────────────────
certs="$work/tls"
mkdir -p "$certs"

gen_key() {
  openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out "$1" 2> /dev/null
}

gen_key "$certs/ca.key"
openssl req -x509 -new -key "$certs/ca.key" -sha256 -days 30 \
  -subj "/O=Lyric ecosystem CI/CN=Lyric ActiveMQ test CA" \
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
issue client lyric-activemq-client "$(printf 'extendedKeyUsage=clientAuth\nkeyUsage=critical,digitalSignature,keyEncipherment')"

# The broker's identity, and the CA it accepts client certificates from. The
# JDK tools must not inherit proxy or agent options either.
openssl pkcs12 -export -name broker -inkey "$certs/server.key" -in "$certs/server.pem" \
  -certfile "$certs/ca.pem" -passout "pass:$store_password" -out "$certs/broker-keystore.p12"
JAVA_TOOL_OPTIONS='' keytool -importcert -noprompt -alias lyric-activemq-test-ca -file "$certs/ca.pem" \
  -storetype PKCS12 -storepass "$store_password" -keystore "$certs/broker-truststore.p12" > /dev/null 2>&1

# ─── The broker ───────────────────────────────────────────────────────────────
# The broker must not inherit proxy or agent options meant for other JVMs.
JAVA_TOOL_OPTIONS='' nohup java \
  -Dactivemq.home="$home" -Dactivemq.base="$work" -Dactivemq.data="$work/data" \
  -Dlyric.activemq.user=lyric -Dlyric.activemq.passcode="$passcode" \
  -Dlyric.activemq.stompPort="$port" \
  -Dlyric.activemq.tlsPort="$tls_port" -Dlyric.activemq.mutualTlsPort="$mtls_port" \
  -Dlyric.activemq.keyStore="$certs/broker-keystore.p12" \
  -Dlyric.activemq.trustStore="$certs/broker-truststore.p12" \
  -Dlyric.activemq.storePassword="$store_password" \
  -cp "$home/bin/activemq.jar:$home/lib/*:$home/lib/optional/*" \
  org.apache.activemq.console.Main start \
  "xbean:file:$root/mq-activemq/tests/integration/broker/activemq.xml" \
  > "$work/broker.log" 2>&1 &

listening() {
  (echo > "/dev/tcp/127.0.0.1/$1") 2> /dev/null
}

for _ in $(seq 60); do
  if listening "$port" && listening "$tls_port" && listening "$mtls_port"; then
    url="stomp://lyric:$passcode@127.0.0.1:$port"
    tls_url="stomp+ssl://lyric:$passcode@127.0.0.1:$tls_port"
    mtls_url="stomp+ssl://lyric:$passcode@127.0.0.1:$mtls_port"
    if [ -n "${GITHUB_ENV:-}" ]; then
      echo "::add-mask::$passcode"
      {
        echo "LYRIC_ACTIVEMQ_URL=$url"
        echo "LYRIC_ACTIVEMQ_TLS_URL=$tls_url"
        echo "LYRIC_ACTIVEMQ_MTLS_URL=$mtls_url"
        echo "LYRIC_ACTIVEMQ_TLS_CA=$certs/ca.pem"
        echo "LYRIC_ACTIVEMQ_TLS_CERT=$certs/client.pem"
        echo "LYRIC_ACTIVEMQ_TLS_KEY=$certs/client.key"
      } >> "$GITHUB_ENV"
      echo "ActiveMQ is up on 127.0.0.1: STOMP $port, TLS $tls_port, TLS with client certificates $mtls_port"
    else
      echo "$url"
      echo "export LYRIC_ACTIVEMQ_TLS_URL=$tls_url"
      echo "export LYRIC_ACTIVEMQ_MTLS_URL=$mtls_url"
      echo "export LYRIC_ACTIVEMQ_TLS_CA=$certs/ca.pem"
      echo "export LYRIC_ACTIVEMQ_TLS_CERT=$certs/client.pem"
      echo "export LYRIC_ACTIVEMQ_TLS_KEY=$certs/client.key"
    fi
    exit 0
  fi
  sleep 1
done
echo "error: the ActiveMQ broker did not open ports $port, $tls_port and $mtls_port; log follows" >&2
cat "$work/broker.log" >&2
exit 1
