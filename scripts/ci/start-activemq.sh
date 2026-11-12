#!/usr/bin/env bash
# Start a disposable ActiveMQ Classic broker (Ubuntu's `activemq` package) for
# the mq-activemq integration suite: in memory, one user, STOMP on
# 127.0.0.1:61613, configured by mq-activemq/tests/integration/broker/activemq.xml.
# Under GitHub Actions it exports LYRIC_ACTIVEMQ_URL through $GITHUB_ENV (passcode
# masked); otherwise it prints the URL on stdout.
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"
home=/usr/share/activemq
port=61613

if [ ! -d "$home" ]; then
  sudo apt-get update -qq
  sudo apt-get install -y --no-install-recommends activemq
fi

work="$(mktemp -d)"
passcode="$(head -c 12 /dev/urandom | od -An -tx1 | tr -d ' \n')"
# The broker must not inherit proxy or agent options meant for other JVMs.
JAVA_TOOL_OPTIONS='' nohup java \
  -Dactivemq.home="$home" -Dactivemq.base="$work" -Dactivemq.data="$work/data" \
  -Dlyric.activemq.user=lyric -Dlyric.activemq.passcode="$passcode" \
  -Dlyric.activemq.stompPort="$port" \
  -cp "$home/bin/activemq.jar:$home/lib/*:$home/lib/optional/*" \
  org.apache.activemq.console.Main start \
  "xbean:file:$root/mq-activemq/tests/integration/broker/activemq.xml" \
  > "$work/broker.log" 2>&1 &

for _ in $(seq 60); do
  if (echo > "/dev/tcp/127.0.0.1/$port") 2>/dev/null; then
    url="stomp://lyric:$passcode@127.0.0.1:$port"
    if [ -n "${GITHUB_ENV:-}" ]; then
      echo "::add-mask::$passcode"
      echo "LYRIC_ACTIVEMQ_URL=$url" >> "$GITHUB_ENV"
      echo "ActiveMQ is up on 127.0.0.1:$port"
    else
      echo "$url"
    fi
    exit 0
  fi
  sleep 1
done
echo "error: the ActiveMQ broker did not open port $port; log follows" >&2
cat "$work/broker.log" >&2
exit 1
