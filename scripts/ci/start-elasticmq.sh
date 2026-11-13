#!/usr/bin/env bash
# Start a disposable ElasticMQ (an in-memory, SQS-compatible server) for the
# mq-sqs integration suite on 127.0.0.1:$ELASTICMQ_PORT (default 9324).
#
# The server is the pinned `org.elasticmq:elasticmq-server_2.13` release from
# Maven Central: its jars are resolved with Maven into $ELASTICMQ_LIB_DIR
# (default ~/.cache/lyric-ci/elasticmq), which CI caches, and run with the JDK
# on the runner. It is started with `sqs-limits = strict`, so it enforces
# SQS's message and batch limits the way Amazon SQS does.
#
# Under GitHub Actions the variables below are exported through $GITHUB_ENV;
# otherwise the script prints one `NAME=value` line for each on stdout:
#
#   LYRIC_SQS_ENDPOINT       http://127.0.0.1:9324
#   LYRIC_SQS_REGION         elasticmq
#   AWS_ACCESS_KEY_ID        only when not already set
#   AWS_SECRET_ACCESS_KEY    only when not already set
#
# ElasticMQ accepts any credentials and does not verify signatures (the
# hermetic suite's fake server does), so the two AWS variables are fixed
# placeholders, not secrets, and an earlier step's values are left alone.
set -euo pipefail

version="1.6.11"
port="${ELASTICMQ_PORT:-9324}"
lib_dir="${ELASTICMQ_LIB_DIR:-$HOME/.cache/lyric-ci/elasticmq}/$version"
mvn="${MVN:-mvn}"

command -v java > /dev/null || { echo "error: java (a JDK or JRE) is not installed" >&2; exit 1; }

work="$(mktemp -d)"

if [ ! -f "$lib_dir/elasticmq-server_2.13-$version.jar" ]; then
  command -v "$mvn" > /dev/null || { echo "error: Maven is not installed (set MVN to the mvn binary)" >&2; exit 1; }
  mkdir -p "$lib_dir"
  cat > "$work/pom.xml" <<POM
<project xmlns="http://maven.apache.org/POM/4.0.0">
  <modelVersion>4.0.0</modelVersion>
  <groupId>lyric.ci</groupId>
  <artifactId>elasticmq-fetch</artifactId>
  <version>1</version>
  <packaging>pom</packaging>
  <dependencies>
    <dependency>
      <groupId>org.elasticmq</groupId>
      <artifactId>elasticmq-server_2.13</artifactId>
      <version>$version</version>
    </dependency>
  </dependencies>
</project>
POM
  if ! "$mvn" -q -B -f "$work/pom.xml" dependency:copy-dependencies -DoutputDirectory="$lib_dir" \
    > "$work/mvn.log" 2>&1; then
    echo "error: Maven could not resolve elasticmq-server_2.13:$version; log follows" >&2
    cat "$work/mvn.log" >&2
    exit 1
  fi
fi

cat > "$work/elasticmq.conf" <<CONF
include classpath("application.conf")
node-address { protocol = http, host = "127.0.0.1", port = $port, context-path = "" }
rest-sqs { enabled = true, bind-port = $port, bind-hostname = "127.0.0.1", sqs-limits = strict }
rest-stats { enabled = false }
aws { region = "elasticmq", accountId = "000000000000" }
CONF

# JAVA_TOOL_OPTIONS is cleared: a proxy or agent configured for build tools has
# no business in the server.
env -u JAVA_TOOL_OPTIONS nohup java -Dconfig.file="$work/elasticmq.conf" -cp "$lib_dir/*" \
  org.elasticmq.server.Main > "$work/elasticmq.log" 2>&1 &

for _ in $(seq 60); do
  if curl -sf -o /dev/null -X POST "http://127.0.0.1:$port/" \
    -H 'X-Amz-Target: AmazonSQS.ListQueues' -H 'Content-Type: application/x-amz-json-1.0' -d '{}'; then
    emit() {
      if [ -n "${GITHUB_ENV:-}" ]; then
        echo "$1=$2" >> "$GITHUB_ENV"
      else
        echo "$1=$2"
      fi
    }
    emit LYRIC_SQS_ENDPOINT "http://127.0.0.1:$port"
    emit LYRIC_SQS_REGION elasticmq
    if [ -z "${AWS_ACCESS_KEY_ID:-}" ]; then
      emit AWS_ACCESS_KEY_ID elasticmq
    fi
    if [ -z "${AWS_SECRET_ACCESS_KEY:-}" ]; then
      emit AWS_SECRET_ACCESS_KEY elasticmq
    fi
    if [ -n "${GITHUB_ENV:-}" ]; then
      echo "ElasticMQ $version is up on 127.0.0.1:$port"
    fi
    exit 0
  fi
  sleep 1
done
echo "error: ElasticMQ did not answer on port $port; log follows" >&2
cat "$work/elasticmq.log" >&2
exit 1
