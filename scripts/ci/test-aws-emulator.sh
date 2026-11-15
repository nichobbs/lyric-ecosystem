#!/usr/bin/env bash
# Run the aws-credentials, mq-sqs and storage-s3 integration suites against the AWS Emulator
# that scripts/ci/start-aws-emulator.sh started (its variables must be in the
# environment).
#
# Usage: scripts/ci/test-aws-emulator.sh <dotnet|jvm>
#
# The emulator serves HTTPS with a certificate from its own CA. The suites
# trust it through the packages' own option (`withCaCertificateFile`, from
# LYRIC_SQS_CA_FILE and LYRIC_S3_CA_FILE), in addition to the system trust
# store; no process-wide trust setting (SSL_CERT_FILE, a JVM trust store) is
# changed, so this run is also the proof that the option works against a real
# HTTPS server with a private certificate, on both targets.
#
# JAVA_TOOL_OPTIONS is cleared for the JVM tests so that nothing in the
# environment (a trust store or an HTTPS proxy a sandbox injects) stands in for
# the option; the emulator is on loopback and needs neither. It is kept for
# `lyric restore`, which may need a proxy to reach Maven Central.
set -euo pipefail

target="${1:?usage: test-aws-emulator.sh <dotnet|jvm>}"
lyric="${LYRIC:-lyric}"
cd "$(dirname "$0")/../.."

for name in LYRIC_SQS_ENDPOINT LYRIC_S3_ENDPOINT AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_EMULATOR_CA_FILE \
  AWS_EMULATOR_CREDENTIALS_FILE AWS_EMULATOR_CONFIG_FILE; do
  [ -n "${!name:-}" ] || { echo "error: $name is not set; run scripts/ci/start-aws-emulator.sh first" >&2; exit 1; }
done

failed=()
# aws-credentials reads the emulator's own credentials and config files
# (AWS_EMULATOR_CREDENTIALS_FILE, AWS_EMULATOR_CONFIG_FILE) and takes nothing
# from AWS_ACCESS_KEY_ID: its suite is the shared-files provider against files
# a real tool wrote.
for pkg in aws-credentials mq-sqs storage-s3; do
  # aws-credentials' only service suite is the emulator's, under its own
  # feature, so that the `integration` job (MinIO, ElasticMQ) does not pick it up.
  feature=integration
  [ "$pkg" = aws-credentials ] && feature=emulator
  echo "== $pkg against AWS Emulator (--target $target)"
  "$lyric" restore --manifest "$pkg/lyric.toml"
  case "$target" in
    dotnet)
      LYRIC_SQS_CA_FILE="$AWS_EMULATOR_CA_FILE" LYRIC_S3_CA_FILE="$AWS_EMULATOR_CA_FILE" \
        "$lyric" test --manifest "$pkg/lyric.toml" --target dotnet --features "$feature" || failed+=("$pkg")
      ;;
    jvm)
      env -u JAVA_TOOL_OPTIONS LYRIC_SQS_CA_FILE="$AWS_EMULATOR_CA_FILE" LYRIC_S3_CA_FILE="$AWS_EMULATOR_CA_FILE" \
        "$lyric" test --manifest "$pkg/lyric.toml" --target jvm --features "$feature" || failed+=("$pkg")
      ;;
    *) echo "error: unknown target $target" >&2; exit 1 ;;
  esac
done

if [ "${#failed[@]}" -gt 0 ]; then
  echo "FAILED against AWS Emulator: ${failed[*]}" >&2
  exit 1
fi
echo "aws-credentials, mq-sqs and storage-s3 passed against AWS Emulator on $target"
