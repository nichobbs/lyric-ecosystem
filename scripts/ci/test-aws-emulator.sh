#!/usr/bin/env bash
# Run the aws-credentials, aws-secrets, mq-sqs and storage-s3 integration suites against the AWS Emulator
# that scripts/ci/start-aws-emulator.sh started (its variables must be in the
# environment).
#
# Usage: scripts/ci/test-aws-emulator.sh <dotnet|jvm|native>
#
# Of the four packages, the ones that claim the target in `[package] targets`
# run; it is an error if none does.
#
# The emulator serves HTTPS with a certificate from its own CA. The suites
# trust it through the packages' own option (`withCaCertificateFile`, from
# LYRIC_SQS_CA_FILE and LYRIC_S3_CA_FILE; aws-secrets reads LYRIC_SECRETS_CA_FILE and
# hands it to the package as AWS_CA_BUNDLE), in addition to the system trust
# store; no process-wide trust setting (SSL_CERT_FILE, a JVM trust store) is
# changed, so this run is also the proof that the option works against a real
# HTTPS server with a private certificate, on every target.
#
# JAVA_TOOL_OPTIONS is cleared for the JVM tests so that nothing in the
# environment (a trust store or an HTTPS proxy a sandbox injects) stands in for
# the option; the emulator is on loopback and needs neither. It is kept for
# `lyric restore`, which may need a proxy to reach Maven Central. A native
# test run compiles its dependencies from source and has nothing to restore.
set -euo pipefail

target="${1:?usage: test-aws-emulator.sh <dotnet|jvm|native>}"
case "$target" in
  dotnet | jvm | native) ;;
  *) echo "error: unknown target $target" >&2; exit 1 ;;
esac
lyric="${LYRIC:-lyric}"
cd "$(dirname "$0")/../.."

for name in LYRIC_SQS_ENDPOINT LYRIC_S3_ENDPOINT LYRIC_SECRETS_ENDPOINT AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_EMULATOR_CA_FILE \
  AWS_EMULATOR_CREDENTIALS_FILE AWS_EMULATOR_CONFIG_FILE; do
  [ -n "${!name:-}" ] || { echo "error: $name is not set; run scripts/ci/start-aws-emulator.sh first" >&2; exit 1; }
done

failed=()
ran=()
# aws-credentials reads the emulator's own credentials and config files
# (AWS_EMULATOR_CREDENTIALS_FILE, AWS_EMULATOR_CONFIG_FILE) and takes nothing
# from AWS_ACCESS_KEY_ID: its suite is the shared-files provider against files
# a real tool wrote.
for pkg in aws-credentials aws-secrets mq-sqs storage-s3; do
  if ! grep -E '^targets[[:space:]]*=' "$pkg/lyric.toml" | grep -q "\"$target\""; then
    echo "== $pkg: does not claim $target, skipped"
    continue
  fi
  # aws-credentials and aws-secrets have no other service suite than the
  # emulator's, under their own feature, so that the `integration` job (MinIO,
  # ElasticMQ) does not pick them up.
  feature=integration
  [ "$pkg" = aws-credentials ] || [ "$pkg" = aws-secrets ] && feature=emulator
  echo "== $pkg against AWS Emulator (--target $target)"
  ran+=("$pkg")
  case "$target" in
    dotnet)
      "$lyric" restore --manifest "$pkg/lyric.toml"
      LYRIC_SQS_CA_FILE="$AWS_EMULATOR_CA_FILE" LYRIC_S3_CA_FILE="$AWS_EMULATOR_CA_FILE" LYRIC_SECRETS_CA_FILE="$AWS_EMULATOR_CA_FILE" \
        "$lyric" test --manifest "$pkg/lyric.toml" --target dotnet --features "$feature" || failed+=("$pkg")
      ;;
    jvm)
      "$lyric" restore --manifest "$pkg/lyric.toml"
      env -u JAVA_TOOL_OPTIONS LYRIC_SQS_CA_FILE="$AWS_EMULATOR_CA_FILE" LYRIC_S3_CA_FILE="$AWS_EMULATOR_CA_FILE" LYRIC_SECRETS_CA_FILE="$AWS_EMULATOR_CA_FILE" \
        "$lyric" test --manifest "$pkg/lyric.toml" --target jvm --features "$feature" || failed+=("$pkg")
      ;;
    native)
      LYRIC_SQS_CA_FILE="$AWS_EMULATOR_CA_FILE" LYRIC_S3_CA_FILE="$AWS_EMULATOR_CA_FILE" LYRIC_SECRETS_CA_FILE="$AWS_EMULATOR_CA_FILE" \
        "$lyric" test --manifest "$pkg/lyric.toml" --target native --features "$feature" || failed+=("$pkg")
      ;;
  esac
done

if [ "${#ran[@]}" -eq 0 ]; then
  echo "error: no package claims $target" >&2
  exit 1
fi
if [ "${#failed[@]}" -gt 0 ]; then
  echo "FAILED against AWS Emulator: ${failed[*]}" >&2
  exit 1
fi
echo "passed against AWS Emulator on $target: ${ran[*]}"
