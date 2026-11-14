#!/usr/bin/env bash
# Start a disposable AWS Emulator (https://github.com/aws/aws-emulator) for the
# aws-credentials, mq-sqs and storage-s3 integration suites, on https://127.0.0.1:$port
# (default 9297).
#
# The pinned Debian package is downloaded into $AWS_EMULATOR_PKG_DIR (default
# ~/.cache/lyric-ci/aws-emulator, which CI caches), its signature is checked
# against the AWS publishing key (by fingerprint) every time, and it is
# installed with apt. The emulator runs with telemetry off and with SigV4
# checks on: it generates an access key for the run and rejects requests that
# are not signed with it, which is the point of running these suites here
# (ElasticMQ does not verify signatures).
#
# It serves HTTPS with a certificate generated at start, from its own CA. No
# process-wide trust setting is exported: scripts/ci/test-aws-emulator.sh hands
# ca.pem to the suites (LYRIC_SQS_CA_FILE, LYRIC_S3_CA_FILE), which trust it
# through the packages' own CA option. The aws-credentials suite is given the
# two INI files the emulator writes (its credentials file and its config
# file) and reads the key, the region, the endpoint and the CA out of them. The PKCS12 trust store is exported for
# other JVM clients; these suites do not use it.
#
# Under GitHub Actions these go to $GITHUB_ENV (the secret masked); otherwise
# they are printed as `NAME=value` lines:
#
#   LYRIC_SQS_ENDPOINT, LYRIC_S3_ENDPOINT   https://127.0.0.1:9297
#   LYRIC_SQS_REGION, LYRIC_S3_REGION       us-east-1
#   LYRIC_S3_BUCKET                         lyric-it
#   AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
#   AWS_EMULATOR_CA_FILE                    ca.pem (PEM)
#   AWS_EMULATOR_CREDENTIALS_FILE           emulator_credentials (INI, [default])
#   AWS_EMULATOR_CONFIG_FILE                config (INI: region, endpoint_url, ca_bundle)
#   AWS_EMULATOR_TRUSTSTORE                 truststore.p12 (PKCS12, no password)
set -euo pipefail

version="1.0.341"
fingerprint="98BCF336A3BE53B734E967C52E5BF49BD940F624"
port="${AWS_EMULATOR_PORT:-9297}"
pkg_dir="${AWS_EMULATOR_PKG_DIR:-$HOME/.cache/lyric-ci/aws-emulator}"
base="https://downloads.aws.com/aws-emulator/packages/aws-emulator/LTS"

case "$(dpkg --print-architecture)" in
  amd64 | arm64) arch="$(dpkg --print-architecture)" ;;
  *) echo "error: AWS Emulator has no package for $(dpkg --print-architecture)" >&2; exit 1 ;;
esac
deb="aws-emulator_${version}-1_${arch}.deb"

mkdir -p "$pkg_dir"
if [ ! -f "$pkg_dir/$deb" ] || [ ! -f "$pkg_dir/$deb.sig" ]; then
  curl -fsSLo "$pkg_dir/$deb" "$base/$deb"
  curl -fsSLo "$pkg_dir/$deb.sig" "$base/$deb.sig"
fi

work="$(mktemp -d)"
export GNUPGHOME="$work/gnupg"
mkdir -m 700 "$GNUPGHOME"
curl -fsSLo "$work/aws-publishing.asc" https://downloads.aws.com/keys/gpg/current.asc
gpg --quiet --import "$work/aws-publishing.asc"
if ! gpg --status-fd 1 --verify "$pkg_dir/$deb.sig" "$pkg_dir/$deb" 2> /dev/null | grep -q "^\[GNUPG:\] VALIDSIG $fingerprint "; then
  rm -f "$pkg_dir/$deb" "$pkg_dir/$deb.sig"
  echo "error: $deb is not signed by the AWS publishing key $fingerprint; the download was deleted" >&2
  exit 1
fi

if [ "$(aws-emulator -v 2> /dev/null | sed -n 's/^Version: //p')" != "$version" ]; then
  sudo=""
  [ "$(id -u)" -eq 0 ] || sudo="sudo"
  $sudo apt-get install -y "$pkg_dir/$deb" > "$work/apt.log" 2>&1 || { cat "$work/apt.log" >&2; exit 1; }
fi

nohup aws-emulator --port "$port" --telemetry off > "$work/aws-emulator.log" 2>&1 &

dir="$HOME/.aws/emulator/$port"
for _ in $(seq 60); do
  if [ -f "$dir/ca.pem" ] && curl -sf --cacert "$dir/ca.pem" "https://127.0.0.1:$port/__admin/health" > /dev/null; then
    key_id="$(sed -n 's/^aws_access_key_id *= *//p' "$dir/emulator_credentials")"
    secret="$(sed -n 's/^aws_secret_access_key *= *//p' "$dir/emulator_credentials")"
    if [ -z "$key_id" ] || [ -z "$secret" ]; then
      echo "error: $dir/emulator_credentials holds no access key" >&2
      exit 1
    fi
    vars=(
      "LYRIC_SQS_ENDPOINT=https://127.0.0.1:$port"
      "LYRIC_SQS_REGION=us-east-1"
      "LYRIC_S3_ENDPOINT=https://127.0.0.1:$port"
      "LYRIC_S3_REGION=us-east-1"
      "LYRIC_S3_BUCKET=lyric-it"
      "AWS_ACCESS_KEY_ID=$key_id"
      "AWS_SECRET_ACCESS_KEY=$secret"
      "AWS_EMULATOR_CA_FILE=$dir/ca.pem"
      "AWS_EMULATOR_CREDENTIALS_FILE=$dir/emulator_credentials"
      "AWS_EMULATOR_CONFIG_FILE=$dir/config"
      "AWS_EMULATOR_TRUSTSTORE=$dir/truststore.p12"
    )
    if [ -n "${GITHUB_ENV:-}" ]; then
      echo "::add-mask::$secret"
      printf '%s\n' "${vars[@]}" >> "$GITHUB_ENV"
      echo "AWS Emulator $version is up on https://127.0.0.1:$port"
    else
      printf '%s\n' "${vars[@]}"
    fi
    exit 0
  fi
  sleep 1
done
echo "error: AWS Emulator did not become healthy on port $port; log follows" >&2
cat "$work/aws-emulator.log" >&2
exit 1
