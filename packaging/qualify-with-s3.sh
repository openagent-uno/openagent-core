#!/usr/bin/env bash
# Run the Linux wheel qualification against a private, ephemeral S3 endpoint.
set -euo pipefail
work=$(mktemp -d "${RUNNER_TEMP:?}/s3-qualification.XXXXXXXX")
curl --fail --silent --show-error --location --retry 3 \
  https://github.com/seaweedfs/seaweedfs/releases/download/4.55/linux_amd64.tar.gz -o "$work/weed.tar.gz"
printf '%s  %s\n' 'f156c609eeaf9e5fcfeea33fd70411222005f83760aa95893c9d58eff006b45c' "$work/weed.tar.gz" | sha256sum --check
tar -xzf "$work/weed.tar.gz" -C "$work" weed
export OPENAGENT_TEST_S3_ENDPOINT=http://127.0.0.1:8333
export OPENAGENT_TEST_S3_KEY=qualification
export OPENAGENT_TEST_S3_SECRET=local-only-engine-qualification
cat > "$work/s3.json" <<'JSON'
{"identities":[{"name":"qualification","credentials":[{"accessKey":"qualification","secretKey":"local-only-engine-qualification"}],"actions":["Admin","Read","Write","List","Tagging"]}]}
JSON
mkdir "$work/data"
(cd "$work" && exec ./weed mini -ip=127.0.0.1 -ip.bind=127.0.0.1 -dir="$work/data" \
  -s3.config="$work/s3.json" -volume.max=3 -admin.ui=false) > "$work/server.log" 2>&1 &
server_pid=$!
trap 'kill "$server_pid" 2>/dev/null || true' EXIT
for attempt in {1..60}; do
  kill -0 "$server_pid"
  if curl --silent --max-time 1 --output /dev/null "$OPENAGENT_TEST_S3_ENDPOINT/"; then
    "$@"
    exit
  fi
  sleep 1
done
echo "S3 qualification endpoint failed to start" >&2
exit 1
