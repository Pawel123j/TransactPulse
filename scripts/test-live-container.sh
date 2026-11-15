#!/usr/bin/env bash
# Uses only the disposable GitHub-hosted job's Compose resources. Local users can
# run tests/test_live_pipeline.py against their own explicitly isolated services.
set -euo pipefail
if [[ "${GITHUB_ACTIONS:-}" != true ]]; then
  echo 'This harness requires a disposable GitHub Actions runner.' >&2
  exit 1
fi
export LAKEHOUSE_BUCKET="tp-stage3-$(python -c 'import uuid; print(uuid.uuid4().hex)')"
export MINIO_ROOT_USER=transactpulse-ci-fixture
export MINIO_ROOT_PASSWORD
MINIO_ROOT_PASSWORD=$(python -c 'import secrets; print(secrets.token_hex(24))')
echo "::add-mask::$MINIO_ROOT_PASSWORD"
compose=(docker compose -f infra/docker-compose.yml)
cleanup() {
  status=$?
  if [[ $status != 0 ]]; then "${compose[@]}" logs --no-color kafka minio; fi
  "${compose[@]}" down --volumes
  exit "$status"
}
trap cleanup EXIT
"${compose[@]}" up -d --no-build --wait --wait-timeout 180 kafka minio
"${compose[@]}" run --rm --no-deps createbuckets
# Extract connector coordinates from the actual runtime image, avoiding a second
# copy of the Spark/Delta/Hadoop version list in this test harness.
submit_args=$(docker image inspect transactpulse/streaming:latest \
  --format '{{json .Config.Entrypoint}}' | python -c \
  'import json,sys; print(" ".join(json.load(sys.stdin)[1:-1]) + " pyspark-shell")')
docker run --rm --network transactpulse \
  --mount "type=bind,src=$PWD,dst=/app,readonly" \
  -e TP_LIVE_INTEGRATION=1 -e TP_REQUIRE_SPARK=1 \
  -e "TP_TEST_BUCKET=$LAKEHOUSE_BUCKET" -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
  -e S3_ENDPOINT=http://minio:9000 -e "S3_ACCESS_KEY=$MINIO_ROOT_USER" \
  -e "S3_SECRET_KEY=$MINIO_ROOT_PASSWORD" \
  -e "PYSPARK_SUBMIT_ARGS=$submit_args" \
  --entrypoint bash transactpulse/streaming:latest -c '
    set -euo pipefail
    python3 -m venv --system-site-packages /tmp/live-test-env
    /tmp/live-test-env/bin/pip install --no-cache-dir -r dashboard/requirements.txt "pytest>=9.1.1,<10"
    export PYSPARK_PYTHON=/tmp/live-test-env/bin/python
    /tmp/live-test-env/bin/python -m pytest -q -p no:cacheprovider tests/test_live_pipeline.py
  '
