#!/usr/bin/env bash
# Probes every local-stack endpoint. Exit non-zero if any check fails.
# Usage: bash tests/stack/smoke.sh   (set LINEAGE=1 to also check Marquez)
set -uo pipefail
cd "$(dirname "$0")/../.."
[ -f .env ] && set -a && . ./.env && set +a

fail=0
check() { # name, command...
  local name=$1; shift
  if "$@" >/dev/null 2>&1; then printf 'ok    %s\n' "$name"; else printf 'FAIL  %s\n' "$name"; fail=1; fi
}
http() { curl -fsS --max-time 5 "$1"; }
dc() { docker compose --profile lineage "$@"; }

check "minio api"          http "http://localhost:${MINIO_API_PORT:-9000}/minio/health/live"
check "minio console"      http "http://localhost:${MINIO_CONSOLE_PORT:-9001}/"
for b in bronze silver gold; do
  check "minio bucket $b" dc run --rm --no-deps -T --entrypoint /bin/sh minio-init \
    -c "mc alias set l http://minio:9000 \"\$MINIO_ROOT_USER\" \"\$MINIO_ROOT_PASSWORD\" && mc stat l/$b"
done
check "redpanda admin"     http "http://localhost:${REDPANDA_ADMIN_PORT:-9644}/v1/status/ready"
check "redpanda console"   http "http://localhost:${REDPANDA_CONSOLE_PORT:-8080}/admin/health"
check "spark master ui"    http "http://localhost:${SPARK_MASTER_UI_PORT:-8081}/"
check "spark worker ui"    http "http://localhost:${SPARK_WORKER_UI_PORT:-8082}/"
check "spark worker registered" bash -c "curl -fsS --max-time 5 http://localhost:${SPARK_MASTER_UI_PORT:-8081}/json/ | grep -q '\"status\" *: *\"ALIVE\"'"
check "postgres"           dc exec -T postgres pg_isready -U "${POSTGRES_USER:-lakehouse}"
if [ -n "${LINEAGE:-}" ]; then
  check "marquez api"      http "http://localhost:${MARQUEZ_API_PORT:-5002}/api/v1/namespaces"
  check "marquez web"      http "http://localhost:${MARQUEZ_WEB_PORT:-3002}/"
fi
exit $fail
