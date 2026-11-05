#!/usr/bin/env bash
# Wait until every long-running compose service reports healthy, and one-shot
# jobs (minio-init) exited 0. (`docker compose up --wait` treats an exited
# one-shot container as a failure, hence this script.)
# Usage: scripts/wait-healthy.sh [timeout_seconds]
set -uo pipefail
cd "$(dirname "$0")/.."
timeout=${1:-240}
deadline=$((SECONDS + timeout))
while :; do
  pending=$(docker compose --profile lineage ps -a --format '{{.Service}}|{{.State}}|{{.Health}}|{{.ExitCode}}' | awk -F'|' '
    $2=="exited" && $4==0 {next}
    $2=="running" && $3=="healthy" {next}
    {print $1 " (" $2 (($3!="")?"/" $3:"") ")"}')
  [ -z "$pending" ] && { echo "all services healthy"; exit 0; }
  if [ $SECONDS -ge $deadline ]; then
    echo "timed out after ${timeout}s; not ready:" >&2; echo "$pending" >&2; exit 1
  fi
  sleep 3
done
