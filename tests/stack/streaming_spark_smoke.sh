#!/usr/bin/env bash
# Integration test for the Spark streaming job against the real local stack (Redpanda + Spark image).
# Needs `make up` first and a venv with requirements-streaming.txt + requirements-claimsgen.txt.
# NOT wired into CI: CI has no broker (see docs/streaming-spark.md); CI covers the same job
# logic with a file source in tests/streaming/test_spark_job.py.
#
# Usage: PYTHON=/path/to/venv/bin/python bash tests/stack/streaming_spark_smoke.sh
# Env:   N=5000 events, WORK=dir for the lake (default: data/streaming_smoke, git-ignored data/)
set -uo pipefail
cd "$(dirname "$0")/../.."
[ -f .env ] && set -a && . ./.env && set +a

PYTHON=${PYTHON:-python}
export PYTHONPATH=src
export KAFKA_BOOTSTRAP=${KAFKA_BOOTSTRAP:-localhost:${REDPANDA_KAFKA_PORT:-19092}}
export SCHEMA_REGISTRY_URL=${SCHEMA_REGISTRY_URL:-http://localhost:${REDPANDA_SCHEMA_REGISTRY_PORT:-18081}}
N=${N:-5000}
TOPIC="claims.events.sparksmoke$(date +%s)"
LAKE=${LAKE:-data/streaming_smoke_$$}
GEN=$(mktemp -d)
fail=0
cleanup() { rm -rf "$GEN" "$LAKE"; $PYTHON -m lakehouse.streaming.topics --delete "$TOPIC" >/dev/null 2>&1; }
trap cleanup EXIT
step() { printf '== %s\n' "$*"; }
ok()   { printf 'ok    %s\n' "$*"; }
bad()  { printf 'FAIL  %s\n' "$*"; fail=1; }

NET=$(docker inspect "$(docker compose ps -q redpanda)" --format '{{range $k,$v := .NetworkSettings.Networks}}{{$k}}{{end}}' 2>/dev/null | head -1)
[ -n "$NET" ] || { bad "redpanda container not found (run make up)"; exit 1; }

step "generate events and produce $N (incl. injected duplicate / out-of-order events)"
$PYTHON -m claimsgen --rows 6000 --out "$GEN" --quiet || { bad claimsgen; exit 1; }
$PYTHON -m lakehouse.streaming.topics --topic "$TOPIC" >/dev/null && \
$PYTHON -m lakehouse.streaming.register_schema --topic "$TOPIC" >/dev/null || { bad "topic/schema"; exit 1; }
head -n "$N" "$GEN/events/claim_events.jsonl" > "$GEN/first.jsonl"
$PYTHON -m lakehouse.streaming.produce --events "$GEN/first.jsonl" --topic "$TOPIC" --rate 0 | tee "$GEN/produce.out"
grep -q "sent=$N dead_lettered=0 .* failed=0" "$GEN/produce.out" && ok "produced $N" || bad "produce"
# ground truth from the file itself
DISTINCT=$($PYTHON -c "import json,sys;print(len({json.loads(l)['event_id'] for l in open(sys.argv[1])}))" "$GEN/first.jsonl")
echo "distinct event_ids in the $N produced: $DISTINCT"

run_job() {
  make streaming-spark-run STREAM_TOPIC="$TOPIC" STREAM_ROOT="$LAKE" STREAM_MODE=availableNow \
    STREAM_ARGS="--max-offsets-per-trigger 1500" 2>&1 | grep -v WARN | tee "$GEN/job.out" | tail -3
}

step "run 1: Kafka -> bronze -> silver (availableNow, 1,500 offsets per batch)"
start=$(date +%s); run_job; secs=$(( $(date +%s) - start ))
SUMMARY=$(grep -o 'streaming-spark {.*}' "$GEN/job.out" | tail -1 | sed 's/^streaming-spark //')
echo "summary: $SUMMARY (wall ${secs}s including JVM start and Kafka package resolution)"
count() { $PYTHON -c "import json,sys;print(json.loads(sys.argv[1])['counts'][sys.argv[2]])" "$SUMMARY" "$1"; }
[ "$(count bronze_claim_events)" = "$N" ] && ok "bronze = $N raw rows (duplicates kept)" || bad "bronze $(count bronze_claim_events) != $N"
[ "$(count silver_claim_events)" = "$DISTINCT" ] && ok "silver = $DISTINCT distinct events" || bad "silver $(count silver_claim_events) != $DISTINCT"
echo "late=$(count silver_claim_events_late) dlq=$(count silver_claim_events_dlq) claims=$(count claim_status)"

step "run 2: restart from checkpoints with no new data -> nothing changes"
run_job >/dev/null
SUMMARY2=$(grep -o 'streaming-spark {.*}' "$GEN/job.out" | tail -1 | sed 's/^streaming-spark //')
for t in bronze_claim_events silver_claim_events silver_claim_events_late silver_claim_events_dlq claim_status; do
  [ "$($PYTHON -c "import json,sys;print(json.loads(sys.argv[1])['counts'][sys.argv[2]])" "$SUMMARY2" $t)" = "$(count $t)" ] || bad "$t changed on restart"
done
[ $fail -eq 0 ] && ok "restart idempotent"

[ $fail -eq 0 ] && echo "streaming spark smoke: PASS" || echo "streaming spark smoke: FAIL"
exit $fail
