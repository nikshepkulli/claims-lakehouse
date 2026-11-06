#!/usr/bin/env bash
# Integration test (ticket #13) for streaming data-quality gates, the Kafka dead-letter topic and
# DLQ replay, against the real local stack (Redpanda + the pinned Spark image).
# Needs `make up` (or `docker compose up -d redpanda`) and a venv with requirements-streaming.txt
# + requirements-claimsgen.txt. NOT wired into CI (no broker there); CI covers the same logic
# with a file source in tests/streaming/test_spark_gates.py and test_dlq_replay.py.
#
# Scenario (one throwaway topic pair, one throwaway lake):
#   A. 400 clean events           -> job (--dlq-kafka): gates pass, silver = distinct(A)
#   B. 100 events + 60 poison     -> job (--dlq-kafka --on-fail divert): dlq_rate 37% > fail 30%,
#                                    whole batch diverted: silver unchanged, DLQ table + DLQ topic
#                                    carry the 60 poison and the 100 diverted (deduped by key)
#   C. restart, no new data       -> no new DLQ topic records (checkpointed batch not re-published)
#   D. replay-dlq --source kafka  -> diverted events re-validate and go to bronze; poison stay dead
#   E. replay again               -> nothing replayed twice
#   F. job without gates          -> silver = distinct(A + B valid)
#
# Usage: PYTHON=/path/to/venv/bin/python bash tests/stack/streaming_dq_smoke.sh
#   HOST_SPARK=1  run the Spark job/replay on the host (needs pyspark + a JDK 17 + PYSPARK_PYTHON)
#                 against the published broker port, instead of the pinned Docker image.
set -uo pipefail
cd "$(dirname "$0")/../.."
[ -f .env ] && set -a && . ./.env && set +a

PYTHON=${PYTHON:-python}
export PYTHONPATH=src
export KAFKA_BOOTSTRAP=${KAFKA_BOOTSTRAP:-localhost:${REDPANDA_KAFKA_PORT:-19092}}
export SCHEMA_REGISTRY_URL=${SCHEMA_REGISTRY_URL:-http://localhost:${REDPANDA_SCHEMA_REGISTRY_PORT:-18081}}
TOPIC="claims.events.dqsmoke$(date +%s)"
LAKE=${LAKE:-data/streaming_dq_smoke_$$}
GEN=$(mktemp -d)
fail=0
cleanup() { rm -rf "$GEN" "$LAKE"; $PYTHON -m lakehouse.streaming.topics --delete "$TOPIC" >/dev/null 2>&1; }
trap cleanup EXIT
step() { printf '== %s\n' "$*"; }
ok()   { printf 'ok    %s\n' "$*"; }
bad()  { printf 'FAIL  %s\n' "$*"; fail=1; }
jget() { $PYTHON -c "import json,sys;d=json.loads(sys.argv[1]);print(eval(sys.argv[2]))" "$1" "$2"; }

HOST_SPARK=${HOST_SPARK:-0}
if [ "$HOST_SPARK" = 1 ]; then BS=$KAFKA_BOOTSTRAP; else BS=redpanda:9092; fi
NET=$([ "$HOST_SPARK" = 1 ] || docker inspect "$(docker compose ps -q redpanda)" --format '{{range $k,$v := .NetworkSettings.Networks}}{{$k}}{{end}}' 2>/dev/null | head -1)
if [ "$HOST_SPARK" != 1 ]; then
  [ -n "$NET" ] || { bad "redpanda container not found (run make up)"; exit 1; }
fi
mkdir -p "$LAKE"
# smoke gates: one rate gate that the poison batch breaches, one that the clean batch cannot
cat > "$LAKE/gates.yaml" <<'YAML'
version: 1
on_fail: divert
min_batch_rows: 20
gates:
  dlq_rate: {warn: 0.05, fail: 0.30}
  late_rate: {warn: 0.5, fail: 0.9}
YAML

step "generate events, create topic pair, register contract"
$PYTHON -m claimsgen --rows 3000 --out "$GEN" --quiet || { bad claimsgen; exit 1; }
$PYTHON -m lakehouse.streaming.topics --topic "$TOPIC" >/dev/null && \
$PYTHON -m lakehouse.streaming.register_schema --topic "$TOPIC" >/dev/null || { bad "topic/schema"; exit 1; }
SRC="$GEN/events/claim_events.jsonl"
head -n 400 "$SRC" > "$GEN/a.jsonl"
sed -n '401,500p' "$SRC" > "$GEN/b.jsonl"
distinct() { $PYTHON -c "import json,sys;print(len({json.loads(l)['event_id'] for f in sys.argv[1:] for l in open(f)}))" "$@"; }
DA=$(distinct "$GEN/a.jsonl"); DAB=$(distinct "$GEN/a.jsonl" "$GEN/b.jsonl"); DB=$(( DAB - DA ))
echo "ground truth: distinct(A)=$DA distinct(A+B)=$DAB"

produce() { $PYTHON -m lakehouse.streaming.produce --events "$1" --topic "$TOPIC" --rate 0 | tail -1; }
poison() {  # 60 messages that bypass the producer's contract check: straight onto the main topic
$PYTHON - "$TOPIC" <<'PY'
import json, sys
from confluent_kafka import Producer
from lakehouse.streaming.config import client_config
p = Producer(client_config(**{"enable.idempotence": True}))
good = {"event_id": "EVT0000000000001", "event_type": "claim_submitted", "event_ts": "2024-03-01T08:00:00Z",
        "claim_id": "CLM000000000001", "member_id": "M000000001", "provider_id": "PRV0000001",
        "claim_type": "PROFESSIONAL", "amount": 10.0}
for i in range(60):
    if i % 3 == 0:
        body = b"{not json %d" % i
    elif i % 3 == 1:
        body = json.dumps({k: v for k, v in good.items() if k != "claim_id"} | {"event_id": f"EVT{i:013d}"}).encode()
    else:
        body = json.dumps(good | {"event_id": f"EVT{i:013d}", "event_type": "claim_refunded"}).encode()
    p.produce(sys.argv[1], value=body)
p.flush(30)
PY
}

step "A. 400 clean events -> job with gates and --dlq-kafka"
produce "$GEN/a.jsonl" | grep -q "failed=0" && ok "produced A" || bad "produce A"
job() {  # $1 = extra args; prints the job output, keeps it in $GEN/job.out
  if [ "$HOST_SPARK" = 1 ]; then
    $PYTHON -m lakehouse.streaming.spark_job --source kafka --bootstrap "$BS" --topic "$TOPIC" \
      --root "$LAKE" --trigger availableNow $1 2>&1
  else
    make streaming-spark-run STREAM_TOPIC="$TOPIC" STREAM_ROOT="$LAKE" STREAM_MODE=availableNow STREAM_ARGS="$1" 2>&1
  fi | grep -v WARN | tee "$GEN/job.out" | tail -4
}
run_job() { job "--dlq-kafka --gates-config $LAKE/gates.yaml $1"; }
SUMMARY() { grep -o 'streaming-spark {.*}' "$GEN/job.out" | tail -1 | sed 's/^streaming-spark //'; }
run_job ""
S=$(SUMMARY); [ -n "$S" ] || { bad "no summary (job failed?)"; exit 1; }
[ "$(jget "$S" "d['counts']['silver_claim_events']")" = "$DA" ] && ok "silver = $DA after A" || bad "silver after A"
[ "$(jget "$S" "d['counts']['stream_dq_batches']")" -ge 1 ] && ok "gate outcomes recorded" || bad "no stream_dq_batches rows"
grep -q '"event": "dq_batch"' "$GEN/job.out" && ok "structured dq_batch log line emitted" || echo "note  (log lines filtered out of the tail)"

step "B. 100 events + 60 poison -> batch breaches dlq_rate and is diverted"
produce "$GEN/b.jsonl" | grep -q "failed=0" && ok "produced B" || bad "produce B"
poison && ok "produced 60 poison messages"
run_job "--on-fail divert"
S=$(SUMMARY)
[ "$(jget "$S" "d['counts']['silver_claim_events']")" = "$DA" ] && ok "silver unchanged (diverted batch not written)" || bad "silver changed"
EXPECT_DLQ=$(( 60 + DB ))
[ "$(jget "$S" "d['counts']['silver_claim_events_dlq']")" = "$EXPECT_DLQ" ] && ok "DLQ table = $EXPECT_DLQ (60 poison + $DB distinct diverted)" || bad "DLQ table $(jget "$S" "d['counts']['silver_claim_events_dlq']") != $EXPECT_DLQ"

check_topic() {  # prints a JSON summary of the DLQ topic: count, distinct keys, key shapes, headers, recoverable payloads
$PYTHON - "$TOPIC.dlq" <<'PY'
import base64, json, re, sys, time, uuid
from confluent_kafka import Consumer
from lakehouse.streaming.config import client_config
from lakehouse.streaming.contract import validate_message
c = Consumer(client_config(**{"group.id": "dqsmoke-" + uuid.uuid4().hex, "auto.offset.reset": "earliest", "enable.auto.commit": False}))
c.subscribe([sys.argv[1]])
msgs, idle = [], 0
while idle < 6:
    m = c.poll(1.0)
    if m is None: idle += 1; continue
    if m.error(): continue
    idle = 0; msgs.append(m)
c.close()
keys = [m.key().decode() for m in msgs]
by_reason, bad_shape, bad_payload = {}, 0, 0
for m in msgs:
    env = json.loads(m.value())
    h = {k: v.decode() for k, v in (m.headers() or [])}
    by_reason[h.get("dlq-reason")] = by_reason.get(h.get("dlq-reason"), 0) + 1
    if not re.fullmatch(r".+:\d+:\d+", m.key().decode()) or env["source_offset"] is None or env["source_topic"] is None: bad_shape += 1
    raw = base64.b64decode(env["payload_b64"])
    if env["error_type"] == "gate_diverted" and not validate_message(raw).ok: bad_payload += 1
    if h.get("dlq-reason") != env["error_type"] or h.get("dlq-stage") != "consumer": bad_shape += 1
print(json.dumps({"records": len(msgs), "distinct_keys": len(set(keys)), "by_reason": by_reason,
                  "bad_shape": bad_shape, "bad_payload": bad_payload}))
PY
}
T=$(check_topic); echo "DLQ topic: $T"
[ "$(jget "$T" "d['distinct_keys']")" = "$EXPECT_DLQ" ] && ok "DLQ topic has $EXPECT_DLQ distinct replay-safe keys" || bad "DLQ topic distinct keys"
[ "$(jget "$T" "d['bad_shape']")" = "0" ] && ok "keys are topic:partition:offset; headers match the envelope" || bad "key/header shape"
[ "$(jget "$T" "d['bad_payload']")" = "0" ] && ok "diverted payloads re-validate against the contract" || bad "diverted payload invalid"
[ "$(jget "$T" "d['by_reason'].get('gate_diverted',0)")" = "$DB" ] && [ "$(jget "$T" "sum(v for k,v in d['by_reason'].items() if k!='gate_diverted')")" = "60" ] && ok "reasons: $DB gate_diverted + 60 poison" || bad "reason split"
N_B=$(jget "$T" "d['records']")

step "C. restart with no new data -> nothing re-published"
run_job "" >/dev/null
T2=$(check_topic)
[ "$(jget "$T2" "d['records']")" = "$N_B" ] && ok "DLQ topic unchanged ($N_B records)" || bad "DLQ topic grew: $N_B -> $(jget "$T2" "d['records']")"

step "D. replay-dlq from the Kafka DLQ topic"
replay() {
  { if [ "$HOST_SPARK" = 1 ]; then
      $PYTHON -m lakehouse.streaming.spark_job replay-dlq --root "$LAKE" --topic "$TOPIC" --source kafka --bootstrap "$BS" 2>&1
    else
      make streaming-dlq-replay STREAM_TOPIC="$TOPIC" STREAM_ROOT="$LAKE" STREAM_ARGS="--source kafka --bootstrap $BS" 2>&1
    fi; } | grep -v WARN | tee "$GEN/replay.out" | tail -2; grep -o 'dlq-replay {.*}' "$GEN/replay.out" | tail -1 | sed 's/^dlq-replay //'; }
R=$(replay | tail -1); echo "replay: $R"
[ "$(jget "$R" "d['replayed']")" = "$DB" ] && [ "$(jget "$R" "d['still_invalid']")" = "60" ] && ok "replayed $DB, 60 poison still invalid" || bad "replay counts"

step "E. replay again -> idempotent"
R2=$(replay | tail -1); echo "replay: $R2"
[ "$(jget "$R2" "d['replayed']")" = "0" ] && [ "$(jget "$R2" "d['already_replayed']")" = "$DB" ] && ok "nothing replayed twice" || bad "second replay not idempotent"

step "F. job without gates picks the replayed events up"
job "--no-gates --dlq-kafka"
S=$(SUMMARY)
[ "$(jget "$S" "d['counts']['silver_claim_events']")" = "$DAB" ] && ok "silver = $DAB (A + replayed B)" || bad "silver $(jget "$S" "d['counts']['silver_claim_events']") != $DAB"

[ $fail -eq 0 ] && echo "streaming dq smoke: PASS" || echo "streaming dq smoke: FAIL"
exit $fail
