#!/usr/bin/env bash
# Streaming integration test against the running local stack (`make up` first).
# (The registry also accepts a new required field on an open schema; that break is caught offline
# by tests/streaming/test_compat.py, see docs/streaming.md.)
# Creates a throwaway topic pair, registers the contract, produces 1,000 generated events,
# consumes them back and counts them, proves the registry rejects a breaking schema, cleans up.
#
# Usage: PYTHON=/path/to/venv/bin/python bash tests/stack/streaming_smoke.sh
# Needs: requirements-streaming.txt + requirements-claimsgen.txt installed in $PYTHON.
# Not wired into CI (needs Docker + the stack); run locally or in a manual workflow.
set -uo pipefail
cd "$(dirname "$0")/../.."
[ -f .env ] && set -a && . ./.env && set +a

PYTHON=${PYTHON:-python}
export PYTHONPATH=src
export KAFKA_BOOTSTRAP=${KAFKA_BOOTSTRAP:-localhost:${REDPANDA_KAFKA_PORT:-19092}}
export SCHEMA_REGISTRY_URL=${SCHEMA_REGISTRY_URL:-http://localhost:${REDPANDA_SCHEMA_REGISTRY_PORT:-18081}}
N=${N:-1000}
TOPIC="claims.events.smoke$(date +%s)"
WORK=$(mktemp -d)
fail=0
trap 'rm -rf "$WORK"; $PYTHON -m lakehouse.streaming.topics --delete "$TOPIC" >/dev/null 2>&1' EXIT

step() { printf '== %s\n' "$*"; }
ok()   { printf 'ok    %s\n' "$*"; }
bad()  { printf 'FAIL  %s\n' "$*"; fail=1; }

step "registry + broker reachable"
curl -fsS --max-time 5 "$SCHEMA_REGISTRY_URL/subjects" >/dev/null && ok "schema registry" || { bad "schema registry at $SCHEMA_REGISTRY_URL (is the stack up?)"; exit 1; }

step "generate events"
$PYTHON -m claimsgen --rows 2000 --out "$WORK/data" --quiet || { bad "claimsgen"; exit 1; }
EVENTS="$WORK/data/events/claim_events.jsonl"

step "create topics"
$PYTHON -m lakehouse.streaming.topics --topic "$TOPIC" && ok "topics $TOPIC (+ .dlq)" || bad "topics"

step "register contract"
$PYTHON -m lakehouse.streaming.register_schema --topic "$TOPIC" && ok "register BACKWARD" || bad "register"
$PYTHON -m lakehouse.streaming.register_schema --topic "$TOPIC" >/dev/null && ok "re-register is idempotent" || bad "re-register"

step "breaking schema is rejected by the registry"
$PYTHON - "$WORK/broken.json" <<'PY'
import json, sys
from lakehouse.streaming.contract import load_schema
s = json.loads(json.dumps(load_schema()))
s["properties"]["amount"]["type"] = "string"  # number -> string: a type break
json.dump(s, open(sys.argv[1], "w"))
PY
$PYTHON -m lakehouse.streaming.register_schema --topic "$TOPIC" --schema "$WORK/broken.json" --check-only >/dev/null 2>&1
[ $? -eq 1 ] && ok "breaking change -> exit 1" || bad "breaking schema was not rejected"

step "produce $N events"
$PYTHON -m lakehouse.streaming.produce --events "$EVENTS" --topic "$TOPIC" --rate 0 --max-events "$N" \
  | tee "$WORK/produce.out"
grep -q "sent=$N dead_lettered=0 .* failed=0" "$WORK/produce.out" && ok "producer sent $N, 0 failed" || bad "producer summary"

step "consume them back"
$PYTHON -m lakehouse.streaming.tail --topic "$TOPIC" --expect "$N" --timeout 20 | tee "$WORK/tail.out"
grep -q "consumed=$N valid=$N with_schema_id_header=$N " "$WORK/tail.out" && ok "consumed $N, all valid, all carry schema-id" || bad "consume count"

[ $fail -eq 0 ] && echo "streaming smoke: PASS" || echo "streaming smoke: FAIL"
exit $fail
