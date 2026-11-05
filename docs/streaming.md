# Streaming ingest (ticket #11)

Claim lifecycle events are replayed from the generator's `claim_events.jsonl` into Redpanda under a versioned, compatibility-checked contract. The Spark streaming job that consumes them (ticket #12) is documented in [streaming-spark.md](streaming-spark.md); this ticket delivers the contract, topics, producer and the validation/DLQ helpers that job will import.

Status: runs against the local Docker stack only. Nothing here has run on a managed Kafka service.

```
claim_events.jsonl --> produce.py --validate--> claims.events.v1      (key = claim_id)
                          |                         |
                          +-- invalid -----> claims.events.v1.dlq     (DLQ envelope)
register_schema.py --> registry subject claims.events.v1-value (BACKWARD)
```

## Run it

```bash
pip install -r requirements-streaming.txt -r requirements-claimsgen.txt   # python 3.11-3.13
make up
make produce-demo            # topics + register schema + generate data if missing + replay 1,000 events at 500/s
PYTHON=python make streaming-smoke   # full round trip: 1,000 events produced, consumed back and counted
make down
```

Individual tools (set `PYTHONPATH=src`; defaults target the local stack, override with `KAFKA_BOOTSTRAP` / `SCHEMA_REGISTRY_URL`):

| Command | Purpose |
|---|---|
| `python -m lakehouse.streaming.topics` | create `claims.events.v1` and `.dlq` (idempotent) |
| `python -m lakehouse.streaming.register_schema [--check-only] [--schema FILE]` | set BACKWARD, compat-check, register; exit 1 if incompatible |
| `python -m lakehouse.streaming.produce --events data/raw/events/claim_events.jsonl --rate 200 --topic claims.events.v1 [--max-events N]` | replay; `--rate 0` = unthrottled |
| `python -m lakehouse.streaming.tail --topic T --expect N` | consume from the beginning, count and validate |

The schema registry is published on host port `18081` (`REDPANDA_SCHEMA_REGISTRY_PORT`; 8081 is taken by the Spark UI). Redpanda Console on 8080 shows topics and messages.

## Contract and compatibility policy

- File: `contracts/claim_event.v1.schema.json` (JSON Schema draft-07). Subject `claims.events.v1-value`, compatibility **BACKWARD**: a consumer on the new schema can read data written with the old one, so consumers upgrade before producers.
- Open content model: unknown properties are allowed. Within v1 you may add optional properties, widen enums, relax bounds or drop `required` entries. You may not add required properties, change a type, narrow an enum, change a `pattern`/`format`, tighten bounds, or set `additionalProperties: false`.
- A breaking change is a new major: `contracts/claim_event.v2.schema.json`, topic `claims.events.v2`, run both until consumers move. Never edit v1 in place.
- Two gates enforce it: the registry (`register_schema`) and an offline checker (`lakehouse.streaming.compat`) run by `tests/streaming/test_compat.py` against `tests/streaming/golden/claim_event.v1.published.json`, the last published v1. Any breaking edit to the contract fails that test, hence CI's pytest job. After a compatible change is released, copy the contract over the golden file.
- Why both: measured on Redpanda v24.3.11, the registry rejects type changes, enum narrowing, pattern changes and closing the content model, but **accepts a new required property on an open schema**, which breaks old data. The offline check catches that. It is a conservative subset (it reports "cannot prove safe" as breaking), so the registry stays authoritative for what it does catch.
- `amount` has no `minimum`: negative amounts exist upstream as an injected defect and belong to silver-layer rules, not the wire contract. Duplicate and out-of-order events are valid by contract by design.

## Topics

| Topic | Partitions | Retention | Cleanup | Notes |
|---|---|---|---|---|
| `claims.events.v1` | 6 | 7 days | `delete` | key = `claim_id` |
| `claims.events.v1.dlq` | 1 | 30 days | `delete` | rejects, see envelope below |

Naming: `<domain>.<entity>.v<major>`; DLQ is `<topic>.dlq`; subject is `<topic>-value`.

- **Partitions:** 6 is above the 1 local worker's parallelism on purpose, so #12 exercises multi-partition reads and per-key ordering without being a toy. Key = `claim_id` keeps one claim's submitted/paid/denied events in order on one partition. Cross-claim order is not guaranteed (and the data has deliberate out-of-order events), so consumers must use event time.
- **Retention 7 days:** long enough to replay after a failed job, short enough for a laptop volume. The lake (bronze) is the system of record, not the topic.
- **Compaction: decided against.** Compaction keeps the latest record per key, which would discard `claim_submitted` once `claim_paid` arrives. The lifecycle history is the point, so the topic is a time-retained log. A compacted "current claim state" topic would be a separate derived topic if serving ever needs one.
- Replication factor 1 (single local broker); use 3 with `min.insync.replicas=2` on a real cluster.

## Delivery semantics

- **Producer to broker:** `acks=all`, `enable.idempotence=true`, at most 5 in flight: retries cannot duplicate or reorder within a partition during one producer session. Compression zstd, `linger.ms=20`.
- **Across restarts or reruns:** a replay is a new session and re-sends. End to end this is **at-least-once**. The producer does not dedupe, on purpose: the generator injects duplicate events (same `event_id`) and out-of-order events, and the consumer must handle them (dedupe on `event_id`, event-time watermark in #12).
- **Message format:** value is the generator's JSON line, verbatim (no Confluent magic-byte prefix, see ADR-0004). Headers: `schema-id` (registry id of the contract used for validation), `producer-version`. DLQ messages add `dlq-reason`, `dlq-stage`.
- Producer-side validation sends contract violations to the DLQ instead of the main topic. Consumers must still validate: schemas are advisory on the wire and other producers exist.

## Consumer helpers (for #12)

```python
from lakehouse.streaming.contract import validate_message
from lakehouse.streaming import dlq

res = validate_message(msg_value)            # never raises; res.ok, res.event, res.errors, res.error_type
if not res.ok:
    env = dlq.build_envelope(raw=msg_value, errors=res.errors, error_type=res.error_type,
                             stage="consumer", source_topic=topic, partition=p, offset=o,
                             key=key, schema_id=schema_id)
    dlq_topic, value, headers = dlq.to_message(env)
```

DLQ envelope (JSON): `envelope_version, stage (producer|consumer), source_topic, source_partition, source_offset, key, schema_id, error_type (malformed_json|contract_violation), errors[], failed_at, payload_b64`. The original bytes are base64 so a non-UTF-8 poison message survives and can be replayed with `dlq.original_payload`.

## Client library

`confluent-kafka` 2.16.1 (librdkafka), pinned in `requirements-streaming.txt`, with `jsonschema` 4.26.0 for validation; the registry client is stdlib HTTP. Chosen over `kafka-python-ng` because it provides a real idempotent producer and admin API; see ADR-0004.

## Tests

- `pytest tests/streaming` (no broker): contract valid/invalid/missing-field cases, the whole generated event stream (incl. injected defects) validates, compatibility cases including breaking changes that must be rejected plus the golden-file gate, producer logic with a fake producer (keys, headers, verbatim duplicates, `--max-events`, rate schedule and catch-up, BufferError retry, DLQ routing, delivery failures), DLQ envelope round trip, topic specs.
- `tests/stack/streaming_smoke.sh` (needs `make up`; not wired into CI because it needs Docker and the stack): creates a throwaway topic pair, registers the contract, checks the registry rejects a type-breaking schema, produces 1,000 events, consumes them back and asserts count, validity and `schema-id` header, then deletes the topics.
