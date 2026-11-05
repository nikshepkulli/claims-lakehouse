# ADR-0004: Streaming contract: JSON Schema in Redpanda's registry, plain-JSON wire format, confluent-kafka

- **Status:** Accepted
- **Date:** 2026-11-05
- **Tickets:** #11, #12 (consumer)

## Context

Claim events are generated as JSON lines (`docs/data-generator.md`). We need an enforceable contract, a compatibility policy, and a client library, on a laptop-sized stack (Redpanda with its built-in schema registry). The Spark Structured Streaming job (#12) is the main consumer.

## Options considered

### A. Avro + Confluent wire format (magic byte + schema id prefix)
- Pros: compact; the industry default; registry-native.
- Cons: the generator emits JSON, so we would add a conversion step; Spark needs the `spark-avro` package plus a prefix-stripping step; payloads stop being human-readable in Redpanda Console.

### B. JSON Schema, plain JSON value, schema id in a record header
- Pros: byte-for-byte what the generator writes (duplicates and out-of-order events replay verbatim); `from_json` works in Spark with no extra jar; readable in the console; the registry still versions and compat-checks the schema.
- Cons: larger payloads (zstd offsets most of it); consumers must read the header rather than a prefix to learn the schema id; Redpanda's JSON compatibility checker is less strict than Confluent's (see `docs/streaming.md`), so we add an offline check.

### C. Client library: `confluent-kafka` vs `kafka-python-ng`
- `confluent-kafka` (librdkafka): real idempotent producer and `acks=all`, mature admin API, wheels for 3.11-3.14. Cons: C extension.
- `kafka-python-ng`: pure Python. Cons: no idempotent producer, slower, thinner admin support.

## Decision

B with `confluent-kafka`, pinned in `requirements-streaming.txt`. Idempotence is a stated requirement and only librdkafka provides it properly. The registry subject is `<topic>-value`, compatibility `BACKWARD`; a breaking change ships as a new major topic (`claims.events.v2`).

## Consequences

Producers and the Spark job share `lakehouse.streaming.contract` for validation and `lakehouse.streaming.dlq` for rejects. We own the compatibility gate in CI (`tests/streaming/test_compat.py`) because the registry alone misses some breaks.

## What would change my mind

Payload volume where JSON overhead shows up in broker disk or network (then Avro with a conversion at the producer), or a second consumer language that prefers the Confluent wire format.
