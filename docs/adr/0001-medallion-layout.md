# ADR-0001: Medallion layout (bronze / silver / gold)

- **Status:** Accepted
- **Date:** 2026-11-05
- **Tickets:** #4 (this record), #6 bronze, #7 silver, #8 gold, #12 streaming
- **Amended:** 2026-11-05: the bronze ingestion-timestamp column is `_ingest_ts` (as implemented in #6), not `_ingested_at` as first written, and `_ingest_date` (the batch date, the partition column) was added to the metadata list. Decision unchanged.

## Context

The platform ingests synthetic healthcare claims (members, providers, claims, claim lines, diagnoses) as both batch files and a Kafka event stream. Claims data has properties that drive the layout: it arrives late and out of order, claims are adjusted and reversed (the same claim ID appears multiple times), source feeds are dirty (bad codes, missing keys), and it carries PII that must be isolated early. Consumers range from analysts (Snowflake/BI) to ML feature pipelines. We need a layout that makes reprocessing cheap, makes bad data visible rather than silently dropped, and gives each consumer a stable contract.

## Options considered

### A. Single curated layer (ETL straight into a warehouse model)
- Pros: fewest moving parts; least storage.
- Cons: no replayable raw record, so a logic bug means re-extracting from sources; bad rows are dropped or fixed in flight with no audit trail; streaming and batch logic diverge.

### B. Two layers (raw + curated)
- Pros: raw is replayable; simpler than three layers.
- Cons: "curated" ends up doing cleansing, dedupe, conformance and business modelling at once, so it is hard to test and hard to reuse for ML versus BI.

### C. Three layers: bronze / silver / gold (medallion)
- Pros: each layer has one job and one test strategy; bronze gives cheap replay; silver is the single trusted, typed, deduplicated record shared by BI and ML; gold is shaped for consumption. Maps cleanly to Spark jobs, dbt models and quality gates per layer.
- Cons: three copies of data (storage cost); more jobs to orchestrate; the pattern is easy to cargo-cult into empty layers.

### D. Data Vault in the middle
- Pros: strong auditability and handling of changing sources.
- Cons: heavy modelling overhead for a single-domain project with one team; join-heavy for analysts.

## Decision

Use **C, medallion**, with these rules:

- **Bronze:** append-only, source-shaped, plus ingestion metadata (`_ingest_ts`, `_source_file`, `_batch_id`, `_ingest_date`). Schema captured, not enforced. Idempotent re-runs keyed on batch ID.
- **Silver:** typed, deduplicated on claim business key with latest-adjustment-wins, validated. Rows failing checks go to a **quarantine table** with a reason code, never dropped. PII columns tagged here.
- **Gold:** dimensional model (claim facts; member, provider, diagnosis, date dimensions) built for analytics. Gold is what Snowflake and dbt consume.
- Streaming and batch write to the **same** bronze/silver tables (Delta supports both), so there is one set of business logic.

## Consequences

- Reprocessing silver or gold from bronze is a job re-run, not a re-extraction. Backfills (#9) rely on this.
- Quality gates (#22, #13) attach naturally at the bronze-to-silver and silver-to-gold boundaries.
- Storage roughly triples for hot data; mitigated by lifecycle rules on bronze (#18, #19) and `VACUUM` retention.
- We must resist adding layers: anything that is not "raw", "trusted" or "consumable" belongs inside one of the three.

## What would change my mind

- If measured bronze storage cost exceeds ~25% of total platform cost with no replay events in 90 days, shorten bronze retention or collapse to a landing zone with expiry.
- If a second domain with many conflicting sources joins, evaluate a Data Vault-style integration layer between silver and gold.
- If gold consumers are exclusively served from Snowflake and the Delta gold copy is never read, make Delta gold optional and build gold in dbt only.
