# ADR-0002: Table format: Delta Lake, with Iceberg interop at the edge

- **Status:** Accepted
- **Date:** 2026-11-05
- **Tickets:** #4, #7 silver, #10 Databricks bundle, #14 Snowflake load

## Context

The lakehouse needs ACID tables on object storage with upserts (claim adjustments), schema evolution, time travel for audit, and both batch and streaming writers. Compute is Spark locally (docker-compose, #3) and on Databricks (#10); the warehouse is Snowflake (#14). The choice is between the open table formats Delta Lake and Apache Iceberg (Hudi is considered below). This is a single-team project, so "who writes" matters more than "who might read someday".

## Options considered

### A. Delta Lake
- Pros: first-class in Spark and Databricks (the runtime we target), mature `MERGE`, streaming sink with exactly-once via transaction log, Change Data Feed, `OPTIMIZE`/`ZORDER`, easy local use with `delta-spark`, no catalog needed (path-based).
- Cons: historically a Databricks-centric ecosystem; non-Spark engines lag in write support; Snowflake cannot natively write it.

### B. Apache Iceberg
- Pros: broadest engine neutrality (Spark, Trino, Flink, Snowflake, Athena); hidden partitioning and partition evolution are better designed; strong catalog story (REST, Glue, Polaris).
- Cons: requires a catalog even to get started; Spark streaming and `MERGE` ergonomics are good but local-dev setup is heavier; on Databricks it is supported but Delta remains the best-integrated path; maintenance (compaction, snapshot expiry) is more manual.

### C. Apache Hudi
- Pros: strong record-level upsert/CDC design.
- Cons: smallest ecosystem of the three, least support in Snowflake and Databricks; hardest to justify to a team that must hire for it.

### D. Delta with UniForm (Iceberg metadata generated alongside Delta)
- Pros: one writer (Delta), Iceberg-readable metadata for engines that want it, so the interop door stays open at low cost.
- Cons: still maturing; read-only from the Iceberg side; feature set limited versus native Iceberg; some Delta features (e.g. deletion vectors) constrain compatibility.

## Decision

**Delta Lake is the system of record for bronze, silver and gold.** Interop with Snowflake is handled at the edge:

1. Primary path: Snowflake **loads gold** into native tables via dbt (#14, #15), because we want Snowflake's performance and governance features on the serving layer.
2. Secondary path, to demonstrate openness: expose gold as **Iceberg** (UniForm metadata, or Snowflake external/Iceberg tables over the same files) so Snowflake can query without copying. Documented and tested as a read path, not relied on for the main demo.

We do not run two writers against the same table.

## Consequences

- Local, CI and Databricks use the same format with zero catalog setup, so `make demo` stays fast.
- Snowflake sees the data either as loaded tables (fast, costs credits) or as Iceberg (no copy, slower, read-only); the trade-off is measured in #16.
- We accept coupling to Delta's feature set; any UniForm incompatibility (e.g. deletion vectors) must be tracked and may require disabling a feature on gold tables.
- Table maintenance (`OPTIMIZE`, `VACUUM`) is our job and goes into the Airflow DAGs (#9).

## What would change my mind

- A second non-Spark writer (Flink, Trino) needs to write the same tables: move to Iceberg with a REST/Glue catalog.
- UniForm or Snowflake's Delta support cannot read gold correctly in a test (schema evolution, deletes) after reasonable fixes.
- The Databricks leg (#10) is dropped and Snowflake becomes the main compute: Iceberg becomes the better neutral format.
- Org-level mandate for a catalog-governed multi-engine lake (Polaris/Unity open APIs) makes Iceberg the default.
