# Glossary

Plain-English meanings of terms used in this repo. All data here is synthetic.

| Term | Meaning |
| --- | --- |
| Claim | A request from a care provider to an insurer to be paid for care given to a member. |
| Member | A person covered by the insurance plan. |
| Medallion (bronze / silver / gold) | Three layers of data: raw copy, cleaned and checked, business-ready. See [ADR-0001](adr/0001-medallion-layout.md). |
| Bronze | The raw layer: data exactly as received, plus load details, so it can be replayed. |
| Silver | The trusted layer: correct types, duplicates removed, rule-checked. |
| Gold | The consumption layer: tables shaped for reporting (a star schema). |
| Star schema | Facts (events with amounts, such as claims) surrounded by dimensions (who, what, when, such as member and date). |
| Fact / dimension | A fact is a measurable event; a dimension describes it. |
| SCD2 (slowly changing dimension, type 2) | Keeping history when a description changes: a member who changes plan gets a new row with start and end dates instead of an overwrite. |
| Surrogate key | A generated ID for a dimension row, used instead of the source ID so history versions stay distinct. |
| Quarantine | A table that holds rows that failed a quality rule, with the reason, instead of deleting them. |
| DLQ (dead-letter queue) | The streaming version of quarantine: a separate topic or table for events that could not be processed. |
| Reconciliation | A count check that every row going in is accounted for coming out (clean + quarantined + duplicates). |
| Idempotent | Safe to run twice: re-running a job gives the same result and creates no duplicates. |
| Watermark | In streaming, "how late is too late": events older than the newest seen time minus a delay are treated as late. |
| Late-arriving data | Records that show up well after the event they describe. |
| Schema drift | A source adding or changing columns without warning. |
| Data contract | A written, versioned definition of what a data feed must look like, checked automatically. |
| Delta Lake | A table format on top of files in object storage that adds transactions, updates and history. |
| Spark | An engine for processing large datasets in parallel. |
| Kafka / Redpanda | A message broker that carries a stream of events between systems; Redpanda is a Kafka-compatible one used locally. |
| dbt | A tool that builds and tests SQL tables from version-controlled models. |
| Snowflake | A cloud data warehouse used for analytics. |
| Terraform / IaC | Describing cloud resources in code so they are reviewed and repeatable (infrastructure as code). |
| OIDC | A way for CI to get short-lived cloud access without storing passwords or keys. |
| ADR | Architecture decision record: a short note explaining one design choice and why. |
| PII / PHI | Personally identifiable / protected health information. None exists in this repo. |
| Masking / pseudonymisation | Replacing identifying values with hashes, buckets or tokens so analysts can still join and count. |
| PMPM | Per member per month: cost divided by member-months, a standard insurance metric. |
| Denial rate | Denied claims divided by all claims. |
| Lineage | The record of where a dataset came from and what it feeds. |
| Orchestration | Scheduling and sequencing jobs, with retries and backfills (Airflow is in progress). |
| Backfill | Re-running a pipeline over past dates. |
