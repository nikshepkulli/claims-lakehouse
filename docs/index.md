# Documentation index

Start with the [README](../README.md) for the overview. Terms are explained in the [glossary](glossary.md). Ticket numbers (`#7`) are GitHub issues.

## Managers

- [README](../README.md): problem, architecture, JD mapping, what is real vs simulated.
- [Architecture decisions (ADRs)](adr/README.md): why the platform is shaped this way.
- [Architecture diagram detail](architecture.md): the planned platform with ticket numbers (includes planned parts).
- [Glossary](glossary.md)

## Engineers

| Area | Doc |
| --- | --- |
| Synthetic data | [data-generator.md](data-generator.md) |
| Bronze | [bronze.md](bronze.md) |
| Silver | [silver.md](silver.md) |
| Gold | [gold.md](gold.md) |
| Orchestration | [orchestration.md](orchestration.md) |
| Data quality (Great Expectations) | [data-quality.md](data-quality.md) |
| Observability | [observability.md](observability.md) |
| Features | [features.md](features.md) |
| Governed Q&A (GenAI) | [genai.md](genai.md) |
| Databricks bundle | [databricks.md](databricks.md) |
| Dashboard | [dashboard.md](dashboard.md) |
| Streaming | [streaming.md](streaming.md), [streaming-spark.md](streaming-spark.md) |
| dbt and analytics | [dbt.md](dbt.md), [analytics-guide.md](analytics-guide.md) |
| Snowflake | [snowflake.md](snowflake.md) |
| Privacy and access | [privacy-and-access.md](privacy-and-access.md) |
| Multi-cloud design | [multi-cloud.md](multi-cloud.md) |
| Terraform | [infra-aws.md](infra-aws.md), [infra-azure.md](infra-azure.md) |

## Operators

- [runbook.md](runbook.md): failed applies, rollback, drift.
- [cd.md](cd.md): OIDC plan/apply workflows and approvals.
- [ci.md](ci.md): CI jobs, required checks, Dependabot policy.
- [local-stack.md](local-stack.md): the Docker stack, ports, pins.

## Decision records

[ADR index](adr/README.md): [0001 medallion](adr/0001-medallion-layout.md), [0002 Delta](adr/0002-delta-vs-iceberg.md), [0003 cloud boundary](adr/0003-cloud-agnostic-boundary.md), [0004 streaming contract](adr/0004-streaming-contract-format.md), [0005 keyed tokens](adr/0005-keyed-tokens-for-identifiers.md).
