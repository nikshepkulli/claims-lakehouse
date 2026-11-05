# Architecture

Planned platform for the claims lakehouse. Labels such as `#7` refer to GitHub issues. Design rationale lives in the [ADRs](adr/README.md): [medallion layout](adr/0001-medallion-layout.md), [Delta vs Iceberg](adr/0002-delta-vs-iceberg.md), [cloud boundary](adr/0003-cloud-agnostic-boundary.md).

## Platform overview

```mermaid
flowchart LR
  subgraph SRC["Sources"]
    GEN["Synthetic claims generator #5"]
  end

  subgraph ING["Ingest"]
    FILES["Batch files"]
    KAFKA["Kafka / Redpanda + schema registry #11"]
  end

  subgraph LAKE["Lakehouse on object storage (Delta Lake)"]
    BRZ["Bronze: raw, append-only #6"]
    SLV["Silver: typed, deduped #7"]
    QUA["Quarantine + dead-letter #7 #13"]
    GLD["Gold: facts and dims #8"]
  end

  subgraph PROC["Processing"]
    BATCH["Spark batch #7 #8"]
    STREAM["Spark Structured Streaming #12"]
    DBX["Databricks bundle #10"]
  end

  subgraph WH["Warehouse and serving"]
    SNOW["Snowflake #14 #16"]
    DBT["dbt: staging, marts #15"]
    SEM["Semantic layer #17"]
    DASH["Dashboards #29 #30"]
  end

  subgraph MLAI["ML and GenAI"]
    FEAT["Point-in-time features #26"]
    SM["SageMaker training and scoring #27"]
    BED["Bedrock governed Q&A #28"]
  end

  subgraph GOV["Quality, lineage, governance"]
    GE["Great Expectations #22"]
    OBS["Observability and alerts #23"]
    LIN["OpenLineage + Marquez #24"]
    PRIV["PII tagging and access control #25"]
  end

  subgraph ORCH["Orchestration"]
    AF["Airflow #9"]
  end

  GEN --> FILES
  GEN --> KAFKA
  FILES --> BATCH
  KAFKA --> STREAM
  BATCH --> BRZ
  STREAM --> BRZ
  BRZ --> SLV
  SLV --> GLD
  SLV -.-> QUA
  DBX -. runs same jobs .- BATCH
  GLD --> SNOW
  SNOW --> DBT
  DBT --> SEM
  SEM --> DASH
  GLD --> FEAT
  FEAT --> SM
  SEM --> BED

  GE -. gates .- SLV
  GE -. gates .- GLD
  AF -. schedules .- BATCH
  AF -. schedules .- DBT
  BATCH -. events .-> LIN
  DBT -. events .-> LIN
  OBS -. metrics .- AF
  PRIV -. policies .- GLD
  PRIV -. policies .- SNOW
```

## Infrastructure and delivery

```mermaid
flowchart TB
  CI["CI: ruff, pytest, sqlfluff, terraform validate #2"]
  CD["CD with OIDC, approvals #21"]
  ABS["Storage URI abstraction #20: s3a, abfss, file"]
  LOCAL["Local stack: MinIO, Redpanda, Spark, Postgres, Marquez #3"]

  subgraph AWS["AWS #18"]
    S3["S3 bronze, silver, gold"]
    KMS["KMS"]
    IAM["IAM least privilege"]
  end

  subgraph AZ["Azure #19"]
    ADLS["ADLS Gen2"]
    KV["Key Vault"]
    RBAC["RBAC and managed identities"]
  end

  CI --> CD
  CD --> AWS
  CD --> AZ
  ABS --> S3
  ABS --> ADLS
  ABS --> LOCAL
  S3 --- KMS
  S3 --- IAM
  ADLS --- KV
  ADLS --- RBAC
```

## Notes

- Streaming and batch converge on the same Delta tables, so business logic exists once.
- Portable: Spark, dbt, SQL, Airflow, Great Expectations, lineage. Cloud-native: Terraform modules, identity, SageMaker/Bedrock. See ADR-0003.
- Snowflake receives gold by load (primary) or reads it as Iceberg metadata over Delta files (secondary). See ADR-0002.
