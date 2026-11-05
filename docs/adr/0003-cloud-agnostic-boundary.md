# ADR-0003: Cloud-agnostic boundary: portable core, native edges

- **Status:** Accepted
- **Date:** 2026-11-05
- **Tickets:** #4, #18 AWS Terraform, #19 Azure Terraform, #20 storage abstraction, #21 CD

## Context

The goal is a platform that runs on AWS and Azure without a rewrite, but also one that does not pretend clouds are identical. "Cloud-agnostic" taken literally (lowest common denominator everywhere) wastes managed-service value and adds abstraction layers nobody debugs. We need an explicit boundary: what is portable, what is allowed to be cloud-native, and how the code reaches storage.

## Options considered

### A. Fully cloud-native per cloud (separate implementations)
- Pros: best use of each provider (Glue, Synapse, IAM, Managed Identity); least abstraction.
- Cons: two codebases to maintain; behaviour drifts; weak story for portability.

### B. Maximum abstraction (a framework hiding every cloud API, e.g. Kubernetes-for-everything)
- Pros: one deployment model.
- Cons: large operational burden for a small project; hides cost and IAM semantics where real incidents happen; "portable" but nobody runs it.

### C. Portable core, native edges
- Pros: pipeline logic (Spark, dbt, SQL, Python) is identical on both clouds; only a thin, explicit layer differs; honest about what is not portable.
- Cons: requires discipline to keep the boundary clean; some duplication in Terraform; integration tests needed for each cloud edge.

## Decision

Adopt **C**.

**Portable (identical on all targets):**
- Spark jobs, dbt models, SQL, Great Expectations suites, Airflow DAGs, Kafka/Redpanda contracts, OpenLineage emission.
- Delta Lake as table format (ADR-0002).
- All data access goes through a **storage URI abstraction** (#20): code receives a URI from config and never builds provider-specific paths. Supported schemes: `s3a://bucket/prefix` (AWS, and MinIO locally), `abfss://container@account.dfs.core.windows.net/prefix` (ADLS Gen2), `file://` for unit tests. A small helper resolves `layer + dataset` to a URI and sets the Hadoop/Spark filesystem config for the scheme.

**Cloud-native (deliberately not abstracted):**
- Terraform modules per cloud (`infra/aws`, `infra/azure`): buckets/containers, KMS vs Key Vault, IAM roles vs RBAC and managed identities, lifecycle rules. They expose the same output contract (storage root URIs, identity names) consumed by the portable core.
- Authentication: OIDC federation for CD (#21) and instance/workload identity at runtime. No long-lived keys on either cloud.
- Managed ML/GenAI: SageMaker and Bedrock (#27, #28) are AWS-native by design; the data they read is portable.
- Snowflake is the cross-cloud warehouse and is itself cloud-neutral.

Local development uses MinIO as S3 so the `s3a://` path is exercised on every PR.

## Consequences

- Moving a pipeline between clouds means changing config (URIs, identity) plus applying the other Terraform module, not editing job code.
- The `abfss://` path needs its own integration check (Azurite or a real storage account in CD); local MinIO only proves the S3 side.
- Semantic differences remain visible: S3 is a flat key space with no atomic rename, while ADLS Gen2 has real directories and atomic rename. The Delta log protocol covers commit safety, but listing-heavy code must be tested on both.
- Cost and egress are not abstracted; cross-cloud data movement is out of scope.

## What would change my mind

- If the abstraction helper grows beyond URI resolution and filesystem config (e.g. wrapping object APIs), delete it and go native per cloud.
- If only one cloud is ever deployed in practice and the other module rots untested, drop the second target rather than keep a fiction.
- If a managed service (e.g. Unity Catalog or a cloud-native catalog) becomes the governance backbone, revisit how much sits in the portable core.
