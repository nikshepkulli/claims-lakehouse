# Azure infrastructure (ADLS Gen2)

Terraform module: `infra/azure/modules/lakehouse_storage`; example environment: `infra/azure/envs/dev`. Nothing here has been applied; CI runs `terraform fmt -check` and `terraform validate` only.

## Design

- One StorageV2 account with hierarchical namespace (ADLS Gen2). Each medallion layer is a filesystem (blob container): `bronze` (raw), `silver` (conformed), `gold` (marts). Filesystems are created through the management plane (`storage_account_id`), so Terraform never needs data-plane or firewall access.
- Three user-assigned managed identities, one per persona: `loader`, `transformer`, `analyst`. Extra Entra principals (CI service principals, groups) can be added through `*_principal_ids`.
- Replication is `LRS` in dev and `GZRS` in prod.
- Naming: `st<project><env>` (override with `storage_account_name` on global collision), `rg-<project>-<env>`, `id-<project>-<env>-<persona>`.

## Security decisions mapped to controls

| Decision | Setting | Control it supports |
|---|---|---|
| TLS 1.2 minimum, HTTPS only | `min_tls_version`, `https_traffic_only_enabled` | Encryption in transit (HIPAA 164.312(e), CIS Azure 3.1/3.15) |
| Public access off by default | `public_network_access_enabled=false`, firewall `Deny` | Network segmentation (CIS Azure 3.8) |
| Optional private endpoints (blob, dfs) | `enable_private_endpoint` | Private connectivity, no internet path |
| Entra-only auth | `shared_access_key_enabled=false`, `default_to_oauth_authentication` | No long-lived keys; every access attributable |
| No anonymous blobs | `allow_nested_items_to_be_public=false`, container access `private` | Data exposure prevention |
| Least-privilege RBAC at filesystem scope | role assignments per container; analyst = Blob Data Reader on gold only | Minimum necessary access (164.502(b)) |
| Soft delete + versioning | blob and container retention (default 14 d), versioning | Recoverability, ransomware/mistake protection |
| Double encryption | `infrastructure_encryption_enabled` | Defence in depth at rest |
| Customer-managed key (optional) | user-assigned identity with `Key Vault Crypto Service Encryption User`, versionless key | Key custody and revocation; Key Vault needs purge protection |
| Audit logging | diagnostic settings (StorageRead/Write/Delete, metrics) to Log Analytics | Audit controls (164.312(b)) |
| Lifecycle policy | bronze Cool 30 d / Archive 180 d; silver Cool 90 d; gold Hot; versions pruned at 90 d | Cost control and retention hygiene |
| Tags | `project`, `environment`, `managed_by`, `data_class` plus caller tags | Ownership and cost allocation |

The data is synthetic; controls are set as if it were PHI-like so the pattern is realistic.

Static analysis: the module is written to satisfy common checkov rules for `azurerm_storage_account`. Known deliberate gaps: no SAS expiration policy (shared keys are disabled) and no geo-replication in dev.

## AWS vs Azure equivalence

| Concern | AWS module | Azure module |
|---|---|---|
| Lake storage | S3 buckets (bronze/silver/gold) | ADLS Gen2 account + 3 filesystems |
| Layer isolation | bucket + bucket policy / IAM | container-scoped RBAC |
| Personas | IAM roles loader/transformer/analyst | User-assigned managed identities, same names |
| Encryption at rest | SSE-S3 or SSE-KMS (CMK) | Microsoft-managed (+ infra encryption) or CMK in Key Vault |
| TLS enforcement | bucket policy `aws:SecureTransport` | `min_tls_version`, HTTPS-only |
| Block public access | S3 Block Public Access | `allow_nested_items_to_be_public=false`, firewall `Deny` |
| Private connectivity | VPC gateway/interface endpoint | Private endpoint (blob, dfs) |
| Recovery | bucket versioning | blob versioning + soft delete |
| Tiering | lifecycle rules (IA, Glacier) | management policy (Cool, Archive) |
| Audit | CloudTrail data events / access logs | Diagnostic settings to Log Analytics |
| Interface | `project`, `environment`, `tags`, layer names, principals | Same variable names |

## Cost notes

- Idle cost is near zero: storage is per GB-month (Hot LRS is the baseline), plus transactions. Private endpoints cost roughly USD 7-8/month each (two created when enabled) plus data processed; leave them off for a demo.
- Log Analytics is billed per GB ingested; blob read/write/delete logs are the main driver. Retention defaults to 90 days; lower `log_retention_days` or drop `StorageRead` in dev if volume grows.
- Soft delete and versions are billed as stored data, so the 90-day version cleanup matters. Archive tier has early-deletion charges (180 days).
- GZRS in prod roughly doubles-plus storage cost vs LRS; dev stays LRS.

## Caveats

- Blob versioning on hierarchical-namespace accounts depends on subscription/region support; if the API rejects it, set `enable_versioning = false`. Soft delete is unaffected.
- With public access off and no private endpoint, data-plane access (Spark, azcopy) is impossible by design; enable a private endpoint or temporarily allow specific IPs.
- Local state is used in `envs/dev`; a commented `azurerm` backend example shows the remote setup.
