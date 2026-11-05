# Azure infrastructure (ADLS Gen2)

Terraform module: `infra/azure/modules/lakehouse_storage`; example environment: `infra/azure/envs/dev`. Nothing here has been applied; CI runs `terraform fmt -check` and `terraform validate` only.

## Design

- One StorageV2 account with hierarchical namespace (ADLS Gen2). Each medallion layer is a filesystem (blob container): `bronze` (raw), `silver` (conformed), `gold` (marts). Filesystems are created through the management plane (`storage_account_id`), so Terraform never needs data-plane or firewall access.
- Three user-assigned managed identities, one per persona: `loader`, `transformer`, `analyst`. Extra Entra principals (CI service principals, groups) can be added through `*_principal_ids`.
- Replication is `LRS` in dev and `GZRS` in prod.
- Naming: storage account = `st<project><env>` truncated to 17 chars plus a 6-char suffix, the first 6 hex chars of `sha1(subscription_id/resource_group_name)` (e.g. `stclaimslakehouse1a2b3c`; at most 23 chars). Both inputs are known at plan time, so the name is stable and mostly collision-free across subscriptions; `data.azurerm_client_config` needs no API call at `validate`. Set `storage_account_name` to pin an exact name. Resource group: `rg-<project>-<env>`, `id-<project>-<env>-<persona>`.

## Security decisions mapped to controls

| Decision | Setting | Control it supports |
|---|---|---|
| TLS 1.2 minimum, HTTPS only | `min_tls_version`, `https_traffic_only_enabled` | Encryption in transit (HIPAA 164.312(e), CIS Azure 3.1/3.15) |
| Public access off by default | `public_network_access_enabled=false`, firewall `Deny` | Network segmentation (CIS Azure 3.8) |
| Optional private endpoints (blob, dfs) | `enable_private_endpoint` | Private connectivity, no internet path |
| Entra-only auth | `shared_access_key_enabled=false`, `default_to_oauth_authentication` | No long-lived keys; every access attributable |
| No anonymous blobs | `allow_nested_items_to_be_public=false`, container access `private` | Data exposure prevention |
| Least-privilege RBAC at filesystem scope | role assignments per container; analyst = Blob Data Reader on gold only | Minimum necessary access (164.502(b)) |
| Soft delete | blob and container retention (default 14 d); versioning is NOT available on HNS (see Caveats) | Recoverability, ransomware/mistake protection |
| Double encryption | `infrastructure_encryption_enabled` | Defence in depth at rest |
| Customer-managed key (optional) | user-assigned identity with `Key Vault Crypto Service Encryption User`, versionless key | Key custody and revocation; Key Vault needs purge protection |
| Audit logging | diagnostic settings (StorageRead/Write/Delete, metrics) to Log Analytics | Audit controls (164.312(b)) |
| Lifecycle policy | bronze Cool 30 d / Archive 180 d; silver Cool 90 d; gold Hot; version/snapshot pruning only when supported (not on HNS) | Cost control and retention hygiene |
| Tags | `project`, `environment`, `managed_by`, `data_class` plus caller tags | Ownership and cost allocation |

The data is synthetic; controls are set as if it were PHI-like so the pattern is realistic.

Static analysis: the module is written to satisfy common checkov rules for `azurerm_storage_account`. Intentionally accepted findings carry `# checkov:skip=<ID>: <reason>` comments in `main.tf` (optional CMK, optional private endpoint, LRS in dev). Known deliberate gaps: no SAS expiration policy (shared keys are disabled) and no geo-replication in dev.

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
| Recovery | bucket versioning | soft delete only (blob versioning unsupported on HNS) |
| Tiering | lifecycle rules (IA, Glacier) | management policy (Cool, Archive) |
| Audit | CloudTrail data events / access logs | Diagnostic settings to Log Analytics |
| Interface | `project`, `environment`, `tags`, layer names, principals | Same variable names |

## Cost notes

- Idle cost is near zero: storage is per GB-month (Hot LRS is the baseline), plus transactions. Private endpoints cost roughly USD 7-8/month each (two created when enabled) plus data processed; leave them off for a demo.
- Log Analytics is billed per GB ingested; blob read/write/delete logs are the main driver. Retention defaults to 90 days; lower `log_retention_days` or drop `StorageRead` in dev if volume grows.
- Soft-deleted data is billed as stored data. Archive tier has early-deletion charges (180 days).
- GZRS in prod roughly doubles-plus storage cost vs LRS; dev stays LRS.

## Caveats

- **HNS feature limits.** Checked 2026-11-05 against Microsoft Learn "Blob Storage feature support in Azure storage accounts" (https://learn.microsoft.com/en-us/azure/storage/blobs/storage-feature-support-in-storage-accounts, page ms.date 2026-05-18): for standard general-purpose v2 with HNS, **blob versioning is not supported**, **blob snapshots are preview-level**, and change feed, point-in-time restore, object replication and version-level WORM are also unsupported. Soft delete (blobs, containers), lifecycle tiering/delete, container-level WORM, CMK and Entra auth are supported.
- Consequently `enable_versioning` defaults to `false` and setting it `true` fails a plan-time precondition. `enable_snapshot_cleanup` (default `false`) is gated the same way. Lifecycle `version`/`snapshot` actions are emitted through `dynamic` blocks only when the feature is enabled and supported, and the `gold-version-cleanup` rule disappears with them, so the policy stays valid. Recovery on this account relies on soft delete (14 d default).
- With public access off and no private endpoint, data-plane access (Spark, azcopy) is impossible by design; enable a private endpoint or temporarily allow specific IPs.
- Local state is used in `envs/dev`; a commented `azurerm` backend example shows the remote setup.

## Interface differences vs AWS module

Variables are intentionally not renamed (renaming would break existing callers). Ticket #20 reconciles the interfaces at the **output** side instead: both modules now emit `layer_uris` and `principals`, validated by `infra/shared/modules/lakehouse_contract`. See [multi-cloud.md](multi-cloud.md#interface-mapping).

| Concern | AWS module | Azure module |
|---|---|---|
| Name prefix | `name_prefix` | `project` (default `claims-lakehouse`) |
| Environment | `environment` | `environment` (validated: dev/staging/prod) |
| Tags | `tags` | `tags` |
| Layer names | fixed bucket naming from `name_prefix` | `bronze_name`, `silver_name`, `gold_name` |
| Loader principals | `loader_trusted_principal_arns` | `loader_principal_ids` (Entra object IDs) |
| Transformer principals | `transformer_trusted_principal_arns` | `transformer_principal_ids` |
| Analyst principals | `analyst_trusted_principal_arns` | `analyst_principal_ids` |
| Version retention | `noncurrent_version_expiration_days` | `noncurrent_version_delete_after_days` (no-op on HNS) |
| Bronze tiering | `bronze_intelligent_tiering_days` | `bronze_cool_after_days`, `bronze_archive_after_days` |
| Log retention | `access_log_retention_days` | `log_retention_days` |
| Encryption key | `kms_deletion_window_days` (module-created KMS) | `enable_customer_managed_key`, `key_vault_id`, `key_vault_key_id` (BYO Key Vault key) |
| Destroy safety | `force_destroy` | none (not exposed) |
| Azure-only | - | `location`, `resource_group_name`, `storage_account_name`, network and private-endpoint inputs, `shared_access_key_enabled`, `enable_snapshot_cleanup` |
| AWS-only | `max_session_duration` | - |

Principal inputs differ in kind: AWS takes trusted principal ARNs that may assume the role; Azure takes object IDs that receive RBAC directly (the module also creates one managed identity per persona).
