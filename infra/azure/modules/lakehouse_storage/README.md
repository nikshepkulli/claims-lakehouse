# lakehouse_storage (Azure)

ADLS Gen2 foundation for the claims lakehouse: one HNS storage account, `bronze`/`silver`/`gold` filesystems, per-persona user-assigned managed identities with filesystem-scoped RBAC, lifecycle tiering, optional private endpoints and customer-managed key, diagnostics to Log Analytics.

Design and control mapping: [docs/infra-azure.md](../../../../docs/infra-azure.md).

## Usage

```hcl
module "lakehouse_storage" {
  source      = "../../modules/lakehouse_storage"
  project     = "claims-lakehouse"
  environment = "dev"
}
```

## Key inputs

| Name | Default | Notes |
|---|---|---|
| `project`, `environment`, `tags` | - | Same names as the AWS module |
| `bronze_name` / `silver_name` / `gold_name` | `bronze`/`silver`/`gold` | Filesystem names |
| `loader_principal_ids` / `transformer_principal_ids` / `analyst_principal_ids` | `[]` | Extra Entra object IDs, added to the module-created identities |
| `public_network_access_enabled` | `false` | Firewall default action is always `Deny` |
| `enable_private_endpoint`, `private_endpoint_subnet_id`, `private_dns_zone_ids` | `false` | blob + dfs endpoints |
| `enable_customer_managed_key`, `key_vault_id`, `key_vault_key_id` | `false` | Versionless key ID for auto-rotation |
| `enable_versioning` | `false` | Versioning is unsupported on HNS accounts; `true` fails a precondition |
| `enable_snapshot_cleanup` | `false` | Snapshots are preview-only on HNS; action never emitted |
| `shared_access_key_enabled` | `false` | Entra-only auth |

## Key outputs

`storage_account_name`, `dfs_endpoint`, `filesystem_names`, `abfss_uris`, `principals` (loader/transformer/analyst identity IDs), `log_analytics_workspace_id`.

## RBAC matrix

| Persona | bronze | silver | gold |
|---|---|---|---|
| loader | Blob Data Contributor | - | - |
| transformer | Blob Data Reader | Blob Data Contributor | Blob Data Contributor |
| analyst | - | - | Blob Data Reader |

Roles are assigned at the container (filesystem) scope, not the account.
