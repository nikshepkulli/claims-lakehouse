# Interface parity with the AWS module: project, environment, tags,
# bronze/silver/gold names, loader/transformer/analyst principals.

variable "project" {
  description = "Project short name, used in resource names and tags."
  type        = string
  default     = "claims-lakehouse"
}

variable "environment" {
  description = "Deployment environment (dev, staging, prod)."
  type        = string

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be one of: dev, staging, prod."
  }
}

variable "tags" {
  description = "Extra tags merged onto every resource."
  type        = map(string)
  default     = {}
}

variable "location" {
  description = "Azure region."
  type        = string
  default     = "centralindia"
}

variable "resource_group_name" {
  description = "Resource group to create. Defaults to rg-<project>-<environment>."
  type        = string
  default     = null
}

variable "storage_account_name" {
  description = "Globally unique storage account name (3-24 lowercase alphanumerics). Default: st<project><env> truncated to 17 chars + 6-char sha1 suffix of subscription ID + resource group name. Override to pin an exact name."
  type        = string
  default     = null

  validation {
    condition     = var.storage_account_name == null ? true : can(regex("^[a-z0-9]{3,24}$", var.storage_account_name))
    error_message = "storage_account_name must be 3-24 lowercase letters and digits."
  }
}

variable "bronze_name" {
  description = "Bronze (raw) filesystem name."
  type        = string
  default     = "bronze"
}

variable "silver_name" {
  description = "Silver (cleaned/conformed) filesystem name."
  type        = string
  default     = "silver"
}

variable "gold_name" {
  description = "Gold (marts/serving) filesystem name."
  type        = string
  default     = "gold"
}

# ---- Principals (extra, in addition to the managed identities this module creates) ----

variable "loader_principal_ids" {
  description = "Extra Entra object IDs granted the loader role (write bronze)."
  type        = list(string)
  default     = []
}

variable "transformer_principal_ids" {
  description = "Extra Entra object IDs granted the transformer role (read bronze, write silver/gold)."
  type        = list(string)
  default     = []
}

variable "analyst_principal_ids" {
  description = "Extra Entra object IDs granted the analyst role (read gold only)."
  type        = list(string)
  default     = []
}

# ---- Network ----

variable "public_network_access_enabled" {
  description = "Allow public network access to the account. Default false (deny); data-plane then requires a private endpoint."
  type        = bool
  default     = false
}

variable "allowed_ip_ranges" {
  description = "Public IPs/CIDRs allowed through the firewall (only effective when public_network_access_enabled = true). /31 and /32 ranges are not accepted by Azure; use single IPs."
  type        = list(string)
  default     = []
}

variable "enable_private_endpoint" {
  description = "Create blob + dfs private endpoints. Requires private_endpoint_subnet_id."
  type        = bool
  default     = false
}

variable "private_endpoint_subnet_id" {
  description = "Subnet ID for the private endpoints."
  type        = string
  default     = null
}

variable "private_dns_zone_ids" {
  description = "Optional map of subresource (blob, dfs) to private DNS zone ID for automatic A-record registration."
  type        = map(string)
  default     = {}
}

# ---- Data protection ----

variable "enable_versioning" {
  description = "Enable blob versioning and version-cleanup lifecycle rules. NOT supported on hierarchical-namespace (ADLS Gen2) accounts, which this module always creates; setting true fails a precondition. Kept for interface parity."
  type        = bool
  default     = false
}

variable "enable_snapshot_cleanup" {
  description = "Add snapshot-deletion lifecycle actions. Snapshots are preview-only on HNS accounts, so the action is never emitted for this module today."
  type        = bool
  default     = false
}

variable "soft_delete_retention_days" {
  description = "Soft-delete retention for blobs and containers (1-365)."
  type        = number
  default     = 14

  validation {
    condition     = var.soft_delete_retention_days >= 1 && var.soft_delete_retention_days <= 365
    error_message = "soft_delete_retention_days must be between 1 and 365."
  }
}

variable "shared_access_key_enabled" {
  description = "Allow account-key / SAS auth. Default false: Entra ID (RBAC) only."
  type        = bool
  default     = false
}

variable "enable_customer_managed_key" {
  description = "Encrypt with a customer-managed key from Key Vault instead of Microsoft-managed keys."
  type        = bool
  default     = false
}

variable "key_vault_id" {
  description = "Key Vault resource ID holding the CMK (needed when enable_customer_managed_key = true). Must have purge protection enabled."
  type        = string
  default     = null
}

variable "key_vault_key_id" {
  description = "Versionless Key Vault key ID (needed when enable_customer_managed_key = true). Versionless enables auto-rotation."
  type        = string
  default     = null
}

# ---- Lifecycle ----

variable "bronze_cool_after_days" {
  description = "Days since last modification before bronze blobs move to Cool."
  type        = number
  default     = 30
}

variable "bronze_archive_after_days" {
  description = "Days since last modification before bronze blobs move to Archive."
  type        = number
  default     = 180
}

variable "silver_cool_after_days" {
  description = "Days since last modification before silver blobs move to Cool."
  type        = number
  default     = 90
}

variable "noncurrent_version_delete_after_days" {
  description = "Days after which noncurrent versions/snapshots are deleted (only applied when versioning/snapshot cleanup is enabled and supported)."
  type        = number
  default     = 90
}

# ---- Observability ----

variable "log_analytics_workspace_id" {
  description = "Existing Log Analytics workspace ID for diagnostics. If null the module creates one."
  type        = string
  default     = null
}

variable "log_retention_days" {
  description = "Retention of the module-created Log Analytics workspace (30-730)."
  type        = number
  default     = 90
}
