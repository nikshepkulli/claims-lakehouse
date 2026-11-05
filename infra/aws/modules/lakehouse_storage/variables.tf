variable "name_prefix" {
  description = "Prefix for all resource names (lowercase letters, digits, hyphens). Bucket names become <prefix>-<layer>-<account_id>."
  type        = string

  validation {
    condition     = can(regex("^[a-z0-9][a-z0-9-]{1,30}[a-z0-9]$", var.name_prefix))
    error_message = "name_prefix must be 3-32 chars: lowercase letters, digits and hyphens, not starting/ending with a hyphen."
  }
}

variable "tags" {
  description = "Extra tags merged onto every resource (on top of module defaults)."
  type        = map(string)
  default     = {}
}

variable "environment" {
  description = "Environment label used in the Environment tag (e.g. dev, prod)."
  type        = string
  default     = "dev"
}

variable "force_destroy" {
  description = "Allow terraform destroy to delete non-empty buckets. Keep false outside throwaway environments."
  type        = bool
  default     = false
}

variable "kms_deletion_window_days" {
  description = "Waiting period before the CMK is deleted after destroy (7-30)."
  type        = number
  default     = 30

  validation {
    condition     = var.kms_deletion_window_days >= 7 && var.kms_deletion_window_days <= 30
    error_message = "kms_deletion_window_days must be between 7 and 30."
  }
}

variable "noncurrent_version_expiration_days" {
  description = "Days after which noncurrent object versions are permanently deleted."
  type        = number
  default     = 90
}

variable "bronze_intelligent_tiering_days" {
  description = "Days after creation before bronze objects transition to INTELLIGENT_TIERING."
  type        = number
  default     = 0
}

variable "access_log_retention_days" {
  description = "Days to retain S3 server access logs."
  type        = number
  default     = 365
}

variable "loader_trusted_principal_arns" {
  description = "IAM principal ARNs allowed to assume the loader role (ingestion job). Empty = account root only."
  type        = list(string)
  default     = []
}

variable "transformer_trusted_principal_arns" {
  description = "IAM principal ARNs allowed to assume the transformer role (dbt/Spark job). Empty = account root only."
  type        = list(string)
  default     = []
}

variable "analyst_trusted_principal_arns" {
  description = "IAM principal ARNs allowed to assume the analyst role (read-only gold). Empty = account root only."
  type        = list(string)
  default     = []
}

variable "max_session_duration" {
  description = "Max STS session duration in seconds for the data roles."
  type        = number
  default     = 3600
}
