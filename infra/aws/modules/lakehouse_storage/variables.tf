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
  description = "REQUIRED. IAM user or role ARNs allowed to assume the loader role (ingestion job). No default and no account-root fallback: an empty list is rejected. Do not pass assumed-role session ARNs (arn:aws:sts::...); use the role ARN."
  type        = list(string)

  validation {
    condition     = length(var.loader_trusted_principal_arns) > 0 && alltrue([for a in var.loader_trusted_principal_arns : can(regex("^arn:aws[a-z-]*:iam::[0-9]{12}:(user|role)/.+$", a))])
    error_message = "loader_trusted_principal_arns must be non-empty and every item an IAM user/role ARN (arn:aws:iam::<12-digit-account>:user|role/...). Account root and sts assumed-role ARNs are rejected."
  }
}

variable "transformer_trusted_principal_arns" {
  description = "REQUIRED. IAM user or role ARNs allowed to assume the transformer role (dbt/Spark job). No default and no account-root fallback: an empty list is rejected. Do not pass assumed-role session ARNs (arn:aws:sts::...); use the role ARN."
  type        = list(string)

  validation {
    condition     = length(var.transformer_trusted_principal_arns) > 0 && alltrue([for a in var.transformer_trusted_principal_arns : can(regex("^arn:aws[a-z-]*:iam::[0-9]{12}:(user|role)/.+$", a))])
    error_message = "transformer_trusted_principal_arns must be non-empty and every item an IAM user/role ARN (arn:aws:iam::<12-digit-account>:user|role/...). Account root and sts assumed-role ARNs are rejected."
  }
}

variable "analyst_trusted_principal_arns" {
  description = "REQUIRED. IAM user or role ARNs allowed to assume the analyst role (read-only gold). No default and no account-root fallback: an empty list is rejected. Do not pass assumed-role session ARNs (arn:aws:sts::...); use the role ARN."
  type        = list(string)

  validation {
    condition     = length(var.analyst_trusted_principal_arns) > 0 && alltrue([for a in var.analyst_trusted_principal_arns : can(regex("^arn:aws[a-z-]*:iam::[0-9]{12}:(user|role)/.+$", a))])
    error_message = "analyst_trusted_principal_arns must be non-empty and every item an IAM user/role ARN (arn:aws:iam::<12-digit-account>:user|role/...). Account root and sts assumed-role ARNs are rejected."
  }
}

variable "key_admin_arns" {
  description = "Optional IAM user/role ARNs granted KMS key ADMINISTRATION (create grants, policy, rotation, schedule deletion) but not key USE (no Encrypt/Decrypt/GenerateDataKey). Key use is granted only by the role policies. The key policy still delegates to IAM via the account root (required by KMS to avoid lockout), so this separates duties by explicit statement rather than by removing delegation."
  type        = list(string)
  default     = []

  validation {
    condition     = alltrue([for a in var.key_admin_arns : can(regex("^arn:aws[a-z-]*:iam::[0-9]{12}:(user|role)/.+$", a))])
    error_message = "key_admin_arns items must be IAM user/role ARNs."
  }
}

variable "gold_object_lock_enabled" {
  description = "OPTIONAL hardening for regulated data: create the gold bucket with S3 Object Lock (WORM). Can only be set when the bucket is created; changing it on an existing bucket forces replacement. Default off."
  type        = bool
  default     = false
}

variable "gold_object_lock_mode" {
  description = "Default Object Lock retention mode for gold when enabled: GOVERNANCE (privileged bypass) or COMPLIANCE (no one can shorten or delete)."
  type        = string
  default     = "GOVERNANCE"

  validation {
    condition     = contains(["GOVERNANCE", "COMPLIANCE"], var.gold_object_lock_mode)
    error_message = "gold_object_lock_mode must be GOVERNANCE or COMPLIANCE."
  }
}

variable "gold_object_lock_retention_days" {
  description = "Default Object Lock retention in days for gold when enabled."
  type        = number
  default     = 30

  validation {
    condition     = var.gold_object_lock_retention_days >= 1
    error_message = "gold_object_lock_retention_days must be >= 1."
  }
}

variable "max_session_duration" {
  description = "Max STS session duration in seconds for the data roles."
  type        = number
  default     = 3600
}
