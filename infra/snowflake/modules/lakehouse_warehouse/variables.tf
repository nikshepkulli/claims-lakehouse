variable "name_prefix" {
  description = "Upper-case prefix for every Snowflake object name (database, warehouses, roles)."
  type        = string
  default     = "CLAIMS"

  validation {
    condition     = can(regex("^[A-Z][A-Z0-9_]{0,30}$", var.name_prefix))
    error_message = "name_prefix must be upper-case letters, digits and underscores, starting with a letter."
  }
}

variable "environment" {
  description = "Upper-case environment suffix, e.g. DEV. The database is <name_prefix>_<environment>."
  type        = string

  validation {
    condition     = can(regex("^[A-Z][A-Z0-9_]{0,15}$", var.environment))
    error_message = "environment must be upper-case letters, digits and underscores."
  }
}

variable "sysadmin_role_name" {
  description = "Role that owns data objects and inherits all functional roles."
  type        = string
  default     = "SYSADMIN"
}

variable "data_retention_days" {
  description = "Time Travel retention for the database. 1 is the Standard-edition maximum and the cheapest."
  type        = number
  default     = 1

  validation {
    condition     = var.data_retention_days >= 0 && var.data_retention_days <= 90
    error_message = "data_retention_days must be between 0 and 90."
  }
}

variable "warehouses" {
  description = <<-EOT
    Virtual warehouse sizing per workload. credit_quota is the MONTHLY credit
    budget of the warehouse's resource monitor (notify at 50/75/90 percent,
    suspend at 100, cancel running queries at 110). All values are
    hypotheses to be revised with measurements from ticket #16.
  EOT
  type = object({
    loader = object({
      size                      = string
      auto_suspend_seconds      = number
      statement_timeout_seconds = number
      credit_quota              = number
    })
    transformer = object({
      size                      = string
      auto_suspend_seconds      = number
      statement_timeout_seconds = number
      credit_quota              = number
    })
    analyst = object({
      size                      = string
      auto_suspend_seconds      = number
      statement_timeout_seconds = number
      credit_quota              = number
    })
  })
  default = {
    loader = {
      size                      = "XSMALL"
      auto_suspend_seconds      = 60
      statement_timeout_seconds = 3600
      credit_quota              = 5
    }
    transformer = {
      size                      = "XSMALL"
      auto_suspend_seconds      = 60
      statement_timeout_seconds = 3600
      credit_quota              = 10
    }
    analyst = {
      size                      = "XSMALL"
      auto_suspend_seconds      = 60
      statement_timeout_seconds = 300
      credit_quota              = 10
    }
  }
}

variable "notify_users" {
  description = "Snowflake user names notified by resource monitors (they must enable notifications in their profile)."
  type        = set(string)
  default     = []
}

variable "enable_external_stage" {
  description = <<-EOT
    Create the S3 storage integration, Parquet file format and external stage
    for the gold export. Off by default because it needs a real S3 URL and an
    AWS IAM role Snowflake can assume (see docs/snowflake.md).
  EOT
  type        = bool
  default     = false
}

variable "gold_s3_url" {
  description = "S3 URL of the gold Parquet export, e.g. s3://my-bucket/gold-export/ (trailing slash). Required when enable_external_stage is true."
  type        = string
  default     = ""

  validation {
    condition     = var.gold_s3_url == "" || can(regex("^s3://[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]/.*/$", var.gold_s3_url))
    error_message = "gold_s3_url must look like s3://bucket/prefix/ (with a prefix and trailing slash) or be empty."
  }
}

variable "storage_aws_role_arn" {
  description = "ARN of the AWS IAM role Snowflake assumes to read gold_s3_url (read-only). Required when enable_external_stage is true."
  type        = string
  default     = ""

  validation {
    condition     = var.storage_aws_role_arn == "" || can(regex("^arn:aws[a-z-]*:iam::[0-9]{12}:role/.+$", var.storage_aws_role_arn))
    error_message = "storage_aws_role_arn must be an IAM role ARN or empty."
  }
}

variable "manage_privacy_policies" {
  description = <<-EOT
    Let Terraform own the classification tags and tag-based masking policies (privacy.tf)
    instead of warehouse/snowflake/90_masking_policies.sql. Mutually exclusive with that
    script. Default false: the SQL script is the documented path. Never applied to a live
    account (ticket #25).
  EOT
  type        = bool
  default     = false
}
