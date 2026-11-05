variable "enable_external_stage" {
  description = "Create the S3 storage integration + external stage for the gold Parquet export."
  type        = bool
  default     = false
}

variable "gold_s3_url" {
  description = "s3://bucket/prefix/ of the gold Parquet export. Required when enable_external_stage is true."
  type        = string
  default     = ""
}

variable "storage_aws_role_arn" {
  description = "IAM role ARN Snowflake assumes to read gold_s3_url. Required when enable_external_stage is true."
  type        = string
  default     = ""
}

variable "notify_users" {
  description = "Snowflake users notified by resource monitors."
  type        = set(string)
  default     = []
}

variable "manage_privacy_policies" {
  description = "Let Terraform own the classification tags and masking policies instead of warehouse/snowflake/90_masking_policies.sql (mutually exclusive)."
  type        = bool
  default     = false
}
