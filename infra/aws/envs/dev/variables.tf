variable "region" {
  description = "AWS region."
  type        = string
  default     = "us-east-1"
}

variable "name_prefix" {
  description = "Resource name prefix."
  type        = string
  default     = "claims-lakehouse-dev"
}

variable "trusted_principal_arns" {
  description = "REQUIRED. IAM user or ROLE ARNs allowed to assume the loader/transformer/analyst roles in dev. No default (no account-root fallback). Use your IAM user ARN (arn:aws:iam::<acct>:user/<name>) or the ROLE ARN behind your session. An assumed-role session ARN (arn:aws:sts::<acct>:assumed-role/<role>/<session>) is NOT a valid trust principal; convert it to arn:aws:iam::<acct>:role/<role> (for IAM Identity Center roles include the path: role/aws-reserved/sso.amazonaws.com/<region>/AWSReservedSSO_<name>_<hash>). Find yours with `aws sts get-caller-identity` and `aws iam get-role --role-name <role> --query Role.Arn`."
  type        = list(string)
}

variable "key_admin_arns" {
  description = "Optional IAM user/role ARNs for KMS key administration (separate from key use)."
  type        = list(string)
  default     = []
}

variable "gold_object_lock_enabled" {
  description = "Optional: create the gold bucket with S3 Object Lock (creation-time only). Default off."
  type        = bool
  default     = false
}
