variable "region" {
  description = "AWS region for the provider (IAM is global; this only picks the API endpoint)."
  type        = string
  default     = "us-east-1"
}

variable "github_repository" {
  description = "GitHub repository allowed to assume the roles, as owner/name."
  type        = string
  default     = "nikshepkulli/claims-lakehouse"

  validation {
    condition     = can(regex("^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$", var.github_repository))
    error_message = "Use owner/name, no wildcards."
  }
}

variable "environment" {
  description = "Deployment environment. Must match the GitHub Environment suffix: aws-<environment>."
  type        = string
  default     = "dev"
}

variable "name_prefix" {
  description = "Resource name prefix of the lakehouse stack this role may manage. Must equal name_prefix in infra/aws/envs/<env>."
  type        = string
  default     = "claims-lakehouse-dev"
}

variable "create_oidc_provider" {
  description = "Create the GitHub OIDC provider. Set false if the account already has one for token.actions.githubusercontent.com (only one per URL is allowed)."
  type        = bool
  default     = true
}

variable "state_bucket_name" {
  description = "Existing S3 bucket that holds Terraform state (remote backend). Empty = no state access granted."
  type        = string
  default     = ""
}

variable "state_lock_table_name" {
  description = "Existing DynamoDB lock table for the S3 backend. Empty = no lock-table access granted."
  type        = string
  default     = ""
}

variable "state_key_prefix" {
  description = "Key prefix inside the state bucket the roles may touch."
  type        = string
  default     = "claims-lakehouse/"
}

variable "pin_workflow_ref" {
  description = "Also require the token's job_workflow_ref to be terraform-apply.yml on refs/heads/main."
  type        = bool
  default     = true
}
