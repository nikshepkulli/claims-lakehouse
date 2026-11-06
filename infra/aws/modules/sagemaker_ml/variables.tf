variable "name_prefix" {
  description = "Prefix for all resource names (lowercase letters, digits, hyphens). The ML bucket becomes <prefix>-ml-<account_id>."
  type        = string

  validation {
    condition     = can(regex("^[a-z0-9][a-z0-9-]{1,30}[a-z0-9]$", var.name_prefix))
    error_message = "name_prefix must be 3-32 chars: lowercase letters, digits and hyphens, not starting/ending with a hyphen."
  }
}

variable "kms_key_arn" {
  description = "REQUIRED. ARN of the customer-managed KMS key that encrypts the data (the lakehouse_storage CMK). The execution role gets Decrypt/GenerateDataKey on this key only; the key policy must still allow the role (it delegates to IAM in lakehouse_storage)."
  type        = string

  validation {
    condition     = can(regex("^arn:aws[a-z-]*:kms:[a-z0-9-]+:[0-9]{12}:key/[0-9a-f-]+$", var.kms_key_arn))
    error_message = "kms_key_arn must be a KMS key ARN (arn:aws:kms:<region>:<account>:key/<id>)."
  }
}

variable "gold_bucket_name" {
  description = "REQUIRED. Name of the gold bucket (lakehouse_storage output). The role reads <gold>/<features_prefix> and writes only <gold>/<scores_prefix>."
  type        = string
}

variable "features_prefix" {
  description = "Key prefix of the point-in-time feature store inside the gold bucket (training input and scoring input). Read-only."
  type        = string
  default     = "features/"

  validation {
    condition     = can(regex("^[A-Za-z0-9_./-]+/$", var.features_prefix))
    error_message = "features_prefix must end with a slash and contain no wildcards."
  }
}

variable "scores_prefix" {
  description = "Key prefix inside the gold bucket where batch scores are written back. The only gold location the role may write."
  type        = string
  default     = "scores/claim_denial_risk/"

  validation {
    condition     = can(regex("^[A-Za-z0-9_./-]+/$", var.scores_prefix))
    error_message = "scores_prefix must end with a slash and contain no wildcards."
  }
}

variable "permissions_boundary_arn" {
  description = "Optional IAM permissions boundary ARN attached to the execution role (set it wherever the account requires one). The role's own policy is least-privilege regardless; the boundary is an additional ceiling."
  type        = string
  default     = null

  validation {
    condition     = var.permissions_boundary_arn == null || can(regex("^arn:aws[a-z-]*:iam::[0-9]{12}:policy/.+$", coalesce(var.permissions_boundary_arn, "x")))
    error_message = "permissions_boundary_arn must be an IAM policy ARN or null."
  }
}

variable "create_model_package_group" {
  description = "Create a SageMaker Model Package Group (the model registry container; free, holds versions and approval status). No model is registered by Terraform."
  type        = bool
  default     = false
}

variable "model_package_group_name" {
  description = "Name of the Model Package Group when created."
  type        = string
  default     = "claim-denial-risk"
}

variable "create_ecr_repository" {
  description = "Create a private ECR repository (immutable tags, scan on push, KMS) for the training/scoring image built from src/lakehouse/ml/Dockerfile."
  type        = bool
  default     = true
}

variable "environment" {
  description = "Environment label used in the Environment tag (e.g. dev, prod)."
  type        = string
  default     = "dev"
}

variable "tags" {
  description = "Extra tags merged onto every resource."
  type        = map(string)
  default     = {}
}

variable "force_destroy" {
  description = "Allow terraform destroy to delete a non-empty ML bucket. Keep false outside throwaway environments."
  type        = bool
  default     = false
}

variable "noncurrent_version_expiration_days" {
  description = "Days after which noncurrent object versions in the ML bucket are permanently deleted."
  type        = number
  default     = 90
}

variable "training_output_expiration_days" {
  description = "Days after which job logs/outputs under training-output/ expire (models under models/ are kept)."
  type        = number
  default     = 90
}
