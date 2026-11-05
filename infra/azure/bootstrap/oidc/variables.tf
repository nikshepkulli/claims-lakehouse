variable "github_repository" {
  description = "GitHub repository allowed to authenticate, as owner/name."
  type        = string
  default     = "nikshepkulli/claims-lakehouse"

  validation {
    condition     = can(regex("^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$", var.github_repository))
    error_message = "Use owner/name, no wildcards."
  }
}

variable "environment" {
  description = "Deployment environment. Must match the GitHub Environment suffix: azure-<environment>."
  type        = string
  default     = "dev"
}

variable "resource_group_name" {
  description = "EXISTING resource group the pipeline may manage (role assignments are scoped here, never to the subscription). Must equal the group envs/<env> deploys into (default rg-claims-lakehouse-<env>). See docs/cd.md for the one-time create + import."
  type        = string
  default     = "rg-claims-lakehouse-dev"
}

variable "app_name_prefix" {
  description = "Display-name prefix for the Entra application."
  type        = string
  default     = "claims-lakehouse"
}

variable "state_storage_account_id" {
  description = "Optional resource ID of the storage account holding Terraform state. Empty = no state access granted."
  type        = string
  default     = ""
}
