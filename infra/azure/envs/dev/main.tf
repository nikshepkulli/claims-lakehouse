terraform {
  required_version = ">= 1.5.0"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.10"
    }
  }

  # Local state by default (portfolio / no credentials). For a real deployment
  # use a remote backend with its own locked-down storage account:
  #
  # backend "azurerm" {
  #   resource_group_name  = "rg-tfstate"
  #   storage_account_name = "sttfstateclaims"
  #   container_name       = "tfstate"
  #   key                  = "claims-lakehouse/dev.tfstate"
  #   use_azuread_auth     = true
  # }
}

provider "azurerm" {
  features {}

  # subscription_id comes from ARM_SUBSCRIPTION_ID / az login; never hardcode.
  storage_use_azuread = true
}

module "lakehouse_storage" {
  source = "../../modules/lakehouse_storage"

  project     = var.project
  environment = "dev"
  location    = var.location
  tags        = var.tags

  # Defaults: public access denied, Entra-only auth, MMK encryption.
  # enable_private_endpoint     = true
  # private_endpoint_subnet_id  = "<subnet id>"
  # enable_customer_managed_key = true
  # key_vault_id                = "<kv id>"
  # key_vault_key_id            = "<versionless key id>"
}

variable "project" {
  type    = string
  default = "claims-lakehouse"
}

variable "location" {
  type    = string
  default = "centralindia"
}

variable "tags" {
  type    = map(string)
  default = {}
}

output "abfss_uris" {
  value = module.lakehouse_storage.abfss_uris
}

output "principals" {
  value = module.lakehouse_storage.principals
}
