terraform {
  required_version = ">= 1.5.0"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.10"
    }
    azuread = {
      source  = "hashicorp/azuread"
      version = "~> 3.0"
    }
  }

  # Bootstrap is applied ONCE by a human (Application Administrator + Owner /
  # User Access Administrator on the resource group). Local state on purpose.
  backend "local" {}
}
