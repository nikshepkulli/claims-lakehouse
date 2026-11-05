provider "azurerm" {
  features {}
  # subscription_id comes from ARM_SUBSCRIPTION_ID / az login; never hardcode.
}

provider "azuread" {}

data "azurerm_resource_group" "target" {
  name = var.resource_group_name
}

locals {
  gh_env = "azure-${var.environment}"

  issuer   = "https://token.actions.githubusercontent.com"
  audience = ["api://AzureADTokenExchange"]

  sub_environment = "repo:${var.github_repository}:environment:${local.gh_env}"
  sub_pr          = "repo:${var.github_repository}:pull_request"
  sub_main        = "repo:${var.github_repository}:ref:refs/heads/main"
}

# Two identities, so the read-only one can be used by untrusted PR code paths.
resource "azuread_application" "plan" {
  display_name = "${var.app_name_prefix}-gha-plan-${var.environment}"
  notes        = "GitHub Actions terraform plan (read-only) for ${var.github_repository}. Federated credentials only; no client secret."
}

resource "azuread_service_principal" "plan" {
  client_id = azuread_application.plan.client_id
}

resource "azuread_application" "apply" {
  display_name = "${var.app_name_prefix}-gha-apply-${var.environment}"
  notes        = "GitHub Actions terraform apply for ${var.github_repository} (${local.gh_env}). Federated credentials only; no client secret."
}

resource "azuread_service_principal" "apply" {
  client_id = azuread_application.apply.client_id
}

# --- Federated credentials: subject match is exact, no wildcards ------------
resource "azuread_application_federated_identity_credential" "plan_pr" {
  application_id = azuread_application.plan.id
  display_name   = "github-pull-request"
  description    = "terraform plan on same-repo pull requests"
  issuer         = local.issuer
  audiences      = local.audience
  subject        = local.sub_pr
}

resource "azuread_application_federated_identity_credential" "plan_main" {
  application_id = azuread_application.plan.id
  display_name   = "github-main"
  description    = "plan job inside the manual apply workflow, run on main"
  issuer         = local.issuer
  audiences      = local.audience
  subject        = local.sub_main
}

# Apply: only tokens minted for the GitHub Environment (approval already given;
# the Environment's deployment-branch policy restricts it to main).
resource "azuread_application_federated_identity_credential" "apply_env" {
  application_id = azuread_application.apply.id
  display_name   = "github-env-${local.gh_env}"
  description    = "terraform apply via GitHub Environment ${local.gh_env}"
  issuer         = local.issuer
  audiences      = local.audience
  subject        = local.sub_environment
}

# --- RBAC: scoped to the one resource group ---------------------------------
resource "azurerm_role_assignment" "plan_reader" {
  scope                = data.azurerm_resource_group.target.id
  role_definition_name = "Reader"
  principal_id         = azuread_service_principal.plan.object_id
}

resource "azurerm_role_assignment" "apply_contributor" {
  scope                = data.azurerm_resource_group.target.id
  role_definition_name = "Contributor"
  principal_id         = azuread_service_principal.apply.object_id
}

# The lakehouse module creates role assignments; Contributor cannot. This role
# can write role assignments but is constrained below so it cannot hand out
# Owner / User Access Administrator or itself escalate.
resource "azurerm_role_assignment" "apply_rbac_admin" {
  scope                = data.azurerm_resource_group.target.id
  role_definition_name = "Role Based Access Control Administrator"
  principal_id         = azuread_service_principal.apply.object_id
  # Allow only the data-plane roles the module assigns.
  condition_version = "2.0"
  condition         = <<-EOT
    (
     (!(ActionMatches{'Microsoft.Authorization/roleAssignments/write'}))
     OR
     (@Request[Microsoft.Authorization/roleAssignments:RoleDefinitionId] ForAnyOfAnyValues:GuidEquals {ba92f5b4-2d11-453d-a403-e96b0029c9fe, 2a2b9908-6ea1-4ae2-8e65-a410df84e7d1})
    )
    AND
    (
     (!(ActionMatches{'Microsoft.Authorization/roleAssignments/delete'}))
     OR
     (@Resource[Microsoft.Authorization/roleAssignments:RoleDefinitionId] ForAnyOfAnyValues:GuidEquals {ba92f5b4-2d11-453d-a403-e96b0029c9fe, 2a2b9908-6ea1-4ae2-8e65-a410df84e7d1})
    )
  EOT
}

# Optional: remote state in a storage account (Entra auth, no keys).
resource "azurerm_role_assignment" "plan_state" {
  count                = var.state_storage_account_id == "" ? 0 : 1
  scope                = var.state_storage_account_id
  role_definition_name = "Storage Blob Data Reader"
  principal_id         = azuread_service_principal.plan.object_id
}

resource "azurerm_role_assignment" "apply_state" {
  count                = var.state_storage_account_id == "" ? 0 : 1
  scope                = var.state_storage_account_id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azuread_service_principal.apply.object_id
}

data "azurerm_client_config" "current" {}
