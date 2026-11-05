output "tenant_id" {
  description = "Set as repo variable AZURE_TENANT_ID."
  value       = data.azurerm_client_config.current.tenant_id
}

output "subscription_id" {
  description = "Set as repo variable AZURE_SUBSCRIPTION_ID."
  value       = data.azurerm_client_config.current.subscription_id
}

output "plan_client_id" {
  description = "Set as repo variable AZURE_PLAN_CLIENT_ID."
  value       = azuread_application.plan.client_id
}

output "apply_client_id" {
  description = "Set as variable AZURE_CLIENT_ID on the GitHub Environment azure-<env>."
  value       = azuread_application.apply.client_id
}

output "github_environment" {
  description = "Name of the GitHub Environment the apply identity trusts."
  value       = local.gh_env
}
