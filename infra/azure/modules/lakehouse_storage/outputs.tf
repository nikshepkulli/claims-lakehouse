output "resource_group_name" {
  description = "Resource group holding the lakehouse."
  value       = azurerm_resource_group.this.name
}

output "storage_account_name" {
  description = "ADLS Gen2 storage account name."
  value       = azurerm_storage_account.this.name
}

output "storage_account_id" {
  description = "ADLS Gen2 storage account resource ID."
  value       = azurerm_storage_account.this.id
}

output "dfs_endpoint" {
  description = "ADLS Gen2 (dfs) primary endpoint."
  value       = azurerm_storage_account.this.primary_dfs_endpoint
}

output "filesystem_names" {
  description = "Map of layer to filesystem name."
  value       = { for k, c in azurerm_storage_container.layer : k => c.name }
}

output "abfss_uris" {
  description = "Map of layer to abfss:// URI (parity with AWS s3:// outputs)."
  value = {
    for k, c in azurerm_storage_container.layer :
    k => "abfss://${c.name}@${azurerm_storage_account.this.name}.dfs.core.windows.net/"
  }
}

output "layer_uris" {
  description = "Map of layer (bronze/silver/gold) to abfss:// root URI. Same shape as the AWS module; alias of abfss_uris."
  value = {
    for k, c in azurerm_storage_container.layer :
    k => "abfss://${c.name}@${azurerm_storage_account.this.name}.dfs.core.windows.net/"
  }
}

output "principals" {
  description = "Managed identities per persona (loader, transformer, analyst)."
  value = {
    for k, i in azurerm_user_assigned_identity.persona : k => {
      id           = i.id
      principal_id = i.principal_id
      client_id    = i.client_id
    }
  }
}

output "log_analytics_workspace_id" {
  description = "Workspace receiving diagnostics."
  value       = local.log_analytics_workspace_id
}

output "private_endpoint_ids" {
  description = "Private endpoint IDs by subresource (empty when disabled)."
  value       = { for k, pe in azurerm_private_endpoint.this : k => pe.id }
}
