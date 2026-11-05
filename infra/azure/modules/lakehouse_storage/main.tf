locals {
  base_name = "${var.project}-${var.environment}"

  resource_group_name = coalesce(var.resource_group_name, "rg-${local.base_name}")
  storage_account_name = coalesce(
    var.storage_account_name,
    substr("st${replace(lower(local.base_name), "-", "")}", 0, 24)
  )

  tags = merge(var.tags, {
    project     = var.project
    environment = var.environment
    managed_by  = "terraform"
    data_class  = "synthetic-phi-like"
  })

  filesystems = {
    bronze = var.bronze_name
    silver = var.silver_name
    gold   = var.gold_name
  }

  # Container-level ARM scope: RBAC at this scope limits access to one layer.
  fs_scope = {
    for k, v in local.filesystems :
    k => "${azurerm_storage_account.this.id}/blobServices/default/containers/${v}"
  }

  # Role grants. for_each keys must be known at plan time, so they are built from
  # static names/indexes, never from (apply-time) principal IDs.
  personas = {
    loader = {
      managed = azurerm_user_assigned_identity.persona["loader"].principal_id
      extra   = var.loader_principal_ids
      roles   = { bronze = "Storage Blob Data Contributor" }
    }
    transformer = {
      managed = azurerm_user_assigned_identity.persona["transformer"].principal_id
      extra   = var.transformer_principal_ids
      roles = {
        bronze = "Storage Blob Data Reader"
        silver = "Storage Blob Data Contributor"
        gold   = "Storage Blob Data Contributor"
      }
    }
    analyst = {
      managed = azurerm_user_assigned_identity.persona["analyst"].principal_id
      extra   = var.analyst_principal_ids
      roles   = { gold = "Storage Blob Data Reader" }
    }
  }

  grants = merge([
    for persona, cfg in local.personas : {
      for pair in setproduct(
        [for i in range(length(cfg.extra) + 1) : i],
        keys(cfg.roles)
        ) : "${persona}-${pair[1]}-${pair[0]}" => {
        principal = pair[0] == 0 ? cfg.managed : cfg.extra[pair[0] - 1]
        layer     = pair[1]
        role      = cfg.roles[pair[1]]
      }
    }
  ]...)

  private_endpoint_subresources = var.enable_private_endpoint ? toset(["blob", "dfs"]) : toset([])
}

resource "azurerm_resource_group" "this" {
  name     = local.resource_group_name
  location = var.location
  tags     = local.tags
}

# ---- Identities ----

resource "azurerm_user_assigned_identity" "persona" {
  for_each = toset(["loader", "transformer", "analyst"])

  name                = "id-${local.base_name}-${each.key}"
  resource_group_name = azurerm_resource_group.this.name
  location            = azurerm_resource_group.this.location
  tags                = local.tags
}

# Dedicated identity the storage account uses to reach the CMK in Key Vault.
resource "azurerm_user_assigned_identity" "cmk" {
  count = var.enable_customer_managed_key ? 1 : 0

  name                = "id-${local.base_name}-cmk"
  resource_group_name = azurerm_resource_group.this.name
  location            = azurerm_resource_group.this.location
  tags                = local.tags
}

resource "azurerm_role_assignment" "cmk" {
  count = var.enable_customer_managed_key ? 1 : 0

  scope                = var.key_vault_id
  role_definition_name = "Key Vault Crypto Service Encryption User"
  principal_id         = azurerm_user_assigned_identity.cmk[0].principal_id
}

# ---- Storage account (ADLS Gen2) ----

resource "azurerm_storage_account" "this" {
  name                = local.storage_account_name
  resource_group_name = azurerm_resource_group.this.name
  location            = azurerm_resource_group.this.location

  account_kind             = "StorageV2"
  account_tier             = "Standard"
  account_replication_type = var.environment == "prod" ? "GZRS" : "LRS"
  access_tier              = "Hot"

  is_hns_enabled                    = true
  min_tls_version                   = "TLS1_2"
  https_traffic_only_enabled        = true
  allow_nested_items_to_be_public   = false
  shared_access_key_enabled         = var.shared_access_key_enabled
  default_to_oauth_authentication   = true
  public_network_access_enabled     = var.public_network_access_enabled
  infrastructure_encryption_enabled = true

  network_rules {
    default_action = "Deny"
    bypass         = ["AzureServices", "Logging", "Metrics"]
    ip_rules       = var.public_network_access_enabled ? var.allowed_ip_ranges : []
  }

  blob_properties {
    versioning_enabled = var.enable_versioning

    delete_retention_policy {
      days = var.soft_delete_retention_days
    }
    container_delete_retention_policy {
      days = var.soft_delete_retention_days
    }
  }

  dynamic "identity" {
    for_each = var.enable_customer_managed_key ? [1] : []
    content {
      type         = "UserAssigned"
      identity_ids = [azurerm_user_assigned_identity.cmk[0].id]
    }
  }

  dynamic "customer_managed_key" {
    for_each = var.enable_customer_managed_key ? [1] : []
    content {
      key_vault_key_id          = var.key_vault_key_id
      user_assigned_identity_id = azurerm_user_assigned_identity.cmk[0].id
    }
  }

  tags = local.tags

  lifecycle {
    precondition {
      condition     = !var.enable_customer_managed_key || (var.key_vault_id != null && var.key_vault_key_id != null)
      error_message = "key_vault_id and key_vault_key_id are required when enable_customer_managed_key = true."
    }
    precondition {
      condition     = !var.enable_private_endpoint || var.private_endpoint_subnet_id != null
      error_message = "private_endpoint_subnet_id is required when enable_private_endpoint = true."
    }
  }

  depends_on = [azurerm_role_assignment.cmk]
}

# On an HNS account a blob container is the ADLS Gen2 filesystem. Created via the
# management plane (storage_account_id) so no data-plane/firewall access is needed.
resource "azurerm_storage_container" "layer" {
  for_each = local.filesystems

  name                  = each.value
  storage_account_id    = azurerm_storage_account.this.id
  container_access_type = "private"
}

# ---- RBAC (least privilege, scoped per filesystem) ----

resource "azurerm_role_assignment" "grant" {
  for_each = local.grants

  scope                = local.fs_scope[each.value.layer]
  role_definition_name = each.value.role
  principal_id         = each.value.principal

  depends_on = [azurerm_storage_container.layer]
}

# ---- Lifecycle management ----

resource "azurerm_storage_management_policy" "this" {
  storage_account_id = azurerm_storage_account.this.id

  rule {
    name    = "bronze-tiering"
    enabled = true
    filters {
      blob_types   = ["blockBlob"]
      prefix_match = ["${var.bronze_name}/"]
    }
    actions {
      base_blob {
        tier_to_cool_after_days_since_modification_greater_than    = var.bronze_cool_after_days
        tier_to_archive_after_days_since_modification_greater_than = var.bronze_archive_after_days
      }
      version {
        delete_after_days_since_creation = var.noncurrent_version_delete_after_days
      }
      snapshot {
        delete_after_days_since_creation_greater_than = var.noncurrent_version_delete_after_days
      }
    }
  }

  rule {
    name    = "silver-tiering"
    enabled = true
    filters {
      blob_types   = ["blockBlob"]
      prefix_match = ["${var.silver_name}/"]
    }
    actions {
      base_blob {
        tier_to_cool_after_days_since_modification_greater_than = var.silver_cool_after_days
      }
      version {
        delete_after_days_since_creation = var.noncurrent_version_delete_after_days
      }
    }
  }

  # Gold stays Hot (serving layer); only prune old versions.
  rule {
    name    = "gold-version-cleanup"
    enabled = true
    filters {
      blob_types   = ["blockBlob"]
      prefix_match = ["${var.gold_name}/"]
    }
    actions {
      version {
        delete_after_days_since_creation = var.noncurrent_version_delete_after_days
      }
    }
  }
}

# ---- Private endpoints (optional) ----

resource "azurerm_private_endpoint" "this" {
  for_each = local.private_endpoint_subresources

  name                = "pe-${local.base_name}-${each.key}"
  resource_group_name = azurerm_resource_group.this.name
  location            = azurerm_resource_group.this.location
  subnet_id           = var.private_endpoint_subnet_id
  tags                = local.tags

  private_service_connection {
    name                           = "psc-${local.base_name}-${each.key}"
    private_connection_resource_id = azurerm_storage_account.this.id
    subresource_names              = [each.key]
    is_manual_connection           = false
  }

  dynamic "private_dns_zone_group" {
    for_each = contains(keys(var.private_dns_zone_ids), each.key) ? [1] : []
    content {
      name                 = "default"
      private_dns_zone_ids = [var.private_dns_zone_ids[each.key]]
    }
  }
}

# ---- Diagnostics ----

resource "azurerm_log_analytics_workspace" "this" {
  count = var.log_analytics_workspace_id == null ? 1 : 0

  name                = "log-${local.base_name}"
  resource_group_name = azurerm_resource_group.this.name
  location            = azurerm_resource_group.this.location
  sku                 = "PerGB2018"
  retention_in_days   = var.log_retention_days
  tags                = local.tags
}

locals {
  log_analytics_workspace_id = coalesce(
    var.log_analytics_workspace_id,
    try(azurerm_log_analytics_workspace.this[0].id, null)
  )
}

# Data-plane read/write/delete audit logs live on the blob service sub-resource.
resource "azurerm_monitor_diagnostic_setting" "blob" {
  name                       = "diag-${local.base_name}-blob"
  target_resource_id         = "${azurerm_storage_account.this.id}/blobServices/default"
  log_analytics_workspace_id = local.log_analytics_workspace_id

  enabled_log {
    category = "StorageRead"
  }
  enabled_log {
    category = "StorageWrite"
  }
  enabled_log {
    category = "StorageDelete"
  }
  enabled_metric {
    category = "Transaction"
  }
}

resource "azurerm_monitor_diagnostic_setting" "account" {
  name                       = "diag-${local.base_name}-account"
  target_resource_id         = azurerm_storage_account.this.id
  log_analytics_workspace_id = local.log_analytics_workspace_id

  enabled_metric {
    category = "Transaction"
  }
  enabled_metric {
    category = "Capacity"
  }
}
