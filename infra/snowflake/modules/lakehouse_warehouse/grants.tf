# Privileges. With the provider's strict privilege management each
# (role, target) pair is managed by exactly one resource below, so resources
# never fight over the same grants.
#
# Terraform runs before the DDL in warehouse/snowflake/, so table and view
# privileges are FUTURE grants: every table SYSADMIN creates in a schema is
# covered the moment it exists.

locals {
  # Schema-level privileges by access level. Only STAGING/MARTS/SECURE are
  # built by TRANSFORMER, so RAW never gets CREATE privileges for non-owners.
  schema_privileges = {
    ro = ["USAGE"]
    rw = ["USAGE", "CREATE TABLE", "CREATE VIEW"]
  }
  schema_privileges_raw_rw = ["USAGE"]

  table_privileges = {
    ro = ["SELECT"]
    rw = ["SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE"]
  }

  # Only the RW roles on RAW use stages and file formats (COPY INTO).
  raw_rw_roles = { for k, v in local.access_roles : k => v if v.schema == "RAW" && v.level == "rw" }
}

resource "snowflake_grant_privileges_to_account_role" "database_usage" {
  provider = snowflake.securityadmin
  for_each = local.access_roles

  account_role_name = snowflake_account_role.access[each.key].name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.this.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "schema" {
  provider = snowflake.securityadmin
  for_each = local.access_roles

  account_role_name = snowflake_account_role.access[each.key].name
  privileges        = each.value.schema == "RAW" && each.value.level == "rw" ? local.schema_privileges_raw_rw : local.schema_privileges[each.value.level]
  on_schema {
    schema_name = snowflake_schema.this[each.value.schema].fully_qualified_name
  }
}

resource "snowflake_grant_privileges_to_account_role" "future_tables" {
  provider = snowflake.securityadmin
  for_each = local.access_roles

  account_role_name = snowflake_account_role.access[each.key].name
  privileges        = local.table_privileges[each.value.level]
  on_schema_object {
    future {
      object_type_plural = "TABLES"
      in_schema          = snowflake_schema.this[each.value.schema].fully_qualified_name
    }
  }
}

resource "snowflake_grant_privileges_to_account_role" "future_views" {
  provider = snowflake.securityadmin
  for_each = local.access_roles

  account_role_name = snowflake_account_role.access[each.key].name
  privileges        = ["SELECT"]
  on_schema_object {
    future {
      object_type_plural = "VIEWS"
      in_schema          = snowflake_schema.this[each.value.schema].fully_qualified_name
    }
  }
}

resource "snowflake_grant_privileges_to_account_role" "future_stages" {
  provider = snowflake.securityadmin
  for_each = local.raw_rw_roles

  account_role_name = snowflake_account_role.access[each.key].name
  privileges        = ["USAGE"]
  on_schema_object {
    future {
      object_type_plural = "STAGES"
      in_schema          = snowflake_schema.this[each.value.schema].fully_qualified_name
    }
  }
}

resource "snowflake_grant_privileges_to_account_role" "future_file_formats" {
  provider = snowflake.securityadmin
  for_each = local.raw_rw_roles

  account_role_name = snowflake_account_role.access[each.key].name
  privileges        = ["USAGE"]
  on_schema_object {
    future {
      object_type_plural = "FILE FORMATS"
      in_schema          = snowflake_schema.this[each.value.schema].fully_qualified_name
    }
  }
}

# Warehouse usage: each functional role may only use its own warehouse.
# No OPERATE/MODIFY/MONITOR: warehouses auto-resume and auto-suspend.
resource "snowflake_grant_privileges_to_account_role" "warehouse_usage" {
  provider = snowflake.securityadmin
  for_each = local.functional_roles

  account_role_name = snowflake_account_role.functional[each.key].name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.this[each.key].name
  }
}
