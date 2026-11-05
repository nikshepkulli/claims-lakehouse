# Role model (see docs/snowflake.md for the diagram):
#
#   access roles (<db>_<SCHEMA>_RO / _RW): hold privileges on exactly one schema
#   functional roles (LOADER, TRANSFORMER, ANALYST): what people / services use,
#       composed from access roles + warehouse usage
#   SYSADMIN inherits every functional role, so object ownership stays in the
#       system-role tree and nothing is owned by an unmanaged role.
#
# Nothing is granted to PUBLIC, and no privilege is granted WITH GRANT OPTION.

locals {
  role_prefix = "${var.name_prefix}_${var.environment}"

  # Access roles: schema + level.
  access_roles = {
    RAW_RO     = { schema = "RAW", level = "ro" }
    RAW_RW     = { schema = "RAW", level = "rw" }
    STAGING_RW = { schema = "STAGING", level = "rw" }
    MARTS_RO   = { schema = "MARTS", level = "ro" }
    MARTS_RW   = { schema = "MARTS", level = "rw" }
    SECURE_RO  = { schema = "SECURE", level = "ro" }
    SECURE_RW  = { schema = "SECURE", level = "rw" }
  }

  # Functional roles -> access roles they inherit.
  functional_roles = {
    LOADER      = ["RAW_RW"]
    TRANSFORMER = ["RAW_RO", "STAGING_RW", "MARTS_RW", "SECURE_RW"]
    ANALYST     = ["MARTS_RO", "SECURE_RO"]
  }

  functional_descriptions = {
    LOADER      = "Loads gold exports from the external stage into RAW. Write access to RAW only."
    TRANSFORMER = "Builds STAGING, MARTS and SECURE from RAW (MERGE/dbt). Read-only on RAW."
    ANALYST     = "Reads MARTS and SECURE (masked). No write access anywhere."
  }

  # Flattened (functional role, access role) edges.
  functional_to_access = merge([
    for fr, ars in local.functional_roles : {
      for ar in ars : "${fr}/${ar}" => { functional = fr, access = ar }
    }
  ]...)
}

resource "snowflake_account_role" "access" {
  provider = snowflake.securityadmin
  for_each = local.access_roles

  name    = "${local.role_prefix}_${each.key}"
  comment = "Access role: ${each.value.level == "rw" ? "read/write" : "read-only"} on schema ${each.value.schema}. Grant to functional roles only, never to users."
}

resource "snowflake_account_role" "functional" {
  provider = snowflake.securityadmin
  for_each = local.functional_roles

  name    = "${local.role_prefix}_${each.key}"
  comment = local.functional_descriptions[each.key]
}

resource "snowflake_grant_account_role" "access_to_functional" {
  provider = snowflake.securityadmin
  for_each = local.functional_to_access

  role_name        = snowflake_account_role.access[each.value.access].name
  parent_role_name = snowflake_account_role.functional[each.value.functional].name
}

resource "snowflake_grant_account_role" "functional_to_sysadmin" {
  provider = snowflake.securityadmin
  for_each = local.functional_roles

  role_name        = snowflake_account_role.functional[each.key].name
  parent_role_name = var.sysadmin_role_name
}
