locals {
  db_name = "${var.name_prefix}_${var.environment}"

  schemas = {
    RAW     = "Landing zone: gold Parquet exports copied as-is, plus the load audit table. Written only by LOADER."
    STAGING = "Intermediate transformation models (dbt staging). Written only by TRANSFORMER."
    MARTS   = "Gold star schema: facts and dimensions served to analysts."
    SECURE  = "Secure views over MARTS with masking and row policies applied (ticket #25)."
  }

  # Warehouse -> sizing. Keys double as the workload name used in object names.
  warehouses = {
    LOADER      = var.warehouses.loader
    TRANSFORMER = var.warehouses.transformer
    ANALYST     = var.warehouses.analyst
  }
}

resource "snowflake_database" "this" {
  provider = snowflake.sysadmin

  name                           = local.db_name
  comment                        = "Synthetic healthcare claims lakehouse (${var.environment}). Contains only synthetic data."
  data_retention_time_in_days    = var.data_retention_days
  drop_public_schema_on_creation = true
}

resource "snowflake_schema" "this" {
  provider = snowflake.sysadmin
  for_each = local.schemas

  database                    = snowflake_database.this.name
  name                        = each.key
  comment                     = each.value
  data_retention_time_in_days = var.data_retention_days
  # Managed access: only the schema owner / MANAGE GRANTS holders can grant
  # privileges on objects, so object creators cannot hand out access.
  with_managed_access = "true"
}

# One monitor per warehouse so a runaway loader cannot starve analysts.
# Resource monitors and the warehouse-to-monitor assignment require ACCOUNTADMIN.
resource "snowflake_resource_monitor" "this" {
  provider = snowflake.accountadmin
  for_each = local.warehouses

  name            = "${var.name_prefix}_${var.environment}_${each.key}_RM"
  credit_quota    = each.value.credit_quota
  frequency       = "MONTHLY"
  start_timestamp = "IMMEDIATELY"
  notify_users    = var.notify_users

  notify_triggers           = [50, 75, 90]
  suspend_trigger           = 100
  suspend_immediate_trigger = 110
}

resource "snowflake_warehouse" "this" {
  provider = snowflake.accountadmin
  for_each = local.warehouses

  name                                = "${var.name_prefix}_${var.environment}_${each.key}_WH"
  comment                             = "${each.key} workload. Auto-suspends after ${each.value.auto_suspend_seconds}s idle; capped by a monthly resource monitor."
  warehouse_size                      = each.value.size
  warehouse_type                      = "STANDARD"
  auto_suspend                        = each.value.auto_suspend_seconds
  auto_resume                         = "true"
  initially_suspended                 = true
  statement_timeout_in_seconds        = each.value.statement_timeout_seconds
  statement_queued_timeout_in_seconds = 600
  resource_monitor                    = snowflake_resource_monitor.this[each.key].name
}
