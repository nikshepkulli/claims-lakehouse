# S3 load path for the gold layer (optional, enable_external_stage = true).
#
# Snowflake cannot read Delta tables natively (ADR-0002), so what this stage
# points at is a Parquet EXPORT of gold (or UniForm/Iceberg metadata, which is
# read through Iceberg tables instead, not this stage). The export job is not
# part of this module. The stage reads plain Parquet files; it does not
# understand the Delta transaction log, so exporting a table that has been
# updated or vacuumed needs a snapshot export, not a copy of the live files.

resource "snowflake_storage_integration_aws" "gold" {
  provider = snowflake.accountadmin
  count    = var.enable_external_stage ? 1 : 0

  name                      = "${local.role_prefix}_GOLD_S3_INT"
  comment                   = "Read-only access to the gold Parquet export bucket."
  enabled                   = true
  storage_provider          = "S3"
  storage_aws_role_arn      = var.storage_aws_role_arn
  storage_allowed_locations = [var.gold_s3_url]

  lifecycle {
    precondition {
      condition     = var.gold_s3_url != "" && var.storage_aws_role_arn != ""
      error_message = "gold_s3_url and storage_aws_role_arn are required when enable_external_stage is true."
    }
  }
}

# SYSADMIN creates the stage, so it needs USAGE on the integration.
resource "snowflake_grant_privileges_to_account_role" "sysadmin_integration_usage" {
  provider = snowflake.securityadmin
  count    = var.enable_external_stage ? 1 : 0

  account_role_name = var.sysadmin_role_name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "INTEGRATION"
    object_name = snowflake_storage_integration_aws.gold[0].name
  }
}

resource "snowflake_file_format_parquet" "gold" {
  provider = snowflake.sysadmin

  name     = "FF_PARQUET"
  database = snowflake_database.this.name
  schema   = snowflake_schema.this["RAW"].name
  comment  = "Parquet files exported from the gold Delta tables."

  # Read Parquet DATE/TIMESTAMP logical types as dates/timestamps, not integers.
  use_logical_type = "true"

  depends_on = [snowflake_grant_privileges_to_account_role.future_file_formats]
}

resource "snowflake_stage_external_s3" "gold" {
  provider = snowflake.sysadmin
  count    = var.enable_external_stage ? 1 : 0

  name                = "GOLD_STAGE"
  database            = snowflake_database.this.name
  schema              = snowflake_schema.this["RAW"].name
  url                 = var.gold_s3_url
  storage_integration = snowflake_storage_integration_aws.gold[0].name
  comment             = "External stage over the gold Parquet export (one folder per table)."

  depends_on = [
    snowflake_grant_privileges_to_account_role.sysadmin_integration_usage,
    snowflake_grant_privileges_to_account_role.future_stages,
  ]
}
