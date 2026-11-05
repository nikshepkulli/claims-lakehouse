# Snowflake credentials come from SNOWFLAKE_* environment variables (see the
# module's providers.tf). Nothing here is applied by CI.

module "lakehouse_warehouse" {
  source = "../../modules/lakehouse_warehouse"

  name_prefix         = "CLAIMS"
  environment         = "DEV"
  data_retention_days = 1

  notify_users          = var.notify_users
  enable_external_stage = var.enable_external_stage
  gold_s3_url           = var.gold_s3_url
  storage_aws_role_arn  = var.storage_aws_role_arn
}
