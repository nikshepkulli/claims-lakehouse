output "database_name" {
  description = "Name of the lakehouse database."
  value       = snowflake_database.this.name
}

output "schema_names" {
  description = "Schema names created in the database."
  value       = keys(snowflake_schema.this)
}

output "warehouse_names" {
  description = "Warehouse name per workload."
  value       = { for k, w in snowflake_warehouse.this : k => w.name }
}

output "functional_role_names" {
  description = "Functional role name per workload. Grant these to users / service users."
  value       = { for k, r in snowflake_account_role.functional : k => r.name }
}

output "resource_monitor_names" {
  description = "Resource monitor name per warehouse."
  value       = { for k, m in snowflake_resource_monitor.this : k => m.name }
}

output "storage_integration_name" {
  description = "Storage integration name, or null when the external stage is disabled."
  value       = try(snowflake_storage_integration_aws.gold[0].name, null)
}

output "storage_aws_iam_user_arn" {
  description = "Snowflake-side IAM user ARN to put in the AWS role's trust policy (null until the integration exists)."
  value       = try(snowflake_storage_integration_aws.gold[0].describe_output[0].iam_user_arn, null)
}

output "storage_aws_external_id" {
  description = "External ID for the AWS role's trust policy sts:ExternalId condition (null until the integration exists)."
  value       = try(snowflake_storage_integration_aws.gold[0].describe_output[0].external_id, null)
}
