output "database_name" {
  value = module.lakehouse_warehouse.database_name
}

output "warehouse_names" {
  value = module.lakehouse_warehouse.warehouse_names
}

output "functional_role_names" {
  value = module.lakehouse_warehouse.functional_role_names
}

output "storage_aws_iam_user_arn" {
  description = "Put this in the AWS role trust policy (principal)."
  value       = module.lakehouse_warehouse.storage_aws_iam_user_arn
}

output "storage_aws_external_id" {
  description = "Put this in the AWS role trust policy (sts:ExternalId condition)."
  value       = module.lakehouse_warehouse.storage_aws_external_id
}
