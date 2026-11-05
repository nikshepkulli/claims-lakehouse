output "bucket_names" {
  value = module.lakehouse_storage.bucket_names
}

output "role_arns" {
  value = module.lakehouse_storage.role_arns
}

output "kms_key_arn" {
  value = module.lakehouse_storage.kms_key_arn
}

output "layer_uris" {
  description = "bronze/silver/gold s3a:// URIs (cross-cloud contract)."
  value       = module.contract.layer_uris
}

output "principals" {
  description = "loader/transformer/analyst => {id, principal_id} (cross-cloud contract)."
  value       = module.contract.principals
}
