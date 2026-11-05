output "bucket_names" {
  value = module.lakehouse_storage.bucket_names
}

output "role_arns" {
  value = module.lakehouse_storage.role_arns
}

output "kms_key_arn" {
  value = module.lakehouse_storage.kms_key_arn
}
