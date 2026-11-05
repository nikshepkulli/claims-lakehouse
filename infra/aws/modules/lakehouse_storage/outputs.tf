output "bucket_names" {
  description = "Map of layer (bronze/silver/gold) to bucket name."
  value       = { for l, b in aws_s3_bucket.data : l => b.id }
}

output "bucket_arns" {
  description = "Map of layer to bucket ARN."
  value       = { for l, b in aws_s3_bucket.data : l => b.arn }
}

output "access_log_bucket_name" {
  description = "Name of the S3 server access log bucket."
  value       = aws_s3_bucket.logs.id
}

output "kms_key_arn" {
  description = "ARN of the customer-managed KMS key encrypting the data buckets."
  value       = aws_kms_key.lakehouse.arn
}

output "kms_key_alias" {
  description = "Alias of the CMK."
  value       = aws_kms_alias.lakehouse.name
}

output "role_arns" {
  description = "Map of role name (loader/transformer/analyst) to IAM role ARN."
  value       = { for k, r in aws_iam_role.this : k => r.arn }
}
