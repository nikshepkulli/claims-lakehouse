output "ml_bucket_name" {
  description = "Name of the ML artifacts bucket."
  value       = aws_s3_bucket.ml.id
}

output "s3_prefixes" {
  description = "Key prefixes: models/, training-output/, processing/ in the ML bucket, plus the gold feature and scores prefixes."
  value = {
    models          = "s3://${aws_s3_bucket.ml.id}/${local.prefixes.models}"
    training_output = "s3://${aws_s3_bucket.ml.id}/${local.prefixes.training_output}"
    processing      = "s3://${aws_s3_bucket.ml.id}/${local.prefixes.processing}"
    features        = "s3://${var.gold_bucket_name}/${var.features_prefix}"
    scores          = "s3://${var.gold_bucket_name}/${var.scores_prefix}"
  }
}

output "execution_role_arn" {
  description = "ARN of the SageMaker execution role to pass as RoleArn to training/processing jobs."
  value       = aws_iam_role.execution.arn
}

output "ecr_repository_url" {
  description = "ECR repository URL for the training/scoring image (null when not created)."
  value       = one(aws_ecr_repository.ml[*].repository_url)
}

output "model_package_group_arn" {
  description = "ARN of the Model Package Group (null when not created)."
  value       = one(aws_sagemaker_model_package_group.this[*].arn)
}
