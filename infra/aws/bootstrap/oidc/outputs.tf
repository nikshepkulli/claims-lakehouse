output "plan_role_arn" {
  description = "Set as repo variable AWS_PLAN_ROLE_ARN."
  value       = aws_iam_role.plan.arn
}

output "apply_role_arn" {
  description = "Set as variable AWS_ROLE_ARN on the GitHub Environment aws-<env>."
  value       = aws_iam_role.apply.arn
}

output "github_environment" {
  description = "Name of the GitHub Environment the apply role trusts."
  value       = local.gh_env
}
