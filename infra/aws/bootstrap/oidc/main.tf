provider "aws" {
  region = var.region

  default_tags {
    tags = {
      Repository = "nikshepkulli/claims-lakehouse"
      ManagedBy  = "terraform-bootstrap-oidc"
    }
  }
}

data "aws_caller_identity" "current" {}
data "aws_partition" "current" {}

locals {
  partition  = data.aws_partition.current.partition
  account_id = data.aws_caller_identity.current.account_id
  gh_env     = "aws-${var.environment}"
  oidc_host  = "token.actions.githubusercontent.com"

  # Trust subjects (the `sub` claim GitHub puts in the token).
  sub_environment = "repo:${var.github_repository}:environment:${local.gh_env}"
  sub_pr          = "repo:${var.github_repository}:pull_request"
  sub_main        = "repo:${var.github_repository}:ref:refs/heads/main"

  apply_workflow_ref = "${var.github_repository}/.github/workflows/terraform-apply.yml@refs/heads/main"

  oidc_provider_arn = var.create_oidc_provider ? aws_iam_openid_connect_provider.github[0].arn : "arn:${local.partition}:iam::${local.account_id}:oidc-provider/${local.oidc_host}"
}

# ---------------------------------------------------------------------------
# GitHub OIDC identity provider. AWS validates GitHub's token signature against
# its own trusted CA library, so the thumbprint is no longer used for
# verification; a value is still required by older provider versions.
# ---------------------------------------------------------------------------
resource "aws_iam_openid_connect_provider" "github" {
  count = var.create_oidc_provider ? 1 : 0

  url             = "https://${local.oidc_host}"
  client_id_list  = ["sts.amazonaws.com"]
  thumbprint_list = ["6938fd4d98bab03faadb97b34396831e3780aea1"]
}

# ---------------------------------------------------------------------------
# PLAN role: read-only. Assumed by PR plan jobs and by the plan job inside the
# manual apply workflow. Can never change infrastructure.
# ---------------------------------------------------------------------------
data "aws_iam_policy_document" "plan_trust" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [local.oidc_provider_arn]
    }

    condition {
      test     = "StringEquals"
      variable = "${local.oidc_host}:aud"
      values   = ["sts.amazonaws.com"]
    }

    # Exact match, no wildcards: same-repo PRs, or runs on main.
    condition {
      test     = "StringEquals"
      variable = "${local.oidc_host}:sub"
      values   = [local.sub_pr, local.sub_main]
    }
  }
}

resource "aws_iam_role" "plan" {
  name                 = "${var.name_prefix}-gha-plan"
  description          = "GitHub Actions terraform plan (read-only) for ${var.github_repository}"
  assume_role_policy   = data.aws_iam_policy_document.plan_trust.json
  max_session_duration = 3600
}

# AWS-managed ReadOnlyAccess would also expose every S3 object and secret in
# the account; use the narrower ViewOnly job-function policy (metadata only)
# plus the explicit reads Terraform's refresh needs.
resource "aws_iam_role_policy_attachment" "plan_view_only" {
  role       = aws_iam_role.plan.name
  policy_arn = "arn:${local.partition}:iam::aws:policy/job-function/ViewOnlyAccess"
}

data "aws_iam_policy_document" "plan_extra" {
  statement {
    sid    = "RefreshReads"
    effect = "Allow"
    actions = [
      "s3:GetBucket*",
      "s3:GetLifecycleConfiguration",
      "s3:GetEncryptionConfiguration",
      "s3:GetAccelerateConfiguration",
      "s3:GetReplicationConfiguration",
      "s3:GetObjectLockConfiguration",
      "s3:ListBucket",
      "kms:DescribeKey",
      "kms:GetKeyPolicy",
      "kms:GetKeyRotationStatus",
      "kms:ListResourceTags",
      "iam:GetRole",
      "iam:GetRolePolicy",
      "iam:ListRolePolicies",
      "iam:ListAttachedRolePolicies",
      "iam:ListRoleTags",
    ]
    resources = ["*"]
  }

  dynamic "statement" {
    for_each = var.state_bucket_name == "" ? [] : [1]
    content {
      sid       = "StateRead"
      effect    = "Allow"
      actions   = ["s3:GetObject"]
      resources = ["arn:${local.partition}:s3:::${var.state_bucket_name}/${var.state_key_prefix}*"]
    }
  }

  dynamic "statement" {
    for_each = var.state_lock_table_name == "" ? [] : [1]
    content {
      sid       = "StateLockForPlan"
      effect    = "Allow"
      actions   = ["dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:DeleteItem"]
      resources = ["arn:${local.partition}:dynamodb:*:${local.account_id}:table/${var.state_lock_table_name}"]
    }
  }
}

resource "aws_iam_role_policy" "plan_extra" {
  name   = "plan-extra-reads"
  role   = aws_iam_role.plan.id
  policy = data.aws_iam_policy_document.plan_extra.json
}

# ---------------------------------------------------------------------------
# APPLY role: write access scoped to the lakehouse stack. Trust is bound to
# the GitHub Environment (where required reviewers = manual approval) AND to
# the apply workflow file on main.
# ---------------------------------------------------------------------------
data "aws_iam_policy_document" "apply_trust" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [local.oidc_provider_arn]
    }

    condition {
      test     = "StringEquals"
      variable = "${local.oidc_host}:aud"
      values   = ["sts.amazonaws.com"]
    }

    # Token is only minted with this sub when the job targets the Environment
    # (so the approval gate has already passed).
    condition {
      test     = "StringEquals"
      variable = "${local.oidc_host}:sub"
      values   = [local.sub_environment]
    }

    # Branch pin: the workflow file must be the one on main. Fails closed.
    # Needs IAM support for the job_workflow_ref federation key; see docs/cd.md
    # ("Untested") and set pin_workflow_ref=false to rely on the Environment
    # deployment-branch policy alone.
    dynamic "condition" {
      for_each = var.pin_workflow_ref ? [1] : []
      content {
        test     = "StringEquals"
        variable = "${local.oidc_host}:job_workflow_ref"
        values   = [local.apply_workflow_ref]
      }
    }
  }
}

resource "aws_iam_role" "apply" {
  name                 = "${var.name_prefix}-gha-apply"
  description          = "GitHub Actions terraform apply for ${var.github_repository} (${local.gh_env}, main only)"
  assume_role_policy   = data.aws_iam_policy_document.apply_trust.json
  max_session_duration = 3600
}

data "aws_iam_policy_document" "apply" {
  # S3: only buckets named <prefix>-*. Bucket-level configuration, no object access.
  statement {
    sid    = "LakehouseBuckets"
    effect = "Allow"
    actions = [
      "s3:CreateBucket",
      "s3:DeleteBucket",
      "s3:ListBucket",
      "s3:GetBucket*",
      "s3:PutBucket*",
      "s3:DeleteBucketPolicy",
      "s3:GetLifecycleConfiguration",
      "s3:PutLifecycleConfiguration",
      "s3:GetEncryptionConfiguration",
      "s3:PutEncryptionConfiguration",
      "s3:GetObjectLockConfiguration",
      "s3:PutObjectLockConfiguration",
      "s3:GetAccelerateConfiguration",
      "s3:GetReplicationConfiguration",
    ]
    resources = ["arn:${local.partition}:s3:::${var.name_prefix}-*"]
  }

  # force_destroy=true in dev empties buckets on destroy. Granted for dev only
  # (the variable below), never for prod-like environments.
  dynamic "statement" {
    for_each = var.environment == "dev" ? [1] : []
    content {
      sid    = "DevForceDestroyObjects"
      effect = "Allow"
      actions = [
        "s3:DeleteObject",
        "s3:DeleteObjectVersion",
        "s3:ListBucketVersions",
        "s3:GetObjectVersion",
      ]
      resources = [
        "arn:${local.partition}:s3:::${var.name_prefix}-*",
        "arn:${local.partition}:s3:::${var.name_prefix}-*/*",
      ]
    }
  }

  # IAM: only roles named <prefix>-* (never the gha-* roles themselves, see deny).
  statement {
    sid    = "LakehouseRoles"
    effect = "Allow"
    actions = [
      "iam:CreateRole",
      "iam:DeleteRole",
      "iam:GetRole",
      "iam:UpdateRole",
      "iam:UpdateAssumeRolePolicy",
      "iam:TagRole",
      "iam:UntagRole",
      "iam:ListRoleTags",
      "iam:PutRolePolicy",
      "iam:GetRolePolicy",
      "iam:DeleteRolePolicy",
      "iam:ListRolePolicies",
      "iam:ListAttachedRolePolicies",
      "iam:ListInstanceProfilesForRole",
    ]
    resources = ["arn:${local.partition}:iam::${local.account_id}:role/${var.name_prefix}-*"]
  }

  # Protect the CD roles from being edited by the pipeline they authenticate.
  statement {
    sid    = "DenyEditingCdRoles"
    effect = "Deny"
    actions = [
      "iam:*",
    ]
    resources = [
      aws_iam_role.apply.arn,
      aws_iam_role.plan.arn,
    ]
  }

  # KMS: key ARNs are not known before creation, so resource-scoping by ARN is
  # impossible for Create/Tag. Constrain to the region instead; aliases are
  # name-scoped. Deletion is a 7-30 day schedule, never immediate.
  statement {
    sid    = "LakehouseKms"
    effect = "Allow"
    actions = [
      "kms:CreateKey",
      "kms:DescribeKey",
      "kms:GetKeyPolicy",
      "kms:PutKeyPolicy",
      "kms:GetKeyRotationStatus",
      "kms:EnableKeyRotation",
      "kms:ScheduleKeyDeletion",
      "kms:TagResource",
      "kms:UntagResource",
      "kms:ListResourceTags",
    ]
    resources = ["*"]

    condition {
      test     = "StringEquals"
      variable = "aws:RequestedRegion"
      values   = [var.region]
    }
  }

  statement {
    sid    = "LakehouseKmsAlias"
    effect = "Allow"
    actions = [
      "kms:CreateAlias",
      "kms:DeleteAlias",
      "kms:UpdateAlias",
    ]
    resources = [
      "arn:${local.partition}:kms:${var.region}:${local.account_id}:alias/${var.name_prefix}-*",
      "arn:${local.partition}:kms:${var.region}:${local.account_id}:key/*",
    ]
  }

  statement {
    sid       = "KmsListAliases"
    effect    = "Allow"
    actions   = ["kms:ListAliases"]
    resources = ["*"]
  }

  statement {
    sid       = "WhoAmI"
    effect    = "Allow"
    actions   = ["sts:GetCallerIdentity"]
    resources = ["*"]
  }

  dynamic "statement" {
    for_each = var.state_bucket_name == "" ? [] : [1]
    content {
      sid       = "StateObjects"
      effect    = "Allow"
      actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
      resources = ["arn:${local.partition}:s3:::${var.state_bucket_name}/${var.state_key_prefix}*"]
    }
  }

  dynamic "statement" {
    for_each = var.state_bucket_name == "" ? [] : [1]
    content {
      sid       = "StateBucketList"
      effect    = "Allow"
      actions   = ["s3:ListBucket"]
      resources = ["arn:${local.partition}:s3:::${var.state_bucket_name}"]
    }
  }

  dynamic "statement" {
    for_each = var.state_lock_table_name == "" ? [] : [1]
    content {
      sid       = "StateLock"
      effect    = "Allow"
      actions   = ["dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:DeleteItem"]
      resources = ["arn:${local.partition}:dynamodb:*:${local.account_id}:table/${var.state_lock_table_name}"]
    }
  }
}

resource "aws_iam_role_policy" "apply" {
  name   = "lakehouse-apply"
  role   = aws_iam_role.apply.id
  policy = data.aws_iam_policy_document.apply.json
}
