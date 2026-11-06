# SageMaker-ready ML plumbing for the claim-denial-risk model (ticket #27).
# What this creates: a private encrypted S3 bucket with job prefixes, an execution role (least
# privilege, optional permissions boundary), an ECR repository and an optional Model Package Group.
# What it deliberately does NOT create: endpoints, notebook instances, domains, training jobs or
# anything else that bills while idle. Jobs are started by hand/CI; see docs/ml.md.
# NOT applied to any AWS account by the author: validated with terraform fmt/validate only.

data "aws_caller_identity" "current" {}
data "aws_partition" "current" {}
data "aws_region" "current" {}

locals {
  account_id = data.aws_caller_identity.current.account_id
  partition  = data.aws_partition.current.partition
  region     = data.aws_region.current.name

  bucket_name = "${var.name_prefix}-ml-${local.account_id}"
  # Prefixes inside the ML bucket. S3 has no folders; these are the key prefixes the role policy
  # and lifecycle rules are scoped to.
  prefixes = {
    models          = "models/"          # model.tar.gz written by training jobs
    training_output = "training-output/" # job output.tar.gz, metrics.json, failure files
    processing      = "processing/"      # scratch for batch-scoring Processing jobs
  }

  common_tags = merge(
    {
      Project     = "claims-lakehouse"
      Environment = var.environment
      ManagedBy   = "terraform"
      DataClass   = "synthetic-phi-like"
      Component   = "sagemaker-ml"
    },
    var.tags,
  )
}

# ---------------------------------------------------------------------------
# ML artifacts bucket: SSE-KMS with the shared CMK, versioned, public access blocked, TLS only.
# ---------------------------------------------------------------------------
resource "aws_s3_bucket" "ml" {
  bucket        = local.bucket_name
  force_destroy = var.force_destroy
  tags          = local.common_tags
}

resource "aws_s3_bucket_public_access_block" "ml" {
  bucket                  = aws_s3_bucket.ml.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "ml" {
  bucket = aws_s3_bucket.ml.id
  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_versioning" "ml" {
  bucket = aws_s3_bucket.ml.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "ml" {
  bucket = aws_s3_bucket.ml.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = var.kms_key_arn
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "ml" {
  bucket = aws_s3_bucket.ml.id

  rule {
    id     = "expire-noncurrent-versions"
    status = "Enabled"
    filter {}
    noncurrent_version_expiration {
      noncurrent_days = var.noncurrent_version_expiration_days
    }
    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }

  rule {
    id     = "expire-training-output"
    status = "Enabled"
    filter {
      prefix = local.prefixes.training_output
    }
    expiration {
      days = var.training_output_expiration_days
    }
  }

  depends_on = [aws_s3_bucket_versioning.ml]
}

data "aws_iam_policy_document" "bucket_tls_only" {
  statement {
    sid       = "DenyInsecureTransport"
    effect    = "Deny"
    actions   = ["s3:*"]
    resources = [aws_s3_bucket.ml.arn, "${aws_s3_bucket.ml.arn}/*"]
    principals {
      type        = "*"
      identifiers = ["*"]
    }
    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }
}

resource "aws_s3_bucket_policy" "ml" {
  bucket     = aws_s3_bucket.ml.id
  policy     = data.aws_iam_policy_document.bucket_tls_only.json
  depends_on = [aws_s3_bucket_public_access_block.ml]
}

# ---------------------------------------------------------------------------
# ECR repository for the container image (no image is pushed by Terraform).
# ---------------------------------------------------------------------------
resource "aws_ecr_repository" "ml" {
  count                = var.create_ecr_repository ? 1 : 0
  name                 = "${var.name_prefix}/claim-denial-risk"
  image_tag_mutability = "IMMUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }

  encryption_configuration {
    encryption_type = "KMS"
    kms_key         = var.kms_key_arn
  }

  tags = local.common_tags
}

# ---------------------------------------------------------------------------
# Execution role used by training and processing jobs (sagemaker.amazonaws.com).
# ---------------------------------------------------------------------------
data "aws_iam_policy_document" "trust" {
  statement {
    sid     = "SageMakerAssumeRole"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["sagemaker.amazonaws.com"]
    }
    # Confused-deputy guard: only SageMaker acting for this account may assume the role.
    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [local.account_id]
    }
    condition {
      test     = "ArnLike"
      variable = "aws:SourceArn"
      values = [
        "arn:${local.partition}:sagemaker:${local.region}:${local.account_id}:training-job/*",
        "arn:${local.partition}:sagemaker:${local.region}:${local.account_id}:processing-job/*",
      ]
    }
  }
}

resource "aws_iam_role" "execution" {
  name                 = "${var.name_prefix}-sagemaker-ml-execution"
  assume_role_policy   = data.aws_iam_policy_document.trust.json
  permissions_boundary = var.permissions_boundary_arn
  max_session_duration = 3600
  tags                 = local.common_tags
}

data "aws_iam_policy_document" "execution" {
  # Read the feature store in gold (training input, scoring input). Never write it.
  statement {
    sid       = "ReadFeatureStore"
    actions   = ["s3:GetObject"]
    resources = ["arn:${local.partition}:s3:::${var.gold_bucket_name}/${var.features_prefix}*"]
  }

  statement {
    sid       = "ListFeatureStorePrefix"
    actions   = ["s3:ListBucket"]
    resources = ["arn:${local.partition}:s3:::${var.gold_bucket_name}"]
    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["${var.features_prefix}*"]
    }
  }

  # The single gold location batch scores may be written back to.
  statement {
    sid       = "WriteScoresToGold"
    actions   = ["s3:PutObject"]
    resources = ["arn:${local.partition}:s3:::${var.gold_bucket_name}/${var.scores_prefix}*"]
  }

  # Models, job outputs and processing scratch in the ML bucket.
  statement {
    sid     = "ReadWriteMlPrefixes"
    actions = ["s3:GetObject", "s3:PutObject"]
    resources = [
      for p in values(local.prefixes) : "${aws_s3_bucket.ml.arn}/${p}*"
    ]
  }

  statement {
    sid       = "ListMlBucket"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [aws_s3_bucket.ml.arn]
  }

  statement {
    sid       = "UseDataKey"
    actions   = ["kms:Decrypt", "kms:GenerateDataKey", "kms:DescribeKey"]
    resources = [var.kms_key_arn]
  }

  statement {
    sid = "JobLogs"
    actions = [
      "logs:CreateLogStream",
      "logs:PutLogEvents",
      "logs:DescribeLogStreams",
    ]
    resources = [
      "arn:${local.partition}:logs:${local.region}:${local.account_id}:log-group:/aws/sagemaker/TrainingJobs:*",
      "arn:${local.partition}:logs:${local.region}:${local.account_id}:log-group:/aws/sagemaker/ProcessingJobs:*",
    ]
  }

  statement {
    sid       = "CreateJobLogGroups"
    actions   = ["logs:CreateLogGroup"]
    resources = ["arn:${local.partition}:logs:${local.region}:${local.account_id}:log-group:/aws/sagemaker/*"]
  }

  statement {
    sid       = "JobMetrics"
    actions   = ["cloudwatch:PutMetricData"]
    resources = ["*"] # PutMetricData has no resource-level permissions
    condition {
      test     = "StringEquals"
      variable = "cloudwatch:namespace"
      values   = ["/aws/sagemaker/TrainingJobs", "/aws/sagemaker/ProcessingJobs"]
    }
  }

  statement {
    sid       = "EcrLogin"
    actions   = ["ecr:GetAuthorizationToken"]
    resources = ["*"] # GetAuthorizationToken has no resource-level permissions
  }

  dynamic "statement" {
    for_each = var.create_ecr_repository ? [1] : []
    content {
      sid = "PullImage"
      actions = [
        "ecr:BatchCheckLayerAvailability",
        "ecr:BatchGetImage",
        "ecr:GetDownloadUrlForLayer",
      ]
      resources = [aws_ecr_repository.ml[0].arn]
    }
  }
}

resource "aws_iam_role_policy" "execution" {
  name   = "sagemaker-ml-least-privilege"
  role   = aws_iam_role.execution.id
  policy = data.aws_iam_policy_document.execution.json
}

# ---------------------------------------------------------------------------
# Optional model registry container. Versions are registered by the pipeline, not by Terraform.
# ---------------------------------------------------------------------------
resource "aws_sagemaker_model_package_group" "this" {
  count                           = var.create_model_package_group ? 1 : 0
  model_package_group_name        = var.model_package_group_name
  model_package_group_description = "Claim denial risk model versions (synthetic data, ticket #27)"
  tags                            = local.common_tags
}
