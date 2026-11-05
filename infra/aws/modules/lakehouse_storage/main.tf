data "aws_caller_identity" "current" {}
data "aws_partition" "current" {}

locals {
  account_id = data.aws_caller_identity.current.account_id
  partition  = data.aws_partition.current.partition
  layers     = toset(["bronze", "silver", "gold"])

  bucket_names = {
    for l in local.layers : l => "${var.name_prefix}-${l}-${local.account_id}"
  }
  log_bucket_name = "${var.name_prefix}-access-logs-${local.account_id}"

  common_tags = merge(
    {
      Project     = "claims-lakehouse"
      Environment = var.environment
      ManagedBy   = "terraform"
      DataClass   = "synthetic-phi-like"
    },
    var.tags,
  )

  root_arn = "arn:${local.partition}:iam::${local.account_id}:root"
}

# ---------------------------------------------------------------------------
# KMS customer-managed key (rotation on). Key policy delegates to IAM so the
# per-role policies below are the single place access is granted.
# ---------------------------------------------------------------------------
data "aws_iam_policy_document" "kms" {
  statement {
    sid       = "EnableIAMPolicies"
    actions   = ["kms:*"]
    resources = ["*"]
    principals {
      type        = "AWS"
      identifiers = [local.root_arn]
    }
  }
}

resource "aws_kms_key" "lakehouse" {
  description             = "${var.name_prefix} lakehouse data encryption key"
  enable_key_rotation     = true
  deletion_window_in_days = var.kms_deletion_window_days
  policy                  = data.aws_iam_policy_document.kms.json
  tags                    = local.common_tags
}

resource "aws_kms_alias" "lakehouse" {
  name          = "alias/${var.name_prefix}-lakehouse"
  target_key_id = aws_kms_key.lakehouse.key_id
}

# ---------------------------------------------------------------------------
# Access-log bucket. S3 server access logging cannot deliver to SSE-KMS
# buckets, so this one uses SSE-S3 (AES256) by design.
# ---------------------------------------------------------------------------
resource "aws_s3_bucket" "logs" {
  bucket        = local.log_bucket_name
  force_destroy = var.force_destroy
  tags          = merge(local.common_tags, { Layer = "access-logs" })
}

resource "aws_s3_bucket_ownership_controls" "logs" {
  bucket = aws_s3_bucket.logs.id
  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_public_access_block" "logs" {
  bucket                  = aws_s3_bucket.logs.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_versioning" "logs" {
  bucket = aws_s3_bucket.logs.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "logs" {
  bucket = aws_s3_bucket.logs.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "logs" {
  bucket = aws_s3_bucket.logs.id

  rule {
    id     = "expire-logs"
    status = "Enabled"
    filter {}
    expiration {
      days = var.access_log_retention_days
    }
    noncurrent_version_expiration {
      noncurrent_days = 30
    }
    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }

  depends_on = [aws_s3_bucket_versioning.logs]
}

data "aws_iam_policy_document" "logs_bucket" {
  statement {
    sid       = "DenyInsecureTransport"
    effect    = "Deny"
    actions   = ["s3:*"]
    resources = [aws_s3_bucket.logs.arn, "${aws_s3_bucket.logs.arn}/*"]
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

  statement {
    sid       = "AllowS3LogDelivery"
    actions   = ["s3:PutObject"]
    resources = ["${aws_s3_bucket.logs.arn}/*"]
    principals {
      type        = "Service"
      identifiers = ["logging.s3.amazonaws.com"]
    }
    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [local.account_id]
    }
    condition {
      test     = "ArnLike"
      variable = "aws:SourceArn"
      values   = [for b in aws_s3_bucket.data : b.arn]
    }
  }
}

resource "aws_s3_bucket_policy" "logs" {
  bucket = aws_s3_bucket.logs.id
  policy = data.aws_iam_policy_document.logs_bucket.json

  depends_on = [aws_s3_bucket_public_access_block.logs]
}

# ---------------------------------------------------------------------------
# Bronze / silver / gold data buckets
# ---------------------------------------------------------------------------
resource "aws_s3_bucket" "data" {
  for_each = local.layers

  bucket        = local.bucket_names[each.key]
  force_destroy = var.force_destroy
  tags          = merge(local.common_tags, { Layer = each.key })
}

resource "aws_s3_bucket_ownership_controls" "data" {
  for_each = local.layers

  bucket = aws_s3_bucket.data[each.key].id
  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_public_access_block" "data" {
  for_each = local.layers

  bucket                  = aws_s3_bucket.data[each.key].id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_versioning" "data" {
  for_each = local.layers

  bucket = aws_s3_bucket.data[each.key].id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data" {
  for_each = local.layers

  bucket = aws_s3_bucket.data[each.key].id
  rule {
    bucket_key_enabled = true
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.lakehouse.arn
    }
  }
}

resource "aws_s3_bucket_logging" "data" {
  for_each = local.layers

  bucket        = aws_s3_bucket.data[each.key].id
  target_bucket = aws_s3_bucket.logs.id
  target_prefix = "${each.key}/"

  depends_on = [aws_s3_bucket_policy.logs]
}

resource "aws_s3_bucket_lifecycle_configuration" "data" {
  for_each = local.layers

  bucket = aws_s3_bucket.data[each.key].id

  rule {
    id     = "noncurrent-and-multipart-cleanup"
    status = "Enabled"
    filter {}
    noncurrent_version_expiration {
      noncurrent_days = var.noncurrent_version_expiration_days
    }
    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }

  dynamic "rule" {
    for_each = each.key == "bronze" ? [1] : []
    content {
      id     = "bronze-intelligent-tiering"
      status = "Enabled"
      filter {}
      transition {
        days          = var.bronze_intelligent_tiering_days
        storage_class = "INTELLIGENT_TIERING"
      }
    }
  }

  depends_on = [aws_s3_bucket_versioning.data]
}

data "aws_iam_policy_document" "data_bucket" {
  for_each = local.layers

  statement {
    sid       = "DenyInsecureTransport"
    effect    = "Deny"
    actions   = ["s3:*"]
    resources = [aws_s3_bucket.data[each.key].arn, "${aws_s3_bucket.data[each.key].arn}/*"]
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

  statement {
    sid       = "DenyWrongKmsKey"
    effect    = "Deny"
    actions   = ["s3:PutObject"]
    resources = ["${aws_s3_bucket.data[each.key].arn}/*"]
    principals {
      type        = "*"
      identifiers = ["*"]
    }
    condition {
      test     = "StringNotEqualsIfExists"
      variable = "s3:x-amz-server-side-encryption-aws-kms-key-id"
      values   = [aws_kms_key.lakehouse.arn]
    }
  }
}

resource "aws_s3_bucket_policy" "data" {
  for_each = local.layers

  bucket = aws_s3_bucket.data[each.key].id
  policy = data.aws_iam_policy_document.data_bucket[each.key].json

  depends_on = [aws_s3_bucket_public_access_block.data]
}

# ---------------------------------------------------------------------------
# Least-privilege roles: loader, transformer, analyst
# ---------------------------------------------------------------------------
locals {
  roles = {
    loader      = var.loader_trusted_principal_arns
    transformer = var.transformer_trusted_principal_arns
    analyst     = var.analyst_trusted_principal_arns
  }
}

data "aws_iam_policy_document" "assume" {
  for_each = local.roles

  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "AWS"
      identifiers = length(each.value) > 0 ? each.value : [local.root_arn]
    }
  }
}

resource "aws_iam_role" "this" {
  for_each = local.roles

  name                 = "${var.name_prefix}-${each.key}"
  assume_role_policy   = data.aws_iam_policy_document.assume[each.key].json
  max_session_duration = var.max_session_duration
  tags                 = local.common_tags
}

locals {
  b = { for l in local.layers : l => aws_s3_bucket.data[l].arn }
}

# loader: write-only into bronze (no read of silver/gold, no delete)
data "aws_iam_policy_document" "loader" {
  statement {
    sid       = "ListBronze"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [local.b["bronze"]]
  }
  statement {
    sid       = "WriteBronze"
    actions   = ["s3:PutObject", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"]
    resources = ["${local.b["bronze"]}/*"]
  }
  statement {
    sid       = "EncryptWithLakehouseKey"
    actions   = ["kms:GenerateDataKey", "kms:Encrypt", "kms:DescribeKey"]
    resources = [aws_kms_key.lakehouse.arn]
  }
}

# transformer: read bronze; read/write silver and gold; no bucket-level admin
data "aws_iam_policy_document" "transformer" {
  statement {
    sid       = "ListLayers"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [local.b["bronze"], local.b["silver"], local.b["gold"]]
  }
  statement {
    sid       = "ReadBronze"
    actions   = ["s3:GetObject", "s3:GetObjectVersion"]
    resources = ["${local.b["bronze"]}/*"]
  }
  statement {
    sid       = "ReadWriteSilverGold"
    actions   = ["s3:GetObject", "s3:GetObjectVersion", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"]
    resources = ["${local.b["silver"]}/*", "${local.b["gold"]}/*"]
  }
  statement {
    sid       = "UseLakehouseKey"
    actions   = ["kms:Decrypt", "kms:GenerateDataKey", "kms:Encrypt", "kms:DescribeKey"]
    resources = [aws_kms_key.lakehouse.arn]
  }
}

# analyst: gold read-only
data "aws_iam_policy_document" "analyst" {
  statement {
    sid       = "ListGold"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [local.b["gold"]]
  }
  statement {
    sid       = "ReadGold"
    actions   = ["s3:GetObject", "s3:GetObjectVersion"]
    resources = ["${local.b["gold"]}/*"]
  }
  statement {
    sid       = "DecryptOnly"
    actions   = ["kms:Decrypt", "kms:DescribeKey"]
    resources = [aws_kms_key.lakehouse.arn]
  }
}

locals {
  role_policies = {
    loader      = data.aws_iam_policy_document.loader.json
    transformer = data.aws_iam_policy_document.transformer.json
    analyst     = data.aws_iam_policy_document.analyst.json
  }
}

resource "aws_iam_role_policy" "this" {
  for_each = local.role_policies

  name   = "${var.name_prefix}-${each.key}-access"
  role   = aws_iam_role.this[each.key].id
  policy = each.value
}
