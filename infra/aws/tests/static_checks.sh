#!/usr/bin/env bash
# Static security-invariant checks for the lakehouse_storage module.
# Needed because Terraform 1.5 has no `terraform test` / mock providers and CI has no AWS creds.
# Usage: bash infra/aws/tests/static_checks.sh
set -euo pipefail
M="$(cd "$(dirname "$0")/../modules/lakehouse_storage" && pwd)"
fail=0
need() { # description, pattern, file
  if grep -Eq "$2" "$M/$3"; then echo "ok   $1"; else echo "FAIL $1"; fail=1; fi
}
forbid() {
  if grep -Eq "$2" "$M/$3"; then echo "FAIL $1"; fail=1; else echo "ok   $1"; fi
}
need "KMS rotation enabled"            'enable_key_rotation[[:space:]]*=[[:space:]]*true' main.tf
need "SSE-KMS on data buckets"         'sse_algorithm[[:space:]]*=[[:space:]]*"aws:kms"' main.tf
need "block_public_acls"               'block_public_acls[[:space:]]*=[[:space:]]*true' main.tf
need "restrict_public_buckets"         'restrict_public_buckets[[:space:]]*=[[:space:]]*true' main.tf
need "versioning enabled"              'status[[:space:]]*=[[:space:]]*"Enabled"' main.tf
need "TLS-only deny"                   'aws:SecureTransport' main.tf
need "access logging configured"       'aws_s3_bucket_logging' main.tf
need "intelligent tiering for bronze"  'INTELLIGENT_TIERING' main.tf
need "noncurrent expiry"               'noncurrent_version_expiration' main.tf
need "analyst role defined"            'analyst' main.tf
forbid "analyst cannot write"          'analyst.*s3:PutObject' main.tf
forbid "no hardcoded access keys"      'AKIA[0-9A-Z]{16}' main.tf
# Role-policy statements must not use Resource "*" except the KMS key policy.
if awk '/data "aws_iam_policy_document" "(loader|transformer|analyst)"/,/^}/' "$M/main.tf" | grep -q 'resources[[:space:]]*=[[:space:]]*\["\*"\]'; then
  echo "FAIL role policies use wildcard resource"; fail=1
else echo "ok   role policies have scoped resources"; fi
exit $fail
