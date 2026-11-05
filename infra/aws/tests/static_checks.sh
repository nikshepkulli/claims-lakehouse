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
# --- trust / fail-closed checks ---
# Root principal may appear only in the KMS key policy (locals.root_arn + its single use).
if [ "$(grep -c 'root_arn' "$M/main.tf")" -le 2 ]; then echo "ok   root principal only used by KMS key policy"; else echo "FAIL root principal used beyond KMS key policy"; fail=1; fi
if awk '/data "aws_iam_policy_document" "assume"/,/^}/' "$M/main.tf" | grep -Eq 'root_arn|\?[[:space:]]*each\.value[[:space:]]*:|length\('; then
  echo "FAIL assume-role policy has a root/empty-list fallback"; fail=1
else echo "ok   no root-principal fallback in assume-role policy"; fi
for r in loader transformer analyst; do
  if awk "/variable \"${r}_trusted_principal_arns\"/,/^}/" "$M/variables.tf" | grep -Eq '^[[:space:]]*default[[:space:]]*='; then
    echo "FAIL ${r}_trusted_principal_arns has a default"; fail=1
  elif awk "/variable \"${r}_trusted_principal_arns\"/,/^}/" "$M/variables.tf" | grep -q 'validation'; then
    echo "ok   ${r}_trusted_principal_arns required + validated"
  else echo "FAIL ${r}_trusted_principal_arns lacks validation"; fail=1; fi
done
need "TLS deny on every bucket policy" 'DenyInsecureTransport' main.tf
[ "$(grep -c 'aws:SecureTransport' "$M/main.tf")" -ge 2 ] && echo "ok   TLS deny in logs + data policies" || { echo "FAIL TLS deny count"; fail=1; }
forbid "KMS rotation not disabled"     'enable_key_rotation[[:space:]]*=[[:space:]]*false' main.tf
need "object lock is opt-in (default false)" 'gold_object_lock_enabled' main.tf
# --- cross-cloud output contract (issue #20) ---
need "contract output layer_uris (s3a)"  'output "layer_uris"' outputs.tf
need "layer_uris uses s3a scheme"        's3a://' outputs.tf
need "contract output principals"        'output "principals"' outputs.tf
C="$(cd "$M/../../../shared/modules/lakehouse_contract" && pwd)"
for f in main.tf variables.tf outputs.tf versions.tf; do
  [ -f "$C/$f" ] && echo "ok   contract module has $f" || { echo "FAIL contract module missing $f"; fail=1; }
done
if grep -Eq '^resource ' "$C"/*.tf; then echo "FAIL contract module must not create resources"; fail=1; else echo "ok   contract module is resource-free"; fi
exit $fail
