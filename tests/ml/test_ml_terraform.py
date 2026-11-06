"""Static invariants of infra/aws/modules/sagemaker_ml (no AWS credentials needed)."""

import re
from pathlib import Path

MOD = Path(__file__).resolve().parents[2] / "infra" / "aws" / "modules" / "sagemaker_ml"


def _tf() -> str:
    return "\n".join(p.read_text() for p in sorted(MOD.glob("*.tf")))


def test_module_files_exist():
    for f in ("main.tf", "variables.tf", "outputs.tf", "versions.tf", "README.md"):
        assert (MOD / f).is_file(), f


def test_no_idle_billing_resources():
    tf = _tf()
    for forbidden in (
        "aws_sagemaker_endpoint",
        "aws_sagemaker_endpoint_configuration",
        "aws_sagemaker_notebook_instance",
        "aws_sagemaker_domain",
        "aws_sagemaker_user_profile",
        "aws_sagemaker_space",
        "aws_sagemaker_app",
        "aws_sagemaker_pipeline",
        "aws_instance",
    ):
        assert f'"{forbidden}"' not in tf, forbidden


def test_permissions_boundary_variable_is_wired_to_the_role():
    tf = _tf()
    assert 'variable "permissions_boundary_arn"' in tf
    role = re.search(r'resource "aws_iam_role" "execution" \{.*?\n\}', tf, re.S).group(0)
    assert "permissions_boundary = var.permissions_boundary_arn" in role


def test_trust_policy_is_sagemaker_only_with_confused_deputy_guards():
    tf = _tf()
    trust = re.search(r'data "aws_iam_policy_document" "trust" \{.*?\n\}', tf, re.S).group(0)
    assert "sagemaker.amazonaws.com" in trust
    assert "aws:SourceAccount" in trust and "aws:SourceArn" in trust
    assert '"*"' not in trust.replace("job/*", "")


def test_only_two_documented_wildcard_resources_in_the_role_policy():
    tf = _tf()
    pol = re.search(
        r'data "aws_iam_policy_document" "execution" \{.*\n\}\n\nresource', tf, re.S
    ).group(0)
    stars = [m.start() for m in re.finditer(r'resources\s*=\s*\["\*"\]', pol)]
    assert len(stars) == 2
    for s in stars:
        assert "no resource-level permissions" in pol[s : s + 120]


def test_gold_is_read_only_except_the_scores_prefix_and_kms_is_scoped():
    tf = _tf()
    pol = re.search(
        r'data "aws_iam_policy_document" "execution" \{.*\n\}\n\nresource', tf, re.S
    ).group(0)
    assert "var.scores_prefix" in pol
    puts = re.findall(r"actions\s*=\s*\[([^\]]*s3:PutObject[^\]]*)\]", pol)
    assert len(puts) == 2  # scores in gold, prefixes in the ML bucket
    assert "s3:DeleteObject" not in pol and "s3:*" not in pol and '"*:*"' not in pol
    assert re.search(r"kms:Decrypt.*?resources\s*=\s*\[var\.kms_key_arn\]", pol, re.S)


def test_bucket_is_private_encrypted_versioned_tls_only():
    tf = _tf()
    for needle in (
        "block_public_acls       = true",
        "restrict_public_buckets = true",
        'sse_algorithm     = "aws:kms"',
        'status = "Enabled"',
        "aws:SecureTransport",
        "BucketOwnerEnforced",
    ):
        assert needle in tf, needle


def test_model_package_group_and_ecr_are_optional_and_no_secrets():
    tf = _tf()
    assert re.search(r"count\s*=\s*var\.create_model_package_group \? 1 : 0", tf)
    assert 'image_tag_mutability = "IMMUTABLE"' in tf and "scan_on_push = true" in tf
    assert not re.search(r"AKIA[0-9A-Z]{16}", tf)
