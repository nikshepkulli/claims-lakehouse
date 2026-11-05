# AWS infrastructure: lakehouse storage

Terraform module `infra/aws/modules/lakehouse_storage`, consumed by
`infra/aws/envs/dev`. Nothing here is applied by CI; there are no AWS
credentials in the repo.

## Design

```
loader ──put──> bronze ──read──> transformer ──rw──> silver ──> gold <──read── analyst
                  \_______________ SSE-KMS (one CMK) _______________/
all buckets ──server access logs──> access-log bucket (SSE-S3)
```

- Three layer buckets, `<prefix>-<layer>-<account_id>` (account id keeps names
  globally unique without random suffixes).
- One customer-managed KMS key with rotation. The key policy only delegates to
  IAM, so grants live in role policies, not scattered key-policy statements.
- Three roles, each scoped to exactly the layers and actions it needs.

## Security decisions mapped to controls

| Decision | Control (generic framework mapping) |
|---|---|
| SSE-KMS with CMK + rotation, bucket keys on | Encryption at rest, key management (HIPAA 164.312(a)(2)(iv), CIS AWS 2.1.1 / 3.8) |
| Block Public Access on all 4 buckets, BucketOwnerEnforced (ACLs off) | Prevent public exposure (CIS 2.1.4/2.1.5) |
| Bucket policy denies `aws:SecureTransport=false` | Encryption in transit (CIS 2.1.2) |
| Bucket policy denies PutObject with a different KMS key id | Prevents bypass of the CMK |
| Versioning + noncurrent expiry (90d) | Recoverability / tamper resistance with bounded cost |
| Server access logging to dedicated bucket | Auditability (CIS 2.1.x logging); log bucket is SSE-S3 because S3 log delivery cannot write to KMS-encrypted targets |
| Roles: loader write-only to bronze, no delete; analyst gold read + decrypt only | Least privilege, separation of duties |
| No wildcard resources in role policies; no IAM users or access keys | Least privilege, no long-lived credentials |
| Trust lists are REQUIRED, validated (non-empty, IAM user/role ARNs only), no account-root fallback | Fail closed: nobody can assume a data role unless explicitly named |
| Optional `key_admin_arns` get an explicit key-administration statement (no Encrypt/Decrypt) | Separates key administration from key use; use stays in role policies |
| Optional S3 Object Lock on gold (`gold_object_lock_enabled`, default off) | WORM hardening for regulated data |
| `force_destroy` default false (true only in dev env) | Accidental-deletion protection |

Not covered (out of scope, noted deliberately): CloudTrail data events,
cross-region replication, VPC endpoints/endpoint policies,
Macie. All are reasonable next increments for real PHI.

## Trusted principals (fail closed)

`loader_`, `transformer_` and `analyst_trusted_principal_arns` have no default.
Each must be a non-empty list of IAM **user or role** ARNs; the account root
and `arn:aws:sts::...:assumed-role/...` session ARNs are rejected by validation.
A session ARN is not a valid trust principal; use the underlying role ARN
(`arn:aws:iam::<acct>:role/<name>`; for IAM Identity Center include the path,
`role/aws-reserved/sso.amazonaws.com/<region>/AWSReservedSSO_<name>_<hash>`).
`envs/dev` takes one required variable, `trusted_principal_arns`, used for all
three roles. Find yours with `aws sts get-caller-identity` then
`aws iam get-role --role-name <role> --query Role.Arn`. Note an IAM principal
also needs its own `sts:AssumeRole` permission; the trust policy is necessary,
not sufficient.

Plan example (dummy ARNs):

```bash
cd infra/aws/envs/dev
terraform plan -var 'trusted_principal_arns=["arn:aws:iam::111122223333:role/dev-engineer"]' \
               -var 'key_admin_arns=["arn:aws:iam::111122223333:role/security-admin"]'
# omitting trusted_principal_arns fails: variable is required
# passing arn:aws:iam::111122223333:root or an sts assumed-role ARN fails validation
```

## KMS administration vs use

The key policy keeps the account-root delegation to IAM (removing it risks an
unmanageable key). `key_admin_arns` (optional) adds an explicit administration
statement (create/update/rotate/schedule deletion, no data-key or Encrypt/Decrypt
actions). Data use is granted only by the role policies.

## Logging and Object Lock notes

- The access-log bucket does not log to itself: logging the log bucket is
  circular. It carries `# checkov:skip=CKV_AWS_18` with this reason; use
  CloudTrail data events if access to the logs themselves must be audited.
- `gold_object_lock_enabled` (default false) creates the gold bucket with Object
  Lock and a default retention (`gold_object_lock_mode`, `..._retention_days`).
  Object Lock can only be set at bucket creation, so enabling it on an existing
  gold bucket forces replacement. COMPLIANCE mode cannot be shortened by anyone,
  including root; start with GOVERNANCE. Note `force_destroy` cannot delete
  locked versions.

## Cost notes

- Bucket keys cut KMS request cost sharply for many small objects.
- Bronze moves to INTELLIGENT_TIERING (no retrieval fees, small per-object
  monitoring fee; objects under 128 KB are never tiered, so compact files).
- One CMK is about $1/month plus requests; sharing it across layers keeps that flat.
- Noncurrent versions and logs expire (90d / 365d) so versioning does not grow unbounded.

## How to plan

```bash
cd infra/aws/envs/dev
terraform init -backend=false          # no credentials needed
terraform fmt -check -recursive ..
terraform validate
# with credentials (not done in CI):
terraform init && terraform plan -out=tfplan
```

The default backend is local. A commented S3 backend block is in
`envs/dev/versions.tf`; supply values via `-backend-config`, never commit them.
`*.tfstate*`, `.terraform/` and `*.tfvars` are git-ignored.

## Tests

Terraform 1.5 (the pinned minimum) has no `terraform test`/mock providers, and
CI has no AWS credentials, so tests are:

1. `terraform fmt -check` and `terraform validate` on the env and module.
2. `bash infra/aws/tests/static_checks.sh`, asserting the security invariants
   (rotation, SSE-KMS, public access block, TLS deny, logging, tiering,
   scoped role resources, no root-principal fallback, trusted principals required).
3. Reviewers run `terraform plan` locally with credentials and attach the output.

Follow-ups: tflint and checkov in CI (acceptance criteria on #18), and
switching to native `terraform test` with `mock_provider` once the minimum
version moves to 1.7+.
