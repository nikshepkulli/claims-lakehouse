# lakehouse_storage

Secure S3 foundation for the claims lakehouse: bronze/silver/gold buckets, one
customer-managed KMS key, an access-log bucket, and three least-privilege
IAM roles.

## What it creates

| Resource | Notes |
|---|---|
| 3 x S3 buckets (`<prefix>-<layer>-<account_id>`) | SSE-KMS (CMK, bucket keys on), versioning, block public access, BucketOwnerEnforced, TLS-only + KMS-key-enforcing policy, server access logging |
| Access-log bucket | SSE-S3 (S3 log delivery cannot write to KMS buckets), versioned, public access blocked, TLS-only, log expiry |
| KMS key + alias | Rotation enabled, 30-day deletion window by default |
| Lifecycle | Noncurrent version expiry on all buckets; bronze to INTELLIGENT_TIERING; multipart cleanup |
| IAM roles | `loader` (write bronze), `transformer` (read bronze, rw silver/gold), `analyst` (read gold only) |

## Usage

```hcl
module "lakehouse_storage" {
  source      = "../../modules/lakehouse_storage"
  name_prefix = "claims-lakehouse-dev"
  environment = "dev"

  transformer_trusted_principal_arns = ["arn:aws:iam::111122223333:role/dbt-runner"]
}
```

## Inputs

See `variables.tf`. Required: `name_prefix`. Trust lists default to the account
root, so tighten them before real use.

## Outputs

`bucket_names`, `bucket_arns`, `access_log_bucket_name`, `kms_key_arn`,
`kms_key_alias`, `role_arns`.

## Requirements

Terraform >= 1.5, AWS provider ~> 5.40. See `docs/infra-aws.md` for design
rationale, control mapping and cost notes.
