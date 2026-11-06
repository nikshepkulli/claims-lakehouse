# sagemaker_ml

Plumbing for the SageMaker-ready claim-denial-risk pipeline (ticket #27, [docs/ml.md](../../../../docs/ml.md)).
**Not applied to any AWS account by the author**: checked with `terraform fmt` and `validate` in CI
and by `tests/ml/test_ml_terraform.py` (static invariants). No endpoint, notebook instance, domain
or job is created, so nothing here bills while idle.

| Resource | Notes |
|---|---|
| S3 bucket `<prefix>-ml-<account_id>` | SSE-KMS (shared CMK), versioning, public access blocked, TLS-only; prefixes `models/`, `training-output/` (expires), `processing/` |
| IAM role `<prefix>-sagemaker-ml-execution` | Trust: `sagemaker.amazonaws.com` for this account's training/processing jobs only. Policy: read `<gold>/<features_prefix>`, write only `<gold>/<scores_prefix>` and the ML bucket prefixes, use the one KMS key, write job logs, pull from the one ECR repo. `permissions_boundary_arn` optional |
| ECR repository (optional, default on) | Immutable tags, scan on push, KMS |
| Model Package Group (optional, default off) | The registry container; versions are registered by the pipeline |

The two statements with `Resource = "*"` (`ecr:GetAuthorizationToken`, `cloudwatch:PutMetricData`)
exist because AWS offers no resource-level permission for them; the second is narrowed by a
namespace condition.

```hcl
module "sagemaker_ml" {
  source           = "../../modules/sagemaker_ml"
  name_prefix      = "claims-lakehouse-dev"
  kms_key_arn      = module.lakehouse_storage.kms_key_arn
  gold_bucket_name = module.lakehouse_storage.bucket_names["gold"]
  # permissions_boundary_arn = "arn:aws:iam::111122223333:policy/team-boundary"
}
```
