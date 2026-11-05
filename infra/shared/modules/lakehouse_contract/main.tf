# Output-contract adapter. Creates no resources: it validates that a cloud storage module
# exposes the shape the portable core depends on, then re-emits it unchanged. See
# docs/multi-cloud.md and ADR-0003.

locals {
  scheme = var.cloud == "aws" ? "s3a" : "abfss"

  # Normalise to exactly {id, principal_id}: the common subset of both clouds.
  principals = {
    for persona, p in var.principals : persona => {
      id           = p.id
      principal_id = p.principal_id
    }
  }
}
