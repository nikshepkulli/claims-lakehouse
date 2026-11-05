provider "aws" {
  region = var.region

  default_tags {
    tags = {
      Repository = "nikshepkulli/claims-lakehouse"
    }
  }
}

module "lakehouse_storage" {
  source = "../../modules/lakehouse_storage"

  name_prefix   = var.name_prefix
  environment   = "dev"
  force_destroy = true # dev only: allow teardown of non-empty buckets

  loader_trusted_principal_arns      = var.trusted_principal_arns
  transformer_trusted_principal_arns = var.trusted_principal_arns
  analyst_trusted_principal_arns     = var.trusted_principal_arns

  key_admin_arns           = var.key_admin_arns
  gold_object_lock_enabled = var.gold_object_lock_enabled
}
