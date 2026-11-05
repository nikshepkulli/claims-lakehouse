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
}
