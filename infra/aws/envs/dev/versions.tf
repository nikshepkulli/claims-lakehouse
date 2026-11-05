terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.40"
    }
  }

  # Local state by default so this env needs no credentials to init/validate.
  # For shared use, enable a remote backend (values come from -backend-config
  # or env, never committed):
  #
  # backend "s3" {
  #   bucket         = "my-tfstate-bucket"
  #   key            = "claims-lakehouse/dev/terraform.tfstate"
  #   region         = "us-east-1"
  #   dynamodb_table = "my-tfstate-lock"
  #   encrypt        = true
  # }
  backend "local" {}
}
