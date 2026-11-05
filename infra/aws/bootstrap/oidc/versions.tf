terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.40"
    }
  }

  # Bootstrap is applied ONCE, by a human with admin rights, from a laptop.
  # Local state is intentional (no chicken-and-egg with the CD backend).
  # Keep the state file private: it is not secret but describes your IAM.
  backend "local" {}
}
