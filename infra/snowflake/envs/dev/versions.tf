terraform {
  required_version = ">= 1.5.0"

  required_providers {
    snowflake = {
      # Published as snowflakedb/snowflake (formerly Snowflake-Labs/snowflake).
      source  = "snowflakedb/snowflake"
      version = "~> 2.22"
    }
  }

  # Local state by default so this env needs no credentials to init/validate.
  # For shared use, enable a remote backend (values come from -backend-config
  # or env, never committed). State holds no secrets for this stack, but it
  # does contain account object names, so keep it private.
  backend "local" {}
}
