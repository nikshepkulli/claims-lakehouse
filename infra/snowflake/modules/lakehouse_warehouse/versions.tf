terraform {
  required_version = ">= 1.5.0"

  required_providers {
    snowflake = {
      # Published as snowflakedb/snowflake (formerly Snowflake-Labs/snowflake).
      source  = "snowflakedb/snowflake"
      version = "~> 2.22"
    }
  }
}
