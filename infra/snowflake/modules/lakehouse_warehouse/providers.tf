# One provider alias per Snowflake system role, the documented best practice:
#   SYSADMIN      owns data objects (database, schemas, file formats, stages)
#   SECURITYADMIN owns roles and grants
#   ACCOUNTADMIN  only for objects that require it (resource monitors, the
#                 warehouses attached to them, storage integrations)
#
# These blocks live in the module (not passed in) so the module validates on
# its own in CI. The module is consumed by a single environment root.
#
# Credentials are NEVER in code. Set, in the shell or CI secret store:
#   SNOWFLAKE_ORGANIZATION_NAME, SNOWFLAKE_ACCOUNT_NAME, SNOWFLAKE_USER,
#   SNOWFLAKE_AUTHENTICATOR=SNOWFLAKE_JWT, SNOWFLAKE_PRIVATE_KEY
#   (PKCS8 key-pair auth; add SNOWFLAKE_PRIVATE_KEY_PASSPHRASE if encrypted).
# The user must be allowed to use all three roles.

provider "snowflake" {
  alias = "sysadmin"
  role  = "SYSADMIN"
}

provider "snowflake" {
  alias = "securityadmin"
  role  = "SECURITYADMIN"
}

provider "snowflake" {
  alias = "accountadmin"
  role  = "ACCOUNTADMIN"
}
