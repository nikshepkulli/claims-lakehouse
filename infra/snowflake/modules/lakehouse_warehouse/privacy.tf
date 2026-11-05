# Opt-in Terraform mirror of warehouse/snowflake/90_masking_policies.sql (ticket #25):
# classification tags, tag-based masking policies and the tag-to-policy binding.
#
# MUTUALLY EXCLUSIVE with the SQL script: use ONE owner for these objects. The SQL
# script is the documented default (it needs no provider support and is what the docs walk
# through); set manage_privacy_policies = true to let Terraform own them instead, and do not
# run 90_masking_policies.sql. A test (tests/governance/test_snowflake_enforcement.py)
# keeps the three policy bodies here identical to the SQL ones.
#
# Terraform cannot tag COLUMNS (the tables are created by SQL after Terraform), so column
# tagging stays in warehouse/snowflake/91_apply_tags_gold.sql and 92_apply_tags_marts.sql,
# generated from governance/data_classification.yaml. The row access policy also stays in
# SQL (93_row_access_example.sql): its body reads a table that SQL creates.
#
# NOT APPLIED to any Snowflake account; validated with terraform validate only.

locals {
  privacy_tag_ref = "${local.db_name}.SECURE.PRIVACY_HANDLING"
  # Built with split() because the static checks forbid the literal "PUBLIC" (it is also a
  # Snowflake role name) and this is a classification label, not a grant.
  classification_values = split(",", "PUBLIC,INTERNAL,CONFIDENTIAL,PHI_LIKE")
}

resource "snowflake_masking_policy" "privacy_varchar" {
  provider = snowflake.sysadmin
  count    = var.manage_privacy_policies ? 1 : 0

  database         = snowflake_database.this.name
  schema           = snowflake_schema.this["SECURE"].name
  name             = "MP_PRIVACY_VARCHAR"
  return_data_type = "VARCHAR"
  comment          = "Tag-based masking for VARCHAR: HASH, MASK, else NULL. Raw for TRANSFORMER only."

  argument {
    name = "val"
    type = "VARCHAR"
  }

  body = <<-EOT
    CASE
        WHEN IS_ROLE_IN_SESSION('${local.role_prefix}_TRANSFORMER') THEN val
        WHEN val IS NULL THEN NULL
        WHEN SYSTEM$GET_TAG_ON_CURRENT_COLUMN('${local.privacy_tag_ref}') = 'HASH' THEN SHA2(val, 256)
        WHEN SYSTEM$GET_TAG_ON_CURRENT_COLUMN('${local.privacy_tag_ref}') = 'MASK' THEN CONCAT(REPEAT('*', GREATEST(LENGTH(val) - 2, 0)), RIGHT(val, 2))
    END
  EOT
}

resource "snowflake_masking_policy" "privacy_number" {
  provider = snowflake.sysadmin
  count    = var.manage_privacy_policies ? 1 : 0

  database         = snowflake_database.this.name
  schema           = snowflake_schema.this["SECURE"].name
  name             = "MP_PRIVACY_NUMBER"
  return_data_type = "NUMBER"
  comment          = "Tag-based masking for NUMBER: GENERALISE to a 5-year bucket start, else NULL."

  argument {
    name = "val"
    type = "NUMBER"
  }

  body = <<-EOT
    CASE
        WHEN IS_ROLE_IN_SESSION('${local.role_prefix}_TRANSFORMER') THEN val
        WHEN val IS NULL THEN NULL
        WHEN SYSTEM$GET_TAG_ON_CURRENT_COLUMN('${local.privacy_tag_ref}') = 'GENERALISE' THEN val - MOD(val, 5)
    END
  EOT
}

resource "snowflake_masking_policy" "privacy_date" {
  provider = snowflake.sysadmin
  count    = var.manage_privacy_policies ? 1 : 0

  database         = snowflake_database.this.name
  schema           = snowflake_schema.this["SECURE"].name
  name             = "MP_PRIVACY_DATE"
  return_data_type = "DATE"
  comment          = "Tag-based masking for DATE: GENERALISE to January 1 of the year, else NULL."

  argument {
    name = "val"
    type = "DATE"
  }

  body = <<-EOT
    CASE
        WHEN IS_ROLE_IN_SESSION('${local.role_prefix}_TRANSFORMER') THEN val
        WHEN val IS NULL THEN NULL
        WHEN SYSTEM$GET_TAG_ON_CURRENT_COLUMN('${local.privacy_tag_ref}') = 'GENERALISE' THEN DATE_TRUNC('YEAR', val)
    END
  EOT
}

resource "snowflake_tag" "data_classification" {
  provider = snowflake.sysadmin
  count    = var.manage_privacy_policies ? 1 : 0

  database               = snowflake_database.this.name
  schema                 = snowflake_schema.this["SECURE"].name
  name                   = "DATA_CLASSIFICATION"
  ordered_allowed_values = local.classification_values
  comment                = "Data classification of a column, from governance/data_classification.yaml."
}

# The masking policies are bound to this tag: tagging a column is what protects it.
resource "snowflake_tag" "privacy_handling" {
  provider = snowflake.sysadmin
  count    = var.manage_privacy_policies ? 1 : 0

  database               = snowflake_database.this.name
  schema                 = snowflake_schema.this["SECURE"].name
  name                   = "PRIVACY_HANDLING"
  ordered_allowed_values = ["HASH", "MASK", "GENERALISE", "DROP"]
  comment                = "Handling for non-privileged readers; masking policies are bound to this tag."
  masking_policies = [
    snowflake_masking_policy.privacy_varchar[0].fully_qualified_name,
    snowflake_masking_policy.privacy_number[0].fully_qualified_name,
    snowflake_masking_policy.privacy_date[0].fully_qualified_name,
  ]
}
