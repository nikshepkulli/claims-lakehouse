-- Tag-based masking: classification tags and masking policies (ticket #25).
-- ALL DATA IS SYNTHETIC, but the controls are written as if it were not.
--
-- NOT EXECUTED against Snowflake. There is no account attached to this repo; this file is
-- validated offline only (sqlfluff syntax parse, structural tests in tests/governance and
-- tests/snowflake). See docs/privacy-and-access.md for the apply order and known risks.
--
-- Run as: SYSADMIN, database selected by the connection, after 01_gold_ddl.sql, e.g.
--   snow sql --database CLAIMS_DEV --role SYSADMIN -f 90_masking_policies.sql
-- Then 91_apply_tags_gold.sql, (dbt build), 92_apply_tags_marts.sql, optionally 93_*.
-- Idempotent: IF NOT EXISTS plus ALTER TAG ... SET MASKING POLICY ... FORCE. To CHANGE a
-- policy body use ALTER MASKING POLICY ... SET BODY (IF NOT EXISTS never updates a body).
-- No GRANTs here (grants are Terraform). Role and database names below are the DEV names
-- (CLAIMS_DEV); the Terraform mirror in infra/snowflake/.../privacy.tf is parameterised and
-- a test keeps the two policy bodies identical.
--
-- Design:
--   * Two tags. DATA_CLASSIFICATION (PUBLIC..PHI_LIKE) is for discovery and audit only.
--     PRIVACY_HANDLING (HASH, MASK, GENERALISE, DROP) carries the masking policies, so
--     tagging a column is what protects it, with no per-column policy wiring.
--   * One policy per data type, bound to the tag. The body branches on the column's tag
--     value, so a new HASH column is protected the moment it is tagged.
--   * Raw values only for the TRANSFORMER role, because its MERGE/dbt jobs must compare and
--     rebuild identifiers. LOADER cannot read MARTS at all; ANALYST and everyone else get the
--     masked value. A break-glass PII_READER role is designed, not built (see the docs).
--   * Fail closed: there is no ELSE branch, so an unknown tag value, or DROP, returns NULL.
--   * HASH is SHA-256 WITHOUT a key, so it is joinable but can be reversed by enumerating
--     sequential ids. Keyed pseudonymisation happens before load (lakehouse.governance).

USE SCHEMA secure;

CREATE TAG IF NOT EXISTS data_classification ALLOWED_VALUES 'PUBLIC', 'INTERNAL', 'CONFIDENTIAL', 'PHI_LIKE'
COMMENT = 'Data classification of a column, from governance/data_classification.yaml.';

CREATE TAG IF NOT EXISTS privacy_handling ALLOWED_VALUES 'HASH', 'MASK', 'GENERALISE', 'DROP'
COMMENT = 'Handling for non-privileged readers; masking policies are bound to this tag.';

-- Linter note: sqlfluff 3.3 cannot parse the SYSTEM$GET_TAG_ON_CURRENT_COLUMN function name and
-- wants `AS(` and lower-case types in policy signatures, which are valid but off-style. The
-- three policy statements below are therefore excluded from parse/layout/capitalisation rules
-- (the rest of the file is linted normally); structure is covered by tests/governance instead.
-- noqa: disable=PRS,LT01,LT02,CP02

CREATE MASKING POLICY IF NOT EXISTS mp_privacy_varchar AS (val VARCHAR) RETURNS VARCHAR ->
CASE
    WHEN IS_ROLE_IN_SESSION('CLAIMS_DEV_TRANSFORMER') THEN val
    WHEN val IS NULL THEN NULL
    WHEN SYSTEM$GET_TAG_ON_CURRENT_COLUMN('CLAIMS_DEV.SECURE.PRIVACY_HANDLING') = 'HASH' THEN SHA2(val, 256)
    WHEN SYSTEM$GET_TAG_ON_CURRENT_COLUMN('CLAIMS_DEV.SECURE.PRIVACY_HANDLING') = 'MASK' THEN CONCAT(REPEAT('*', GREATEST(LENGTH(val) - 2, 0)), RIGHT(val, 2))
END
COMMENT = 'Tag-based masking for VARCHAR: HASH, MASK, else NULL. Raw for TRANSFORMER only.';

CREATE MASKING POLICY IF NOT EXISTS mp_privacy_number AS (val NUMBER) RETURNS NUMBER ->
CASE
    WHEN IS_ROLE_IN_SESSION('CLAIMS_DEV_TRANSFORMER') THEN val
    WHEN val IS NULL THEN NULL
    WHEN SYSTEM$GET_TAG_ON_CURRENT_COLUMN('CLAIMS_DEV.SECURE.PRIVACY_HANDLING') = 'GENERALISE' THEN val - MOD(val, 5)
END
COMMENT = 'Tag-based masking for NUMBER: GENERALISE to a 5-year bucket start, else NULL.';

CREATE MASKING POLICY IF NOT EXISTS mp_privacy_date AS (val DATE) RETURNS DATE ->
CASE
    WHEN IS_ROLE_IN_SESSION('CLAIMS_DEV_TRANSFORMER') THEN val
    WHEN val IS NULL THEN NULL
    WHEN SYSTEM$GET_TAG_ON_CURRENT_COLUMN('CLAIMS_DEV.SECURE.PRIVACY_HANDLING') = 'GENERALISE' THEN DATE_TRUNC('YEAR', val)
END
COMMENT = 'Tag-based masking for DATE: GENERALISE to January 1 of the year, else NULL.';

-- noqa: enable=PRS,LT01,LT02,CP02

ALTER TAG privacy_handling SET MASKING POLICY mp_privacy_varchar FORCE;
ALTER TAG privacy_handling SET MASKING POLICY mp_privacy_number FORCE;
ALTER TAG privacy_handling SET MASKING POLICY mp_privacy_date FORCE;
