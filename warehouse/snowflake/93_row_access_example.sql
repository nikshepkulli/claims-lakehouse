-- Row access policy EXAMPLE: restrict dimension rows by state (ticket #25).
-- ALL DATA IS SYNTHETIC. NOT EXECUTED against Snowflake (offline validation only).
--
-- OPT-IN, not part of the default apply order: attaching it changes what ANALYST sees and
-- fails CLOSED (an analyst with no entitlement row sees zero rows). Run as SYSADMIN after
-- 01_gold_ddl.sql and 90_masking_policies.sql.
--
-- Model: SECURE.STATE_ENTITLEMENT maps a role to the states it may see ('*' = all). The
-- policy admits TRANSFORMER (pipeline jobs need every row) and any role in the session that
-- has an entitlement for the row's state. Entitlements are data, so changing who sees what
-- is an INSERT/DELETE reviewed in a PR, not a policy rewrite. Policy subqueries run with the
-- policy owner's rights (SYSADMIN), so analysts need no privilege on the entitlement table
-- beyond the SELECT they already inherit on SECURE.
--
-- Only dim_member.state and dim_provider.state carry a state; facts have no state column.
-- Consequence to know about: hiding a dimension row makes joins from facts to it drop out, so
-- an entitled-to-NY analyst sees only NY-member claims. That is the intent of the example.
--
-- Not idempotent: ADD ROW ACCESS POLICY fails if the policy is already attached. To change
-- the policy, ALTER ROW ACCESS POLICY ... SET BODY; to detach, DROP ROW ACCESS POLICY.

USE SCHEMA secure;

CREATE TABLE IF NOT EXISTS state_entitlement (
    role_name VARCHAR(255) NOT NULL,
    state_code VARCHAR(2) NOT NULL,
    CONSTRAINT pk_state_entitlement PRIMARY KEY (role_name, state_code)
)
COMMENT = 'Role to state entitlements for rap_state. State code * means all states. Empty means analysts see no rows (fail closed).';

-- Example entitlements, commented out on purpose; add real ones through a reviewed PR.
-- INSERT INTO state_entitlement VALUES ('CLAIMS_DEV_ANALYST', 'NY');

-- Linter note: sqlfluff 4.5 parses CREATE ROW ACCESS POLICY now; only the layout and capitalisation rules below are suppressed for this statement.
-- RF03: row_state is the policy's own argument, it cannot be table-qualified.
-- noqa: disable=LT01,CP02,RF03
CREATE ROW ACCESS POLICY IF NOT EXISTS rap_state AS (row_state VARCHAR) RETURNS BOOLEAN ->
IS_ROLE_IN_SESSION('CLAIMS_DEV_TRANSFORMER')
OR EXISTS (
    SELECT 1
    FROM claims_dev.secure.state_entitlement AS e
    WHERE IS_ROLE_IN_SESSION(e.role_name) AND e.state_code IN ('*', row_state)
)
COMMENT = 'Region access: TRANSFORMER sees all rows, others only entitled states.';
-- noqa: enable=LT01,CP02,RF03

ALTER TABLE marts.dim_member ADD ROW ACCESS POLICY rap_state ON (state);
ALTER TABLE marts.dim_provider ADD ROW ACCESS POLICY rap_state ON (state);
