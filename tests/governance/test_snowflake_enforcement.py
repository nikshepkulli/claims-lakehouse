"""Offline structural checks of the Snowflake privacy code (ticket #25).

NOTHING HERE EXECUTES SQL ON SNOWFLAKE. These tests guard what can be checked without an
account: that the policies exist, cover every handling the registry uses, grant raw access
only to the intended role, that the generated tag scripts match the registry, and that the
Terraform mirror has the same policy bodies as the SQL.
"""

import re

import pytest
from governance_helpers import ROOT
from test_registry import ddl_columns

from lakehouse.governance import registry, snowflake_sql

SQL = ROOT / "warehouse" / "snowflake"
TF = ROOT / "infra" / "snowflake" / "modules" / "lakehouse_warehouse" / "privacy.tf"


def strip(sql: str) -> str:
    return re.sub(r"--[^\n]*", "", sql)


def policies(sql: str) -> dict[str, str]:
    """name -> body expression (between -> and COMMENT) of each CREATE MASKING POLICY."""
    out = {}
    for m in re.finditer(
        r"CREATE MASKING POLICY IF NOT EXISTS (\w+) AS \((.*?)\) RETURNS (\w+) ->\s*(.*?)\nCOMMENT",
        strip(sql),
        re.S,
    ):
        out[m.group(1).lower()] = m.group(4).strip()
    return out


@pytest.fixture(scope="module")
def sql90() -> str:
    return (SQL / "90_masking_policies.sql").read_text()


def test_three_typed_policies_bound_to_the_handling_tag(sql90):
    body = strip(sql90)
    assert set(policies(sql90)) == {"mp_privacy_varchar", "mp_privacy_number", "mp_privacy_date"}
    assert re.search(r"CREATE TAG IF NOT EXISTS privacy_handling\b", body)
    assert re.search(r"CREATE TAG IF NOT EXISTS data_classification\b", body)
    for p in ("varchar", "number", "date"):
        assert f"ALTER TAG privacy_handling SET MASKING POLICY mp_privacy_{p} FORCE" in body


def test_raw_values_only_for_the_transformer_role(sql90):
    for name, expr in policies(sql90).items():
        roles = re.findall(r"IS_ROLE_IN_SESSION\('(\w+)'\)", expr)
        assert roles == ["CLAIMS_DEV_TRANSFORMER"], name
        first_when = re.search(r"WHEN (.*?) THEN val", expr).group(1)
        assert "TRANSFORMER" in first_when  # the privileged branch is the only one returning val
        assert (
            len(re.findall(r"THEN val$", expr, re.M)) == 1
        ), f"{name} returns raw val more than once"
        assert "ELSE" not in expr, f"{name}: no ELSE so unknown tag values fail closed to NULL"


def test_policies_cover_every_handling_the_registry_uses(sql90):
    reg = registry.load()
    used = {c.handling for t in reg.values() for c in t.columns.values()} - {"none"}
    pol = policies(sql90)
    body = {
        "hash": "= 'HASH' THEN SHA2(val, 256)",
        "mask": "= 'MASK' THEN",
        "generalise": "= 'GENERALISE' THEN",
    }
    for h in used - {"drop"}:  # drop = no matching WHEN, falls through to NULL
        assert any(body[h] in e for e in pol.values()), h
    assert "drop" in used  # record_hash: exercised through the fall-through branch


def test_sql_scripts_never_grant_and_never_leak_secrets(sql90):
    for name in ("90_masking_policies.sql", "93_row_access_example.sql"):
        sql = strip((SQL / name).read_text())
        assert not re.search(r"\bGRANT\b", sql, re.I)
        assert not re.search(r"PASSWORD|PRIVATE_KEY|SECRET", sql, re.I)


def test_generated_tag_scripts_match_the_registry():
    assert snowflake_sql.main(["--check"]) == 0


def test_every_tag_statement_targets_a_real_ddl_column():
    ddl = ddl_columns()
    text = (SQL / "91_apply_tags_gold.sql").read_text()
    stmts = re.findall(
        r"ALTER TABLE (\w+) MODIFY COLUMN (\w+) SET TAG secure\.(\w+) = '(\w+)';", text
    )
    assert stmts
    for table, col, tag, value in stmts:
        assert col in ddl[table], f"{table}.{col}"
        assert tag in {"data_classification", "privacy_handling"}
        assert value in {
            "PUBLIC",
            "INTERNAL",
            "CONFIDENTIAL",
            "PHI_LIKE",
            "HASH",
            "MASK",
            "GENERALISE",
            "DROP",
        }
    # every DDL column carries a classification tag
    tagged = {(t, c) for t, c, tag, _ in stmts if tag == "data_classification"}
    assert tagged == {(t, c) for t, cols in ddl.items() for c in cols}


def test_member_and_provider_ids_are_tagged_hash_in_gold_and_marts():
    gold = (SQL / "91_apply_tags_gold.sql").read_text()
    marts = (SQL / "92_apply_tags_marts.sql").read_text()
    for text, table, col in [
        (gold, "dim_member", "member_id"),
        (gold, "dim_provider", "provider_id"),
        (marts, "mart_member_risk", "member_id"),
        (marts, "mart_provider_performance", "provider_id"),
    ]:
        assert (
            f"ALTER TABLE {table} MODIFY COLUMN {col} SET TAG secure.privacy_handling = 'HASH';"
            in text
        )


def test_row_access_policy_example_fails_closed_and_is_opt_in():
    raw = (SQL / "93_row_access_example.sql").read_text()
    sql = strip(raw)
    assert "CREATE ROW ACCESS POLICY IF NOT EXISTS rap_state" in sql
    assert "state_entitlement" in sql and "IS_ROLE_IN_SESSION(e.role_name)" in sql
    assert "ADD ROW ACCESS POLICY rap_state ON (state)" in sql
    assert not re.search(r"\bINSERT\b", sql), "no default entitlements: empty table fails closed"
    assert "OPT-IN" in raw and "NOT EXECUTED" in raw
    assert "TRANSFORMER" in sql and "ANALYST" not in sql


def test_unexecuted_banner_present_on_every_new_script():
    for name in (
        "90_masking_policies.sql",
        "91_apply_tags_gold.sql",
        "92_apply_tags_marts.sql",
        "93_row_access_example.sql",
    ):
        assert "NOT EXECUTED" in (SQL / name).read_text(), name


def _tf_bodies() -> dict[str, str]:
    tf = TF.read_text()
    out = {}
    for m in re.finditer(r'name\s*=\s*"(MP_\w+)".*?body = <<-EOT\n(.*?)\n\s*EOT', tf, re.S):
        body = m.group(2)
        body = body.replace("${local.privacy_tag_ref}", "CLAIMS_DEV.SECURE.PRIVACY_HANDLING")
        body = body.replace("${local.role_prefix}", "CLAIMS_DEV")
        out[m.group(1).lower()] = body
    return out


def norm(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()


def test_terraform_mirror_has_the_same_policy_bodies(sql90):
    tf, sql = _tf_bodies(), policies(sql90)
    assert set(tf) == set(sql)
    for name in sql:
        assert norm(tf[name]) == norm(sql[name]), name


def test_terraform_mirror_is_opt_in_and_mutually_exclusive_with_sql():
    tf = TF.read_text()
    assert tf.count("count    = var.manage_privacy_policies ? 1 : 0") == 5
    variables = (TF.parent / "variables.tf").read_text()
    assert re.search(
        r'variable "manage_privacy_policies" \{.*?default\s*=\s*false', variables, re.S
    )
    assert "MUTUALLY EXCLUSIVE" in tf
