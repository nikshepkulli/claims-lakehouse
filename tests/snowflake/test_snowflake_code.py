"""Offline sanity checks for the Snowflake SQL and Terraform (ticket #14).

There is no Snowflake account in CI, so none of this proves the code runs on
Snowflake; it guards structural invariants (least privilege, documented tables,
idempotent loads, no secrets) that regressions would silently break.
"""

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SQL_DIR = ROOT / "warehouse" / "snowflake"
MODULE = ROOT / "infra" / "snowflake" / "modules" / "lakehouse_warehouse"
ENV = ROOT / "infra" / "snowflake" / "envs" / "dev"

GOLD_TABLES = {
    "dim_date",
    "dim_member",
    "dim_provider",
    "dim_diagnosis",
    "fact_claim",
    "fact_claim_line",
}
SCD2_COLUMNS = {"effective_from", "effective_to", "is_current"}

SQL_FILES = sorted(SQL_DIR.glob("*.sql"))
TF_FILES = sorted(MODULE.glob("*.tf")) + sorted(ENV.glob("*.tf"))


def strip_sql_comments(sql: str) -> str:
    return re.sub(r"--[^\n]*", "", sql)


def strip_tf_comments(tf: str) -> str:
    return re.sub(r"(?m)^\s*#.*$", "", tf)


def all_create_statements(sql: str) -> list[tuple[str, str]]:
    stmts = []
    for stmt in strip_sql_comments(sql).split(";"):
        m = re.search(
            r"CREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)", stmt, re.I
        )
        if m:
            stmts.append((m.group(1).lower(), stmt))
    return stmts


def test_sql_files_exist() -> None:
    names = {p.name for p in SQL_FILES}
    for expected in (
        "01_gold_ddl.sql",
        "02_raw_landing_ddl.sql",
        "20_copy_into.sql",
        "30_merge_dimensions.sql",
        "31_merge_facts.sql",
        "90_masking_policies.sql",
    ):
        assert expected in names


@pytest.mark.parametrize("path", SQL_FILES, ids=lambda p: p.name)
def test_no_public_grants_or_credentials(path: Path) -> None:
    sql = strip_sql_comments(path.read_text())
    assert not re.search(r"\bTO\s+(ROLE\s+)?PUBLIC\b", sql, re.I), "GRANT ... TO PUBLIC"
    assert not re.search(r"\bGRANT\b", sql, re.I), "grants belong in Terraform, not SQL scripts"
    assert not re.search(r"AWS_KEY_ID|AWS_SECRET_KEY|AWS_TOKEN|\bPASSWORD\b|PRIVATE_KEY", sql, re.I)
    assert not re.search(r"AKIA[0-9A-Z]{16}", sql)
    assert not re.search(r"https?://[^\s']*[?&](sig|token|X-Amz-Signature)=", sql, re.I)


@pytest.mark.parametrize("path", SQL_FILES, ids=lambda p: p.name)
def test_sql_scripts_are_not_destructive(path: Path) -> None:
    sql = strip_sql_comments(path.read_text())
    assert not re.search(r"\bDROP\s+(TABLE|SCHEMA|DATABASE)\b", sql, re.I)
    assert not re.search(r"\bCREATE\s+OR\s+REPLACE\s+TABLE\b", sql, re.I)
    assert not re.search(r"\bTRUNCATE\b", sql, re.I)


def test_every_table_is_idempotent_and_commented() -> None:
    tables = []
    for name in ("01_gold_ddl.sql", "02_raw_landing_ddl.sql"):
        body = strip_sql_comments((SQL_DIR / name).read_text())
        statements = [s for s in body.split(";") if re.search(r"CREATE\s+TABLE", s, re.I)]
        assert statements
        for stmt in statements:
            assert re.search(r"CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+\w+", stmt, re.I), stmt[:80]
            assert re.search(r"COMMENT\s*=\s*'[^']{10,}'", stmt), (
                f"table without COMMENT: {stmt[:80]}"
            )
            tables.append(stmt)
    assert len(tables) >= len(GOLD_TABLES) * 2  # gold + landing, plus load_audit


def test_gold_star_schema_tables_and_scd2() -> None:
    sql = strip_sql_comments((SQL_DIR / "01_gold_ddl.sql").read_text())
    found = dict(all_create_statements(sql))
    assert set(found) == GOLD_TABLES
    for dim in ("dim_member", "dim_provider"):
        for col in SCD2_COLUMNS:
            assert re.search(rf"\b{col}\b", found[dim]), f"{dim} lacks SCD2 column {col}"
    for fact in ("fact_claim", "fact_claim_line"):
        assert "load_batch_id" in found[fact] and "loaded_at" in found[fact]
        assert re.search(r"PRIMARY KEY", found[fact], re.I)
    for table, stmt in found.items():
        assert re.search(r"PRIMARY KEY", stmt, re.I), f"{table} has no (informational) PK"


def test_money_columns_are_decimal_not_float() -> None:
    sql = re.sub(r"'[^']*'", "''", strip_sql_comments((SQL_DIR / "01_gold_ddl.sql").read_text()))
    for m in re.finditer(r"(\w*amount\w*)\s+(\w+)", sql):
        assert m.group(2).upper() == "NUMBER", f"{m.group(1)} must be NUMBER(12, 2)"
    assert not re.search(r"\b(FLOAT|DOUBLE|REAL)\b", sql, re.I)


def test_landing_tables_carry_lineage_columns() -> None:
    sql = strip_sql_comments((SQL_DIR / "02_raw_landing_ddl.sql").read_text())
    for table in GOLD_TABLES:
        m = re.search(rf"landing_{table}\s*\((.*?)\)\s*COMMENT", sql, re.S)
        assert m, f"landing_{table} missing"
        for col in ("_source_file", "_batch_id", "_loaded_at"):
            assert col in m.group(1)
    assert re.search(r"CREATE TABLE IF NOT EXISTS load_audit", sql)


def test_copy_into_is_safe_and_audited() -> None:
    sql = strip_sql_comments((SQL_DIR / "20_copy_into.sql").read_text())
    copies = re.findall(r"COPY INTO landing_(\w+)", sql)
    assert set(copies) == GOLD_TABLES
    assert sql.count("ON_ERROR = ABORT_STATEMENT") == len(GOLD_TABLES)
    assert not re.search(r"FORCE\s*=\s*TRUE", sql, re.I), (
        "FORCE must be a deliberate, documented reload"
    )
    assert "INSERT INTO load_audit" in sql and "NOT EXISTS" in sql
    assert "copy_history" in sql.lower()
    assert "PURGE" not in sql.upper()


@pytest.mark.parametrize("name", ["30_merge_dimensions.sql", "31_merge_facts.sql"])
def test_merges_are_idempotent(name: str) -> None:
    sql = strip_sql_comments((SQL_DIR / name).read_text())
    merges = sql.count("MERGE INTO")
    assert merges >= 2
    # Deduplicate the source to the latest landed row per key ...
    assert sql.count("QUALIFY ROW_NUMBER()") == merges
    # ... and only touch matched rows whose content changed.
    assert sql.count("WHEN MATCHED AND HASH(") == merges
    assert sql.count("WHEN NOT MATCHED THEN") == merges
    assert not re.search(r"WHEN\s+MATCHED\s+THEN\s+DELETE", sql, re.I)


def test_masking_policies_reference_ticket_and_are_unexecuted() -> None:
    """The #14 placeholder became real policies in #25 (detailed checks: tests/governance)."""
    raw = (SQL_DIR / "90_masking_policies.sql").read_text()
    assert "#25" in raw
    assert "NOT EXECUTED" in raw
    assert re.search(r"CREATE\s+MASKING\s+POLICY", strip_sql_comments(raw), re.I)


def test_clustering_is_not_enabled_by_default() -> None:
    sql = strip_sql_comments((SQL_DIR / "50_clustering_experiments.sql").read_text())
    assert not re.search(r"\bALTER\s+TABLE\b", sql, re.I)
    for name in ("01_gold_ddl.sql", "02_raw_landing_ddl.sql"):
        assert not re.search(
            r"CLUSTER\s+BY", strip_sql_comments((SQL_DIR / name).read_text()), re.I
        )


# ---------------------------------------------------------------- Terraform


def tf_text(name: str) -> str:
    return strip_tf_comments((MODULE / name).read_text())


def test_terraform_files_present() -> None:
    assert len(TF_FILES) >= 10
    assert (ENV / ".terraform.lock.hcl").exists()


@pytest.mark.parametrize("path", TF_FILES, ids=lambda p: f"{p.parent.name}/{p.name}")
def test_terraform_has_no_secrets_or_public(path: Path) -> None:
    tf = strip_tf_comments(path.read_text())
    assert not re.search(r"(?i)\b(password|private_key|aws_secret_key|aws_key_id|token)\s*=", tf)
    assert not re.search(r"AKIA[0-9A-Z]{16}", tf)
    assert not re.search(r"-----BEGIN", tf)
    assert not re.search(r'"PUBLIC"', tf, re.I), "never grant to PUBLIC"
    assert "all_privileges" not in tf
    assert not re.search(r"with_grant_option\s*=\s*true", tf)
    assert not re.search(r'"(OWNERSHIP|MANAGE GRANTS|ALL PRIVILEGES)"', tf)
    assert not re.search(r"\baccount\s*=", tf), "account identifiers come from env vars"
    assert not re.search(r"organization_name\s*=|account_name\s*=", tf)


def test_provider_is_pinned_everywhere() -> None:
    for d in (MODULE, ENV):
        tf = strip_tf_comments((d / "versions.tf").read_text())
        assert re.search(r'source\s*=\s*"snowflakedb/snowflake"', tf)
        assert re.search(r'version\s*=\s*"~> 2\.\d+"', tf)


def parse_role_map(block: str) -> dict[str, list[str]]:
    m = re.search(rf"{block}\s*=\s*\{{(.*?)\n  \}}", tf_text("roles.tf"), re.S)
    assert m, block
    return {
        k: re.findall(r'"(\w+)"', v) for k, v in re.findall(r"(\w+)\s*=\s*\[(.*?)\]", m.group(1))
    }


def test_role_hierarchy_least_privilege() -> None:
    functional = parse_role_map("functional_roles")
    assert set(functional) == {"LOADER", "TRANSFORMER", "ANALYST"}
    # Analysts are read-only everywhere.
    assert functional["ANALYST"], "analyst needs at least one access role"
    assert all(r.endswith("_RO") for r in functional["ANALYST"])
    # Loader writes RAW only; transformer cannot write RAW.
    assert functional["LOADER"] == ["RAW_RW"]
    assert "RAW_RW" not in functional["TRANSFORMER"]
    assert "RAW_RO" in functional["TRANSFORMER"]
    # Analyst never reaches STAGING or RAW.
    assert not any(r.startswith(("RAW", "STAGING")) for r in functional["ANALYST"])


def test_functional_roles_roll_up_to_sysadmin_and_no_user_grants() -> None:
    tf = tf_text("roles.tf")
    assert "functional_to_sysadmin" in tf and "var.sysadmin_role_name" in tf
    assert "user_name" not in tf


def test_ro_access_roles_only_get_select_and_usage() -> None:
    tf = tf_text("grants.tf")
    assert re.search(r'ro\s*=\s*\["USAGE"\]', tf)
    assert re.search(r'ro\s*=\s*\["SELECT"\]', tf)
    assert not re.search(r"ro\s*=\s*\[[^\]]*(INSERT|UPDATE|DELETE|CREATE)", tf)
    for resource in ("future_tables", "future_views"):
        assert f'"{resource}"' in tf
    assert "future {" in tf, "table privileges must be FUTURE grants"


def test_warehouse_cost_guardrails() -> None:
    main = tf_text("main.tf")
    variables = tf_text("variables.tf")
    assert re.search(r"initially_suspended\s*=\s*true", main)
    assert "statement_timeout_in_seconds" in main
    assert re.search(r"suspend_trigger\s*=\s*100", main)
    assert re.search(r"suspend_immediate_trigger\s*=\s*110", main)
    assert re.search(r"notify_triggers\s*=\s*\[50, 75, 90\]", main)
    assert re.search(r'frequency\s*=\s*"MONTHLY"', main)
    assert "resource_monitor" in main
    # Loader auto-suspends at 60s by default; every warehouse <= 60s.
    sizes = re.findall(r"auto_suspend_seconds\s*=\s*(\d+)", variables)
    assert sizes and all(int(s) <= 60 for s in sizes)
    assert re.findall(r'size\s*=\s*"(\w+)"', variables) == ["XSMALL"] * 3


def test_schemas_are_the_required_four() -> None:
    main = tf_text("main.tf")
    for s in ("RAW", "STAGING", "MARTS", "SECURE"):
        assert re.search(rf"^\s*{s}\s*=\s*\"", main, re.M), s
    assert 'with_managed_access = "true"' in main


def test_external_stage_is_opt_in_and_integration_based() -> None:
    tf = tf_text("external_stage.tf")
    assert "snowflake_storage_integration_aws" in tf and "storage_integration" in tf
    assert "count    = var.enable_external_stage ? 1 : 0" in tf
    assert not re.search(r"credentials\s*\{", tf), "stage must use the integration, not keys"
    assert re.search(
        r"variable \"enable_external_stage\"[^}]*default\s*=\s*false", tf_text("variables.tf"), re.S
    )
