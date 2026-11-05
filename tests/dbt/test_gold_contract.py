"""The dbt gold sources, the fixture generator and the Snowflake DDL must agree (ticket #15).

These tests need only PyYAML, so they run in the plain pytest job without dbt or DuckDB.
Nothing here proves anything about a live Snowflake account.
"""

import re

import pytest
import yaml
from dbt_fixture_loader import ROOT, load_fixture_module

DDL = ROOT / "warehouse" / "snowflake" / "01_gold_ddl.sql"
DBT = ROOT / "dbt"
SOURCES = DBT / "models" / "staging" / "gold" / "_gold__sources.yml"

fixture = load_fixture_module()


def normalise_type(sf_type: str) -> str:
    t = re.sub(r"\s+", "", sf_type.upper())
    t = re.sub(r"^VARCHAR\(\d+\)$", "VARCHAR", t)
    t = t.replace("NUMBER(", "DECIMAL(")
    return {"TIMESTAMP_NTZ": "TIMESTAMP"}.get(t, t)


def parse_ddl() -> dict[str, list[tuple[str, str, bool]]]:
    """table -> [(column, normalised type, not_null)] in declaration order."""
    sql = re.sub(r"--[^\n]*", "", DDL.read_text())
    tables = {}
    for m in re.finditer(r"CREATE TABLE IF NOT EXISTS (\w+) \((.*?)\n\)\s*COMMENT", sql, re.S):
        cols = []
        for raw in re.split(r",\s*\n", m.group(2)):
            line = raw.strip()
            if not line or line.upper().startswith("CONSTRAINT"):
                continue
            cm = re.match(r"(\w+)\s+([A-Z_]+(?:\s*\(\s*\d+(?:\s*,\s*\d+)?\s*\))?)(.*)$", line, re.S)
            assert cm, f"cannot parse DDL column: {line!r}"
            cols.append(
                (
                    cm.group(1).lower(),
                    normalise_type(cm.group(2)),
                    "NOT NULL" in cm.group(3).upper(),
                )
            )
        tables[m.group(1).lower()] = cols
    return tables


DDL_TABLES = parse_ddl()


def test_ddl_parser_found_the_six_gold_tables():
    assert set(DDL_TABLES) == {
        "dim_date",
        "dim_member",
        "dim_provider",
        "dim_diagnosis",
        "fact_claim",
        "fact_claim_line",
    }
    assert ("total_paid_amount", "DECIMAL(12,2)", True) in DDL_TABLES["fact_claim"]


def test_fixture_schema_equals_snowflake_ddl():
    """Names, order, types and NOT NULL of GOLD_SCHEMA must equal the DDL exactly."""
    ours = {
        t: [(c, normalise_type(ty), nn) for c, ty, nn in cols]
        for t, cols in fixture.GOLD_SCHEMA.items()
    }
    assert ours == DDL_TABLES


def load_sources():
    doc = yaml.safe_load(SOURCES.read_text())
    (src,) = doc["sources"]
    return src


def test_dbt_source_columns_equal_ddl_columns():
    src = load_sources()
    assert src["name"] == "gold"
    by_table = {t["name"]: [c["name"] for c in t["columns"]] for t in src["tables"]}
    assert set(by_table) == set(DDL_TABLES)
    for table, cols in by_table.items():
        assert cols == [c for c, _, _ in DDL_TABLES[table]], table


def test_every_source_table_has_a_staging_model():
    for table in DDL_TABLES:
        sql = (DBT / "models" / "staging" / "gold" / f"stg_gold__{table}.sql").read_text()
        assert f"source('gold', '{table}')" in sql


def test_external_location_pattern_matches_fixture_layout():
    """Source reads <GOLD_PATH>/<table>/*.parquet, which is what the fixture writes."""
    loc = load_sources()["config"]["meta"]["external_location"]
    assert loc.endswith("/{name}/*.parquet")
    assert "GOLD_PATH" in loc


MODEL_YMLS = sorted((DBT / "models").rglob("*.yml"))


def test_all_models_sources_and_columns_are_documented():
    missing = []
    for path in MODEL_YMLS:
        doc = yaml.safe_load(path.read_text())
        items = list(doc.get("models", []))
        for s in doc.get("sources", []):
            items.append(s)
            items.extend(s["tables"])
        for item in items:
            if not item.get("description"):
                missing.append(f"{path.name}:{item['name']}")
            for col in item.get("columns", []):
                if not col.get("description"):
                    missing.append(f"{path.name}:{item['name']}.{col['name']}")
    assert not missing, missing


def test_mart_contracts_are_enforced_and_fully_typed():
    project = yaml.safe_load((DBT / "dbt_project.yml").read_text())
    assert project["models"]["claims_lakehouse"]["marts"]["+contract"]["enforced"] is True
    doc = yaml.safe_load((DBT / "models" / "marts" / "_marts__models.yml").read_text())
    names = {m["name"] for m in doc["models"]}
    assert {
        "mart_claims_monthly",
        "mart_provider_performance",
        "mart_member_risk",
        "mart_denials",
    } <= names
    for model in doc["models"]:
        for col in model["columns"]:
            assert col.get("data_type"), f"{model['name']}.{col['name']} lacks data_type"
        # grain is documented in the description
        assert "grain" in model["description"].lower()


def test_marts_have_unique_grain_tests():
    text = (DBT / "models" / "marts" / "_marts__models.yml").read_text()
    assert text.count("unique") >= 4


def test_profiles_example_has_both_targets_and_no_literal_secrets():
    prof = yaml.safe_load((DBT / "profiles.yml.example").read_text())["claims_lakehouse"]
    assert set(prof["outputs"]) == {"duckdb", "snowflake"}
    sf = prof["outputs"]["snowflake"]
    for key in ("account", "user", "password"):
        assert re.fullmatch(r"\{\{ env_var\('SNOWFLAKE_\w+'\) \}\}", sf[key]), key
    for key, value in sf.items():
        if isinstance(value, str) and key not in {"type", "schema"}:
            assert "env_var(" in value, f"{key} must come from the environment"


@pytest.mark.parametrize("name", ["profiles.yml", "dbt_packages", "target"])
def test_local_dbt_artifacts_are_gitignored(name):
    ignored = (ROOT / ".gitignore").read_text()
    assert name in ignored or f"dbt/{name}" in ignored
