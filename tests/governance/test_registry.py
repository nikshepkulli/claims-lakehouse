"""The classification registry must cover every gold/mart column, and stay valid."""

import re
from pathlib import Path

import pytest
import yaml
from governance_helpers import ROOT

from lakehouse.governance import registry, views

DDL = ROOT / "warehouse" / "snowflake" / "01_gold_ddl.sql"
MARTS_YML = ROOT / "dbt" / "models" / "marts" / "_marts__models.yml"


def ddl_columns() -> dict[str, list[str]]:
    sql = re.sub(r"--[^\n]*", "", DDL.read_text())
    tables = {}
    for m in re.finditer(r"CREATE TABLE IF NOT EXISTS (\w+) \((.*?)\n\)\s*COMMENT", sql, re.S):
        cols = []
        for raw in re.split(r",\s*\n", m.group(2)):
            line = raw.strip()
            if line and not line.upper().startswith("CONSTRAINT"):
                cols.append(line.split()[0].lower())
        tables[m.group(1).lower()] = cols
    return tables


def mart_columns() -> dict[str, list[str]]:
    models = yaml.safe_load(MARTS_YML.read_text())["models"]
    return {m["name"]: [c["name"] for c in m["columns"]] for m in models}


@pytest.fixture(scope="module")
def reg():
    return registry.load()


def test_registry_loads_and_is_valid(reg):
    assert reg
    assert {t.layer for t in reg.values()} == {"gold", "mart"}


def test_every_ddl_column_is_classified_and_nothing_extra(reg):
    expected = ddl_columns()
    assert expected, "DDL parser found no tables"
    gold = {n: list(t.columns) for n, t in reg.items() if t.layer == "gold"}
    assert gold == expected  # same tables, same columns, same order


def test_every_dbt_mart_column_is_classified_and_nothing_extra(reg):
    expected = mart_columns()
    assert expected, "no marts found in the dbt YAML"
    marts = {n: list(t.columns) for n, t in reg.items() if t.layer == "mart"}
    assert marts == expected


def test_unclassified_column_is_detected_by_the_view_builder(reg):
    """The guard that makes a new column fail loudly rather than pass through unmasked."""
    table = reg["dim_member"]
    types = {c: "VARCHAR" for c in table.columns} | {"ssn_new_column": "VARCHAR"}
    with pytest.raises(KeyError, match="ssn_new_column"):
        views.analyst_view_sql(table, types)


def test_identifier_columns_are_never_shown_raw(reg):
    for t in reg.values():
        for c in t.columns.values():
            if c.identifier == "direct":
                assert c.handling in {"hash", "mask", "drop"}, f"{t.name}.{c.name}"
            if c.name in {"member_id", "provider_id"}:
                assert c.handling == "hash", f"{t.name}.{c.name}"


def test_sensitive_columns_left_as_is_carry_a_justification(reg):
    for t in reg.values():
        for c in t.columns.values():
            if c.sensitive and c.handling == "none":
                assert len(c.justification) > 20, f"{t.name}.{c.name}"


def test_birth_year_and_dates_of_birth_like_columns_are_generalised(reg):
    for t, c in [
        ("dim_member", "birth_year"),
        ("dim_member", "enrollment_date"),
        ("mart_member_risk", "birth_year"),
        ("mart_member_risk", "age_years"),
        ("mart_member_risk", "enrollment_date"),
        ("mart_member_risk", "last_service_date"),
    ]:
        assert reg[t].columns[c].handling == "generalise", f"{t}.{c}"


def test_record_hash_of_member_attributes_is_dropped(reg):
    """md5 over (birth_year, sex, state, plan_type) is brute-forceable back to birth_year."""
    assert reg["dim_member"].columns["record_hash"].handling == "drop"


@pytest.mark.parametrize(
    "bad, msg",
    [
        ({"classification": "SECRET", "identifier": "none", "handling": "none"}, "classification"),
        ({"classification": "PHI_LIKE", "identifier": "direct", "handling": "none"}, "direct"),
        (
            {"classification": "PHI_LIKE", "identifier": "quasi", "handling": "none"},
            "justification",
        ),
        ({"classification": "PUBLIC", "identifier": "quasi", "handling": "none"}, "PUBLIC"),
        ({"classification": "INTERNAL", "identifier": "none", "handling": "encrypt"}, "handling"),
    ],
)
def test_validator_rejects_bad_entries(tmp_path: Path, bad, msg):
    p = tmp_path / "r.yaml"
    p.write_text(
        yaml.safe_dump({"version": 1, "tables": {"t": {"layer": "gold", "columns": {"c": bad}}}})
    )
    with pytest.raises(registry.RegistryError, match=msg):
        registry.load(p)
