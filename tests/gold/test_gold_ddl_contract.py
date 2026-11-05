"""Contract: the in-code gold schema must equal the Snowflake DDL (plain pytest, no Spark).

``warehouse/snowflake/01_gold_ddl.sql`` is the source of truth that the Snowflake load and the
dbt sources are written against. If a column is added, retyped or made NULL/NOT NULL on one
side only, this fails. The Spark suite additionally checks that the Delta tables the job really
writes carry the same schema (tests/gold/test_gold_spark.py).
"""

import re

import pytest
from ddl import DDL, ddl_contract, to_spark, varchar_lengths

from lakehouse.gold import schema

CONTRACT = ddl_contract()


def test_ddl_parser_finds_the_six_gold_tables():
    assert set(CONTRACT) == {
        "dim_date",
        "dim_member",
        "dim_provider",
        "dim_diagnosis",
        "fact_claim",
        "fact_claim_line",
    }
    assert ("total_paid_amount", "decimal(12,2)", False) in CONTRACT["fact_claim"]
    assert ("effective_to", "timestamp_ntz", True) in CONTRACT["dim_member"]


def test_gold_schema_covers_exactly_the_ddl_tables():
    assert set(schema.TABLES) == set(CONTRACT)


@pytest.mark.parametrize("table", sorted(CONTRACT))
def test_columns_names_order_types_nullability_equal_the_ddl(table):
    ours = [(c.name, c.type, c.nullable) for c in schema.TABLES[table]]
    assert ours == CONTRACT[table], f"{table} drifted from {DDL.name}"


def test_unknown_ddl_type_is_rejected_loudly():
    with pytest.raises(AssertionError, match="no Spark mapping"):
        to_spark("GEOGRAPHY")


def test_load_order_puts_dimensions_before_facts():
    names = list(schema.TABLES)
    assert max(names.index(t) for t in names if t.startswith("dim_")) < min(
        names.index(t) for t in names if t.startswith("fact_")
    )


def test_partition_columns_exist_in_their_tables():
    for table, col in schema.PARTITION_BY.items():
        assert col in schema.column_names(table)


def test_varchar_lengths_cover_the_key_scheme():
    """sha256 hex is 64 chars: every *_sk column must be wide enough."""
    for table in ("dim_member", "dim_provider", "dim_diagnosis", "fact_claim"):
        for col, n in varchar_lengths(table).items():
            if col.endswith("_sk"):
                assert n >= 64, f"{table}.{col} VARCHAR({n}) cannot hold a sha256 hex key"
    assert varchar_lengths("dim_member")["record_hash"] >= 64


def test_dbt_gold_path_switch_defaults_to_the_fixture():
    """The dbt sources must keep reading the fixture unless the real gold is selected."""
    from ddl import ROOT

    src = (ROOT / "dbt" / "models" / "staging" / "gold" / "_gold__sources.yml").read_text()
    m = re.search(r"external_location:\s*\"([^\"]+)\"", src)
    assert m, "external_location not found"
    assert "gold_path" in m.group(1) and "GOLD_PATH" in m.group(1)
    assert "'data/gold'" in m.group(1)
