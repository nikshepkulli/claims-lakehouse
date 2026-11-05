"""Build the gold TEST FIXTURE from a small claimsgen run and check it against the DDL.

Skipped when DuckDB is not installed (the plain pytest job); the dbt workflow installs it.
"""

import os
import subprocess
import sys

import pytest
from dbt_fixture_loader import ROOT, load_fixture_module
from test_gold_contract import DDL_TABLES, normalise_type

duckdb = pytest.importorskip("duckdb")

fixture = load_fixture_module()


@pytest.fixture(scope="module")
def gold(tmp_path_factory):
    base = tmp_path_factory.mktemp("gold_fixture")
    raw, out = base / "raw", base / "gold"
    env = {**os.environ, "PYTHONPATH": str(ROOT / "src")}
    subprocess.run(
        [sys.executable, "-m", "claimsgen", "--rows", "1000", "--seed", "7", "--out", str(raw)],
        check=True,
        env=env,
        capture_output=True,
    )
    counts = fixture.build(raw, out)
    con = duckdb.connect()
    for table in counts:
        con.execute(f"CREATE VIEW {table} AS SELECT * FROM read_parquet('{out / table}/*.parquet')")
    return con, counts


def test_all_gold_tables_written_and_non_empty(gold):
    _, counts = gold
    assert set(counts) == set(DDL_TABLES)
    assert all(n > 0 for n in counts.values())


def test_parquet_columns_and_types_equal_snowflake_ddl(gold):
    con, _ = gold
    for table, cols in DDL_TABLES.items():
        described = con.execute(f"DESCRIBE {table}").fetchall()
        actual = [(name.lower(), normalise_type(typ)) for name, typ, *_ in described]
        assert actual == [(c, t) for c, t, _ in cols], table


def test_not_null_columns_have_no_nulls(gold):
    con, _ = gold
    for table, cols in DDL_TABLES.items():
        for col, _, not_null in cols:
            if not_null:
                n = con.execute(f"SELECT count(*) FROM {table} WHERE {col} IS NULL").fetchone()[0]
                assert n == 0, f"{table}.{col}"


def test_primary_keys_unique_and_foreign_keys_resolve(gold):
    con, _ = gold
    anti = "SELECT count(*) FROM {c} WHERE {k} NOT IN (SELECT {k} FROM {p})"
    checks = [
        "SELECT count(*) - count(DISTINCT claim_id) FROM fact_claim",
        "SELECT count(*) - count(DISTINCT line_id) FROM fact_claim_line",
        "SELECT count(*) - count(DISTINCT member_sk) FROM dim_member",
        anti.format(c="fact_claim", k="member_sk", p="dim_member"),
        anti.format(c="fact_claim", k="provider_sk", p="dim_provider"),
        anti.format(c="fact_claim_line", k="claim_id", p="fact_claim"),
    ]
    for sql in checks:
        assert con.execute(sql).fetchone()[0] == 0, sql
    orphan_dates = (
        "SELECT count(*) FROM fact_claim "
        "WHERE service_date_key NOT IN (SELECT date_key FROM dim_date)"
    )
    assert con.execute(orphan_dates).fetchone()[0] == 0


def test_header_totals_equal_sum_of_lines_and_no_negatives(gold):
    con, _ = gold
    bad = con.execute(
        """SELECT count(*) FROM fact_claim c JOIN (
               SELECT claim_id, sum(paid_amount) p, count(*) n FROM fact_claim_line GROUP BY 1
           ) l USING (claim_id) WHERE c.total_paid_amount != l.p OR c.line_count != l.n"""
    ).fetchone()[0]
    assert bad == 0
    neg = con.execute("SELECT count(*) FROM fact_claim_line WHERE paid_amount < 0").fetchone()[0]
    assert neg == 0


def test_scd2_has_multiple_versions_with_one_current(gold):
    con, _ = gold
    multi = con.execute(
        "SELECT count(*) FROM (SELECT member_id FROM dim_member GROUP BY 1 HAVING count(*) > 1)"
    ).fetchone()[0]
    assert multi > 0
    bad = con.execute(
        "SELECT count(*) FROM (SELECT member_id FROM dim_member GROUP BY 1 "
        "HAVING count(*) FILTER (WHERE is_current) != 1)"
    ).fetchone()[0]
    assert bad == 0
