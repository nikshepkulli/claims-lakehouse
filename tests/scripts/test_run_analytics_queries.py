"""Unit tests for scripts/run_analytics_queries.py.

The header-parser tests need nothing but the standard library, so they run in the plain
pytest job. The execution tests skip when duckdb is not installed; the dbt workflow runs
the real queries against the built marts.
"""

import importlib.util
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location(
    "run_analytics_queries", ROOT / "scripts" / "run_analytics_queries.py"
)
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)

QUERY_FILES = sorted((ROOT / "analytics" / "queries").glob("*.sql"))

GOOD = """-- question: How much?
-- grain: one row per month
-- caveat: synthetic data
--   and a second line
-- columns: month, total
-- key_columns: month
-- rows: 12
SELECT month, total FROM marts.mart_claims_monthly;
"""


def test_parse_header_reads_fields_lists_and_continuations():
    h = mod.parse_header(GOOD)
    assert h["question"] == "How much?"
    assert h["caveat"] == "synthetic data and a second line"
    assert h["columns"] == ["month", "total"]
    assert h["key_columns"] == ["month"]


def test_parse_header_rejects_missing_field():
    with pytest.raises(mod.QueryError, match="caveat"):
        mod.parse_header(GOOD.replace("-- caveat: synthetic data\n", ""))


def test_parse_header_rejects_key_column_outside_columns():
    with pytest.raises(mod.QueryError, match="key_columns"):
        mod.parse_header(GOOD.replace("key_columns: month", "key_columns: nope"))


def test_parse_header_stops_at_first_non_comment_line():
    text = GOOD + "-- columns: other\n"
    assert mod.parse_header(text)["columns"] == ["month", "total"]


def test_sql_body_drops_comments_and_semicolon():
    assert mod.sql_body(GOOD) == "SELECT month, total FROM marts.mart_claims_monthly"


@pytest.mark.parametrize(
    "body",
    [
        "SELECT 1 FROM staging.stg_gold__fact_claim",
        "SELECT 1 FROM marts.mart_denials JOIN marts.int_claims_enriched USING (x)",
        "SELECT 1 FROM mart_denials",
        "SELECT 1 FROM raw.claims",
    ],
)
def test_check_tables_rejects_non_mart_tables(body):
    with pytest.raises(mod.QueryError):
        mod.check_tables(body)


def test_check_tables_allows_marts_and_ctes():
    mod.check_tables("WITH t AS (SELECT 1 FROM marts.mart_denials) SELECT * FROM t")


def test_shipped_queries_have_valid_headers_and_read_only_marts():
    assert len(QUERY_FILES) >= 8
    for path in QUERY_FILES:
        text = path.read_text()
        mod.parse_header(text)
        mod.check_tables(mod.sql_body(text))


def test_run_query_checks_columns_nulls_and_empty_results(tmp_path):
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect()
    con.execute("CREATE SCHEMA marts")
    con.execute("CREATE TABLE marts.mart_claims_monthly AS SELECT 1 AS month, NULL AS total")

    def run(sql_text):
        p = tmp_path / "q.sql"
        p.write_text(sql_text)
        return mod.run_query(con, p)

    with pytest.raises(mod.QueryError, match="NULL"):
        run(GOOD.replace("key_columns: month", "key_columns: month, total"))
    assert run(GOOD) == 1
    with pytest.raises(mod.QueryError, match="columns"):
        run(GOOD.replace("columns: month, total", "columns: month, other"))
    with pytest.raises(mod.QueryError, match="0 rows"):
        run(GOOD.replace(";", " WHERE 1 = 0;"))
