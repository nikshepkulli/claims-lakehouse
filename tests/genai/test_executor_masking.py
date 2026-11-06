import pytest

from lakehouse.genai.executor import QueryTimeout
from lakehouse.genai.masking import REDACTED, mask_rows, scrub


def test_sandbox_has_no_pii_columns_even_if_guardrails_were_bypassed(analyst_executor):
    types = analyst_executor.column_types
    cols = {c for t in types.values() for c in t}
    assert not cols & {"member_id", "provider_id", "birth_year", "enrollment_date"}
    assert "record_hash" not in types["dim_member"]  # CONFIDENTIAL, handling=drop
    import duckdb

    with pytest.raises(duckdb.Error):
        analyst_executor.run("SELECT member_id FROM dim_member", 5, 10)


def test_sandbox_blocks_file_access_and_reconfiguration(analyst_executor, gold_dir):
    import duckdb

    with pytest.raises(duckdb.Error):
        analyst_executor.run(
            f"SELECT * FROM read_parquet('{gold_dir}/dim_member/*.parquet')", 5, 10
        )
    with pytest.raises(duckdb.Error):
        analyst_executor.run("SET enable_external_access=true", 5, 10)


def test_row_cap_and_truncation(analyst_executor):
    res = analyst_executor.run("SELECT claim_id FROM fact_claim", 5, 10)
    assert len(res.rows) == 10 and res.truncated


def test_timeout_cancels_query(analyst_executor):
    with pytest.raises(QueryTimeout):
        analyst_executor.run(
            "SELECT SUM(sin(CAST(x AS DOUBLE)) * cos(x) + ln(x + 1)) FROM range(20000000000) t(x)",
            0.3,
            10,
        )
    # connection still usable afterwards
    assert analyst_executor.run("SELECT 1", 5, 10).rows == [(1,)]


def test_marts_attached_when_requested(tmp_path):
    import duckdb

    from lakehouse.genai.catalog import build_catalog
    from lakehouse.genai.executor import Executor
    from lakehouse.governance import registry

    db = tmp_path / "src_marts.duckdb"
    con = duckdb.connect(str(db))
    con.execute("CREATE SCHEMA marts")
    reg = registry.load()
    for name, t in reg.items():
        if t.layer == "mart":
            cols = ", ".join(f'1 AS "{c}"' for c in t.columns)
            con.execute(f"CREATE TABLE marts.{name} AS SELECT {cols}")
    con.close()
    cat = build_catalog("analyst", layers=("mart",))
    ex = Executor(cat, tmp_path, marts_db=db)
    assert "provider_id" not in ex.column_types["mart_provider_performance"]
    assert ex.run("SELECT claim_count FROM mart_denials", 5, 10).rows == [(1,)]
    ex.close()
    with pytest.raises(ValueError):
        Executor(cat, tmp_path)


@pytest.mark.parametrize(
    "text",
    ["M123456789", "PRV1234567", "123-45-6789", "a.b@example.com", "call 415-555-0199 now"],
)
def test_scrub_identifier_shapes(text):
    assert REDACTED in scrub(text)


def test_mask_rows_counts_changes():
    rows, n = mask_rows([("M123456789", 5), ("ok", 2.5)])
    assert rows == [(REDACTED, 5), ("ok", 2.5)] and n == 1
