"""DuckDB proof that an analyst-facing view cannot return raw identifiers (ticket #25).

Builds the gold TEST FIXTURE (same approach as test_gold_fixture.py), generates the analyst
views from governance/data_classification.yaml, and scans EVERY cell the views can return.
Skipped when DuckDB is not installed. This models the ANALYST role's view of the data in
DuckDB (which has no roles); it does not prove the Snowflake policies behave the same way,
because those have never been executed.
"""

import os
import re
import subprocess
import sys

import pytest
from dbt_fixture_loader import ROOT, load_fixture_module

duckdb = pytest.importorskip("duckdb")
sys.path.insert(0, str(ROOT / "src"))

from lakehouse.governance import registry, views  # noqa: E402

fixture = load_fixture_module()
RAW_ID = re.compile(r"\b(?:M\d{9}|PRV\d{7})\b")


@pytest.fixture(scope="module")
def con():
    import tempfile
    from pathlib import Path

    with tempfile.TemporaryDirectory() as tmp:
        base = Path(tmp)
        raw, out = base / "raw", base / "gold"
        env = {**os.environ, "PYTHONPATH": str(ROOT / "src")}
        subprocess.run(
            [sys.executable, "-m", "claimsgen", "--rows", "1000", "--seed", "7", "--out", str(raw)],
            check=True,
            env=env,
            capture_output=True,
        )
        counts = fixture.build(raw, out)
        c = duckdb.connect()
        # The SOURCE ids (never present in gold since #71): used to prove none leaks into it.
        c.execute(
            "CREATE TABLE source_ids AS "
            f"SELECT member_id AS id FROM read_parquet('{fixture._glob(raw, 'members')}') "
            f"UNION SELECT provider_id FROM read_parquet('{fixture._glob(raw, 'providers')}')"
        )
        for table in counts:
            c.execute(
                f"CREATE TABLE {table} AS SELECT * FROM read_parquet('{out / table}/*.parquet')"
            )
        reg = registry.load()
        for name, table in reg.items():
            if table.layer != "gold":
                continue
            types = {r[0]: r[1] for r in c.execute(f"DESCRIBE {name}").fetchall()}
            c.execute(views.analyst_view_sql(table, types))
        yield c


GOLD = [n for n, t in registry.load().items() if t.layer == "gold"]


def all_strings(con, relation: str) -> list[str]:
    """Every cell of the relation rendered as text (so leaks in any column type are caught)."""
    cols = [r[0] for r in con.execute(f"DESCRIBE {relation}").fetchall()]
    out: list[str] = []
    for c in cols:
        out += [
            r[0]
            for r in con.execute(f'SELECT CAST("{c}" AS VARCHAR) FROM {relation}').fetchall()
            if r[0]
        ]
    return out


def test_control_detector_finds_source_ids_when_they_are_there(con):
    """Guards the test itself: the detector must find ids when they are there. Gold holds only
    keyed tokens since #71, so the control is the SOURCE id list, not a gold table."""
    ids = [r[0] for r in con.execute("SELECT id FROM source_ids").fetchall()]
    assert len(ids) > 100 and all(RAW_ID.fullmatch(i) for i in ids)
    assert RAW_ID.search("see member M000000042 here")
    assert not any(RAW_ID.search(s) for s in all_strings(con, "dim_member"))


@pytest.mark.parametrize("table", GOLD)
def test_no_raw_member_or_provider_id_in_any_analyst_cell(con, table):
    leaked = [s for s in all_strings(con, f"analyst_{table}") if RAW_ID.search(s)]
    assert not leaked, leaked[:3]


def test_raw_id_values_do_not_appear_even_as_substrings(con):
    raw = {r[0] for r in con.execute("SELECT id FROM source_ids").fetchall()}
    blob = "\n".join(
        s for t in GOLD for s in all_strings(con, f"analyst_{t}")
    )  # concatenated: substring, not equality
    # Boundary-aware: 'M000000000' is a substring of the claim id 'CLM000000000562'.
    leaked = [r for r in raw if re.search(rf"(?<![A-Za-z0-9]){re.escape(r)}(?![0-9])", blob)]
    assert not leaked, leaked[:5]


def test_identifiers_are_keyed_tokens_stable_and_still_joinable(con):
    n_raw, n_hash = con.execute(
        "SELECT count(DISTINCT member_id),"
        " (SELECT count(DISTINCT member_id) FROM analyst_dim_member) FROM dim_member"
    ).fetchone()
    assert n_raw == n_hash > 0  # injective: no collisions, so counts and joins are preserved
    ids = [r[0] for r in con.execute("SELECT member_id FROM analyst_dim_member").fetchall()]
    # keyed HMAC token (ticket #71), shown as stored: no unkeyed SHA-256 of an id anywhere
    assert all(re.fullmatch(r"tok_[a-z0-9]{1,8}_[0-9a-f]{32}", i) for i in ids)
    # hashing is deterministic across the dimension and its SCD2 versions
    versions = con.execute(
        "SELECT count(*) FROM"
        " (SELECT member_id FROM analyst_dim_member GROUP BY 1 HAVING count(*) > 1)"
    ).fetchone()[0]
    assert versions > 0


def test_quasi_identifiers_are_generalised(con):
    assert (
        con.execute("SELECT count(*) FROM analyst_dim_member WHERE birth_year % 5 <> 0").fetchone()[
            0
        ]
        == 0
    )
    assert (
        con.execute(
            "SELECT count(*) FROM analyst_dim_member"
            " WHERE enrollment_date <> date_trunc('year', enrollment_date)"
        ).fetchone()[0]
        == 0
    )
    raw_years = con.execute("SELECT count(DISTINCT birth_year) FROM dim_member").fetchone()[0]
    masked_years = con.execute(
        "SELECT count(DISTINCT birth_year) FROM analyst_dim_member"
    ).fetchone()[0]
    assert masked_years < raw_years


def test_dropped_columns_are_null(con):
    assert con.execute("SELECT count(record_hash) FROM analyst_dim_member").fetchone()[0] == 0
    assert con.execute("SELECT count(record_hash) FROM dim_member").fetchone()[0] > 0


def test_columns_and_row_counts_are_unchanged_so_dashboards_keep_working(con):
    for t in GOLD:
        raw = [r[0] for r in con.execute(f"DESCRIBE {t}").fetchall()]
        masked = [r[0] for r in con.execute(f"DESCRIBE analyst_{t}").fetchall()]
        assert raw == masked
        assert (
            con.execute(f"SELECT count(*) FROM {t}").fetchone()
            == con.execute(f"SELECT count(*) FROM analyst_{t}").fetchone()
        )
