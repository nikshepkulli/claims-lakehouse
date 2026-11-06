"""Gold-shaped fixture (DuckDB) holds keyed tokens, never raw ids or unkeyed hashes (#71).

The Spark gold applies the same scheme (tests/gold/test_gold_keyed_spark.py proves it there);
this plain-pytest leg guards the fixture that the dbt tests and the analyst-view proof use.
Skipped when DuckDB is not installed.
"""

import hashlib
import importlib.util
import re

import pytest
from governance_helpers import ROOT

duckdb = pytest.importorskip("duckdb")

TOKEN = re.compile(r"tok_[a-z0-9]{1,8}_[0-9a-f]{32}")
GOLD_TABLES = ("dim_member", "dim_provider", "dim_diagnosis", "fact_claim", "fact_claim_line")
ID_SPACE = [f"M{i:09d}" for i in range(0, 1200)] + [f"PRV{i:07d}" for i in range(0, 600)]


def _fixture():
    spec = importlib.util.spec_from_file_location(
        "make_gold_fixture_71", ROOT / "scripts" / "make_gold_fixture.py"
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _build(raw_dir, out, key=None, monkeypatch=None):
    if key is None:
        monkeypatch.delenv("LAKEHOUSE_PSEUDONYM_KEY", raising=False)
    else:
        monkeypatch.setenv("LAKEHOUSE_PSEUDONYM_KEY", key)
    _fixture().build(raw_dir, out)
    return out


def _col(out, table, col):
    c = duckdb.connect()
    return [
        r[0]
        for r in c.execute(
            f"SELECT {col} FROM read_parquet('{(out / table).as_posix()}/*.parquet') ORDER BY 1"
        ).fetchall()
    ]


@pytest.fixture(scope="module")
def built(raw_dir, tmp_path_factory):
    mp = pytest.MonkeyPatch()
    out = tmp_path_factory.mktemp("fx_default")
    try:
        yield _build(raw_dir, out, None, mp)
    finally:
        mp.undo()


def test_no_raw_id_and_no_unkeyed_digest_in_any_cell_of_any_gold_table(built):
    needles = set(ID_SPACE)
    for i in ID_SPACE:
        for msg in (i, f"member:v1|{i}", f"provider:v1|{i}"):
            needles |= {
                hashlib.sha256(msg.encode()).hexdigest(),
                hashlib.md5(msg.encode()).hexdigest(),
            }
    con = duckdb.connect()
    for t in GOLD_TABLES:
        src = f"read_parquet('{(built / t).as_posix()}/*.parquet')"
        cols = [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM {src}").fetchall()]
        for c in cols:
            vals = {
                r[0] for r in con.execute(f'SELECT CAST("{c}" AS VARCHAR) FROM {src}').fetchall()
            }
            hit = vals & needles
            assert not hit, f"{t}.{c} leaks {sorted(hit)[:2]}"
            # raw ids embedded in longer strings (substring), boundary-aware like the DuckDB test
            for v in vals:
                assert not re.search(r"(?<![A-Za-z0-9])(?:M\d{9}|PRV\d{7})(?![0-9])", v or ""), (
                    t,
                    c,
                )


def test_identifier_columns_are_tokens_and_surrogate_keys_are_derived_from_them(built):
    for t, col in (("dim_member", "member_id"), ("dim_provider", "provider_id")):
        vals = _col(built, t, col)
        assert vals and all(TOKEN.fullmatch(v) for v in vals)
    con = duckdb.connect()
    rows = con.execute(
        f"SELECT member_id, member_sk, CAST(effective_from AS VARCHAR) FROM "
        f"read_parquet('{(built / 'dim_member').as_posix()}/*.parquet')"
    ).fetchall()
    for tok, sk, eff in rows[:200]:
        assert sk == hashlib.sha256(f"member:v1|{tok}|{eff}".encode()).hexdigest()


def test_same_key_is_deterministic_and_idempotent_across_runs(raw_dir, tmp_path, monkeypatch):
    key = "fixture-run-key-0123456789-abcdefghij"
    a = _build(raw_dir, tmp_path / "a", key, monkeypatch)
    b = _build(raw_dir, tmp_path / "b", key, monkeypatch)
    for t, c in (
        ("dim_member", "member_id"),
        ("dim_member", "member_sk"),
        ("dim_provider", "provider_sk"),
        ("fact_claim", "member_sk"),
        ("fact_claim", "provider_sk"),
    ):
        assert _col(a, t, c) == _col(b, t, c), (t, c)


def test_different_keys_give_different_tokens_and_surrogate_keys(raw_dir, tmp_path, monkeypatch):
    a = _build(raw_dir, tmp_path / "a", "first-key-0123456789-abcdefghijklmnop", monkeypatch)
    b = _build(raw_dir, tmp_path / "b", "second-key-9876543210-zyxwvutsrqponm", monkeypatch)
    for t, c in (
        ("dim_member", "member_id"),
        ("dim_member", "member_sk"),
        ("dim_provider", "provider_id"),
        ("fact_claim", "member_sk"),
    ):
        va, vb = set(_col(a, t, c)), set(_col(b, t, c))
        assert va and not (va & vb), (t, c)
    # non-identifier surrogate keys are key-independent (public ICD-10 codes)
    assert _col(a, "dim_diagnosis", "diagnosis_sk") == _col(b, "dim_diagnosis", "diagnosis_sk")


def test_fixture_default_key_is_labelled_test_only(built):
    src = (ROOT / "scripts" / "make_gold_fixture.py").read_text()
    assert "FIXTURE-ONLY-NOT-A-SECRET" in src and "TEST-ONLY" in src
    assert all(v.startswith("tok_fx_") for v in _col(built, "dim_member", "member_id"))
