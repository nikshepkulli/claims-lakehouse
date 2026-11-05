"""Keyed pseudonymisation: determinism, rotation, secrecy, referential integrity."""

import json

import pyarrow as pa
import pyarrow.dataset as ds
import pytest
from governance_helpers import OTHER_KEY, TEST_KEY

from lakehouse.governance.tokenise import KEY_ENV_VAR, MissingKeyError, Tokeniser


def test_deterministic_and_prefixed():
    a, b = Tokeniser(TEST_KEY), Tokeniser(TEST_KEY)
    assert a.token("M000000001", "member") == b.token("M000000001", "member")
    assert a.token("M000000001", "member").startswith("tok_")
    assert a.token("M000000001", "member") != a.token("M000000002", "member")


def test_key_rotation_changes_every_token():
    old, new = Tokeniser(TEST_KEY), Tokeniser(OTHER_KEY)
    ids = [f"M{i:09d}" for i in range(500)]
    assert all(old.token(i, "member") != new.token(i, "member") for i in ids)


def test_domains_are_separated():
    t = Tokeniser(TEST_KEY)
    assert t.token("X1", "member") != t.token("X1", "provider")


def test_nulls_stay_null_and_key_is_never_printed():
    t = Tokeniser(TEST_KEY)
    assert t.token(None, "member") is None
    assert TEST_KEY not in repr(t) and TEST_KEY not in str(t)


def test_key_comes_from_env_and_must_be_long_enough():
    with pytest.raises(MissingKeyError):
        Tokeniser.from_env({})
    with pytest.raises(MissingKeyError):
        Tokeniser("short")
    assert Tokeniser.from_env({KEY_ENV_VAR: TEST_KEY}).token("a", "d")


def test_no_key_literal_in_the_repo_source():
    from pathlib import Path

    src = Path(__file__).resolve().parents[2] / "src" / "lakehouse" / "governance"
    for f in src.glob("*.py"):
        assert TEST_KEY not in f.read_text()


def _load(raw_dir):
    claims = ds.dataset(raw_dir / "claims", format="parquet").to_table()
    members = ds.dataset(raw_dir / "members", format="parquet").to_table()
    providers = ds.dataset(raw_dir / "providers", format="parquet").to_table()
    notes = [
        json.loads(line)
        for f in sorted((raw_dir / "notes").rglob("*.jsonl"))
        for line in f.read_text().splitlines()
    ]
    return claims, members, providers, notes


def test_no_raw_identifier_substring_survives_and_integrity_is_preserved(raw_dir):
    claims, members, providers, _ = _load(raw_dir)
    t = Tokeniser(TEST_KEY)
    cmap = {"member_id": "member", "provider_id": "provider"}
    c2 = t.tokenise_table(claims, cmap)
    m2 = t.tokenise_table(members, {"member_id": "member"})
    p2 = t.tokenise_table(providers, {"provider_id": "provider"})

    raw_ids = set(claims.column("member_id").to_pylist()) | set(
        members.column("member_id").to_pylist()
    )
    raw_ids |= set(claims.column("provider_id").to_pylist()) | set(
        providers.column("provider_id").to_pylist()
    )
    raw_ids.discard(None)
    blob = "\n".join(
        v
        for tbl, cols in ((c2, cmap), (m2, ["member_id"]), (p2, ["provider_id"]))
        for col in cols
        for v in tbl.column(col).to_pylist()
        if v
    )
    assert not any(r in blob for r in raw_ids)  # substring search, not just equality

    # Same raw id -> same token in every table, so joins still work...
    assert set(c2.column("member_id").to_pylist()) & set(m2.column("member_id").to_pylist())
    raw_matched = set(claims.column("member_id").to_pylist()) & set(
        members.column("member_id").to_pylist()
    )
    tok_matched = set(c2.column("member_id").to_pylist()) & set(m2.column("member_id").to_pylist())
    assert len(raw_matched) == len(tok_matched)
    # ...and orphans (claims whose member is missing) stay orphans: no accidental links.
    raw_orphans = set(claims.column("member_id").to_pylist()) - set(
        members.column("member_id").to_pylist()
    )
    tok_orphans = set(c2.column("member_id").to_pylist()) - set(m2.column("member_id").to_pylist())
    assert len(raw_orphans) == len(tok_orphans) > 0
    # Tokenising is injective on this data and keeps row count and column order.
    assert len(set(claims.column("member_id").to_pylist())) == len(
        set(c2.column("member_id").to_pylist())
    )
    assert c2.num_rows == claims.num_rows and c2.schema.names == claims.schema.names


def test_unknown_column_raises():
    t = Tokeniser(TEST_KEY)
    with pytest.raises(KeyError):
        t.tokenise_table(pa.table({"a": ["x"]}), {"nope": "d"})
