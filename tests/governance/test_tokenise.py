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


# ---------------------------------------------------------------- key ids and rotation (#71)

from lakehouse.governance.tokenise import (  # noqa: E402
    KEY_ID_ENV_VAR,
    MAX_TOKEN_LENGTH,
    RETIRED_KEYS_ENV_VAR,
    KeyRing,
    key_id_counts,
    parse_token,
)

IDS = [f"M{i:09d}" for i in range(1, 2001)]  # the sequential id space an attacker enumerates


def test_token_carries_its_key_id_and_fits_the_gold_columns():
    t = Tokeniser(TEST_KEY, "v7").token("M000000001", "member")
    assert parse_token(t) == ("v7", t.rsplit("_", 1)[1])
    assert len(t) <= MAX_TOKEN_LENGTH <= 48  # VARCHAR(48) in 01_gold_ddl.sql
    assert (
        Tokeniser(TEST_KEY, "abcdefgh").token("x", "d")
        and len(Tokeniser(TEST_KEY, "abcdefgh").token("x", "d")) == MAX_TOKEN_LENGTH
    )
    assert parse_token("not-a-token") is None and parse_token(None) is None


@pytest.mark.parametrize("bad", ["", "V1", "v 1", "toolongid1", "v_1"])
def test_key_id_must_be_a_short_lowercase_slug(bad):
    with pytest.raises(ValueError):
        Tokeniser(TEST_KEY, bad)


def test_enumerating_sequential_ids_does_not_recover_tokens_without_the_key():
    """The point of #71: every unkeyed digest an attacker can compute over the whole id space
    misses every keyed token, even when the attacker tries the obvious domain/format variants."""
    import hashlib

    tk = Tokeniser(TEST_KEY)
    tokens = {tk.token(i, "member") for i in IDS}
    digests = set()
    for i in IDS:
        for msg in (i, f"member\x00{i}", f"member:v1|{i}"):
            for algo in (hashlib.sha256, hashlib.md5, hashlib.sha1):
                h = algo(msg.encode()).hexdigest()
                digests |= {h, h[:32], "tok_v1_" + h[:32]}
    assert not (tokens & digests)
    # ...whereas an attacker WITH a (wrong) key guess still gets nothing, and the real key
    # reproduces every token (so the test would notice if tokens were not actually keyed).
    wrong = Tokeniser("attacker-guess-0123456789-abcdefghijk")
    assert not ({wrong.token(i, "member") for i in IDS} & tokens)
    assert {Tokeniser(TEST_KEY).token(i, "member") for i in IDS} == tokens


def test_different_keys_give_different_tokens_with_the_same_format():
    a, b = Tokeniser(TEST_KEY, "v1"), Tokeniser(OTHER_KEY, "v2")
    for i in IDS[:200]:
        ta, tb = a.token(i, "member"), b.token(i, "member")
        assert ta != tb and ta.split("_", 2)[2] != tb.split("_", 2)[2]  # digests differ too
    # same key id but different key is also different (id alone is not the secret)
    assert Tokeniser(OTHER_KEY, "v1").token("M1", "member") != a.token("M1", "member")


def test_old_version_tokens_stay_verifiable_while_new_writes_use_the_new_key():
    old_ring = KeyRing(Tokeniser(TEST_KEY, "v1"))
    old_tokens = {i: old_ring.token(i, "member") for i in IDS[:50]}

    # rotation: v2 becomes active, v1 is retired (verify-only)
    ring = KeyRing(Tokeniser(OTHER_KEY, "v2"), [Tokeniser(TEST_KEY, "v1")])
    assert ring.key_ids == ("v2", "v1")
    for i, t in old_tokens.items():
        assert ring.verify(t, i, "member")  # old data still verifiable
        assert not ring.is_current(t)
        assert not ring.verify(t, i, "provider")  # wrong domain
        assert not ring.verify(t, "M999999999", "member")  # wrong value
        new = ring.token(i, "member")
        assert new != t and parse_token(new)[0] == "v2"  # new writes use the new key
        assert ring.verify(new, i, "member") and ring.is_current(new)
    # a ring that no longer holds v1 cannot verify v1 tokens (key dropped after re-tokenisation)
    dropped = KeyRing(Tokeniser(OTHER_KEY, "v2"))
    assert not dropped.verify(next(iter(old_tokens.values())), IDS[0], "member")
    # rotation progress is observable from the data alone
    mixed = [*old_tokens.values(), ring.token("M1", "member"), None]
    assert key_id_counts(mixed) == {"v1": 50, "v2": 1}


def test_ring_rejects_duplicate_ids_and_reads_the_environment():
    with pytest.raises(ValueError):
        KeyRing(Tokeniser(TEST_KEY, "v1"), [Tokeniser(OTHER_KEY, "v1")])
    env = {
        KEY_ENV_VAR: OTHER_KEY,
        KEY_ID_ENV_VAR: "v2",
        RETIRED_KEYS_ENV_VAR: json.dumps({"v1": TEST_KEY}),
    }
    ring = KeyRing.from_env(env)
    assert ring.active.key_id == "v2" and ring.key_ids == ("v2", "v1")
    assert TEST_KEY not in repr(ring) and OTHER_KEY not in repr(ring)
    assert KeyRing.from_env({KEY_ENV_VAR: TEST_KEY}).active.key_id == "v1"  # default id
    with pytest.raises(MissingKeyError) as e:
        KeyRing.from_env({KEY_ENV_VAR: TEST_KEY, RETIRED_KEYS_ENV_VAR: "not json " + OTHER_KEY})
    assert OTHER_KEY not in str(e.value)  # error text never echoes key material
    with pytest.raises(MissingKeyError):
        KeyRing.from_env({})


def test_token_fn_is_self_contained_and_equals_token():
    """The Spark UDF body: its closure holds only bytes/str and stdlib, so cloudpickle ships it
    by value and a worker needs neither this package nor the Tokeniser class (#71)."""
    tk = Tokeniser(TEST_KEY, "v3")
    fn = tk.token_fn("member")
    assert [fn(i) for i in (*IDS[:50], None)] == [tk.token(i, "member") for i in (*IDS[:50], None)]
    assert fn(IDS[0]) == fn(IDS[0])  # memoised call returns the same value
    assert all(
        isinstance(c.cell_contents, bytes | str | dict | int) for c in (fn.__closure__ or ())
    )
    assert TEST_KEY not in repr(fn)
