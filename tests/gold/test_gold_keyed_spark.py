"""Keyed identifier tokens in the Spark gold (ticket #71, -m spark).

Hand-built silver, the real ``gold_batch``: tokens are keyed and deterministic across runs
(idempotent gold), different keys give different tokens and surrogate keys, no source id or
unkeyed digest reaches gold, and a key rotation leaves old tokens verifiable while the rebuilt
gold uses only the new key. Keys here are TEST-ONLY and fake.
"""

import hashlib
import os
from datetime import date, datetime

import pytest
from test_gold_scd2_spark import (
    MEM,
    PRV,
    claim,
    line,
    member,
    provider,
    put,
    seed_claims,
    tbl,
)

pytest.importorskip("pyspark")
pytest.importorskip("delta")

from pyspark.sql import functions as F  # noqa: E402

from lakehouse.gold.job import gold_batch  # noqa: E402
from lakehouse.governance.tokenise import (  # noqa: E402
    KEY_ENV_VAR,
    KEY_ID_ENV_VAR,
    KeyRing,
    Tokeniser,
    key_id_counts,
)

pytestmark = pytest.mark.spark

D1, D2 = date(2024, 1, 1), date(2024, 2, 1)
RUN = datetime(2025, 1, 1, 12, 0, 0)
KEY_A = "TEST-ONLY-KEY-A-0123456789-abcdefghijklmn"
KEY_B = "TEST-ONLY-KEY-B-9876543210-zyxwvutsrqponm"
MEMBERS = [f"M{i:09d}" for i in range(1, 31)]  # sequential, as in the generator


@pytest.fixture
def lake(tmp_path):
    return str(tmp_path / "silver"), str(tmp_path / "gold")


@pytest.fixture
def use_key(monkeypatch):
    def _use(key, key_id="v1"):
        monkeypatch.setenv(KEY_ENV_VAR, key)
        monkeypatch.setenv(KEY_ID_ENV_VAR, key_id)

    return _use


def build(spark, silver, gold):
    put(spark, silver, "members", [member(m) for m in MEMBERS], MEM, D1)
    put(spark, silver, "providers", [provider(f"PRV{i:07d}") for i in range(1, 6)], PRV, D1)
    svc = date(2024, 1, 10)
    seed_claims(
        spark,
        silver,
        D2,
        [claim(f"C{i}", MEMBERS[i], "PRV0000001", svc) for i in range(10)],
        [line(f"C{i}", 1, svc) for i in range(10)],
    )
    for d in (D1, D2):
        assert all(r.identity_ok for r in gold_batch(spark, silver, gold, d, run_ts=RUN))


def snapshot(spark, gold):
    """Everything classified as an identifier or derived from one, without run-time columns."""
    out = {}
    for t, cols in (
        ("dim_member", ["member_id", "member_sk"]),
        ("dim_provider", ["provider_id", "provider_sk"]),
        ("fact_claim", ["claim_id", "member_sk", "provider_sk"]),
    ):
        out[t] = sorted(tuple(r) for r in tbl(spark, gold, t).select(*cols).collect())
    return out


def test_same_key_is_deterministic_and_the_rerun_changes_nothing(spark, lake, use_key):
    use_key(KEY_A)
    silver, gold = lake
    build(spark, silver, gold)
    first = snapshot(spark, gold)
    for d in (D1, D2):  # re-run every batch with the same key: idempotent gold
        gold_batch(spark, silver, gold, d, run_ts=RUN)
    assert snapshot(spark, gold) == first
    # and an independent second lake built from scratch with the same key is identical
    silver2, gold2 = lake[0] + "2", lake[1] + "2"
    build(spark, silver2, gold2)
    assert snapshot(spark, gold2) == first


def test_different_keys_give_different_tokens_and_surrogate_keys(spark, lake, use_key):
    silver, gold = lake
    use_key(KEY_A)
    build(spark, silver, gold)
    a = snapshot(spark, gold)
    use_key(KEY_B)
    silver_b, gold_b = lake[0] + "b", lake[1] + "b"
    build(spark, silver_b, gold_b)
    b = snapshot(spark, gold_b)
    for t, i in (("dim_member", 0), ("dim_member", 1), ("dim_provider", 0), ("dim_provider", 1)):
        assert not ({r[i] for r in a[t]} & {r[i] for r in b[t]}), (t, i)
    assert {r[0] for r in a["fact_claim"]} == {
        r[0] for r in b["fact_claim"]
    }  # claim ids are not keyed


def test_no_source_id_or_unkeyed_digest_in_any_gold_cell(spark, lake, use_key):
    use_key(KEY_A)
    silver, gold = lake
    build(spark, silver, gold)
    ids = [*MEMBERS, *[f"PRV{i:07d}" for i in range(1, 6)]]
    needles = set(ids)
    for i in ids:
        for msg in (i, f"member:v1|{i}", f"provider:v1|{i}", f"member:v1|{i}|1900-01-01 00:00:00"):
            needles |= {
                hashlib.sha256(msg.encode()).hexdigest(),
                hashlib.md5(msg.encode()).hexdigest(),
            }
    for t in ("dim_member", "dim_provider", "fact_claim", "gold_exclusions"):
        df = tbl(spark, gold, t)
        for c in df.columns:
            vals = {r[0] for r in df.select(F.col(c).cast("string")).collect()}
            assert not (vals & needles), f"{t}.{c}"
    tk = Tokeniser(KEY_A)
    assert {r[0] for r in tbl(spark, gold, "dim_member").select("member_id").collect()} == {
        tk.token(m, "member") for m in MEMBERS
    }


def test_exclusion_detail_holds_tokens_not_source_ids(spark, lake, use_key):
    use_key(KEY_A)
    silver, gold = lake
    put(spark, silver, "members", [member("M000000001")], MEM, D1)
    put(spark, silver, "providers", [provider("PRV0000001")], PRV, D1)
    gold_batch(spark, silver, gold, D1, run_ts=RUN)
    svc = date(2024, 1, 10)
    # header says 2 lines, silver has 1: held back whole, detail names the (tokenised) ids
    seed_claims(
        spark,
        silver,
        D2,
        [claim("CSHORT", "M000000001", "PRV0000001", svc, lines=2, amt="200.00")],
        [line("CSHORT", 1, svc)],
    )
    gold_batch(spark, silver, gold, D2, run_ts=RUN)
    detail = (
        tbl(spark, gold, "gold_exclusions").where("business_key = 'CSHORT'").first().reason_detail
    )
    assert "M000000001" not in detail and "PRV0000001" not in detail
    assert Tokeniser(KEY_A).token("M000000001", "member") in detail


def test_rotation_old_tokens_verify_and_the_rebuild_uses_only_the_new_key(spark, lake, use_key):
    silver, gold = lake
    use_key(KEY_A, "v1")
    build(spark, silver, gold)
    old_tokens = {r[0] for r in tbl(spark, gold, "dim_member").select("member_id").collect()}
    assert key_id_counts(old_tokens) == {"v1": len(MEMBERS)}

    # rotate: v2 active, v1 retired (verify-only). Gold is rebuilt from silver (raw ids).
    use_key(KEY_B, "v2")
    os.environ["LAKEHOUSE_PSEUDONYM_RETIRED_KEYS"] = f'{{"v1": "{KEY_A}"}}'
    try:
        ring = KeyRing.from_env()
        gold2 = lake[1] + "-v2"
        build(spark, silver, gold2)
        new_tokens = {r[0] for r in tbl(spark, gold2, "dim_member").select("member_id").collect()}
        assert key_id_counts(new_tokens) == {"v2": len(MEMBERS)}  # new writes use the new key
        assert not (old_tokens & new_tokens)
        for m in MEMBERS:  # both generations verify against the ring
            assert ring.verify(Tokeniser(KEY_A, "v1").token(m, "member"), m, "member")
            assert ring.verify(ring.token(m, "member"), m, "member")
        assert old_tokens <= {Tokeniser(KEY_A, "v1").token(m, "member") for m in MEMBERS}
        assert all(not ring.is_current(t) for t in old_tokens)
    finally:
        os.environ.pop("LAKEHOUSE_PSEUDONYM_RETIRED_KEYS", None)


def test_missing_key_fails_before_any_gold_is_written(spark, lake, monkeypatch):
    from lakehouse.governance.tokenise import MissingKeyError

    silver, gold = lake
    put(spark, silver, "members", [member("M000000001")], MEM, D1)
    monkeypatch.delenv(KEY_ENV_VAR, raising=False)
    with pytest.raises(MissingKeyError):
        gold_batch(spark, silver, gold, D1, run_ts=RUN)
