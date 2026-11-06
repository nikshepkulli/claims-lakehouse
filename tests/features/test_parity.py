"""Leakage test 4: training/serving parity. The batch (SQL) features equal an independent
row-at-a-time Python recompute for sampled keys."""

import math
import random
from datetime import datetime

import pytest

from lakehouse.features import store
from lakehouse.features.config import FeatureConfig
from lakehouse.features.online import OnlineFeatures


def _eq(a, b) -> bool:
    if isinstance(a, float) or isinstance(b, float):
        return (a is None) == (b is None) and (a is None or math.isclose(a, b, abs_tol=1e-6))
    return a == b


@pytest.fixture(scope="module")
def built(gold_export, tmp_path_factory):
    from lakehouse.features import sql

    out = tmp_path_factory.mktemp("parity")
    store.build(gold_export, out, guard=False)
    con = store.connect()
    sql.prepare_sources(con, str(gold_export), FeatureConfig())
    online = OnlineFeatures.from_connection(con, FeatureConfig())
    reader = store.connect()
    for t in ("member_features", "provider_features", "claim_features"):
        store.read_table(reader, out, t)
    return online, reader


def _sample(reader, table, n, seed):
    rows = reader.execute(f"SELECT * FROM {table} ORDER BY 1, as_of").fetchall()
    names = [d[0] for d in reader.description]
    rng = random.Random(seed)
    return [dict(zip(names, r, strict=True)) for r in rng.sample(rows, n)]


def _compare(batch: dict, live: dict, keys: set[str]):
    assert set(batch) - keys == set(live), (set(batch) - keys) ^ set(live)
    for col, v in live.items():
        assert _eq(batch[col], v), f"{col}: batch={batch[col]!r} online={v!r}"


def test_member_parity(built):
    online, reader = built
    for row in _sample(reader, "member_features", 60, 1):
        _compare(row, online.member(row["member_id"], row["as_of"]), {"member_id", "as_of"})


def test_provider_parity_including_peer_z_scores(built):
    online, reader = built
    rows = _sample(reader, "provider_features", 60, 2)
    assert any(r["cost_z_365d"] is not None for r in rows), "sample must exercise the z-score"
    for row in rows:
        _compare(row, online.provider(row["provider_id"], row["as_of"]), {"provider_id", "as_of"})


def test_claim_parity(built):
    online, reader = built
    keys = {"claim_id", "member_id", "provider_id", "as_of"}
    rows = _sample(reader, "claim_features", 80, 3)
    assert any(r["m_denial_rate_365d"] is not None for r in rows)
    assert any(r["p_cost_z_365d"] is not None for r in rows)
    for row in rows:
        live = online.claim(row["claim_id"])
        assert row["as_of"] == datetime.combine(
            online.by_id[row["claim_id"]]["received_date"], datetime.min.time()
        )
        _compare(row, live, keys)


def test_parity_sample_covers_scd2_changes(built):
    """The second plan_type version (2024-07-01) is served as-of, in both paths."""
    online, reader = built
    rows = reader.execute(
        "SELECT member_id, as_of, plan_type FROM member_features "
        "WHERE member_id IN (SELECT member_id FROM member_features GROUP BY 1 "
        "HAVING count(DISTINCT plan_type) > 1) ORDER BY 1, 2"
    ).fetchall()
    assert rows, "fixture should contain members with two plan versions"
    for mid, as_of, plan in rows[:80]:
        assert online.member(mid, as_of)["plan_type"] == plan
