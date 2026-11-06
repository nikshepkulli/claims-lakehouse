"""Leakage test 3: deterministic and idempotent output. Same inputs -> same hash."""

import json
import random
import shutil

import pyarrow.parquet as pq
import pytest
from features_helpers import random_dataset

from lakehouse.features import store
from lakehouse.features.fixture import write_gold_export


@pytest.fixture(scope="module")
def first(gold_export, tmp_path_factory):
    out = tmp_path_factory.mktemp("store_a")
    return out, store.build(gold_export, out)


def test_same_inputs_same_hashes_and_same_bytes(first, gold_export, tmp_path):
    out_a, m_a = first
    m_b = store.build(gold_export, tmp_path / "b")
    assert m_a == m_b
    assert (out_a / "_manifest.json").read_text() == (tmp_path / "b" / "_manifest.json").read_text()
    files = sorted(p.relative_to(out_a) for p in out_a.rglob("*.parquet"))
    assert files and files == sorted(
        p.relative_to(tmp_path / "b") for p in (tmp_path / "b").rglob("*.parquet")
    )
    for f in files:
        assert (out_a / f).read_bytes() == (tmp_path / "b" / f).read_bytes(), f


def test_rebuild_into_the_same_directory_is_idempotent(first, gold_export):
    out, m = first
    before = {p: p.read_bytes() for p in out.rglob("*") if p.is_file()}
    assert store.build(gold_export, out) == m
    after = {p: p.read_bytes() for p in out.rglob("*") if p.is_file()}
    assert before == after


def test_stored_files_match_the_manifest_and_tampering_is_detected(first, tmp_path):
    out, _ = first
    store.verify(out)
    copy = tmp_path / "copy"
    shutil.copytree(out, copy)
    victim = sorted((copy / "feature_store" / "member_features").rglob("*.parquet"))[0]
    t = pq.read_table(victim).to_pydict()
    t["claims_30d"][0] += 1
    import pyarrow as pa

    pq.write_table(pa.table(t, schema=pq.read_schema(victim)), victim)
    with pytest.raises(ValueError, match="do not match the manifest"):
        store.verify(copy)


def test_input_row_order_does_not_change_the_hashes(tmp_path):
    members, providers, claims = random_dataset(5)
    a = write_gold_export(tmp_path / "a", members, providers, claims)
    rng = random.Random(1)
    for xs in (members, providers, claims):
        rng.shuffle(xs)
    b = write_gold_export(tmp_path / "b", members, providers, claims)
    ma, mb = (
        store.build(a, tmp_path / "oa", guard=False),
        store.build(b, tmp_path / "ob", guard=False),
    )
    assert ma == mb


def test_manifest_has_no_wall_clock_and_records_inputs(first):
    _, m = first
    text = json.dumps(m)
    assert set(m["sources"]) == {"src_claims", "src_dim_member", "src_dim_provider"}
    assert m["config"]["outcome_lag_days"] == 30
    assert m["leakage_guard"]["comparisons"] > 0
    assert "built_at" not in text and "timestamp" not in text
