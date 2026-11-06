"""Replay of dead letters (ticket #13): re-validation, bronze append, idempotency."""

from __future__ import annotations

import shutil

import pytest
from test_spark_gates import build_diverted_lake, divert_cfg
from test_spark_job import ev, ids, make_cfg, table, write_file

pytestmark = pytest.mark.spark

pytest.importorskip("pyspark")

from lakehouse.streaming.dlq_replay import REPLAY_TABLE, replay_dlq  # noqa: E402
from lakehouse.streaming.spark_job import StreamConfig, Tables, run_job  # noqa: E402


@pytest.fixture(scope="module")
def diverted(spark, tmp_path_factory):
    """One job run shared by the replay tests: 3 gate_diverted + 3 malformed_json dead letters."""
    tmp = tmp_path_factory.mktemp("divert")
    inp = tmp / "in"
    inp.mkdir()
    cfg, _ = build_diverted_lake(spark, tmp, inp)
    return cfg, inp


@pytest.fixture
def lake_copy(diverted, tmp_path):
    """A private copy of the lake per test (replay mutates bronze and the replay log)."""
    cfg, inp = diverted
    src = tmp_path / "src"
    src.mkdir()
    for sub in ("bronze", "silver", "ck"):
        shutil.copytree(cfg.bronze_root.rsplit("/", 1)[0] + f"/{sub}", src / sub)
    inp2 = src / "in"
    shutil.copytree(inp, inp2)
    return make_cfg(src, inp2), src


def replay_log(spark, cfg):
    return {r.dlq_key: r for r in table(spark, cfg, REPLAY_TABLE).collect()}


def test_dry_run_counts_and_writes_nothing(spark, lake_copy):
    cfg, _ = lake_copy
    before = table(spark, cfg, Tables.BRONZE).count()
    r = replay_dlq(spark, cfg, dry_run=True)
    assert (r.considered, r.replayed, r.still_invalid) == (6, 3, 3)
    assert r.still_invalid_by_reason == {"malformed_json": 3}
    assert table(spark, cfg, Tables.BRONZE).count() == before
    assert table(spark, cfg, REPLAY_TABLE).count() == 0


def test_replay_appends_only_the_now_valid_and_is_idempotent(spark, lake_copy):
    cfg, _ = lake_copy
    bronze_before = table(spark, cfg, Tables.BRONZE).count()

    first = replay_dlq(spark, cfg)
    assert (first.replayed, first.still_invalid, first.already_replayed) == (3, 3, 0)
    assert table(spark, cfg, Tables.BRONZE).count() == bronze_before + 3
    log = replay_log(spark, cfg)
    assert sorted(r.status for r in log.values()) == ["replayed"] * 3 + ["still_invalid"] * 3
    attempts_after_first = {k: r.attempts for k, r in log.items() if r.status == "replayed"}

    # second run: replayed ones are skipped entirely; the 3 malformed are re-checked, not replayed
    second = replay_dlq(spark, cfg)
    assert (second.replayed, second.already_replayed, second.considered) == (0, 3, 3)
    assert table(spark, cfg, Tables.BRONZE).count() == bronze_before + 3
    log2 = replay_log(spark, cfg)
    assert {k: log2[k].attempts for k in attempts_after_first} == attempts_after_first
    assert {r.attempts for r in log2.values() if r.status == "still_invalid"} == {2}

    # crash between the bronze append and the log write: the log row is lost, bronze has the data
    spark.sql(f"DELETE FROM delta.`{cfg.uri(REPLAY_TABLE)}` WHERE status = 'replayed'")
    third = replay_dlq(spark, cfg)
    assert third.replayed == 3  # re-validated and re-logged ...
    assert table(spark, cfg, Tables.BRONZE).count() == bronze_before + 3  # ... but NOT re-appended
    assert sorted(r.status for r in replay_log(spark, cfg).values()) == (
        ["replayed"] * 3 + ["still_invalid"] * 3
    )


def test_replayed_events_flow_through_the_streaming_job_into_silver(spark, tmp_path):
    """The silver query keeps its checkpoint and must pick up the insert-only MERGE into bronze.
    Event 4 (hour 20) is also still in the dedupe state from the diverted batch: replay must
    not lose it as a "duplicate"."""
    inp = tmp_path / "in"
    inp.mkdir()
    cfg, _ = build_diverted_lake(spark, tmp_path, inp)
    assert ids(spark, cfg) == [f"EVT{n:013d}" for n in (1, 2, 6, 7)]
    replay_dlq(spark, cfg)
    res = run_job(spark, cfg)
    assert ids(spark, cfg) == [f"EVT{n:013d}" for n in (1, 2, 3, 4, 5, 6, 7)]
    assert res.counts[Tables.LATE] == 0
    # replayed events are ordinary arrivals: nothing changes on another full re-run
    again = run_job(spark, cfg)
    assert again.counts == res.counts


def test_reason_filter_and_limit(spark, lake_copy):
    cfg, _ = lake_copy
    r = replay_dlq(spark, cfg, reasons=("gate_diverted",), limit=2)
    assert (r.considered, r.replayed, r.still_invalid) == (2, 2, 0)
    r2 = replay_dlq(spark, cfg, reasons=("gate_diverted",))
    assert (r2.considered, r2.replayed, r2.already_replayed) == (1, 1, 2)
    assert sorted(x.status for x in replay_log(spark, cfg).values()) == ["replayed"] * 3


def test_validation_uses_the_current_contract_so_unfixable_stay_dead(spark, tmp_path):
    inp = tmp_path / "in"
    inp.mkdir()
    refunded = {**ev(2, "submitted", 2), "event_type": "claim_refunded"}
    write_file(inp, "f1.jsonl", [ev(1, "submitted", 1), refunded, "null", "[]"], 1000)
    cfg = make_cfg(tmp_path, inp)
    run_job(spark, cfg)
    r = replay_dlq(spark, cfg)
    assert r.replayed == 0 and r.still_invalid == r.considered > 0
    assert table(spark, cfg, REPLAY_TABLE).where("status = 'replayed'").count() == 0


def test_missing_dlq_table_is_a_clear_error(spark, tmp_path):
    cfg = StreamConfig(
        bronze_root=str(tmp_path / "b"),
        silver_root=str(tmp_path / "s"),
        checkpoint_root=str(tmp_path / "c"),
    )
    with pytest.raises(SystemExit, match="no DLQ table"):
        replay_dlq(spark, cfg)


def test_kafka_source_requires_a_bootstrap(spark, tmp_path):
    cfg = divert_cfg(tmp_path, tmp_path)
    with pytest.raises(ValueError, match="bootstrap"):
        replay_dlq(spark, cfg, source="kafka")
