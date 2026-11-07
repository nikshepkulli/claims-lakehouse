"""Spark Structured Streaming job (ticket #12) against a deterministic FILE source.

CI has no broker, so bronze reads JSONL files (``--source files``); the silver half, which holds
all the logic (validate, dedupe, watermark, late diversion, MERGEs, rollup, metrics), is the same
code that runs against Kafka. Each input file becomes one micro-batch (``max_files_per_trigger=1``)
and files are consumed in modification-time order, so the tests control batch boundaries.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

pytestmark = pytest.mark.spark

pytest.importorskip("pyspark")

from lakehouse.streaming.spark_job import (  # noqa: E402
    StreamConfig,
    Tables,
    run_job,
)

T0 = datetime(2024, 3, 1, 8, 0, 0, tzinfo=timezone.utc)  # noqa: UP017


def ts(hours: float) -> str:
    return (T0 + timedelta(hours=hours)).strftime("%Y-%m-%dT%H:%M:%SZ")


def ev(n: int, kind: str, hours: float, claim: int | None = None, amount: float = 100.0) -> dict:
    return {
        "event_id": f"EVT{n:013d}",
        "event_type": f"claim_{kind}",
        "event_ts": ts(hours),
        "claim_id": f"CLM{(claim if claim is not None else n):012d}",
        "member_id": "M000000001",
        "provider_id": "PRV0000001",
        "claim_type": "PROFESSIONAL",
        "amount": amount,
    }


def write_file(dirpath: Path, name: str, lines: list[object], mtime: int) -> None:
    """One JSONL file (dicts are dumped, strings written verbatim). mtime orders the batches."""
    p = dirpath / name
    p.write_text(
        "\n".join(x if isinstance(x, str) else json.dumps(x) for x in lines) + "\n",
        encoding="utf-8",
    )
    os.utime(p, (mtime, mtime))


@pytest.fixture
def lake(tmp_path):
    inp = tmp_path / "in"
    inp.mkdir()
    return inp, tmp_path


def make_cfg(tmp_path: Path, inp: Path, **kw) -> StreamConfig:
    base = {
        "bronze_root": str(tmp_path / "bronze"),
        "silver_root": str(tmp_path / "silver"),
        "checkpoint_root": str(tmp_path / "ck"),
        "source": "files",
        "input_dir": str(inp),
        "max_files_per_trigger": 1,
        "watermark_delay": "1 hour",
    }
    return StreamConfig(**{**base, **kw})


def table(spark, cfg: StreamConfig, name: str):
    return spark.read.format("delta").load(cfg.uri(name))


def ids(spark, cfg, name=Tables.SILVER) -> list[str]:
    return sorted(r.event_id for r in table(spark, cfg, name).select("event_id").collect())


def metrics_sum(spark, cfg, col: str) -> int:
    row = table(spark, cfg, Tables.METRICS).where("query_name = 'silver_events'")
    return row.agg({col: "sum"}).first()[0] or 0


def test_duplicates_collapse_and_out_of_order_within_watermark_accepted(spark, lake):
    inp, tmp = lake
    # batch 1: submit A, B.  batch 2: a verbatim duplicate of A plus an out-of-order event
    # (20 min older than the newest seen so far; well inside the 1h watermark).
    write_file(inp, "f1.jsonl", [ev(1, "submitted", 1), ev(2, "submitted", 2)], 1000)
    write_file(inp, "f2.jsonl", [ev(1, "submitted", 1), ev(3, "submitted", 1.7)], 2000)
    cfg = make_cfg(tmp, inp)
    res = run_job(spark, cfg)

    assert res.counts[Tables.BRONZE] == 4  # bronze is raw and append-only: duplicate kept
    assert ids(spark, cfg) == [f"EVT{n:013d}" for n in (1, 2, 3)]  # duplicate collapsed
    assert res.counts[Tables.LATE] == 0 and res.counts[Tables.DLQ] == 0


def test_event_beyond_watermark_is_diverted_and_counted(spark, lake):
    inp, tmp = lake
    write_file(inp, "f1.jsonl", [ev(1, "submitted", 1), ev(2, "submitted", 2)], 1000)
    write_file(inp, "f2.jsonl", [ev(3, "submitted", 10)], 2000)  # high-water mark -> 10:00
    # watermark for batch 3 = 10:00 - 1h = 09:00 (relative to T0): hour 5 is late, 9.5 is not.
    write_file(inp, "f3.jsonl", [ev(4, "submitted", 5), ev(5, "submitted", 9.5)], 3000)
    cfg = make_cfg(tmp, inp)
    run_job(spark, cfg)

    assert ids(spark, cfg) == [f"EVT{n:013d}" for n in (1, 2, 3, 5)]
    late = table(spark, cfg, Tables.LATE).collect()
    assert [r.event_id for r in late] == [f"EVT{4:013d}"]
    assert late[0].reason_code == "late_beyond_watermark"
    assert late[0].lateness_seconds == 4 * 3600  # cutoff 09:00 minus event time 05:00
    assert metrics_sum(spark, cfg, "late_count") == 1
    # diverted, not dropped: the claim is not in the rollup either (it only exists in late)
    assert table(spark, cfg, Tables.STATUS).where(f"claim_id = 'CLM{4:012d}'").count() == 0


def test_late_duplicate_of_accepted_event_is_not_reported_late(spark, lake):
    inp, tmp = lake
    write_file(inp, "f1.jsonl", [ev(1, "submitted", 1)], 1000)
    write_file(inp, "f2.jsonl", [ev(2, "submitted", 10)], 2000)
    write_file(inp, "f3.jsonl", [ev(1, "submitted", 1)], 3000)  # resend of an accepted event
    cfg = make_cfg(tmp, inp)
    run_job(spark, cfg)
    assert ids(spark, cfg) == [f"EVT{n:013d}" for n in (1, 2)]
    assert table(spark, cfg, Tables.LATE).count() == 0


def test_late_events_survive_several_batches_after_the_watermark_advanced(spark, lake):
    """Regression for #89. Spark's own dropDuplicatesWithinWatermark drops rows behind its
    event-time watermark from the batch AFTER the advance on, so a never-seen late event in batch
    3 vanished (no late row, no count) while one in batch 2 survived. All must reach `late`."""
    inp, tmp = lake
    write_file(inp, "f1.jsonl", [ev(1, "submitted", 1), ev(2, "submitted", 2)], 1000)
    write_file(inp, "f2.jsonl", [ev(3, "submitted", 10)], 2000)  # high-water mark -> hour 10
    write_file(inp, "f3.jsonl", [ev(4, "submitted", 4)], 3000)  # late, one batch after the advance
    write_file(inp, "f4.jsonl", [ev(5, "submitted", 3), ev(6, "submitted", 4.5)], 4000)  # two later
    write_file(inp, "f5.jsonl", [ev(7, "submitted", 5)], 5000)
    cfg = make_cfg(tmp, inp)
    run_job(spark, cfg)

    assert ids(spark, cfg) == [f"EVT{n:013d}" for n in (1, 2, 3)]
    assert ids(spark, cfg, Tables.LATE) == [f"EVT{n:013d}" for n in (4, 5, 6, 7)]
    assert metrics_sum(spark, cfg, "late_count") == 4


def test_resent_event_far_behind_the_watermark_is_still_deduped_not_late(spark, lake):
    """The arrival-time dedupe window still collapses resends of an already-accepted event, even
    when the resend's event time is far behind the high-water mark (it is a duplicate, not late)."""
    inp, tmp = lake
    write_file(inp, "f1.jsonl", [ev(1, "submitted", 3), ev(2, "submitted", 20)], 1000)
    write_file(inp, "f2.jsonl", [ev(3, "submitted", 3)], 2000)
    write_file(inp, "f3.jsonl", [ev(1, "submitted", 3), ev(1, "submitted", 3)], 3000)
    cfg = make_cfg(tmp, inp)
    run_job(spark, cfg)

    assert ids(spark, cfg) == [f"EVT{n:013d}" for n in (1, 2)]
    assert ids(spark, cfg, Tables.LATE) == [f"EVT{3:013d}"]
    assert metrics_sum(spark, cfg, "duplicates_dropped") >= 1


def test_invalid_events_go_to_dlq_with_reason_codes(spark, lake):
    inp, tmp = lake
    bad_enum = {**ev(3, "submitted", 1.5), "event_type": "claim_refunded"}
    missing = {k: v for k, v in ev(4, "submitted", 1.6).items() if k != "claim_id"}
    write_file(
        inp,
        "f1.jsonl",
        [ev(1, "submitted", 1), "{this is not json", bad_enum, missing, ev(2, "submitted", 2)],
        1000,
    )
    cfg = make_cfg(tmp, inp)
    run_job(spark, cfg)

    assert ids(spark, cfg) == [f"EVT{n:013d}" for n in (1, 2)]
    rows = {r.reason_code: r for r in table(spark, cfg, Tables.DLQ).collect() if r.reason_code}
    dead = table(spark, cfg, Tables.DLQ).collect()
    assert len(dead) == 3
    assert sorted(r.reason_code for r in dead) == [
        "contract_violation",
        "contract_violation",
        "malformed_json",
    ]
    env = json.loads(rows["malformed_json"].envelope_json)
    assert env["stage"] == "consumer" and env["error_type"] == "malformed_json"
    assert bytes(rows["malformed_json"].payload) == b"{this is not json"
    fields = sorted(f for r in dead for f in (r.failed_fields or []))
    assert fields == ["claim_id", "event_type"]
    assert metrics_sum(spark, cfg, "dlq_count") == 3


def test_status_rollup_latest_event_time_wins_across_batches(spark, lake):
    inp, tmp = lake
    # claim 1: submitted -> paid.  claim 2: submitted -> denied.  claim 3: the paid event arrives
    # BEFORE its (older) submission (out of order, within watermark): paid must still win.
    write_file(
        inp, "f1.jsonl", [ev(1, "submitted", 1, claim=1), ev(2, "submitted", 1.1, claim=2)], 1000
    )
    write_file(
        inp,
        "f2.jsonl",
        [
            ev(3, "paid", 2, claim=1, amount=80),
            ev(4, "denied", 2.1, claim=2, amount=0),
            ev(5, "paid", 2.5, claim=3, amount=55),
        ],
        2000,
    )
    write_file(inp, "f3.jsonl", [ev(6, "submitted", 2.0, claim=3)], 3000)
    cfg = make_cfg(tmp, inp)
    run_job(spark, cfg)

    got = {
        r.claim_id: (r.status, r.last_amount, r.submitted_ts is not None)
        for r in table(spark, cfg, Tables.STATUS).collect()
    }
    assert got == {
        f"CLM{1:012d}": ("paid", 80.0, True),
        f"CLM{2:012d}": ("denied", 0.0, True),
        f"CLM{3:012d}": ("paid", 55.0, True),
    }


def test_rerun_with_no_new_data_changes_nothing(spark, lake):
    inp, tmp = lake
    write_file(inp, "f1.jsonl", [ev(1, "submitted", 1), "garbage"], 1000)
    cfg = make_cfg(tmp, inp)
    first = run_job(spark, cfg).counts
    second = run_job(spark, cfg).counts
    assert first == second


def test_restart_from_checkpoint_adds_only_new_events(spark, lake):
    inp, tmp = lake
    cfg = make_cfg(tmp, inp)
    write_file(inp, "f1.jsonl", [ev(1, "submitted", 1), ev(2, "submitted", 2)], 1000)
    run_job(spark, cfg)
    assert ids(spark, cfg) == [f"EVT{n:013d}" for n in (1, 2)]

    # job restarts later: new file contains a replay of event 2 and one genuinely new event
    write_file(inp, "f2.jsonl", [ev(2, "submitted", 2), ev(3, "paid", 3, claim=1)], 2000)
    res = run_job(spark, cfg)
    assert ids(spark, cfg) == [f"EVT{n:013d}" for n in (1, 2, 3)]  # no duplicates, no loss
    assert res.counts[Tables.BRONZE] == 4
    assert (
        table(spark, cfg, Tables.STATUS).where(f"claim_id = 'CLM{1:012d}'").first().status == "paid"
    )


class Boom(RuntimeError):
    pass


@pytest.mark.parametrize("step", ["state", "silver", "late", "dlq"])
def test_crash_mid_batch_then_restart_has_no_duplicates_or_loss(spark, lake, step):
    """Kill the sink partway through batch 1 (after `step` committed), restart, compare to a
    clean single-pass run of the same input: the replayed batch must converge to the same data."""
    inp, tmp = lake
    write_file(inp, "f1.jsonl", [ev(1, "submitted", 1, claim=1)], 1000)
    write_file(
        inp,
        "f2.jsonl",
        [
            ev(2, "paid", 12, claim=1),
            ev(3, "submitted", 3, claim=3),
            "oops",
            ev(2, "paid", 12, claim=1),
        ],
        2000,
    )
    cfg = make_cfg(tmp, inp)
    crashed = {"done": False}

    def hook(name: str, batch_id: int) -> None:
        if name == step and batch_id == 1 and not crashed["done"]:
            crashed["done"] = True
            raise Boom(name)

    with pytest.raises(Exception, match="Boom|boom"):
        run_job(spark, cfg, fault_hook=hook)
    assert crashed["done"]
    run_job(spark, cfg)  # resume from the checkpoint: replays batch 1

    ref_tmp = tmp / "ref"
    ref_tmp.mkdir()
    ref = make_cfg(ref_tmp, inp)
    run_job(spark, ref)
    for name in (Tables.SILVER, Tables.LATE, Tables.DLQ):
        assert ids_or_keys(spark, cfg, name) == ids_or_keys(spark, ref, name), name
    got = {(r.claim_id, r.status) for r in table(spark, cfg, Tables.STATUS).collect()}
    want = {(r.claim_id, r.status) for r in table(spark, ref, Tables.STATUS).collect()}
    assert got == want
    assert (
        table(spark, cfg, Tables.SILVER).count()
        == table(spark, cfg, Tables.SILVER).dropDuplicates(["event_id"]).count()
    )


def ids_or_keys(spark, cfg, name) -> list[str]:
    col = "dlq_key" if name == Tables.DLQ else "event_id"
    return sorted(r[0] for r in table(spark, cfg, name).select(col).collect())


def test_metrics_table_has_throughput_duration_watermark_and_counts(spark, lake):
    inp, tmp = lake
    write_file(inp, "f1.jsonl", [ev(1, "submitted", 1), ev(2, "submitted", 9)], 1000)
    write_file(inp, "f2.jsonl", [ev(3, "submitted", 2), "bad"], 2000)
    cfg = make_cfg(tmp, inp)
    res = run_job(spark, cfg)
    assert res.metrics_rows > 0
    m = table(spark, cfg, Tables.METRICS)
    silver = m.where("query_name = 'silver_events' AND num_input_rows > 0").collect()
    assert {r.batch_id for r in silver} >= {0, 1}
    assert all(r.batch_duration_ms is not None and r.batch_duration_ms >= 0 for r in silver)
    assert any(r.input_rows_per_sec for r in silver) or any(
        r.processed_rows_per_sec for r in silver
    )
    assert any(r.watermark_ts is not None and r.watermark_lag_s is not None for r in silver)
    assert m.where("query_name = 'bronze_events'").count() > 0
    assert metrics_sum(spark, cfg, "dlq_count") == 1
    assert metrics_sum(spark, cfg, "late_count") == 1  # event 3 (hour 2) vs cutoff 8:00 (9 - 1h)
    # one row per (query, batch): a re-run must not double-count
    before = m.count()
    run_job(spark, cfg)
    assert table(spark, cfg, Tables.METRICS).count() == before


def test_generator_stream_with_injected_defects(spark, generated_events, tmp_path):
    """Real generator output (duplicate_event + out_of_order_event defects ON), fed in 8 chunks
    with the default 6h watermark: duplicates collapse to the distinct event ids, every
    out-of-order event is accepted (the generator shifts by at most 4h), nothing is late."""
    lines = [ln for ln in generated_events.read_text(encoding="utf-8").splitlines() if ln]
    events = [json.loads(ln) for ln in lines]
    distinct = {e["event_id"] for e in events}
    assert len(distinct) < len(events), "fixture should contain injected duplicates"
    running, ooo = None, 0
    for e in events:
        if running is not None and e["event_ts"] < running:
            ooo += 1
        running = e["event_ts"] if running is None else max(running, e["event_ts"])
    assert ooo > 0, "fixture should contain out-of-order events"

    inp = tmp_path / "gen"
    inp.mkdir()
    chunk = -(-len(lines) // 8)
    for i in range(8):
        write_file(inp, f"g{i}.jsonl", lines[i * chunk : (i + 1) * chunk], 1000 + i)
    cfg = make_cfg(tmp_path, inp, watermark_delay="6 hours")
    res = run_job(spark, cfg)

    assert res.counts[Tables.BRONZE] == len(lines)
    assert res.counts[Tables.SILVER] == len(distinct)
    assert res.counts[Tables.LATE] == 0 and res.counts[Tables.DLQ] == 0
    # rollup: per claim, the status of its latest event by (event_ts, terminal-beats-submitted)
    rank = {"claim_submitted": 0, "claim_paid": 1, "claim_denied": 1}
    best: dict[str, tuple] = {}
    for e in events:
        k = (e["event_ts"], rank[e["event_type"]], e["event_id"])
        if e["claim_id"] not in best or k > best[e["claim_id"]][0]:
            best[e["claim_id"]] = (k, e["event_type"].removeprefix("claim_"))
    got = {r.claim_id: r.status for r in table(spark, cfg, Tables.STATUS).collect()}
    assert got == {c: v[1] for c, v in best.items()}
