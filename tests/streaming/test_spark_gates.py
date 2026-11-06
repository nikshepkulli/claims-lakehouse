"""Per-batch data-quality gates in the Spark job (ticket #13), file source.

Each input file is one micro-batch (see test_spark_job.py). Gate math itself is unit-tested without
Spark in test_gates.py; these tests prove the wiring: what is measured, what is written, and what
happens on warn / halt / divert.
"""

from __future__ import annotations

import json
import logging

import pytest
from test_spark_job import ev, ids, make_cfg, table, write_file

pytestmark = pytest.mark.spark

pytest.importorskip("pyspark")

from lakehouse.streaming.gates import GateConfig, GateFailure, Threshold  # noqa: E402
from lakehouse.streaming.spark_job import Tables, run_job  # noqa: E402


@pytest.fixture
def lake(tmp_path):
    inp = tmp_path / "in"
    inp.mkdir()
    return inp, tmp_path


def gate_rows(spark, cfg, batch_id: int) -> dict:
    rows = table(spark, cfg, Tables.GATES).where(f"batch_id = {batch_id}").collect()
    return {r.gate: r for r in rows}


def dq_batch(spark, cfg, batch_id: int):
    return table(spark, cfg, Tables.DQ_BATCHES).where(f"batch_id = {batch_id}").first()


def test_warn_measures_every_family_logs_json_and_keeps_processing(spark, lake, caplog):
    inp, tmp = lake
    write_file(inp, "f1.jsonl", [ev(1, "submitted", 1), ev(2, "submitted", 10)], 1000)
    # batch 1: event 3 is late (cutoff 09:00), event 4 is fine, event 5 has amount = null
    write_file(
        inp,
        "f2.jsonl",
        [
            ev(3, "submitted", 5),
            ev(4, "submitted", 10.5),
            {**ev(5, "submitted", 10.6), "amount": None},
        ],
        2000,
    )
    warn_all = Threshold(0.0, None)  # any occurrence warns, nothing can fail
    gates = GateConfig(
        rates=dict.fromkeys(("contract_failure_rate", "dlq_rate", "late_rate"), warn_all),
        null_rate={"amount": warn_all},
        watermark_lag_seconds=Threshold(1.0, None),  # synthetic 2024 data: always behind
    )
    cfg = make_cfg(tmp, inp, gates=gates)
    with caplog.at_level(logging.INFO, logger="lakehouse.streaming.spark"):
        res = run_job(spark, cfg)

    g = gate_rows(spark, cfg, 1)
    assert (g["late_rate"].numerator, g["late_rate"].denominator) == (1, 2)  # per valid row
    assert (g["dlq_rate"].numerator, g["dlq_rate"].denominator) == (1, 3)
    assert g["contract_failure_rate"].numerator == 1
    assert (g["null_rate:amount"].numerator, g["null_rate:amount"].denominator) == (1, 3)
    assert g["watermark_lag_seconds"].value > 1.0
    assert {r.status for r in g.values()} == {"warn"}
    b = dq_batch(spark, cfg, 1)
    assert (b.status, b.action, b.total_rows, b.valid_rows, b.dlq_rows, b.late_rows) == (
        "warn",
        "none",
        3,
        2,
        1,
        1,
    )
    assert "late_rate" in b.breaches
    assert gate_rows(spark, cfg, 0)["late_rate"].status == "pass"  # first batch: none late

    # warn does not stop or divert anything
    assert ids(spark, cfg) == [f"EVT{n:013d}" for n in (1, 2, 4)]
    assert [r.event_id for r in table(spark, cfg, Tables.LATE).collect()] == [f"EVT{3:013d}"]
    assert res.counts[Tables.DLQ] == 1 and res.counts[Tables.DQ_BATCHES] == 2

    docs = [json.loads(r.message) for r in caplog.records if r.message.startswith("{")]
    warns = [
        d for d in docs if d["event"] == "dq_gate" and d["status"] == "warn" and d["batch_id"] == 1
    ]
    assert {d["gate"] for d in warns} >= {"late_rate", "dlq_rate", "null_rate:amount"}
    summary = [d for d in docs if d["event"] == "dq_batch" and d["batch_id"] == 1][0]
    assert summary["status"] == "warn" and summary["late"] == 1
    assert any(r.levelno == logging.WARNING for r in caplog.records)


def bad_rows(n: int, base: int = 100) -> list[str]:
    return [f"{{garbage {base + i}" for i in range(n)]


def halting_cfg(tmp, inp, fail: float = 0.3, **kw):
    gates = GateConfig(on_fail="halt", rates={"dlq_rate": Threshold(0.1, fail)})
    return make_cfg(tmp, inp, gates=gates, **kw)


def test_fail_with_halt_stops_before_writing_then_replays_after_the_fix(spark, lake):
    inp, tmp = lake
    write_file(inp, "f1.jsonl", [ev(1, "submitted", 1), ev(2, "submitted", 2)], 1000)
    write_file(inp, "f2.jsonl", [ev(3, "submitted", 3), ev(4, "submitted", 4), *bad_rows(2)], 2000)
    cfg = halting_cfg(tmp, inp)

    with pytest.raises(GateFailure) as e:
        run_job(spark, cfg)
    msg = str(e.value)
    assert "DQ_GATE_FAILED" in msg and "batch=1" in msg and "dlq_rate=0.5" in msg

    # batch 0 committed; batch 1 left NOTHING behind in the data tables, only the evidence
    assert ids(spark, cfg) == [f"EVT{n:013d}" for n in (1, 2)]
    assert table(spark, cfg, Tables.DLQ).count() == 0
    b = dq_batch(spark, cfg, 1)
    assert (b.status, b.action, b.diverted_rows) == ("fail", "halt", 0)
    assert gate_rows(spark, cfg, 1)["dlq_rate"].status == "fail"

    # fix = raise the threshold (the real fix would be upstream); the SAME batch is replayed
    fixed = halting_cfg(tmp, inp, fail=0.9)
    res = run_job(spark, fixed)
    assert ids(spark, fixed) == [f"EVT{n:013d}" for n in (1, 2, 3, 4)]
    assert res.counts[Tables.DLQ] == 2
    b = dq_batch(spark, fixed, 1)
    assert (b.status, b.action) == ("warn", "none")  # evidence row upserted, not duplicated
    assert table(spark, fixed, Tables.DQ_BATCHES).count() == 2


def divert_cfg(tmp, inp, **kw):
    gates = GateConfig(on_fail="divert", rates={"dlq_rate": Threshold(0.1, 0.3)})
    return make_cfg(tmp, inp, gates=gates, **kw)


def build_diverted_lake(spark, tmp, inp):
    """batch 0 clean; batch 1 breaches (3 bad of 6) and is diverted; batch 2 clean again."""
    write_file(inp, "f1.jsonl", [ev(1, "submitted", 1), ev(2, "submitted", 2)], 1000)
    write_file(
        inp,
        "f2.jsonl",
        [
            ev(3, "submitted", 3),
            ev(4, "submitted", 20),  # would drag the watermark to 19:00 if the batch counted
            ev(5, "submitted", 3.5),
            *bad_rows(3),
        ],
        2000,
    )
    write_file(inp, "f3.jsonl", [ev(6, "submitted", 3), ev(7, "submitted", 3.2)], 3000)
    cfg = divert_cfg(tmp, inp)
    return cfg, run_job(spark, cfg)


def test_fail_with_divert_sends_the_whole_batch_to_the_dlq_and_continues(spark, lake):
    inp, tmp = lake
    cfg, res = build_diverted_lake(spark, tmp, inp)

    # silver has batches 0 and 2 only; the job did not stop
    assert ids(spark, cfg) == [f"EVT{n:013d}" for n in (1, 2, 6, 7)]
    # the diverted batch did not advance the watermark: events 6 and 7 are NOT late
    assert res.counts[Tables.LATE] == 0
    assert table(spark, cfg, Tables.STATUS).where(f"claim_id = 'CLM{4:012d}'").count() == 0

    dead = table(spark, cfg, Tables.DLQ).collect()
    by = {}
    for r in dead:
        by.setdefault(r.reason_code, []).append(r)
    assert sorted((k, len(v)) for k, v in by.items()) == [
        ("gate_diverted", 3),
        ("malformed_json", 3),
    ]
    one = by["gate_diverted"][0]
    assert "batch 1 diverted" in one.reason_detail and "dlq_rate" in one.reason_detail
    env = json.loads(one.envelope_json)
    assert env["error_type"] == "gate_diverted" and env["stage"] == "consumer"
    # the payload is a canonical event that satisfies the contract again
    from lakehouse.streaming.contract import validate_message

    assert all(validate_message(bytes(r.payload)).ok for r in by["gate_diverted"])
    assert {json.loads(bytes(r.payload))["event_id"] for r in by["gate_diverted"]} == {
        f"EVT{n:013d}" for n in (3, 4, 5)
    }

    b = dq_batch(spark, cfg, 1)
    assert (b.status, b.action, b.diverted_rows, b.dlq_rows) == ("fail", "divert", 3, 3)
    assert dq_batch(spark, cfg, 2).status == "pass"


def test_crash_while_diverting_then_restart_converges(spark, lake):
    inp, tmp = lake
    write_file(inp, "f1.jsonl", [ev(1, "submitted", 1), ev(2, "submitted", 2), *bad_rows(2)], 1000)
    cfg = divert_cfg(tmp, inp)
    fired = {"n": 0}

    def hook(name: str, batch_id: int) -> None:
        if name == "gate_divert" and not fired["n"]:
            fired["n"] = 1
            raise RuntimeError("boom")

    with pytest.raises(Exception, match="boom"):
        run_job(spark, cfg, fault_hook=hook)
    run_job(spark, cfg)
    rows = table(spark, cfg, Tables.DLQ).collect()
    assert len(rows) == 4 and len({r.dlq_key for r in rows}) == 4
    assert table(spark, cfg, Tables.SILVER).count() == 0
