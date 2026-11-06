"""Gate math, threshold semantics and YAML validation (ticket #13). No Spark."""

from __future__ import annotations

import json

import pytest
import yaml

from lakehouse.streaming import gates as g
from lakehouse.streaming.gates import (
    BatchStats,
    GateConfig,
    GateConfigError,
    Threshold,
    evaluate,
    load_gate_config,
    parse_gate_config,
)


def cfg(**kw) -> GateConfig:
    base = {
        "rates": {
            "dlq_rate": Threshold(0.02, 0.10),
            "late_rate": Threshold(0.01, 0.05),
            "contract_failure_rate": Threshold(0.01, 0.05),
            "duplicate_rate": Threshold(0.10, 0.30),
        },
        "min_batch_rows": 1,
    }
    return GateConfig(**{**base, **kw})


def by_gate(report):
    return {r.gate: r for r in report.results}


def test_threshold_is_strictly_greater_than():
    t = Threshold(0.05, 0.10)
    assert t.status(0.05) == g.PASS  # equal is not a breach
    assert t.status(0.0500001) == g.WARN
    assert t.status(0.10) == g.WARN
    assert t.status(0.1000001) == g.FAIL
    assert (
        Threshold(None, 0.0).status(0.0) == g.PASS and Threshold(None, 0.0).status(1e-9) == g.FAIL
    )
    assert Threshold(None, None).status(1.0) == g.PASS  # both levels disabled


def test_rates_use_the_documented_denominators():
    s = BatchStats(
        total_rows=200,
        valid_rows=180,
        parsed_rows=190,
        contract_failures=10,
        dlq_rows=20,
        late_rows=9,
        duplicate_rows=18,
    )
    r = g.rates(s)
    assert r["contract_failure_rate"][0] == pytest.approx(10 / 200)
    assert r["dlq_rate"][0] == pytest.approx(20 / 200)
    assert r["late_rate"][0] == pytest.approx(9 / 180)  # per VALID row
    assert r["duplicate_rate"][0] == pytest.approx(18 / 180)
    assert r["late_rate"][1:] == (9, 180)


def test_empty_batch_and_zero_denominators_are_zero_not_errors():
    rep = evaluate(BatchStats(total_rows=0), cfg(), query="q", batch_id=0)
    assert rep.status == g.PASS and all(r.value == 0.0 for r in rep.results)
    # all rows dead-lettered: no valid rows, so late/duplicate rates have a 0 denominator
    rep = evaluate(BatchStats(total_rows=50, dlq_rows=50), cfg(), query="q", batch_id=1)
    assert by_gate(rep)["late_rate"].value == 0.0
    assert by_gate(rep)["dlq_rate"].status == g.FAIL


def test_overall_status_is_the_worst_gate_and_action_follows_on_fail():
    s = BatchStats(total_rows=100, valid_rows=97, dlq_rows=3, contract_failures=0)
    rep = evaluate(s, cfg(), query="q", batch_id=4)
    assert by_gate(rep)["dlq_rate"].status == g.WARN  # 3% > 2% warn, < 10% fail
    assert rep.status == g.WARN and rep.action == "none"

    s = BatchStats(total_rows=100, valid_rows=80, dlq_rows=20, contract_failures=20)
    assert evaluate(s, cfg(on_fail="halt"), query="q", batch_id=5).action == "halt"
    assert evaluate(s, cfg(on_fail="divert"), query="q", batch_id=5).action == "divert"


def test_rate_gates_skip_small_batches_but_lag_does_not():
    c = cfg(min_batch_rows=20, watermark_lag_seconds=Threshold(60, 600))
    s = BatchStats(total_rows=5, valid_rows=4, dlq_rows=1, watermark_lag_seconds=900)
    rep = evaluate(s, c, query="q", batch_id=0)
    assert by_gate(rep)["dlq_rate"].status == g.SKIPPED
    assert by_gate(rep)["watermark_lag_seconds"].status == g.FAIL
    assert rep.status == g.FAIL


def test_no_valid_rows_means_no_watermark_lag_to_judge():
    c = cfg(watermark_lag_seconds=Threshold(None, 10))
    rep = evaluate(BatchStats(total_rows=3, dlq_rows=3), c, query="q", batch_id=0)
    assert by_gate(rep)["watermark_lag_seconds"].status == g.SKIPPED


def test_null_rate_is_per_field_over_parseable_rows():
    c = cfg(null_rate={"amount": Threshold(0.0, 0.2), "claim_id": Threshold(0.0, 0.2)})
    s = BatchStats(
        total_rows=110,
        valid_rows=90,
        parsed_rows=100,  # 10 malformed rows are not parseable and not in the denominator
        null_counts={"amount": 30, "claim_id": 5},
    )
    rep = by_gate(evaluate(s, c, query="q", batch_id=0))
    assert rep["null_rate:amount"].value == pytest.approx(0.30)
    assert rep["null_rate:amount"].status == g.FAIL
    assert rep["null_rate:claim_id"].status == g.WARN  # 5% > warn 0, <= fail 20%
    # nothing parseable -> nothing to judge
    none_parsed = evaluate(BatchStats(total_rows=50, dlq_rows=50), c, query="q", batch_id=1)
    assert by_gate(none_parsed)["null_rate:amount"].status == g.SKIPPED


def test_disabled_levels_and_absent_gates():
    c = GateConfig(rates={"late_rate": Threshold(None, None)})
    rep = evaluate(BatchStats(total_rows=10, valid_rows=10, late_rows=10), c, query="q", batch_id=0)
    assert [r.gate for r in rep.results] == ["late_rate"] and rep.status == g.PASS


def test_failure_message_and_structured_log_lines():
    s = BatchStats(total_rows=100, valid_rows=60, dlq_rows=40, contract_failures=40, late_rows=0)
    rep = evaluate(s, cfg(), query="silver_events", batch_id=7)
    msg = rep.failure_message()
    assert msg.startswith(g.FAILURE_MARKER) and "batch=7" in msg and "dlq_rate=0.4" in msg
    recs = rep.log_records()
    levels = {json.loads(line)["gate"]: lvl for lvl, line in recs if "gate" in json.loads(line)}
    assert levels["dlq_rate"] == "error" and levels["late_rate"] == "info"
    last = json.loads(recs[-1][1])
    assert last["event"] == "dq_batch" and last["status"] == "fail" and last["action"] == "halt"
    assert last["rows"] == 100 and last["dlq"] == 40


def test_evaluation_is_deterministic():
    s = BatchStats(total_rows=100, valid_rows=90, dlq_rows=10, late_rows=2, duplicate_rows=1)
    assert evaluate(s, cfg(), query="q", batch_id=1) == evaluate(s, cfg(), query="q", batch_id=1)


# ---------------------------------------------------------------- YAML validation


def valid_doc() -> dict:
    return {
        "version": 1,
        "on_fail": "divert",
        "min_batch_rows": 10,
        "gates": {
            "dlq_rate": {"warn": 0.01, "fail": 0.1},
            "null_rate": {"fields": {"amount": {"warn": 0, "fail": 0.5}}},
            "watermark_lag_seconds": {"fail": 3600},
        },
    }


def test_parse_valid_document():
    c = parse_gate_config(valid_doc())
    assert c.on_fail == "divert" and c.min_batch_rows == 10
    assert c.rates["dlq_rate"] == Threshold(0.01, 0.1)
    assert c.null_rate["amount"] == Threshold(0.0, 0.5)
    assert c.watermark_lag_seconds == Threshold(None, 3600.0)
    assert c.describe()["null_rate:amount"] == [0.0, 0.5]


@pytest.mark.parametrize(
    ("mutate", "expect"),
    [
        (lambda d: d.update(on_fail="explode"), "on_fail"),
        (lambda d: d.update(min_batch_rows=0), "min_batch_rows"),
        (lambda d: d.update(version=2), "version"),
        (lambda d: d.update(surprise=1), "unknown top-level key"),
        (lambda d: d["gates"].update(typo_rate={"fail": 0.1}), "unknown gate 'typo_rate'"),
        (lambda d: d["gates"].update(dlq_rate={"warn": 0.5, "fail": 0.1}), "warn (0.5) must not"),
        (lambda d: d["gates"].update(dlq_rate={"fail": 1.5}), "outside [0, 1]"),
        (lambda d: d["gates"].update(dlq_rate={"fail": -0.1}), "outside [0, 1]"),
        (lambda d: d["gates"].update(dlq_rate={"fail": "high"}), "must be a number"),
        (lambda d: d["gates"].update(dlq_rate={"fail": True}), "must be a number"),
        (lambda d: d["gates"].update(dlq_rate={}), "expected a mapping"),
        (lambda d: d["gates"].update(dlq_rate={"warn": None}), "at least one"),
        (lambda d: d["gates"].update(dlq_rate={"fial": 0.1}), "expected a mapping"),
        (lambda d: d["gates"]["null_rate"]["fields"].update(nope={"fail": 0.1}), "unknown event"),
        (lambda d: d["gates"].update(null_rate={"amount": {"fail": 0.1}}), "null_rate: expected"),
        (lambda d: d["gates"].update(watermark_lag_seconds={"fail": -5}), "outside >= 0"),
        (lambda d: d.update(gates={}), "non-empty"),
    ],
)
def test_invalid_documents_are_rejected_with_a_clear_message(mutate, expect):
    doc = valid_doc()
    mutate(doc)
    with pytest.raises(GateConfigError) as e:
        parse_gate_config(doc)
    assert expect in str(e.value)


def test_all_problems_are_reported_together():
    doc = valid_doc()
    doc["on_fail"] = "nope"
    doc["gates"]["dlq_rate"] = {"fail": 9}
    with pytest.raises(GateConfigError) as e:
        parse_gate_config(doc)
    assert "on_fail" in str(e.value) and "outside [0, 1]" in str(e.value)


def test_non_mapping_document():
    with pytest.raises(GateConfigError, match="mapping"):
        parse_gate_config(["not", "a", "mapping"])


def test_load_errors_name_the_file(tmp_path):
    with pytest.raises(GateConfigError, match="cannot read"):
        load_gate_config(tmp_path / "missing.yaml")
    bad = tmp_path / "bad.yaml"
    bad.write_text("gates: [unclosed", encoding="utf-8")
    with pytest.raises(GateConfigError, match="not valid YAML"):
        load_gate_config(bad)
    wrong = tmp_path / "wrong.yaml"
    wrong.write_text(yaml.safe_dump({"on_fail": "halt", "gates": {"x": {}}}), encoding="utf-8")
    with pytest.raises(GateConfigError, match="wrong.yaml"):
        load_gate_config(wrong)


def test_shipped_config_is_valid_and_sensible():
    c = load_gate_config()  # config/streaming_gates.yaml
    assert c.on_fail in g.ON_FAIL_ACTIONS
    assert set(c.rates) == set(g.RATE_GATES)
    assert set(c.null_rate) <= set(g.EVENT_FIELDS)
    for t in [*c.rates.values(), *c.null_rate.values()]:
        assert t.warn is None or t.fail is None or t.warn <= t.fail
