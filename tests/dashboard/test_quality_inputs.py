"""Quality-scorecard inputs: pure logic and the extension-point loaders."""

import json

import pytest

from dashboard import quality


def _inv(cid, violations):
    return {"id": cid, "label": cid, "checked": 10, "violations": violations,
            "status": "pass" if not violations else "fail"}  # fmt: skip


MANIFEST = {
    "defects": {
        "duplicate_claim": {"count": 10, "table": "claims"},
        "null_procedure_code": {"count": 4, "table": "claim_lines"},
        "invalid_procedure_code": {"count": 6, "table": "claim_lines"},
        "late_arrival": {"count": 20, "table": "claims"},
        "duplicate_event": {"count": 5, "table": "events"},
    }
}


def test_catch_table_rates_and_notes():
    rows = {r["defect"]: r for r in quality.defect_catch_table(MANIFEST, [
        _inv("no_duplicate_claim_ids", 0), _inv("no_invalid_procedure_codes", 5),
    ])}  # fmt: skip
    assert rows["duplicate_claim"]["catch_rate"] == 1.0
    assert rows["null_procedure_code"]["still_in_gold"] == 5
    assert rows["null_procedure_code"]["shared_check"] is True
    assert rows["duplicate_claim"]["shared_check"] is False
    assert rows["late_arrival"]["catch_rate"] is None and rows["late_arrival"]["note"]
    assert rows["duplicate_event"]["catch_rate"] is None


def test_catch_rate_never_negative():
    rows = quality.defect_catch_table(
        {"defects": {"duplicate_claim": {"count": 2}}}, [_inv("no_duplicate_claim_ids", 9)]
    )
    assert rows[0]["catch_rate"] == 0.0


def test_load_quality_json_validates(tmp_path):
    good = tmp_path / "q.json"
    good.write_text(json.dumps({"suites": [{"name": "s", "layer": "silver", "evaluated": 3,
                                            "passed": 3, "failed": 0}]}))  # fmt: skip
    assert quality.load_quality_json(good)["suites"][0]["name"] == "s"
    assert quality.load_quality_json(None) is None
    bad = tmp_path / "bad.json"
    bad.write_text(json.dumps({"suites": [{"name": "s"}]}))
    with pytest.raises(ValueError, match="missing"):
        quality.load_quality_json(bad)
    empty = tmp_path / "empty.json"
    empty.write_text("{}")
    with pytest.raises(ValueError, match="non-empty"):
        quality.load_quality_json(empty)


def test_load_reconciliation_json_keeps_documented_columns(tmp_path):
    p = tmp_path / "r.json"
    p.write_text(json.dumps([{"gold_table": "fact_claim", "batch_id": "b1", "silver_rows": 5,
                              "gold_rows": 4, "excluded_rows": 1, "identity_ok": True,
                              "detail": "ignored"}]))  # fmt: skip
    rows = quality.load_reconciliation(p)
    assert rows == [{"gold_table": "fact_claim", "batch_id": "b1", "silver_rows": 5,
                     "gold_rows": 4, "excluded_rows": 1, "identity_ok": True}]  # fmt: skip
    assert quality.load_reconciliation(None) is None


def test_manifest_absent(tmp_path):
    assert quality.load_manifest(tmp_path) is None


def test_short_label_keeps_tail_and_leaves_short_ids():
    from dashboard.build import short_label

    tok = "tok_fx_81314f0a9c2d7e5b6a4f3c1d2e8b9a07"
    assert short_label(tok) == "…" + tok[-6:]
    assert len(short_label(tok)) == 7
    assert short_label("P0001") == "P0001"


def test_load_quality_json_accepts_runner_result(tmp_path):
    """The runner's own gold-result.json: one suite per table, a missing table is not a pass."""
    doc = {
        "layer": "gold",
        "great_expectations_version": "1.11.3",
        "tables": {
            "dim_date": {"status": "validated", "problem": None, "expectations": [
                {"success": True}, {"success": True}, {"success": False}]},
            "gold_exclusions": {"status": "missing", "problem": "required table not found",
                                "expectations": []},
        },
    }  # fmt: skip
    f = tmp_path / "gold-result.json"
    f.write_text(json.dumps(doc))
    out = quality.load_quality_json(f)
    assert "great_expectations 1.11.3" in out["generated_by"]
    by = {s["name"]: s for s in out["suites"]}
    assert (by["gold.dim_date"]["evaluated"], by["gold.dim_date"]["passed"]) == (3, 2)
    assert by["gold.dim_date"]["failed"] == 1
    assert by["gold.gold_exclusions"]["status"] == "missing"
    assert by["gold.gold_exclusions"]["evaluated"] == 0
