"""Event shape, OpenLineage schema validity, START/COMPLETE/FAIL ordering, run id continuity."""

import json
import re
from datetime import UTC, datetime

import pytest

pytest.importorskip("jsonschema")

from lakehouse.lineage import build_event, ledger_events, quality_events, run_uuid  # noqa: E402
from lakehouse.lineage import dq as DQ  # noqa: E402
from lakehouse.lineage import validate as V  # noqa: E402
from lakehouse.lineage.events import RunSpec, Stats  # noqa: E402
from lakehouse.lineage.topology import layer_io  # noqa: E402

T0 = datetime(2024, 1, 5, 6, 0, tzinfo=UTC)

RFC3339 = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$")


def _by_run(events):
    runs = {}
    for e in events:
        runs.setdefault(e["run"]["runId"], []).append(e)
    return runs


def test_every_ledger_event_is_schema_valid(ledger, roots):
    events = ledger_events(ledger.read(), roots)
    assert events
    for e in events:
        assert V.errors(e) == [], e["job"]["name"]
        assert RFC3339.match(e["eventTime"])


def test_validator_actually_rejects_bad_events(ledger, roots):
    good = ledger_events(ledger.read(), roots)[0]
    bad = json.loads(json.dumps(good))
    del bad["run"]["runId"]
    assert V.errors(bad)
    bad = json.loads(json.dumps(good))
    bad["eventType"] = "BEGIN"
    assert V.errors(bad)
    bad = json.loads(json.dumps(good))
    bad["outputs"][0]["facets"]["schema"]["fields"] = [{"type": "int"}]  # field without a name
    assert any("schema" in e for e in V.errors(bad))
    bad = json.loads(json.dumps(good))
    bad["outputs"][0]["facets"]["dataSource"].pop("_producer")
    assert V.errors(bad)
    with pytest.raises(V.InvalidEvent):
        V.check({"eventType": "START"})


def test_start_precedes_terminal_event_and_run_id_is_continuous(ledger, roots):
    events = ledger_events(ledger.read(), roots)
    runs = _by_run(events)
    assert len(runs) == 4  # bronze, silver, gold reconciliation (2 rows, one run), failed gold
    for evs in runs.values():
        assert [e["eventType"] for e in evs][0] == "START"
        assert len(evs) == 2 and evs[1]["eventType"] in ("COMPLETE", "FAIL")
        assert evs[0]["eventTime"] <= evs[1]["eventTime"]
        assert evs[0]["job"] == evs[1]["job"] or evs[0]["job"]["name"] == evs[1]["job"]["name"]
    times = [e["eventTime"] for e in events]
    assert times == sorted(times)  # globally ordered for replay


def test_run_id_is_the_ledger_run_id_as_uuid(ledger, roots):
    ids = {e["run"]["runId"] for e in ledger_events(ledger.read(), roots)}
    assert run_uuid("1" * 32) in ids and run_uuid("4" * 32) in ids
    assert "11111111-1111-1111-1111-111111111111" in ids


def test_failed_run_emits_fail_with_error_message_and_no_row_counts(ledger, roots):
    evs = _by_run(ledger_events(ledger.read(), roots))[run_uuid("4" * 32)]
    fail = evs[-1]
    assert fail["eventType"] == "FAIL"
    msg = fail["run"]["facets"]["errorMessage"]
    assert "ReconciliationError" in msg["message"] and msg["programmingLanguage"] == "PYTHON"
    assert all("outputFacets" not in o for o in fail["outputs"])


def test_complete_carries_output_statistics_from_the_ledger(ledger, roots):
    silver = _by_run(ledger_events(ledger.read(), roots))[run_uuid("2" * 32)]
    start, done = silver
    assert all("outputFacets" not in o for o in start["outputs"])  # unknown until it finishes
    stats = {
        o["name"].rsplit("/", 1)[-1]: o["outputFacets"]["outputStatistics"]
        for o in done["outputs"]
        if "outputFacets" in o
    }
    # single-row run with table_name == layer: counts go to the run facet, not to a guessed table
    assert stats == {}
    info = done["run"]["facets"]["lakehouse_run"]
    assert (info["rows_in"], info["rows_out"], info["rows_quarantined"], info["bytes_out"]) == (
        100,
        97,
        3,
        2048,
    )


def test_per_table_ledger_rows_become_per_dataset_row_counts(ledger, roots):
    done = _by_run(ledger_events(ledger.read(), roots))[run_uuid("3" * 32)][-1]
    stats = {
        o["name"].rsplit("/", 1)[-1]: o["outputFacets"]["outputStatistics"]["rowCount"]
        for o in done["outputs"]
        if "outputFacets" in o
    }
    assert stats == {"fact_claim": 95, "fact_claim_line": 228}


def test_inputs_and_outputs_follow_the_layer_map(ledger, roots):
    silver_start = _by_run(ledger_events(ledger.read(), roots))[run_uuid("2" * 32)][0]
    ins = {i["name"].rsplit("/", 1)[-1] for i in silver_start["inputs"]}
    outs = {o["name"].rsplit("/", 1)[-1] for o in silver_start["outputs"]}
    assert {"claims", "members", "notes"} <= ins and "silver_quarantine" in outs
    assert all("/bronze/" in i["name"] for i in silver_start["inputs"])
    assert all("/silver/" in o["name"] for o in silver_start["outputs"])


def test_schema_facets_for_gold_come_from_the_contract(ledger, roots):
    start = _by_run(ledger_events(ledger.read(), roots))[run_uuid("4" * 32)][0]
    fc = next(o for o in start["outputs"] if o["name"].endswith("/fact_claim"))
    fields = fc["facets"]["schema"]["fields"]
    assert [f["name"] for f in fields][:3] == ["claim_id", "member_sk", "provider_sk"]
    assert fields[0]["ordinal_position"] == 1
    assert next(f for f in fields if f["name"] == "total_paid_amount")["type"] == "decimal(12,2)"


def test_gold_outputs_carry_column_lineage_pointing_at_the_job_inputs(ledger, roots):
    start = _by_run(ledger_events(ledger.read(), roots))[run_uuid("4" * 32)][0]
    input_ids = {(i["namespace"], i["name"]) for i in start["inputs"]}
    n = 0
    for o in start["outputs"]:
        cl = o["facets"].get("columnLineage")
        if not cl:
            continue
        n += 1
        for col, spec in cl["fields"].items():
            assert spec["inputFields"], col
            for f in spec["inputFields"]:
                if f["field"]:
                    assert (f["namespace"], f["name"]) in input_ids, (o["name"], col, f)
    assert n == 5  # the five star-schema tables; gold_reconciliation/exclusions have no mapping


def test_rebuilding_the_same_ledger_gives_identical_events(ledger, roots):
    a = ledger_events(ledger.read(), roots)
    b = ledger_events(ledger.read(), roots)
    assert json.dumps(a, sort_keys=True) == json.dumps(b, sort_keys=True)


def test_build_event_rejects_unknown_type():
    with pytest.raises(ValueError):
        build_event(RunSpec("r", "j"), "BEGIN", T0)


def test_event_without_datasets_is_still_valid():
    e = build_event(RunSpec("r1", "streaming_job"), "COMPLETE", T0)
    assert e["inputs"] == [] and e["outputs"] == []
    V.check(e)


def test_stats_without_counts_add_no_facet(roots):
    _, outs = layer_io("gold", roots)
    spec = RunSpec("r", "gold", outputs=outs, stats={"fact_claim": Stats()})
    assert all("outputFacets" not in o for o in build_event(spec, "COMPLETE", T0)["outputs"])


# ---- data quality facets (#22 result JSON) ----------------------------------------------------


def _dq_result(success=True):
    return {
        "layer": "silver",
        "root": "/data/e2e/silver",
        "generated_at": "2024-01-05T07:00:00+00:00",
        "success": success,
        "summary": {
            "expectations": 3,
            "passed": 2 if success else 1,
            "failed_blocking": 0 if success else 1,
            "failed_warn": 0,
        },
        "tables": {
            "claims": {
                "path": "claims",
                "status": "validated",
                "rows": 97,
                "expectations": [
                    {
                        "type": "expect_column_values_to_be_unique",
                        "kwargs": {"column": "claim_id"},
                        "severity": "blocking",
                        "success": True,
                        "note": "claim_id is the key",
                    },
                    {
                        "type": "expect_column_values_to_not_be_null",
                        "kwargs": {"column": "member_id"},
                        "severity": "blocking",
                        "success": success,
                        "unexpected_count": 0 if success else 4,
                        "observed_value": None,
                    },
                    {
                        "type": "expect_column_values_to_be_in_set",
                        "kwargs": {"column": "present_ok", "value_set": [True]},
                        "severity": "warn",
                        "success": True,
                    },
                ],
            },
            "notes": {"path": "notes", "status": "skipped", "expectations": []},
        },
    }


def test_quality_facets_are_schema_valid_and_mapped():
    evs = quality_events(_dq_result(), None)
    assert [e["eventType"] for e in evs] == ["START", "COMPLETE"]
    for e in evs:
        V.check(e)
    assert e["job"]["name"] == "quality.silver"
    assert [i["name"] for i in e["inputs"]] == [
        "/data/e2e/silver/claims"
    ]  # skipped table: no dataset
    f = e["inputs"][0]["inputFacets"]
    a = f["dataQualityAssertions"]["assertions"]
    assert [x["assertion"] for x in a] == [
        "column_values_to_be_unique",
        "column_values_to_not_be_null",
        "schema_present_ok",
    ]
    assert (
        a[0]["column"] == "claim_id" and a[0]["severity"] == "error" and a[2]["severity"] == "warn"
    )
    assert "column" not in a[2]  # verdict columns of the schema frame are not dataset columns
    assert f["dataQualityMetrics"]["rowCount"] == 97
    assert f["dataQualityMetrics"]["columnMetrics"] == {"member_id": {"nullCount": 0}}


def test_failed_quality_run_is_a_fail_event_and_ids_are_stable():
    evs = quality_events(_dq_result(success=False), None)
    assert [e["eventType"] for e in evs] == ["START", "FAIL"]
    assert "1 blocking" in evs[1]["run"]["facets"]["errorMessage"]["message"]
    V.check(evs[1])
    assert (
        evs[0]["run"]["runId"] == quality_events(_dq_result(success=False), None)[0]["run"]["runId"]
    )
    assert (
        evs[0]["run"]["runId"]
        != quality_events(_dq_result() | {"generated_at": "2024-01-06T00:00:00+00:00"}, None)[0][
            "run"
        ]["runId"]
    )


def test_load_result_rejects_non_result_files(tmp_path):
    p = tmp_path / "x.json"
    p.write_text("{}")
    with pytest.raises(ValueError):
        DQ.load_result(p)
