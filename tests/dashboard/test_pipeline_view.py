"""Pipeline runs view (#30): golden numbers against the raw inputs, declared views, honesty.

The unit tests build tiny inputs by hand and need only pyarrow/PyYAML. The integration tests use
``dashboard.sample`` (real observability, lineage and quality CLI runs on the fixture) and compare
the page payload with the raw files read independently of ``dashboard/pipeline.py``.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from dashboard import pipeline

DASH = Path(__file__).resolve().parents[2] / "dashboard"
ROOT = DASH.parent


def _js() -> str:
    return (DASH / "pipeline.js").read_text()


# ---------------------------------------------------------------- unit: pure functions
def _row(run_id, layer, job, table, start, dur, status="success", batch="b1", **kw):
    import datetime as dt

    s = dt.datetime(2026, 1, 1, tzinfo=dt.UTC) + dt.timedelta(seconds=start)
    return {
        "run_id": run_id, "layer": layer, "job": job, "table_name": table, "batch_id": batch,
        "started_at": s.isoformat(), "ended_at": (s + dt.timedelta(seconds=dur)).isoformat(),
        "duration_s": dur, "status": status, "rows_in": kw.get("rows_in"),
        "rows_out": kw.get("rows_out"), "rows_quarantined": kw.get("rows_quarantined"),
        "error": None,
    }  # fmt: skip


def test_timeline_numbers_equal_ledger_rows_and_retries_are_derived():
    rows = [
        _row("a", "silver", "silver_batch", "silver", 0, 12.5, "failed", rows_in=100),
        _row("b", "silver", "silver_batch", "silver", 20, 10.25, rows_in=100, rows_out=97,
             rows_quarantined=3),
        _row("c", "gold", "gold_batch", "gold", 40, 3.0, batch="b2", rows_out=50),
    ]  # fmt: skip
    t = pipeline.timeline_section(rows, None)
    assert [r["run_id"] for r in t["runs"]] == ["a", "b", "c"]
    for got, want in zip(t["runs"], rows, strict=True):
        assert got["duration_s"] == want["duration_s"]
        assert (got["rows_in"], got["rows_out"], got["rows_quarantined"]) == (
            want["rows_in"], want["rows_out"], want["rows_quarantined"])  # fmt: skip
        assert got["status"] == want["status"]
    assert [r["retries"] for r in t["runs"]] == [0, 1, 0]  # 2nd silver_batch/b1 attempt = 1 retry
    assert t["layers"] == ["silver", "gold"] or t["layers"] == ["gold", "silver"]
    assert t["alerts"] is None  # not supplied stays None, never []


def test_alerts_attach_to_their_run_or_the_tables_latest_run():
    rows = [_row("a", "silver", "j", "silver", 0, 1), _row("b", "silver", "j", "silver", 5, 1)]
    alerts = [
        {"alert_id": "x", "severity": "critical", "rule": "run_failed", "table": "silver",
         "message": "m", "run_id": "a"},
        {"alert_id": "y", "severity": "warning", "rule": "freshness_sla", "table": "silver",
         "message": "m", "run_id": None},
    ]  # fmt: skip
    t = pipeline.timeline_section(rows, alerts)
    by = {r["run_id"]: [a["alert_id"] for a in r["alerts"]] for r in t["runs"]}
    assert by == {"a": ["x"], "b": ["y"]}
    assert t["alerts"] == alerts


def test_empty_ledger_is_supplied_but_empty():
    t = pipeline.timeline_section([], None)
    assert t["runs"] == [] and t["layers"] == []


def test_every_input_missing_gives_none_everywhere():
    p = pipeline.pipeline_section()
    assert p["lineage"] is None and p["timeline"] is None and p["trend"] is None
    assert p["views"] == list(pipeline.PIPELINE_VIEWS)
    for key in ("lineage_events", "column_lineage", "runs", "alerts", "quality_history"):
        assert p["commands"][key]


@pytest.mark.parametrize(
    ("ns", "name", "want"),
    [
        ("file://localhost", "/d/e2e/silver/claims", ("silver", "claims")),
        ("file://localhost", "/d/raw/claim_lines", ("raw", "claim_lines")),
        ("file://localhost", "/d/landing/members", ("raw", "members")),
        ("s3://claims-gold", "dim_member", ("gold", "dim_member")),
        ("s3://claims-bronze", "claims/claims", ("bronze", "claims")),
        ("abfss://lake@acct.dfs.core.windows.net", "/lakehouse/other/x", ("other", "x")),
    ],
)
def test_dataset_ref_names_stage_and_table(ns, name, want):
    assert pipeline.dataset_ref(ns, name) == want


def _event(kind, run, job, ins, outs, t="2026-01-01T00:00:00Z"):
    ds = lambda p: {"namespace": "file://localhost", "name": p}  # noqa: E731
    return {"eventType": kind, "eventTime": t, "run": {"runId": run}, "job": {"name": job},
            "inputs": [ds(p) for p in ins], "outputs": [ds(p) for p in outs]}  # fmt: skip


def test_lineage_edges_come_from_final_events_only():
    ev = [
        _event("START", "r1", "j", ["/d/raw/a"], ["/d/gold/x"]),
        _event("COMPLETE", "r1", "j", ["/d/raw/a"], ["/d/gold/x"], "2026-01-01T00:00:05Z"),
        _event("START", "r2", "k", ["/d/gold/x"], []),  # never finished, no outputs: no edge
    ]
    g = pipeline.lineage_section(ev, None)
    obs = {
        (e["src"], e["dst"], e["job"], e["status"]) for e in g["edges"] if e["kind"] == "observed"
    }
    assert obs == {("raw.a", "gold.x", "j", "success")}
    assert {r["status"] for r in g["runs"]} == {"success", "incomplete"}


def test_quality_results_must_be_runner_output(tmp_path):
    bad = tmp_path / "gold-result.json"
    bad.write_text(json.dumps({"hello": 1}))
    with pytest.raises(SystemExit):
        pipeline.trend_section([bad])


def test_trend_counts_pass_and_fail_and_missing_tables(tmp_path):
    def exp(ok, sev="blocking"):
        return {"success": ok, "severity": sev}

    doc = {"layer": "gold", "generated_at": "2026-01-01T00:00:00+00:00", "success": False,
           "tables": {"t1": {"status": "validated", "expectations": [exp(True), exp(False), exp(False, "warn")]},  # noqa: E501
                      "t2": {"status": "missing", "expectations": []}}}  # fmt: skip
    f = tmp_path / "r1" / "gold-result.json"
    f.parent.mkdir()
    f.write_text(json.dumps(doc))
    t = pipeline.trend_section([f])
    (r,) = t["layers"]["gold"]
    assert (r["evaluated"], r["passed"], r["failed_blocking"], r["failed_warn"]) == (3, 1, 1, 1)
    assert r["tables_missing"] == 1 and r["label"] == "r1"
    assert t["layers"]["bronze"] == [] and t["layers"]["silver"] == []


def test_ledger_reads_parquet_and_jsonl_the_same(tmp_path):
    import datetime as dt

    import pyarrow as pa
    import pyarrow.parquet as pq

    row = _row("a", "gold", "g", "gold", 0, 2.5, rows_out=7)
    p = tmp_path / "ledger"
    p.mkdir()
    pq.write_table(
        pa.Table.from_pylist(
            [{**row, "started_at": dt.datetime.fromisoformat(row["started_at"]),
              "ended_at": dt.datetime.fromisoformat(row["ended_at"])}]
        ),
        p / "part-1.parquet",
    )  # fmt: skip
    j = tmp_path / "runs.jsonl"
    j.write_text(json.dumps(row) + "\n")
    assert pipeline.load_ledger(p) == pipeline.load_ledger(j)


# ---------------------------------------------------------------- static: views, honesty, a11y
def test_every_declared_view_has_a_renderer_a_section_and_a_nav_target():
    js = _js()
    assert "PIPELINE_RENDERERS" in js
    for v in pipeline.PIPELINE_VIEWS:
        assert re.search(rf"\b{v}:\s*render\w+", js), f"no renderer registered for {v}"
        assert '"#pipeline/" + v' in js  # in-page jump links use the declared ids
    tpl = (DASH / "template.html").read_text()
    assert 'id="view-pipeline"' in tpl and 'href="#pipeline"' in tpl and 'id="nav-pipeline"' in tpl
    app = (DASH / "app.js").read_text()
    assert '"pipeline"' in app and "pipelineView()" in app and app.count("/*__PIPELINE__*/") == 1
    assert 'throw new Error("pipeline view \'"' in js  # declared view without renderer fails loud


def test_pipeline_js_makes_no_network_or_html_injection():
    js = _js()
    assert not re.search(r"https?://", js)
    assert not re.search(r"\b(fetch|XMLHttpRequest|WebSocket|EventSource|sendBeacon)\b", js)
    assert ".innerHTML" not in js and "import(" not in js


def test_keyboard_reachability_and_status_not_colour_only():
    js = _js()
    for needle in ("tabindex: 0", '"focus"', '"blur"', 'type: "button"', "aria-pressed"):
        assert needle in js, needle
    # nodes, runs and bars are focusable groups with text labels; failures carry a cross and hatch
    assert js.count("tabindex: 0") >= 3
    assert re.search(r"\\u2715|✕", js) and re.search(r"\\u2713|✓", js) and "hatch" in js


def test_narrow_layouts_fall_back_to_lists_and_no_horizontal_overflow_rules():
    js, css = _js(), (DASH / "style.css").read_text()
    assert "w < 860 ? lineageList" in js  # lineage graph -> nested list under 860 px
    assert "w < 640 ? runList" in js  # Gantt -> run list under 640 px
    assert "@media (max-width: 559px)" in css and "grid-template-columns: 1fr;" in css
    block = css[css.index("pipeline runs view") :]
    assert not re.search(r"[;{]\s*min-width:\s*\d{3,}px", block)  # no fixed wide boxes


def test_empty_inputs_say_not_supplied_with_the_command(built):
    p = built["payload"]["pipeline"]
    assert p["lineage"] is None and p["timeline"] is None and p["trend"] is None
    js = _js()
    for key in ("lineage_events", "column_lineage", "runs", "alerts", "quality_history"):
        assert f'"{key}"' in js
    assert "Not supplied. " in js
    assert "lakehouse.observability" in p["commands"]["runs"]
    assert "lakehouse.quality" in p["commands"]["quality_history"]


# ---------------------------------------------------------------- integration: sample run
def test_sample_is_labelled_and_page_has_no_external_requests(pipeline_built):
    html, p = pipeline_built["html"], pipeline_built["pipe"]
    assert p["label"] == "sample run from the synthetic fixture"
    assert 'id="view-pipeline"' in html
    body = re.sub(r'<script type="application/json".*?</script>', "", html, flags=re.S)
    assert not re.search(r"<(script|img|iframe|link)\b[^>]*\b(src|href)=[\"']https?:", body, re.I)
    assert not re.search(r"\b(fetch|XMLHttpRequest|WebSocket|sendBeacon)\b", body)


def test_timeline_equals_the_ledger_rows(pipeline_built, sample):
    import pyarrow.parquet as pq

    ledger = []
    for f in sorted((sample / "ops" / "pipeline_runs").glob("*.parquet")):
        ledger += pq.read_table(f).to_pylist()
    got = {r["run_id"]: r for r in pipeline_built["pipe"]["timeline"]["runs"]}
    assert len(got) == len(ledger) == 4
    for r in ledger:
        g = got[r["run_id"]]
        assert g["duration_s"] == r["duration_s"] and g["status"] == r["status"]
        assert (g["rows_in"], g["rows_out"], g["rows_quarantined"]) == (
            r["rows_in"], r["rows_out"], r["rows_quarantined"])  # fmt: skip
        assert (g["layer"], g["job"], g["table"], g["batch_id"]) == (
            r["layer"], r["job"], r["table_name"], r["batch_id"])  # fmt: skip
    # sample facts that follow from the real runs
    by_job = {(r["job"], r["batch_id"]): r for r in got.values()}
    assert by_job[("claimsgen", "sample-1")]["rows_out"] == 28187
    assert by_job[("gold_fixture", "sample-1")]["rows_in"] == 28187
    assert by_job[("quality_gold", "sample-1")]["status"] == "failed"  # 2 tables missing, real
    assert pipeline_built["pipe"]["timeline"]["layers"]


def test_timeline_row_counts_match_the_files_on_disk(pipeline_built, sample):
    import pyarrow.parquet as pq

    def n(root: Path) -> int:
        return sum(pq.ParquetFile(f).metadata.num_rows for f in root.rglob("*.parquet"))

    runs = {
        r["job"]: r
        for r in pipeline_built["pipe"]["timeline"]["runs"]
        if r["batch_id"] == "sample-1"
    }
    raw = sum(
        n(sample / "raw" / t)
        for t in ("claims", "claim_lines", "members", "providers", "diagnoses")
    )
    gold_tables = (
        "dim_date",
        "dim_member",
        "dim_provider",
        "dim_diagnosis",
        "fact_claim",
        "fact_claim_line",
    )
    gold = sum(n(sample / "gold" / t) for t in gold_tables)
    assert runs["claimsgen"]["rows_out"] == raw and runs["gold_fixture"]["rows_out"] == gold


def test_alerts_marked_on_the_timeline_equal_alerts_file(pipeline_built, sample):
    alerts = [json.loads(x) for x in (sample / "ops" / "alerts.jsonl").read_text().splitlines()]
    t = pipeline_built["pipe"]["timeline"]
    assert t["alerts"] == alerts and alerts  # run_failed + never_succeeded are real here
    marked = sorted(a["alert_id"] for r in t["runs"] for a in r["alerts"])
    assert marked == sorted(a["alert_id"] for a in alerts)


def test_graph_edges_equal_the_events(pipeline_built, sample):
    events = [json.loads(x) for x in (sample / "lineage" / "events.jsonl").read_text().splitlines()]
    final = {}
    for e in events:  # last COMPLETE/FAIL per run
        if e["eventType"] != "START" or e["run"]["runId"] not in final:
            final[e["run"]["runId"]] = e

    def ref(d):
        parts = d["name"].strip("/").split("/")
        stage = "raw" if parts[-2] == "raw" else parts[-2]
        return f"{stage}.{parts[-1]}"

    want = {
        (ref(i), ref(o), e["job"]["name"])
        for e in final.values() for i in e["inputs"] for o in e["outputs"]
    }  # fmt: skip
    g = pipeline_built["pipe"]["lineage"]
    got = {(e["src"], e["dst"], e["job"]) for e in g["edges"] if e["kind"] == "observed"}
    assert got == want and len(want) == 5 * 6  # five landing tables -> six gold tables
    assert {n["id"] for n in g["nodes"] if n["source"] == "events"} >= {s for s, _, _ in want} | {
        d for _, d, _ in want
    }
    assert g["stages"][0] == "raw" and "gold" in g["stages"] and g["stages"][-1] == "dashboard"
    # quality runs annotate gold tables and failures stay visible
    q = {n["id"]: n["quality"] for n in g["nodes"] if n["quality"]}
    assert q["gold.fact_claim"]["status"] == "failed"


def test_declared_edges_cover_marts_and_dashboard(pipeline_built):
    g = pipeline_built["pipe"]["lineage"]
    decl = {(e["src"], e["dst"]) for e in g["edges"] if e["kind"] == "declared"}
    assert ("gold.fact_claim", "marts.mart_denials") in decl
    assert ("marts.mart_denials", "dashboard.dashboard") in decl
    assert ("silver.claims", "gold.fact_claim") in decl


def test_column_map_equals_column_lineage_yaml(pipeline_built):
    import yaml

    doc = yaml.safe_load((ROOT / "lineage" / "column_lineage.yaml").read_text())
    cols = pipeline_built["pipe"]["lineage"]["columns"]
    assert set(cols) == set(doc["tables"])
    for table, spec in doc["tables"].items():
        assert [c["name"] for c in cols[table]] == list(spec)
        for c in cols[table]:
            y = spec[c["name"]]
            assert [f"{s['layer']}.{s['table']}.{s['column']}" for s in c["sources"]] == y.get(
                "sources", []
            )
            assert [f"{s['layer']}.{s['table']}.{s['column']}" for s in c["indirect"]] == y.get(
                "indirect", []
            )
            assert c["masking"] == bool(y.get("masking"))
            assert c["derived"] == y.get("derived")
    # spot check from docs/lineage.md: fact_claim.member_sk <- silver.claims.member_id (masked)
    ms = next(c for c in cols["fact_claim"] if c["name"] == "member_sk")
    assert ms["sources"][0]["column"] == "member_id" and ms["masking"] and ms["indirect"]


def test_quality_trend_counts_equal_result_json(pipeline_built, sample):
    runs = pipeline_built["pipe"]["trend"]["layers"]["gold"]
    assert [r["label"] for r in runs] == ["sample-1", "sample-2"]
    for r in runs:
        doc = json.loads((sample / "dq" / r["label"] / "gold-result.json").read_text())
        exps = [e for t in doc["tables"].values() for e in t["expectations"]]
        assert r["passed"] == sum(bool(e["success"]) for e in exps)
        assert r["evaluated"] == len(exps)
        assert r["generated_at"] == doc["generated_at"]
    assert runs[0]["failed_blocking"] == 0 and runs[0]["tables_missing"] == 2
    assert runs[1]["failed_blocking"] > 0  # the planted defects are caught
    assert pipeline_built["pipe"]["trend"]["layers"]["bronze"] == []  # honest: not run here
