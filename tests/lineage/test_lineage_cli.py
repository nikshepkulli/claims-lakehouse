"""End to end through the real CLI in a subprocess: wrap, ledger replay, quality, nesting."""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from lakehouse.lineage import run_uuid

ROOT = Path(__file__).resolve().parents[2]


def run_cli(*args, env=None, module="lakehouse.lineage"):
    e = dict(os.environ, PYTHONPATH=str(ROOT / "src"))
    e.pop("LAKEHOUSE_RUN_ID", None)
    e.pop("MARQUEZ_URL", None)
    e.update(env or {})
    return subprocess.run(  # noqa: S603
        [sys.executable, "-m", module, *args], cwd=ROOT, env=e, capture_output=True, text=True
    )


def events(path):
    return [json.loads(x) for x in Path(path).read_text().splitlines()]


def summary(proc):
    lines = [json.loads(x) for x in proc.stderr.splitlines() if x.startswith("{")]
    return next(x for x in reversed(lines) if x["event"] == "lineage_summary")


@pytest.fixture
def out(tmp_path):
    return tmp_path / "events.jsonl"


def test_wrap_emits_start_then_complete_with_one_run_id_and_passes_env_to_child(tmp_path, out):
    marker = tmp_path / "child_run_id"
    code = (
        "import os,pathlib; "
        f"pathlib.Path({str(marker)!r}).write_text(os.environ['LAKEHOUSE_RUN_ID'])"
    )
    r = run_cli(
        "wrap", "--job", "bronze", "--layer", "bronze", "--bronze", str(tmp_path / "bronze"),
        "--landing", str(tmp_path / "raw"), "--file", str(out), "--validate",
        "--", sys.executable, "-c", code,
    )  # fmt: skip
    assert r.returncode == 0, r.stderr
    start, done = events(out)
    assert (start["eventType"], done["eventType"]) == ("START", "COMPLETE")
    assert start["run"]["runId"] == done["run"]["runId"] == run_uuid(marker.read_text())
    assert start["job"]["name"] == "bronze"
    assert {o["name"].rsplit("/", 1)[-1] for o in done["outputs"]} >= {"claims", "events"}
    assert all("/raw/" in i["name"] for i in done["inputs"])
    s = summary(r)
    assert s["failed"] == 0 and s["delivered"] == 2


def test_wrap_reuses_an_existing_run_id(tmp_path, out):
    rid = "ab" * 16
    r = run_cli("wrap", "--job", "x", "--file", str(out), "--", sys.executable, "-c", "pass",
                env={"LAKEHOUSE_RUN_ID": rid})  # fmt: skip
    assert r.returncode == 0
    assert {e["run"]["runId"] for e in events(out)} == {run_uuid(rid)}


def test_wrap_failing_command_emits_fail_and_passes_exit_code_through(tmp_path, out):
    r = run_cli("wrap", "--job", "silver_batch", "--file", str(out), "--",
                sys.executable, "-c", "import sys; sys.exit(7)")  # fmt: skip
    assert r.returncode == 7
    start, fail = events(out)
    assert (start["eventType"], fail["eventType"]) == ("START", "FAIL")
    assert "code 7" in fail["run"]["facets"]["errorMessage"]["message"]
    assert start["run"]["runId"] == fail["run"]["runId"]


def test_wrap_missing_executable_is_fail_and_127(tmp_path, out):
    r = run_cli("wrap", "--job", "j", "--file", str(out), "--", "/nonexistent/binary")
    assert r.returncode == 127
    assert [e["eventType"] for e in events(out)] == ["START", "FAIL"]


def test_wrap_without_command_is_usage_error(out):
    assert run_cli("wrap", "--job", "j", "--file", str(out)).returncode == 2
    assert not out.exists()


def test_wrap_with_dead_marquez_still_returns_zero_and_counts_failures(tmp_path, out):
    r = run_cli(
        "wrap", "--job", "gold", "--transport", "http", "--transport", "file",
        "--marquez-url", "http://127.0.0.1:9", "--file", str(out),
        "--", sys.executable, "-c", "print('job output')",
    )  # fmt: skip
    assert r.returncode == 0
    assert "job output" in r.stdout
    s = summary(r)
    assert s["failed"] == 2 and s["delivered"] == 2  # http failed twice, file delivered twice
    assert s["by_transport"]["http"]["failed"] == 2
    assert [e["eventType"] for e in events(out)] == ["START", "COMPLETE"]
    assert "lineage_emit_failed" in r.stderr


def test_wrap_posts_to_marquez_stand_in(tmp_path, make_marquez):
    m = make_marquez()
    r = run_cli("wrap", "--job", "gold", "--transport", "http", "--marquez-url", m.url,
                "--", sys.executable, "-c", "pass")  # fmt: skip
    assert r.returncode == 0
    assert [b["eventType"] for _, _, b in m.requests] == ["START", "COMPLETE"]
    assert {p for p, _, _ in m.requests} == {"/api/v1/lineage"}


def test_bad_dataset_uri_disables_lineage_but_still_runs_the_job(tmp_path, out):
    marker = tmp_path / "ran"
    r = run_cli("wrap", "--job", "j", "--inputs", "ftp://nope/x", "--file", str(out), "--",
                sys.executable, "-c", f"open({str(marker)!r}, 'w').write('1')")  # fmt: skip
    assert r.returncode == 0 and marker.exists()
    assert "lineage_disabled" in r.stderr
    assert not out.exists()


def test_wrap_explicit_datasets_stats_and_gold_column_lineage(tmp_path, out):
    silver, gold = tmp_path / "silver", tmp_path / "gold"
    r = run_cli(
        "wrap", "--job", "gold_batch",
        "--inputs", f"silver={silver}/claims", f"silver={silver}/claim_lines",
        "--outputs", f"{gold}/fact_claim",
        "--rows-out", "95", "--bytes-out", "4096", "--file", str(out), "--validate",
        "--", sys.executable, "-c", "pass",
    )  # fmt: skip
    assert r.returncode == 0, r.stderr
    _, done = events(out)
    stats = done["outputs"][0]["outputFacets"]["outputStatistics"]
    assert (stats["rowCount"], stats["size"]) == (95, 4096)
    cl = done["outputs"][0]["facets"]["columnLineage"]["fields"]
    assert cl["total_paid_amount"]["inputFields"][0]["name"] == f"{silver}/claims"
    assert summary(r)["failed"] == 0


def test_wrap_reads_metrics_file_written_by_the_child(tmp_path, out):
    code = (
        "import json,os; open(os.environ['LAKEHOUSE_RUN_METRICS_FILE'],'w')"
        ".write(json.dumps({'rows_out': 11, 'rows_in': 12}))"
    )
    metrics = tmp_path / "m.json"
    r = run_cli(
        "wrap", "--job", "j", "--outputs", f"{tmp_path}/silver/claims", "--file", str(out),
        "--", sys.executable, "-c", code,
        env={"LAKEHOUSE_RUN_METRICS_FILE": str(metrics)},
    )  # fmt: skip
    assert r.returncode == 0, r.stderr
    done = events(out)[-1]
    assert done["outputs"][0]["outputFacets"]["outputStatistics"]["rowCount"] == 11
    assert done["run"]["facets"]["lakehouse_run"]["rows_in"] == 12


def test_wrap_attaches_dq_facets_to_matching_inputs_on_complete(tmp_path, out):
    dq = tmp_path / "silver-result.json"
    dq.write_text(json.dumps({
        "layer": "silver", "root": "x", "success": True,
        "generated_at": "2024-01-05T07:00:00+00:00",
        "summary": {"expectations": 1, "passed": 1, "failed_blocking": 0, "failed_warn": 0},
        "tables": {"claims": {"path": "claims", "status": "validated", "rows": 5, "expectations": [
            {"type": "expect_column_values_to_be_unique", "kwargs": {"column": "claim_id"},
             "severity": "blocking", "success": True}]}},
    }))  # fmt: skip
    r = run_cli("wrap", "--job", "gold_batch", "--inputs", f"silver={tmp_path}/silver/claims",
                "--dq-result", str(dq), "--file", str(out), "--validate", "--",
                sys.executable, "-c", "pass")  # fmt: skip
    assert r.returncode == 0, r.stderr
    start, done = events(out)
    assert "inputFacets" not in start["inputs"][0]
    assert done["inputs"][0]["inputFacets"]["dataQualityMetrics"]["rowCount"] == 5


def test_nested_inside_observability_wrap_shares_one_run_id_and_reads_its_metrics(tmp_path, out):
    ops = tmp_path / "ops"
    code = (
        "import json,os; open(os.environ['LAKEHOUSE_RUN_METRICS_FILE'],'w')"
        ".write(json.dumps({'rows_out': 42}))"
    )
    r = run_cli(
        "--ops-root", str(ops), "wrap", "--layer", "silver", "--job", "silver_batch", "--",
        sys.executable, "-m", "lakehouse.lineage", "wrap", "--job", "silver_batch",
        "--outputs", f"{tmp_path}/silver/claims", "--file", str(out), "--",
        sys.executable, "-c", code,
        module="lakehouse.observability",
    )  # fmt: skip
    assert r.returncode == 0, r.stderr
    from lakehouse.observability.ledger import Ledger

    (row,) = Ledger(ops).read()
    start, done = events(out)
    assert start["run"]["runId"] == done["run"]["runId"] == run_uuid(row["run_id"])
    assert done["outputs"][0]["outputFacets"]["outputStatistics"]["rowCount"] == 42
    assert row["rows_out"] == 42  # observability saw the same metrics


def test_lineage_outside_observability_reads_counts_from_the_ledger(tmp_path, out):
    ops = tmp_path / "ops"
    code = (
        "import json,os; open(os.environ['LAKEHOUSE_RUN_METRICS_FILE'],'w')"
        ".write(json.dumps({'rows_out': 9}))"
    )
    r = run_cli(
        "wrap", "--job", "silver_batch", "--ops-root", str(ops),
        "--outputs", f"{tmp_path}/silver/claims", "--file", str(out), "--",
        sys.executable, "-m", "lakehouse.observability", "--ops-root", str(ops), "wrap",
        "--layer", "silver", "--job", "silver_batch", "--table", "claims", "--",
        sys.executable, "-c", code,
    )  # fmt: skip
    assert r.returncode == 0, r.stderr
    from lakehouse.observability.ledger import Ledger

    (row,) = Ledger(ops).read()
    start, done = events(out)
    assert start["run"]["runId"] == run_uuid(row["run_id"])  # the ledger row carries OUR run id
    assert done["outputs"][0]["outputFacets"]["outputStatistics"]["rowCount"] == 9


def test_emit_replays_ledger_and_is_idempotent(ledger, roots, out):
    base = ["emit", "--ops-root", str(ledger.path.parent), "--file", str(out), "--validate"]
    for name in ("silver", "gold", "bronze"):
        base += [f"--{name}", getattr(roots, name)]
    base += ["--landing", roots.landing]
    r1 = run_cli(*base)
    assert r1.returncode == 0, r1.stderr
    first = out.read_text()
    assert len(first.splitlines()) == 8 and summary(r1)["failed"] == 0
    r2 = run_cli(*base)
    assert r2.returncode == 0
    assert out.read_text() == first  # same run, same events: nothing appended
    assert summary(r2)["skipped_duplicates"] == 8 and summary(r2)["delivered"] == 0


def test_emit_filters_by_run_id_and_since(ledger, roots, out):
    r = run_cli("emit", "--ops-root", str(ledger.path.parent), "--file", str(out),
                "--run-id", "4" * 32)  # fmt: skip
    assert r.returncode == 0
    assert [e["eventType"] for e in events(out)] == ["START", "FAIL"]
    out.unlink()
    run_cli("emit", "--ops-root", str(ledger.path.parent), "--file", str(out),
            "--since", "2024-01-05T06:25:00+00:00")  # fmt: skip
    assert {e["job"]["name"] for e in events(out)} == {"gold_batch"}


def test_emit_exit_code_only_fails_on_transport_error_when_asked(ledger, out):
    args = ["emit", "--ops-root", str(ledger.path.parent), "--transport", "http",
            "--marquez-url", "http://127.0.0.1:9"]  # fmt: skip
    r = run_cli(*args)
    assert r.returncode == 0 and summary(r)["failed"] > 0
    assert run_cli(*args, "--fail-on-transport-error").returncode == 1


def test_quality_command_emits_run_with_dq_facets(tmp_path, out):
    dq = tmp_path / "gold-result.json"
    dq.write_text(json.dumps({
        "layer": "gold", "root": str(tmp_path / "gold"),
        "generated_at": "2024-01-05T07:00:00+00:00",
        "success": False, "summary": {"failed_blocking": 2},
        "tables": {"fact_claim": {"path": "fact_claim", "status": "validated", "rows": 3,
                                  "expectations": []}},
    }))  # fmt: skip
    r = run_cli("quality", "--result", str(dq), "--file", str(out), "--validate")
    assert r.returncode == 0, r.stderr
    start, end = events(out)
    assert (start["job"]["name"], end["eventType"]) == ("quality.gold", "FAIL")
    assert start["inputs"][0]["name"].endswith("/gold/fact_claim")


def test_check_mapping_command_passes_on_repo_mapping():
    r = run_cli("check-mapping")
    assert r.returncode == 0 and "OK" in r.stdout
