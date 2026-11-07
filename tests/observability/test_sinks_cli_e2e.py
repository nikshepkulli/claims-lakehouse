import json
import sys
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pyarrow as pa
import pyarrow.parquet as pq
from conftest import NOW, history

from lakehouse.observability.alerts import dispatch
from lakehouse.observability.checks import evaluate
from lakehouse.observability.cli import main
from lakehouse.observability.config import load_config
from lakehouse.observability.ledger import Ledger, RunRecord
from lakehouse.observability.reconciliation import read_reconciliation, to_run_records
from lakehouse.observability.schema import RUN_SCHEMA

DROP = (1000, 1010, 990, 1005, 995, 300)


def _alerts(now=NOW):
    return evaluate(history(values=DROP), load_config(None), now)


def test_file_sink_and_disabled_webhook(tmp_path, monkeypatch):
    monkeypatch.setenv("LAKEHOUSE_ALERT_WEBHOOK_URL", "http://127.0.0.1:1/never")
    sinks = {
        "log": {"enabled": False},
        "file": {"enabled": True, "path": str(tmp_path / "a.jsonl")},
        "webhook": {"enabled": False},
    }
    sent = dispatch(_alerts(), sinks)
    assert sent["webhook"] == 0 and sent["file"] == 2
    recs = [json.loads(x) for x in (tmp_path / "a.jsonl").read_text().splitlines()]
    assert {r["rule"] for r in recs} == {"volume_zscore", "volume_drift"}


def test_webhook_posts_filtered_by_severity(tmp_path, monkeypatch):
    got = []

    class H(BaseHTTPRequestHandler):
        def do_POST(self):
            got.append(json.loads(self.rfile.read(int(self.headers["Content-Length"]))))
            self.send_response(200)
            self.end_headers()

        def log_message(self, *a):
            pass

    srv = HTTPServer(("127.0.0.1", 0), H)
    threading.Thread(target=srv.handle_request, daemon=True).start()
    monkeypatch.setenv("HOOK", f"http://127.0.0.1:{srv.server_port}/x")
    sinks = {
        "log": {"enabled": False},
        "file": {"enabled": False},
        "webhook": {"enabled": True, "url_env": "HOOK", "min_severity": "critical"},
    }
    sent = dispatch(_alerts(), sinks)
    srv.server_close()
    assert sent["webhook"] == 2 and got[0]["schema_version"] == 1
    assert all(a["severity"] == "critical" for a in got[0]["alerts"])


def test_webhook_failure_does_not_raise(monkeypatch):
    monkeypatch.setenv("HOOK", "http://127.0.0.1:1/x")
    sinks = {
        "log": {"enabled": False},
        "file": {"enabled": False},
        "webhook": {"enabled": True, "url_env": "HOOK", "timeout_s": 1},
    }
    assert dispatch(_alerts(), sinks)["webhook"] == 0


def test_wrap_records_success_failure_and_metrics(tmp_path):
    ops = str(tmp_path / "ops")
    out = tmp_path / "out"
    out.mkdir()
    (out / "f.bin").write_bytes(b"x" * 100)
    code = (
        "import json,os;"
        "json.dump({'rows_out':42,'rows_in':50},open(os.environ['LAKEHOUSE_RUN_METRICS_FILE'],'w'))"
    )
    rc = main(
        [
            "--ops-root",
            ops,
            "wrap",
            "--layer",
            "silver",
            "--job",
            "j",
            "--table",
            "claims",
            "--bytes-path",
            str(out),
            "--",
            sys.executable,
            "-c",
            code,
        ]
    )
    assert rc == 0
    rc = main(
        [
            "--ops-root",
            ops,
            "wrap",
            "--layer",
            "gold",
            "--job",
            "g",
            "--",
            sys.executable,
            "-c",
            "raise SystemExit(3)",
        ]
    )
    assert rc == 3
    rc = main(
        ["--ops-root", ops, "wrap", "--layer", "gold", "--job", "g", "--", "no-such-binary-xyz"]
    )
    assert rc == 127
    rows = {(r["table_name"], r["status"]): r for r in Ledger(ops).read()}
    ok = rows[("claims", "success")]
    assert (ok["rows_out"], ok["rows_in"], ok["bytes_out"]) == (42, 50, 100)
    assert [k for k in rows if k[1] == "failed"] == [("gold", "failed")]
    assert (
        "code 3" in rows[("gold", "failed")]["error"]
        or "cannot execute" in rows[("gold", "failed")]["error"]
    )


def test_wrap_without_command_is_usage_error(tmp_path):
    assert main(["--ops-root", str(tmp_path), "wrap", "--layer", "gold", "--job", "j", "--"]) == 2


def _write_delta_like(d, rows, stale_rows=None):
    d.mkdir()
    (d / "_delta_log").mkdir()
    t = pa.Table.from_pylist(rows)
    pq.write_table(t, d / "live.parquet")
    pq.write_table(pa.Table.from_pylist(stale_rows or rows[:1]), d / "stale.parquet")
    (d / "_delta_log" / "00000000000000000000.json").write_text(
        json.dumps({"add": {"path": "stale.parquet"}})
        + "\n"
        + json.dumps({"add": {"path": "live.parquet"}})
        + "\n"
    )
    (d / "_delta_log" / "00000000000000000001.json").write_text(
        json.dumps({"remove": {"path": "stale.parquet"}}) + "\n"
    )


RECON = [
    {
        "gold_table": "fact_claim_line",
        "batch_id": "b1",
        "silver_rows": 100,
        "gold_rows": 98,
        "excluded_rows": 2,
        "key_violations": 0,
        "identity_ok": True,
        "_run_ts": NOW,
    },
    {
        "gold_table": "dim_member",
        "batch_id": "b1",
        "silver_rows": 10,
        "gold_rows": 9,
        "excluded_rows": 0,
        "key_violations": 0,
        "identity_ok": False,
        "_run_ts": NOW,
    },
]


def test_reconciliation_ingest_replays_delta_log(tmp_path):
    _write_delta_like(tmp_path / "gold_reconciliation", RECON)
    rows, exact = read_reconciliation(tmp_path / "gold_reconciliation")
    assert exact and len(rows) == 2  # stale (removed) file ignored
    recs = {r.table_name: r for r in to_run_records(rows)}
    assert recs["fact_claim_line"].rows_in == 100 and recs["fact_claim_line"].rows_quarantined == 2
    assert recs["fact_claim_line"].status == "success"
    assert recs["dim_member"].status == "failed"


def test_checkpointed_log_is_flagged_inexact(tmp_path):
    d = tmp_path / "r"
    _write_delta_like(d, RECON)
    (d / "_delta_log" / "00000000000000000010.checkpoint.parquet").write_bytes(b"")
    assert read_reconciliation(d)[1] is False


def test_end_to_end_fixture(tmp_path, capsys):
    """Ledger fixture -> check CLI -> alerts file with the documented record schema."""
    ops = tmp_path / "ops"
    led = Ledger(ops)
    # silver: healthy history then a volume collapse; bronze healthy; gold never ran
    for tbl, vals in (("bronze", (500,) * 6), ("silver", DROP)):
        led.append(
            [
                RunRecord(
                    **{
                        k: v
                        for k, v in r.items()
                        if k in RunRecord.__dataclass_fields__ and k != "duration_s"
                    }
                )
                for r in history(tbl, vals, last_ago=0.01)
            ]
        )
        cfgp = tmp_path / "c.yaml"
    alerts = tmp_path / "alerts.jsonl"
    cfgp.write_text(
        "expected_tables: [bronze, silver, gold]\n"
        f"sinks: {{log: {{enabled: false}}, file: {{enabled: true, path: '{alerts}'}}}}\n"
    )
    rc = main(["--config", str(cfgp), "--ops-root", str(ops), "check", "--now", NOW.isoformat()])
    recs = [json.loads(x) for x in alerts.read_text().splitlines()]
    got = {(r["table"], r["rule"], r["severity"]) for r in recs}
    assert ("gold", "freshness_sla", "critical") in got
    assert ("silver", "volume_drift", "critical") in got
    assert not any(r["table"] == "bronze" for r in recs)
    assert rc == 1
    assert (
        main(
            [
                "--config",
                str(cfgp),
                "--ops-root",
                str(ops),
                "check",
                "--fail-on",
                "never",
                "--no-dispatch",
            ]
        )
        == 0
    )
    assert RUN_SCHEMA.names[0] == "run_id"


def test_alerts_file_env_override_moves_the_file_sink(tmp_path, monkeypatch):
    """Airflow keeps the alert file beside its own ledger root (LAKEHOUSE_ALERTS_FILE)."""
    ops = tmp_path / "ops"
    Ledger(ops).append(
        [
            RunRecord(**{k: v for k, v in r.items() if k in RunRecord.__dataclass_fields__})
            for r in history("silver", [1000] * 6)
        ]
    )
    target = tmp_path / "elsewhere" / "alerts.jsonl"
    monkeypatch.setenv("LAKEHOUSE_ALERTS_FILE", str(target))
    monkeypatch.chdir(tmp_path)  # default data/ops/alerts.jsonl must NOT be used
    rc = main(["--ops-root", str(ops), "check", "--now", "2030-01-01T00:00:00+00:00"])
    assert rc == 1  # stale silver and never-run bronze/gold are critical
    assert target.exists() and target.read_text().strip()
    assert not (tmp_path / "data" / "ops" / "alerts.jsonl").exists()
