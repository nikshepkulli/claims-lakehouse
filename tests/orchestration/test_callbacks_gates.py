from __future__ import annotations

import json
import logging
from types import SimpleNamespace

import pytest
from claims_dags import callbacks, gates


def _ctx():
    ti = SimpleNamespace(task_id="silver", try_number=4, max_tries=3, log_url="http://x/log")
    return {
        "task_instance": ti,
        "dag": SimpleNamespace(dag_id="claims_daily"),
        "ds": "2024-01-05",
        "run_id": "scheduled__2024-01-05",
        "exception": RuntimeError("boom"),
    }


def test_build_alert_is_json_safe_and_structured():
    a = callbacks.build_alert("task_failed", _ctx())
    assert a["event"] == "claims_pipeline_task_failed"
    assert (a["dag_id"], a["task_id"], a["logical_date"]) == (
        "claims_daily",
        "silver",
        "2024-01-05",
    )
    assert a["exception"] == "RuntimeError: boom"
    json.dumps(a)


def test_on_failure_logs_one_json_line_without_webhook(caplog, monkeypatch):
    monkeypatch.delenv("CLAIMS_ALERT_WEBHOOK_URL", raising=False)
    with caplog.at_level(logging.ERROR, logger="claims_dags.alerts"):
        callbacks.on_failure(_ctx())
    (rec,) = caplog.records
    payload = json.loads(rec.getMessage().removeprefix("ALERT "))
    assert payload["task_id"] == "silver"


def test_webhook_failure_never_raises(caplog, monkeypatch):
    monkeypatch.setenv("CLAIMS_ALERT_WEBHOOK_URL", "http://127.0.0.1:1/hook")
    callbacks.on_failure(_ctx())  # connection refused is swallowed
    monkeypatch.setenv("CLAIMS_ALERT_WEBHOOK_URL", "file:///etc/passwd")
    callbacks.on_failure(_ctx())  # non-http scheme rejected, not opened


def test_webhook_posts_json(monkeypatch):
    seen = {}

    class _Resp:
        def close(self):
            pass

    def fake_urlopen(req, timeout):
        seen["url"], seen["body"] = req.full_url, json.loads(req.data)
        return _Resp()

    monkeypatch.setattr(callbacks.urllib.request, "urlopen", fake_urlopen)
    callbacks.emit({"event": "e", "task_id": "t"}, url="https://hooks.example/x")
    assert seen["url"] == "https://hooks.example/x" and seen["body"]["task_id"] == "t"


def test_sla_miss_callback_logs(caplog):
    sla = SimpleNamespace(task_id="gold", execution_date="2024-01-05")
    with caplog.at_level(logging.ERROR, logger="claims_dags.alerts"):
        callbacks.on_sla_miss(SimpleNamespace(dag_id="d"), "gold", "", [sla], [])
    assert "claims_pipeline_sla_miss" in caplog.text


# ------------------------------------------------------------------------------------- gates
pa = pytest.importorskip("pyarrow")


def _write(export, name, table):
    import pyarrow.parquet as pq

    (export / name).mkdir(parents=True)
    pq.write_table(pa.table(table), export / name / "part-0.parquet")


def _gold(tmp_path, claim_ids=("c1", "c2"), line_ids=("c1", "c2"), batch="b20240105"):
    exp = tmp_path / "gold" / "export"
    for t in ("dim_date", "dim_member", "dim_provider", "dim_diagnosis"):
        _write(exp, t, {"k": [1]})
    _write(exp, "fact_claim", {"claim_id": list(claim_ids), "load_batch_id": [batch] * 2})
    _write(exp, "fact_claim_line", {"claim_id": list(line_ids)})
    return tmp_path / "gold"


def _raw(tmp_path, date="2024-01-05"):
    (tmp_path / "raw" / "claims" / f"ingest_date={date}").mkdir(parents=True, exist_ok=True)
    return tmp_path / "raw"


def test_gates_pass(tmp_path):
    assert gates.run_gates(_gold(tmp_path), _raw(tmp_path), "2024-01-05") == []
    assert gates.main(["--gold", str(tmp_path / "gold"), "--batch-date", "2024-01-05"]) == 0


def test_gates_catch_duplicates_orphans_and_empty_batch(tmp_path):
    bad = gates.run_gates(_gold(tmp_path, ("c1", "c1"), ("c9",)), _raw(tmp_path), "2024-01-05")
    assert any("duplicate" in b for b in bad) and any("missing in fact_claim" in b for b in bad)
    bad = gates.run_gates(_gold(tmp_path / "o", batch="b20240101"), _raw(tmp_path), "2024-01-05")
    assert any("no rows for b20240105" in b for b in bad)
    # no raw partition for the date: an empty batch is legitimate
    assert gates.run_gates(_gold(tmp_path / "p", batch="b20240101"), None, "2024-01-05") == []


def test_gates_missing_table_and_bad_date(tmp_path):
    assert any("missing export table" in b for b in gates.run_gates(tmp_path, None, "2024-01-05"))
    assert gates.main(["--gold", str(tmp_path), "--batch-date", "nope"]) == 2
