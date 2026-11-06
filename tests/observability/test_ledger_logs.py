import json
import logging
from datetime import UTC, datetime, timedelta

import pytest

from lakehouse.observability.ledger import Ledger, RunRecord
from lakehouse.observability.logs import JsonFormatter, emit, get_logger
from lakehouse.observability.schema import LOG_KEYS, RUN_COLUMNS, SCHEMA_VERSION
from lakehouse.observability.tracker import track_run, tracked


def _rec(**kw):
    t0 = datetime(2024, 1, 1, tzinfo=UTC)
    base = dict(  # noqa: C408
        run_id="a",
        layer="silver",
        job="j",
        started_at=t0,
        ended_at=t0 + timedelta(seconds=90),
        status="success",
        rows_out=5,
    )
    base.update(kw)
    return RunRecord(**base)


def test_record_defaults_and_duration():
    r = _rec()
    assert r.table_name == "silver" and r.duration_s == 90.0


@pytest.mark.parametrize("kw", [{"layer": "nope"}, {"status": "weird"}])
def test_record_rejects_bad_enums(kw):
    with pytest.raises(ValueError):
        _rec(**kw)


def test_record_rejects_negative_duration():
    with pytest.raises(ValueError):
        _rec(ended_at=datetime(2023, 1, 1, tzinfo=UTC))


def test_ledger_roundtrip_schema_and_order(tmp_path):
    led = Ledger(tmp_path)
    assert led.read() == []
    t0 = datetime(2024, 1, 1, tzinfo=UTC)
    led.append(
        [
            _rec(
                run_id="b",
                started_at=t0 + timedelta(days=1),
                ended_at=t0 + timedelta(days=1, seconds=1),
            )
        ]
    )
    led.append([_rec(run_id="a"), _rec(run_id="a", table_name="other")])
    rows = led.read()
    assert [r["run_id"] for r in rows] == ["a", "a", "b"]
    assert list(rows[0]) == RUN_COLUMNS
    assert rows[0]["schema_version"] == SCHEMA_VERSION
    assert not list(led.path.glob(".*.tmp"))  # atomic rename left nothing behind


def test_log_schema_has_required_keys(tmp_path):
    f = tmp_path / "l.jsonl"
    log = get_logger(f, stream=False)
    emit(log, "something", run_id="r1", layer="gold", job="j", rows=3)
    line = json.loads(f.read_text().splitlines()[0])
    assert set(LOG_KEYS) <= set(line)
    assert line["run_id"] == "r1" and line["rows"] == 3 and line["level"] == "info"
    assert line["schema_version"] == SCHEMA_VERSION
    datetime.fromisoformat(line["ts"])


def test_formatter_never_crashes_on_unserialisable():
    rec = logging.LogRecord("x", logging.INFO, "f", 1, "evt", None, None)
    rec.fields = {"obj": object()}
    json.loads(JsonFormatter().format(rec))


def test_track_run_success_and_log(tmp_path):
    led, f = Ledger(tmp_path), tmp_path / "l.jsonl"
    with track_run(
        "silver",
        "job",
        table="claims",
        batch_id="b1",
        ledger=led,
        logger=get_logger(f, stream=False),
    ) as run:
        run.set(rows_in=10, rows_out=9, rows_quarantined=1)
    (row,) = led.read()
    assert (row["status"], row["rows_out"], row["table_name"], row["batch_id"]) == (
        "success",
        9,
        "claims",
        "b1",
    )
    lines = [json.loads(x) for x in f.read_text().splitlines()]
    assert [x["event"] for x in lines] == ["run_started", "run_finished"]
    assert {x["run_id"] for x in lines} == {row["run_id"]}


def test_track_run_failure_records_and_reraises(tmp_path):
    led = Ledger(tmp_path)
    quiet = get_logger(None, stream=False)
    with pytest.raises(ValueError, match="bad"), track_run("gold", "job", ledger=led, logger=quiet):
        raise ValueError("bad")
    (row,) = led.read()
    assert row["status"] == "failed" and row["error"] == "ValueError: bad"


def test_track_run_survives_ledger_outage(tmp_path):
    blocker = tmp_path / "file"
    blocker.write_text("x")
    led = Ledger(blocker / "sub")  # cannot mkdir under a file
    with track_run("gold", "job", ledger=led, logger=get_logger(None, stream=False)) as run:
        run.set(rows_out=1)  # must not raise


def test_set_rejects_unknown_metric(tmp_path):
    quiet = get_logger(None, stream=False)
    with track_run("gold", "j", ledger=Ledger(tmp_path), logger=quiet) as r:  # noqa: SIM117
        with pytest.raises(KeyError):
            r.set(bogus=1)


def test_multi_table_rows(tmp_path):
    led = Ledger(tmp_path)
    with track_run("gold", "j", ledger=led, logger=get_logger(None, stream=False)) as r:
        r.set_table("dim_member", rows_out=5)
        r.set_table("fact_claim_line", rows_out=7)
    assert {x["table_name"]: x["rows_out"] for x in led.read()} == {
        "dim_member": 5,
        "fact_claim_line": 7,
    }


def test_decorator_records_returned_metrics(tmp_path, monkeypatch):
    monkeypatch.setenv("LAKEHOUSE_OPS_ROOT", str(tmp_path))

    @tracked("bronze", "ingest")
    def work():
        return {"rows_in": 3, "rows_out": 3, "ignored": 1}

    assert work()["ignored"] == 1
    (row,) = Ledger().read()
    assert row["rows_out"] == 3 and row["layer"] == "bronze"
