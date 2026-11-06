"""Transports: file/console/HTTP behaviour and the rule that a transport failure never raises."""

import json
from datetime import UTC, datetime

import pytest

from lakehouse.lineage import (
    ConsoleTransport,
    Emitter,
    FileTransport,
    HttpTransport,
    build_event,
)
from lakehouse.lineage.events import RunSpec

T0 = datetime(2024, 1, 5, 6, 0, tzinfo=UTC)


def ev(job="silver_batch", kind="START"):
    return build_event(RunSpec("a" * 32, job), kind, T0)


@pytest.fixture
def marquez(make_marquez):
    return make_marquez()


def test_file_transport_appends_json_lines_and_skips_exact_duplicates(tmp_path):
    t = FileTransport(tmp_path / "d" / "e.jsonl")
    assert t.send(ev()) is True
    assert t.send(ev()) is False  # identical event: idempotent
    assert t.send(ev(kind="COMPLETE")) is True
    lines = (tmp_path / "d" / "e.jsonl").read_text().splitlines()
    assert [json.loads(x)["eventType"] for x in lines] == ["START", "COMPLETE"]
    # a fresh process re-reads the file and still dedupes
    assert FileTransport(tmp_path / "d" / "e.jsonl").send(ev()) is False


def test_console_transport_prints_one_json_line(capsys):
    ConsoleTransport().send(ev())
    err = capsys.readouterr().err.strip().splitlines()
    assert len(err) == 1 and json.loads(err[0])["job"]["name"] == "silver_batch"


def test_http_posts_to_marquez_lineage_endpoint(marquez, monkeypatch):
    monkeypatch.setenv("MARQUEZ_API_KEY", "s3cret")
    assert HttpTransport(marquez.url).send(ev()) is True
    path, headers, body = marquez.requests[0]
    assert path == "/api/v1/lineage"
    assert headers["Content-Type"] == "application/json"
    assert headers["Authorization"] == "Bearer s3cret"
    assert body["eventType"] == "START" and body["job"]["name"] == "silver_batch"


def test_http_accepts_full_endpoint_url_and_rejects_non_http(marquez):
    assert HttpTransport(marquez.url + "/api/v1/lineage").url.count("/api/v1/lineage") == 1
    with pytest.raises(ValueError):
        HttpTransport("file:///etc/passwd")


def test_http_retries_5xx_then_succeeds(make_marquez):
    m = make_marquez([503, 502])
    assert HttpTransport(m.url, retries=2, backoff_s=0).send(ev()) is True
    assert len(m.requests) == 3


def test_http_does_not_retry_4xx_and_reports_server_message(make_marquez):
    m = make_marquez([400])
    with pytest.raises(RuntimeError, match="HTTP 400.*nope"):
        HttpTransport(m.url, retries=3, backoff_s=0).send(ev())
    assert len(m.requests) == 1


def test_emitter_counts_connection_failures_and_never_raises(tmp_path):
    # port 9 (discard) on loopback: connection refused, no server needed
    em = Emitter([HttpTransport("http://127.0.0.1:9", retries=0, timeout_s=1)])
    assert em.emit(ev()) is False
    assert em.stats.failed == 1 and em.stats.attempted == 1 and em.stats.delivered == 0
    assert em.stats.by_transport["http"]["failed"] == 1
    assert "URLError" in em.stats.last_error or "Connection" in em.stats.last_error


def test_one_dead_transport_does_not_stop_the_others(tmp_path):
    f = tmp_path / "e.jsonl"
    em = Emitter([HttpTransport("http://127.0.0.1:9", retries=0, timeout_s=1), FileTransport(f)])
    em.emit(ev())
    em.emit(ev(kind="COMPLETE"))
    assert len(f.read_text().splitlines()) == 2
    assert em.stats.failed == 2 and em.stats.delivered == 2
    assert em.stats.by_transport == {
        "http": {"delivered": 0, "skipped": 0, "failed": 2},
        "file": {"delivered": 2, "skipped": 0, "failed": 0},
    }


def test_failure_is_logged_as_json_event(caplog):
    class Boom:
        name = "boom"

        def send(self, event):
            raise OSError("disk full")

    import logging

    logging.getLogger("lakehouse.observability").propagate = True
    with caplog.at_level(logging.ERROR, logger="lakehouse.observability"):
        Emitter([Boom()]).emit(ev())
    rec = next(r for r in caplog.records if r.getMessage() == "lineage_emit_failed")
    assert rec.fields["transport"] == "boom" and "disk full" in rec.fields["error"]
    assert rec.fields["job"] == "silver_batch"


def test_unwritable_file_is_a_counted_failure_not_a_crash(tmp_path):
    blocker = tmp_path / "file"
    blocker.write_text("x")
    em = Emitter([FileTransport(blocker / "sub" / "e.jsonl")])  # parent is a file
    assert em.emit(ev()) is False and em.stats.failed == 1


def test_duplicates_are_counted_separately_from_failures(tmp_path):
    em = Emitter([FileTransport(tmp_path / "e.jsonl")])
    em.emit(ev())
    em.emit(ev())
    s = em.stats.as_dict()
    assert (s["delivered"], s["skipped_duplicates"], s["failed"], s["attempted"]) == (1, 1, 0, 2)


def test_validation_blocks_invalid_events_and_counts_them(tmp_path):
    pytest.importorskip("jsonschema")
    f = tmp_path / "e.jsonl"
    em = Emitter([FileTransport(f)], validate=True)
    bad = ev()
    del bad["producer"]
    assert em.emit(bad) is False
    assert em.emit(ev()) is True
    assert em.stats.invalid == 1 and em.stats.failed == 1
    assert len(f.read_text().splitlines()) == 1  # the invalid event was not sent
