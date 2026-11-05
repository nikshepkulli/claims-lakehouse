import json

import pytest

from lakehouse.streaming import PRODUCER_VERSION
from lakehouse.streaming.produce import PRODUCER_CONFIG, headers_for, read_lines, replay
from lakehouse.streaming.topics import specs_for


def event(i: int, **over) -> bytes:
    ev = {
        "event_id": f"EVT{i:013d}",
        "event_type": "claim_submitted",
        "event_ts": "2024-01-02T07:57:18Z",
        "claim_id": f"CLM{i:012d}",
        "member_id": "M000000324",
        "provider_id": "PRV0000133",
        "claim_type": "PROFESSIONAL",
        "amount": 1.0,
    }
    ev.update(over)
    return json.dumps(ev).encode()


class FakeProducer:
    def __init__(self, buffer_errors: int = 0):
        self.sent: list[dict] = []
        self.polls = 0
        self.flushed = False
        self._buffer_errors = buffer_errors

    def produce(self, topic, key=None, value=None, headers=None, on_delivery=None):
        if self._buffer_errors:
            self._buffer_errors -= 1
            raise BufferError("queue full")
        self.sent.append({"topic": topic, "key": key, "value": value, "headers": headers})
        on_delivery(None, None)

    def poll(self, timeout=0):
        self.polls += 1
        return 0

    def flush(self, timeout=0):
        self.flushed = True
        return 0


class FakeClock:
    """Sleeping advances time exactly, so pacing is deterministic."""

    def __init__(self):
        self.now = 0.0
        self.sleeps: list[float] = []

    def clock(self):
        return self.now

    def sleep(self, s):
        self.sleeps.append(s)
        self.now += s


def test_keys_headers_and_verbatim_values():
    p = FakeProducer()
    lines = [event(i) for i in range(5)]
    stats = replay(lines, p, topic="claims.events.v1", schema_id=9)
    assert stats.sent == 5 and stats.delivered == 5 and stats.failed == 0 and p.flushed
    assert [m["value"] for m in p.sent] == lines
    assert p.sent[2]["key"] == b"CLM000000000002"
    assert dict(p.sent[0]["headers"]) == {
        "schema-id": b"9",
        "producer-version": PRODUCER_VERSION.encode(),
    }


def test_max_events_caps_output():
    p = FakeProducer()
    stats = replay((event(i) for i in range(100)), p, topic="t", max_events=7)
    assert stats.sent == 7 and len(p.sent) == 7


def test_rate_pacing_follows_schedule_without_drift():
    p, c = FakeProducer(), FakeClock()
    replay([event(i) for i in range(11)], p, topic="t", rate=10, clock=c.clock, sleep=c.sleep)
    # 11 events at 10/s: the last is due at t=1.0s; sleeps sum to the schedule, no extra drift.
    assert c.now == pytest.approx(1.0)
    assert sum(c.sleeps) == pytest.approx(1.0)


def test_rate_catches_up_after_slow_poll():
    p, c = FakeProducer(), FakeClock()
    slow = {"done": False}
    orig = p.poll

    def poll(t=0):
        if not slow["done"]:
            slow["done"] = True
            c.now += 0.5  # one stall of 500 ms
        return orig(t)

    p.poll = poll
    replay([event(i) for i in range(11)], p, topic="t", rate=10, clock=c.clock, sleep=c.sleep)
    assert c.now == pytest.approx(1.0)  # stall absorbed, still finishes on schedule


def test_unthrottled_rate_zero_never_sleeps():
    c = FakeClock()
    replay(
        [event(i) for i in range(50)],
        FakeProducer(),
        topic="t",
        rate=0,
        clock=c.clock,
        sleep=c.sleep,
    )
    assert c.sleeps == []


def test_buffer_error_is_retried_not_lost():
    p = FakeProducer(buffer_errors=3)
    stats = replay([event(1)], p, topic="t")
    assert stats.sent == 1 and len(p.sent) == 1 and p.polls >= 3


def test_contract_violations_go_to_dlq_not_main_topic():
    p = FakeProducer()
    lines = [event(1), event(2, event_type="claim_refunded"), b"{oops", event(3)]
    stats = replay(lines, p, topic="claims.events.v1", schema_id=1)
    assert stats.sent == 2 and stats.dead_lettered == 2
    topics = [m["topic"] for m in p.sent]
    assert topics == [
        "claims.events.v1",
        "claims.events.v1.dlq",
        "claims.events.v1.dlq",
        "claims.events.v1",
    ]
    env = json.loads(p.sent[1]["value"])
    assert env["stage"] == "producer" and env["key"] == "CLM000000000002"
    assert json.loads(p.sent[2]["value"])["error_type"] == "malformed_json"


def test_duplicates_are_replayed_verbatim():
    """Producer must not dedupe: duplicates are a deliberate defect for the consumer to handle."""
    p = FakeProducer()
    e = event(1)
    replay([e, e, e], p, topic="t")
    assert [m["value"] for m in p.sent] == [e, e, e]


def test_delivery_failures_are_counted():
    class Failing(FakeProducer):
        def produce(self, topic, key=None, value=None, headers=None, on_delivery=None):
            on_delivery(RuntimeError("broker said no"), None)

    stats = replay([event(1), event(2)], Failing(), topic="t")
    assert stats.failed == 2 and stats.delivered == 0


def test_read_lines_skips_blank_lines(tmp_path):
    f = tmp_path / "e.jsonl"
    f.write_bytes(b'{"a":1}\n\n{"b":2}\r\n')
    assert list(read_lines(f)) == [b'{"a":1}', b'{"b":2}']


def test_headers_without_schema_id():
    assert [k for k, _ in headers_for(None)] == ["producer-version"]


def test_producer_is_idempotent_and_acks_all():
    assert PRODUCER_CONFIG["acks"] == "all"
    assert PRODUCER_CONFIG["enable.idempotence"] is True
    assert PRODUCER_CONFIG["max.in.flight.requests.per.connection"] <= 5


def test_topic_specs():
    events, dead = specs_for("claims.events.v1")
    assert events.cleanup_policy == "delete"  # no compaction: the full lifecycle log matters
    assert dead.name == "claims.events.v1.dlq" and dead.retention_ms > events.retention_ms
    assert events.configs()["retention.ms"] == str(7 * 24 * 3600 * 1000)


def test_replays_generated_stream_end_to_end(generated_events):
    p = FakeProducer()
    stats = replay(
        read_lines(generated_events),
        p,
        topic="claims.events.v1",
        schema_id=1,
    )
    assert stats.dead_lettered == 0
    assert stats.sent == len(generated_events.read_bytes().splitlines())
