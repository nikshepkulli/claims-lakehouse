import json
from datetime import UTC, datetime

from lakehouse.streaming import dlq


def test_envelope_roundtrip_preserves_bytes_and_context():
    raw = b'{"claim_id": "x"}\xff'  # not valid UTF-8: must survive
    env = dlq.build_envelope(
        raw=raw,
        errors=["claim_id: bad"],
        error_type="contract_violation",
        stage="consumer",
        source_topic="claims.events.v1",
        partition=3,
        offset=42,
        key="x",
        schema_id=7,
        failed_at=datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC),
    )
    assert env["failed_at"] == "2026-01-02T03:04:05Z"
    assert env["source_partition"] == 3 and env["source_offset"] == 42
    assert dlq.original_payload(env) == raw
    topic, value, headers = dlq.to_message(env)
    assert topic == "claims.events.v1.dlq"
    assert json.loads(value) == env
    assert dict(headers) == {"dlq-reason": b"contract_violation", "dlq-stage": b"consumer"}


def test_envelope_accepts_str_payload():
    env = dlq.build_envelope(
        raw="héllo",
        errors=[],
        error_type="malformed_json",
        stage="producer",
        source_topic="t",
    )
    assert dlq.original_payload(env).decode() == "héllo"
    assert env["envelope_version"] == dlq.ENVELOPE_VERSION
