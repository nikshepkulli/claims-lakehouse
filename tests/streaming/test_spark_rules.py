"""Pure-Python parts of the Spark job: no JVM needed (run in the plain pytest job)."""

import json

import pytest

from lakehouse.streaming import dlq
from lakehouse.streaming.spark_cli import parse_args, resolve_roots
from lakehouse.streaming.spark_rules import (
    REASON_CONTRACT,
    REASON_MALFORMED,
    REASON_NULL_PAYLOAD,
    checkpoint_dir,
    envelope_json,
    parse_duration_seconds,
    roots_from_root,
    validation_outcome,
)

GOOD = {
    "event_id": "EVT0000000000001",
    "event_type": "claim_submitted",
    "event_ts": "2024-01-01T10:00:00Z",
    "claim_id": "CLM000000000001",
    "member_id": "M000000001",
    "provider_id": "PRV0000001",
    "claim_type": "PROFESSIONAL",
    "amount": 10.5,
}


def test_valid_event():
    o = validation_outcome(json.dumps(GOOD).encode())
    assert o.ok and o.errors == [] and o.error_type is None


def test_malformed_json_reason():
    o = validation_outcome(b"{not json")
    assert not o.ok and o.error_type == REASON_MALFORMED


def test_contract_violation_names_failed_fields():
    bad = {**GOOD, "event_type": "claim_refunded"}
    del bad["amount"]
    o = validation_outcome(json.dumps(bad).encode())
    assert o.error_type == REASON_CONTRACT
    assert o.failed_fields == ["amount", "event_type"]  # missing + bad-enum fields
    assert validation_outcome(b"{nope").failed_fields == []


def test_null_payload():
    assert validation_outcome(None).error_type == REASON_NULL_PAYLOAD


def test_envelope_round_trips_original_bytes():
    raw = b"\xff\xfenot utf8"
    env = json.loads(envelope_json(raw, ["x"], REASON_MALFORMED, "claims.events.v1", 3, 99))
    assert (
        env["stage"] == "consumer" and env["source_partition"] == 3 and env["source_offset"] == 99
    )
    assert dlq.original_payload(env) == raw


@pytest.mark.parametrize(
    ("text", "secs"),
    [("90 seconds", 90), ("30 minutes", 1800), ("6 hours", 21600), ("1 day", 86400)],
)
def test_duration(text, secs):
    assert parse_duration_seconds(text) == secs


@pytest.mark.parametrize("bad", ["", "6", "hours", "x hours", "6 fortnights"])
def test_duration_rejects(bad):
    with pytest.raises(ValueError):
        parse_duration_seconds(bad)


def test_roots_and_checkpoints():
    r = roots_from_root("s3a://lake/streaming/")
    assert (r.bronze, r.silver) == ("s3a://lake/streaming/bronze", "s3a://lake/streaming/silver")
    assert checkpoint_dir(r.checkpoints, "silver_events") == (
        "s3a://lake/streaming/_checkpoints/silver_events"
    )


def test_cli_defaults_and_storage_resolution():
    args = parse_args(["--root", "/tmp/x", "--source", "files", "--input-dir", "/tmp/in"])
    assert args.trigger == "availableNow" and args.watermark_delay == "6 hours"
    assert resolve_roots(args).silver == "/tmp/x/silver"
    env_args = parse_args(["--env", "local"])
    assert resolve_roots(env_args).silver.endswith("/silver/streaming")  # via lakehouse_common
    with pytest.raises(SystemExit):
        resolve_roots(parse_args([]))
