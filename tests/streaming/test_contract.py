import copy
import json

import pytest

from lakehouse.streaming.config import dlq_topic, subject_for
from lakehouse.streaming.contract import load_schema, validate_event, validate_message

VALID = {
    "event_id": "EVT0000000000004",
    "event_type": "claim_submitted",
    "event_ts": "2024-01-02T07:57:18Z",
    "claim_id": "CLM000000000002",
    "member_id": "M000000324",
    "provider_id": "PRV0000133",
    "claim_type": "PROFESSIONAL",
    "amount": 101.99,
}


def test_valid_event_passes():
    assert validate_event(VALID) == []
    assert validate_message(json.dumps(VALID)).ok


def test_unknown_extra_field_is_allowed():
    """Open content model: a compatible producer revision may add optional fields."""
    assert validate_event({**VALID, "prior_auth_flag": True}) == []


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("event_type", "claim_refunded"),
        ("claim_id", "clm1"),
        ("event_ts", "2024-01-02 07:57:18"),
        ("amount", "101.99"),
        ("claim_type", None),
        ("member_id", ""),
    ],
)
def test_invalid_field_values_rejected(field, value):
    errs = validate_event({**VALID, field: value})
    assert errs and errs[0].startswith(field)


@pytest.mark.parametrize("field", list(VALID))
def test_missing_required_field_rejected(field):
    ev = {k: v for k, v in VALID.items() if k != field}
    assert any(field in e for e in validate_event(ev))


def test_malformed_and_non_object_messages():
    bad = validate_message(b"{not json")
    assert not bad.ok and bad.error_type == "malformed_json"
    arr = validate_message(b"[1,2]")
    assert not arr.ok and arr.error_type == "contract_violation"
    assert not validate_message(b"\xff\xfe").ok


def test_whole_generated_stream_conforms(generated_events):
    """Includes the injected duplicate / out-of-order events: they are valid by contract."""
    n = 0
    for line in generated_events.read_bytes().splitlines():
        res = validate_message(line)
        assert res.ok, (line, res.errors)
        n += 1
    assert n >= 1000


def test_schema_is_valid_draft7_and_not_mutated():
    s = load_schema()
    assert s["$schema"].endswith("draft-07/schema#")
    assert set(s["required"]) <= set(s["properties"])
    assert copy.deepcopy(s) == load_schema()


def test_naming_conventions():
    assert dlq_topic("claims.events.v1") == "claims.events.v1.dlq"
    assert dlq_topic("claims.events.v1.dlq") == "claims.events.v1.dlq"
    assert subject_for("claims.events.v1") == "claims.events.v1-value"
