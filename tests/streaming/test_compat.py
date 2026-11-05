"""Contract evolution rules (BACKWARD). The golden-file test is the CI gate: editing
contracts/claim_event.v1.schema.json in a breaking way fails it."""

import copy
import json
from pathlib import Path

import pytest

from lakehouse.streaming.compat import check_backward, is_backward_compatible
from lakehouse.streaming.contract import load_schema

GOLDEN = Path(__file__).parent / "golden" / "claim_event.v1.published.json"


def base():
    return copy.deepcopy(load_schema())


def test_current_contract_is_backward_compatible_with_published_golden():
    published = json.loads(GOLDEN.read_text())
    problems = check_backward(published, load_schema())
    assert not problems, "contract break vs published v1; ship a v2 topic instead: " + str(problems)


def test_identical_schema_is_compatible():
    assert is_backward_compatible(base(), base())


def test_adding_optional_field_is_compatible():
    new = base()
    new["properties"]["prior_auth_flag"] = {"type": "boolean"}
    assert is_backward_compatible(base(), new)


def test_widening_is_compatible():
    new = base()
    new["properties"]["event_type"]["enum"].append("claim_adjusted")
    new["required"].remove("amount")
    assert is_backward_compatible(base(), new)


def test_integer_to_number_widening_is_compatible():
    old, new = base(), base()
    old["properties"]["amount"]["type"] = "integer"
    assert is_backward_compatible(old, new)


def breaking_new_required_field():
    s = base()
    s["properties"]["region"] = {"type": "string"}
    s["required"].append("region")
    return s


def breaking_type_change():
    s = base()
    s["properties"]["amount"]["type"] = "string"
    return s


def breaking_enum_narrowed():
    s = base()
    s["properties"]["event_type"]["enum"].remove("claim_denied")
    return s


def breaking_pattern_changed():
    s = base()
    s["properties"]["claim_id"]["pattern"] = "^C[0-9]+$"
    return s


def breaking_closed_model():
    s = base()
    s["additionalProperties"] = False
    return s


def breaking_new_required_on_existing_optional():
    old = base()
    old["required"].remove("amount")
    return old, base()


@pytest.mark.parametrize(
    "make",
    [
        breaking_new_required_field,
        breaking_type_change,
        breaking_enum_narrowed,
        breaking_pattern_changed,
        breaking_closed_model,
    ],
)
def test_breaking_changes_are_rejected(make):
    problems = check_backward(base(), make())
    assert problems, make.__name__


def test_making_optional_field_required_is_rejected():
    old, new = breaking_new_required_on_existing_optional()
    problems = check_backward(old, new)
    assert any("amount" in p and "required" in p for p in problems)


def test_removing_property_from_closed_schema_is_rejected():
    old = base()
    new = base()
    new["additionalProperties"] = False
    del new["properties"]["amount"]
    new["required"].remove("amount")
    assert any("amount" in p for p in check_backward(old, new))
