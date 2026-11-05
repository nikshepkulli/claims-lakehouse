"""Claim-event contract loading and validation (producer- and consumer-side).

The Spark streaming job (#12) imports ``validate_message`` and routes failures to the DLQ
via ``lakehouse.streaming.dlq``.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from functools import lru_cache
from typing import Any

from jsonschema import Draft7Validator

from .config import contract_path


@lru_cache(maxsize=8)
def load_schema(major: int = 1) -> dict[str, Any]:
    """Load the committed contract for ``major`` (cached; do not mutate the result)."""
    with contract_path(major=major).open(encoding="utf-8") as fh:
        return json.load(fh)


@lru_cache(maxsize=8)
def _validator(major: int) -> Draft7Validator:
    schema = load_schema(major)
    Draft7Validator.check_schema(schema)
    return Draft7Validator(schema)


@dataclass(frozen=True)
class ValidationResult:
    ok: bool
    event: dict[str, Any] | None = None
    errors: list[str] = field(default_factory=list)
    error_type: str | None = None  # "malformed_json" | "contract_violation"


def validate_event(event: Any, major: int = 1) -> list[str]:
    """Contract violations for a parsed event, as readable strings (empty = valid)."""
    errs = sorted(_validator(major).iter_errors(event), key=lambda e: list(e.absolute_path))
    return [f"{'.'.join(str(p) for p in e.absolute_path) or '$'}: {e.message}" for e in errs]


def validate_message(raw: bytes | str, major: int = 1) -> ValidationResult:
    """Parse and validate one message value. Never raises on bad input."""
    try:
        event = json.loads(raw)
    except (ValueError, UnicodeDecodeError) as exc:
        return ValidationResult(False, None, [f"not valid JSON: {exc}"], "malformed_json")
    errors = validate_event(event, major)
    if errors:
        return ValidationResult(
            False, event if isinstance(event, dict) else None, errors, "contract_violation"
        )
    return ValidationResult(True, event)
