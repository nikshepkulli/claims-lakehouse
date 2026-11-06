"""Pure-Python rules for the Spark streaming job (no PySpark import, so unit tests need no JVM).

Everything here is deterministic and side-effect free: contract validation outcome and reason
codes, the claim-status ordering used by the rollup, and storage-root resolution.
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from pathlib import Path

from lakehouse_common.storage import LakehouseLocation

from . import dlq
from .contract import validate_message

# Reason codes written to the dead-letter table (and, with --dlq-kafka, to the DLQ topic).
REASON_MALFORMED = "malformed_json"
REASON_CONTRACT = "contract_violation"
REASON_NULL_PAYLOAD = "null_payload"
# A valid event dead-lettered because its whole batch failed a data-quality gate (on_fail=divert).
# Its payload is a canonical re-serialisation of the parsed event, so it re-validates on replay.
REASON_GATE_DIVERTED = "gate_diverted"
# Not a DLQ code: late events are diverted to their own table with this reason.
REASON_LATE = "late_beyond_watermark"

# Status rollup: latest event_ts wins; at equal event_ts a terminal event beats a submission
# (a claim cannot be un-paid by a same-second submit); paid/denied ties break on event_id.
EVENT_STATUS = {"claim_submitted": "submitted", "claim_paid": "paid", "claim_denied": "denied"}
STATUS_RANK = {"submitted": 0, "paid": 1, "denied": 1}


@dataclass(frozen=True)
class Outcome:
    ok: bool
    error_type: str | None
    errors: list[str]
    failed_fields: list[str]


def validation_outcome(raw: bytes | bytearray | None) -> Outcome:
    """Validate one raw Kafka value with the shared contract helper. Never raises."""
    if raw is None:
        return Outcome(False, REASON_NULL_PAYLOAD, ["payload is null"], [])
    res = validate_message(bytes(raw))
    if res.ok:
        return Outcome(True, None, [], [])
    return Outcome(False, res.error_type or REASON_CONTRACT, res.errors, _failed_fields(res))


_REQUIRED = re.compile(r"'(\w+)' is a required property")


def _failed_fields(res) -> list[str]:
    """Property names behind contract violations (empty for malformed JSON)."""
    if res.error_type != REASON_CONTRACT:
        return []
    fields: set[str] = set()
    for e in res.errors:
        path, _, msg = e.partition(": ")
        if path != "$":
            fields.add(path.split(".")[0])
        elif m := _REQUIRED.search(msg):
            fields.add(m.group(1))
    return sorted(fields)


def envelope_json(
    raw: bytes | bytearray | None,
    outcome_errors: list[str],
    error_type: str,
    topic: str | None,
    partition: int | None,
    offset: int | None,
) -> str:
    """DLQ envelope (``lakehouse.streaming.dlq``) as compact JSON, stage ``consumer``."""
    env = dlq.build_envelope(
        raw=bytes(raw) if raw is not None else b"",
        errors=outcome_errors,
        error_type=error_type,
        stage="consumer",
        source_topic=topic or "unknown",
        partition=partition,
        offset=offset,
    )
    return json.dumps(env, separators=(",", ":"))


@dataclass(frozen=True)
class Roots:
    bronze: str
    silver: str
    checkpoints: str


def roots_from_root(root: str) -> Roots:
    """``<root>/bronze``, ``<root>/silver``, ``<root>/_checkpoints`` from one plain URI."""
    r = root.rstrip("/")
    if "://" not in r:  # Delta treats a relative path as a table name; make it absolute
        r = os.path.abspath(r)
    return Roots(f"{r}/bronze", f"{r}/silver", f"{r}/_checkpoints")


def roots_from_location(loc: LakehouseLocation) -> Roots:
    """Table roots from the storage abstraction (ADR-0003): no provider path is built here."""
    return Roots(
        bronze=loc.uri("bronze", "streaming"),
        silver=loc.uri("silver", "streaming"),
        checkpoints=loc.uri("silver", "streaming_checkpoints"),
    )


def parse_duration_seconds(text: str) -> int:
    """'6 hours' / '30 minutes' / '90 seconds' -> seconds (the watermark delay grammar)."""
    parts = text.split()
    if len(parts) != 2 or not parts[0].isdigit():
        raise ValueError(f"expected '<n> <unit>' like '6 hours', got {text!r}")
    mult = {"second": 1, "minute": 60, "hour": 3600, "day": 86400}
    unit = parts[1].rstrip("s")
    if unit not in mult:
        raise ValueError(f"unknown unit in {text!r}; use seconds/minutes/hours/days")
    return int(parts[0]) * mult[unit]


def checkpoint_dir(checkpoints_root: str, query_name: str) -> str:
    return f"{checkpoints_root.rstrip('/')}/{query_name}"


def local_path(uri: str) -> Path | None:
    """Filesystem path for a plain or ``file://`` URI, else None (s3a/abfss)."""
    if uri.startswith("file://"):
        return Path(uri[len("file://") :])
    if "://" in uri:
        return None
    return Path(uri)
