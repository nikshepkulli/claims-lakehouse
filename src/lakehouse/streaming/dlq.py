"""Dead-letter envelope. One format for producer-side and consumer-side rejects.

The original bytes are preserved base64-encoded (a poison message may not be valid UTF-8),
alongside where it came from and why it failed, so it can be inspected and replayed.
Convention: topic ``T`` dead-letters to ``T.dlq`` (see ``config.dlq_topic``); key is the
original key when known.
"""

from __future__ import annotations

import base64
import json
from datetime import UTC, datetime

from .config import dlq_topic

ENVELOPE_VERSION = 1


def build_envelope(
    *,
    raw: bytes | str,
    errors: list[str],
    error_type: str,
    stage: str,
    source_topic: str,
    partition: int | None = None,
    offset: int | None = None,
    key: str | None = None,
    schema_id: int | None = None,
    failed_at: datetime | None = None,
) -> dict:
    """Build the DLQ envelope. ``stage`` is ``producer`` or ``consumer`` (who rejected it)."""
    raw_bytes = raw.encode("utf-8") if isinstance(raw, str) else raw
    return {
        "envelope_version": ENVELOPE_VERSION,
        "stage": stage,
        "source_topic": source_topic,
        "source_partition": partition,
        "source_offset": offset,
        "key": key,
        "schema_id": schema_id,
        "error_type": error_type,
        "errors": errors,
        "failed_at": (failed_at or datetime.now(UTC)).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "payload_b64": base64.b64encode(raw_bytes).decode("ascii"),
    }


def to_message(envelope: dict) -> tuple[str, bytes, list[tuple[str, bytes]]]:
    """(dlq_topic, value, headers) ready for a producer."""
    headers = [
        ("dlq-reason", envelope["error_type"].encode()),
        ("dlq-stage", envelope["stage"].encode()),
    ]
    return (
        dlq_topic(envelope["source_topic"]),
        json.dumps(envelope, separators=(",", ":")).encode("utf-8"),
        headers,
    )


def original_payload(envelope: dict) -> bytes:
    """Recover the exact original bytes (for replay after a fix)."""
    return base64.b64decode(envelope["payload_b64"])
