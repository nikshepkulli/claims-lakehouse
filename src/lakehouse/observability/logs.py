"""JSON-lines structured logging with run ids.

Every line is one JSON object with the keys in ``schema.LOG_KEYS`` plus event-specific fields.
No secrets are logged: callers pass metrics and identifiers only.
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .schema import SCHEMA_VERSION

LOGGER_NAME = "lakehouse.observability"


class JsonFormatter(logging.Formatter):
    """Formats a record as one JSON line. Event fields come from ``extra={"fields": {...}}``."""

    def format(self, record: logging.LogRecord) -> str:
        fields: dict[str, Any] = dict(getattr(record, "fields", {}) or {})
        out: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, UTC).isoformat(timespec="microseconds"),
            "level": record.levelname.lower(),
            "event": record.getMessage(),
            "run_id": fields.pop("run_id", None),
            "layer": fields.pop("layer", None),
            "job": fields.pop("job", None),
            "schema_version": SCHEMA_VERSION,
        }
        out.update(fields)
        return json.dumps(out, default=str, sort_keys=False)


def get_logger(path: str | Path | None = None, *, stream: bool = True) -> logging.Logger:
    """Logger emitting JSON lines to stderr and (optionally) appending to ``path``.

    Idempotent per (path, stream) combination: handlers are rebuilt so tests can redirect.
    """
    log = logging.getLogger(LOGGER_NAME)
    log.setLevel(logging.INFO)
    log.propagate = False
    for h in list(log.handlers):
        log.removeHandler(h)
        h.close()
    fmt = JsonFormatter()
    if stream:
        sh = logging.StreamHandler(sys.stderr)
        sh.setFormatter(fmt)
        log.addHandler(sh)
    if path is not None:
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        fh = logging.FileHandler(p, encoding="utf-8")
        fh.setFormatter(fmt)
        log.addHandler(fh)
    return log


def emit(log: logging.Logger, event: str, level: int = logging.INFO, **fields: Any) -> None:
    log.log(level, event, extra={"fields": fields})
