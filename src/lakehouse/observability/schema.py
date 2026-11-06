"""Stable output schemas. Bump SCHEMA_VERSION on any breaking change (documented in
docs/observability.md); consumers (dashboard #29, Airflow #9) key off it."""

from __future__ import annotations

import pyarrow as pa

SCHEMA_VERSION = 1

LAYERS = ("bronze", "silver", "gold", "streaming", "quality", "other")
STATUSES = ("success", "failed")
SEVERITIES = ("info", "warning", "critical")

_TS = pa.timestamp("us", tz="UTC")

#: ``ops.pipeline_runs``: one row per finished run (and per table when a run covers several).
RUN_SCHEMA = pa.schema(
    [
        pa.field("run_id", pa.string(), nullable=False),
        pa.field("layer", pa.string(), nullable=False),
        pa.field("job", pa.string(), nullable=False),
        pa.field("table_name", pa.string(), nullable=False),
        pa.field("batch_id", pa.string()),
        pa.field("started_at", _TS, nullable=False),
        pa.field("ended_at", _TS, nullable=False),
        pa.field("duration_s", pa.float64(), nullable=False),
        pa.field("status", pa.string(), nullable=False),
        pa.field("rows_in", pa.int64()),
        pa.field("rows_out", pa.int64()),
        pa.field("rows_quarantined", pa.int64()),
        pa.field("bytes_out", pa.int64()),
        pa.field("error", pa.string()),
        pa.field("schema_version", pa.int32(), nullable=False),
    ]
)

RUN_COLUMNS = RUN_SCHEMA.names

#: Alert record keys, in output order.
ALERT_KEYS = (
    "schema_version",
    "alert_id",
    "ts",
    "severity",
    "table",
    "rule",
    "observed",
    "threshold",
    "message",
    "run_id",
)

#: Keys present on every structured log line.
LOG_KEYS = ("ts", "level", "event", "run_id", "layer", "job", "schema_version")
