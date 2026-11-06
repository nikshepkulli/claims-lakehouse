"""The run ledger: ``ops.pipeline_runs`` as a directory of Parquet part files.

One immutable file per write (named by start time + run id), so concurrent jobs never contend
and a half-written run cannot corrupt earlier rows (write to ``.tmp`` then rename). Readers use
``pyarrow.dataset`` / DuckDB / Spark on the directory. It is Parquet, not Delta: this module must
run without Spark. Rows are never updated; a run that dies before the context exits leaves no row.
"""

from __future__ import annotations

import os
import uuid
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from .schema import LAYERS, RUN_COLUMNS, RUN_SCHEMA, SCHEMA_VERSION, STATUSES

DEFAULT_ROOT = "data/ops"
TABLE_DIR = "pipeline_runs"


def new_run_id() -> str:
    return uuid.uuid4().hex


def utcnow() -> datetime:
    return datetime.now(UTC)


@dataclass
class RunRecord:
    run_id: str
    layer: str
    job: str
    started_at: datetime
    ended_at: datetime
    status: str
    table_name: str = ""
    batch_id: str | None = None
    rows_in: int | None = None
    rows_out: int | None = None
    rows_quarantined: int | None = None
    bytes_out: int | None = None
    error: str | None = None
    duration_s: float = field(default=0.0)

    def __post_init__(self) -> None:
        if self.layer not in LAYERS:
            raise ValueError(f"layer {self.layer!r} not in {LAYERS}")
        if self.status not in STATUSES:
            raise ValueError(f"status {self.status!r} not in {STATUSES}")
        if self.ended_at < self.started_at:
            raise ValueError("ended_at before started_at")
        if not self.table_name:
            self.table_name = self.layer
        self.duration_s = round((self.ended_at - self.started_at).total_seconds(), 3)

    def to_row(self) -> dict[str, Any]:
        row = asdict(self)
        row["schema_version"] = SCHEMA_VERSION
        return row


class Ledger:
    """Append-only run ledger rooted at ``<root>/pipeline_runs``."""

    def __init__(self, root: str | os.PathLike[str] | None = None):
        base = root if root is not None else os.environ.get("LAKEHOUSE_OPS_ROOT", DEFAULT_ROOT)
        self.path = Path(base) / TABLE_DIR

    def append(self, records: list[RunRecord]) -> Path | None:
        if not records:
            return None
        self.path.mkdir(parents=True, exist_ok=True)
        rows = [r.to_row() for r in records]
        table = pa.Table.from_pylist(rows, schema=RUN_SCHEMA)
        first = min(r.started_at for r in records)
        name = f"part-{first:%Y%m%dT%H%M%S%f}-{records[0].run_id}-{uuid.uuid4().hex[:6]}.parquet"
        final = self.path / name
        tmp = self.path / f".{name}.tmp"
        pq.write_table(table, tmp)
        os.replace(tmp, final)
        return final

    def read(self) -> list[dict[str, Any]]:
        """All rows, oldest first (by ended_at, then run_id for stable order)."""
        files = sorted(self.path.glob("part-*.parquet")) if self.path.exists() else []
        rows: list[dict[str, Any]] = []
        for f in files:
            rows.extend(pq.read_table(f).to_pylist())
        rows.sort(key=lambda r: (r["ended_at"], r["run_id"], r["table_name"]))
        return rows


def validate_rows(rows: list[dict[str, Any]]) -> None:
    """Raise ValueError when rows do not match the documented columns (used by CLI ingest)."""
    for r in rows:
        missing = [c for c in RUN_COLUMNS if c not in r]
        if missing:
            raise ValueError(f"ledger row missing columns: {missing}")
