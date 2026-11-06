"""Turn existing reconciliation outputs into ledger rows (no edits to the jobs).

Gold writes ``gold_reconciliation`` (Delta; see docs/gold.md): per (gold_table, batch) the silver,
gold and excluded row counts and an ``identity_ok`` flag. We read its Parquet data files directly
(Delta ``_delta_log`` JSON commits decide which files are live), so no Spark is needed.
Checkpointed logs (>10 commits compacted) are not parsed: we then fall back to every data file
and say so, which can double count after rewrites. Prefer pointing at the Parquet export or at a
freshly written table; the limitation is documented in docs/observability.md.
"""

from __future__ import annotations

import json
from datetime import UTC
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from .ledger import RunRecord, new_run_id, utcnow


def live_parquet_files(table_dir: Path) -> tuple[list[Path], bool]:
    """(files, exact). ``exact`` is False when the Delta log had a checkpoint we cannot replay."""
    log = table_dir / "_delta_log"
    if not log.is_dir():
        return sorted(table_dir.glob("**/*.parquet")), True
    if any(log.glob("*.checkpoint*.parquet")):
        return sorted(
            p for p in table_dir.glob("**/*.parquet") if "_delta_log" not in p.parts
        ), False
    live: dict[str, None] = {}
    for commit in sorted(log.glob("*.json")):
        for line in commit.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            act = json.loads(line)
            if "add" in act:
                live[act["add"]["path"]] = None
            elif "remove" in act:
                live.pop(act["remove"]["path"], None)
    return [table_dir / p for p in live], True


def read_reconciliation(table_dir: str | Path) -> tuple[list[dict[str, Any]], bool]:
    files, exact = live_parquet_files(Path(table_dir))
    rows: list[dict[str, Any]] = []
    for f in files:
        rows.extend(pq.read_table(f).to_pylist())
    return rows, exact


def to_run_records(rows: list[dict[str, Any]], run_id: str | None = None) -> list[RunRecord]:
    """One ledger row per reconciliation row: rows_in=silver, rows_out=gold, quarantined=excluded.

    Status is ``failed`` when ``identity_ok`` is false or key violations exist, so the standard
    ``run_failed`` alert fires on a reconciliation break.
    """
    rid = run_id or new_run_id()
    out = []
    for r in rows:
        ts = r.get("_run_ts") or utcnow()
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=UTC)
        bad = (r.get("identity_ok") is False) or (r.get("key_violations") or 0) > 0
        out.append(
            RunRecord(
                run_id=rid,
                layer="gold",
                job="gold_reconciliation",
                started_at=ts,
                ended_at=ts,
                status="failed" if bad else "success",
                table_name=str(r["gold_table"]),
                batch_id=str(r["batch_id"]) if r.get("batch_id") is not None else None,
                rows_in=r.get("silver_rows"),
                rows_out=r.get("gold_rows"),
                rows_quarantined=r.get("excluded_rows"),
                error="reconciliation identity/key check failed" if bad else None,
            )
        )
    return out
