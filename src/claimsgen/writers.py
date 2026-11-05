"""Output sink: date-partitioned Parquet/CSV batches, JSONL notes and event stream."""

from __future__ import annotations

import json
import shutil
from collections import Counter
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import IO

import numpy as np
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

EPOCH = date(1970, 1, 1)
OWNED = ["members", "providers", "claims", "claim_lines", "diagnoses", "notes", "events",
         "defects_manifest.json"]


def epoch_days(d: date) -> int:
    return (d - EPOCH).days


def iso(day_num: int) -> str:
    return (EPOCH + timedelta(days=int(day_num))).isoformat()


def date32(days: np.ndarray) -> pa.Array:
    return pa.array(days.astype(np.int32)).cast(pa.date32())


@dataclass(frozen=True)
class Drift:
    table: str
    day: int  # epoch days of the ingest_date partition that drifts
    column: str


class Sink:
    def __init__(self, out: Path, formats: tuple[str, ...], overwrite: bool, drift: Drift | None = None):
        self.out, self.formats, self.drift = out, formats, drift
        self.rows: Counter[str] = Counter()
        self.files: Counter[str] = Counter()
        out.mkdir(parents=True, exist_ok=True)
        existing = [n for n in OWNED if (out / n).exists()]
        if existing and not overwrite:
            raise FileExistsError(f"{out} already contains {existing}; pass --overwrite to replace")
        for n in existing:
            p = out / n
            shutil.rmtree(p) if p.is_dir() else p.unlink()
        (out / "events").mkdir()
        self._events: IO[str] = open(out / "events" / "claim_events.jsonl", "w", encoding="utf-8")

    def close(self) -> None:
        self._events.close()

    # -- tables ---------------------------------------------------------------
    def _write(self, name: str, part_dir: str, fname: str, table: pa.Table) -> None:
        d = self.out / name / part_dir
        d.mkdir(parents=True, exist_ok=True)
        if "parquet" in self.formats:
            pq.write_table(table, d / f"{fname}.parquet", compression="zstd")
            self.files[name] += 1
        if "csv" in self.formats:
            pacsv.write_csv(table, d / f"{fname}.csv")
            self.files[name] += 1
        self.rows[name] += table.num_rows

    def write_snapshot(self, name: str, table: pa.Table, snapshot: date) -> None:
        self._write(name, f"snapshot_date={snapshot.isoformat()}", "part-00000", table)

    def write_partitioned(self, name: str, table: pa.Table, days: np.ndarray, chunk: int) -> None:
        """Split a chunk by ingest_date (epoch days) and write one file per day."""
        order = np.argsort(days, kind="stable")
        t = table.take(pa.array(order))
        uniq, starts = np.unique(days[order], return_index=True)
        ends = np.append(starts[1:], len(order))
        for d, s, e in zip(uniq, starts, ends):
            part = t.slice(int(s), int(e - s))
            if self.drift and name == self.drift.table and int(d) == self.drift.day:
                flag = np.where(np.arange(part.num_rows) % 3 == 0, "Y", "N")
                part = part.append_column(self.drift.column, pa.array(flag))
            self._write(name, f"ingest_date={iso(d)}", f"part-{chunk:05d}", part)

    # -- jsonl ------------------------------------------------------------------
    def write_notes(self, lines: list[str], days: np.ndarray, chunk: int) -> None:
        by_day: dict[int, list[str]] = {}
        for ln, d in zip(lines, days.tolist()):
            by_day.setdefault(d, []).append(ln)
        for d, group in sorted(by_day.items()):
            p = self.out / "notes" / f"ingest_date={iso(d)}"
            p.mkdir(parents=True, exist_ok=True)
            (p / f"part-{chunk:05d}.jsonl").write_text("\n".join(group) + "\n", encoding="utf-8")
            self.files["notes"] += 1
        self.rows["notes"] += len(lines)

    def write_events(self, lines: list[str]) -> None:
        self._events.write("\n".join(lines) + "\n")
        self.rows["events"] += len(lines)
