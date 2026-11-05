"""Output sink: date-partitioned Parquet/CSV tables, JSONL notes and the event stream.

Chunks are first spilled to ``_staging/`` (one small file per chunk per ingest_date, never
visible in the final tree). ``finalize`` then compacts each ingest_date partition, reading
one day at a time (so memory stays bounded by one day's data) and writing files of about
``target_bytes`` each. A table/day therefore gets ``ceil(day_bytes / target)`` files, not
one file per chunk.
"""

from __future__ import annotations

import math
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
STAGING = "_staging"
TABLES = ("claims", "claim_lines", "diagnoses")
OWNED = [
    "members",
    "providers",
    "claims",
    "claim_lines",
    "diagnoses",
    "notes",
    "events",
    STAGING,
    "defects_manifest.json",
]


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


def n_splits(total_bytes: int, target_bytes: int) -> int:
    return max(1, math.ceil(total_bytes / target_bytes))


class Sink:
    def __init__(
        self,
        out: Path,
        formats: tuple[str, ...],
        overwrite: bool,
        drift: Drift | None = None,
        target_file_mb: float = 128.0,
    ):
        self.out, self.formats, self.drift = out, formats, drift
        self.target_bytes = int(target_file_mb * 1024 * 1024)
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
        events_path = out / "events" / "claim_events.jsonl"
        # held open across chunks, closed in close()
        self._events: IO[str] = open(events_path, "w", encoding="utf-8")  # noqa: SIM115

    def close(self) -> None:
        self._events.close()

    # -- final file writers -----------------------------------------------------
    def _write_files(self, name: str, part_dir: str, table: pa.Table) -> None:
        """Write one partition, split into ~target-sized files (same split for every format)."""
        d = self.out / name / part_dir
        d.mkdir(parents=True, exist_ok=True)
        k = n_splits(table.nbytes, self.target_bytes)
        per = math.ceil(table.num_rows / k) if table.num_rows else 0
        for i in range(k):
            part = table.slice(i * per, per)
            if "parquet" in self.formats:
                pq.write_table(part, d / f"part-{i:05d}.parquet", compression="zstd")
                self.files[name] += 1
            if "csv" in self.formats:
                pacsv.write_csv(part, d / f"part-{i:05d}.csv")
                self.files[name] += 1
        self.rows[name] += table.num_rows

    def write_snapshot(self, name: str, table: pa.Table, snapshot: date) -> None:
        self._write_files(name, f"snapshot_date={snapshot.isoformat()}", table)

    # -- chunk spill ---------------------------------------------------------------
    def write_partitioned(self, name: str, table: pa.Table, days: np.ndarray, chunk: int) -> None:
        """Split a chunk by ingest_date (epoch days) and spill one staging file per day."""
        order = np.argsort(days, kind="stable")
        t = table.take(pa.array(order))
        uniq, starts = np.unique(days[order], return_index=True)
        ends = np.append(starts[1:], len(order))
        for d, s, e in zip(uniq, starts, ends, strict=True):
            sd = self.out / STAGING / name / iso(d)
            sd.mkdir(parents=True, exist_ok=True)
            part = t.slice(int(s), int(e - s))
            pq.write_table(part, sd / f"c{chunk:05d}.parquet", compression="none")

    def write_notes(self, lines: list[str], days: np.ndarray, chunk: int) -> None:
        by_day: dict[int, list[str]] = {}
        for ln, d in zip(lines, days.tolist(), strict=True):
            by_day.setdefault(d, []).append(ln)
        for d, group in sorted(by_day.items()):
            sd = self.out / STAGING / "notes" / iso(d)
            sd.mkdir(parents=True, exist_ok=True)
            (sd / f"c{chunk:05d}.jsonl").write_text("\n".join(group) + "\n", encoding="utf-8")

    def write_events(self, lines: list[str]) -> None:
        self._events.write("\n".join(lines) + "\n")
        self.rows["events"] += len(lines)

    # -- compaction ----------------------------------------------------------------
    def finalize(self) -> None:
        """Compact staging into target-sized files, one ingest_date at a time; drop staging."""
        stage = self.out / STAGING
        for name in TABLES:
            for day_dir in sorted((stage / name).glob("*")):
                t = pa.concat_tables([pq.read_table(f) for f in sorted(day_dir.glob("c*.parquet"))])
                if self.drift and name == self.drift.table and day_dir.name == iso(self.drift.day):
                    flag = np.where(np.arange(t.num_rows) % 3 == 0, "Y", "N")
                    t = t.append_column(self.drift.column, pa.array(flag))
                self._write_files(name, f"ingest_date={day_dir.name}", t)
        for day_dir in sorted((stage / "notes").glob("*")):
            lines: list[str] = []
            for f in sorted(day_dir.glob("c*.jsonl")):
                lines.extend(f.read_text(encoding="utf-8").splitlines())
            total = sum(len(x) + 1 for x in lines)  # ascii-only synthetic text
            k = n_splits(total, self.target_bytes)
            per = math.ceil(len(lines) / k)
            d = self.out / "notes" / f"ingest_date={day_dir.name}"
            d.mkdir(parents=True, exist_ok=True)
            for i in range(k):
                chunk_lines = lines[i * per : (i + 1) * per]
                (d / f"part-{i:05d}.jsonl").write_text(
                    "\n".join(chunk_lines) + "\n", encoding="utf-8"
                )
                self.files["notes"] += 1
            self.rows["notes"] += len(lines)
        shutil.rmtree(stage, ignore_errors=True)
