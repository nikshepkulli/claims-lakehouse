"""Find the raw files that belong to a batch date. Pure Python (no Spark)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

from .datasets import Dataset


@dataclass(frozen=True)
class SourceFile:
    rel: str  # path relative to the input root, stored as ``_source_file``
    abs: str
    fmt: str  # "parquet" | "csv" | "jsonl"


def _kind_of(path: Path) -> str | None:
    return {".parquet": "parquet", ".csv": "csv", ".jsonl": "jsonl"}.get(path.suffix)


def _partition_dirs(root: Path, ds: Dataset) -> list[tuple[date, Path]]:
    out = []
    base = root / ds.glob_root
    if ds.partition_key is None or not base.is_dir():
        return out
    prefix = ds.partition_key + "="
    for d in sorted(base.iterdir()):
        if d.is_dir() and d.name.startswith(prefix):
            try:
                out.append((date.fromisoformat(d.name[len(prefix) :]), d))
            except ValueError:
                continue
    return out


def available_dates(root: Path, datasets: list[Dataset]) -> list[date]:
    """All batch dates that have at least one partition directory (flat-file datasets excluded)."""
    return sorted({d for ds in datasets for d, _ in _partition_dirs(root, ds)})


def discover(root: Path, ds: Dataset, batch_date: date) -> list[SourceFile]:
    """Files of ``ds`` for ``batch_date``.

    Partitioned datasets: files in ``<key>=<batch_date>/``. If a directory holds both Parquet and
    CSV copies of the same data (claimsgen ``--formats parquet,csv``), only Parquet is ingested so
    rows are not landed twice. Flat datasets (events): every ``*.jsonl`` under the dataset dir;
    the caller routes rows to batches by timestamp.
    """
    files: list[Path] = []
    if ds.partition_key is None:
        base = root / ds.glob_root
        files = sorted(p for p in base.rglob("*") if p.is_file()) if base.is_dir() else []
    else:
        for d, folder in _partition_dirs(root, ds):
            if d == batch_date:
                files = sorted(p for p in folder.iterdir() if p.is_file())
    if ds.kind == "parquet" and any(p.suffix == ".parquet" for p in files):
        files = [p for p in files if p.suffix == ".parquet"]
    wanted = {"parquet": {"parquet", "csv"}, "jsonl": {"jsonl"}}[ds.kind]
    out = []
    for p in files:
        k = _kind_of(p)
        if k in wanted and not p.name.startswith(("_", ".")):
            out.append(SourceFile(p.relative_to(root).as_posix(), str(p), k))
    return out
