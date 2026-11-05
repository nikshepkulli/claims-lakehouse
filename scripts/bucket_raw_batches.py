"""Merge claimsgen's daily ingest partitions into N batch folders, for fast end-to-end runs.

claimsgen at ~2000 claims still spreads rows over ~30 distinct ``ingest_date`` partitions
(late arrivals land 31-90 days after service). Bronze, silver and gold each run one Spark batch
per date, so a full ``--batch-date all`` would take many minutes. This copies the tree so that
every partition of ``claims``, ``claim_lines``, ``diagnoses`` and ``notes`` falls into one of N
folders named after the first date of its chunk, as ONE file per folder (a bronze batch reads
its files one by one, so hundreds of tiny files would dominate the run time);
``members`` / ``providers`` are copied as is.
The data itself is unchanged, only the batch boundaries move, and the pipeline does not depend
on how many batches there are.

``events`` (one flat file, routed by event_ts) is not copied: gold does not read it. Run bronze
and silver with ``--datasets claims,claim_lines,diagnoses,members,providers,notes``.

Usage:
    python scripts/bucket_raw_batches.py --raw data/raw --out data/raw_batched --batches 3
"""

from __future__ import annotations

import argparse
import shutil
import sys
from datetime import date
from pathlib import Path

PARTITIONED = ("claims", "claim_lines", "diagnoses", "notes")
DIMENSIONS = ("members", "providers")
DATASETS_ARG = "claims,claim_lines,diagnoses,members,providers,notes"


def partition_dates(raw: Path) -> list[str]:
    found: set[str] = set()
    for name in PARTITIONED:
        if (raw / name).is_dir():
            found.update(p.name.split("=", 1)[1] for p in (raw / name).iterdir())
    return sorted(found)


def _merge(files: list[Path], dst: Path) -> None:
    """Concatenate part files into one ``part-00000`` file (Parquet tables or JSONL lines)."""
    if files[0].suffix == ".parquet":
        import pyarrow as pa
        import pyarrow.parquet as pq

        tables = [pq.read_table(f) for f in files]
        pq.write_table(
            pa.concat_tables(tables, promote_options="default"), dst / "part-00000.parquet"
        )
    else:
        with (dst / f"part-00000{files[0].suffix}").open("wb") as out:
            for f in files:
                out.write(f.read_bytes())


def bucket(raw: Path, out: Path, batches: int) -> list[date]:
    """Copy ``raw`` to ``out`` with its partitions merged into ``batches`` folders."""
    if batches < 1:
        raise ValueError("batches must be >= 1")
    dates = partition_dates(raw)
    if not dates:
        raise ValueError(f"no ingest_date partitions under {raw}")
    size = -(-len(dates) // batches)
    chunks = [dates[i : i + size] for i in range(0, len(dates), size)]
    for name in PARTITIONED:
        for chunk in chunks:
            files = [
                f
                for d in chunk
                if (raw / name / f"ingest_date={d}").is_dir()
                for f in sorted((raw / name / f"ingest_date={d}").iterdir())
            ]
            if files:
                dst = out / name / f"ingest_date={chunk[0]}"
                dst.mkdir(parents=True, exist_ok=True)
                _merge(files, dst)
    for name in DIMENSIONS:
        shutil.copytree(raw / name, out / name, dirs_exist_ok=True)
    return [date.fromisoformat(c[0]) for c in chunks]


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--raw", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--batches", type=int, default=3)
    args = ap.parse_args(argv)
    if args.out.exists():
        shutil.rmtree(args.out)
    starts = bucket(args.raw, args.out, args.batches)
    print("batch dates:", ", ".join(d.isoformat() for d in starts))
    print("bronze/silver --datasets", DATASETS_ARG, file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
