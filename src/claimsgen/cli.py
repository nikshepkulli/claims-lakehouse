"""``python -m claimsgen --rows N --seed S --out data/raw``"""

from __future__ import annotations

import argparse
import sys
import time
from datetime import date
from pathlib import Path

from .config import GenConfig
from .generator import generate


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        prog="claimsgen", description="Generate 100% synthetic healthcare claims data."
    )
    ap.add_argument("--rows", type=int, default=100_000, help="number of distinct claims (>=1000)")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--out", type=Path, default=Path("data/raw"))
    ap.add_argument(
        "--chunk-size", type=int, default=250_000, help="claims per chunk (memory bound)"
    )
    ap.add_argument(
        "--formats", default="parquet", help="comma list from: parquet,csv (csv is opt-in)"
    )
    ap.add_argument(
        "--target-file-mb",
        type=float,
        default=128.0,
        help="target uncompressed size of each output file (per table per day)",
    )
    ap.add_argument("--start-date", type=date.fromisoformat, default=date(2024, 1, 1))
    ap.add_argument("--days", type=int, default=366)
    ap.add_argument("--notes-rate", type=float, default=0.25)
    ap.add_argument(
        "--overwrite", action="store_true", help="replace previous generator output in --out"
    )
    ap.add_argument("--quiet", action="store_true")
    a = ap.parse_args(argv)
    try:
        cfg = GenConfig(
            rows=a.rows,
            out=a.out,
            seed=a.seed,
            chunk_size=a.chunk_size,
            start_date=a.start_date,
            days=a.days,
            notes_rate=a.notes_rate,
            formats=tuple(f.strip() for f in a.formats.split(",") if f.strip()),
            overwrite=a.overwrite,
            target_file_mb=a.target_file_mb,
        )
        t0 = time.perf_counter()
        m = generate(
            cfg, log=(lambda s: None) if a.quiet else (lambda s: print(s, file=sys.stderr))
        )
    except (ValueError, FileExistsError) as e:
        print(f"error: {e}", file=sys.stderr)
        return 2
    dt = time.perf_counter() - t0
    tot = sum(m["rows_written"].get(k, 0) for k in ("claims", "claim_lines", "diagnoses"))
    print(f"done in {dt:.1f}s: {m['rows_written']} ({tot / dt:,.0f} table rows/s) -> {cfg.out}")
    return 0
