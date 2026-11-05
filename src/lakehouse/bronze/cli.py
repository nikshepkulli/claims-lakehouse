"""``python -m lakehouse.bronze --input data/raw --bronze <uri> --batch-date 2024-01-05``"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date
from pathlib import Path

from .datasets import DATASETS
from .discover import available_dates


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        prog="lakehouse.bronze",
        description="Ingest claimsgen raw files into append-only Delta bronze tables.",
    )
    ap.add_argument("--input", type=Path, default=Path("data/raw"), help="claimsgen output root")
    ap.add_argument(
        "--bronze",
        required=True,
        help="bronze root URI: local path, file://, or s3a://bucket/prefix",
    )
    ap.add_argument(
        "--batch-date",
        required=True,
        help="YYYY-MM-DD to ingest one day, or 'all' for every date found, one batch per date",
    )
    ap.add_argument(
        "--datasets",
        default=",".join(DATASETS),
        help="comma list (default: all): " + ",".join(DATASETS),
    )
    ap.add_argument("--master", default=None, help="Spark master (default: local[*])")
    return ap.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    names = [n.strip() for n in args.datasets.split(",") if n.strip()]
    unknown = [n for n in names if n not in DATASETS]
    if unknown:
        print(f"unknown datasets: {unknown}", file=sys.stderr)
        return 2
    if args.batch_date == "all":
        dates = available_dates(args.input, [DATASETS[n] for n in names])
    else:
        try:
            dates = [date.fromisoformat(args.batch_date)]
        except ValueError:
            print(
                f"--batch-date must be YYYY-MM-DD or 'all', got {args.batch_date!r}",
                file=sys.stderr,
            )
            return 2
    if not args.input.is_dir():
        print(f"--input {args.input} is not a directory", file=sys.stderr)
        return 2

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    from .ingest import ingest_batch  # lazy: keeps arg parsing Spark-free
    from .spark import build_spark

    spark = build_spark(master=args.master, bronze_root=args.bronze)
    spark.sparkContext.setLogLevel("WARN")
    try:
        for d in dates:
            for r in ingest_batch(spark, args.input, args.bronze, d, names):
                if r.skipped:
                    print(f"{d} {r.dataset:12s} batch={r.batch_id} skipped (no input files)")
                    continue
                print(
                    f"{d} {r.dataset:12s} batch={r.batch_id} rows={r.rows_written} "
                    f"files={r.files_read} quarantined_files={r.files_quarantined} "
                    f"quarantined_rows={r.rows_quarantined} drift={r.columns_added or '-'}"
                )
    finally:
        spark.stop()
    return 0
