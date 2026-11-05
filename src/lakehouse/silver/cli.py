"""``python -m lakehouse.silver --bronze <uri> --silver <uri> --batch-date 2024-01-05``"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date

from lakehouse.bronze.datasets import DATASETS


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        prog="lakehouse.silver",
        description="Build typed, deduplicated silver Delta tables from bronze, with quarantine.",
    )
    ap.add_argument(
        "--bronze", required=True, help="bronze root URI (as written by lakehouse.bronze)"
    )
    ap.add_argument("--silver", required=True, help="silver root URI: path, file:// or s3a://...")
    ap.add_argument(
        "--batch-date",
        required=True,
        help="YYYY-MM-DD to build one batch, or 'all' for every bronze batch date, in order",
    )
    ap.add_argument(
        "--datasets",
        default=",".join(DATASETS),
        help="comma list (default: all): " + ",".join(DATASETS),
    )
    ap.add_argument("--master", default=None, help="Spark master (default: local[*])")
    ap.add_argument(
        "--optimize",
        action="store_true",
        help="after the run, compact small files in every silver table (Delta OPTIMIZE)",
    )
    return ap.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    names = [n.strip() for n in args.datasets.split(",") if n.strip()]
    unknown = [n for n in names if n not in DATASETS]
    if unknown:
        print(f"unknown datasets: {unknown}", file=sys.stderr)
        return 2
    one: date | None = None
    if args.batch_date != "all":
        try:
            one = date.fromisoformat(args.batch_date)
        except ValueError:
            print(
                f"--batch-date must be YYYY-MM-DD or 'all', got {args.batch_date!r}",
                file=sys.stderr,
            )
            return 2

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    from lakehouse.bronze.spark import build_spark  # lazy: keeps arg parsing Spark-free

    from .job import ReconciliationError, bronze_batch_dates, optimize_tables, silver_batch

    s3 = ("s3a://", "s3://")
    root = args.bronze if args.bronze.startswith(s3) else args.silver  # s3a config if either is
    spark = build_spark("silver-batch", master=args.master, bronze_root=root)
    spark.sparkContext.setLogLevel("WARN")
    rc = 0
    try:
        dates = [one] if one else bronze_batch_dates(spark, args.bronze)
        for d in dates:
            try:
                results = silver_batch(spark, args.bronze, args.silver, d, names)
            except ReconciliationError as e:
                print(f"{d} RECONCILIATION FAILED: {e}", file=sys.stderr)
                rc = 1
                continue
            for r in results:
                if r.skipped:
                    print(f"{d} {r.dataset:12s} batch={r.batch_id} skipped (no bronze rows)")
                    continue
                print(
                    f"{d} {r.dataset:12s} batch={r.batch_id} bronze={r.bronze_rows} "
                    f"silver={r.silver_rows} quarantined={r.quarantined_rows} "
                    f"deduped={r.deduped_rows} reconciled={r.identity_ok}"
                )
        if args.optimize:
            print("optimized:", ", ".join(optimize_tables(spark, args.silver)))
    finally:
        spark.stop()
    return rc
