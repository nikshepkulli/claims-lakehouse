"""``python -m lakehouse.gold --silver <uri> --gold <uri> --batch-date 2024-01-05|all``"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date

from .schema import MIN_VACUUM_HOURS


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        prog="lakehouse.gold",
        description="Build the gold star schema (facts + SCD2 dimensions) from silver Delta.",
    )
    ap.add_argument(
        "--silver",
        default=None,
        help="silver root URI (default: the silver layer of lakehouse_common.storage config)",
    )
    ap.add_argument(
        "--gold",
        default=None,
        help="gold root URI: path, file:// or s3a://... (default: storage config gold layer)",
    )
    ap.add_argument(
        "--batch-date",
        required=True,
        help="YYYY-MM-DD to build one batch, or 'all' for every silver batch date, in order",
    )
    ap.add_argument("--master", default=None, help="Spark master (default: local[*])")
    ap.add_argument(
        "--no-export",
        action="store_true",
        help="skip the Parquet export under <gold>/export/ (default: export after a clean run)",
    )
    ap.add_argument(
        "--optimize",
        action="store_true",
        help="after the run, compact small files in every gold table (Delta OPTIMIZE)",
    )
    ap.add_argument(
        "--vacuum",
        action="store_true",
        help=f"after the run, VACUUM every gold table (retention >= {MIN_VACUUM_HOURS} h)",
    )
    ap.add_argument(
        "--vacuum-retention-hours",
        type=int,
        default=MIN_VACUUM_HOURS,
        help="retention window for --vacuum (default and minimum: %(default)s)",
    )
    return ap.parse_args(argv)


def _roots(args: argparse.Namespace) -> tuple[str, str, dict[str, str]]:
    """Explicit URIs win; otherwise the storage config (LAKEHOUSE_ENV) supplies both layers."""
    if args.silver and args.gold:
        return args.silver, args.gold, {}
    from lakehouse_common.storage import load_location, spark_conf_for

    loc = load_location()
    return (
        args.silver or loc.uri("silver"),
        args.gold or loc.uri("gold"),
        spark_conf_for(loc),
    )


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
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
    if args.vacuum and args.vacuum_retention_hours < MIN_VACUUM_HOURS:
        print(f"--vacuum-retention-hours must be >= {MIN_VACUUM_HOURS}", file=sys.stderr)
        return 2
    silver, gold, conf = _roots(args)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    from lakehouse.bronze.spark import build_spark  # lazy: keeps arg parsing Spark-free

    from .job import (
        ReconciliationError,
        export_parquet,
        gold_batch,
        optimize_tables,
        silver_batch_dates,
        vacuum_tables,
    )

    s3 = ("s3a://", "s3://")
    root = silver if silver.startswith(s3) else gold  # s3a config if either is
    spark = build_spark("gold-batch", master=args.master, bronze_root=root, conf=conf)
    spark.sparkContext.setLogLevel("WARN")
    rc = 0
    try:
        dates = [one] if one else silver_batch_dates(spark, silver)
        for d in dates:
            try:
                results = gold_batch(spark, silver, gold, d)
            except ReconciliationError as e:
                print(f"{d} RECONCILIATION FAILED: {e}", file=sys.stderr)
                rc = 1
                continue
            for r in results:
                print(
                    f"{d} {r.gold_table:16s} silver={r.silver_rows} gold={r.gold_rows} "
                    f"excluded={r.excluded_rows} violations={r.key_violations} "
                    f"reconciled={r.identity_ok}"
                )
        if rc == 0 and not args.no_export:
            counts = export_parquet(spark, gold)
            print("exported:", ", ".join(f"{t}={n}" for t, n in counts.items()))
        elif rc != 0:
            print("export skipped: reconciliation failed", file=sys.stderr)
        if args.optimize:
            print("optimized:", ", ".join(optimize_tables(spark, gold)))
        if args.vacuum:
            print("vacuumed:", ", ".join(vacuum_tables(spark, gold, args.vacuum_retention_hours)))
    finally:
        spark.stop()
    return rc
