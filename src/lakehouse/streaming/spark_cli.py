"""CLI for the streaming job: ``python -m lakehouse.streaming.spark_job --help``."""

from __future__ import annotations

import argparse
import json
import logging
import sys

from .config import EVENTS_TOPIC
from .spark_rules import Roots, roots_from_location, roots_from_root


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        prog="lakehouse.streaming.spark_job",
        description="Spark Structured Streaming: Kafka -> bronze -> silver.",
    )
    where = ap.add_argument_group("storage (one of --root, --env)")
    where.add_argument("--root", help="one plain URI; uses <root>/bronze, /silver, /_checkpoints")
    where.add_argument(
        "--env",
        choices=["local", "aws", "azure"],
        help="resolve roots via lakehouse_common.storage (config/lakehouse.<env>.yaml)",
    )
    ap.add_argument("--source", choices=["kafka", "files"], default="kafka")
    ap.add_argument(
        "--bootstrap", help="Kafka bootstrap (inside the compose network: redpanda:9092)"
    )
    ap.add_argument("--topic", default=EVENTS_TOPIC)
    ap.add_argument("--input-dir", help="JSONL directory for --source files")
    ap.add_argument("--trigger", choices=["availableNow", "processingTime"], default="availableNow")
    ap.add_argument("--interval", default="5 seconds", help="processingTime interval")
    ap.add_argument("--watermark-delay", default="6 hours", help="event-time watermark delay")
    ap.add_argument("--stages", default="bronze,silver", help="comma list of: bronze, silver")
    ap.add_argument("--max-offsets-per-trigger", type=int, help="Kafka bronze micro-batch cap")
    ap.add_argument("--max-files-per-trigger", type=int, help="files bronze / delta silver cap")
    ap.add_argument("--run-seconds", type=float, help="processingTime: stop after N seconds")
    ap.add_argument(
        "--dlq-kafka", action="store_true", help="also publish dead letters to <topic>.dlq"
    )
    ap.add_argument("--master", default=None, help="Spark master (default: local[*])")
    return ap.parse_args(argv)


def resolve_roots(args: argparse.Namespace) -> Roots:
    if bool(args.root) == bool(args.env):
        raise SystemExit("give exactly one of --root or --env")
    if args.root:
        return roots_from_root(args.root)
    from lakehouse_common.storage import load_config, resolve_location

    return roots_from_location(resolve_location(load_config(args.env)))


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    roots = resolve_roots(args)

    from .spark_job import StreamConfig, build_stream_spark, run_job

    cfg = StreamConfig(
        bronze_root=roots.bronze,
        silver_root=roots.silver,
        checkpoint_root=roots.checkpoints,
        topic=args.topic,
        source=args.source,
        bootstrap=args.bootstrap,
        input_dir=args.input_dir,
        trigger=args.trigger,
        processing_interval=args.interval,
        watermark_delay=args.watermark_delay,
        stages=tuple(s.strip() for s in args.stages.split(",") if s.strip()),
        max_offsets_per_trigger=args.max_offsets_per_trigger,
        max_files_per_trigger=args.max_files_per_trigger,
        run_seconds=args.run_seconds,
        dlq_kafka=args.dlq_kafka,
    )
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    spark = build_stream_spark(cfg, master=args.master)
    spark.sparkContext.setLogLevel("WARN")
    try:
        result = run_job(spark, cfg)
    finally:
        spark.stop()
    print(
        "streaming-spark "
        + json.dumps({"counts": result.counts, "metrics_rows": result.metrics_rows})
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
