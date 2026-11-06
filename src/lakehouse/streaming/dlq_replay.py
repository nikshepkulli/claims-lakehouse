"""Replay dead-lettered events after a fix (ticket #13).

    python -m lakehouse.streaming.spark_job replay-dlq --root data/streaming [--source table]
    python -m lakehouse.streaming.dlq_replay --root data/streaming --source kafka --bootstrap ...

What it does: read dead letters (the ``silver_claim_events_dlq`` table, or the Kafka DLQ topic),
re-validate each payload with the CURRENT contract rules, and append the ones that now pass to
the bronze table, where the silver query picks them up like any other arrival. Records that still
fail stay in the DLQ and are logged as ``still_invalid``.

Idempotent by construction, two keyed steps:

1. bronze append is an insert-if-absent MERGE keyed on ``source_file = 'dlq-replay:<dlq_key>'``
   (an insert-only MERGE adds files and removes none, so the streaming reader of bronze is fine);
2. ``stream_dlq_replay`` records ``replayed`` per ``dlq_key``; keys already replayed are never
   considered again.

A crash between the two steps is repaired by re-running: step 1 is a no-op, step 2 completes.
Replayed events are classified by the silver watermark like any other arrival, so one older than
the watermark lands in ``silver_claim_events_late`` rather than silver.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from .config import EVENTS_TOPIC, dlq_topic
from .spark_job import (
    REPLAY_SOURCE_PREFIX,
    StreamConfig,
    Tables,
    _create_table,
    _merge_insert_only,
    _validate_udf,
    build_stream_spark,
)

log = logging.getLogger("lakehouse.streaming.replay")

REPLAY_TABLE = "stream_dlq_replay"
STATUS_REPLAYED = "replayed"
STATUS_STILL_INVALID = "still_invalid"

BRONZE_SCHEMA = T.StructType(
    [
        T.StructField("value", T.BinaryType()),
        T.StructField("topic", T.StringType()),
        T.StructField("partition", T.IntegerType()),
        T.StructField("offset", T.LongType()),
        T.StructField("kafka_ts", T.TimestampType()),
        T.StructField("source_file", T.StringType()),
        T.StructField("ingested_at", T.TimestampType()),
    ]
)

REPLAY_SCHEMA = T.StructType(
    [
        T.StructField("dlq_key", T.StringType(), False),
        T.StructField("status", T.StringType()),
        T.StructField("reason_code", T.StringType()),  # the reason on the LAST attempt
        T.StructField("attempts", T.LongType()),
        T.StructField("run_id", T.StringType()),
        T.StructField("last_attempt_at", T.TimestampType()),
        T.StructField("replayed_at", T.TimestampType()),
    ]
)

_ENVELOPE = T.StructType(
    [
        T.StructField("source_topic", T.StringType()),
        T.StructField("source_partition", T.IntegerType()),
        T.StructField("source_offset", T.LongType()),
        T.StructField("error_type", T.StringType()),
        T.StructField("payload_b64", T.StringType()),
    ]
)


@dataclass
class ReplayResult:
    considered: int = 0  # candidates not yet replayed (after the reason filter and limit)
    already_replayed: int = 0  # skipped: logged as replayed by an earlier run
    replayed: int = 0  # passed validation now, appended to bronze
    still_invalid: int = 0
    still_invalid_by_reason: dict[str, int] = field(default_factory=dict)
    dry_run: bool = False


def _candidates_from_table(spark: SparkSession, cfg: StreamConfig) -> DataFrame:
    try:
        df = spark.read.format("delta").load(cfg.uri(Tables.DLQ))
    except Exception as e:  # noqa: BLE001 - AnalysisException is not importable portably
        raise SystemExit(f"no DLQ table at {cfg.uri(Tables.DLQ)}: {e}") from e
    return df.select("dlq_key", "payload", "source_topic", "source_partition", "reason_code")


def _candidates_from_kafka(spark: SparkSession, cfg: StreamConfig) -> DataFrame:
    """Batch-read the whole DLQ topic. Key = dlq_key; duplicates (at-least-once) collapse."""
    raw = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", cfg.bootstrap)
        .option("subscribe", dlq_topic(cfg.topic))
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
    )
    env = F.from_json(F.col("value").cast("string"), _ENVELOPE)
    return (
        raw.where(F.col("key").isNotNull())
        .select(
            F.col("key").cast("string").alias("dlq_key"),
            F.unbase64(env["payload_b64"]).alias("payload"),
            env["source_topic"].alias("source_topic"),
            env["source_partition"].alias("source_partition"),
            env["error_type"].alias("reason_code"),
        )
        .dropDuplicates(["dlq_key"])
    )


def replay_dlq(
    spark: SparkSession,
    cfg: StreamConfig,
    *,
    source: str = "table",
    reasons: tuple[str, ...] = (),
    limit: int | None = None,
    dry_run: bool = False,
) -> ReplayResult:
    """Re-validate dead letters and append the now-valid ones to bronze. Safe to re-run."""
    if source not in ("table", "kafka"):
        raise ValueError(f"source must be table or kafka, got {source!r}")
    if source == "kafka" and not cfg.bootstrap:
        raise ValueError("--bootstrap is required for --source kafka")
    bronze_uri = cfg.uri(Tables.BRONZE)
    log_uri = cfg.uri(REPLAY_TABLE)
    _create_table(spark, log_uri, REPLAY_SCHEMA)
    _create_table(spark, bronze_uri, BRONZE_SCHEMA)  # no-op when the stream created it

    cand = (
        _candidates_from_table(spark, cfg)
        if source == "table"
        else _candidates_from_kafka(spark, cfg)
    )
    if reasons:
        cand = cand.where(F.col("reason_code").isin(*reasons))
    done = (
        spark.read.format("delta")
        .load(log_uri)
        .where(F.col("status") == STATUS_REPLAYED)
        .select("dlq_key", F.lit(True).alias("_done"))
    )
    flagged = cand.join(done, "dlq_key", "left").persist()
    res = ReplayResult(dry_run=dry_run)
    try:
        res.already_replayed = flagged.where(F.col("_done")).count()
        todo = flagged.where(F.col("_done").isNull()).drop("_done").orderBy("dlq_key")
        if limit:
            todo = todo.limit(limit)
        checked = todo.withColumn("_v", _validate_udf(F.col("payload"))).persist()
        try:
            res.considered = checked.count()
            ok = checked.where("_v.ok")
            bad = checked.where(~F.col("_v.ok"))
            res.replayed = ok.count()
            by_reason = bad.groupBy("_v.error_type").count().collect()
            res.still_invalid_by_reason = {r[0]: r[1] for r in by_reason}
            res.still_invalid = sum(res.still_invalid_by_reason.values())
            if not dry_run and res.considered:
                _apply(spark, bronze_uri, log_uri, ok, bad)
        finally:
            checked.unpersist()
    finally:
        flagged.unpersist()
    log.info(json.dumps({"event": "dlq_replay", **vars(res)}, sort_keys=True))
    return res


def _apply(spark: SparkSession, bronze_uri: str, log_uri: str, ok: DataFrame, bad: DataFrame):
    # 1. bronze: insert-if-absent, so a re-run (or a crash before step 2) cannot double-append
    rows = ok.select(
        F.col("payload").alias("value"),
        F.col("source_topic").alias("topic"),
        F.coalesce(F.col("source_partition"), F.lit(0)).cast("int").alias("partition"),
        F.lit(None).cast("long").alias("offset"),
        F.current_timestamp().alias("kafka_ts"),
        F.concat(F.lit(REPLAY_SOURCE_PREFIX), F.col("dlq_key")).alias("source_file"),
        F.current_timestamp().alias("ingested_at"),
    )
    _merge_insert_only(spark, bronze_uri, rows, ("source_file",))

    # 2. the replay log: one row per dlq_key
    run_id = uuid.uuid4().hex[:12]
    now = datetime.now(timezone.utc)  # noqa: UP017 - the Spark image runs Python 3.10
    logged = ok.select(
        "dlq_key",
        F.lit(STATUS_REPLAYED).alias("status"),
        F.lit("replayed").alias("reason_code"),
    ).unionByName(
        bad.select(
            "dlq_key",
            F.lit(STATUS_STILL_INVALID).alias("status"),
            F.col("_v.error_type").alias("reason_code"),
        )
    )
    src = logged.select(
        "dlq_key",
        "status",
        "reason_code",
        F.lit(1).cast("long").alias("attempts"),
        F.lit(run_id).alias("run_id"),
        F.lit(now).cast("timestamp").alias("last_attempt_at"),
        F.when(F.col("status") == STATUS_REPLAYED, F.lit(now).cast("timestamp")).alias(
            "replayed_at"
        ),
    )
    (
        DeltaTable.forPath(spark, log_uri)
        .alias("t")
        .merge(src.alias("s"), "t.dlq_key = s.dlq_key")
        .whenMatchedUpdate(
            set={
                "status": "s.status",
                "reason_code": "s.reason_code",
                "attempts": "t.attempts + 1",
                "run_id": "s.run_id",
                "last_attempt_at": "s.last_attempt_at",
                "replayed_at": "coalesce(s.replayed_at, t.replayed_at)",
            }
        )
        .whenNotMatchedInsertAll()
        .execute()
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        prog="lakehouse.streaming.spark_job replay-dlq",
        description="Re-validate dead letters; append the now-valid ones to bronze (idempotent).",
    )
    ap.add_argument("--root", help="same --root the streaming job uses")
    ap.add_argument("--env", choices=["local", "aws", "azure"])
    ap.add_argument("--source", choices=["table", "kafka"], default="table")
    ap.add_argument("--bootstrap", help="Kafka bootstrap (--source kafka)")
    ap.add_argument("--topic", default=EVENTS_TOPIC, help="main topic; the DLQ is <topic>.dlq")
    ap.add_argument("--reason", action="append", default=[], help="only this reason_code (repeat)")
    ap.add_argument("--limit", type=int, help="replay at most N dead letters")
    ap.add_argument("--dry-run", action="store_true", help="validate and count, write nothing")
    ap.add_argument("--master", default=None)
    return ap.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    from .spark_cli import resolve_roots

    args = parse_args(argv)
    roots = resolve_roots(args)
    cfg = StreamConfig(
        bronze_root=roots.bronze,
        silver_root=roots.silver,
        checkpoint_root=roots.checkpoints,
        topic=args.topic,
        source="kafka" if args.source == "kafka" else "files",
        bootstrap=args.bootstrap,
        stages=("bronze",) if args.source == "kafka" else (),  # only decides the Kafka package
    )
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    spark = build_stream_spark(cfg, master=args.master)
    spark.sparkContext.setLogLevel("WARN")
    try:
        res = replay_dlq(
            spark,
            cfg,
            source=args.source,
            reasons=tuple(args.reason),
            limit=args.limit,
            dry_run=args.dry_run,
        )
    finally:
        spark.stop()
    print("dlq-replay " + json.dumps(vars(res), sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
