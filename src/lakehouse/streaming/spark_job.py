"""Structured Streaming job: Kafka -> bronze Delta -> silver Delta (ticket #12).

    bronze query:  Kafka (or a directory of JSONL files) -> ``bronze_claim_events`` (append-only)
    silver query:  bronze Delta -> validate -> dedupe -> late/dead-letter split -> idempotent MERGEs
                   into ``silver_claim_events`` / ``..._late`` / ``..._dlq`` / ``claim_status``

Design and guarantees are in docs/streaming-spark.md. In short:

* Delivery: the source is at-least-once, every sink write is keyed (MERGE on ``event_id``,
  ``claim_id``, ``dlq_key``, ``(query, batch)``), so replaying a micro-batch changes nothing:
  effectively-once. A restart resumes from the checkpoint with no duplicates and no loss.
* Dedupe: ``dropDuplicatesWithinWatermark("event_id")`` bounds state; the MERGE is the correctness
  backstop for duplicates that outlive that state.
* Late data: Spark's watermark only bounds state here (``dropDuplicatesWithinWatermark`` does not
  drop late rows). Lateness is classified in ``foreachBatch`` from a per-batch high-water mark that
  mirrors Spark's rule (watermark = max event time of PREVIOUS batches - delay), so late events are
  diverted to their own table and counted, never silently dropped.
"""

from __future__ import annotations

import json
import logging
import math
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.streaming import StreamingQuery, StreamingQueryListener

from lakehouse.bronze.paths import table_uri
from lakehouse.bronze.spark import build_spark

from .config import EVENTS_TOPIC, dlq_topic
from .spark_rules import (
    REASON_LATE,
    checkpoint_dir,
    envelope_json,
    parse_duration_seconds,
    validation_outcome,
)

log = logging.getLogger("lakehouse.streaming.spark")

BRONZE_QUERY = "bronze_events"
SILVER_QUERY = "silver_events"


class Tables:
    """Table names under the bronze / silver roots."""

    BRONZE = "bronze_claim_events"
    SILVER = "silver_claim_events"
    LATE = "silver_claim_events_late"
    DLQ = "silver_claim_events_dlq"
    STATUS = "claim_status"
    WM_STATE = "stream_watermark_state"
    METRICS = "stream_metrics"


@dataclass
class StreamConfig:
    bronze_root: str
    silver_root: str
    checkpoint_root: str
    topic: str = EVENTS_TOPIC
    source: str = "kafka"  # "kafka" | "files" (JSONL directory; deterministic, used in CI)
    bootstrap: str | None = None
    input_dir: str | None = None
    trigger: str = "availableNow"  # "availableNow" | "processingTime"
    processing_interval: str = "5 seconds"
    watermark_delay: str = "6 hours"
    stages: tuple[str, ...] = ("bronze", "silver")
    max_offsets_per_trigger: int | None = None  # kafka bronze batch size
    max_files_per_trigger: int | None = None  # files bronze / delta silver batch size
    run_seconds: float | None = None  # processingTime only: stop after N seconds
    dlq_kafka: bool = False  # also publish dead letters to <topic>.dlq (at-least-once)
    fail_on_data_loss: bool = True

    def uri(self, table: str) -> str:
        root = self.bronze_root if table == Tables.BRONZE else self.silver_root
        return table_uri(root, table)

    def checkpoint(self, query: str) -> str:
        return checkpoint_dir(self.checkpoint_root, query)

    def validate(self) -> None:
        if self.source not in ("kafka", "files"):
            raise ValueError(f"source must be kafka or files, got {self.source!r}")
        if self.trigger not in ("availableNow", "processingTime"):
            raise ValueError(
                f"trigger must be availableNow or processingTime, got {self.trigger!r}"
            )
        if self.source == "kafka" and "bronze" in self.stages and not self.bootstrap:
            raise ValueError("--bootstrap is required for the kafka source")
        if self.source == "files" and "bronze" in self.stages and not self.input_dir:
            raise ValueError("--input-dir is required for the files source")
        parse_duration_seconds(self.watermark_delay)
        bad = set(self.stages) - {"bronze", "silver"}
        if bad:
            raise ValueError(f"unknown stages {sorted(bad)}")


# ---------------------------------------------------------------- schemas

EVENT_SCHEMA = T.StructType(
    [
        T.StructField("event_id", T.StringType()),
        T.StructField("event_type", T.StringType()),
        T.StructField("event_ts", T.StringType()),
        T.StructField("claim_id", T.StringType()),
        T.StructField("member_id", T.StringType()),
        T.StructField("provider_id", T.StringType()),
        T.StructField("claim_type", T.StringType()),
        T.StructField("amount", T.DoubleType()),
    ]
)

_EVENT_FIELDS = (
    "event_id",
    "event_type",
    "event_ts",
    "claim_id",
    "member_id",
    "provider_id",
    "claim_type",
    "amount",
)

_COORDS = T.StructType(
    [
        T.StructField("source_topic", T.StringType()),
        T.StructField("source_partition", T.IntegerType()),
        T.StructField("source_offset", T.LongType()),
    ]
)

SILVER_SCHEMA = T.StructType(
    [
        T.StructField("event_id", T.StringType(), False),
        T.StructField("event_type", T.StringType()),
        T.StructField("event_ts", T.TimestampType()),
        T.StructField("claim_id", T.StringType()),
        T.StructField("member_id", T.StringType()),
        T.StructField("provider_id", T.StringType()),
        T.StructField("claim_type", T.StringType()),
        T.StructField("amount", T.DoubleType()),
        *_COORDS.fields,
        T.StructField("batch_id", T.LongType()),
        T.StructField("processed_at", T.TimestampType()),
    ]
)

LATE_SCHEMA = T.StructType(
    [
        *SILVER_SCHEMA.fields,
        T.StructField("reason_code", T.StringType()),
        T.StructField("watermark_cutoff", T.TimestampType()),
        T.StructField("lateness_seconds", T.LongType()),
    ]
)

DLQ_SCHEMA = T.StructType(
    [
        T.StructField("dlq_key", T.StringType(), False),
        T.StructField("reason_code", T.StringType()),
        T.StructField("reason_detail", T.StringType()),
        T.StructField("failed_fields", T.ArrayType(T.StringType())),
        *_COORDS.fields,
        T.StructField("source_file", T.StringType()),
        T.StructField("payload", T.BinaryType()),
        T.StructField("envelope_json", T.StringType()),
        T.StructField("batch_id", T.LongType()),
        T.StructField("dlq_at", T.TimestampType()),
    ]
)

STATUS_SCHEMA = T.StructType(
    [
        T.StructField("claim_id", T.StringType(), False),
        T.StructField("status", T.StringType()),
        T.StructField("status_rank", T.IntegerType()),
        T.StructField("last_event_id", T.StringType()),
        T.StructField("last_event_ts", T.TimestampType()),
        T.StructField("last_amount", T.DoubleType()),
        T.StructField("member_id", T.StringType()),
        T.StructField("provider_id", T.StringType()),
        T.StructField("claim_type", T.StringType()),
        T.StructField("submitted_ts", T.TimestampType()),
        T.StructField("last_batch_id", T.LongType()),
        T.StructField("updated_at", T.TimestampType()),
    ]
)

WM_STATE_SCHEMA = T.StructType(
    [
        T.StructField("query_key", T.StringType(), False),
        T.StructField("batch_id", T.LongType(), False),
        T.StructField("max_event_ts", T.TimestampType()),
        T.StructField("updated_at", T.TimestampType()),
    ]
)

METRICS_SCHEMA = T.StructType(
    [
        T.StructField("query_name", T.StringType(), False),
        T.StructField("query_id", T.StringType(), False),
        T.StructField("batch_id", T.LongType(), False),
        T.StructField("progress_ts", T.TimestampType()),
        T.StructField("num_input_rows", T.LongType()),
        T.StructField("input_rows_per_sec", T.DoubleType()),
        T.StructField("processed_rows_per_sec", T.DoubleType()),
        T.StructField("batch_duration_ms", T.LongType()),
        T.StructField("watermark_ts", T.TimestampType()),
        T.StructField("watermark_lag_s", T.DoubleType()),
        T.StructField("duplicates_dropped", T.LongType()),
        T.StructField("offsets_behind_latest", T.LongType()),
        T.StructField("on_time_count", T.LongType()),
        T.StructField("late_count", T.LongType()),
        T.StructField("dlq_count", T.LongType()),
    ]
)


def _create_table(spark: SparkSession, uri: str, schema: T.StructType) -> None:
    DeltaTable.createIfNotExists(spark).location(uri).addColumns(schema).execute()


def ensure_tables(spark: SparkSession, cfg: StreamConfig) -> None:
    """Create the silver-side tables up front so the first MERGE has a target."""
    for name, schema in (
        (Tables.SILVER, SILVER_SCHEMA),
        (Tables.LATE, LATE_SCHEMA),
        (Tables.DLQ, DLQ_SCHEMA),
        (Tables.STATUS, STATUS_SCHEMA),
        (Tables.WM_STATE, WM_STATE_SCHEMA),
        (Tables.METRICS, METRICS_SCHEMA),
    ):
        _create_table(spark, cfg.uri(name), schema)


def _merge_insert_only(
    spark: SparkSession, uri: str, source: DataFrame, keys: tuple[str, ...]
) -> None:
    """INSERT rows whose key is absent; matched rows are left alone (replay-safe)."""
    cond = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    (
        DeltaTable.forPath(spark, uri)
        .alias("t")
        .merge(source.alias("s"), cond)
        .whenNotMatchedInsertAll()
        .execute()
    )


# ---------------------------------------------------------------- bronze

_VALIDATION = T.StructType(
    [
        T.StructField("ok", T.BooleanType()),
        T.StructField("error_type", T.StringType()),
        T.StructField("errors", T.ArrayType(T.StringType())),
        T.StructField("failed_fields", T.ArrayType(T.StringType())),
    ]
)


@F.udf(returnType=_VALIDATION)
def _validate_udf(raw):  # pragma: no cover - runs on executors
    o = validation_outcome(raw)
    return (o.ok, o.error_type, o.errors, o.failed_fields)


@F.udf(returnType=T.StringType())
def _envelope_udf(raw, errors, error_type, topic, partition, offset):  # pragma: no cover
    return envelope_json(
        raw, list(errors or []), error_type or "contract_violation", topic, partition, offset
    )


def bronze_stream(spark: SparkSession, cfg: StreamConfig) -> DataFrame:
    """Raw source -> the bronze row shape (value kept as bytes: a poison message survives)."""
    if cfg.source == "kafka":
        r = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", cfg.bootstrap)
            .option("subscribe", cfg.topic)
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", str(cfg.fail_on_data_loss).lower())
        )
        if cfg.max_offsets_per_trigger:
            r = r.option("maxOffsetsPerTrigger", cfg.max_offsets_per_trigger)
        return r.load().select(
            F.col("value"),
            F.col("topic"),
            F.col("partition"),
            F.col("offset"),
            F.col("timestamp").alias("kafka_ts"),
            F.lit(None).cast("string").alias("source_file"),
            F.current_timestamp().alias("ingested_at"),
        )
    # files: stand-in for a broker in CI. No offsets, so source_offset is NULL and the DLQ key
    # falls back to a payload hash.
    r = spark.readStream.format("text").schema("value string")
    if cfg.max_files_per_trigger:
        r = r.option("maxFilesPerTrigger", cfg.max_files_per_trigger)
    return r.load(cfg.input_dir).select(
        F.col("value").cast("binary").alias("value"),
        F.lit(cfg.topic).alias("topic"),
        F.lit(0).alias("partition"),
        F.lit(None).cast("long").alias("offset"),
        F.current_timestamp().alias("kafka_ts"),
        F.input_file_name().alias("source_file"),
        F.current_timestamp().alias("ingested_at"),
    )


def _trigger_kwargs(cfg: StreamConfig) -> dict[str, object]:
    if cfg.trigger == "availableNow":
        return {"availableNow": True}
    return {"processingTime": cfg.processing_interval}


def start_bronze(spark: SparkSession, cfg: StreamConfig) -> StreamingQuery:
    """Append-only raw landing. Delta's streaming sink is itself exactly-once per batch."""
    return (
        bronze_stream(spark, cfg)
        .writeStream.queryName(BRONZE_QUERY)
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", cfg.checkpoint(BRONZE_QUERY))
        .trigger(**_trigger_kwargs(cfg))
        .start(cfg.uri(Tables.BRONZE))
    )


# ---------------------------------------------------------------- silver

_OUT_COLS = (
    "source_topic",
    "source_partition",
    "source_offset",
    "source_file",
    "value",
    "ok",
    "error_type",
    "errors",
    "failed_fields",
    *_EVENT_FIELDS,
)


def silver_plan(spark: SparkSession, cfg: StreamConfig) -> DataFrame:
    """bronze -> validated rows; valid rows are deduped on event_id within the watermark.

    Invalid rows bypass the stateful operator (they have no trustworthy event time) and are
    unioned back so the sink sees one stream.
    """
    r = spark.readStream.format("delta")
    if cfg.max_files_per_trigger:
        r = r.option("maxFilesPerTrigger", cfg.max_files_per_trigger)
    checked = r.load(cfg.uri(Tables.BRONZE)).withColumn("_v", _validate_udf(F.col("value")))
    ev = F.from_json(F.col("value").cast("string"), EVENT_SCHEMA)
    base = checked.select(
        F.col("topic").alias("source_topic"),
        F.col("partition").alias("source_partition"),
        F.col("offset").alias("source_offset"),
        "source_file",
        "value",
        F.col("_v.ok").alias("ok"),
        F.col("_v.error_type").alias("error_type"),
        F.col("_v.errors").alias("errors"),
        F.col("_v.failed_fields").alias("failed_fields"),
        ev.alias("_e"),
    )
    typed = base.select(
        *[c for c in _OUT_COLS if c not in _EVENT_FIELDS],
        *[F.col(f"_e.{f}").alias(f) for f in _EVENT_FIELDS if f != "event_ts"],
        F.to_timestamp(F.col("_e.event_ts"), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("event_ts"),
    ).select(*_OUT_COLS)
    valid = (
        typed.where("ok")
        .withColumn("value", F.lit(None).cast("binary"))  # payload only needed for dead letters
        .withWatermark("event_ts", cfg.watermark_delay)
        .dropDuplicatesWithinWatermark(["event_id"])
    )
    return valid.unionByName(typed.where(~F.col("ok")))


# ---------------------------------------------------------------- metrics


class MetricsCollector:
    """Thread-safe hand-off between ``foreachBatch`` (late/DLQ counts) and the listener."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._batch: dict[tuple[str, int], dict[str, int]] = {}
        self._rows: list[dict] = []
        self.flushed = 0

    def record_batch(self, query: str, batch_id: int, **counts: int) -> None:
        with self._lock:
            self._batch[(query, batch_id)] = counts

    def add_progress(self, row: dict) -> None:
        with self._lock:
            extra = self._batch.get((row["query_name"], row["batch_id"]), {})
            self._rows.append({**row, **extra})

    def drain(self) -> list[dict]:
        with self._lock:
            rows, self._rows = self._rows, []
            return rows

    def pending(self) -> int:
        with self._lock:
            return len(self._rows)


def _finite(x: object) -> float | None:
    try:
        f = float(x)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    return f if math.isfinite(f) else None


def _ts(text: str | None) -> datetime | None:
    return datetime.fromisoformat(text.replace("Z", "+00:00")) if text else None


def progress_row(p: dict) -> dict:
    """One ``stream_metrics`` row from a StreamingQueryProgress JSON document."""
    when = _ts(p.get("timestamp"))
    wm = _ts((p.get("eventTime") or {}).get("watermark"))
    state = (p.get("stateOperators") or [{}])[0]
    behind = None
    for src in p.get("sources") or []:
        m = (src.get("metrics") or {}).get("maxOffsetsBehindLatest")
        if m is not None:
            behind = int(float(m))
    return {
        "query_name": p.get("name"),
        "query_id": p.get("id"),
        "batch_id": int(p["batchId"]),
        "progress_ts": when,
        "num_input_rows": int(p.get("numInputRows") or 0),
        "input_rows_per_sec": _finite(p.get("inputRowsPerSecond")),
        "processed_rows_per_sec": _finite(p.get("processedRowsPerSecond")),
        "batch_duration_ms": (p.get("durationMs") or {}).get("triggerExecution"),
        "watermark_ts": wm,
        "watermark_lag_s": (when - wm).total_seconds() if when and wm else None,
        "duplicates_dropped": (state.get("customMetrics") or {}).get("numDroppedDuplicateRows")
        and int(state["customMetrics"]["numDroppedDuplicateRows"]),
        "offsets_behind_latest": behind,
    }


class MetricsListener(StreamingQueryListener):
    """Buffers per-batch progress; ``flush_metrics`` MERGEs it into ``stream_metrics``.

    Writing from inside the listener callback would run Spark jobs on the listener-bus thread,
    so it only buffers; the driver loop flushes.
    """

    def __init__(self, collector: MetricsCollector, queries: tuple[str, ...]) -> None:
        super().__init__()
        self.collector = collector
        self.queries = queries

    def onQueryStarted(self, event) -> None:  # noqa: N802
        pass

    def onQueryProgress(self, event) -> None:  # noqa: N802
        doc = json.loads(event.progress.json)
        if doc.get("name") in self.queries and doc.get("batchId") is not None:
            self.collector.add_progress(progress_row(doc))

    def onQueryIdle(self, event) -> None:  # noqa: N802
        pass

    def onQueryTerminated(self, event) -> None:  # noqa: N802
        pass


def flush_metrics(spark: SparkSession, cfg: StreamConfig, collector: MetricsCollector) -> int:
    rows = collector.drain()
    if not rows:
        return 0
    collector.flushed += len(rows)
    names = METRICS_SCHEMA.fieldNames()
    df = spark.createDataFrame(
        [tuple(r.get(n) for n in names) for r in rows], schema=METRICS_SCHEMA
    )
    (
        DeltaTable.forPath(spark, cfg.uri(Tables.METRICS))
        .alias("t")
        .merge(df.alias("s"), "t.query_id = s.query_id AND t.batch_id = s.batch_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    return len(rows)


# ---------------------------------------------------------------- the idempotent sink

FaultHook = Callable[[str, int], None]


class SilverSink:
    """``foreachBatch`` body. Every step is keyed, so re-running a batch is a no-op.

    Steps, in order (a crash between any two is repaired by replaying the whole batch):
    watermark state -> silver events -> late events -> dead letters -> status rollup.
    """

    def __init__(
        self,
        cfg: StreamConfig,
        collector: MetricsCollector,
        fault_hook: FaultHook | None = None,
    ) -> None:
        self.cfg = cfg
        self.collector = collector
        self.fault_hook = fault_hook
        self.delay_s = parse_duration_seconds(cfg.watermark_delay)
        self.query_key = cfg.checkpoint(SILVER_QUERY)

    def _step(self, name: str, batch_id: int) -> None:
        if self.fault_hook:
            self.fault_hook(name, batch_id)

    def __call__(self, batch_df: DataFrame, batch_id: int) -> None:
        batch = batch_df.persist()
        try:
            self._process(batch.sparkSession, batch, batch_id)
        finally:
            batch.unpersist()

    def _prev_high_water(self, spark: SparkSession, batch_id: int) -> int | None:
        """Epoch seconds of max event time over PREVIOUS batches (Spark's watermark rule)."""
        row = (
            spark.read.format("delta")
            .load(self.cfg.uri(Tables.WM_STATE))
            .where((F.col("query_key") == self.query_key) & (F.col("batch_id") < batch_id))
            .agg(F.max(F.col("max_event_ts").cast("long")))
            .first()
        )
        return row[0] if row else None

    def _process(self, spark: SparkSession, batch: DataFrame, batch_id: int) -> None:
        cfg = self.cfg
        valid = batch.where("ok").dropDuplicates(["event_id"])  # MERGE needs unique sources
        invalid = batch.where(~F.col("ok"))

        # 1. watermark state: this batch's max event time (all valid rows, late ones included,
        #    exactly what Spark's own watermark sees), keyed by (query, batch) so a replay
        #    overwrites itself and never changes the cutoff used for the same batch.
        prev = self._prev_high_water(spark, batch_id)
        cutoff = None if prev is None else prev - self.delay_s
        state = valid.agg(F.max("event_ts").alias("max_event_ts")).select(
            F.lit(self.query_key).alias("query_key"),
            F.lit(batch_id).cast("long").alias("batch_id"),
            "max_event_ts",
            F.current_timestamp().alias("updated_at"),
        )
        (
            DeltaTable.forPath(spark, cfg.uri(Tables.WM_STATE))
            .alias("t")
            .merge(state.alias("s"), "t.query_key = s.query_key AND t.batch_id = s.batch_id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        self._step("state", batch_id)

        is_late = F.lit(False) if cutoff is None else F.col("event_ts").cast("long") < F.lit(cutoff)
        on_time = valid.where(~is_late)
        silver_cols = [c.name for c in SILVER_SCHEMA.fields]
        meta = (
            F.lit(batch_id).cast("long").alias("batch_id"),
            F.current_timestamp().alias("processed_at"),
        )

        # 2. silver events: insert-if-absent on event_id (the effectively-once step).
        silver_uri = cfg.uri(Tables.SILVER)
        _merge_insert_only(
            spark,
            silver_uri,
            on_time.select(
                *[c for c in silver_cols if c not in ("batch_id", "processed_at")], *meta
            ).select(*silver_cols),
            ("event_id",),
        )
        self._step("silver", batch_id)

        # 3. late events: diverted, not dropped. A "late" row whose event_id is already in silver
        #    is a duplicate of an accepted event, not a late arrival, so it is excluded.
        accepted = spark.read.format("delta").load(silver_uri).select("event_id")
        late = valid.where(is_late).join(accepted, "event_id", "left_anti").persist()
        late_n = late.count()
        if late_n:
            late_out = late.select(
                *[c for c in silver_cols if c not in ("batch_id", "processed_at")],
                *meta,
                F.lit(REASON_LATE).alias("reason_code"),
                F.timestamp_seconds(F.lit(cutoff)).alias("watermark_cutoff"),
                (F.lit(cutoff) - F.col("event_ts").cast("long")).alias("lateness_seconds"),
            ).select(*[c.name for c in LATE_SCHEMA.fields])
            _merge_insert_only(spark, cfg.uri(Tables.LATE), late_out, ("event_id",))
        late.unpersist()
        self._step("late", batch_id)

        # 4. dead letters.
        dlq_n = self._dead_letters(spark, invalid, batch_id)
        self._step("dlq", batch_id)

        # 5. claim-status rollup from accepted (on-time) events only.
        self._rollup(spark, on_time, batch_id)
        self._step("rollup", batch_id)

        self.collector.record_batch(
            SILVER_QUERY,
            batch_id,
            on_time_count=on_time.count(),
            late_count=late_n,
            dlq_count=dlq_n,
        )

    def _dead_letters(self, spark: SparkSession, invalid: DataFrame, batch_id: int) -> int:
        cfg = self.cfg
        n = invalid.count()
        if n == 0:
            return 0
        key = F.when(
            F.col("source_offset").isNotNull(),
            F.concat_ws(":", "source_topic", "source_partition", "source_offset"),
        ).otherwise(
            F.concat(
                F.lit("sha256:"), F.sha2(F.coalesce(F.col("value"), F.lit("").cast("binary")), 256)
            )
        )
        dead = invalid.select(
            key.alias("dlq_key"),
            F.col("error_type").alias("reason_code"),
            F.concat_ws("; ", "errors").alias("reason_detail"),
            "failed_fields",
            "source_topic",
            "source_partition",
            "source_offset",
            "source_file",
            F.col("value").alias("payload"),
            _envelope_udf(
                "value", "errors", "error_type", "source_topic", "source_partition", "source_offset"
            ).alias("envelope_json"),
            F.lit(batch_id).cast("long").alias("batch_id"),
            F.current_timestamp().alias("dlq_at"),
        ).dropDuplicates(["dlq_key"])
        _merge_insert_only(spark, cfg.uri(Tables.DLQ), dead, ("dlq_key",))
        if cfg.dlq_kafka and cfg.bootstrap:
            # At-least-once: a replayed batch re-publishes. Consumers dedupe on the record key.
            (
                dead.select(
                    F.col("dlq_key").alias("key"),
                    F.col("envelope_json").alias("value"),
                    F.array(
                        F.struct(
                            F.lit("dlq-reason").alias("key"),
                            F.col("reason_code").cast("binary").alias("value"),
                        ),
                        F.struct(
                            F.lit("dlq-stage").alias("key"),
                            F.lit("consumer").cast("binary").alias("value"),
                        ),
                    ).alias("headers"),
                )
                .write.format("kafka")
                .option("kafka.bootstrap.servers", cfg.bootstrap)
                .option("topic", dlq_topic(cfg.topic))
                .save()
            )
        return n

    def _rollup(self, spark: SparkSession, on_time: DataFrame, batch_id: int) -> None:
        status = (
            F.when(F.col("event_type") == "claim_submitted", "submitted")
            .when(F.col("event_type") == "claim_paid", "paid")
            .otherwise("denied")
        )
        rank = F.when(F.col("event_type") == "claim_submitted", 0).otherwise(1)
        ranked = on_time.select(
            "claim_id", "event_id", "event_ts", "amount", "member_id", "provider_id", "claim_type",
            status.alias("status"), rank.alias("status_rank"),
        )  # fmt: skip
        newest = F.max(
            F.struct(
                "event_ts",
                "status_rank",
                "event_id",
                "status",
                "amount",
                "member_id",
                "provider_id",
                "claim_type",
            )  # fmt: skip
        ).alias("m")
        src = (
            ranked.groupBy("claim_id")
            .agg(
                newest,
                F.min(F.when(F.col("status") == "submitted", F.col("event_ts"))).alias(
                    "submitted_ts"
                ),
            )
            .select(
                "claim_id",
                F.col("m.status").alias("status"),
                F.col("m.status_rank").alias("status_rank"),
                F.col("m.event_id").alias("last_event_id"),
                F.col("m.event_ts").alias("last_event_ts"),
                F.col("m.amount").alias("last_amount"),
                F.col("m.member_id").alias("member_id"),
                F.col("m.provider_id").alias("provider_id"),
                F.col("m.claim_type").alias("claim_type"),
                "submitted_ts",
                F.lit(batch_id).cast("long").alias("last_batch_id"),
                F.current_timestamp().alias("updated_at"),
            )
        )
        newer = (
            "s.last_event_ts > t.last_event_ts OR (s.last_event_ts = t.last_event_ts AND "
            "(s.status_rank > t.status_rank OR (s.status_rank = t.status_rank AND "
            "s.last_event_id > t.last_event_id)))"
        )
        cols = [c.name for c in STATUS_SCHEMA.fields if c.name not in ("claim_id", "submitted_ts")]
        (
            DeltaTable.forPath(spark, self.cfg.uri(Tables.STATUS))
            .alias("t")
            .merge(src.alias("s"), "t.claim_id = s.claim_id")
            .whenMatchedUpdate(
                condition=newer,
                set={
                    **{c: f"s.{c}" for c in cols},
                    "submitted_ts": "least(s.submitted_ts, t.submitted_ts)",
                },
            )
            .whenMatchedUpdate(
                condition=(
                    "s.submitted_ts IS NOT NULL AND "
                    "(t.submitted_ts IS NULL OR s.submitted_ts < t.submitted_ts)"
                ),
                set={"submitted_ts": "s.submitted_ts"},
            )
            .whenNotMatchedInsertAll()
            .execute()
        )


def start_silver(
    spark: SparkSession,
    cfg: StreamConfig,
    collector: MetricsCollector,
    fault_hook: FaultHook | None = None,
) -> StreamingQuery:
    return (
        silver_plan(spark, cfg)
        .writeStream.queryName(SILVER_QUERY)
        .foreachBatch(SilverSink(cfg, collector, fault_hook))
        .option("checkpointLocation", cfg.checkpoint(SILVER_QUERY))
        .trigger(**_trigger_kwargs(cfg))
        .start()
    )


# ---------------------------------------------------------------- driver


@dataclass
class JobResult:
    counts: dict[str, int] = field(default_factory=dict)
    metrics_rows: int = 0  # rows MERGEd into stream_metrics during this run


def summarize(spark: SparkSession, cfg: StreamConfig) -> dict[str, int]:
    """Row counts of every table the job maintains (missing tables count as 0)."""
    out: dict[str, int] = {}
    for t in (
        Tables.BRONZE,
        Tables.SILVER,
        Tables.LATE,
        Tables.DLQ,
        Tables.STATUS,
        Tables.METRICS,
    ):
        try:
            out[t] = spark.read.format("delta").load(cfg.uri(t)).count()
        except Exception:  # noqa: BLE001 - table not created yet
            out[t] = 0
    return out


def build_stream_spark(
    cfg: StreamConfig, master: str | None = None, conf: dict[str, str] | None = None
) -> SparkSession:
    import os
    from pathlib import Path

    import pyspark

    # Python UDF workers must import lakehouse.* too (they do not inherit sys.path edits)
    src = str(Path(__file__).resolve().parents[2])
    pypath = os.pathsep.join(p for p in (src, os.environ.get("PYTHONPATH")) if p)

    packages = []
    if cfg.source == "kafka" and "bronze" in cfg.stages:
        packages.append(f"org.apache.spark:spark-sql-kafka-0-10_2.12:{pyspark.__version__}")
    return build_spark(
        "claims-streaming",
        master=master,
        bronze_root=cfg.bronze_root,
        conf={
            "spark.sql.shuffle.partitions": "4",
            "spark.executorEnv.PYTHONPATH": pypath,
            **(conf or {}),
        },
        extra_packages=packages,
    )


def _wait(spark, cfg, collector, queries: list[StreamingQuery]) -> None:
    """availableNow: until all done. processingTime: until run_seconds or a failure."""
    deadline = None if cfg.run_seconds is None else time.monotonic() + cfg.run_seconds
    while any(q.isActive for q in queries):
        for q in queries:
            exc = q.exception()
            if exc is not None:
                raise exc
        if deadline is not None and time.monotonic() >= deadline:
            for q in queries:
                q.stop()
            break
        time.sleep(1.0)
        flush_metrics(spark, cfg, collector)
    for q in queries:
        exc = q.exception()
        if exc is not None:
            raise exc


def run_job(
    spark: SparkSession, cfg: StreamConfig, fault_hook: FaultHook | None = None
) -> JobResult:
    """Run the configured stages. availableNow: bronze to completion, then silver."""
    cfg.validate()
    ensure_tables(spark, cfg)
    collector = MetricsCollector()
    listener = MetricsListener(collector, (BRONZE_QUERY, SILVER_QUERY))
    spark.streams.addListener(listener)
    try:
        stages = [s for s in ("bronze", "silver") if s in cfg.stages]
        if cfg.trigger == "availableNow":
            for stage in stages:
                q = (
                    start_bronze(spark, cfg)
                    if stage == "bronze"
                    else start_silver(spark, cfg, collector, fault_hook)
                )
                _wait(spark, cfg, collector, [q])
        else:
            qs = [
                start_bronze(spark, cfg)
                if s == "bronze"
                else start_silver(spark, cfg, collector, fault_hook)
                for s in stages
            ]
            _wait(spark, cfg, collector, qs)
    finally:
        _drain_listener_bus(spark)
        spark.streams.removeListener(listener)
        flush_metrics(spark, cfg, collector)
    return JobResult(counts=summarize(spark, cfg), metrics_rows=collector.flushed)


def _drain_listener_bus(spark: SparkSession) -> None:
    """Progress events are delivered asynchronously; wait so the last batch's row is not lost."""
    try:
        spark.sparkContext._jsc.sc().listenerBus().waitUntilEmpty(10000)
    except Exception:  # noqa: BLE001 - private API; fall back to a short sleep
        time.sleep(1.5)
    time.sleep(0.5)


def main(argv: list[str] | None = None) -> int:
    from .spark_cli import main as cli_main

    return cli_main(argv)


if __name__ == "__main__":
    import sys

    sys.exit(main())
