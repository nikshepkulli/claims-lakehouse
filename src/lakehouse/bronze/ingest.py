"""Bronze ingestion: raw claimsgen files -> append-only Delta tables.

Idempotency (see docs/bronze.md): every row carries ``_batch_id`` and ``_ingest_date`` (the
batch date). A batch is written with ``mode=overwrite`` + ``replaceWhere`` on exactly that
batch, so re-running a batch atomically swaps *its own* rows and never touches any other batch,
and never duplicates. Delta's transaction log keeps the previous version (time travel / replay).
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import date, datetime
from functools import reduce
from pathlib import Path
from zoneinfo import ZoneInfo

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from .datasets import DATASETS, Dataset
from .discover import SourceFile, discover
from .paths import QUARANTINE, SCHEMA_LOG, table_uri

log = logging.getLogger("lakehouse.bronze")

META_COLS = ("_ingest_ts", "_source_file", "_batch_id", "_ingest_date")

QUARANTINE_SCHEMA = T.StructType(
    [
        T.StructField("_batch_id", T.StringType(), False),
        T.StructField("dataset", T.StringType(), False),
        T.StructField("source_file", T.StringType(), False),
        T.StructField("reason", T.StringType(), False),
        T.StructField("payload", T.StringType(), True),  # offending raw line (JSONL), else null
        T.StructField("_ingest_ts", T.TimestampType(), False),
        T.StructField("_ingest_date", T.DateType(), False),
    ]
)

SCHEMA_LOG_SCHEMA = T.StructType(
    [
        T.StructField("dataset", T.StringType(), False),
        T.StructField("batch_id", T.StringType(), False),
        T.StructField("batch_date", T.DateType(), False),
        T.StructField("observed_schema", T.StringType(), False),  # StructType.json()
        T.StructField("observed_columns", T.ArrayType(T.StringType()), False),
        T.StructField("columns_added", T.ArrayType(T.StringType()), False),
        T.StructField("drift_detected", T.BooleanType(), False),
        T.StructField("files_read", T.IntegerType(), False),
        T.StructField("files_quarantined", T.IntegerType(), False),
        T.StructField("rows_written", T.LongType(), False),
        T.StructField("_ingest_ts", T.TimestampType(), False),
    ]
)


def batch_id_for(batch_date: date) -> str:
    return f"b{batch_date:%Y%m%d}"


@dataclass
class DatasetResult:
    dataset: str
    batch_id: str
    files_read: int = 0
    files_quarantined: int = 0
    rows_quarantined: int = 0
    # Footer-declared rows of Parquet files quarantined for CORRUPT_DATA_PAGE. Those rows are never
    # decoded, so they are not in rows_quarantined (which counts quarantine *records*).
    quarantined_file_rows: int = 0
    rows_written: int = 0
    columns_added: list[str] = field(default_factory=list)
    skipped: bool = False  # no input files for this batch


def _exists(spark: SparkSession, uri: str) -> bool:
    return DeltaTable.isDeltaTable(spark, uri)


def _read_file(spark: SparkSession, f: SourceFile) -> DataFrame:
    """Read one structured file. Raises if it is unreadable (footer/schema read fails)."""
    if f.fmt == "parquet":
        return spark.read.parquet(f.abs)
    if f.fmt == "csv":
        # CSV carries no types: land everything as string (bronze does not interpret).
        return spark.read.option("header", True).option("inferSchema", False).csv(f.abs)
    raise ValueError(f"unsupported structured format {f.fmt}")


CORRUPT_DATA_PAGE = "CORRUPT_DATA_PAGE"


def _decode_probe(spark: SparkSession, f: SourceFile) -> None:
    """Decode every column of every page of one Parquet file (raises if a page is corrupt).

    The ``noop`` sink consumes all columns without pruning, so nothing is skipped; nothing is
    written. Runs as a Spark job, so it works for any URI scheme the bronze reader supports.
    """
    spark.read.parquet(f.abs).write.format("noop").mode("overwrite").save()


def _footer_rows(spark: SparkSession, f: SourceFile) -> int | None:
    """Row count declared by the footer (no page decode, so it still works on a corrupt page)."""
    try:
        return spark.read.parquet(f.abs).count()
    except Exception:  # noqa: BLE001 - best effort, only used for reconciliation text
        return None


def _find_corrupt_page_files(
    spark: SparkSession, frames: list[tuple[SourceFile, DataFrame]]
) -> dict[str, tuple[SourceFile, str, int | None]]:
    """Slow path, run only after a batch write failed: probe each Parquet file on its own.

    Returns ``{rel: (file, error summary, footer row count)}`` for files whose pages cannot be
    decoded. Empty when no file is at fault (the original failure is then re-raised).
    """
    bad: dict[str, tuple[SourceFile, str, int | None]] = {}
    for f, _ in frames:
        if f.fmt != "parquet":
            continue
        try:
            _decode_probe(spark, f)
        except Exception as e:  # noqa: BLE001
            bad[f.rel] = (f, _error_summary(e), _footer_rows(spark, f))
    return bad


def _error_summary(e: Exception) -> str:
    """Root cause line of a (possibly Py4J-wrapped) Spark error, kept short."""
    lines = [ln.strip() for ln in str(e).splitlines() if ln.strip()]
    caused = [ln for ln in lines if ln.startswith("Caused by:")]
    return (caused[-1] if caused else (lines[0] if lines else type(e).__name__))[:500]


def _quarantine_row(
    batch_id: str, ds: str, src: str, reason: str, ts: datetime, bd: date
) -> tuple[str, str, str, str, None, datetime, date]:
    return (batch_id, ds, src, reason[:1000], None, ts, bd)


def _jsonl_frames(
    spark: SparkSession,
    f: SourceFile,
    ds: Dataset,
    batch_date: date,
    batch_id: str,
    ts: datetime,
    bd: date,
) -> tuple[DataFrame, DataFrame]:
    """Return (good rows with raw line kept in ``_raw``, quarantine rows) for a JSONL file."""
    lines = spark.read.text(f.abs).where(F.trim("value") != "")
    opts = {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt_record"}
    parsed = lines.select(
        F.col("value").alias("_raw"),
        F.from_json("value", f"{ds.json_ddl}, _corrupt_record STRING", opts).alias("j"),
    )
    is_bad = F.col("j").isNull() | F.col("j._corrupt_record").isNotNull()
    ok = parsed.where(~is_bad).select("j.*", "_raw").drop("_corrupt_record")
    bad = parsed.where(is_bad).select(
        F.lit("malformed JSON line or field type mismatch").alias("reason"),
        F.col("_raw").alias("payload"),
    )
    if ds.partition_key is None:  # flat stream file: route rows to batches by event date
        day = F.to_date(F.substring("event_ts", 1, 10))
        undated = ok.where(day.isNull()).select(
            F.lit("missing or unparseable event_ts").alias("reason"), F.col("_raw").alias("payload")
        )
        bad = bad.unionByName(undated)
        ok = ok.where(day == F.lit(batch_date))
    quarantine = bad.select(
        F.lit(batch_id).alias("_batch_id"),
        F.lit(ds.name).alias("dataset"),
        F.lit(f.rel).alias("source_file"),
        "reason",
        "payload",
        F.lit(ts).cast("timestamp").alias("_ingest_ts"),
        F.lit(bd).cast("date").alias("_ingest_date"),
    )
    return ok, quarantine


def _baseline_columns(
    spark: SparkSession, bronze_root: str, ds: str, before: date
) -> set[str] | None:
    """Columns seen in earlier batches of ``ds`` (None when there is no history to compare to)."""
    uri = table_uri(bronze_root, SCHEMA_LOG)
    if not _exists(spark, uri):
        return None
    rows = (
        spark.read.format("delta")
        .load(uri)
        .where((F.col("dataset") == ds) & (F.col("batch_date") < F.lit(before)))
        .select("observed_columns")
        .collect()
    )
    if not rows:
        return None
    return {c for r in rows for c in r["observed_columns"]}


def _write_replace(df: DataFrame, uri: str, predicate: str, partition_by: list[str] | None) -> None:
    w = (
        df.write.format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .option("replaceWhere", predicate)
    )
    if partition_by and not DeltaTable.isDeltaTable(df.sparkSession, uri):
        w = w.partitionBy(*partition_by)
    w.save(uri)


def ingest_dataset(
    spark: SparkSession,
    input_root: Path,
    bronze_root: str,
    ds: Dataset,
    batch_date: date,
    ingest_ts: datetime,
) -> DatasetResult:
    batch_id = batch_id_for(batch_date)
    res = DatasetResult(ds.name, batch_id)
    files = discover(input_root, ds, batch_date)
    if not files:
        res.skipped = True
        log.info("bronze %s %s: no input files, nothing to do", ds.name, batch_id)
        return res

    uri = table_uri(bronze_root, ds.name)
    # Column types already accepted for this table (existing table first, then earlier files).
    accepted: dict[str, T.DataType] = {}
    if _exists(spark, uri):
        accepted = {
            f.name: f.dataType
            for f in spark.read.format("delta").load(uri).schema
            if f.name not in META_COLS
        }

    good: list[tuple[SourceFile, DataFrame]] = []
    quarantine_rows: list[tuple] = []
    quarantine_frames: list[DataFrame] = []
    for f in files:
        try:
            if ds.kind == "jsonl":
                df, q = _jsonl_frames(spark, f, ds, batch_date, batch_id, ingest_ts, batch_date)
                quarantine_frames.append(q)
            else:
                df = _read_file(spark, f)
        except Exception as e:  # unreadable file: quarantine it, keep going
            reason = (
                f"unreadable file: {type(e).__name__}: {str(e).splitlines()[0] if str(e) else ''}"
            )
            log.warning("bronze %s: quarantining %s (%s)", ds.name, f.rel, reason)
            quarantine_rows.append(
                _quarantine_row(batch_id, ds.name, f.rel, reason, ingest_ts, batch_date)
            )
            res.files_quarantined += 1
            continue
        conflicts = [
            f"{c.name}: file has {c.dataType.simpleString()}, "
            f"table has {accepted[c.name].simpleString()}"
            for c in df.schema
            if c.name in accepted and accepted[c.name] != c.dataType
        ]
        if conflicts:
            reason = "incompatible column types: " + "; ".join(conflicts)
            log.warning("bronze %s: quarantining %s (%s)", ds.name, f.rel, reason)
            quarantine_rows.append(
                _quarantine_row(batch_id, ds.name, f.rel, reason, ingest_ts, batch_date)
            )
            res.files_quarantined += 1
            continue
        for c in df.schema:
            accepted.setdefault(c.name, c.dataType)
        good.append((f, df.withColumn("_source_file", F.lit(f.rel))))
        res.files_read += 1

    q_uri = table_uri(bronze_root, QUARANTINE)
    q_pred = f"dataset = '{ds.name}' AND _batch_id = '{batch_id}'"

    def write_quarantine() -> None:
        # idempotent per batch+dataset, even when empty so a clean re-run clears old rows
        q_df = spark.createDataFrame(quarantine_rows, QUARANTINE_SCHEMA)
        for qf in quarantine_frames:
            q_df = q_df.unionByName(qf)
        _write_replace(q_df, q_uri, q_pred, ["_ingest_date"])
        res.rows_quarantined = spark.read.format("delta").load(q_uri).where(q_pred).count()

    write_quarantine()
    if not good:
        log.warning("bronze %s %s: no readable files; table untouched", ds.name, batch_id)
        return res

    pred = f"_ingest_date = DATE'{batch_date.isoformat()}' AND _batch_id = '{batch_id}'"
    probed = False
    while True:
        data = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), [d for _, d in good])
        observed = [c.name for c in data.schema if c.name != "_source_file"]
        observed_schema = T.StructType([c for c in data.schema if c.name != "_source_file"])

        baseline = _baseline_columns(spark, bronze_root, ds.name, batch_date)
        added = sorted(set(observed) - baseline) if baseline is not None else []
        res.columns_added = added

        out = (
            data.withColumn("_ingest_ts", F.lit(ingest_ts).cast("timestamp"))
            .withColumn("_batch_id", F.lit(batch_id))
            .withColumn("_ingest_date", F.lit(batch_date).cast("date"))
        )
        meta_last = [c for c in out.columns if c not in META_COLS]
        out = out.select(*meta_last, *META_COLS)
        try:
            _write_replace(out, uri, pred, ["_ingest_date"])
            break
        except Exception:
            # The footer check above cannot see a file that is corrupt only inside a data page; it
            # surfaces here, at decode time, and a Delta overwrite commits nothing when it fails.
            # Optimistic path costs nothing; only on failure probe each file once (issue #55).
            if probed:
                raise
            probed = True
            bad = _find_corrupt_page_files(spark, good)
            if not bad:
                raise  # not a page problem (permissions, schema merge, ...): do not mask it
            for rel, (_, err, nrows) in bad.items():
                declared = f"{nrows} rows" if nrows is not None else "unknown row count"
                reason = (
                    f"{CORRUPT_DATA_PAGE}: footer readable ({declared} declared) but a data page "
                    f"cannot be decoded: {err}"
                )
                log.warning("bronze %s: quarantining %s (%s)", ds.name, rel, reason)
                quarantine_rows.append(
                    _quarantine_row(batch_id, ds.name, rel, reason, ingest_ts, batch_date)
                )
                res.files_quarantined += 1
                res.files_read -= 1
                res.quarantined_file_rows += nrows or 0
            good = [(f, d) for f, d in good if f.rel not in bad]
            write_quarantine()
            if not good:
                log.warning("bronze %s %s: no readable files; table untouched", ds.name, batch_id)
                return res

    if added:
        log.warning(
            "SCHEMA_DRIFT dataset=%s batch=%s new_columns=%s (merged into the bronze schema)",
            ds.name,
            batch_id,
            added,
        )
    res.rows_written = spark.read.format("delta").load(uri).where(pred).count()

    log_row = [
        (
            ds.name,
            batch_id,
            batch_date,
            json.dumps(json.loads(observed_schema.json())),
            observed,
            added,
            bool(added),
            res.files_read,
            res.files_quarantined,
            res.rows_written,
            ingest_ts,
        )
    ]
    _write_replace(
        spark.createDataFrame(log_row, SCHEMA_LOG_SCHEMA),
        table_uri(bronze_root, SCHEMA_LOG),
        f"dataset = '{ds.name}' AND batch_id = '{batch_id}'",
        None,
    )
    log.info(
        "bronze %s %s: files=%d quarantined_files=%d quarantined_rows=%d rows=%d drift=%s",
        ds.name,
        batch_id,
        res.files_read,
        res.files_quarantined,
        res.rows_quarantined,
        res.rows_written,
        added,
    )
    return res


def ingest_batch(
    spark: SparkSession,
    input_root: Path,
    bronze_root: str,
    batch_date: date,
    datasets: list[str] | None = None,
    ingest_ts: datetime | None = None,
) -> list[DatasetResult]:
    """Ingest every (selected) dataset for ``batch_date``. Safe to call repeatedly."""
    ts = (ingest_ts or datetime.now(ZoneInfo("UTC"))).replace(tzinfo=None)  # session tz is UTC
    names = datasets or list(DATASETS)
    return [
        ingest_dataset(spark, input_root, bronze_root, DATASETS[n], batch_date, ts) for n in names
    ]
