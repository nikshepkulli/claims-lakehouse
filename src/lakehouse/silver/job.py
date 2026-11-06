"""Silver batch job: bronze Delta -> silver Delta + silver_quarantine + silver_reconciliation.

Idempotency (docs/silver.md): a batch is the bronze ``_ingest_date`` D. Every silver table,
and the quarantine / reconciliation tables, is written with ``mode=overwrite`` +
``replaceWhere`` on exactly that batch, so a re-run atomically swaps its own rows and never
touches another batch. Silver is a pure function of the bronze batch (plus the reference sets
of bronze members/providers), so re-running with the same bronze gives the same rows.
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from datetime import date, datetime
from zoneinfo import ZoneInfo

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from lakehouse.bronze.datasets import DATASETS
from lakehouse.bronze.ingest import _write_replace as write_replace  # one replaceWhere helper
from lakehouse.bronze.ingest import batch_id_for
from lakehouse.bronze.paths import table_uri as bronze_uri

from .datasets import SPECS
from .paths import QUARANTINE, RECONCILIATION, table_uri
from .rules import LATE_ARRIVAL_DAYS
from .transform import DEDUPED, SILVER, classify, prepare, quarantine_frame, silver_frame
from .transform import QUARANTINE as Q

log = logging.getLogger("lakehouse.silver")

# Target rows per output file of a batch write (~100-200 MB of Parquet for these narrow tables).
# A batch is written as ceil(rows / ROWS_PER_FILE) files instead of one per shuffle partition.
ROWS_PER_FILE = 1_000_000

GROUPS = ("members", "providers", "claims", "claim_lines", "diagnoses", "notes", "events")

QUARANTINE_COLUMNS = (
    "dataset",
    "business_key",
    "reason_code",
    "reason_detail",
    "all_reason_codes",
    "source_batch",
    "source_file",
    "payload",
    "_ingest_date",
    "_quarantined_at",
)

RECONCILIATION_SCHEMA = T.StructType(
    [
        T.StructField("dataset", T.StringType(), False),
        T.StructField("batch_id", T.StringType(), False),
        T.StructField("batch_date", T.DateType(), False),
        T.StructField("bronze_rows", T.LongType(), False),
        T.StructField("silver_rows", T.LongType(), False),
        T.StructField("quarantined_rows", T.LongType(), False),
        T.StructField("deduped_rows", T.LongType(), False),
        T.StructField("identity_ok", T.BooleanType(), False),
        T.StructField("_run_ts", T.TimestampType(), False),
    ]
)


class ReconciliationError(RuntimeError):
    """bronze_rows != silver_rows + quarantined_rows + deduped_rows for some dataset."""


@dataclass
class DatasetResult:
    dataset: str
    batch_id: str
    bronze_rows: int = 0
    silver_rows: int = 0
    quarantined_rows: int = 0
    deduped_rows: int = 0
    identity_ok: bool = True
    skipped: bool = False  # no bronze rows for this batch

    def ok(self) -> bool:
        return self.bronze_rows == self.silver_rows + self.quarantined_rows + self.deduped_rows


def _predicate(d: date) -> str:
    return f"_ingest_date = DATE'{d.isoformat()}' AND _batch_id = '{batch_id_for(d)}'"


def _read_bronze(spark: SparkSession, bronze_root: str, name: str, d: date) -> DataFrame | None:
    uri = bronze_uri(bronze_root, name)
    if not DeltaTable.isDeltaTable(spark, uri):
        return None
    return spark.read.format("delta").load(uri).where(_predicate(d))


def _write_table(df: DataFrame, uri: str, d: date, n_rows: int) -> None:
    """Replace batch ``d`` in ``uri``, writing ceil(n_rows / ROWS_PER_FILE) files (>= 1)."""
    files = max(1, math.ceil(n_rows / ROWS_PER_FILE))
    write_replace(df.repartition(files), uri, _predicate(d), ["_ingest_date"])


def reference_ids(spark: SparkSession, bronze_root: str, name: str, key_col: str) -> DataFrame:
    """Ids of a dimension that pass its own silver rules, across ALL bronze snapshots.

    Independent of whether the dimension's silver batch has run, so claim batches do not depend
    on job order. A member that would itself be quarantined is not a valid reference.
    """
    uri = bronze_uri(bronze_root, name)
    if not DeltaTable.isDeltaTable(spark, uri):
        raise RuntimeError(
            f"bronze {name} not found at {uri}: cannot validate {key_col} references. "
            "Ingest the dimension snapshots into bronze first."
        )
    df, _ = prepare(spark.read.format("delta").load(uri), SPECS[name])
    return df.where(F.size("_reasons") == 0).select(F.col("_key").alias("id"))


def _counts(df: DataFrame) -> dict[str, int]:
    out = {SILVER: 0, Q: 0, DEDUPED: 0}
    for r in df.groupBy("_disp").count().collect():
        out[r["_disp"]] = r["count"]
    return out


def _enrich_claims(silver: DataFrame, lines_silver: DataFrame | None) -> DataFrame:
    """Late-arrival flag + completeness of the child lines that made it into silver.

    Without a lines batch (``lines_silver is None``) no line reached silver, so every claim is
    reported as ``lines_in_silver = 0`` / ``is_line_complete = false``.
    """
    if lines_silver is None:
        per_claim = silver.sparkSession.createDataFrame([], "_cid string, lines_in_silver int")
    else:
        per_claim = lines_silver.groupBy(F.col("claim_id").alias("_cid")).agg(
            F.count("*").cast("int").alias("lines_in_silver")
        )
    return (
        silver.withColumn("arrival_lag_days", F.datediff("received_date", "service_date"))
        .withColumn("is_late_arrival", F.col("arrival_lag_days") > LATE_ARRIVAL_DAYS)
        .join(per_claim, F.col("claim_id") == F.col("_cid"), "left")
        .drop("_cid")
        .withColumn("lines_in_silver", F.coalesce("lines_in_silver", F.lit(0)))
        .withColumn("is_line_complete", F.col("lines_in_silver") == F.col("line_count"))
    )


def _order_cols(df: DataFrame) -> DataFrame:
    meta = [c for c in df.columns if c.startswith("_")]
    data = [c for c in df.columns if not c.startswith("_")]
    return df.select(*data, *meta)


def silver_batch(
    spark: SparkSession,
    bronze_root: str,
    silver_root: str,
    batch_date: date,
    datasets: list[str] | None = None,
    run_ts: datetime | None = None,
) -> list[DatasetResult]:
    """Build silver for one batch date. Safe to call repeatedly.

    Raises ``ReconciliationError`` (after recording the numbers in ``silver_reconciliation``)
    if any dataset breaks ``bronze = silver + quarantined + deduped``.
    """
    ts = (run_ts or datetime.now(ZoneInfo("UTC"))).replace(tzinfo=None)  # session tz is UTC
    names = list(datasets or GROUPS)
    unknown = [n for n in names if n not in SPECS]
    if unknown:
        raise ValueError(f"unknown datasets: {unknown}")
    batch_id = batch_id_for(batch_date)
    bronze = {n: _read_bronze(spark, bronze_root, n, batch_date) for n in GROUPS}
    classified: dict[str, tuple[DataFrame, list[str]]] = {}
    refs: dict[str, DataFrame] = {}
    results: dict[str, DatasetResult] = {}

    family = ("claims", "claim_lines", "diagnoses")
    # lines/diagnoses are validated against the claims decision, so the family runs together.
    need_family = any(n in names for n in family)
    todo = [n for n in GROUPS if n in names or (need_family and n in family)]

    persisted: list[DataFrame] = []
    try:
        for name in todo:
            df = bronze[name]
            bronze_rows = 0 if df is None else df.count()
            if bronze_rows == 0:
                results[name] = DatasetResult(name, batch_id, skipped=True)
                if name == "claims":  # keep children well-defined: no claims -> every ref misses
                    refs["claims_in_batch"] = refs["claims_valid"] = spark.createDataFrame(
                        [], "id string"
                    )
                continue
            if name == "claims":
                refs["members"] = reference_ids(spark, bronze_root, "members", "member_id")
                refs["providers"] = reference_ids(spark, bronze_root, "providers", "provider_id")
            cls, cols = classify(df, SPECS[name], refs)
            cls = cls.persist()
            persisted.append(cls)
            classified[name] = (cls, cols)
            res = DatasetResult(name, batch_id, bronze_rows=bronze_rows)
            c = _counts(cls)
            res.silver_rows, res.quarantined_rows, res.deduped_rows = c[SILVER], c[Q], c[DEDUPED]
            results[name] = res
            if name == "claims":
                refs["claims_in_batch"] = cls.where(F.col("_key").isNotNull()).select(
                    F.col("_key").alias("id")
                )
                refs["claims_valid"] = cls.where(F.col("_disp") == SILVER).select(
                    F.col("_key").alias("id")
                )

        for name in todo:
            if name not in names or name not in classified:
                continue
            cls, cols = classified[name]
            out = silver_frame(cls, cols, ts)
            if name == "claims":
                lines = None
                if "claim_lines" in classified:
                    lines_cls, lines_cols = classified["claim_lines"]
                    lines = silver_frame(lines_cls, lines_cols, ts)
                out = _order_cols(_enrich_claims(out, lines))
            res = results[name]
            _write_table(out, table_uri(silver_root, name), batch_date, res.silver_rows)
            q = quarantine_frame(cls, name, ts)
            q_pred = f"dataset = '{name}' AND _ingest_date = DATE'{batch_date.isoformat()}'"
            write_replace(
                q.repartition(max(1, math.ceil(res.quarantined_rows / ROWS_PER_FILE))),
                table_uri(silver_root, QUARANTINE),
                q_pred,
                ["_ingest_date"],
            )
            # Verify from what was actually persisted, not from the in-memory counts.
            res.silver_rows = (
                spark.read.format("delta")
                .load(table_uri(silver_root, name))
                .where(_predicate(batch_date))
                .count()
            )
            res.quarantined_rows = (
                spark.read.format("delta")
                .load(table_uri(silver_root, QUARANTINE))
                .where(q_pred)
                .count()
            )
            res.identity_ok = res.ok()
            log.info(
                "silver %s %s: bronze=%d silver=%d quarantined=%d deduped=%d ok=%s",
                name,
                batch_id,
                res.bronze_rows,
                res.silver_rows,
                res.quarantined_rows,
                res.deduped_rows,
                res.identity_ok,
            )
    finally:
        for p in persisted:
            p.unpersist()

    final = [results[n] for n in GROUPS if n in names and n in results]
    for r in final:
        if r.skipped:
            continue
        rec = [
            (
                r.dataset,
                r.batch_id,
                batch_date,
                r.bronze_rows,
                r.silver_rows,
                r.quarantined_rows,
                r.deduped_rows,
                r.identity_ok,
                ts,
            )
        ]
        write_replace(
            spark.createDataFrame(rec, RECONCILIATION_SCHEMA),
            table_uri(silver_root, RECONCILIATION),
            f"dataset = '{r.dataset}' AND batch_id = '{r.batch_id}'",
            None,
        )
    broken = [r for r in final if not r.skipped and not r.identity_ok]
    if broken:
        raise ReconciliationError(
            "bronze != silver + quarantined + deduped for "
            + "; ".join(
                f"{r.dataset} {r.batch_id}: {r.bronze_rows} != {r.silver_rows} + "
                f"{r.quarantined_rows} + {r.deduped_rows}"
                for r in broken
            )
        )
    return final


def optimize_tables(spark: SparkSession, silver_root: str) -> list[str]:
    """Compact small files in every silver table (Delta OPTIMIZE). Not run by default."""
    done = []
    for name in (*GROUPS, QUARANTINE, RECONCILIATION):
        uri = table_uri(silver_root, name)
        if DeltaTable.isDeltaTable(spark, uri):
            DeltaTable.forPath(spark, uri).optimize().executeCompaction()
            done.append(name)
    return done


def bronze_batch_dates(spark: SparkSession, bronze_root: str) -> list[date]:
    """Every batch date that has rows in any bronze dataset table, ascending."""
    dates: set[date] = set()
    for name in DATASETS:
        uri = bronze_uri(bronze_root, name)
        if DeltaTable.isDeltaTable(spark, uri):
            rows = spark.read.format("delta").load(uri).select("_ingest_date").distinct().collect()
            dates.update(r[0] for r in rows)
    return sorted(dates)
