"""Row-level silver logic: standardise, cast, dedupe with a deterministic winner, validate.

Every bronze row ends up in exactly one disposition, which is what the reconciliation identity
(`bronze = silver + quarantined + deduped`) relies on:

* ``silver``      the dedupe winner, passed every rule
* ``quarantine``  a winner that broke at least one rule, or a conflicting duplicate
* ``deduped``     a loser that is byte-identical (all source columns) to its winner

Dedupe winner rule (per business key, within the batch): greatest bronze ``_ingest_ts``, then
greatest ``_source_file`` (string order), then greatest ``_row_hash``. Fully deterministic: the
same bronze rows always give the same winner regardless of read order.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from .datasets import Spec
from .rules import CONFLICTING_DUPLICATE, Rule

BRONZE_META = ("_ingest_ts", "_source_file", "_batch_id", "_ingest_date")
SILVER_META = (
    "_row_hash",
    "_source_file",
    "_batch_id",
    "_ingest_date",
    "_bronze_ingest_ts",
    "_silver_ts",
)

SILVER, QUARANTINE, DEDUPED = "silver", "quarantine", "deduped"


def source_columns(df: DataFrame) -> list[str]:
    """Data columns of a bronze table: everything except ingestion metadata and `_raw`."""
    return [c for c in df.columns if c not in BRONZE_META and c != "_raw"]


def standardise(df: DataFrame, spec: Spec, cols: list[str]) -> DataFrame:
    """Trim strings (blank -> NULL), fold enum columns to upper case, cast to the target types.

    Adds ``_cf_<col>`` (cast failed: value present but not castable) for every column. Codes
    (procedure, ICD-10) are only trimmed, never case-folded: the validity regexes are
    case-sensitive and a lower-case code is a defect, not something to repair.
    """
    exprs: dict[str, F.Column] = {}
    flags: dict[str, F.Column] = {}
    for c in cols:
        dt = df.schema[c].dataType
        col = F.col(c)
        if isinstance(dt, T.StringType):
            trimmed = F.trim(col)
            col = F.when(trimmed == "", F.lit(None).cast("string")).otherwise(trimmed)
        target = spec.casts.get(c) or spec.soft_casts.get(c)
        flag = F.lit(False)
        if target and dt.simpleString() != target:
            casted = col.cast(target)
            if c in spec.casts:
                flag = col.isNotNull() & casted.isNull()
            col = casted
        if c in spec.upper:
            col = F.upper(col)
        exprs[c] = col
        flags[c] = flag
    # Flags first: they are defined over the ORIGINAL columns, which the casts then overwrite.
    out = df
    for c, f in flags.items():
        out = out.withColumn(f"_cf_{c}", f)
    for c, e in exprs.items():
        out = out.withColumn(c, e)
    return out


def with_hash_and_payload(df: DataFrame, cols: list[str]) -> DataFrame:
    """`_row_hash` over the raw source columns (sorted, nulls ignored) and a JSON `_payload`."""
    s = F.struct(*[F.col(c) for c in sorted(cols)])
    return df.withColumn("_row_hash", F.sha2(F.to_json(s), 256)).withColumn(
        "_payload", F.to_json(F.struct(*[F.col(c) for c in cols]), {"ignoreNullFields": "false"})
    )


def add_reasons(df: DataFrame, rules: list[Rule]) -> DataFrame:
    """`_reasons`: array<struct<code,detail>> of every rule the row breaks, in rule order."""
    fired = [
        F.when(
            F.coalesce(r.fail, F.lit(False)),
            F.struct(F.lit(r.code).alias("code"), r.detail.cast("string").alias("detail")),
        )
        for r in rules
    ]
    return df.withColumn("_reasons", F.filter(F.array(*fired), lambda x: x.isNotNull()))


def prepare(
    bronze: DataFrame,
    spec: Spec,
    refs: dict[str, DataFrame] | None = None,
) -> tuple[DataFrame, list[str]]:
    """Bronze rows -> hashed, standardised rows carrying `_key`, `_ref_*` flags and `_reasons`.

    Returns the frame and the ordered list of output data columns (incl. optional ones).
    """
    cols = source_columns(bronze)
    df = with_hash_and_payload(bronze, cols)
    for c in spec.columns:
        if c not in df.columns:
            df = df.withColumn(c, F.lit(None).cast("string"))
            cols = [*cols, c]
    for c, t in spec.optional.items():
        if c not in df.columns:
            df = df.withColumn(c, F.lit(None).cast(t))
            cols = [*cols, c]
    df = standardise(df, spec, cols).withColumn("_key", spec.key())
    for ref_col, ref_name in spec.refs:
        ids = (refs or {})[ref_name].select(F.col("id").alias(f"_id_{ref_name}")).distinct()
        flag = f"_ref_{ref_name}"
        df = (
            df.join(F.broadcast(ids), F.col(ref_col) == F.col(f"_id_{ref_name}"), "left")
            .withColumn(flag, F.col(f"_id_{ref_name}").isNotNull())
            .drop(f"_id_{ref_name}")
        )
    return add_reasons(df, spec.rules()), cols


def classify(
    bronze: DataFrame,
    spec: Spec,
    refs: dict[str, DataFrame] | None = None,
) -> tuple[DataFrame, list[str]]:
    """Dedupe + validate. Output adds `_disp` (silver|quarantine|deduped) and the final `_reasons`.

    Row count is preserved 1:1 with the input (that is the reconciliation guarantee).
    """
    df, cols = prepare(bronze, spec, refs)
    # Rows with a NULL key are never grouped with each other by key; identical ones still collapse.
    df = df.withColumn("_dkey", F.coalesce(F.col("_key"), F.concat(F.lit("<null>:"), "_row_hash")))
    w = Window.partitionBy("_dkey").orderBy(
        F.col("_ingest_ts").desc(), F.col("_source_file").desc(), F.col("_row_hash").desc()
    )
    df = (
        df.withColumn("_rn", F.row_number().over(w))
        .withColumn("_win_hash", F.first("_row_hash").over(w))
        .withColumn(
            "_role",
            F.when(F.col("_rn") == 1, "winner")
            .when(F.col("_row_hash") == F.col("_win_hash"), "exact_dup")
            .otherwise("conflict"),
        )
    )
    conflict = F.array(
        F.struct(
            F.lit(CONFLICTING_DUPLICATE).alias("code"),
            F.concat(
                F.lit("same key, different content than the winning row "),
                F.substring("_win_hash", 1, 12),
                F.lit(" (winner = latest _ingest_ts, then _source_file, then _row_hash)"),
            ).alias("detail"),
        )
    )
    rule_reasons = F.col("_reasons")
    df = df.withColumn(
        "_disp",
        F.when(F.col("_role") == "exact_dup", DEDUPED)
        .when(F.col("_role") == "conflict", QUARANTINE)
        .when(F.size(rule_reasons) > 0, QUARANTINE)
        .otherwise(SILVER),
    ).withColumn("_reasons", F.when(F.col("_role") == "conflict", conflict).otherwise(rule_reasons))
    return df, cols


def silver_frame(df: DataFrame, cols: list[str], silver_ts) -> DataFrame:
    """Project the `silver` rows to the published schema (data columns first, then metadata)."""
    return (
        df.where(F.col("_disp") == SILVER)
        .withColumnRenamed("_ingest_ts", "_bronze_ingest_ts")
        .withColumn("_silver_ts", F.lit(silver_ts).cast("timestamp"))
        .select(*cols, *SILVER_META)
    )


def quarantine_frame(df: DataFrame, dataset: str, quarantined_at) -> DataFrame:
    """Project the `quarantine` rows to the silver_quarantine schema."""
    detail = F.array_join(
        F.transform("_reasons", lambda x: F.concat(x["code"], F.lit(": "), x["detail"])), "; "
    )
    return df.where(F.col("_disp") == QUARANTINE).select(
        F.lit(dataset).alias("dataset"),
        F.col("_key").alias("business_key"),
        F.col("_reasons")[0]["code"].alias("reason_code"),
        detail.alias("reason_detail"),
        F.transform("_reasons", lambda x: x["code"]).alias("all_reason_codes"),
        F.col("_batch_id").alias("source_batch"),
        F.col("_source_file").alias("source_file"),
        F.col("_payload").alias("payload"),
        F.col("_ingest_date"),
        F.lit(quarantined_at).cast("timestamp").alias("_quarantined_at"),
    )
