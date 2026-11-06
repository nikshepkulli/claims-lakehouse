"""Type-1 / type-0 dimensions: ``dim_diagnosis`` (upsert by ICD-10 code) and ``dim_date``."""

from __future__ import annotations

from datetime import date, datetime

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .keys import derive_sk
from .schema import TABLES
from .tables import align, ensure_table, exists, read

DEFAULT_DESCRIPTION = "Synthetic ICD-10 code"
MONTH_NAMES = [
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
]


def icd_descriptions() -> dict[str, str]:
    """Code -> description from the generator's reference code set (empty if unavailable)."""
    try:
        from claimsgen.reference import DIAGNOSES
    except ImportError:  # claimsgen's deps (numpy) are not installed: fall back to the default
        return {}
    return {code: desc for code, desc, _ in DIAGNOSES}


def _ntz(ts: datetime) -> F.Column:
    return F.lit(ts.strftime("%Y-%m-%d %H:%M:%S")).cast("timestamp_ntz")


def diagnosis_rows(
    spark: SparkSession, silver_dx: DataFrame, batch_id: str, run_ts: datetime
) -> DataFrame:
    """Distinct valid ICD-10 codes of a silver diagnoses batch as ``dim_diagnosis`` rows.

    ``icd10_chapter`` is the leading letter (the ICD-10 chapter block letter, VARCHAR(2) in the
    DDL), not the roman-numeral chapter title.
    """
    desc = icd_descriptions()
    ref = spark.createDataFrame(list(desc.items()), "_code string, _desc string")
    codes = silver_dx.where(F.col("icd10_code").isNotNull()).select("icd10_code").distinct()
    out = (
        codes.join(F.broadcast(ref), F.col("icd10_code") == F.col("_code"), "left")
        .withColumn("diagnosis_sk", derive_sk("diagnosis", F.col("icd10_code")))
        .withColumn("icd10_chapter", F.substring("icd10_code", 1, 1))
        .withColumn("description", F.coalesce("_desc", F.lit(DEFAULT_DESCRIPTION)))
        .withColumn("load_batch_id", F.lit(batch_id))
        .withColumn("loaded_at", _ntz(run_ts))
    )
    return align(out, TABLES["dim_diagnosis"])


def merge_diagnosis(spark: SparkSession, uri: str, rows: DataFrame) -> int:
    """Upsert by ``diagnosis_sk``; returns how many codes were new. Re-running changes nothing."""
    ensure_table(spark, uri, "dim_diagnosis")
    before = read(spark, uri).count()
    cols = [c.name for c in TABLES["dim_diagnosis"]]
    (
        DeltaTable.forPath(spark, uri)
        .alias("t")
        .merge(rows.alias("s"), "t.diagnosis_sk = s.diagnosis_sk")
        .whenMatchedUpdate(
            condition=(
                "NOT (t.description <=> s.description AND t.icd10_chapter <=> s.icd10_chapter)"
            ),
            set={c: f"s.{c}" for c in cols},
        )
        .whenNotMatchedInsert(values={c: f"s.{c}" for c in cols})
        .execute()
    )
    return read(spark, uri).count() - before


def date_rows(spark: SparkSession, start: date, end: date) -> DataFrame:
    """One ``dim_date`` row per calendar day in [start, end]."""
    days = spark.range(1).select(
        F.explode(F.sequence(F.lit(start), F.lit(end), F.expr("interval 1 day"))).alias("full_date")
    )
    d = F.col("full_date")
    iso_dow = ((F.dayofweek(d) + 5) % 7) + 1  # Spark: Sunday = 1; ISO: Monday = 1
    names = F.array(*[F.lit(n) for n in MONTH_NAMES])
    out = (
        days.withColumn("date_key", F.year(d) * 10000 + F.month(d) * 100 + F.dayofmonth(d))
        .withColumn("year", F.year(d))
        .withColumn("quarter", F.quarter(d))
        .withColumn("month", F.month(d))
        .withColumn("month_name", F.element_at(names, F.month(d)))  # locale independent
        .withColumn("day_of_month", F.dayofmonth(d))
        .withColumn("day_of_week", iso_dow)
        .withColumn("is_weekend", iso_dow >= 6)
    )
    return align(out, TABLES["dim_date"])


def ensure_dim_date(spark: SparkSession, uri: str, lo: date, hi: date) -> bool:
    """Make ``dim_date`` cover every whole calendar year touched by [lo, hi].

    The window only ever grows (a late-arriving claim from an earlier year widens it); the table
    is rewritten when it must grow and left alone otherwise. Returns True when it was written.
    """
    start, end = date(lo.year, 1, 1), date(hi.year, 12, 31)
    if exists(spark, uri):
        cur = read(spark, uri).agg(F.min("full_date"), F.max("full_date")).first()
        if cur[0] is not None:
            if cur[0] <= start and cur[1] >= end:
                return False
            start, end = min(start, cur[0]), max(end, cur[1])
    ensure_table(spark, uri, "dim_date")
    date_rows(spark, start, end).write.format("delta").mode("overwrite").save(uri)
    return True
