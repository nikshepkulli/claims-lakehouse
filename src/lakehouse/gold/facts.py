"""Fact builders: silver claims / lines -> ``fact_claim`` / ``fact_claim_line`` for one batch.

A silver claim reaches gold only if all of these hold, otherwise it is HELD BACK with a reason
(recorded in ``gold_exclusions`` and counted in ``gold_reconciliation``; nothing is dropped
silently):

* ``UNRESOLVED_MEMBER`` / ``UNRESOLVED_PROVIDER``: no dimension version covers the service date.
  Silver already guarantees the id exists in bronze, so this means a dimension snapshot has not
  been loaded yet (ordering error). The job treats it as a failure, not as a data defect.
* ``INCOMPLETE_LINES``: the silver lines of the claim are not exactly ``line_count`` (a line was
  quarantined in silver, or the claim has none). Its header totals would not equal its lines.
* ``TOTALS_MISMATCH``: header totals differ from the sum of the lines.

Lines follow their claim: ``PARENT_CLAIM_EXCLUDED`` / ``PARENT_CLAIM_NOT_IN_BATCH``.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from .keys import derive_sk, natural_id
from .schema import TABLES
from .tables import align

UNRESOLVED = ("UNRESOLVED_MEMBER", "UNRESOLVED_PROVIDER")


def date_key(col: Column) -> Column:
    """DATE -> integer YYYYMMDD, the ``dim_date`` key."""
    return (F.year(col) * 10000 + F.month(col) * 100 + F.dayofmonth(col)).cast("int")


def _as_of(dim: DataFrame, id_col: str, sk_col: str, prefix: str) -> DataFrame:
    return dim.select(
        F.col(id_col).alias(f"{prefix}_id"),
        F.col(sk_col).alias(f"{prefix}_sk"),
        F.col("effective_from").alias(f"{prefix}_from"),
        F.col("effective_to").alias(f"{prefix}_to"),
    )


def resolve_dimensions(
    claims: DataFrame, dim_member: DataFrame, dim_provider: DataFrame
) -> DataFrame:
    """Add ``member_sk`` / ``provider_sk``: the version valid on the claim's service date.

    Valid means ``effective_from <= service_date < effective_to`` (``effective_to`` NULL = open).
    A claim whose id has no covering version gets NULL. Versions never overlap, so the join
    cannot multiply rows.
    """
    svc = F.col("service_date").cast("timestamp_ntz")
    m = _as_of(dim_member, "member_id", "member_sk", "_m")
    p = _as_of(dim_provider, "provider_id", "provider_sk", "_p")
    out = claims.join(
        m,
        (natural_id(F.col("member_id")) == F.col("_m_id"))
        & (svc >= F.col("_m_from"))
        & (F.col("_m_to").isNull() | (svc < F.col("_m_to"))),
        "left",
    ).join(
        p,
        (natural_id(F.col("provider_id")) == F.col("_p_id"))
        & (svc >= F.col("_p_from"))
        & (F.col("_p_to").isNull() | (svc < F.col("_p_to"))),
        "left",
    )
    return out.withColumnRenamed("_m_sk", "member_sk").withColumnRenamed("_p_sk", "provider_sk")


@dataclass
class FactFrames:
    fact_claim: DataFrame
    fact_claim_line: DataFrame
    exclusions: DataFrame  # dataset, business_key, reason_code, reason_detail
    cached: DataFrame | None = None  # persisted intermediate; call release() when done

    def release(self) -> None:
        if self.cached is not None:
            self.cached.unpersist()


def build_facts(
    silver_claims: DataFrame,
    silver_lines: DataFrame,
    silver_dx: DataFrame,
    dim_member: DataFrame,
    dim_provider: DataFrame,
    batch_id: str,
    run_ts: datetime,
) -> FactFrames:
    """Pure transformation of one silver batch (already filtered to the batch) into fact frames."""
    claims = silver_claims
    if "place_of_service" not in claims.columns:
        claims = claims.withColumn("place_of_service", F.lit(None).cast("string"))

    sums = silver_lines.groupBy("claim_id").agg(
        F.count("*").alias("_n"),
        F.sum("billed_amount").alias("_sb"),
        F.sum("allowed_amount").alias("_sa"),
        F.sum("paid_amount").alias("_sp"),
    )
    primary = (
        silver_dx.where((F.col("diagnosis_type") == "PRIMARY") & F.col("icd10_code").isNotNull())
        .groupBy("claim_id")
        .agg(F.min_by("icd10_code", "dx_seq").alias("_primary_code"))
    )
    c = resolve_dimensions(claims, dim_member, dim_provider)
    c = c.join(sums, "claim_id", "left").join(primary, "claim_id", "left")

    reason = (
        F.when(F.col("member_sk").isNull(), "UNRESOLVED_MEMBER")
        .when(F.col("provider_sk").isNull(), "UNRESOLVED_PROVIDER")
        .when(F.col("_n").isNull() | (F.col("_n") != F.col("line_count")), "INCOMPLETE_LINES")
        .when(
            (F.col("total_billed_amount") != F.col("_sb"))
            | (F.col("total_allowed_amount") != F.col("_sa"))
            | (F.col("total_paid_amount") != F.col("_sp")),
            "TOTALS_MISMATCH",
        )
    )
    detail = F.concat_ws(
        " ",
        F.lit("line_count="),
        F.col("line_count").cast("string"),
        F.lit("lines_in_silver="),
        F.coalesce(F.col("_n"), F.lit(0)).cast("string"),
        F.lit("member_id="),
        F.col("member_id"),
        F.lit("provider_id="),
        F.col("provider_id"),
    )
    c = c.withColumn("_reason", reason).withColumn("_detail", detail).persist()

    ts = F.lit(run_ts.strftime("%Y-%m-%d %H:%M:%S")).cast("timestamp_ntz")
    good = c.where(F.col("_reason").isNull())
    fact_claim = align(
        good.withColumn(
            "primary_diagnosis_sk",
            F.when(
                F.col("_primary_code").isNotNull(), derive_sk("diagnosis", F.col("_primary_code"))
            ),
        )
        .withColumn("service_date_key", date_key(F.col("service_date")))
        .withColumn("received_date_key", date_key(F.col("received_date")))
        .withColumn("load_batch_id", F.lit(batch_id))
        .withColumn("loaded_at", ts),
        TABLES["fact_claim"],
    )

    verdict = c.select(F.col("claim_id").alias("_v_claim"), F.col("_reason").alias("_v_reason"))
    ln = silver_lines.join(verdict, F.col("claim_id") == F.col("_v_claim"), "left")
    line_reason = F.when(F.col("_v_claim").isNull(), "PARENT_CLAIM_NOT_IN_BATCH").when(
        F.col("_v_reason").isNotNull(), "PARENT_CLAIM_EXCLUDED"
    )
    fact_line = align(
        ln.where(line_reason.isNull())
        .withColumn("service_date_key", date_key(F.col("service_date")))
        .withColumn("load_batch_id", F.lit(batch_id))
        .withColumn("loaded_at", ts),
        TABLES["fact_claim_line"],
    )

    excl_claims = c.where(F.col("_reason").isNotNull()).select(
        F.lit("claims").alias("dataset"),
        F.col("claim_id").alias("business_key"),
        F.col("_reason").alias("reason_code"),
        F.col("_detail").alias("reason_detail"),
    )
    excl_lines = ln.where(line_reason.isNotNull()).select(
        F.lit("claim_lines").alias("dataset"),
        F.col("line_id").alias("business_key"),
        line_reason.alias("reason_code"),
        F.concat(F.lit("claim_id="), F.col("claim_id")).alias("reason_detail"),
    )
    return FactFrames(fact_claim, fact_line, excl_claims.unionByName(excl_lines), c)
