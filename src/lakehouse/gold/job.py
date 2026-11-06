"""Gold batch job: silver Delta -> star-schema Delta + gold_reconciliation + Parquet export.

A batch is a silver ``_ingest_date`` D (same ``bYYYYMMDD`` id as bronze and silver). For batch D:

1. dimensions: the silver members / providers snapshot of D is merged into the SCD2 tables
   (``scd2.py``), new diagnosis codes are upserted, ``dim_date`` is widened if needed;
2. if the merge changed history that already existed (a late-arriving correction), facts already
   loaded for the affected members / providers are re-pointed at the right versions;
3. facts: silver claims / lines of D are resolved against the dimensions as of each service date
   and written with ``replaceWhere load_batch_id = 'bYYYYMMDD'`` (idempotent per batch);
   claims that cannot be loaded are held back in ``gold_exclusions`` with a reason;
4. reconciliation: silver = gold + excluded for row counts and billed/allowed/paid sums, plus key
   integrity, all re-read from the written Delta tables and recorded in ``gold_reconciliation``.
   Any mismatch raises ``ReconciliationError`` after the numbers are written.

Orphan and quarantined silver rows are not in silver, so they cannot reach gold; the job never
reads ``silver_quarantine`` as data.
"""

from __future__ import annotations

import json
import logging
import math
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from lakehouse.bronze.ingest import batch_id_for
from lakehouse.silver.paths import table_uri as silver_uri

from .dims import diagnosis_rows, ensure_dim_date, merge_diagnosis
from .facts import UNRESOLVED, build_facts, resolve_dimensions
from .paths import EXCLUSIONS, RECONCILIATION, export_uri, table_uri
from .scd2 import MEMBER, PROVIDER, DimSpec, ScdResult, scd2_merge, scd2_violations, typed_snapshot
from .schema import EXCLUSION_COLS, MIN_VACUUM_HOURS, RECONCILIATION_COLS, TABLES
from .tables import align, ensure_table, exists, read, struct_of, write_replace

log = logging.getLogger("lakehouse.gold")

# Target rows per output file (same figure as silver): one file per batch partition until a
# batch exceeds it.
ROWS_PER_FILE = 1_000_000

SILVER_INPUTS = ("members", "providers", "claims", "claim_lines", "diagnoses")
AMOUNTS = {"fact_claim": ("total_billed_amount", "total_allowed_amount", "total_paid_amount")}
LINES_DDL = (
    "line_id string, claim_id string, line_number int, procedure_code string, units int, "
    "service_date date, billed_amount decimal(12,2), allowed_amount decimal(12,2), "
    "paid_amount decimal(12,2)"
)
DX_DDL = "claim_id string, dx_seq int, icd10_code string, diagnosis_type string"
AMOUNTS["fact_claim_line"] = ("billed_amount", "allowed_amount", "paid_amount")


class ReconciliationError(RuntimeError):
    """Silver does not reconcile to gold (rows, sums or keys) for some table."""


@dataclass
class TableResult:
    gold_table: str
    batch_id: str
    silver_rows: int = 0
    gold_rows: int = 0
    excluded_rows: int = 0
    key_violations: int = 0
    identity_ok: bool = True
    detail: dict = field(default_factory=dict)


# ----------------------------------------------------------------------------------- reading


def _pred(d: date) -> str:
    return f"_ingest_date = DATE'{d.isoformat()}'"


def _silver(spark: SparkSession, silver_root: str, name: str, d: date) -> DataFrame | None:
    """Silver rows of batch ``d`` (None when the table is missing or the batch is empty)."""
    uri = silver_uri(silver_root, name)
    if not exists(spark, uri):
        return None
    df = read(spark, uri).where(_pred(d))
    return df if df.limit(1).count() else None


def silver_batch_dates(spark: SparkSession, silver_root: str) -> list[date]:
    """Every batch date with rows in a silver input table, ascending."""
    dates: set[date] = set()
    for name in SILVER_INPUTS:
        uri = silver_uri(silver_root, name)
        if exists(spark, uri):
            dates.update(r[0] for r in read(spark, uri).select("_ingest_date").distinct().collect())
    return sorted(dates)


def _ts(run_ts: datetime | None) -> datetime:
    return (run_ts or datetime.now(ZoneInfo("UTC"))).replace(tzinfo=None, microsecond=0)


def _dec(v) -> Decimal:
    return v if v is not None else Decimal(0)


# ----------------------------------------------------------------------------------- dimensions


def _merge_dimension(
    spark: SparkSession,
    silver_root: str,
    gold_root: str,
    spec: DimSpec,
    silver_name: str,
    d: date,
    ts: datetime,
) -> ScdResult | None:
    snap = _silver(spark, silver_root, silver_name, d)
    if snap is None:
        return None
    res = scd2_merge(spark, table_uri(gold_root, spec.table), spec, snap, d, batch_id_for(d), ts)
    log.info(
        "gold %s %s: snapshot_keys=%d inserted=%d updated=%d removed=%d",
        spec.table,
        batch_id_for(d),
        res.snapshot_keys,
        res.versions_inserted,
        res.versions_updated,
        res.versions_removed,
    )
    return res


def _date_window(*frames: tuple[DataFrame | None, tuple[str, ...]]) -> tuple[date, date] | None:
    los, his = [], []
    for df, cols in frames:
        if df is None:
            continue
        r = df.agg(*[F.min(c) for c in cols], *[F.max(c) for c in cols]).first()
        vals = list(r)
        los += [v for v in vals[: len(cols)] if v is not None]
        his += [v for v in vals[len(cols) :] if v is not None]
    return (min(los), max(his)) if los else None


def rekey_facts(
    spark: SparkSession,
    silver_root: str,
    gold_root: str,
    changed_members: DataFrame | None,
    changed_providers: DataFrame | None,
) -> int:
    """Re-point loaded ``fact_claim`` rows of members / providers whose history changed.

    A late-arriving snapshot can split or merge versions below facts that are already loaded, so
    the version valid on a claim's service date may no longer be the one the fact points at.
    Recomputes ``member_sk`` / ``provider_sk`` for claims of the changed keys (all silver
    batches) and updates only rows whose keys differ. Returns the number of facts updated.
    """
    fc_uri = table_uri(gold_root, "fact_claim")
    if not exists(spark, fc_uri) or (changed_members is None and changed_providers is None):
        return 0
    claims_uri = silver_uri(silver_root, "claims")
    if not exists(spark, claims_uri):
        return 0
    claims = read(spark, claims_uri).select("claim_id", "member_id", "provider_id", "service_date")
    flag = F.lit(False)
    if changed_members is not None:
        cm = changed_members.select(F.col("member_id").alias("_cm_id")).distinct()
        claims = claims.join(cm, F.col("member_id") == F.col("_cm_id"), "left")
        flag = flag | F.col("_cm_id").isNotNull()
    if changed_providers is not None:
        cp = changed_providers.select(F.col("provider_id").alias("_cp_id")).distinct()
        claims = claims.join(cp, F.col("provider_id") == F.col("_cp_id"), "left")
        flag = flag | F.col("_cp_id").isNotNull()
    claims = claims.where(flag).dropDuplicates(["claim_id"])
    fixed = resolve_dimensions(
        claims,
        read(spark, table_uri(gold_root, "dim_member")),
        read(spark, table_uri(gold_root, "dim_provider")),
    ).where(F.col("member_sk").isNotNull() & F.col("provider_sk").isNotNull())
    fixed = fixed.select("claim_id", "member_sk", "provider_sk").persist()
    try:
        stale = (
            read(spark, fc_uri)
            .alias("t")
            .join(fixed.alias("s"), "claim_id")
            .where("t.member_sk != s.member_sk OR t.provider_sk != s.provider_sk")
            .count()
        )
        if stale:
            (
                DeltaTable.forPath(spark, fc_uri)
                .alias("t")
                .merge(fixed.alias("s"), "t.claim_id = s.claim_id")
                .whenMatchedUpdate(
                    condition="t.member_sk != s.member_sk OR t.provider_sk != s.provider_sk",
                    set={"member_sk": "s.member_sk", "provider_sk": "s.provider_sk"},
                )
                .execute()
            )
            log.info(
                "gold rekey: %d fact_claim rows re-pointed after dimension history changed", stale
            )
        return stale
    finally:
        fixed.unpersist()


# ----------------------------------------------------------------------------------- facts


def _files(n_rows: int) -> int:
    return max(1, math.ceil(n_rows / ROWS_PER_FILE))


def _write_facts(spark: SparkSession, gold_root: str, name: str, df: DataFrame, b: str) -> int:
    uri = table_uri(gold_root, name)
    ensure_table(spark, uri, name)
    df = df.persist()
    try:
        n = df.count()
        write_replace(df.repartition(_files(n)), uri, f"load_batch_id = '{b}'", ["load_batch_id"])
    finally:
        df.unpersist()
    return n


def _write_ops(
    spark: SparkSession,
    gold_root: str,
    name: str,
    cols,
    df: DataFrame,
    predicate: str,
) -> None:
    uri = table_uri(gold_root, name)
    ensure_table(spark, uri, name, cols)
    write_replace(align(df, cols), uri, predicate, None)


# ----------------------------------------------------------------------------------- reconcile


def _sums(df: DataFrame, cols: tuple[str, ...]) -> tuple[int, dict[str, Decimal]]:
    r = df.agg(F.count("*"), *[F.sum(c) for c in cols]).first()
    return r[0], {c: _dec(r[i + 1]) for i, c in enumerate(cols)}


def _count_anti(left: DataFrame, right: DataFrame, lk: str, rk: str) -> int:
    return left.join(
        right.select(F.col(rk).alias("_rk")), F.col(lk) == F.col("_rk"), "left_anti"
    ).count()


def _reconcile_fact(
    spark: SparkSession,
    silver_root: str,
    gold_root: str,
    name: str,
    silver_name: str,
    key: str,
    d: date,
) -> tuple[TableResult, dict[str, dict[str, Decimal]]]:
    b = batch_id_for(d)
    cols = AMOUNTS[name]
    s = read(spark, silver_uri(silver_root, silver_name)).where(_pred(d))
    gold = read(spark, table_uri(gold_root, name))
    g = gold.where(F.col("load_batch_id") == b)
    ex = read(spark, table_uri(gold_root, EXCLUSIONS)).where(
        (F.col("batch_id") == b) & (F.col("dataset") == silver_name)
    )
    s_rows, s_sum = _sums(s, cols)
    g_rows, g_sum = _sums(g.select(*cols), cols)
    e_rows, e_sum = _sums(
        s.join(
            ex.select(F.col("business_key").alias("_bk")), F.col(key) == F.col("_bk"), "left_semi"
        ),
        cols,
    )
    ok = s_rows == g_rows + e_rows and all(s_sum[c] == g_sum[c] + e_sum[c] for c in cols)

    v: dict[str, int] = {}
    ids = g.select(key)
    v[f"duplicate_{key}_across_batches"] = gold.join(ids, key, "left_semi").count() - g_rows
    dates = read(spark, table_uri(gold_root, "dim_date")).select("date_key")
    v["service_date_key_not_in_dim_date"] = _count_anti(g, dates, "service_date_key", "date_key")
    if name == "fact_claim":
        v["member_sk_not_in_dim_member"] = _count_anti(
            g, read(spark, table_uri(gold_root, "dim_member")), "member_sk", "member_sk"
        )
        v["provider_sk_not_in_dim_provider"] = _count_anti(
            g, read(spark, table_uri(gold_root, "dim_provider")), "provider_sk", "provider_sk"
        )
        v["primary_diagnosis_sk_not_in_dim_diagnosis"] = _count_anti(
            g.where(F.col("primary_diagnosis_sk").isNotNull()),
            read(spark, table_uri(gold_root, "dim_diagnosis")),
            "primary_diagnosis_sk",
            "diagnosis_sk",
        )
        v["received_date_key_not_in_dim_date"] = _count_anti(
            g, dates, "received_date_key", "date_key"
        )
        lines = (
            read(spark, table_uri(gold_root, "fact_claim_line"))
            .where(F.col("load_batch_id") == b)
            .groupBy("claim_id")
            .agg(
                F.count("*").alias("_n"),
                F.sum("billed_amount").alias("_b"),
                F.sum("allowed_amount").alias("_a"),
                F.sum("paid_amount").alias("_p"),
            )
        )
        v["header_not_equal_to_sum_of_lines"] = (
            g.join(lines, "claim_id", "left")
            .where(
                F.col("_n").isNull()
                | (F.col("_n") != F.col("line_count"))
                | (F.col("_b") != F.col("total_billed_amount"))
                | (F.col("_a") != F.col("total_allowed_amount"))
                | (F.col("_p") != F.col("total_paid_amount"))
            )
            .count()
        )
        v["unresolved_dimension_claims"] = ex.where(F.col("reason_code").isin(*UNRESOLVED)).count()
    else:
        v["claim_id_not_in_fact_claim"] = _count_anti(
            g, read(spark, table_uri(gold_root, "fact_claim")), "claim_id", "claim_id"
        )
    violations = sum(v.values())
    res = TableResult(
        name,
        b,
        s_rows,
        g_rows,
        e_rows,
        violations,
        ok and violations == 0,
        {"violations": {k: n for k, n in v.items() if n}, "sums_ok": ok},
    )
    return res, {"silver": s_sum, "gold": g_sum, "excluded": e_sum}


def _reconcile_dim(
    spark: SparkSession, gold_root: str, spec: DimSpec, snap: DataFrame, d: date
) -> TableResult:
    """Every snapshot key must have, at the snapshot date, a version with the snapshot's content."""
    b = batch_id_for(d)
    typed = typed_snapshot(snap, spec)
    dim = read(spark, table_uri(gold_root, spec.table))
    at = F.lit(d.strftime("%Y-%m-%d 00:00:00")).cast("timestamp_ntz")
    valid = dim.where(
        (F.col("effective_from") <= at)
        & (F.col("effective_to").isNull() | (F.col("effective_to") > at))
    )
    n = typed.count()
    reflected = (
        typed.alias("a")
        .join(valid.alias("v"), spec.key)
        .where("a.record_hash = v.record_hash")
        .count()
    )
    viol = scd2_violations(dim, spec)
    bad = {k: x for k, x in viol.items() if x}
    return TableResult(
        spec.table,
        b,
        n,
        reflected,
        0,
        sum(viol.values()),
        n == reflected and not bad,
        {"violations": bad, "snapshot_keys_not_reflected": n - reflected},
    )


def _recon_frame(
    spark: SparkSession, results: list[TableResult], sums: dict, d: date, ts: datetime
) -> DataFrame:
    rows = []
    for r in results:
        s = sums.get(r.gold_table)
        amounts = []
        for col_ix in range(3):
            for part in ("silver", "gold", "excluded"):
                amounts.append(list(s[part].values())[col_ix] if s else None)
        # order in RECONCILIATION_COLS is billed(s,g,e), allowed(s,g,e), paid(s,g,e)
        rows.append(
            (
                r.gold_table,
                r.batch_id,
                d,
                r.silver_rows,
                r.gold_rows,
                r.excluded_rows,
                *amounts,
                r.key_violations,
                r.identity_ok,
                json.dumps(r.detail, default=str),
                ts,
            )
        )
    return spark.createDataFrame(rows, struct_of(RECONCILIATION_COLS))


# ----------------------------------------------------------------------------------- batch


def gold_batch(
    spark: SparkSession,
    silver_root: str,
    gold_root: str,
    batch_date: date,
    run_ts: datetime | None = None,
) -> list[TableResult]:
    """Build gold for one batch date. Safe to call repeatedly (same input -> same gold).

    Raises ``ReconciliationError`` after recording the numbers in ``gold_reconciliation`` if
    silver does not reconcile to gold (rows, sums, keys, SCD2 integrity) for any table.
    """
    ts = _ts(run_ts)
    b = batch_id_for(batch_date)
    members = _silver(spark, silver_root, "members", batch_date)
    providers = _silver(spark, silver_root, "providers", batch_date)
    claims = _silver(spark, silver_root, "claims", batch_date)
    lines = _silver(spark, silver_root, "claim_lines", batch_date)
    dx = _silver(spark, silver_root, "diagnoses", batch_date)
    scd: dict[str, ScdResult | None] = {}
    for dim in ("dim_date", "dim_member", "dim_provider", "dim_diagnosis"):
        ensure_table(spark, table_uri(gold_root, dim), dim)  # facts always have all dimensions
    # 1. dimensions
    scd["dim_member"] = _merge_dimension(
        spark, silver_root, gold_root, MEMBER, "members", batch_date, ts
    )
    scd["dim_provider"] = _merge_dimension(
        spark, silver_root, gold_root, PROVIDER, "providers", batch_date, ts
    )
    if dx is not None:
        merge_diagnosis(
            spark, table_uri(gold_root, "dim_diagnosis"), diagnosis_rows(spark, dx, b, ts)
        )
    window = _date_window((claims, ("service_date", "received_date")), (lines, ("service_date",)))
    if window:
        ensure_dim_date(spark, table_uri(gold_root, "dim_date"), *window)

    # 2. facts already loaded may now point at the wrong version
    rekey_facts(
        spark,
        silver_root,
        gold_root,
        scd["dim_member"].changed_keys if scd["dim_member"] else None,
        scd["dim_provider"].changed_keys if scd["dim_provider"] else None,
    )

    # 3. facts of this batch
    results: list[TableResult] = []
    sums: dict[str, dict] = {}
    if claims is not None:
        frames = build_facts(
            claims,
            lines if lines is not None else _empty(spark, LINES_DDL),
            dx if dx is not None else _empty(spark, DX_DDL),
            read(spark, table_uri(gold_root, "dim_member")),
            read(spark, table_uri(gold_root, "dim_provider")),
            b,
            ts,
        )
        try:
            _write_facts(spark, gold_root, "fact_claim", frames.fact_claim, b)
            _write_facts(spark, gold_root, "fact_claim_line", frames.fact_claim_line, b)
            _write_ops(
                spark,
                gold_root,
                EXCLUSIONS,
                EXCLUSION_COLS,
                frames.exclusions.withColumn("batch_id", F.lit(b)).withColumn(
                    "_run_ts", F.lit(ts).cast("timestamp")
                ),
                f"batch_id = '{b}'",
            )
        finally:
            frames.release()
        for name, silver_name, key in (
            ("fact_claim", "claims", "claim_id"),
            ("fact_claim_line", "claim_lines", "line_id"),
        ):
            if name == "fact_claim_line" and lines is None:
                continue
            r, s = _reconcile_fact(
                spark, silver_root, gold_root, name, silver_name, key, batch_date
            )
            results.append(r)
            sums[name] = s
    for spec, snap in ((MEMBER, members), (PROVIDER, providers)):
        if snap is not None:
            results.append(_reconcile_dim(spark, gold_root, spec, snap, batch_date))

    for r in results:
        log.info(
            "gold %s %s: silver=%d gold=%d excluded=%d violations=%d ok=%s",
            r.gold_table,
            b,
            r.silver_rows,
            r.gold_rows,
            r.excluded_rows,
            r.key_violations,
            r.identity_ok,
        )
    if results:
        _write_ops(
            spark,
            gold_root,
            RECONCILIATION,
            RECONCILIATION_COLS,
            _recon_frame(spark, results, sums, batch_date, ts),
            f"batch_id = '{b}'",
        )
    broken = [r for r in results if not r.identity_ok]
    if broken:
        raise ReconciliationError(
            "silver does not reconcile to gold: "
            + "; ".join(
                f"{r.gold_table} {r.batch_id}: silver={r.silver_rows} gold={r.gold_rows} "
                f"excluded={r.excluded_rows} violations={r.key_violations} "
                f"{json.dumps(r.detail, default=str)}"
                for r in broken
            )
        )
    return results


def _empty(spark: SparkSession, ddl: str) -> DataFrame:
    return spark.createDataFrame([], ddl)


# ----------------------------------------------------------------------------------- export / ops


def export_parquet(spark: SparkSession, gold_root: str) -> dict[str, int]:
    """Write every star-schema table as Parquet under ``<gold>/export/<table>/``.

    This is the load path for Snowflake ``COPY INTO`` and the source for the dbt DuckDB target.
    Each table directory is replaced as a whole; call it only after the batches reconciled.
    """
    out: dict[str, int] = {}
    for name in TABLES:
        uri = table_uri(gold_root, name)
        if not exists(spark, uri):
            continue
        df = read(spark, uri)
        n = df.count()
        df.repartition(_files(n)).write.mode("overwrite").parquet(export_uri(gold_root, name))
        out[name] = n
    return out


def _tables(gold_root: str, spark: SparkSession) -> list[str]:
    return [
        n for n in (*TABLES, RECONCILIATION, EXCLUSIONS) if exists(spark, table_uri(gold_root, n))
    ]


def optimize_tables(
    spark: SparkSession, gold_root: str, tables: list[str] | None = None
) -> list[str]:
    """Compact small files in the gold Delta tables (OPTIMIZE); default: all. Not run by default."""
    done = []
    for n in tables or _tables(gold_root, spark):
        DeltaTable.forPath(spark, table_uri(gold_root, n)).optimize().executeCompaction()
        done.append(n)
    return done


def vacuum_tables(
    spark: SparkSession,
    gold_root: str,
    retention_hours: int = MIN_VACUUM_HOURS,
    tables: list[str] | None = None,
) -> list[str]:
    """Delete files no longer referenced by a table version older than ``retention_hours``.

    Refuses anything below Delta's 168 h default: a shorter window can remove files a running
    reader or a time-travel query still needs. Not run by default.
    """
    if retention_hours < MIN_VACUUM_HOURS:
        raise ValueError(f"retention_hours must be >= {MIN_VACUUM_HOURS}, got {retention_hours}")
    done = []
    for n in tables or _tables(gold_root, spark):
        DeltaTable.forPath(spark, table_uri(gold_root, n)).vacuum(retention_hours)
        done.append(n)
    return done
