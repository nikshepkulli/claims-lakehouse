"""Checks on the REAL end-to-end output: claimsgen -> bronze -> silver -> gold (-m spark).

These run in the ``pipeline e2e`` CI job, after the three layers have been built on the
2000-claim fixture (3 merged batches, scripts/bucket_raw_batches.py), and are skipped everywhere
else (the build takes minutes, so the Spark test legs do not repeat it). Locally:

    make pipeline-e2e E2E=data/e2e                       # builds the layers, runs dbt
    GOLD_E2E_DIR=data/e2e pytest -m spark tests/gold/test_gold_pipeline_spark.py

They check what must hold on the generator's defect-laden data: gold equals the Snowflake DDL,
silver reconciles to gold, nothing quarantined or orphaned reaches gold, every key resolves,
re-running every batch changes nothing, and the Parquet export equals the Delta tables.
"""

import json
import os
from decimal import Decimal
from pathlib import Path

import pytest
from ddl import ddl_contract, varchar_lengths

pytest.importorskip("pyspark")
pytest.importorskip("delta")

from pyspark.sql import functions as F  # noqa: E402

from lakehouse.gold.job import gold_batch, silver_batch_dates  # noqa: E402
from lakehouse.gold.schema import EPOCH, TABLES  # noqa: E402

pytestmark = pytest.mark.spark

E2E = os.environ.get("GOLD_E2E_DIR")


@pytest.fixture(scope="module")
def pipeline(spark):
    if not E2E:
        pytest.skip("GOLD_E2E_DIR not set: run via the `pipeline e2e` CI job or make pipeline-e2e")
    root = Path(E2E).resolve()

    class P:
        pass

    p = P()
    p.spark, p.silver, p.gold = spark, str(root / "silver"), str(root / "gold")
    p.dates = silver_batch_dates(spark, p.silver)
    p.manifest = json.loads((root / "raw" / "defects_manifest.json").read_text())
    assert len(p.dates) >= 2, "expected a dimension batch and at least one claims batch"
    return p


def delta(p, root, name):
    return p.spark.read.format("delta").load(f"{root}/{name}")


def g(p, name):
    return delta(p, p.gold, name)


def s(p, name):
    return delta(p, p.silver, name)


# ---------------------------------------- contract on real tables


@pytest.mark.parametrize("table", sorted(TABLES))
def test_written_delta_tables_match_the_snowflake_ddl(pipeline, table):
    """Names, order, Spark types and nullability of what the job really wrote == the DDL."""
    got = [(f.name, f.dataType.simpleString(), f.nullable) for f in g(pipeline, table).schema]
    assert got == ddl_contract()[table]


@pytest.mark.parametrize("table", sorted(TABLES))
def test_string_values_fit_the_ddl_varchar_lengths(pipeline, table):
    df = g(pipeline, table)
    for col, limit in varchar_lengths(table).items():
        longest = df.agg(F.max(F.length(col))).first()[0] or 0
        assert longest <= limit, f"{table}.{col} has a value of {longest} chars > VARCHAR({limit})"


# ---------------------------------------- reconciliation


def test_every_batch_reconciles_and_is_recorded(pipeline):
    p = pipeline
    rec = g(p, "gold_reconciliation")
    assert rec.count() > 0 and rec.where("NOT identity_ok").count() == 0
    assert rec.where("key_violations != 0").count() == 0
    assert {"fact_claim", "fact_claim_line", "dim_member", "dim_provider"} <= {
        r[0] for r in rec.select("gold_table").distinct().collect()
    }


def test_fact_totals_reconcile_to_silver_independently(pipeline):
    """Recomputed here from the tables, not read back from gold_reconciliation."""
    p = pipeline
    amount_cols = {
        "fact_claim": ("total_billed_amount", "total_allowed_amount", "total_paid_amount"),
        "fact_claim_line": ("billed_amount", "allowed_amount", "paid_amount"),
    }
    for name, silver_name, key in (
        ("fact_claim", "claims", "claim_id"),
        ("fact_claim_line", "claim_lines", "line_id"),
    ):
        cols = amount_cols[name]
        silver = s(p, silver_name)
        gold = g(p, name)
        held = g(p, "gold_exclusions").where(F.col("dataset") == silver_name)
        held_rows = silver.join(held, F.col(key) == F.col("business_key"), "left_semi")
        assert silver.count() == gold.count() + held.count() > 0
        for c in cols:
            tot = silver.agg(F.sum(c)).first()[0]
            assert tot == (gold.agg(F.sum(c)).first()[0] or 0) + (
                held_rows.agg(F.sum(c)).first()[0] or Decimal(0)
            ), f"{name}.{c}"


def test_only_expected_reasons_hold_claims_back(pipeline):
    """No dimension gap, no total mismatch: only claims that silver itself left incomplete."""
    reasons = {
        r[0] for r in g(pipeline, "gold_exclusions").select("reason_code").distinct().collect()
    }
    assert reasons <= {"INCOMPLETE_LINES", "PARENT_CLAIM_EXCLUDED"}, reasons


# ---------------------------------------- nothing bad reaches gold


def quarantined(p, dataset):
    q = s(p, "silver_quarantine").where(F.col("dataset") == dataset)
    return {r[0] for r in q.select("business_key").where("business_key IS NOT NULL").collect()}


def test_quarantined_and_orphan_rows_never_reach_gold(pipeline):
    p = pipeline
    claim_ids = {r[0] for r in g(p, "fact_claim").select("claim_id").collect()}
    line_ids = {r[0] for r in g(p, "fact_claim_line").select("line_id").collect()}
    q_claims, q_lines = quarantined(p, "claims"), quarantined(p, "claim_lines")
    assert q_claims and q_lines, "fixture too small: no quarantined rows"
    assert not claim_ids & q_claims and not line_ids & q_lines
    orphans = set(p.manifest["defects"]["orphan_member_id"]["sample_ids"])
    assert orphans and not orphans & claim_ids
    # a claim that lost a line to quarantine is held back whole, with its remaining lines
    bad_line_claims = {i.rsplit("-", 1)[0] for i in q_lines}
    assert not bad_line_claims & claim_ids
    held = {
        r[0]
        for r in g(p, "gold_exclusions")
        .where("dataset = 'claims' AND reason_code = 'INCOMPLETE_LINES'")
        .select("business_key")
        .collect()
    }
    assert bad_line_claims - q_claims <= held


def test_duplicates_were_resolved_before_gold(pipeline):
    p = pipeline
    for name, key in (("fact_claim", "claim_id"), ("fact_claim_line", "line_id")):
        gold = g(p, name)
        assert gold.count() == gold.select(key).distinct().count()
    for name, key in (
        ("dim_member", "member_sk"),
        ("dim_provider", "provider_sk"),
        ("dim_diagnosis", "diagnosis_sk"),
        ("dim_date", "date_key"),
    ):  # noqa: E501
        gold = g(p, name)
        assert gold.count() == gold.select(key).distinct().count(), name


# ---------------------------------------- star-schema integrity


def anti(left, right, lk, rk):
    return left.join(
        right.select(F.col(rk).alias("_k")), F.col(lk) == F.col("_k"), "left_anti"
    ).count()


def test_every_foreign_key_resolves(pipeline):
    p = pipeline
    fc, fl = g(p, "fact_claim"), g(p, "fact_claim_line")
    assert anti(fc, g(p, "dim_member"), "member_sk", "member_sk") == 0
    assert anti(fc, g(p, "dim_provider"), "provider_sk", "provider_sk") == 0
    dx = fc.where("primary_diagnosis_sk IS NOT NULL")
    assert (
        dx.count() > 0
        and anti(dx, g(p, "dim_diagnosis"), "primary_diagnosis_sk", "diagnosis_sk") == 0
    )
    for col in ("service_date_key", "received_date_key"):
        assert anti(fc, g(p, "dim_date"), col, "date_key") == 0
    assert anti(fl, g(p, "dim_date"), "service_date_key", "date_key") == 0
    assert anti(fl, fc, "claim_id", "claim_id") == 0


def test_header_equals_sum_of_lines_and_line_count(pipeline):
    p = pipeline
    lines = (
        g(p, "fact_claim_line")
        .groupBy("claim_id")
        .agg(
            F.count("*").alias("n"),
            F.sum("billed_amount").alias("b"),
            F.sum("allowed_amount").alias("a"),
            F.sum("paid_amount").alias("pd"),
        )
    )
    bad = (
        g(p, "fact_claim")
        .join(lines, "claim_id", "left")
        .where(
            "n IS NULL OR n != line_count OR b != total_billed_amount "
            "OR a != total_allowed_amount OR pd != total_paid_amount"
        )
    )
    assert bad.count() == 0


def test_dim_date_covers_the_data_window_with_no_gaps(pipeline):
    p = pipeline
    dd = g(p, "dim_date")
    lo, hi = dd.agg(F.min("full_date"), F.max("full_date")).first()
    assert (lo.month, lo.day, hi.month, hi.day) == (1, 1, 12, 31)
    assert dd.count() == (hi - lo).days + 1
    fc = s(p, "claims")
    assert fc.agg(F.min("service_date")).first()[0] >= lo
    assert fc.agg(F.max("received_date")).first()[0] <= hi


def test_generator_snapshot_gives_one_open_version_per_entity(pipeline):
    p = pipeline
    for dim, silver_name, key in (
        ("dim_member", "members", "member_id"),
        ("dim_provider", "providers", "provider_id"),
    ):  # noqa: E501
        d = g(p, dim)
        assert d.count() == s(p, silver_name).select(key).distinct().count()
        assert d.where("is_current AND effective_to IS NULL").count() == d.count()
        assert d.where(F.col("effective_from") != F.lit(EPOCH).cast("timestamp_ntz")).count() == 0


def test_late_arriving_claims_are_kept_and_flagged_upstream(pipeline):
    p = pipeline
    late = s(p, "claims").where("is_late_arrival")
    assert late.count() > 0
    kept = late.join(g(p, "fact_claim"), "claim_id", "left_semi").count()
    held = late.join(
        g(p, "gold_exclusions").where("dataset = 'claims'"),
        F.col("claim_id") == F.col("business_key"),
        "left_semi",
    ).count()
    assert kept + held == late.count() and kept > 0


# ---------------------------------------- CLI: export, idempotency, hooks


def content(p, name):
    """Order-independent fingerprint of a table without run-time columns."""
    df = g(p, name)
    cols = [c for c in df.columns if c not in ("loaded_at", "_run_ts")]
    return sorted(r[0] for r in df.select(F.sha2(F.to_json(F.struct(*cols)), 256)).collect())


def test_rerunning_every_batch_is_idempotent(pipeline):
    p = pipeline
    names = [*TABLES, "gold_exclusions"]
    before = {n: content(p, n) for n in names}
    for d in p.dates:
        assert all(r.identity_ok for r in gold_batch(p.spark, p.silver, p.gold, d))
    assert {n: content(p, n) for n in names} == before  # same rows, nothing duplicated


@pytest.mark.parametrize("table", sorted(TABLES))
def test_parquet_export_equals_the_delta_table_and_the_ddl(pipeline, table):
    """The export is what dbt/DuckDB and Snowflake COPY read: same rows, same contract types."""
    p = pipeline
    out = p.spark.read.parquet(f"{p.gold}/export/{table}")
    assert [(f.name, f.dataType.simpleString()) for f in out.schema] == [
        (c, t) for c, t, _ in ddl_contract()[table]
    ]
    delta_rows = g(p, table)
    assert out.count() == delta_rows.count() > 0
    # loaded_at is a run timestamp: the idempotency test re-runs batches after the export
    cols = [c for c in out.columns if c != "loaded_at"]
    assert out.select(*cols).exceptAll(delta_rows.select(*cols)).count() == 0
