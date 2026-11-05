"""claimsgen -> bronze -> silver -> gold on a 2000-claim fixture, with the real code (-m spark).

Checks what must hold on the generator's defect-laden data: gold equals the Snowflake DDL,
silver reconciles to gold, nothing quarantined or orphaned reaches gold, every key resolves, and
the CLI run is idempotent and exports Parquet. The fixture's ~370 daily partitions are merged
into 3 bronze batches (scripts/bucket_raw_batches.py) to keep the suite fast.
"""

import importlib.util
import json
import os
import subprocess
import sys
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import pytest
from ddl import ddl_contract, varchar_lengths

pytest.importorskip("pyspark")
pytest.importorskip("delta")

from pyspark.sql import functions as F  # noqa: E402

from lakehouse.bronze.ingest import ingest_batch  # noqa: E402
from lakehouse.gold.job import gold_batch, silver_batch_dates  # noqa: E402
from lakehouse.gold.schema import EPOCH, TABLES  # noqa: E402
from lakehouse.silver.job import silver_batch  # noqa: E402

pytestmark = pytest.mark.spark

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
RUN_TS = datetime(2025, 1, 1, 12, 0, 0)
DIMS_DAY = date(2024, 1, 1)
DATASETS = ["claims", "claim_lines", "diagnoses", "members", "providers", "notes"]
FACT_TABLES = ("fact_claim", "fact_claim_line")


def _bucket_module():
    spec = importlib.util.spec_from_file_location(
        "bucket_raw_batches", ROOT / "scripts" / "bucket_raw_batches.py"
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def pipeline(spark, tmp_path_factory):
    work = tmp_path_factory.mktemp("lake")
    raw = work / "raw"
    subprocess.run(
        [sys.executable, "-m", "claimsgen", "--rows", "2000", "--seed", "42", "--out", str(raw),
         "--overwrite", "--quiet"],
        check=True,
        env={**os.environ, "PYTHONPATH": os.pathsep.join([str(SRC), os.environ.get("PYTHONPATH", "")])},  # noqa: E501
    )  # fmt: skip
    batched = work / "batched"
    starts = _bucket_module().bucket(raw, batched, 3)
    bronze, silver, gold = (str(work / n) for n in ("bronze", "silver", "gold"))
    ingest_batch(spark, batched, bronze, DIMS_DAY, ["members", "providers"], RUN_TS)
    for d in starts:
        ingest_batch(
            spark, batched, bronze, d, ["claims", "claim_lines", "diagnoses", "notes"], RUN_TS
        )
    for d in sorted({DIMS_DAY, *starts}):
        silver_batch(spark, bronze, silver, d, DATASETS, run_ts=RUN_TS)
    dates = silver_batch_dates(spark, silver)
    results = {d: gold_batch(spark, silver, gold, d, run_ts=RUN_TS) for d in dates}

    class P:
        pass

    p = P()
    p.spark, p.raw, p.silver, p.gold, p.dates, p.results = spark, raw, silver, gold, dates, results
    p.manifest = json.loads((raw / "defects_manifest.json").read_text())
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
    assert all(r.identity_ok for rs in p.results.values() for r in rs)
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


def run_cli(*args):
    return subprocess.run(
        [sys.executable, "-m", "lakehouse.gold", *args],
        capture_output=True,
        text=True,
        env={
            **os.environ,
            "PYTHONPATH": os.pathsep.join([str(SRC), os.environ.get("PYTHONPATH", "")]),
        },  # noqa: E501
    )


def test_cli_exports_parquet_with_the_ddl_schema_and_runs_maintenance(pipeline):
    p = pipeline
    counts = {n: g(p, n).count() for n in TABLES}
    r = run_cli("--silver", p.silver, "--gold", p.gold, "--batch-date", str(p.dates[-1]),
                "--optimize", "--vacuum")  # fmt: skip
    assert r.returncode == 0, r.stderr[-3000:]
    assert "exported:" in r.stdout and "optimized:" in r.stdout and "vacuumed:" in r.stdout
    for n, rows in counts.items():
        out = p.spark.read.parquet(f"{p.gold}/export/{n}")
        assert out.count() == rows, n
        # the export carries the contract types too (what DuckDB / Snowflake COPY will read)
        assert [(f.name, f.dataType.simpleString()) for f in out.schema] == [
            (c, t) for c, t, _ in ddl_contract()[n]
        ]


def test_cli_fails_loudly_and_skips_export_when_silver_is_missing_a_dimension(pipeline, tmp_path):
    """Claims whose members were never loaded: non-zero exit, nothing exported."""
    p = pipeline
    day = p.dates[1]  # first claims batch
    silver = tmp_path / "silver"
    for name in ("claims", "claim_lines", "diagnoses", "providers"):
        df = s(p, name).where(F.col("_ingest_date") == F.lit(day))
        df.write.format("delta").partitionBy("_ingest_date").save(str(silver / name))
    r = run_cli("--silver", str(silver), "--gold", str(tmp_path / "gold"), "--batch-date", str(day))
    assert r.returncode == 1 and "RECONCILIATION FAILED" in r.stderr
    assert "unresolved_dimension_claims" in r.stderr
    assert not (tmp_path / "gold" / "export").exists()
