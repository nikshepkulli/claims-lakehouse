"""Silver end-to-end tests: real bronze ingest -> silver over a claimsgen fixture.

Need PySpark + Java: `make test-spark` (Docker) or the CI `spark tests (silver-e2e)` job.
Checks EVERY defect in defects_manifest.json against silver / silver_quarantine, the
reconciliation identity, idempotent re-runs and file layout. Rules claimsgen does not produce
are in test_silver_rules.py.

The fixture (1000 claims, seed 42) compresses claimsgen's ~370 daily partitions into 2 bronze
batches (files are moved into 2 `ingest_date=` folders and ingested by the real bronze code) so
the suite stays fast; silver logic does not depend on how many batches there are.
"""

import json
import os
import shutil
import subprocess
import sys
from datetime import date, datetime
from pathlib import Path

import pytest

pytest.importorskip("pyspark")
pytest.importorskip("delta")

from pyspark.sql import functions as F  # noqa: E402

from lakehouse.bronze.datasets import EVENTS_DDL  # noqa: E402
from lakehouse.bronze.ingest import ingest_batch  # noqa: E402
from lakehouse.silver import job  # noqa: E402
from lakehouse.silver.job import optimize_tables, silver_batch  # noqa: E402

pytestmark = pytest.mark.spark

SRC = Path(__file__).resolve().parents[2] / "src"
RUN_TS = datetime(2025, 1, 1, 12, 0, 0)
N_BUCKETS = 2
DIMS_DAY = date(2024, 1, 1)

# --------------------------------------------------------------------------- fixture pipeline


@pytest.fixture(scope="module")
def raw(tmp_path_factory):
    out = tmp_path_factory.mktemp("raw")
    subprocess.run(
        [sys.executable, "-m", "claimsgen", "--rows", "1000", "--seed", "42", "--out", str(out),
         "--overwrite", "--quiet"],
        check=True,
        env={**os.environ, "PYTHONPATH": os.pathsep.join([str(SRC), os.environ.get("PYTHONPATH", "")])},  # noqa: E501
    )  # fmt: skip
    return out


def _partition_dates(raw: Path) -> list[str]:
    ds: set[str] = set()
    for name in ("claims", "claim_lines", "diagnoses", "notes"):
        ds.update(p.name.split("=")[1] for p in (raw / name).iterdir())
    return sorted(ds)


@pytest.fixture(scope="module")
def bucketed(raw, tmp_path_factory):
    """Copy of the raw tree whose daily partitions are merged into N_BUCKETS batch folders."""
    dates = _partition_dates(raw)
    size = -(-len(dates) // N_BUCKETS)
    chunks = [dates[i : i + size] for i in range(0, len(dates), size)]
    starts = [date.fromisoformat(c[0]) for c in chunks]
    out = tmp_path_factory.mktemp("bucketed")
    for name in ("claims", "claim_lines", "diagnoses", "notes"):
        for chunk in chunks:
            dst = out / name / f"ingest_date={chunk[0]}"
            dst.mkdir(parents=True)
            for d in chunk:
                src = raw / name / f"ingest_date={d}"
                if src.is_dir():
                    for f in src.iterdir():
                        shutil.copy(f, dst / f"part-{d}-{f.name}")
    for name in ("members", "providers"):
        shutil.copytree(raw / name, out / name)
    return out, starts


@pytest.fixture(scope="module")
def pipeline(spark, raw, bucketed, tmp_path_factory):
    """bronze (real ingest code) -> silver for every batch. Returns a namespace of facts."""
    root = tmp_path_factory.mktemp("lake")
    bronze, silver = str(root / "bronze"), str(root / "silver")
    inp, starts = bucketed
    ingest_batch(spark, inp, bronze, DIMS_DAY, ["members", "providers"], RUN_TS)
    for d in starts:
        ingest_batch(spark, inp, bronze, d, ["claims", "claim_lines", "diagnoses", "notes"], RUN_TS)
    # events: one flat file in claimsgen; land it bronze-shaped, routed into the same batches
    ev = spark.read.text(str(raw / "events" / "claim_events.jsonl")).where(F.trim("value") != "")
    parsed = ev.select(F.from_json("value", EVENTS_DDL).alias("j"), F.col("value").alias("_raw"))
    day = F.to_date(F.substring("event_ts", 1, 10))
    bucket = F.lit(starts[0])
    for s in starts[1:]:
        bucket = F.when(day >= F.lit(s), F.lit(s)).otherwise(bucket)
    (
        parsed.select("j.*", "_raw")
        .withColumn("_ingest_ts", F.lit(RUN_TS).cast("timestamp"))
        .withColumn("_source_file", F.lit("events/claim_events.jsonl"))
        .withColumn("_ingest_date", bucket.cast("date"))
        .withColumn("_batch_id", F.concat(F.lit("b"), F.date_format("_ingest_date", "yyyyMMdd")))
        .write.format("delta")
        .partitionBy("_ingest_date")
        .save(f"{bronze}/events")
    )
    batches = sorted({DIMS_DAY, *starts})
    results = {d: silver_batch(spark, bronze, silver, d, run_ts=RUN_TS) for d in batches}

    class P:
        pass

    p = P()
    p.spark, p.raw, p.bronze, p.silver, p.batches, p.results = (
        spark,
        raw,
        bronze,
        silver,
        batches,
        results,
    )
    p.manifest = json.loads((raw / "defects_manifest.json").read_text())
    return p


def tbl(spark, root, name):
    return spark.read.format("delta").load(f"{root}/{name}")


def qkeys(p, dataset, code):
    q = tbl(p.spark, p.silver, "silver_quarantine")
    rows = (
        q.where((F.col("dataset") == dataset) & F.array_contains("all_reason_codes", code))
        .select("business_key")
        .distinct()
        .collect()
    )
    return {r[0] for r in rows}


def recon_sum(p, dataset, col):
    r = tbl(p.spark, p.silver, "silver_reconciliation").where(F.col("dataset") == dataset)
    return r.agg(F.sum(col)).first()[0] or 0


# ------------------------------------------------------------------- manifest-driven defect checks


def _chk_duplicate_claim(p, d):
    for ds, key in (("claims", "claim_id"), ("claim_lines", "line_id")):
        b = tbl(p.spark, p.bronze, ds)
        extra = b.count() - b.select(key).distinct().count()
        assert recon_sum(p, ds, "deduped_rows") == extra > 0
        s = tbl(p.spark, p.silver, ds)
        assert s.count() == s.select(key).distinct().count()  # no duplicate left in silver
    b = tbl(p.spark, p.bronze, "claims")
    dup_ids = b.groupBy("claim_id").count().where("count > 1").count()
    assert dup_ids == d["count"]
    assert recon_sum(p, "claims", "deduped_rows") == dup_ids  # one extra copy per duplicated claim
    # byte-identical re-sends are dropped silently-but-counted, never treated as conflicts
    assert not qkeys(p, "claims", "CONFLICTING_DUPLICATE")


def _chk_late_arrival(p, d):
    s = tbl(p.spark, p.silver, "claims")
    flagged = {r[0] for r in s.where("is_late_arrival").select("claim_id").collect()}
    q = tbl(p.spark, p.silver, "silver_quarantine").where("dataset = 'claims'")
    lag = F.datediff(
        F.to_date(F.get_json_object("payload", "$.received_date")),
        F.to_date(F.get_json_object("payload", "$.service_date")),
    )
    late_quarantined = {r[0] for r in q.where(lag > 30).select("business_key").collect()}
    # none discarded: every late claim is flagged in silver or quarantined for another reason
    assert len(flagged | late_quarantined) == d["count"]
    assert set(d["sample_ids"]) <= flagged | late_quarantined
    assert s.where("is_late_arrival").where("arrival_lag_days <= 30").count() == 0
    assert s.where("NOT is_late_arrival").where("arrival_lag_days > 30").count() == 0


def _chk_orphan_member(p, d):
    keys = qkeys(p, "claims", "ORPHAN_MEMBER_ID")
    assert len(keys) == d["count"] and set(d["sample_ids"]) <= keys
    reasons = tbl(p.spark, p.silver, "silver_quarantine").where("reason_code = 'ORPHAN_MEMBER_ID'")
    assert reasons.where("dataset = 'claims'").count() == d["count"]  # quarantined, not dropped
    # children of an orphan claim follow their parent into quarantine
    assert qkeys(p, "claim_lines", "PARENT_CLAIM_QUARANTINED")
    assert qkeys(p, "diagnoses", "PARENT_CLAIM_QUARANTINED")


def _line_chk(code):
    def f(p, d):
        keys = qkeys(p, "claim_lines", code)
        assert len(keys) == d["count"] and set(d["sample_ids"]) <= keys
        silver = tbl(p.spark, p.silver, "claim_lines")
        assert silver.where(F.col("line_id").isin(*keys)).count() == 0

    return f


def _dx_chk(code):
    def f(p, d):
        keys = qkeys(p, "diagnoses", code)
        assert len(keys) == d["count"] and set(d["sample_ids"]) <= keys
        silver = tbl(p.spark, p.silver, "diagnoses")
        assert (
            silver.withColumn("k", F.concat_ws("-", "claim_id", "dx_seq"))
            .where(F.col("k").isin(*keys))
            .count()
            == 0
        )

    return f


def _chk_negative(p, d):
    _line_chk("NEGATIVE_AMOUNT")(p, d)
    s = tbl(p.spark, p.silver, "claim_lines")
    assert s.where("billed_amount < 0 OR allowed_amount < 0 OR paid_amount < 0").count() == 0


def _chk_duplicate_event(p, d):
    b = tbl(p.spark, p.bronze, "events")
    ids = b.groupBy("event_id").count().where("count > 1")
    assert ids.count() == d["count"]
    assert (
        recon_sum(p, "events", "deduped_rows")
        == b.count() - b.select("event_id").distinct().count()
    )
    s = tbl(p.spark, p.silver, "events")
    assert (
        s.count()
        == s.select("event_id").distinct().count()
        == b.select("event_id").distinct().count()
    )


def _chk_out_of_order(p, d):
    # batch silver keeps every out-of-order event (ordering is the streaming job's concern, #12)
    s = tbl(p.spark, p.silver, "events")
    got = {r[0] for r in s.select("event_id").collect()}
    assert set(d["sample_ids"]) <= got
    assert not tbl(p.spark, p.silver, "silver_quarantine").where("dataset = 'events'").count()


def _chk_schema_drift(p, d):
    c = tbl(p.spark, p.silver, "claims")
    assert dict(c.dtypes)[d["added_column"]] == "boolean"
    assert c.where(f"{d['added_column']} IS NOT NULL").count() > 0
    assert c.where(f"{d['added_column']} IS NULL").count() > 0  # carried forward as nullable
    q = tbl(p.spark, p.silver, "silver_quarantine")
    assert q.where(F.col("reason_detail").contains(d["added_column"])).count() == 0


CHECKS = {
    "duplicate_claim": _chk_duplicate_claim,
    "late_arrival": _chk_late_arrival,
    "orphan_member_id": _chk_orphan_member,
    "null_procedure_code": _line_chk("NULL_PROCEDURE_CODE"),
    "invalid_procedure_code": _line_chk("INVALID_PROCEDURE_CODE"),
    "negative_amount": _chk_negative,
    "null_icd10": _dx_chk("NULL_ICD10"),
    "invalid_icd10": _dx_chk("INVALID_ICD10"),
    "duplicate_event": _chk_duplicate_event,
    "out_of_order_event": _chk_out_of_order,
    "schema_drift": _chk_schema_drift,
}


def test_every_manifest_defect_has_a_check(raw):
    manifest = json.loads((raw / "defects_manifest.json").read_text())
    injected = set(manifest["defects"]) | {"schema_drift"}
    assert injected == set(CHECKS), "generator defect catalogue changed: add/adjust a silver check"


@pytest.mark.parametrize("defect", sorted(CHECKS))
def test_manifest_defect_is_caught(pipeline, defect):
    p = pipeline
    entry = p.manifest["defects"].get(defect) or p.manifest[defect]
    if defect != "schema_drift":
        assert entry["count"] > 0, "fixture too small: defect was never injected"
    entry = {**entry, "added_column": entry.get("added_column", "prior_auth_flag")}
    CHECKS[defect](p, entry)


# ------------------------------------------------------------ reconciliation / idempotency / shape


def test_reconciliation_identity_holds_and_matches_bronze(pipeline):
    p = pipeline
    rec = tbl(p.spark, p.silver, "silver_reconciliation")
    rows = rec.collect()
    assert rows and all(r["identity_ok"] for r in rows)
    for r in rows:
        assert r["bronze_rows"] == r["silver_rows"] + r["quarantined_rows"] + r["deduped_rows"]
    for ds in ("claims", "claim_lines", "diagnoses", "members", "providers", "notes", "events"):
        b = tbl(p.spark, p.bronze, ds).count()
        sums = rec.where(F.col("dataset") == ds).agg(
            F.sum("bronze_rows"), F.sum("silver_rows"), F.sum("quarantined_rows"),
            F.sum("deduped_rows"),
        ).first()  # fmt: skip
        assert sums[0] == b, ds
        assert sums[1] == tbl(p.spark, p.silver, ds).count(), ds
        q = tbl(p.spark, p.silver, "silver_quarantine").where(F.col("dataset") == ds).count()
        assert sums[2] == q, ds
        assert sums[1] + sums[2] + sums[3] == b, ds


def test_quarantine_schema_and_payload(pipeline):
    p = pipeline
    q = tbl(p.spark, p.silver, "silver_quarantine")
    assert q.columns == list(job.QUARANTINE_COLUMNS)
    assert (
        q.where("reason_code IS NULL OR reason_detail IS NULL OR source_batch IS NULL").count() == 0
    )
    assert q.where("size(all_reason_codes) = 0 OR all_reason_codes[0] != reason_code").count() == 0
    one = q.where("dataset = 'claim_lines' AND reason_code = 'INVALID_PROCEDURE_CODE'").first()
    payload = json.loads(one["payload"])
    assert payload["line_id"] == one["business_key"] and "procedure_code" in payload
    assert one["source_batch"].startswith("b") and one["_quarantined_at"] == RUN_TS


def test_silver_is_referentially_clean_and_typed(pipeline):
    p = pipeline
    claims, lines = tbl(p.spark, p.silver, "claims"), tbl(p.spark, p.silver, "claim_lines")
    dx, members = tbl(p.spark, p.silver, "diagnoses"), tbl(p.spark, p.silver, "members")
    assert claims.join(members, "member_id", "left_anti").count() == 0
    assert lines.join(claims, "claim_id", "left_anti").count() == 0
    assert dx.join(claims, "claim_id", "left_anti").count() == 0
    types = dict(claims.dtypes)
    assert types["total_billed_amount"] == "decimal(12,2)"
    assert types["service_date"] == "date" and types["line_count"] == "int"
    assert dict(lines.dtypes)["billed_amount"] == "decimal(12,2)"
    assert dict(tbl(p.spark, p.silver, "events").dtypes)["event_ts"] == "timestamp"
    # header vs lines: completeness flag matches what is actually in silver
    per = lines.groupBy("claim_id").count()
    chk = claims.join(per, "claim_id", "left").withColumn("n", F.coalesce("count", F.lit(0)))
    assert chk.where("lines_in_silver != n").count() == 0
    assert chk.where("is_line_complete != (n = line_count)").count() == 0
    assert (
        claims.where("NOT is_line_complete").count() > 0
    )  # claims that lost a bad line are flagged


def test_rerun_is_idempotent(pipeline):
    p = pipeline
    names = ["claims", "claim_lines", "diagnoses", "members", "providers", "notes", "events"]
    names += ["silver_quarantine", "silver_reconciliation"]

    def snap():
        out = {}
        for n in names:
            df = tbl(p.spark, p.silver, n)
            out[n] = (df.count(), df.select(F.sha2(F.concat_ws("|", *sorted(
                c for c in df.columns if not c.startswith("_silver") and c != "_quarantined_at"
                and c != "_run_ts"
            )), 256)).distinct().count())  # fmt: skip
        return out

    before = snap()
    ids = {
        n: sorted(r[0] for r in tbl(p.spark, p.silver, n).select("_row_hash").collect())
        for n in ("claims", "claim_lines", "diagnoses", "events")
    }
    # replaying one batch swaps only that batch's rows
    first, other = p.batches[1], p.batches[2]
    silver_batch(p.spark, p.bronze, p.silver, first, run_ts=datetime(2031, 1, 1))
    stamps = {
        r[0]: r[1]
        for r in tbl(p.spark, p.silver, "claims")
        .groupBy("_ingest_date")
        .agg(F.min("_silver_ts"))
        .collect()
    }
    assert stamps[first] == datetime(2031, 1, 1) and stamps[other] == RUN_TS
    for d in p.batches:
        silver_batch(p.spark, p.bronze, p.silver, d, run_ts=datetime(2030, 1, 1))
    assert snap() == before
    for n, hashes in ids.items():
        assert (
            sorted(r[0] for r in tbl(p.spark, p.silver, n).select("_row_hash").collect()) == hashes
        )
    # the replayed batch carries the new run timestamp, nothing is duplicated
    assert tbl(p.spark, p.silver, "claims").select("_silver_ts").distinct().first()[0] == datetime(
        2030, 1, 1
    )


def test_optimize_hook_compacts_without_changing_data(pipeline):
    p = pipeline
    n = tbl(p.spark, p.silver, "claims").count()
    done = optimize_tables(p.spark, p.silver)
    assert {"claims", "silver_quarantine", "silver_reconciliation"} <= set(done)
    assert tbl(p.spark, p.silver, "claims").count() == n


def test_one_file_per_batch_partition(pipeline):
    """Small batches must not fan out into one file per shuffle partition."""
    p = pipeline
    files = tbl(p.spark, p.silver, "claims").select("_ingest_date", F.input_file_name().alias("f"))
    per = files.groupBy("_ingest_date").agg(F.countDistinct("f").alias("n")).collect()
    assert per and all(r["n"] == 1 for r in per)
