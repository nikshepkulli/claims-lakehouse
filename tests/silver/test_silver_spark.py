"""Silver tests. Need PySpark + Java: `make test-spark` (Docker) or the CI `spark tests` job.

Two layers:
* an end-to-end run bronze -> silver over a small claimsgen fixture (2000 claims, seed 42)
  that checks EVERY defect in defects_manifest.json against silver / silver_quarantine;
* hand-built bronze rows for the rules that claimsgen does not produce (conflicting duplicates,
  tie-breaks, type-cast failures, whitespace, reconciliation failure, CLI).

The fixture compresses claimsgen's ~370 daily partitions into 3 bronze batches (files are moved
into 3 `ingest_date=` folders and ingested by the real bronze code) so the suite stays fast;
silver logic does not depend on how many batches there are.
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
from lakehouse.silver.job import ReconciliationError, optimize_tables, silver_batch  # noqa: E402
from lakehouse.silver.transform import with_hash_and_payload  # noqa: E402

pytestmark = pytest.mark.spark

SRC = Path(__file__).resolve().parents[2] / "src"
RUN_TS = datetime(2025, 1, 1, 12, 0, 0)
N_BUCKETS = 3
DIMS_DAY = date(2024, 1, 1)
# bronze events carry `_ingest_ts` etc. exactly like lakehouse.bronze writes them
META_DDL = "_ingest_ts timestamp, _source_file string, _batch_id string, _ingest_date date"


# --------------------------------------------------------------------------- fixture pipeline


@pytest.fixture(scope="module")
def raw(tmp_path_factory):
    out = tmp_path_factory.mktemp("raw")
    subprocess.run(
        [sys.executable, "-m", "claimsgen", "--rows", "2000", "--seed", "42", "--out", str(out),
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


def test_single_batch_replay_leaves_other_batches_alone(pipeline):
    p = pipeline
    first, other = p.batches[1], p.batches[2]
    c = tbl(p.spark, p.silver, "claims")
    other_before = c.where(F.col("_ingest_date") == F.lit(other)).count()
    silver_batch(p.spark, p.bronze, p.silver, first, ["claims"], run_ts=datetime(2031, 1, 1))
    c = tbl(p.spark, p.silver, "claims")
    assert c.where(F.col("_ingest_date") == F.lit(other)).count() == other_before
    assert c.where(F.col("_ingest_date") == F.lit(other)).select("_silver_ts").distinct().first()[
        0
    ] != datetime(2031, 1, 1)
    silver_batch(p.spark, p.bronze, p.silver, first, run_ts=RUN_TS)  # restore for later tests


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


# ---------------------------------------------------------------- hand-built bronze rule tests

CLAIM_COLS = {
    "claim_id": "string", "member_id": "string", "provider_id": "string",
    "claim_type": "string", "place_of_service": "string", "service_date": "date",
    "received_date": "date", "claim_status": "string", "line_count": "int",
    "total_billed_amount": "double", "total_allowed_amount": "double",
    "total_paid_amount": "double",
}  # fmt: skip
LINE_COLS = {
    "line_id": "string", "claim_id": "string", "line_number": "string", "procedure_code": "string",
    "units": "string", "service_date": "string", "billed_amount": "string",
    "allowed_amount": "string", "paid_amount": "string",
}  # fmt: skip
DX_COLS = {
    "claim_id": "string", "dx_seq": "int", "icd10_code": "string", "diagnosis_type": "string",
}  # fmt: skip
D = date(2024, 5, 1)
T1, T2, T3 = datetime(2024, 5, 1, 9), datetime(2024, 5, 1, 10), datetime(2024, 5, 1, 11)


def put(spark, root, name, cols, rows, d=D):
    """Write hand-made rows as a bronze batch (metadata columns exactly as bronze adds them)."""
    ddl = ", ".join(f"{c} {t}" for c, t in cols.items()) + ", " + META_DDL
    data = []
    for r in rows:
        meta = (
            r.get("_ingest_ts", T1),
            r.get("_source_file", f"{name}/f1.parquet"),
            f"b{d:%Y%m%d}",
            d,
        )
        data.append((*[r.get(c) for c in cols], *meta))
    (
        spark.createDataFrame(data, ddl)
        .write.format("delta")
        .mode("append")
        .partitionBy("_ingest_date")
        .save(f"{root}/{name}")
    )


def claim(cid, **kw):
    base = {
        "claim_id": cid, "member_id": "M1", "provider_id": "P1", "claim_type": "PROFESSIONAL",
        "place_of_service": "11", "service_date": date(2024, 4, 29), "received_date": D,
        "claim_status": "PAID", "line_count": 1, "total_billed_amount": 100.0,
        "total_allowed_amount": 80.0, "total_paid_amount": 60.0,
    }  # fmt: skip
    return {**base, **kw}


def seed_dims(spark, root):
    put(spark, root, "members", {"member_id": "string", "birth_year": "int", "sex": "string"},
        [{"member_id": "M1", "birth_year": 1980, "sex": "F"}], date(2024, 1, 1))  # fmt: skip
    put(spark, root, "providers", {"provider_id": "string", "cost_tier": "string"},
        [{"provider_id": "P1", "cost_tier": "STANDARD"}], date(2024, 1, 1))  # fmt: skip


@pytest.fixture
def lake(tmp_path):
    return str(tmp_path / "bronze"), str(tmp_path / "silver")


def run(spark, lake, **kw):
    return silver_batch(spark, lake[0], lake[1], D, run_ts=RUN_TS, **kw)


def test_dedupe_winner_is_deterministic_and_conflicts_are_quarantined(spark, lake, tmp_path):
    b, s = lake
    seed_dims(spark, b)
    rows = [
        # C1: newest _ingest_ts wins; an identical re-send is deduped; an older, different copy
        # is a conflicting duplicate
        claim("C1", total_billed_amount=100.0, _ingest_ts=T2),
        claim("C1", total_billed_amount=200.0, _ingest_ts=T3),
        claim("C1", total_billed_amount=200.0, _ingest_ts=T3),
        claim("C1", total_billed_amount=300.0, _ingest_ts=T1),
        # C2: same timestamp -> greatest _source_file wins
        claim("C2", total_billed_amount=1.0, _source_file="claims/a.parquet"),
        claim("C2", total_billed_amount=2.0, _source_file="claims/b.parquet"),
        # C3: same timestamp and file -> greatest row hash wins
        claim("C3", total_billed_amount=5.0),
        claim("C3", total_billed_amount=6.0),
    ]
    put(spark, b, "claims", CLAIM_COLS, rows)
    res = {r.dataset: r for r in run(spark, lake, datasets=["claims"])}["claims"]
    counts = (res.bronze_rows, res.silver_rows, res.quarantined_rows, res.deduped_rows)
    assert counts == (8, 3, 4, 1)
    got = {r["claim_id"]: r for r in tbl(spark, s, "claims").collect()}
    assert float(got["C1"]["total_billed_amount"]) == 200.0
    assert float(got["C2"]["total_billed_amount"]) == 2.0
    # hash input includes every source column, so rebuild the expected winner from the bronze rows
    c3 = tbl(spark, b, "claims").where("claim_id = 'C3'")
    cols = [c for c in c3.columns if not c.startswith("_")]
    hs = {
        float(r["total_billed_amount"]): r["_row_hash"]
        for r in with_hash_and_payload(c3, cols).collect()
    }
    assert float(got["C3"]["total_billed_amount"]) == max(hs, key=lambda k: hs[k])
    q = tbl(spark, s, "silver_quarantine").where("dataset = 'claims'").collect()
    assert {r["reason_code"] for r in q} == {"CONFLICTING_DUPLICATE"}
    assert sorted(r["business_key"] for r in q) == ["C1", "C1", "C2", "C3"]
    assert all("winner" in r["reason_detail"] for r in q)
    assert all(json.loads(r["payload"])["claim_id"] == r["business_key"] for r in q)

    # same bronze rows presented in the opposite order, in a fresh lake -> identical outcome
    b2, s2 = str(tmp_path / "bronze2"), str(tmp_path / "silver2")
    seed_dims(spark, b2)
    put(spark, b2, "claims", CLAIM_COLS, list(reversed(rows)))
    silver_batch(spark, b2, s2, D, ["claims"], run_ts=RUN_TS)
    got2 = {r["claim_id"]: r["_row_hash"] for r in tbl(spark, s2, "claims").collect()}
    assert got2 == {k: v["_row_hash"] for k, v in got.items()}


def test_standardise_cast_and_code_rules(spark, lake):
    b, s = lake
    seed_dims(spark, b)
    put(spark, b, "claims", CLAIM_COLS, [
        claim("C1", claim_type=" professional ", claim_status="paid", line_count=3),
        claim("C2", claim_type="DENTAL"),
    ])  # fmt: skip
    line = {
        "claim_id": "C1", "line_number": "1", "units": "1", "service_date": "2024-04-29",
        "billed_amount": "10.50", "allowed_amount": "8.00", "paid_amount": "6.00",
    }  # fmt: skip
    put(spark, b, "claim_lines", LINE_COLS, [
        {**line, "line_id": "C1-01", "procedure_code": " 12345 ", "billed_amount": " 10.5 "},
        {**line, "line_id": "C1-02", "procedure_code": "A1234"},
        {**line, "line_id": "C1-03", "procedure_code": "W1234"},  # W is outside [A-V]
        {**line, "line_id": "C1-04", "procedure_code": "12345", "billed_amount": "abc"},
        {**line, "line_id": "C1-05", "procedure_code": "12345", "paid_amount": "-1"},
        {**line, "line_id": "C1-06", "procedure_code": "   "},
        {**line, "line_id": "C9-01", "claim_id": "C9", "procedure_code": "12345"},
        {**line, "line_id": "C2-01", "claim_id": "C2", "procedure_code": "12345"},
    ])  # fmt: skip
    dx = {"claim_id": "C1", "diagnosis_type": "primary"}
    put(spark, b, "diagnoses", DX_COLS, [
        {**dx, "dx_seq": 1, "icd10_code": "S83.511A"},
        {**dx, "dx_seq": 2, "icd10_code": "i1o"},  # lower case: invalid, must not be "repaired"
        {**dx, "dx_seq": 3, "icd10_code": "U07.1"},  # U is outside [A-TV-Z]
        {**dx, "dx_seq": 4, "icd10_code": None},
    ])  # fmt: skip
    res = {r.dataset: r for r in run(spark, lake)}
    assert all(r.identity_ok for r in res.values())
    c = tbl(spark, s, "claims").collect()
    assert [(r["claim_id"], r["claim_type"], r["claim_status"]) for r in c] == [
        ("C1", "PROFESSIONAL", "PAID")
    ]
    ok = tbl(spark, s, "claim_lines").collect()
    assert sorted(r["line_id"] for r in ok) == ["C1-01", "C1-02"]
    assert str(next(r for r in ok if r["line_id"] == "C1-01")["billed_amount"]) == "10.50"
    code = {
        r["business_key"]: r["all_reason_codes"]
        for r in tbl(spark, s, "silver_quarantine").where("dataset = 'claim_lines'").collect()
    }
    assert code["C1-03"] == ["INVALID_PROCEDURE_CODE"]
    assert code["C1-04"] == ["TYPE_CAST_FAILURE"]
    assert code["C1-05"] == ["NEGATIVE_AMOUNT"]
    assert code["C1-06"] == ["NULL_PROCEDURE_CODE"]  # blank -> NULL
    assert code["C9-01"] == ["ORPHAN_CLAIM_ID"]
    assert code["C2-01"] == ["PARENT_CLAIM_QUARANTINED"]
    cq = tbl(spark, s, "silver_quarantine").where("dataset = 'claims'").first()
    assert (cq["business_key"], cq["reason_code"]) == ("C2", "INVALID_CLAIM_TYPE")
    dxq = {
        r["business_key"]: r["reason_code"]
        for r in tbl(spark, s, "silver_quarantine").where("dataset = 'diagnoses'").collect()
    }
    assert dxq == {"C1-2": "INVALID_ICD10", "C1-3": "INVALID_ICD10", "C1-4": "NULL_ICD10"}
    assert [r["icd10_code"] for r in tbl(spark, s, "diagnoses").collect()] == ["S83.511A"]


def test_late_arrival_is_flagged_not_dropped(spark, lake):
    b, s = lake
    seed_dims(spark, b)
    put(spark, b, "claims", CLAIM_COLS, [
        claim("ON_TIME", service_date=date(2024, 4, 29)),
        claim("LATE", service_date=date(2024, 3, 17)),  # 45 days
        claim("EDGE", service_date=date(2024, 4, 1)),  # 30 days: not late
        claim("TIME_TRAVEL", service_date=date(2024, 5, 3)),  # received before service
    ])  # fmt: skip
    run(spark, lake, datasets=["claims"])
    got = {r["claim_id"]: r for r in tbl(spark, s, "claims").collect()}
    assert set(got) == {"ON_TIME", "LATE", "EDGE"}
    assert (got["LATE"]["is_late_arrival"], got["LATE"]["arrival_lag_days"]) == (True, 45)
    assert not got["EDGE"]["is_late_arrival"] and not got["ON_TIME"]["is_late_arrival"]
    q = tbl(spark, s, "silver_quarantine").first()
    assert (q["business_key"], q["reason_code"]) == ("TIME_TRAVEL", "RECEIVED_BEFORE_SERVICE")
    assert got["ON_TIME"]["prior_auth_flag"] is None  # optional drift column present, nullable


def test_reconciliation_failure_is_loud_and_recorded(spark, lake, monkeypatch):
    b, s = lake
    seed_dims(spark, b)
    put(spark, b, "claims", CLAIM_COLS, [claim("C1"), claim("C2")])
    real = job.classify

    def lossy(df, spec, refs=None):  # a buggy transform that silently loses a row
        out, cols = real(df, spec, refs)
        return out.where(F.col("_key") != "C2"), cols

    monkeypatch.setattr(job, "classify", lossy)
    with pytest.raises(ReconciliationError, match="claims"):
        run(spark, lake, datasets=["claims"])
    rec = tbl(spark, s, "silver_reconciliation").where("dataset = 'claims'").first()
    assert rec["identity_ok"] is False and rec["bronze_rows"] == 2


def test_missing_dimension_fails_instead_of_orphaning_everything(spark, lake):
    b, _ = lake
    put(spark, b, "claims", CLAIM_COLS, [claim("C1")])
    with pytest.raises(RuntimeError, match="bronze members not found"):
        run(spark, lake, datasets=["claims"])


def test_cli_end_to_end(spark, lake):
    b, s = lake
    seed_dims(spark, b)
    put(spark, b, "claims", CLAIM_COLS, [claim("C1"), claim("C2", member_id="NOBODY")])
    env = {
        **os.environ,
        "PYTHONPATH": os.pathsep.join([str(SRC), os.environ.get("PYTHONPATH", "")]),
    }
    out = subprocess.run(
        [sys.executable, "-m", "lakehouse.silver", "--bronze", b, "--silver", s,
         "--batch-date", "all", "--datasets", "claims", "--master", "local[1]", "--optimize"],
        check=True, capture_output=True, text=True, env=env,
    )  # fmt: skip
    assert (
        "claims" in out.stdout and "silver=1 quarantined=1 deduped=0 reconciled=True" in out.stdout
    )
    assert "optimized:" in out.stdout
    bad = subprocess.run(
        [sys.executable, "-m", "lakehouse.silver", "--bronze", b, "--silver", s,
         "--batch-date", "nope"],
        capture_output=True, text=True, env=env,
    )  # fmt: skip
    assert bad.returncode == 2
