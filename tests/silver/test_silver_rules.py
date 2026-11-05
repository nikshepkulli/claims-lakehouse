"""Silver rule tests on hand-built bronze rows (what claimsgen does not produce).

Need PySpark + Java: `make test-spark` (Docker) or the CI `spark tests (silver-rules)` job.
Conflicting duplicates, winner tie-breaks, cast failures, trimming, case-sensitive codes,
late-arrival boundary, forced reconciliation failure, missing dimension, CLI subprocess.
"""

import json
import os
import subprocess
import sys
from datetime import date, datetime
from pathlib import Path

import pytest

pytest.importorskip("pyspark")
pytest.importorskip("delta")

from pyspark.sql import functions as F  # noqa: E402

from lakehouse.silver import job  # noqa: E402
from lakehouse.silver.job import ReconciliationError, silver_batch  # noqa: E402
from lakehouse.silver.transform import with_hash_and_payload  # noqa: E402

pytestmark = pytest.mark.spark

SRC = Path(__file__).resolve().parents[2] / "src"
RUN_TS = datetime(2025, 1, 1, 12, 0, 0)
# bronze tables carry these metadata columns exactly as lakehouse.bronze writes them
META_DDL = "_ingest_ts timestamp, _source_file string, _batch_id string, _ingest_date date"


def tbl(spark, root, name):
    return spark.read.format("delta").load(f"{root}/{name}")


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
