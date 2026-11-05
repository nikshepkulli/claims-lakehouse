"""SCD2, surrogate keys, as-of fact resolution and reconciliation on hand-built silver (-m spark).

The data generator emits ONE member/provider snapshot, so versioning cannot be proven from it.
These tests build silver Delta tables by hand, including a changed second snapshot and a
late-arriving one, and run the real gold job against them.
"""

import hashlib
from datetime import date, datetime
from decimal import Decimal

import pytest

pytest.importorskip("pyspark")
pytest.importorskip("delta")

from pyspark.sql import functions as F  # noqa: E402

from lakehouse.bronze.ingest import _write_replace as write_replace  # noqa: E402
from lakehouse.gold import job  # noqa: E402
from lakehouse.gold.job import ReconciliationError, gold_batch  # noqa: E402
from lakehouse.gold.schema import EPOCH  # noqa: E402

pytestmark = pytest.mark.spark

D1, D2, D3 = date(2024, 1, 1), date(2024, 2, 1), date(2024, 3, 1)
RUN = datetime(2025, 1, 1, 12, 0, 0)
RUN2 = datetime(2025, 6, 1, 12, 0, 0)

MEM = (
    "member_id string, birth_year int, sex string, state string, plan_type string, "
    "enrollment_date date"
)
PRV = "provider_id string, specialty string, state string, network_status string, cost_tier string"
CLM = (
    "claim_id string, member_id string, provider_id string, claim_type string, "
    "place_of_service string, claim_status string, service_date date, received_date date, "
    "line_count int, total_billed_amount decimal(12,2), total_allowed_amount decimal(12,2), "
    "total_paid_amount decimal(12,2)"
)
LIN = (
    "line_id string, claim_id string, line_number int, procedure_code string, units int, "
    "service_date date, billed_amount decimal(12,2), allowed_amount decimal(12,2), "
    "paid_amount decimal(12,2)"
)
DX = "claim_id string, dx_seq int, icd10_code string, diagnosis_type string"


def sk(kind, *parts):
    """Python mirror of lakehouse.gold.keys.derive_sk, written from the documented scheme."""
    return hashlib.sha256("|".join([f"{kind}:v1", *map(str, parts)]).encode()).hexdigest()


def put(spark, root, name, rows, ddl, d):
    uri = f"{root}/{name}"
    df = spark.createDataFrame(rows, ddl).withColumn("_ingest_date", F.lit(d).cast("date"))
    write_replace(df, uri, f"_ingest_date = DATE'{d}'", ["_ingest_date"])


def tbl(spark, root, name):
    return spark.read.format("delta").load(f"{root}/{name}")


def member(mid, plan="HMO", sex="F", year=1980):
    return (mid, year, sex, "KA", plan, date(2020, 1, 1))


def provider(pid, tier="STANDARD"):
    return (pid, "CARDIOLOGY", "KA", "IN_NETWORK", tier)


def claim(cid, mid, pid, svc, lines=1, amt="100.00", status="PAID"):
    a = Decimal(amt)
    return (cid, mid, pid, "PROFESSIONAL", "11", status, svc, svc, lines, a, a, a)


def line(cid, n, svc, amt="100.00", proc="99213"):
    a = Decimal(amt)
    return (f"{cid}-{n:02d}", cid, n, proc, 1, svc, a, a, a)


@pytest.fixture
def lake(tmp_path):
    return str(tmp_path / "silver"), str(tmp_path / "gold")


def versions(spark, gold, mid):
    return [
        r.asDict()
        for r in tbl(spark, gold, "dim_member")
        .where(F.col("member_id") == mid)
        .orderBy("effective_from")
        .collect()
    ]


def load_dims(spark, silver, gold, d, members, run=RUN, providers=None):
    put(spark, silver, "members", members, MEM, d)
    if providers:
        put(spark, silver, "providers", providers, PRV, d)
    return gold_batch(spark, silver, gold, d, run_ts=run)


# ---------------------------------------- key scheme


def test_surrogate_keys_follow_the_documented_scheme(spark, lake):
    silver, gold = lake
    load_dims(spark, silver, gold, D1, [member("M1")], providers=[provider("P1")])
    (v,) = versions(spark, gold, "M1")
    assert v["member_sk"] == sk("member", "M1", "1900-01-01 00:00:00")
    (p,) = tbl(spark, gold, "dim_provider").collect()
    assert p["provider_sk"] == sk("provider", "P1", "1900-01-01 00:00:00")
    put(spark, silver, "diagnoses", [("C1", 1, "I10", "PRIMARY")], DX, D1)
    gold_batch(spark, silver, gold, D1, run_ts=RUN)
    (dx,) = tbl(spark, gold, "dim_diagnosis").collect()
    assert dx["diagnosis_sk"] == sk("diagnosis", "I10")
    assert len(dx["diagnosis_sk"]) == 64


def test_key_derivation_is_isolated_in_keys_module():
    """Ticket #71 swaps the digest for a keyed HMAC: nobody else may mint keys."""
    from pathlib import Path

    for f in Path(job.__file__).parent.glob("*.py"):
        text = f.read_text()
        assert "md5(" not in text, f.name
        if f.name not in ("keys.py", "scd2.py"):  # scd2.py hashes attributes (record_hash) only
            assert "sha2(" not in text, f"{f.name} derives a key outside keys.py"


# ---------------------------------------- SCD2


def test_first_load_opens_one_version_from_epoch(spark, lake):
    silver, gold = lake
    load_dims(spark, silver, gold, D1, [member("M1"), member("M2")])
    for mid in ("M1", "M2"):
        (v,) = versions(spark, gold, mid)
        assert v["effective_from"] == EPOCH and v["effective_to"] is None and v["is_current"]
        assert (
            v["load_batch_id"] == "b20240101" and v["record_hash"] and len(v["record_hash"]) == 64
        )


def test_changed_second_snapshot_closes_old_and_opens_new_version(spark, lake):
    silver, gold = lake
    load_dims(spark, silver, gold, D1, [member("M1", "HMO"), member("M2", "PPO")])
    r = load_dims(spark, silver, gold, D2, [member("M1", "PPO"), member("M2", "PPO")], RUN2)
    v1, v2 = versions(spark, gold, "M1")
    assert (v1["plan_type"], v1["effective_from"], v1["effective_to"], v1["is_current"]) == (
        "HMO",
        EPOCH,
        datetime(2024, 2, 1),
        False,
    )
    assert (v2["plan_type"], v2["effective_from"], v2["effective_to"], v2["is_current"]) == (
        "PPO",
        datetime(2024, 2, 1),
        None,
        True,
    )
    assert v1["member_sk"] != v2["member_sk"] and v1["record_hash"] != v2["record_hash"]
    assert v2["load_batch_id"] == "b20240201" and v2["loaded_at"] == RUN2
    assert len(versions(spark, gold, "M2")) == 1  # unchanged member: no new version
    assert tbl(spark, gold, "dim_member").count() == 3
    dim = next(x for x in r if x.gold_table == "dim_member")
    assert dim.identity_ok and dim.silver_rows == 2


def test_rerun_and_replay_change_nothing(spark, lake):
    silver, gold = lake
    load_dims(spark, silver, gold, D1, [member("M1", "HMO")])
    load_dims(spark, silver, gold, D2, [member("M1", "PPO")], RUN)
    before = sorted(tbl(spark, gold, "dim_member").collect(), key=lambda r: r.effective_from)
    load_dims(spark, silver, gold, D2, [member("M1", "PPO")], RUN2)  # same batch again
    gold_batch(spark, silver, gold, D1, run_ts=RUN2)  # replay the OLDER batch afterwards
    after = sorted(tbl(spark, gold, "dim_member").collect(), key=lambda r: r.effective_from)
    assert before == after  # incl. loaded_at / load_batch_id of untouched versions


def test_late_arriving_snapshot_is_inserted_into_the_middle(spark, lake):
    silver, gold = lake
    load_dims(spark, silver, gold, D1, [member("M1", "HMO")])
    load_dims(spark, silver, gold, D3, [member("M1", "EPO")])
    sk_before = {r["effective_from"]: r["member_sk"] for r in versions(spark, gold, "M1")}
    # a snapshot dated D2 arrives after D3 was loaded: the member was PPO in February
    load_dims(spark, silver, gold, D2, [member("M1", "PPO")], RUN2)
    vs = versions(spark, gold, "M1")
    assert [(v["plan_type"], v["effective_from"]) for v in vs] == [
        ("HMO", EPOCH),
        ("PPO", datetime(2024, 2, 1)),
        ("EPO", datetime(2024, 3, 1)),
    ]
    assert [v["effective_to"] for v in vs] == [datetime(2024, 2, 1), datetime(2024, 3, 1), None]
    assert [v["is_current"] for v in vs] == [False, False, True]
    assert vs[2]["member_sk"] == sk_before[datetime(2024, 3, 1)]  # later version keeps its key


def test_correction_at_an_existing_date_updates_in_place_and_keeps_the_key(spark, lake):
    silver, gold = lake
    load_dims(spark, silver, gold, D1, [member("M1", "HMO")])
    load_dims(spark, silver, gold, D2, [member("M1", "PPO")])
    key_before = versions(spark, gold, "M1")[1]["member_sk"]
    load_dims(spark, silver, gold, D2, [member("M1", "EPO")], RUN2)  # D2 snapshot corrected
    vs = versions(spark, gold, "M1")
    assert [v["plan_type"] for v in vs] == ["HMO", "EPO"]
    assert vs[1]["member_sk"] == key_before and vs[1]["loaded_at"] == RUN2


def test_correction_equal_to_neighbour_collapses_the_version(spark, lake):
    silver, gold = lake
    load_dims(spark, silver, gold, D1, [member("M1", "HMO")])
    load_dims(spark, silver, gold, D2, [member("M1", "PPO")])
    r = load_dims(spark, silver, gold, D2, [member("M1", "HMO")], RUN2)  # D2 was an error
    (v,) = versions(spark, gold, "M1")
    assert (v["plan_type"], v["effective_to"], v["is_current"]) == ("HMO", None, True)
    assert next(x for x in r if x.gold_table == "dim_member").identity_ok


def test_snapshot_with_duplicate_keys_is_rejected(spark, lake):
    silver, gold = lake
    put(spark, silver, "members", [member("M1", "HMO"), member("M1", "PPO")], MEM, D1)
    with pytest.raises(ValueError, match="several rows"):
        gold_batch(spark, silver, gold, D1, run_ts=RUN)


def test_provider_dimension_is_scd2_too(spark, lake):
    silver, gold = lake
    load_dims(spark, silver, gold, D1, [member("M1")], providers=[provider("P1", "STANDARD")])
    put(spark, silver, "providers", [provider("P1", "HIGH_COST")], PRV, D2)
    gold_batch(spark, silver, gold, D2, run_ts=RUN2)
    rows = tbl(spark, gold, "dim_provider").orderBy("effective_from").collect()
    assert [(r.cost_tier, r.is_current) for r in rows] == [("STANDARD", False), ("HIGH_COST", True)]


# ---------------------------------------- facts


def seed_claims(spark, silver, d, claims, lines, dx=()):
    put(spark, silver, "claims", claims, CLM, d)
    put(spark, silver, "claim_lines", lines, LIN, d)
    put(spark, silver, "diagnoses", list(dx), DX, d)


def test_facts_point_at_the_version_valid_on_the_service_date(spark, lake):
    silver, gold = lake
    load_dims(spark, silver, gold, D1, [member("M1", "HMO")], providers=[provider("P1")])
    load_dims(spark, silver, gold, D2, [member("M1", "PPO")])
    early, late = date(2024, 1, 15), date(2024, 2, 15)
    seed_claims(
        spark,
        silver,
        D3,
        [claim("C1", "M1", "P1", early), claim("C2", "M1", "P1", late)],
        [line("C1", 1, early), line("C2", 1, late)],
        [
            ("C1", 1, "I10", "PRIMARY"),
            ("C1", 2, "E11.9", "SECONDARY"),
            ("C2", 1, "E11.9", "PRIMARY"),
        ],
    )
    r = gold_batch(spark, silver, gold, D3, run_ts=RUN)
    assert all(x.identity_ok for x in r)
    fc = {x.claim_id: x for x in tbl(spark, gold, "fact_claim").collect()}
    assert fc["C1"].member_sk == sk("member", "M1", "1900-01-01 00:00:00")
    assert fc["C2"].member_sk == sk("member", "M1", "2024-02-01 00:00:00")
    assert fc["C1"].primary_diagnosis_sk == sk("diagnosis", "I10")
    assert fc["C2"].primary_diagnosis_sk == sk("diagnosis", "E11.9")
    assert (fc["C1"].service_date_key, fc["C1"].received_date_key) == (20240115, 20240115)
    assert fc["C1"].load_batch_id == "b20240301"
    assert tbl(spark, gold, "fact_claim_line").count() == 2


def test_late_snapshot_re_points_facts_that_are_already_loaded(spark, lake):
    silver, gold = lake
    load_dims(spark, silver, gold, D1, [member("M1", "HMO")], providers=[provider("P1")])
    svc = date(2024, 2, 20)
    seed_claims(spark, silver, D3, [claim("C1", "M1", "P1", svc)], [line("C1", 1, svc)])
    gold_batch(spark, silver, gold, D3, run_ts=RUN)
    old_sk = tbl(spark, gold, "fact_claim").first().member_sk
    assert old_sk == sk("member", "M1", "1900-01-01 00:00:00")
    # the member changed plan on 2024-02-01; that snapshot only arrives now
    load_dims(spark, silver, gold, D2, [member("M1", "PPO")], RUN2)
    new_sk = tbl(spark, gold, "fact_claim").first().member_sk
    assert new_sk == sk("member", "M1", "2024-02-01 00:00:00") != old_sk
    # integrity holds after the re-key, checked by the next batch's reconciliation too
    assert all(x.identity_ok for x in gold_batch(spark, silver, gold, D3, run_ts=RUN2))


def test_incomplete_and_inconsistent_claims_are_held_back_not_dropped(spark, lake):
    silver, gold = lake
    load_dims(spark, silver, gold, D1, [member("M1")], providers=[provider("P1")])
    svc = date(2024, 1, 10)
    seed_claims(
        spark,
        silver,
        D2,
        [
            claim("OK", "M1", "P1", svc),
            claim("SHORT", "M1", "P1", svc, lines=2, amt="200.00"),  # one line was quarantined
            claim("BADSUM", "M1", "P1", svc, amt="100.00"),  # header != lines
        ],
        [line("OK", 1, svc), line("SHORT", 1, svc), line("BADSUM", 1, svc, amt="90.00")],
    )
    r = gold_batch(spark, silver, gold, D2, run_ts=RUN)  # identity holds: nothing is lost
    assert all(x.identity_ok for x in r)
    assert {x.claim_id for x in tbl(spark, gold, "fact_claim").collect()} == {"OK"}
    ex = {
        (x.dataset, x.business_key): x.reason_code
        for x in tbl(spark, gold, "gold_exclusions").collect()
    }
    assert ex[("claims", "SHORT")] == "INCOMPLETE_LINES"
    assert ex[("claims", "BADSUM")] == "TOTALS_MISMATCH"
    assert ex[("claim_lines", "SHORT-01")] == "PARENT_CLAIM_EXCLUDED"
    rec = {x.gold_table: x for x in tbl(spark, gold, "gold_reconciliation").collect()}
    c = rec["fact_claim"]
    assert (c.silver_rows, c.gold_rows, c.excluded_rows) == (3, 1, 2)
    assert (c.silver_billed, c.gold_billed, c.excluded_billed) == (
        Decimal("400.00"),
        Decimal("100.00"),
        Decimal("300.00"),
    )
    assert rec["fact_claim_line"].excluded_rows == 2 and rec["fact_claim_line"].identity_ok


def test_claim_whose_dimension_snapshot_is_missing_fails_loudly(spark, lake):
    silver, gold = lake
    svc = date(2024, 1, 10)
    load_dims(spark, silver, gold, D1, [member("M1")], providers=[provider("P1")])
    seed_claims(
        spark, silver, D2, [claim("C1", "GHOST", "P1", svc)], [line("C1", 1, svc)]
    )  # GHOST was never loaded into dim_member
    with pytest.raises(ReconciliationError, match="unresolved_dimension_claims"):
        gold_batch(spark, silver, gold, D2, run_ts=RUN)
    rec = tbl(spark, gold, "gold_reconciliation").where("gold_table = 'fact_claim'").first()
    assert not rec.identity_ok and rec.key_violations == 1  # recorded before raising
    held = tbl(spark, gold, "gold_exclusions").where("dataset = 'claims'").first()
    assert held.reason_code == "UNRESOLVED_MEMBER"


def test_lost_fact_row_breaks_reconciliation_loudly(spark, lake, monkeypatch):
    silver, gold = lake
    load_dims(spark, silver, gold, D1, [member("M1")], providers=[provider("P1")])
    svc = date(2024, 1, 10)
    seed_claims(
        spark,
        silver,
        D2,
        [claim("C1", "M1", "P1", svc), claim("C2", "M1", "P1", svc)],
        [line("C1", 1, svc), line("C2", 1, svc)],
    )
    real = job._write_facts

    def lossy(spark_, root, name, df, b):
        return real(spark_, root, name, df.limit(1) if name == "fact_claim" else df, b)

    monkeypatch.setattr(job, "_write_facts", lossy)
    with pytest.raises(ReconciliationError, match="fact_claim"):
        gold_batch(spark, silver, gold, D2, run_ts=RUN)
    rec = tbl(spark, gold, "gold_reconciliation").where("gold_table = 'fact_claim'").first()
    assert (rec.silver_rows, rec.gold_rows, rec.identity_ok) == (2, 1, False)


def test_fact_batch_rerun_is_idempotent_and_isolated(spark, lake):
    silver, gold = lake
    load_dims(spark, silver, gold, D1, [member("M1")], providers=[provider("P1")])
    s1, s2 = date(2024, 1, 10), date(2024, 2, 10)
    seed_claims(spark, silver, D2, [claim("A", "M1", "P1", s1)], [line("A", 1, s1)])
    seed_claims(spark, silver, D3, [claim("B", "M1", "P1", s2)], [line("B", 1, s2)])
    gold_batch(spark, silver, gold, D2, run_ts=RUN)
    gold_batch(spark, silver, gold, D3, run_ts=RUN)
    gold_batch(spark, silver, gold, D2, run_ts=RUN2)  # replay batch 2 only
    fc = {x.claim_id: x for x in tbl(spark, gold, "fact_claim").collect()}
    assert set(fc) == {"A", "B"}
    assert fc["A"].loaded_at == RUN2 and fc["B"].loaded_at == RUN  # B untouched
    assert tbl(spark, gold, "gold_reconciliation").where("gold_table='fact_claim'").count() == 2


# ---------------------------------------- dim_date / dim_diagnosis


def test_dim_date_is_a_full_calendar_over_the_data_window(spark, lake):
    silver, gold = lake
    load_dims(spark, silver, gold, D1, [member("M1")], providers=[provider("P1")])
    svc = date(2024, 3, 9)
    seed_claims(spark, silver, D2, [claim("C1", "M1", "P1", svc)], [line("C1", 1, svc)])
    gold_batch(spark, silver, gold, D2, run_ts=RUN)
    dd = tbl(spark, gold, "dim_date")
    assert dd.count() == 366 and dd.select("date_key").distinct().count() == 366  # leap year 2024
    assert dd.agg(F.min("full_date"), F.max("full_date")).first() == (
        date(2024, 1, 1),
        date(2024, 12, 31),
    )
    sat = dd.where("date_key = 20240309").first()  # 2024-03-09 was a Saturday
    assert (sat.month_name, sat.day_of_week, sat.is_weekend, sat.quarter) == ("March", 6, True, 1)
    assert dd.where("date_key = 20240311").first().day_of_week == 1  # Monday = 1 (ISO)
    assert dd.where("is_weekend").count() == 104  # 2024 = 52 weeks + Mon + Tue


def test_dim_date_widens_for_a_late_claim_and_never_shrinks(spark, lake):
    silver, gold = lake
    load_dims(spark, silver, gold, D1, [member("M1")], providers=[provider("P1")])
    s = date(2024, 5, 5)
    seed_claims(spark, silver, D2, [claim("C1", "M1", "P1", s)], [line("C1", 1, s)])
    gold_batch(spark, silver, gold, D2, run_ts=RUN)
    old = date(2023, 11, 20)  # service date in an earlier year, received late
    seed_claims(spark, silver, D3, [claim("C2", "M1", "P1", old)], [line("C2", 1, old)])
    gold_batch(spark, silver, gold, D3, run_ts=RUN)
    dd = tbl(spark, gold, "dim_date")
    assert dd.agg(F.min("full_date"), F.max("full_date")).first() == (
        date(2023, 1, 1),
        date(2024, 12, 31),
    )
    assert dd.count() == 365 + 366
    gold_batch(spark, silver, gold, D2, run_ts=RUN)  # replaying the narrower batch keeps it
    assert tbl(spark, gold, "dim_date").count() == 365 + 366


def test_dim_diagnosis_is_an_idempotent_type1_upsert(spark, lake):
    silver, gold = lake
    put(
        spark,
        silver,
        "diagnoses",
        [("C1", 1, "I10", "PRIMARY"), ("C1", 2, "ZZ9", "SECONDARY")],
        DX,
        D1,
    )
    gold_batch(spark, silver, gold, D1, run_ts=RUN)
    put(
        spark,
        silver,
        "diagnoses",
        [("C2", 1, "I10", "PRIMARY"), ("C2", 2, "E11.9", "SECONDARY")],
        DX,
        D2,
    )
    gold_batch(spark, silver, gold, D2, run_ts=RUN2)
    gold_batch(spark, silver, gold, D2, run_ts=RUN2)
    rows = {r.icd10_code: r for r in tbl(spark, gold, "dim_diagnosis").collect()}
    assert set(rows) == {"I10", "ZZ9", "E11.9"}
    assert rows["I10"].load_batch_id == "b20240101"  # first seen, not rewritten
    assert rows["I10"].icd10_chapter == "I" and "hypertension" in rows["I10"].description
    assert rows["ZZ9"].description == "Synthetic ICD-10 code"


# ---------------------------------------- maintenance


def test_optimize_and_vacuum_hooks(spark, lake):
    silver, gold = lake
    load_dims(spark, silver, gold, D1, [member("M1")], providers=[provider("P1")])
    assert job._tables(gold, spark)[:3] == ["dim_date", "dim_member", "dim_provider"]
    assert job.optimize_tables(spark, gold, ["dim_member"]) == ["dim_member"]
    assert job.vacuum_tables(spark, gold, tables=["dim_member"]) == ["dim_member"]
    with pytest.raises(ValueError, match="168"):
        job.vacuum_tables(spark, gold, retention_hours=1)
