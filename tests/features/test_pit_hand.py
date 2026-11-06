"""Hand-computed point-in-time expectations on tiny datasets (every number derivable by eye)."""

from datetime import date, datetime
from decimal import Decimal

import pytest
from features_helpers import features_at

from lakehouse.features.fixture import EPOCH, write_gold_export

D = Decimal
AS_OF = datetime(2024, 3, 1)


def _m(mid, versions):
    return [
        {
            "member_id": mid,
            "birth_year": 1980,
            "sex": "F",
            "state": "GA",
            "plan_type": plan,
            "enrollment_date": date(2020, 1, 1),
            "effective_from": f,
            "effective_to": t,
        }
        for plan, f, t in versions
    ]


def _p(pid, spec="Cardiology", tier="STANDARD"):
    return {
        "provider_id": pid,
        "specialty": spec,
        "state": "GA",
        "network_status": "IN_NETWORK",
        "cost_tier": tier,
        "effective_from": EPOCH,
        "effective_to": None,
    }


def _c(cid, mid, pid, recv, status="PAID", billed=100, paid=80, icd=None):
    r = date.fromisoformat(recv)
    return {
        "claim_id": cid,
        "member_id": mid,
        "provider_id": pid,
        "service_date": r,
        "received_date": r,
        "claim_status": status,
        "billed": D(billed),
        "paid": D(paid),
        "icd10": icd,
    }


@pytest.fixture(scope="module")
def export(tmp_path_factory):
    members = _m("M1", [("HMO", EPOCH, datetime(2024, 3, 1)), ("PPO", datetime(2024, 3, 1), None)])
    members += _m("M2", [("HMO", EPOCH, None)])
    providers = [_p("P1"), _p("P2")]
    claims = [
        _c("A1", "M1", "P1", "2024-01-02", "PAID", 100, 80, "E11.9"),
        _c("A2", "M1", "P1", "2024-02-10", "DENIED", 200, 0),
        _c("A3", "M1", "P2", "2024-02-20", "PAID", 300, 240),
        _c("A4", "M1", "P2", "2024-02-29", "PAID", 400, 320),
        _c("A5", "M1", "P1", "2024-03-01", "DENIED", 500, 0, "I10"),  # received AT as_of
        _c("A6", "M1", "P1", "2024-03-02", "PAID", 600, 480),  # future
        # window boundary for M2: as_of - 30d = 2024-01-31 00:00
        _c("B1", "M2", "P1", "2024-01-30", billed=10),  # event 01-30 23:59:59 <= boundary: out
        _c("B2", "M2", "P1", "2024-01-31", billed=20),  # in
    ]
    return write_gold_export(tmp_path_factory.mktemp("hand") / "export", members, providers, claims)


@pytest.fixture(scope="module")
def at(export):
    return features_at(export, AS_OF)


def test_window_counts_exclude_the_as_of_day_and_the_future(at):
    r = at[0]["M1"]
    assert r["claims_365d"] == 4  # A1..A4; A5 (received at as_of) and A6 (future) excluded
    assert r["claims_90d"] == 4
    assert r["claims_30d"] == 3  # A2, A3, A4; A1 is 59 days old
    assert r["billed_sum_90d"] == D("1000.00")
    assert r["billed_sum_365d"] == D("1000.00")
    assert r["distinct_providers_365d"] == 2
    assert r["days_since_last_claim"] == 1  # 2024-02-29 -> 2024-03-01


def test_window_boundary_is_exclusive_below_inclusive_above(at):
    # B1 received 01-30 (event 23:59:59) is not > 2024-01-31 00:00; B2 (01-31) is in the 30d window
    assert at[0]["M2"]["claims_30d"] == 1
    assert at[0]["M2"]["claims_90d"] == 2
    assert at[0]["M2"]["billed_sum_90d"] == D("30.00")


def test_no_history_gives_zero_counts_and_null_recency(export):
    r = features_at(export, datetime(2024, 1, 1))[0]["M1"]
    assert (r["claims_365d"], r["billed_sum_365d"]) == (0, D("0.00"))
    assert r["days_since_last_claim"] is None and r["denial_rate_365d"] is None
    assert r["chronic_condition_count"] == 0


def test_outcomes_only_count_once_known(at, export):
    r = at[0]["M1"]
    # outcome lag 30d: A1 (received 01-02) is resolved by 03-01; A2 (02-10, DENIED) only on 03-11
    assert r["denied_count_365d"] == 0
    assert r["denial_rate_365d"] == 0.0
    assert r["paid_sum_365d"] == D("80.00")
    later = features_at(export, datetime(2024, 3, 12))[0]["M1"]
    assert later["denied_count_365d"] == 1  # A2 now known
    assert later["denial_rate_365d"] == 0.5  # A1 paid, A2 denied; A3 lands 03-21


def test_future_diagnosis_is_not_visible_but_old_one_is(at, export):
    r = at[0]["M1"]
    assert r["has_diabetes"] is True  # A1 E11.9 received 01-02
    assert r["has_hypertension"] is False  # I10 is on A5, received AT as_of
    later = features_at(export, datetime(2024, 3, 2))[0]["M1"]
    assert later["has_hypertension"] is True
    assert later["chronic_condition_count"] == 2


def test_scd2_as_of_boundary(at, export):
    # version 2 starts exactly at 2024-03-01 00:00: valid_from <= as_of -> PPO
    assert at[0]["M1"]["plan_type"] == "PPO"
    before = features_at(export, datetime(2024, 2, 29))[0]["M1"]
    assert before["plan_type"] == "HMO"
    assert before["age_band"] == "40-44"  # 2024 - 1980 = 44


def test_claim_features_use_receipt_day_start_and_never_the_claim_itself(at):
    c = at[2]["A5"]  # claims received on 2024-03-01
    assert set(at[2]) == {"A5"}
    assert c["m_claims_365d"] == 4  # does not see itself (A5) or A6
    assert c["m_plan_type"] == "PPO"
    assert c["billed_amount"] == D("500.00")
    assert c["service_to_received_lag_days"] == 0
    assert c["p_claims_365d"] == 4  # P1 before the as_of: A1, A2, B1, B2


def test_peer_cost_z_score_exact(tmp_path):
    """Q1/Q2/Q3 (same specialty) average 100/200/300 over 5 claims each: z = -1, 0, +1."""
    members = _m("M9", [("HMO", EPOCH, None)])
    providers = [_p("Q1"), _p("Q2"), _p("Q3"), _p("QX", "Pediatrics")]
    claims = []
    for pid, amt in (("Q1", 100), ("Q2", 200), ("Q3", 300), ("QX", 999)):
        for j in range(5):
            claims.append(_c(f"{pid}-{j}", "M9", pid, f"2024-01-{10 + j:02d}", billed=amt))
    claims.append(_c("Q1-late", "M9", "Q1", "2024-03-05", billed=100000))  # future: ignored
    export = write_gold_export(tmp_path / "export", members, providers, claims)
    p = features_at(export, AS_OF)[1]
    assert p["Q1"]["cost_z_365d"] == pytest.approx(-1.0)
    assert p["Q2"]["cost_z_365d"] == pytest.approx(0.0)
    assert p["Q3"]["cost_z_365d"] == pytest.approx(1.0)
    assert p["Q1"]["avg_billed_365d"] == 100.0
    assert p["QX"]["cost_z_365d"] is None  # alone in its specialty (< min_peers)
    assert p["Q1"]["claims_365d"] == 5
    # once the big claim is in the past the Q1 mean moves and the z-scores change
    later = features_at(export, datetime(2024, 3, 6))[1]
    assert later["Q1"]["avg_billed_365d"] > 100.0
