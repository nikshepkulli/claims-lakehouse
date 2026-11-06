"""Build the dashboard on the gold fixture and check it end to end."""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]


def test_build_is_fast_enough_for_ci(built):
    assert built["seconds"] < 60, f"build took {built['seconds']:.1f}s"


def test_site_is_one_self_contained_file(built):
    site = built["index"].parent
    assert [p.name for p in site.iterdir()] == ["index.html"]
    assert built["index"].stat().st_size < 400_000


def test_every_query_obeys_the_contract(built):
    from dashboard.build import QUERY_DIRS

    files = [p for d in QUERY_DIRS for p in d.glob("*.sql")]
    assert len(files) >= 14
    assert set(built["payload"]["meta"]["queries"]) == {p.stem for p in files}


def _con(built, name):
    import duckdb

    return duckdb.connect(), (built["gold"] / name).as_posix() + "/*.parquet"


def test_headline_numbers_match_gold_parquet(built):
    """Independent path: sum the gold fact table directly, not via the marts."""
    con, fc = _con(built, "fact_claim")
    claims, billed, allowed, paid, denied = con.execute(
        f"SELECT count(*), sum(total_billed_amount), sum(total_allowed_amount), "
        f"sum(total_paid_amount), count(*) FILTER (WHERE claim_status = 'DENIED') "
        f"FROM read_parquet('{fc}')"
    ).fetchone()
    k = built["payload"]["kpis"]
    assert k["claims"] == claims
    assert k["denied_claims"] == denied
    assert k["billed"] == pytest.approx(float(billed), abs=0.01)
    assert k["allowed"] == pytest.approx(float(allowed), abs=0.01)
    assert k["paid"] == pytest.approx(float(paid), abs=0.01)
    assert k["denial_rate"] == pytest.approx(denied / claims)
    assert k["paid_to_billed"] == pytest.approx(float(paid) / float(billed))


def test_monthly_rows_match_gold_parquet(built):
    con, fc = _con(built, "fact_claim")
    rows = con.execute(
        f"""SELECT date_trunc('month', strptime(CAST(service_date_key AS VARCHAR), '%Y%m%d'))::DATE,
                   count(*), sum(total_paid_amount), sum(total_billed_amount)
            FROM read_parquet('{fc}') GROUP BY 1 ORDER BY 1"""
    ).fetchall()
    monthly = built["payload"]["monthly"]
    assert len(monthly) == len(rows) == 12
    for m, (month, n, paid, billed) in zip(monthly, rows, strict=True):
        assert m["service_month"] == month.isoformat()
        assert m["claim_count"] == n
        assert m["paid_amount"] == pytest.approx(float(paid), abs=0.01)
        assert m["billed_amount"] == pytest.approx(float(billed), abs=0.01)


def test_pmpm_series_and_trend_match_query_01(built):
    """The PMPM series is query 01 unchanged; the trend is an OLS fit of it."""
    monthly = built["payload"]["monthly"]
    t = built["payload"]["pmpm_trend"]
    xs = range(len(monthly))
    mean = (len(monthly) - 1) / 2
    slope = sum((i - mean) * m["pmpm_paid"] for i, m in zip(xs, monthly, strict=True)) / sum(
        (i - mean) ** 2 for i in xs
    )
    assert t["slope"] == pytest.approx(slope, rel=1e-9)
    assert len(t["fitted"]) == len(monthly)
    # PMPM = paid / member_months (rounded to cents in the mart)
    for m in monthly:
        assert m["pmpm_paid"] == pytest.approx(m["paid_amount"] / m["member_months"], abs=0.01)


def test_member_concentration_matches_independent_computation(built):
    con, fc = _con(built, "fact_claim")
    _, dm = _con(built, "dim_member")
    totals = [
        float(r[0])
        for r in con.execute(
            f"""SELECT coalesce(sum(c.total_paid_amount), 0)
                FROM (SELECT DISTINCT member_id FROM read_parquet('{dm}')) m
                LEFT JOIN read_parquet('{dm}') d ON d.member_id = m.member_id
                LEFT JOIN read_parquet('{fc}') c ON c.member_sk = d.member_sk
                GROUP BY m.member_id ORDER BY 1"""
        ).fetchall()
    ]
    assert len(totals) == 1000
    total = sum(totals)
    conc = built["payload"]["members"]["concentration"]
    for p in (1, 5, 10, 20):
        k = round(len(totals) * p / 100)
        assert conc[f"top{p}"] == pytest.approx(sum(totals[-k:]) / total, abs=1e-4), p
    # Gini from the sorted values (discrete formula) vs trapezoid on the 1% grid
    n = len(totals)
    gini = sum((2 * (i + 1) - n - 1) * x for i, x in enumerate(totals)) / (n * total)
    assert conc["gini"] == pytest.approx(gini, abs=2e-3)


def test_denial_cuts_sum_to_headline(built):
    d, k = built["payload"]["denials"], built["payload"]["kpis"]
    for cut in ("by_claim_type", "by_specialty"):
        assert sum(r["claim_count"] for r in d[cut]) == k["claims"]
        assert sum(r["denied_claim_count"] for r in d[cut]) == k["denied_claims"]
    for r in d["by_claim_type"] + d["by_specialty"] + d["by_plan_type"]:
        assert r["ci_low"] <= r["denial_rate"] <= r["ci_high"]
    assert d["has_reason_or_payer"] is False


def test_flagged_providers_are_consistent_with_peer_index(built):
    p = built["payload"]["providers"]
    expected = {
        r["provider_id"] for r in p["peer_index"] if r["weighted_cost_index"] >= p["flag_threshold"]
    }
    assert {r["provider_id"] for r in p["flagged"]} == expected
    assert all(r["claim_count"] >= p["min_claims"] for r in p["peer_index"])
    assert sum(t["flagged"] for t in p["tier_summary"].values()) == len(p["flagged"])


def test_quality_section_matches_gold_and_manifest(built):
    q = built["payload"]["quality"]
    assert all(i["status"] == "pass" for i in q["invariants"]), q["invariants"]
    manifest = json.loads((built["raw"] / "defects_manifest.json").read_text())
    by_name = {d["defect"]: d for d in q["defects"]}
    for name, info in manifest["defects"].items():
        assert by_name[name]["injected"] == info["count"]
    assert by_name["duplicate_claim"]["catch_rate"] == 1.0
    claims, lines = q["raw_to_gold"]
    k = built["payload"]["kpis"]
    assert claims["gold_rows"] == k["claims"]
    assert (
        claims["raw_rows"]
        == claims["duplicates_removed"] + claims["quarantined_or_excluded"] + claims["gold_rows"]
    )  # noqa: E501
    assert (
        lines["raw_rows"]
        == lines["duplicates_removed"] + lines["quarantined_or_excluded"] + lines["gold_rows"]
    )  # noqa: E501
    assert q["reconciliation"] is None and q["quality_framework"] is None


def test_invariants_detect_a_planted_defect(built, tmp_path):
    """The scorecard must fail when gold is bad: plant a duplicate claim and a negative amount."""
    import shutil

    import duckdb

    from dashboard import quality

    bad = tmp_path / "gold"
    shutil.copytree(built["gold"], bad)
    con = duckdb.connect()
    claims = (bad / "fact_claim" / "part-00000.parquet").as_posix()
    lines = (bad / "fact_claim_line" / "part-00000.parquet").as_posix()
    con.execute(
        f"COPY (SELECT * FROM read_parquet('{claims}') UNION ALL "
        f"(SELECT * FROM read_parquet('{claims}') LIMIT 1)) TO '{claims}.new' (FORMAT parquet)"
    )
    con.execute(
        f"COPY (SELECT * REPLACE (-billed_amount AS billed_amount) FROM read_parquet('{lines}')) "
        f"TO '{lines}.new' (FORMAT parquet)"
    )
    Path(claims + ".new").replace(claims)
    Path(lines + ".new").replace(lines)
    res = {i["id"]: i for i in quality.gold_invariants(con, bad)}
    assert res["no_duplicate_claim_ids"]["violations"] == 1
    assert res["no_duplicate_claim_ids"]["status"] == "fail"
    assert res["no_negative_amounts"]["status"] == "fail"


def test_extension_points_render_when_supplied(built, tmp_path):
    from dashboard.build import build

    qj = tmp_path / "q.json"
    qj.write_text(json.dumps({"generated_by": "test", "suites": [
        {"name": "silver_claims", "layer": "silver", "evaluated": 12, "passed": 11, "failed": 1}
    ]}))  # fmt: skip
    rj = tmp_path / "r.json"
    rj.write_text(json.dumps([{"gold_table": "fact_claim", "batch_id": "b20240101",
                               "silver_rows": 10, "gold_rows": 9, "excluded_rows": 1,
                               "identity_ok": True}]))  # fmt: skip
    out = build(
        built["gold"], tmp_path / "site", raw=built["raw"], reconciliation=rj, quality_json=qj
    )
    html = out.read_text()
    m = re.search(r'id="dashboard-data">(.*?)</script>', html, re.S)
    q = json.loads(m.group(1))["quality"]
    assert q["quality_framework"]["suites"][0]["failed"] == 1
    assert q["reconciliation"][0]["batch_id"] == "b20240101"


def test_gold_root_with_export_subdir_is_resolved(built, tmp_path):
    from dashboard.build import resolve_gold

    root = tmp_path / "gold_root"
    (root / "export").mkdir(parents=True)
    (root / "export" / "fact_claim").mkdir()
    assert resolve_gold(root) == root / "export"
    with pytest.raises(SystemExit):
        resolve_gold(tmp_path / "nothing")


# ---------- member cost concentration: every declared view renders with valid data ----------
def test_member_section_declares_and_renders_every_view(built):
    from dashboard.build import MEMBER_VIEWS

    m = built["payload"]["members"]
    assert tuple(m["views"]) == MEMBER_VIEWS == ("lorenz", "pareto", "risk_tiers")
    js = (ROOT / "dashboard" / "app.js").read_text()
    for view in m["views"]:
        assert f"{view}: function ()" in js, f"app.js has no renderer for view {view!r}"
        assert m[view], f"no data for view {view!r}"
    # unknown views fail loudly instead of silently disappearing
    assert "member cost view has no renderer" in js
    # the page itself must not hide them
    css = (ROOT / "dashboard" / "style.css").read_text()
    assert "data-view" not in css and "[data-section" not in css
    html = built["html"]
    assert "ResizeObserver" in html  # charts draw whenever their container gets a width


def test_lorenz_curve_is_valid(built):
    pts = [(p["x"], p["y"]) for p in built["payload"]["members"]["lorenz"]]
    assert pts[0] == (0.0, 0.0)
    assert pts[-1] == (1.0, pytest.approx(1.0))
    assert len(pts) == 101
    for (x0, y0), (x1, y1) in zip(pts, pts[1:], strict=False):
        assert x1 > x0 and y1 >= y0 - 1e-12  # monotone
    for x, y in pts:
        assert 0 <= y <= 1 and y <= x + 1e-9  # on or below the equality line
        assert x == x and y == y  # not NaN


def test_pareto_summary_matches_member_totals(built):
    """'Top X% of members hold Y% of paid cost' equals a direct computation on gold."""
    con, fc = _con(built, "fact_claim")
    _, dm = _con(built, "dim_member")
    totals = [
        float(r[0])
        for r in con.execute(
            f"""SELECT coalesce(sum(c.total_paid_amount), 0)
                FROM (SELECT DISTINCT member_id FROM read_parquet('{dm}')) m
                LEFT JOIN read_parquet('{dm}') d ON d.member_id = m.member_id
                LEFT JOIN read_parquet('{fc}') c ON c.member_sk = d.member_sk
                GROUP BY m.member_id ORDER BY 1"""
        ).fetchall()
    ]
    total = sum(totals)
    pareto = built["payload"]["members"]["pareto"]
    assert [p["top_pct"] for p in pareto] == [1, 5, 10, 20, 50]
    for p in pareto:
        k = round(len(totals) * p["top_pct"] / 100)
        assert p["share"] == pytest.approx(sum(totals[-k:]) / total, abs=1e-4), p
        assert 0 < p["share"] <= 1 and p["share"] > p["top_pct"] / 100  # more than equal split
    assert [p["share"] for p in pareto] == sorted(p["share"] for p in pareto)


def test_risk_tier_view_data(built):
    rows = built["payload"]["members"]["risk_tiers"]
    assert {r["risk_tier"] for r in rows} == {"HIGH", "MEDIUM"}
    assert sum(r["share_of_total_paid"] for r in rows) == pytest.approx(1.0, abs=1e-3)
