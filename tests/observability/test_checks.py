import math

import pytest
from conftest import NOW, history, make_row

from lakehouse.observability.alerts import Alert
from lakehouse.observability.checks import age_hours, evaluate, pct_drift, zscore
from lakehouse.observability.config import load_config


def cfg(**over):
    c = load_config(None)
    c.update(over)
    return c


def by_rule(alerts):
    return {(a.table, a.rule): a for a in alerts}


def test_zscore_math():
    h = [10.0, 12.0, 14.0]  # mean 12, sample sd 2
    assert zscore(20.0, h) == pytest.approx(4.0)
    assert zscore(12.0, h) == 0
    assert zscore(5.0, [3.0]) is None
    assert zscore(5.0, [3.0, 3.0, 3.0]) is None  # zero spread: undefined, not inf


def test_pct_drift_math():
    assert pct_drift(50.0, [100.0, 100.0]) == pytest.approx(-50.0)
    assert pct_drift(150.0, [100.0]) == pytest.approx(50.0)
    assert pct_drift(1.0, [0.0, 0.0]) is None
    assert pct_drift(1.0, []) is None


def test_age_hours():
    assert age_hours(NOW.replace(hour=0), NOW) == 12.0
    assert not math.isnan(age_hours(NOW, NOW))


def test_healthy_table_no_alerts(now):
    assert evaluate(history(), cfg(), now) == []


@pytest.mark.parametrize(
    "age,sev",
    [(25.9, None), (26.1, "warning"), (49.9, "warning"), (50.1, "critical")],
)
def test_freshness_boundaries(now, age, sev):
    runs = [make_row("silver", age, 1000)]
    got = by_rule(evaluate(runs, cfg(), now)).get(("silver", "freshness_sla"))
    assert (got.severity if got else None) == sev


def test_freshness_reports_threshold_crossed(now):
    (a,) = evaluate([make_row("silver", 60)], cfg(), now)
    assert (a.severity, a.observed, a.threshold) == ("critical", 60.0, 50.0)


def test_expected_table_never_ran_is_critical(now):
    alerts = evaluate([], cfg(expected_tables=["gold"]), now)
    (a,) = alerts
    assert (a.table, a.rule, a.severity, a.observed) == (
        "gold",
        "freshness_sla",
        "critical",
        "never_succeeded",
    )


def test_per_table_override(now):
    c = load_config(None)
    c["tables"] = {
        "streaming": {
            **c["defaults"],
            "freshness": {"warn_after_hours": 0.5, "critical_after_hours": 2},
        }
    }
    runs = [make_row("streaming", 1.0), make_row("silver", 1.0, i=1)]
    got = by_rule(evaluate(runs, c, now))
    assert got[("streaming", "freshness_sla")].severity == "warning"
    assert ("silver", "freshness_sla") not in got


def test_latest_failure_alerts_and_recovery_clears(now):
    runs = history() + [make_row("silver", 0.5, None, status="failed", i=99)]
    got = by_rule(evaluate(runs, cfg(), now))
    assert got[("silver", "run_failed")].severity == "critical"
    assert "boom" in got[("silver", "run_failed")].message
    recovered = runs + [make_row("silver", 0.1, 1000, i=100)]
    assert ("silver", "run_failed") not in by_rule(evaluate(recovered, cfg(), now))


def test_volume_drop_fires_both_rules(now):
    runs = history(values=(1000, 1010, 990, 1005, 995, 300))
    got = by_rule(evaluate(runs, cfg(), now))
    assert got[("silver", "volume_zscore")].severity == "critical"
    d = got[("silver", "volume_drift")]
    assert d.severity == "critical" and d.observed < -60 and d.threshold == 60.0


def test_volume_spike_is_symmetric(now):
    runs = history(values=(1000, 1010, 990, 1005, 995, 1400))
    d = by_rule(evaluate(runs, cfg(), now))[("silver", "volume_drift")]
    assert d.severity == "warning" and d.observed > 30


def test_volume_needs_min_history(now):
    runs = history(values=(1000, 1010, 100))  # only 2 prior points < min_history 5
    assert not [a for a in evaluate(runs, cfg(), now) if a.rule.startswith("volume")]


def test_volume_zero_spread_uses_pct_only(now):
    runs = history(values=(1000, 1000, 1000, 1000, 1000, 100))
    rules = {a.rule for a in evaluate(runs, cfg(), now)}
    assert rules == {"volume_drift"}


def test_volume_ignores_failed_runs_and_null_rows(now):
    runs = history() + [make_row("silver", 0.5, 5, status="failed", i=50)]
    runs.append(make_row("silver", 0.6, None, i=51))
    got = by_rule(evaluate(runs, cfg(), now))
    assert ("silver", "volume_drift") not in got and ("silver", "run_failed") in got


def test_alert_id_is_stable_and_record_schema(now):
    runs = history(values=(1000, 1010, 990, 1005, 995, 300))
    a1, a2 = evaluate(runs, cfg(), now), evaluate(runs, cfg(), now.replace(minute=30))
    assert [a.alert_id for a in a1] == [a.alert_id for a in a2]
    rec = a1[0].to_record()
    assert list(rec) == [
        "schema_version",
        "alert_id",
        "ts",
        "severity",
        "table",
        "rule",
        "observed",
        "threshold",
        "message",
        "run_id",
    ]


def test_alert_rejects_bad_severity(now):
    with pytest.raises(ValueError):
        Alert("fatal", "t", "r", 1, 1, "m", now)


def test_config_validation(tmp_path):
    p = tmp_path / "c.yaml"
    p.write_text("defaults: {freshness: {warn_after_hours: 10, critical_after_hours: 5}}")
    with pytest.raises(ValueError, match="warn_after_hours"):
        load_config(p)
    p.write_text("defaults: {freshness: {warn_after_hours: 1.0e9}}")  # PyYAML parses as str
    with pytest.raises(ValueError, match="must be a number"):
        load_config(p)
    p.write_text("defaults: {volume: {min_history: 1}}")
    with pytest.raises(ValueError, match="min_history"):
        load_config(p)


def test_repo_config_loads():
    from pathlib import Path

    c = load_config(Path(__file__).resolve().parents[2] / "config" / "observability.yaml")
    assert c["sinks"]["webhook"]["enabled"] is False
    assert c["tables"]["streaming"]["volume"]["z_crit"] == 4.5  # inherited default
