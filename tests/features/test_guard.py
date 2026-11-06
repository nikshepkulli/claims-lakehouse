"""Leakage test 2: an injected future-leak is caught, by the property test AND by the guard that
fails the build. The leaks are injected by breaking the two point-in-time conditions in the
engine (``lakehouse.features.sql``) the way a careless edit would."""

import pytest
from features_helpers import future_invariance_violations

from lakehouse.features import cli, guards, sql, store
from lakehouse.features.config import FeatureConfig
from lakehouse.features.guards import LeakageError

LEAKS = {
    "no upper bound on history": ("HIST_COND", "TRUE"),
    "peeks 40 days ahead": ("HIST_COND", "c.event_ts <= s.as_of + INTERVAL 40 DAY"),
    "outcome used before its lag": (
        "OUTCOME_COND",
        "c.status IN ('PAID', 'DENIED')",
    ),
    "current dimension version only": ("SCD2_COND", "d.effective_to IS NULL"),
}


def test_guard_passes_on_the_real_engine(gold_export):
    con = store.connect()
    sql.prepare_sources(con, str(gold_export), FeatureConfig())
    summary = guards.run_leakage_guard(con, FeatureConfig(), probes=4)
    assert summary["comparisons"] == 4 * 2 * 3


@pytest.mark.parametrize("leak", LEAKS)
def test_guard_detects_injected_leak(leak, gold_export, monkeypatch):
    name, value = LEAKS[leak]
    monkeypatch.setattr(sql, name, value)
    con = store.connect()
    sql.prepare_sources(con, str(gold_export), FeatureConfig())
    with pytest.raises(LeakageError, match="features at as_of"):
        guards.run_leakage_guard(con, FeatureConfig(), probes=5)


@pytest.mark.parametrize("leak", ["no upper bound on history", "outcome used before its lag"])
def test_property_test_also_detects_injected_leak(leak, tmp_path, monkeypatch):
    name, value = LEAKS[leak]
    monkeypatch.setattr(sql, name, value)
    bad = [v for seed in range(4) for v in future_invariance_violations(seed, tmp_path / str(seed))]
    assert bad, "the property test has no teeth: the leak went unnoticed"


def test_build_fails_and_writes_nothing_when_the_guard_trips(gold_export, tmp_path, monkeypatch):
    monkeypatch.setattr(sql, "HIST_COND", "TRUE")
    out = tmp_path / "store"
    with pytest.raises(LeakageError):
        store.build(gold_export, out)
    assert not out.exists()


def test_cli_exits_nonzero_on_leak(gold_export, tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(sql, "HIST_COND", "TRUE")
    rc = cli.main(["--gold-export", str(gold_export), "--out", str(tmp_path / "s")])
    assert rc == 1
    assert "LEAKAGE GUARD FAILED" in capsys.readouterr().err


def test_label_columns_are_rejected_in_feature_tables():
    guards.assert_label_separation(["claim_id", "billed_amount"])
    for bad in ("is_denied", "claim_status", "total_paid_amount", "Status"):
        with pytest.raises(LeakageError, match="label/outcome"):
            guards.assert_label_separation(["claim_id", bad])


def test_a_label_column_in_the_claim_table_fails_the_build(gold_export, tmp_path, monkeypatch):
    real = sql.build_claim_features

    def leaky(con, n, out, cfg, as_of_filter=None):
        real(con, n, out, cfg, as_of_filter)
        con.execute(
            f"CREATE OR REPLACE TABLE {out} AS SELECT c.*, s.status AS claim_status "
            f"FROM {out} c JOIN src_claims s USING (claim_id)"
        )

    monkeypatch.setattr(store, "build_claim_features", leaky)
    with pytest.raises(LeakageError, match="label/outcome"):
        store.build(gold_export, tmp_path / "s", guard=False)
