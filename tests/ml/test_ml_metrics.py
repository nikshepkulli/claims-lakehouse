import numpy as np
import pytest
from sklearn.metrics import average_precision_score, roc_auc_score

from lakehouse.ml import metrics


def test_perfect_and_inverted_ranking():
    y = np.array([0, 0, 0, 0, 1, 1])
    p = np.array([0.1, 0.2, 0.15, 0.3, 0.9, 0.8])
    r = metrics.ranking(y, p)
    assert r["auc_pr"] == 1.0 and r["roc_auc"] == 1.0
    assert metrics.ranking(y, 1 - p)["roc_auc"] == 0.0


def test_matches_sklearn_and_hand_brier():
    rng = np.random.default_rng(1)
    y = (rng.random(500) < 0.2).astype(int)
    p = np.clip(0.2 + 0.3 * y + rng.normal(0, 0.15, 500), 0, 1)
    m = metrics.evaluate(y, p)
    assert m["auc_pr"] == pytest.approx(average_precision_score(y, p), abs=1e-6)
    assert m["roc_auc"] == pytest.approx(roc_auc_score(y, p), abs=1e-6)
    assert m["brier"] == pytest.approx(float(np.mean((p - y) ** 2)), abs=1e-6)
    assert m["brier_prevalence_reference"] == pytest.approx(y.mean() * (1 - y.mean()), abs=1e-6)


def test_precision_at_k_by_hand_and_tie_break_is_stable():
    y = np.array([1, 0, 1, 0, 0, 0, 0, 0, 0, 0])
    p = np.array([0.9, 0.8, 0.7, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1])
    r = metrics.precision_at_k(y, p, fracs=(0.1, 0.2, 0.3))
    assert [x["k"] for x in r] == [1, 2, 3]
    assert [x["precision"] for x in r] == [1.0, 0.5, pytest.approx(0.666667, abs=1e-6)]
    assert r[2]["recall"] == 1.0
    assert r[0]["lift"] == pytest.approx(1.0 / 0.2)
    assert metrics.precision_at_k(y, p) == metrics.precision_at_k(y, p)


def test_calibration_table_of_a_calibrated_and_a_miscalibrated_model():
    rng = np.random.default_rng(2)
    p = rng.random(20000) * 0.4
    y = (rng.random(20000) < p).astype(int)
    good = metrics.calibration_table(y, p)
    assert len(good["bins"]) == 10 and sum(b["n"] for b in good["bins"]) == 20000
    assert good["ece"] < 0.02
    assert metrics.calibration_table(y, np.clip(p * 2, 0, 1))["ece"] > 0.1


def test_single_class_block_gives_none_not_an_exception():
    y = np.zeros(300, dtype=int)
    m = metrics.evaluate(y, np.linspace(0, 1, 300))
    assert m["auc_pr"] is None and m["roc_auc"] is None


def test_segment_slices_report_small_segments_without_scoring_them():
    rng = np.random.default_rng(3)
    n = 1000
    seg = np.array(["A"] * 900 + ["B"] * 100, dtype=object)
    y = (rng.random(n) < 0.3).astype(int)
    p = np.where(y == 1, 0.7, 0.3) + rng.normal(0, 0.1, n)
    m = metrics.evaluate(y, p, {"grp": seg}, min_segment_rows=200)
    a, b = m["slices"]["grp"]
    assert a["value"] == "A" and a["roc_auc"] > 0.9 and a["n"] == 900
    assert b["n"] == 100 and "roc_auc" not in b and "not scored" in b["note"]
