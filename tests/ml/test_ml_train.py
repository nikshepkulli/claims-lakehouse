import json

import numpy as np
import pytest

from lakehouse.ml import artifact, guard
from lakehouse.ml.hyperparams import Hyperparameters
from lakehouse.ml.train import train


def test_trained_model_learns_a_planted_signal(trained):
    _, report = trained
    t = report["models"][report["chosen"]]["test"]
    assert t["roc_auc"] > 0.8 and t["auc_pr"] > 2 * t["prevalence"]
    assert t["brier"] < t["brier_prevalence_reference"]
    assert t["calibration"]["ece"] < 0.05
    lift = t["precision_at_k"][1]["lift"]
    assert lift > 2
    assert "claim_type" in t["slices"] and "m_plan_type" in t["slices"]


def test_both_candidates_reported_and_selection_uses_calibration_block(trained):
    _, report = trained
    assert set(report["models"]) == {"logreg", "hgb"}
    raw = {k: v["calibration_block_auc_pr_raw"] for k, v in report["models"].items()}
    best = max(raw, key=raw.get)
    assert report["chosen"] == best or raw[best] == raw[report["chosen"]]
    assert "calibration block" in report["selection"]


def test_split_in_report_is_time_ordered_with_purge(trained):
    _, report = trained
    s = report["split"]
    assert s["train"]["as_of_max"] < s["calibration"]["as_of_min"]
    assert s["calibration"]["as_of_max"] < s["test"]["as_of_min"]
    assert s["purge_days"] == 30 and s["dropped_by_purge"] > 0


def test_model_features_come_from_guarded_allow_list(trained):
    model_dir, _ = trained
    meta = json.loads((model_dir / "model_meta.json").read_text())
    names = [f["name"] for f in meta["features"]]
    guard.assert_safe(names)
    assert names == [s.name for s in guard.allow_list()]
    assert meta["data_is_synthetic"] is True
    assert meta["model_version"].startswith("v") and len(meta["model_version"]) == 13


def test_training_is_deterministic(signal_store, tmp_path):
    hp = Hyperparameters(verify_inputs=False, hgb_max_iter=40)
    a = train(signal_store, tmp_path / "m1", tmp_path / "o1", hp)
    b = train(signal_store, tmp_path / "m2", tmp_path / "o2", hp)
    assert (tmp_path / "o1" / "metrics.json").read_bytes() == (
        tmp_path / "o2" / "metrics.json"
    ).read_bytes()
    assert a["model_version"] == b["model_version"]
    # different seed -> different version (the version is a real fingerprint)
    c = train(
        signal_store,
        tmp_path / "m3",
        tmp_path / "o3",
        Hyperparameters(seed=1, verify_inputs=False, hgb_max_iter=40),
    )
    assert c["model_version"] != a["model_version"]


def test_null_result_on_iid_labels_is_called_out(feature_store, tmp_path):
    """The claimsgen labels are iid: the pipeline must run end to end and say 'no signal'."""
    r = train(feature_store, tmp_path / "m", tmp_path / "o", Hyperparameters(hgb_max_iter=30))
    t = r["models"][r["chosen"]]["test"]
    assert abs(t["roc_auc"] - 0.5) < 0.1
    assert "NO DETECTABLE SIGNAL" in r["interpretation"] or t["roc_auc"] > 0.53


def test_store_integrity_is_verified_before_training(signal_store, tmp_path):
    with pytest.raises(ValueError, match="manifest hash"):
        train(signal_store, tmp_path / "m", tmp_path / "o", Hyperparameters(verify_inputs=True))


def test_feature_override_goes_through_the_guard(signal_store, tmp_path):
    with pytest.raises(guard.FeatureGuardError):
        train(signal_store, tmp_path / "m", tmp_path / "o",
              Hyperparameters(verify_inputs=False, features="billed_amount,member_id"))  # fmt: skip


def test_tampered_model_file_is_refused(trained, tmp_path):
    import shutil

    d = tmp_path / "copy"
    shutil.copytree(trained[0], d)
    (d / "model.joblib").write_bytes((d / "model.joblib").read_bytes() + b"x")
    with pytest.raises(ValueError, match="sha256"):
        artifact.load(d)


def test_artifact_roundtrip_predicts_identically(trained, signal_store):
    from lakehouse.ml import data

    art = artifact.load(trained[0])
    f = data.load_features(signal_store, art.specs)
    p = art.predict(f.cols)
    assert p.shape == (len(f),) and np.isfinite(p).all() and p.min() >= 0 and p.max() <= 1
    assert np.array_equal(p, artifact.load(trained[0]).predict(f.cols))
