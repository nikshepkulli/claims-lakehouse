"""Train the claim-denial-risk classifier (``python -m lakehouse.ml train``).

Flow: guarded feature allow-list -> ``features.store.training_set`` -> time split (train /
calibration / test, purge gap) -> fit logistic baseline and boosted model on train -> pick by
AUC-PR on the *calibration* block (never the test block) -> calibrate on the calibration block ->
evaluate once on the test block -> write model dir + metrics. Deterministic: seeded estimators,
single-threaded BLAS/OpenMP, no wall-clock in any output.
"""

from __future__ import annotations

import hashlib
import json
import platform
import sys
from pathlib import Path

import numpy as np
import sklearn
from sklearn.metrics import average_precision_score
from threadpoolctl import threadpool_limits

from lakehouse.features import registry, store

from . import MODEL_NAME, data, metrics, models
from .artifact import ModelArtifact, save
from .calibrate import Calibrator
from .guard import allow_list
from .hyperparams import Hyperparameters
from .manifest import feature_manifest_sha256, read_store_manifest
from .preprocess import Preprocessor
from .split import check_blocks, time_split

SLICE_COLUMNS = ("claim_type", "place_of_service", "m_plan_type", "p_cost_tier", "p_specialty")


def _version(specs, hp, manifest_sha: str, frame_train: data.Frame) -> str:
    h = hashlib.sha256()
    h.update(json.dumps([s.name for s in specs]).encode())
    h.update(json.dumps(hp.as_dict(), sort_keys=True).encode())
    h.update(manifest_sha.encode())
    h.update("\n".join(frame_train.claim_id).encode())
    h.update(frame_train.y.tobytes())
    h.update(sklearn.__version__.encode())
    return "v" + h.hexdigest()[:12]


def train(
    store_dir: str | Path,
    model_dir: str | Path,
    output_dir: str | Path,
    hp: Hyperparameters | None = None,
) -> dict:
    """Returns the full report (also written to ``output_dir/metrics.json``)."""
    hp = hp or Hyperparameters()
    store_dir = Path(store_dir)
    if hp.verify_inputs:
        store.verify(store_dir)  # re-hash the Parquet against the manifest before trusting it
    manifest = read_store_manifest(store_dir)
    if manifest["registry_sha256"] != registry_hash():
        raise ValueError("feature store was built with a different feature registry; rebuild it")
    specs = allow_list(include_quasi=hp.include_quasi, override=hp.feature_override or None)
    purge = hp.purge_days if hp.purge_days >= 0 else int(manifest["config"]["outcome_lag_days"])

    with threadpool_limits(limits=1):  # deterministic float summation order
        frame = data.load_training(store_dir, specs)
        split = time_split(frame.as_of, hp.train_frac, hp.cal_frac, purge)
        check_blocks(frame.y, split)
        tr, ca, te = (frame.take(ix) for ix in (split.train, split.cal, split.test))

        pre = Preprocessor(specs).fit(tr.cols)
        Xtr, Xca, Xte = (pre.transform(f.cols) for f in (tr, ca, te))
        candidates = models.CANDIDATES if hp.model == "auto" else (hp.model,)
        fitted, cal_ap = {}, {}
        for name in candidates:
            est = models.build(name, pre, hp).fit(Xtr, tr.y)
            fitted[name] = est
            cal_ap[name] = float(average_precision_score(ca.y, models.raw_scores(est, Xca)))
        chosen = max(candidates, key=lambda n: (cal_ap[n], n == "logreg"))

        def segs(f: data.Frame) -> dict:
            return {c: f.cols[c] for c in SLICE_COLUMNS if c in f.cols}

        report_models = {}
        for name, est in fitted.items():
            cal = Calibrator(hp.calibration).fit(models.raw_scores(est, Xca), ca.y)
            p_te = cal.predict(models.raw_scores(est, Xte))
            report_models[name] = {
                "calibration_block_auc_pr_raw": round(cal_ap[name], 6),
                "test": metrics.evaluate(te.y, p_te, segs(te), hp.min_segment_rows),
            }
            if name == chosen:
                chosen_cal = cal
        # no-skill reference: constant train prevalence
        prev = float(tr.y.mean())
        ref = {
            "constant_prevalence_brier_on_test": round(float(np.mean((te.y - prev) ** 2)), 6),
            "constant_prevalence_auc_pr_on_test": round(float(te.y.mean()), 6),
        }

    manifest_sha = feature_manifest_sha256(store_dir)
    version = _version(specs, hp, manifest_sha, tr)
    meta = {
        "model_name": MODEL_NAME,
        "model_version": version,
        "algorithm": chosen,
        "label": "is_denied",
        "features": [
            {"name": s.name, "kind": s.kind, "outcome_dependent": s.outcome_dependent}
            for s in specs
        ],
        "feature_manifest_sha256": manifest_sha,
        "registry_sha256": manifest["registry_sha256"],
        "split": split.describe(frame.as_of),
        "hyperparameters": hp.as_dict(),
        "headline_test_metrics": {
            k: report_models[chosen]["test"][k]
            for k in ("auc_pr", "roc_auc", "brier", "prevalence")
        },
        "versions": {
            "python": platform.python_version(),
            "scikit_learn": sklearn.__version__,
            "numpy": np.__version__,
        },
        "data_is_synthetic": True,
    }
    art = ModelArtifact(pre, fitted[chosen], chosen_cal, meta)
    save(art, model_dir)

    report = {
        "model_name": MODEL_NAME,
        "model_version": version,
        "chosen": chosen,
        "selection": "highest raw AUC-PR on the calibration block; test block untouched until then",
        "split": meta["split"],
        "models": report_models,
        "reference": ref,
        "interpretation": interpret(report_models[chosen]["test"]),
        "feature_manifest_sha256": manifest_sha,
        "data_is_synthetic": True,
    }
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    (out / "metrics.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return report


def interpret(test: dict) -> str:
    """One honest sentence next to the numbers (a ROC-AUC near 0.5 is no skill)."""
    auc = test["roc_auc"]
    if auc is not None and abs(auc - 0.5) < 0.03:
        return (
            "NO DETECTABLE SIGNAL: test ROC-AUC is within 0.03 of 0.5 (chance). Expected on the "
            "claimsgen data, where denials are drawn i.i.d. at 8% independent of every feature; "
            "the pipeline is verified mechanically, not as a predictor."
        )
    return "Test ROC-AUC differs from chance; still synthetic data, not a real-world estimate."


def registry_hash() -> str:
    return hashlib.sha256(registry.registry_path().read_bytes()).hexdigest()


def sagemaker_metric_lines(report: dict) -> list[str]:
    """Lines a SageMaker training job's ``MetricDefinitions`` regexes can scrape from stdout."""
    t = report["models"][report["chosen"]]["test"]
    return [
        f"test:auc_pr={t['auc_pr']};",
        f"test:roc_auc={t['roc_auc']};",
        f"test:brier={t['brier']};",
        f"test:ece={t['calibration']['ece']};",
    ]


def print_report(report: dict, file=None) -> None:
    file = file or sys.stdout
    c = report["chosen"]
    print(f"model {report['model_name']} {report['model_version']} (chosen: {c})", file=file)
    for name, m in report["models"].items():
        t = m["test"]
        print(
            f"  {name:7s} test: AUC-PR={t['auc_pr']} ROC-AUC={t['roc_auc']} Brier={t['brier']} "
            f"ECE={t['calibration']['ece']} prevalence={t['prevalence']}",
            file=file,
        )
    print(f"  note: {report['interpretation']}", file=file)
    for line in sagemaker_metric_lines(report):
        print(line, file=file)
