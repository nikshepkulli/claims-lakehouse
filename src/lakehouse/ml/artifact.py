"""The model directory written to ``/opt/ml/model`` and read back by scoring / inference.

    model.joblib       {preprocessor, estimator, calibrator}   (pickle: load only your own models)
    model_meta.json    version, features, feature-manifest hash, split, hyperparameters, metrics

``model_meta.json`` carries the sha256 of ``model.joblib``; ``load`` checks it before unpickling
(tamper evidence for a copy gone wrong, NOT authenticity: pickle can execute code, so never load
a model from a bucket you do not control).
"""

from __future__ import annotations

import hashlib
import json
import tarfile
import tempfile
from dataclasses import dataclass
from pathlib import Path

import joblib
import numpy as np

from .calibrate import Calibrator
from .guard import FeatureSpec, assert_safe
from .models import raw_scores
from .preprocess import Preprocessor

MODEL_FILE = "model.joblib"
META_FILE = "model_meta.json"


@dataclass
class ModelArtifact:
    preprocessor: Preprocessor
    estimator: object
    calibrator: Calibrator
    meta: dict

    @property
    def version(self) -> str:
        return self.meta["model_version"]

    @property
    def specs(self) -> list[FeatureSpec]:
        return self.preprocessor.specs

    def predict(self, cols) -> np.ndarray:
        """Calibrated denial probabilities for columnar input (name -> values)."""
        extra = [c for c in cols if c not in {s.name for s in self.specs}]
        if extra:
            raise ValueError(f"columns not in the model's feature list: {sorted(extra)}")
        return self.calibrator.predict(
            raw_scores(self.estimator, self.preprocessor.transform(cols))
        )


def save(art: ModelArtifact, model_dir: str | Path) -> Path:
    d = Path(model_dir)
    d.mkdir(parents=True, exist_ok=True)
    joblib.dump(
        {
            "preprocessor": art.preprocessor,
            "estimator": art.estimator,
            "calibrator": art.calibrator,
        },
        d / MODEL_FILE,
        compress=3,
    )
    meta = {
        **art.meta,
        "model_file_sha256": hashlib.sha256((d / MODEL_FILE).read_bytes()).hexdigest(),
    }
    (d / META_FILE).write_text(json.dumps(meta, indent=2, sort_keys=True) + "\n")
    return d


def resolve_dir(model_dir: str | Path, scratch: str | Path | None = None) -> Path:
    """``model_dir`` itself, or, if it only holds SageMaker's ``model.tar.gz``, its extraction."""
    d = Path(model_dir)
    if (d / MODEL_FILE).is_file():
        return d
    tgz = d / "model.tar.gz"
    if not tgz.is_file():
        raise FileNotFoundError(f"{d}: neither {MODEL_FILE} nor model.tar.gz found")
    dest = Path(scratch) if scratch else Path(tempfile.mkdtemp(prefix="model-"))
    dest.mkdir(parents=True, exist_ok=True)
    with tarfile.open(tgz) as tf:
        tf.extractall(dest, filter="data")  # refuses absolute paths, links outside, devices
    return dest


def load(model_dir: str | Path) -> ModelArtifact:
    d = resolve_dir(model_dir)
    meta = json.loads((d / META_FILE).read_text())
    got = hashlib.sha256((d / MODEL_FILE).read_bytes()).hexdigest()
    if got != meta.get("model_file_sha256"):
        raise ValueError(f"{d / MODEL_FILE}: sha256 does not match {META_FILE}")
    obj = joblib.load(d / MODEL_FILE)
    art = ModelArtifact(obj["preprocessor"], obj["estimator"], obj["calibrator"], meta)
    assert_safe([s.name for s in art.specs])  # a tampered/old model must not bring PII columns
    return art
