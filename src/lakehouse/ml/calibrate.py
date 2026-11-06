"""Post-hoc probability calibration fitted on the calibration time block.

Class weights (used to train under 8% prevalence) inflate the raw scores, which is fine for
ranking but wrong as probabilities. The calibrator is a monotone map raw score -> probability fit
on rows the model never saw, so AUC-PR / ROC-AUC are unchanged and Brier / calibration improve.
"""

from __future__ import annotations

import numpy as np
from sklearn.isotonic import IsotonicRegression
from sklearn.linear_model import LogisticRegression

_EPS = 1e-6


def _logit(p: np.ndarray) -> np.ndarray:
    p = np.clip(p, _EPS, 1 - _EPS)
    return np.log(p / (1 - p))


class Calibrator:
    def __init__(self, method: str = "sigmoid"):
        if method not in {"sigmoid", "isotonic"}:
            raise ValueError(method)
        self.method = method
        self._m = None

    def fit(self, raw: np.ndarray, y: np.ndarray) -> Calibrator:
        if self.method == "sigmoid":  # Platt scaling on the logit of the raw score
            self._m = LogisticRegression(C=1e6, max_iter=1000).fit(_logit(raw)[:, None], y)
        else:
            self._m = IsotonicRegression(out_of_bounds="clip", y_min=0.0, y_max=1.0).fit(raw, y)
        return self

    def predict(self, raw: np.ndarray) -> np.ndarray:
        if self._m is None:
            raise RuntimeError("calibrator is not fitted")
        if self.method == "sigmoid":
            return self._m.predict_proba(_logit(raw)[:, None])[:, 1]
        return np.asarray(self._m.predict(raw), dtype=np.float64)
