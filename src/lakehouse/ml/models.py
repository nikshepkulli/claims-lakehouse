"""Candidate estimators: an interpretable baseline and one stronger model.

Both handle the ~8% positive rate with ``class_weight="balanced"`` during fitting (ranking
quality), then ``calibrate.Calibrator`` restores usable probabilities. All randomness is seeded
and ``early_stopping=False`` so the boosted model does not draw a random validation split.
"""

from __future__ import annotations

import numpy as np
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

from .hyperparams import Hyperparameters
from .preprocess import Preprocessor

CANDIDATES = ("logreg", "hgb")


def build(name: str, pre: Preprocessor, hp: Hyperparameters):
    if name == "logreg":
        n_num = len(pre.numeric)
        n_all = len(pre.names)
        ct = ColumnTransformer(
            [
                (
                    "num",
                    Pipeline(
                        [
                            ("imp", SimpleImputer(strategy="median", add_indicator=True)),
                            ("sc", StandardScaler()),
                        ]
                    ),
                    list(range(n_num)),
                ),
                (
                    "cat",
                    Pipeline(
                        [
                            ("imp", SimpleImputer(strategy="constant", fill_value=-1.0)),
                            ("oh", OneHotEncoder(handle_unknown="ignore")),
                        ]
                    ),
                    list(range(n_num, n_all)),
                ),
            ]
        )
        return Pipeline(
            [
                ("prep", ct),
                (
                    "clf",
                    LogisticRegression(
                        C=hp.lr_c, class_weight="balanced", max_iter=2000, random_state=hp.seed
                    ),
                ),
            ]
        )
    if name == "hgb":
        return HistGradientBoostingClassifier(
            max_iter=hp.hgb_max_iter,
            learning_rate=hp.hgb_learning_rate,
            max_depth=hp.hgb_max_depth,
            l2_regularization=1.0,
            categorical_features=pre.categorical_mask if pre.categorical else None,
            class_weight="balanced",
            early_stopping=False,
            random_state=hp.seed,
        )
    raise ValueError(f"unknown model {name!r}")


def raw_scores(estimator, X: np.ndarray) -> np.ndarray:
    return np.asarray(estimator.predict_proba(X)[:, 1], dtype=np.float64)
