"""Feature matrix construction shared by training, scoring and the inference handler.

One class, fitted on the training block only: numeric features pass through as float64 (NaN =
null), categorical features become integer codes from a vocabulary learnt on train (unseen or
null -> NaN, i.e. "missing"). Both estimators consume the same matrix; the logistic baseline
one-hot encodes the categorical columns, the boosted model uses them as native categoricals.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence

import numpy as np

from .guard import FeatureSpec

MAX_VOCAB = 200  # HistGradientBoosting supports <= 255 categories per feature


def to_float(values: Sequence | np.ndarray, name: str) -> np.ndarray:
    if isinstance(values, np.ndarray) and values.dtype.kind == "f":
        return values.astype(np.float64, copy=False)
    out = np.empty(len(values), dtype=np.float64)
    for i, v in enumerate(values):
        if v is None or v == "":
            out[i] = np.nan
        elif isinstance(v, bool | int | float | np.number | np.bool_):
            out[i] = float(v)
        else:
            try:
                out[i] = float(v)
            except (TypeError, ValueError) as e:
                raise ValueError(f"feature {name}: row {i}: {v!r} is not numeric") from e
    return out


class Preprocessor:
    def __init__(self, specs: list[FeatureSpec]):
        self.specs = list(specs)
        self.numeric = [s.name for s in specs if s.kind == "numeric"]
        self.categorical = [s.name for s in specs if s.kind == "categorical"]
        self.vocab: dict[str, list[str]] = {}

    @property
    def names(self) -> list[str]:
        """Matrix column order: numeric features, then categorical codes."""
        return self.numeric + self.categorical

    @property
    def categorical_mask(self) -> np.ndarray:
        return np.array([False] * len(self.numeric) + [True] * len(self.categorical))

    def fit(self, cols: Mapping[str, Sequence]) -> Preprocessor:
        for c in self.categorical:
            vals, counts = np.unique([v for v in cols[c] if v is not None], return_counts=True)
            order = sorted(range(len(vals)), key=lambda i: (-counts[i], vals[i]))[:MAX_VOCAB]
            self.vocab[c] = sorted(str(vals[i]) for i in order)
        return self

    def transform(self, cols: Mapping[str, Sequence]) -> np.ndarray:
        n = len(next(iter(cols.values()))) if cols else 0
        parts = []
        for c in self.numeric:
            parts.append(to_float(cols[c], c) if c in cols else np.full(n, np.nan))
        for c in self.categorical:
            index = {v: float(i) for i, v in enumerate(self.vocab[c])}
            src = cols.get(c)
            parts.append(
                np.array(
                    [index.get(str(v), np.nan) if v is not None else np.nan for v in src]
                    if src is not None
                    else [np.nan] * n,
                    dtype=np.float64,
                )
            )
        return np.column_stack(parts) if parts else np.empty((n, 0))
