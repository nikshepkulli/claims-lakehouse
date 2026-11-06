"""Evaluation metrics: ranking (AUC-PR, ROC-AUC), calibration (Brier, table, ECE),
operating-point precision@k with lift, and per-segment slices."""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence

import numpy as np
from sklearn.metrics import average_precision_score, brier_score_loss, roc_auc_score

K_FRACTIONS = (0.01, 0.05, 0.10)


def _r(x: float | None, nd: int = 6) -> float | None:
    return None if x is None or (isinstance(x, float) and math.isnan(x)) else round(float(x), nd)


def _both(y: np.ndarray) -> bool:
    return 0 < int(y.sum()) < len(y)


def ranking(y: np.ndarray, p: np.ndarray) -> dict:
    if not _both(y):
        return {"auc_pr": None, "roc_auc": None}
    return {
        "auc_pr": _r(average_precision_score(y, p)),
        "roc_auc": _r(roc_auc_score(y, p)),
    }


def precision_at_k(
    y: np.ndarray, p: np.ndarray, fracs: Sequence[float] = K_FRACTIONS
) -> list[dict]:
    """Top-k by score (ties broken by row order, so deterministic). k = ceil(frac * n)."""
    order = np.argsort(-p, kind="stable")
    base = float(y.mean())
    out = []
    for f in fracs:
        k = max(1, math.ceil(f * len(y)))
        prec = float(y[order[:k]].mean())
        out.append(
            {
                "k_fraction": f,
                "k": k,
                "precision": _r(prec),
                "recall": _r(float(y[order[:k]].sum()) / max(1, int(y.sum()))),
                "lift": _r(prec / base) if base > 0 else None,
            }
        )
    return out


def calibration_table(y: np.ndarray, p: np.ndarray, bins: int = 10) -> dict:
    """Equal-frequency bins on the predicted probability; ECE = weighted |mean_pred - observed|."""
    order = np.argsort(p, kind="stable")
    rows, ece = [], 0.0
    for i, ix in enumerate(np.array_split(order, bins)):
        if len(ix) == 0:
            continue
        mp, ob = float(p[ix].mean()), float(y[ix].mean())
        ece += len(ix) / len(y) * abs(mp - ob)
        rows.append({"bin": i + 1, "n": int(len(ix)), "mean_pred": _r(mp), "observed": _r(ob)})
    return {"bins": rows, "ece": _r(ece)}


def evaluate(
    y: np.ndarray,
    p: np.ndarray,
    segments: Mapping[str, np.ndarray] | None = None,
    min_segment_rows: int = 200,
) -> dict:
    y = np.asarray(y)
    p = np.asarray(p, dtype=np.float64)
    out = {
        "n": int(len(y)),
        "positives": int(y.sum()),
        "prevalence": _r(float(y.mean())),
        "mean_prediction": _r(float(p.mean())),
        **ranking(y, p),
        "brier": _r(brier_score_loss(y, np.clip(p, 0.0, 1.0))),
        # Brier of always predicting the block's own prevalence (the no-skill reference)
        "brier_prevalence_reference": _r(float(y.mean() * (1 - y.mean()))),
        "precision_at_k": precision_at_k(y, p),
        "calibration": calibration_table(y, p),
    }
    slices: dict[str, list[dict]] = {}
    for seg, values in (segments or {}).items():
        rows = []
        for v in sorted({str(x) for x in values}):
            m = np.array([str(x) == v for x in values])
            n = int(m.sum())
            if n < min_segment_rows:
                rows.append({"value": v, "n": n, "note": f"< {min_segment_rows} rows, not scored"})
                continue
            rows.append(
                {
                    "value": v,
                    "n": n,
                    "prevalence": _r(float(y[m].mean())),
                    "mean_prediction": _r(float(p[m].mean())),
                    **ranking(y[m], p[m]),
                }
            )
        slices[seg] = rows
    out["slices"] = slices
    return out
