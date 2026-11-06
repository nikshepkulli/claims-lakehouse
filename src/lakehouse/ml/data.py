"""Load model inputs from the feature store and turn them into numpy.

Training reads ``features.store.training_set()``, the one place features and labels meet. Scoring
reads ``claim_features`` only (no labels). Both are narrowed to the guarded allow-list before
anything else touches them, so ids and PII never reach the model code.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

from lakehouse.features import store

from .guard import LABEL, FeatureSpec


@dataclass
class Frame:
    """Columnar model input. ``cols`` holds float64 arrays (NaN = null) for numeric features and
    object arrays of str/None for categorical ones."""

    claim_id: np.ndarray
    as_of: np.ndarray  # datetime64[us]
    cols: dict[str, np.ndarray]
    y: np.ndarray | None = None  # int8, training only

    def __len__(self) -> int:
        return len(self.claim_id)

    def take(self, idx: np.ndarray) -> Frame:
        return Frame(
            self.claim_id[idx],
            self.as_of[idx],
            {k: v[idx] for k, v in self.cols.items()},
            None if self.y is None else self.y[idx],
        )


def _column(t: pa.Table, spec: FeatureSpec) -> np.ndarray:
    col = t.column(spec.name)
    if spec.kind == "categorical":
        return np.array(col.to_pylist(), dtype=object)
    f = pc.cast(col, pa.float64())
    return pc.fill_null(f, float("nan")).to_numpy(zero_copy_only=False).astype(np.float64)


def frame_from_table(t: pa.Table, specs: list[FeatureSpec], with_label: bool) -> Frame:
    y = None
    if with_label:
        y = np.asarray(pc.cast(t.column(LABEL), pa.int8()).to_numpy(zero_copy_only=False))
    return Frame(
        claim_id=np.array(t.column("claim_id").to_pylist(), dtype=object),
        as_of=np.asarray(t.column("as_of").to_numpy(zero_copy_only=False)).astype("datetime64[us]"),
        cols={s.name: _column(t, s) for s in specs},
        y=y,
    )


def load_training(store_dir: str | Path, specs: list[FeatureSpec]) -> Frame:
    t = store.training_set(store_dir)
    missing = [s.name for s in specs if s.name not in t.column_names]
    if missing:
        raise KeyError(
            f"feature store lacks columns {missing}; rebuild it with the current registry"
        )
    return frame_from_table(t, specs, with_label=True)


def load_features(store_dir: str | Path, specs: list[FeatureSpec]) -> Frame:
    """claim_features only (no label table is read)."""
    con = store.connect()
    store.read_table(con, store_dir, "claim_features")
    t = con.sql("SELECT * FROM claim_features ORDER BY claim_id").to_arrow_table()
    con.close()
    missing = [s.name for s in specs if s.name not in t.column_names]
    if missing:
        raise KeyError(f"feature store lacks columns {missing}; model and store registry differ")
    return frame_from_table(t, specs, with_label=False)
