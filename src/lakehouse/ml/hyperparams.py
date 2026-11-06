"""Typed hyperparameters with SageMaker string coercion.

SageMaker delivers every hyperparameter as a string (``hyperparameters.json``), sometimes
JSON-encoded (``"\\"abc\\""``). ``Hyperparameters.from_mapping`` coerces by field type, ignores the
platform's reserved ``sagemaker_*`` keys and rejects anything else (a typo must not silently
train with defaults).
"""

from __future__ import annotations

import json
from dataclasses import dataclass, fields


@dataclass(frozen=True)
class Hyperparameters:
    seed: int = 42
    model: str = "auto"  # auto | logreg | hgb ; auto picks by AUC-PR on the calibration block
    train_frac: float = 0.6
    cal_frac: float = 0.2
    purge_days: int = -1  # -1: use the feature store's outcome_lag_days
    calibration: str = "sigmoid"  # sigmoid | isotonic
    lr_c: float = 1.0
    hgb_max_iter: int = 200
    hgb_learning_rate: float = 0.05
    hgb_max_depth: int = 4
    include_quasi: bool = False
    features: str = ""  # comma separated override of the allow-list (still guarded)
    min_segment_rows: int = 200
    verify_inputs: bool = True

    def __post_init__(self) -> None:
        if self.model not in {"auto", "logreg", "hgb"}:
            raise ValueError(f"model must be auto|logreg|hgb, got {self.model!r}")
        if self.calibration not in {"sigmoid", "isotonic"}:
            raise ValueError(f"calibration must be sigmoid|isotonic, got {self.calibration!r}")
        if not (0.2 <= self.train_frac < 0.9 and 0.05 <= self.cal_frac < 0.5):
            raise ValueError("train_frac in [0.2, 0.9) and cal_frac in [0.05, 0.5) required")
        if self.train_frac + self.cal_frac >= 0.95:
            raise ValueError("train_frac + cal_frac must leave at least 5% for the test block")

    @property
    def feature_override(self) -> list[str]:
        return [f.strip() for f in self.features.split(",") if f.strip()]

    @classmethod
    def from_mapping(cls, raw: dict) -> Hyperparameters:
        types = {f.name: f.type for f in fields(cls)}
        out = {}
        for k, v in raw.items():
            if k.startswith("sagemaker_") or k.startswith("_tuning"):
                continue
            if k not in types:
                raise ValueError(f"unknown hyperparameter {k!r}; known: {sorted(types)}")
            out[k] = _coerce(k, v, types[k])
        return cls(**out)

    def as_dict(self) -> dict:
        return {f.name: getattr(self, f.name) for f in fields(self)}


def _coerce(name: str, v, typ):
    typ = typ if isinstance(typ, str) else typ.__name__
    if isinstance(v, str):
        s = v.strip()
        if typ == "str":
            try:
                dec = json.loads(s)
                return dec if isinstance(dec, str) else s
            except ValueError:
                return s
        if typ == "bool":
            if s.lower() in {"true", "1", "yes"}:
                return True
            if s.lower() in {"false", "0", "no"}:
                return False
            raise ValueError(f"hyperparameter {name}: not a boolean: {v!r}")
        try:
            return int(json.loads(s)) if typ == "int" else float(json.loads(s))
        except ValueError as e:
            raise ValueError(f"hyperparameter {name}: not {typ}: {v!r}") from e
    if typ == "bool":
        return bool(v)
    return {"int": int, "float": float, "str": str}[typ](v)
