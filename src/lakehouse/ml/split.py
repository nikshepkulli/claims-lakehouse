"""Time-based train / calibration / test split with a purge gap.

Why not a random split: claims of one member and provider are correlated over time and the
features are trailing windows, so a random split lets the model train on rows that sit between
its test rows (neighbour leakage) and, worse, measures a world where the future is known. A
deployed model only ever scores claims received *after* everything it was trained on. So blocks
are ordered by ``as_of`` (receipt day): train < calibration < test.

Purge gap: the label of a row becomes known ``purge_days`` (the feature store's
``outcome_lag_days``) after its ``as_of``. Training rows closer than that to the next block would
carry labels that, at the next block's start, were not yet knowable. They are dropped, so every
training label was known before the first row it is evaluated against.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np


class SplitError(ValueError):
    pass


@dataclass(frozen=True)
class Split:
    train: np.ndarray
    cal: np.ndarray
    test: np.ndarray
    cal_start: np.datetime64
    test_start: np.datetime64
    purge_days: int

    def describe(self, as_of: np.ndarray) -> dict:
        def span(ix):
            return {
                "rows": int(len(ix)),
                "as_of_min": str(as_of[ix].min().astype("datetime64[D]")),
                "as_of_max": str(as_of[ix].max().astype("datetime64[D]")),
            }

        return {
            "purge_days": self.purge_days,
            "train": span(self.train),
            "calibration": span(self.cal),
            "test": span(self.test),
            "dropped_by_purge": int(len(as_of) - len(self.train) - len(self.cal) - len(self.test)),
        }


def _quantile_day(days: np.ndarray, frac: float) -> np.datetime64:
    """Earliest day d such that at least ``frac`` of rows have as_of < d."""
    s = np.sort(days)
    k = int(np.ceil(frac * len(s)))
    k = min(max(k, 1), len(s) - 1)
    return s[k]  # first row of the next block; a whole day goes to one side


def time_split(as_of: np.ndarray, train_frac: float, cal_frac: float, purge_days: int) -> Split:
    if purge_days < 0:
        raise SplitError("purge_days must be >= 0")
    days = np.asarray(as_of).astype("datetime64[D]")
    if len(days) < 100:
        raise SplitError(f"only {len(days)} rows; need at least 100 for a three-way time split")
    cal_start = _quantile_day(days, train_frac)
    test_start = _quantile_day(days, train_frac + cal_frac)
    if not cal_start < test_start:
        raise SplitError("as_of spans too few distinct days for the requested fractions")
    gap = np.timedelta64(purge_days, "D")
    train = np.flatnonzero(days < cal_start - gap)
    cal = np.flatnonzero((days >= cal_start) & (days < test_start - gap))
    test = np.flatnonzero(days >= test_start)
    for name, ix in (("train", train), ("calibration", cal), ("test", test)):
        if len(ix) == 0:
            raise SplitError(f"{name} block is empty")
    return Split(train, cal, test, cal_start, test_start, purge_days)


def check_blocks(y: np.ndarray, split: Split, min_pos: int = 20) -> None:
    for name, ix in (("train", split.train), ("calibration", split.cal), ("test", split.test)):
        pos = int(y[ix].sum())
        neg = len(ix) - pos
        if pos < min_pos or neg < min_pos:
            raise SplitError(
                f"{name} block has {pos} positives / {neg} negatives; need >= {min_pos} of each "
                "(generate more rows or widen the fractions)"
            )
