import numpy as np
import pytest

from lakehouse.ml.split import SplitError, check_blocks, time_split


def _days(n_days=200, per_day=10):
    base = np.datetime64("2024-01-01")
    return np.repeat(base + np.arange(n_days), per_day).astype("datetime64[us]")


def test_blocks_are_ordered_disjoint_and_purged():
    as_of = _days()
    s = time_split(as_of, 0.6, 0.2, purge_days=30)
    d = as_of.astype("datetime64[D]")
    assert not set(s.train) & set(s.cal) and not set(s.cal) & set(s.test)
    gap = np.timedelta64(30, "D")
    # every train row is >= purge before the first calibration row, and cal before test
    assert d[s.train].max() + gap < d[s.cal].min()
    assert d[s.cal].max() + gap < d[s.test].min()
    assert d[s.train].max() < d[s.cal].min() < d[s.cal].max() < d[s.test].min()


def test_no_future_label_leaks_across_the_boundary():
    """A label is known at as_of + purge. Every training label must be known before the first
    calibration/test row is scored, i.e. nothing in train 'sees' a later block's period."""
    as_of = _days(300, 5)
    lag = 21
    s = time_split(as_of, 0.5, 0.25, purge_days=lag)
    d = as_of.astype("datetime64[D]")
    known_at = d + np.timedelta64(lag, "D")
    assert known_at[s.train].max() < d[s.cal].min()
    assert known_at[s.cal].max() < d[s.test].min()


def test_split_is_a_function_of_time_not_row_order():
    as_of = _days(120, 8)
    perm = np.random.default_rng(0).permutation(len(as_of))
    a = time_split(as_of, 0.6, 0.2, 14)
    b = time_split(as_of[perm], 0.6, 0.2, 14)
    ids = np.arange(len(as_of))
    assert set(ids[a.train]) == set(ids[perm][b.train])
    assert set(ids[a.test]) == set(ids[perm][b.test])


def test_a_whole_day_never_straddles_a_boundary():
    as_of = _days(100, 7)
    s = time_split(as_of, 0.6, 0.2, 0)
    d = as_of.astype("datetime64[D]")
    for ix in (s.train, s.cal, s.test):
        for other in (s.train, s.cal, s.test):
            if ix is not other:
                assert not set(d[ix]) & set(d[other])


def test_rejects_degenerate_inputs():
    with pytest.raises(SplitError):
        time_split(_days(2, 10), 0.6, 0.2, 0)
    with pytest.raises(SplitError):
        time_split(_days(), 0.6, 0.2, -1)
    one_day = np.full(500, np.datetime64("2024-01-01"), dtype="datetime64[us]")
    with pytest.raises(SplitError):
        time_split(one_day, 0.6, 0.2, 0)


def test_check_blocks_demands_both_classes():
    as_of = _days()
    s = time_split(as_of, 0.6, 0.2, 0)
    y = np.zeros(len(as_of), dtype=np.int8)
    with pytest.raises(SplitError):
        check_blocks(y, s)
    y[::7] = 1
    check_blocks(y, s)
