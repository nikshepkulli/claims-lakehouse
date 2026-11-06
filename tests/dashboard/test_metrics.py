"""Unit tests for the pure helpers (no DuckDB needed)."""

import math

import pytest

from dashboard import metrics


def test_linear_trend_exact_line():
    t = metrics.linear_trend([1, 3, 5, 7])
    assert t["slope"] == pytest.approx(2)
    assert t["intercept"] == pytest.approx(1)
    assert t["r2"] == pytest.approx(1)
    assert t["fitted"] == pytest.approx([1, 3, 5, 7])


def test_linear_trend_flat_and_short():
    assert metrics.linear_trend([4, 4, 4])["r2"] == 0.0
    with pytest.raises(ValueError):
        metrics.linear_trend([1])


def test_wilson_interval_known_values():
    lo, hi = metrics.wilson_interval(8, 100)
    assert lo == pytest.approx(0.0412, abs=1e-3)
    assert hi == pytest.approx(0.1496, abs=1e-3)
    assert metrics.wilson_interval(0, 50)[0] == 0.0
    assert metrics.wilson_interval(0, 0) == (0.0, 0.0)
    lo, hi = metrics.wilson_interval(50, 50)
    assert hi == 1.0 and lo < 1.0


def test_wilson_interval_narrows_with_n():
    narrow = metrics.wilson_interval(80, 1000)
    wide = metrics.wilson_interval(8, 100)
    assert narrow[1] - narrow[0] < wide[1] - wide[0]


def test_gini_equal_and_extreme():
    equal = [(i / 100, i / 100) for i in range(101)]
    assert metrics.gini_from_lorenz(equal) == pytest.approx(0, abs=1e-12)
    one_holds_all = [(i / 100, 0.0) for i in range(100)] + [(1.0, 1.0)]
    assert metrics.gini_from_lorenz(one_holds_all) == pytest.approx(0.99, abs=1e-9)
    # missing origin is added
    assert metrics.gini_from_lorenz(equal[1:]) == pytest.approx(0, abs=1e-12)


def test_top_share():
    pts = [(i / 100, (i / 100) ** 2) for i in range(101)]
    assert metrics.top_share(pts, 10) == pytest.approx(1 - 0.81)
    assert metrics.top_share(pts, 100) == pytest.approx(1.0)
    # interpolates between grid points (needed when there are fewer than 100 members)
    assert metrics.top_share(pts, 10.5) == pytest.approx(1 - 0.895**2, abs=1e-3)
    with pytest.raises(ValueError):
        metrics.top_share([(0.0, 0.0), (0.5, 0.1)], 10)


def test_pct_zero_denominator():
    assert metrics.pct(1, 0) is None
    assert math.isclose(metrics.pct(1, 4), 0.25)
