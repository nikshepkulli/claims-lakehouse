"""Pure helpers for the dashboard: no I/O, no DuckDB, so they unit-test anywhere.

All inputs are plain Python numbers produced by the SQL in ``analytics/`` and
``dashboard/queries/``. All data is synthetic.
"""

from __future__ import annotations

import math
from collections.abc import Sequence


def linear_trend(values: Sequence[float]) -> dict[str, float]:
    """Ordinary least squares of ``values`` on 0..n-1.

    Returns slope per step, intercept, r2 and the fitted values. Needs >= 2 points.
    """
    n = len(values)
    if n < 2:
        raise ValueError("need at least two points for a trend")
    xs = range(n)
    mx = (n - 1) / 2
    my = sum(values) / n
    sxx = sum((x - mx) ** 2 for x in xs)
    sxy = sum((x - mx) * (y - my) for x, y in zip(xs, values, strict=True))
    slope = sxy / sxx
    intercept = my - slope * mx
    fitted = [intercept + slope * x for x in xs]
    ss_tot = sum((y - my) ** 2 for y in values)
    ss_res = sum((y - f) ** 2 for y, f in zip(values, fitted, strict=True))
    r2 = 1 - ss_res / ss_tot if ss_tot else 0.0
    return {"slope": slope, "intercept": intercept, "r2": r2, "fitted": fitted}


def wilson_interval(successes: int, n: int, z: float = 1.96) -> tuple[float, float]:
    """Wilson score interval for a proportion (95 percent by default).

    Preferred over the normal approximation because denial counts per cell are small.
    """
    if n <= 0:
        return (0.0, 0.0)
    p = successes / n
    denom = 1 + z * z / n
    centre = (p + z * z / (2 * n)) / denom
    half = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / denom
    return (max(0.0, centre - half), min(1.0, centre + half))


def gini_from_lorenz(points: Sequence[tuple[float, float]]) -> float:
    """Gini coefficient from Lorenz points ``(member_share, cum_paid_share)`` in 0..1.

    ``(0, 0)`` is prepended when missing. Trapezoid area under the curve; Gini = 1 - 2 * area.
    """
    pts = list(points)
    if not pts or pts[0] != (0.0, 0.0):
        pts = [(0.0, 0.0), *pts]
    area = sum((x1 - x0) * (y0 + y1) / 2 for (x0, y0), (x1, y1) in zip(pts, pts[1:], strict=False))
    return 1 - 2 * area


def top_share(points: Sequence[tuple[float, float]], top_pct: float) -> float:
    """Share of total paid held by the top ``top_pct`` percent of members.

    ``points`` are Lorenz points ``(member_share 0..1, cum_paid_share 0..1)`` ascending. The
    cumulative share at ``1 - top_pct/100`` is linearly interpolated, so it also works when
    fewer than 100 members leave gaps in the percentile grid.
    """
    cutoff = 1 - top_pct / 100
    if cutoff <= 0:
        return 1.0
    pts = [(0.0, 0.0), *points] if points and points[0] != (0.0, 0.0) else list(points)
    for (x0, y0), (x1, y1) in zip(pts, pts[1:], strict=False):
        if x0 <= cutoff <= x1:
            if x1 == x0:
                return 1 - y1
            return 1 - (y0 + (y1 - y0) * (cutoff - x0) / (x1 - x0))
    raise ValueError(f"Lorenz points do not cover {cutoff:.2f}")


def pct(num: float, den: float) -> float | None:
    """Ratio as a fraction, or None when the denominator is zero."""
    return None if not den else num / den
