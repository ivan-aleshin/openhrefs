"""Ladder extrapolation: fit a slope + confidence band across slice sizes.

Used to decide the S4-0a -> S4-0b gate: a stable (high-r2) near-linear fit lets the
full-crawl extrapolation be trusted; a low r2 / blow-up at the largest ladder point
halts the full pass (spec: distinct-host/dedup non-linearity stop condition).
"""

from __future__ import annotations

from typing import Any

from scipy import stats


def fit_ladder(points: list[tuple[int, float]]) -> dict[str, Any]:
    """Linear least-squares fit of ``value`` vs ``size`` over ladder points.

    Returns ``slope``, ``intercept``, ``r2``, ``stderr``, a ``ci95`` half-width on
    the slope, and a ``predict(size)`` callable for extrapolation. Needs >= 2 points.
    """
    if len(points) < 2:
        raise ValueError("fit_ladder needs at least two ladder points")
    xs = [float(s) for s, _ in points]
    ys = [float(v) for _, v in points]
    reg = stats.linregress(xs, ys)
    ci95 = 1.96 * reg.stderr

    def predict(size: float) -> float:
        return reg.slope * size + reg.intercept

    return {
        "slope": reg.slope,
        "intercept": reg.intercept,
        "r2": reg.rvalue**2,
        "stderr": reg.stderr,
        "ci95": ci95,
        "predict": predict,
    }
