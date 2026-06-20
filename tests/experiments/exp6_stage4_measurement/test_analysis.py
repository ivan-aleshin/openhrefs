"""Unit tests for experiments.exp6_stage4_measurement.analysis."""

from experiments.exp6_stage4_measurement.analysis import fit_ladder


def test_fit_ladder_recovers_linear_slope() -> None:
    # value = 100 * size exactly -> slope 100, intercept 0, r2 1.
    points = [(200, 20_000.0), (1000, 100_000.0), (3000, 300_000.0)]
    fit = fit_ladder(points)
    assert abs(fit["slope"] - 100.0) < 1e-6
    assert abs(fit["intercept"]) < 1e-6
    assert fit["r2"] > 0.999
    # extrapolation to a full 100k-file crawl
    assert abs(fit["predict"](100_000) - 10_000_000.0) < 1.0


def test_fit_ladder_flags_nonlinearity_with_low_r2() -> None:
    points = [(200, 1.0), (1000, 5.0), (3000, 9000.0)]  # super-linear blow-up
    fit = fit_ladder(points)
    assert fit["r2"] < 0.95
