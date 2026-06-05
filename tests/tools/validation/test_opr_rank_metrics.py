"""Unit tests for the off-cluster OPR rank-metric profile."""

from __future__ import annotations

import pandas as pd
import pytest

from tools.validation.opr_rank_metrics import compute_metrics, rank_biased_overlap


def test_rbo_identical_lists_hits_closed_form() -> None:
    # Truncated RBO over two identical prefixes is 1 - p**depth, not 1.0 — it only
    # reaches 1.0 as depth → ∞. Here depth caps at the list length (10).
    items = [f"d{i}" for i in range(10)]
    assert rank_biased_overlap(items, items, p=0.9, depth=10) == pytest.approx(1 - 0.9**10)


def test_rbo_disjoint_lists_is_zero() -> None:
    left = [f"l{i}" for i in range(10)]
    right = [f"r{i}" for i in range(10)]
    assert rank_biased_overlap(left, right, p=0.9, depth=10) == 0.0


def test_rbo_rewards_top_agreement_over_tail_agreement() -> None:
    # Same head, swapped tail beats swapped head, same tail — RBO is top-weighted.
    base = [f"d{i}" for i in range(6)]
    top_agree = base[:3] + base[3:][::-1]
    tail_agree = base[:3][::-1] + base[3:]
    p, depth = 0.9, 6
    assert rank_biased_overlap(base, top_agree, p, depth) > rank_biased_overlap(
        base, tail_agree, p, depth
    )


def test_compute_metrics_perfect_agreement() -> None:
    df = pd.DataFrame(
        {
            "domain": [f"d{i}" for i in range(50)],
            "our_pr": [50 - i for i in range(50)],
            "opr_score": [50 - i for i in range(50)],
        }
    )
    m = compute_metrics(df, rbo_depth=50, kendall_sample=100)
    assert m.n == 50
    assert m.spearman == pytest.approx(1.0)
    assert m.kendall_tau_b == pytest.approx(1.0)
    assert m.median_abs_delta == 0.0
    # identical orderings ⇒ every top-k Jaccard is 1.0
    assert all(j == pytest.approx(1.0) for _, j in m.topk_jaccard)
    assert m.delta_hosts_spearman is None


def test_compute_metrics_inverse_agreement() -> None:
    df = pd.DataFrame(
        {
            "domain": [f"d{i}" for i in range(50)],
            "our_pr": [50 - i for i in range(50)],
            "opr_score": [i for i in range(50)],
        }
    )
    m = compute_metrics(df, rbo_depth=50, kendall_sample=100)
    assert m.spearman == pytest.approx(-1.0)
    assert m.median_abs_delta > 0


def test_compute_metrics_picks_up_n_hosts_when_present() -> None:
    # Rankings disagree so Δrank varies — exercises the real Spearman(Δrank, n_hosts) path.
    df = pd.DataFrame(
        {
            "domain": [f"d{i}" for i in range(10)],
            "our_pr": [10 - i for i in range(10)],
            "opr_score": [(i * 7) % 10 for i in range(10)],
            "n_hosts": [i + 1 for i in range(10)],
        }
    )
    m = compute_metrics(df, rbo_depth=10, kendall_sample=100)
    assert m.delta_hosts_spearman is not None


def test_compute_metrics_rejects_tiny_overlap() -> None:
    df = pd.DataFrame({"domain": ["d0"], "our_pr": [1.0], "opr_score": [1.0]})
    with pytest.raises(ValueError, match="overlap too small"):
        compute_metrics(df)
