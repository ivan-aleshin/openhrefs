"""Off-cluster rank-metric profile for a PageRank output vs OpenPageRank (SPEC §5).

The Spark validation gate (``spark_jobs/validation/stage2_opr``) writes the small
per-crawl overlap Parquet ``(domain, our_pr, opr_score[, crawl])``. This module reads
that overlap and computes the rank-agreement profile — Spearman, Kendall tau-b,
per-bucket Spearman, top-k Jaccard, RBO, and rank divergence.

It runs **locally**, not on Dataproc: scipy/numpy carry native extensions that stay out
of the Serverless runtime image, and the overlap (≤10M rows) is laptop-sized.

This is a *profile*, not a single pass/fail number — the ~0.9-vs-OPR bar was rejected as
miscalibrated (OPR is a reference, not ground truth). The gate reads the profile: the
prod engine should reproduce the validated Exp 3 V3 behavior (Spearman ~0.40, tau-b ~0.28,
RBO ~0.53, clean authoritative head, no farms).
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass

import numpy as np
import pandas as pd
from scipy import stats

_TOPK = (100, 1_000, 10_000, 100_000)
_BUCKETS = ((1, 1_000), (1_000, 10_000), (10_000, 100_000), (100_000, 1_000_000))


@dataclass(frozen=True)
class RankMetrics:
    """Rank-agreement profile for an overlap of (our_pr, opr_score) over shared domains."""

    n: int
    spearman: float
    kendall_tau_b: float
    bucket_spearman: list[tuple[int, int, int, float]]  # (lo, hi, n, spearman)
    topk_jaccard: list[tuple[int, float]]
    rbo: float
    median_abs_delta: float
    p90_abs_delta: float
    top20: list[tuple[str, int, int]]  # (domain, our_rank, opr_rank)
    delta_hosts_spearman: float | None  # Spearman(Δrank, hosts/domain) if n_hosts present


def rank_biased_overlap(left: list[str], right: list[str], p: float, depth: int) -> float:
    """RBO for two ranked lists, truncated at ``depth`` (Webber et al. 2010)."""
    seen_l: set[str] = set()
    seen_r: set[str] = set()
    rbo = 0.0
    d = min(depth, max(len(left), len(right)))
    for i in range(d):
        if i < len(left):
            seen_l.add(left[i])
        if i < len(right):
            seen_r.add(right[i])
        rbo += (len(seen_l & seen_r) / (i + 1)) * (p**i)
    return (1.0 - p) * rbo


def _correlations(our: np.ndarray, opr: np.ndarray, kendall_sample: int) -> tuple[float, float]:
    """Global Spearman and Kendall tau-b (tau-b sampled when the overlap is large)."""
    spearman = float(stats.spearmanr(our, opr).statistic)
    if our.size > kendall_sample:
        idx = np.random.default_rng(0).choice(our.size, kendall_sample, replace=False)
        tau = stats.kendalltau(our[idx], opr[idx], variant="b").statistic
    else:
        tau = stats.kendalltau(our, opr, variant="b").statistic
    return spearman, float(tau)


def _bucket_spearman(df: pd.DataFrame) -> list[tuple[int, int, int, float]]:
    """Spearman within each OPR-rank band — head agreement vs tail agreement."""
    out: list[tuple[int, int, int, float]] = []
    for lo, hi in _BUCKETS:
        b = df[(df["opr_rank"] >= lo) & (df["opr_rank"] < hi)]
        if len(b) > 2:
            r = float(stats.spearmanr(b["our_pr"], b["opr_score"]).statistic)
            out.append((lo, hi, len(b), r))
    return out


def _topk_jaccard(df: pd.DataFrame, n: int) -> list[tuple[int, float]]:
    """Set overlap of the two top-k domain lists, as Jaccard, over the overlap."""
    out: list[tuple[int, float]] = []
    for k in _TOPK:
        if k <= n:
            ours_top = set(df.nsmallest(k, "our_rank")["domain"])
            opr_top = set(df.nsmallest(k, "opr_rank")["domain"])
            out.append((k, len(ours_top & opr_top) / len(ours_top | opr_top)))
    return out


def compute_metrics(
    df: pd.DataFrame,
    *,
    rbo_p: float = 0.99,
    rbo_depth: int = 100_000,
    kendall_sample: int = 1_000_000,
) -> RankMetrics:
    """Compute the rank-agreement profile for an overlap (domain, our_pr, opr_score)."""
    n = len(df)
    if n < 2:
        raise ValueError(f"overlap too small for metrics: {n} rows")
    spearman, tau_b = _correlations(
        df["our_pr"].to_numpy(), df["opr_score"].to_numpy(), kendall_sample
    )
    df = df.assign(
        our_rank=(-df["our_pr"]).rank(method="average").astype(int),
        opr_rank=(-df["opr_score"]).rank(method="average").astype(int),
    )
    rbo = rank_biased_overlap(
        df.sort_values("our_rank")["domain"].tolist(),
        df.sort_values("opr_rank")["domain"].tolist(),
        rbo_p,
        rbo_depth,
    )
    delta = np.abs((df["our_rank"] - df["opr_rank"]).to_numpy())
    delta_hosts = None
    if "n_hosts" in df.columns:
        corr = stats.spearmanr(df["our_rank"] - df["opr_rank"], df["n_hosts"]).statistic
        delta_hosts = float(corr)
    top20 = [
        (r["domain"], int(r["our_rank"]), int(r["opr_rank"]))
        for _, r in df.nsmallest(20, "our_rank").iterrows()
    ]
    return RankMetrics(
        n=n,
        spearman=spearman,
        kendall_tau_b=tau_b,
        bucket_spearman=_bucket_spearman(df),
        topk_jaccard=_topk_jaccard(df, n),
        rbo=rbo,
        median_abs_delta=float(np.median(delta)),
        p90_abs_delta=float(np.percentile(delta, 90)),
        top20=top20,
        delta_hosts_spearman=delta_hosts,
    )


def render(metrics: RankMetrics, label: str, rbo_p: float, rbo_depth: int) -> None:
    """Print the metric profile as a human-readable report."""
    m = metrics
    print(f"\n### Variant: {label} ###")
    print(f"\n=== Overlap ===\nrows (domains in both): {m.n:,}")
    print("\n=== Global correlation ===")
    print(f"Spearman:      {m.spearman:.4f}")
    print(f"Kendall tau-b: {m.kendall_tau_b:.4f}")
    print("\n=== Spearman per OPR-rank bucket ===")
    for lo, hi, nb, r in m.bucket_spearman:
        print(f"  rank [{lo:>7,}, {hi:>8,}): n={nb:>9,}  Spearman={r:.4f}")
    print("\n=== top-k Jaccard (over overlap) ===")
    for k, j in m.topk_jaccard:
        print(f"  k={k:>7,}: Jaccard={j:.4f}")
    print(f"\n=== RBO (p={rbo_p}, depth={rbo_depth:,}) ===\nRBO: {m.rbo:.4f}")
    print("\n=== Divergence (|our_rank - opr_rank|) ===")
    print(f"  median |Δrank|: {m.median_abs_delta:,.0f}")
    print(f"  p90 |Δrank|:    {m.p90_abs_delta:,.0f}")
    if m.delta_hosts_spearman is not None:
        print(
            f"  Spearman(Δrank, hosts/domain): {m.delta_hosts_spearman:.4f}"
            "  (<0 ⇒ multi-host over-ranked)"
        )
    print("\n=== Our top-20 domains (eyeball) ===")
    for domain, our_rank, opr_rank in m.top20:
        print(f"  {domain}  our_rank={our_rank}  opr_rank={opr_rank}")


def main(argv: list[str] | None = None) -> None:
    """Entrypoint — read the overlap Parquet and print the metric profile."""
    parser = argparse.ArgumentParser(description="PageRank vs OpenPageRank rank metrics (local).")
    parser.add_argument(
        "--overlap", required=True, help="Overlap Parquet (domain, our_pr, opr_score)."
    )
    parser.add_argument(
        "--crawl", default=None, help="Filter to one crawl if the overlap is partitioned."
    )
    parser.add_argument("--label", default="variant")
    parser.add_argument("--rbo-p", type=float, default=0.99)
    parser.add_argument("--rbo-depth", type=int, default=100_000)
    parser.add_argument("--kendall-sample", type=int, default=1_000_000)
    args = parser.parse_args(argv)

    df = pd.read_parquet(args.overlap)
    if args.crawl is not None and "crawl" in df.columns:
        df = df[df["crawl"] == args.crawl]
    metrics = compute_metrics(
        df, rbo_p=args.rbo_p, rbo_depth=args.rbo_depth, kendall_sample=args.kendall_sample
    )
    render(metrics, args.label, args.rbo_p, args.rbo_depth)


if __name__ == "__main__":
    main()
