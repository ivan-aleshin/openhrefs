"""Exp 3 — metric profile for a PageRank variant vs OpenPageRank.

Runs **locally** (laptop), not on Dataproc: scipy/numpy have native extensions
and don't package cleanly into Serverless `--py-files`. The graph-scale join (our 150M+
domains ⋈ OPR top-10M) is produced by `join_opr.py` (a Spark job on Dataproc), which
writes a small overlap parquet (≤10M
rows); this script reads that overlap parquet with pandas and computes the metrics.

Overlap parquet schema: `domain` (str), `our_pr` (float), `opr_score` (float),
optionally `n_hosts` (int) for the divergence-vs-subdomain-count analysis.

This is a *profile*, not one pass/fail number. Real gate: V3 ≈ OPR (validates the
PageRank engine). V1↔V3 divergence informs the Stage 2 construction choice — see
DESIGN "Конструкция PageRank".
"""

import argparse

import numpy as np
import pandas as pd
from scipy import stats

_TOPK = (100, 1_000, 10_000, 100_000)
_BUCKETS = [(1, 1_000), (1_000, 10_000), (10_000, 100_000), (100_000, 1_000_000)]


def rank_biased_overlap(left: list[str], right: list[str], p: float, depth: int) -> float:
    """RBO for two ranked lists, truncated at `depth` (Webber et al. 2010)."""
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


def compute_metrics(df: pd.DataFrame, rbo_p: float, rbo_depth: int, kendall_sample: int) -> None:
    """Print the metric profile for the overlap (domain, our_pr, opr_score[, n_hosts])."""
    n = len(df)
    print(f"\n=== Overlap ===\nrows (domains in both): {n:,}")
    if n < 2:
        print("overlap too small — abort metrics")
        return

    our = df["our_pr"].to_numpy()
    opr = df["opr_score"].to_numpy()

    print("\n=== Global correlation ===")
    print(f"Spearman: {stats.spearmanr(our, opr).statistic:.4f}")
    if n > kendall_sample:
        idx = np.random.default_rng(0).choice(n, kendall_sample, replace=False)
        tau = stats.kendalltau(our[idx], opr[idx], variant="b").statistic
        print(f"Kendall tau-b (sample {kendall_sample:,}): {tau:.4f}")
    else:
        print(f"Kendall tau-b: {stats.kendalltau(our, opr, variant='b').statistic:.4f}")

    # ranks: 1 = most authoritative
    df = df.assign(
        our_rank=(-df["our_pr"]).rank(method="average").astype(int),
        opr_rank=(-df["opr_score"]).rank(method="average").astype(int),
    )
    print("\n=== Spearman per OPR-rank bucket ===")
    for lo, hi in _BUCKETS:
        b = df[(df["opr_rank"] >= lo) & (df["opr_rank"] < hi)]
        if len(b) > 2:
            r = stats.spearmanr(b["our_pr"], b["opr_score"]).statistic
            print(f"  rank [{lo:>7,}, {hi:>8,}): n={len(b):>9,}  Spearman={r:.4f}")

    print("\n=== top-k Jaccard (over overlap) ===")
    for k in _TOPK:
        if k <= n:
            ours_top = set(df.nsmallest(k, "our_rank")["domain"])
            opr_top = set(df.nsmallest(k, "opr_rank")["domain"])
            j = len(ours_top & opr_top) / len(ours_top | opr_top)
            print(f"  k={k:>7,}: Jaccard={j:.4f}")

    rbo = rank_biased_overlap(
        df.sort_values("our_rank")["domain"].tolist(),
        df.sort_values("opr_rank")["domain"].tolist(),
        rbo_p,
        rbo_depth,
    )
    print(f"\n=== RBO (p={rbo_p}, depth={rbo_depth:,}) ===\nRBO: {rbo:.4f}")

    print("\n=== Divergence (our_rank - opr_rank) ===")
    delta = np.abs((df["our_rank"] - df["opr_rank"]).to_numpy())
    print(f"  median |Δrank|: {np.median(delta):,.0f}")
    print(f"  p90 |Δrank|:    {np.percentile(delta, 90):,.0f}")
    if "n_hosts" in df.columns:
        c = stats.spearmanr(df["our_rank"] - df["opr_rank"], df["n_hosts"]).statistic
        print(f"  Spearman(Δrank, hosts/domain): {c:.4f}  (<0 ⇒ multi-host domains over-ranked)")

    print("\n=== Our top-20 domains (eyeball) ===")
    for _, row in df.nsmallest(20, "our_rank").iterrows():
        print(f"  {row['domain']}  our_rank={row['our_rank']}  opr_rank={row['opr_rank']}")


def main(argv: list[str] | None = None) -> None:
    """Entrypoint — read the overlap parquet and print the metric profile."""
    parser = argparse.ArgumentParser(description="Exp 3 — variant vs OpenPageRank (local).")
    parser.add_argument("--overlap", required=True, help="parquet (domain, our_pr, opr_score).")
    parser.add_argument("--label", default="variant")
    parser.add_argument("--rbo-p", type=float, default=0.99)
    parser.add_argument("--rbo-depth", type=int, default=100_000)
    parser.add_argument("--kendall-sample", type=int, default=1_000_000)
    args = parser.parse_args(argv)

    df = pd.read_parquet(args.overlap)
    print(f"\n### Variant: {args.label} ###")
    compute_metrics(df, args.rbo_p, args.rbo_depth, args.kendall_sample)


if __name__ == "__main__":
    main()
