"""Exp 4.4 — local metric profile: open_authority vs the trust reference.

Runs locally (pandas/scipy) on the small overlap parquet pulled from GCS — the validation gate for
`open_authority`. The reference's trust score is zero for a large share of the panel (found-but-
zero-trust domains), so a single rank correlation over everything blends two questions; the profile
separates them:
  - **binary separation** — does OA tell `ref_trust>0` from `ref_trust==0`? (`auc_trust_positive`,
    `positive_rate_by_decile`)
  - **ranking among trusted domains** — does OA order the `ref_trust>0` set? (`spearman_pos`,
    `tau_b_pos`)
plus `tau_b_all` (ties-robust, whole overlap) and the positive/zero/null counts. No Spark, no spend.
AUC is computed from the Mann-Whitney U rank statistic — no extra dependency.
"""

import argparse
import pathlib

import pandas as pd
from scipy import stats


def social_shares(
    ordered_domains: list[str],
    social: tuple[str, ...],
    social_ks: tuple[int, ...] = (100, 1_000),
) -> dict:
    """Share of social/platform domains in the top-k of a descending open_authority list.

    `ordered_domains` should be the full-ranking top-K slice (not the reference overlap) so the
    top-k isn't biased by reference composition. `social_share_top2` is kept for the unit test.
    """
    social_set = set(social)
    out = {
        "social_share_top2": len(set(ordered_domains[:2]) & social_set)
        / max(min(2, len(ordered_domains)), 1)
    }
    for k in social_ks:
        kk = min(k, len(ordered_domains))
        out[f"social_share_top{k}"] = len(set(ordered_domains[:kk]) & social_set) / max(kk, 1)
    return out


def _auc_positive(oa: pd.Series, is_pos: pd.Series) -> float | None:
    """AUC of OA discriminating `ref_trust>0` from `ref_trust==0`, via the Mann-Whitney U statistic
    (AUC = U / (n_pos * n_neg)); None if either class is empty."""
    pos, neg = oa[is_pos], oa[~is_pos]
    if len(pos) == 0 or len(neg) == 0:
        return None
    u = stats.mannwhitneyu(pos, neg, alternative="two-sided").statistic
    return float(u / (len(pos) * len(neg)))


def _positive_rate_by_decile(sub: pd.DataFrame) -> list[float]:
    """`ref_trust>0` rate per OA decile, ordered highest-OA decile → lowest."""
    if len(sub) < 10:
        return []
    dec = pd.qcut(sub["open_authority"], 10, labels=False, duplicates="drop")
    rate = (sub["ref_trust"] > 0).groupby(dec).mean()
    return [float(rate[b]) for b in sorted(rate.index, reverse=True)]


def compute_oa_metrics(
    df: pd.DataFrame,
    social: tuple[str, ...] = (),
    social_ks: tuple[int, ...] = (100, 1_000),
) -> dict:
    """`df`: `(domain, open_authority, ref_trust[, …])` overlap → the metric profile (see module).

    Social shares are computed on `df`'s own ordering here (for the unit test); a real run overrides
    them from the unbiased full-ranking top-K slice (`main` with `--oa-top`).
    """
    null_n = int(df["ref_trust"].isna().sum())
    sub = df.dropna(subset=["ref_trust"])
    oa, trust = sub["open_authority"], sub["ref_trust"]
    is_pos = trust > 0
    out: dict = {
        "panel_n": len(df),
        "positive_n": int(is_pos.sum()),
        "zero_n": int((trust == 0).sum()),
        "null_n": null_n,
        "tau_b_all": float(stats.kendalltau(oa, trust, variant="b").statistic)
        if len(sub) >= 2
        else None,
        "spearman_pos": None,
        "tau_b_pos": None,
        "auc_trust_positive": _auc_positive(oa, is_pos),
        "positive_rate_by_decile": _positive_rate_by_decile(sub),
        "social_basis": "overlap",
    }
    if is_pos.sum() >= 2:
        oap, trp = oa[is_pos], trust[is_pos]
        out["spearman_pos"] = float(stats.spearmanr(oap, trp).statistic)
        out["tau_b_pos"] = float(stats.kendalltau(oap, trp, variant="b").statistic)
    ordered = df.sort_values("open_authority", ascending=False)["domain"].tolist()
    out.update(social_shares(ordered, social, social_ks))
    return out


def _read_lines(path: str) -> list[str]:
    return [x.strip() for x in pathlib.Path(path).read_text().splitlines() if x.strip()]


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Exp 4.4 — open_authority vs the trust reference.")
    p.add_argument(
        "--overlap", required=True, help="parquet (domain, open_authority, ref_trust, …)."
    )
    p.add_argument(
        "--oa-top",
        help="parquet (domain, open_authority) — the OA top-K slice for unbiased social-share.",
    )
    p.add_argument("--social-file", help="optional newline list of social/platform domains.")
    a = p.parse_args(argv)
    social = tuple(_read_lines(a.social_file)) if a.social_file else ()
    m = compute_oa_metrics(pd.read_parquet(a.overlap), social)
    if a.oa_top:  # social-share on the full-ranking top-K slice (unbiased by reference composition)
        top = pd.read_parquet(a.oa_top)
        ordered = top.sort_values("open_authority", ascending=False)["domain"].tolist()
        m.update(social_shares(ordered, social))
        m["social_basis"] = "full_oa_topk"
    print(m)


if __name__ == "__main__":
    main()
