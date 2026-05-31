"""Exp 4.4 — local metric profile: open_authority vs the trust-flow reference.

Runs locally (pandas/scipy) on the small overlap parquet pulled from GCS — the validation gate for
`open_authority`: rank-correlation against the trust-flow reference (OA ↔ TF, both seeded trust) +
the share of social/platform domains in the OA head (a cleaning diagnostic). No Spark, no spend.
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


def compute_oa_metrics(
    df: pd.DataFrame,
    social: tuple[str, ...] = (),
    social_ks: tuple[int, ...] = (100, 1_000),
) -> dict:
    """`df`: `(domain, open_authority, tf)` overlap → Spearman/Kendall vs TF + `panel_n`.

    Social shares are computed on `df`'s own ordering here (for the unit test); a real run overrides
    them from the unbiased full-ranking top-K slice (`main` with `--oa-top`).
    """
    oa, tf = df["open_authority"].to_numpy(), df["tf"].to_numpy()
    ordered = df.sort_values("open_authority", ascending=False)["domain"].tolist()
    out = {
        "panel_n": len(df),
        "spearman_tf": float(stats.spearmanr(oa, tf).statistic),
        "tau_tf": float(stats.kendalltau(oa, tf, variant="b").statistic),
        "social_basis": "overlap",
    }
    out.update(social_shares(ordered, social, social_ks))
    return out


def _read_lines(path: str) -> list[str]:
    return [x.strip() for x in pathlib.Path(path).read_text().splitlines() if x.strip()]


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Exp 4.4 — open_authority vs trust-flow reference.")
    p.add_argument("--overlap", required=True, help="parquet (domain, open_authority, tf).")
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
