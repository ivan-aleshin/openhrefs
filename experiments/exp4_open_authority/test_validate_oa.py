import numpy as np
import pandas as pd
from validate_oa import compute_oa_metrics


def _df(oa, trust, domains=None):
    n = len(oa)
    return pd.DataFrame(
        {"domain": domains or [f"d{i}" for i in range(n)], "open_authority": oa, "ref_trust": trust}
    )


def test_separation_and_ranking_profile():
    # top 5 by OA have positive, concordant trust; bottom 5 are zero-trust (the 57%-zeros shape)
    oa = list(range(10, 0, -1))  # 10..1 descending
    trust = [50.0, 40.0, 30.0, 20.0, 10.0, 0.0, 0.0, 0.0, 0.0, 0.0]
    m = compute_oa_metrics(_df(oa, trust))
    assert m["positive_n"] == 5 and m["zero_n"] == 5 and m["null_n"] == 0
    assert m["auc_trust_positive"] == 1.0  # OA perfectly separates positive-trust from zero
    assert m["spearman_pos"] > 0.99 and m["tau_b_pos"] > 0.99  # ranks the trusted set right
    assert m["tau_b_all"] > 0  # concordant overall
    # head OA deciles are all-positive, tail all-zero -> monotone non-increasing positive rate
    rates = m["positive_rate_by_decile"]
    assert rates[0] == 1.0 and rates[-1] == 0.0 and rates == sorted(rates, reverse=True)


def test_auc_below_half_when_oa_anticorrelates_with_having_trust():
    # positive trust sits at LOW OA -> OA is a bad separator -> AUC < 0.5
    oa = list(range(10, 0, -1))
    trust = [0.0, 0.0, 0.0, 0.0, 0.0, 10.0, 20.0, 30.0, 40.0, 50.0]
    m = compute_oa_metrics(_df(oa, trust))
    assert (
        m["auc_trust_positive"] == 0.0
    )  # every positive-trust domain has lower OA than every zero


def test_null_trust_counted_and_excluded():
    m = compute_oa_metrics(_df([3.0, 2.0, 1.0], [10.0, np.nan, 0.0]))
    assert m["null_n"] == 1 and m["positive_n"] == 1 and m["zero_n"] == 1
    assert m["panel_n"] == 3  # panel counts all rows; metrics use the non-null ones


def test_social_share_top2():
    m = compute_oa_metrics(
        _df([0.4, 0.3, 0.2], [9.0, 8.0, 7.0], domains=["a", "b", "c"]), social=("b",)
    )
    assert m["social_share_top2"] == 0.5  # b is 1 of the top-2 by OA
