import pandas as pd
from validate_oa import compute_oa_metrics


def test_concordant_ranking_high_spearman_and_social_share():
    df = pd.DataFrame(
        {
            "domain": ["a", "b", "c", "d"],
            "open_authority": [0.4, 0.3, 0.2, 0.1],
            "tf": [90.0, 70.0, 50.0, 30.0],  # perfectly concordant with OA
        }
    )
    m = compute_oa_metrics(df, social=("b",))
    assert m["spearman_tf"] > 0.99  # OA tracks TF
    assert m["tau_tf"] > 0.99
    assert m["panel_n"] == 4
    assert m["social_share_top2"] == 0.5  # b is 1 of the top-2 by open_authority


def test_discordant_ranking_negative_spearman():
    # OA ranks the exact reverse of TF — the metric must register strong anti-correlation,
    # not just "some positive number". Guards against a metric that always looks concordant.
    df = pd.DataFrame(
        {
            "domain": ["a", "b", "c", "d"],
            "open_authority": [0.4, 0.3, 0.2, 0.1],
            "tf": [10.0, 30.0, 60.0, 95.0],  # inverse of OA order
        }
    )
    m = compute_oa_metrics(df)
    assert m["spearman_tf"] < -0.99
    assert m["social_share_top2"] == 0.0  # no social domains supplied
