"""Exp 4 — pure transforms for the open_authority seed teleport vector.

Turn the composite-DR consensus seed into a normalized teleport vector `(id, w)` for
`analyze.py --teleport`: weight each seed domain, map it onto its V3 graph node via v3_map,
and normalize the surviving weights to sum 1. Seed cleaning (out-neighbor quality, boilerplate
denylist, …) is a later iteration — `raw` here means no cleaning.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def weight_from_consensus(df: DataFrame, formula: str) -> DataFrame:
    """`(domain, consensus_score)` -> `(domain, w)`. Rank is by descending score.

    Ties broken by domain ascending so the rank — and thus rank-based weights — are
    deterministic across runs.
    """
    ranked = df.withColumn(
        "rank",
        F.row_number().over(Window.orderBy(F.col("consensus_score").desc(), F.col("domain").asc())),
    )
    if formula == "log_rank":
        w = F.lit(1.0) / F.log2(F.col("rank") + 1.0)
    elif formula == "sqrt_rank":
        w = F.lit(1.0) / F.sqrt(F.col("rank").cast("double"))
    elif formula == "uniform":
        w = F.lit(1.0)
    elif formula == "score":
        w = F.col("consensus_score").cast("double")
    else:
        raise ValueError(f"unknown weight formula: {formula}")
    return ranked.select("domain", w.alias("w"))


def to_teleport_vector(weights: DataFrame, v3_map: DataFrame) -> DataFrame:
    """`(domain, w)` ⋈ `v3_map(id, domain)` -> `(id, w)` normalized to sum 1; drop off-graph.

    Inner-joins on domain (seeds not present in the graph are dropped), then divides by the
    total surviving weight so `w` sums to 1 — the teleport-vector contract `analyze.py`
    expects. The output column is literally `w`.
    """
    joined = weights.join(v3_map.select("id", "domain"), "domain")
    total = joined.agg(F.sum("w")).first()[0]
    if total is None or total <= 0:
        raise ValueError("teleport vector has zero mapped weight (all seeds off-graph)")
    return joined.select("id", (F.col("w") / F.lit(total)).alias("w"))
