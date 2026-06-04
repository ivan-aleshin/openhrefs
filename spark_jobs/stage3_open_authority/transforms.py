"""Pure DataFrame transforms for Stage 3 — open_authority (SPEC.md §5 Stage 3).

Turns the composite-DR consensus seed into a normalized teleport vector for personalized
PageRank (``common.pagerank``), and assembles the ``cc_domain_authority`` output. No I/O:
every function is ``DataFrame -> DataFrame`` (or with scalar params), unit-tested against
synthetic data. Inputs use the canonical seed schema ``(domain, consensus)`` — the source
column mapping lives in ``io.read_seed``.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from spark_jobs.common.errors import DataSourceError

_WEIGHTS = {"log_rank", "sqrt_rank", "uniform", "score"}


def weight_from_consensus(consensus: DataFrame, weight: str, seed_size: int) -> DataFrame:
    """``(domain, consensus)`` → ``(domain, w)`` for the top-``seed_size`` domains.

    Rows with a null consensus are unranked non-candidates (the full composite-DR export
    carries ~millions of sparse no-score rows) and are dropped before ranking. Duplicate
    domains are collapsed to their max consensus. Rank is by consensus descending, domain
    ascending (deterministic tiebreak), then the top ``seed_size`` are weighted by
    ``weight``. A ``score`` weight whose surviving values are all non-positive is rejected
    (an all-zero teleport vector is meaningless).

    Raises:
        ValueError: if ``weight`` is not a known formula.
        DataSourceError: if no ranked candidate survives, a candidate has a null/blank
            domain or a NaN/negative consensus, or ``score`` weighting yields no positive weight.
    """
    if weight not in _WEIGHTS:
        raise ValueError(f"unknown seed weight formula: {weight}")
    candidates = consensus.where(F.col("consensus").isNotNull())
    _validate_seed(candidates)
    collapsed = candidates.groupBy("domain").agg(F.max("consensus").alias("consensus"))
    ranked = collapsed.withColumn(
        "rank",
        F.row_number().over(Window.orderBy(F.col("consensus").desc(), F.col("domain").asc())),
    ).filter(F.col("rank") <= seed_size)
    weighted = ranked.select("domain", _weight_expr(weight).alias("w"))
    if weight == "score":
        total = weighted.agg(F.sum(F.when(F.col("w") > 0, F.col("w")))).first()
        if total is None or total[0] is None:
            raise DataSourceError("seed 'score' weights are all zero/non-positive")
    return weighted


def _validate_seed(candidates: DataFrame) -> None:
    """Reject a malformed candidate set before ranking; raise ``DataSourceError`` on violation.

    Runs on the ranked candidates (null consensus already dropped upstream). A candidate
    with a null/blank domain or a NaN/negative consensus signals a corrupt seed, not just
    an absent score, and must not receive a teleport weight or reach the paid iteration.
    An empty candidate set (e.g. every row unranked) is fatal too.
    """
    chk = candidates.agg(
        F.count("*").alias("rows"),
        F.sum(
            F.when(F.col("domain").isNull() | (F.trim(F.col("domain")) == ""), 1).otherwise(0)
        ).alias("bad_domain"),
        F.sum(F.when(F.isnan("consensus") | (F.col("consensus") < 0), 1).otherwise(0)).alias(
            "bad_consensus"
        ),
    ).first()
    assert chk is not None
    if chk["rows"] == 0:
        raise DataSourceError("seed is empty (no ranked candidates)")
    if chk["bad_domain"]:
        raise DataSourceError(f"seed has {chk['bad_domain']} rows with a null/blank domain")
    if chk["bad_consensus"]:
        raise DataSourceError(f"seed has {chk['bad_consensus']} rows with a NaN/negative consensus")


def _weight_expr(weight: str) -> Column:
    if weight == "log_rank":
        return F.lit(1.0) / F.log2(F.col("rank") + 1.0)
    if weight == "sqrt_rank":
        return F.lit(1.0) / F.sqrt(F.col("rank").cast("double"))
    if weight == "uniform":
        return F.lit(1.0)
    return F.col("consensus").cast("double")  # score


def to_teleport_vector(weights: DataFrame, vertices: DataFrame) -> DataFrame:
    """``(domain, w)`` ⋈ ``vertices(id, domain)`` → ``(id, w)`` normalized to sum 1.

    Inner-joins on domain (off-graph seeds dropped), then divides by the surviving total.

    Raises:
        DataSourceError: if no seed maps onto the graph (total weight 0).
    """
    joined = vertices.select("id", "domain").join(F.broadcast(weights), "domain")
    total_row = joined.agg(F.sum("w")).first()
    total = total_row[0] if total_row else None
    if total is None or total <= 0:
        raise DataSourceError("teleport vector has zero mapped weight (all seeds off-graph)")
    return joined.select("id", (F.col("w") / F.lit(total)).alias("w"))


def in_degrees(edges: DataFrame, nodes: DataFrame) -> DataFrame:
    """Inbound-edge count per node, including nodes with none (0)."""
    in_deg = edges.groupBy("to_id").agg(F.count("*").alias("in_degree"))
    return nodes.join(in_deg, nodes["id"] == in_deg["to_id"], "left").select(
        nodes["id"],
        F.coalesce(F.col("in_degree"), F.lit(0)).cast("long").alias("in_degree"),
    )


def to_authority_output(
    ranks: DataFrame,
    in_degree: DataFrame,
    vertices: DataFrame,
    crawl: str,
) -> DataFrame:
    """Assemble ``cc_domain_authority`` (SPEC.md §6, schema CC_DOMAIN_AUTHORITY).

    ``open_volume = ln(1 + in_degree)`` (log1p): a log-scaled link-quantity signal, 0 at
    zero in-degree and monotonic.

    Args:
        ranks: ``(id, rank)`` personalized-PageRank scores → ``open_authority``.
        in_degree: ``(id, in_degree)`` from :func:`in_degrees`.
        vertices: ``(id, domain)``.
        crawl: crawl identifier stamped on every row.
    """
    return (
        vertices.join(ranks, "id")
        .join(in_degree, "id")
        .select(
            F.col("domain"),
            F.lit(crawl).alias("crawl"),
            F.col("rank").cast("double").alias("open_authority"),
            F.log1p(F.col("in_degree").cast("double")).alias("open_volume"),
        )
    )
