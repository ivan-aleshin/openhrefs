"""Pure transforms for the Stage 2 PageRank vs OpenPageRank validation gate.

Collapses the OpenPageRank reference (host-keyed, top-10M CSV) to the registered
domain the pipeline graph uses, joins it with the Stage 2 output, and summarizes
the full output for the mass/sanity checks. Metrics (Spearman, RBO, …) are computed
off-cluster on the small overlap; they stay out of the Serverless runtime package.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from spark_jobs.common.domains import registered_domain

_registered_domain_udf = F.udf(registered_domain, T.StringType())


def normalize_opr(
    opr_raw: DataFrame,
    domain_col: str = "Domain",
    score_col: str = "Open Page Rank",
) -> DataFrame:
    """Collapse OpenPageRank host rows to one registered-domain key, keeping the max score.

    The CSV is host-keyed (``www.facebook.com``, ``m.facebook.com``); the graph is
    registered-domain-keyed, so several OPR rows map to one domain — take the strongest.
    """
    return (
        opr_raw.select(
            _registered_domain_udf(F.col(domain_col)).alias("domain"),
            F.col(score_col).cast("double").alias("opr_score"),
        )
        .where(F.col("domain").isNotNull() & F.col("opr_score").isNotNull())
        .groupBy("domain")
        .agg(F.max("opr_score").alias("opr_score"))
    )


def filter_crawl(pagerank: DataFrame, crawl: str) -> DataFrame:
    """Restrict the (multi-crawl, partitioned) Stage 2 output to a single crawl.

    Reading the ``cc_domain_pagerank`` root yields every crawl partition; the gate
    must score one crawl. A deliberately pre-narrowed single-partition path has no
    ``crawl`` column, so pass it through unchanged in that case.
    """
    if "crawl" not in pagerank.columns:
        return pagerank
    return pagerank.where(F.col("crawl") == crawl)


def to_overlap(pagerank: DataFrame, opr_norm: DataFrame) -> DataFrame:
    """Inner-join Stage 2 PageRank with normalized OPR on the registered domain.

    Stage 2 ``domain`` is already normalized (from ``v3_map``), so only the OPR side
    is collapsed; the join key is the registered domain on both sides.
    """
    ours = pagerank.select("domain", F.col("pagerank_score").cast("double").alias("our_pr"))
    return ours.join(opr_norm, "domain", "inner")


def output_stats(pagerank: DataFrame) -> DataFrame:
    """Single-row stats over the **full** Stage 2 output: count, mass sum, range, nulls.

    Mass conservation is checked across the whole output, not the OPR overlap.
    """
    return pagerank.agg(
        F.count("*").alias("pagerank_count"),
        F.sum("pagerank_score").alias("pagerank_sum"),
        F.min("pagerank_score").alias("pagerank_min"),
        F.max("pagerank_score").alias("pagerank_max"),
        F.sum(F.when(F.col("pagerank_score").isNull(), 1).otherwise(0)).alias("null_count"),
        F.sum(F.when(F.col("domain").isNull(), 1).otherwise(0)).alias("domain_null_count"),
    )
