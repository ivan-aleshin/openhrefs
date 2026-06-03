"""Pure DataFrame transforms for Stage 2 — global PageRank (SPEC.md §5 Stage 2).

No I/O: every function is ``DataFrame -> DataFrame`` (or with scalar params) and
is unit-tested against synthetic graphs. Reads, writes, and SparkSession setup
live in ``io.py`` / ``main.py``.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def compute_degrees(edges: DataFrame, nodes: DataFrame) -> DataFrame:
    """In- and out-degree for every node, including isolated/dangling ones.

    Args:
        edges: ``(from_id, to_id)`` directed edges.
        nodes: ``(id)`` — the full vertex set; nodes absent from ``edges`` get 0.

    Returns:
        ``(id, in_degree, out_degree)`` with one row per node in ``nodes``.
    """
    out_deg = edges.groupBy("from_id").agg(F.count("*").alias("out_degree"))
    in_deg = edges.groupBy("to_id").agg(F.count("*").alias("in_degree"))
    return (
        nodes.join(in_deg, nodes["id"] == in_deg["to_id"], "left")
        .join(out_deg, nodes["id"] == out_deg["from_id"], "left")
        .select(
            nodes["id"],
            F.coalesce(F.col("in_degree"), F.lit(0)).cast("long").alias("in_degree"),
            F.coalesce(F.col("out_degree"), F.lit(0)).cast("long").alias("out_degree"),
        )
    )


def to_pagerank_output(
    ranks: DataFrame,
    degrees: DataFrame,
    vertices: DataFrame,
    crawl: str,
) -> DataFrame:
    """Assemble the ``cc_domain_pagerank`` output (SPEC.md §6, schema CC_DOMAIN_PAGERANK).

    Args:
        ranks: ``(id, rank)`` from the power iteration.
        degrees: ``(id, in_degree, out_degree)`` from :func:`compute_degrees`.
        vertices: ``(id, domain)`` — the registered-domain label per node.
        crawl: crawl identifier stamped on every row.

    Returns:
        ``(domain, crawl, pagerank_score, in_degree, out_degree)``.
    """
    return (
        vertices.join(ranks, "id")
        .join(degrees, "id")
        .select(
            F.col("domain"),
            F.lit(crawl).alias("crawl"),
            F.col("rank").cast("double").alias("pagerank_score"),
            F.col("in_degree").cast("long").alias("in_degree"),
            F.col("out_degree").cast("long").alias("out_degree"),
        )
    )
