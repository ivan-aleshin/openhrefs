"""Boundary validation of the V3 dense domain graph (shared across authority stages).

The dense-id contract — vertices are dense ``[0, n)`` with non-null, unique domains and
every edge endpoint is non-null and in range — is a property of the V3 graph itself
(ADR-0002; Exp 4.0 gate), not a Stage 2 specific. Stages call this before the paid
power iteration so a corrupt staged graph fails loudly instead of producing a silently
wrong PageRank, leaking mass, or emitting a null-domain row.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from spark_jobs.common.errors import DataSourceError


def validate_graph(vertices: DataFrame, edges: DataFrame, n: int) -> None:
    """Validate the full dense-id graph contract (vertices then edges).

    Convenience facade; callers that interleave other work (e.g. building a seed
    teleport between the two) can call :func:`validate_vertices` and
    :func:`validate_edges` directly. Raises ``DataSourceError`` on violation.
    """
    validate_vertices(vertices, n)
    validate_edges(edges, n)


def validate_vertices(vertices: DataFrame, n: int) -> None:
    """Validate vertices are dense ``[0, n)`` with non-null, unique domains."""
    v = vertices.agg(
        F.countDistinct("id").alias("distinct"),
        F.min("id").alias("min"),
        F.max("id").alias("max"),
        F.sum(F.when(F.col("domain").isNull(), 1).otherwise(0)).alias("null_domain"),
        F.countDistinct("domain").alias("distinct_domain"),
    ).first()
    assert v is not None
    if v["distinct"] != n or v["min"] != 0 or v["max"] != n - 1:
        raise DataSourceError(
            f"vertices are not dense [0, {n}): distinct={v['distinct']} "
            f"min={v['min']} max={v['max']}"
        )
    if v["null_domain"]:
        raise DataSourceError(f"vertex map has {v['null_domain']} rows with a null domain")
    if v["distinct_domain"] != n:
        raise DataSourceError(
            f"vertex map domains are not unique: {v['distinct_domain']} distinct for {n} ids"
        )


def validate_edges(edges: DataFrame, n: int) -> None:
    """Validate every edge endpoint is non-null and within the vertex range ``[0, n)``."""
    e = edges.agg(
        F.min("from_id").alias("fmn"),
        F.max("from_id").alias("fmx"),
        F.min("to_id").alias("tmn"),
        F.max("to_id").alias("tmx"),
        F.sum(F.when(F.col("from_id").isNull() | F.col("to_id").isNull(), 1).otherwise(0)).alias(
            "null_ends"
        ),
    ).first()
    assert e is not None
    if e["null_ends"]:
        raise DataSourceError(f"edges have {e['null_ends']} rows with a null endpoint")
    mins = [x for x in (e["fmn"], e["tmn"]) if x is not None]
    maxs = [x for x in (e["fmx"], e["tmx"]) if x is not None]
    if mins and (min(mins) < 0 or max(maxs) >= n):
        raise DataSourceError(
            f"edge ids fall outside the vertex range [0, {n}): min={min(mins)} max={max(maxs)}"
        )
