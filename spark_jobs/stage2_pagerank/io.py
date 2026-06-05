"""I/O for Stage 2 — PageRank.

Reads the staged domain graph (edges + vertex map) and writes ``cc_domain_pagerank``.
The graph is read as Parquet from the staged location (the TSV→Parquet convert and
the vertex-map build are ingest-stage concerns, not Stage 2 — see the CommonCrawl
access strategy). All reads select an explicit column set so a schema drift in the
staged graph fails loudly here rather than mid-iteration.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from spark_jobs.common.errors import DataSourceError

_EDGE_COLUMNS = {"from_id": "bigint", "to_id": "bigint"}
_VERTEX_COLUMNS = {"id": "bigint", "domain": "string"}


def read_edges(spark: SparkSession, path: str) -> DataFrame:
    """Read the domain edge list as ``(from_id, to_id)`` from Parquet."""
    return _read_columns(spark, path, _EDGE_COLUMNS)


def read_vertices(spark: SparkSession, path: str) -> DataFrame:
    """Read the vertex map as ``(id, domain)`` from Parquet (the v3_map)."""
    return _read_columns(spark, path, _VERTEX_COLUMNS)


def _read_columns(spark: SparkSession, path: str, columns: dict[str, str]) -> DataFrame:
    """Read Parquet, validating that each expected column is present with the right type."""
    df = spark.read.parquet(path)
    missing = [c for c in columns if c not in df.columns]
    if missing:
        raise DataSourceError(f"{path} is missing columns {missing}; found {df.columns}")
    dtypes = dict(df.dtypes)
    wrong = {c: dtypes[c] for c, expected in columns.items() if dtypes[c] != expected}
    if wrong:
        raise DataSourceError(f"{path} has wrong column type(s) {wrong}; expected {columns}")
    return df.select(*[F.col(c) for c in columns])


def write_pagerank(df: DataFrame, path: str) -> None:
    """Write ``cc_domain_pagerank`` Parquet, partitioned by ``crawl`` (SPEC.md §6).

    Uses dynamic partition overwrite so re-running one crawl replaces only that
    crawl's partition; other crawls in the sliding window (SPEC.md §4) survive.
    Static overwrite would wipe the whole dataset.
    """
    df.write.option("partitionOverwriteMode", "dynamic").mode("overwrite").partitionBy(
        "crawl"
    ).parquet(path)
