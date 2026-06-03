"""I/O for Stage 3 — open_authority.

Reads the staged domain graph (edges + vertex map) and the composite-DR consensus seed,
and writes ``cc_domain_authority``. The thin graph readers are duplicated from Stage 2
(rule of two — not shared yet) rather than importing another stage's io. ``read_seed``
maps the source columns to the canonical ``(domain, consensus)`` schema here, so the
transforms never see source-specific names.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from spark_jobs.common.errors import DataSourceError

_EDGE_COLUMNS = {"from_id": "bigint", "to_id": "bigint"}
_VERTEX_COLUMNS = {"id": "bigint", "domain": "string"}
# composite-DR consensus source columns (release `data-latest`); mapped to canon below.
_SEED_DOMAIN = "registered_domain"
_SEED_SCORE = "consensus_score"


def read_edges(spark: SparkSession, path: str) -> DataFrame:
    """Read the domain edge list as ``(from_id, to_id)`` from Parquet."""
    return _read_columns(spark, path, _EDGE_COLUMNS)


def read_vertices(spark: SparkSession, path: str) -> DataFrame:
    """Read the vertex map as ``(id, domain)`` from Parquet (the v3_map)."""
    return _read_columns(spark, path, _VERTEX_COLUMNS)


def read_seed(spark: SparkSession, path: str) -> DataFrame:
    """Read the composite-DR consensus seed → canonical ``(domain, consensus)``.

    Format is chosen by extension: ``.csv`` / ``.csv.gz`` are read as header CSV, anything
    else (a ``.parquet`` file or directory) as Parquet. The source columns
    ``registered_domain`` / ``consensus_score`` are renamed to the canonical schema here.
    """
    if path.endswith(".csv") or path.endswith(".csv.gz"):
        df = spark.read.option("header", "true").csv(path)
    else:
        df = spark.read.parquet(path)
    missing = [c for c in (_SEED_DOMAIN, _SEED_SCORE) if c not in df.columns]
    if missing:
        raise DataSourceError(f"{path} seed is missing columns {missing}; found {df.columns}")
    return df.select(
        F.col(_SEED_DOMAIN).alias("domain"),
        F.col(_SEED_SCORE).cast("double").alias("consensus"),
    )


def write_authority(df: DataFrame, path: str) -> None:
    """Write ``cc_domain_authority`` Parquet, partitioned by ``crawl`` (SPEC.md §6).

    Dynamic partition overwrite so re-running one crawl replaces only that crawl's
    partition; other crawls in the sliding window (SPEC.md §4) survive.
    """
    df.write.option("partitionOverwriteMode", "dynamic").mode("overwrite").partitionBy(
        "crawl"
    ).parquet(path)


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
