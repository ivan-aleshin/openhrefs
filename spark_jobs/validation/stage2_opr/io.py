"""I/O for the Stage 2 OpenPageRank validation gate.

Reads the Stage 2 ``cc_domain_pagerank`` Parquet and the OpenPageRank reference CSV,
writes the small joined overlap Parquet (partitioned by crawl, same write contract as
the stage outputs) for off-cluster metric computation.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession


def read_pagerank(spark: SparkSession, path: str) -> DataFrame:
    """Read the Stage 2 ``cc_domain_pagerank`` Parquet output."""
    return spark.read.parquet(path)


def read_opr_csv(spark: SparkSession, path: str) -> DataFrame:
    """Read the OpenPageRank reference CSV (header row, quoted host + score columns)."""
    return spark.read.option("header", True).csv(path)


def write_overlap(df: DataFrame, path: str) -> None:
    """Write the overlap Parquet, dynamic partition overwrite by crawl (SPEC §4 pattern)."""
    df.write.option("partitionOverwriteMode", "dynamic").mode("overwrite").partitionBy(
        "crawl"
    ).parquet(path)
