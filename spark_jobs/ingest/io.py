"""I/O for the graph ingest stage.

Reads the CommonCrawl published domain graph as TSV (explicit schemas — inference on
.gz is slow/unreliable) and writes the canonical staged Parquet inputs the authority
stages consume. The single ~15 GiB edges gzip must be split into parts upstream (a
transient-VM operational pre-step); this stage reads the split TSV parts.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

_VERTEX_TSV_SCHEMA = "id long, rev_domain string, num_hosts long"
_EDGE_TSV_SCHEMA = "from_id long, to_id long"


def read_vertices_tsv(spark: SparkSession, path: str) -> DataFrame:
    """Read CC domain vertices ``(id, rev_domain, num_hosts)`` from tab-separated text/gzip."""
    return spark.read.option("sep", "\t").schema(_VERTEX_TSV_SCHEMA).csv(path)


def read_edges_tsv(spark: SparkSession, path: str) -> DataFrame:
    """Read CC domain edges ``(from_id, to_id)`` from tab-separated text parts."""
    return spark.read.option("sep", "\t").schema(_EDGE_TSV_SCHEMA).csv(path)


def write_parquet(df: DataFrame, path: str) -> None:
    """Write a staged graph artifact as Parquet (splittable input for the authority stages)."""
    df.write.mode("overwrite").parquet(path)
