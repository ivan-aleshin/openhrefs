"""Shared Spark IO for Exp 3 graph jobs — vertices→domain map and edges.

Runs on Dataproc; only pure-Python deps (tldextract via domain_utils) used in UDFs.
"""

from domain_utils import registered_domain, unreverse_host
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

_VERTEX_SCHEMA = T.StructType(
    [T.StructField("id", T.LongType()), T.StructField("rev_host", T.StringType())]
)
_EDGE_SCHEMA = T.StructType(
    [T.StructField("from_id", T.LongType()), T.StructField("to_id", T.LongType())]
)
_MAP_SCHEMA = T.StructType(
    [T.StructField("domain", T.StringType()), T.StructField("is_apex", T.BooleanType())]
)


def _map_host(rev_host: str | None) -> tuple[str | None, bool]:
    """Reversed host -> (registered domain, is-apex). (None, False) if invalid."""
    if not rev_host:
        return (None, False)
    host = unreverse_host(rev_host)
    reg = registered_domain(host)
    if reg is None:
        return (None, False)
    return (reg, host == reg)


def read_id_domain(spark: SparkSession, vertices_path: str) -> DataFrame:
    """Vertices (id, reversed-host) -> (id, domain, is_apex); invalid domains dropped."""
    map_udf = F.udf(_map_host, _MAP_SCHEMA)
    v = spark.read.option("sep", "\t").schema(_VERTEX_SCHEMA).csv(vertices_path)
    m = v.select("id", map_udf("rev_host").alias("m"))
    return m.select("id", "m.domain", "m.is_apex").where(F.col("domain").isNotNull())


def read_edges(spark: SparkSession, path: str) -> DataFrame:
    """Edges from parquet or tab-separated .txt(.gz)."""
    if "parquet" in path:
        return spark.read.parquet(path)
    return spark.read.option("sep", "\t").schema(_EDGE_SCHEMA).csv(path)
