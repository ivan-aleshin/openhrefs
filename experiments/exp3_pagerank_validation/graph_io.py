"""Shared Spark IO for Exp 3 graph jobs — vertices→domain map and edges.

Runs on Dataproc; pure-Python deps (tldextract via domain_utils) in a vectorized
`pandas_udf` (Arrow batches — far less per-row overhead than a row-at-a-time UDF, which
timed out over 279M vertices). The map is computed once by `build_id_domain.py` and
read back by the other jobs via `read_map`.
"""

import pandas as pd
from domain_utils import registered_domain, unreverse_host
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.functions import pandas_udf

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


@pandas_udf(_MAP_SCHEMA)
def _map_udf(rev: pd.Series) -> pd.DataFrame:
    """Vectorized reversed-host -> (domain, is_apex) over an Arrow batch."""
    return pd.DataFrame([_map_host(h) for h in rev], columns=["domain", "is_apex"])


def compute_id_domain(spark: SparkSession, vertices_path: str, partitions: int = 256) -> DataFrame:
    """Vertices (id, reversed-host) -> (id, domain, is_apex); invalid domains dropped.

    Repartitions before the UDF because the source gz parts are non-splittable (one
    task each), which otherwise starves parallelism on the heavy mapping stage.
    """
    v = spark.read.option("sep", "\t").schema(_VERTEX_SCHEMA).csv(vertices_path)
    v = v.repartition(partitions)
    m = v.select("id", _map_udf("rev_host").alias("m"))
    return m.select("id", "m.domain", "m.is_apex").where(F.col("domain").isNotNull())


def read_map(spark: SparkSession, id_domain_path: str) -> DataFrame:
    """Read a precomputed id→domain map (id, domain, is_apex)."""
    return spark.read.parquet(id_domain_path)


def read_edges(spark: SparkSession, path: str) -> DataFrame:
    """Edges from parquet or tab-separated .txt(.gz)."""
    if "parquet" in path:
        return spark.read.parquet(path)
    return spark.read.option("sep", "\t").schema(_EDGE_SCHEMA).csv(path)
