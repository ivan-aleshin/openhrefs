"""Unit tests for spark_jobs.ingest.transforms."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import types as T

from spark_jobs.ingest.transforms import to_v3_map

_VERTEX_SCHEMA = T.StructType(
    [
        T.StructField("id", T.LongType(), nullable=False),
        T.StructField("rev_domain", T.StringType(), nullable=False),
        T.StructField("num_hosts", T.LongType(), nullable=False),
    ]
)


def _vertices(spark: SparkSession, rows: list[tuple[int, str, int]]) -> DataFrame:
    return spark.createDataFrame(rows, _VERTEX_SCHEMA)


def test_to_v3_map_unreverses_domain_and_drops_num_hosts(spark: SparkSession) -> None:
    v = _vertices(spark, [(0, "com.example", 5), (1, "uk.co.bbc", 3)])
    out = to_v3_map(v)
    assert [f.name for f in out.schema] == ["id", "domain"]
    assert {r["id"]: r["domain"] for r in out.collect()} == {0: "example.com", 1: "bbc.co.uk"}


def test_to_v3_map_single_label(spark: SparkSession) -> None:
    v = _vertices(spark, [(0, "localhost", 1)])
    assert {r["id"]: r["domain"] for r in to_v3_map(v).collect()} == {0: "localhost"}
