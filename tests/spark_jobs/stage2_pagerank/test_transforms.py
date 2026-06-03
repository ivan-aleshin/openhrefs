"""Unit tests for spark_jobs.stage2_pagerank.transforms."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import types as T

from spark_jobs.stage2_pagerank.transforms import compute_degrees, to_pagerank_output

_NODE_SCHEMA = T.StructType([T.StructField("id", T.LongType(), nullable=False)])
_EDGE_SCHEMA = T.StructType(
    [
        T.StructField("from_id", T.LongType(), nullable=False),
        T.StructField("to_id", T.LongType(), nullable=False),
    ]
)


def _nodes(spark: SparkSession, ids: list[int]) -> DataFrame:
    return spark.createDataFrame([(i,) for i in ids], _NODE_SCHEMA)


def _edges(spark: SparkSession, pairs: list[tuple[int, int]]) -> DataFrame:
    return spark.createDataFrame(pairs, _EDGE_SCHEMA)


def test_compute_degrees_counts_in_and_out(spark: SparkSession) -> None:
    nodes = _nodes(spark, [0, 1, 2, 3])
    edges = _edges(spark, [(0, 1), (0, 2), (1, 2)])
    result = {
        r["id"]: (r["in_degree"], r["out_degree"]) for r in compute_degrees(edges, nodes).collect()
    }
    assert result == {
        0: (0, 2),
        1: (1, 1),
        2: (2, 0),
        3: (0, 0),
    }


def test_compute_degrees_covers_all_nodes_including_isolated(spark: SparkSession) -> None:
    nodes = _nodes(spark, [0, 1, 2])
    edges = _edges(spark, [(0, 1)])
    result = compute_degrees(edges, nodes)
    assert result.count() == 3
    isolated = result.filter("id = 2").first()
    assert (isolated["in_degree"], isolated["out_degree"]) == (0, 0)


def test_compute_degrees_empty_edges(spark: SparkSession) -> None:
    nodes = _nodes(spark, [0, 1])
    edges = _edges(spark, [])
    result = {
        r["id"]: (r["in_degree"], r["out_degree"]) for r in compute_degrees(edges, nodes).collect()
    }
    assert result == {0: (0, 0), 1: (0, 0)}


def _vertices(spark: SparkSession, rows: list[tuple[int, str]]) -> DataFrame:
    schema = T.StructType(
        [
            T.StructField("id", T.LongType(), nullable=False),
            T.StructField("domain", T.StringType(), nullable=False),
        ]
    )
    return spark.createDataFrame(rows, schema)


def test_to_pagerank_output_matches_schema(spark: SparkSession) -> None:
    from spark_jobs.common.schemas import CC_DOMAIN_PAGERANK

    ranks = spark.createDataFrame([(0, 0.6), (1, 0.4)], ["id", "rank"])
    degrees = spark.createDataFrame([(0, 2, 1), (1, 0, 3)], ["id", "in_degree", "out_degree"])
    vertices = _vertices(spark, [(0, "a.com"), (1, "b.org")])
    out = to_pagerank_output(ranks, degrees, vertices, crawl="CC-MAIN-2025-51")
    assert [f.name for f in out.schema] == [f.name for f in CC_DOMAIN_PAGERANK]
    assert dict(out.dtypes) == {f.name: f.dataType.simpleString() for f in CC_DOMAIN_PAGERANK}


def test_to_pagerank_output_joins_fields_by_id(spark: SparkSession) -> None:
    ranks = spark.createDataFrame([(0, 0.6), (1, 0.4)], ["id", "rank"])
    degrees = spark.createDataFrame([(0, 2, 1), (1, 0, 3)], ["id", "in_degree", "out_degree"])
    vertices = _vertices(spark, [(0, "a.com"), (1, "b.org")])
    rows = {
        r["domain"]: r
        for r in to_pagerank_output(ranks, degrees, vertices, crawl="CC-MAIN-2025-51").collect()
    }
    assert rows["a.com"]["pagerank_score"] == 0.6
    assert (rows["a.com"]["in_degree"], rows["a.com"]["out_degree"]) == (2, 1)
    assert rows["b.org"]["crawl"] == "CC-MAIN-2025-51"
