"""Unit tests for spark_jobs.stage3_open_authority.transforms."""

import math

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import types as T

from spark_jobs.common.errors import DataSourceError
from spark_jobs.common.schemas import CC_DOMAIN_AUTHORITY
from spark_jobs.stage3_open_authority.transforms import (
    in_degrees,
    to_authority_output,
    to_teleport_vector,
    weight_from_consensus,
)

_CONSENSUS_SCHEMA = T.StructType(
    [
        T.StructField("domain", T.StringType(), nullable=False),
        T.StructField("consensus", T.DoubleType(), nullable=False),
    ]
)
_VERTEX_SCHEMA = T.StructType(
    [
        T.StructField("id", T.LongType(), nullable=False),
        T.StructField("domain", T.StringType(), nullable=False),
    ]
)
_EDGE_SCHEMA = T.StructType(
    [
        T.StructField("from_id", T.LongType(), nullable=False),
        T.StructField("to_id", T.LongType(), nullable=False),
    ]
)


def _consensus(spark: SparkSession, rows: list[tuple[str, float]]) -> DataFrame:
    return spark.createDataFrame(rows, _CONSENSUS_SCHEMA)


def test_weight_from_consensus_log_rank_is_deterministic(spark: SparkSession) -> None:
    df = _consensus(spark, [("a.com", 0.9), ("b.org", 0.5)])
    w = {r["domain"]: r["w"] for r in weight_from_consensus(df, "log_rank", 10).collect()}
    assert w["a.com"] == pytest.approx(1.0 / math.log2(2))  # rank 1
    assert w["b.org"] == pytest.approx(1.0 / math.log2(3))  # rank 2
    assert w["a.com"] > w["b.org"]


def test_weight_from_consensus_breaks_score_ties_by_domain(spark: SparkSession) -> None:
    df = _consensus(spark, [("b.org", 0.5), ("a.com", 0.5)])  # equal score
    w = {r["domain"]: r["w"] for r in weight_from_consensus(df, "log_rank", 10).collect()}
    assert w["a.com"] > w["b.org"]  # domain asc tiebreak → a ranked first


def test_weight_from_consensus_collapses_duplicate_domains(spark: SparkSession) -> None:
    df = _consensus(spark, [("a.com", 0.3), ("a.com", 0.9), ("b.org", 0.5)])
    rows = weight_from_consensus(df, "log_rank", 10).collect()
    domains = [r["domain"] for r in rows]
    assert domains.count("a.com") == 1  # collapsed
    w = {r["domain"]: r["w"] for r in rows}
    assert w["a.com"] > w["b.org"]  # collapsed to max(0.9) → ranked above b (0.5)


def test_weight_from_consensus_applies_top_n_cut(spark: SparkSession) -> None:
    df = _consensus(spark, [("a.com", 0.9), ("b.org", 0.5), ("c.net", 0.1)])
    domains = {r["domain"] for r in weight_from_consensus(df, "log_rank", 2).collect()}
    assert domains == {"a.com", "b.org"}


def test_weight_from_consensus_score_all_zero_fails(spark: SparkSession) -> None:
    df = _consensus(spark, [("a.com", 0.0), ("b.org", 0.0)])
    with pytest.raises(DataSourceError, match="zero"):
        weight_from_consensus(df, "score", 10).collect()


def test_weight_from_consensus_rejects_unknown_formula(spark: SparkSession) -> None:
    df = _consensus(spark, [("a.com", 0.9)])
    with pytest.raises(ValueError, match="weight"):
        weight_from_consensus(df, "bogus", 10)


_NULLABLE_SEED = T.StructType(
    [
        T.StructField("domain", T.StringType(), nullable=True),
        T.StructField("consensus", T.DoubleType(), nullable=True),
    ]
)


def test_weight_from_consensus_rejects_empty_seed(spark: SparkSession) -> None:
    df = spark.createDataFrame([], _NULLABLE_SEED)
    with pytest.raises(DataSourceError, match="empty"):
        weight_from_consensus(df, "log_rank", 10)


def test_weight_from_consensus_drops_null_consensus(spark: SparkSession) -> None:
    # null consensus = unranked (sparse c-d-r rows); dropped as a non-candidate, not fatal.
    df = spark.createDataFrame([("a.com", 0.9), ("b.org", None)], _NULLABLE_SEED)
    domains = {r["domain"] for r in weight_from_consensus(df, "log_rank", 10).collect()}
    assert domains == {"a.com"}


def test_weight_from_consensus_rejects_all_null_consensus(spark: SparkSession) -> None:
    df = spark.createDataFrame([("a.com", None), ("b.org", None)], _NULLABLE_SEED)
    with pytest.raises(DataSourceError, match="empty"):
        weight_from_consensus(df, "log_rank", 10)


def test_weight_from_consensus_rejects_nan_consensus(spark: SparkSession) -> None:
    df = spark.createDataFrame([("a.com", float("nan"))], _NULLABLE_SEED)
    with pytest.raises(DataSourceError, match="consensus"):
        weight_from_consensus(df, "log_rank", 10)


def test_weight_from_consensus_rejects_negative_consensus(spark: SparkSession) -> None:
    df = spark.createDataFrame([("a.com", -0.5)], _NULLABLE_SEED)
    with pytest.raises(DataSourceError, match="consensus"):
        weight_from_consensus(df, "log_rank", 10)


def test_weight_from_consensus_rejects_null_domain(spark: SparkSession) -> None:
    df = spark.createDataFrame([(None, 0.9)], _NULLABLE_SEED)
    with pytest.raises(DataSourceError, match="domain"):
        weight_from_consensus(df, "log_rank", 10)


def test_weight_from_consensus_rejects_blank_domain(spark: SparkSession) -> None:
    df = spark.createDataFrame([("  ", 0.9)], _NULLABLE_SEED)
    with pytest.raises(DataSourceError, match="domain"):
        weight_from_consensus(df, "log_rank", 10)


def test_to_teleport_vector_normalizes_and_drops_off_graph(spark: SparkSession) -> None:
    weights = spark.createDataFrame(
        [("a.com", 1.0), ("b.org", 1.0), ("z.io", 1.0)], ["domain", "w"]
    )
    vertices = spark.createDataFrame([(0, "a.com"), (1, "b.org"), (2, "c.net")], _VERTEX_SCHEMA)
    out = {r["id"]: r["w"] for r in to_teleport_vector(weights, vertices).collect()}
    assert set(out) == {0, 1}  # z.io off-graph dropped
    assert sum(out.values()) == pytest.approx(1.0)


def test_to_teleport_vector_all_off_graph_fails(spark: SparkSession) -> None:
    weights = spark.createDataFrame([("z.io", 1.0)], ["domain", "w"])
    vertices = spark.createDataFrame([(0, "a.com")], _VERTEX_SCHEMA)
    with pytest.raises(DataSourceError, match="off-graph"):
        to_teleport_vector(weights, vertices)


def test_in_degrees_counts_inbound_over_all_nodes(spark: SparkSession) -> None:
    nodes = spark.createDataFrame([(0,), (1,), (2,)], "id long")
    edges = spark.createDataFrame([(1, 0), (2, 0), (2, 1)], _EDGE_SCHEMA)
    result = {r["id"]: r["in_degree"] for r in in_degrees(edges, nodes).collect()}
    assert result == {0: 2, 1: 1, 2: 0}


def test_to_authority_output_matches_schema_and_log1p_volume(spark: SparkSession) -> None:
    ranks = spark.createDataFrame([(0, 0.6), (1, 0.4)], ["id", "rank"])
    in_deg = spark.createDataFrame([(0, 99), (1, 0)], ["id", "in_degree"])
    vertices = spark.createDataFrame([(0, "a.com"), (1, "b.org")], _VERTEX_SCHEMA)
    out = to_authority_output(ranks, in_deg, vertices, crawl="CC-MAIN-2025-51")
    assert [f.name for f in out.schema] == [f.name for f in CC_DOMAIN_AUTHORITY]
    assert dict(out.dtypes) == {f.name: f.dataType.simpleString() for f in CC_DOMAIN_AUTHORITY}
    rows = {r["domain"]: r for r in out.collect()}
    assert rows["a.com"]["open_authority"] == 0.6
    assert rows["a.com"]["open_volume"] == pytest.approx(math.log1p(99))
    assert rows["b.org"]["open_volume"] == 0.0  # zero in-degree → 0.0
    assert rows["b.org"]["crawl"] == "CC-MAIN-2025-51"
