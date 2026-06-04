"""Unit tests for spark_jobs.common.pagerank — shared power-iteration PageRank."""

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import types as T
from structlog.testing import capture_logs

from spark_jobs.common.pagerank import power_iteration

_NODE_SCHEMA = T.StructType([T.StructField("id", T.LongType(), nullable=False)])
_EDGE_SCHEMA = T.StructType(
    [
        T.StructField("from_id", T.LongType(), nullable=False),
        T.StructField("to_id", T.LongType(), nullable=False),
    ]
)
_TELEPORT_SCHEMA = T.StructType(
    [
        T.StructField("id", T.LongType(), nullable=False),
        T.StructField("w", T.DoubleType(), nullable=True),
    ]
)


def _nodes(spark: SparkSession, ids: list[int]) -> DataFrame:
    return spark.createDataFrame([(i,) for i in ids], _NODE_SCHEMA)


def _edges(spark: SparkSession, pairs: list[tuple[int, int]]) -> DataFrame:
    return spark.createDataFrame(pairs, _EDGE_SCHEMA)


def _teleport(spark: SparkSession, weights: list[tuple[int, float | None]]) -> DataFrame:
    return spark.createDataFrame(weights, _TELEPORT_SCHEMA)


def _ranks(result: DataFrame) -> dict[int, float]:
    return {r["id"]: r["rank"] for r in result.collect()}


def test_power_iteration_conserves_mass(spark: SparkSession) -> None:
    nodes = _nodes(spark, [0, 1, 2])
    edges = _edges(spark, [(1, 0), (2, 0), (2, 1)])
    result = power_iteration(edges, nodes, max_iter=6, tol=1e-9, checkpoint_every=2)
    assert abs(sum(_ranks(result).values()) - 1.0) < 1e-6


def test_power_iteration_logs_each_iteration(spark: SparkSession) -> None:
    nodes = _nodes(spark, [0, 1, 2])
    edges = _edges(spark, [(1, 0), (2, 0), (2, 1)])
    with capture_logs() as logs:
        power_iteration(edges, nodes, max_iter=3, tol=0.0, checkpoint_every=0)
    iters = [e for e in logs if e["event"] == "pagerank_iteration"]
    assert [e["iteration"] for e in iters] == [1, 2, 3]
    assert all("l1_delta" in e and "checkpointed" in e for e in iters)


def test_power_iteration_rejects_empty_node_set(spark: SparkSession) -> None:
    nodes = _nodes(spark, [])
    edges = _edges(spark, [])
    with pytest.raises(ValueError, match="empty"):
        power_iteration(edges, nodes, max_iter=5, tol=1e-9, checkpoint_every=0)


def test_power_iteration_accepts_explicit_n_vertices(spark: SparkSession) -> None:
    # n_vertices lets production skip the nodes.count() scan; the result must match.
    nodes = _nodes(spark, [0, 1, 2])
    edges = _edges(spark, [(1, 0), (2, 0), (2, 1)])
    result = power_iteration(edges, nodes, n_vertices=3, max_iter=6, tol=1e-9, checkpoint_every=2)
    assert abs(sum(_ranks(result).values()) - 1.0) < 1e-6


def test_power_iteration_ranks_inbound_heavy_node_highest(spark: SparkSession) -> None:
    # node 0 receives from 1 and 2; node 1 receives from 2; node 2 receives nothing.
    nodes = _nodes(spark, [0, 1, 2])
    edges = _edges(spark, [(1, 0), (2, 0), (2, 1)])
    ranks = _ranks(power_iteration(edges, nodes, max_iter=6, tol=1e-9, checkpoint_every=2))
    assert ranks[0] > ranks[1] > ranks[2]


def test_power_iteration_conserves_mass_with_dangling_sink(spark: SparkSession) -> None:
    # node 0 is a dangling sink (no out-edges); its mass must be redistributed, not leaked.
    nodes = _nodes(spark, [0, 1, 2, 3])
    edges = _edges(spark, [(1, 0), (2, 0), (3, 0)])
    result = power_iteration(edges, nodes, max_iter=6, tol=1e-9, checkpoint_every=2)
    assert abs(sum(_ranks(result).values()) - 1.0) < 1e-6


def test_power_iteration_personalized_concentrates_on_seed(spark: SparkSession) -> None:
    nodes = _nodes(spark, [0, 1, 2, 3])
    edges = _edges(spark, [(0, 1)])
    teleport = _teleport(spark, [(0, 1.0)])
    ranks = _ranks(
        power_iteration(edges, nodes, max_iter=6, tol=1e-9, teleport=teleport, checkpoint_every=2)
    )
    assert abs(sum(ranks.values()) - 1.0) < 1e-6
    # off-seed isolated node 3 gets no teleport and no inflow → essentially zero.
    assert ranks[3] < 1e-6
    # the seed (and what it links to) hold the mass.
    assert ranks[0] > ranks[3]


def test_power_iteration_rejects_unnormalized_teleport(spark: SparkSession) -> None:
    nodes = _nodes(spark, [0, 1])
    edges = _edges(spark, [(0, 1)])
    teleport = _teleport(spark, [(0, 0.3), (1, 0.3)])  # sums to 0.6
    with pytest.raises(ValueError, match="normalized"):
        power_iteration(edges, nodes, max_iter=5, tol=1e-9, teleport=teleport, checkpoint_every=0)


def test_power_iteration_rejects_null_teleport_weight(spark: SparkSession) -> None:
    nodes = _nodes(spark, [0, 1])
    edges = _edges(spark, [(0, 1)])
    teleport = _teleport(spark, [(0, 1.0), (1, None)])
    with pytest.raises(ValueError, match="null/NaN"):
        power_iteration(edges, nodes, max_iter=5, tol=1e-9, teleport=teleport, checkpoint_every=0)


def test_power_iteration_rejects_teleport_id_outside_graph(spark: SparkSession) -> None:
    nodes = _nodes(spark, [0, 1])
    edges = _edges(spark, [(0, 1)])
    teleport = _teleport(spark, [(9, 1.0)])  # id 9 not in the node set
    with pytest.raises(ValueError, match="outside"):
        power_iteration(edges, nodes, max_iter=5, tol=1e-9, teleport=teleport, checkpoint_every=0)


def test_power_iteration_rejects_invalid_damping(spark: SparkSession) -> None:
    nodes = _nodes(spark, [0, 1])
    edges = _edges(spark, [(0, 1)])
    with pytest.raises(ValueError, match="damping"):
        power_iteration(edges, nodes, max_iter=5, tol=1e-9, damping=1.5, checkpoint_every=0)
