"""Tests for the shared V3 dense-domain-graph boundary validation."""

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import types as T

from spark_jobs.common.errors import DataSourceError
from spark_jobs.common.graph_validation import validate_graph

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


def _verts(spark: SparkSession, rows: list[tuple[int, str]]) -> DataFrame:
    return spark.createDataFrame(rows, _VERTEX_SCHEMA)


def _edges(spark: SparkSession, rows: list[tuple[int, int]]) -> DataFrame:
    return spark.createDataFrame(rows, _EDGE_SCHEMA)


def test_validate_graph_accepts_dense_graph(spark: SparkSession) -> None:
    verts = _verts(spark, [(0, "a"), (1, "b"), (2, "c")])
    edges = _edges(spark, [(0, 1), (1, 2)])
    validate_graph(verts, edges, n=3)  # no raise


def test_validate_graph_rejects_duplicate_ids(spark: SparkSession) -> None:
    verts = _verts(spark, [(0, "a"), (1, "b"), (1, "c")])
    with pytest.raises(DataSourceError, match="dense"):
        validate_graph(verts, _edges(spark, [(0, 1)]), n=3)


def test_validate_graph_rejects_non_contiguous_ids(spark: SparkSession) -> None:
    verts = _verts(spark, [(0, "a"), (1, "b"), (3, "c")])  # gap: max=3, n-1=2
    with pytest.raises(DataSourceError, match="dense"):
        validate_graph(verts, _edges(spark, [(0, 1)]), n=3)


def test_validate_graph_rejects_edge_id_out_of_range(spark: SparkSession) -> None:
    verts = _verts(spark, [(0, "a"), (1, "b"), (2, "c")])
    edges = _edges(spark, [(0, 5)])  # to_id 5 outside [0, 3)
    with pytest.raises(DataSourceError, match="outside"):
        validate_graph(verts, edges, n=3)


def test_validate_graph_rejects_null_edge_endpoint(spark: SparkSession) -> None:
    verts = _verts(spark, [(0, "a"), (1, "b"), (2, "c")])
    nullable_edges = T.StructType(
        [
            T.StructField("from_id", T.LongType(), nullable=True),
            T.StructField("to_id", T.LongType(), nullable=True),
        ]
    )
    edges = spark.createDataFrame([(0, 1), (None, 2)], nullable_edges)
    with pytest.raises(DataSourceError, match="null"):
        validate_graph(verts, edges, n=3)


def test_validate_graph_rejects_duplicate_domain(spark: SparkSession) -> None:
    # dense unique ids, but two ids map to the same domain (the mart key must be unique).
    verts = _verts(spark, [(0, "a.com"), (1, "a.com"), (2, "c.net")])
    with pytest.raises(DataSourceError, match="domain"):
        validate_graph(verts, _edges(spark, [(0, 1)]), n=3)


def test_validate_graph_rejects_null_domain(spark: SparkSession) -> None:
    nullable_verts = T.StructType(
        [
            T.StructField("id", T.LongType(), nullable=False),
            T.StructField("domain", T.StringType(), nullable=True),
        ]
    )
    verts = spark.createDataFrame([(0, "a"), (1, None), (2, "c")], nullable_verts)
    with pytest.raises(DataSourceError, match="null/blank domain"):
        validate_graph(verts, _edges(spark, [(0, 1)]), n=3)


def test_validate_graph_rejects_blank_domain(spark: SparkSession) -> None:
    # A null rev_domain becomes "" through concat_ws, not null — the blank must be rejected.
    verts = _verts(spark, [(0, "a"), (1, "  "), (2, "c")])
    with pytest.raises(DataSourceError, match="null/blank domain"):
        validate_graph(verts, _edges(spark, [(0, 1)]), n=3)


def test_validate_graph_rejects_duplicate_identical_vertex_rows(spark: SparkSession) -> None:
    # distinct(id)==3, dense min/max, distinct(domain)==3 — passes every check except the
    # row count. Without it the duplicate id=0 double-counts in power_iteration.
    verts = _verts(spark, [(0, "a"), (0, "a"), (1, "b"), (2, "c")])
    with pytest.raises(DataSourceError, match="duplicate id rows"):
        validate_graph(verts, _edges(spark, [(0, 1)]), n=3)
