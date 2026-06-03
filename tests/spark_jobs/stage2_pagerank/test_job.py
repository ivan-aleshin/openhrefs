"""Integration + boundary-validation tests for the Stage 2 PageRank job."""

import argparse

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import types as T

from spark_jobs.common.errors import ConfigError, DataSourceError, SparkJobError
from spark_jobs.stage2_pagerank import io
from spark_jobs.stage2_pagerank.main import (
    _checked_output_path,
    _resolve_n_vertices,
    _run,
    _validate_graph,
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


def test_checked_output_path_refuses_committed_fixtures() -> None:
    with pytest.raises(ConfigError, match="fixtures"):
        _checked_output_path("tests/fixtures/parquet/cc_domain_pagerank")


def test_checked_output_path_allows_other_locations() -> None:
    assert _checked_output_path("/tmp/build/raw/cc_domain_pagerank") == (
        "/tmp/build/raw/cc_domain_pagerank"
    )


def test_read_edges_rejects_wrong_column_type(spark: SparkSession, tmp_path) -> None:
    path = str(tmp_path / "edges")
    spark.createDataFrame([("x", "y")], "from_id string, to_id string").write.parquet(path)
    with pytest.raises(DataSourceError, match="type"):
        io.read_edges(spark, path)


def test_read_edges_accepts_valid_schema(spark: SparkSession, tmp_path) -> None:
    path = str(tmp_path / "edges")
    spark.createDataFrame([(1, 0)], _EDGE_SCHEMA).write.parquet(path)
    assert dict(io.read_edges(spark, path).dtypes) == {"from_id": "bigint", "to_id": "bigint"}


def test_read_vertices_rejects_missing_column(spark: SparkSession, tmp_path) -> None:
    path = str(tmp_path / "verts")
    spark.createDataFrame([(0,)], "id long").write.parquet(path)
    with pytest.raises(DataSourceError, match="missing"):
        io.read_vertices(spark, path)


def test_write_pagerank_preserves_other_crawl_partitions(spark: SparkSession, tmp_path) -> None:
    out = str(tmp_path / "out")
    schema = "domain string, crawl string, pagerank_score double, in_degree long, out_degree long"

    def _row(crawl: str) -> DataFrame:
        return spark.createDataFrame([("a.com", crawl, 0.5, 1, 1)], schema)

    io.write_pagerank(_row("CC-MAIN-A"), out)
    io.write_pagerank(_row("CC-MAIN-B"), out)  # must not wipe the CC-MAIN-A partition
    crawls = {r["crawl"] for r in spark.read.parquet(out).collect()}
    assert crawls == {"CC-MAIN-A", "CC-MAIN-B"}


def _verts(spark: SparkSession, rows: list[tuple[int, str]]) -> DataFrame:
    return spark.createDataFrame(rows, _VERTEX_SCHEMA)


def _edges(spark: SparkSession, rows: list[tuple[int, int]]) -> DataFrame:
    return spark.createDataFrame(rows, _EDGE_SCHEMA)


def test_validate_graph_accepts_dense_graph(spark: SparkSession) -> None:
    verts = _verts(spark, [(0, "a"), (1, "b"), (2, "c")])
    edges = _edges(spark, [(0, 1), (1, 2)])
    _validate_graph(verts, edges, n=3)  # no raise


def test_validate_graph_rejects_duplicate_ids(spark: SparkSession) -> None:
    verts = _verts(spark, [(0, "a"), (1, "b"), (1, "c")])
    with pytest.raises(DataSourceError, match="dense"):
        _validate_graph(verts, _edges(spark, [(0, 1)]), n=3)


def test_validate_graph_rejects_non_contiguous_ids(spark: SparkSession) -> None:
    verts = _verts(spark, [(0, "a"), (1, "b"), (3, "c")])  # gap: max=3, n-1=2
    with pytest.raises(DataSourceError, match="dense"):
        _validate_graph(verts, _edges(spark, [(0, 1)]), n=3)


def test_validate_graph_rejects_edge_id_out_of_range(spark: SparkSession) -> None:
    verts = _verts(spark, [(0, "a"), (1, "b"), (2, "c")])
    edges = _edges(spark, [(0, 5)])  # to_id 5 outside [0, 3)
    with pytest.raises(DataSourceError, match="outside"):
        _validate_graph(verts, edges, n=3)


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
        _validate_graph(verts, edges, n=3)


def test_validate_graph_rejects_duplicate_domain(spark: SparkSession) -> None:
    # dense unique ids, but two ids map to the same domain (the mart key must be unique).
    verts = _verts(spark, [(0, "a.com"), (1, "a.com"), (2, "c.net")])
    with pytest.raises(DataSourceError, match="domain"):
        _validate_graph(verts, _edges(spark, [(0, 1)]), n=3)


def test_validate_graph_rejects_null_domain(spark: SparkSession) -> None:
    nullable_verts = T.StructType(
        [
            T.StructField("id", T.LongType(), nullable=False),
            T.StructField("domain", T.StringType(), nullable=True),
        ]
    )
    verts = spark.createDataFrame([(0, "a"), (1, None), (2, "c")], nullable_verts)
    with pytest.raises(DataSourceError, match="null domain"):
        _validate_graph(verts, _edges(spark, [(0, 1)]), n=3)


def test_resolve_n_vertices_returns_count(spark: SparkSession) -> None:
    assert _resolve_n_vertices(_verts(spark, [(0, "a"), (1, "b")]), None) == 2


def test_resolve_n_vertices_rejects_mismatch(spark: SparkSession) -> None:
    with pytest.raises(SparkJobError, match="does not match"):
        _resolve_n_vertices(_verts(spark, [(0, "a"), (1, "b")]), 99)


def _job_args(tmp_path, **overrides) -> argparse.Namespace:
    base = {
        "edges_path": str(tmp_path / "edges"),
        "vertices_path": str(tmp_path / "verts"),
        "output_path": str(tmp_path / "out"),
        "crawl": "CC-MAIN-2025-51",
        "max_iter": 8,
        "tol": 1e-9,
        "damping": 0.85,
        "edge_partitions": None,
        "checkpoint_every": 2,
        "checkpoint_dir": str(tmp_path / "ck"),
        "n_vertices": None,
    }
    base.update(overrides)
    return argparse.Namespace(**base)


def test_run_writes_conserved_pagerank(spark: SparkSession, tmp_path) -> None:
    args = _job_args(tmp_path)
    _edges(spark, [(1, 0), (2, 0), (2, 1), (3, 0)]).write.parquet(args.edges_path)
    _verts(spark, [(0, "a.com"), (1, "b.org"), (2, "c.net"), (3, "d.io")]).write.parquet(
        args.vertices_path
    )
    _run(spark, args)
    rows = {r["domain"]: r for r in spark.read.parquet(args.output_path).collect()}
    assert len(rows) == 4
    assert abs(sum(r["pagerank_score"] for r in rows.values()) - 1.0) < 1e-6
    assert (rows["a.com"]["in_degree"], rows["a.com"]["out_degree"]) == (3, 0)
    assert {r["crawl"] for r in rows.values()} == {"CC-MAIN-2025-51"}
