"""Integration test for the graph ingest job."""

import argparse

import pytest
from pyspark.sql import SparkSession

from spark_jobs.common.errors import ConfigError
from spark_jobs.ingest.main import _parse_args, _run


def test_parse_args_rejects_fixture_output(tmp_path) -> None:
    with pytest.raises(ConfigError, match="fixtures"):
        _parse_args(
            [
                "--vertices-path",
                "v",
                "--edges-path",
                "e",
                "--vertices-out",
                "tests/fixtures/parquet/cc_domain_pagerank",
                "--edges-out",
                str(tmp_path / "edges"),
            ]
        )


def test_run_converts_graph_to_staged_parquet(spark: SparkSession, tmp_path) -> None:
    vtsv = tmp_path / "vertices.tsv"
    vtsv.write_text("0\tcom.example\t5\n1\tuk.co.bbc\t3\n")
    etsv = tmp_path / "edges.tsv"
    etsv.write_text("0\t1\n1\t0\n")
    args = argparse.Namespace(
        vertices_path=str(vtsv),
        edges_path=str(etsv),
        vertices_out=str(tmp_path / "v3_map"),
        edges_out=str(tmp_path / "v3_edges"),
    )

    _run(spark, args)

    v3_map = spark.read.parquet(args.vertices_out)
    assert dict(v3_map.dtypes) == {"id": "bigint", "domain": "string"}
    assert {r["id"]: r["domain"] for r in v3_map.collect()} == {0: "example.com", 1: "bbc.co.uk"}

    v3_edges = spark.read.parquet(args.edges_out)
    assert dict(v3_edges.dtypes) == {"from_id": "bigint", "to_id": "bigint"}
    assert {(r["from_id"], r["to_id"]) for r in v3_edges.collect()} == {(0, 1), (1, 0)}
