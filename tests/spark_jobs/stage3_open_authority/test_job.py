"""Integration + io tests for the Stage 3 open_authority job."""

import argparse

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import types as T

from spark_jobs.common.config import load_pipeline_config
from spark_jobs.common.errors import ConfigError, DataSourceError
from spark_jobs.stage3_open_authority import io
from spark_jobs.stage3_open_authority.main import _parse_args, _run


def _parse(*extra: str) -> argparse.Namespace:
    base = [
        "--edges-path",
        "e",
        "--vertices-path",
        "v",
        "--seed-path",
        "s.csv",
        "--crawl",
        "C",
        "--output-path",
        "/tmp/o",
    ]
    return _parse_args(base + list(extra))


def test_parse_args_sources_seed_and_authority_from_config() -> None:
    args = _parse()
    cfg = load_pipeline_config()
    assert args.seed_size == cfg.seed.size
    assert args.seed_weight == cfg.seed.weight
    assert args.damping == cfg.authority.damping


def test_parse_args_rejects_invalid_seed_weight_override() -> None:
    with pytest.raises(ConfigError, match="override"):
        _parse("--seed-weight", "bogus")


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


def test_read_seed_csv_maps_to_canonical_schema(spark: SparkSession, tmp_path) -> None:
    path = tmp_path / "seed.csv"
    path.write_text("registered_domain,consensus_score\na.com,0.9\nb.org,0.5\n")
    out = io.read_seed(spark, str(path))
    assert dict(out.dtypes) == {"domain": "string", "consensus": "double"}
    assert {r["domain"]: r["consensus"] for r in out.collect()} == {"a.com": 0.9, "b.org": 0.5}


def test_read_seed_parquet(spark: SparkSession, tmp_path) -> None:
    path = str(tmp_path / "seed_pq")
    spark.createDataFrame(
        [("a.com", 0.9)], "registered_domain string, consensus_score double"
    ).write.parquet(path)
    out = io.read_seed(spark, path)
    assert {r["domain"]: r["consensus"] for r in out.collect()} == {"a.com": 0.9}


def test_read_seed_rejects_missing_source_column(spark: SparkSession, tmp_path) -> None:
    path = tmp_path / "bad.csv"
    path.write_text("registered_domain,score\na.com,0.9\n")
    with pytest.raises(DataSourceError, match="missing"):
        io.read_seed(spark, str(path))


def test_write_authority_preserves_other_crawl_partitions(spark: SparkSession, tmp_path) -> None:
    out = str(tmp_path / "out")
    schema = "domain string, crawl string, open_authority double, open_volume double"

    def _row(crawl: str) -> DataFrame:
        return spark.createDataFrame([("a.com", crawl, 0.5, 1.0)], schema)

    io.write_authority(_row("CC-MAIN-A"), out)
    io.write_authority(_row("CC-MAIN-B"), out)  # must not wipe the CC-MAIN-A partition
    crawls = {r["crawl"] for r in spark.read.parquet(out).collect()}
    assert crawls == {"CC-MAIN-A", "CC-MAIN-B"}


def _job_args(tmp_path, **overrides) -> argparse.Namespace:
    base = {
        "edges_path": str(tmp_path / "edges"),
        "vertices_path": str(tmp_path / "verts"),
        "seed_path": str(tmp_path / "seed.csv"),
        "output_path": str(tmp_path / "out"),
        "crawl": "CC-MAIN-2025-51",
        "max_iter": 8,
        "tol": 1e-9,
        "damping": 0.85,
        "seed_size": 10,
        "seed_weight": "log_rank",
        "edge_partitions": None,
        "checkpoint_every": 2,
        "checkpoint_dir": str(tmp_path / "ck"),
        "n_vertices": None,
    }
    base.update(overrides)
    return argparse.Namespace(**base)


def test_run_writes_conserved_open_authority(spark: SparkSession, tmp_path) -> None:
    args = _job_args(tmp_path)
    spark.createDataFrame([(1, 0), (2, 0), (2, 1)], _EDGE_SCHEMA).write.parquet(args.edges_path)
    spark.createDataFrame([(0, "a.com"), (1, "b.org"), (2, "c.net")], _VERTEX_SCHEMA).write.parquet(
        args.vertices_path
    )
    (tmp_path / "seed.csv").write_text("registered_domain,consensus_score\na.com,0.9\nb.org,0.5\n")

    _run(spark, args)

    out = spark.read.parquet(args.output_path)
    assert [f.name for f in out.schema] == ["domain", "open_authority", "open_volume", "crawl"]
    assert dict(out.dtypes)["open_authority"] == "double"
    rows = {r["domain"]: r for r in out.collect()}
    assert len(rows) == 3
    assert abs(sum(r["open_authority"] for r in rows.values()) - 1.0) < 1e-6
    assert rows["a.com"]["open_authority"] > rows["c.net"]["open_authority"]  # seed + inbound
    assert {r["crawl"] for r in rows.values()} == {"CC-MAIN-2025-51"}
