"""Shared pytest fixtures for the openhrefs test suite."""

import tempfile
from collections.abc import Iterator

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    """Session-scoped local SparkSession for unit-testing pure transforms.

    Uses ``local[2]`` with a single shuffle partition to keep synthetic-data
    tests fast. The JVM starts lazily on first use, so tests that never request
    this fixture incur no Spark/JVM startup cost. A checkpoint directory is set
    so iterative transforms (PageRank) can truncate lineage in tests as they do
    in production.
    """
    with tempfile.TemporaryDirectory() as checkpoint_dir:
        session = (
            SparkSession.builder.master("local[2]")
            .appName("openhrefs-tests")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )
        session.sparkContext.setCheckpointDir(checkpoint_dir)
        yield session
        session.stop()
