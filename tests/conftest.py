"""Shared pytest fixtures for the openhrefs test suite."""

from collections.abc import Iterator

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    """Session-scoped local SparkSession for unit-testing pure transforms.

    Uses ``local[2]`` with a single shuffle partition to keep synthetic-data
    tests fast. The JVM starts lazily on first use, so tests that never request
    this fixture incur no Spark/JVM startup cost.
    """
    session = (
        SparkSession.builder.master("local[2]")
        .appName("openhrefs-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()
