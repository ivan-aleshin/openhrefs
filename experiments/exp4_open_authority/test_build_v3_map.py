import pytest
from build_v3_map import to_v3_map
from pyspark.sql import SparkSession


@pytest.fixture(scope="module")
def spark():
    s = SparkSession.builder.master("local[2]").appName("exp4.0-test").getOrCreate()
    yield s
    s.stop()


def test_to_v3_map_unreverses_dot_segments(spark):
    rows = [(0, "com.example", 5), (1, "uk.co.bbc", 3), (2, "org.wikipedia", 10)]
    df = spark.createDataFrame(rows, "id long, rev_domain string, num_hosts long")
    out = {r["id"]: r["domain"] for r in to_v3_map(df).collect()}
    assert out == {0: "example.com", 1: "bbc.co.uk", 2: "wikipedia.org"}
