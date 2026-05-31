import pathlib

import pytest
from normalize_ref import normalize_ref_domains
from pyspark.sql import SparkSession


@pytest.fixture(scope="module")
def spark():
    s = SparkSession.builder.master("local[2]").appName("exp4-normref").getOrCreate()
    # normalize_ref's PSL UDF runs on Spark workers, which DON'T inherit the driver's runtime
    # sys.path (conftest insert) — ship domain_utils.py to them, mirroring the real submit's
    # `--py-files domain_utils.py`. Without this the UDF fails with ModuleNotFoundError off-cwd.
    s.sparkContext.addPyFile(str(pathlib.Path(__file__).resolve().parent / "domain_utils.py"))
    yield s
    s.stop()


def test_normalize_ref_dedupes_subdomains_to_registered_max_tf(spark):
    rows = [
        ("www.example.com", "50.0", "40.0"),
        ("blog.example.com", "70.0", "30.0"),
        ("other.org", "10.0", "5.0"),
    ]
    df = spark.createDataFrame(rows, ["host", "tf", "cf"])
    out = {
        r["domain"]: (r["tf"], r["cf"])
        for r in normalize_ref_domains(df, "host", "tf", "cf").collect()
    }
    assert out["example.com"] == (70.0, 40.0)  # tf/cf maxed per registered domain
    assert out["other.org"] == (10.0, 5.0)


def test_normalize_ref_drops_null_or_noncastable_tf(spark):
    # non-castable tf casts to null; tf drives the gate, a null would poison Spearman → drop it
    rows = [("a.com", "n/a", "1.0"), ("b.com", "55.0", "2.0")]
    df = spark.createDataFrame(rows, ["host", "tf", "cf"])
    out = {r["domain"] for r in normalize_ref_domains(df, "host", "tf", "cf").collect()}
    assert out == {"b.com"}
