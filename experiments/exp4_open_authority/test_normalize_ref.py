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


_COLS = ["host", "trust", "volume", "status", "rd", "eb"]
_KW = dict(status_col="status", refdomains_col="rd", extbacklinks_col="eb")


def test_normalize_ref_dedupes_to_registered_domain_maxes_numerics_and_best_status(spark):
    rows = [
        ("www.example.com", "50.0", "40.0", "Found", "100", "1000"),
        ("blog.example.com", "70.0", "30.0", "MayExist", "200", "2000"),
        ("other.org", "10.0", "5.0", "Found", "3", "30"),
    ]
    df = spark.createDataFrame(rows, _COLS)
    out = {
        r["domain"]: r
        for r in normalize_ref_domains(df, "host", "trust", "volume", **_KW).collect()
    }
    ex = out["example.com"]
    assert (ex["ref_trust"], ex["ref_volume"]) == (70.0, 40.0)  # maxed independently per domain
    assert ex["ref_domains"] == 200 and ex["ext_backlinks"] == 2000
    assert ex["status"] == "Found"  # Found outranks MayExist in the priority agg
    assert out["other.org"]["ref_trust"] == 10.0


def test_normalize_ref_keeps_zero_trust_but_drops_null(spark):
    # found-but-zero-trust must survive (a primary result); a non-castable trust → null → dropped
    rows = [
        ("a.com", "n/a", "1.0", "MayExist", "0", "0"),
        ("b.com", "0", "2.0", "NotFound", "0", "0"),
        ("c.com", "55.0", "3.0", "Found", "5", "9"),
    ]
    df = spark.createDataFrame(rows, _COLS)
    out = {
        r["domain"]: r
        for r in normalize_ref_domains(df, "host", "trust", "volume", **_KW).collect()
    }
    assert set(out) == {"b.com", "c.com"}  # a.com (null trust) dropped; b.com (zero) kept
    assert out["b.com"]["ref_trust"] == 0.0 and out["b.com"]["status"] == "NotFound"
