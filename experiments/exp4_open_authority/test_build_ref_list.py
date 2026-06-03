import pytest
from build_ref_list import stratified_ref_list
from pyspark.sql import SparkSession


@pytest.fixture(scope="module")
def spark():
    s = SparkSession.builder.master("local[2]").appName("exp4-reflist").getOrCreate()
    yield s
    s.stop()


def test_stratified_ref_list_downsamples_large_bucket_keeps_small(spark):
    # bucket log10(0.1)=-1 has 2 domains; bucket log10(0.001)=-3 has 1000. Per-bucket sampling
    # must keep the small bucket whole and downsample the large one — a uniform sample would
    # drown the small bucket. head=1 forces the single top-OA domain in regardless.
    rows = [("a0.com", 0.1), ("a1.com", 0.1)] + [(f"b{i}.com", 0.001) for i in range(1000)]
    dom = spark.createDataFrame(rows, ["domain", "oa"])
    out = {
        r["domain"]
        for r in stratified_ref_list(dom, head=1, per_bucket=5, max_domains=50).collect()
    }
    assert "a0.com" in out and "a1.com" in out  # small bucket (frac 1.0) kept whole
    assert sum(d.startswith("b") for d in out) < 30  # large bucket downsampled from 1000
    assert len(out) <= 50  # hard cap respected


def test_stratified_ref_list_keeps_head_caps_and_dedupes(spark):
    rows = [("h0.com", 1.0), ("h1.com", 0.5)] + [(f"t{i}.com", 0.01) for i in range(8)]
    dom = spark.createDataFrame(rows, ["domain", "oa"])
    out = [
        r["domain"]
        for r in stratified_ref_list(dom, head=2, per_bucket=100, max_domains=5).collect()
    ]
    assert "h0.com" in out and "h1.com" in out  # top-2 by OA always included
    assert len(out) == 5  # filled exactly to the hard cap
    assert len(out) == len(set(out))  # no duplicates (head not double-counted)
    assert set(out) <= {r[0] for r in rows}  # every output domain came from the input
