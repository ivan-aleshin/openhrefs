"""Unit tests for Stage 2 OpenPageRank validation transforms."""

from pyspark.sql import SparkSession

from spark_jobs.validation.stage2_opr import transforms


def test_normalize_opr_collapses_hosts_to_registered_max(spark: SparkSession) -> None:
    raw = spark.createDataFrame(
        [
            ("www.facebook.com", "9.0"),
            ("m.facebook.com", "7.0"),
            ("facebook.com", "8.0"),
            ("fonts.googleapis.com", "6.5"),
            ("8.8.8.8", "5.0"),
            (None, "1.0"),
        ],
        "Domain string, `Open Page Rank` string",
    )

    out = {r["domain"]: r["opr_score"] for r in transforms.normalize_opr(raw).collect()}

    assert out == {"facebook.com": 9.0, "googleapis.com": 6.5}


def test_normalize_opr_honors_column_overrides(spark: SparkSession) -> None:
    raw = spark.createDataFrame([("bbc.co.uk", "4.2")], "host string, score string")

    out = transforms.normalize_opr(raw, domain_col="host", score_col="score").collect()

    assert (out[0]["domain"], out[0]["opr_score"]) == ("bbc.co.uk", 4.2)


def test_to_overlap_inner_joins_on_domain(spark: SparkSession) -> None:
    pagerank = spark.createDataFrame(
        [("a.com", 0.1), ("b.com", 0.2), ("c.com", 0.3)],
        "domain string, pagerank_score double",
    )
    opr = spark.createDataFrame([("a.com", 9.0), ("c.com", 7.0)], "domain string, opr_score double")

    out = {
        r["domain"]: (r["our_pr"], r["opr_score"])
        for r in transforms.to_overlap(pagerank, opr).collect()
    }

    assert out == {"a.com": (0.1, 9.0), "c.com": (0.3, 7.0)}


def test_filter_crawl_keeps_only_requested_crawl(spark: SparkSession) -> None:
    pagerank = spark.createDataFrame(
        [("a.com", 0.1, "c1"), ("b.com", 0.2, "c2"), ("c.com", 0.3, "c1")],
        "domain string, pagerank_score double, crawl string",
    )

    out = {r["domain"] for r in transforms.filter_crawl(pagerank, "c1").collect()}

    assert out == {"a.com", "c.com"}


def test_filter_crawl_passthrough_when_no_crawl_column(spark: SparkSession) -> None:
    pagerank = spark.createDataFrame(
        [("a.com", 0.1), ("b.com", 0.2)], "domain string, pagerank_score double"
    )

    assert transforms.filter_crawl(pagerank, "c1").count() == 2


def test_output_stats_reports_mass_and_nulls(spark: SparkSession) -> None:
    pagerank = spark.createDataFrame(
        [("a.com", 0.5), ("b.com", 0.3), ("c.com", None), (None, 0.1)],
        "domain string, pagerank_score double",
    )

    row = transforms.output_stats(pagerank).collect()[0]

    assert row["pagerank_count"] == 4
    assert row["null_count"] == 1
    assert row["domain_null_count"] == 1
    assert abs(row["pagerank_sum"] - 0.9) < 1e-9
    assert row["pagerank_min"] == 0.1
    assert row["pagerank_max"] == 0.5
