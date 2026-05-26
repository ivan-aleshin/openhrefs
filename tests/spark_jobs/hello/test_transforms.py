"""Unit tests for spark_jobs.hello.transforms."""

from pyspark.sql import SparkSession

from spark_jobs.hello.transforms import drop_empty_lines


def test_drop_empty_lines_removes_blank_rows(spark: SparkSession) -> None:
    df = spark.createDataFrame([("line one",), ("",), ("  ",), ("line two",)], ["value"])
    result = drop_empty_lines(df)
    assert result.count() == 2
    values = {r["value"] for r in result.collect()}
    assert values == {"line one", "line two"}


def test_drop_empty_lines_all_empty(spark: SparkSession) -> None:
    df = spark.createDataFrame([("",), ("   ",)], ["value"])
    assert drop_empty_lines(df).count() == 0


def test_drop_empty_lines_no_empty(spark: SparkSession) -> None:
    rows = [("com,example)/ 20241201120000 {}",), ("com,foo)/ 20241201120001 {}",)]
    df = spark.createDataFrame(rows, ["value"])
    result = drop_empty_lines(df)
    assert result.count() == 2


def test_drop_empty_lines_null_value(spark: SparkSession) -> None:
    # read.text() never yields nulls, but null propagation through F.trim != "" is safe.
    df = spark.createDataFrame([(None,), ("line",)], ["value"])
    result = drop_empty_lines(df)
    assert result.count() == 1
