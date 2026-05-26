"""I/O for the hello-world CDX row-count job."""

from pyspark.sql import DataFrame, SparkSession


def read_cdx_text(spark: SparkSession, path: str) -> DataFrame:
    """Read a CDX cluster-index file as raw text lines.

    Each row has a single column ``value`` (StringType). Handles gzip
    transparently (Spark infers compression from the file extension).
    """
    return spark.read.text(path)
