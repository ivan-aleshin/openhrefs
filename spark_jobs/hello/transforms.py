"""Pure DataFrame transforms for the hello-world CDX row-count job."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def drop_empty_lines(df: DataFrame) -> DataFrame:
    """Remove empty or whitespace-only lines from a raw text DataFrame."""
    return df.filter(F.trim(F.col("value")) != "")
