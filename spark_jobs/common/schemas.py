"""Explicit Spark schemas for stage outputs (SPEC.md §5).

Schemas are declared explicitly rather than inferred: CommonCrawl source
inference is slow and unreliable, and pinning output schemas keeps the
canonical Parquet contract stable across crawls and adapters.

Only the graph/authority datasets are defined here. Link-level schemas
(cc_backlinks, cc_outbound_links) are added with Stage 4, where their fields
are finalized.
"""

from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

LANGUAGE_SHARE = StructType(
    [
        StructField("lang", StringType(), nullable=False),
        StructField("share", DoubleType(), nullable=False),
    ]
)

CC_DOMAIN_LANGUAGES = StructType(
    [
        StructField("domain", StringType(), nullable=False),
        StructField("crawl", StringType(), nullable=False),
        StructField("languages", ArrayType(LANGUAGE_SHARE), nullable=False),
        StructField("page_count", LongType(), nullable=False),
    ]
)

CC_DOMAIN_PAGERANK = StructType(
    [
        StructField("domain", StringType(), nullable=False),
        StructField("crawl", StringType(), nullable=False),
        StructField("pagerank_score", DoubleType(), nullable=False),
        StructField("in_degree", LongType(), nullable=False),
        StructField("out_degree", LongType(), nullable=False),
    ]
)

CC_DOMAIN_AUTHORITY = StructType(
    [
        StructField("domain", StringType(), nullable=False),
        StructField("crawl", StringType(), nullable=False),
        StructField("open_authority", DoubleType(), nullable=False),
        StructField("open_volume", DoubleType(), nullable=False),
    ]
)
