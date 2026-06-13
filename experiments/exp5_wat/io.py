"""I/O for Exp 5 WAT extraction. All reads/writes; no business logic.

cc-index parquet (Hadoop S3A), the staged id-based domain graph (gs), WAT ``.gz`` files
(warcio over an fsspec/s3fs stream), and projection/output parquet + metrics text.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from typing import Any

import fsspec
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from warcio.archiveiterator import ArchiveIterator

from experiments.exp5_wat.transforms import (
    extract_links,
    normalize_content_languages,
    registered_domain_of_url,
)

_CDX_COLUMNS = [
    "url_host_registered_domain",
    "content_languages",
    "fetch_status",
    "warc_filename",
]


def read_cdx_projection(spark: SparkSession, cdx_path: str) -> DataFrame:
    """Read the cc-index columnar table, project the narrow column set, filter 200s.

    ``cdx_path`` points at the crawl partition (``s3a://commoncrawl/cc-index/table/
    cc-main/warc/crawl=CC-MAIN-2026-21/subset=warc``). ``url_host_registered_domain``
    is renamed to ``registered_domain``; ``fetch_status`` is **kept** (spec §3 — aids
    auditing the checkpoint artifact even though it is filtered to 200 here).

    ``content_languages`` schema varies (CC has shipped it both as a comma-separated
    ``string`` and as ``array<string>``); ``normalize_content_languages`` coerces it to a
    comma-separated string so ``primary_language`` can split it uniformly (that helper is
    unit-tested off-cluster — Task 6).
    """
    df = normalize_content_languages(spark.read.parquet(cdx_path).select(*_CDX_COLUMNS))
    return df.where(F.col("fetch_status") == 200).select(
        F.col("url_host_registered_domain").alias("registered_domain"),
        F.col("content_languages"),
        F.col("fetch_status"),
        F.col("warc_filename"),
    )


def read_v3_edges(spark: SparkSession, edges_path: str) -> DataFrame:
    """Read the staged id-based domain edges ``(from_id, to_id)`` (see ingest)."""
    return spark.read.parquet(edges_path).select("from_id", "to_id")


def read_v3_map(spark: SparkSession, v3_map_path: str) -> DataFrame:
    """Read the staged id→domain map ``(id, domain)`` (see ingest)."""
    return spark.read.parquet(v3_map_path).select("id", "domain")


def iter_wat_links(wat_path: str, **storage_options: Any) -> Iterator[dict[str, str | None]]:
    """Iterate anchor links from one WAT file, tagged with the source page's domain.

    Opens ``wat_path`` (``s3a://``/``s3://``/local) via fsspec, walks WARC ``metadata``
    records with warcio, parses each WAT JSON payload, and yields one row per anchor
    href: ``{domain_from, url_from, url_to, anchor, rel}``. ``url_from`` is the record's
    ``WARC-Target-URI`` (the source page); ``domain_from`` is its registered domain.
    Records with no parseable source domain are skipped.
    """
    open_path = wat_path.replace("s3a://", "s3://", 1)
    with fsspec.open(open_path, "rb", **storage_options) as raw:
        for record in ArchiveIterator(raw):
            if record.rec_type != "metadata":
                continue
            try:
                payload = json.loads(record.content_stream().read())
            except (ValueError, UnicodeDecodeError):
                continue
            target_uri = (
                payload.get("Envelope", {}).get("WARC-Header-Metadata", {}).get("WARC-Target-URI")
            )
            domain_from = registered_domain_of_url(target_uri)
            if not domain_from:
                continue
            for link in extract_links(payload):
                yield {
                    "domain_from": domain_from,
                    "url_from": target_uri,
                    "url_to": link["url"],
                    "anchor": link["anchor"],
                    "rel": link["rel"],
                }


def write_parquet(df: DataFrame, path: str) -> None:
    """Overwrite ``df`` to ``path`` as Parquet (experiment outputs are re-runnable)."""
    df.write.mode("overwrite").parquet(path)


def write_json(spark: SparkSession, obj: dict[str, Any], path: str) -> None:
    """Write a single-object metrics summary as one JSON text file under ``path``.

    Uses the Spark writer (Hadoop FS) rather than fsspec: the plan adds ``s3fs`` but not
    ``gcsfs``, so an ``fsspec.open("gs://…")`` would fail. ``coalesce(1)`` so the summary
    lands as a single ``part-*`` file in the ``path`` directory.
    """
    payload = json.dumps(obj, indent=2)
    spark.createDataFrame([(payload,)], ["json"]).coalesce(1).write.mode("overwrite").text(path)
