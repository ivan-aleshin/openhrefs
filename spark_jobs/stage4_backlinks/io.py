"""I/O for Stage 4 backlinks extraction. All reads/writes; no business logic.

Reads WAT slice lists and WAT files via fsspec + fastwarc + orjson; writes
``cc_domain_link_pairs`` Parquet and run-level metrics JSON via the Spark writer.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Iterator
from typing import Any

import fsspec
import orjson
from fastwarc.warc import ArchiveIterator, WarcRecordType
from pyspark.sql import DataFrame, SparkSession

from spark_jobs.stage4_backlinks.transforms import links_from_payload


def read_wat_list(path: str, **opts: Any) -> list[str]:
    """Read a plain-text WAT slice list, one path per line.

    Opens ``path`` via fsspec in text mode. Returns stripped non-empty lines
    in order — the same canonical view as ``manifest_sha256`` in transforms,
    so the hash and the resolved paths describe one identical set.

    Args:
        path: fsspec-addressable path to a plain-text file (``gs://``, ``s3://``,
            or local) listing one WAT path per line.
        **opts: Additional keyword arguments forwarded to ``fsspec.open`` as
            storage options.

    Returns:
        List of stripped non-empty lines.
    """
    with fsspec.open(path, "r", **opts) as f:
        return [line.strip() for line in f if line.strip()]


def iter_wat_links_raw(
    wat_path: str,
    on_record: Callable[[], None] | None = None,
    on_malformed: Callable[[], None] | None = None,
    on_no_links: Callable[[], None] | None = None,
    **storage_options: Any,
) -> Iterator[dict[str, str | None]]:
    """Yield raw anchor-link rows ``{url_from, url_to, anchor, rel}`` from one WAT file
    via fastwarc + orjson (``s3a://``→``s3://``; gzip auto-detected, no ``compression=``).
    Optional callbacks wrap Spark accumulators (production) / counters (tests) and count
    metadata RECORDS, not files: ``on_record`` once per record before parse;
    ``on_malformed`` on a per-record JSON decode failure (record skipped, stream
    continues); ``on_no_links`` on a well-formed zero-link record. Whole-file read/gzip
    failures propagate — the caller counts ``files_unreadable``.
    """
    open_path = wat_path.replace("s3a://", "s3://", 1)
    with fsspec.open(open_path, "rb", **storage_options) as stream:
        for rec in ArchiveIterator(stream, record_types=WarcRecordType.metadata):
            if on_record is not None:
                on_record()
            try:
                payload = orjson.loads(rec.reader.read())
            except orjson.JSONDecodeError:
                if on_malformed is not None:
                    on_malformed()
                continue
            rows = links_from_payload(payload)
            if not rows and on_no_links is not None:
                on_no_links()
            yield from rows


def write_domain_link_pairs(df: DataFrame, path: str) -> None:
    """Write ``cc_domain_link_pairs`` partitioned by ``crawl``.

    Dynamic partition overwrite so only the affected crawl partitions are
    replaced; unrelated crawls in the same output path are preserved.

    Args:
        df: DataFrame with the ``CC_DOMAIN_LINK_PAIRS`` columns plus ``crawl``.
        path: Destination path (``gs://``, ``s3://``, or local).
    """
    (
        df.write.option("partitionOverwriteMode", "dynamic")
        .mode("overwrite")
        .partitionBy("crawl")
        .parquet(path)
    )


def write_metrics_json(spark: SparkSession, obj: dict[str, Any], path: str) -> None:
    """Write a metrics dict as a single JSON text file under ``path``.

    Uses the Spark/Hadoop FS writer so the output mechanism is uniform across
    every storage scheme regardless of which fsspec backends are installed.
    ``coalesce(1)`` ensures a single ``part-*`` file in the ``path`` directory.

    Args:
        spark: Active SparkSession.
        obj: Metrics dict to serialize. Non-serializable values are coerced
            via ``default=str``.
        path: Destination directory path. A single ``part-*`` file is written
            inside it.
    """
    payload = json.dumps(obj, indent=2, default=str)
    spark.createDataFrame([(payload,)], ["json"]).coalesce(1).write.mode("overwrite").text(path)
