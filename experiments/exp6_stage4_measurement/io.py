"""I/O for Exp 6 measurement. All reads/writes; no business logic."""

from __future__ import annotations

import gzip
import json
from collections.abc import Callable, Iterator
from typing import Any

import fsspec
from pyspark.sql import DataFrame, SparkSession
from warcio.archiveiterator import ArchiveIterator

from experiments.exp6_stage4_measurement.transforms import links_from_payload


def read_wat_manifest(manifest_path: str, **storage_options: Any) -> list[str]:
    """Read a CommonCrawl ``wat.paths.gz`` manifest into a list of relative WAT paths."""
    with fsspec.open(manifest_path, "rb", **storage_options) as raw:
        text = gzip.decompress(raw.read()).decode()
    return [line for line in text.splitlines() if line.strip()]


def iter_wat_links_raw(
    wat_path: str,
    on_record: Callable[[], None] | None = None,
    on_malformed: Callable[[], None] | None = None,
    on_no_links: Callable[[], None] | None = None,
    **storage_options: Any,
) -> Iterator[dict[str, str | None]]:
    """Iterate raw anchor-link rows from one WAT file (no domain extraction).

    Opens ``wat_path`` (``s3://``/``gs://``/``file://``) via fsspec, walks WARC
    ``metadata`` records with warcio, parses each WAT JSON payload, and yields
    ``{url_from, url_to, anchor, rel}``. ``on_record`` fires once per metadata record
    seen; ``on_malformed`` fires once per record whose JSON fails to parse (then skipped);
    ``on_no_links`` fires once per well-formed record that yields zero anchor links. All
    are optional no-ops — in Spark they wrap accumulators so parse/error/skip rates are
    measured; in tests they take plain counters.
    """
    open_path = wat_path.replace("s3a://", "s3://", 1)
    with fsspec.open(open_path, "rb", **storage_options) as raw:
        for record in ArchiveIterator(raw):
            if record.rec_type != "metadata":
                continue
            if on_record is not None:
                on_record()
            try:
                payload = json.loads(record.content_stream().read())
            except (ValueError, UnicodeDecodeError):
                if on_malformed is not None:
                    on_malformed()
                continue
            rows = links_from_payload(payload)
            if not rows and on_no_links is not None:
                on_no_links()
            yield from rows


def write_parquet(df: DataFrame, path: str) -> None:
    """Overwrite ``df`` to ``path`` as Parquet (experiment outputs are re-runnable)."""
    df.write.mode("overwrite").parquet(path)


def write_metrics_json(spark: SparkSession, obj: dict[str, Any], path: str) -> None:
    """Write a single-object metrics summary as one JSON text file under ``path``."""
    payload = json.dumps(obj, indent=2, default=str)
    spark.createDataFrame([(payload,)], ["json"]).coalesce(1).write.mode("overwrite").text(path)
