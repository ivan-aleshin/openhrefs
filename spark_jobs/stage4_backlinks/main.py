"""cc_domain_link_pairs is a productionized domain-grain validation artifact; it does not
replace SPEC §6 URL-grain cc_backlinks / cc_outbound_links. The SPEC Stage 4 contract
remains open until the 200-slice validation.

Cost annotation: Exp 6 baseline 0.0999 DCU-h/file diagnostic / 0.069 optimized-thick;
this path targets ~0.06; measured production cost pending the 200-slice (update as a
follow-up).
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

import structlog
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from spark_jobs.common.config import checked_stage_output_path
from spark_jobs.common.errors import DataSourceError
from spark_jobs.stage4_backlinks import io
from spark_jobs.stage4_backlinks.transforms import (
    RAW_LINK_SCHEMA,
    aggregate_domain_pairs,
    default_metrics_path,
    manifest_sha256,
    resolve_domain_pairs,
    resolve_wat_path,
)

log = structlog.get_logger()


@dataclass
class _ExtractionStats:
    """WAT extraction counters captured after the single-pass count action."""

    sha: str
    n_wat_files: int
    raw_link_rows: int
    files_attempted: int
    files_unreadable: int
    records_seen: int
    malformed_records: int
    records_with_no_links: int


def main(argv: list[str] | None = None) -> None:
    """Entrypoint: parse args, run the stage, stop the session."""
    args = _parse_args(argv)
    spark = SparkSession.builder.appName("openhrefs-stage4-backlinks").getOrCreate()
    try:
        _run(spark, args)
    finally:
        spark.stop()


def _run(spark: SparkSession, args: argparse.Namespace) -> None:
    """Orchestrate WAT read → link extraction → domain aggregation → write."""
    spark.conf.set("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
    spark.sparkContext.setLogLevel("WARN")
    log.info("stage4_start", run_id=args.run_id, shuffle_partitions=args.shuffle_partitions)
    paths, sha = _read_wat_list(args)
    accs = _make_accumulators(spark)
    links = _extract_rdd(spark, paths, accs).cache()
    raw_link_rows = links.count()  # single parse pass; populates accumulators
    stats = _harvest_stats(sha, paths, raw_link_rows, accs)
    resolved = resolve_domain_pairs(links)
    pairs = aggregate_domain_pairs(resolved).withColumn("crawl", F.lit(args.crawl)).cache()
    out = checked_stage_output_path(args.output_path)
    io.write_domain_link_pairs(pairs, out)
    metrics = _build_metrics(args, pairs, out, stats)
    metrics_out = f"{args.metrics_path}/crawl={args.crawl}/run_id={args.run_id}"
    io.write_metrics_json(spark, metrics, metrics_out)
    links.unpersist()
    pairs.unpersist()
    log.info(
        "stage4_backlinks_complete",
        crawl=args.crawl,
        run_id=args.run_id,
        raw_link_rows=stats.raw_link_rows,
        domain_pair_rows=metrics["domain_pair_rows"],
        link_count_total=metrics["link_count_total"],
    )


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Stage 4 — cc_domain_link_pairs extraction.")
    p.add_argument("--wat-list", required=True, help="Plain-text WAT slice list path.")
    p.add_argument(
        "--wat-prefix",
        required=True,
        help="Bucket/root prefix for relative WAT entries. Never a config fallback.",
    )
    p.add_argument("--output-path", required=True, help="Exact dataset output path.")
    p.add_argument("--run-id", required=True, help="Operator-supplied run identifier.")
    p.add_argument("--crawl", default="CC-MAIN-2026-21", help="Crawl id stamped as partition.")
    p.add_argument("--metrics-path", default=None, help="Defaults to sibling _metrics/<ds>.")
    p.add_argument("--shuffle-partitions", type=int, default=400)
    args = p.parse_args(argv)
    if args.metrics_path is None:
        args.metrics_path = default_metrics_path(args.output_path)
    return args


def _read_wat_list(args: argparse.Namespace) -> tuple[list[str], str]:
    """Read the WAT slice list, fail-fast on empty, return resolved paths + SHA-256."""
    lines = io.read_wat_list(args.wat_list)
    if not lines:
        raise DataSourceError(f"WAT slice list is empty: {args.wat_list}")
    sha = manifest_sha256("\n".join(lines))
    # Collecting ≤100k manifest path strings is bounded; NEVER collect link/domain rows.
    paths = [resolve_wat_path(line, args.wat_prefix) for line in lines]
    log.info("wat_list_read", n_files=len(paths), sample=paths[0], sha_prefix=sha[:8])
    return paths, sha


def _make_accumulators(spark: SparkSession) -> dict[str, Any]:
    """Create the 5 named Spark accumulators for the extraction pass."""
    sc = spark.sparkContext
    return {
        "files_attempted": sc.accumulator(0),
        "files_unreadable": sc.accumulator(0),
        "records_seen": sc.accumulator(0),
        "malformed_records": sc.accumulator(0),
        "records_with_no_links": sc.accumulator(0),
    }


def _extract_rdd(spark: SparkSession, paths: list[str], accs: dict[str, Any]) -> DataFrame:
    """Build a raw-links DataFrame by flatMapping iter_wat_links_raw over all WAT paths."""

    def extract(path: str) -> Iterator[dict[str, str | None]]:
        # Generator (yield from), not list(): a real WAT yields millions of link
        # rows — materializing per file in executor memory risks OOM. Streaming
        # matches the Exp 6 measured path. The file-level read failure surfaces
        # lazily during iteration, so the try still catches it (files_unreadable).
        accs["files_attempted"].add(1)
        try:
            yield from io.iter_wat_links_raw(
                path,
                on_record=lambda: accs["records_seen"].add(1),
                on_malformed=lambda: accs["malformed_records"].add(1),
                on_no_links=lambda: accs["records_with_no_links"].add(1),
            )
        except (OSError, ValueError, EOFError) as exc:
            # OSError covers fsspec open + cloud read failures; ValueError and
            # EOFError cover truncated or malformed gzip streams from fastwarc.
            accs["files_unreadable"].add(1)
            log.warning("wat_unreadable", path=path, error=str(exc))

    rdd = spark.sparkContext.parallelize(paths, numSlices=len(paths)).flatMap(extract)
    return spark.createDataFrame(rdd, RAW_LINK_SCHEMA)


def _harvest_stats(
    sha: str, paths: list[str], raw_link_rows: int, accs: dict[str, Any]
) -> _ExtractionStats:
    """Snapshot accumulator values after the extraction count action (reads .value once)."""
    return _ExtractionStats(
        sha=sha,
        n_wat_files=len(paths),
        raw_link_rows=raw_link_rows,
        files_attempted=accs["files_attempted"].value,
        files_unreadable=accs["files_unreadable"].value,
        records_seen=accs["records_seen"].value,
        malformed_records=accs["malformed_records"].value,
        records_with_no_links=accs["records_with_no_links"].value,
    )


def _build_metrics(
    args: argparse.Namespace,
    pairs: DataFrame,
    out: str,
    stats: _ExtractionStats,
) -> dict[str, Any]:
    """Assemble the run-level metrics dict; computes pair counts from the cached DataFrame."""
    domain_pair_rows = pairs.count()
    agg_row = pairs.agg(F.sum("link_count")).first()
    link_count_total = int(agg_row[0] or 0) if agg_row is not None else 0
    return {
        "n_wat_files": stats.n_wat_files,
        "raw_link_rows": stats.raw_link_rows,
        "domain_pair_rows": domain_pair_rows,
        "link_count_total": link_count_total,
        "files_attempted": stats.files_attempted,
        "files_unreadable": stats.files_unreadable,
        "records_seen": stats.records_seen,
        "malformed_records": stats.malformed_records,
        "records_with_no_links": stats.records_with_no_links,
        "crawl": args.crawl,
        "run_id": args.run_id,
        "manifest_sha256": stats.sha,
        "wat_prefix": args.wat_prefix,
        "output_path": out,
        "artifact_kind": "domain-grain validation (non-canonical for SPEC §6)",
    }


if __name__ == "__main__":
    main(sys.argv[1:])
