"""Graph ingest — build the canonical staged V3 graph from the CommonCrawl source.

Converts the CommonCrawl published domain graph (vertices + edges) into the splittable
Parquet inputs the authority stages read: ``v3_map`` ``(id, domain)`` and ``v3_edges``
``(from_id, to_id)`` under the staged path (ADR-0003 — staging is the Track A access path
while direct ``gs://commoncrawl`` requester-pays is blocked).

Operational pre-step (NOT this job): download the CC domain graph from the CloudFront
endpoint (``data.commoncrawl.org``) and split the single non-splittable ~15 GiB edges
gzip into parts on a transient VM, so Spark can read the edges in parallel. The ~840 MiB
vertices gzip is read directly. This job reads the staged TSV (vertices gzip, edge parts).

The staged graph is validated by the consuming stages at read (``common.graph_validation``),
so this job does not re-validate.

Expected data volume (crawl ``cc-main-2026-mar-apr-may``): 118.76M vertices, 4.3B edges.
Cost (Exp 4.0/4.1, same graph): vertex map build ≈ negligible; edge TSV→Parquet convert
≈ $0.5–1 (us-central1, 2026-05-30).
"""

from __future__ import annotations

import argparse
import sys

import structlog
from pyspark.sql import SparkSession

from spark_jobs.common.config import checked_stage_output_path
from spark_jobs.ingest import io, transforms

log = structlog.get_logger()


def main(argv: list[str] | None = None) -> None:
    """Entrypoint: parse args, run the ingest, stop the session."""
    args = _parse_args(argv)
    spark = SparkSession.builder.appName("openhrefs-ingest-graph").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        _run(spark, args)
    finally:
        spark.stop()


def _run(spark: SparkSession, args: argparse.Namespace) -> None:
    """Convert vertices → v3_map and edges → v3_edges, writing canonical staged Parquet."""
    vertices = io.read_vertices_tsv(spark, args.vertices_path)
    io.write_parquet(transforms.to_v3_map(vertices), args.vertices_out)
    edges = io.read_edges_tsv(spark, args.edges_path)
    io.write_parquet(edges, args.edges_out)
    log.info("ingest_complete", vertices_out=args.vertices_out, edges_out=args.edges_out)


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Ingest — build the staged V3 graph from CC source.")
    p.add_argument(
        "--vertices-path", required=True, help="CC domain vertices TSV (.txt.gz, 3-col)."
    )
    p.add_argument("--edges-path", required=True, help="CC domain edges TSV parts (2-col).")
    p.add_argument("--vertices-out", required=True, help="Output v3_map Parquet path (id, domain).")
    p.add_argument(
        "--edges-out", required=True, help="Output v3_edges Parquet path (from_id, to_id)."
    )
    args = p.parse_args(argv)
    args.vertices_out = checked_stage_output_path(args.vertices_out)
    args.edges_out = checked_stage_output_path(args.edges_out)
    return args


if __name__ == "__main__":
    main(sys.argv[1:])
