"""Stage 2 — global PageRank over the full domain graph (SPEC.md §5 Stage 2).

Reads the staged domain graph (edges + vertex map), runs power-iteration PageRank
on the **full global graph** (input is never scope-filtered — SPEC.md §3), and
writes ``cc_domain_pagerank`` Parquet. Personalized ``open_authority`` is Stage 3.

Expected data volume (crawl ``cc-main-2026-mar-apr-may``): 118.76M domain vertices,
4.3B edges. Last measured Dataproc Serverless cost (Exp 4.1 global PageRank, same
graph): ~$1.9 / run — converged at iteration 10, ~56 min wall, final mass 1.000000
(28.8 DCU-hr + shuffle, us-central1, 2026-05-30).

Operational (Exp 2): set an explicit Dataproc batch ``--ttl`` at submit time — the
default 4h TTL silently cancels long iterative jobs. Periodic Parquet rank snapshots
for cross-job resume after a TTL kill are a follow-up; a TTL sized to the converged
run covers the happy path.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import structlog
from pyspark.sql import DataFrame, SparkSession

from spark_jobs.common.config import checked_stage_output_path, load_storage, resolve_authority
from spark_jobs.common.errors import SparkJobError
from spark_jobs.common.graph_validation import validate_graph
from spark_jobs.common.pagerank import power_iteration
from spark_jobs.stage2_pagerank import io, transforms

log = structlog.get_logger()


def main(argv: list[str] | None = None) -> None:
    """Entrypoint: parse args, run the stage, stop the session."""
    args = _parse_args(argv)
    spark = SparkSession.builder.appName("openhrefs-stage2-pagerank").getOrCreate()
    spark.sparkContext.setCheckpointDir(args.checkpoint_dir)
    spark.sparkContext.setLogLevel("WARN")
    try:
        _run(spark, args)
    finally:
        spark.stop()


def _run(spark: SparkSession, args: argparse.Namespace) -> None:
    """Orchestrate read → PageRank + degrees → assemble → write."""
    edges = io.read_edges(spark, args.edges_path)
    vertices = io.read_vertices(spark, args.vertices_path)
    n_vertices = _resolve_n_vertices(vertices, args.n_vertices)
    validate_graph(vertices, edges, n_vertices)
    nodes = vertices.select("id")
    ranks = power_iteration(
        edges,
        nodes,
        max_iter=args.max_iter,
        tol=args.tol,
        damping=args.damping,
        edge_partitions=args.edge_partitions,
        checkpoint_every=args.checkpoint_every,
        n_vertices=n_vertices,
    )
    degrees = transforms.compute_degrees(edges, nodes)
    output = transforms.to_pagerank_output(ranks, degrees, vertices, crawl=args.crawl)
    io.write_pagerank(output, args.output_path)
    log.info(
        "stage2_pagerank_complete", crawl=args.crawl, n_vertices=n_vertices, output=args.output_path
    )


def _resolve_n_vertices(vertices: DataFrame, declared: int | None) -> int:
    """Vertex count is authoritative; a declared value is only validated against it.

    Sources ``n`` from the vertex file rather than trusting a hand-edited config
    value (the dense-id contract of :func:`power_iteration` depends on a correct ``n``).
    """
    actual = vertices.count()
    if declared is not None and declared != actual:
        raise SparkJobError(f"--n-vertices {declared} does not match vertex count {actual}")
    return actual


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Stage 2 — global PageRank.")
    p.add_argument(
        "--edges-path", required=True, help="Staged domain edges Parquet (from_id, to_id)."
    )
    p.add_argument("--vertices-path", required=True, help="Vertex map Parquet (id, domain).")
    p.add_argument("--crawl", required=True, help="Crawl identifier stamped on output rows.")
    p.add_argument(
        "--output-path", help="Output Parquet path; defaults to <RAW_PATH>/cc_domain_pagerank."
    )
    p.add_argument("--config", help="Pipeline config path; defaults to the committed config.yml.")
    # Calibrated params come from config (authority block); these flags only override.
    p.add_argument("--max-iter", type=int, help="Override config authority.max_iter.")
    p.add_argument("--tol", type=float, help="Override config authority.tol.")
    p.add_argument("--damping", type=float, help="Override config authority.damping.")
    p.add_argument("--edge-partitions", type=int, default=1000)
    p.add_argument("--checkpoint-every", type=int, default=4)
    p.add_argument("--checkpoint-dir", default="/tmp/stage2-ckpt")
    p.add_argument("--n-vertices", type=int, help="Optional; validated against the vertex count.")
    args = p.parse_args(argv)
    authority = resolve_authority(
        Path(args.config) if args.config else None, args.damping, args.tol, args.max_iter
    )
    args.damping, args.tol, args.max_iter = authority.damping, authority.tol, authority.max_iter
    if args.output_path is None:
        args.output_path = f"{load_storage().raw_path}/cc_domain_pagerank"
    args.output_path = checked_stage_output_path(args.output_path)
    return args


if __name__ == "__main__":
    main(sys.argv[1:])
