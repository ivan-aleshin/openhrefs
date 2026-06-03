"""Stage 3 — open_authority: personalized PageRank from the composite-DR seed (SPEC §5).

Runs personalized PageRank on the **full global graph** (input is never scope-filtered —
SPEC §3), teleporting onto the top-N composite-DR consensus seed, and writes
``cc_domain_authority`` (open_authority + log-scaled open_volume).

Expected data volume (crawl ``cc-main-2026-mar-apr-may``): 118.76M domain vertices,
4.3B edges. Last measured Dataproc Serverless cost (Exp 4.3 personalized-PR pilot, same
graph + top-10K seed): ~$1.5 / run — converged at iteration 8, ~47 min, final mass
1.000000 (23.4 DCU-hr, us-central1, 2026-05-31).

Operational (Exp 2): set an explicit Dataproc batch ``--ttl`` at submit time.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import structlog
from pyspark.sql import DataFrame, SparkSession

from spark_jobs.common.config import (
    checked_stage_output_path,
    load_storage,
    resolve_authority,
    resolve_seed,
)
from spark_jobs.common.errors import SparkJobError
from spark_jobs.common.graph_validation import validate_edges, validate_vertices
from spark_jobs.common.pagerank import power_iteration
from spark_jobs.stage3_open_authority import io, transforms

log = structlog.get_logger()


def main(argv: list[str] | None = None) -> None:
    """Entrypoint: parse args, run the stage, stop the session."""
    args = _parse_args(argv)
    spark = SparkSession.builder.appName("openhrefs-stage3-open-authority").getOrCreate()
    spark.sparkContext.setCheckpointDir(args.checkpoint_dir)
    spark.sparkContext.setLogLevel("WARN")
    try:
        _run(spark, args)
    finally:
        spark.stop()


def _run(spark: SparkSession, args: argparse.Namespace) -> None:
    """Orchestrate read → seed teleport → personalized PageRank → assemble → write."""
    edges = io.read_edges(spark, args.edges_path)
    vertices = io.read_vertices(spark, args.vertices_path)
    seed = io.read_seed(spark, args.seed_path)
    # Fail fast in increasing cost order: weight the (small) seed, validate vertices,
    # build the teleport (off-graph seed fails here), and only then validate edges and
    # iterate — so a bad seed or seed/graph mismatch never pays the 4.3B-edge passes.
    weights = transforms.weight_from_consensus(seed, args.seed_weight, args.seed_size)
    n_vertices = _resolve_n_vertices(vertices, args.n_vertices)
    validate_vertices(vertices, n_vertices)
    nodes = vertices.select("id")
    teleport = transforms.to_teleport_vector(weights, vertices)
    validate_edges(edges, n_vertices)
    ranks = power_iteration(
        edges,
        nodes,
        max_iter=args.max_iter,
        tol=args.tol,
        damping=args.damping,
        teleport=teleport,
        edge_partitions=args.edge_partitions,
        checkpoint_every=args.checkpoint_every,
        n_vertices=n_vertices,
    )
    in_degree = transforms.in_degrees(edges, nodes)
    output = transforms.to_authority_output(ranks, in_degree, vertices, crawl=args.crawl)
    io.write_authority(output, args.output_path)
    log.info(
        "stage3_open_authority_complete",
        crawl=args.crawl,
        n_vertices=n_vertices,
        output=args.output_path,
    )


def _resolve_n_vertices(vertices: DataFrame, declared: int | None) -> int:
    """Vertex count is authoritative; a declared value is only validated against it."""
    actual = vertices.count()
    if declared is not None and declared != actual:
        raise SparkJobError(f"--n-vertices {declared} does not match vertex count {actual}")
    return actual


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Stage 3 — open_authority (personalized PageRank).")
    p.add_argument(
        "--edges-path", required=True, help="Staged domain edges Parquet (from_id, to_id)."
    )
    p.add_argument("--vertices-path", required=True, help="Vertex map Parquet (id, domain).")
    p.add_argument(
        "--seed-path",
        required=True,
        help="composite-DR consensus seed; .csv/.csv.gz read as CSV, else Parquet.",
    )
    p.add_argument("--crawl", required=True, help="Crawl identifier stamped on output rows.")
    p.add_argument(
        "--output-path", help="Output Parquet path; defaults to <RAW_PATH>/cc_domain_authority."
    )
    p.add_argument("--config", help="Pipeline config path; defaults to the committed config.yml.")
    # Calibrated params come from config (authority + seed blocks); these flags only override.
    p.add_argument("--max-iter", type=int, help="Override config authority.max_iter.")
    p.add_argument("--tol", type=float, help="Override config authority.tol.")
    p.add_argument("--damping", type=float, help="Override config authority.damping.")
    p.add_argument("--seed-size", type=int, help="Override config seed.size.")
    p.add_argument("--seed-weight", help="Override config seed.weight.")
    p.add_argument("--edge-partitions", type=int, default=1000)
    p.add_argument("--checkpoint-every", type=int, default=4)
    p.add_argument("--checkpoint-dir", default="/tmp/stage3-ckpt")
    p.add_argument("--n-vertices", type=int, help="Optional; validated against the vertex count.")
    args = p.parse_args(argv)
    config_path = Path(args.config) if args.config else None
    authority = resolve_authority(config_path, args.damping, args.tol, args.max_iter)
    args.damping, args.tol, args.max_iter = authority.damping, authority.tol, authority.max_iter
    seed = resolve_seed(config_path, args.seed_size, args.seed_weight)
    args.seed_size, args.seed_weight = seed.size, seed.weight
    if args.output_path is None:
        args.output_path = f"{load_storage().raw_path}/cc_domain_authority"
    args.output_path = checked_stage_output_path(args.output_path)
    return args


if __name__ == "__main__":
    main(sys.argv[1:])
