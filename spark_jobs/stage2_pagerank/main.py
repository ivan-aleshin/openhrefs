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

import structlog
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from spark_jobs.common.config import load_storage
from spark_jobs.common.errors import ConfigError, DataSourceError, SparkJobError
from spark_jobs.common.pagerank import DAMPING, power_iteration
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
    _validate_graph(vertices, edges, n_vertices)
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


def _validate_graph(vertices: DataFrame, edges: DataFrame, n: int) -> None:
    """Enforce the dense-id graph contract at the Stage 2 boundary (see ``power_iteration``).

    A corrupt staged graph (non-dense/duplicate ids, null or out-of-range edge endpoints,
    null domains) would otherwise yield a silently wrong PageRank, leak mass, or emit a
    null-domain row. Vertices must be dense ``[0, n)`` with non-null domains; every edge
    endpoint must be non-null and fall in that range.
    """
    _validate_vertices(vertices, n)
    _validate_edges(edges, n)


def _validate_vertices(vertices: DataFrame, n: int) -> None:
    v = vertices.agg(
        F.countDistinct("id").alias("distinct"),
        F.min("id").alias("min"),
        F.max("id").alias("max"),
        F.sum(F.when(F.col("domain").isNull(), 1).otherwise(0)).alias("null_domain"),
        F.countDistinct("domain").alias("distinct_domain"),
    ).first()
    assert v is not None
    if v["distinct"] != n or v["min"] != 0 or v["max"] != n - 1:
        raise DataSourceError(
            f"vertices are not dense [0, {n}): distinct={v['distinct']} "
            f"min={v['min']} max={v['max']}"
        )
    if v["null_domain"]:
        raise DataSourceError(f"vertex map has {v['null_domain']} rows with a null domain")
    if v["distinct_domain"] != n:
        raise DataSourceError(
            f"vertex map domains are not unique: {v['distinct_domain']} distinct for {n} ids"
        )


def _validate_edges(edges: DataFrame, n: int) -> None:
    e = edges.agg(
        F.min("from_id").alias("fmn"),
        F.max("from_id").alias("fmx"),
        F.min("to_id").alias("tmn"),
        F.max("to_id").alias("tmx"),
        F.sum(F.when(F.col("from_id").isNull() | F.col("to_id").isNull(), 1).otherwise(0)).alias(
            "null_ends"
        ),
    ).first()
    assert e is not None
    if e["null_ends"]:
        raise DataSourceError(f"edges have {e['null_ends']} rows with a null endpoint")
    mins = [x for x in (e["fmn"], e["tmn"]) if x is not None]
    maxs = [x for x in (e["fmx"], e["tmx"]) if x is not None]
    if mins and (min(mins) < 0 or max(maxs) >= n):
        raise DataSourceError(
            f"edge ids fall outside the vertex range [0, {n}): min={min(mins)} max={max(maxs)}"
        )


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
    p.add_argument("--max-iter", type=int, default=30)
    p.add_argument("--tol", type=float, default=0.001)
    p.add_argument("--damping", type=float, default=DAMPING)
    p.add_argument("--edge-partitions", type=int, default=1000)
    p.add_argument("--checkpoint-every", type=int, default=4)
    p.add_argument("--checkpoint-dir", default="/tmp/stage2-ckpt")
    p.add_argument("--n-vertices", type=int, help="Optional; validated against the vertex count.")
    args = p.parse_args(argv)
    if args.output_path is None:
        args.output_path = f"{load_storage().raw_path}/cc_domain_pagerank"
    args.output_path = _checked_output_path(args.output_path)
    return args


def _checked_output_path(path: str) -> str:
    """Reject writing stage output into the committed fixtures tree.

    The ``local`` storage ``raw_path`` is ``tests/fixtures/parquet`` (committed fixtures
    that dbt-local reads), so the config-derived default would overwrite them. A local
    run must point ``--output-path`` at a scratch/build location instead.
    """
    if "tests/fixtures" in path:
        raise ConfigError(
            f"refusing to write stage output into committed fixtures: {path}; "
            "pass an explicit --output-path (e.g. a build/ or /tmp path)"
        )
    return path


if __name__ == "__main__":
    main(sys.argv[1:])
