"""Experiment 2 — Host Graph structure and PageRank convergence.

Reads one CommonCrawl host-level webgraph release (vertices + edges) and
reports the structural facts that calibrate Stage 2 (SPEC §5 Stage 2):
vertex/edge counts, in-degree distribution, dangling-node ratio, and the
power-iteration PageRank convergence curve (L1 delta per iteration vs `tol`).

Exploratory; runs on Dataproc Serverless against the staged graph in our own
bucket (gs://.../hostgraph/<release>/{vertices,edges}/), avoiding
gs://commoncrawl requester-pays.

Edge file format: `<from_id>\\t<to_id>` (tab). Vertex file: `<id>\\t<rev-host>`.

PageRank is the expensive part (iterative shuffle-join over all edges). Use
`--max-iter` to bound cost; convert edges to parquet first (`--edges-parquet`)
so each iteration reads columnar/splittable data instead of re-decompressing
gzip.
"""

import argparse

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

DAMPING = 0.85

_EDGE_SCHEMA = T.StructType(
    [
        T.StructField("from_id", T.LongType()),
        T.StructField("to_id", T.LongType()),
    ]
)


def read_edges(spark: SparkSession, path: str) -> DataFrame:
    """Read edges from tab-separated .txt.gz parts or from parquet."""
    if path.rstrip("/").endswith(".parquet") or "parquet" in path:
        return spark.read.parquet(path)
    return spark.read.option("sep", "\t").schema(_EDGE_SCHEMA).csv(path)


def graph_stats(spark: SparkSession, edges: DataFrame, vertices_path: str) -> int:
    """Report vertex/edge counts, in-degree distribution, dangling ratio."""
    n_vertices = spark.read.option("sep", "\t").csv(vertices_path).count()
    n_edges = edges.count()
    print("\n=== Counts ===")
    print(f"vertices: {n_vertices:,}")
    print(f"edges:    {n_edges:,}")

    indeg = edges.groupBy("to_id").count()
    pct = indeg.approxQuantile("count", [0.5, 0.9, 0.99, 0.999, 1.0], 0.001)
    print("\n=== In-degree distribution ===")
    print(f"nodes with >=1 inbound: {indeg.count():,}")
    print(f"p50/p90/p99/p99.9/max:  {[int(x) for x in pct]}")

    out_nodes = edges.select("from_id").distinct().count()
    dangling = n_vertices - out_nodes
    print("\n=== Dangling nodes (no out-edges) ===")
    print(f"nodes with out-edges: {out_nodes:,}")
    print(f"dangling:             {dangling:,} ({dangling / n_vertices:.1%})")
    return n_vertices


def pagerank(
    edges: DataFrame,
    n_vertices: int,
    max_iter: int,
    tol: float,
    edge_partitions: int = 1000,
    checkpoint_every: int = 4,
    resume_ranks: DataFrame | None = None,
    start_iter: int = 0,
    ranks_out: str | None = None,
) -> None:
    """Power-iteration PageRank over all N nodes; print L1 delta per iteration.

    Mass is conserved at 1.0: ranks start uniform on all N nodes and the rank
    held by dangling nodes (no out-edges) is redistributed uniformly each
    iteration. Pass `resume_ranks` (a previously persisted `(id, rank)` snapshot)
    plus `start_iter` to continue a run instead of starting from uniform — used
    when a batch is killed by the TTL before convergence.

    Cost optimizations for the iterative loop: edges are read once and persisted
    (repartitioned by from_id) so each iteration reads them from executor disk
    instead of re-decompressing parquet from GCS; the node set and out-degrees
    are persisted once; reliable checkpointing (lineage truncation) happens every
    `checkpoint_every` iterations instead of every one, with a disk persist in
    between.
    """
    spark = edges.sparkSession
    n = n_vertices
    base = (1.0 - DAMPING) / n
    edges = edges.repartition(edge_partitions, "from_id").persist(StorageLevel.DISK_ONLY)
    outdeg = edges.groupBy("from_id").agg(F.count("*").alias("out")).persist(StorageLevel.DISK_ONLY)
    nodes = spark.range(0, n).persist(StorageLevel.DISK_ONLY)
    outdeg.count()
    if resume_ranks is not None:
        ranks = resume_ranks.select("id", "rank")
    else:
        ranks = nodes.select(F.col("id"), F.lit(1.0 / n).alias("rank"))

    print(f"\n=== PageRank (d={DAMPING}, N={n:,}, tol={tol}, start_iter={start_iter}) ===")
    converged_at = None
    for i in range(start_iter + 1, max_iter + 1):
        src = (
            ranks.join(outdeg, ranks.id == outdeg.from_id)
            .select("from_id", "rank", (F.col("rank") / F.col("out")).alias("contrib"))
            .persist(StorageLevel.DISK_ONLY)
        )
        nondangling_mass = src.agg(F.sum("rank")).first()[0]
        redistribute = DAMPING * (1.0 - nondangling_mass) / n

        inflow = edges.join(src, "from_id").groupBy("to_id").agg(F.sum("contrib").alias("inflow"))
        new_ranks = nodes.join(inflow, nodes.id == inflow.to_id, "left").select(
            "id",
            (F.lit(base + redistribute) + DAMPING * F.coalesce("inflow", F.lit(0.0))).alias("rank"),
        )
        if i % checkpoint_every == 0 or i == max_iter:
            new_ranks = new_ranks.checkpoint(eager=True)
        else:
            new_ranks = new_ranks.persist(StorageLevel.DISK_ONLY)

        delta = (
            new_ranks.join(ranks.withColumnRenamed("rank", "old"), "id")
            .select(F.abs(F.col("rank") - F.col("old")).alias("d"))
            .agg(F.sum("d"))
            .first()[0]
        )
        if isinstance(ranks.storageLevel, StorageLevel) and ranks.storageLevel.useDisk:
            ranks.unpersist()
        src.unpersist()
        ranks = new_ranks
        print(f"  iter {i:>2}: L1 delta = {delta:.6e}")
        if delta < tol and converged_at is None:
            converged_at = i
            break
    total_mass = ranks.agg(F.sum("rank")).first()[0]
    print(f"converged (delta<{tol}) at iteration: {converged_at}")
    print(f"final total mass (should be ~1.0): {total_mass:.6f}")
    if ranks_out:
        ranks.write.mode("overwrite").parquet(ranks_out)
        print(f"=== wrote final ranks parquet -> {ranks_out} ===")


def main(argv: list[str] | None = None) -> None:
    """Entrypoint. Modes: convert-parquet, stats-only, pagerank-only, or full."""
    parser = argparse.ArgumentParser(description="Exp 2 — host graph + PageRank.")
    parser.add_argument("--edges-path", required=True)
    parser.add_argument("--vertices-path")
    parser.add_argument("--max-iter", type=int, default=20)
    parser.add_argument("--tol", type=float, default=0.001)
    parser.add_argument("--stats-only", action="store_true")
    parser.add_argument("--pagerank-only", action="store_true")
    parser.add_argument("--n-vertices", type=int)
    parser.add_argument("--convert-parquet", help="Write edges to this parquet path and exit.")
    parser.add_argument("--checkpoint-dir", default="/tmp/exp2-ckpt")
    parser.add_argument("--edge-partitions", type=int, default=1000)
    parser.add_argument("--checkpoint-every", type=int, default=4)
    parser.add_argument("--resume-from", help="parquet (id, rank) snapshot to continue from.")
    parser.add_argument("--start-iter", type=int, default=0, help="iteration the snapshot is at.")
    parser.add_argument("--ranks-out", help="write final (id, rank) parquet on convergence.")
    args = parser.parse_args(argv)

    spark = SparkSession.builder.appName("exp2-host-graph").getOrCreate()
    spark.sparkContext.setCheckpointDir(args.checkpoint_dir)
    spark.sparkContext.setLogLevel("WARN")
    try:
        edges = read_edges(spark, args.edges_path)
        if args.convert_parquet:
            edges.write.mode("overwrite").parquet(args.convert_parquet)
            print(f"\n=== wrote edges parquet -> {args.convert_parquet} ===")
            return
        resume_ranks = spark.read.parquet(args.resume_from) if args.resume_from else None
        if args.pagerank_only:
            pagerank(
                edges,
                args.n_vertices,
                args.max_iter,
                args.tol,
                args.edge_partitions,
                args.checkpoint_every,
                resume_ranks,
                args.start_iter,
                args.ranks_out,
            )
            return
        n_vertices = graph_stats(spark, edges, args.vertices_path)
        if not args.stats_only:
            pagerank(
                edges,
                n_vertices,
                args.max_iter,
                args.tol,
                args.edge_partitions,
                args.checkpoint_every,
                ranks_out=args.ranks_out,
            )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
