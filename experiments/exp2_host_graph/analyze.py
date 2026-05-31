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
    teleport: DataFrame | None = None,
    damping: float = DAMPING,
    return_ranks: bool = False,
) -> DataFrame | None:
    """Power-iteration PageRank over all N nodes; print L1 delta per iteration.

    Mass is conserved at 1.0: ranks start uniform on all N nodes and the rank
    held by dangling nodes (no out-edges) is redistributed uniformly each
    iteration. Pass `resume_ranks` (a previously persisted `(id, rank)` snapshot)
    plus `start_iter` to continue a run instead of starting from uniform — used
    when a batch is killed by the TTL before convergence.

    With a `teleport` vector `(id, w)` (w >= 0, sums to 1), this runs *personalized*
    PageRank (`open_authority`): the teleport term and the dangling redistribution
    both land on `w(v)` instead of uniform `1/N`, so mass concentrates around the
    seeded nodes. `damping` is the chain-follow probability `d` (default `DAMPING`);
    `return_ranks` returns the final `(id, rank)` DataFrame (used by tests).

    Cost optimizations for the iterative loop: edges are read once and persisted
    (repartitioned by from_id) so each iteration reads them from executor disk
    instead of re-decompressing parquet from GCS; the node set and out-degrees
    are persisted once; reliable checkpointing (lineage truncation) happens every
    `checkpoint_every` iterations instead of every one, with a disk persist in
    between.
    """
    if not 0.0 < damping < 1.0:
        raise ValueError(f"damping must be in (0, 1), got {damping}")
    spark = edges.sparkSession
    n = n_vertices
    base = (1.0 - damping) / n
    edges = edges.repartition(edge_partitions, "from_id").persist(StorageLevel.DISK_ONLY)
    outdeg = edges.groupBy("from_id").agg(F.count("*").alias("out")).persist(StorageLevel.DISK_ONLY)
    nodes = spark.range(0, n).persist(StorageLevel.DISK_ONLY)
    outdeg.count()

    tele = _build_teleport(nodes, teleport, n) if teleport is not None else None
    if resume_ranks is not None:
        ranks = resume_ranks.select("id", "rank")
    elif tele is not None:
        ranks = tele.select("id", F.col("w").alias("rank"))  # start from the teleport vector
    else:
        ranks = nodes.select(F.col("id"), F.lit(1.0 / n).alias("rank"))

    print(f"\n=== PageRank (d={damping}, N={n:,}, tol={tol}, start_iter={start_iter}) ===")
    converged_at = None
    for i in range(start_iter + 1, max_iter + 1):
        src = (
            ranks.join(outdeg, ranks.id == outdeg.from_id)
            .select("from_id", "rank", (F.col("rank") / F.col("out")).alias("contrib"))
            .persist(StorageLevel.DISK_ONLY)
        )
        nondangling_mass = src.agg(F.sum("rank")).first()[0]
        dangling_mass = 1.0 - nondangling_mass

        inflow = edges.join(src, "from_id").groupBy("to_id").agg(F.sum("contrib").alias("inflow"))
        if tele is not None:
            # personalized: the teleport term AND the dangling redistribution land on w(v)
            teleport_coef = (1.0 - damping) + damping * dangling_mass
            new_ranks = (
                nodes.join(inflow, nodes.id == inflow.to_id, "left")
                .join(tele, "id")
                .select(
                    "id",
                    (
                        F.lit(teleport_coef) * F.col("w")
                        + damping * F.coalesce("inflow", F.lit(0.0))
                    ).alias("rank"),
                )
            )
        else:
            redistribute = damping * dangling_mass / n
            new_ranks = nodes.join(inflow, nodes.id == inflow.to_id, "left").select(
                "id",
                (F.lit(base + redistribute) + damping * F.coalesce("inflow", F.lit(0.0))).alias(
                    "rank"
                ),
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
    if return_ranks:
        return ranks
    return None


def _build_teleport(nodes: DataFrame, teleport: DataFrame, n: int) -> DataFrame:
    """Validate the teleport vector and broadcast it onto all N nodes (off-seed w=0).

    Rejects vectors that would silently break mass conservation: negative weights,
    duplicate ids, a sum != 1, or ids outside [0, n) (these survive the sum check but
    get dropped by the node join, leaking mass). All checks come from one aggregate
    over the (small) teleport vector — no scan/join against the N-node set.
    """
    t = teleport.select(F.col("id"), F.col("w").cast("double"))
    chk = t.agg(
        F.sum("w").alias("s"),
        F.min("w").alias("mn"),
        F.count("*").alias("c"),
        F.countDistinct("id").alias("d"),
        F.min("id").alias("id_min"),
        F.max("id").alias("id_max"),
    ).first()
    if chk["mn"] is not None and chk["mn"] < 0:
        raise ValueError(f"teleport vector has negative weight (min={chk['mn']})")
    if chk["c"] != chk["d"]:
        raise ValueError(f"teleport vector has duplicate ids ({chk['c']} rows, {chk['d']} unique)")
    if chk["s"] is None or abs(chk["s"] - 1.0) > 1e-6:
        raise ValueError(f"teleport vector not normalized (sum={chk['s']})")
    if chk["id_min"] is not None and (chk["id_min"] < 0 or chk["id_max"] >= n):
        raise ValueError(f"teleport vector has ids outside [0, {n}) — not in the graph")
    tele = (
        nodes.join(t, "id", "left")
        .select("id", F.coalesce("w", F.lit(0.0)).alias("w"))
        .persist(StorageLevel.DISK_ONLY)
    )
    tele.count()
    return tele


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
    parser.add_argument("--teleport", help="parquet (id, w) teleport vector for personalized PR.")
    parser.add_argument("--damping", type=float, default=DAMPING)
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
        teleport = spark.read.parquet(args.teleport) if args.teleport else None
        if args.pagerank_only:
            pagerank(
                edges,
                n_vertices=args.n_vertices,
                max_iter=args.max_iter,
                tol=args.tol,
                edge_partitions=args.edge_partitions,
                checkpoint_every=args.checkpoint_every,
                resume_ranks=resume_ranks,
                start_iter=args.start_iter,
                ranks_out=args.ranks_out,
                teleport=teleport,
                damping=args.damping,
            )
            return
        n_vertices = graph_stats(spark, edges, args.vertices_path)
        if not args.stats_only:
            pagerank(
                edges,
                n_vertices=n_vertices,
                max_iter=args.max_iter,
                tol=args.tol,
                edge_partitions=args.edge_partitions,
                checkpoint_every=args.checkpoint_every,
                ranks_out=args.ranks_out,
                teleport=teleport,
                damping=args.damping,
            )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
