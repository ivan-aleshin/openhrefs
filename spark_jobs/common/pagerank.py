"""Shared power-iteration PageRank for Stage 2 and Stage 3 (SPEC.md §5).

Stage 2 (global ``pagerank_score``) and Stage 3 (personalized ``open_authority``)
run the *same* power iteration: with no teleport vector it is uniform PageRank;
with one it is personalized PageRank seeded on that vector. Mass is conserved at
1.0 — ranks start uniform (or on the teleport vector) and the mass held by
dangling nodes (no out-edges) is redistributed each iteration rather than leaked.

Pure ``DataFrame -> DataFrame``; reads/writes and SparkSession setup stay in the
stage ``io.py`` / ``main.py``.
"""

from __future__ import annotations

from pyspark import StorageLevel
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

DAMPING = 0.85


def power_iteration(
    edges: DataFrame,
    nodes: DataFrame,
    *,
    max_iter: int,
    tol: float,
    damping: float = DAMPING,
    teleport: DataFrame | None = None,
    checkpoint_every: int = 4,
    edge_partitions: int | None = None,
    n_vertices: int | None = None,
) -> DataFrame:
    """Power-iteration PageRank over every node in ``nodes``.

    **Precondition:** ``nodes`` holds dense, contiguous integer ids ``[0, n)`` — the
    realization the production graph satisfies (CommonCrawl's vertex file and our
    fallback ``collapse_to_domain`` both emit dense ids; ratified by the Exp 4.0
    graph-adoption gate). The teleport id-range check relies on this; a sparse-id
    graph would need membership validation against ``nodes`` instead.

    Args:
        edges: ``(from_id, to_id)`` directed edges; every id must appear in ``nodes``.
        nodes: ``(id)`` — the full vertex set, dense ids ``[0, n)``.
        max_iter: iteration cap; the loop also stops early on convergence.
        tol: convergence threshold on the L1 delta between iterations.
        damping: chain-follow probability ``d`` (must be in ``(0, 1)``).
        teleport: optional ``(id, w)`` vector (``w >= 0``, sums to 1). When given,
            the teleport term and the dangling mass both land on ``w`` instead of
            uniform ``1/n``, concentrating mass around the seeded nodes.
        checkpoint_every: reliable-checkpoint cadence to truncate lineage; ``0``
            disables checkpointing (used by unit tests on tiny graphs).
        edge_partitions: if set, repartition edges by ``from_id`` for shuffle
            locality across iterations; ``None`` leaves partitioning untouched.
        n_vertices: node count if already known from graph metadata; avoids a
            ``nodes.count()`` scan. Must equal ``nodes.count()`` when given.

    Returns:
        ``(id, rank)`` — the converged (or ``max_iter``) rank per node.

    Raises:
        ValueError: if ``damping`` is outside ``(0, 1)``, the node set is empty, or
            ``teleport`` is invalid.
    """
    if not 0.0 < damping < 1.0:
        raise ValueError(f"damping must be in (0, 1), got {damping}")
    n = n_vertices if n_vertices is not None else nodes.count()
    if n == 0:
        raise ValueError("power_iteration requires a non-empty node set")
    if edge_partitions:
        edges = edges.repartition(edge_partitions, "from_id")
    edges = edges.persist(StorageLevel.DISK_ONLY)
    outdeg = edges.groupBy("from_id").agg(F.count("*").alias("out")).persist(StorageLevel.DISK_ONLY)
    nodes = nodes.persist(StorageLevel.DISK_ONLY)
    outdeg.count()

    tele = _build_teleport(nodes, teleport, n) if teleport is not None else None
    try:
        ranks = _run_iterations(
            edges,
            nodes,
            outdeg,
            tele,
            n=n,
            max_iter=max_iter,
            tol=tol,
            damping=damping,
            checkpoint_every=checkpoint_every,
        )
    finally:
        for df in (edges, outdeg, nodes, tele):
            if df is not None:
                df.unpersist()
    return ranks.select("id", "rank")


def _run_iterations(
    edges: DataFrame,
    nodes: DataFrame,
    outdeg: DataFrame,
    tele: DataFrame | None,
    *,
    n: int,
    max_iter: int,
    tol: float,
    damping: float,
    checkpoint_every: int,
) -> DataFrame:
    """Iterate the rank vector to convergence (or ``max_iter``), returning final ranks."""
    if tele is not None:
        ranks = tele.select("id", F.col("w").alias("rank"))
    else:
        ranks = nodes.select("id", F.lit(1.0 / n).alias("rank"))
    for i in range(1, max_iter + 1):
        new_ranks, src = _step(edges, ranks, outdeg, nodes, n=n, damping=damping, teleport=tele)
        if checkpoint_every and (i % checkpoint_every == 0 or i == max_iter):
            new_ranks = new_ranks.checkpoint(eager=True)
        else:
            new_ranks = new_ranks.persist(StorageLevel.DISK_ONLY)
        delta = _l1_delta(new_ranks, ranks)
        if getattr(ranks.storageLevel, "useDisk", False):
            ranks.unpersist()
        src.unpersist()
        ranks = new_ranks
        if delta < tol:
            break
    return ranks


def _step(
    edges: DataFrame,
    ranks: DataFrame,
    outdeg: DataFrame,
    nodes: DataFrame,
    *,
    n: int,
    damping: float,
    teleport: DataFrame | None,
) -> tuple[DataFrame, DataFrame]:
    """One PageRank iteration: distribute rank along edges, redistribute dangling mass.

    Returns ``(new_ranks, src)``. ``src = ranks ⋈ outdeg`` is persisted because it
    feeds both the dangling-mass aggregate and the edge join into ``new_ranks``;
    without caching, Spark recomputes that node-scale join twice per iteration. The
    caller unpersists ``src`` once ``new_ranks`` is materialized.
    """
    src = (
        ranks.join(outdeg, ranks["id"] == outdeg["from_id"])
        .select("from_id", "rank", (F.col("rank") / F.col("out")).alias("contrib"))
        .persist(StorageLevel.DISK_ONLY)
    )
    nondangling_row = src.agg(F.sum("rank")).first()
    nondangling = (nondangling_row[0] if nondangling_row else None) or 0.0
    dangling_mass = 1.0 - nondangling
    inflow = edges.join(src, "from_id").groupBy("to_id").agg(F.sum("contrib").alias("inflow"))
    carried = damping * F.coalesce(F.col("inflow"), F.lit(0.0))

    if teleport is not None:
        teleport_coef = (1.0 - damping) + damping * dangling_mass
        new_ranks = (
            nodes.join(inflow, nodes["id"] == inflow["to_id"], "left")
            .join(teleport, "id")
            .select("id", (F.lit(teleport_coef) * F.col("w") + carried).alias("rank"))
        )
    else:
        uniform = (1.0 - damping) / n + damping * dangling_mass / n
        new_ranks = nodes.join(inflow, nodes["id"] == inflow["to_id"], "left").select(
            "id", (F.lit(uniform) + carried).alias("rank")
        )
    return new_ranks, src


def _l1_delta(new_ranks: DataFrame, old_ranks: DataFrame) -> float:
    """Sum of absolute per-node rank changes between two iterations."""
    row = (
        new_ranks.join(old_ranks.withColumnRenamed("rank", "old"), "id")
        .select(F.abs(F.col("rank") - F.col("old")).alias("d"))
        .agg(F.sum("d"))
        .first()
    )
    return float(row[0]) if row is not None and row[0] is not None else 0.0


def _build_teleport(nodes: DataFrame, teleport: DataFrame, n: int) -> DataFrame:
    """Validate a teleport vector and broadcast it onto all nodes (off-seed ``w = 0``).

    Rejects vectors that would silently break mass conservation: null/NaN weights,
    negative weights, duplicate ids, a sum != 1, or ids outside the node set (these
    survive the sum check but get dropped by the node join, leaking mass). All
    checks come from one aggregate over the small teleport vector.
    """
    t = teleport.select(F.col("id"), F.col("w").cast("double"))
    chk = t.agg(
        F.sum("w").alias("s"),
        F.min("w").alias("mn"),
        F.count("*").alias("c"),
        F.countDistinct("id").alias("d"),
        F.min("id").alias("id_min"),
        F.max("id").alias("id_max"),
        F.sum(F.when(F.col("w").isNull() | F.isnan("w"), 1).otherwise(0)).alias("bad"),
    ).first()
    assert chk is not None  # an agg without group-by always yields exactly one row
    if chk["bad"]:
        raise ValueError(f"teleport vector has null/NaN weights ({chk['bad']} rows)")
    if chk["mn"] is not None and chk["mn"] < 0:
        raise ValueError(f"teleport vector has negative weight (min={chk['mn']})")
    if chk["c"] != chk["d"]:
        raise ValueError(f"teleport vector has duplicate ids ({chk['c']} rows, {chk['d']} unique)")
    if chk["s"] is None or abs(chk["s"] - 1.0) > 1e-6:
        raise ValueError(f"teleport vector not normalized (sum={chk['s']})")
    if chk["id_min"] is not None and (chk["id_min"] < 0 or chk["id_max"] >= n):
        raise ValueError(f"teleport vector has ids outside the node set [0, {n})")
    tele = (
        nodes.join(t, "id", "left")
        .select("id", F.coalesce("w", F.lit(0.0)).alias("w"))
        .persist(StorageLevel.DISK_ONLY)
    )
    tele.count()
    return tele
