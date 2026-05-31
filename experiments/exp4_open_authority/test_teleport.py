import analyze
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="module")
def spark():
    s = (
        SparkSession.builder.master("local[2]")
        .appName("exp4-teleport")
        .config("spark.sql.shuffle.partitions", "4")  # tiny graph: avoid 200 tasks/join
        .getOrCreate()
    )
    s.sparkContext.setCheckpointDir("/tmp/exp4-teleport-ckpt")
    yield s
    s.stop()


# 0->1->2->0 cycle + isolated dangling node 3. damping=0.5 so the cycle converges in
# a handful of iters (ratio = damping); checkpoint_every=5 truncates lineage on the local
# run (an uncheckpointed loop OOMs the driver); edge_partitions=2 skips the 1000-part
# repartition on a 3-edge graph.
_CYCLE = [(0, 1), (1, 2), (2, 0)]
_PR_KW = dict(n_vertices=4, max_iter=20, tol=1e-2, edge_partitions=2, checkpoint_every=5)


def test_personalized_pr_favors_seed_and_conserves_mass(spark):
    edges = spark.createDataFrame(_CYCLE, ["from_id", "to_id"])
    teleport = spark.createDataFrame([(0, 1.0)], ["id", "w"])  # all weight on node 0
    ranks = analyze.pagerank(edges, teleport=teleport, damping=0.5, return_ranks=True, **_PR_KW)
    d = {r["id"]: r["rank"] for r in ranks.collect()}
    assert abs(sum(d.values()) - 1.0) < 1e-6  # mass conserved
    assert d[0] > d[1] and d[0] > d[2] and d[0] > d[3]  # seed node dominates
    assert d[3] < 1e-3  # off-seed dangling (no in-edges) stays at ~0


def test_uniform_pr_unchanged_no_teleport(spark):
    # Without a teleport vector the cycle is symmetric: the three cycle nodes share rank
    # equally and the isolated dangling node 3 (no in-edges) gets only the teleport floor.
    edges = spark.createDataFrame(_CYCLE, ["from_id", "to_id"])
    ranks = analyze.pagerank(edges, damping=0.5, return_ranks=True, **_PR_KW)
    d = {r["id"]: r["rank"] for r in ranks.collect()}
    assert abs(sum(d.values()) - 1.0) < 1e-6  # mass conserved
    assert abs(d[0] - d[1]) < 1e-2 and abs(d[1] - d[2]) < 1e-2  # cycle nodes equal
    assert d[0] > d[3]  # dangling node ranks below the cycle


def test_unnormalized_teleport_rejected(spark):
    edges = spark.createDataFrame(_CYCLE, ["from_id", "to_id"])
    teleport = spark.createDataFrame([(0, 0.4), (1, 0.4)], ["id", "w"])  # sums to 0.8
    with pytest.raises(ValueError, match="not normalized"):
        analyze.pagerank(edges, teleport=teleport, return_ranks=True, **_PR_KW)


def test_out_of_range_teleport_id_rejected(spark):
    edges = spark.createDataFrame(_CYCLE, ["from_id", "to_id"])
    teleport = spark.createDataFrame([(9, 1.0)], ["id", "w"])  # id 9 not in [0, 4)
    with pytest.raises(ValueError, match="outside"):
        analyze.pagerank(edges, teleport=teleport, return_ranks=True, **_PR_KW)


def test_invalid_damping_rejected(spark):
    edges = spark.createDataFrame(_CYCLE, ["from_id", "to_id"])
    with pytest.raises(ValueError, match="damping"):
        analyze.pagerank(edges, damping=1.5, return_ranks=True, **_PR_KW)


def test_dangling_mass_routes_to_teleport_not_uniform(spark):
    # 0->1 with node 1 dangling (fed, so it holds real dangling mass), plus node 2
    # isolated and off-seed (w=0, no in-edges). Teleport all on node 0.
    # Correct routing sends dangling mass onto w(v), so the off-seed isolated node 2
    # stays at exactly 0; a uniform redistribution would leak d*dangling_mass/n onto it.
    # Discriminates the two without needing convergence (true at any iter with dangling mass).
    edges = spark.createDataFrame([(0, 1)], ["from_id", "to_id"])
    teleport = spark.createDataFrame([(0, 1.0)], ["id", "w"])
    ranks = analyze.pagerank(
        edges,
        n_vertices=3,
        max_iter=3,
        tol=0.0,
        edge_partitions=2,
        checkpoint_every=2,
        teleport=teleport,
        return_ranks=True,
    )
    d = {r["id"]: r["rank"] for r in ranks.collect()}
    assert abs(sum(d.values()) - 1.0) < 1e-6  # mass conserved
    assert d[2] < 1e-9  # isolated off-seed node gets no dangling leak (would be >0 if uniform)
