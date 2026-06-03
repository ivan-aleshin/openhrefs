import pytest
from pyspark.sql import SparkSession
from seed_transforms import to_teleport_vector, weight_from_consensus


@pytest.fixture(scope="module")
def spark():
    s = SparkSession.builder.master("local[2]").appName("exp4-seed").getOrCreate()
    yield s
    s.stop()


def test_weight_formulas_monotone_and_positive(spark):
    df = spark.createDataFrame([("a.com", 100.0), ("b.com", 50.0)], ["domain", "consensus_score"])
    for formula in ("log_rank", "sqrt_rank", "uniform", "score"):
        w = {r["domain"]: r["w"] for r in weight_from_consensus(df, formula).collect()}
        assert w["a.com"] >= w["b.com"] > 0  # higher-score domain weighted at least as much


def test_unknown_weight_formula_rejected(spark):
    df = spark.createDataFrame([("a.com", 1.0)], ["domain", "consensus_score"])
    with pytest.raises(ValueError, match="unknown weight formula"):
        weight_from_consensus(df, "bogus")


def test_to_teleport_vector_maps_to_nodes_and_normalizes(spark):
    weights = spark.createDataFrame(
        [("a.com", 3.0), ("b.com", 1.0), ("z.com", 9.0)], ["domain", "w"]
    )
    v3_map = spark.createDataFrame([(10, "a.com"), (11, "b.com")], ["id", "domain"])  # z absent
    tele = {r["id"]: r["w"] for r in to_teleport_vector(weights, v3_map).collect()}
    assert set(tele) == {10, 11}  # z.com dropped (not in graph)
    assert abs(sum(tele.values()) - 1.0) < 1e-9  # normalized to sum 1
    assert tele[10] == pytest.approx(0.75)  # 3 / (3 + 1)


def test_to_teleport_vector_all_off_graph_rejected(spark):
    weights = spark.createDataFrame([("z.com", 9.0)], ["domain", "w"])
    v3_map = spark.createDataFrame([(10, "a.com")], ["id", "domain"])
    with pytest.raises(ValueError, match="zero mapped weight"):
        to_teleport_vector(weights, v3_map)
