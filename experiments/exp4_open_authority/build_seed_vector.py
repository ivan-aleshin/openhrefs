"""Exp 4.3 — build the open_authority teleport vector `(id, w)` from the consensus seed.

Reads the composite-DR consensus CSV, takes the top-N by score, weights each domain, maps it
onto its V3 graph node (v3_map), and writes the normalized teleport vector for
`analyze.py --teleport`. Raw seed only — cleaning methods land in a later iteration. I/O only;
the weighting/mapping/normalization logic is in `seed_transforms` (unit-tested).
"""

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from seed_transforms import to_teleport_vector, weight_from_consensus


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Exp 4.3 — build raw seed teleport vector.")
    p.add_argument("--consensus-csv", required=True)
    p.add_argument("--v3-map", required=True)
    p.add_argument("--seed-size", type=int, default=10_000)
    p.add_argument("--domain-col", default="registered_domain")
    p.add_argument("--score-col", default="consensus_score")
    p.add_argument("--weight-formula", default="log_rank")
    p.add_argument("--out", required=True)
    a = p.parse_args(argv)
    spark = SparkSession.builder.appName("exp4.3-build-seed").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        seed = (
            spark.read.option("header", True)
            .csv(a.consensus_csv)
            .select(
                F.col(a.domain_col).alias("domain"),
                F.col(a.score_col).cast("double").alias("consensus_score"),
            )
            .where(F.col("consensus_score").isNotNull())
            .orderBy(F.col("consensus_score").desc(), F.col("domain").asc())  # deterministic cut
            .limit(a.seed_size)
        )
        weights = weight_from_consensus(seed, a.weight_formula)
        v3_map = spark.read.parquet(a.v3_map)
        tele = to_teleport_vector(weights, v3_map)
        tele.write.mode("overwrite").parquet(a.out)
        seed_n = seed.count()
        mapped_n = tele.count()
        print(
            f"=== seed_n={seed_n:,} mapped_n={mapped_n:,} "
            f"mapped_ratio={mapped_n / seed_n:.2%} formula={a.weight_formula} -> {a.out} ==="
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
