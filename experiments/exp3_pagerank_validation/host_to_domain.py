"""Exp 3 — aggregate host-level PageRank to registered-domain level (V1, V2).

Joins host ranks (id, rank) with the vertices→domain map and aggregates per domain:
`pr_sum` (Σ over all hosts of the domain), `pr_apex` (rank of the apex host, 0 if
absent), `n_hosts` (host count). `join_opr.py` then picks pr_sum or pr_apex for the
OpenPageRank comparison. V3 ranks are already domain-level (see collapse_to_domain.py).
"""

import argparse

from graph_io import read_id_domain
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main(argv: list[str] | None = None) -> None:
    """Entrypoint."""
    parser = argparse.ArgumentParser(description="Exp 3 — host ranks -> domain ranks.")
    parser.add_argument("--ranks", required=True, help="parquet (id, rank) host ranks.")
    parser.add_argument("--vertices", required=True, help="vertices (id, reversed-host).")
    parser.add_argument("--out", required=True, help="parquet (domain, pr_sum, pr_apex, n_hosts).")
    args = parser.parse_args(argv)

    spark = SparkSession.builder.appName("exp3-host-to-domain").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        idmap = read_id_domain(spark, args.vertices)
        ranks = spark.read.parquet(args.ranks).select(
            "id", F.col("rank").cast("double").alias("rank")
        )
        agg = (
            ranks.join(idmap, "id")
            .groupBy("domain")
            .agg(
                F.sum("rank").alias("pr_sum"),
                F.sum(F.when(F.col("is_apex"), F.col("rank")).otherwise(0.0)).alias("pr_apex"),
                F.count("*").alias("n_hosts"),
            )
        )
        agg.write.mode("overwrite").parquet(args.out)
        print(f"=== wrote domain ranks -> {args.out} ===")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
