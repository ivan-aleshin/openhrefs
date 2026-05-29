"""Exp 3 — join a variant's domain ranks with OpenPageRank into the overlap parquet.

Produces the small (≤10M-row) overlap that `validate.py` consumes locally. Two input
shapes:
- V1/V2 (host_to_domain output): parquet (domain, pr_sum, pr_apex, n_hosts) — pick the
  pr column with --pr-col.
- V3 (domain-collapsed): parquet (id, rank) + --domain-map (id, domain).

OpenPageRank: the public top-10M CSV (`top10milliondomains.csv.zip`); domains are
normalized to registered form with the same PSL as our side.
"""

import argparse

from domain_utils import registered_domain
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


def main(argv: list[str] | None = None) -> None:
    """Entrypoint."""
    parser = argparse.ArgumentParser(description="Exp 3 — join variant ranks with OpenPageRank.")
    parser.add_argument("--our-ranks", required=True, help="parquet: (domain, pr_*) or (id, rank).")
    parser.add_argument("--opr-csv", required=True, help="OpenPageRank CSV (header).")
    parser.add_argument(
        "--out", required=True, help="overlap parquet (domain, our_pr, opr_score[, n_hosts])."
    )
    parser.add_argument("--pr-col", default="pr_sum", help="rank column for V1/V2 inputs.")
    parser.add_argument(
        "--domain-map", help="parquet (id, domain) — provide for V3 (id, rank) inputs."
    )
    parser.add_argument("--opr-domain-col", default="Domain")
    parser.add_argument("--opr-score-col", default="Open Page Rank")
    args = parser.parse_args(argv)

    spark = SparkSession.builder.appName("exp3-join-opr").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        ours = spark.read.parquet(args.our_ranks)
        if args.domain_map:
            dmap = spark.read.parquet(args.domain_map).select("id", "domain")
            ours = ours.join(dmap, "id").select(
                "domain", F.col("rank").cast("double").alias("our_pr")
            )
        else:
            cols = ["domain", F.col(args.pr_col).cast("double").alias("our_pr")]
            if "n_hosts" in ours.columns:
                cols.append("n_hosts")
            ours = ours.select(*cols)

        norm = F.udf(registered_domain, T.StringType())
        opr = (
            spark.read.option("header", True)
            .csv(args.opr_csv)
            .select(
                norm(F.col(args.opr_domain_col)).alias("domain"),
                F.col(args.opr_score_col).cast("double").alias("opr_score"),
            )
            .where(F.col("domain").isNotNull() & F.col("opr_score").isNotNull())
            .groupBy("domain")
            .agg(F.max("opr_score").alias("opr_score"))
        )

        overlap = ours.join(opr, "domain", "inner")
        overlap.write.mode("overwrite").parquet(args.out)
        print(f"=== overlap rows = {overlap.count():,} -> {args.out} ===")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
