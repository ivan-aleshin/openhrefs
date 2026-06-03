"""Exp 4 — OA vs global-PR lift probe: attach each OA-head domain's global PageRank score.

Outputs `(domain, oa_score, global_score)` for the top-`top_k` domains by `open_authority`, so the
ubiquity baseline (global PR) can be subtracted/divided locally. Ubiquitous infra ranks high in
BOTH OA and global PR → small lift; seed-trusted editorial domains rank higher in OA than global →
positive lift. An automated head re-rank by lift demotes ubiquity without a denylist's precision
problem.
"""

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Exp 4 — OA vs global-PR lift probe.")
    p.add_argument("--oa-ranks", required=True)  # (id, rank) personalized
    p.add_argument("--global-ranks", required=True)  # (id, rank) uniform-teleport
    p.add_argument("--v3-map", required=True)  # (id, domain)
    p.add_argument("--out", required=True)  # (domain, oa_score, global_score)
    p.add_argument("--top-k", type=int, default=100_000)
    a = p.parse_args(argv)
    spark = SparkSession.builder.appName("exp4-delta-probe").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        oa = spark.read.parquet(a.oa_ranks).select("id", F.col("rank").alias("oa_score"))
        glob = spark.read.parquet(a.global_ranks).select("id", F.col("rank").alias("global_score"))
        m = spark.read.parquet(a.v3_map).select("id", "domain")
        oa_top = oa.orderBy(F.col("oa_score").desc()).limit(a.top_k)
        out = oa_top.join(glob, "id").join(m, "id").select("domain", "oa_score", "global_score")
        out.write.mode("overwrite").parquet(a.out)
        print(f"=== delta probe rows = {out.count():,} (top_k={a.top_k}) -> {a.out} ===")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
