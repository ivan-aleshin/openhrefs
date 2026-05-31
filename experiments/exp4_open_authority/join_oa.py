"""Exp 4.4 — open_authority ranks ⋈ v3_map ⋈ trust-flow reference → overlap parquet.

I/O glue (mirrors Exp 3's `join_opr.py`): maps OA node ids to domains, inner-joins the normalized
reference, and writes the small `(domain, open_authority, tf)` overlap that `validate_oa.py` reads
locally. Optionally also writes the FULL-ranking top-K slice for an unbiased social-share metric.
"""

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Exp 4.4 — build open_authority overlap vs reference.")
    p.add_argument("--oa-ranks", required=True)  # (id, rank)
    p.add_argument("--v3-map", required=True)  # (id, domain)
    p.add_argument("--ref", required=True)  # (domain, tf, cf)
    p.add_argument("--out", required=True)  # overlap (domain, open_authority, tf)
    p.add_argument("--out-top", help="small (domain, open_authority) top-K slice for social-share")
    p.add_argument("--top-k", type=int, default=10_000)
    a = p.parse_args(argv)
    spark = SparkSession.builder.appName("exp4.4-join-oa").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        oa = spark.read.parquet(a.oa_ranks).select("id", F.col("rank").alias("open_authority"))
        m = spark.read.parquet(a.v3_map).select("id", "domain")
        ref = spark.read.parquet(a.ref).select("domain", "tf")
        oa_domains = oa.join(m, "id").select("domain", "open_authority")
        if (
            a.out_top
        ):  # top-K of the FULL ranking (Spark top-k → tiny parquet for pandas social-share)
            (
                oa_domains.orderBy(F.col("open_authority").desc())
                .limit(a.top_k)
                .write.mode("overwrite")
                .parquet(a.out_top)
            )
        overlap = oa_domains.join(ref, "domain", "inner")
        overlap.write.mode("overwrite").parquet(a.out)
        print(f"=== oa overlap rows = {overlap.count():,} -> {a.out} ===")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
