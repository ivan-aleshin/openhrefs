"""Exp 4 — flag boilerplate/CDN domains by in-degree fraction (one-off, recorded threshold).

A domain linked to by more than `threshold` of all domains is structurally boilerplate/CDN-class
(embedded via <script>/CDN on a large fraction of the web), not an organic authority. The flagged
`(id, domain)` list feeds the automated seed-cleaning methods (out-neighbor-quality filters these
as neighbors; the denylist variant removes them from the seed) and doubles as an output denylist
for the published authority ranking.
"""

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Exp 4 — in-degree-fraction boilerplate detector.")
    p.add_argument("--v3-edges", required=True)  # (from_id, to_id)
    p.add_argument("--v3-map", required=True)  # (id, domain)
    p.add_argument("--threshold", type=float, default=0.05)
    p.add_argument("--out", required=True)  # (id, domain)
    a = p.parse_args(argv)
    spark = SparkSession.builder.appName("exp4-detect-boilerplate").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        edges = spark.read.parquet(a.v3_edges).dropDuplicates(["from_id", "to_id"])
        m = spark.read.parquet(a.v3_map)
        n = float(m.count())
        # after dropDuplicates, (from_id, to_id) is unique -> count(*) == distinct source domains
        indeg = edges.groupBy("to_id").agg((F.count("*") / F.lit(n)).alias("frac"))
        flagged = (
            indeg.where(F.col("frac") > a.threshold)
            .join(m, indeg.to_id == m.id)
            .select("id", "domain", "frac")
        )
        flagged.write.mode("overwrite").parquet(a.out)
        n_flagged = flagged.count()
        print(f"=== boilerplate flagged = {n_flagged:,} (threshold={a.threshold}) -> {a.out} ===")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
