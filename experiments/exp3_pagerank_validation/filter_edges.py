"""Exp 3 — V2: drop intra-domain host edges (keep only cross-registered-domain links).

Output is a host-level edge set with internal links removed; PageRank then runs on it
(host granularity preserved, N unchanged) warm-started from V1:

    analyze.py --pagerank-only --edges-path <out> --n-vertices 279356058 \\
        --resume-from <V1 host ranks> --start-iter 0 --ranks-out <V2 host ranks>
"""

import argparse

from graph_io import read_edges, read_map
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main(argv: list[str] | None = None) -> None:
    """Entrypoint."""
    parser = argparse.ArgumentParser(description="Exp 3 — V2 edge filter (drop intra-domain).")
    parser.add_argument("--edges", required=True, help="host edges (parquet or tsv).")
    parser.add_argument("--id-domain", required=True, help="precomputed map (id, domain, is_apex).")
    parser.add_argument(
        "--out", required=True, help="filtered host edges parquet (from_id, to_id)."
    )
    args = parser.parse_args(argv)

    spark = SparkSession.builder.appName("exp3-filter-edges").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        idmap = read_map(spark, args.id_domain).select("id", "domain")
        edges = read_edges(spark, args.edges)
        fd = idmap.withColumnRenamed("id", "from_id").withColumnRenamed("domain", "from_domain")
        td = idmap.withColumnRenamed("id", "to_id").withColumnRenamed("domain", "to_domain")
        external = (
            edges.join(fd, "from_id")
            .join(td, "to_id")
            .where(F.col("from_domain") != F.col("to_domain"))
            .select("from_id", "to_id")
        )
        external.write.mode("overwrite").parquet(args.out)
        print(f"=== wrote external host edges -> {args.out} ===")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
