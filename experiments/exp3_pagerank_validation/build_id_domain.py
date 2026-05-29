"""Exp 3 — precompute the id→registered-domain map once (the expensive PSL pass).

Runs the vectorized tldextract mapping over all vertices a single time and writes
(id, domain, is_apex) parquet. host_to_domain / filter_edges / collapse_to_domain then
read this map (a fast join) instead of recomputing the UDF — also a resume checkpoint.
"""

import argparse

from graph_io import compute_id_domain
from pyspark.sql import SparkSession


def main(argv: list[str] | None = None) -> None:
    """Entrypoint."""
    parser = argparse.ArgumentParser(description="Exp 3 — build id->domain map.")
    parser.add_argument("--vertices", required=True, help="vertices (id, reversed-host).")
    parser.add_argument("--out", required=True, help="parquet (id, domain, is_apex).")
    parser.add_argument("--partitions", type=int, default=256)
    args = parser.parse_args(argv)

    spark = SparkSession.builder.appName("exp3-build-id-domain").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        compute_id_domain(spark, args.vertices, args.partitions).write.mode("overwrite").parquet(
            args.out
        )
        print(f"=== wrote id->domain map -> {args.out} ===")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
