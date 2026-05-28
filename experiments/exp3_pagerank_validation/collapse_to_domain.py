"""Exp 3 — V3: collapse the host graph to a domain graph (≈ OpenPageRank construction).

Maps both edge endpoints to their registered domain, drops intra-domain self-loops,
dedupes to unique domain→domain edges, and assigns dense 0-based domain ids (so the
edge parquet feeds analyze.py's PageRank directly). Writes the domain edge list and the
id→domain map; PageRank then runs on the (smaller) domain graph:

    analyze.py --pagerank-only --edges-path <out-edges> --n-vertices <n_domains> \\
        --ranks-out <V3 domain-id ranks>
    join_opr.py --our-ranks <V3 domain-id ranks> --domain-map <out-map> ...
"""

import argparse

from graph_io import read_edges, read_id_domain
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main(argv: list[str] | None = None) -> None:
    """Entrypoint."""
    parser = argparse.ArgumentParser(description="Exp 3 — V3 collapse to domain graph.")
    parser.add_argument("--edges", required=True, help="host edges (parquet or tsv).")
    parser.add_argument("--vertices", required=True, help="vertices (id, reversed-host).")
    parser.add_argument("--out-edges", required=True, help="domain edge parquet (from_id, to_id).")
    parser.add_argument("--out-map", required=True, help="domain map parquet (id, domain).")
    args = parser.parse_args(argv)

    spark = SparkSession.builder.appName("exp3-collapse-domain").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        idmap = read_id_domain(spark, args.vertices).select("id", "domain")
        edges = read_edges(spark, args.edges)
        fd = idmap.withColumnRenamed("id", "from_id").withColumnRenamed("domain", "from_domain")
        td = idmap.withColumnRenamed("id", "to_id").withColumnRenamed("domain", "to_domain")
        domain_edges = (
            edges.join(fd, "from_id")
            .join(td, "to_id")
            .where(F.col("from_domain") != F.col("to_domain"))
            .select("from_domain", "to_domain")
            .distinct()
        )

        domains = (
            domain_edges.select(F.col("from_domain").alias("domain"))
            .union(domain_edges.select(F.col("to_domain").alias("domain")))
            .distinct()
        )
        # dense contiguous 0-based ids so analyze.py's spark.range(0, n) covers all nodes
        dmap = domains.rdd.zipWithIndex().map(lambda r: (r[0][0], r[1])).toDF(["domain", "id"])
        dmap = dmap.persist()

        fm = dmap.withColumnRenamed("domain", "from_domain").withColumnRenamed("id", "from_id")
        tm = dmap.withColumnRenamed("domain", "to_domain").withColumnRenamed("id", "to_id")
        out_edges = (
            domain_edges.join(fm, "from_domain").join(tm, "to_domain").select("from_id", "to_id")
        )

        out_edges.write.mode("overwrite").parquet(args.out_edges)
        dmap.select("id", "domain").write.mode("overwrite").parquet(args.out_map)
        print(f"=== n_domains = {dmap.count():,} ===")
        print(f"=== wrote domain edges -> {args.out_edges} ; map -> {args.out_map} ===")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
