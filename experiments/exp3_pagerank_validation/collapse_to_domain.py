"""Exp 3 — V3: collapse the host graph to a domain graph (≈ OpenPageRank construction).

Maps both edge endpoints to their registered domain (via the precomputed id→domain
map), drops intra-domain self-loops, dedupes to unique domain→domain edges, assigns
dense 0-based domain ids, and writes the domain edge list + id→domain map. PageRank then
runs on the (smaller) domain graph:

    analyze.py --pagerank-only --edges-path <out-edges> --n-vertices <n_domains> \\
        --ranks-out <V3 domain-id ranks>
    join_opr.py --our-ranks <V3 domain-id ranks> --domain-map <out-map> ...

Checkpoint: the expensive 13.4B-edge join+distinct writes `--pairs` first; on rerun, if
`--pairs` exists it is reused (the cheap id-assignment is redone), so a TTL/quota kill
does not recompute the heavy step.
"""

import argparse

from graph_io import read_edges, read_map
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def _exists(spark: SparkSession, path: str) -> bool:
    """True if `path` has a committed `_SUCCESS` marker."""
    jvm = spark.sparkContext._jvm
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    marker = jvm.org.apache.hadoop.fs.Path(path.rstrip("/") + "/_SUCCESS")
    return marker.getFileSystem(conf).exists(marker)


def main(argv: list[str] | None = None) -> None:
    """Entrypoint."""
    parser = argparse.ArgumentParser(description="Exp 3 — V3 collapse to domain graph.")
    parser.add_argument("--edges", required=True, help="host edges (parquet or tsv).")
    parser.add_argument("--id-domain", required=True, help="precomputed map (id, domain, is_apex).")
    parser.add_argument(
        "--pairs", required=True, help="intermediate distinct domain edges (checkpoint)."
    )
    parser.add_argument("--out-edges", required=True, help="domain edge parquet (from_id, to_id).")
    parser.add_argument("--out-map", required=True, help="domain map parquet (id, domain).")
    args = parser.parse_args(argv)

    spark = SparkSession.builder.appName("exp3-collapse-domain").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        if _exists(spark, args.pairs):
            print(f"=== resume: reusing existing domain pairs {args.pairs} ===")
        else:
            idmap = read_map(spark, args.id_domain).select("id", "domain")
            edges = read_edges(spark, args.edges)
            fd = idmap.withColumnRenamed("id", "from_id").withColumnRenamed("domain", "from_domain")
            td = idmap.withColumnRenamed("id", "to_id").withColumnRenamed("domain", "to_domain")
            (
                edges.join(fd, "from_id")
                .join(td, "to_id")
                .where(F.col("from_domain") != F.col("to_domain"))
                .select("from_domain", "to_domain")
                .distinct()
                .write.mode("overwrite")
                .parquet(args.pairs)
            )
            print(f"=== wrote distinct domain pairs -> {args.pairs} ===")

        domain_edges = spark.read.parquet(args.pairs)
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
