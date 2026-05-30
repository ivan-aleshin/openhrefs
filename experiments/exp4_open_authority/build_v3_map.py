"""Exp 4.0 — build v3_map (id, domain) from CommonCrawl's domain vertices.

CC domain vertices are 3-col `<id>\\t<rev_domain>\\t<num_hosts>` (reversed for sort locality,
e.g. `com.example`). CC already aggregated host→PLD via PSL, so the vertex *is* the registered
domain — we only un-reverse it. The PLD-vs-our-tldextract boundary is validated separately by the
Exp 4.0 gate (`gate_checks`); this script just materializes the map and reports id density.
"""
import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

_VERTEX_SCHEMA = "id long, rev_domain string, num_hosts long"


def to_v3_map(vertices: DataFrame) -> DataFrame:
    """3-col CC domain vertices → (id, domain); un-reverse `com.example` → `example.com`."""
    return vertices.select(
        "id", F.concat_ws(".", F.reverse(F.split("rev_domain", "\\."))).alias("domain")
    )


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Exp 4.0 — build v3_map from CC domain vertices.")
    p.add_argument("--vertices", required=True, help="CC domain vertices .txt.gz (3-col TSV)")
    p.add_argument("--out", required=True, help="v3_map parquet (id, domain)")
    p.add_argument("--partitions", type=int, default=256)
    a = p.parse_args(argv)
    spark = SparkSession.builder.appName("exp4.0-build-v3-map").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        vertices = spark.read.option("sep", "\t").schema(_VERTEX_SCHEMA).csv(a.vertices)
        to_v3_map(vertices).repartition(a.partitions).write.mode("overwrite").parquet(a.out)
        # stats from the written parquet (splittable/parallel) — not from `vertices`, whose single
        # non-splittable .gz would be re-read by each action.
        written = spark.read.parquet(a.out)
        n = written.count()
        mx = written.agg(F.max("id")).first()[0]
        print(f"=== n_domains={n:,} max_id={mx:,} contiguous={mx == n - 1} -> {a.out} ===")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
