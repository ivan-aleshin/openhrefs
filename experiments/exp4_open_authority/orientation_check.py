"""Exp 4.1 — edge-orientation sanity check on the global V3 PageRank output.

Convergence + mass=1.0 do NOT prove the edges were read in the right direction: if
`from_id`/`to_id` are swapped, PageRank still converges and conserves mass, but ranks
"who links out a lot" instead of "who is linked to". The cheap discriminator is the
top of the ranking: a correctly-oriented web graph puts the universal authorities
(google.com, youtube.com, facebook.com, wikipedia.org, …) at the top.

Reads `(id, rank)` ranks + `(id, domain)` v3_map, prints the top-N domains by rank.
Top-N is a tiny payload, so the topk rows are broadcast into the join.
"""

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Exp 4.1 — V3 PageRank edge-orientation check.")
    p.add_argument("--ranks", required=True, help="v3_ranks parquet (id, rank)")
    p.add_argument("--v3-map", required=True, help="v3_map parquet (id, domain)")
    p.add_argument("--top", type=int, default=100)
    a = p.parse_args(argv)
    spark = SparkSession.builder.appName("exp4.1-orientation-check").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        ranks = spark.read.parquet(a.ranks)
        v3_map = spark.read.parquet(a.v3_map)
        topk = ranks.orderBy(F.desc("rank")).limit(a.top)
        named = (
            F.broadcast(topk).join(v3_map, "id").orderBy(F.desc("rank")).select("domain", "rank")
        )
        print(f"=== top {a.top} V3 domains by global PageRank ===")
        for i, row in enumerate(named.collect(), start=1):
            print(f"{i:>4}. {row['rank']:.3e}  {row['domain']}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
