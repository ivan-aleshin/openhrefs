"""Exp 4.0 — Graph-Adoption Gate checks on the CC domain graph v3_map.

Three checks whose numbers drive ACCEPT / REJECT / AMBIGUOUS (see the gate doc):
  - boundary equivalence (PRIMARY): `registered_domain(d) == d` on a deterministic hash sample —
    catches the whole class of CC-PSL-vs-our-tldextract divergence, not just named platforms;
  - platform canaries: high-cardinality private-suffix platforms must be single nodes;
  - seed coverage (optional): top-N consensus seed mappable into v3_map.

Submit with `--py-files=<exp4 domain_utils.py>,<exp3 pydeps zip>` — it imports `registered_domain`
→ `tldextract` at module load.
"""
import argparse

from domain_utils import registered_domain
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

_reg = F.udf(registered_domain, T.StringType())

_CANARIES = (
    "blogspot.com", "wordpress.com", "github.io", "pages.dev", "netlify.app", "wixsite.com",
    "weebly.com", "webflow.io", "herokuapp.com", "vercel.app", "appspot.com", "firebaseapp.com",
    "readthedocs.io",
)


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Exp 4.0 — graph-adoption gate checks.")
    p.add_argument("--v3-map", required=True)
    p.add_argument("--sample-mod", type=int, default=1000, help="hash %% mod < 1 → ~1/mod sample")
    p.add_argument("--seed-csv", help="consensus seed csv.gz (enables the seed-coverage check)")
    p.add_argument("--seed-domain-col", default="registered_domain")
    p.add_argument("--seed-score-col", default="consensus_score")
    p.add_argument("--seed-size", type=int, default=10_000)
    a = p.parse_args(argv)
    spark = SparkSession.builder.appName("exp4.0-gate-checks").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        vmap = spark.read.parquet(a.v3_map)

        # --- Boundary equivalence (PRIMARY) ---
        samp = vmap.where((F.abs(F.hash("domain")) % a.sample_mod) < 1)  # deterministic ~1/mod
        chk = samp.select("domain", _reg("domain").alias("reg")).persist()
        n = chk.count()
        nulls = chk.where(F.col("reg").isNull()).count()
        mism = chk.where(F.col("reg").isNotNull() & (F.col("reg") != F.col("domain")))
        m = mism.count()
        rate = m / n if n else 0.0
        print(f"=== BOUNDARY: sample={n:,} null={nulls:,} mismatch={m:,} rate={rate:.4%} ===")
        print("=== top mismatch reg-clusters (large count => platform/private-PSL split) ===")
        mism.groupBy("reg").count().orderBy(F.desc("count")).show(20, False)
        chk.unpersist()

        # --- Platform canaries (expect self=1, sub_nodes ~ 0) ---
        print("=== CANARIES ===")
        for d in _CANARIES:
            self_n = vmap.where(F.col("domain") == d).count()
            sub_n = vmap.where(F.col("domain").endswith("." + d)).count()
            print(f"  {d}: self={self_n} sub_nodes={sub_n}")

        # --- Seed coverage (optional) ---
        if a.seed_csv:
            seed = (
                spark.read.option("header", True).csv(a.seed_csv)
                .select(F.col(a.seed_domain_col).alias("domain"),
                        F.col(a.seed_score_col).cast("double").alias("score"))
                .where(F.col("score").isNotNull())
                .orderBy(F.col("score").desc(), F.col("domain").asc())
                .limit(a.seed_size)
            )
            seed_n = seed.count()
            covered = seed.join(vmap.select("domain"), "domain").count()
            cov = covered / seed_n if seed_n else 0.0
            print(f"=== SEED COVERAGE: top-{a.seed_size} "
                  f"covered={covered:,}/{seed_n:,} = {cov:.2%} ===")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
