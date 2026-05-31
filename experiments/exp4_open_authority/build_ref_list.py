"""Exp 4.4 — stratified domain list for the trust-flow export.

Samples domains ACROSS the pilot `open_authority` distribution so the trust-flow validation isn't
head-only: the top-`head` by OA is always kept, plus a per-`log10(OA)`-bucket Bernoulli sample so
the long tail can't swamp the head, capped at `--max-domains` (the export-budget guard). One
registered domain per line → handed to the user for the targeted trust-flow (TF/CF) export.

The stratification logic (`stratified_ref_list`) is a pure transform; `main` is I/O only.
"""

import argparse

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def stratified_ref_list(
    dom: DataFrame, head: int, per_bucket: int, max_domains: int, seed: int = 42
) -> DataFrame:
    """`(domain, oa)` -> `(domain)`: top-`head` by OA always kept + per-`log10(OA)`-bucket sample.

    `sampleBy` (per-key Bernoulli) avoids the within-bucket sort/skew a `row_number` window would
    hit on the ~10s-of-millions long tail. The hard cap keeps all of the head and fills the rest
    from the stratified sample in `crc32(domain)` order — stable across runs, not re-biased to head.
    """
    binned = dom.withColumn("b", F.floor(F.log10(F.col("oa"))).cast("int"))
    # domain-asc tiebreak so the head cut is deterministic even on exact-equal OA values
    head_df = binned.orderBy(F.col("oa").desc(), F.col("domain").asc()).limit(head).select("domain")
    head_n = head_df.count()
    counts = {
        r["b"]: r["cnt"] for r in binned.groupBy("b").agg(F.count("*").alias("cnt")).collect()
    }
    fracs = {b: min(1.0, per_bucket / c) for b, c in counts.items()}
    strat = (
        binned.sampleBy("b", fractions=fracs, seed=seed)
        .select("domain")
        .join(head_df, "domain", "left_anti")  # don't double-count the head
    )
    strat_cap = strat.orderBy(F.crc32(F.col("domain"))).limit(max(max_domains - head_n, 0))
    return head_df.union(strat_cap)


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Exp 4.4 — build the OA-stratified ref-export list.")
    p.add_argument("--oa-ranks", required=True)  # pilot (id, rank)
    p.add_argument("--v3-map", required=True)  # (id, domain)
    p.add_argument("--head", type=int, default=20_000)
    p.add_argument("--per-bucket", type=int, default=10_000)
    p.add_argument("--max-domains", type=int, default=100_000)  # HARD cap (export budget)
    p.add_argument("--out", required=True)
    a = p.parse_args(argv)
    if a.head > a.max_domains:
        raise ValueError(f"--head ({a.head}) must be <= --max-domains ({a.max_domains})")
    spark = SparkSession.builder.appName("exp4.4-build-ref-list").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        oa = (
            spark.read.parquet(a.oa_ranks)
            .select("id", F.col("rank").alias("oa"))
            .where(F.col("oa") > 0)
        )
        m = spark.read.parquet(a.v3_map).select("id", "domain")
        dom = oa.join(m, "id").select("domain", "oa").persist(StorageLevel.DISK_ONLY)  # read 3x
        out = stratified_ref_list(dom, a.head, a.per_bucket, a.max_domains)
        n_out = out.count()
        out.coalesce(1).write.mode("overwrite").option("header", False).csv(a.out)
        print(
            f"=== ref list domains={n_out:,} head={a.head:,} cap={a.max_domains:,} -> {a.out} ==="
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
