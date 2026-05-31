"""Exp 4.4 — PSL-normalize + dedupe the trust-flow reference CSV to registered domains.

The trust-flow export lists raw hosts; the V3 graph is keyed by registered domain. Normalize hosts
via the pinned PSL (`domain_utils.registered_domain`) and dedupe to one row per registered domain so
the reference joins cleanly to `v3_map` / `open_authority`. PSL lives only on this path —
`seed_transforms` stays tldextract-free (the consensus seed is already registered-domain), so
`build_seed_vector` submits need no pydeps zip; only this `normalize_ref` submit does.

`normalize_ref_domains` is a pure transform; `main` is I/O only.
"""

import argparse

from domain_utils import registered_domain
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

_reg = F.udf(registered_domain, T.StringType())


def normalize_ref_domains(df: DataFrame, host_col: str, tf_col: str, cf_col: str) -> DataFrame:
    """Reference rows `(host, tf, cf)` → `(domain, tf, cf)`, PSL-normalized, maxed per domain."""
    return (
        df.select(
            _reg(F.col(host_col)).alias("domain"),
            F.col(tf_col).cast("double").alias("tf"),
            F.col(cf_col).cast("double").alias("cf"),
        )
        # drop rows with null / non-castable tf (cast → null): tf drives the gate, a null would
        # poison Spearman/τ downstream
        .where(F.col("domain").isNotNull() & F.col("tf").isNotNull())
        .groupBy("domain")
        # tf and cf maxed INDEPENDENTLY — a domain's (tf, cf) may come from different source rows;
        # fine here, tf drives the gate and cf is only a loose volume sanity
        .agg(F.max("tf").alias("tf"), F.max("cf").alias("cf"))
    )


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Exp 4.4 — normalize trust-flow reference.")
    p.add_argument("--ref-csv", required=True)
    p.add_argument("--host-col", default="domain")
    p.add_argument("--tf-col", default="tf")
    p.add_argument("--cf-col", default="cf")
    p.add_argument("--out", required=True)
    a = p.parse_args(argv)
    spark = SparkSession.builder.appName("exp4.4-normalize-ref").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        raw = spark.read.option("header", True).csv(a.ref_csv)
        raw_n = raw.count()
        out = normalize_ref_domains(raw, a.host_col, a.tf_col, a.cf_col)
        n_out = out.count()
        print(f"=== ref: {raw_n:,} rows -> {n_out:,} domains (null tf dropped) -> {a.out} ===")
        out.write.mode("overwrite").parquet(a.out)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
