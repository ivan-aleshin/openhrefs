"""Exp 4.4 — PSL-normalize + dedupe the trust reference CSV to registered domains.

The trust reference export lists raw hosts; the V3 graph is keyed by registered domain. Normalize
hosts via the pinned PSL (`domain_utils.registered_domain`) and dedupe to one row per registered
domain so the reference joins cleanly to `v3_map` / `open_authority`. PSL lives only on this path —
`seed_transforms` stays tldextract-free (the consensus seed is already registered-domain), so
`build_seed_vector` submits need no pydeps zip; only this `normalize_ref` submit does.

Carries `ref_trust` (trusted-authority score), `ref_volume` (link-volume score), and the coverage
fields `status` / `ref_domains` / `ext_backlinks` — `status` separates "found-but-zero-trust" from
"not found", a primary coverage-vs-trust result, so it must survive the normalize/join.

`normalize_ref_domains` is a pure transform; `main` is I/O only.
"""

import argparse

from domain_utils import registered_domain
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

_reg = F.udf(registered_domain, T.StringType())


def _status_priority():
    """Per-domain dedupe priority — prefer the most-informative status across host rows.

    Built inside a function (not at import) because creating a Column with literals needs an active
    Spark context, which isn't present at module import / test collection.
    """
    return (
        F.when(F.col("_status") == "Found", 3)
        .when(F.col("_status") == "MayExist", 2)
        .when(F.col("_status") == "NotFound", 1)
        .otherwise(0)
    )


def normalize_ref_domains(
    df: DataFrame,
    host_col: str,
    trust_col: str,
    volume_col: str,
    *,
    status_col: str = "status",
    refdomains_col: str = "ref_domains",
    extbacklinks_col: str = "ext_backlinks",
) -> DataFrame:
    """Reference rows -> `(domain, ref_trust, ref_volume, ref_domains, ext_backlinks, status)`,
    PSL-normalized and deduped per registered domain.

    Numerics are maxed independently per domain (a domain's rows may differ); `status` takes its
    highest priority (Found > MayExist > NotFound). Rows with null/non-castable `ref_trust` are
    dropped (a null poisons the rank metrics); a genuine `0` is kept, so zero-trust rows survive.
    """
    typed = df.select(
        _reg(F.col(host_col)).alias("domain"),
        F.col(trust_col).cast("double").alias("ref_trust"),
        F.col(volume_col).cast("double").alias("ref_volume"),
        F.col(refdomains_col).cast("long").alias("ref_domains"),
        F.col(extbacklinks_col).cast("long").alias("ext_backlinks"),
        F.col(status_col).alias("_status"),
    ).where(F.col("domain").isNotNull() & F.col("ref_trust").isNotNull())
    return (
        typed.withColumn("_prio", _status_priority())
        .groupBy("domain")
        .agg(
            F.max("ref_trust").alias("ref_trust"),
            F.max("ref_volume").alias("ref_volume"),
            F.max("ref_domains").alias("ref_domains"),
            F.max("ext_backlinks").alias("ext_backlinks"),
            F.max(F.struct("_prio", "_status")).alias("_best"),
        )
        .select(
            "domain",
            "ref_trust",
            "ref_volume",
            "ref_domains",
            "ext_backlinks",
            F.col("_best._status").alias("status"),
        )
    )


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Exp 4.4 — normalize the trust reference.")
    p.add_argument("--ref-csv", required=True)
    p.add_argument("--host-col", default="domain")
    p.add_argument("--trust-col", default="ref_trust")
    p.add_argument("--volume-col", default="ref_volume")
    p.add_argument("--status-col", default="status")
    p.add_argument("--refdomains-col", default="ref_domains")
    p.add_argument("--extbacklinks-col", default="ext_backlinks")
    p.add_argument("--out", required=True)
    a = p.parse_args(argv)
    spark = SparkSession.builder.appName("exp4.4-normalize-ref").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        raw = spark.read.option("header", True).csv(a.ref_csv)
        raw_n = raw.count()
        out = normalize_ref_domains(
            raw,
            a.host_col,
            a.trust_col,
            a.volume_col,
            status_col=a.status_col,
            refdomains_col=a.refdomains_col,
            extbacklinks_col=a.extbacklinks_col,
        )
        n_out = out.count()
        print(f"=== ref: {raw_n:,} rows -> {n_out:,} domains (null trust dropped) -> {a.out} ===")
        out.write.mode("overwrite").parquet(a.out)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
