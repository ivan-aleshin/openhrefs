"""Experiment 1 — CDX language extraction.

Validates the CDX `languages` field as the source for Stage 1 language
classification: coverage, language-code sanity, and the 5% storage threshold
(SPEC §5 Stage 1). Also reports sensitivity around example `min_language_share`
values — an informational, user-configurable scope parameter, not calibrated
here.

Exploratory one-off (experiments rules: ruff-clean, no mypy/tests). Runs
locally on a single CDX shard:

    uv run python -m experiments.exp1_cdx_language.analyze \
        --cdx-path /tmp/cdx-00000.gz

CDX line format: `<surt-key> <timestamp> <json>` where json carries
`status`, `url`, `languages` (ISO 639-3, comma-separated, ordered), `charset`.
"""

import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

TARGET_LANGS = ["bul", "ron"]
STORAGE_THRESHOLD = 0.05
SHARE_DEFAULTS = [0.25, 0.30]

_JSON_SCHEMA = T.StructType(
    [
        T.StructField("status", T.StringType()),
        T.StructField("url", T.StringType()),
        T.StructField("languages", T.StringType()),
        T.StructField("charset", T.StringType()),
    ]
)


def parse_cdx(spark: SparkSession, path: str) -> DataFrame:
    """Read CDX text and project status, url, languages, primary lang, domain."""
    raw = spark.read.text(path)
    json_str = F.regexp_extract("value", r"^\S+ \S+ (\{.*\})$", 1)
    parsed = raw.select(F.from_json(json_str, _JSON_SCHEMA).alias("r")).select("r.*")
    host = F.parse_url(F.col("url"), F.lit("HOST"))
    # root_only: registrable domain approximated as last two labels.
    # Correct for single-level TLDs (.bg, .ro); production needs a public
    # suffix list for multi-level TLDs (.co.uk).
    labels = F.split(host, r"\.")
    n = F.size(labels)
    two_label = F.concat_ws(".", F.element_at(labels, -2), F.element_at(labels, -1))
    domain = F.when(n >= 2, two_label).otherwise(host)
    primary = F.when(
        (F.col("languages").isNotNull()) & (F.col("languages") != ""),
        F.split("languages", ",").getItem(0),
    )
    return parsed.select(
        "status",
        "url",
        "languages",
        F.when(host.isNull() | (host == ""), None).otherwise(domain).alias("domain"),
        primary.alias("primary_lang"),
    )


def report_coverage(df: DataFrame) -> None:
    """% of records carrying a non-empty languages field, overall and on 200s."""
    total = df.count()
    with_lang = df.filter(F.col("primary_lang").isNotNull()).count()
    ok = df.filter(F.col("status") == "200")
    ok_total = ok.count()
    ok_lang = ok.filter(F.col("primary_lang").isNotNull()).count()
    print("\n=== Coverage ===")
    print(f"total records:           {total:,}")
    print(f"with languages:          {with_lang:,} ({with_lang / total:.1%})")
    print(f"status 200 records:      {ok_total:,} ({ok_total / total:.1%})")
    print(f"  of those w/ languages: {ok_lang:,} ({ok_lang / ok_total:.1%})")


def report_distribution(df: DataFrame) -> None:
    """Per-language page counts by primary language, top 20 + target langs."""
    langs = (
        df.filter(F.col("primary_lang").isNotNull())
        .groupBy("primary_lang")
        .count()
        .orderBy(F.desc("count"))
    )
    total = df.filter(F.col("primary_lang").isNotNull()).count()
    print("\n=== Primary-language distribution (top 20) ===")
    for row in langs.limit(20).collect():
        lang = row["primary_lang"] or "<none>"
        cnt = row["count"]
        print(f"  {lang:<8} {cnt:>10,} ({cnt / total:.2%})")
    print("\n=== Target languages present? ===")
    for lang in TARGET_LANGS:
        c = langs.filter(F.col("primary_lang") == lang).collect()
        n = c[0]["count"] if c else 0
        print(f"  {lang}: {n:,} pages")


def report_domain_shares(df: DataFrame) -> None:
    """Per-domain language shares with the 5% threshold; min_language_share sensitivity."""
    pages = df.filter(F.col("primary_lang").isNotNull() & F.col("domain").isNotNull())
    per_domain_lang = pages.groupBy("domain", "primary_lang").count()
    per_domain_total = pages.groupBy("domain").agg(F.count("*").alias("domain_pages"))
    shares = (
        per_domain_lang.join(per_domain_total, "domain")
        .withColumn("share", F.col("count") / F.col("domain_pages"))
        .filter(F.col("share") >= STORAGE_THRESHOLD)
    )

    total_domains = per_domain_total.count()
    kept = shares.select("domain").distinct().count()
    print("\n=== Domains (root_only) ===")
    print(f"distinct domains:               {total_domains:,}")
    print(f"domains w/ >=1 lang above 5%:   {kept:,}")

    print("\n=== min_language_share sensitivity (target langs bul/ron) ===")
    target = shares.filter(F.col("primary_lang").isin(TARGET_LANGS))
    for thr in SHARE_DEFAULTS:
        q = target.filter(F.col("share") >= thr).select("domain").distinct().count()
        print(f"  domains with bul/ron share >= {thr:.2f}: {q:,}")


def main(argv: list[str] | None = None) -> None:
    """Entrypoint: parse CDX shard and print all experiment measurements."""
    parser = argparse.ArgumentParser(description="Exp 1 — CDX language extraction.")
    parser.add_argument("--cdx-path", default="/tmp/cdx-00000.gz")
    args = parser.parse_args(argv)

    spark = SparkSession.builder.appName("exp1-cdx-language").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        df = parse_cdx(spark, args.cdx_path).cache()
        report_coverage(df)
        report_distribution(df)
        report_domain_shares(df)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
