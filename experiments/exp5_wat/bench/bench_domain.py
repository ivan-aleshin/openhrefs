"""Exp 5 domain-extraction benchmark — UDF-per-row vs dedup. Diagnostic / provenance script.

Reads the durable warcio backlinks_sample under gs://openhrefs-data/raw/exp5/runs/ (NOT the
lifecycle-deleted _staging/), so this stays re-runnable. Isolates THE lever (domain
extraction) on existing data (203M rows with url_to), no WAT parse, no downstream metrics.
Times two ways to produce the target registered domain, back-to-back on the same cluster:
  A) per-row tldextract UDF over all url_to (what backlink_edges does today)
  B) dedup: native parse_url(HOST) -> distinct hosts -> UDF only on distinct -> count
Both end in distinct-domain count (same action), so the wall delta isolates the UDF-call-count
effect. Relative wall on one cluster ~ relative cost.
"""

import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from experiments.exp5_wat.transforms import registered_domain_of_url
from spark_jobs.common.domains import registered_domain

SRC = "gs://openhrefs-data/raw/exp5/runs/throughput-1k-warcio/backlinks_sample"

spark = SparkSession.builder.appName("exp5-bench-domain").getOrCreate()
df = spark.read.parquet(SRC).select("url_to").cache()
n_rows = df.count()
print(f"BENCH rows={n_rows}")

rdu_udf = F.udf(registered_domain_of_url)  # url -> registered domain (current path)
rd_udf = F.udf(registered_domain)  # host -> registered domain (for dedup path)

# A) per-row UDF over all url_to
t = time.time()
a = (
    df.select(rdu_udf("url_to").alias("d"))
    .where(F.col("d").isNotNull())
    .select("d")
    .distinct()
    .count()
)
ta = time.time() - t
print(f"BENCH A_udf_perrow distinct_domains={a} time={ta:.1f}s udf_calls={n_rows}")

# B) dedup: native host -> distinct -> UDF on distinct hosts only
t = time.time()
hosts = df.select(F.expr("parse_url(url_to, 'HOST')").alias("host")).where(
    F.col("host").isNotNull()
)
distinct_hosts = hosts.select("host").distinct().cache()
n_hosts = distinct_hosts.count()
b = (
    distinct_hosts.select(rd_udf("host").alias("d"))
    .where(F.col("d").isNotNull())
    .select("d")
    .distinct()
    .count()
)
tb = time.time() - t
print(f"BENCH B_dedup distinct_domains={b} time={tb:.1f}s udf_calls={n_hosts} (vs {n_rows})")

print(f"BENCH SPEEDUP_walltime={ta / tb:.2f}x  udf_call_reduction={n_rows / max(n_hosts, 1):.1f}x")
spark.stop()
