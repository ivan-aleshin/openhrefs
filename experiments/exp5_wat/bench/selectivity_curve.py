"""Exp 5 selectivity curve — WAT fraction vs target-scope size. Diagnostic / provenance script.

Inputs are durable Exp 5 outputs under gs://openhrefs-data/raw/exp5/ (NOT the
lifecycle-deleted _staging/), so this stays re-runnable. Reads expected_edges (domain_from,
domain_to) + source_wat_files (registered_domain, wat_path). For a random sample of K target
domains, finds their inbound source domains and counts the distinct WAT files they occupy.
Sweeps K to show where crawl_fraction drops below 1 (i.e. how narrow the product scope must be
for WAT-filtering to pay off). gs:// only, no WAT read.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

EXP5 = "gs://openhrefs-data/raw/exp5"
TOTAL_WAT = 100000

spark = SparkSession.builder.appName("exp5-selectivity-curve").getOrCreate()
edges = spark.read.parquet(f"{EXP5}/expected_edges").select("domain_from", "domain_to").cache()
swf = spark.read.parquet(f"{EXP5}/source_wat_files").select("registered_domain", "wat_path").cache()

targets = edges.select("domain_to").distinct().cache()
n_targets = targets.count()
print(f"SELCURVE n_targets={n_targets}")

for k in [10, 100, 1000, 10000, 100000]:
    if k >= n_targets:
        break
    sample = targets.orderBy(F.rand(42)).limit(k)
    srcs = (
        edges.join(F.broadcast(sample) if k <= 10000 else sample, "domain_to")
        .select(F.col("domain_from").alias("registered_domain"))
        .distinct()
    )
    n_src = srcs.count()
    files = swf.join(srcs, "registered_domain").select("wat_path").distinct().count()
    frac = files / TOTAL_WAT
    print(f"SELCURVE k_targets={k} source_domains={n_src} wat_files={files} fraction={frac:.4f}")

# full set (known baseline)
print(
    f"SELCURVE k_targets={n_targets} source_domains=27559940 wat_files={TOTAL_WAT} fraction=1.0000"
)
spark.stop()
