"""Exp 5 tight profile — confirm two theories in practice. Diagnostic / provenance script.

NOTE: reads staged WAT under gs://.../raw/exp5/_staging/{wat_1k,watlist_1k}, which the bucket
lifecycle auto-deletes (age>=3 days). To re-run after that, re-stage a WAT sample via STS first
(see the README method) and point WAT_PREFIX / the watlist read at it.

Both steps via RDD so sc.show_profiles() captures their Python cProfile (verified locally that
show_profiles emits, and that it includes serialization internals: _struct/io/from_iterable).
  THEORY 1 (parse breakdown): 10 WAT files -> flatMap parse -> count. cProfile shows
            fastwarc / orjson / extract_links / domain_from(tldextract) + serialization.
  THEORY 2 (domain cost = tldextract vs serialization): 5M url_to -> map(registered_domain_of_url)
            -> count. cProfile splits tldextract internals vs pickle/struct serialization.
Small inputs -> fast. The cProfile ratios are the result (wall is secondary, profiler inflates it).
"""

import time

from pyspark.sql import SparkSession

from experiments.exp5_wat.parse_wat import _open_wat
from experiments.exp5_wat.transforms import registered_domain_of_url

EXP5 = "gs://openhrefs-data/raw/exp5"
WAT_PREFIX = f"{EXP5}/_staging/wat_1k/"

spark = (
    SparkSession.builder.appName("exp5-profile-v2")
    .config("spark.python.profile", "true")
    .getOrCreate()
)
sc = spark.sparkContext

# THEORY 1: parse breakdown (10 files)
files = [
    r["wat_path"] for r in spark.read.parquet(f"{EXP5}/_staging/watlist_1k").limit(10).collect()
]
t = time.time()
n_links = (
    sc.parallelize(files, len(files))
    .flatMap(lambda p: _open_wat(p, WAT_PREFIX, "fastwarc"))
    .count()
)
print(f"PROFILE2 parse_links={n_links} files={len(files)} time={time.time() - t:.1f}s")

# THEORY 2: domain extraction cost = tldextract CPU vs serialization (5M url_to sample)
sample = (
    spark.read.parquet(f"{EXP5}/runs/throughput-1k-warcio/backlinks_sample")
    .select("url_to")
    .limit(5_000_000)
    .repartition(50)
)
t = time.time()
n_dom = (
    sample.rdd.map(lambda r: registered_domain_of_url(r["url_to"]))
    .filter(lambda d: d is not None)
    .count()
)
print(f"PROFILE2 domain_resolved={n_dom}/5000000 time={time.time() - t:.1f}s")

print("PROFILE2 === show_profiles (per-RDD cProfile: parse flatMap, then domain map) ===")
sc.show_profiles()
spark.stop()
