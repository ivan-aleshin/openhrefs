"""Exp 3 — packaging smoke test. Verifies domain_utils/graph_io/tldextract load on
Dataproc executors (incl. tldextract's offline PSL snapshot from the --py-files zip)
before the expensive graph runs. Cheap (~$0.01).
"""

import graph_io  # noqa: F401  — import-only check that the shared module loads
from domain_utils import registered_domain, unreverse_host
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


def main() -> None:
    """Apply the PSL UDF to a tiny in-memory DataFrame (runs on executors)."""
    spark = SparkSession.builder.appName("exp3-smoke-psl").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        udf = F.udf(lambda h: registered_domain(unreverse_host(h)), T.StringType())
        hosts = ["com.facebook.www", "uk.co.bbc.foo", "br.com.site.a.b", "org.python"]
        df = spark.createDataFrame([(i, h) for i, h in enumerate(hosts)], ["id", "rev_host"])
        print("=== PSL smoke (rev_host -> registered domain) ===")
        for r in df.withColumn("domain", udf("rev_host")).collect():
            print(f"  {r['rev_host']} -> {r['domain']}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
