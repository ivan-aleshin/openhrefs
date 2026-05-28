"""Exp 2 — salvage a reliable RDD checkpoint into parquet.

The exp2 PageRank batch was killed by the 4h Serverless TTL at ~iter 12. Its
reliable checkpoints survive in GCS as LZ4-compressed Java-serialized
`InternalRow` parts (rdd-238/460/682 = iter 4/8/12). That format is only
readable by Spark's internal `ReliableCheckpointRDD`, so we reach it through
the `protected[spark]` `SparkContext.checkpointFile` via py4j, wrap the
resulting `RDD[InternalRow]` back into a DataFrame with the original ranks
schema, and persist it as parquet so the run can be resumed cleanly.

Must run on the SAME runtime version that wrote the checkpoint (2.2.81): the
serialized payload is Spark-internal and version-sensitive.

Usage (run on Dataproc Serverless):
  recover_checkpoint.py --rdd-path gs://.../exp2-ckpt/<app>/rdd-682 \\
      --out gs://.../exp2-resume/ranks_iter12
"""

import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

RANKS_SCHEMA = T.StructType(
    [
        T.StructField("id", T.LongType()),
        T.StructField("rank", T.DoubleType()),
    ]
)


def read_checkpoint(spark: SparkSession, rdd_path: str) -> DataFrame:
    """Read a reliable RDD[InternalRow] checkpoint back into a DataFrame.

    Uses the internal `SparkContext.checkpointFile[InternalRow]` and
    `SparkSession.internalCreateDataFrame` (both bytecode-public) via py4j.
    """
    sc = spark.sparkContext
    jvm = sc._jvm
    jsc = sc._jsc.sc()

    internal_row_cls = jvm.java.lang.Class.forName("org.apache.spark.sql.catalyst.InternalRow")
    class_tag = jvm.scala.reflect.ClassTag.apply(internal_row_cls)
    jrdd = jsc.checkpointFile(rdd_path, class_tag)

    jschema = spark._jsparkSession.parseDataType(RANKS_SCHEMA.json())
    jdf = spark._jsparkSession.internalCreateDataFrame(jrdd, jschema, False)
    return DataFrame(jdf, spark)


def main(argv: list[str] | None = None) -> None:
    """Read the checkpoint, verify ranks invariants, write parquet."""
    parser = argparse.ArgumentParser(description="Exp 2 — recover checkpoint to parquet.")
    parser.add_argument("--rdd-path", required=True, help="gs://.../rdd-<id> directory.")
    parser.add_argument("--out", required=True, help="parquet output prefix.")
    parser.add_argument("--n-vertices", type=int, default=279_356_058)
    args = parser.parse_args(argv)

    spark = SparkSession.builder.appName("exp2-recover-checkpoint").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        ranks = read_checkpoint(spark, args.rdd_path).persist()
        n, mass = ranks.agg(F.count("*"), F.sum("rank")).first()
        print("\n=== Recovered checkpoint ===")
        print(f"rdd-path:    {args.rdd_path}")
        print(f"rows:        {n:,} (expected {args.n_vertices:,})")
        print(f"sum(rank):   {mass:.6f} (expected ~1.0)")
        if n != args.n_vertices:
            print("WARNING: row count != n_vertices — checkpoint may be a different RDD")

        ranks.write.mode("overwrite").parquet(args.out)
        print(f"=== wrote ranks parquet -> {args.out} ===")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
