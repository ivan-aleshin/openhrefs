"""Hello-world Dataproc job: read one CDX file and print row count.

Validates that Dataproc Serverless can read a CDX shard from GCS and
that the job submission pipeline works end-to-end.

Expected data volume: ~630 MB compressed per CDX shard (~9 M rows).
Last measured Dataproc Serverless cost: ~$0.013 per run (CC-MAIN-2024-51
cdx-00000.gz, 9,237,013 rows; 0.20 DCU-hours + 0.03 GB-month shuffle,
us-central1, 2026-05-27).
"""

import argparse
import sys

import structlog
from pyspark.sql import SparkSession

from spark_jobs.hello import io, transforms

log = structlog.get_logger()


def main(argv: list[str] | None = None) -> None:
    """Entrypoint: parse args, run job, log result."""
    parser = argparse.ArgumentParser(description="Count rows in a CDX cluster-index file.")
    parser.add_argument(
        "--cdx-path",
        required=True,
        help="Path to CDX file(s). GCS URI (gs://...) or local path. Glob patterns accepted.",
    )
    args = parser.parse_args(argv)

    spark = SparkSession.builder.appName("openhrefs-hello").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try:
        raw = io.read_cdx_text(spark, args.cdx_path)
        filtered = transforms.drop_empty_lines(raw)
        count = filtered.count()
        log.info("cdx_row_count", count=count, path=args.cdx_path)
    finally:
        spark.stop()


if __name__ == "__main__":
    main(sys.argv[1:])
