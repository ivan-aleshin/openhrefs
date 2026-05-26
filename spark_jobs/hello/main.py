"""Hello-world Dataproc job: read one CDX file and print row count.

Validates that Dataproc Serverless can reach gs://commoncrawl and
that the job submission pipeline works end-to-end.

Expected data volume: ~100 MB compressed per CDX shard (~3-4 M rows).
Last measured Dataproc Serverless cost: TBD — record after first run.
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
