"""Exp 5 Job 1 — the single expensive cc-index read → narrow projection.

Reads the cc-index columnar crawl partition over s3a, projects
``(registered_domain, content_languages, warc_filename)`` filtered to fetch_status=200,
and writes it to a staging path. This projection is a checkpoint artifact: Job 2's
derivations re-run against it without re-reading cc-index.

Cost: dominated by the cc-index read (~250–300 GB parquet for one crawl, column- and
partition-pruned). Records measured DCU-hr/$ in the README after the run.

Submit (DATAPROC_EXTRA_PKGS ships experiments/; DATAPROC_EXTRA_PROPERTIES carries the s3a
config for the cc-index read — see infra/gcp/submit_s3a.md):
    DATAPROC_IMAGE=<tag> \
    DATAPROC_EXTRA_PKGS="experiments/__init__.py experiments/exp5_wat" \
    DATAPROC_EXTRA_PROPERTIES="<s3a props, see submit_s3a.md>" \
    ./infra/gcp/submit_job.sh \
        experiments/exp5_wat/project_cdx.py \
        --cdx-path s3a://commoncrawl/cc-index/table/cc-main/warc/crawl=CC-MAIN-2026-21/subset=warc \
        --output-path gs://openhrefs-data/raw/exp5/cdx_projection
"""

from __future__ import annotations

import argparse

import structlog
from pyspark.sql import SparkSession

from experiments.exp5_wat.io import read_cdx_projection, write_parquet

log = structlog.get_logger()


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Exp 5 cc-index projection")
    p.add_argument("--cdx-path", required=True)
    p.add_argument("--output-path", required=True)
    return p.parse_args()


def main() -> None:
    args = _parse_args()
    spark = SparkSession.builder.appName("exp5-project-cdx").getOrCreate()
    projection = read_cdx_projection(spark, args.cdx_path)
    write_parquet(projection, args.output_path)
    log.info("exp5.project_cdx.done", output_path=args.output_path)
    spark.stop()


if __name__ == "__main__":
    main()
