"""Stage 2 validation gate — PageRank vs OpenPageRank overlap (SPEC §5).

Joins the Stage 2 ``cc_domain_pagerank`` output with the OpenPageRank reference and
writes the small per-crawl overlap Parquet ``(domain, our_pr, opr_score)``, then logs
full-output stats (count, mass sum, range, nulls) and the overlap row count. Rank
metrics (Spearman, Kendall, RBO) are computed off-cluster from the overlap by
``tools.validation.opr_rank_metrics`` — they pull in scipy/numpy, which stay out of the
Serverless runtime image.

Cost: a single join + one full scan of ``cc_domain_pagerank`` (118.76M rows for
``cc-main-2026-mar-apr-may``); ~$0.2 / run, no power iteration.
"""

from __future__ import annotations

import argparse
import sys

import structlog
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from spark_jobs.common.config import checked_stage_output_path
from spark_jobs.validation.stage2_opr import io, transforms

log = structlog.get_logger()


def main(argv: list[str] | None = None) -> None:
    """Entrypoint: parse args, run the gate, stop the session."""
    args = _parse_args(argv)
    spark = SparkSession.builder.appName("openhrefs-stage2-opr-validation").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        _run(spark, args)
    finally:
        spark.stop()


def _run(spark: SparkSession, args: argparse.Namespace) -> None:
    """Read PageRank + OPR, write the overlap, log full-output stats and overlap size."""
    pagerank = transforms.filter_crawl(io.read_pagerank(spark, args.pagerank_path), args.crawl)
    opr = transforms.normalize_opr(
        io.read_opr_csv(spark, args.opr_csv), args.opr_domain_col, args.opr_score_col
    )
    overlap = transforms.to_overlap(pagerank, opr).withColumn("crawl", F.lit(args.crawl))
    io.write_overlap(overlap, args.output_path)

    stats = transforms.output_stats(pagerank).first()
    assert stats is not None  # a group-less agg always yields one row
    log.info(
        "stage2_opr_validation_complete",
        crawl=args.crawl,
        output=args.output_path,
        pagerank_count=stats["pagerank_count"],
        pagerank_sum=stats["pagerank_sum"],
        pagerank_min=stats["pagerank_min"],
        pagerank_max=stats["pagerank_max"],
        null_count=stats["null_count"],
        domain_null_count=stats["domain_null_count"],
        overlap_count=overlap.count(),
    )


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Stage 2 PageRank vs OpenPageRank validation gate.")
    p.add_argument("--pagerank-path", required=True, help="Stage 2 cc_domain_pagerank Parquet.")
    p.add_argument("--opr-csv", required=True, help="OpenPageRank reference CSV (header).")
    p.add_argument("--crawl", required=True, help="Crawl identifier stamped on the overlap.")
    p.add_argument(
        "--output-path", required=True, help="Overlap Parquet root (partitioned by crawl)."
    )
    p.add_argument("--opr-domain-col", default="Domain", help="OPR CSV host column.")
    p.add_argument("--opr-score-col", default="Open Page Rank", help="OPR CSV score column.")
    args = p.parse_args(argv)
    args.output_path = checked_stage_output_path(args.output_path)
    return args


if __name__ == "__main__":
    main(sys.argv[1:])
