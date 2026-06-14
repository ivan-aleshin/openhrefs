"""Exp 5 Job 3 — parse selected WAT files, extract backlinks, compute metrics.

Reads a WAT-path list (full segment_list or a sample) plus target_domains (S) and
expected_edges, distributes WAT iteration across executors, extracts anchor links,
keeps backlinks into S from D_src sources, aggregates to domain edges, and writes the
backlinks sample + a metrics summary.

Recall is computed only when --parsed-sources-path is given (full / domain-targeted
modes, where every page of each counted source was parsed). For file-sample mode, omit
it: recall is not valid (page-coverage lower bound only) — see the design §4.

IMPORTANT: --parsed-sources-path must name **only sources whose WAT files were actually
parsed**, NOT expected_edges. expected_edges carries all graph D_src, including sources
with no 200-page in the projection (hence no WAT file) — counting them would inflate the
denominator with never-parsed sources. For full mode pass `source_wat_files` (every D_src
that has a WAT file = exactly what full parses); for domain-targeted pass the chosen
subset of source domains.

Submit (full) — WAT is read via s3fs (Python), not Spark s3a, so no s3a props are needed;
DATAPROC_EXTRA_PKGS still ships experiments/ to the driver AND executors (the flatMap):
    DATAPROC_IMAGE=<tag> \
    DATAPROC_EXTRA_PKGS="experiments/__init__.py experiments/exp5_wat" \
    ./infra/gcp/submit_job.sh \
        experiments/exp5_wat/parse_wat.py \
        --wat-list-path gs://openhrefs-data/raw/exp5/segment_list \
        --target-domains-path gs://openhrefs-data/raw/exp5/target_domains \
        --expected-edges-path gs://openhrefs-data/raw/exp5/expected_edges \
        --parsed-sources-path gs://openhrefs-data/raw/exp5/source_wat_files \
        --output-root gs://openhrefs-data/raw/exp5 --run-name full

Outputs land under <output-root>/runs/<run-name>/{backlinks_sample,metrics}.
"""

from __future__ import annotations

import argparse
import re

import structlog
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from experiments.exp5_wat.io import iter_wat_links, write_json, write_parquet
from experiments.exp5_wat.transforms import (
    backlink_edges,
    conditional_recall,
    with_wat_prefix,
)

log = structlog.get_logger()

_LINK_SCHEMA = T.StructType(
    [
        T.StructField("domain_from", T.StringType()),
        T.StructField("url_from", T.StringType()),
        T.StructField("url_to", T.StringType()),
        T.StructField("anchor", T.StringType()),
        T.StructField("rel", T.StringType()),
    ]
)


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Exp 5 WAT parse + metrics")
    p.add_argument("--wat-list-path", required=True)
    p.add_argument("--target-domains-path", required=True)
    p.add_argument("--expected-edges-path", required=True)
    p.add_argument(
        "--parsed-sources-path",
        default=None,
        help="Parsed source set (registered_domain or domain_from). When set, it is BOTH "
        "the extraction source filter and the recall denominator (full / domain-targeted).",
    )
    p.add_argument(
        "--wat-prefix",
        default="s3://commoncrawl/",
        help="Prepended to each relative wat_path (cc-index warc_filename is relative).",
    )
    p.add_argument("--output-root", required=True)
    p.add_argument(
        "--run-name",
        required=True,
        help="Mode/run label (e.g. full, file-sample-5pct, domain-targeted). Outputs go to "
        "<output-root>/runs/<run-name>/ so successive modes do not overwrite each other. "
        "Must match ^[a-z0-9][a-z0-9._-]*$ (no slashes/spaces — it becomes a path segment).",
    )
    args = p.parse_args()
    if not re.fullmatch(r"[a-z0-9][a-z0-9._-]*", args.run_name):
        p.error("--run-name must match ^[a-z0-9][a-z0-9._-]*$ (no slashes or spaces)")
    return args


def main() -> None:
    args = _parse_args()
    spark = SparkSession.builder.appName("exp5-parse-wat").getOrCreate()

    # collect() the path list to the driver — fine for the expected ~90k WAT files per
    # crawl. If Job 2 stats show wat_files vastly larger, treat that as a checkpoint signal
    # NOT to run full as-is (driver memory + broadcasting the list), and sample instead.
    wat_paths = [r["wat_path"] for r in spark.read.parquet(args.wat_list_path).collect()]
    target_domains = spark.read.parquet(args.target_domains_path)
    expected = spark.read.parquet(args.expected_edges_path)
    d_src_all = expected.select(F.col("domain_from").alias("registered_domain")).distinct()

    # The extraction source filter MUST match the chosen mode: a sampled WAT file holds
    # many domains, and domain-targeted mode parsed ALL files of a chosen 20–50 sources,
    # so only those sources' pages may pass. When --parsed-sources-path is set it is the
    # source filter (and the recall denominator); otherwise the full D_src is the filter.
    if args.parsed_sources_path:
        ps = spark.read.parquet(args.parsed_sources_path)
        src_col = "registered_domain" if "registered_domain" in ps.columns else "domain_from"
        source_filter = ps.select(F.col(src_col).alias("registered_domain")).distinct()
    else:
        source_filter = d_src_all

    # Distribute WAT iteration: one task per file, flatMap to link rows. Return the
    # iterator directly (do NOT materialize a list — one WAT file's links can be large).
    # cc-index warc_filename (hence wat_path) is relative; prepend the CommonCrawl prefix
    # (with_wat_prefix leaves already-absolute paths untouched).
    wat_prefix = args.wat_prefix
    link_rows = spark.sparkContext.parallelize(wat_paths, max(len(wat_paths), 1)).flatMap(
        lambda path: iter_wat_links(with_wat_prefix(path, wat_prefix), anon=True)
    )
    # Tuples + explicit schema: toDF() schema inference fails on an empty RDD, which is a
    # real case for tiny/domain-targeted samples.
    links = spark.createDataFrame(
        link_rows.map(
            lambda d: (d["domain_from"], d["url_from"], d["url_to"], d["anchor"], d["rel"])
        ),
        _LINK_SCHEMA,
    )

    # Per-run output dir so successive modes (file-sample / domain-targeted / full) do not
    # overwrite each other.
    run_root = f"{args.output_root}/runs/{args.run_name}"

    # Materialize backlinks to Parquet once, then read it back for the metric actions —
    # cheaper and safer than caching the whole DataFrame across many actions (full mode can
    # be large enough to pressure executor memory/disk).
    write_parquet(
        backlink_edges(links, source_filter, target_domains), f"{run_root}/backlinks_sample"
    )
    backlinks = spark.read.parquet(f"{run_root}/backlinks_sample")

    found_edges = backlinks.select("domain_from", "domain_to").distinct()
    expected_pairs = expected.select("domain_from", "domain_to").distinct()
    wat_only = found_edges.subtract(expected_pairs)

    # Backlinks-based counts in a single pass (one .agg) instead of 5 separate actions.
    empty_anchor = F.col("anchor").isNull() | (F.length(F.trim(F.col("anchor"))) == 0)
    agg = backlinks.agg(
        F.count(F.lit(1)).alias("backlink_rows"),
        F.coalesce(F.sum(empty_anchor.cast("long")), F.lit(0)).alias("empty_anchor_rows"),
        F.coalesce(F.sum(F.col("is_nofollow").cast("long")), F.lit(0)).alias("nofollow_rows"),
        F.coalesce(F.sum(F.col("is_ugc").cast("long")), F.lit(0)).alias("ugc_rows"),
        F.coalesce(F.sum(F.col("is_sponsored").cast("long")), F.lit(0)).alias("sponsored_rows"),
    ).collect()[0]
    metrics: dict[str, object] = {
        "wat_files_parsed": len(wat_paths),
        "backlink_rows": agg["backlink_rows"],
        "found_edges": found_edges.count(),
        "wat_only_edges": wat_only.count(),
        "empty_anchor_rows": agg["empty_anchor_rows"],
        "nofollow_rows": agg["nofollow_rows"],
        "ugc_rows": agg["ugc_rows"],
        "sponsored_rows": agg["sponsored_rows"],
    }
    if args.parsed_sources_path:
        # source_filter == the parsed source set here; reuse it as the recall denominator
        # (valid only when every page of each counted source was parsed — full / domain-
        # targeted modes).
        metrics["recall"] = conditional_recall(found_edges, expected_pairs, source_filter)
    else:
        metrics["recall"] = "not computed (file-sample mode — page-coverage lower bound only)"

    write_json(spark, metrics, f"{run_root}/metrics")
    log.info(
        "exp5.parse_wat.done",
        run_name=args.run_name,
        **{k: v for k, v in metrics.items() if k != "recall"},
    )
    spark.stop()


if __name__ == "__main__":
    main()
