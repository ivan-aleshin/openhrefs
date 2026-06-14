"""Exp 5 Job 2 — cheap derivations from the staged cc-index projection + webgraph.

Reads the projection (Job 1 output) and the staged id-based domain graph
(``v3_edges`` + ``v3_map``), then writes: target_domains (S + language counters),
expected_edges (recall denominator), source_wat_files (source domain → WAT path), and
the distinct segment_list, plus selectivity stats. No cc-index read — cheap, re-runnable.

Submit (reads only gs:// — no s3a props needed; DATAPROC_EXTRA_PKGS still ships experiments/):
    DATAPROC_IMAGE=<tag> \
    DATAPROC_EXTRA_PKGS="experiments/__init__.py experiments/exp5_wat" \
    ./infra/gcp/submit_job.sh \
        experiments/exp5_wat/derive_segments.py \
        --projection-path gs://openhrefs-data/raw/exp5/cdx_projection \
        --edges-path gs://openhrefs-data/raw/webgraph/cc-main-2026-mar-apr-may-domain/v3_edges \
        --v3-map-path gs://openhrefs-data/raw/webgraph/cc-main-2026-mar-apr-may-domain/v3_map \
        --output-root gs://openhrefs-data/raw/exp5 \
        --targets bul,ron --min-share 0.25 --crawl-wat-total 90000 --avg-wat-bytes 1000000000
"""

from __future__ import annotations

import argparse

import structlog
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from experiments.exp5_wat.io import (
    read_v3_edges,
    read_v3_map,
    write_json,
    write_parquet,
)
from experiments.exp5_wat.transforms import (
    derive_wat_path,
    expected_edges,
    qualify_target_domains,
    source_wat_files,
)

log = structlog.get_logger()


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Exp 5 segment derivation")
    p.add_argument("--projection-path", required=True)
    p.add_argument("--edges-path", required=True, help="Staged v3_edges (from_id, to_id).")
    p.add_argument("--v3-map-path", required=True, help="Staged v3_map (id, domain).")
    p.add_argument("--output-root", required=True)
    p.add_argument("--targets", default="bul,ron")
    p.add_argument("--min-share", type=float, default=0.25)
    p.add_argument("--crawl-wat-total", type=int, default=90000)
    p.add_argument("--avg-wat-bytes", type=int, default=1_000_000_000, help="Est. bytes/WAT file.")
    return p.parse_args()


def main() -> None:
    args = _parse_args()
    targets = [t.strip() for t in args.targets.split(",") if t.strip()]
    spark = SparkSession.builder.appName("exp5-derive-segments").getOrCreate()

    projection = spark.read.parquet(args.projection_path)
    edges = read_v3_edges(spark, args.edges_path)
    vmap = read_v3_map(spark, args.v3_map_path)

    s = qualify_target_domains(projection, targets, args.min_share).cache()
    exp_edges = expected_edges(edges, vmap, s).cache()
    d_src = exp_edges.select(F.col("domain_from").alias("registered_domain")).distinct()
    src_files = source_wat_files(projection, d_src).cache()
    segments = src_files.select("wat_path").distinct().cache()

    write_parquet(s, f"{args.output_root}/target_domains")
    write_parquet(exp_edges, f"{args.output_root}/expected_edges")
    write_parquet(src_files, f"{args.output_root}/source_wat_files")
    write_parquet(segments, f"{args.output_root}/segment_list")

    # Malformed/null WAT paths: warc_filenames for D_src whose derivation returned null.
    wat_udf = F.udf(derive_wat_path)
    malformed_wat_paths = (
        projection.join(d_src, on="registered_domain", how="inner")
        .select("warc_filename")
        .distinct()
        .where(wat_udf(F.col("warc_filename")).isNull())
        .count()
    )

    wat_files = segments.count()
    stats = {
        "target_domains": s.count(),
        "target_domains_meeting_share": s.where(F.col("meets_share")).count(),
        "source_domains": d_src.count(),
        "expected_edges": exp_edges.count(),
        "wat_files": wat_files,
        "estimated_bytes": wat_files * args.avg_wat_bytes,
        "malformed_wat_paths": malformed_wat_paths,
        "crawl_wat_total": args.crawl_wat_total,
        "crawl_fraction": wat_files / args.crawl_wat_total if args.crawl_wat_total else None,
    }
    write_json(spark, stats, f"{args.output_root}/segment_list_stats")
    log.info("exp5.derive_segments.done", **stats)
    spark.stop()


if __name__ == "__main__":
    main()
