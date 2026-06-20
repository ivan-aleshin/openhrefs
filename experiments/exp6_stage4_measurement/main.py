"""Exp 6 single extraction run: WAT slice -> global domain-grain aggregate + metrics.

Reads a newline-delimited file of WAT paths (one slice of the ladder), parses links
globally, resolves both domains via host-dedup, aggregates to domain-grain, computes
scoped/quality/skew metrics, writes the domain-grain Parquet + a sampled URL-grain
Parquet + a metrics JSON. The same entrypoint serves the gated full pass (S4-0b) by
pointing ``--wat-list`` at the full manifest.

Scope is single per run (``--scope-path`` = the target set S). The spec wants two scoped
ratios (the bul/ron proxy and one synthetic narrow/high-authority scope, if cheap); produce the
second by re-running with a different ``--scope-path`` and a distinct ``--output-root`` so the
two scoped metric JSONs are labelled by output path. Multi-scope in one pass is deliberately not
built (YAGNI for a measurement run).

Cost facts (wall-clock, DCU-hr, $, STS transfer) are recorded by the operator from the
Dataproc/billing console into the write-up; they are not emitted here.
"""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from experiments.exp6_stage4_measurement.io import (
    iter_wat_links_raw,
    write_metrics_json,
    write_parquet,
)
from experiments.exp6_stage4_measurement.transforms import (
    aggregate_domain_pairs,
    host_parse_fail_rates,
    null_domain_rates,
    resolve_domains,
    scoped_counts,
    top_domain_skew,
)

_LINK_SCHEMA = T.StructType(
    [
        T.StructField("url_from", T.StringType()),
        T.StructField("url_to", T.StringType()),
        T.StructField("anchor", T.StringType()),
        T.StructField("rel", T.StringType()),
    ]
)


def _read_lines(spark: SparkSession, path: str) -> list[str]:
    return [r["value"] for r in spark.read.text(path).collect() if r["value"].strip()]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--wat-list", required=True, help="file of WAT paths (one per line)")
    parser.add_argument("--wat-prefix", required=True, help="scheme+root, e.g. gs://bucket/staged")
    parser.add_argument(
        "--scope-path", required=True, help="parquet with registered_domain col (S)"
    )
    parser.add_argument("--output-root", required=True, help="root for parquet + metrics outputs")
    parser.add_argument("--url-sample-fraction", type=float, default=0.001)
    parser.add_argument("--shuffle-partitions", type=int, default=400)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("exp6-stage4-measurement").getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", str(args.shuffle_partitions))

    wat_paths = _read_lines(spark, args.wat_list)
    prefix = args.wat_prefix.rstrip("/")
    full_paths = [p if "://" in p else f"{prefix}/{p.lstrip('/')}" for p in wat_paths]

    sc = spark.sparkContext
    files_attempted = sc.accumulator(0)
    files_unreadable = sc.accumulator(0)
    records_seen = sc.accumulator(0)
    malformed_records = sc.accumulator(0)
    records_with_no_links = sc.accumulator(0)

    def extract(path: str):
        # Accumulators give experiment-grade parse/error rates. They can over-count on
        # Spark task retries/speculation; acceptable for a measurement run, noted in the
        # write-up. Caching `resolved` keeps the flatMap to a single execution.
        files_attempted.add(1)
        try:
            yield from iter_wat_links_raw(
                path,
                on_record=lambda: records_seen.add(1),
                on_malformed=lambda: malformed_records.add(1),
                on_no_links=lambda: records_with_no_links.add(1),
            )
        except Exception:  # unreadable WAT (network / gzip / truncation): count and skip
            files_unreadable.add(1)

    paths_rdd = sc.parallelize(full_paths, len(full_paths))
    links = spark.createDataFrame(paths_rdd.flatMap(extract), schema=_LINK_SCHEMA)

    resolved = resolve_domains(
        resolve_domains(links, "url_from", "domain_from"), "url_to", "domain_to"
    )
    resolved = resolved.cache()

    pairs = aggregate_domain_pairs(
        resolved.select("domain_from", "domain_to", "rel").where(
            F.col("domain_from").isNotNull() & F.col("domain_to").isNotNull()
        )
    )
    pairs = pairs.cache()

    scope = spark.read.parquet(args.scope_path).select("registered_domain")

    metrics = {
        "n_wat_files": len(full_paths),
        "raw_link_rows": resolved.count(),  # triggers the flatMap -> populates accumulators
        "global_domain_pairs": pairs.count(),
        "distinct_url_from_hosts": resolved.select(F.expr("parse_url(url_from, 'HOST')"))
        .distinct()
        .count(),
        "distinct_url_to_hosts": resolved.select(F.expr("parse_url(url_to, 'HOST')"))
        .distinct()
        .count(),
        "distinct_domain_from": pairs.select("domain_from").distinct().count(),
        "distinct_domain_to": pairs.select("domain_to").distinct().count(),
        "distinct_registered_domains": (
            pairs.select("domain_from").union(pairs.select("domain_to")).distinct().count()
        ),
        **host_parse_fail_rates(resolved),
        **null_domain_rates(resolved),
        **scoped_counts(pairs, scope),
        **top_domain_skew(pairs, n=20),
        # read AFTER the actions above so the flatMap has executed:
        "files_attempted": files_attempted.value,
        "files_unreadable": files_unreadable.value,
        "records_seen": records_seen.value,
        "malformed_records": malformed_records.value,
        "records_with_no_links": records_with_no_links.value,
    }

    write_parquet(pairs, f"{args.output_root}/domain_grain")
    url_sample = resolved.sample(False, args.url_sample_fraction, seed=42)
    write_parquet(url_sample, f"{args.output_root}/url_grain_sample")
    write_metrics_json(spark, metrics, f"{args.output_root}/metrics")

    resolved.unpersist()
    pairs.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
