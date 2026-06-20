"""Pure transforms for Exp 6 measurement. No I/O."""

from __future__ import annotations

import random
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from experiments.exp5_wat.transforms import extract_links
from spark_jobs.common.domains import registered_domain


def sample_wat_paths(all_paths: list[str], n: int, seed: int) -> list[str]:
    """Deterministic uniform-random sample of ``n`` WAT paths.

    Returns a sorted list so the slice is reproducible and the exact same file set
    can be reused across clouds. If ``n >= len(all_paths)`` returns all paths sorted.
    """
    if n >= len(all_paths):
        return sorted(all_paths)
    rng = random.Random(seed)
    return sorted(rng.sample(all_paths, n))


def nested_ladder_samples(
    all_paths: list[str], sizes: list[int], seed: int
) -> dict[int, list[str]]:
    """Nested uniform-random ladder: one draw of ``max(sizes)``; smaller sizes are prefixes.

    Draw a single uniform-random subset of size ``max(sizes)`` with ``seed`` (random order),
    then take prefixes so each smaller slice is a subset of every larger one. Nesting cuts slope
    noise across ladder points and lets the smaller runs reuse the staged objects of the largest
    (stage the 3000 set once). Each returned slice is sorted for reproducibility; sizes above
    ``len(all_paths)`` are clamped to all paths. Empty ``sizes`` → ``{}``.
    """
    if not sizes:
        return {}
    max_size = min(max(sizes), len(all_paths))
    rng = random.Random(seed)
    base = rng.sample(all_paths, max_size)
    return {size: sorted(base[: min(size, len(all_paths))]) for size in sizes}


def links_from_payload(payload: dict[str, Any]) -> list[dict[str, str | None]]:
    """Raw anchor-link rows from a parsed WAT payload — NO domain extraction.

    Unlike Exp 5's iterator, ``domain_from`` is NOT computed here: keeping the
    hot loop free of ``tldextract`` is the point (domain resolution happens in
    Spark via host-dedup). Emits ``{url_from, url_to, anchor, rel}``; ``url_from``
    is the record's ``WARC-Target-URI`` (may be ``None`` — counted downstream).
    """
    target_uri = payload.get("Envelope", {}).get("WARC-Header-Metadata", {}).get("WARC-Target-URI")
    return [
        {
            "url_from": target_uri,
            "url_to": link["url"],
            "anchor": link["anchor"],
            "rel": link["rel"],
        }
        for link in extract_links(payload)
    ]


def resolve_domains(df: DataFrame, url_col: str, out_col: str) -> DataFrame:
    """Resolve ``url_col`` to a registered domain ``out_col`` via host-dedup.

    The cost lever from Exp 5: extract the host natively (``parse_url(_, 'HOST')``),
    run the ``tldextract``-backed ``registered_domain`` UDF on the DISTINCT host set
    only, then join back. ~189x fewer UDF calls than per-row extraction. The pinned
    PSL (``spark_jobs.common.domains.registered_domain``) keeps normalization aligned
    with the rest of the pipeline.
    """
    reg_udf = F.udf(registered_domain, T.StringType())
    with_host = df.withColumn("_host", F.expr(f"parse_url({url_col}, 'HOST')"))
    resolved_hosts = (
        with_host.select("_host").distinct().withColumn(out_col, reg_udf(F.col("_host")))
    )
    return with_host.join(resolved_hosts, on="_host", how="left").drop("_host")


def aggregate_domain_pairs(df: DataFrame) -> DataFrame:
    """Aggregate ``(domain_from, domain_to, rel)`` rows to the global domain-grain.

    Output: ``(domain_from, domain_to, link_count, dofollow_count, ugc_count,
    sponsored_count)``. ``dofollow = NOT nofollow`` (public contract uses
    ``is_dofollow``). rel is split on any whitespace run to match Exp 5's
    ``parse_rel_flags`` tokenization.
    """
    rel_tokens = F.split(F.trim(F.lower(F.coalesce(F.col("rel"), F.lit("")))), r"\s+")
    flagged = (
        df.withColumn("is_nofollow", F.array_contains(rel_tokens, "nofollow"))
        .withColumn("is_ugc", F.array_contains(rel_tokens, "ugc"))
        .withColumn("is_sponsored", F.array_contains(rel_tokens, "sponsored"))
    )
    return flagged.groupBy("domain_from", "domain_to").agg(
        F.count(F.lit(1)).alias("link_count"),
        F.sum((~F.col("is_nofollow")).cast("long")).alias("dofollow_count"),
        F.sum(F.col("is_ugc").cast("long")).alias("ugc_count"),
        F.sum(F.col("is_sponsored").cast("long")).alias("sponsored_count"),
    )


def scoped_counts(pairs: DataFrame, scope_domains: DataFrame) -> dict[str, int]:
    """Global vs scoped pair/link counts for the global-vs-scoped ratio.

    ``scope_domains`` has a ``registered_domain`` column (the target set S). Returns
    a small dict (two aggregate rows collected — cheap, not a large dataset).
    """
    s = scope_domains.select(F.col("registered_domain").alias("domain_to")).distinct()
    g = pairs.agg(
        F.count(F.lit(1)).alias("global_pairs"),
        F.sum("link_count").alias("global_links"),
    ).first()
    scoped = (
        pairs.join(F.broadcast(s), on="domain_to", how="inner")
        .agg(
            F.count(F.lit(1)).alias("scoped_pairs"),
            F.sum("link_count").alias("scoped_links"),
        )
        .first()
    )
    return {
        "global_pairs": int(g["global_pairs"]),
        "global_links": int(g["global_links"] or 0),
        "scoped_pairs": int(scoped["scoped_pairs"]),
        "scoped_links": int(scoped["scoped_links"] or 0),
    }


def null_domain_rates(links: DataFrame) -> dict[str, int]:
    """Null-domain quality counters on resolved link rows."""
    row = links.agg(
        F.count(F.lit(1)).alias("total_rows"),
        F.sum(F.col("domain_from").isNull().cast("long")).alias("domain_from_null"),
        F.sum(F.col("domain_to").isNull().cast("long")).alias("domain_to_null"),
    ).first()
    return {
        "total_rows": int(row["total_rows"]),
        "domain_from_null": int(row["domain_from_null"] or 0),
        "domain_to_null": int(row["domain_to_null"] or 0),
    }


def top_domain_skew(pairs: DataFrame, n: int) -> dict[str, list[dict[str, Any]]]:
    """Top-``n`` skew by link_count for domain_from, domain_to, and domain_pair."""
    by_from = (
        pairs.groupBy("domain_from")
        .agg(F.sum("link_count").alias("link_count"))
        .orderBy(F.desc("link_count"))
        .limit(n)
    )
    by_to = (
        pairs.groupBy("domain_to")
        .agg(F.sum("link_count").alias("link_count"))
        .orderBy(F.desc("link_count"))
        .limit(n)
    )
    by_pair = pairs.orderBy(F.desc("link_count")).limit(n)
    return {
        "top_domain_from": [r.asDict() for r in by_from.collect()],
        "top_domain_to": [r.asDict() for r in by_to.collect()],
        "top_domain_pair": [r.asDict() for r in by_pair.collect()],
    }


def host_parse_fail_rates(links: DataFrame) -> dict[str, int]:
    """Host-parse failures BEFORE PSL: a non-null URL whose ``parse_url(HOST)`` is null.

    Separates "URL unparseable" (counted here) from "host parsed but PSL rejected it" (which
    surfaces as a domain null in :func:`null_domain_rates` after registered-domain resolution).
    A ``None`` URL is not a failure and is not counted.
    """
    row = links.agg(
        F.sum(
            (F.expr("parse_url(url_from, 'HOST')").isNull() & F.col("url_from").isNotNull()).cast(
                "long"
            )
        ).alias("url_from_parse_fail"),
        F.sum(
            (F.expr("parse_url(url_to, 'HOST')").isNull() & F.col("url_to").isNotNull()).cast(
                "long"
            )
        ).alias("url_to_parse_fail"),
    ).first()
    return {
        "url_from_parse_fail": int(row["url_from_parse_fail"] or 0),
        "url_to_parse_fail": int(row["url_to_parse_fail"] or 0),
    }
