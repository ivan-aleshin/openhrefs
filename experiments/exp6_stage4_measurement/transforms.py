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
