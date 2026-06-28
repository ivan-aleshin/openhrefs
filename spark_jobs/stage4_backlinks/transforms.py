"""Pure transforms for Stage 4 backlinks extraction. No I/O.

Global WAT link-pair extraction, domain resolution via host-dedup,
and domain-grain aggregation for ``cc_domain_link_pairs`` (SPEC §5 Stage 4).
All functions are pure ``DataFrame → DataFrame`` or pure-Python; no
``SparkSession.read`` calls, no file or network I/O.
"""

from __future__ import annotations

import hashlib
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from spark_jobs.common.domains import registered_domain
from spark_jobs.common.errors import DataSourceError

RAW_LINK_SCHEMA = T.StructType(
    [
        T.StructField("url_from", T.StringType()),
        T.StructField("url_to", T.StringType()),
        T.StructField("anchor", T.StringType()),
        T.StructField("rel", T.StringType()),
    ]
)

# Output schema for aggregate_domain_pairs / cc_domain_link_pairs.
# ``crawl`` is added by main.py — not present here.
CC_DOMAIN_LINK_PAIRS = T.StructType(
    [
        T.StructField("domain_from", T.StringType()),
        T.StructField("domain_to", T.StringType()),
        T.StructField("link_count", T.LongType()),
        T.StructField("dofollow_count", T.LongType()),
        T.StructField("ugc_count", T.LongType()),
        T.StructField("sponsored_count", T.LongType()),
    ]
)


def manifest_sha256(text: str) -> str:
    """Canonical SHA-256 of a WAT manifest text.

    Lines are stripped and blanks dropped before hashing; order is preserved
    so different path orderings produce different digests. A trailing newline
    is appended, making the hash stable whether or not the input ends with a
    blank line.
    """
    lines = [line.strip() for line in text.splitlines()]
    non_empty = [line for line in lines if line]
    normalized = "\n".join(non_empty) + "\n"
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def resolve_wat_path(entry: str, prefix: str) -> str:
    """Resolve a WAT manifest entry to an absolute URI.

    Blank entries are rejected. Already-absolute URIs (containing ``"://"``)
    are returned unchanged. Relative entries are joined to ``prefix`` with
    exactly one slash. Never uses ``pathlib.Path`` (which mangles ``gs://``).

    Args:
        entry: A single WAT path entry, absolute or relative.
        prefix: Bucket/root prefix for relative entries (e.g. ``gs://b/root``).

    Raises:
        DataSourceError: if ``entry`` is empty or whitespace-only.
    """
    stripped = entry.strip()
    if not stripped:
        raise DataSourceError(f"WAT manifest entry is empty: {entry!r}")
    if "://" in stripped:
        return stripped
    return prefix.rstrip("/") + "/" + stripped.lstrip("/")


def default_metrics_path(output_path: str) -> str:
    """Derive a sibling ``_metrics/<dataset>`` path from an output dataset path.

    For ``gs://bucket/raw/ds`` returns ``gs://bucket/raw/_metrics/ds``.
    Trailing slashes are stripped before parsing. Scheme roots such as
    ``gs://bucket`` or ``s3://bucket`` (no dataset leaf) are rejected.

    Args:
        output_path: Absolute output path for a dataset (``gs://``/``s3://``
            or local absolute path).

    Raises:
        DataSourceError: if ``output_path`` has no dataset-level leaf component.
    """
    p = output_path.rstrip("/")
    if "://" in p:
        after_scheme = p.split("://", 1)[1]
        if "/" not in after_scheme:
            raise DataSourceError(f"output_path has no dataset leaf: {output_path!r}")
    slash_idx = p.rfind("/")
    if slash_idx == -1:
        raise DataSourceError(f"output_path has no dataset leaf: {output_path!r}")
    parent = p[:slash_idx]
    leaf = p[slash_idx + 1 :]
    if not leaf:
        raise DataSourceError(f"output_path has no dataset leaf: {output_path!r}")
    return f"{parent}/_metrics/{leaf}"


def extract_links(payload: dict[str, Any]) -> list[dict[str, str | None]]:
    """Extract anchor (``A@/href``) hyperlinks from a parsed WAT record payload.

    Navigates ``Envelope.Payload-Metadata.HTTP-Response-Metadata.HTML-Metadata.Links``
    defensively; any missing level returns ``[]``. Only ``A@/href`` links with a
    truthy ``url`` field are included.

    Args:
        payload: Parsed WAT JSON record (top-level envelope dict).

    Returns:
        List of ``{url, anchor, rel}`` dicts. ``anchor`` and ``rel`` may be ``None``.
    """
    links: list[Any] = (
        payload.get("Envelope", {})
        .get("Payload-Metadata", {})
        .get("HTTP-Response-Metadata", {})
        .get("HTML-Metadata", {})
        .get("Links", [])
    )
    out: list[dict[str, str | None]] = []
    for link in links:
        if link.get("path") != "A@/href":
            continue
        url = link.get("url")
        if not url:
            continue
        out.append({"url": url, "anchor": link.get("text"), "rel": link.get("rel")})
    return out


def links_from_payload(payload: dict[str, Any]) -> list[dict[str, str | None]]:
    """Raw anchor-link rows from a parsed WAT payload — no domain extraction.

    ``url_from`` is the record's ``WARC-Target-URI`` (may be ``None``). Domain
    resolution happens downstream via Spark host-dedup; keeping the hot loop
    free of PSL calls is the cost lever. Emits ``{url_from, url_to, anchor, rel}``
    for every ``A@/href`` link in the payload.

    Args:
        payload: Parsed WAT JSON record.

    Returns:
        List of raw link-row dicts. ``url_from`` may be ``None`` when
        ``WARC-Target-URI`` is absent.
    """
    url_from: str | None = (
        payload.get("Envelope", {}).get("WARC-Header-Metadata", {}).get("WARC-Target-URI")
    )
    return [
        {
            "url_from": url_from,
            "url_to": e["url"],
            "anchor": e["anchor"],
            "rel": e["rel"],
        }
        for e in extract_links(payload)
    ]


def resolve_domain_pairs(df: DataFrame) -> DataFrame:
    """Resolve ``(url_from, url_to)`` URL columns to ``(domain_from, domain_to)``.

    INVARIANT: the null-host early-drop controls OUTPUT ELIGIBILITY — rows
    where either URL cannot be parsed to a host are discarded before domain
    resolution. Parse counters for observability are captured upstream in
    ``io.py``, not here. The output contains only rows where BOTH sides
    resolve to a registered domain. Self-links are preserved.

    INVARIANT: domain resolution runs only on the deduplicated host set, never
    on raw URL rows (the host-dedup cost lever). The distinct-host UDF may be
    evaluated once per join branch (≈2× distinct-host scale) — far below
    raw-row scale, and an accepted tradeoff.

    Steps:
        1. Extract ``host_from``/``host_to`` via ``parse_url``.
        2. Early-drop rows where either host is null.
        3. Resolve distinct hosts to registered domains (UDF on deduplicated
           host set only, never on raw URL rows).
        4. Join resolved domains back to rows.
        5. Drop rows where either domain is null.

    Args:
        df: DataFrame with at least ``url_from``, ``url_to``, ``rel`` columns.

    Returns:
        DataFrame with original columns plus ``domain_from``, ``domain_to``;
        ``host_from`` and ``host_to`` are dropped.
    """
    # registered_domain is in fact deterministic; .asNondeterministic() is a Catalyst
    # optimizer barrier so the UDF is NOT pushed below .distinct() onto raw URL rows —
    # it must run only on the deduplicated host set (the host-dedup cost lever).
    reg_udf = F.udf(registered_domain, T.StringType()).asNondeterministic()
    with_hosts = df.withColumn("host_from", F.expr("parse_url(url_from, 'HOST')")).withColumn(
        "host_to", F.expr("parse_url(url_to, 'HOST')")
    )
    filtered = with_hosts.where(F.col("host_from").isNotNull() & F.col("host_to").isNotNull())
    distinct_hosts = (
        filtered.select(F.col("host_from").alias("host"))
        .union(filtered.select(F.col("host_to").alias("host")))
        .distinct()
        .withColumn("domain", reg_udf(F.col("host")))
    )
    from_map = distinct_hosts.select(
        F.col("host").alias("host_from"), F.col("domain").alias("domain_from")
    )
    to_map = distinct_hosts.select(
        F.col("host").alias("host_to"), F.col("domain").alias("domain_to")
    )
    resolved = (
        filtered.join(from_map, on="host_from", how="left")
        .join(to_map, on="host_to", how="left")
        .drop("host_from", "host_to")
    )
    return resolved.where(F.col("domain_from").isNotNull() & F.col("domain_to").isNotNull())


def aggregate_domain_pairs(df: DataFrame) -> DataFrame:
    """Aggregate ``(domain_from, domain_to, rel)`` rows to the domain-grain.

    ``rel`` is lowercased and split on any whitespace run so mixed-case and
    tab-separated tokens (e.g. ``"NOFOLLOW"`` / ``"nofollow\\tugc"``) are
    handled correctly. ``dofollow = NOT nofollow``. Self-links are kept.
    Output schema equals ``CC_DOMAIN_LINK_PAIRS``.

    Args:
        df: DataFrame with at least ``domain_from``, ``domain_to``, ``rel``.

    Returns:
        Aggregated DataFrame with columns matching ``CC_DOMAIN_LINK_PAIRS``
        (excluding ``crawl``, which is added by main).
    """
    rel_tokens = F.split(F.trim(F.lower(F.coalesce(F.col("rel"), F.lit("")))), r"\s+")
    is_nofollow = F.array_contains(rel_tokens, "nofollow")
    is_ugc = F.array_contains(rel_tokens, "ugc")
    is_sponsored = F.array_contains(rel_tokens, "sponsored")
    return df.groupBy("domain_from", "domain_to").agg(
        F.count(F.lit(1)).alias("link_count"),
        F.sum((~is_nofollow).cast("long")).alias("dofollow_count"),
        F.sum(is_ugc.cast("long")).alias("ugc_count"),
        F.sum(is_sponsored.cast("long")).alias("sponsored_count"),
    )
