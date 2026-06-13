"""Pure transforms for Exp 5 WAT extraction. No I/O.

WAT-JSON link extraction, rel-flag parsing, warc→wat path derivation, primary-language
attribution, URL→registered-domain, and the DataFrame transforms used by the three jobs.
"""

from __future__ import annotations

from typing import Any
from urllib.parse import urlsplit

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from spark_jobs.common.domains import registered_domain


def derive_wat_path(warc_filename: str | None) -> str | None:
    """Map a cc-index ``warc_filename`` to its sibling WAT path.

    CommonCrawl WAT files mirror WARC files: the ``/warc/`` path segment becomes
    ``/wat/`` and the ``.warc.gz`` suffix becomes ``.warc.wat.gz``. Returns ``None``
    for ``None`` input or any path not matching the expected WARC shape (so callers
    can count malformed paths before spending a Dataproc run).
    """
    if warc_filename is None:
        return None
    if "/warc/" not in warc_filename or not warc_filename.endswith(".warc.gz"):
        return None
    return warc_filename.replace("/warc/", "/wat/", 1)[: -len(".warc.gz")] + ".warc.wat.gz"


def with_wat_prefix(path: str, prefix: str) -> str:
    """Prepend ``prefix`` to a relative WAT path; leave already-absolute URIs unchanged.

    ``derive_wat_path`` returns paths relative to the CommonCrawl root, but operator-built
    sample/artifact lists may already carry a scheme. Any path containing ``"://"`` is
    treated as absolute and returned unchanged; otherwise ``prefix`` and ``path`` are joined
    with exactly one slash (so a ``--wat-prefix`` without a trailing slash still works).
    WAT reading supports ``local`` / ``s3://`` / ``s3a://`` (via s3fs); ``gs://`` for WAT is
    NOT supported (Exp 5 adds no gcsfs).
    """
    if "://" in path:
        return path
    return prefix.rstrip("/") + "/" + path.lstrip("/")


def parse_rel_flags(rel: str | None) -> dict[str, bool]:
    """Parse an HTML ``rel`` attribute into nofollow/ugc/sponsored booleans.

    ``rel`` is a space-separated, case-insensitive token list. Unknown tokens are
    ignored. Returns all-``False`` for ``None``/empty.
    """
    tokens = {t.lower() for t in (rel or "").split()}
    return {
        "is_nofollow": "nofollow" in tokens,
        "is_ugc": "ugc" in tokens,
        "is_sponsored": "sponsored" in tokens,
    }


def extract_links(payload: dict[str, Any]) -> list[dict[str, str | None]]:
    """Extract hyperlink (``A@/href``) entries from a parsed WAT record payload.

    Navigates the WAT JSON envelope to ``HTML-Metadata.Links`` and keeps only anchor
    hyperlinks with a ``url``. Returns ``{url, anchor, rel}`` dicts; tolerates any
    missing level of the structure (non-HTML / non-response records → empty list).
    """
    links = (
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


def primary_language(content_languages: str | None) -> str | None:
    """Primary language = first element of the comma-separated ``content_languages``.

    Matches Exp 1 / SPEC §5 Stage 1 primary-language attribution. ``None``/empty → ``None``.
    """
    if not content_languages:
        return None
    first = content_languages.split(",", 1)[0].strip().lower()
    return first or None


def registered_domain_of_url(url: str | None) -> str | None:
    """Registered domain of a URL's host via the pinned PSL helper.

    Uses ``spark_jobs.common.domains.registered_domain`` (offline ``tldextract``
    snapshot) so WAT URL parsing uses the pipeline's pinned PSL. Returns ``None`` for
    unparseable input or hosts the helper rejects (IPs, reserved suffixes, IDNA errors).
    """
    if not url:
        return None
    try:
        host = urlsplit(url).hostname
    except ValueError:
        return None
    return registered_domain(host)


def normalize_content_languages(df: DataFrame) -> DataFrame:
    """Return ``df`` with ``content_languages`` guaranteed to be a comma-separated string.

    CC has shipped ``content_languages`` both as ``string`` and as ``array<string>``;
    isolating the schema-drift handling here (pure DataFrame transform) lets it be tested
    off-cluster instead of only inside the expensive Job 1.
    """
    if dict(df.dtypes)["content_languages"].startswith("array"):
        return df.withColumn("content_languages", F.concat_ws(",", F.col("content_languages")))
    return df


def qualify_target_domains(
    projection: DataFrame, targets: list[str], min_share: float
) -> DataFrame:
    """Per-URL cc-index projection → target domains S with language counters.

    A page's language is its primary (first ``content_languages`` element). A domain is
    a **language hit** (returned) when ≥1 page's primary language is in ``targets``.
    Output columns: ``registered_domain``, ``total_200_pages``, ``target_language_pages``
    (each page counts once, by its primary), ``language_share``, ``meets_share``
    (``language_share >= min_share``). The caller treats the language-hit set as S and
    uses ``meets_share`` for the share-filtered sensitivity subset.
    """
    primary_udf = F.udf(primary_language, T.StringType())
    with_primary = projection.withColumn("primary", primary_udf(F.col("content_languages")))
    is_target = F.col("primary").isin(targets)
    agg = with_primary.groupBy("registered_domain").agg(
        F.count(F.lit(1)).alias("total_200_pages"),
        F.sum(is_target.cast("long")).alias("target_language_pages"),
    )
    return (
        agg.where(F.col("target_language_pages") > 0)
        .withColumn(
            "language_share",
            F.col("target_language_pages") / F.col("total_200_pages"),
        )
        .withColumn("meets_share", F.col("language_share") >= F.lit(min_share))
    )


def expected_edges(edges: DataFrame, v3_map: DataFrame, target_domains: DataFrame) -> DataFrame:
    """Webgraph edges into S, resolved to domain strings — recall denominator.

    ``edges`` is the id-based graph ``(from_id, to_id)``; ``v3_map`` is ``(id, domain)``;
    ``target_domains`` has ``registered_domain`` (S). **Filters in id space first** —
    resolve S→target ids, keep only edges whose ``to_id`` is a target id — *before*
    resolving the full 4.3B-edge graph to strings (avoids a double join of the whole
    graph against the 100M-row map). Only the reduced into-scope edge set is resolved.
    Output: ``(domain_from, domain_to)``.
    """
    s = target_domains.select("registered_domain").distinct()
    target_ids = (
        v3_map.join(F.broadcast(s), v3_map["domain"] == s["registered_domain"], "inner")
        .select(F.col("id").alias("to_id"))
        .distinct()
    )
    into_scope = edges.join(F.broadcast(target_ids), on="to_id", how="inner")
    dst = v3_map.select(F.col("id").alias("to_id"), F.col("domain").alias("domain_to"))
    src = v3_map.select(F.col("id").alias("from_id"), F.col("domain").alias("domain_from"))
    return (
        into_scope.join(dst, on="to_id", how="inner")
        .join(src, on="from_id", how="inner")
        .select("domain_from", "domain_to")
        .distinct()
    )


def backlink_edges(
    links: DataFrame, source_domains: DataFrame, target_domains: DataFrame
) -> DataFrame:
    """WAT link rows → candidate backlinks into S from D_src sources.

    ``links`` has ``domain_from`` (the source page's registered domain), ``url_from``,
    ``url_to``, ``anchor``, ``rel``. A WAT file holds many domains, so first restrict to
    ``domain_from ∈ D_src``; then derive ``domain_to`` from ``url_to`` via the pinned
    PSL helper and keep links whose ``domain_to ∈ S``. rel → nofollow/ugc/sponsored flags.
    Output carries ``url_from``/``url_to`` for the backlink sample (design data model).
    """
    reg_udf = F.udf(registered_domain_of_url, T.StringType())
    d_src = source_domains.select(F.col("registered_domain").alias("domain_from")).distinct()
    s = target_domains.select(F.col("registered_domain").alias("domain_to")).distinct()
    # Plain shuffle joins by default — S / D_src sizes are unknown until Job 2 runs. An
    # operator may wrap d_src / s in F.broadcast() once a Job 2 run confirms they are small.
    kept = (
        links.join(d_src, on="domain_from", how="inner")
        .withColumn("domain_to", reg_udf(F.col("url_to")))
        .join(s, on="domain_to", how="inner")
    )
    # Split on ANY run of whitespace (tabs / multiple spaces), matching parse_rel_flags'
    # Python str.split() — F.split(_, " ") would mis-tokenize "nofollow\tugc".
    rel_tokens = F.split(F.trim(F.lower(F.coalesce(F.col("rel"), F.lit("")))), r"\s+")
    return kept.select(
        "domain_from",
        "domain_to",
        "url_from",
        "url_to",
        "anchor",
        "rel",
        F.array_contains(rel_tokens, "nofollow").alias("is_nofollow"),
        F.array_contains(rel_tokens, "ugc").alias("is_ugc"),
        F.array_contains(rel_tokens, "sponsored").alias("is_sponsored"),
    )
