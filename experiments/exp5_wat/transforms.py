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
