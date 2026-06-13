"""Pure transforms for Exp 5 WAT extraction. No I/O.

WAT-JSON link extraction, rel-flag parsing, warc→wat path derivation, primary-language
attribution, URL→registered-domain, and the DataFrame transforms used by the three jobs.
"""

from __future__ import annotations

from typing import Any


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
