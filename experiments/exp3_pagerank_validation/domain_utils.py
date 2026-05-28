"""Host/domain normalization via PSL — vendored from composite-domain-rating.

Source: ivan-aleshin/composite-domain-rating scripts/domain_utils.py. Kept as a
copy so the experiment is self-contained; the production version belongs in
spark_jobs/common/ for Stage 1. Reproducibility depends on a pinned `tldextract`
version (the offline PSL snapshot ships with the package) — pin it in uv.lock.

CommonCrawl host-graph vertices are *reversed* hosts (`com.example.www`); call
`unreverse_host` before normalizing.
"""

from __future__ import annotations

import ipaddress
import re
from functools import lru_cache

import idna
import tldextract

CONTROL_CHARS = re.compile(r"[\x00-\x1f\x7f]")
RESERVED_SUFFIXES = {"local", "localhost", "internal", "test"}


@lru_cache(maxsize=1)
def _extractor() -> tldextract.TLDExtract:
    """Reproducible PSL extractor that does not fetch live updates."""
    return tldextract.TLDExtract(suffix_list_urls=None, fallback_to_snapshot=True)


def unreverse_host(reversed_host: str) -> str:
    """`com.example.www` -> `www.example.com`."""
    return ".".join(reversed(reversed_host.split(".")))


def registered_domain(raw_domain: str | None) -> str | None:
    """Normalize a hostname to its ASCII registered domain, or None if invalid."""
    if raw_domain is None:
        return None
    domain = raw_domain.strip().strip(".").lower()
    if not domain or len(domain) > 253 or CONTROL_CHARS.search(domain):
        return None
    try:
        ipaddress.ip_address(domain)
        return None
    except ValueError:
        pass
    try:
        ascii_domain = idna.encode(domain, uts46=True).decode("ascii")
    except idna.IDNAError:
        return None
    extracted = _extractor()(ascii_domain)
    suffix = extracted.suffix.lower()
    if not extracted.domain or not suffix or suffix in RESERVED_SUFFIXES:
        return None
    reg = extracted.registered_domain
    if not reg or len(reg) > 253:
        return None
    return reg


def is_apex(raw_domain: str | None) -> bool:
    """True when the host equals its registered domain (no subdomain)."""
    reg = registered_domain(raw_domain)
    return reg is not None and raw_domain is not None and raw_domain.strip(".").lower() == reg
