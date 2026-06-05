"""PSL host/domain normalization.

Reduces a hostname to its ASCII registered domain via the Public Suffix List
(offline ``tldextract`` snapshot, pinned in ``uv.lock`` for reproducibility).
Used wherever an external host-keyed source must be collapsed to the same
registered-domain key the pipeline graph uses (e.g. the OpenPageRank reference
in Stage 2 validation).
"""

from __future__ import annotations

import ipaddress
import re
from functools import lru_cache

import idna
import tldextract

_CONTROL_CHARS = re.compile(r"[\x00-\x1f\x7f]")
_RESERVED_SUFFIXES = {"local", "localhost", "internal", "test"}


@lru_cache(maxsize=1)
def _extractor() -> tldextract.TLDExtract:
    """Reproducible PSL extractor — offline snapshot, writable cache dir.

    ``cache_dir`` under ``/tmp`` avoids failures on read-only home dirs
    (Spark executors, CI).
    """
    # suffix_list_urls=None disables the online PSL fetch (offline snapshot only);
    # valid at runtime but the stub types it as Sequence[str].
    return tldextract.TLDExtract(
        suffix_list_urls=None,  # type: ignore[arg-type]
        fallback_to_snapshot=True,
        cache_dir="/tmp/tldextract",
    )


def registered_domain(raw_domain: str | None) -> str | None:
    """Normalize a hostname to its ASCII registered domain, or ``None`` if invalid.

    Rejects empty/oversized input, control characters, bare IP addresses, IDNA
    failures, and reserved suffixes (``localhost``/``local``/``internal``/``test``).
    """
    if raw_domain is None:
        return None
    domain = raw_domain.strip().strip(".").lower()
    if not domain or len(domain) > 253 or _CONTROL_CHARS.search(domain):
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
    if not extracted.domain or not suffix or suffix in _RESERVED_SUFFIXES:
        return None
    reg = extracted.top_domain_under_public_suffix
    if not reg or len(reg) > 253:
        return None
    return reg
