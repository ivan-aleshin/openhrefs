"""Build the transfer manifest to stage a sampled WAT slice S3 -> GCS via Storage Transfer.

The slice is deliberately small relative to a full crawl; staging is transient
(STANDARD class, <=48h lifecycle per the spec staging guard). This module only builds the
STS S3-source transfer manifest — the actual STS job is created by the operator (see README).
"""

from __future__ import annotations

import csv
import io


def build_sts_manifest(wat_paths: list[str]) -> str:
    """Build a Storage Transfer Service S3-source manifest for the WAT slice.

    CSV, no header, one column: the object name **relative to the source bucket**, written raw
    (exactly as in ``wat.paths.gz``). Per Google's STS manifest format the object name is NOT
    URL-encoded — URL-encoding is only for the separate HTTP/HTTPS URL-list format. The csv
    writer adds quoting only if a key contains a comma/quote/newline (CC WAT keys don't). NOT the
    ``TsvHttpData-1.0`` URL-list format (that needs per-file size + MD5, absent from the paths
    manifest). Ref: https://docs.cloud.google.com/storage-transfer/docs/manifest
    """
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n")
    for key in wat_paths:
        writer.writerow([key])
    return buf.getvalue()
