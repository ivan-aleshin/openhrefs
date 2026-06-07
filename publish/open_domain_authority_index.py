"""Extract and publish the open-domain-authority-index snapshot.

Reads `mart_domain_authority` Parquet, extracts the published-columns subset
for one crawl window, and (opt-in) pushes it to a Hugging Face dataset and a
GitHub Release. Default behavior is a local extract only; uploads happen only
with explicit --push-hf / --push-github flags. The dataset is a best-effort
snapshot, not a maintained service.
"""

from __future__ import annotations

import argparse
import json
import subprocess
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import structlog

_DATASET_NAME = "open-domain-authority-index"
_PUBLISHED_COLUMNS = ["domain", "open_authority", "open_volume", "window_id"]
_REPO_URL = "https://github.com/ivan-aleshin/openhrefs"

log = structlog.get_logger()


class PublishError(Exception):
    """A publish step could not complete."""


def _resolve_window(window_id: str | None, available: list[str]) -> str:
    """Select exactly one crawl window, never guessing.

    With an explicit window_id, require it to exist. Without one, use the sole
    window if there is exactly one; otherwise fail loudly.
    """
    if window_id is not None:
        if window_id not in available:
            raise PublishError(f"window_id {window_id!r} not in mart; available: {available}")
        return window_id
    if len(available) == 1:
        return available[0]
    raise PublishError(
        f"mart has {len(available)} windows {available}; pass --window-id to select one"
    )
