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


@dataclass
class PublishArtifacts:
    """Local files produced by an extract, plus the metadata dict."""

    parquet_path: Path
    csv_path: Path
    metadata_path: Path
    metadata: dict


def _sql_str(value: str) -> str:
    """Escape a string for safe inlining into a DuckDB SQL literal."""
    return value.replace("'", "''")


def _source_commit() -> str | None:
    try:
        out = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
        )
        return out.stdout.strip() or None
    except (subprocess.CalledProcessError, FileNotFoundError):
        return None


def extract_public_index(
    mart_path: str,
    output_dir: str | Path,
    top_n: int = 100_000,
    window_id: str | None = None,
) -> PublishArtifacts:
    """Extract the published subset for one crawl window from the mart.

    Writes three deterministic files into output_dir (overwriting only those
    files, not the directory): the published-columns Parquet (no rank), a top-N
    CSV with a presentation-only rank ordered by open_authority desc, domain
    asc, and a metadata.json. Raises PublishError if the window is ambiguous or
    top_n is not positive.
    """
    if top_n <= 0:
        raise PublishError(f"top_n must be positive, got {top_n}")
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    cols = ", ".join(_PUBLISHED_COLUMNS)
    con = duckdb.connect()
    try:
        rel = f"read_parquet('{_sql_str(str(mart_path))}')"
        source_row_count = con.sql(f"select count(*) from {rel}").fetchone()[0]
        available = [
            r[0]
            for r in con.sql(f"select distinct window_id from {rel} order by window_id").fetchall()
        ]
        selected = _resolve_window(window_id, available)
        where = f"where window_id = '{_sql_str(selected)}'"

        parquet_path = out / f"{_DATASET_NAME}.parquet"
        con.sql(f"select {cols} from {rel} {where}").write_parquet(str(parquet_path))

        csv_path = out / f"{_DATASET_NAME}-top-{top_n}.csv"
        con.sql(
            f"select row_number() over (order by open_authority desc, domain asc) as rank, "
            f"{cols} from {rel} {where} "
            f"order by open_authority desc, domain asc limit {top_n}"
        ).write_csv(str(csv_path), header=True)

        row_count = con.sql(f"select count(*) from {rel} {where}").fetchone()[0]
    finally:
        con.close()

    metadata = {
        "dataset": _DATASET_NAME,
        "window_id": selected,
        "available_window_ids": available,
        "row_count": row_count,
        "source_row_count": source_row_count,
        "top_n": top_n,
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "source_mart_path": str(mart_path),
        "source_commit": _source_commit(),
    }
    metadata_path = out / "metadata.json"
    metadata_path.write_text(json.dumps(metadata, indent=2) + "\n")
    return PublishArtifacts(parquet_path, csv_path, metadata_path, metadata)
