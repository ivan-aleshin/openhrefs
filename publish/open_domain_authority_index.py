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


def build_dataset_card(metadata: dict, repo_id: str) -> str:
    """Hugging Face dataset card (README.md) with license and source terms."""
    return f"""---
license: other
tags:
- domain-authority
- common-crawl
- pagerank
---

# open-domain-authority-index

Domain-level authority metrics over the **global** Common Crawl link graph,
produced by the [openhrefs]({_REPO_URL}) pipeline. Columns: `domain`,
`open_authority`, `open_volume`, `window_id`.

**Best-effort snapshot, not a maintained service.** This is an on-demand
byproduct of the openhrefs pipeline, provided **as-is**. Run the pipeline
yourself for current or custom data.

## Source terms

Derived from [Common Crawl](https://commoncrawl.org/) and
[composite-domain-rating](https://github.com/ivan-aleshin/composite-domain-rating).
Source terms apply; you are responsible for compliance with the upstream data
licenses. Published under `license: other` for that reason.

## Usage

```python
from datasets import load_dataset

ds = load_dataset("{repo_id}")  # one-off snapshot; not regularly updated
```

Snapshot window `{metadata["window_id"]}` · {metadata["row_count"]} rows ·
generated {metadata["generated_at_utc"]}.
"""


def push_hf(artifacts: PublishArtifacts, repo_id: str, *, dry_run: bool = False) -> None:
    """Upload the Parquet + dataset card to a Hugging Face dataset repo.

    Idempotent: the repo is created if missing (exist_ok), uploads overwrite.
    HF_TOKEN is read from the environment by huggingface_hub.
    """
    card = build_dataset_card(artifacts.metadata, repo_id)
    if dry_run:
        log.info("hf.push.dry_run", repo_id=repo_id, parquet=artifacts.parquet_path.name)
        return
    from huggingface_hub import HfApi, create_repo

    create_repo(repo_id, repo_type="dataset", exist_ok=True)
    api = HfApi()
    api.upload_file(
        path_or_fileobj=str(artifacts.parquet_path),
        path_in_repo=artifacts.parquet_path.name,
        repo_id=repo_id,
        repo_type="dataset",
    )
    api.upload_file(
        path_or_fileobj=card.encode(),
        path_in_repo="README.md",
        repo_id=repo_id,
        repo_type="dataset",
    )
    log.info("hf.push.done", repo_id=repo_id, parquet=artifacts.parquet_path.name)


def push_github_release(
    artifacts: PublishArtifacts, tag: str, title: str, *, dry_run: bool = False
) -> None:
    """Publish the top-N CSV as a GitHub Release asset via the gh CLI.

    Idempotent: creates the release if missing, then uploads with --clobber so
    a rerun overwrites the existing asset.
    """
    create = [
        "gh",
        "release",
        "create",
        tag,
        "--title",
        title,
        "--notes",
        f"{_DATASET_NAME} snapshot (window {artifacts.metadata['window_id']})",
    ]
    upload = ["gh", "release", "upload", tag, str(artifacts.csv_path), "--clobber"]
    if dry_run:
        log.info("github.release.dry_run", create=" ".join(create), upload=" ".join(upload))
        return
    exists = subprocess.run(["gh", "release", "view", tag], capture_output=True).returncode == 0
    if not exists:
        subprocess.run(create, check=True)
    subprocess.run(upload, check=True)
    log.info("github.release.done", tag=tag, asset=artifacts.csv_path.name)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mart-path", required=True, help="Path to mart_domain_authority Parquet")
    parser.add_argument("--output-dir", default="build/publish", help="Local staging dir")
    parser.add_argument("--top-n", type=int, default=100_000, help="Rows in the top-N CSV")
    parser.add_argument("--window-id", default=None, help="Crawl window to publish")
    parser.add_argument("--hf-repo-id", default=None, help="Hugging Face dataset repo id")
    parser.add_argument("--github-tag", default=None, help="GitHub Release tag")
    parser.add_argument("--push-hf", action="store_true", help="Upload to Hugging Face")
    parser.add_argument("--push-github", action="store_true", help="Publish GitHub Release")
    parser.add_argument("--dry-run", action="store_true", help="Skip network calls")
    args = parser.parse_args(argv)

    artifacts = extract_public_index(
        args.mart_path, args.output_dir, top_n=args.top_n, window_id=args.window_id
    )
    log.info(
        "extract.done",
        rows=artifacts.metadata["row_count"],
        window_id=artifacts.metadata["window_id"],
        parquet=str(artifacts.parquet_path),
    )

    if args.push_hf:
        if not args.hf_repo_id:
            raise PublishError("--push-hf requires --hf-repo-id")
        push_hf(artifacts, args.hf_repo_id, dry_run=args.dry_run)
    if args.push_github:
        if not args.github_tag:
            raise PublishError("--push-github requires --github-tag")
        push_github_release(
            artifacts,
            args.github_tag,
            f"{_DATASET_NAME} {artifacts.metadata['window_id']}",
            dry_run=args.dry_run,
        )


if __name__ == "__main__":
    main()
