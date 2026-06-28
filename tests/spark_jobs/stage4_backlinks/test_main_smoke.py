"""Deterministic smoke test for Stage 4 main._run against the committed WAT fixture.

Runs _run directly (not main()) so the session-scoped spark fixture is not stopped
by main()'s finally block. The fixture at tests/experiments/exp5_wat/fixtures/sample.wat.gz
contains 2 metadata records:
  - http://src1.com/page  → https://a.bg/x   rel='nofollow'
  - http://src1.com/other → https://x.net/y  rel=None

Raw link extraction gives 2 rows (raw_link_rows == 2). After resolve_domain_pairs, only
ONE domain pair survives: ``a.bg`` is a PSL public suffix (verified via tldextract —
``tldextract.extract('a.bg')`` yields ``suffix='a.bg'``, empty domain), so
``registered_domain('a.bg')`` returns None and that row is dropped.

Verified ground-truth domain-pair output (1 row):
  (src1.com, x.net) link_count=1 dofollow_count=1  [rel=None → dofollow]
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from spark_jobs.stage4_backlinks.main import _run

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_FIXTURE_WAT = (
    Path(__file__).resolve().parents[2] / "experiments" / "exp5_wat" / "fixtures" / "sample.wat.gz"
)

_CRAWL = "CC-MAIN-2026-21"
_RUN_ID = "smoke"

# ---------------------------------------------------------------------------
# Shared fixture — runs _run once per module, all assertions read the outputs
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def smoke_args(tmp_path_factory: pytest.TempPathFactory) -> argparse.Namespace:
    """Write a one-line WAT list and return a Namespace pointing at it."""
    tmp = tmp_path_factory.mktemp("stage4_smoke")
    wat_list = tmp / "wat_list.txt"
    wat_list.write_text("sample.wat.gz\n")
    return argparse.Namespace(
        wat_list=str(wat_list),
        wat_prefix=str(_FIXTURE_WAT.parent),
        output_path=str(tmp / "pairs"),
        metrics_path=str(tmp / "_metrics"),
        run_id=_RUN_ID,
        crawl=_CRAWL,
        shuffle_partitions=2,
    )


@pytest.fixture(scope="module")
def smoke_result(spark: SparkSession, smoke_args: argparse.Namespace) -> argparse.Namespace:
    """Run _run once; return args so downstream tests can locate written files."""
    _run(spark, smoke_args)
    return smoke_args


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------


def _load_metrics(args: argparse.Namespace) -> dict[str, object]:
    metrics_dir = Path(args.metrics_path) / f"crawl={args.crawl}" / f"run_id={args.run_id}"
    parts = list(metrics_dir.glob("part-*"))
    assert len(parts) == 1, f"expected 1 metrics part file, found {len(parts)}: {parts}"
    return json.loads(parts[0].read_text())  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# Assertions (each reads from smoke_result outputs)
# ---------------------------------------------------------------------------


def test_smoke_crawl_partition_exists(smoke_result: argparse.Namespace) -> None:
    """Parquet output contains the crawl partition directory."""
    partition_dir = Path(smoke_result.output_path) / f"crawl={_CRAWL}"
    assert partition_dir.exists(), f"partition dir not found: {partition_dir}"


def test_smoke_raw_link_rows(smoke_result: argparse.Namespace) -> None:
    """metrics JSON records raw_link_rows == 2 (one A@/href per WAT metadata record)."""
    metrics = _load_metrics(smoke_result)
    assert metrics["raw_link_rows"] == 2


def test_smoke_domain_pairs(spark: SparkSession, smoke_result: argparse.Namespace) -> None:
    """Domain-pair output: only (src1.com, x.net) survives; a.bg is a PSL suffix, dropped."""
    df = spark.read.parquet(smoke_result.output_path)
    rows = {(r["domain_from"], r["domain_to"]): r for r in df.collect()}
    # a.bg is a PSL public suffix — registered_domain('a.bg') == None, row dropped.
    assert set(rows) == {("src1.com", "x.net")}

    net = rows[("src1.com", "x.net")]  # rel=None → dofollow
    assert net["link_count"] == 1
    assert net["dofollow_count"] == 1
    assert net["ugc_count"] == 0
    assert net["sponsored_count"] == 0


def test_smoke_metrics_required_keys(smoke_result: argparse.Namespace) -> None:
    """Metrics JSON contains all required keys."""
    metrics = _load_metrics(smoke_result)
    required = {
        "n_wat_files",
        "raw_link_rows",
        "domain_pair_rows",
        "link_count_total",
        "files_attempted",
        "files_unreadable",
        "records_seen",
        "malformed_records",
        "records_with_no_links",
        "crawl",
        "run_id",
        "manifest_sha256",
        "wat_prefix",
        "output_path",
        "artifact_kind",
    }
    missing = required - set(metrics)
    assert not missing, f"missing metrics keys: {missing}"


def test_smoke_metrics_deterministic_values(smoke_result: argparse.Namespace) -> None:
    """All deterministic fixture metric values (the single main-level oracle)."""
    metrics = _load_metrics(smoke_result)
    assert metrics["n_wat_files"] == 1
    assert metrics["raw_link_rows"] == 2  # one A@/href per metadata record
    assert metrics["domain_pair_rows"] == 1  # a.bg dropped (PSL suffix)
    assert metrics["link_count_total"] == 1
    assert metrics["files_attempted"] == 1
    assert metrics["files_unreadable"] == 0
    assert metrics["records_seen"] == 2  # 2 metadata records
    assert metrics["malformed_records"] == 0
    assert metrics["records_with_no_links"] == 0
    assert metrics["crawl"] == _CRAWL
    assert metrics["run_id"] == _RUN_ID
    assert metrics["wat_prefix"] == str(_FIXTURE_WAT.parent)
    assert metrics["output_path"] == smoke_result.output_path


def test_smoke_metrics_sha256_format(smoke_result: argparse.Namespace) -> None:
    """manifest_sha256 is a 64-character lowercase hex string."""
    metrics = _load_metrics(smoke_result)
    sha = metrics["manifest_sha256"]
    assert isinstance(sha, str) and len(sha) == 64
    assert all(c in "0123456789abcdef" for c in sha)


def test_smoke_metrics_artifact_kind(smoke_result: argparse.Namespace) -> None:
    """artifact_kind matches the exact validation string."""
    metrics = _load_metrics(smoke_result)
    assert metrics["artifact_kind"] == "domain-grain validation (non-canonical for SPEC §6)"
