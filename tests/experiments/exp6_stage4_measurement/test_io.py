"""Unit tests for experiments.exp6_stage4_measurement.io."""

import gzip
from pathlib import Path

from experiments.exp6_stage4_measurement.io import iter_wat_links_raw, read_wat_manifest
from experiments.exp6_stage4_measurement.stage_wat import build_sts_manifest

_WAT_FIXTURE = Path(__file__).resolve().parents[1] / "exp5_wat" / "fixtures" / "sample.wat.gz"


def test_read_wat_manifest_parses_gzip_lines(tmp_path: Path) -> None:
    manifest = tmp_path / "wat.paths.gz"
    lines = ["crawl-data/CC-MAIN-2026-21/wat/a.warc.wat.gz", "crawl-data/.../b.warc.wat.gz"]
    manifest.write_bytes(gzip.compress(("\n".join(lines) + "\n").encode()))
    assert read_wat_manifest(f"file://{manifest}") == lines


def test_iter_wat_links_raw_yields_raw_rows() -> None:
    rows = list(iter_wat_links_raw(f"file://{_WAT_FIXTURE}"))
    assert rows, "fixture should yield at least one link row"
    sample = rows[0]
    assert set(sample) == {"url_from", "url_to", "anchor", "rel"}
    assert sample["url_to"]


def test_iter_wat_links_raw_counts_records_via_callbacks() -> None:
    counters = {"records": 0, "malformed": 0, "no_links": 0}

    def bump(key: str):
        def _inc() -> None:
            counters[key] += 1

        return _inc

    list(
        iter_wat_links_raw(
            f"file://{_WAT_FIXTURE}",
            on_record=bump("records"),
            on_malformed=bump("malformed"),
            on_no_links=bump("no_links"),
        )
    )
    assert counters["records"] > 0  # the fixture has metadata records
    assert counters["malformed"] == 0  # fixture payloads are well-formed JSON
    assert counters["no_links"] >= 0  # records yielding no anchor links (fixture-dependent)


def test_build_sts_manifest_one_key_per_line_no_header() -> None:
    paths = ["crawl-data/CC-MAIN-2026-21/wat/a.warc.wat.gz", "crawl-data/x/b.warc.wat.gz"]
    manifest = build_sts_manifest(paths)
    lines = manifest.splitlines()
    assert lines[0] != "TsvHttpData-1.0"  # S3-source manifest, not a URL list
    assert lines == paths  # one raw object key per line (relative to the source bucket)
