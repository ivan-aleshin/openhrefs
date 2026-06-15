"""Tests for experiments.exp5_wat.io (WAT iteration)."""

from pathlib import Path

from experiments.exp5_wat.io import iter_wat_links

_FIXTURE = str(Path(__file__).parent / "fixtures" / "sample.wat.gz")


def test_iter_wat_links_yields_anchor_rows_with_source_domain() -> None:
    rows = list(iter_wat_links(_FIXTURE))
    # two anchor hrefs across two records (the IMG entry is skipped)
    assert {(r["domain_from"], r["url_to"]) for r in rows} == {
        ("src1.com", "https://a.bg/x"),
        ("src1.com", "https://x.net/y"),
    }
    buy = next(r for r in rows if r["url_to"] == "https://a.bg/x")
    assert buy["url_from"] == "http://src1.com/page"
    assert buy["anchor"] == "buy"
    assert buy["rel"] == "nofollow"
