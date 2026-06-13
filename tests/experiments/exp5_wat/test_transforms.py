"""Unit tests for experiments.exp5_wat.transforms."""

from experiments.exp5_wat.transforms import derive_wat_path, parse_rel_flags, with_wat_prefix


def test_derive_wat_path_substitutes_warc_segment_and_suffix() -> None:
    warc = (
        "crawl-data/CC-MAIN-2026-21/segments/1700000000000.0/"
        "warc/CC-MAIN-20260521-00000.warc.gz"
    )
    assert derive_wat_path(warc) == (
        "crawl-data/CC-MAIN-2026-21/segments/1700000000000.0/"
        "wat/CC-MAIN-20260521-00000.warc.wat.gz"
    )


def test_derive_wat_path_none_on_missing() -> None:
    assert derive_wat_path(None) is None


def test_derive_wat_path_none_on_unexpected_shape() -> None:
    assert derive_wat_path("crawl-data/CC-MAIN-2026-21/robots/foo.txt") is None


def test_with_wat_prefix_prepends_only_relative() -> None:
    assert with_wat_prefix("crawl-data/x.warc.wat.gz", "s3://commoncrawl/") == (
        "s3://commoncrawl/crawl-data/x.warc.wat.gz"
    )


def test_with_wat_prefix_normalizes_missing_slash() -> None:
    assert with_wat_prefix("crawl-data/x.wat.gz", "s3://commoncrawl") == (
        "s3://commoncrawl/crawl-data/x.wat.gz"
    )


def test_with_wat_prefix_leaves_absolute_unchanged() -> None:
    for absolute in (
        "s3://commoncrawl/crawl-data/x.wat.gz",
        "s3a://commoncrawl/crawl-data/x.wat.gz",
        "file:///tmp/x.wat.gz",
    ):
        assert with_wat_prefix(absolute, "s3://commoncrawl/") == absolute


def test_parse_rel_flags_multitoken_case_insensitive() -> None:
    assert parse_rel_flags("Nofollow UGC") == {
        "is_nofollow": True,
        "is_ugc": True,
        "is_sponsored": False,
    }


def test_parse_rel_flags_none_all_false() -> None:
    assert parse_rel_flags(None) == {
        "is_nofollow": False,
        "is_ugc": False,
        "is_sponsored": False,
    }


def test_parse_rel_flags_sponsored() -> None:
    assert parse_rel_flags("sponsored")["is_sponsored"] is True
