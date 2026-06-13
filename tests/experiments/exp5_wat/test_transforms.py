"""Unit tests for experiments.exp5_wat.transforms."""

from experiments.exp5_wat.transforms import (
    derive_wat_path,
    extract_links,
    parse_rel_flags,
    primary_language,
    registered_domain_of_url,
    with_wat_prefix,
)


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


def _wat_payload(links: list[dict]) -> dict:
    return {
        "Envelope": {
            "Payload-Metadata": {
                "HTTP-Response-Metadata": {"HTML-Metadata": {"Links": links}}
            }
        }
    }


def test_extract_links_keeps_only_anchor_hrefs() -> None:
    payload = _wat_payload(
        [
            {"path": "A@/href", "url": "http://t.example/p", "text": "buy", "rel": "nofollow"},
            {"path": "IMG@/src", "url": "http://t.example/i.png"},
            {"path": "A@/href", "url": "http://u.example/", "text": "home"},
        ]
    )
    out = extract_links(payload)
    assert out == [
        {"url": "http://t.example/p", "anchor": "buy", "rel": "nofollow"},
        {"url": "http://u.example/", "anchor": "home", "rel": None},
    ]


def test_extract_links_tolerates_missing_structure() -> None:
    assert extract_links({}) == []
    assert extract_links({"Envelope": {}}) == []


def test_extract_links_skips_entries_without_url() -> None:
    payload = _wat_payload([{"path": "A@/href", "text": "x"}])
    assert extract_links(payload) == []


def test_primary_language_takes_first_element() -> None:
    assert primary_language("ron,eng,bul") == "ron"


def test_primary_language_none_on_empty() -> None:
    assert primary_language(None) is None
    assert primary_language("") is None


def test_registered_domain_of_url_extracts_pld() -> None:
    assert registered_domain_of_url("https://www.example.co.uk/path?q=1") == "example.co.uk"


def test_registered_domain_of_url_none_on_garbage() -> None:
    assert registered_domain_of_url("not a url") is None
    assert registered_domain_of_url(None) is None
