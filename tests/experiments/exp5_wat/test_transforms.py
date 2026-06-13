"""Unit tests for experiments.exp5_wat.transforms."""

from pyspark.sql import SparkSession
from pyspark.sql import types as T

from experiments.exp5_wat.transforms import (
    backlink_edges,
    derive_wat_path,
    expected_edges,
    extract_links,
    normalize_content_languages,
    parse_rel_flags,
    primary_language,
    qualify_target_domains,
    registered_domain_of_url,
    with_wat_prefix,
)

_PROJ_SCHEMA = T.StructType(
    [
        T.StructField("registered_domain", T.StringType()),
        T.StructField("content_languages", T.StringType()),
    ]
)


def test_derive_wat_path_substitutes_warc_segment_and_suffix() -> None:
    warc = "crawl-data/CC-MAIN-2026-21/segments/1700000000000.0/warc/CC-MAIN-20260521-00000.warc.gz"
    assert derive_wat_path(warc) == (
        "crawl-data/CC-MAIN-2026-21/segments/1700000000000.0/wat/CC-MAIN-20260521-00000.warc.wat.gz"
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
            "Payload-Metadata": {"HTTP-Response-Metadata": {"HTML-Metadata": {"Links": links}}}
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


def test_normalize_content_languages_passes_through_string(spark: SparkSession) -> None:
    df = spark.createDataFrame([("a.bg", "bul,eng")], _PROJ_SCHEMA)
    out = normalize_content_languages(df).collect()[0]
    assert out["content_languages"] == "bul,eng"


def test_normalize_content_languages_joins_array(spark: SparkSession) -> None:
    schema = T.StructType(
        [
            T.StructField("registered_domain", T.StringType()),
            T.StructField("content_languages", T.ArrayType(T.StringType())),
        ]
    )
    df = spark.createDataFrame([("a.bg", ["bul", "eng"])], schema)
    out = normalize_content_languages(df).collect()[0]
    assert out["content_languages"] == "bul,eng"


def test_qualify_target_domains_counts_and_share(spark: SparkSession) -> None:
    rows = [
        ("a.bg", "bul,eng"),  # a.bg: 2 target of 3 → share 0.666
        ("a.bg", "ron"),
        ("a.bg", "eng"),
        ("b.com", "eng,bul"),  # b.com: primary eng → 0 target of 1 → not a hit
    ]
    out = {
        r["registered_domain"]: r
        for r in qualify_target_domains(
            spark.createDataFrame(rows, _PROJ_SCHEMA), ["bul", "ron"], 0.25
        ).collect()
    }
    assert out["a.bg"]["total_200_pages"] == 3
    assert out["a.bg"]["target_language_pages"] == 2
    assert abs(out["a.bg"]["language_share"] - 2 / 3) < 1e-9
    assert out["a.bg"]["meets_share"] is True
    assert "b.com" not in out


_EDGE_SCHEMA = T.StructType(
    [
        T.StructField("domain_from", T.StringType()),
        T.StructField("domain_to", T.StringType()),
    ]
)
_S_SCHEMA = T.StructType([T.StructField("registered_domain", T.StringType())])
_LINK_SCHEMA = T.StructType(
    [
        T.StructField("domain_from", T.StringType()),
        T.StructField("url_from", T.StringType()),
        T.StructField("url_to", T.StringType()),
        T.StructField("anchor", T.StringType()),
        T.StructField("rel", T.StringType()),
    ]
)


_V3_EDGE_SCHEMA = T.StructType(
    [T.StructField("from_id", T.LongType()), T.StructField("to_id", T.LongType())]
)
_V3_MAP_SCHEMA = T.StructType(
    [T.StructField("id", T.LongType()), T.StructField("domain", T.StringType())]
)


def test_expected_edges_filters_in_id_space_then_resolves(spark: SparkSession) -> None:
    # (1,2) duplicated → must collapse to one (domain_from, domain_to) pair.
    edges = spark.createDataFrame([(1, 2), (1, 2), (3, 4), (1, 5)], _V3_EDGE_SCHEMA)
    vmap = spark.createDataFrame(
        [(1, "src1.com"), (2, "a.bg"), (3, "src2.com"), (4, "out.com"), (5, "b.ro")],
        _V3_MAP_SCHEMA,
    )
    s = spark.createDataFrame([("a.bg",), ("b.ro",)], _S_SCHEMA)
    rows = expected_edges(edges, vmap, s).collect()
    assert len(rows) == 2  # deduped, not 3
    assert {(r["domain_from"], r["domain_to"]) for r in rows} == {
        ("src1.com", "a.bg"),
        ("src1.com", "b.ro"),
    }


def test_backlink_edges_filters_source_in_dsrc_and_target_in_S(spark: SparkSession) -> None:
    # Use target.bg — a.bg is itself a PSL suffix so registered_domain_of_url returns None.
    links = spark.createDataFrame(
        [
            ("src1.com", "https://src1.com/p", "https://target.bg/p", "buy", "nofollow"),  # kept
            ("other.com", "https://other.com/p", "https://target.bg/p", "x", None),  # not in D_src
            ("src1.com", "https://src1.com/q", "https://x.net/p", "y", None),  # target not in S
        ],
        _LINK_SCHEMA,
    )
    d_src = spark.createDataFrame([("src1.com",)], _S_SCHEMA)
    s = spark.createDataFrame([("target.bg",)], _S_SCHEMA)
    out = backlink_edges(links, d_src, s).collect()
    assert len(out) == 1
    row = out[0]
    assert (row["domain_from"], row["domain_to"]) == ("src1.com", "target.bg")
    assert row["url_from"] == "https://src1.com/p"
    assert row["anchor"] == "buy"
    assert row["is_nofollow"] is True


def test_backlink_edges_rel_splits_on_any_whitespace(spark: SparkSession) -> None:
    # Use target.bg — a.bg is itself a PSL suffix so registered_domain_of_url returns None.
    rel = "nofollow\tugc  sponsored"
    links = spark.createDataFrame(
        [("src1.com", "https://src1.com/p", "https://target.bg/p", "x", rel)],
        _LINK_SCHEMA,
    )
    d_src = spark.createDataFrame([("src1.com",)], _S_SCHEMA)
    s = spark.createDataFrame([("target.bg",)], _S_SCHEMA)
    row = backlink_edges(links, d_src, s).collect()[0]
    assert (row["is_nofollow"], row["is_ugc"], row["is_sponsored"]) == (True, True, True)
