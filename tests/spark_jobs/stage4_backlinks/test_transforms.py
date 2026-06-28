"""Unit tests for spark_jobs.stage4_backlinks.transforms."""

from __future__ import annotations

import pytest
from chispa import assert_df_equality
from pyspark.sql import SparkSession
from pyspark.sql import types as T

from spark_jobs.common.errors import DataSourceError
from spark_jobs.stage4_backlinks.transforms import (
    CC_DOMAIN_LINK_PAIRS,
    RAW_LINK_SCHEMA,
    aggregate_domain_pairs,
    default_metrics_path,
    extract_links,
    links_from_payload,
    manifest_sha256,
    resolve_domain_pairs,
    resolve_wat_path,
)

# ---------------------------------------------------------------------------
# manifest_sha256
# ---------------------------------------------------------------------------


def test_manifest_sha256_blank_lines_ignored() -> None:
    with_blanks = "path/a.gz\n\npath/b.gz\n"
    without_blanks = "path/a.gz\npath/b.gz"
    assert manifest_sha256(with_blanks) == manifest_sha256(without_blanks)


def test_manifest_sha256_whitespace_stripped() -> None:
    padded = "  path/a.gz  \n  path/b.gz  "
    clean = "path/a.gz\npath/b.gz"
    assert manifest_sha256(padded) == manifest_sha256(clean)


def test_manifest_sha256_order_preserved() -> None:
    ab = "path/a.gz\npath/b.gz"
    ba = "path/b.gz\npath/a.gz"
    assert manifest_sha256(ab) != manifest_sha256(ba)


def test_manifest_sha256_trailing_newline_stable() -> None:
    without_trailing = "path/a.gz\npath/b.gz"
    with_trailing_blank = "path/a.gz\npath/b.gz\n"
    assert manifest_sha256(without_trailing) == manifest_sha256(with_trailing_blank)


def test_manifest_sha256_returns_64_hex_chars() -> None:
    digest = manifest_sha256("path/a.gz")
    assert len(digest) == 64
    assert all(c in "0123456789abcdef" for c in digest)


# ---------------------------------------------------------------------------
# resolve_wat_path
# ---------------------------------------------------------------------------


def test_resolve_wat_path_relative_joined_to_prefix() -> None:
    result = resolve_wat_path("crawl-data/x.wat.gz", "gs://b/root")
    assert result == "gs://b/root/crawl-data/x.wat.gz"


def test_resolve_wat_path_single_slash_between_prefix_and_entry() -> None:
    # prefix without trailing slash
    r1 = resolve_wat_path("crawl-data/x.wat.gz", "gs://b/root")
    # prefix with trailing slash
    r2 = resolve_wat_path("crawl-data/x.wat.gz", "gs://b/root/")
    assert r1 == r2 == "gs://b/root/crawl-data/x.wat.gz"


def test_resolve_wat_path_entry_with_leading_slash() -> None:
    result = resolve_wat_path("/crawl-data/x.wat.gz", "gs://b/root")
    assert result == "gs://b/root/crawl-data/x.wat.gz"


def test_resolve_wat_path_gs_passthrough() -> None:
    full = "gs://bucket/crawl-data/x.wat.gz"
    assert resolve_wat_path(full, "gs://b/root") == full


def test_resolve_wat_path_s3_passthrough() -> None:
    full = "s3://commoncrawl/crawl-data/x.wat.gz"
    assert resolve_wat_path(full, "gs://b/root") == full


def test_resolve_wat_path_empty_raises() -> None:
    with pytest.raises(DataSourceError):
        resolve_wat_path("", "gs://b/root")


def test_resolve_wat_path_blank_raises() -> None:
    with pytest.raises(DataSourceError):
        resolve_wat_path("   ", "gs://b/root")


# ---------------------------------------------------------------------------
# default_metrics_path
# ---------------------------------------------------------------------------


def test_default_metrics_path_standard() -> None:
    result = default_metrics_path("gs://bucket/raw/cc_domain_link_pairs")
    assert result == "gs://bucket/raw/_metrics/cc_domain_link_pairs"


def test_default_metrics_path_trailing_slash_stripped() -> None:
    result = default_metrics_path("gs://bucket/raw/cc_domain_link_pairs/")
    assert result == "gs://bucket/raw/_metrics/cc_domain_link_pairs"


def test_default_metrics_path_s3_scheme() -> None:
    result = default_metrics_path("s3://my-bucket/data/links")
    assert result == "s3://my-bucket/data/_metrics/links"


def test_default_metrics_path_gs_bucket_root_raises() -> None:
    with pytest.raises(DataSourceError):
        default_metrics_path("gs://bucket")


def test_default_metrics_path_s3_bucket_root_raises() -> None:
    with pytest.raises(DataSourceError):
        default_metrics_path("s3://bucket")


def test_default_metrics_path_gs_bucket_root_trailing_slash_raises() -> None:
    with pytest.raises(DataSourceError):
        default_metrics_path("gs://bucket/")


# ---------------------------------------------------------------------------
# extract_links
# ---------------------------------------------------------------------------


def _make_payload(links: list[dict[str, str | None]] | None = None) -> dict[object, object]:
    """Build a minimal WAT payload with the given links list."""
    envelope: dict[object, object] = {}
    if links is not None:
        envelope = {
            "Payload-Metadata": {
                "HTTP-Response-Metadata": {
                    "HTML-Metadata": {"Links": links},
                },
            }
        }
    return {"Envelope": envelope}


def test_extract_links_returns_anchor_links() -> None:
    payload = _make_payload(
        [{"path": "A@/href", "url": "https://b.com/", "text": "click", "rel": "nofollow"}]
    )
    result = extract_links(payload)
    assert len(result) == 1
    assert result[0] == {"url": "https://b.com/", "anchor": "click", "rel": "nofollow"}


def test_extract_links_skips_non_anchor_path() -> None:
    payload = _make_payload(
        [
            {"path": "IMG@/src", "url": "https://b.com/img.png"},
            {"path": "A@/href", "url": "https://b.com/"},
        ]
    )
    result = extract_links(payload)
    assert len(result) == 1
    assert result[0]["url"] == "https://b.com/"


def test_extract_links_skips_link_without_url() -> None:
    payload = _make_payload([{"path": "A@/href", "url": None}])
    assert extract_links(payload) == []


def test_extract_links_skips_empty_url() -> None:
    payload = _make_payload([{"path": "A@/href", "url": ""}])
    assert extract_links(payload) == []


def test_extract_links_text_none_allowed() -> None:
    payload = _make_payload([{"path": "A@/href", "url": "https://b.com/", "text": None}])
    result = extract_links(payload)
    assert result[0]["anchor"] is None


def test_extract_links_rel_absent_is_none() -> None:
    payload = _make_payload([{"path": "A@/href", "url": "https://b.com/"}])
    result = extract_links(payload)
    assert result[0]["rel"] is None


def test_extract_links_missing_links_key() -> None:
    payload: dict[object, object] = {
        "Envelope": {"Payload-Metadata": {"HTTP-Response-Metadata": {"HTML-Metadata": {}}}}
    }
    assert extract_links(payload) == []


def test_extract_links_missing_html_metadata() -> None:
    payload: dict[object, object] = {
        "Envelope": {"Payload-Metadata": {"HTTP-Response-Metadata": {}}}
    }
    assert extract_links(payload) == []


def test_extract_links_missing_envelope() -> None:
    assert extract_links({}) == []


# ---------------------------------------------------------------------------
# links_from_payload
# ---------------------------------------------------------------------------


def _make_full_payload(
    target_uri: str | None,
    links: list[dict[str, str | None]] | None = None,
) -> dict[object, object]:
    """Build a WAT payload with WARC header and links."""
    warc_header: dict[object, object] = {}
    if target_uri is not None:
        warc_header = {"WARC-Target-URI": target_uri}
    envelope: dict[object, object] = {"WARC-Header-Metadata": warc_header}
    if links is not None:
        envelope["Payload-Metadata"] = {
            "HTTP-Response-Metadata": {
                "HTML-Metadata": {"Links": links},
            }
        }
    return {"Envelope": envelope}


def test_links_from_payload_emits_url_from() -> None:
    payload = _make_full_payload(
        "https://a.com/page",
        [{"path": "A@/href", "url": "https://b.com/", "text": "b", "rel": None}],
    )
    rows = links_from_payload(payload)
    assert len(rows) == 1
    assert rows[0]["url_from"] == "https://a.com/page"
    assert rows[0]["url_to"] == "https://b.com/"


def test_links_from_payload_url_from_none_when_uri_missing() -> None:
    payload = _make_full_payload(None, [{"path": "A@/href", "url": "https://b.com/"}])
    rows = links_from_payload(payload)
    assert rows[0]["url_from"] is None


def test_links_from_payload_no_links_returns_empty() -> None:
    payload = _make_full_payload("https://a.com/page", [])
    assert links_from_payload(payload) == []


def test_links_from_payload_carries_anchor_and_rel() -> None:
    payload = _make_full_payload(
        "https://a.com/",
        [{"path": "A@/href", "url": "https://b.com/", "text": "B", "rel": "ugc"}],
    )
    rows = links_from_payload(payload)
    assert rows[0]["anchor"] == "B"
    assert rows[0]["rel"] == "ugc"


# ---------------------------------------------------------------------------
# resolve_domain_pairs
# ---------------------------------------------------------------------------

_INPUT_SCHEMA = T.StructType(
    [
        T.StructField("url_from", T.StringType()),
        T.StructField("url_to", T.StringType()),
        T.StructField("rel", T.StringType()),
    ]
)


def test_resolve_domain_pairs_valid_rows_resolved(spark: SparkSession) -> None:
    rows = [("http://sub.example.com/a", "http://another.example.org/b", None)]
    df = spark.createDataFrame(rows, _INPUT_SCHEMA)
    out = resolve_domain_pairs(df)
    collected = out.collect()
    assert len(collected) == 1
    assert collected[0]["domain_from"] == "example.com"
    assert collected[0]["domain_to"] == "example.org"


def test_resolve_domain_pairs_drops_unparseable_url_to(spark: SparkSession) -> None:
    rows = [
        ("http://good.com/", "not-a-url", None),
        ("http://good.com/", "http://target.org/", None),
    ]
    df = spark.createDataFrame(rows, _INPUT_SCHEMA)
    out = resolve_domain_pairs(df)
    assert out.count() == 1


def test_resolve_domain_pairs_drops_unparseable_url_from(spark: SparkSession) -> None:
    rows = [
        ("/relative-path", "http://target.org/", None),
        ("http://source.com/", "http://target.org/", None),
    ]
    df = spark.createDataFrame(rows, _INPUT_SCHEMA)
    out = resolve_domain_pairs(df)
    assert out.count() == 1


def test_resolve_domain_pairs_drops_ip_url(spark: SparkSession) -> None:
    # IP address: parse_url extracts host but registered_domain rejects it.
    rows = [
        ("http://source.com/", "http://10.0.0.1/", None),
        ("http://source.com/", "http://target.org/", None),
    ]
    df = spark.createDataFrame(rows, _INPUT_SCHEMA)
    out = resolve_domain_pairs(df)
    assert out.count() == 1


def test_resolve_domain_pairs_host_dedup_maps_subdomains_consistently(
    spark: SparkSession,
) -> None:
    # Two different subdomains of the same registered domain must both map to it.
    rows = [
        ("http://sub1.example.com/", "http://target.org/", None),
        ("http://sub2.example.com/", "http://target.org/", None),
    ]
    df = spark.createDataFrame(rows, _INPUT_SCHEMA)
    out = resolve_domain_pairs(df)
    domains_from = {r["domain_from"] for r in out.collect()}
    assert domains_from == {"example.com"}


def test_resolve_domain_pairs_rel_carried_through(spark: SparkSession) -> None:
    rows = [("http://a.com/", "http://b.org/", "nofollow")]
    df = spark.createDataFrame(rows, _INPUT_SCHEMA)
    out = resolve_domain_pairs(df)
    assert out.collect()[0]["rel"] == "nofollow"


def test_resolve_domain_pairs_output_count_less_than_input(spark: SparkSession) -> None:
    rows = [
        ("http://good.com/", "http://target.org/", None),  # valid
        ("bad-url", "http://target.org/", None),  # dropped: no host_from
        ("http://good.com/", "bad-url", None),  # dropped: no host_to
    ]
    df = spark.createDataFrame(rows, _INPUT_SCHEMA)
    out = resolve_domain_pairs(df)
    assert out.count() < len(rows)
    assert out.count() == 1


def test_resolve_domain_pairs_udf_not_pushed_to_raw_rows(spark: SparkSession) -> None:
    # Regression guard: the host-dedup cost lever must not be defeated by Catalyst
    # pushing the registered_domain UDF below .distinct() onto raw URL rows.
    # .asNondeterministic() on the UDF acts as a Catalyst barrier.
    rows = [
        ("http://sub.example.com/a", "http://target.org/b", None),
        ("http://sub.example.com/c", "http://target.org/d", None),
    ]
    df = spark.createDataFrame(rows, _INPUT_SCHEMA)
    result = resolve_domain_pairs(df)
    plan = result._jdf.queryExecution().executedPlan().toString()
    # UDF must NOT be pushed onto raw URL columns (the push-down bug signature).
    assert "registered_domain(parse_url" not in plan
    # Sanity: the UDF still appears in the plan (on the deduplicated host column).
    assert "registered_domain(" in plan


# ---------------------------------------------------------------------------
# aggregate_domain_pairs
# ---------------------------------------------------------------------------

_PAIR_SCHEMA = T.StructType(
    [
        T.StructField("domain_from", T.StringType()),
        T.StructField("domain_to", T.StringType()),
        T.StructField("rel", T.StringType()),
    ]
)


def test_aggregate_domain_pairs_null_rel_is_dofollow(spark: SparkSession) -> None:
    rows = [("a.com", "b.org", None)]
    df = spark.createDataFrame(rows, _PAIR_SCHEMA)
    out = aggregate_domain_pairs(df)
    row = out.collect()[0]
    assert row["dofollow_count"] == 1
    assert row["link_count"] == 1


def test_aggregate_domain_pairs_empty_rel_is_dofollow(spark: SparkSession) -> None:
    rows = [("a.com", "b.org", "")]
    df = spark.createDataFrame(rows, _PAIR_SCHEMA)
    out = aggregate_domain_pairs(df)
    row = out.collect()[0]
    assert row["dofollow_count"] == 1


def test_aggregate_domain_pairs_nofollow_not_counted_as_dofollow(
    spark: SparkSession,
) -> None:
    rows = [("a.com", "b.org", "nofollow")]
    df = spark.createDataFrame(rows, _PAIR_SCHEMA)
    out = aggregate_domain_pairs(df)
    row = out.collect()[0]
    assert row["dofollow_count"] == 0
    assert row["link_count"] == 1


def test_aggregate_domain_pairs_nofollow_case_insensitive(spark: SparkSession) -> None:
    rows = [("a.com", "b.org", "NOFOLLOW")]
    df = spark.createDataFrame(rows, _PAIR_SCHEMA)
    out = aggregate_domain_pairs(df)
    assert out.collect()[0]["dofollow_count"] == 0


def test_aggregate_domain_pairs_multi_token_rel(spark: SparkSession) -> None:
    rows = [("a.com", "b.org", "nofollow ugc sponsored")]
    df = spark.createDataFrame(rows, _PAIR_SCHEMA)
    out = aggregate_domain_pairs(df)
    row = out.collect()[0]
    assert row["dofollow_count"] == 0
    assert row["ugc_count"] == 1
    assert row["sponsored_count"] == 1


def test_aggregate_domain_pairs_tab_separated_rel(spark: SparkSession) -> None:
    # Tab-separated tokens must be split correctly.
    rows = [("a.com", "b.org", "nofollow\tugc")]
    df = spark.createDataFrame(rows, _PAIR_SCHEMA)
    out = aggregate_domain_pairs(df)
    row = out.collect()[0]
    assert row["dofollow_count"] == 0
    assert row["ugc_count"] == 1


def test_aggregate_domain_pairs_self_link_kept(spark: SparkSession) -> None:
    rows = [("a.com", "a.com", None)]
    df = spark.createDataFrame(rows, _PAIR_SCHEMA)
    out = aggregate_domain_pairs(df)
    assert out.count() == 1
    assert out.collect()[0]["domain_from"] == "a.com"
    assert out.collect()[0]["domain_to"] == "a.com"


def test_aggregate_domain_pairs_aggregates_multiple_rows(spark: SparkSession) -> None:
    rows = [
        ("a.com", "b.org", None),
        ("a.com", "b.org", "nofollow ugc"),
        ("a.com", "b.org", "sponsored"),
    ]
    df = spark.createDataFrame(rows, _PAIR_SCHEMA)
    out = aggregate_domain_pairs(df)
    expected = spark.createDataFrame(
        [("a.com", "b.org", 3, 2, 1, 1)],
        CC_DOMAIN_LINK_PAIRS,
    )
    assert_df_equality(out, expected, ignore_nullable=True, ignore_row_order=True)


def test_aggregate_domain_pairs_output_schema_matches_cc_domain_link_pairs(
    spark: SparkSession,
) -> None:
    rows = [("a.com", "b.org", None)]
    df = spark.createDataFrame(rows, _PAIR_SCHEMA)
    out = aggregate_domain_pairs(df)
    # Verify field names in order
    assert [f.name for f in out.schema] == [f.name for f in CC_DOMAIN_LINK_PAIRS]
    # Verify types (all counts must be LongType)
    assert dict(out.dtypes) == {f.name: f.dataType.simpleString() for f in CC_DOMAIN_LINK_PAIRS}


def test_aggregate_domain_pairs_empty_input_returns_empty(spark: SparkSession) -> None:
    df = spark.createDataFrame([], _PAIR_SCHEMA)
    out = aggregate_domain_pairs(df)
    assert out.count() == 0
    assert [f.name for f in out.schema] == [f.name for f in CC_DOMAIN_LINK_PAIRS]
    assert dict(out.dtypes) == {f.name: f.dataType.simpleString() for f in CC_DOMAIN_LINK_PAIRS}


def test_raw_link_schema_has_expected_fields() -> None:
    field_names = [f.name for f in RAW_LINK_SCHEMA]
    assert field_names == ["url_from", "url_to", "anchor", "rel"]
