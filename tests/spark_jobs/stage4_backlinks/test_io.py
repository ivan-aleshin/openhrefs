"""Unit tests for spark_jobs.stage4_backlinks.io."""

from __future__ import annotations

import json
import uuid
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession

from spark_jobs.stage4_backlinks.io import (
    iter_wat_links_raw,
    read_wat_list,
    write_domain_link_pairs,
    write_metrics_json,
)
from spark_jobs.stage4_backlinks.transforms import CC_DOMAIN_LINK_PAIRS

_FIXTURE = Path(__file__).parents[2] / "experiments" / "exp5_wat" / "fixtures" / "sample.wat.gz"


# ---------------------------------------------------------------------------
# Helpers for synthetic WARC construction
# ---------------------------------------------------------------------------


def _make_warc_record(target_uri: str, payload: bytes) -> bytes:
    """Build minimal WARC/1.0 metadata record bytes (uncompressed)."""
    header = (
        "WARC/1.0\r\n"
        "WARC-Type: metadata\r\n"
        f"WARC-Record-ID: <urn:uuid:{uuid.uuid4()}>\r\n"
        f"WARC-Target-URI: {target_uri}\r\n"
        "Content-Type: application/json\r\n"
        f"Content-Length: {len(payload)}\r\n"
        "\r\n"
    ).encode()
    return header + payload + b"\r\n\r\n"


def _write_synthetic_wat(path: Path, records: list[tuple[str, bytes]]) -> None:
    """Write an uncompressed WARC file with the given metadata records."""
    path.write_bytes(b"".join(_make_warc_record(uri, body) for uri, body in records))


def _payload_with_link(target_uri: str, url_to: str) -> bytes:
    """Build a minimal WAT JSON payload with one A@/href link."""
    return json.dumps(
        {
            "Envelope": {
                "WARC-Header-Metadata": {"WARC-Target-URI": target_uri},
                "Payload-Metadata": {
                    "HTTP-Response-Metadata": {
                        "HTML-Metadata": {"Links": [{"path": "A@/href", "url": url_to}]}
                    }
                },
            }
        }
    ).encode()


def _payload_no_links(target_uri: str) -> bytes:
    """Build a minimal WAT JSON payload with an empty Links list."""
    return json.dumps(
        {
            "Envelope": {
                "WARC-Header-Metadata": {"WARC-Target-URI": target_uri},
                "Payload-Metadata": {"HTTP-Response-Metadata": {"HTML-Metadata": {"Links": []}}},
            }
        }
    ).encode()


# ---------------------------------------------------------------------------
# parser_equivalence — fastwarc+orjson must match warcio+json on real bytes
# ---------------------------------------------------------------------------


def test_parser_equivalence_fastwarc_matches_warcio() -> None:
    """iter_wat_links_raw output must equal the warcio+json oracle on the fixture."""
    import json as _json

    from warcio.archiveiterator import ArchiveIterator as WarcioIterator

    from spark_jobs.stage4_backlinks.transforms import links_from_payload

    # warcio+json reference oracle
    oracle_rows: list[dict[str, Any]] = []
    with open(_FIXTURE, "rb") as fh:
        for rec in WarcioIterator(fh):
            if rec.rec_type != "metadata":
                continue
            payload = _json.loads(rec.content_stream().read())
            oracle_rows.extend(links_from_payload(payload))

    # fastwarc+orjson under test
    fast_rows = list(iter_wat_links_raw(str(_FIXTURE)))

    assert fast_rows == oracle_rows


# ---------------------------------------------------------------------------
# on_record fires once per metadata record
# ---------------------------------------------------------------------------


def test_on_record_fires_for_each_metadata_record() -> None:
    """on_record callback count == number of metadata records in the fixture."""
    counter: list[int] = []
    rows = list(iter_wat_links_raw(str(_FIXTURE), on_record=lambda: counter.append(1)))

    # Fixture has 2 metadata records, each with one anchor link
    assert len(counter) == 2
    assert len(rows) == 2


def test_rows_have_expected_keys_and_url_from_from_payload() -> None:
    """Each yielded row has exactly {url_from, url_to, anchor, rel}; url_from
    is the payload Envelope.WARC-Header-Metadata.WARC-Target-URI."""
    rows = list(iter_wat_links_raw(str(_FIXTURE)))

    assert rows, "fixture must yield at least one row"
    for row in rows:
        assert set(row.keys()) == {"url_from", "url_to", "anchor", "rel"}
        # url_from must be non-None (fixture records have a target URI)
        assert row["url_from"] is not None

    # Fixture record 1: url_from == "http://src1.com/page"
    assert rows[0]["url_from"] == "http://src1.com/page"


# ---------------------------------------------------------------------------
# malformed record → on_malformed, skip, stream continues
# ---------------------------------------------------------------------------


def test_malformed_record_calls_on_malformed_and_skips(tmp_path: Path) -> None:
    """A JSON-unparseable metadata record calls on_malformed; the iterator does
    not raise, and the valid record's rows still come through."""
    valid_body = _payload_with_link("http://good.com/", "https://target.org/")
    bad_body = b"NOT VALID JSON {"

    wat_file = tmp_path / "mixed.warc"
    _write_synthetic_wat(
        wat_file,
        [
            ("http://good.com/", valid_body),
            ("http://bad.com/", bad_body),
        ],
    )

    malformed_calls: list[int] = []
    rows = list(
        iter_wat_links_raw(
            str(wat_file),
            on_malformed=lambda: malformed_calls.append(1),
        )
    )

    assert len(malformed_calls) == 1, "on_malformed must fire once for the bad record"
    assert len(rows) == 1, "the good record's row must still be yielded"
    assert rows[0]["url_from"] == "http://good.com/"


# ---------------------------------------------------------------------------
# on_no_links fires for records with zero anchor links
# ---------------------------------------------------------------------------


def test_on_no_links_does_not_fire_on_fixture() -> None:
    """Fixture records each have one anchor; on_no_links must NOT fire."""
    no_link_calls: list[int] = []
    list(
        iter_wat_links_raw(
            str(_FIXTURE),
            on_no_links=lambda: no_link_calls.append(1),
        )
    )
    assert len(no_link_calls) == 0


def test_on_no_links_fires_for_empty_links_record(tmp_path: Path) -> None:
    """A metadata record with empty Links calls on_no_links once."""
    wat_file = tmp_path / "nolinks.warc"
    _write_synthetic_wat(
        wat_file, [("http://nolinks.com/", _payload_no_links("http://nolinks.com/"))]
    )

    no_link_calls: list[int] = []
    rows = list(
        iter_wat_links_raw(
            str(wat_file),
            on_no_links=lambda: no_link_calls.append(1),
        )
    )

    assert len(no_link_calls) == 1
    assert rows == []


# ---------------------------------------------------------------------------
# read_wat_list — strips blank lines and surrounding whitespace
# ---------------------------------------------------------------------------


def test_read_wat_list_strips_and_drops_blank_lines(tmp_path: Path) -> None:
    """Returns only stripped non-empty lines in order."""
    content = (
        "  crawl-data/CC-MAIN-2026-21/segments/001.warc.wat.gz  \n"
        "\n"
        "   \n"
        "crawl-data/CC-MAIN-2026-21/segments/002.warc.wat.gz\n"
        "\n"
    )
    wat_list = tmp_path / "watlist.txt"
    wat_list.write_text(content)

    result = read_wat_list(str(wat_list))

    assert result == [
        "crawl-data/CC-MAIN-2026-21/segments/001.warc.wat.gz",
        "crawl-data/CC-MAIN-2026-21/segments/002.warc.wat.gz",
    ]


def test_read_wat_list_preserves_order(tmp_path: Path) -> None:
    """Line order is preserved."""
    paths = [f"segment/{i:04d}.wat.gz" for i in range(5)]
    (tmp_path / "list.txt").write_text("\n".join(paths) + "\n")
    assert read_wat_list(str(tmp_path / "list.txt")) == paths


# ---------------------------------------------------------------------------
# write_domain_link_pairs — partitioned Parquet round-trip
# ---------------------------------------------------------------------------


def test_write_domain_link_pairs_creates_crawl_partition(
    spark: SparkSession, tmp_path: Path
) -> None:
    """Parquet is written partitioned by crawl; partition dir exists after write."""
    from pyspark.sql import types as T

    schema = T.StructType([*CC_DOMAIN_LINK_PAIRS, T.StructField("crawl", T.StringType())])
    rows = [("a.com", "b.org", 3, 2, 0, 1, "CC-MAIN-2026-21")]
    df = spark.createDataFrame(rows, schema)

    out = str(tmp_path / "pairs")
    write_domain_link_pairs(df, out)

    partition_dir = tmp_path / "pairs" / "crawl=CC-MAIN-2026-21"
    assert partition_dir.exists(), f"expected partition dir {partition_dir}"


def test_write_domain_link_pairs_rows_round_trip(spark: SparkSession, tmp_path: Path) -> None:
    """Written rows survive a read-back intact (domain_from, domain_to, link_count)."""
    from pyspark.sql import types as T

    schema = T.StructType([*CC_DOMAIN_LINK_PAIRS, T.StructField("crawl", T.StringType())])
    src_rows = [
        ("a.com", "b.org", 3, 2, 0, 1, "CC-MAIN-2026-21"),
        ("c.com", "d.net", 1, 1, 0, 0, "CC-MAIN-2026-21"),
    ]
    df = spark.createDataFrame(src_rows, schema)

    out = str(tmp_path / "pairs")
    write_domain_link_pairs(df, out)

    read_back = spark.read.parquet(out)
    collected = {(r["domain_from"], r["domain_to"], r["link_count"]) for r in read_back.collect()}
    assert collected == {("a.com", "b.org", 3), ("c.com", "d.net", 1)}


def test_write_domain_link_pairs_dynamic_overwrite_preserves_other_crawls(
    spark: SparkSession, tmp_path: Path
) -> None:
    """Dynamic partition overwrite replaces only the written crawl; others survive.

    Regression guard: without ``partitionOverwriteMode=dynamic`` a static overwrite
    would wipe the whole dataset, deleting the unrelated ``CC-MAIN-2026-18`` partition.
    """
    from pyspark.sql import types as T

    schema = T.StructType([*CC_DOMAIN_LINK_PAIRS, T.StructField("crawl", T.StringType())])
    out = str(tmp_path / "pairs")

    initial = spark.createDataFrame(
        [
            ("a.com", "b.org", 1, 1, 0, 0, "CC-MAIN-2026-21"),
            ("c.com", "d.net", 5, 5, 0, 0, "CC-MAIN-2026-18"),
        ],
        schema,
    )
    write_domain_link_pairs(initial, out)

    rewrite = spark.createDataFrame(
        [("a.com", "b.org", 99, 99, 0, 0, "CC-MAIN-2026-21")],
        schema,
    )
    write_domain_link_pairs(rewrite, out)

    read_back = {
        (r["crawl"], r["domain_from"], r["link_count"]) for r in spark.read.parquet(out).collect()
    }
    assert read_back == {
        ("CC-MAIN-2026-21", "a.com", 99),  # written crawl replaced
        ("CC-MAIN-2026-18", "c.com", 5),  # unrelated crawl preserved
    }


# ---------------------------------------------------------------------------
# write_metrics_json — single-file JSON round-trip
# ---------------------------------------------------------------------------


def test_write_metrics_json_single_file_round_trip(spark: SparkSession, tmp_path: Path) -> None:
    """Metrics dict serializes to a single part-* file and deserializes correctly."""
    metrics: dict[str, Any] = {
        "records_seen": 1_000_000,
        "files_processed": 42,
        "crawl": "CC-MAIN-2026-21",
    }

    out = str(tmp_path / "metrics")
    write_metrics_json(spark, metrics, out)

    part_files = list((tmp_path / "metrics").glob("part-*"))
    assert len(part_files) == 1, "expected exactly one part-* file"

    raw = part_files[0].read_text()
    loaded = json.loads(raw)

    assert loaded["records_seen"] == 1_000_000
    assert loaded["files_processed"] == 42
    assert loaded["crawl"] == "CC-MAIN-2026-21"


def test_write_metrics_json_non_serializable_values_coerced(
    spark: SparkSession, tmp_path: Path
) -> None:
    """Non-JSON-native values (e.g. datetime) are coerced via default=str."""
    import datetime

    metrics: dict[str, Any] = {"ts": datetime.datetime(2026, 6, 28, 12, 0, 0)}
    out = str(tmp_path / "metrics_ts")
    # must not raise
    write_metrics_json(spark, metrics, out)

    part_file = next((tmp_path / "metrics_ts").glob("part-*"))
    loaded = json.loads(part_file.read_text())
    assert "ts" in loaded
    assert "2026" in loaded["ts"]
