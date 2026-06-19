"""Unit tests for experiments.exp6_stage4_measurement.transforms."""

from pyspark.sql import SparkSession

from experiments.exp6_stage4_measurement.transforms import (
    links_from_payload,
    nested_ladder_samples,
    resolve_domains,
    sample_wat_paths,
)


def test_package_imports() -> None:
    import experiments.exp6_stage4_measurement  # noqa: F401


def test_sample_wat_paths_is_deterministic_for_seed() -> None:
    paths = [f"crawl-data/CC-MAIN-2026-21/wat/f{i}.warc.wat.gz" for i in range(100)]
    a = sample_wat_paths(paths, 10, seed=42)
    b = sample_wat_paths(paths, 10, seed=42)
    assert a == b
    assert len(a) == 10
    assert set(a).issubset(set(paths))


def test_sample_wat_paths_different_seed_differs() -> None:
    paths = [f"f{i}" for i in range(100)]
    assert sample_wat_paths(paths, 10, seed=1) != sample_wat_paths(paths, 10, seed=2)


def test_sample_wat_paths_n_ge_len_returns_all_sorted() -> None:
    paths = ["b", "a", "c"]
    assert sample_wat_paths(paths, 10, seed=0) == ["a", "b", "c"]


def test_nested_ladder_samples_are_nested_and_deterministic() -> None:
    paths = [f"f{i}" for i in range(5000)]
    out = nested_ladder_samples(paths, [200, 1000, 3000], seed=42)
    assert [len(out[s]) for s in (200, 1000, 3000)] == [200, 1000, 3000]
    assert set(out[200]).issubset(set(out[1000]))
    assert set(out[1000]).issubset(set(out[3000]))
    assert nested_ladder_samples(paths, [200, 1000, 3000], seed=42) == out
    assert all(out[s] == sorted(out[s]) for s in (200, 1000, 3000))


def test_nested_ladder_samples_unordered_sizes_and_empty() -> None:
    paths = [f"f{i}" for i in range(5000)]
    out = nested_ladder_samples(paths, [3000, 200, 1000], seed=7)
    assert set(out[200]).issubset(set(out[1000]))
    assert set(out[1000]).issubset(set(out[3000]))
    assert nested_ladder_samples(paths, [], seed=7) == {}


def test_sample_wat_paths_empty_population() -> None:
    assert sample_wat_paths([], 5, seed=0) == []


def _payload(target_uri: str | None, links: list[dict]) -> dict:
    return {
        "Envelope": {
            "WARC-Header-Metadata": {"WARC-Target-URI": target_uri},
            "Payload-Metadata": {"HTTP-Response-Metadata": {"HTML-Metadata": {"Links": links}}},
        }
    }


def test_links_from_payload_emits_raw_fields_only() -> None:
    payload = _payload(
        "https://src.example/page",
        [
            {"path": "A@/href", "url": "https://dst.example/a", "text": "hi", "rel": "nofollow"},
            {"path": "IMG@/src", "url": "https://dst.example/img.png"},
        ],
    )
    rows = links_from_payload(payload)
    assert rows == [
        {
            "url_from": "https://src.example/page",
            "url_to": "https://dst.example/a",
            "anchor": "hi",
            "rel": "nofollow",
        }
    ]


def test_links_from_payload_keeps_rows_when_target_uri_missing() -> None:
    payload = _payload(None, [{"path": "A@/href", "url": "https://dst.example/a"}])
    rows = links_from_payload(payload)
    assert rows[0]["url_from"] is None
    assert rows[0]["url_to"] == "https://dst.example/a"


def test_links_from_payload_empty_when_no_anchor() -> None:
    # only a non-anchor link -> zero rows; this is the condition that drives on_no_links.
    payload = _payload("https://src.example/p", [{"path": "IMG@/src", "url": "https://x/y.png"}])
    assert links_from_payload(payload) == []


def test_resolve_domains_adds_registered_domain(spark: SparkSession) -> None:
    df = spark.createDataFrame(
        [("https://www.bbc.co.uk/news",), ("http://shop.example.com/x",), (None,)],
        ["url_to"],
    )
    result = resolve_domains(df, "url_to", "domain_to").collect()
    out = {r["url_to"]: r["domain_to"] for r in result}
    assert out["https://www.bbc.co.uk/news"] == "bbc.co.uk"
    assert out["http://shop.example.com/x"] == "example.com"
    assert out[None] is None
