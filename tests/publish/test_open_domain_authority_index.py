"""Unit tests for the open-domain-authority-index publish tool."""

from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pytest

from publish.open_domain_authority_index import (
    PublishError,
    _resolve_window,
    extract_public_index,
)


def _write_mart(path: Path, rows: list[tuple]) -> None:
    """Write a tiny mart_domain_authority Parquet fixture.

    rows: (domain, window_id, open_authority, open_volume, pagerank_score)
    """
    con = duckdb.connect()
    con.execute(
        "create table m("
        "domain varchar, window_id varchar, open_authority double, "
        "open_volume double, pagerank_score double)"
    )
    con.executemany("insert into m values (?, ?, ?, ?, ?)", rows)
    con.sql("select * from m").write_parquet(str(path))
    con.close()


def test_resolve_window_single_returns_only_window():
    assert _resolve_window(None, ["CC-MAIN-2025-51"]) == "CC-MAIN-2025-51"


def test_resolve_window_explicit_returns_match():
    assert _resolve_window("CC-MAIN-2025-45", ["CC-MAIN-2025-45", "CC-MAIN-2025-51"]) == (
        "CC-MAIN-2025-45"
    )


def test_resolve_window_multiple_without_id_raises():
    with pytest.raises(PublishError, match="pass --window-id"):
        _resolve_window(None, ["CC-MAIN-2025-45", "CC-MAIN-2025-51"])


def test_resolve_window_unknown_id_raises():
    with pytest.raises(PublishError, match="not in mart"):
        _resolve_window("CC-MAIN-2099-01", ["CC-MAIN-2025-51"])


_ROWS = [
    ("a.com", "CC-MAIN-2025-51", 0.90, 7.0, 0.01),
    ("b.com", "CC-MAIN-2025-51", 0.50, 6.0, 0.005),
    ("c.com", "CC-MAIN-2025-51", 0.50, 4.0, 0.004),
    ("old.com", "CC-MAIN-2025-45", 0.99, 8.0, 0.02),
]


def test_extract_parquet_has_subset_columns_no_rank(tmp_path):
    mart = tmp_path / "mart.parquet"
    _write_mart(mart, _ROWS)
    arts = extract_public_index(str(mart), tmp_path / "out", top_n=10, window_id="CC-MAIN-2025-51")
    cols = duckdb.sql(f"select * from read_parquet('{arts.parquet_path}')").columns
    assert cols == ["domain", "open_authority", "open_volume", "window_id"]
    n = duckdb.sql(f"select count(*) from read_parquet('{arts.parquet_path}')").fetchone()[0]
    assert n == 3  # only the 2025-51 window


def test_extract_csv_has_rank_and_deterministic_order(tmp_path):
    mart = tmp_path / "mart.parquet"
    _write_mart(mart, _ROWS)
    arts = extract_public_index(str(mart), tmp_path / "out", top_n=10, window_id="CC-MAIN-2025-51")
    rows = duckdb.sql(
        f"select rank, domain from read_csv_auto('{arts.csv_path}') order by rank"
    ).fetchall()
    # open_authority desc, then domain asc → a (0.90), b (0.50, 'b'<'c'), c (0.50)
    assert rows == [(1, "a.com"), (2, "b.com"), (3, "c.com")]


def test_extract_top_n_limits_csv(tmp_path):
    mart = tmp_path / "mart.parquet"
    _write_mart(mart, _ROWS)
    arts = extract_public_index(str(mart), tmp_path / "out", top_n=2, window_id="CC-MAIN-2025-51")
    n = duckdb.sql(f"select count(*) from read_csv_auto('{arts.csv_path}')").fetchone()[0]
    assert n == 2


def test_extract_metadata_fields(tmp_path):
    mart = tmp_path / "mart.parquet"
    _write_mart(mart, _ROWS)
    arts = extract_public_index(str(mart), tmp_path / "out", top_n=10, window_id="CC-MAIN-2025-51")
    meta = json.loads(arts.metadata_path.read_text())
    assert meta["window_id"] == "CC-MAIN-2025-51"
    assert meta["row_count"] == 3
    assert meta["source_row_count"] == 4
    assert meta["available_window_ids"] == ["CC-MAIN-2025-45", "CC-MAIN-2025-51"]
    assert meta["top_n"] == 10


def test_extract_single_window_needs_no_window_id(tmp_path):
    mart = tmp_path / "mart.parquet"
    _write_mart(mart, _ROWS[:3])  # only 2025-51
    arts = extract_public_index(str(mart), tmp_path / "out", top_n=10)
    assert arts.metadata["window_id"] == "CC-MAIN-2025-51"


def test_extract_multi_window_without_id_raises(tmp_path):
    mart = tmp_path / "mart.parquet"
    _write_mart(mart, _ROWS)
    with pytest.raises(PublishError, match="pass --window-id"):
        extract_public_index(str(mart), tmp_path / "out", top_n=10)


@pytest.mark.parametrize("bad_top_n", [0, -5])
def test_extract_rejects_nonpositive_top_n(tmp_path, bad_top_n):
    mart = tmp_path / "mart.parquet"
    _write_mart(mart, _ROWS[:3])
    with pytest.raises(PublishError, match="top_n"):
        extract_public_index(str(mart), tmp_path / "out", top_n=bad_top_n)
