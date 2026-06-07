"""Unit tests for the open-domain-authority-index publish tool."""

from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pytest

from publish.open_domain_authority_index import PublishError, _resolve_window


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
