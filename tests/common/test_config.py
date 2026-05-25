"""Tests for the configuration loader (no Spark required)."""

from pathlib import Path

import pytest

from spark_jobs.common.config import (
    Settings,
    load_pipeline_config,
    load_storage,
)
from spark_jobs.common.errors import ConfigError


def _write_config(tmp_path: Path, scope_params: str) -> Path:
    config = tmp_path / "config.yml"
    config.write_text(f"scope:\n  all_of:\n    - language: [bul]\nscope_params:\n{scope_params}\n")
    return config


def test_load_storage_local_resolves_fixture_paths() -> None:
    paths = load_storage(Settings(openhrefs_env="local"))
    assert paths.raw_path == "tests/fixtures/parquet"
    assert paths.marts_path == "build/marts"
    assert paths.host_graph_path.startswith("https://")


def test_load_storage_gcp_uses_gcs_uris() -> None:
    paths = load_storage(Settings(openhrefs_env="gcp"))
    assert paths.raw_path.startswith("gs://")
    assert paths.host_graph_path == "gs://commoncrawl/projects/hyperlinkgraph"


def test_env_vars_override_raw_and_marts_paths() -> None:
    settings = Settings(
        openhrefs_env="local",
        raw_path="gs://override/raw",
        marts_path="gs://override/marts",
    )
    paths = load_storage(settings)
    assert paths.raw_path == "gs://override/raw"
    assert paths.marts_path == "gs://override/marts"
    # Source paths are not overridable — only the canonical outputs are.
    assert paths.host_graph_path.startswith("https://")


def test_unknown_environment_raises_config_error() -> None:
    with pytest.raises(ConfigError, match="unknown OPENHREFS_ENV"):
        load_storage(Settings(openhrefs_env="does-not-exist"))


def test_missing_storage_file_raises_config_error(tmp_path) -> None:  # type: ignore[no-untyped-def]
    with pytest.raises(ConfigError, match="config file not found"):
        load_storage(Settings(openhrefs_env="local"), storage_file=tmp_path / "absent.yml")


def test_pipeline_config_loads_scope_window_and_languages() -> None:
    config = load_pipeline_config()
    assert "all_of" in config.scope
    # Range, not an exact value — min_language_share is a tunable knob.
    assert 0.0 <= config.scope_params.min_language_share <= 1.0
    assert config.scope_params.subdomain_handling == "root_only"
    assert config.crawl_window.recent_count == 6
    assert config.languages.targets == ["bul", "ron"]


def test_pipeline_config_rejects_out_of_range_language_share(tmp_path: Path) -> None:
    config = _write_config(tmp_path, "  min_language_share: 1.5")
    with pytest.raises(ConfigError, match="invalid pipeline config"):
        load_pipeline_config(config)


def test_pipeline_config_rejects_unknown_subdomain_handling(tmp_path: Path) -> None:
    config = _write_config(tmp_path, "  subdomain_handling: nonsense")
    with pytest.raises(ConfigError, match="invalid pipeline config"):
        load_pipeline_config(config)
