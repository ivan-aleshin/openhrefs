"""Tests for the configuration loader (no Spark required)."""

from pathlib import Path

import pytest

from spark_jobs.common.config import (
    Settings,
    checked_stage_output_path,
    load_pipeline_config,
    load_storage,
    resolve_authority,
    resolve_seed,
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


def test_default_config_resolves_cwd_local_files(tmp_path: Path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    # Simulate packaged execution: cwd holds the Spark --files landing (config.yml,
    # storage.yml by basename), not a repo checkout. Defaults must find them.
    (tmp_path / "config.yml").write_text("scope:\n  all_of:\n    - language: [bul]\n")
    (tmp_path / "storage.yml").write_text(
        "gcp:\n"
        "  raw_path: gs://b/raw\n"
        "  marts_path: gs://b/marts\n"
        "  host_graph_path: gs://commoncrawl/x\n"
        "  cdx_path: gs://commoncrawl/cdx\n"
        "  wat_path: gs://commoncrawl/wat\n"
    )
    monkeypatch.chdir(tmp_path)
    assert "all_of" in load_pipeline_config().scope
    assert load_storage(Settings(openhrefs_env="gcp")).raw_path == "gs://b/raw"


def test_default_config_missing_everywhere_raises_clear_error(tmp_path: Path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    # An empty cwd with no repo-root checkout: the default must fail with a clear error,
    # naming the file it looked for, not an opaque traceback.
    monkeypatch.setattr("spark_jobs.common.config._CONFIG_CANDIDATES", (tmp_path / "config.yml",))
    monkeypatch.chdir(tmp_path)
    with pytest.raises(ConfigError, match="config file not found"):
        load_pipeline_config()


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


def test_pipeline_config_loads_authority_calibration() -> None:
    cfg = load_pipeline_config()
    assert cfg.authority.damping == 0.85
    assert cfg.authority.tol == 0.001
    assert cfg.authority.max_iter == 30


def _write_authority_config(tmp_path: Path, authority: str) -> Path:
    config = tmp_path / "config.yml"
    config.write_text(f"scope:\n  all_of:\n    - language: [bul]\nauthority:\n{authority}\n")
    return config


def test_pipeline_config_rejects_damping_out_of_unit_interval(tmp_path: Path) -> None:
    config = _write_authority_config(tmp_path, "  damping: 1.5")
    with pytest.raises(ConfigError, match="invalid pipeline config"):
        load_pipeline_config(config)


def test_pipeline_config_rejects_non_positive_max_iter(tmp_path: Path) -> None:
    config = _write_authority_config(tmp_path, "  max_iter: 0")
    with pytest.raises(ConfigError, match="invalid pipeline config"):
        load_pipeline_config(config)


def test_pipeline_config_rejects_non_positive_tol(tmp_path: Path) -> None:
    config = _write_authority_config(tmp_path, "  tol: 0")
    with pytest.raises(ConfigError, match="invalid pipeline config"):
        load_pipeline_config(config)


def test_resolve_authority_falls_back_to_config_defaults() -> None:
    resolved = resolve_authority(None, None, None, None)
    cfg = load_pipeline_config().authority
    assert (resolved.damping, resolved.tol, resolved.max_iter) == (
        cfg.damping,
        cfg.tol,
        cfg.max_iter,
    )


def test_resolve_authority_applies_overrides() -> None:
    resolved = resolve_authority(None, 0.9, 0.01, 5)
    assert (resolved.damping, resolved.tol, resolved.max_iter) == (0.9, 0.01, 5)


@pytest.mark.parametrize(
    ("damping", "tol", "max_iter"),
    [(1.5, None, None), (None, 0.0, None), (None, None, 0)],
)
def test_resolve_authority_rejects_invalid_overrides(
    damping: float | None, tol: float | None, max_iter: int | None
) -> None:
    with pytest.raises(ConfigError, match="override"):
        resolve_authority(None, damping, tol, max_iter)


def test_resolve_authority_reads_custom_config(tmp_path: Path) -> None:
    config = _write_authority_config(tmp_path, "  damping: 0.7")
    assert resolve_authority(config, None, None, None).damping == 0.7


def test_checked_stage_output_path_rejects_fixtures() -> None:
    with pytest.raises(ConfigError, match="fixtures"):
        checked_stage_output_path("tests/fixtures/parquet/cc_domain_pagerank")


def test_checked_stage_output_path_allows_build_location() -> None:
    assert checked_stage_output_path("/tmp/build/raw/x") == "/tmp/build/raw/x"


def _write_seed_config(tmp_path: Path, seed: str) -> Path:
    config = tmp_path / "config.yml"
    config.write_text(f"scope:\n  all_of:\n    - language: [bul]\nseed:\n{seed}\n")
    return config


def test_pipeline_config_loads_seed_defaults() -> None:
    cfg = load_pipeline_config()
    assert cfg.seed.size == 10000
    assert cfg.seed.weight == "log_rank"


def test_pipeline_config_rejects_invalid_seed_weight(tmp_path: Path) -> None:
    config = _write_seed_config(tmp_path, "  weight: nonsense")
    with pytest.raises(ConfigError, match="invalid pipeline config"):
        load_pipeline_config(config)


def test_pipeline_config_rejects_non_positive_seed_size(tmp_path: Path) -> None:
    config = _write_seed_config(tmp_path, "  size: 0")
    with pytest.raises(ConfigError, match="invalid pipeline config"):
        load_pipeline_config(config)


def test_pipeline_config_rejects_unknown_authority_key(tmp_path: Path) -> None:
    config = _write_authority_config(tmp_path, "  max_ter: 5")  # typo of max_iter
    with pytest.raises(ConfigError, match="invalid pipeline config"):
        load_pipeline_config(config)


def test_pipeline_config_rejects_unknown_top_level_key(tmp_path: Path) -> None:
    config = tmp_path / "config.yml"
    config.write_text("scope:\n  all_of:\n    - language: [bul]\nnonsense_block:\n  x: 1\n")
    with pytest.raises(ConfigError, match="invalid pipeline config"):
        load_pipeline_config(config)


def test_resolve_seed_falls_back_to_config_defaults() -> None:
    resolved = resolve_seed(None, None, None)
    cfg = load_pipeline_config().seed
    assert (resolved.size, resolved.weight) == (cfg.size, cfg.weight)


def test_resolve_seed_applies_overrides() -> None:
    resolved = resolve_seed(None, 500, "sqrt_rank")
    assert (resolved.size, resolved.weight) == (500, "sqrt_rank")


@pytest.mark.parametrize(("size", "weight"), [(0, None), (None, "nonsense")])
def test_resolve_seed_rejects_invalid_overrides(size: int | None, weight: str | None) -> None:
    with pytest.raises(ConfigError, match="override"):
        resolve_seed(None, size, weight)
