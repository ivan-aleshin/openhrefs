"""Configuration loading: environment settings, storage paths, pipeline config.

Pure Python (YAML + pydantic) with no Spark dependency, so it is unit-testable
without a JVM. Config files are resolved relative to the repository root.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, Field, ValidationError
from pydantic_settings import BaseSettings, SettingsConfigDict

from spark_jobs.common.errors import ConfigError

_REPO_ROOT = Path(__file__).resolve().parents[2]
_DEFAULT_CONFIG = _REPO_ROOT / "config.yml"
_DEFAULT_STORAGE = _REPO_ROOT / "config" / "storage.yml"


class Settings(BaseSettings):
    """Environment-driven settings, read from the process env and ``.env``.

    ``OPENHREFS_ENV`` selects the storage block; ``RAW_PATH`` / ``MARTS_PATH``
    override the canonical output locations.
    """

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    openhrefs_env: str = "local"
    raw_path: str | None = None
    marts_path: str | None = None
    gcp_project: str | None = None
    hf_token: str | None = None


class StoragePaths(BaseModel):
    """Resolved storage locations for the active environment."""

    raw_path: str
    marts_path: str
    host_graph_path: str
    cdx_path: str
    wat_path: str


class ScopeParams(BaseModel):
    """Per-domain thresholds applied during scope filtering (SPEC.md §3)."""

    min_language_share: float = Field(default=0.30, ge=0.0, le=1.0)
    min_crawls_language: int = Field(default=2, ge=0)
    subdomain_handling: Literal["root_only", "include", "aggregate"] = "root_only"


class CrawlWindow(BaseModel):
    """Sliding scoring window configuration (SPEC.md §4)."""

    recent_count: int = Field(default=6, ge=1)
    historical_anchors: bool = True


class Languages(BaseModel):
    """Target languages the pipeline classifies and reports."""

    targets: list[str] = Field(default_factory=list)


class PipelineConfig(BaseModel):
    """Parsed ``config.yml``.

    The ``scope`` grammar is kept as a raw mapping; it is evaluated by Stage 1
    (language classification), not at load time.
    """

    scope: dict[str, Any]
    scope_params: ScopeParams = Field(default_factory=ScopeParams)
    crawl_window: CrawlWindow = Field(default_factory=CrawlWindow)
    languages: Languages = Field(default_factory=Languages)


def _read_yaml(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise ConfigError(f"config file not found: {path}")
    try:
        data = yaml.safe_load(path.read_text())
    except yaml.YAMLError as exc:
        raise ConfigError(f"failed to parse {path}: {exc}") from exc
    if not isinstance(data, dict):
        raise ConfigError(f"expected a mapping at top of {path}, got {type(data).__name__}")
    return data


def load_storage(
    settings: Settings | None = None,
    storage_file: Path = _DEFAULT_STORAGE,
) -> StoragePaths:
    """Resolve storage paths for the active environment, applying env overrides."""
    settings = settings or Settings()
    envs = _read_yaml(storage_file)
    env_block = envs.get(settings.openhrefs_env)
    if not isinstance(env_block, dict):
        raise ConfigError(
            f"unknown OPENHREFS_ENV '{settings.openhrefs_env}'; known: {sorted(envs)}"
        )
    try:
        paths = StoragePaths(**env_block)
    except ValidationError as exc:
        raise ConfigError(f"invalid storage block for '{settings.openhrefs_env}': {exc}") from exc
    if settings.raw_path:
        paths.raw_path = settings.raw_path
    if settings.marts_path:
        paths.marts_path = settings.marts_path
    return paths


def load_pipeline_config(config_file: Path = _DEFAULT_CONFIG) -> PipelineConfig:
    """Load and validate ``config.yml``."""
    try:
        return PipelineConfig(**_read_yaml(config_file))
    except ValidationError as exc:
        raise ConfigError(f"invalid pipeline config {config_file}: {exc}") from exc
