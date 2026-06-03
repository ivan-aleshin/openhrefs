"""Configuration loading: environment settings, storage paths, pipeline config.

Pure Python (YAML + pydantic) with no Spark dependency, so it is unit-testable
without a JVM. Config files are resolved relative to the repository root.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError
from pydantic_settings import BaseSettings, SettingsConfigDict

from spark_jobs.common.errors import ConfigError

_REPO_ROOT = Path(__file__).resolve().parents[2]
_DEFAULT_CONFIG = _REPO_ROOT / "config.yml"
_DEFAULT_STORAGE = _REPO_ROOT / "config" / "storage.yml"


class _StrictModel(BaseModel):
    """Base for config models: reject unknown keys so a typo fails loudly, not silently."""

    model_config = ConfigDict(extra="forbid")


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


class StoragePaths(_StrictModel):
    """Resolved storage locations for the active environment."""

    raw_path: str
    marts_path: str
    host_graph_path: str
    cdx_path: str
    wat_path: str


class ScopeParams(_StrictModel):
    """Per-domain thresholds applied during scope filtering (SPEC.md §3)."""

    min_language_share: float = Field(default=0.30, ge=0.0, le=1.0)
    min_crawls_language: int = Field(default=2, ge=0)
    subdomain_handling: Literal["root_only", "include", "aggregate"] = "root_only"


class CrawlWindow(_StrictModel):
    """Sliding scoring window configuration (SPEC.md §4)."""

    recent_count: int = Field(default=6, ge=1)
    historical_anchors: bool = True


class Languages(_StrictModel):
    """Target languages the pipeline classifies and reports."""

    targets: list[str] = Field(default_factory=list)


class Authority(_StrictModel):
    """Calibrated PageRank / open_authority parameters (SPEC §5; Exp 4).

    Damping and convergence are algorithm calibration read by Stage 2/3; operational
    tuning (edge partitions, checkpoint cadence, TTL) stays a submission concern.
    """

    damping: float = Field(default=0.85, gt=0.0, lt=1.0)
    tol: float = Field(default=0.001, gt=0.0)
    max_iter: int = Field(default=30, ge=1)


class Seed(_StrictModel):
    """composite-DR seed-set parameters for Stage 3 personalized PageRank (SPEC §5).

    Production-validated mode is top-10K + ``log_rank`` + damping 0.85 (Exp 4); the other
    weights are deliberate override modes, not equally calibrated.
    """

    size: int = Field(default=10000, ge=1)
    weight: Literal["log_rank", "sqrt_rank", "uniform", "score"] = "log_rank"


class PipelineConfig(_StrictModel):
    """Parsed ``config.yml``.

    The ``scope`` grammar is kept as a raw mapping; it is evaluated by Stage 1
    (language classification), not at load time.
    """

    scope: dict[str, Any]
    scope_params: ScopeParams = Field(default_factory=ScopeParams)
    crawl_window: CrawlWindow = Field(default_factory=CrawlWindow)
    languages: Languages = Field(default_factory=Languages)
    authority: Authority = Field(default_factory=Authority)
    seed: Seed = Field(default_factory=Seed)


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


def resolve_authority(
    config_path: Path | None,
    damping: float | None,
    tol: float | None,
    max_iter: int | None,
) -> Authority:
    """Authority (PageRank iteration) params from config, with optional CLI overrides.

    Overrides pass the same ``Authority`` validation as config values, so an invalid
    override (e.g. ``max_iter=0``, which would skip iteration and emit the initial
    vector) raises ``ConfigError`` rather than silently degrading the run.
    """
    cfg = (load_pipeline_config(config_path) if config_path else load_pipeline_config()).authority
    try:
        return Authority(
            damping=damping if damping is not None else cfg.damping,
            tol=tol if tol is not None else cfg.tol,
            max_iter=max_iter if max_iter is not None else cfg.max_iter,
        )
    except ValidationError as exc:
        raise ConfigError(f"invalid authority parameter override: {exc}") from exc


def resolve_seed(
    config_path: Path | None,
    size: int | None,
    weight: str | None,
) -> Seed:
    """Seed-set params from config, with optional CLI overrides (validated as one unit).

    Kept separate from :func:`resolve_authority` so Stage 2 (PageRank iteration params
    only) never carries Stage 3 seed context.
    """
    cfg = (load_pipeline_config(config_path) if config_path else load_pipeline_config()).seed
    try:
        return Seed.model_validate(
            {
                "size": size if size is not None else cfg.size,
                "weight": weight if weight is not None else cfg.weight,
            }
        )
    except ValidationError as exc:
        raise ConfigError(f"invalid seed parameter override: {exc}") from exc


def checked_stage_output_path(path: str) -> str:
    """Reject writing a stage's output into the committed fixtures tree.

    The ``local`` storage ``raw_path`` is the committed ``tests/fixtures/parquet`` that
    dbt-local reads, so a config-derived default would overwrite fixtures; a local run
    must target a scratch/build location via an explicit output path.
    """
    if "tests/fixtures" in path:
        raise ConfigError(
            f"refusing to write stage output into committed fixtures: {path}; "
            "pass an explicit output path (e.g. a build/ or /tmp location)"
        )
    return path
