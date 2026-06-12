"""Unit tests for the dbt runner's pure helpers (no Spark, no dbt connection)."""

from __future__ import annotations

import pytest

from spark_jobs.common.errors import SparkJobError
from spark_jobs.dbt_runner import main


def test_build_dbt_args_defaults():
    args = main.build_dbt_args("build", "prod", "/opt/openhrefs/dbt", "/opt/openhrefs/dbt", [])
    assert args == [
        "build",
        "--target",
        "prod",
        "--project-dir",
        "/opt/openhrefs/dbt",
        "--profiles-dir",
        "/opt/openhrefs/dbt",
    ]


def test_build_dbt_args_appends_passthrough():
    args = main.build_dbt_args("build", "prod", "/p", "/p", ["--select", "+mart_domain_authority"])
    assert args[-2:] == ["--select", "+mart_domain_authority"]


def test_apply_dbt_env_sets_paths_and_schema():
    env: dict[str, str] = {}
    main.apply_dbt_env(env, raw_path="gs://b/raw", marts_path="gs://b/marts", schema="openhrefs")
    assert env["RAW_PATH"] == "gs://b/raw"
    assert env["MARTS_PATH"] == "gs://b/marts"
    assert env["SPARK_SCHEMA"] == "openhrefs"


def test_require_prod_paths_missing_raises():
    with pytest.raises(SparkJobError, match="RAW_PATH"):
        main.require_prod_paths({}, "prod")


def test_require_prod_paths_relative_raises():
    with pytest.raises(SparkJobError, match="absolute"):
        main.require_prod_paths({"RAW_PATH": "rel/raw", "MARTS_PATH": "gs://b/marts"}, "prod")


def test_require_prod_paths_absolute_ok():
    main.require_prod_paths({"RAW_PATH": "gs://b/raw", "MARTS_PATH": "/abs/marts"}, "prod")


def test_require_prod_paths_skips_non_prod():
    main.require_prod_paths({}, "local")  # no raise


def test_apply_dbt_env_none_does_not_overwrite():
    env: dict[str, str] = {"RAW_PATH": "existing"}
    main.apply_dbt_env(env, raw_path=None, marts_path=None, schema=None)
    assert env["RAW_PATH"] == "existing"
    assert "MARTS_PATH" not in env
    assert "SPARK_SCHEMA" not in env


def test_require_prod_paths_missing_marts_raises():
    with pytest.raises(SparkJobError, match="MARTS_PATH"):
        main.require_prod_paths({"RAW_PATH": "gs://b/raw"}, "prod")


class _FakeResult:
    def __init__(self, success: bool, exception: object | None = None):
        self.success = success
        self.exception = exception


class _FakeRunner:
    def __init__(self, result: _FakeResult):
        self._result = result
        self.invoked_with: list[str] | None = None

    def invoke(self, args: list[str]) -> _FakeResult:
        self.invoked_with = args
        return self._result


def test_invoke_dbt_success_passes_args_through():
    runner = _FakeRunner(_FakeResult(success=True))
    main.invoke_dbt(["build", "--target", "prod"], runner=runner)
    assert runner.invoked_with == ["build", "--target", "prod"]


def test_invoke_dbt_failure_raises():
    runner = _FakeRunner(_FakeResult(success=False, exception=RuntimeError("boom")))
    with pytest.raises(SparkJobError, match="dbt invocation failed"):
        main.invoke_dbt(["build"], runner=runner)
