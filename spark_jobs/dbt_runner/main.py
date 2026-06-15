"""Run dbt against the Spark (prod) target on Dataproc Serverless.

Submitted as a normal PySpark batch via ``infra/gcp/submit_job.sh`` against the
dedicated dbt image (``infra/gcp/Dockerfile.dbt``), which bakes the dbt project,
its resolved ``dbt_packages``, and the dbt-core/dbt-spark adapters. The entrypoint
creates a ``SparkSession`` and calls dbt programmatically; the dbt-spark ``session``
adapter reuses that session.

``register_spark_sources`` (on-run-start) and the marts read ``RAW_PATH`` /
``MARTS_PATH`` from the process env, and require ``RAW_PATH`` to be absolute. Dataproc
forwards job args but no driver env, so the paths arrive as CLI flags and are written
into ``os.environ`` here before dbt runs.

Vendor-neutral: no cloud SDK imports — GCP specifics live in ``infra/gcp/``.

Cost (last measured): ``dbt build --target prod`` against ``gs://openhrefs-data``
on crawl window ``cc-main-2026-mar-apr-may`` — PASS=41, mart 3.25 GiB / 118.76M
domains, ~13m38s, ~$0.40 (6.43 DCU-hr). Drift triggers an engineering note entry.

Submit (prod) — full build:
    DATAPROC_IMAGE=<dbt-image-tag> ./infra/gcp/submit_job.sh \
        spark_jobs/dbt_runner/main.py \
        --raw-path gs://openhrefs-data/raw \
        --marts-path gs://openhrefs-data/marts \
        --schema openhrefs

To narrow to the mart, pass its ancestors (a fresh Spark catalog has no upstream
views yet), i.e. ``--select +mart_domain_authority`` — never
``mart_domain_authority+`` (that selects children, of which there are none).
"""

from __future__ import annotations

import argparse
import os
import sys
from collections.abc import MutableMapping
from typing import Any, Protocol

import structlog
from pyspark.sql import SparkSession

from spark_jobs.common.errors import SparkJobError

log = structlog.get_logger()

_DEFAULT_PROJECT_DIR = "/opt/openhrefs/dbt"


class _DbtRunner(Protocol):
    def invoke(self, args: list[str], **kwargs: Any) -> Any: ...


def build_dbt_args(
    command: str,
    target: str,
    project_dir: str,
    profiles_dir: str,
    passthrough: list[str],
) -> list[str]:
    """Assemble the argv passed to ``dbtRunner().invoke()``."""
    return [
        command,
        "--target",
        target,
        "--project-dir",
        project_dir,
        "--profiles-dir",
        profiles_dir,
        *passthrough,
    ]


def apply_dbt_env(
    env: MutableMapping[str, str],
    raw_path: str | None,
    marts_path: str | None,
    schema: str | None,
) -> None:
    """Write path/schema values into ``env`` so dbt's ``env_var()`` resolves them.

    Only non-empty values are written; an unset CLI flag leaves any pre-existing
    process env untouched.
    """
    if raw_path:
        env["RAW_PATH"] = raw_path
    if marts_path:
        env["MARTS_PATH"] = marts_path
    if schema:
        env["SPARK_SCHEMA"] = schema


def require_prod_paths(env: MutableMapping[str, str], target: str) -> None:
    """For the Spark prod target, ``RAW_PATH`` and ``MARTS_PATH`` must be absolute.

    Mirrors ``register_spark_sources``: a relative or missing ``RAW_PATH`` silently
    reads the wrong location, so fail fast at submit instead of mid-run.
    """
    if target != "prod":
        return
    for name in ("RAW_PATH", "MARTS_PATH"):
        value = env.get(name)
        if not value:
            raise SparkJobError(f"{name} must be set for --target prod")
        if "://" not in value and not value.startswith("/"):
            raise SparkJobError(
                f"{name} must be an absolute path for --target prod (got: {value!r})"
            )


def invoke_dbt(dbt_args: list[str], runner: _DbtRunner | None = None) -> None:
    """Invoke dbt and raise ``SparkJobError`` on failure.

    ``runner`` is injectable for tests; in production a real ``dbtRunner`` is
    constructed lazily so the import is only required where dbt is installed.
    """
    if runner is None:
        from dbt.cli.main import dbtRunner

        runner = dbtRunner()
    result = runner.invoke(dbt_args)
    if not getattr(result, "success", False):
        exc = getattr(result, "exception", None)
        detail = f"{type(exc).__name__}: {exc}" if exc is not None else "see dbt logs"
        raise SparkJobError(f"dbt invocation failed: {detail}")


def _parse_args(argv: list[str] | None) -> tuple[argparse.Namespace, list[str]]:
    p = argparse.ArgumentParser(description="Run dbt against the Spark (prod) target.")
    p.add_argument("--command", default="build", help="dbt subcommand (default: build).")
    p.add_argument("--target", default="prod", help="dbt target (default: prod).")
    p.add_argument("--raw-path", default=os.environ.get("RAW_PATH"))
    p.add_argument("--marts-path", default=os.environ.get("MARTS_PATH"))
    p.add_argument("--schema", default=os.environ.get("SPARK_SCHEMA", "openhrefs"))
    p.add_argument("--project-dir", default=os.environ.get("DBT_PROJECT_DIR", _DEFAULT_PROJECT_DIR))
    p.add_argument(
        "--profiles-dir", default=os.environ.get("DBT_PROFILES_DIR", _DEFAULT_PROJECT_DIR)
    )
    # Unknown args (e.g. --select, --full-refresh) pass straight through to dbt.
    return p.parse_known_args(argv)


def main(argv: list[str] | None = None, runner: _DbtRunner | None = None) -> None:
    """Entrypoint: set env, validate, create a session, run dbt, stop the session."""
    args, passthrough = _parse_args(argv)
    apply_dbt_env(os.environ, args.raw_path, args.marts_path, args.schema)
    require_prod_paths(os.environ, args.target)
    dbt_args = build_dbt_args(
        args.command, args.target, args.project_dir, args.profiles_dir, passthrough
    )
    spark = SparkSession.builder.appName("openhrefs-dbt-runner").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        invoke_dbt(dbt_args, runner)
        log.info("dbt_runner_complete", command=args.command, target=args.target)
    finally:
        spark.stop()


if __name__ == "__main__":
    main(sys.argv[1:])
