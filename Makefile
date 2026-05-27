.PHONY: check lint format fix typecheck sql dbt-parse dbt-build-local test run-spark

# Single gate before committing: lint + type-check + sql + dbt parse + tests.
# Steps over not-yet-created directories (spark_jobs/, dbt/) skip cleanly so
# `make check` is green on an empty project and gains coverage as code lands.
check: lint typecheck sql dbt-parse test

lint:
	uv run ruff check .
	uv run ruff format --check .

format:
	uv run ruff format .

fix:
	uv run ruff check --fix .

typecheck:
	@if [ -d spark_jobs ]; then \
		uv run mypy spark_jobs/; \
	else \
		echo "skip mypy: no spark_jobs/ yet"; \
	fi

sql:
	@if [ -d dbt/models ]; then \
		uv run sqlfluff lint dbt/models/; \
	else \
		echo "skip sqlfluff: no dbt/models/ yet"; \
	fi

dbt-parse:
	@if [ -d dbt ]; then \
		cd dbt && uv run dbt deps --quiet && uv run dbt parse --target local && uv run dbt parse --target prod; \
	else \
		echo "skip dbt parse: no dbt/ yet"; \
	fi

dbt-build-local:
	cd dbt && uv run dbt deps && uv run dbt build --target local

# Run a Spark job locally. PYTHONPATH=. is required because spark_jobs/ is not
# installed as a package (package=false in pyproject.toml).
# Usage: make run-spark JOB=spark_jobs/hello/main.py ARGS="--cdx-path /tmp/sample.gz"
run-spark:
	PYTHONPATH=. uv run python $(JOB) $(ARGS)

test:
	@if find tests -name 'test_*.py' 2>/dev/null | grep -q .; then \
		uv run pytest tests/; \
	else \
		echo "skip pytest: no test files yet"; \
	fi
