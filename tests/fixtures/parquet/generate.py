"""Generate tiny synthetic Parquet fixtures for dbt --target local / CI.

Writes one directory per raw dataset under ``tests/fixtures/parquet/``, matching
the RAW_PATH layout (``<RAW_PATH>/<dataset>/``) and the schemas in
``spark_jobs.common.schemas``. Uses DuckDB (the dbt local engine) so no Spark or
JVM is needed. Re-run after changing a fixture; commit the resulting .parquet.

    uv run python tests/fixtures/parquet/generate.py
"""

from __future__ import annotations

from pathlib import Path

import duckdb

_HERE = Path(__file__).resolve().parent

_DATASETS = {
    "cc_domain_pagerank": """
        SELECT
            CAST(domain AS VARCHAR)         AS domain
          , CAST(crawl AS VARCHAR)          AS crawl
          , CAST(pagerank_score AS DOUBLE)  AS pagerank_score
          , CAST(in_degree AS BIGINT)       AS in_degree
          , CAST(out_degree AS BIGINT)      AS out_degree
        FROM (VALUES
            ('example.com', 'CC-MAIN-2025-51', 0.0153, 1200, 340),
            ('example.org', 'CC-MAIN-2025-51', 0.0091,  640, 210),
            ('beispiel.de', 'CC-MAIN-2025-51', 0.0042,  180,  95),
            ('example.com', 'CC-MAIN-2025-45', 0.0148, 1150, 330),
            ('example.org', 'CC-MAIN-2025-45', 0.0088,  610, 200)
        ) AS t(domain, crawl, pagerank_score, in_degree, out_degree)
    """,
    "cc_domain_authority": """
        SELECT
            CAST(domain AS VARCHAR)        AS domain
          , CAST(crawl AS VARCHAR)         AS crawl
          , CAST(open_authority AS DOUBLE) AS open_authority
          , CAST(open_volume AS DOUBLE)    AS open_volume
        FROM (VALUES
            ('example.com', 'CC-MAIN-2025-51', 0.84, 7.09),
            ('example.org', 'CC-MAIN-2025-51', 0.61, 6.46),
            ('beispiel.de', 'CC-MAIN-2025-51', 0.33, 5.19),
            ('example.com', 'CC-MAIN-2025-45', 0.82, 7.05),
            ('example.org', 'CC-MAIN-2025-45', 0.59, 6.41)
        ) AS t(domain, crawl, open_authority, open_volume)
    """,
}


def main() -> None:
    con = duckdb.connect()
    for dataset, select in _DATASETS.items():
        out_dir = _HERE / dataset
        out_dir.mkdir(parents=True, exist_ok=True)
        target = out_dir / "data.parquet"
        con.execute(f"COPY ({select}) TO '{target}' (FORMAT parquet)")
        print(f"wrote {target}")


if __name__ == "__main__":
    main()
