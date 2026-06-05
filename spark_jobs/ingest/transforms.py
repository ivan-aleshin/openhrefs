"""Pure DataFrame transforms for the graph ingest stage.

Builds the canonical staged V3 graph inputs (v3_map, v3_edges) from the CommonCrawl
published domain graph. No I/O: reads/writes live in ``io.py`` / ``main.py``.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def to_v3_map(vertices: DataFrame) -> DataFrame:
    """CC domain vertices ``(id, rev_domain, num_hosts)`` → ``(id, domain)``.

    Un-reverses the sort-locality form (``com.example`` → ``example.com``). CC already
    aggregated host→registered-domain via PSL, so the vertex *is* the registered domain;
    this only restores label order. ``num_hosts`` is dropped.
    """
    return vertices.select(
        "id",
        F.concat_ws(".", F.reverse(F.split("rev_domain", "\\."))).alias("domain"),
    )
