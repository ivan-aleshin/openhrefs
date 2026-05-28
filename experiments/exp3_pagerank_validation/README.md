# Experiment 3 — PageRank validation + host/domain comparison

Gates Track A. Two outputs: (1) validate the PageRank engine against OpenPageRank;
(2) measure the host-vs-domain / internal-link effect across three graph
constructions (V1 host-internal, V2 host-external, V3 domain-collapsed) to choose
the Stage 2 construction consciously. OpenPageRank is a reference, not ground truth.
Full design: PLAN.md / DESIGN.md "Конструкция PageRank".

Stacked on `experiment/host-graph-pagerank` — reuses exp2's `analyze.py` PageRank loop.

## Status: scaffold (in progress)

Present:
- `domain_utils.py` — PSL host→registered-domain (vendored from composite-domain-rating;
  `tldextract` offline snapshot, pinned via uv.lock).
- `validate.py` — **local** metric profile (RBO, Kendall τ-b, bucketed Spearman, top-k
  Jaccard, divergence) over an overlap parquet. Runs on the laptop, not Dataproc
  (scipy/numpy native extensions don't package into Serverless `--py-files`).
- `graph_io.py` — shared Spark IO: vertices→domain map (PSL) and edges.
- `host_to_domain.py` — host ranks → domain ranks (pr_sum, pr_apex, n_hosts) via PSL, for V1/V2.
- `filter_edges.py` — V2: drop intra-domain host edges (keep cross-domain links only).
- `collapse_to_domain.py` — V3: collapse host graph → domain graph (drop self-loops),
  dense domain ids + id→domain map.
- `join_opr.py` — join a variant's domain ranks with OpenPageRank top-10M → overlap parquet
  (the input `validate.py` consumes).

The PageRank runs themselves reuse exp2's `analyze.py` (`--pagerank-only`, `--ranks-out`,
`--resume-from` for V2 warm-start). `_smoke_psl.py` is a cheap packaging check.

## Progress

Done:
- All jobs written; OpenPageRank top-10M staged to `gs://openhrefs-data/raw/refs/openpagerank/`
  (note: OPR rows are *hosts*, e.g. `www.facebook.com` — normalized to registered domain by
  the same PSL, then max per domain).
- Dataproc packaging validated by `_smoke_psl.py`: `tldextract` + its PSL snapshot load from the
  `--py-files` zip on executors; multi-level TLDs map correctly (`uk.co.bbc.foo`→`bbc.co.uk`).

Pending: run V1/V2/V3 on Dataproc, `validate.py` locally, record the V1/V2/V3-vs-OPR profile +
the host/domain divergence finding here.
