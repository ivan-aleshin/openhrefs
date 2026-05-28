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

Deferred — next step (Task 3, Spark jobs on Dataproc):
- `host_to_domain.py` — host ranks → domain ranks (sum + apex) via PSL, for V1/V2.
- `join_opr.py` — join our domain ranks with OpenPageRank top-10M → overlap parquet
  (the input `validate.py` consumes).
- V3: collapse host edges → domain graph (drop self-loops) → PageRank.
- V2: drop intra-domain edges → PageRank warm-started from V1 (`--resume-from`).
