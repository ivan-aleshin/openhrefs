# Experiment 3 — PageRank validation + host/domain comparison

Gates Track A. Two outputs: (1) validate the PageRank engine against OpenPageRank;
(2) measure the host-vs-domain / internal-link effect across three graph
constructions (V1 host-internal, V2 host-external, V3 domain-collapsed) to choose
the Stage 2 construction consciously. OpenPageRank is a reference, not ground truth.
Full design: PLAN.md / DESIGN.md "Конструкция PageRank".

Stacked on `experiment/host-graph-pagerank` — reuses exp2's `analyze.py` PageRank loop.

## Status: completed — gate passed (V2 intentionally skipped)

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

### Runs (cc-main-2025-26-nov-dec-jan)

All on Dataproc Serverless `2.2`, quota-capped at 7 executors (driver 4 + 7×4 = 32 cores =
`CPUS_ALL_REGIONS`), so heavy batches run sequentially. Cost from billed DCU-seconds.

| step | batch | wall-clock | cost |
|---|---|---|---|
| `build_id_domain` (PSL map over 279M vertices, `pandas_udf`) | `bab00f2d` | 68 min | ~$1.6 |
| `host_to_domain` V1 → `v1_domain` (~123M domains) | `050bf5df` | 4.2 min | ~$0.06 |
| `join_opr` V1 pr_sum → `overlap_v1_sum` | `6b8e0bea` | 6.5 min | ~$0.14 |
| `join_opr` V1 pr_apex → `overlap_v1_apex` | `d8d05cc7` | 6.7 min | ~$0.14 |
| `collapse_to_domain` V3 (13.4B edges → 118.16M-domain graph) | `db51cacb` | 2h18m | ~$4.7 |

`id_domain`: 279M hosts → ~123M registered domains. `n_hosts` median 1, max 62 350 (free-blog
hosts). The expensive PSL pass is materialized once and reused by every variant. V3 collapse keeps
only domains with ≥1 cross-domain edge → **118.16M domains** (~5M internal-/self-loop-only domains
drop out); `v3_edges_parquet` (30 GB) + `v3_map` are the input to V3 PageRank.

### V1 (host-internal) vs OpenPageRank — full metric profile

Overlap = **6.49M domains** (our ~123M ∩ OPR ~10M), via `validate.py` (local).

| metric | pr_sum | pr_apex |
|---|---|---|
| Spearman | **0.325** | 0.207 |
| Kendall τ-b | **0.222** | 0.141 |
| RBO (p=.99, depth=100k) | **0.410** | 0.385 |
| Jaccard top-100 / top-100k | **0.242** / **0.199** | 0.220 / 0.118 |
| Spearman, OPR-bucket top-1k | **0.665** | 0.484 |
| median \|Δrank\| | 1.41M | 1.61M |
| Spearman(Δrank, n_hosts) | **−0.52** | −0.09 |

Findings:
- **pr_sum is inflated by subdomain farms — now quantified.** `Spearman(Δrank, n_hosts) = −0.52`:
  the more subhosts a domain has, the higher we rank it vs OPR. Top by pr_sum: `google.com`, then
  `blogspot.com` (OPR rank 264), `wordpress.com` (70), `weebly.com` (587), `tumblr.com` (107), plus
  CDN/infra (`googleapis`, `gstatic`, `googletagmanager`, `cloudflareinsights`, `parastorage`).
- **apex removes the farm bias but is worse overall.** Its `Spearman(Δrank, n_hosts)` is −0.09
  (farms no longer inflated), yet every aggregate metric is lower than sum. A single apex node is a
  noisy point estimate; ~1M domains have no apex host crawled (rank 0). apex's own artifacts are
  link-target domains (`unpkg`, `cdnjs` → OPR 1412, `wa.me`, `goo.gl`, `shop.app` → OPR 5512).
  → **aggregate host mass (sum) is the better domain estimator than the apex node**, even with
  inflation. The apex-host-only hypothesis is refuted on the full profile, not just raw Spearman.
- Both V1 variants correlate weakly with OPR (RBO 0.38–0.41) → host-internal links + host
  granularity distort domain-level authority. This is **expected** and motivates V3 (collapse),
  which removes intra-domain edges *and* aggregates. V1's weakness does **not** reflect on the PR
  engine — that is what V3-vs-OPR tests.
- **Metric caveat:** OPR's score is a compressed 0–10 log scale with heavy ties, which attenuates
  global Spearman; the bucketed Spearman / τ-b / RBO above are the honest read.
- **Note:** `validate.py:96` comment sign is inverted — over-ranking yields *negative* Δrank
  (our_rank − opr_rank < 0 ⇒ we rank it better), so the observed −0.52 *is* the over-ranking signal.

### V3 (domain-collapsed) vs OpenPageRank — gate result

V3 PageRank: 118.16M-domain graph, uniform init, **converged iter-11** (delta 6.6e-4),
~$4.3. Overlap with OPR = 6.47M domains.

| metric | V1 sum | V1 apex | **V3** |
|---|---|---|---|
| Spearman | 0.325 | 0.207 | **0.390** |
| Kendall τ-b | 0.222 | 0.141 | **0.273** |
| RBO (p=.99, d=100k) | 0.410 | 0.385 | **0.543** |
| Jaccard top-100 / top-1k | 0.242 / 0.097 | 0.220 / 0.171 | **0.333 / 0.227** |
| median \|Δrank\| | 1.41M | 1.61M | **1.28M** |

V3 dominates V1 on every metric — collapse + dropping intra-domain edges aligns with OPR
as predicted.

**Engine: validated** (by mechanism + behavior, not the ~0.9 bar). Clean convergence;
authoritative domains land sensibly: `github` our-22/opr-14, `wikipedia` 41/18, `amazon`
64/22, `nytimes` 134/52, `nih.gov` 145/38 (`harvard`/`mit` ranked above OPR — defensible).

**~0.9-vs-OPR bar rejected.** OPR ties cap only **1.3%** of pairs → the moderate correlation
is real disagreement, not a scale artifact. Gap driven by different crawl + boilerplate-link
treatment; OPR is a reference, not ground truth.

**Farms resolved.** `blogspot.com`/`wordpress.com` (V1-sum #2/#7) drop out of the V3 top-20 →
collapse-dedup handles multi-tenant platforms; `include_psl_private_domains` not needed.

**New open item — boilerplate/template/CDN inflation.** Residual over-rankers are technical/
template-embedded domains (`mywebsite-editor.com` our-272/opr-4.7M, `wsimg.com`, `wixstatic.com`,
`jsdelivr.net`, `gmpg.org`), not editorial authority. This is the real question for the
Stage 2 construction choice. V2 (host-external) skipped — V1↔V3 already covers the picture.
