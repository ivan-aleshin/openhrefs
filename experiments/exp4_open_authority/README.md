# Exp 4 — open_authority seed sensitivity

Calibrates `open_authority` (personalized PageRank on the V3 domain-collapsed graph) — seed-set
size, weight formula, seed-cleaning, damping. Run as gated iterations (4.0 → 4.6); each gate must
pass before the next spends compute.

## Exp 4.0 — Graph Adoption Gate (2026-05-30): **ACCEPTED**

**Decision:** adopt CommonCrawl's **published domain-level graph** for `cc-main-2026-mar-apr-may` as
the V3 (domain-collapsed, PLD-via-PSL) construction. No host-graph rebuild or collapse needed for
this crawl; our own `collapse_to_domain.py` path (Exp 3) stays the validated fallback (unused).

**Graph:** `n_domains = 118,760,321` (matches CC's published 118.8M), dense 0-based ids
(`max_id = n-1`, `contiguous = True`) → `analyze.py`'s `spark.range(0, n)` covers all nodes.

**Checks** (`build_v3_map.py`, `gate_checks.py`; batches `exp40-v3map-20260530-185355`,
`exp40-gate-20260530-204725`, `exp40-gateinspect-20260530-210326`):

- **Boundary** `registered_domain(d) == d` on a deterministic ~1/1000 hash sample (119,102 domains):
  mismatch = 2 (**rate 0.0017 %**), null = 38 (0.03 %).
  - Both mismatches are **PSL-version drift on legitimate public suffixes**, not platform splits:
    `clawd.ia.br → ia.br`, `mzv.gov.cz → gov.cz`. CC's newer PSL snapshot treats `ia.br` / `gov.cz`
    as suffixes; our pinned (older) `tldextract` does not, so it over-collapses. CC is arguably more
    correct here.
  - The 38 nulls are malformed / edge-case CC nodes our normalizer rejects (underscores like
    `jim_fisheragilent.com` / `img_0133.mov`; ccTLD-direct hosts like `gt99.bd` / `wax.er` /
    `soffco.mm`; IDN punycode) — junk nodes, not a boundary divergence.
- **Platform canaries** (13 high-cardinality private-suffix platforms — `blogspot.com`,
  `wordpress.com`, `github.io`, `pages.dev`, `netlify.app`, `wixsite.com`, `weebly.com`,
  `webflow.io`, `herokuapp.com`, `vercel.app`, `appspot.com`, `firebaseapp.com`, `readthedocs.io`):
  all `self = 1`, `sub_nodes = 0` — **no private-PSL platform split**.
- **Seed coverage** (top-10K composite-DR consensus → v3_map): 9,940 / 10,000 = **99.40 %** (≥ 90 %).

**Verdict:** boundary mismatch ≈ 0 (no `*.platform` clusters), canaries clean, coverage 99.4 % ⇒
the CC domain graph is a faithful V3 realization for this crawl → **ACCEPT**.

**Cost:** v3_map build + gate checks ≈ negligible (< $1 — vertices only, no edges, no PageRank).

## Exp 4.1 — edges + global V3 PageRank (2026-05-30): **DONE**

Staged the domain edges and ran an unpersonalized (uniform-teleport) PageRank over the full V3 graph
to confirm the adopted graph runs end-to-end and converges before building the PPR engine (4.2).

- **Edges staged** on a transient GCE VM (CloudFront read + in-region GCS write = free; only VM
  uptime billed): the single 15.4 GiB domain-edges gzip is non-splittable, so `stage_edges.sh` splits
  it into 869 `edges_part_*.txt.gz` (~5M edges each) for parallel Spark reads → `_STAGED`; VM deleted.
  Single-threaded `gzip` dominated (~40 min); future runs could use `pigz`.
- **Convert** (batch `exp41-convert-20260530-224934`): gzip parts → Parquet at
  `gs://…/tmp/exp4/v3_edges_parquet` (157 files, `_SUCCESS`), so downstream reads are splittable.
- **Global PageRank** (batch `exp41-globalpr-20260530-230833`, `--n-vertices 118760321 --tol 0.001
  --max-iter 30`): **converged at iteration 10** (`L1 delta 7.55e-04 < 0.001`), monotone;
  **final total mass = 1.000000** (no dangling-mass leak). Output `gs://…/tmp/exp4/v3_ranks/`
  (`_SUCCESS`, 2000 files), wall-time ≈ 56 min.

**Cost:** convert + global PR ≈ **$1.9** (PR ≈ 28.8 DCU-hr × $0.06 ≈ $1.73 + shuffle ≈ $0.16),
under the ~$4.3 estimate — the 869-part split parallelised the read well.

**Edge-orientation gate** (batch `exp41-orient-20260531-042211`, `orientation_check.py`): convergence
+ mass = 1.0 don't prove `from_id`/`to_id` were read in the link direction, so check the top of the
ranking. Top-100 by global PageRank is the textbook shape of a correctly-oriented web graph —
`googleapis.com`, `google.com`, `facebook.com`, `googletagmanager.com`, `instagram.com`,
`cloudflare.com`, `gstatic.com`, `youtube.com`, … A swapped graph would surface outbound-heavy
domains (link aggregators / spam farms), not link *targets*. **Orientation correct** ⇒ `pagerank_score`
and the downstream PPR chain are valid.

Note: the top is dominated by ubiquitous infrastructure (CDN / analytics / widget domains embedded on
millions of pages via `<script>`/CDN references), i.e. global PageRank measures *ubiquity*, not
editorial authority. This is exactly why `open_authority` is **personalized** PageRank toward the
curated composite-DR seed — the teleport vector pulls mass to editorially-trusted domains instead of
`googleapis`/`gstatic`. The orientation result reinforces the PPR design (relevant to 4.2/4.3).

**Verdict:** the adopted V3 graph runs end-to-end, the PageRank engine converges with mass
conservation, and edge orientation is confirmed → 4.1 closed; next is the personalized-teleport (PPR)
engine in 4.2.

## Exp 4.3 — raw seed teleport vector + PPR pilot (2026-05-31): **DONE**

First personalized-PageRank run on a real seed, to confirm the seed→teleport→PPR chain works and to
read what raw `open_authority` looks like before any seed cleaning or calibration.

- **Seed vector** (`build_seed_vector.py`, batch `exp43-seedvec-20260531-055056`): top-10K consensus
  domains, `log_rank` weight → mapped onto v3_map. `seed_n=10,000 mapped_n=9,940 mapped_ratio=99.40%`
  (the 60 unmapped are off-graph, matching the 4.0 gate's seed coverage) → normalized teleport `(id, w)`.
- **PPR** (`analyze.py --teleport`, batch `exp43-oapr-20260531-055709`): **converged at iteration 8**
  (`L1 delta 7.4e-04 < 0.001`, faster than the global PR's iter 10 — a concentrated teleport converges
  quicker), **final mass = 1.000000**, ~47 min, ≈**$1.5** (23.4 DCU-hr). Output
  `gs://…/tmp/exp4/pilot/oa_ranks_raw_top10k_logrank`.

**Top-N reading (`orientation_check.py` on the OA ranks).** Raw top-10K/`log_rank` at `d=0.85`
resembles the global PR head — `googleapis`/`gstatic`/`cloudflare`/`jsdelivr` stay high because 85 % of
the mass still follows links and the top-10K seed *overlaps* the already-popular set. But the
personalization is visible in the relative moves: editorial/seed domains rise (`google.com` #2→#1,
`wikipedia.org` #42→#22) while non-editorial domains are demoted (`gmpg.org`, a WordPress-header
boilerplate domain, #9→#17; the parking/registrar domains `godaddy.com`/`afternic.com`/`hugedomains.com`
drop out of the head entirely). So the teleport *does* pull mass toward the seed — it just doesn't
dominate the head at `d=0.85` with a seed that already contains the popular set.

**Verdict:** the seed→PPR chain works and produces a sane, mass-conserving personalized ranking; raw OA
gives weak separation at the head. Levers for stronger separation (4.4/4.5): lower damping, seed
cleaning (the parking/boilerplate demotion is already visible — cleaning would sharpen it), and
analyzing the OA-minus-global delta rather than the absolute head. 4.4 quantifies this against a
trust-flow reference.
