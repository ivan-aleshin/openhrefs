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
