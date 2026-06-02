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
resembles global PR at the head. Whether that's a defect is a 4.4 question, not a head-shape judgment:
the trust reference (`ref_trust`) — itself a seeded trust-propagation, the same family as personalized
PR — also ranks `googleapis`/`cloudflare` high, so OA tracking them may be *correct*. The right
validation is **OA vs `ref_trust`** (both seeded trust) and **global PR vs `ref_volume`** (both volume),
measured across the distribution. Only if that correlation is weak do the calibration levers apply
(lower damping, seed cleaning — the parking/boilerplate demotion is already visible — or the
OA-minus-global delta). 4.4 builds the reference and measures it.

## Exp 4.4 — trust-reference validation (2026-06-02): **PASS**

Sanity-checks raw `open_authority` against an external trust reference (`ref_trust`). The reference is
an **orientation point, not a target** — the goal is not to reproduce TF (that would make OA redundant)
and we do not calibrate toward it. The question is only: does OA's trust signal stay coherent with an
independent seeded trust-propagation across the distribution, or is it contradicting one (→ cleaning /
lower damping in 4.5)?

**Reference panel.** `build_ref_list.py` (`stratified_ref_list`, batch `exp44-reflist-20260531-090227`)
sampled a 100k-domain OA-stratified panel off the converged iter-8 pilot ranking — head + per-`log10(OA)`
bucket `sampleBy` + `crc32` hard cap — written to `/tmp/exp4_ref_list.txt` and handed to the user for the
one-shot external export. Built on the converged pilot OA (tol 0.001); the plan's tol-0 iter8/10/14
cap-calibration is skippable since the ranking already converged.

**Export finding (100k panel returned).** ~**99 % Found** — the external index *has* the domains, so this
is not a coverage gap — but **57 % carry a zero trust score** (found-but-zero-trust), and trust-positivity
falls steeply down OA strata: **head-20k 97.4 % → tail-20k 29.3 %**. A single rank correlation over a
57 %-tied distribution blends "does the domain *have* trust" with "how is trust *ranked*" and misleads.

**Harness (vendor-neutral, committed).**

- `normalize_ref.py` (`normalize_ref_domains`) — PSL-normalizes the returned domains via `tldextract`.
  Lives here (not in `seed_transforms.py`) so the seed path stays tldextract-free: only the `normalize_ref`
  submit carries the pydeps zip; `build_seed_vector` submits need none.
- `join_oa.py` — Spark glue joining normalized ref rows to the pilot OA ranks; carries `status` /
  `ref_domains` / `ext_backlinks` (status priority-agg `Found > MayExist > NotFound`) so the
  coverage-vs-trust split survives the join.
- `validate_oa.py` — rewritten as a **profile**, not a single number: counts (panel/positive/zero/null),
  `tau_b_all` (ties-robust), `spearman_pos` / `tau_b_pos` on the `ref_trust > 0` subset (ranking *among*
  trusted), and **binary separation** `auc_trust_positive` (Mann-Whitney `U/(n_pos·n_neg)`, no sklearn) +
  `positive_rate_by_decile`. Local pandas/scipy — already in the venv, no new dep.

**Metric naming.** Even "TF"/"CF" point at the vendor, so committed code uses `ref_trust` / `ref_volume`.
The vendor fetcher and raw JSONL archive (134 fields/item) stay in `_local/` (git-excluded).

**Gate run.** `normalize_ref` (local pyspark): 100,000 export rows → **99,960** registered domains
(40 null-trust/PSL-null dropped, 0 dupes; 57,240 zero-trust / 42,720 positive — confirms the export
finding). `join_oa` (batch `exp44-joinoa-20260602-084955`, ~5 min, <$1): OA ranks ⋈ v3_map ⋈ ref →
`oa_overlap` (99,960 rows; the whole panel maps, expected — the panel was sampled *from* the OA
ranking) + `oa_top10k_full` (full-ranking top-K for unbiased social-share). `validate_oa` (local):

| Metric | Value | Reading |
| --- | --- | --- |
| `auc_trust_positive` | **0.938** | OA cleanly separates `ref_trust>0` from `ref_trust==0` |
| `tau_b_all` (whole panel, ties-robust) | 0.654 | strong despite the 57 % zero-trust ties |
| `spearman_pos` (`ref_trust>0` subset) | 0.769 | OA orders the trusted set well vs `ref_trust` |
| `tau_b_pos` | 0.579 | same, Kendall |
| `positive_rate_by_decile` (top→bottom OA) | `.99 .96 .93 .70 .35 .12 .09 .04 .06 .04` | top-3 OA deciles 93–99 % trusted, steep monotone decline |

(`social_share` = 0 — no `--social-file` passed; an optional secondary metric, not part of the gate.)

**Verdict: PASS.** Raw OA (top-10K seed / `log_rank` / `d=0.85`) stays coherent with the trust
reference both in binary separation (AUC 0.94 — high OA ⇒ the domain *has* trust) and in ranking among
trusted domains (Spearman 0.77). This resolves the 4.3 "weak head separation" worry: the head's
resemblance to global PR is not a defect — trusted-inbound infra genuinely carries trust, matching the
OA↔TF / globalPR↔CF framing. Because the reference is an orientation point and not a target, the strong
agreement is a sanity check that OA isn't contradicting an established trust measure, not a claim that
OA reproduces TF. **Calibration (4.5) is not required** by these numbers; it stays available as an
option only if a later need to sharpen the head appears.

Caveat: the panel is OA-stratified by construction, so the metrics are conditional on that panel (even
OA coverage across buckets), not the natural domain distribution — which is the right validation set
(uniform OA coverage strengthens the separation read rather than inflating it).

## Exp 4.5 — head-sharpening probes (2026-06-02): **raw OA accepted, no calibration applied**

4.4 passed, but the OA head is dominated by ubiquitous infra/CDN (`googleapis`, `gstatic`,
`cloudflare`, `jsdelivr`, …). 4.5 asked whether an **automated** lever sharpens the head (demote
ubiquity, surface editorial) at acceptable cost. Manual seed curation is out of scope — automated
methods only. Three levers, all read against the d=0.85 raw-seed pilot:

- **Damping sweep** (`exp45-oapr-d075-…`, d=0.75; d=0.90 skipped). Trust metrics invariant
  (AUC 0.937 vs 0.938, Spearman 0.770 vs 0.769; decile curves identical) and head **Jaccard 0.969**
  vs d=0.85 — the top-20 is the same infra set, only cosmetic swaps. **Damping is not a head lever**:
  it's a global teleport/flow knob, blind to good-vs-bad nodes. Mid-tail even degrades slightly (lower
  d demotes editorial carried by link structure, promotes seed-proximity noise).
- **In-degree-fraction boilerplate detector** (`detect_boilerplate.py`, `exp45-boilerplate-…`,
  threshold 0.05 → **10 flagged**: `googleapis`, `facebook`, `google`, `instagram`, `googletagmanager`,
  `youtube`, `gmpg.org`, `gstatic`, `twitter`, `linkedin`). Output-denylist probe: **too blunt on both
  axes** — precision (removes `youtube`/`linkedin`/`facebook` — genuine authorities) and recall (leaves
  `cloudflare`/`jsdelivr`/`amazonaws`/`cloudfront` below threshold; the post-denylist head is still
  CDN/ad-tech). In-degree fraction can't separate "embedded by templates" from "genuinely popular
  destination" — both saturate. **Rejected.**
- **OA−global lift** (`delta_probe.py`, `exp45-deltaprobe-…`, `lift = log(oa_score / global_score)` on
  the OA top-100k). Raw lift correctly sinks infra to the bottom (oa≈global → lift≈0) but is
  denominator-unstable — low-support spam (`global≈0`) explodes the ratio and floods the head. **Gated
  to the OA top-1000 then sorted by lift**, the head becomes genuinely editorial (`miraheze`, `wowhead`,
  `thesun.co.uk`, `msnbc`, `tesla`, `proton.me`, `abcnews`, …) with infra gone. This is a real,
  automated, output-side lever — `lift` is essentially trust-over-ubiquity (OA↔TF, global↔CF; lift ≈
  TF/CF). The hard floor is crude (top-2000 readmits noise); the proper form is a smoothed denominator
  (`global + prior`) or an `oa_score × f(lift)` blend.

**Verdict: accept raw OA as-is; no head calibration applied.** Infra/boilerplate are **not** zero-trust
— they carry genuine trust authority, and the trust reference ranks them high too (4.4 OA↔TF), so a
head with trusted infra near the top is **correct**, not a defect. Damping and the in-degree denylist
don't help; the OA−global lift is a real but **optional** output-side refinement (a ubiquity-net
"editorial lift" view), deferred to the Stage 3 production-scoring design if a use case needs it — not
adopted into the base `open_authority` metric.

**Cost:** damping PPR ~$1.5 + detector ~$0.5 + delta join ~$0.5 (+ two small overlap/delta joins) ≈
**$3** total.

## Side-track (non-blocking) — open backlink-index insights from the reference panel

Opportunistic analysis riding on the one reference export (local, free; does not gate Exp 4). All
figures are **aggregates over the OA-stratified 100k panel** — no per-domain reference metric is
redistributed. By OA decile (top → bottom):

- **Authority tracks link volume across ~6 orders.** Median referring-domain count falls
  13,348 (top decile) → ~10 (tail); external backlinks 1.97M → ~13. Trust-positive rate 0.99 → 0.04.
- **No-follow share inverts with authority.** Top-decile inbound is mostly editorial dofollow
  (~0.11 nofollow) vs ~0.84–0.93 nofollow in the tail — a strong link-quality gradient.
- **EDU/GOV referring domains concentrate in the head only** (0.81 % of refdomains in the top decile,
  ~0 below the 6th) — a premium-trust signal aligned with OA.
- **External spam labels concentrate in low OA** (anti-spam-flag rate 0.001 → 0.012 down the deciles;
  443 flagged total, 0.4 %, median trust 0) — OA implicitly de-prioritizes flagged spam.

Correlations: **OA ↔ ref_trust 0.769**, **OA ↔ ref_volume 0.823** (OA, still 85 % link-flow, tracks
volume marginally harder than trust — nuances the OA↔trust framing). The global-PR ↔ ref_volume leg is
inconclusive here (0.262, n=24k) — global score was only joined for the OA top-100k, a restricted
sample, not the full panel; a full-panel join is the follow-up. Outlier disagreement is near-zero
(high-OA/low-trust = 1 CDN; high-trust/low-OA = 0), reinforcing 4.4.

Honest negatives / caveats: the subnet/ref-domain link-farm ratio barely separates flagged from clean
(0.471 vs 0.500) on this panel; topical/language fields are empty for the ~57 % zero-trust domains
(no reference data when trust is zero), so those slices are effectively on the trusted ~42k. The full
**CC-vs-commercial coverage gap** (our graph's in-degree vs the reference's RefDomains by stratum)
needs a cheap keystone Spark pass for per-domain CC in/out-degree — deferred to the decoupled insight
track, not Exp 4. Detail (vendor-schema) stays local-only.

## Exp 4.6 — summary: open_authority calibrated and validated

**Goal met.** `open_authority` is personalized PageRank on the V3 domain-collapsed graph, calibrated
and validated end-to-end for crawl `cc-main-2026-mar-apr-may`.

**Calibrated configuration** (production reads these from config — experiments document *why*, prod
code is written fresh in pipeline order, never copy-pasted):

| Parameter | Value | Set by |
| --- | --- | --- |
| Graph | CC published domain graph (PLD-via-PSL), 118.76M nodes / 4.3B edges | 4.0 gate ACCEPT |
| Seed | top-10K composite-DR consensus | 4.3 pilot, 4.4 validated |
| Seed weight | `log_rank` | 4.3 / 4.4 |
| Damping `d` | 0.85 | 4.1 (global), 4.5 (sweep — no better) |
| Teleport / dangling | personalized teleport, dangling mass routed onto seed (not uniform) | 4.2 engine |

**Validation.** 4.4 trust-reference gate **PASS**: `auc_trust_positive` 0.938 (OA separates
trusted from zero-trust), `spearman_pos` 0.769 (ranks the trusted set), across a 57 %-zero-trust panel.
The side-track adds convergent evidence (volume/nofollow/EDU-GOV/anti-spam all gradient cleanly with
OA). 4.5 confirmed no automated head calibration is needed — infra/boilerplate carry genuine trust
(OA↔TF), so a trusted-infra head is correct.

**Deferred (not part of the base metric).** The OA−global *lift* (trust-over-ubiquity, ≈ TF/CF) is a
real optional output-side ranking for a future "editorial authority" view → Stage 3 production-scoring
design. The CC-vs-commercial coverage study and the topical/link-type/anti-spam shorts ride the same
reference export on a decoupled insight track.

**Total Exp 4 cost** ≈ **$10–11** (4.0 ~$1; 4.1 convert+global PR ~$1.9; 4.3 pilot ~$1.5; 4.4 ref
list + join ~$1; 4.5 damping + detector + delta ~$3; misc joins) — well under the Phase-0 ~$5 + pilot
budget envelope.
