# Exp 6 — Stage 4 (S4-0) decision experiment

Decides Stage 4 cloud path (AWS in-region vs STS->GCS) and global-vs-scoped output by
measuring a uniform-random WAT slice ladder on `CC-MAIN-2026-21`.

This experiment may supersede the current Stage 4 assumptions in `SPEC.md`; final contract
changes are recorded after the measurement decision, before S4-1.

## Running

Run the scripts from the repo root as modules so the `experiments` package resolves:

```bash
uv run python -m experiments.exp6_stage4_measurement.run_ladder fit \
  --points '[[200,..],[1000,..],[3000,..]]'
```

The Dataproc submit path ships the package via `DATAPROC_EXTRA_PKGS=experiments/` instead.

## Method
- Ladder sizes: 200 / 1000 / 3000 (3000 required for the full-pass gate), nested uniform-random
  over the WAT manifest,
  fixed seed, same file list reused across clouds when the AWS arm is run.
- Global domain-grain extraction with the dedup lever (host-dedup domain resolution).
- STS->GCS arm measured; AWS in-region modeled from price lists (run only if non-obvious).

## Slice ladder results
| size | wall-clock | compute (DCU-h / $) | transfer/stage | input GiB | raw rows | global pairs | scoped pairs / links (bul/ron) | distinct url_from hosts | distinct url_to hosts | domain-grain GiB | url-sample GiB | parse/null errors |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 200 | 29 min | 13.83 / ~$0.8–1.0 | (shared STS) | ~31 | 0.61B | 22,371,464 | 67,761 / 4.32M | 2,301,015 | 7,242,043 | 0.51 | 0.07 | url_to fail ~42%, ~69% records no-links |
| 1000 | 3h04m | 95.66 / ~$5.7–7.2 | (shared STS) | ~157 | 3.04B | 74,664,394 | 171,035 / 21.5M | 6,637,436 | 16,994,720 | 1.74 | 0.30 | url_to fail ~42%, ~69% no-links |
| 3000 | 9h18m | 293.97 / ~$17.6–22.0 | STS S3→GCS ~470 GiB, server-side, ~negligible (STANDARD ≤48h) | ~470 | 9.10B | 153,449,373 | 277,202 / 64.3M | 11,742,838 | 28,574,562 | 3.50 | 0.81 | url_to fail ~42%, ~69% no-links |

The ladder is nested, so a single 3000 STS stage (~470 GiB) covers all three runs; transfer
cost is server-side S3→GCS and effectively negligible. Ladder compute total ≈ 403 DCU-h
(≈ $24–30). All three runs SUCCEEDED; `files_unreadable = 0`, `malformed_records = 0`.

Full `metrics` JSON keys emitted per run (`main.py`): `n_wat_files`, `raw_link_rows`,
`global_domain_pairs`, `distinct_url_from_hosts`, `distinct_url_to_hosts`,
`distinct_domain_from`, `distinct_domain_to`, `distinct_registered_domains`, `total_rows`,
`url_from_parse_fail`, `url_to_parse_fail`, `domain_from_null`, `domain_to_null`,
`global_pairs`, `global_links`, `scoped_pairs`, `scoped_links`, `top_domain_from/to/pair`,
`files_attempted`, `files_unreadable`, `records_seen`, `malformed_records`,
`records_with_no_links`. Output file count / avg size / partition skew + compressed bytes are
read from `gsutil ls -l` on the written Parquet (operator).

Accumulator note: `files_attempted` / `files_unreadable` / `records_seen` /
`malformed_records` / `records_with_no_links` are populated by a single cached WAT-parse pass
(`links` is cached before the host-dedup resolution), so they are exact except for rare Spark
task retries / speculation, which can still slightly over-count.

3000 is mandatory to pass the full-pass gate (two points fit a line trivially). The ladder is
nested (200 ⊂ 1000 ⊂ 3000), so staging `manifest_3000.csv` once covers all three runs.

## Fit + extrapolation
- Cost is **linear** in WAT-file count: slope **0.0999 DCU-h/file**, intercept −5.34,
  **r² = 0.99995**; wall-clock ~0.187 min/file. (The early 200→1000 jump looked superlinear
  but was a two-point artifact of fixed small-slice overhead — the third point resolves it.)
- Full-crawl (100k files) extrapolation: domain-grain ~103 GiB; compute ~9,982 DCU-h
  (~$600–750) with the diagnostic harness, ~7,500 DCU-h (~$450–565) for a thin
  production-like path; staged input ~15.7 TB.
- Wall-clock at the current 32-vCPU quota (~8 executors) extrapolates to ~312 h (~13 days)
  as a single batch — quota-independent in cost but infeasible to run as one job; needs a
  quota raise, crawl chunking, or AWS in-region.
- gate decision: **PASS** (≥3 points, r² ≥ 0.95) → S4-0b go/no-go unlocked; if run, use thin
  mode and an operator-decided cloud path.

## Decisions / follow-up
- Cloud: STS→GCS arm **measured**; AWS in-region modeled. Path for the full pass deferred
  to the operator on credit balance — GCP (raise CPUS_ALL_REGIONS + Dataproc on staged WAT)
  vs AWS in-region (no per-region vCPU quota, direct s3://commoncrawl).
- Global-vs-scoped: the WAT **read + parse** is global regardless (scope = 0.18% of pairs
  but zero source-side selectivity, so the full WAT pass is mandatory either way). A
  scoped-only pipeline could trim some post-parse aggregation/output, but the full pass
  writes **global** domain-grain on product/contract grounds — the global backlink index is
  the deliverable; scope narrows downstream stages only.
- Required before S4-1: update `SPEC.md` Stage 4 assumptions and `config/storage.yml`
  input semantics if the measurement invalidates the current contract.

## Actual cost
- Dataproc ladder (200 + 1000 + 3000): **403.46 DCU-h ≈ $24–30** (13.83 + 95.66 + 293.97).
- STS staging: one S3→GCS transfer of the nested 3000 set, **~470 GiB**, server-side,
  effectively negligible (STANDARD, ≤48h lifecycle).
- AWS/EMR arm: **not run — modeled only** (in-region s3://commoncrawl, $0 egress).
- Full single-crawl pass (S4-0b): not yet run; extrapolated ~$450–565 (thin) — see
  Fit + extrapolation.
