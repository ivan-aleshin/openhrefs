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
| size | wall-clock | compute $ | transfer/stage $ | input GiB | raw rows | global pairs | scoped_pairs / scoped_links (bul/ron) | distinct url_from hosts | distinct url_to hosts | domain-grain bytes | url-sample bytes/row | parse/null errors |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 200 | [ ] | [ ] | [ ] | [ ] | [ ] | [ ] | [ ] | [ ] | [ ] | [ ] | [ ] | [ ] |
| 1000 | [ ] | ... | | | | | | | | | | |
| 3000 | [ ] | ... | | | | | | | | | | |

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
- slope / r2 / ci95 per `run_ladder.py fit`: [ ]
- full-crawl (~100k files) extrapolation: domain-grain size [ ], compute $ [ ], staged input [ ].
- gate decision: [ pass -> S4-0b full pass on <cloud> | fail -> extrapolation-only, full measurement deferred ].

## Decisions / follow-up
- Cloud: [ measured | modeled ] -> [ AWS in-region | STS->GCS ].
- Global-vs-scoped: [ ].
- Required before S4-1: update `SPEC.md` Stage 4 assumptions and `config/storage.yml`
  input semantics if the measurement invalidates the current contract.

## Actual cost
- STS/Dataproc: [ ]  (+ AWS/EMR if run: [ ]).
