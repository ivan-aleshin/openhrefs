# Experiment 5 — WAT Extraction

Gates Track B / Phase 4 (Stage 4 backlinks). Validates the Stage 4 WAT-filtering
hypothesis on crawl `CC-MAIN-2026-21`: `target domains → inbound source domains →
cc-index segment map` narrows the WAT read while recovering an acceptable share of
backlink edges. Design: `docs/superpowers/specs/2026-06-13-exp5-wat-extraction-design.md`.

Runtime deps for the final experiment: `warcio` (WAT/WARC record iteration; added now)
plus `fsspec` + `s3fs` (anonymous streaming of WAT `.gz` from `s3://commoncrawl`; added
later, in the I/O task). `s3fs`/`fsspec` are needed **only** for Exp 5's WAT `.gz`
streaming — cc-index Parquet uses the Hadoop S3A connector, and the rest of the pipeline
does not stream s3 from Python. These are experiment dependencies, **not** a production
commitment. If WAT parsing later moves into production Stage 4, record them as new runtime
dependencies in `docs/engineering_notes.md` per the dependency-discipline rule.

## Data slice
- cc-index columnar, crawl `CC-MAIN-2026-21`, columns `(url_host_registered_domain,
  content_languages, fetch_status, warc_filename)`, `fetch_status=200`.
- Staged domain webgraph `cc-main-2026-mar-apr-may-domain`.
- WAT subset selected by the segment-list job (mode chosen at the checkpoint).

## What was measured
_(filled after the run)_

## Results
_(filled after the run)_

## Decision-informing conclusions for Stage 4
_(filled after the run)_

## Limitations
- Crawl alignment is approximate: cc-index/WAT are single-crawl (`CC-MAIN-2026-21`),
  the graph is a multi-crawl window. Recall vs Host Graph is approximate.
- `S` is a cc-index-derived proxy for the future Stage 1 scope (primary-language hit),
  not the full Stage 1 resolution.
- `file-sample` mode yields no valid recall (page-coverage lower bound only).

## Actual cost
_(Dataproc DCU-hr and $ per job, filled after the run)_
