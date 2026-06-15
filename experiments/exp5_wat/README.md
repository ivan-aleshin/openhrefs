# Experiment 5 — WAT Extraction

Gates Track B / Phase 4 (Stage 4 backlinks). Set out to validate the Stage 4 WAT-filtering
hypothesis on crawl `CC-MAIN-2026-21`: `target domains → inbound source domains →
cc-index segment map` narrows the WAT read while recovering an acceptable share of
backlink edges.

**Headline: the narrowing hypothesis failed for the tested scope.** For the bul/ron scope — and
random non-trivial target-subset samples within it, down to 10 target domains — source-domain
WAT-filtering gives **zero selectivity** (a full WAT pass is needed). The shard-coverage mechanism
(below) strongly suggests this generalizes, but it was **not** tested adversarially (no
deliberately-narrow or low-inbound target sets, no direct shard-coverage instrumentation), so the
universal claim is a hypothesis, not a measured result. The experiment also produced a measured
cost model and the dominant optimization lever.

Runtime deps: `warcio` + `fsspec`/`s3fs` (WAT `.gz` streaming, anonymous), plus `gcsfs`
(staged-GCS WAT) and `fastwarc`/`orjson` (parser A/B). Experiment deps, **not** a production
commitment; record them as production deps (per the dependency-discipline rule) if Stage 4 adopts them.

## How it was actually run (method)

Dataproc Serverless **cannot reach `s3://commoncrawl`** — its default subnet has Private
Google Access but **no Cloud NAT**, so no internet egress (first s3a read failed with
`SocketTimeoutException`; default s3a retries stalled it ~45 min before fail-fast props were
added). Rather than provision NAT (recurring `$0.045/GB` egress tax), the pipeline ran via
**transient GCS-staging**: Storage Transfer Service (server-side) copies cc-index / WAT
`s3://commoncrawl → gs://`, Dataproc reads `gs://` (free in-region), data deleted after.

Jobs (all on staged `gs://` inputs): `project_cdx` (cc-index → projection) → `derive_segments`
(projection + staged id-graph → target_domains / expected_edges / source_wat_files / segment_list
/ stats) → `parse_wat` (WAT → backlinks + metrics), plus throwaway benchmarks for the selectivity
curve, the domain-extraction optimization, and a cProfile run.

## Data slice
- cc-index columnar, crawl `CC-MAIN-2026-21`, columns `(url_host_registered_domain,
  content_languages, fetch_status, warc_filename)`, `fetch_status=200` (300 parquet files,
  163 GiB).
- Staged id-based domain webgraph `cc-main-2026-mar-apr-may-domain` (parquet `v3_edges` 19.45 GiB
  / `v3_map` 1.39 GiB; 118.76M domains, 4.3B edges).
- WAT: crawl has 100,000 files (~160 MiB each, ~16 TB). Subsets staged per measurement.
- Scope `S` = bul/ron, language-qualified from cc-index (`min_language_share=0.25`).

## What was measured & results (with provenance)

### 1. Selectivity — REFUTED (the headline)
`derive_segments` stats (`segment_list_stats`): 247,744 target domains (179,936 meeting share),
**27,559,940 inbound source domains**, 101,506,393 expected edges, **wat_files = 100,000 = ALL**,
**crawl_fraction = 1.0**.

Selectivity curve (throwaway job over `expected_edges` + `source_wat_files`, random target samples):

| target domains | inbound source domains | WAT files needed | crawl_fraction |
|---|---|---|---|
| 10 | 65 | 100,000 | 1.0 |
| 100 | 3,101 | 100,000 | 1.0 |
| 1,000 | 123,925 | 100,000 | 1.0 |
| 10,000 | 4,025,238 | 100,000 | 1.0 |
| 100,000 | 19,658,774 | 100,000 | 1.0 |

**Even 10 sampled target domains → 65 sources → still ALL 100k WAT files.** WAT is sharded by
source-page URL uniformly; a file is skippable only if NONE of your sources has a page in it.
Backlinks to a non-trivial target tend to come from high-page-count domains (social/search/
directories/CDN) whose pages blanket every shard — one or two among the sources cover the whole
crawl. So across every sampled target subset in this scope, no narrow set escaped a full-WAT read;
the only "lever" (drop big-domain sources) would discard the most valuable backlinks. **The
mechanism is intrinsic to CommonCrawl — WAT is indexed by source, backlinks filter by target,
unknowable without reading the record — which is why we expect this to hold beyond the tested
scope, though that was not measured.**

### 2. Conditional recall — 12.5% (heavily caveated)
`parse_wat` domain-targeted run (30 niche source domains, 1–2 WAT files each, 39 files / 5.7 GiB;
all their files parsed → full this-crawl coverage): expected 18,216 → found 2,284 = **12.5%**.
This is NOT extraction failure: `expected_edges` is the **3-crawl** graph, the WAT is a **single**
crawl, so one crawl captures ~1/8 of the edges three crawls accumulate. **Takeaway: backlink
completeness needs WAT accumulated across crawls over time (like Ahrefs/Majestic), not one crawl.**

### 3. Anchor / rel quality — measured on a representative random sample
`parse_wat` throughput run (1,000 random WAT files, 203,037,962 backlink rows into S):
- **empty anchor 18.3%** (37.1M / 203M) — anchor text present ~82% of the time on random links.
- nofollow 7.5%, ugc 0.22%, sponsored 0.08%.
- **wat_only_edges = 88,789** — domain edges found in WAT that are ABSENT from the 3-crawl graph
  (WAT surfaces anchor-level links the domain graph missed).

(The niche-source run gave 0.08% empty anchors / 10.6% ugc — biased by those specific sources;
the random sample above is the representative figure.)

### 4. Cost / throughput — measured, with the right attribution
Per-crawl WAT processing is **compute-bound, and the compute is dominated by domain extraction
(`tldextract`), NOT the WARC parser nor serialization.**

- **Naive warcio full run** (1,000 files / 146 GiB, 5h54m, **186.2 DCU-hr ≈ $11.2**). Two-point
  fit with a 39-file run → ~1.30 DCU-hr/GiB; naive full 16 TB ≈ ~$1,270/crawl. **This number is
  contaminated** — the full `parse_wat` also runs heavy downstream metrics (distinct/subtract/agg
  over 200M+ rows), parser-independent, so it is not clean parse throughput.
- **Parser A/B (warcio vs fastwarc)** abandoned: full-pipeline runs are dominated by the
  parser-independent downstream + run-to-run autoscale noise — a poor parse benchmark.
- **Domain-extraction benchmark** (isolated, on the existing 203M-row sample, $1.05): per-row
  `tldextract` UDF **2191.5s / 203M calls** vs **dedup** (native `parse_url(HOST)` → distinct hosts
  → UDF on distinct → join) **113.3s / 1.08M calls** = **~19.3x faster, 188.8x fewer calls**, result
  identical (108,428 vs 108,345 distinct domains, 0.08% drift = `parse_url` vs our URL parser).
- **cProfile** (tight RDD run, `spark.python.profile`): the domain cost is **tldextract's own CPU**
  (`_extract_netloc`/`suffix_index`/punycode/`extract_str`) — for 5M url_to, `registered_domain_of_url`
  cumtime 1376s ≈ all tldextract; **serialization (`_struct`/`serializers`/`pickle`) ≈ 0** (PySpark
  batches it). Parse cProfile (30.4M links): domain_from tldextract **~38%**, `extract_links` ~17%,
  orjson ~9%, **fastwarc read ~1%** — confirming the WARC parser was never the bottleneck.

Storage of the output (URL-grain backlink Parquet): ~**26 bytes/row** compressed
(`backlinks_sample` 4.88 GiB / 203M rows). A global URL-grain index ≈ ~few TB/crawl; domain-grain
≈ ~hundreds of GB/crawl (see conclusions).

## Decision-informing conclusions for Stage 4

1. **Full WAT pass is needed at the tested scope** (selectivity 1.0 across all sampled target-subset
   sizes for bul/ron; expected but not proven to generalize). The SPEC §5 "no full WAT scan" premise
   does not hold here; revisiting the contract needs the generalization confirmed (see open items).
2. **Run where the data is.** `s3://commoncrawl` is in AWS us-east-1; in-region read is egress-free.
   GCP needs either Cloud NAT or transient GCS-staging. Order-of-magnitude only (NOT billing-validated):
   GCS Standard is ~$0.0000274/GiB-hr → ~$11 to hold 16 TiB for one day; same-region GCS→Dataproc read
   is free; STS has no per-GiB service fee but **may** incur Cloud Storage operations / external-provider
   costs. Cloud NAT bills hourly + per-GiB + IP + data-transfer components. On *this experiment's*
   observation GCP-transient-staging looked far cheaper than NAT and comparable to AWS in-region, but
   the AWS-vs-GCP equality and the NAT/STS line items **need billing validation** before any decision.
   Stage 4 is kept cloud-agnostic regardless.
3. **The cost lever is domain extraction, not parsing.** `tldextract` dominates; the WARC parser is
   ~1%. Fix = fewer + cheaper calls: **dedup (extract on distinct hosts — verified 19x, pure Spark)**
   + a faster PSL (Rust) or CC's own `url_host_registered_domain` (removes tldextract CPU). A native
   JVM parser rewrite is NOT justified by the data.
4. **Scoped vs global output is OPEN — not decided by this experiment.** The measured pipeline keeps
   backlinks into `S` (`parse_wat` / `backlink_edges`), matching current SPEC (Stage 4 narrowed to the
   carry-forward domain set). Since the full WAT is read regardless, extracting a *global* backlink/
   anchor graph once and scoping downstream is an attractive option (it would mirror the project's
   "authority computed globally, scope after" principle), but it is a **proposal**: it needs a separate
   global-output volume/cost measurement and an ADR + SPEC change before adoption. Narrowing at
   extraction saves output volume + some downstream shuffle, not the dominant parse cost.
5. **Completeness is multi-crawl.** Single-crawl recall vs the multi-crawl graph is low (~12.5%);
   a real index accumulates WAT across crawls.

## Limitations
- Crawl alignment is approximate: cc-index/WAT are single-crawl (`CC-MAIN-2026-21`), the graph is a
  multi-crawl window — recall vs the graph is a lower bound (the 12.5% figure).
- `S` is a cc-index-derived proxy for the future Stage 1 scope (primary-language hit), not full
  Stage 1 resolution.
- Throughput absolutes are profiler/contamination-affected; the **ratios** (dedup 19x; tldextract vs
  parser; serialization ≈ 0) are the reliable results.
- An initial inference that serialization dominated the domain cost was **refuted by cProfile** — the
  cost is tldextract CPU. Findings here are split into clean measurements vs caveated extrapolations
  accordingly.

## Actual cost (measured Dataproc DCU-hr; ~$0.06/DCU-hr)
Measurement pipeline: project_cdx 2.49 · derive_segments 31.6 · selectivity curve 25.3 · Job 3
domain-targeted 4.49 · warcio 1k throughput 186.2 · domain bench 17.5 · tight cProfile 1.13 DCU-hr
≈ **~$16**. Iteration/exploration overhead: s3a smoke hang 9.5 · fastwarc 1k cancelled 109.8 ·
over-scoped profile cancelled 66.4 DCU-hr (+ prep/quota-fail) ≈ **~$12**. STS transfers and image
builds did not incur Dataproc DCU. **Total ≈ ~$28.**

## Appendix — provenance (durable raw facts)

Dataproc batches (region `us-central1`; look up DCU/logs by id):

| run | batch id | state | DCU-hr |
|---|---|---|---|
| Job 1 project_cdx | `a9f23c720c534c73977e30aebd7ed0a8` | SUCCEEDED | 2.49 |
| Job 2 derive_segments | `1758960a1baa41e892da61cf62f4ae6f` | SUCCEEDED | 31.6 |
| selectivity curve | `f0498e0de9524c4fa45fc56f9c4b8a94` | SUCCEEDED | 25.3 |
| Job 3 domain-targeted (`run-name=domain-targeted`) | `038de76ffd214c95bb046b94924ded21c` | SUCCEEDED | 4.49 |
| warcio 1k (`run-name=throughput-1k-warcio`) | `63ae22583ef94b629da1f8f019f6929c` | SUCCEEDED | 186.24 |
| fastwarc 1k (abandoned) | `93805d8a6b794d5ca7c874560caed21c` | CANCELLED | 109.79 |
| domain dedup bench | `d001d121c4b74c2c96fb27bd8ebd38c8` | SUCCEEDED | 17.46 |
| over-scoped profile (abandoned) | `3481719c5c86445c9f08015dcca9ee3a` | CANCELLED | 66.41 |
| tight cProfile (`spark.python.profile`) | `7a291a095d0449f7b8ad9923939b8c4e` | SUCCEEDED | 1.13 |

Key metric outputs (Parquet/JSON under `gs://openhrefs-data/raw/exp5/`; the `_staging/` inputs are
lifecycle-deleted, these outputs are not):
- `segment_list_stats`: `{"target_domains":247744,"target_domains_meeting_share":179936,
  "source_domains":27559940,"expected_edges":101506393,"wat_files":100000,
  "estimated_bytes":16069855000000,"malformed_wat_paths":0,"crawl_wat_total":100000,
  "crawl_fraction":1.0}`
- `runs/domain-targeted/metrics`: `{"wat_files_parsed":39,"backlink_rows":35473,"found_edges":2284,
  "wat_only_edges":0,"empty_anchor_rows":28,"nofollow_rows":3783,"ugc_rows":3760,"sponsored_rows":0,
  "recall":{"expected":18216,"found":2284,"recall":0.1254}}`
- `runs/throughput-1k-warcio/metrics`: `{"parser":"warcio","wat_files_parsed":1000,
  "backlink_rows":203037962,"found_edges":7351868,"wat_only_edges":88789,
  "empty_anchor_rows":37141219,"nofollow_rows":15206915,"ugc_rows":451029,"sponsored_rows":162906,
  "recall":"not computed (file-sample)"}`

Throwaway analysis jobs are kept under `bench/` (re-runnable):
- `bench/selectivity_curve.py` — the selectivity-curve job (random target samples over `expected_edges`
  + `source_wat_files`); output lines `SELCURVE k_targets=… source_domains=… wat_files=… fraction=…`.
- `bench/bench_domain.py` — UDF-per-row vs dedup on `runs/throughput-1k-warcio/backlinks_sample`;
  measured `A_udf_perrow time=2191.5s calls=203037962` vs `B_dedup time=113.3s calls=1075417`
  (19.3x; distinct domains 108428 vs 108345).
- `bench/profile_v2.py` — `spark.python.profile` cProfile of parse (10 files) + domain map (5M url_to);
  driver output (cProfile table) was read from the batch `runtimeInfo.outputUri`. Parse cProfile:
  `iter_wat_links_fast` cumtime 362s (domain_from tldextract ~138s, extract_links ~60s, orjson 33s,
  fastwarc read 3.3s); domain map: `registered_domain_of_url` cumtime 1376s ≈ all tldextract,
  serializers/_struct/pickle ≈ 0.

Method/run commands ship in the repo: `experiments/exp5_wat/{project_cdx,derive_segments,parse_wat}.py`
docstrings + `infra/gcp/submit_job.sh` (`DATAPROC_EXTRA_PKGS`/`DATAPROC_EXTRA_PROPERTIES` hooks).
