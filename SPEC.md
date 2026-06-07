# openhrefs — Technical Specification

> **Status.** This document specifies the *intended* system. Implementation is in **Phase 2** (authority pipeline) — see `README.md` for current status. Sections describing pipeline behavior, marts, and publishing should be read as the target contract, not a description of currently working features. Future public contract changes are reflected in this document.

## 1. Product Overview

**openhrefs is a data pipeline.** Users clone the repository, configure a scope of domains (by language, TLD, authority threshold, or any combination), run the pipeline on their own infrastructure, and produce their own domain authority and backlink index from [CommonCrawl](https://commoncrawl.org/) data. The pipeline is the primary product — a transparent, reproducible alternative to commercial tools such as Majestic and Ahrefs.

Alongside the pipeline, openhrefs may publish a free public dataset — **`open-domain-authority-index`** — containing domain-level authority metrics (`open_authority`, `open_volume`) computed over the **global** CommonCrawl link graph, before any scope filtering. It is an on-demand, best-effort snapshot — not a maintained service — reproducible by anyone who runs the pipeline. Full marts (link-level, scope-specific) are not published; users who need them run the pipeline. See §11 for publication details.

### Key properties

- **Configurable per user**: scope is defined by language, TLD, authority threshold, or any combination. Any ISO 639-3 language or "all languages" is supported.
- **Methodologically transparent**: PageRank and authority scoring use documented, reproducible algorithms. Seed sets are derived from the public [composite-domain-rating](https://github.com/ivan-aleshin/composite-domain-rating) project.
- **Vendor-independent**: PySpark business logic contains no cloud-specific code. Storage paths and submission mechanisms are configurable per environment (GCP, AWS, local).
- **Sliding crawl window**: the index methodology uses a rolling window of recent crawls, so a fresh pipeline run reflects current CommonCrawl data (~monthly cadence). Any published snapshot is best-effort, not a maintained feed.

---

## 2. Data Sources

All sources are publicly available at no cost. CommonCrawl publishes via several equivalent endpoints — the project reads paths from `config/storage.yml`, so the user picks per deployment.

| Source | Canonical location | Used for |
|---|---|---|
| CommonCrawl CDX Index | `https://index.commoncrawl.org/` (CDXJ API) or `s3://commoncrawl/cc-index/collections/` | Language detection per domain per crawl |
| CommonCrawl Host Graph | `https://data.commoncrawl.org/projects/hyperlinkgraph/` or `s3://commoncrawl/projects/hyperlinkgraph/` | Domain-level link graph, PageRank, link profile |
| CommonCrawl WAT Files | `https://data.commoncrawl.org/crawl-data/CC-MAIN-*/wat.paths.gz` or `s3://commoncrawl/crawl-data/CC-MAIN-*/wat.paths.gz` | URL-level backlinks, anchor text, rel attributes |
| composite-domain-rating | GitHub Releases (CSV) | Seed set for open_authority (Personalized PageRank) |

The reference GCP deployment uses the GCS mirror (`gs://commoncrawl/...`) to avoid cross-cloud egress when running on Dataproc. Concrete paths are deployment-specific and verified by smoke tests before large runs.

---

## 3. Scope Configuration

### Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `language` | list[str] | null | ISO 639-3 codes, e.g. `[ita, spa, jpn]` |
| `tld` | list[str] | null | TLDs without dot, e.g. `[com, it, es]` |
| `tld_class` | list[str] | null | `gov` / `edu` / `normal` |
| `min_language_share` | float | **0.25** | Minimum share of target language pages on domain (0.0–1.0) |
| `min_referring_domains` | int | 0 | Authority threshold — exclude low-signal domains |
| `min_pagerank` | float | 0.0 | Minimum PageRank score |
| `min_crawls_active` | int | 1 | Crawls in window where domain must appear |
| `min_crawls_language` | int | 2 | Crawls where target language share must meet threshold |
| `subdomain_handling` | str | `root_only` | `root_only` / `include` / `aggregate` |

### Filter logic

Filters use a two-level `all_of` / `any_of` structure. Within each level: `all_of` = AND, `any_of` = OR. Nesting is arbitrary. `min_*` parameters always apply via AND regardless of group structure.

```yaml
# Example: Italian or Spanish language OR respective ccTLD,
#          AND normal domains only, AND minimum link signal
scope:
  all_of:
    - any_of:
        language: [ita, spa]
        tld: [it, es]
    - tld_class: [normal]
    - min_referring_domains: 3
    - min_crawls_active: 1
```

```yaml
# Example: Japanese language only
scope:
  all_of:
    language: [jpn]
```

```yaml
# Example: full index, quality domains only
scope:
  all_of:
    min_referring_domains: 10
    min_crawls_active: 2
```

### Subdomain handling

| Mode | Behavior |
|---|---|
| `root_only` | Only root domains included; subdomains ignored (default) |
| `include` | Subdomains treated as separate entities |
| `aggregate` | Subdomains aggregated into root domain |

Subdomain handling is applied at Stage 1 (CDX scan), shaping the target domain list early. It does *not* alter the input to Stages 2/3 — those still run on the full global Host Graph (see "Filter application order" below).

### Filter application order

| Parameter | Applied at |
|---|---|
| `language`, `tld`, `tld_class`, `subdomain_handling` | Stage 1 — CDX scan |
| `min_crawls_language`, `min_crawls_active` | After Stage 1 — target domain list construction |
| `min_referring_domains` | After Stage 2 — Host Graph (post-metric filter) |
| `min_pagerank` | After Stage 2 — PageRank computation (post-metric filter) |

**Scope filter is never applied to the input of Stages 2 (PageRank) or 3 (open_authority).** These stages run on the **full global Host Graph** — global context is required for accurate scoring, and the `open-domain-authority-index` published dataset (§11) is computed at this point, before any scope narrowing. Scope-derived filters (`min_referring_domains`, `min_pagerank`) and the Stage 1 target domain list are applied **after** the global metrics exist, narrowing the carry-forward domain set used by Stage 4 (URL-level WAT extraction) and the dbt link-level marts. This ordering is what makes the global index publishable as a byproduct of any scoped pipeline run.

---

## 4. Crawl Window Strategy

The index is computed over a sliding window of recent crawls plus permanent historical anchors.

```
Recent window (rolling, N=6):
  [CC-2025-13] [CC-2025-18] [CC-2025-23] [CC-2025-39] [CC-2025-45] [CC-2025-51]
                                                                          ↑ new crawl added
  [CC-2025-13] exits scoring window → retained in historical data

Historical anchors (permanent, 1 per 6-month period):
  [CC-2024-H2] [CC-2024-H1] [CC-2023-H2] ...
```

When a new crawl is added:
- The oldest recent crawl exits the scoring window (but its data is retained)
- Language history, backlinks, and outbound links accumulate across all crawls
- PageRank and open_authority are recomputed on the merged graph of the new window
- Active backlinks and outbound links are updated with `is_active`, `is_broken`, `is_lost` flags

---

## 5. Pipeline Architecture

```
Cloud Scheduler (monthly)
        │
        ▼
GitHub Actions
  ├── Detect new CommonCrawl crawl
  └── If found: trigger pipeline
              │
              ├── Stage 1: Language Classification
              │   Source:  CDX Index (~15 GB/crawl)
              │   Output:  cc_domain_languages (Parquet on GCS)
              │
              ├── Stage 2: PageRank
              │   Source:  Host Graph (~5–15 GB/crawl)
              │   Output:  cc_domain_pagerank (Parquet on GCS)
              │
              ├── Stage 3: open_authority
              │   Source:  Host Graph + composite-domain-rating seed set
              │   Output:  cc_domain_authority (Parquet on GCS)
              │
              ├── Stage 4: Backlinks & Outbound
              │   Source:  WAT files (filtered via host graph index)
              │   Output:  cc_backlinks, cc_outbound_links (Parquet on GCS)
              │
              ├── dbt run (target=prod, dbt-spark adapter)
              │   Staging → Intermediate → Marts (all materialized as Parquet)
              │
              └── Publish (optional, best-effort snapshot)
                  ├── Hugging Face dataset update
                  └── GitHub Release (CSV top-N summary)
```

All Spark jobs run on **Dataproc Serverless** — no cluster management, resources released on completion. Canonical storage is Parquet at every stage; warehouses are publication channels only (see §9 and §11).

### Stage 1 — Language Classification

A domain is included in the target segment when the share of target-language pages meets or exceeds `min_language_share` (default **0.25**) in at least `min_crawls_language` crawls. This excludes bilingual domains where the target language is secondary.

Language field format per domain:
```json
[{"lang": "ita", "share": 0.74}, {"lang": "spa", "share": 0.18}]
```
Sorted descending by share. Storage threshold: **5%** (languages below excluded from array). Dominant language = first element.

Output: Parquet at `<RAW_PATH>/cc_domain_languages/` — schema `(domain STRING, crawl STRING, languages ARRAY<STRUCT<lang STRING, share FLOAT64>>, page_count BIGINT)`. Partitioning: `crawl`.

### Stage 2 — PageRank

Reads the domain-level link graph (vertices and edges derived from the CommonCrawl Host Graph, collapsed host→domain). Runs power-iteration PageRank over the **full global graph** — input is *not* filtered by user scope. Global context is required for accurate scoring, and the global metrics are what gets published as `open-domain-authority-index` (§11). Scope-derived filters (`min_referring_domains`, `min_pagerank`) are applied to the *output* as post-metric filters on the carry-forward domain set.

**Algorithm:** standard PageRank with damping factor `d = 0.85`. Convergence criterion: `Σ|PR_new - PR_old| < tol` where `tol = 0.001`. Typically converges in 10–20 iterations on domain-level graphs. Dangling domains (no out-links) redistribute their mass uniformly across all domains each iteration, preserving total mass = 1.

**Validation:** rank-agreement profile against OpenPageRank on overlapping domains — Spearman, Kendall τ-b, RBO, and top-k Jaccard across head/tail rank buckets. A sanity gate that the engine reproduces an established public reference, not a single correlation to maximize.

Output: Parquet at `<RAW_PATH>/cc_domain_pagerank/` — schema `(domain STRING, crawl STRING, pagerank_score DOUBLE, in_degree BIGINT, out_degree BIGINT)`. Partitioning: `crawl`.

### Stage 3 — open_authority

**Personalized PageRank** with teleportation concentrated on a seed set instead of uniform distribution. Like Stage 2, runs on the **full global domain-level graph** — input is not filtered by user scope.

**Seed set:** top-N domains from composite-domain-rating, ranked by consensus percentile score.

**Seed weights** (log-scaled by rank position, normalized to sum = 1):
```
weight(rank) = 1 / log2(rank + 1)

rank 1:      1.000
rank 10:     0.289
rank 100:    0.150
rank 1000:   0.100
rank 10000:  0.075
```

**Formula** (per iteration, mass-conserving):
```
OA(v) = (1 - d) * w_norm(v)
      + d * Σ_u [ OA(u) / OutDeg(u) ]
      + d * D * w_norm(v)
```
where `w_norm(v) = 0` for non-seed domains, and `D = Σ_{dangling u} OA(u)` is the total mass on dangling domains (no out-links), routed back to the seed via the personalization vector rather than spread uniformly. This keeps `Σ OA = 1` and concentrates both teleport and dangling mass on the seed. Optimal `d` and seed-set size N determined by Experiment 4 (see Section 8).

**open_volume**: log-scaled in-degree, reflecting link quantity independent of quality.

Output: Parquet at `<RAW_PATH>/cc_domain_authority/` — schema `(domain STRING, crawl STRING, open_authority DOUBLE, open_volume DOUBLE)`. Partitioning: `crawl`.

### Stage 4 — Backlinks and Outbound Links

**WAT filtering strategy** (avoids full 30TB scan):
1. From Host Graph: identify all `source_domains` that link to target domains
2. From CDX Index (Stage 1 output): locate WAT segment files containing those source domains
3. Process only those WAT segments

For outbound links: read WAT for pages on target domains directly (target domains are known from Stage 1).

**Broken/lost status detection:**
- `is_broken`: link present in WAT but target domain has no 200 responses in latest crawl (from CDX)
- `is_lost`: link was present in previous crawl window but absent in current (source domain still active)
- Status derived by diff between consecutive crawl windows

Output: Parquet datasets at
- `<RAW_PATH>/cc_backlinks/` — `(domain_to, url_from, url_to, ...link fields..., crawl)`
- `<RAW_PATH>/cc_outbound_links/` — `(domain_from, url_from, url_to, ...link fields..., crawl)`

Both partitioned by `crawl`.

---

## 6. Data Model

### mart_domain_authority

One row per domain per crawl scoring window. The grain is the **window**, not a single crawl — Stage 2/3 metrics are computed on the merged graph of the window.

| Field | Type | Description |
|---|---|---|
| `domain` | STRING | Root domain |
| `window_id` | STRING | Crawl scoring window identifier (typically the most recent crawl ID in the window, e.g. `CC-MAIN-2025-51`) |
| `window_end_crawl` | STRING | Most recent crawl included in the window |
| `window_crawls` | ARRAY<STRING> | All crawls included in this window's computation |
| `pagerank_score` | FLOAT64 | Global PageRank (power iteration over full graph) |
| `open_volume` | FLOAT64 | Log-scaled in-degree (link quantity signal) |
| `open_authority` | FLOAT64 | Personalized PageRank from composite-DR seed set |
| `authority_ratio` | FLOAT64 | `open_authority / open_volume` — quality vs quantity |

### mart_domain_link_profile

One row per domain per crawl.

| Field | Type | Description |
|---|---|---|
| `domain` | STRING | Root domain |
| `crawl` | STRING | Source crawl |
| `domain_languages` | ARRAY<STRUCT<lang,share>> | Languages sorted desc, threshold ≥5% |
| `language_stability_score` | FLOAT64 | Stability of dominant language across crawls (0–1) |
| `crawl_frequency` | FLOAT64 | Average URLs crawled per month |
| `first_seen` | DATE | First appearance in CommonCrawl |
| `is_redirect` | BOOL | Root URL returns a redirect |
| `redirect_code` | INT64 | 301 / 302 / 307 / 308 (null if not redirect) |
| `redirect_target` | STRING | Target domain of redirect (null if not redirect) |
| **Inbound** | | |
| `referring_domains` | INT64 | Unique domains linking in |
| `referring_ips` | INT64 | Unique IPs linking in |
| `referring_subnets` | INT64 | Unique /24 subnets linking in |
| `inbound_links_total` | INT64 | Total inbound links |
| `dofollow_links` | INT64 | Dofollow inbound links |
| `dofollow_ratio` | FLOAT64 | `dofollow_links / inbound_links_total` |
| `dofollow_referring_domains` | INT64 | Domains with at least one dofollow link |
| `ugc_links` | INT64 | UGC inbound links |
| `sponsored_links` | INT64 | Sponsored inbound links |
| `gov_links` | INT64 | Links from .gov domains |
| `edu_links` | INT64 | Links from .edu domains |
| `tld_diversity_score` | INT64 | Unique TLDs among referring domains |
| `inbound_top10k_ratio` | FLOAT64 | Share of links from top-10K global domains |
| `inbound_lang_distribution` | ARRAY<STRUCT<lang,share>> | Language distribution of referring domains |
| `cross_language_authority` | FLOAT64 | Share of PageRank from domains with different dominant language |
| **Outbound** | | |
| `outbound_links_total` | INT64 | Total outbound links |
| `linked_domains` | INT64 | Unique domains linked to |
| `outbound_dofollow_ratio` | FLOAT64 | Share of dofollow in outbound |
| `outbound_broken_count` | INT64 | Outbound links with is_broken=true |
| `outbound_broken_ratio` | FLOAT64 | `outbound_broken_count / outbound_links_total` |
| `outbound_lost_count` | INT64 | Outbound links removed since previous window |
| `outbound_tld_distribution` | ARRAY<STRUCT<tld,share>> | TLD distribution of link targets |
| `outbound_tld_class_dist` | ARRAY<STRUCT<class,share>> | gov/edu/normal distribution of targets |
| `outbound_lang_distribution` | ARRAY<STRUCT<lang,share>> | Language distribution of indexed targets |
| `avg_target_pagerank` | FLOAT64 | Average PageRank of indexed link targets |
| `indexed_targets_ratio` | FLOAT64 | Share of outbound targets present in index |

### mart_backlinks

One row per unique (url_from, url_to) per crawl. Inbound links to target-scope domains.

| Field | Type | Description |
|---|---|---|
| `domain_to` | STRING | Target domain (in scope) |
| `url_from` | STRING | Source URL |
| `url_to` | STRING | Target URL |
| `domain_from` | STRING | Source domain |
| `anchor` | STRING | Link anchor text |
| `snippet_left` | STRING | Text before the link |
| `snippet_right` | STRING | Text after the link |
| `link_type` | STRING | text / image / rss / canonical / alternate / redirect / frame / form |
| `is_dofollow` | BOOL | No nofollow attribute present |
| `is_ugc` | BOOL | rel=ugc present |
| `is_sponsored` | BOOL | rel=sponsored present |
| `is_canonical` | BOOL | rel=canonical |
| `is_alternate` | BOOL | rel=alternate |
| `is_image` | BOOL | Link contains img tag |
| `is_rss` | BOOL | Link found in RSS feed |
| `is_homepage_link` | BOOL | Source is root URL of domain |
| `is_content` | BOOL | Link found in main content area |
| `http_code` | INT64 | HTTP status of source page |
| `encoding` | STRING | Character encoding of source page |
| `page_size` | INT64 | Source page size in bytes |
| `title` | STRING | HTML title of source page |
| `links_internal` | INT64 | Internal links on source page |
| `links_external` | INT64 | External links on source page |
| `tld_class_source` | STRING | gov / edu / normal |
| `source_languages` | ARRAY<STRUCT<lang,share>> | Languages of source domain |
| `target_languages` | ARRAY<STRUCT<lang,share>> | Languages of target domain |
| `is_cross_language` | BOOL | Dominant language of source ≠ dominant language of target |
| `first_seen` | DATE | First crawl where this link was observed |
| `crawl` | STRING | Source crawl |

> `nofollow` is implicit: `NOT is_dofollow`. `is_ugc` and `is_sponsored` are independent of `is_dofollow`.

### mart_outbound_links

One row per unique (url_from, url_to) per crawl. Outbound links from target-scope domains.

Same link-level fields as `mart_backlinks` with source/target perspective swapped, plus:

| Field | Type | Description |
|---|---|---|
| `domain_from` | STRING | Source domain (in scope) |
| `target_domain` | STRING | Target domain (may be outside scope) |
| `tld_class_target` | STRING | gov / edu / normal of target |
| `first_seen_crawl` | STRING | First crawl where link was observed |
| `last_seen_crawl` | STRING | Last crawl where link was present |
| `crawls_active` | INT64 | Number of crawls link was active |
| `is_active` | BOOL | Link present in latest crawl window |
| `is_broken` | BOOL | Link present but target returns non-200 |
| `is_lost` | BOOL | Link removed from source page (source still active) |
| `target_last_http_code` | INT64 | Last known HTTP status of target |
| `is_target_indexed` | BOOL | Target domain is present in our index |
| `target_languages` | ARRAY<STRUCT<lang,share>> | Target languages (null if not indexed) |
| `target_pagerank_score` | FLOAT64 | Target PageRank (null if not indexed) |
| `target_open_authority` | FLOAT64 | Target open_authority (null if not indexed) |
| `target_referring_domains` | INT64 | Target referring domains (null if not indexed) |
| `is_cross_language` | BOOL | Dominant language of source ≠ dominant language of target |

> `is_broken` and `is_lost` are independent flags. `is_broken` concerns target availability; `is_lost` concerns link presence in source HTML. All `target_*` fields are nullable when `is_target_indexed = false`.

---

## 7. dbt Layer

dbt runs on **two adapters** sharing the same canonical Parquet:

- **`target: local` — dbt-duckdb.** CI, local development on fixtures, narrow-scope tool users. Reads source Parquet via `read_parquet()`, materializes marts as Parquet via `materialized='external'`.
- **`target: prod` — dbt-spark.** Production runs against real Parquet on GCS, wide-scope tool users. Reuses the same Spark runtime that executes Stages 1–4. Reads and writes Parquet natively.

Models are written portable across both adapters. Adapter-specific syntax is isolated in `dbt/macros/cross_db/` via `adapter.dispatch()`. Marts are materialized as Parquet at the configured marts path on both adapters; warehouses (if any) are downstream publication channels, not the dbt target.

```
models/
├── staging/
│   ├── stg_cc_domain_languages.sql
│   ├── stg_cc_host_graph.sql
│   ├── stg_cc_backlinks.sql
│   └── stg_cc_outbound_links.sql
├── intermediate/
│   ├── int_domain_language_history.sql   # language per domain per crawl, stability score
│   ├── int_crawl_window.sql              # which crawls are in current window
│   ├── int_pagerank.sql                  # pagerank + open_authority joined
│   ├── int_link_profile_inbound.sql      # inbound aggregates from host graph + WAT
│   ├── int_link_profile_outbound.sql     # outbound aggregates with broken/lost flags
│   └── int_backlink_status.sql           # diff logic for is_lost, discovered_status
└── marts/
    ├── mart_domain_authority.sql
    ├── mart_domain_link_profile.sql
    ├── mart_backlinks.sql
    └── mart_outbound_links.sql

snapshots/
└── scd_domain_authority.sql              # SCD2: track authority score changes over time
```

Materialization: staging and intermediate models as views (no copy of source data); marts as `materialized='external'` Parquet (DuckDB) or `materialized='table'` with `file_format='parquet'` and explicit `location_root` (Spark). Snapshots are accepted as engine-native — a small DuckDB file under local, a Parquet table at `<MARTS_PATH>/_snapshots/` under prod.

---

## 8. Experiments (Required Before Full Implementation)

All experiments run on minimal data slices to validate assumptions and calibrate parameters before committing to full-scale processing.

### Experiment 1 — CDX Language Extraction
**Data:** Single CDX file (~100 MB)
**Validates:** Coverage of `languages` field; language distribution quality; 5% share threshold appropriateness; handling of multi-language domains.

### Experiment 2 — Host Graph Structure
**Data:** Single crawl Host Graph (~15 GB)
**Validates:** In-degree power-law distribution; dangling node ratio; actual vertex/edge counts; PageRank convergence iterations at `tol=0.001`; actual Dataproc Serverless cost vs estimate.

### Experiment 3 — PageRank Validation
**Data:** Full Host Graph, single crawl
**Validates:** Spearman correlation with OpenPageRank on overlapping domains; top-100 sanity check; cost calibration.

### Experiment 4 — open_authority Seed Sensitivity
**Data:** Full Host Graph, single crawl
**Validates:** Optimal seed-set size (test: top-1K, top-5K, top-10K, top-50K); optimal weight formula (`1/log2(rank+1)` vs `1/sqrt(rank)` vs uniform); optimal damping factor; correlation with a publicly available reference domain ranking.

### Experiment 5 — WAT Extraction
**Data:** 10–20 WAT segments for a small language target
**Validates:** Link coverage vs Host Graph; anchor text and rel attribute quality; cost of filtered WAT scan vs full scan estimate.

---

## 9. Infrastructure

### Compute

All Spark jobs run on **GCP Dataproc Serverless** in the reference deployment. No persistent cluster — resources are allocated per job submission and released on completion. The same PySpark code runs locally via `pyspark` (or `uv run python`) against `file://` paths for narrow scopes or development.

dbt runs on the same Spark runtime for the production target (`target: prod`, dbt-spark adapter) and on an in-process DuckDB engine for the development / CI target (`target: local`, dbt-duckdb adapter). See §7.

No persistent compute infrastructure outside of job execution windows.

### Storage

Canonical storage is **Parquet on object storage** at every layer. No warehouse-native tables appear in the core pipeline; any warehouse downstream is a publication channel (see §11).

| Layer | Storage | Format | Notes |
|---|---|---|---|
| Raw (Spark stage outputs) | `<RAW_PATH>` | Parquet, partitioned by `crawl` | Written by Stage 1–4 jobs |
| Marts (dbt outputs) | `<MARTS_PATH>` | Parquet via dbt-spark / dbt-duckdb | Canonical analytical layer |
| Snapshots (SCD2) | `<MARTS_PATH>/_snapshots/` | Parquet (Spark) or DuckDB file (local) | Engine-native; small volume |

`<RAW_PATH>` and `<MARTS_PATH>` resolve via env vars and `config/storage.yml`. For the GCP reference deployment: `gs://openhrefs-data/raw/` and `gs://openhrefs-data/marts/`. For local development: `file:///data/openhrefs/raw/` and `file:///data/openhrefs/marts/`. For AWS: `s3://openhrefs-data/...`.

Storage cost (GCS Standard, GCP reference deployment): ~$0.02/GB/month. Narrow-scope reference data fits in ~1–5 GB total; wide-scope production data depends on user choice.

### Cost per crawl (estimated, compute only)

| Stage | Data volume | Duration | Cost |
|---|---|---|---|
| CDX → Language | ~15 GB | 20–30 min | ~$0.20 |
| Host Graph → PageRank + open_authority | ~15 GB | 1–2 hr | ~$1–2 |
| WAT → Backlinks + Outbound (filtered) | ~30–300 GB | 2–4 hr | ~$2–5 |
| dbt run (Parquet → Parquet, dbt-spark) | depends on scope | 10–30 min | ~$0.20–0.50 |
| **Total per crawl** | | | **~$3.5–7.5** |

Cost calibration and budget planning are deployment-specific. Narrow-scope reference runs sit at the low end of the range above; wide-scope or large URL-level mart users should expect proportionally higher cost.

### Always-free / low-cost GCP services used

Cloud Scheduler (3 jobs/month free) and Cloud Monitoring (basic metrics). BigQuery and dashboard tools are not part of the reference deployment; users can load the canonical Parquet into them themselves if needed.

### Vendor independence

PySpark jobs contain no cloud-specific imports. Storage paths are configured via `config/storage.yml`:

```yaml
# GCP
raw_path: gs://openhrefs-data/raw/
marts_path: gs://openhrefs-data/marts/
host_graph_path: gs://commoncrawl/projects/hyperlinkgraph/...

# AWS (drop-in replacement)
# raw_path: s3://openhrefs-data/raw/
# marts_path: s3://openhrefs-data/marts/
# host_graph_path: s3://commoncrawl/projects/hyperlinkgraph/...

# Local
# raw_path: file:///data/openhrefs/raw/
# marts_path: file:///data/openhrefs/marts/
```

Submission scripts are isolated under `infra/gcp/` and `infra/aws/`, separate from business logic. Publication helpers live under `publish/` and are invoked after the pipeline; the pipeline itself never imports cloud SDKs.

---

## 10. Update Mechanism

CommonCrawl publishes a new crawl approximately monthly with no webhook notification. Detection is polling-based.

```
Cloud Scheduler: monthly cron
    → triggers GitHub Actions workflow
        → polls CommonCrawl index for new crawl
        → if new crawl found:
            → submit Dataproc Serverless jobs (Stage 1 → 2 → 3 → 4, sequential)
            → dbt run (recompute sliding window models)
            → export Parquet to GCS
            → (optional, manual/best-effort) refresh the public snapshot:
              upload to Hugging Face + publish a GitHub Release CSV summary
```

Manual trigger available via `workflow_dispatch` for development and on-demand reprocessing.

---

## 11. Publishing

Canonical output is Parquet on the configured marts path. Publication channels are independent fan-outs from that canonical store; enabling or disabling any channel does not touch the pipeline.

The public **`open-domain-authority-index`** dataset (`mart_domain_authority` computed over the *global* CommonCrawl link graph, before scope filtering) is an on-demand, best-effort snapshot. Full marts (link-level, scope-specific) are not published — users who need them run the pipeline themselves.

| Channel | Content | Update cadence |
|---|---|---|
| Hugging Face Datasets | `open-domain-authority-index` (published-columns Parquet) | Best-effort / on demand |
| GitHub Releases | Top-N domain CSV summary | Best-effort / on demand |

These are the only first-party channels, and both are best-effort snapshots, not maintained feeds. Any other channel — a BigQuery public dataset (`bq load`), a dashboard, a mirror — is reproducible by users directly from the canonical Parquet; openhrefs does not operate them.

Publication scripts live under `publish/`. They run after the pipeline and read only from canonical Parquet — they do not modify any pipeline state.

### Published columns: `open-domain-authority-index`

`open-domain-authority-index` is a **subset** of `mart_domain_authority` (§6), not the full mart. Published columns:

| Column | Type | Source |
|---|---|---|
| `domain` | STRING | Root domain |
| `open_authority` | FLOAT64 | Personalized PageRank score |
| `open_volume` | FLOAT64 | Log-scaled in-degree |
| `window_id` | STRING | Crawl scoring window identifier |

`pagerank_score`, `authority_ratio`, and `window_crawls` exist in the canonical mart but are **not** published — they remain available to users who run the pipeline themselves and read `mart_domain_authority` directly. This subset is deliberate: the published dataset is a lightweight authority signal, not a full link-intelligence product.

---

## 12. Repository Structure

```
openhrefs/
├── README.md                       # public-facing overview
├── SPEC.md                         # this document
├── LICENSE
├── CONTRIBUTING.md
├── Makefile                        # `make check` — lint, type-check, tests
├── pyproject.toml                  # uv, ruff, mypy, pytest config
├── uv.lock
├── requirements.txt                # generated from uv.lock for Dataproc submissions
├── .pre-commit-config.yaml
├── .sqlfluff
├── .github/
│   └── workflows/
│       ├── ci.yml                  # lint + tests on every PR
│       ├── dbt-prod-sample.yml     # prod-adapter validation (manual)
│       ├── crawl_check.yml         # Cloud Scheduler trigger
│       └── run_pipeline.yml        # manual / triggered pipeline run
├── config.yml                      # scope, language targets, crawl window
├── config/
│   └── storage.yml                 # storage paths per environment (GCP / AWS / local)
├── spark_jobs/
│   ├── stage1_language/
│   │   ├── main.py                 # entrypoint, argparse, SparkSession init
│   │   ├── io.py                   # read CDX, write Parquet
│   │   └── transforms.py           # pure DataFrame → DataFrame functions
│   ├── stage2_pagerank/
│   ├── stage3_open_authority/
│   ├── stage4_backlinks/
│   └── common/                     # shared utilities (config loader, schemas)
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml                # two targets: local (DuckDB), prod (Spark)
│   ├── packages.yml                # dbt_utils, dbt_expectations, audit_helper
│   ├── models/
│   │   ├── staging/
│   │   ├── intermediate/
│   │   └── marts/                  # materialized as Parquet
│   ├── snapshots/
│   ├── tests/
│   ├── macros/
│   │   └── cross_db/               # adapter.dispatch() portability macros
│   └── seeds/
├── publish/                        # canonical Parquet → public snapshot (manual)
│   └── open_domain_authority_index.py  # extract subset → Hugging Face + GitHub Release
├── infra/
│   ├── gcp/
│   │   └── submit_job.sh           # Dataproc Serverless submission
│   ├── aws/                        # vendor parity placeholder
│   └── monitoring/
│       └── alerts.yml
├── tests/
│   ├── conftest.py                 # shared SparkSession fixture
│   ├── fixtures/
│   │   └── parquet/                # tiny synthetic Parquet for dbt CI
│   ├── spark_jobs/
│   │   ├── stage1_language/
│   │   ├── stage2_pagerank/
│   │   ├── stage3_open_authority/
│   │   └── stage4_backlinks/
│   └── common/
└── experiments/                    # Phase 1 validation; relaxed rules
    ├── exp1_cdx_language/
    ├── exp2_host_graph/
    ├── exp3_pagerank_validation/
    ├── exp4_seed_sensitivity/
    └── exp5_wat_extraction/
```
