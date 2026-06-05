# openhrefs

An open-source pipeline for building a domain authority and backlink index over
[CommonCrawl](https://commoncrawl.org/) data — configurable for any language
segment of the web. A transparent, reproducible alternative to commercial
tools like Majestic and Ahrefs.

The pipeline is the project's main product: clone the repository, configure
a scope of domains, run it on your own infrastructure, get your own link
index. A free public dataset of global domain-level metrics is planned as
a lightweight alternative for users who only need authority scores and don't
want to run the pipeline themselves.

> **Status:** Phase 2 — authority pipeline. Active development. The Stage 2
> (global PageRank) and Stage 3 (open_authority) Spark jobs have run
> end-to-end on a full crawl; their outputs are verified and validated against
> an OpenPageRank reference profile. The dbt authority mart and the published
> `open-domain-authority-index` are next; no dataset is published yet. The
> technical specification ([SPEC.md](SPEC.md)) defines the intended system;
> implementation continues phase by phase. Sections below will be expanded as
> features land.

---

## Why openhrefs

[To be written as the pipeline becomes usable. Will cover: motivation
(transparency vs. commercial tools), what the index contains, who it's for,
when to use it.]

## What you get

[To be written when first marts are produced. Will describe the four marts
— `mart_domain_authority`, `mart_domain_link_profile`, `mart_backlinks`,
`mart_outbound_links` — and how to access them.]

See [SPEC.md §6](SPEC.md) for the planned data model.

## Quick start

[To be written when the pipeline runs end-to-end. Will cover: installation,
running against a small sample scope locally, where to find outputs.]

## Architecture

[To be written when the architecture stabilizes. See [SPEC.md](SPEC.md) for
the full specification.]

## Roadmap

### Planned: `open-domain-authority-index` — free public dataset

A Parquet dataset of domain-level authority metrics computed by openhrefs
over the **global** CommonCrawl link graph (no scope restrictions on
input). Published columns: `domain`, `open_authority`, `open_volume`,
`window_id` — for every domain reachable from CommonCrawl. This is a
subset of the canonical `mart_domain_authority` (see `SPEC.md` §11);
extra fields like `pagerank_score` and `authority_ratio` remain
available to users who run the pipeline themselves.

This is the lightweight alternative to running the pipeline yourself. If
you need authority scores for an arbitrary set of domains but do not need
URL-level backlink data, this is sufficient and free. Distributed via
Parquet on GCS, Hugging Face Datasets, and a top-N CSV summary in GitHub
Releases.

Publication cadence to be determined — depends on actual compute cost per
refresh and demonstrated interest from consumers. The first publication
will follow Phase 3 completion (the authority pipeline and its mart),
ahead of the link-intelligence work.

### Planned: mart samples

To illustrate the full data model (link-level backlinks, anchor
distributions, link profiles), a small first-N-row sample of each mart
will be published alongside the index. Full marts are scope-specific and
not published — users who need them run the pipeline on their own
infrastructure.

### Pipeline phases

Phase 0 (setup) → Phase 1 (validation experiments) → Phase 2 (authority
pipeline: PageRank + open_authority) → Phase 3 (authority mart + first
public dataset) → Phase 4 (link-intelligence marts) → Phase 5
(orchestration + full multi-window index). The free public dataset lands
at Phase 3, roughly halfway, ahead of the heavier link-intelligence work.
Sections of this README will be filled in as each phase lands working
artifacts.

## License

[To be added.]

## Contributing

[To be added.]
