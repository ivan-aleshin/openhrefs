# Experiment 1 — CDX Language Extraction

Gates Track B. Validates the CommonCrawl CDX `languages` field as the source
for Stage 1 language classification (SPEC §5 Stage 1).

## Data slice

- One CDX shard: `CC-MAIN-2024-51`, `cdx-00000.gz` (632 MB gzip, 9,237,013 rows).
- Run **locally** (`uv run python -m experiments.exp1_cdx_language.analyze`),
  single 632 MB file — no Dataproc needed.
- **Actual cost: $0** (local pyspark; ~1 min wall). Dataproc scaling cost is
  out of scope for Exp 1 and is measured in Exp 2 on the Host Graph.

CDX line: `<surt-key> <timestamp> <json>`; json carries `status`, `url`,
`languages` (ISO 639-3, comma-separated, ordered), `charset`. Primary language
= first element of `languages`.

## What was measured

1. `languages` coverage — overall and among `status==200`.
2. Primary-language distribution (top 20) and whether target codes `bul`, `ron`
   appear and look sane.
3. 5% storage threshold — effect on per-domain language arrays.
4. `subdomain_handling=root_only` — registrable-domain extraction.

`min_language_share` is a **user-configurable scope parameter**, not something
this experiment calibrates — see the note under Decision gate.

## Results

### Coverage
| metric | value |
|---|---|
| total records | 9,237,013 |
| with `languages` | 7,276,862 (78.8%) |
| `status==200` | 7,652,405 (82.8%) |
| of those, with `languages` | **95.1%** |

The field is absent mostly on non-200 records (redirects, robots, errors),
which carry no body to classify. For fetched HTML the field is near-universal.

### Distribution (top, primary language)
`eng` 60.6%, `hye` 7.5%, `sqi` 5.2%, `ara` 4.6%, `rus` 3.7%, then a long tail
(`deu`, `fra`, `por`, `spa`, `jpn`, …). All observed codes look like valid
ISO 639-3 (not checked against a code dictionary — eyeballed).

Target languages on this shard: `bul` 3,850 pages, `ron` 12,706 pages — present
and recognized, but sparse (see limitations).

### 5% storage threshold
129,097 distinct root domains; 129,093 retain ≥1 language above 5%. This only
shows the threshold does **not drop whole domains** — expected, since with
primary-language attribution almost every domain has one dominant language well
above 5%. How aggressively 5% trims the long tail *within* multi-language
arrays is not directly measured here; no reason to change the default.

### root_only extraction
Registrable domain approximated as the last two labels. Correct for the target
TLDs (`.bg`, `.ro` are single-level). **Production needs a public suffix list**
for multi-level TLDs (`.co.uk`); flagged for Stage 1 implementation.

## Decision gate

- **Source:** use the CDX `languages` field directly as the Stage 1 source —
  95.1% coverage on `status==200` is sufficient. ✓
- **Language codes:** observed codes look like valid ISO 639-3 and target codes
  resolve; good enough to proceed. ✓
- **5% storage threshold:** no reason to change the SPEC default — keep. ✓

`min_language_share` is **not** an experiment output. It is a scope parameter
the user sets per their intent (how strictly to require the target language on
a domain); the pipeline default lives in `config.yml` / SPEC §3. The sensitivity
measurement (on this shard, bul/ron qualifying domains: 226 at 0.25 vs 215 at
0.30 — a ~5% spread) is informational only.

## Limitations

- **Single shard, not scope-representative.** CDX is SURT-sorted, so shard 0 is
  dominated by numeric-IP and early-alphabet hosts (hence the `hye`/`sqi`
  skew); `bul`/`ron` domains are scattered across all shards. Coverage and
  mechanics are validated here; the `min_language_share` sensitivity numbers
  are illustrative, not representative.
- Primary-language-only attribution (first element). Multi-language pages count
  once, toward their dominant language — consistent with SPEC's "dominant
  language = first element".
