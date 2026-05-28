# Experiment 2 — Host Graph structure + PageRank convergence

Gates Track A. Validates that global PageRank on a real CommonCrawl host-level
webgraph converges in the expected range and calibrates the Stage 2 iteration
budget and cost. Exploratory code — production Stage 2 is written fresh.

## Data slice

- Release: `cc-main-2025-26-nov-dec-jan` host-level webgraph (vertices + edges).
- Size: **279,356,058 vertices**.
- Staged to own bucket via `stage_host_graph.sh` (streams `data.commoncrawl.org`
  → GCS, no requester-pays), then edges converted to parquet for splittable,
  columnar iteration.

## Scripts

- `stage_host_graph.sh <release> <dest-gcs-prefix>` — stage vertices+edges to GCS.
- `analyze.py` — graph stats + power-iteration PageRank (d=0.85, tol-based).
  Supports `--convert-parquet`, `--stats-only`, `--pagerank-only`, and resume
  (`--resume-from <parquet> --start-iter <n>`).
- `recover_checkpoint.py` — salvage a reliable Spark RDD checkpoint into parquet
  (used after the TTL kill, see below).

## Commands (Dataproc Serverless, runtime `--version=2.2`, explicit `--ttl`)

```bash
# stage graph
./stage_host_graph.sh cc-main-2025-26-nov-dec-jan \
    gs://openhrefs-data/raw/hostgraph/cc-main-2025-26-nov-dec-jan

# convert edges to parquet (once)
analyze.py --edges-path gs://.../edges/ --convert-parquet gs://.../edges_parquet/

# pagerank (pagerank-only, edges as parquet)
analyze.py --edges-path gs://.../edges_parquet/ --pagerank-only \
    --n-vertices 279356058 --max-iter 25 --tol 0.001 \
    --edge-partitions 2000 --checkpoint-every 4 \
    --checkpoint-dir gs://.../tmp/exp2-ckpt

# resume from a salvaged snapshot (after a TTL kill)
analyze.py ... --pagerank-only --resume-from gs://.../ranks_iter12 --start-iter 12
```

## Results

Structure:
- Vertices **279,356,058**, edges **13,431,887,360** (~13.4B).
- In-degree distribution **p50/p90/p99 = 1/8/729**, max **~19.8M** inbound on a
  single host — strongly power-law. 249.7M hosts (89%) have ≥1 inbound.
- **Dangling nodes (no out-edges): 211.2M = 75.6%** — only 24.4% of hosts have
  out-edges. Dangling-mass redistribution is therefore load-bearing.

PageRank:
- **Converges to tol=1e-3 at iteration 14** (L1 delta: iter12 1.38e-3,
  iter13 1.13e-3, iter14 9.35e-4).
- Final total mass **1.000000** — dangling-mass redistribution conserves mass.
- **~$0.67 / iteration** (maxExecutors=50, 2000 shuffle partitions, parquet edges,
  DISK_ONLY edge/outdeg persist, reliable checkpoint every 4 iters).

## Cost

~**$13** total: stats run ~$2.2 + edges→parquet ~$0.5 + PageRank runs ~$10 (first
full run iters 1–12 killed by the 4h default TTL ~$8 + checkpoint salvage ~$0.2 +
resume iters 13–14 ~$2).

## Conclusions / gate

- **Gate met:** PageRank converges in the expected range → Stage 2 budget ~14–15
  iterations for a crawl this size; keep tol-based convergence, not a fixed count.
- Operational lessons for Stage 2 (full detail in `docs/engineering_notes.md`
  2026-05-28):
  - Set `--ttl` explicitly — the Serverless default (14400s / 4h) silently
    cancels long iterative jobs (`CANCELLED`, "ttl exceeded").
  - Pin runtime minor only: `--version=2.2` (subminor `2.2.81` is rejected).
  - Native Spark `.checkpoint()` is **not** a cross-job resume artifact (LZ4 +
    Java-serialized `InternalRow`). Stage 2 should snapshot ranks to parquet
    periodically for resumability.
