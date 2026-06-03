#!/usr/bin/env bash
# Stage the CommonCrawl DOMAIN-level edges into GCS, SPLIT into ~gzip parts.
#
# Unlike the host graph (multi-part via *.paths.gz), the domain graph ships as a SINGLE
# 15.4 GiB gzip with no manifest. A single non-splittable gzip would be read by ONE Spark
# task (4.3B edges -> hours + OOM risk on the convert), so we split it into ~gzip parts here
# (each part = one parallel read task downstream).
#
# Run on a GCE VM in us-central1 (CloudFront read free, in-region GCS write free; the only
# cost is short VM uptime). Needs ~20 GiB scratch for the parts. Then verify the listing /
# _STAGED marker and delete the VM. Do NOT run on the laptop (15.4 GiB + local split).
#
# Usage: ./stage_edges.sh
# Requires: curl, gsutil (preinstalled on Google public VM images).

set -euo pipefail

BASE="https://data.commoncrawl.org/projects/hyperlinkgraph/cc-main-2026-mar-apr-may/domain"
DEST="gs://openhrefs-data/raw/webgraph/cc-main-2026-mar-apr-may-domain/edges"
LINES="${LINES:-5000000}"                              # ~5M edges/part -> ~860 parts
WORK="${WORK:-/mnt/scratch/edges}"
mkdir -p "$WORK"; cd "$WORK"

echo "==> downloading + splitting domain edges (~860 parts of ${LINES} lines each)…"
curl -fsSL "${BASE}/cc-main-2026-mar-apr-may-domain-edges.txt.gz" \
  | zcat | split -l "$LINES" -d -a4 --filter='gzip > $FILE.txt.gz' - edges_part_

echo "==> uploading $(ls edges_part_*.txt.gz | wc -l) parts -> ${DEST}/"
gsutil -m cp edges_part_*.txt.gz "${DEST}/"

echo "done $(date -u)" | gsutil cp - "${DEST}/_STAGED"  # completion marker (detect done w/o polling the VM)
echo "==> staged domain edges -> ${DEST}/"
