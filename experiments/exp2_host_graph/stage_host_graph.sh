#!/usr/bin/env bash
# Stage a CommonCrawl host-level webgraph release from data.commoncrawl.org
# into a GCS bucket, streaming each part file (no local disk needed).
#
# Intended to run on a GCE VM in the same region as the destination bucket:
# data.commoncrawl.org (CloudFront) is free to read, GCS writes within the
# region are free, so the only cost is the VM uptime. This sidesteps both the
# laptop-bandwidth bottleneck and gs://commoncrawl requester-pays.
#
# Usage: ./stage_host_graph.sh <release> <dest-gcs-prefix>
# Example:
#   ./stage_host_graph.sh cc-main-2025-26-nov-dec-jan \
#       gs://openhrefs-data/raw/hostgraph/cc-main-2025-26-nov-dec-jan
#
# Requires: curl, gsutil (preinstalled on Google public VM images).

set -euo pipefail

RELEASE="${1:?Usage: $0 <release> <dest-gcs-prefix>}"
DEST="${2:?Usage: $0 <release> <dest-gcs-prefix>}"
PARALLEL="${PARALLEL:-8}"
BASE="https://data.commoncrawl.org/projects/hyperlinkgraph/${RELEASE}/host"

stage_set() {
  local kind="$1" # vertices | edges
  local list
  list="$(curl -fsSL "${BASE}/${RELEASE}-host-${kind}.paths.gz" | zcat)"
  echo "==> ${kind}: $(echo "$list" | wc -l) files -> ${DEST}/${kind}/"
  echo "$list" | xargs -P "$PARALLEL" -I{} bash -c '
    p="$1"; dest="$2"; kind="$3"
    base="$(basename "$p")"
    curl -fsSL "https://data.commoncrawl.org/${p}" \
      | gsutil -q cp - "${dest}/${kind}/${base}"
    echo "    done ${base}"
  ' _ {} "$DEST" "$kind"
}

stage_set vertices
stage_set edges
echo "==> staged ${RELEASE} -> ${DEST}"
