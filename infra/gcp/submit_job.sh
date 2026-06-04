#!/usr/bin/env bash
# Submit a PySpark job to Dataproc Serverless.
#
# Usage: ./infra/gcp/submit_job.sh <path/to/main.py> [job_args...]
#
# Quick job (hello):
#   ./infra/gcp/submit_job.sh spark_jobs/hello/main.py \
#     --cdx-path "gs://.../CC-MAIN-2024-51/indexes/cdx-00000.gz"
#
# Iterative job (Stage 2/3) — size the run via env vars, use a GCS checkpoint dir:
#   DATAPROC_TTL=6h DATAPROC_MAX_EXECUTORS=50 DATAPROC_SHUFFLE_PARTITIONS=2000 \
#   ./infra/gcp/submit_job.sh spark_jobs/stage2_pagerank/main.py \
#     --edges-path gs://.../staged/v3_edges --vertices-path gs://.../staged/v3_map \
#     --crawl cc-main-2026-mar-apr-may --checkpoint-dir gs://.../checkpoints/stage2
#
# Dependencies ship in a custom container image (built by build_image.sh); only
# project code rides --py-files. Set DATAPROC_IMAGE to an explicit versioned tag.
#
# Env vars:
#   DATAPROC_IMAGE            custom container image (REQUIRED). Explicit versioned tag, never :latest.
#   DATAPROC_RUNTIME_VERSION  Serverless runtime, minor only (default 2.2; subminor is rejected).
#   DATAPROC_TTL              batch time-to-live (default 6h). The platform default (4h) silently
#                             CANCELs long iterative jobs — always set this for Stage 2/3.
#   DATAPROC_MAX_EXECUTORS    spark.dynamicAllocation.maxExecutors (added only if set).
#   DATAPROC_SHUFFLE_PARTITIONS  spark.sql.shuffle.partitions (added only if set).
#
# Requires: gcloud CLI, gsutil, zip. Run from repo root.

set -euo pipefail

SCRIPT="${1:?Usage: $0 <job_script> [job_args...]}"
shift

[ -f .env ] && set -a && source .env && set +a

PROJECT="${GCP_PROJECT:-$(gcloud config get-value project 2>/dev/null)}"
: "${PROJECT:?GCP_PROJECT not set and gcloud has no active project — run: gcloud config set project <project>}"
REGION="${GCP_REGION:-us-central1}"
SA="${DATAPROC_SA:?DATAPROC_SA not set — add to .env: DATAPROC_SA=<sa>@<project>.iam.gserviceaccount.com}"
DEPS_GCS="${DATAPROC_DEPS_BUCKET:?DATAPROC_DEPS_BUCKET not set — add to .env: DATAPROC_DEPS_BUCKET=gs://<bucket>/deps}"
IMAGE="${DATAPROC_IMAGE:?DATAPROC_IMAGE not set — build with infra/gcp/build_image.sh and pass an explicit versioned tag}"
case "${IMAGE##*/}" in
  *:latest) echo "DATAPROC_IMAGE must use an explicit versioned tag, not :latest" >&2; exit 1 ;;
  *:*) ;;
  *) echo "DATAPROC_IMAGE must include an explicit tag (image:tag)" >&2; exit 1 ;;
esac
RUNTIME_VERSION="${DATAPROC_RUNTIME_VERSION:-2.2}"
TTL="${DATAPROC_TTL:-6h}"
ZIP_JOBS="/tmp/openhrefs_spark_jobs.zip"

rm -f "$ZIP_JOBS"

echo "==> Packaging spark_jobs/"
zip -r "$ZIP_JOBS" spark_jobs/ -x "**/__pycache__/*" -x "**/*.pyc" -q

echo "==> Uploading to ${DEPS_GCS}/"
gsutil -q cp "$ZIP_JOBS" "${DEPS_GCS}/spark_jobs.zip"

# Requester-pays stays wired for future direct gs://commoncrawl reads (once billing
# access is unblocked). Stage 2/3 currently use staged graph inputs, so these properties
# are harmless there. Iterative-job tuning is appended only when the env var is set, so
# quick jobs (hello) don't inherit Stage 2/3 executor/shuffle sizing.
PROPS="spark.hadoop.fs.gs.requester.pays.mode=ENABLED,spark.hadoop.fs.gs.requester.pays.project.id=${PROJECT}"
if [ -n "${DATAPROC_MAX_EXECUTORS:-}" ]; then
  PROPS="${PROPS},spark.dynamicAllocation.maxExecutors=${DATAPROC_MAX_EXECUTORS}"
fi
if [ -n "${DATAPROC_SHUFFLE_PARTITIONS:-}" ]; then
  PROPS="${PROPS},spark.sql.shuffle.partitions=${DATAPROC_SHUFFLE_PARTITIONS}"
fi

echo "==> Submitting ${SCRIPT} to Dataproc Serverless"
echo "    project=${PROJECT}  region=${REGION}  version=${RUNTIME_VERSION}  ttl=${TTL}"
echo "    image=${IMAGE}"

gcloud dataproc batches submit pyspark "$SCRIPT" \
  --project="$PROJECT" \
  --region="$REGION" \
  --version="$RUNTIME_VERSION" \
  --ttl="$TTL" \
  --service-account="$SA" \
  --container-image="$IMAGE" \
  --deps-bucket="${DEPS_GCS%/*}" \
  --py-files="${DEPS_GCS}/spark_jobs.zip" \
  --files="config.yml,config/storage.yml" \
  --properties="$PROPS" \
  -- "$@"
