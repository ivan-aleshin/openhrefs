#!/usr/bin/env bash
# Submit a PySpark job to Dataproc Serverless.
#
# Usage: ./infra/gcp/submit_job.sh <path/to/main.py> [job_args...]
# Example:
#   ./infra/gcp/submit_job.sh spark_jobs/hello/main.py \
#     --cdx-path "gs://commoncrawl/cc-index/collections/CC-MAIN-2024-51/indexes/cdx-00000.gz"
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
ZIP_JOBS="/tmp/openhrefs_spark_jobs.zip"
ZIP_DEPS="/tmp/openhrefs_pypackages.zip"
DEPS_DIR="/tmp/openhrefs_pypackages"

rm -f "$ZIP_JOBS" "$ZIP_DEPS"

echo "==> Packaging spark_jobs/"
zip -r "$ZIP_JOBS" spark_jobs/ -x "**/__pycache__/*" -x "**/*.pyc" -q

echo "==> Installing Python dependencies"
rm -rf "$DEPS_DIR"
uv export --format requirements-txt --no-hashes --no-dev -q \
  | uv pip install -r /dev/stdin --target "$DEPS_DIR" -q
(cd "$DEPS_DIR" && zip -r "$ZIP_DEPS" . -x "**/__pycache__/*" -x "**/*.pyc" -x "*.dist-info/*" -q)

echo "==> Uploading to ${DEPS_GCS}/"
gsutil -q cp "$ZIP_JOBS" "${DEPS_GCS}/spark_jobs.zip"
gsutil -q cp "$ZIP_DEPS" "${DEPS_GCS}/pypackages.zip"

echo "==> Submitting ${SCRIPT} to Dataproc Serverless"
echo "    project=${PROJECT}  region=${REGION}"

gcloud dataproc batches submit pyspark "$SCRIPT" \
  --project="$PROJECT" \
  --region="$REGION" \
  --service-account="$SA" \
  --deps-bucket="${DEPS_GCS%/*}" \
  --py-files="${DEPS_GCS}/spark_jobs.zip,${DEPS_GCS}/pypackages.zip" \
  --properties="spark.hadoop.fs.gs.requester.pays.mode=ENABLED,spark.hadoop.fs.gs.requester.pays.project.id=${PROJECT}" \
  -- "$@"
