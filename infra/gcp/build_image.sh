#!/usr/bin/env bash
# Build and push the Dataproc Serverless custom container image.
#
# Usage: ./infra/gcp/build_image.sh [tag]
#   tag defaults to py311-<YYYYMMDD>-<uv.lock sha8>-<Dockerfile sha8> — keyed on
#   the build inputs we control (deps + recipe). debian:12-slim and Miniforge
#   latest are unpinned upstream, so pass an explicit tag for a fully reproducible
#   pin. Submit jobs with an explicit tag, never :latest — part of the submit contract.
#
# One-time Artifact Registry repo (run once per project):
#   gcloud artifacts repositories create "${DATAPROC_IMAGE_REPO:-openhrefs}" \
#     --repository-format=docker --location="${GCP_REGION:-us-central1}"
#
# Rebuild only when uv.lock changes (deps are decoupled from project code,
# which ships via --py-files at submit time). Requires: docker, gcloud, uv.

set -euo pipefail

[ -f .env ] && set -a && source .env && set +a

PROJECT="${GCP_PROJECT:-$(gcloud config get-value project 2>/dev/null)}"
: "${PROJECT:?GCP_PROJECT not set and gcloud has no active project}"
REGION="${GCP_REGION:-us-central1}"
REPO="${DATAPROC_IMAGE_REPO:-openhrefs}"
TAG="${1:-py311-$(date +%Y%m%d)-$(sha256sum uv.lock | cut -c1-8)-$(sha256sum infra/gcp/Dockerfile | cut -c1-8)}"
IMAGE="${REGION}-docker.pkg.dev/${PROJECT}/${REPO}/openhrefs-spark:${TAG}"

CTX="$(mktemp -d)"
trap 'rm -rf "$CTX"' EXIT

echo "==> Exporting runtime requirements (excluding pyspark/py4j)"
uv export --format requirements-txt --no-hashes --no-dev \
  | grep -ivE '^(pyspark|py4j)([=<> ]|$)' > "$CTX/requirements.txt"
cp infra/gcp/Dockerfile "$CTX/Dockerfile"

echo "==> Building ${IMAGE}"
docker build -t "$IMAGE" "$CTX"

echo "==> Pushing to Artifact Registry"
gcloud auth configure-docker "${REGION}-docker.pkg.dev" -q
docker push "$IMAGE"

echo "==> Done. Submit with:"
echo "    DATAPROC_IMAGE=${IMAGE} ./infra/gcp/submit_job.sh <job> ..."
