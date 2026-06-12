#!/usr/bin/env bash
# Build and push the dedicated dbt-runner Dataproc Serverless image.
#
# Usage: ./infra/gcp/build_dbt_image.sh [tag]
#   tag defaults to dbt-<YYYYMMDD>-<HEAD:dbt sha8>-<uv.lock sha8>-<Dockerfile.dbt sha8>
#   — keyed on every baked input (project tree + deps + recipe). Submit with an
#   explicit tag, never :latest.
#
# The dbt project (models/macros/profiles) is baked into the image from the
# COMMITTED tree, so rebuild when the project OR uv.lock changes (unlike the
# Stage 2/3 image, which only tracks deps). Commit dbt/ changes before building —
# the image reflects HEAD, not the working tree. dbt_packages are resolved here on
# the host and copied in, so the build needs neither pyspark nor a live dbt
# connection.
#
# Requires: docker, gcloud, uv, git. Run from repo root.

set -euo pipefail

[ -f .env ] && set -a && source .env && set +a

PROJECT="${GCP_PROJECT:-$(gcloud config get-value project 2>/dev/null)}"
: "${PROJECT:?GCP_PROJECT not set and gcloud has no active project}"
REGION="${GCP_REGION:-us-central1}"
REPO="${DATAPROC_IMAGE_REPO:-openhrefs}"
# Tag is keyed on every baked input: the dbt project tree (HEAD:dbt git object —
# changes iff any committed dbt/ file changes), the locked deps, and the recipe.
# Without the project tree sha, an SQL-only change would collide on the same tag.
DBT_TREE_SHA="$(git rev-parse --short=8 "HEAD:dbt")"
TAG="${1:-dbt-$(date +%Y%m%d)-${DBT_TREE_SHA}-$(sha256sum uv.lock | cut -c1-8)-$(sha256sum infra/gcp/Dockerfile.dbt | cut -c1-8)}"
IMAGE="${REGION}-docker.pkg.dev/${PROJECT}/${REPO}/openhrefs-dbt:${TAG}"

# The image bakes the committed dbt/ tree (HEAD:dbt). Refuse to build with
# uncommitted dbt/ changes, which would silently bake stale SQL into a prod image.
if [ -n "$(git status --porcelain -- dbt)" ]; then
  echo "ERROR: dbt/ has uncommitted changes — commit them first (the image bakes HEAD:dbt, not the working tree)." >&2
  exit 1
fi

CTX="$(mktemp -d)"
trap 'rm -rf "$CTX"' EXIT

echo "==> Exporting dbt-group requirements (excluding pyspark/py4j)"
uv export --only-group dbt --no-hashes --no-emit-project \
  | grep -ivE '^(pyspark|py4j)([=<> ]|$)' > "$CTX/requirements.txt"

echo "==> Resolving dbt_packages on the host (clean)"
rm -rf dbt/dbt_packages
( cd dbt && uv run dbt deps --quiet )
[ -d dbt/dbt_packages ] || { echo "ERROR: dbt deps did not produce dbt/dbt_packages" >&2; exit 1; }

echo "==> Staging dbt project (committed tree + freshly-resolved dbt_packages only)"
mkdir -p "$CTX/dbt"
# Committed project files only — git archive excludes ALL local cruft by
# construction: target/, logs/, metastore_db/, spark-warehouse/, derby.log,
# .user.yml (none are tracked).
git archive "HEAD:dbt" | tar -x -C "$CTX/dbt"
# dbt_packages are git-ignored but required at runtime; copy the freshly-resolved set.
cp -r dbt/dbt_packages "$CTX/dbt/dbt_packages"

cp infra/gcp/Dockerfile.dbt "$CTX/Dockerfile"

echo "==> Building ${IMAGE}"
docker build -t "$IMAGE" "$CTX"

echo "==> Pushing to Artifact Registry"
gcloud auth configure-docker "${REGION}-docker.pkg.dev" -q
docker push "$IMAGE"

echo "==> Done. Submit the mart build with:"
echo "    DATAPROC_IMAGE=${IMAGE} ./infra/gcp/submit_job.sh spark_jobs/dbt_runner/main.py \\"
echo "      --raw-path gs://openhrefs-data/raw --marts-path gs://openhrefs-data/marts --schema openhrefs"
