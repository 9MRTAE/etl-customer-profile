#!/usr/bin/env bash
# scripts/build.sh — Docker build + push to Artifact Registry
set -euo pipefail

REPO="<YOUR_REGISTRY>/docker-registry  # [SCRUBBED]"
IMAGE_NAME="etl-pms-customer"
BRANCH="${CI_COMMIT_BRANCH:-develop}"
COMMIT="${CI_COMMIT_SHORT_SHA:-local}"
IMAGE_TAG="${REPO}/${IMAGE_NAME}:${BRANCH}-${COMMIT}"

echo "Building ${IMAGE_TAG}"
docker build -t "${IMAGE_TAG}" .
docker push "${IMAGE_TAG}"
echo "Pushed ${IMAGE_TAG}"
export IMAGE_TAG
