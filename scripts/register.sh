#!/usr/bin/env bash
# scripts/register.sh — run deploy.py inside the Docker container
set -euo pipefail

docker run --rm \
  -e CI_COMMIT_BRANCH="${CI_COMMIT_BRANCH}" \
  -e IMAGE_TAG="${IMAGE_TAG}" \
  -e PREFECT_API_URL="${PREFECT_API_URL}" \
  -e PREFECT_WORK_POOL="${PREFECT_WORK_POOL:-kubernetes-pool}" \
  -e PREFECT_WORK_QUEUE="${PREFECT_WORK_QUEUE:-default}" \
  -e SECRET_PROJECT_ID="${SECRET_PROJECT_ID:-your-gcp-project-id}" \
  "${IMAGE_TAG}" \
  python deploy.py
