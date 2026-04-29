#!/usr/bin/env bash
set -euo pipefail
# Push to Artifact Registry only — do not revert to gcr.io (see repo .github/workflows/softprobe-publish.yml).

cd "$(dirname "$0")"

AR_IMAGE="us-central1-docker.pkg.dev/cs-poc-sasxbttlzroculpau4u6e2l/softprobe/splake"
docker buildx build --platform linux/amd64 --push -t "${AR_IMAGE}:latest" .