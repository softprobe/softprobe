#!/usr/bin/env bash
set -euo pipefail
# Emergency / local only. Official images: create a GitHub Release with tag vX.Y.Z
# → .github/workflows/release.yml (same Softprobe convention as otel/backend).

cd "$(dirname "$0")"

TAG="${1:-latest}"
AR_IMAGE="us-central1-docker.pkg.dev/cs-poc-sasxbttlzroculpau4u6e2l/softprobe/splake"

if [[ "$TAG" == "latest" ]]; then
  docker buildx build --platform linux/amd64 --push -t "${AR_IMAGE}:latest" .
else
  docker buildx build --platform linux/amd64 --push \
    -t "${AR_IMAGE}:${TAG}" \
    -t "${AR_IMAGE}:latest" \
    .
fi
