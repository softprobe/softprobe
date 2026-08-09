#!/usr/bin/env bash
set -euo pipefail
# Image build/push implementation for `make publish-docker`.
# Prefer the Make target (CI release.yml and local both use it).
#
# Usage (via Make):
#   make publish-docker TAG=v1.2.3
#   make publish-docker TAG=v1.2.3-rc.1 TAG_LATEST=0
#   PRINT_TAGS=1 ./build.sh v1.2.3   # print planned tags only (no docker)
#
# TAG defaults to latest. When TAG is not "latest" and TAG_LATEST is unset/true,
# also pushes :latest.

cd "$(dirname "$0")"

TAG="${1:-latest}"
TAG_LATEST="${TAG_LATEST:-1}"
AR_IMAGE="us-central1-docker.pkg.dev/cs-poc-sasxbttlzroculpau4u6e2l/softprobe/splake"

image_tags=("${AR_IMAGE}:${TAG}")
case "${TAG_LATEST}" in
  0|false|FALSE|no|NO) ;;
  *)
    if [[ "${TAG}" != "latest" ]]; then
      image_tags+=("${AR_IMAGE}:latest")
    fi
    ;;
esac

if [[ "${PRINT_TAGS:-0}" == "1" ]]; then
  printf '%s\n' "${image_tags[@]}"
  exit 0
fi

docker_tags=()
for t in "${image_tags[@]}"; do
  docker_tags+=(-t "${t}")
done

echo "Building ${AR_IMAGE}:${TAG} (linux/amd64)${image_tags[1]:+ + :latest}"
docker buildx build --platform linux/amd64 --push "${docker_tags[@]}" .
echo "Pushed ${image_tags[*]}"
