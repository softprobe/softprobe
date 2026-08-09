#!/usr/bin/env bash
set -euo pipefail
# Image build/push implementation for `make publish-docker`.
# Prefer the Make target (CI release.yml and local both use it).
#
# Usage (via Make):
#   make publish-docker TAG=v1.2.3
#   make publish-docker TAG=v1.2.3-rc.1 TAG_LATEST=0
#   PRINT_TAGS=1 ./build.sh v1.2.3         # print planned product tags only
#   PRINT_BUILDX_ARGS=1 ./build.sh v1.2.3  # print buildx argv (no docker)
#
# TAG defaults to latest. When TAG is not "latest" and TAG_LATEST is unset/true,
# also pushes :latest.
#
# BuildKit registry cache (Artifact Registry): intermediate layers including
# cargo-chef cook are stored at ${AR_IMAGE}:buildcache (not a runtime image —
# do not deploy this tag). Ephemeral GHA runners have no local layer cache;
# without registry cache every release cold-compiles dependencies (~7m).
# --cache-from miss is fine (BuildKit continues). --cache-to write failure
# fails the whole publish (fail-fast: release needs push rights on :buildcache).
# Registry cache export requires a non-default Buildx driver; this script
# ensures docker-container builder `thelake-builder`.

cd "$(dirname "$0")"

TAG="${1:-latest}"
TAG_LATEST="${TAG_LATEST:-1}"
AR_IMAGE="us-central1-docker.pkg.dev/cs-poc-sasxbttlzroculpau4u6e2l/softprobe/splake"
# Separate mutable tag: must not collide with product :latest / release tags.
CACHE_REF="${AR_IMAGE}:buildcache"
BUILDER_NAME="thelake-builder"

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

buildx_args=(
  --builder "${BUILDER_NAME}"
  --platform linux/amd64
  --cache-from "type=registry,ref=${CACHE_REF}"
  --cache-to "type=registry,ref=${CACHE_REF},mode=max"
  --push
  "${docker_tags[@]}"
  .
)

if [[ "${PRINT_BUILDX_ARGS:-0}" == "1" ]]; then
  printf '%s\n' "${buildx_args[@]}"
  exit 0
fi

ensure_cache_builder() {
  if ! docker buildx inspect "${BUILDER_NAME}" >/dev/null 2>&1; then
    echo "Creating Buildx builder ${BUILDER_NAME} (docker-container; required for registry cache)..."
    docker buildx create --name "${BUILDER_NAME}" --driver docker-container
  fi
  # Bootstrap so the first build doesn't race the builder container start.
  docker buildx inspect --bootstrap "${BUILDER_NAME}" >/dev/null
  local driver
  driver="$(docker buildx inspect "${BUILDER_NAME}" | awk '/^Driver:/{print $2; exit}')"
  if [[ "${driver}" != "docker-container" ]]; then
    echo "error: builder ${BUILDER_NAME} uses driver '${driver}'; need docker-container for --cache-to type=registry" >&2
    exit 1
  fi
}

ensure_cache_builder

echo "Building ${AR_IMAGE}:${TAG} (linux/amd64)${image_tags[1]:+ + :latest}"
echo "BuildKit cache: ${CACHE_REF} (builder ${BUILDER_NAME})"
docker buildx build "${buildx_args[@]}"
echo "Pushed ${image_tags[*]}"
